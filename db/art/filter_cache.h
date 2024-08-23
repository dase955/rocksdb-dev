#pragma once

#include <iostream>
#include <fstream>
#include <mutex>
#include <cassert>
#include <vector>
#include <map>
#include <set>
#include <unordered_map>
#include "macros.h"
#include "greedy_algo.h"
#include "clf_model.h"
#include "heat_buckets.h"
#include "filter_cache_heap.h"
#include "filter_cache_item.h"

namespace ROCKSDB_NAMESPACE {

class FilterCache;
class FilterCacheManager;

// FilterCache main component is a STL Map, key -- segment id, value -- Structure of Filter Units （ called FilterCacheItem）
// its main job is auto enable/disable filter units for one segment, and check whether one key exists in enabled units
// its work is below:
// 1. enable / disable units for a batch of segments (one segment may not exist in FilterCache)
// 2. check whether one given key exists in one segment
// 3. check whether filter cache is approximately full
// 4. check whether ready to train first model
// 5. release FilterCacheItem of these merged (outdated) segments
class FilterCache {
private:
    std::map<uint32_t, FilterCacheItem> filter_cache_;
    uint32_t used_space_size_;
    uint32_t cache_size_; // max size of cache
    std::mutex filter_cache_mutex_;

public:
    FilterCache() { filter_cache_.clear(); cache_size_ = CACHE_SPACE_SIZE; used_space_size_ = 0; }

    ~FilterCache() { /* do nothing */ }

    // check whether one given key exist in one segment
    bool check_key(const uint32_t& segment_id, const std::string& key);

    // enable / disable units for a batch of segments (one segment may not exist in FilterCache)
    // if enabled units num exceed given units num, it will disable units
    void enable_for_segments(std::unordered_map<uint32_t, uint16_t>& segment_units_num_recorder);

    // the only difference from enable_for_segments is:
    // this func dont insert any filter units for segments that dont exist in cache, but enable_for_segments unc does
    void update_for_segments(std::unordered_map<uint32_t, uint16_t>& segment_units_num_recorder);

    // check whether filter cache is approximately full
    // actually, we will leave (1-FULL_RATE) * cache_size_ space for emergency usage
    bool is_full();

    // check whether ready to train first model
    bool is_ready();

    // release filter units of merged segments
    void release_for_segments(std::vector<uint32_t>& segment_ids);
};

// FilterCacheManager主体为FilterCache
// 其为一个Map，Key为segment id，Value为一个Segment的Filter Units的相关结构，其能自行启用/禁用Unit，并可以输入key判断存在性
// 其次还需维护HeatBuckets、FilterCacheHeapManager、ClfModel
// 功能如下：
// 1. 实时记录每一个可用的Segment的访问频数来更新周期
// 2. 根据周期更新HeatBuckets和ClfModel
// 3. 一旦更新ClfModel，重新预测并对FilterCacheHeapManager里的Heap进行更新 (工作负载发生改变)
// 4. 如果有旧的Segment被合并，同时必须有新的Segment，新的Segment将继承其访问频数
// 5. 新的Segment将用ClfModel预测并插入到FilterCacheHeapManager里的Heap中，先尽量插入unit，后台的Heap会自动调整，来为新的Segment自适应启用Units
// 6. 同时维护上一个大周期的访问频数，一旦这个周期里Segment被合并，其访问频数会继承到子Segment上

// FilterCacheManager is combined of these components:
// HeatBuckets, ClfModel, FilterCacheHeapManager, ...
// its work is below:
// 1. input segment id and target key, check whether target key exist in this segment 
// 2. if one check done, add 1 to the get cnt of this segment. we need to maintain get cnts record in last long period and current long period
// 3. Inherit: convert get cnts of merged segments to get cnts of newly generated segments (get cnts in both last long period and current long period)
// 4. use existing counts of segments in last and current long period to estimate a approximate get cnt for one alive segment
// 5. record total get cnt and update short periods, reminded TRAIN_PERIODS short periods is a long period
// 6. if a short period end, update HeatBuckets
// 7. if a long period end, use greedy algorithm to solve filter units allocation problem and evaluate old model with this solution. if model doesnt work well, retrain it
// 8. if model still works well or model already retrained, we predict new ideal units num for current segments, release unnecessary filter units , add necessary filter unit and update FIlterCacheHeap
// 9. if old segments are merged, remove related filter units from FilterCache and FilterCacheHeap
// TODO: 10. if new segments are generated, we need to predict ideal units num for them, insert filter units (if cache have space left) and update FIlterCacheHeap.
// TODO: 11. after one short period end (but current long period not end), estimate current segments' approximate get cnts, then use these estimated get cnts to update FIlterCacheHeap 
// TODO: 12. before FilterCache becomes full for the first time, just set default units num for every segments and insert filter units for segments
// TODO: 13. After FilterCache becomes full for the first time, start a background thread to monitor FitlerCacheHeap and use two-heaps adjustment to optimize FilterCache (this thread never ends)
class FilterCacheManager {
private:
    // TODO: mutex can be optimized or use a message queue or a thread pool to reduce time costed by mutex
    static FilterCache filter_cache_;
    static HeatBuckets heat_buckets_;
    static ClfModel clf_model_;
    static GreedyAlgo greedy_algo_;
    static FilterCacheHeapManager heap_manager_;
    static uint32_t get_cnt_; // record get cnt in current period, when exceeding PERIOD_COUNT, start next period
    static uint32_t period_cnt_; // record period cnt, if period_cnt_ - last_train_period_ >= TRAIN_PERIODS, start to evaluate or retrain ClfModel
    static uint32_t last_long_period_; // record last short period cnt of last long period
    static std::mutex update_mutex_; // guarantee counts records only updated once
    static bool train_signal_; // if true, try to retrain model. we call one background thread to monitor this flag and retrain
    static std::map<uint32_t, uint32_t> last_count_recorder_; // get cnt recorder of segments in last long period
    static std::map<uint32_t, uint32_t> current_count_recorder_; // get cnt recorder of segments in current long period
    static std::mutex count_mutex_; // guarentee last_count_recorder and current_count_recorder treated orderedly
public:
    FilterCacheManager() { get_cnt_ = 0; train_signal_ = false; };

    ~FilterCacheManager();

    bool need_retrain() { return train_signal_; }

    // input segment id and target key, check whether target key exist in this segment 
    // return true when target key may exist (may cause false positive fault)
    // if there is no cache item for this segment, always return true
    bool check_key(const uint32_t& segment_id, const std::string& key);

    // add 1 to get cnt of specified segment in current long period
    void hit_count_recorder(const uint32_t& segment_id);

    // copy counts to last_count_recorder and reset counts of current_count_recorder
    void update_count_recorder();

    // inherit counts of merged segments to counts of new segments and remove counts of merged segments
    // inherit_infos_recorder: { {new segment 1: [{old segment 1: inherit rate 1}, {old segment 2: inherit rate 2}, ...]}, ...}
    void inherit_count_recorder(std::vector<uint32_t>& merged_segment_ids, std::vector<uint32_t>& new_segment_ids,
                                std::map<uint32_t, std::unordered_map<uint32_t, double>>& inherit_infos_recorder);

    // estimate approximate get cnts for every alive segment
    void estimate_count(std::map<uint32_t, uint32_t>& approximate_counts_recorder);

    // noticed that at the beginning, heat buckets need to sample put keys to init itself before heat buckets start to work
    // segment_info_recorder is external variable that records every alive segments' min key and max key
    // it is like { segment 1: [min_key_1, max_key_1], segment 2: [min_key_2, max_key_2], ... }
    // return true when heat_buckets is ready, so no need to call this func again
    bool make_heat_buckets_ready(const std::string& key, std::unordered_map<uint32_t, std::vector<std::string>>& segment_info_recorder);

    // add 1 to get cnt of target key range for every get operation
    // update short periods if get cnt exceeds PERIOD_COUNT
    // every get opt will make add 1 to only one heat bucket counter 
    // also need to update count records if one long period end
    // TODO: we should call one thread to exec this func, reducing tail latency
    void hit_heat_buckets(const std::string& key);

    // if one long period end, we need to check effectiveness of model. 
    // if model doesnt work well in current workload, we retrain this model
    // 1. use greedy algorithm to solve filter units allocation problem (receive ideal enabled units num for every current segments)
    // 2. write filter units nums and segment-related features to a csv file
    // 3. python lightgbm server maintain latest model. it will read the csv file and use I/O cost metric to check effectiveness of this model
    // 4. if effectiveness check not pass, retrain this model
    // reminded that if this new model training not end, lightgbm still use old model to predict ideal units num for segments
    // level_recorder: { segment 1: level_1, segment 2: level_2, ... }, level_k is the index of LSM-Tree level (0, 1, 2, ...)
    // range_heat_recorder: { segment 1: range_id_1, ...}, ... },
    // unit_size_recorder: { segment 1: unit_size_1, segment 2: unit_size_2, ... }
    // Attention: we assume for every segment, its ranges in range_heat_recorder value must be unique!!!
    // all 3 recorders need to maintain all current segments info, and their keys size and keys set should be the same (their keys are segment ids)
    // TODO 1: because of the time cost of writing csv file, we need to do this func with a background thread
    // TODO 2: need real benchmark data to debug this func
    void try_retrain_model(std::map<uint32_t, uint16_t>& level_recorder,
                           std::map<uint32_t, std::vector<uint32_t>>& segment_ranges_recorder,
                           std::map<uint32_t, uint32_t>& unit_size_recorder);

    // after one long period end, we may retrain model. when try_retrain_model func end, we need to predict units num for every segments
    // then use these units num to update FilterCache, at last update units num limit in FilterCacheHeap
    // TODO: only be called after try_retrain_model (should be guaranteed)
    // TODO: we can guarantee this by putting try_retrain_model and update_cache into one background thread
    void update_cache_and_heap(std::map<uint32_t, uint16_t>& level_recorder,
                               std::map<uint32_t, std::vector<uint32_t>>& segment_ranges_recorder);

    // remove merged segments' filter units in the filter cache
    // also remove related items in FilterCacheHeap
    // TODO: should be called by one background thread
    void remove_segments(std::vector<uint32_t>& segment_ids);
};