#pragma once

#include <iostream>
#include <fstream>
#include <mutex>
#include <cassert>
#include <vector>
#include <map>
#include <unordered_map>
#include "macros.h"
#include "greedy_algo.h"
#include "clf_model.h"
#include "heat_buckets.h"
#include "filter_cache_heap.h"
#include "filter_cache_item.h"

namespace ROCKSDB_NAMESPACE {

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


// FilterCache main component is a STL Map, key -- segment id, value -- Structure of Filter Units （ called FilterCacheItem）
// its main job is auto enable/disable filter units for one segment, and check whether one key exists in enabled units
// its work is below:
// 1. enable / disable units for a batch of segments (one segment may not exist in FilterCache)
// 2. check whether one given key exist in one segment
// 3. check whether filter cache is approximately full
// 4. check whether ready to start tow-heaps units adjustment
class FilterCache {
private:
    std::map<uint32_t, FilterCacheItem> filter_cache_;
    uint32_t used_space_size_;
    uint32_t cache_size_; // max size of cache
    std::mutex filter_cache_mutex_;

public:
    FilterCache(const uint32_t& cache_size) { filter_cache_.clear(); cache_size_ = cache_size; used_space_size_ = 0; }

    ~FilterCache() { /* do nothing */ }

    // check whether one given key exist in one segment
    bool check_key(const uint32_t& segment_id, const std::string& key);

    // enable / disable units for a batch of segments (one segment may not exist in FilterCache)
    // if enabled units num exceed given units num, it will disable units
    void enable_for_segments(std::unordered_map<uint32_t, uint16_t>& segments_units_num);

    // check whether filter cache is approximately full
    // actually, we will leave (1-FULL_RATE) * cache_size_ space for emergency usage
    bool is_full();

    // check whether ready to start tow-heaps units adjustment
    bool is_ready();
};

}