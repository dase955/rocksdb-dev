#include "filter_cache.h"
#include <algorithm>

namespace ROCKSDB_NAMESPACE {

FilterCache FilterCacheManager::filter_cache_;
HeatBuckets FilterCacheManager::heat_buckets_;
ClfModel FilterCacheManager::clf_model_;
GreedyAlgo FilterCacheManager::greedy_algo_;
FilterCacheHeapManager FilterCacheManager::heap_manager_;
uint32_t FilterCacheManager::get_cnt_;
uint32_t FilterCacheManager::period_cnt_;
uint32_t FilterCacheManager::last_long_period_;
std::mutex FilterCacheManager::update_mutex_;
bool FilterCacheManager::train_signal_;
std::map<uint32_t, uint32_t> FilterCacheManager::last_count_recorder_; 
std::map<uint32_t, uint32_t> FilterCacheManager::current_count_recorder_; 
std::mutex FilterCacheManager::count_mutex_;

bool FilterCache::check_key(const uint32_t& segment_id, const std::string& key) {
    auto it = filter_cache_.find(segment_id);
    if (it == filter_cache_.end()) {
        // not in cache, that means we havent insert segment FilterCacheItem info into cache
        // actually, we start inserting after every segment becomes available
        return true;
    } else {
        return (it->second).check_key(key);
    }
}

void FilterCache::enable_for_segments(std::unordered_map<uint32_t, uint16_t>& segment_units_num_recorder) {
    filter_cache_mutex_.lock();
    for (auto it = segment_units_num_recorder.begin(); it != segment_units_num_recorder.end(); it ++) {
        const uint32_t segment_id = it->first;
        const uint16_t units_num = it->second;
        auto cache_it = filter_cache_.find(segment_id);
        if (cache_it != filter_cache_.end()) {
            // filter units cached
            const uint32_t old_size = (cache_it->second).approximate_size();
            (cache_it->second).enable_units(units_num);
            used_space_size_ = used_space_size_ - old_size + (cache_it->second).approximate_size();
        } else {
            // filter units not cached
            // now cache it
            FilterCacheItem cache_item(units_num);
            filter_cache_.insert(std::make_pair(segment_id, cache_item));
            used_space_size_ = used_space_size_ + cache_item.approximate_size();
        }
    }
    filter_cache_mutex_.unlock();
}

void FilterCache::update_for_segments(std::unordered_map<uint32_t, uint16_t>& segment_units_num_recorder) {
    filter_cache_mutex_.lock();
    for (auto it = segment_units_num_recorder.begin(); it != segment_units_num_recorder.end(); it ++) {
        const uint32_t segment_id = it->first;
        const uint16_t units_num = it->second;
        auto cache_it = filter_cache_.find(segment_id);
        if (cache_it != filter_cache_.end()) {
            // filter units cached
            const uint32_t old_size = (cache_it->second).approximate_size();
            (cache_it->second).enable_units(units_num);
            used_space_size_ = used_space_size_ - old_size + (cache_it->second).approximate_size();
        } else {
            // filter units not cached
            // do nothing!!!
        }
    }
    filter_cache_mutex_.unlock();
}

bool FilterCache::is_full() {
    return double(used_space_size_) / double(cache_size_) >= FULL_RATE;
}

bool FilterCache::is_ready() {
    return double(used_space_size_) / double(cache_size_) >= READY_RATE;
}

void FilterCache::release_for_segments(std::vector<uint32_t>& segment_ids) {
    std::sort(segment_ids.begin(), segment_ids.end());
    // delete key-value pair in filter_cache_
    filter_cache_mutex_.lock();
    auto it = filter_cache_.begin();
    size_t idx = 0;
    while(it != filter_cache_.end() && idx < segment_ids.size()) {
        if (it->first < segment_ids[idx]) {
            it ++;
        } else if (it->first > segment_ids[idx]) {
            idx ++;
        } else {
            used_space_size_ = used_space_size_ - (it->second).approximate_size();
            it = filter_cache_.erase(it);
        }
    }
    filter_cache_mutex_.unlock();
}

bool FilterCacheManager::make_heat_buckets_ready(const std::string& key, 
                                                 std::unordered_map<uint32_t, std::vector<std::string>>& segment_info_recorder) {
    // heat_buckets not ready, still sample into pool
    if (!heat_buckets_.is_ready()) {
        std::vector<std::vector<std::string>> segments_infos;
        for (auto it = segment_info_recorder.begin(); it != segment_info_recorder.end(); it ++) {
            assert((it->second).size() == 2);
            segments_infos.emplace_back(it->second);
        }
        heat_buckets_.sample(key, segments_infos);
    }
}

void FilterCacheManager::hit_heat_buckets(const std::string& key) {
    if (heat_buckets_.is_ready()) {
        get_cnt_ += 1;
        if (get_cnt_ >= PERIOD_COUNT) {
            heat_buckets_.hit(key, true);
            get_cnt_ = 0;
            period_cnt_ += 1;
        } else {
            heat_buckets_.hit(key, false);
        }
    }
    if (period_cnt_ - last_long_period_ >= TRAIN_PERIODS) {
        update_mutex_.lock();

        if (period_cnt_ - last_long_period_ >= TRAIN_PERIODS) {
            last_long_period_ = period_cnt_;
            update_count_recorder();
            train_signal_ = true;
        }

        update_mutex_.unlock();
    }
    if (period_cnt_ - last_short_period_ >= 1) {
        update_mutex_.lock();

        if (period_cnt_ - last_short_period_ >= 1) {
            last_short_period_ = period_cnt_;
            std::map<uint32_t, uint32_t> estimate_count_recorder;
            estimate_counts_for_all(estimate_count_recorder);
            heap_manager_.sync_visit_cnt(estimate_count_recorder);
        }

        update_mutex_.unlock();
    }
}

bool FilterCacheManager::check_key(const uint32_t& segment_id, const std::string& key) {
    hit_count_recorder(segment_id); // one get opt will cause query to many segments.
    // so one get opt only call one hit_heat_buckets, but call many hit_count_recorder
    return filter_cache_.check_key(segment_id, key);
}

void FilterCacheManager::hit_count_recorder(const uint32_t& segment_id) {
    count_mutex_.lock();

    auto it = current_count_recorder_.find(segment_id);
    if (it == current_count_recorder_.end()) {
        // segment havent been visited, need to insert count
        current_count_recorder_.insert(std::make_pair(segment_id, 1));
    } else {
        // segment have been visited, only update count
        it->second = it->second + 1;
    }

    count_mutex_.unlock();
}

void FilterCacheManager::update_count_recorder() {
    count_mutex_.lock();

    last_count_recorder_.clear();
    last_count_recorder_.insert(current_count_recorder_.begin(), current_count_recorder_.end());
    for (auto it = current_count_recorder_.begin(); it != current_count_recorder_.end(); it++) {
        it->second = 0;
    }

    count_mutex_.unlock();
}

void FilterCacheManager::inherit_count_recorder(std::vector<uint32_t>& merged_segment_ids, std::vector<uint32_t>& new_segment_ids,
                                                std::map<uint32_t, std::unordered_map<uint32_t, double>>& inherit_infos_recorder) {
    std::map<uint32_t, uint32_t> merged_last_count_recorder, merged_current_count_recorder; // cache merged segment count temporarily
    for (uint32_t& merged_segment_id : merged_segment_ids) {
        merged_last_count_recorder.insert(std::make_pair(merged_segment_id, last_count_recorder_[merged_segment_id]));
        last_count_recorder_.erase(merged_segment_id);
        merged_current_count_recorder.insert(std::make_pair(merged_segment_id, current_count_recorder_[merged_segment_id]));
        current_count_recorder_.erase(merged_segment_id);
    }

    std::map<uint32_t, uint32_t> new_last_count_recorder, new_current_count_recorder;
    for (auto infos_it = inherit_infos_recorder.begin(); infos_it != inherit_infos_recorder.end(); infos_it ++) {
        double last_count = 0, current_count = 0;
        std::unordered_map<uint32_t, double>& info = infos_it->second;
        for (auto info_it = info.begin(); info_it != info.end(); info_it ++) {
            last_count = last_count + (merged_last_count_recorder[info_it->first] * info_it->second);
            current_count = current_count + (merged_current_count_recorder[info_it->first] * info_it->second);
        }
        new_last_count_recorder.insert(std::make_pair(infos_it->first, uint32_t(last_count)));
        new_current_count_recorder.insert(std::make_pair(infos_it->first, uint32_t(current_count)));
    }

    count_mutex_.lock();

    for (uint32_t& new_segment_id : new_segment_ids) {
        auto last_it = last_count_recorder_.find(new_segment_id);
        if (last_it != last_count_recorder_.end()) {
            last_it->second = last_it->second + new_last_count_recorder[new_segment_id];
        } else {
            last_count_recorder_.insert(std::make_pair(new_segment_id, new_last_count_recorder[new_segment_id]));
        }

        auto current_it = current_count_recorder_.find(new_segment_id);
        if (current_it != current_count_recorder_.end()) {
            current_it->second = current_it->second + new_current_count_recorder[new_segment_id];
        } else {
            current_count_recorder_.insert(std::make_pair(new_segment_id, new_current_count_recorder[new_segment_id]));
        }
    }

    count_mutex_.unlock();
}

void FilterCacheManager::estimate_counts_for_all(std::map<uint32_t, uint32_t>& approximate_counts_recorder) {
    const uint32_t long_period_total_count = TRAIN_PERIODS * PERIOD_COUNT;
    uint32_t current_long_period_count = PERIOD_COUNT * (period_cnt_ % TRAIN_PERIODS) + get_cnt_;
    double current_long_period_rate = std::min(double(current_long_period_count) / double(long_period_total_count), 1.0);

    approximate_counts_recorder.clear();
    approximate_counts_recorder.insert(current_count_recorder_.begin(), current_count_recorder_.end());
    auto approx_it = approximate_counts_recorder.begin();
    auto last_it = last_count_recorder_.begin();
    while (approx_it != approximate_counts_recorder.end() && last_it != last_count_recorder_.end()) {
        if (approx_it->first > last_it->first) {
            last_it ++;
        } else if(approx_it->first < last_it->first) {
            approx_it ++;
        } else {
            approx_it->second = approx_it->second + uint32_t((1 - current_long_period_rate) * last_it->second);
            approx_it ++;
        }
    }

    // return nothing, already write result to approximate_counts_recorder
}


void FilterCacheManager::try_retrain_model(std::map<uint32_t, uint16_t>& level_recorder,
                                           std::map<uint32_t, std::vector<uint32_t>>& segment_ranges_recorder,
                                           std::map<uint32_t, uint32_t>& unit_size_recorder) {
    // we should guarantee these 3 external recorder share the same keys set
    // we need to do this job outside FilterCacheManager
    assert(level_recorder.size() == segment_ranges_recorder.size());
    assert(level_recorder.size() == unit_size_recorder.size());
    if (train_signal_ == false) {
        return;
    }

    // solve programming problem
    std::map<uint32_t, uint16_t> label_recorder;
    std::map<uint32_t, SegmentAlgoInfo> algo_infos;
    auto get_cnt_it = last_count_recorder_.begin();
    auto unit_size_it = unit_size_recorder.begin();
    while (unit_size_it != unit_size_recorder.end() && get_cnt_it != last_count_recorder_.end()) {
        if (unit_size_it->first > get_cnt_it->first) {
            get_cnt_it ++;
        } else if (unit_size_it->first < get_cnt_it->first) {
            unit_size_it ++;
        } else {
            algo_infos.insert(std::make_pair(unit_size_it->first, SegmentAlgoInfo(get_cnt_it->second, unit_size_it->second)));
            unit_size_it ++;
        }
    }
    greedy_algo_.solve(algo_infos, label_recorder, CACHE_SPACE_SIZE * FULL_RATE);

    // programming problem may not include some latest segments, we need to ignore them
    auto old_level_it = level_recorder.begin();
    auto old_range_it = segment_ranges_recorder.begin();
    auto old_label_it = label_recorder.begin();
    while (old_level_it != level_recorder.end() && 
           old_range_it != segment_ranges_recorder.end() && 
           old_label_it != label_recorder.end()) {
        assert(old_level_it->first == old_range_it->first);
        if (old_level_it->first != old_label_it->first) {
            old_level_it = level_recorder.erase(old_label_it);
            old_range_it = segment_ranges_recorder.erase(old_range_it);
        } else {
            old_level_it ++;
            old_range_it ++;
            old_label_it ++;
        }
    }

    std::vector<Bucket> buckets = heat_buckets_.buckets();
    std::vector<std::vector<uint32_t>> datas;
    std::vector<uint16_t> labels;
    std::vector<uint32_t> get_cnts;

    auto level_it = level_recorder.begin(); // key range id start with 0
    auto ranges_it = segment_ranges_recorder.begin();
    auto count_it = last_count_recorder_.begin();
    auto label_it = label_recorder.begin();
    while (level_it != level_recorder.end() && ranges_it != segment_ranges_recorder.end() &&
          count_it != last_count_recorder_.end() && label_it != label_recorder.end()) {
        assert(level_it->first == ranges_it->first);
        assert(level_it->first == label_it->first);
        if (count_it->first < level_it->first) {
            count_it ++;
        } else if (count_it->first > level_it->first) {
            level_it ++;
            ranges_it ++;
            label_it ++;
        } else {
            if (level_it->second > 0) {
                // add data row
                std::vector<uint32_t> data;
                data.emplace_back(level_it->second);
                for (uint32_t& range_id : ranges_it->second) {
                    assert(range_id >= 0 && range_id < buckets.size());
                    data.emplace_back(range_id);
                    data.emplace_back(uint32_t(SIGNIFICANT_DIGITS_FACTOR * buckets[range_id].hotness_));
                }
                datas.emplace_back(data);
                // add label row
                labels.emplace_back(label_it->second);
                // add get cnt row
                get_cnts.emplace_back(count_it->second);
            }

            level_it ++;
            ranges_it ++;
            label_it ++;
        }
    }

    clf_model_.make_train(datas, labels, get_cnts);

    train_signal_ = false;
}

void FilterCacheManager::update_cache_and_heap(std::map<uint32_t, uint16_t>& level_recorder,
                                               std::map<uint32_t, std::vector<uint32_t>>& segment_ranges_recorder) {
    assert(level_recorder.size() == segment_ranges_recorder.size());
    std::vector<uint32_t> segment_ids;
    std::vector<std::vector<uint32_t>> datas;
    std::vector<uint16_t> preds;
    std::unordered_map<uint32_t, uint16_t> segment_units_num_recorder;
    std::map<uint32_t, uint16_t> current_units_num_limit_recorder;
    std::vector<Bucket> buckets = heat_buckets_.buckets();

    // build data rows into datas
    auto level_it = level_recorder.begin();
    auto range_it = segment_ranges_recorder.begin();
    while (level_it != level_recorder.end() && range_it != segment_ranges_recorder.end()) {
        assert(level_it->first == range_it->first);

        if (level_it->second > 0) {
            segment_ids.emplace_back(level_it->first);

            std::vector<uint32_t> data;
            data.emplace_back(level_it->second);
            for (uint32_t& range_id : range_it->second) {
                assert(range_id >= 0 && range_id < buckets.size());
                data.emplace_back(range_id);
                data.emplace_back(uint32_t(SIGNIFICANT_DIGITS_FACTOR * buckets[range_id].hotness_));
            }
            datas.emplace_back(data);
        }

        level_it ++;
        range_it ++;
    }

    // use datas to make prediction
    clf_model_.make_predict(datas, preds);
    assert(segment_ids.size() == preds.size());
    size_t idx = 0;
    while (idx < segment_ids.size() && idx < preds.size()) {
        segment_units_num_recorder.insert(std::make_pair(segment_ids[idx], preds[idx]));
        current_units_num_limit_recorder.insert(std::make_pair(segment_ids[idx], preds[idx]));
        idx = idx + 1;
    }

    // update filter cache
    filter_cache_.update_for_segments(segment_units_num_recorder);

    // update filter cache helper heaps
    heap_manager_.sync_units_num_limit(current_units_num_limit_recorder);
}

void FilterCacheManager::remove_segments(std::vector<uint32_t>& segment_ids) {
    filter_cache_.release_for_segments(segment_ids);
    heap_manager_.batch_delete(segment_ids);
}

}