#include "filter_cache.h"
#include <algorithm>

namespace ROCKSDB_NAMESPACE {

FilterCache FilterCacheManager::filter_cache_;
HeatBuckets FilterCacheManager::heat_buckets_;
ClfModel FilterCacheManager::clf_model_;
uint32_t FilterCacheManager::get_cnt_;
uint32_t FilterCacheManager::period_cnt_;
std::mutex FilterCacheManager::train_mutex_;
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

bool FilterCache::is_full() {
    return double(used_space_size_) / double(cache_size_) >= FULL_RATE;
}

bool FilterCache::is_ready() {
    return double(used_space_size_) / double(cache_size_) >= READY_RATE;
}

void FilterCache::enable_for_segments(std::vector<uint32_t>& segment_ids) {
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
}

void FilterCacheManager::try_retrain_model(std::map<uint32_t, uint16_t>& level_recorder,
                                           std::map<uint32_t, std::vector<RangeHeatPair>>& range_heat_recorder,
                                           std::map<uint32_t, uint32_t>& get_cnt_recorder) {
    
}

bool FilterCacheManager::check_key(const uint32_t& segment_id, const std::string& key) {
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

void FilterCacheManager::inherit_count_recorder(std::map<uint32_t, std::unordered_map<uint32_t, double>>& inherit_infos_recorder) {

}

void FilterCacheManager::estimate_count(std::map<uint32_t, uint32_t>& approximate_counts_recorder) {
    const uint32_t long_period_total_count = TRAIN_PERIODS * PERIOD_COUNT;
    uint32_t current_long_period_count = PERIOD_COUNT * (period_cnt_ % TRAIN_PERIODS) + get_cnt_;
    double current_long_period_rate = double(current_long_period_count) / double(long_period_total_count);

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

}