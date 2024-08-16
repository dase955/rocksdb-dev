#include "filter_cache.h"

namespace ROCKSDB_NAMESPACE {

HeatBuckets FilterCacheManager::heat_buckets_;
ClfModel FilterCacheManager::clf_model_;
uint32_t FilterCacheManager::get_cnt_;
uint32_t FilterCacheManager::period_cnt_;
std::mutex FilterCacheManager::train_mutex_;

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


void FilterCacheManager::make_heat_buckets_ready(const std::string& key, 
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

void FilterCacheManager::hit_one_heat_bucket(const std::string& key) {
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



}