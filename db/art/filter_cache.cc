#include "filter_cache.h"

namespace ROCKSDB_NAMESPACE {

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

void FilterCache::enable_for_segments(std::unordered_map<uint32_t, uint16_t>& segments_units_num) {
    filter_cache_mutex_.lock();
    for (auto it = segments_units_num.begin(); it != segments_units_num.end(); it ++) {
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

}