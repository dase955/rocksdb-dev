#pragma once

#include <iostream>
#include <cassert>
#include <task_thread_pool.hpp>
#include "macros.h"
#include "filter_cache.h" 

namespace ROCKSDB_NAMESPACE {

class FilterCacheClient;

class FilterCacheClient {
private:
    static task_thread_pool::task_thread_pool pool_;
    static FilterCacheManager filter_cache_manager_;
    // we need heat_buckets_ready_ to become true before filter_cache_ready_
    // In YCSB benchmark, we first load data (insert key-value pairs) then may try get operation
    // so we can guarantee that heat_buckets_ready_ become true before filter_cache_ready_
    static bool filter_cache_ready_; // the same as FilterCacheManager.is_ready_
    static bool heat_buckets_ready_; // the same as FilterCacheManager.heat_buckets_.is_ready()

    // background thread part of prepare_heat_buckets
    static bool do_prepare_heat_buckets(const std::string& key, std::unordered_map<uint32_t, std::vector<std::string>>& segment_info_recorder);

public:
    FilterCacheClient() {
        filter_cache_ready_ = false;
        heat_buckets_ready_ = false;
    }

    // corresponding to FilterCacheManager.make_heat_buckets_ready
    bool prepare_heat_buckets(const std::string& key, std::unordered_map<uint32_t, std::vector<std::string>>& segment_info_recorder);
};

}
