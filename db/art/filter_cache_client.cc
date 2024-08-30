#include "filter_cache_client.h"

namespace ROCKSDB_NAMESPACE {

task_thread_pool::task_thread_pool FilterCacheClient::pool_{FILTER_CACHE_THREADS_NUM};
FilterCacheManager FilterCacheClient::filter_cache_manager_;
bool FilterCacheClient::filter_cache_ready_; 
bool FilterCacheClient::heat_buckets_ready_;

bool FilterCacheClient::do_prepare_heat_buckets(const std::string& key, std::unordered_map<uint32_t, std::vector<std::string>>& segment_info_recorder) {
    return filter_cache_manager_.make_heat_buckets_ready(key, segment_info_recorder);
}

bool FilterCacheClient::prepare_heat_buckets(const std::string& key, std::unordered_map<uint32_t, std::vector<std::string>>& segment_info_recorder) {
    if (!heat_buckets_ready_) {
        // if heat_buckets_ready_ false
        pool_.submit_detach(do_prepare_heat_buckets, key, segment_info_recorder);
        heat_buckets_ready_ = filter_cache_manager_.heat_buckets_ready();
    }
    return heat_buckets_ready_;
}

}