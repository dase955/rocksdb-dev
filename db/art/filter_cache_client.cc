#include "filter_cache_client.h"

namespace ROCKSDB_NAMESPACE {

task_thread_pool::task_thread_pool FilterCacheClient::pool_{FILTER_CACHE_THREADS_NUM};
FilterCacheManager FilterCacheClient::filter_cache_manager_;
bool FilterCacheClient::filter_cache_ready_; 
bool FilterCacheClient::heat_buckets_ready_;

void FilterCacheClient::do_prepare_heat_buckets(const std::string& key, std::unordered_map<uint32_t, std::vector<std::string>>*& segment_info_recorder) {
    filter_cache_manager_.make_heat_buckets_ready(key, *segment_info_recorder);
}

bool FilterCacheClient::prepare_heat_buckets(const std::string& key, std::unordered_map<uint32_t, std::vector<std::string>>*& segment_info_recorder) {
    heat_buckets_ready_ = filter_cache_manager_.heat_buckets_ready();
    if (!heat_buckets_ready_) {
        // if heat_buckets_ready_ false
        pool_.submit_detach(do_prepare_heat_buckets, key, segment_info_recorder);
        heat_buckets_ready_ = filter_cache_manager_.heat_buckets_ready();
    }
    return heat_buckets_ready_;
}

void FilterCacheClient::do_retrain_or_keep_model(std::vector<uint16_t>*& features_nums, 
                                                 std::map<uint32_t, uint16_t>*& level_recorder,
                                                 std::map<uint32_t, std::vector<RangeRatePair>>*& segment_ranges_recorder,
                                                 std::map<uint32_t, uint32_t>*& unit_size_recorder) {
    // if this func background monitor signal, how can it receive latest argument? input pointer!
    while (!filter_cache_manager_.ready_work()); // wait for manager ready
    assert(filter_cache_manager_.heat_buckets_ready()); // must guarantee that heat buckets ready before we make filter cache manager ready
    // actually we will load data before we test, so we can ensure that heat buckets ready first
    filter_cache_manager_.make_clf_model_ready(*features_nums);
    filter_cache_manager_.try_retrain_model(*level_recorder, *segment_ranges_recorder, *unit_size_recorder);
    filter_cache_manager_.update_cache_and_heap(*level_recorder, *segment_ranges_recorder);

    while (true) {
        // in one long period
        while (!filter_cache_manager_.need_retrain()); // wait for long period end
        filter_cache_manager_.try_retrain_model(*level_recorder, *segment_ranges_recorder, *unit_size_recorder);
        // python lgb_model server will decide on whether keep model or retrain model
        filter_cache_manager_.update_cache_and_heap(*level_recorder, *segment_ranges_recorder);
    }
    // this loop never end
}

void FilterCacheClient::retrain_or_keep_model(std::vector<uint16_t>*& features_nums, 
                                              std::map<uint32_t, uint16_t>*& level_recorder,
                                              std::map<uint32_t, std::vector<RangeRatePair>>*& segment_ranges_recorder,
                                              std::map<uint32_t, uint32_t>*& unit_size_recorder) {
    pool_.submit_detach(do_retrain_or_keep_model, features_nums, level_recorder, segment_ranges_recorder, unit_size_recorder);
    // if first model training not end, python lgb_model server still return default units num
    // then retrain model when every long period end. if model still work well, keep this model instead
    // no need to return any value
}

void FilterCacheClient::do_hit_count_recorder(const uint32_t& segment_id) {
    filter_cache_manager_.hit_count_recorder(segment_id);
}

bool FilterCacheClient::check_key(const uint32_t& segment_id, const std::string& key) {
    bool result = filter_cache_manager_.check_key(segment_id, key);
    pool_.submit_detach(do_hit_count_recorder, segment_id);
    return result;
}

void FilterCacheClient::do_hit_heat_buckets(const std::string& key) {
    filter_cache_manager_.hit_heat_buckets(key);
}

void FilterCacheClient::get_updating_work(const std::string& key) {
    pool_.submit_detach(do_hit_heat_buckets, key);
}

void FilterCacheClient::do_make_adjustment() {
    filter_cache_manager_.adjust_cache_and_heap();
}

void FilterCacheClient::make_adjustment() {
    pool_.submit_detach(do_make_adjustment);
}

void FilterCacheClient::do_batch_insert_segments(std::vector<uint32_t>*& merged_segment_ids, std::vector<uint32_t>*& new_segment_ids,
                                                 std::map<uint32_t, std::unordered_map<uint32_t, double>>*& inherit_infos_recorder,
                                                 std::map<uint32_t, uint16_t>*& level_recorder, const uint32_t& level_0_base_count,
                                                 std::map<uint32_t, std::vector<RangeRatePair>>*& segment_ranges_recorder) {
    filter_cache_manager_.insert_segments(*merged_segment_ids, *new_segment_ids, *inherit_infos_recorder,
                                          *level_recorder, level_0_base_count, *segment_ranges_recorder);
}

void FilterCacheClient::batch_insert_segments(std::vector<uint32_t>*& merged_segment_ids, std::vector<uint32_t>*& new_segment_ids,
                                              std::map<uint32_t, std::unordered_map<uint32_t, double>>*& inherit_infos_recorder,
                                              std::map<uint32_t, uint16_t>*& level_recorder, const uint32_t& level_0_base_count,
                                              std::map<uint32_t, std::vector<RangeRatePair>>*& segment_ranges_recorder) {
    pool_.submit_detach(do_batch_insert_segments, merged_segment_ids, new_segment_ids, inherit_infos_recorder, level_recorder, level_0_base_count, segment_ranges_recorder);
}

}