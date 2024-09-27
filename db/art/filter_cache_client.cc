#include "filter_cache_client.h"

namespace ROCKSDB_NAMESPACE {

task_thread_pool::task_thread_pool FilterCacheClient::pool_{FILTER_CACHE_THREADS_NUM};
FilterCacheManager FilterCacheClient::filter_cache_manager_;
bool FilterCacheClient::heat_buckets_ready_;

void FilterCacheClient::do_prepare_heat_buckets(const std::string& key, std::unordered_map<uint32_t, std::vector<std::string>>* const segment_info_recorder) {
    filter_cache_manager_.make_heat_buckets_ready(key, *segment_info_recorder);
}

bool FilterCacheClient::prepare_heat_buckets(const std::string& key, std::unordered_map<uint32_t, std::vector<std::string>>* const segment_info_recorder) {
    heat_buckets_ready_ = filter_cache_manager_.heat_buckets_ready();
    if (!heat_buckets_ready_) {
        // if heat_buckets_ready_ false
        assert(segment_info_recorder->size() == 0); // should always empty
        heat_buckets_ready_ = filter_cache_manager_.heat_buckets_ready();
        if (!heat_buckets_ready_) {
            pool_.submit_detach(do_prepare_heat_buckets, key, segment_info_recorder);
            heat_buckets_ready_ = filter_cache_manager_.heat_buckets_ready();
        }
    }
    return heat_buckets_ready_;
}

void FilterCacheClient::do_retrain_or_keep_model(std::vector<uint16_t>* const features_nums_except_level_0, 
                                                 std::map<uint32_t, uint16_t>* const level_recorder,
                                                 std::map<uint32_t, std::vector<RangeRatePair>>* const segment_ranges_recorder,
                                                 std::map<uint32_t, uint32_t>* const unit_size_recorder) {
    std::map<uint32_t, uint16_t> level_copy;
    std::map<uint32_t, std::vector<RangeRatePair>> segment_ranges_copy;
    // if this func background monitor signal, how can it receive latest argument? input pointer!
    while (!filter_cache_manager_.heat_buckets_ready());
    while (!filter_cache_manager_.ready_work()); // wait for manager ready
    assert(filter_cache_manager_.heat_buckets_ready()); // must guarantee that heat buckets ready before we make filter cache manager ready
    // actually we will load data before we test, so we can ensure that heat buckets ready first
    filter_cache_manager_.make_clf_model_ready(*features_nums_except_level_0);
    while (level_recorder->size() != segment_ranges_recorder->size());
    level_copy = *level_recorder; segment_ranges_copy = *segment_ranges_recorder;
    while (level_copy.size() != segment_ranges_copy.size()) {
        while (level_recorder->size() != segment_ranges_recorder->size());
        level_copy = *level_recorder; segment_ranges_copy = *segment_ranges_recorder;
    }
    assert(unit_size_recorder->size() == 0); // should be empty, then we use default unit size DEFAULT_UNIT_SIZE
    filter_cache_manager_.try_retrain_model(level_copy, segment_ranges_copy, *unit_size_recorder);
    filter_cache_manager_.update_cache_and_heap(level_copy, segment_ranges_copy);

    while (true) {
        // in one long period
        while (!filter_cache_manager_.need_retrain()); // wait for long period end
        while (level_recorder->size() != segment_ranges_recorder->size());
        level_copy = *level_recorder; segment_ranges_copy = *segment_ranges_recorder;
        while (level_copy.size() != segment_ranges_copy.size()) {
            while (level_recorder->size() != segment_ranges_recorder->size());
            level_copy = *level_recorder; segment_ranges_copy = *segment_ranges_recorder;
        }
        assert(unit_size_recorder->size() == 0); // should be empty, then we use default unit size DEFAULT_UNIT_SIZE
        filter_cache_manager_.try_retrain_model(level_copy, segment_ranges_copy, *unit_size_recorder);
        filter_cache_manager_.update_cache_and_heap(level_copy, segment_ranges_copy);
    }
    // this loop never end
}

void FilterCacheClient::retrain_or_keep_model(std::vector<uint16_t>* const features_nums_except_level_0, 
                                              std::map<uint32_t, uint16_t>* const level_recorder,
                                              std::map<uint32_t, std::vector<RangeRatePair>>* const segment_ranges_recorder,
                                              std::map<uint32_t, uint32_t>* const unit_size_recorder) {
    pool_.submit_detach(do_retrain_or_keep_model, features_nums_except_level_0, level_recorder, segment_ranges_recorder, unit_size_recorder);
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
    while (true) {
        // never stop making heap adjustment
        filter_cache_manager_.adjust_cache_and_heap();
    }
}

void FilterCacheClient::make_adjustment() {
    pool_.submit_detach(do_make_adjustment);
}

void FilterCacheClient::do_batch_insert_segments(std::vector<uint32_t>& merged_segment_ids, std::vector<uint32_t>& new_segment_ids,
                                                 std::map<uint32_t, std::unordered_map<uint32_t, double>>& inherit_infos_recorder,
                                                 std::map<uint32_t, uint16_t>& level_recorder, const uint32_t& level_0_base_count,
                                                 std::map<uint32_t, std::vector<RangeRatePair>>& segment_ranges_recorder) {
    filter_cache_manager_.insert_segments(merged_segment_ids, new_segment_ids, inherit_infos_recorder,
                                          level_recorder, level_0_base_count, segment_ranges_recorder);
}

void FilterCacheClient::batch_insert_segments(std::vector<uint32_t> merged_segment_ids, std::vector<uint32_t> new_segment_ids,
                                              std::map<uint32_t, std::unordered_map<uint32_t, double>> inherit_infos_recorder,
                                              std::map<uint32_t, uint16_t> level_recorder, const uint32_t& level_0_base_count,
                                              std::map<uint32_t, std::vector<RangeRatePair>> segment_ranges_recorder) {
    if (level_0_base_count == 0) {
        pool_.submit_detach(do_batch_insert_segments, merged_segment_ids, new_segment_ids, inherit_infos_recorder, level_recorder, INIT_LEVEL_0_COUNT, segment_ranges_recorder);
    } else {
        pool_.submit_detach(do_batch_insert_segments, merged_segment_ids, new_segment_ids, inherit_infos_recorder, level_recorder, level_0_base_count, segment_ranges_recorder);
    }
}

}