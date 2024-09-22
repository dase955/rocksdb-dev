#include "filter_cache_item.h"
#include <cstdint>
#include "db/table_cache.h"
#include "table/table_reader.h"

namespace ROCKSDB_NAMESPACE {

    // 构造函数，可以初始化成员变量
    FilterCacheItem::FilterCacheItem(const uint32_t& segment_id) {

    }

    // 清理成员变量，避免内存泄漏，如果new了空间，就可能需要在这里清理
    FilterCacheItem::~FilterCacheItem() {

    }

    uint32_t FilterCacheItem::approximate_size() {
        // uint32_t sum = 0;
        // for (const auto& filter_block : filter_block_data_) {
        //     sum += filter_block.ApproximateMemoryUsage();
        // }
        // return sum;
        return 0;
    }

    bool FilterCacheItem::check_key(const std::string& key) {
        // TableReader* file_meta_ = TableCache::InitFileTableReader()
    }

    void FilterCacheItem::enable_units(const uint32_t& units_num) {

    }
}