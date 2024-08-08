#pragma once

#include <iostream>
#include <vector>
#include <algorithm>
#include <map>
#include <cmath>
#include <cassert>
#include <mutex>
#include "macros.h"

namespace ROCKSDB_NAMESPACE {

struct FilterCacheHeapItem;
typedef FilterCacheHeapItem* FilterCacheHeapNode;
struct FilterCacheModifyResult;
class FilterCacheHeap;
class FilterCacheHeapManager;
inline bool FilterCacheHeapNodeLessComparor(const FilterCacheHeapNode& node_1, const FilterCacheHeapNode& node_2);
inline bool FilterCacheHeapNodeGreaterComparor(const FilterCacheHeapNode& node_1, const FilterCacheHeapNode& node_2);

struct FilterCacheHeapItem {
    uint32_t segment_id;
    uint32_t approx_visit_cnt; // estimated visit cnt
    uint16_t current_units_num; // enabled units num for this segment
    double benefit_or_cost; // can represent enable benefit or disable cost
    bool is_alive; // sign whether this item still used, if false, that means this segment already merged and freed
    FilterCacheHeapItem(const uint32_t& id, const uint32_t& cnt, const uint16_t& units, const double& heap_value) {
        segment_id = id; approx_visit_cnt = cnt; current_units_num = units; benefit_or_cost = heap_value; is_alive = true;
    }
    /*
    FilterCacheHeapItem(const FilterCacheHeapItem& item) {
        segment_id = item.segment_id; 
        approx_visit_cnt = item.approx_visit_cnt; 
        current_units_num = item.current_units_num; 
        benefit_or_cost = item.benefit_or_cost; 
        is_alive = item.is_alive;
    }
    */
};

struct FilterCacheModifyResult {
    uint32_t enable_segment_id;
    uint32_t disable_segment_id;
    uint16_t enable_segment_next_units_num;
    uint16_t disable_segment_next_units_num;
};

inline bool FilterCacheHeapNodeLessComparor(const FilterCacheHeapNode& node_1, const FilterCacheHeapNode& node_2) {
    return node_1->benefit_or_cost < node_2->benefit_or_cost;
}

inline bool FilterCacheHeapNodeGreaterComparor(const FilterCacheHeapNode& node_1, const FilterCacheHeapNode& node_2) {
    return node_1->benefit_or_cost > node_2->benefit_or_cost;
}

class FilterCacheHeap {
private:
    int heap_type_;
    // map<segment id, node>, use this map to fastly locate node in heap
    std::map<uint32_t, FilterCacheHeapNode> heap_index_; 
    // use make_heap, push_heap, pop_heap to manage heap
    std::vector<FilterCacheHeapNode> heap_; 
    // std::mutex heap_mutex_;

public:
    FilterCacheHeap() {
        heap_type_ = UNKNOWN_HEAP;
        heap_index_.clear();
        heap_.clear();
    }

    void set_type(const int type) {
        heap_type_ = type;
    }

    // return heap top
    FilterCacheHeapNode heap_top();

    // pop one node with deleting node from heap_index_
    // void pop();

    // push one node with upsert node into heap_index_
    // void push(FilterCacheHeapNode& node);

    // given a batch of segment id, return needed nodes. 
    // only support batch query and reminded that one return node may be null 
    // if segment not available or segment not exists in heap_index_
    // result will write into return_nodes
    void batch_query(std::vector<uint32_t>& segment_ids, std::vector<FilterCacheHeapNode>& return_nodes);

    // upsert batch nodes into heap_index_ and heap_
    // only support batch upsert, if one node already exists in heap_index_, it must in heap
    // so we only need to update the content of that existing node
    void batch_upsert(std::vector<FilterCacheHeapNode>& nodes);

    // delete batch nodes from heap_index_ and heap_
    // only support batch delete, if one node not exist in heap_index_, it must not exist in heap
    // so we only need to delete these existing nodes
    void batch_delete(std::vector<uint32_t>& segment_ids);
};

class FilterCacheHeapManager {
private: 
    static FilterCacheHeap benefit_heap_;
    static FilterCacheHeap cost_heap_;
    // set heap node visit cnt = c_1, real estimated visit cnt = c_2
    // we only update c_1 when | c_1 - c_2 | >= VISIT_CNT_UPDATE_BOUND
    // update c_1 means we need to update this recorder and heap
    // heap_visit_cnt_recorder: map<segment id : visit cnt in heap>
    // when filter cache call delete, this recorder will automately delete these merged segment ids
    // when filter cache call upsert, this recorder will automately upsert these segment ids
    static std::map<uint32_t, uint32_t> heap_visit_cnt_recorder_;
    static std::mutex manager_mutex_; 

public:
    FilterCacheHeapManager() {
        benefit_heap_.set_type(BENEFIT_HEAP);
        cost_heap_.set_type(COST_HEAP);
        heap_visit_cnt_recorder_.clear();
    }

    // sync visit cnt in heap and real estimated visit cnt
    // reminded that we will not insert or delete nodes in this method
    // we only update these nodes that already exist in two heaps
    void sync_visit_cnt(std::map<uint32_t, uint32_t>& current_visit_cnt_recorder);

    // try to read benefit_heap top and cost_heap top, then judge whether we need to modify units num in filter cache
    // return true when we can modify units num of several segments, return false when we cannot
    // reminded that this func only modify heap, we still need to update filter units in filter cache
    bool try_modify(FilterCacheModifyResult& result);

    // delete batch segment nodes in benefit_heap and cost_heap, also need to update heap_visit_cnt_recorder_
    void batch_delete(std::vector<uint32_t>& segment_ids);

    // upsert batch segment nodes in benefit_heap and cost_heap, also need to update heap_visit_cnt_recorder_
    // only input items, we will allocate space for nodes later
    // reminded that we will also update heap_visit_cnt_recorder_ if we update a existed node
    // because we need to keep heap visit cnt and recorder visit cnt the same
    void batch_upsert(std::vector<FilterCacheHeapItem>& items);
};

// TODO: every segment have max units num k, not MAX_UNITS_NUM, fix it

}
