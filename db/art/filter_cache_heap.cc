#include "filter_cache_heap.h"

namespace ROCKSDB_NAMESPACE {

FilterCacheHeapNode FilterCacheHeap::heap_top() {
    // need lock heap, or we may retrive outdated node
    heap_mutex_.lock();

    if (!heap_.empty()) {
        return heap_[0];
    } else {
        return nullptr;
    }

    heap_mutex_.unlock();
}

void FilterCacheHeap::pop() {
    heap_mutex_.lock();

    FilterCacheHeapNode node;
    const size_t size = heap_.size();

    if (heap_type_ == BENEFIT_HEAP) {
        std::pop_heap(heap_.begin(), heap_.end(), FilterCacheHeapNodeLessComparor);
    } else if (heap_type_ == COST_HEAP) {
        std::pop_heap(heap_.begin(), heap_.end(), FilterCacheHeapNodeGreaterComparor);
    }
    node = heap_[size - 1];
    heap_.pop_back();

    // remove node from heap_index_
    if (node == nullptr) {
        return;
    }
    const uint32_t segment_id = node->segment_id;
    auto it = heap_index_.find(segment_id);
    if (it != heap_index_.end()) {
        heap_index_.erase(node->segment_id);
    }
    if (node != nullptr) {
        delete node; // remember to release node !!!
    }
    assert(heap_.size() == heap_index_.size());

    heap_mutex_.unlock();
}

void FilterCacheHeap::push(FilterCacheHeapNode& node) {
    if (node == nullptr) {
        return;
    }

    heap_mutex_.lock();

    heap_.emplace_back(node);
    if (heap_type_ == BENEFIT_HEAP) {
        std::push_heap(heap_.begin(), heap_.end(), FilterCacheHeapNodeLessComparor);
    } else if (heap_type_ == COST_HEAP) {
        std::push_heap(heap_.begin(), heap_.end(), FilterCacheHeapNodeGreaterComparor);
    }

    // remember to upsert node into heap_index_
    // upsert(node);
    if (node == nullptr) {
        return;
    }
    const uint32_t segment_id = node->segment_id;
    auto it = heap_index_.find(segment_id);
    if (it != heap_index_.end()) {
        it->second = node; // already exist in heap_index_, only update
    } else {
        heap_index_.insert(std::make_pair(segment_id, node)); // insert into heap_index_
    }
    assert(heap_.size() == heap_index_.size());

    heap_mutex_.unlock();
}

void FilterCacheHeap::batch_query(std::vector<uint32_t>& segment_ids, std::vector<FilterCacheHeapNode>& return_nodes) {
    heap_mutex_.lock();

    return_nodes.clear();
    for (uint32_t& segment_id : segment_ids) {
        auto it = heap_index_.find(segment_id);
        FilterCacheHeapNode return_node = nullptr;
        // if node->is_alive is false, the segment already merged and never exists in storage
        // so we should return null when query a merged segment id
        if (it != heap_index_.end() && (it->second)->is_alive == true) { 
            return_node = it->second; // node exists in heap_index_ and segment alive
        }
        return_nodes.emplace_back(return_node);
    }

    heap_mutex_.unlock();
}

void FilterCacheHeap::batch_upsert(std::vector<FilterCacheHeapNode>& nodes) {
    heap_mutex_.lock();

    // we guarantee that if one node already exists in heap_index_, it must exist in heap
    for (FilterCacheHeapNode& node : nodes) {
        const uint32_t segment_id = node->segment_id;
        auto it = heap_index_.find(segment_id);
        if (it != heap_index_.end()) {
            // exist in heap_index_ and heap_
            *(it->second) = *(node); // only copy content, this will update content of node in heap_index_ and heap_
            delete node; // remember to free unnecessary space!
        } else {
            // not exist in heap_index_ and heap_
            heap_index_.insert(std::make_pair(segment_id, node)); // insert into heap_index_
            heap_.emplace_back(node); // push into heap_
        }
    }
    // update or insert done, need to rebuild heap_
    if (heap_type_ == BENEFIT_HEAP) {
        std::make_heap(heap_.begin(), heap_.end(), FilterCacheHeapNodeLessComparor);
    } else if (heap_type_ == COST_HEAP) {
        std::make_heap(heap_.begin(), heap_.end(), FilterCacheHeapNodeGreaterComparor);
    }
    assert(heap_.size() == heap_index_.size());

    heap_mutex_.unlock();
}

void FilterCacheHeap::batch_delete(std::vector<uint32_t>& segment_ids) {
    heap_mutex_.lock();

    // we guarantee that if one node not exist in heap_index_, it must not exist in heap
    for (uint32_t& segment_id : segment_ids) {
        auto it = heap_index_.find(segment_id);
        if (it == heap_index_.end()) {
            // not exist in heap_index_ and heap_
            // do nothing
        } else {
            // exist in heap_index_ and heap_
            // set is_alive to false and delete after that
            it->second->is_alive = false;
        }
    }

    // delete nodes that is_alive == false
    auto it = heap_.begin();
    FilterCacheHeapNode node = nullptr;
    while(it != heap_.end()) {
        node = (*it);
        if (node->is_alive == false) {
            // need delete
            const uint32_t segment_id = node->segment_id;
            // already delete node in heap_
            it = heap_.erase(it); // it will point to next available node
            // already delete node in heap_index_
            heap_index_.erase(segment_id);
            // remember to free node after that
            delete node;
        } else {
            it ++;
        }
    }

    // delete done, need to rebuild heap_
    if (heap_type_ == BENEFIT_HEAP) {
        std::make_heap(heap_.begin(), heap_.end(), FilterCacheHeapNodeLessComparor);
    } else if (heap_type_ == COST_HEAP) {
        std::make_heap(heap_.begin(), heap_.end(), FilterCacheHeapNodeGreaterComparor);
    }

    heap_mutex_.unlock();
}

}