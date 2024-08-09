#include "filter_cache_heap.h"
#include "greedy_algo.h"

namespace ROCKSDB_NAMESPACE {

FilterCacheHeap FilterCacheHeapManager::benefit_heap_;
FilterCacheHeap FilterCacheHeapManager::cost_heap_;
std::map<uint32_t, uint32_t> FilterCacheHeapManager::heap_visit_cnt_recorder_;
std::map<uint32_t, uint16_t> FilterCacheHeapManager::units_num_limit_recorder_;
std::mutex FilterCacheHeapManager::manager_mutex_; 

FilterCacheHeapNode FilterCacheHeap::heap_top() {
    // need lock heap, or we may retrive outdated node
    // heap_mutex_.lock();

    if (!heap_.empty()) {
        return heap_[0];
    } else {
        return nullptr;
    }

    // heap_mutex_.unlock();
}

/*
void FilterCacheHeap::pop() {
    // heap_mutex_.lock();

    FilterCacheHeapNode node;
    const size_t size = heap_.size();

    assert(heap_type_ != UNKNOWN_HEAP);
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

    // heap_mutex_.unlock();
}
*/

/*
void FilterCacheHeap::push(FilterCacheHeapNode& node) {
    if (node == nullptr) {
        return;
    }

    // heap_mutex_.lock();

    heap_.emplace_back(node);
    assert(heap_type_ != UNKNOWN_HEAP);
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

    // heap_mutex_.unlock();
}
*/

void FilterCacheHeap::batch_query(std::vector<uint32_t>& segment_ids, std::vector<FilterCacheHeapNode>& return_nodes) {
    // heap_mutex_.lock();

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

    // heap_mutex_.unlock();
}

void FilterCacheHeap::batch_upsert(std::vector<FilterCacheHeapNode>& nodes) {
    // heap_mutex_.lock();

    // we guarantee that if one node already exists in heap_index_, it must exist in heap
    for (FilterCacheHeapNode& node : nodes) {
        const uint32_t segment_id = node->segment_id;
        auto it = heap_index_.find(segment_id);
        if (it != heap_index_.end()) {
            // exist in heap_index_ and heap_
            // we may query nodes from this heap, and update var in nodes, then upsert original nodes
            // check it->second != node to make sure that we won't free a refered sapce
            if (it->second != node) { 
                *(it->second) = *(node); // only copy content, this will update content of node in heap_index_ and heap_
                delete node; // remember to free unnecessary space!
            }
        } else {
            // not exist in heap_index_ and heap_
            heap_index_.insert(std::make_pair(segment_id, node)); // insert into heap_index_
            heap_.emplace_back(node); // push into heap_
        }
    }

    // update or insert done, need to rebuild heap_
    rebuild_heap();

    // heap_mutex_.unlock();
}

void FilterCacheHeap::batch_delete(std::vector<uint32_t>& segment_ids) {
    // heap_mutex_.lock();

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
    rebuild_heap();

    // heap_mutex_.unlock();
}

void FilterCacheHeapManager::batch_delete(std::vector<uint32_t>& segment_ids) {
    manager_mutex_.lock();

    for (uint32_t& segment_id : segment_ids) {
        auto cnt_it = heap_visit_cnt_recorder_.find(segment_id);
        auto limit_it = units_num_limit_recorder_.find(segment_id);
        if (cnt_it != heap_visit_cnt_recorder_.end()) {
            heap_visit_cnt_recorder_.erase(segment_id);
        }
        if (limit_it != units_num_limit_recorder_.end()) {
            units_num_limit_recorder_.erase(segment_id);
        }
    }

    benefit_heap_.batch_delete(segment_ids);
    cost_heap_.batch_delete(segment_ids);

    manager_mutex_.unlock();
}

void FilterCacheHeapManager::batch_upsert(std::vector<FilterCacheHeapItem>& items) {
    manager_mutex_.lock();

    std::vector<FilterCacheHeapNode> benefit_nodes, cost_nodes;
    for (FilterCacheHeapItem& item : items) {
        assert(item.current_units_num >= MIN_UNITS_NUM);
        assert(item.current_units_num <= item.units_num_limit);
        double benefit = StandardBenefitWithMaxBound(item.approx_visit_cnt, item.current_units_num, item.units_num_limit);
        double cost = StandardCostWithMinBound(item.approx_visit_cnt, item.current_units_num, MIN_UNITS_NUM);
        // item meets at least one conditions
        // so that item always upsert into heap
        // if item.approx_visit_cnt = 0, still push into heap
        // we may modify its visit cnt in heap later
        /*
        if (item.current_units_num > MIN_UNITS_NUM) {
            // make ready to upsert cost nodes
            // FilterCacheHeapItem(const uint32_t& id, const uint32_t& cnt, const uint16_t& units, const double& heap_value, const uint16_t& limit)
            cost_nodes.emplace_back(new FilterCacheHeapItem(item.segment_id,
                                                            item.approx_visit_cnt,
                                                            item.current_units_num,
                                                            cost,
                                                            item.units_num_limit)
                                    );
        }
        if (item.current_units_num < item.units_num_limit) {
            // make ready to upsert benefit nodes
            // FilterCacheHeapItem(const uint32_t& id, const uint32_t& cnt, const uint16_t& units, const double& heap_value, const uint16_t& limit)
            benefit_nodes.emplace_back(new FilterCacheHeapItem(item.segment_id,
                                                                item.approx_visit_cnt,
                                                                item.current_units_num,
                                                                benefit,
                                                                item.units_num_limit)
                                        );
        }
        */

        if (item.current_units_num >= MIN_UNITS_NUM && item.current_units_num <= item.units_num_limit) {
            cost_nodes.emplace_back(new FilterCacheHeapItem(item.segment_id,
                                                            item.approx_visit_cnt,
                                                            item.current_units_num,
                                                            cost,
                                                            item.units_num_limit)
                                    );
            benefit_nodes.emplace_back(new FilterCacheHeapItem(item.segment_id,
                                                                item.approx_visit_cnt,
                                                                item.current_units_num,
                                                                benefit,
                                                                item.units_num_limit)
                                        );
        }

        // update visit cnt, we need to keep recorder visit cnt and heap visit cnt the same
        const uint32_t segment_id = item.segment_id;
        const uint32_t visit_cnt = item.approx_visit_cnt;
        const uint16_t units_limit = item.units_num_limit;
        auto cnt_it = heap_visit_cnt_recorder_.find(segment_id);
        auto limit_it = units_num_limit_recorder_.find(segment_id);
        if (cnt_it != heap_visit_cnt_recorder_.end()) {
            cnt_it->second = visit_cnt;
        } else {
            heap_visit_cnt_recorder_.insert(std::make_pair(segment_id, visit_cnt));
        }
        if (limit_it != units_num_limit_recorder_.end()) {
            limit_it->second = units_limit;
        } else {
            units_num_limit_recorder_.insert(std::make_pair(segment_id, units_limit));
        }
    }

    // upsert nodes into heaps
    benefit_heap_.batch_upsert(benefit_nodes);
    cost_heap_.batch_upsert(cost_nodes);

    manager_mutex_.unlock();
}

bool FilterCacheHeapManager::try_modify(FilterCacheModifyResult& result) {
    manager_mutex_.lock();

    FilterCacheHeapNode benefit_node = benefit_heap_.heap_top();
    FilterCacheHeapNode cost_node = cost_heap_.heap_top();
    // if benefit heap or cost heap empty, no need to modify
    if (benefit_node == nullptr || cost_node == nullptr) {
        manager_mutex_.unlock(); // remember to unlock, or we will cause deadlock
        return false;
    }

    const double benefit = benefit_node->benefit_or_cost;
    const double cost = cost_node->benefit_or_cost;
    // if benefit of enable one unit <= cost of disable one unit, no need to modify
    if (benefit <= cost) {
        manager_mutex_.unlock(); // remember to unlock, or we will cause deadlock
        return false;
    }

    const uint32_t benefit_segment_id = benefit_node->segment_id;
    const uint32_t cost_segment_id = cost_node->segment_id;
    // if we will enable and disable one unit of the same segment, ignore it
    if (benefit_segment_id == cost_segment_id) {
        manager_mutex_.unlock(); // remember to unlock, or we will cause deadlock
        return false;
    }

    // FilterCacheHeapItem(const uint32_t& id, const uint32_t& cnt, const uint16_t& units, const double& heap_value) 
    // we can try filter unit modification, reminded that this modification will modify units num of two segments
    // so we need to upsert new nodes of these two segments into benefit heap and cost heap
    std::vector<FilterCacheHeapNode> new_benefit_nodes, new_cost_nodes;

    /*
    if (benefit_node->current_units_num + 1 < benefit_node->units_num_limit) { 
        new_benefit_nodes.emplace_back(new FilterCacheHeapItem(benefit_node->segment_id,
                                                                benefit_node->approx_visit_cnt,
                                                                benefit_node->current_units_num + 1,
                                                                StandardBenefit(benefit_node->approx_visit_cnt,
                                                                                benefit_node->current_units_num + 1
                                                                                ),
                                                                benefit_node->units_num_limit
                                                                )
                                        );
    }
    // benefit node will enable one unit, so its units num will always > MIN_UNITS_NUM
    new_cost_nodes.emplace_back(new FilterCacheHeapItem(benefit_node->segment_id,
                                                        benefit_node->approx_visit_cnt,
                                                        benefit_node->current_units_num + 1,
                                                        StandardCost(benefit_node->approx_visit_cnt,
                                                                     benefit_node->current_units_num + 1
                                                                     ),
                                                        benefit_node->units_num_limit
                                                        )
                                );
    
    if (cost_node->current_units_num - 1 > MIN_UNITS_NUM) {
        new_cost_nodes.emplace_back(new FilterCacheHeapItem(cost_node->segment_id,
                                                            cost_node->approx_visit_cnt,
                                                            cost_node->current_units_num - 1,
                                                            StandardCost(cost_node->approx_visit_cnt,
                                                                         cost_node->current_units_num - 1
                                                                        ),
                                                            cost_node->units_num_limit
                                                            )
                                    );
    }
    // cost node will disable one unit, so its units num will always < MAX_UNITS_NUM
    new_benefit_nodes.emplace_back(new FilterCacheHeapItem(cost_node->segment_id,
                                                           cost_node->approx_visit_cnt,
                                                           cost_node->current_units_num - 1,
                                                           StandardBenefit(cost_node->approx_visit_cnt,
                                                                           cost_node->current_units_num - 1
                                                                            ),
                                                           cost_node->units_num_limit
                                                            )
                                    );
    */
    // we set benefit of nodes (units num == units num limit) to 0.0
    // and cost of nodes (units num == 0) to Infinite
    // these prevent modifying these nodes' units num
    // so we dont need to check units num
    new_benefit_nodes.emplace_back(new FilterCacheHeapItem(benefit_node->segment_id,
                                                            benefit_node->approx_visit_cnt,
                                                            benefit_node->current_units_num + 1,
                                                            StandardBenefitWithMaxBound(benefit_node->approx_visit_cnt,
                                                                                        benefit_node->current_units_num + 1,
                                                                                        benefit_node->units_num_limit
                                                                                        ),
                                                            benefit_node->units_num_limit
                                                            )
                                    );
    new_cost_nodes.emplace_back(new FilterCacheHeapItem(benefit_node->segment_id,
                                                        benefit_node->approx_visit_cnt,
                                                        benefit_node->current_units_num + 1,
                                                        StandardCostWithMinBound(benefit_node->approx_visit_cnt,
                                                                                 benefit_node->current_units_num + 1,
                                                                                 MIN_UNITS_NUM
                                                                                 ),
                                                        benefit_node->units_num_limit
                                                        )
                                );
    new_cost_nodes.emplace_back(new FilterCacheHeapItem(cost_node->segment_id,
                                                        cost_node->approx_visit_cnt,
                                                        cost_node->current_units_num - 1,
                                                        StandardCostWithMinBound(cost_node->approx_visit_cnt,
                                                                                 cost_node->current_units_num - 1,
                                                                                 MIN_UNITS_NUM),
                                                        cost_node->units_num_limit
                                                        )
                                );
    new_benefit_nodes.emplace_back(new FilterCacheHeapItem(cost_node->segment_id,
                                                           cost_node->approx_visit_cnt,
                                                           cost_node->current_units_num - 1,
                                                           StandardBenefitWithMaxBound(cost_node->approx_visit_cnt,
                                                                                       cost_node->current_units_num - 1,
                                                                                       cost_node->units_num_limit
                                                                                       ),
                                                           cost_node->units_num_limit
                                                            )
                                    );
    // already make ready for upsert
    benefit_heap_.batch_upsert(new_benefit_nodes);
    cost_heap_.batch_upsert(new_cost_nodes);

    // write result
    result.enable_segment_id = benefit_node->segment_id;
    result.disable_segment_id = cost_node->segment_id;
    result.enable_segment_next_units_num = benefit_node->current_units_num + 1;
    result.disable_segment_next_units_num = cost_node->current_units_num - 1;

    // return nothing, result already written into var result

    manager_mutex_.unlock();

    return true;
}

void FilterCacheHeapManager::sync_visit_cnt(std::map<uint32_t, uint32_t>& current_visit_cnt_recorder) {
    manager_mutex_.lock();

    std::vector<FilterCacheHeapNode> sync_nodes;
    std::vector<uint32_t> sync_segment_ids;

    auto heap_it = heap_visit_cnt_recorder_.begin();
    auto current_it = current_visit_cnt_recorder.begin();
    while (heap_it != heap_visit_cnt_recorder_.end() &&
            current_it != current_visit_cnt_recorder.end()) {
        if (heap_it->first < current_it->first) {
            heap_it ++;
        } else if (heap_it->first > current_it->first) {
            current_it ++;
        } else { 
            // heap_it->first == current_it->first
            assert(heap_it->first == current_it->first);
            int64_t old_visit_cnt = heap_it->second;
            int64_t cur_visit_cnt = current_it->second;
            if (std::abs(cur_visit_cnt-old_visit_cnt) > VISIT_CNT_UPDATE_BOUND) {
                heap_it->second = current_it->second; // remember to update heap visit cnt recorder
                sync_segment_ids.emplace_back(current_it->first);
            }
            // heap_it ++;
            current_it ++;
        }
    }

    // query nodes in heap
    std::vector<FilterCacheHeapNode> sync_benefit_nodes, sync_cost_nodes;
    benefit_heap_.batch_query(sync_segment_ids, sync_benefit_nodes);
    cost_heap_.batch_query(sync_segment_ids, sync_cost_nodes);

    // update visit cnt and benefit/cost in these nodes 
    for (FilterCacheHeapNode& sync_benefit_node : sync_benefit_nodes) {
        if (sync_benefit_node != nullptr) {
            sync_benefit_node->approx_visit_cnt = current_visit_cnt_recorder[sync_benefit_node->segment_id];
            sync_benefit_node->benefit_or_cost = StandardBenefitWithMaxBound(sync_benefit_node->approx_visit_cnt,
                                                                             sync_benefit_node->current_units_num,
                                                                             sync_benefit_node->units_num_limit);
        }
    }
    for (FilterCacheHeapNode& sync_cost_node : sync_cost_nodes) {
        if (sync_cost_node != nullptr) {
            sync_cost_node->approx_visit_cnt = current_visit_cnt_recorder[sync_cost_node->segment_id];
            sync_cost_node->benefit_or_cost = StandardCostWithMinBound(sync_cost_node->approx_visit_cnt,
                                                                       sync_cost_node->current_units_num,
                                                                       MIN_UNITS_NUM);
        }
    }

    // upsert nodes into benefit heap and cost heap
    // benefit_heap_.batch_upsert(sync_benefit_nodes);
    // cost_heap_.batch_upsert(sync_cost_nodes);

    
    // notice that we already updated these nodes in heap, we only need to rebuild heap
    // but heap.upsert include the step of checking whether these segments already in heap
    // this will waste some time, can we rebuild heap directly?
    benefit_heap_.rebuild_heap();
    cost_heap_.rebuild_heap();

    manager_mutex_.unlock();
}

void FilterCacheHeapManager::sync_units_num_limit(std::map<uint32_t, uint16_t>& current_units_num_limit_recorder) {
    manager_mutex_.lock();

    std::vector<FilterCacheHeapNode> sync_nodes;
    std::vector<uint32_t> sync_segment_ids;

    auto origin_it = units_num_limit_recorder_.begin();
    auto current_it = current_units_num_limit_recorder.begin();
    while (origin_it != units_num_limit_recorder_.end() &&
            current_it != current_units_num_limit_recorder.end()) {
        if (origin_it->first < current_it->first) {
            origin_it ++;
        } else if (origin_it->first > current_it->first) {
            current_it ++;
        } else { 
            // origin_it->first == current_it->first
            assert(origin_it->first == current_it->first);
            assert(current_it->second <= MAX_UNITS_NUM);
            if (origin_it->second != current_it->second) {
                origin_it->second = current_it->second;
                sync_segment_ids.emplace_back(current_it->first);
            }
            current_it ++;
        }
    }

    // query nodes in heap
    std::vector<FilterCacheHeapNode> sync_benefit_nodes, sync_cost_nodes;
    benefit_heap_.batch_query(sync_segment_ids, sync_benefit_nodes);
    cost_heap_.batch_query(sync_segment_ids, sync_cost_nodes);

    // update units num limit, units num and benefit/cost in these nodes 
    for (FilterCacheHeapNode& sync_benefit_node : sync_benefit_nodes) {
        if (sync_benefit_node != nullptr) {
            sync_benefit_node->units_num_limit = current_units_num_limit_recorder[sync_benefit_node->segment_id];
            sync_benefit_node->current_units_num = std::min(sync_benefit_node->units_num_limit,
                                                            sync_benefit_node->current_units_num);
            sync_benefit_node->benefit_or_cost = StandardBenefitWithMaxBound(sync_benefit_node->approx_visit_cnt,
                                                                             sync_benefit_node->current_units_num,
                                                                             sync_benefit_node->units_num_limit);
        }
    }
    for (FilterCacheHeapNode& sync_cost_node : sync_cost_nodes) {
        if (sync_cost_node != nullptr) {
            sync_cost_node->units_num_limit = current_units_num_limit_recorder[sync_cost_node->segment_id];
            sync_cost_node->current_units_num = std::min(sync_cost_node->units_num_limit,
                                                         sync_cost_node->current_units_num);
            sync_cost_node->benefit_or_cost = StandardCostWithMinBound(sync_cost_node->approx_visit_cnt,
                                                                        sync_cost_node->current_units_num,
                                                                        MIN_UNITS_NUM);
        }
    }

    // upsert nodes into benefit heap and cost heap
    // benefit_heap_.batch_upsert(sync_benefit_nodes);
    // cost_heap_.batch_upsert(sync_cost_nodes);

    
    // notice that we already updated these nodes in heap, we only need to rebuild heap
    // but heap.upsert include the step of checking whether these segments already in heap
    // this will waste some time, can we rebuild heap directly?
    benefit_heap_.rebuild_heap();
    cost_heap_.rebuild_heap();

    manager_mutex_.unlock();
}

}