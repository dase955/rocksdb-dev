#include "heat_buckets.h"
#include <fstream>
#include <iostream>
#include <cassert>

namespace ROCKSDB_NAMESPACE {
std::vector<std::string> HeatBuckets::seperators_;
std::vector<Bucket> HeatBuckets::buckets_;
uint32_t HeatBuckets::period_cnt_;  // the get count of one period, should be fixed
uint32_t HeatBuckets::current_cnt_; // current get count in this period
double HeatBuckets::alpha_;
std::vector<std::unique_ptr<std::mutex>> HeatBuckets::mutex_ptrs_;
std::mutex HeatBuckets::cnt_mutex_;


Bucket::Bucket() {
    hit_cnt_ = 0;
    hotness_ = 0;
    // keys_.clear();
}

Bucket::~Bucket() {
    return; // destroy nothing
}

/*
const size_t& Bucket::keys_cnt() {
    return keys_.size();
}
*/

void Bucket::update(const double& alpha, const uint32_t& period_cnt) {
    // mutex_.lock();
    hotness_ = (1 - alpha) * hotness_ + 
                alpha * double(hit_cnt_) / double(period_cnt);
    hit_cnt_ = 0; // remember to reset counter
    // keys_.clear();
    // mutex_.unlock(); // remember to unlock!!!
}

void Bucket::hit() {
    // mutex_.lock();
    hit_cnt_ += 1;
    // keys_.insert(key);
    // mutex_.unlock(); // remember to unlock!!!
}

HeatBuckets::HeatBuckets() {
    // TODO：support reading rocksdb options to get seperators path
    HeatBuckets(SEPERATORS_PATH, BUCKETS_ALPHA, BUCKETS_PERIOD);
}

HeatBuckets::HeatBuckets(const std::string& path, const double& alpha, const uint32_t& period_cnt) {
    // 1. init seperators_
    std::ifstream input;
    input.open(path, std::ios::in);
    std::string seperator;

    if (!input.is_open()) {
        // std::cout << "failed to open key range seperators file, skip building head buckets!" << std::endl;
        throw "failed to open key range seperators file!";
    }

    seperators_.resize(0);
    while (std::getline(input, seperator)) {
        seperators_.push_back(seperator);
    }
    // std::cout << "success load key range seperators" << std::endl; 
    // std::cout << "key range seperators count : " << seperators_.size() << std::endl;
    input.close();

    // 2. init buckets_
    const size_t buckets_num = seperators_.size() - 1; // bucketss number = seperators num - 1
    buckets_.resize(0);
    buckets_.resize(buckets_num); // auto call Bucket::Bucket()
    // std::cout << "set heat buckets size to " << buckets_.size() << std::endl;

    // 3. init period_cnt_ and alpha_, two variables should be fixed after init
    period_cnt_ = period_cnt;
    alpha_ = alpha;
    // std::cout << "set period_cnt_ to " << period_cnt_ << std::endl;
    // std::cout << "set alpha_ to " << alpha_ << std::endl;

    // 4. init current_cnt_
    current_cnt_ = 0;
    // std::cout << "set current_cnt_ to " << current_cnt_ << std::endl;

    // 5. init mutex ptr container
    const size_t mutex_ptrs_num = buckets_num;
    mutex_ptrs_.resize(0);
    for (size_t i=0; i<mutex_ptrs_num; i++) {
        mutex_ptrs_.push_back(std::unique_ptr<std::mutex>(new std::mutex()));
    }
    // std::cout << "set mutex ptrs size to " << mutex_ptrs_.size() << std::endl;
    std::cout << "enable heat buckets estimates" << std::endl;
}

HeatBuckets::~HeatBuckets() {
    return; // destroy nothing
}

void HeatBuckets::debug() {
    std::cout << "[Debug] current cnt in this period: " << current_cnt_ << std::endl;
    for (auto& bucket : buckets_) {
        std::cout << "[Debug] ";
        std::cout << "bucket hotness : " << bucket.hotness_;
        std::cout << ", bucket hit cnt : " << bucket.hit_cnt_;
        // std::cout << ", bucket keys cnt : " << bucket.keys_cnt();
        std::cout << std::endl;
    }
}

void HeatBuckets::update() {

    // bug : when update, the sum of all buckets cnt may not more or less than period_cnt_.
    // we decide to use bigger period_cnt_ and divide into more buckets.

    assert(mutex_ptrs_.size() == buckets_.size());
    for (size_t i=0; i<mutex_ptrs_.size(); i++) {
        mutex_ptrs_[i]->lock();
    }

    // TODO: use multiple threads to update hotness of all buckets
    for (size_t i=0; i<buckets_.size(); i++) {
        buckets_[i].update(alpha_, period_cnt_);
        mutex_ptrs_[i]->unlock();
    }
    // remember to reset current_cnt_ counter
    current_cnt_ = 0;
}

int32_t HeatBuckets::locate(const std::string& key) {
    int32_t left = 0, right = seperators_.size()-1;
    while (left < right - 1){				  
        int32_t mid = left + ((right-left) / 2);
        if (seperators_[mid] > key) {
            right = mid;	  		 
        } else if (seperators_[mid] <= key) {
            left = mid;		        
        }
    }
    return left;
}

void HeatBuckets::hit(const std::string& key) {
    // use linear search to find index i, making seperators_[i] <= key and seperators_[i+1] > i
    // reminding we have set border guard, so dont worry about out of bounds error
    // after we find the index i, we call buckets_[i].hit(), then add 1 to current_cnt_
    // if current_cnt_ >= period_cnt_, call update() to update hotness of all buckets and reset current cnt counter

    int32_t index = 0;
    // last element is border guard
    // means last element always bigger than key
    // first element is border guard
    // means first element always smaller than key

    // linear search version
    /* 
    while (seperators_[index+1] <= key) {
        index += 1;
    }
    */
    index = locate(key);

    // std::cout << "debug seperators_ size : " << seperators_.size() << std::endl;
    // std::cout << "debug buckets_ size : " << buckets_.size() << std::endl;
    // std::cout << "debug mutex_ptrs_ size : " << mutex_ptrs_.size() << std::endl;
    // std::cout << "debug period_cnt_ : " << period_cnt_ << std::endl;
    // std::cout << "debug alpha_ : " << alpha_ << std::endl;
    assert(index >= 0 && index < buckets_.size());
    assert(seperators_[index] <= key && key < seperators_[index+1]);
    assert(index >= 0 && index < mutex_ptrs_.size());
    
    mutex_ptrs_[index]->lock();
    buckets_[index].hit(); // mutex only permits one write opr to one bucket
    mutex_ptrs_[index]->unlock();

    cnt_mutex_.lock();
    current_cnt_ += 1;
   
    if (current_cnt_ % period_cnt_ == 0) {
        // debug();
        update();
    }
    cnt_mutex_.unlock();
}

}