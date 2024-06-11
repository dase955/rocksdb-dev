#include "heat_buckets.h"
#include <fstream>
#include <iostream>
#include <cassert>

namespace ROCKSDB_NAMESPACE {

Bucket::Bucket() {
    hit_cnt_ = 0;
    hotness_ = 0;
    // keys_.clear();
}

Bucket::~Bucket() {
    return; // destroy nothing
}

const double& Bucket::hotness() {
    return hotness_;
}

const uint32_t& Bucket::hit_cnt() {
    return hit_cnt_;
}

/*
const size_t& Bucket::keys_cnt() {
    return keys_.size();
}
*/

void Bucket::update(const double& alpha, const uint32_t& period_cnt) {
    // mutex_.lock();
    hotness_ = alpha * hotness_ + 
                double(hit_cnt_) / double(period_cnt);
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

    while (std::getline(input, seperator)) {
        seperators_.push_back(seperator);
    }
    std::cout << "success load key range seperators" << std::endl; 
    std::cout << "key range seperators count : " << seperators_.size() << std::endl;
    input.close();

    // 2. init buckets_
    const size_t buckets_num = seperators_.size() - 1; // bucketss number = seperators num - 1
    buckets_.resize(buckets_num); // auto call Bucket::Bucket()
    std::cout << "set heat buckets size to " << buckets_.size() << std::endl;

    // 3. init period_cnt_ and alpha_, two variables should be fixed after init
    period_cnt_ = period_cnt;
    alpha_ = alpha;
    std::cout << "set period_cnt_ to " << period_cnt_ << std::endl;
    std::cout << "set alpha_ to " << alpha_ << std::endl;

    // 4. init current_cnt_
    current_cnt_ = 0;
    std::cout << "set current_cnt_ to " << current_cnt_ << std::endl;

    // 5. init mutex ptr container
    const size_t mutex_ptrs_num = buckets_num;
    for (int i=0; i<mutex_ptrs_num; i++) {
        mutex_ptrs_.push_back(std::unique_ptr<std::mutex>(new std::mutex()));
    }
    std::cout << "set mutex ptrs size to " << mutex_ptrs_.size() << std::endl;
}

HeatBuckets::~HeatBuckets() {
    return; // destroy nothing
}

const uint32_t& HeatBuckets::period_cnt() {
    return period_cnt_;
}

const uint32_t& HeatBuckets::current_cnt() {
    return current_cnt_;
}

const double& HeatBuckets::alpha() {
    return alpha_;
}

void HeatBuckets::debug() {
    std::cout << "[Debug] current cnt in this period: " << current_cnt() << std::endl;
    for (auto& bucket : buckets_) {
        std::cout << "[Debug] ";
        std::cout << "bucket hotness : " << bucket.hotness();
        std::cout << ", bucket hit cnt : " << bucket.hit_cnt();
        // std::cout << ", bucket keys cnt : " << bucket.keys_cnt();
        std::cout << std::endl;
    }
}

void HeatBuckets::update() {
    /*
    we use mutexs container in HeatBuckets instead of Bucket, so this bug description below is derelict

    // bug : only guarentee one bucket writen by one thread
    // but when you update hotness of all buckets, previous buckets will update hotness earlier.
    // if we hit this previous bucket, this will add 1 to the counter of this bucket
    // however the current cnt of HeatBuckets still not reset to 0
    // this means when the current cnt of this HeatBuckets reset to 0
    // there are several buckets counter already more than 0
    // therefore the estimated frequency of those buckets will be more than true frequency
    // the simplest way is make there is only one write to HeatBuckets
    // but this method will make read performance of db worser
    // so we will make the fixed period cnt big enouch and divide into more key range (more buckets)
    // this will lower the influence of these over-estimated frequencies
    // In the same way, bucket set keys_ may contain uncorrect keys
    */

    assert(mutex_ptrs_.size() == buckets_.size());
    for (int i=0; i<mutex_ptrs_.size(); i++) {
        mutex_ptrs_[i]->lock();
    }

    // TODO: use multiple threads to update hotness of all buckets
    for (int i=0; i<buckets_.size(); i++) {
        buckets_[i].update(alpha(), period_cnt());
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

    std::cout << "debug seperators_ size : " << seperators_.size() << std::endl;
    std::cout << "debug buckets_ size : " << buckets_.size() << std::endl;
    std::cout << "debug mutex_ptrs_ size : " << mutex_ptrs_.size() << std::endl;
    std::cout << "debug period_cnt_ : " << period_cnt_ << std::endl;
    std::cout << "debug alpha_ : " << alpha_ << std::endl;
    assert(index >= 0 && index < buckets_.size());
    assert(seperators_[index] <= key && key < seperators_[index+1]);
    assert(index >= 0 && index < mutex_ptrs_.size());
    
    mutex_ptrs_[index]->lock();
    buckets_[index].hit(); // mutex only permits one write opr to one bucket
    mutex_ptrs_[index]->unlock();

    cnt_mutex_.lock();
    current_cnt_ += 1;

    // DEBUG code
    if (current_cnt_ % BUCKETS_PERIOD == 0) {
        debug();
    }

    if (current_cnt_ >= period_cnt_) {
        update();
    }
    cnt_mutex_.unlock();
}

}