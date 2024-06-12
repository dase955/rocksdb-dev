#pragma once
#include <mutex>
#include <vector>
#include <string>
#include <set>
#include "macros.h"
#include <memory>

namespace ROCKSDB_NAMESPACE {

class Bucket;
class HeatBuckets;

class Bucket {
public:
    double hotness_;
    uint32_t hit_cnt_;

    Bucket();
    ~Bucket();

    // when one time period end, update hotness_, h_(i+1) = alpha * h_i + hit_cnt_ / period_cnt
    void update(const double& alpha, const uint32_t& period_cnt); 
    void hit(); 
};


/*
 load previous read records (read workload) to generate key range seperators, see rocksdb/workload/generator.cc
 after we generate seperators file, we read this file to generate these heat buckets to estimate hotness of every key range
 every bucket corresponding to one key range
*/
class HeatBuckets {
private:
    static std::vector<std::string> seperators_;
    static std::vector<Bucket> buckets_;
    static uint32_t period_cnt_;  // the get count of one period, should be fixed
    static uint32_t current_cnt_; // current get count in this period
    static double alpha_;
    static std::vector<std::unique_ptr<std::mutex>> mutex_ptrs_;
    static std::mutex cnt_mutex_;
    
public:
    HeatBuckets();
    HeatBuckets(const std::string& path, const double& alpha, const uint32_t& period_cnt);
    ~HeatBuckets();

    int32_t locate(const std::string& key); // locate which bucket hitted by this key

    void update(); // update hotness value of all buckets
    void hit(const std::string& key); // one key only hit one bucket (also mean only hit one key range)
    void debug(); // output debug message in standard output
};

}