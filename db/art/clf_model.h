#pragma once

#include <string>
#include <vector>
#include <algorithm>
#include <string>
#include <Python.h>
#include <cassert>
#include <iostream>
#include "macros.h"

// dataset data point format: 
// every data point accounts for one segment
// supposed that considering r key range
// every key range have id and hotness ( see heat_buckets )
// so data point features format : 
// LSM-Tree level, Key Range 1 id, Key Range 1 hotness, Key Range 2 id, Key Range 2 hotness, ..., Key Range r id, Key Range r hotness
// remind that heat_buckets recorded hotness is double type, we use uint32_t(uint32_t(SIGNIFICANT_DIGITS * hotness))

namespace ROCKSDB_NAMESPACE {

class ClfModel;

class ClfModel {
private:
    static std::string base_dir_; // dir path for dataset and model
    static uint16_t model_cnt_; // trained model file cnt
    static uint16_t dataset_cnt_; // written dataset file cnt
    static uint16_t feature_num_; // model input features num
public:
    // init member vars
    ClfModel() {
        base_dir_ = MODEL_PATH;
        model_cnt_ = 0;
        dataset_cnt_ = 0;
        feature_num_ = 0;
    }

    // next model file path
    std::string next_model_path() { return base_dir_ + MODEL_PREFIX + std::to_string(model_cnt_) + MODEL_SUFFIX; }

    // next dataset file path
    std::string next_dataset_path() { return base_dir_ + DATASET_PREFIX + std::to_string(dataset_cnt_) + DATASET_SUFFIX; }

    // latest model file path
    std::string latest_model_path() { 
        assert(model_cnt_ > 0);
        return base_dir_ + MODEL_PREFIX + std::to_string(model_cnt_ - 1) + MODEL_SUFFIX;
    }

    // latest dataset file path
    std::string latest_dataset_path() { 
        assert(dataset_cnt_ > 0);
        return base_dir_ + DATASET_PREFIX + std::to_string(dataset_cnt_ - 1) + DATASET_SUFFIX;
    }

    // check whether ready, only need check feature_nums_ now
    bool is_ready() { return feature_num_ > 0; }

    // make ready for training, only need init feature_nums_ now
    void make_ready(std::vector<uint16_t>& features_nums) { 
        Py_Initialize();
	    assert(Py_IsInitialized());

        PyRun_SimpleString("import sys");
	    PyRun_SimpleString("sys.path.append('../models')");

        if (features_nums.empty()) {
            feature_num_ = 41; // debug feature num
        } else {
            feature_num_ = *max_element(features_nums.begin(), features_nums.end()); 
        }

        std::cout << "[DEBUG] ClfModel ready, feature_num_: " << feature_num_ << std::endl;
    }

    ~ClfModel() {
        Py_Finalize();
    }

    // resize data point features
    void prepare_data(std::vector<uint32_t>& data) { 
        assert(data.size() >= 3);
        data.resize(feature_num_, 0); 
    }

    // resize every data point and write to csv file for training
    void write_debug_dataset();
    void write_real_dataset(std::vector<std::vector<uint32_t>>& datas);
    void write_dataset(std::vector<std::vector<uint32_t>>& datas);

    // write dataset and train, return model cnt
    // filter cache caller will check this model cnt and cnt it records, 
    // if model cnt not equal to caller cnt, it will do update job in filter cache
    uint16_t make_train(std::vector<std::vector<uint32_t>>& datas);

    // predict
    void make_predict_samples(std::vector<std::vector<uint32_t>>& datas);
    void make_real_predict(std::vector<std::vector<uint32_t>>& datas, std::vector<uint16_t>& preds);
    void make_predict(std::vector<std::vector<uint32_t>>& datas, std::vector<uint16_t>& preds);
};

}