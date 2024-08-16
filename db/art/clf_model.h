#pragma once

#include <string>
#include <vector>
#include <algorithm>
#include <string>
#include <cassert>
#include <iostream>
#include "macros.h"

// dataset data point format: 
// every data point accounts for one segment
// supposed that considering r key range
// every key range have id and hotness ( see heat_buckets )
// so data point features format : 
// LSM-Tree level, Key Range 1 id, Key Range 1 hotness, Key Range 2 id, Key Range 2 hotness, ..., Key Range r id, Key Range r hotness
// remind that heat_buckets recorded hotness is double type, 
// we use uint32_t(uint32_t(SIGNIFICANT_DIGITS * hotness)) to closely estimate its hotness value

namespace ROCKSDB_NAMESPACE {

struct RangeHeatPair;
class ClfModel;
bool RangeHeatPairLesserComparor(const RangeHeatPair& pair_1, const RangeHeatPair& pair_2);
bool RangeHeatPairGreaterComparor(const RangeHeatPair& pair_1, const RangeHeatPair& pair_2);

struct RangeHeatPair {
    uint32_t range_id;
    double hotness_val;
    RangeHeatPair(const uint32_t& id, const double& hotness) {
        range_id = id; hotness_val = hotness;
    }
};

bool RangeHeatPairLesserComparor(const RangeHeatPair& pair_1, const RangeHeatPair& pair_2) {
    return pair_1.hotness_val < pair_2.hotness_val;
}

bool RangeHeatPairGreaterComparor(const RangeHeatPair& pair_1, const RangeHeatPair& pair_2) {
    return pair_1.hotness_val > pair_2.hotness_val;
}

class ClfModel {
private:
    static uint16_t feature_num_; // model input features num
    static std::string dataset_name_; // dataset csv file name
    static std::string dataset_path_; // path to save dataset csv file
    static std::string host_, port_; // lightgbm server connection
    static size_t buffer_size_; // socket receive buffer max size
public:
    // init member vars
    ClfModel() {
        feature_num_ = 0;
        dataset_name_ = DATASET_NAME;
        // MODEL_PATH must end with '/'
        dataset_path_ = MODEL_PATH;
        dataset_path_ = dataset_path_ + DATASET_NAME;
        host_ = HOST; port_ = PORT;
        buffer_size_ = BUFFER_SIZE;
    }

    // check whether ready, only need check feature_nums_ now
    bool is_ready() { return feature_num_ > 0; }

    // make ready for training, only need init feature_nums_ now
    // when first call ClfModel, we need to use current segments information to init features_num_
    // we can calcuate feature nums for every segment, 
    // feature num = level feature num (1) + 2 * num of key ranges segment covers
    // we set features_num_ to largest feature num
    void make_ready(std::vector<uint16_t>& features_nums) { 
        if (features_nums.empty()) {
            feature_num_ = 41; // debug feature num, see ../lgb_server files
        } else {
            feature_num_ = *max_element(features_nums.begin(), features_nums.end()); 
        }

        // std::cout << "[DEBUG] ClfModel ready, feature_num_: " << feature_num_ << std::endl;
    }

    ~ClfModel() {
        return; // do nothing
    }

    // resize data point features
    void prepare_data(std::vector<uint32_t>& data) { 
        // at least level feature and one key range, so data size always >= 3
        assert(data.size() >= 3);
        data.resize(feature_num_, 0); 
    }

    // resize every data point and write to csv file for training
    void write_debug_dataset();
    void write_real_dataset(std::vector<std::vector<uint32_t>>& datas, std::vector<uint16_t>& tags, std::vector<uint32_t>& get_cnts);
    void write_dataset(std::vector<std::vector<uint32_t>>& datas, std::vector<uint16_t>& tags, std::vector<uint32_t>& get_cnts);

    // write dataset then send msg to train new model in LightGBM server side
    void make_train(std::vector<std::vector<uint32_t>>& datas, std::vector<uint16_t>& tags, std::vector<uint32_t>& get_cnts);

    // predict
    void make_predict_samples(std::vector<std::vector<uint32_t>>& datas);
    void make_real_predict(std::vector<std::vector<uint32_t>>& datas, std::vector<uint16_t>& preds);
    void make_predict(std::vector<std::vector<uint32_t>>& datas, std::vector<uint16_t>& preds);
};

}