#include "clf_model.h"
#include <csv2/writer.hpp>
#include <map>
#include <random>
#include <chrono>

namespace ROCKSDB_NAMESPACE {

std::string ClfModel::base_dir_; 
uint16_t ClfModel::model_cnt_; 
uint16_t ClfModel::dataset_cnt_; 
uint16_t ClfModel::feature_num_; 

void ClfModel::write_debug_dataset() {
    // ready for writer
    std::ofstream stream(next_dataset_path());
    csv2::Writer<csv2::delimiter<','>> writer(stream);

    // init hotness values
    std::map<uint32_t, double> hotness_map;
    double base_hotness = 0.01;
    for (int i = 0; i < 200; i ++) {
        float r = static_cast <float> (rand()) / static_cast <float> (RAND_MAX) + base_hotness;
        hotness_map[i] = r;
    }

    // init header vector
    std::vector<std::vector<std::string>> rows;
    std::vector<std::string> header;
    header.emplace_back("Level");
    for (int i = 0; i < 20; i ++) {
        header.emplace_back("Range_" + std::to_string(i));
        header.emplace_back("Hotness_" + std::to_string(i));
    }
    header.emplace_back("Target");
    rows.emplace_back(header);

    // ready for shuffling
    std::vector<uint32_t> ids;
    for(int i = 0; i < 200; i ++) {
        ids.emplace_back(i);
    }

    // generate values
    for (int i = 0; i < 1000; i ++) {
        // std::vector<double> value;
        std::vector<std::string> values;
        uint32_t level = i / 200;
        uint32_t target = 5 - level;
        float r = static_cast <float> (rand()) / static_cast <float> (RAND_MAX);
        if (r > 0.10 * level) {
            target -= 1;
        }

        auto seed = std::chrono::system_clock::now().time_since_epoch().count();
        std::shuffle(ids.begin(), ids.end(), std::default_random_engine(seed));
        values.emplace_back(std::to_string(level));
        for (int i = 0; i < 20; i ++) {
            values.emplace_back(std::to_string(ids[i]));
            values.emplace_back(std::to_string(uint32_t(SIGNIFICANT_DIGITS * hotness_map[ids[i]])));
        }
        values.emplace_back(std::to_string(target));

        rows.emplace_back(values);
    }

    writer.write_rows(rows);
    stream.close();
}

void ClfModel::write_true_dataset(std::vector<std::vector<uint32_t>>& datas) {
    // ready for writer
    std::ofstream stream(next_dataset_path());
    csv2::Writer<csv2::delimiter<','>> writer(stream);

    // init csv header vector
    std::vector<std::vector<std::string>> rows;
    std::vector<std::string> header;
    uint16_t ranges_num = (feature_num_ - 1) / 2;
    header.emplace_back("Level");
    for (int i = 0; i < ranges_num; i ++) {
        header.emplace_back("Range_" + std::to_string(i));
        header.emplace_back("Hotness_" + std::to_string(i));
    }
    // remind that targeted class is in csv Target column
    // corresponding to code of lgb.py in ../models dir
    header.emplace_back("Target");
    rows.emplace_back(header);

    for (std::vector<uint32_t>& data : datas) {
        prepare_data(data);
        rows.emplace_back(data);
    }

    writer.write_rows(rows);
    stream.close();
}

void ClfModel::write_dataset(std::vector<std::vector<uint32_t>>& datas) {
    if (datas.empty()) {
        write_debug_dataset();
        // dataset_cnt_ += 1;
        return;
    }

    assert(feature_num_ % 2 != 0); // features num: 2r + 1 

    write_true_dataset(datas);
    // dataset_cnt_ += 1;
    return;
}

uint16_t ClfModel::make_train(std::vector<std::vector<uint32_t>>& datas) {
    write_dataset(datas);

    PyObject* pModule = PyImport_ImportModule("lgb");
	assert(pModule != nullptr);

    PyObject* pFunc = PyObject_GetAttrString(pModule, "train");
	assert(pFunc != nullptr && PyCallable_Check(pFunc));


    PyObject* pArg = PyTuple_New(2);
    PyTuple_SetItem(pArg, 0, Py_BuildValue("s", next_dataset_path().c_str())); 
    PyTuple_SetItem(pArg, 1, Py_BuildValue("s", next_model_path().c_str())); 

    PyObject_CallObject(pFunc, pArg);

    dataset_cnt_ += 1;
    model_cnt_ += 1;

    Py_DECREF(pModule);
    Py_DECREF(pFunc);
    Py_DECREF(pArg);

    return model_cnt_;
}

void ClfModel::make_predict(std::vector<std::vector<uint32_t>>& datas, std::vector<uint16_t>& preds) {
    preds.clear();
    for (int i = 0; i < datas.size(); i ++) {
        prepare_data(datas[i]);
    }

    PyObject* pModule = PyImport_ImportModule("lgb");
	assert(pModule == nullptr);

    PyObject* pFunc = PyObject_GetAttrString(pModule, "predict");
	assert(pFunc != nullptr && PyCallable_Check(pFunc));

    PyObject* pArg = PyTuple_New(2);
    PyTuple_SetItem(pArg, 0, Py_BuildValue("s", latest_model_path())); 

    PyObject* pDatas = PyList_New(0);
    PyObject* pData = nullptr;
    for (std::vector<uint32_t>& data : datas) {
        pData = PyList_New(0);
        for (uint32_t& feature : data) {
            PyList_Append(pData, Py_BuildValue("i", feature));
        }
        PyList_Append(pDatas, pData);
    }
    
    PyTuple_SetItem(pArg, 1, pDatas); 

    PyObject* pReturn = PyObject_CallObject(pFunc, pArg); // should return list

    for (int i = 0; i < datas.size(); i ++) {
        int nResult = 0;
        PyArg_Parse(PyList_GetItem(pReturn, i), "i", &nResult);
        preds.emplace_back(nResult);
    }
    assert(preds.size() != 0);

    Py_DECREF(pModule);
    Py_DECREF(pFunc);
    Py_DECREF(pArg);
    Py_DECREF(pDatas);
    Py_DECREF(pReturn);
}

}