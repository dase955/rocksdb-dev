#include <LightGBM/c_api.h>
#include <csv2/writer.hpp>
#include <LightGBM/boosting.h>

#include <Python.h>

#include <map>
#include <iostream>
#include <random>
#include <algorithm>
#include <chrono>
#include <vector>
#include <string>

void generate_samples() {
    // ready for writer
    std::ofstream stream("lgb.csv");
    csv2::Writer<csv2::delimiter<','>> writer(stream);

    // init hotness values
    std::map<int, double> hotness_map;
    double base_hotness = 0.1;
    for (int i=0; i<200; i++) {
        float r = static_cast <float> (rand()) / static_cast <float> (RAND_MAX) + base_hotness;
        hotness_map[i] = r;
    }

    // init header vector
    std::vector<std::vector<std::string>> rows;
    std::vector<std::string> header;
    header.emplace_back("Level");
    for (int i=0; i<20; i++) {
        header.emplace_back("Range_" + std::to_string(i));
        header.emplace_back("Hotness_" + std::to_string(i));
    }
    header.emplace_back("Target");
    rows.emplace_back(header);

    // ready for shuffling
    std::vector<int> ids;
    for(int i=0; i<200; i++) {
        ids.emplace_back(i);
    }

    // generate values
    for (int i=0; i<1000; i++) {
        // std::vector<double> value;
        std::vector<std::string> values;
        int level = i / 200;
        int target = 5 - level;
        float r = static_cast <float> (rand()) / static_cast <float> (RAND_MAX);
        if (r > 0.10 * level) {
            target -= 1;
        }

        auto seed = std::chrono::system_clock::now().time_since_epoch().count();
        std::shuffle(ids.begin(), ids.end(), std::default_random_engine(seed));
        values.emplace_back(std::to_string(level));
        for (int i=0; i<20; i++) {
            values.emplace_back(std::to_string(ids[i]));
            values.emplace_back(std::to_string(int(1e8 * hotness_map[ids[i]])));
        }
        values.emplace_back(std::to_string(target));

        rows.emplace_back(values);
    }

    writer.write_rows(rows);
    stream.close();
}

void train() {
    PyObject* pModule = PyImport_ImportModule("lgb");
	if( pModule == NULL ){
		std::cout <<"module not found" << std::endl;
		exit(EXIT_FAILURE);
	}

    PyObject* pFunc = PyObject_GetAttrString(pModule, "train");
	if( !pFunc || !PyCallable_Check(pFunc)){
		std::cout <<"not found function" << std::endl;
		exit(EXIT_FAILURE);
	}

    PyObject* pArg = PyTuple_New(2);
    PyTuple_SetItem(pArg, 0, Py_BuildValue("s", "lgb.csv")); 
    PyTuple_SetItem(pArg, 1, Py_BuildValue("s", "lgb.txt")); 

    PyObject_CallObject(pFunc, pArg);
}

int predict() {
    std::vector<int> sample = {
        0,1,49438291,178,101302671,106,74108064,43,
        99152946,172,10416160,118,26960709,191,97761380,
        185,19964006,44,38331472,49,104932701,123,
        86047524,51,18605583,69,76772379,158,56142050,175,
        28037226,81,26597416,197,102537655,177,49169021,
        159,91967731,145,34006237
    };

    PyObject* pModule = PyImport_ImportModule("lgb");
	if( pModule == NULL ){
		std::cout <<"module not found" << std::endl;
		exit(EXIT_FAILURE);
	}

    PyObject* pFunc = PyObject_GetAttrString(pModule, "predict");
	if( !pFunc || !PyCallable_Check(pFunc)){
		std::cout <<"not found function" << std::endl;
		exit(EXIT_FAILURE);
	}

    PyObject* pArg = PyTuple_New(2);
    PyTuple_SetItem(pArg, 0, Py_BuildValue("s", "lgb.txt")); 

    PyObject* pData = PyList_New(0);
    for (int feature : sample) {
        PyList_Append(pData, Py_BuildValue("i", feature));
    }
    PyTuple_SetItem(pArg, 1, pData); 

    PyObject* pReturn = PyObject_CallObject(pFunc, pArg);

    int nResult;
    PyArg_Parse(pReturn, "i", &nResult);
    // std::cout << "return result is " << nResult << std::endl;
    return nResult;
}

int main() {
    Py_Initialize();
	if(!Py_IsInitialized()){
		std::cout << "python init fail" << std::endl;
		exit(EXIT_FAILURE);
	}

    PyRun_SimpleString("import sys");
	PyRun_SimpleString("sys.path.append('.')");

    generate_samples(); 
    train();

    std::cout << "return result is " << predict() << std::endl;

    Py_Finalize();

    return EXIT_SUCCESS;
}