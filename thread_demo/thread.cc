#include <iostream>
// Use #include "task_thread_pool.hpp" for relative path,
// and #include <task_thread_pool.hpp> if installed in include path
#include "task_thread_pool.hpp"

// func outside class with arguments input and return value
bool outerFunc1(const bool& arg1, const bool& arg2) {
    bool arg = arg1 && arg2;
    std::cout << "call to outerFunc1: " << (arg) << std::endl;
    return arg;
}

// func outside class with return value
bool outerFunc2() {
    bool arg = true;
    std::cout << "call to outerFunc2: " << (arg) << std::endl;
    return arg;
}

// func outside class with arguments input
void outerFunc3(const bool& arg1, const bool& arg2) {
    bool arg = arg1 && arg2;
    std::cout << "call to outerFunc3: " << (arg) << std::endl;
    return;
}

// func outside class
void outerFunc4() {
    bool arg = false;
    std::cout << "call to outerFunc4: " << (arg) << std::endl;
    return;
}

class ThreadDemo {
private:
    static task_thread_pool::task_thread_pool pool_;

    // func in class should be "static"
    // func inside class with arguments input and return value
    static bool innerFunc1(const bool& arg1, const bool& arg2) {
        bool arg = arg1 && arg2;
        std::cout << "call to innerFunc1: " << (arg) << std::endl;
        return arg;
    }

    // func inside class with return value
    static bool innerFunc2() {
        bool arg = true;
        std::cout << "call to innerFunc2: " << (arg) << std::endl;
        return arg;
    }

    // func inside class with arguments input
    static void innerFunc3(const bool& arg1, const bool& arg2) {
        bool arg = arg1 && arg2;
        std::cout << "call to innerFunc3: " << (arg) << std::endl;
        return;
    }

    // func inside class
    static void innerFunc4() {
        bool arg = false;
        std::cout << "call to innerFunc4: " << (arg) << std::endl;
        return;
    }

public:
    void thread_test() {
        std::future<bool> func_future_1, func_future_2, func_future_3, func_future_4;
        func_future_1 = pool_.submit(outerFunc1, true, true);
        func_future_2 = pool_.submit(outerFunc2);
        pool_.submit_detach(outerFunc3, true, false);
        pool_.submit_detach(outerFunc4);
        func_future_3 = pool_.submit(innerFunc1, true, true);
        func_future_4 = pool_.submit(innerFunc2);
        pool_.submit_detach(innerFunc3, true, false);
        pool_.submit_detach(innerFunc4);

        pool_.wait_for_tasks();
    }
};

task_thread_pool::task_thread_pool ThreadDemo::pool_;

int main() {
    ThreadDemo demo;
    demo.thread_test();
    return 0;
}