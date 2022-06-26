#include <algorithm>
#include <cstdio>
#include <iostream>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"

using namespace ROCKSDB_NAMESPACE;

const std::string kDBPath = "/Users/chenlixiang/rocksdb_test";
const size_t kInsert = 1000000;
const size_t kOps = 500000;
const size_t insert_rate = 3;
const size_t query_rate = 7;

const size_t kPrintGap = 100000;

const int hot_range = 1;  // 1/10
const int hot_rate = 9;   // 9/10

const double hot_prob = static_cast<double>(hot_rate) / 10.0;

const size_t op_insert_num = (insert_rate * kOps) / (insert_rate + query_rate);
const size_t op_query_num = (query_rate * kOps) / (insert_rate + query_rate);

const uint64_t hot_insert_up_bound = UINT64_MAX;
const uint64_t hot_insert_low_bound =
    (10 - static_cast<uint64_t>(hot_range)) * (hot_insert_up_bound / 10);
const uint64_t hot_read_low_bound = 0;
const uint64_t hot_read_up_bound =
    hot_read_low_bound + static_cast<uint64_t>(hot_range) * (UINT64_MAX / 10);

// threads num
const int t_num = 8;

struct TestContext {
  std::atomic<size_t> inserted;
  std::atomic<size_t> run_insert;
  std::atomic<size_t> run_query;
  std::atomic<size_t> total_ops;

  uint64_t* insert_nums;
  uint64_t* op_insert;
  uint64_t* op_query;

  TestContext() : inserted(0), run_insert(0), run_query(0), total_ops(0) {
    insert_nums = new uint64_t[kInsert];
    op_insert = new uint64_t[op_insert_num];
    op_query = new uint64_t[op_query_num];
  }

  ~TestContext() {
    delete[] insert_nums;
    delete[] op_insert;
    delete[] op_query;
  }
};

uint64_t systemTime() {
  uint64_t ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch())
                    .count();
  return ms;
}

bool randomBool(double prob) {
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_real_distribution<> dis(0, 1);
  double rn = dis(gen);
  return rn < prob;
}

uint64_t randomUINT64T(uint64_t a, uint64_t b) {
  std::random_device rd;
  std::mt19937_64 gen(rd());
  std::uniform_int_distribution<uint64_t> dis(a, b);
  return dis(gen);
}

size_t randomSIZET(size_t a, size_t b) {
  std::random_device rd;
  std::mt19937_64 gen(rd());
  std::uniform_int_distribution<size_t> dis(a, b);
  return dis(gen);
}

inline std::string numToKey(uint64_t num) {
  std::string num_str = std::to_string(num);
  while (num_str.size() < 20) num_str = '0' + num_str;
  return "user_data_" + num_str;
}

inline std::string numToValue(uint64_t num) {
  std::string value = numToKey(num);
  value.append(1024 - value.size(), 'x');
  return value;
}

void genData(TestContext* ctx) {
  std::vector<uint64_t> hot_read_keys;
  // generate op insert data
  for (size_t i = 0; i < op_insert_num; i++) {
    if (randomBool(hot_prob)) {
      ctx->op_insert[i] =
          randomUINT64T(hot_insert_low_bound, hot_insert_up_bound);
    } else {
      uint64_t num = randomUINT64T(0, hot_insert_low_bound);
      ctx->op_insert[i] = num;
      if (num >= hot_read_low_bound && num <= hot_insert_up_bound) {
        hot_read_keys.push_back(ctx->op_insert[i]);
      }
    }
  }

  // generate init data
  for (size_t i = 0; i < kInsert; i++) {
    uint64_t num = randomUINT64T(0, UINT64_MAX);
    ctx->insert_nums[i] = num;
    if (num >= hot_read_low_bound && num <= hot_read_up_bound) {
      hot_read_keys.push_back(num);
    }
  }

  // generate op query data
  for (size_t i = 0; i < op_query_num; i++) {
    if (randomBool(hot_prob)) {
      size_t idx = randomSIZET(0, hot_read_keys.size() - 1);
      ctx->op_query[i] = hot_read_keys[idx];
    } else {
      size_t idx = randomSIZET(0, op_insert_num - 1);
      ctx->op_query[i] = ctx->op_insert[idx];
    }
  }
}

void insertData(DB* db, TestContext* ctx, uint64_t start) {
  Status s;
  size_t idx;
  auto options = WriteOptions();
  std::string key, value;
  while ((idx = ctx->inserted.fetch_add(1)) < kInsert) {
    uint64_t num = ctx->insert_nums[idx];
    key = numToKey(num), value = numToValue(num);
    s = db->Put(options, key, value);
    assert(s.ok());
    if ((idx + 1) % kPrintGap == 0) {
      uint64_t delta = systemTime() - start;
      std::cout << idx + 1 << " inserts take " << delta / 1000 << "."
                << delta % 1000 << "s" << std::endl;
    }
  }
}

void runOps(DB* db, TestContext* ctx, uint64_t start) {
  Status s;
  size_t idx;
  auto read_options = ReadOptions();
  auto write_options = WriteOptions();
  std::string key, value;
  uint64_t num;
  for (;;) {
    for (size_t i = 0; i < insert_rate; i++) {
      idx = ctx->run_insert.fetch_add(1);
      if (idx >= op_insert_num) {
        break;
      }
      num = ctx->op_insert[idx];
      key = numToKey(num), value = numToValue(num);
      s = db->Put(write_options, key, value);
      assert(s.ok());
    }

    for (size_t i = 0; i < query_rate; i++) {
      idx = ctx->run_query.fetch_add(1);
      if (idx >= op_query_num) {
        break;
      }
      num = ctx->op_query[idx];
      key = numToKey(num);
      s = db->Get(read_options, key, &value);
      if (s.ok()) {
        assert(value == numToValue(num));
      }
    }

    size_t cur_ops = ctx->total_ops.fetch_add(insert_rate + query_rate);
    if ((cur_ops % kPrintGap) == 0) {
      uint64_t delta = systemTime() - start;
      std::cout << cur_ops << " operations take " << delta / 1000 << "."
                << delta % 1000 << " s" << std::endl;
    }
    if (cur_ops >= kOps) {
      break;
    }
  }
}

int main() {
  DB* db;
  Options options;
  // Optimize RocksDB. This is the easiest way to get RocksDB to perform well
  options.OptimizeUniversalStyleCompaction();
  options.IncreaseParallelism(16);
  options.use_direct_io_for_flush_and_compaction = true;
  options.use_direct_reads = true;
  // create the DB if it's not already present
  options.create_if_missing = true;
  options.write_buffer_size = 4 << 20;
  options.max_bytes_for_level_base = 64 << 20;
  options.level0_file_num_compaction_trigger = 4;

  // open DB
  Status s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  // init test context
  std::cout << "start generating data..." << std::endl;
  TestContext ctx;
  genData(&ctx);
  std::cout << "generating data ok!" << std::endl;

  std::thread ts[t_num];
  uint64_t start = systemTime();
  for (int i = 0; i < t_num; i++) {
    ts[i] = std::thread(insertData, db, &ctx, start);
  }
  for (int i = 0; i < t_num; i++) {
    ts[i].join();
  }

  std::cout << "************************" << std::endl;

  start = systemTime();
  for (int i = 0; i < t_num; i++) {
    ts[i] = std::thread(runOps, db, &ctx, start);
  }
  for (int i = 0; i < t_num; i++) {
    ts[i].join();
  }
  std::cout << "Run operations take " << systemTime() - start << std::endl;

  return 0;
}
