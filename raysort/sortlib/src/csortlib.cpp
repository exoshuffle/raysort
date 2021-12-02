#include "csortlib.h"

#include <algorithm>
#include <array>
#include <cassert>
#include <chrono>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <fstream>
#include <iostream>
#include <limits>
#include <memory>
#include <queue>
#include <thread>
#include <vector>

namespace csortlib {

template <typename T>
size_t _TotalSize(const std::vector<T> &parts) {
  size_t ret = 0;
  for (const auto &part : parts) {
    ret += part.size;
  }
  return ret;
}

#ifdef CSORTLIB_USE_ALT_MEMORY_LAYOUT

// 16-byte word-aligned data structure for sorting.
// Significantly more cache-friendly.
struct SortItem {
  uint8_t header[HEADER_SIZE];
  Record *ptr;
  uint8_t unused[2];
};

struct SortItemComparator {
  inline bool operator()(const SortItem &a, const SortItem &b) {
    return std::memcmp(a.header, b.header, HEADER_SIZE) < 0;
  }
};

std::unique_ptr<std::vector<SortItem>> _MakeSortItems(const Array<Record> &record_array) {
  Record *const records = record_array.ptr;
  const size_t num_records = record_array.size;
  auto ret = std::make_unique<std::vector<SortItem>>();
  ret->reserve(num_records);
  SortItem item;
  for (Record *ptr = records; ptr != records + num_records; ++ptr) {
    memcpy(item.header, ptr->header, HEADER_SIZE);
    item.ptr = ptr;
    ret->emplace_back(item);
  }
  return ret;
}

#endif

std::vector<Partition> SortAndPartition(const Array<Record> &record_array,
                                        const std::vector<Key> &boundaries, bool skip_sorting) {
  Record *const records = record_array.ptr;
  const size_t num_records = record_array.size;

  if (!skip_sorting) {
#ifdef CSORTLIB_USE_ALT_MEMORY_LAYOUT
  auto sort_items = _MakeSortItems(record_array);

  const auto start1 = std::chrono::high_resolution_clock::now();

  std::sort(sort_items->begin(), sort_items->end(), HeaderComparator<SortItem>());

  const auto stop1 = std::chrono::high_resolution_clock::now();
  printf("Sort,%ld\n",
         std::chrono::duration_cast<std::chrono::milliseconds>(stop1 - start1).count());

  Record *buffer = new Record[num_records];
  Record *cur = buffer;
  for (const auto &item : *sort_items) {
    memcpy(cur++, item.ptr, sizeof(Record));
  }
  memcpy(records, buffer, sizeof(Record) * num_records);
  delete[] buffer;
#else
  std::sort(records, records + num_records, HeaderComparator<Record>());
#endif
  }

  std::vector<Partition> ret;
  ret.reserve(boundaries.size());
  auto bound_it = boundaries.begin();
  size_t off = 0;
  size_t prev_off = 0;
  while (off < num_records && bound_it != boundaries.end()) {
    const Key bound = *bound_it;
    while (off < num_records && records[off].key() < bound) {
      ++off;
    }
    const size_t size = off - prev_off;
    if (!ret.empty()) {
      ret.back().size = size;
    }
    ret.emplace_back(Partition{off, 0});
    ++bound_it;
    prev_off = off;
  }
  if (!ret.empty()) {
    ret.back().size = num_records - prev_off;
  }
  assert(ret.size() == boundaries.size());
  assert(_TotalSize(ret) == num_records);
  return ret;
}

std::vector<Key> GetBoundaries(size_t num_partitions) {
  std::vector<Key> ret;
  ret.reserve(num_partitions);
  const Key min = 0;
  const Key max = std::numeric_limits<Key>::max();
  const Key step = ceil(max / num_partitions);
  Key boundary = min;
  for (size_t i = 0; i < num_partitions; ++i) {
    ret.emplace_back(boundary);
    boundary += step;
  }
  return ret;
}

struct SortData {
  const Record *record;
  int partition;
  size_t index;
};

struct SortDataComparator {
  bool operator()(const SortData &a, const SortData &b) {
    return !HeaderComparator<Record>()(*a.record, *b.record);
  }
};

class Merger::Impl {
 public:
  std::vector<ConstArray<Record>> parts_;
  std::priority_queue<SortData, std::vector<SortData>, SortDataComparator> heap_;

  Impl(const std::vector<ConstArray<Record>> &parts) : parts_(parts) {
    for (int i = 0; i < (int)parts_.size(); ++i) {
      _PushFirstItem(parts_[i], i);
    }
  }

  inline void _PushFirstItem(const ConstArray<Record> &part, int part_id) {
    if (part.size > 0) {
      heap_.push({part.ptr, part_id, 0});
    }
  }

  void Refill(const ConstArray<Record> &part, int part_id) {
    assert(part_id < parts_.size());
    parts_[part_id] = part;
    _PushFirstItem(part, part_id);
  }

  GetBatchRetVal GetBatch(Record *const &ret, size_t max_num_records) {
    size_t cnt = 0;
    auto cur = ret;
    while (!heap_.empty()) {
      if (cnt >= max_num_records) {
        return std::make_pair(cnt, -1);
      }
      const SortData top = heap_.top();
      heap_.pop();
      const int i = top.partition;
      const size_t j = top.index;
      // Copy record to output array
      *cur++ = *top.record;
      ++cnt;
      if (j + 1 < parts_[i].size) {
        heap_.push({top.record + 1, i, j + 1});
      } else {
        return std::make_pair(cnt, i);
      }
    }
    return std::make_pair(cnt, -1);
  }
};

Merger::Merger(const std::vector<ConstArray<Record>> &parts)
    : impl_(std::make_unique<Impl>(parts)) {}

GetBatchRetVal Merger::GetBatch(Record *const &ret, size_t max_num_records) {
  return impl_->GetBatch(ret, max_num_records);
}

void Merger::Refill(const ConstArray<Record> &part, int part_id) {
  return impl_->Refill(part, part_id);
}

Array<Record> MergePartitions(const std::vector<ConstArray<Record>> &parts) {
  const size_t num_records = _TotalSize(parts);
  if (num_records == 0) {
    return {nullptr, 0};
  }
  Record *const ret = new Record[num_records];  // need to manually delete
  Merger merger(parts);
  merger.GetBatch(ret, num_records);
  return {ret, num_records};
}

void _ReadPartition(const std::string &filename, size_t offset, char *buffer,
                    size_t buffer_size, ConstArray<Record> *ret) {
  printf("Read %s offset=%lu\n", filename.c_str(), offset);
  std::ifstream fin(filename, std::ios::in | std::ios::binary);
  fin.seekg(offset);  // TODO: what bits does this set, if any?
  fin.read(buffer, buffer_size);
  const auto bytes_read = fin.gcount();
  assert(bytes_read % RECORD_SIZE == 0);
  ret->ptr = (Record *)buffer;
  ret->size = bytes_read / RECORD_SIZE;
}

FileMerger::FileMerger(const std::vector<std::string> &input_files,
                       const std::string &output_file, size_t input_batch_bytes,
                       size_t output_batch_records)
    : input_files_(input_files),
      output_file_(output_file),
      input_batch_bytes_(input_batch_bytes),
      output_batch_records_(output_batch_records),
      num_inputs_(input_files.size()) {}

size_t FileMerger::Run() {
  // Allocate buffers.
  std::vector<std::unique_ptr<char[]>> input_buffers;
  std::vector<ConstArray<Record>> input_record_arrays;
  input_buffers.reserve(num_inputs_);
  input_record_arrays.reserve(num_inputs_);
  for (size_t i = 0; i < num_inputs_; ++i) {
    input_buffers.emplace_back(std::make_unique<char[]>(input_batch_bytes_));
    input_record_arrays.emplace_back(ConstArray<Record>());
  }
  auto output_buffer = std::make_unique<char[]>(output_batch_records_ * RECORD_SIZE);

  // Load initial partitions.
  std::vector<std::thread> input_threads;
  input_threads.reserve(num_inputs_);
  for (size_t i = 0; i < num_inputs_; ++i) {
    input_threads.emplace_back(std::thread(_ReadPartition, input_files_[i], 0,
                                           input_buffers[i].get(), input_batch_bytes_,
                                           &input_record_arrays[i]));
  }
  for (auto &thread : input_threads) {
    thread.join();
  }

  // initialize merger with these partitions
  // keep track of file offsets
  // launch file write thread
  // Merger merger;
  return 0;
}

}  // namespace csortlib
