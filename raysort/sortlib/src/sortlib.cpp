#include <algorithm>
#include <array>
#include <cassert>
#include <cstdio>
#include <cstring>
#include <limits>
#include <queue>
#include <vector>

#include "sortlib.h"

namespace sortlib {

size_t _TotalSize(const std::vector<RecordArray>& parts) {
    size_t ret = 0;
    for (const auto& part : parts) {
        ret += part.size;
    }
    return ret;
}

std::vector<RecordArray> PartitionAndSort(
    const RecordArray& record_array,
    const std::vector<Header>& boundaries) {
    Record* records = record_array.ptr;
    const size_t num_records = record_array.size;
    std::sort(records, records + num_records, RecordComparator());

    std::vector<RecordArray> ret;
    ret.reserve(boundaries.size());
    auto bound = boundaries.begin();
    Record* prev_ptr = records;
    Record* ptr = records;
    while (ptr != records + num_records && bound != boundaries.end()) {
        while (ptr->header() < *bound) {
            ++ptr;
        }
        const size_t size = ptr - prev_ptr;
        if (!ret.empty()) {
            ret.back().size = size;
        }
        ret.emplace_back(RecordArray{ptr, 0});
        ++bound;
        prev_ptr = ptr;
    }
    if (!ret.empty()) {
        ret.back().size = records + num_records - prev_ptr;
    }
    assert(ret.size() == boundaries.size());
    assert(_TotalSize(ret) == num_records);
    return ret;
}

std::vector<Header> GetBoundaries(size_t num_partitions) {
    std::vector<Header> ret;
    ret.reserve(num_partitions);
    const Header min_limit = std::numeric_limits<Header>::min();
    assert(min_limit == 0);
    const Header max_limit = std::numeric_limits<Header>::max();
    const Header step = (max_limit - min_limit) / num_partitions + 1;
    Header boundary = min_limit;
    for (size_t i = 0; i < num_partitions; ++i) {
        ret.emplace_back(boundary);
        boundary += step;
    }
    return ret;
}

struct SortData {
    Record* record;
    size_t partition;
    size_t index;
};

struct SortDataComparator {
    bool operator()(const SortData& a, const SortData& b) {
        return !RecordComparator()(*a.record, *b.record);
    }
};

RecordArray MergePartitions(const std::vector<RecordArray>& parts) {
    const size_t num_parts = parts.size();
    if (num_parts == 0) {
        return {nullptr, 0};
    }
    const size_t num_records = _TotalSize(parts);
    if (num_records == 0) {
        return {nullptr, 0};
    }
    Record* const ret = new Record[num_records];
    auto cur = ret;
    std::priority_queue<SortData, std::vector<SortData>, SortDataComparator>
        heap;

    for (size_t i = 0; i < num_parts; ++i) {
        if (parts[i].size > 0) {
            heap.push({parts[i].ptr, i, 0});
        }
    }
    while (!heap.empty()) {
        const SortData top = heap.top();
        heap.pop();
        const size_t i = top.partition;
        const size_t j = top.index;
        // Copy record to output array
        *cur++ = parts[i].ptr[j];
        if (j + 1 < parts[i].size) {
            heap.push({parts[i].ptr + j + 1, i, j + 1});
        }
    }
    assert(cur == ret + num_records);
    return {ret, num_records};
}

}  // namespace sortlib
