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
    printf("CPP: PartitionAndSort(%p, %lu)\n", record_array.ptr,
           record_array.size);
    Record* records = record_array.ptr;
    const size_t num_records = record_array.size;
    printf("first byte: %d\n", records[0].key[0]);
    std::sort(records, records + num_records, RecordComparator());
    printf("sorted\n");

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

std::vector<Record> MergePartitions(const std::vector<RecordArray>& parts) {
    const size_t num_parts = parts.size();
    std::vector<Record> ret;
    if (num_parts == 0) {
        return ret;
    }
    ret.reserve(_TotalSize(parts));
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
        // printf("Popping %lu %lu\n", i, j);
        ret.push_back(parts[i].ptr[j]);  // copying record!
        if (j + 1 < parts[i].size) {
            // printf("Pushing %lu %lu, limit %lu\n", i, j + 1, part_sizes[i]);
            heap.push({parts[i].ptr + j + 1, i, j + 1});
        }
    }
    return ret;
}

}  // namespace sortlib
