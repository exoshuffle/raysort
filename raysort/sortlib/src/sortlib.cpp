#include <algorithm>
#include <array>
#include <cassert>
#include <cmath>
#include <cstdio>
#include <cstring>
#include <limits>
#include <queue>
#include <vector>

#include "sortlib.h"

namespace sortlib {

template <typename T>
size_t _TotalSize(const std::vector<T>& parts) {
    size_t ret = 0;
    for (const auto& part : parts) {
        ret += part.size;
    }
    return ret;
}

std::vector<Partition> SortAndPartition(const Array<Record> record_array,
                                        const std::vector<Key>& boundaries) {
    Record* const records = record_array.ptr;
    const size_t num_records = record_array.size;
    std::sort(records, records + num_records, RecordComparator());

    std::vector<Partition> ret;
    ret.reserve(boundaries.size());
    auto bound = boundaries.begin();
    size_t off = 0;
    size_t prev_off = 0;
    while (off < num_records && bound != boundaries.end()) {
        while (records[off].key() < *bound) {
            ++off;
        }
        const size_t size = off - prev_off;
        if (!ret.empty()) {
            ret.back().size = size;
        }
        ret.emplace_back(Partition{off, 0});
        ++bound;
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
    Record* record;
    size_t partition;
    size_t index;
};

struct SortDataComparator {
    bool operator()(const SortData& a, const SortData& b) {
        return !RecordComparator()(*a.record, *b.record);
    }
};

Array<Record> MergePartitions(const std::vector<Array<Record>>& parts) {
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
