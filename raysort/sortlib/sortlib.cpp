#include <algorithm>
#include <array>
#include <cassert>
#include <cstring>
#include <limits>
#include <queue>
#include <vector>

#include "sortlib.h"

void partition_and_sort(Record* records,
                        const size_t num_records,
                        const std::vector<Header>& boundaries,
                        std::vector<Record*>& out_partition_ptrs) {
    std::sort(records, records + num_records, RecordComparator());

    out_partition_ptrs.reserve(boundaries.size());
    Record* ptr = records;
    auto bound = boundaries.begin();
    while (ptr != records + num_records && bound != boundaries.end()) {
        while (ptr->header() < *bound) {
            ++ptr;
        }
        out_partition_ptrs.emplace_back(ptr);
        ++bound;
    }
    assert(out_partition_ptrs.size() == boundaries.size());
}

std::vector<Header> get_boundaries(size_t num_partitions) {
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

std::vector<Record> merge_partitions(std::vector<Record*> parts,
                                     std::vector<size_t> part_sizes) {
    const size_t num_parts = parts.size();
    assert(parts.size() == part_sizes.size());
    std::vector<Record> ret;
    if (num_parts == 0) {
        return ret;
    }
    ret.reserve(part_sizes.front() * num_parts);
    std::priority_queue<SortData, std::vector<SortData>, SortDataComparator>
        heap;

    for (size_t i = 0; i < num_parts; ++i) {
        if (part_sizes[i] > 0) {
            heap.push({parts[i], i, 0});
        }
    }
    while (!heap.empty()) {
        const SortData top = heap.top();
        heap.pop();
        const size_t i = top.partition;
        const size_t j = top.index;
        // printf("Popping %lu %lu\n", i, j);
        ret.push_back(parts[i][j]);  // copying record!
        if (j < part_sizes[i] - 1) {
            // printf("Pushing %lu %lu, limit %lu\n", i, j + 1, part_sizes[i]);
            heap.push({parts[i] + j + 1, i, j + 1});
        }
    }
    return ret;
}