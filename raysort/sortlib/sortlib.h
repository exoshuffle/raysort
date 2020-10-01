#ifndef __SORTLIB_H__
#define __SORTLIB_H__

#include <cstring>
#include <memory>
#include <vector>

const size_t KEY_SIZE = 10;
const size_t RECORD_SIZE = 100;

// We consider the first 8 bytes of the key as a 64-bit unsigned integer, and
// use it to partition the key space. We call it the "header".
using Header = unsigned long long;
const size_t HEADER_SIZE = sizeof(Header);

struct Record {
    char key[KEY_SIZE];
    char data[RECORD_SIZE - KEY_SIZE];

    Header header() const { return (Header)*key; }
};

struct RecordComparator {
    bool operator()(const Record& a, const Record& b) {
        return std::memcmp(a.key, b.key, KEY_SIZE) <= 0;
    }
};

// Sort the data in-place, then return a list of pointers to the partition
// boundaries. The i-th pointer is the first record in the i-th partition.
// If the i-th partition is empty, then
// out_partition_ptrs[i] == out_partition_ptrs[i + 1].
//
// Invariants:
// - out_partition_ptrs[0] === data
// - out_partition_ptrs[i] < data + num_records * sizeof(Record) for all i
//
// CPU cost: O(Pm * log(Pm))
// Memory cost: 0
// where Pm == len(records)
void partition_and_sort(Record* records,
                        const size_t num_records,
                        const std::vector<Header>& boundaries,
                        std::vector<Record*>& out_partition_ptrs);

// Partition the data by copying them into chunks (unsorted).
// TODO: not implemented. Asymptotically slower than using partition_and_sort.
//
// CPU cost: O(Pm * R)
// Memory cost: O(Pm)
// where Pm == len(records), R == len(boundaries)
std::vector<std::vector<Record>> partition(
    Record* records,
    const size_t num_records,
    const std::vector<Header>& boundaries);

// Compute the boundaries by partitioning the key space into partitions.
// Return the boundary integers.
// E.g. the headers (first 8 bytes) of all records in the i-th partition
// must be in the half-open interval [ boundaries[i], boundaries[i + 1] ).
// TODO: this will be more complicated for skewed distribution.
std::vector<Header> get_boundaries(size_t num_partitions);

// Merge M sorted partitions into final output.
//
// CPU cost: O(Pr * log(M))
// where Pr == sum(len(p) for p in partitions), M == len(partitions)
std::vector<Record> merge_partitions(std::vector<Record*> partitions,
                                     std::vector<size_t> partition_sizes);

#endif
