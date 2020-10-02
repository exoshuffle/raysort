#ifndef __SORTLIB_H__
#define __SORTLIB_H__

#include <cstring>
#include <memory>
#include <vector>

namespace sortlib {

const size_t KEY_SIZE = 10;
const size_t RECORD_SIZE = 100;

// We consider the first 8 bytes of the key as a 64-bit unsigned integer, and
// use it to partition the key space. We call it the "header".
typedef unsigned long long Header;
const size_t HEADER_SIZE = sizeof(Header);

struct Record {
    char key[KEY_SIZE];
    char data[RECORD_SIZE - KEY_SIZE];

    Header header() const { return (Header)*key; }
};

struct RecordComparator {
    bool operator()(const Record& a, const Record& b) {
        return std::memcmp(a.key, b.key, KEY_SIZE) < 0;
    }
};

struct RecordArray {
    Record* ptr;
    size_t size;
};

inline bool operator==(const RecordArray& a, const RecordArray& b) {
    return a.ptr == b.ptr && a.size == b.size;
}

// Sort the data in-place, then return a list of pointers to the partition
// boundaries. The i-th pointer is the first record in the i-th partition.
// If the i-th partition is empty, then ret[i] == ret[i + 1].
//
// Invariants:
// - ret[0] === data
// - ret[i] < data + num_records * sizeof(Record) for all i
//
// CPU cost: O(Pm * log(Pm))
// Memory cost: 0
// where Pm == len(records)
std::vector<RecordArray> PartitionAndSort(
    const RecordArray& record_array,
    const std::vector<Header>& boundaries);

// Partition the data by copying them into chunks (unsorted).
// TODO: not implemented. Asymptotically slower than using PartitionAndSort.
//
// CPU cost: O(Pm * R)
// Memory cost: O(Pm)
// where Pm == len(records), R == len(boundaries)
// std::vector<std::vector<Record>> Partition(
//     Record* records,
//     const size_t num_records,
//     const std::vector<Header>& boundaries);

// Compute the boundaries by partitioning the key space into partitions.
// Return the boundary integers.
// E.g. the headers (first 8 bytes) of all records in the i-th partition
// must be in the half-open interval [ boundaries[i], boundaries[i + 1] ).
// TODO: this will be more complicated for skewed distribution.
std::vector<Header> GetBoundaries(size_t num_partitions);

// Merge M sorted partitions into final output.
//
// CPU cost: O(Pr * log(M))
// where Pr == sum(len(p) for p in partitions), M == len(partitions)
RecordArray MergePartitions(const std::vector<RecordArray>& partitions);

}  // namespace sortlib

#endif
