#ifndef __SORTLIB_H__
#define __SORTLIB_H__

#include <cstring>
#include <memory>
#include <vector>

namespace sortlib {

const size_t HEADER_SIZE = 10;
const size_t RECORD_SIZE = 100;

// We consider the first 8 bytes of the header as a 64-bit unsigned integer
// "key". The key is used to partition records.
typedef uint64_t Key;
const size_t KEY_SIZE = sizeof(Key);

struct Record {
    uint8_t header[HEADER_SIZE];
    uint8_t body[RECORD_SIZE - HEADER_SIZE];

    // Assuming current architecture is little endian.
    inline Key key() const { return __builtin_bswap64(*(Key*)header); }
};

struct RecordComparator {
    inline bool operator()(const Record& a, const Record& b) {
        return std::memcmp(a.header, b.header, HEADER_SIZE) < 0;
    }
};

template <typename T>
struct Array {
    T* ptr;
    size_t size;
};

struct Partition {
    size_t offset;
    size_t size;
};

inline bool operator==(const Partition& a, const Partition& b) {
    return a.offset == b.offset && a.size == b.size;
}

// Sort the data in-place, then return a list of partitions. A partition
// is represented by an offset and a size. If the i-th partition is empty,
// then ret[i].offset == ret[i + 1].offset, and ret[i].size == 0.
//
// Invariants:
// - ret[0].offset === 0
// - ret[i] < num_records for all i
//
// CPU cost: O(Pm * log(Pm))
// Memory cost: 0
// where Pm == len(records)
std::vector<Partition> SortAndPartition(const Array<Record> record_array,
                                        const std::vector<Key>& boundaries);

// Compute the boundaries by partitioning the key space into partitions.
// Return the boundary integers.
// E.g. the keys (first 8 bytes) of all records in the i-th partition
// must be in the half-open interval [ boundaries[i], boundaries[i + 1] ).
// TODO: this will be more complicated for skewed distribution.
std::vector<Key> GetBoundaries(size_t num_partitions);

// Merge M sorted partitions into final output.
//
// CPU cost: O(Pr * log(M))
// where Pr == sum(len(p) for p in partitions), M == len(partitions)
Array<Record> MergePartitions(const std::vector<Array<Record>>& partitions);

}  // namespace sortlib

#endif
