# distutils: language = c++
# distutils: sources = src/sortlib.cpp

from cpython.array cimport array
from libc.stdint cimport uint8_t, uint64_t
from libcpp.vector cimport vector

import numpy as np

cdef extern from "src/sortlib.h" namespace "sortlib":
    const size_t HEADER_SIZE
    const size_t RECORD_SIZE
    ctypedef uint64_t Key
    ctypedef struct Record:
        uint8_t header[HEADER_SIZE]
        uint8_t body[RECORD_SIZE - HEADER_SIZE]
    ctypedef struct Partition:
        size_t offset
        size_t size
    cdef cppclass Array[T]:
        T* ptr
        size_t size
    cdef cppclass ConstArray[T]:
        const T* ptr
        size_t size
    cdef vector[Key] GetBoundaries(size_t num_partitions)
    cdef vector[Partition] SortAndPartition(const Array[Record]& record_array, const vector[Key]& boundaries)
    cdef Array[Record] MergePartitions(const vector[ConstArray[Record]]& parts)


HeaderT = np.dtype((np.uint8, HEADER_SIZE))
PayloadT = np.dtype((np.uint8, RECORD_SIZE - HEADER_SIZE))
RecordT = np.dtype([("header", HeaderT), ("body", PayloadT)])
FlatRecordT = np.dtype((np.uint8, RECORD_SIZE))


def get_boundaries(n):
    return GetBoundaries(n)


def _to_numpy(mv):
    if mv is None:
        return np.empty(0, dtype=RecordT)
    return np.frombuffer(mv, dtype=RecordT)


cdef Record[:] _to_memoryview(const Array[Record]& arr):
    if arr.size == 0:
        return None
    return <Record[:arr.size]>arr.ptr


cdef Array[Record] _to_record_array(data):
    cdef Array[Record] ret
    cdef Record[:] memview = data
    ret.ptr = <Record*>&memview[0]
    ret.size = data.size
    return ret


cdef ConstArray[Record] _to_const_record_array(data):
    cdef ConstArray[Record] ret
    # FIXME: The next two lines of code is a temporary workaround due to a
    # known Cython bug [https://github.com/cython/cython/issues/2251]
    # preventing the creation of const memory views of cdef classes.
    # The semantically correct version should be:
    #     cdef const Record[:] memview = data
    cdef const uint8_t[:,:] memview = data.view(FlatRecordT)
    ret.ptr = <const Record*>&memview[0,0]
    ret.size = data.size
    return ret


def sort_and_partition(data, boundaries):
    arr = _to_record_array(data)
    chunks = SortAndPartition(arr, boundaries)
    return [data[c.offset : c.offset + c.size] for c in chunks]


def merge_partitions(parts):
    cdef vector[ConstArray[Record]] record_arrays
    record_arrays.reserve(len(parts))
    for data in parts:
        ra = _to_const_record_array(data)
        record_arrays.push_back(ra)
    cdef Array[Record] merged = MergePartitions(record_arrays)
    return _to_numpy(_to_memoryview(merged))
