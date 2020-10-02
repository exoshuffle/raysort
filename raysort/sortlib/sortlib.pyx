# distutils: language = c++
# distutils: sources = src/sortlib.cpp

from cpython.array cimport array
from libcpp.vector cimport vector

import numpy as np

ctypedef long ptr_t

cdef extern from "src/sortlib.h" namespace "sortlib":
    const size_t RECORD_SIZE
    ctypedef unsigned long long Header
    ctypedef struct Record:
        pass
    ctypedef struct RecordArray:
        Record* ptr
        size_t size
    cdef vector[Header] GetBoundaries(size_t num_partitions)
    cdef vector[RecordArray] PartitionAndSort(const RecordArray& record_array, const vector[Header]& boundaries)
    cdef RecordArray MergePartitions(const vector[RecordArray]& parts)


cdef _get_num_records(data):
    size = len(data)
    assert size % RECORD_SIZE == 0, (
        size,
        "input data size must be multiples of RECORD_SIZE",
    )
    num_records = int(size / RECORD_SIZE)
    return num_records

def get_boundaries(n):
    return GetBoundaries(n)

def _to_numpy(mv):
    arr = np.frombuffer(mv, dtype=np.uint8)
    return arr

cdef char[:] _to_memoryview(const RecordArray& ra):
    if ra.size == 0:
        return None
    n_bytes = ra.size * RECORD_SIZE
    return <char[:n_bytes]><char*>ra.ptr

cdef RecordArray _to_record_array(data):
    cdef RecordArray ret
    cdef char[:] memview = data
    ret.ptr = <Record*>&memview[0]
    ret.size = _get_num_records(data)
    return ret

def partition_and_sort(data, boundaries):
    cdef char[:] memview = data
    record_array = _to_record_array(data)
    chunks = PartitionAndSort(record_array, boundaries)
    return [_to_numpy(_to_memoryview(ra)) for ra in chunks]

def merge_partitions(parts):
    cdef vector[RecordArray] record_arrays
    record_arrays.reserve(len(parts))
    for data in parts:
        ra = _to_record_array(data)
        record_arrays.push_back(ra)
    cdef RecordArray merged = MergePartitions(record_arrays)
    return _to_numpy(_to_memoryview(merged))
