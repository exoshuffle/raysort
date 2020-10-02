# distutils: language = c++
# distutils: sources = src/sortlib.cpp

from cpython.array cimport array
from libcpp.vector cimport vector

import numpy as np


ctypedef long ptr_t

RECORD_SIZE = 100

cdef extern from "src/sortlib.h" namespace "sortlib":
    ctypedef unsigned long long Header
    ctypedef struct Record:
        pass
    ctypedef struct RecordArray:
        Record* ptr
        size_t size
    cdef vector[Header] GetBoundaries(size_t num_partitions)
    cdef vector[RecordArray] PartitionAndSort(const RecordArray& recordArray, const vector[Header]& boundaries)

def get_boundaries(n):
    return GetBoundaries(n)

def _to_numpy(mv):
    arr = np.frombuffer(mv, dtype="B")
    return arr

cdef char[:] _to_memoryview(const RecordArray& ra):
    if ra.size == 0:
        return None
    n_bytes = ra.size * RECORD_SIZE
    return <char[:n_bytes]><char*>ra.ptr

def partition_and_sort(data, num_records, boundaries):
    cdef char[:] memview = data
    cdef RecordArray record_array
    record_array.ptr = <Record*>&memview[0]
    record_array.size = num_records
    print("py first byte", data[0])
    print("calling cpp", len(data), num_records)
    chunks = PartitionAndSort(record_array, boundaries)
    print("done calling cpp")
    return [_to_numpy(_to_memoryview(ra)) for ra in chunks]
