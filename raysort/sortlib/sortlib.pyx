# distutils: language = c++
# distutils: sources = src/sortlib.cpp

from libc.stdint cimport uint8_t, uint64_t
from libcpp.vector cimport vector

import io
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
    cdef void MergePartitions(const vector[ConstArray[Record]]& parts, Record* const& ptr)


HeaderT = np.dtype((np.uint8, HEADER_SIZE))
PayloadT = np.dtype((np.uint8, RECORD_SIZE - HEADER_SIZE))
RecordT = np.dtype([("header", HeaderT), ("body", PayloadT)])


def get_boundaries(n):
    return GetBoundaries(n)


cdef Array[Record] _to_record_array(buf):
    cdef Array[Record] ret
    cdef uint8_t[:] mv = buf
    ret.ptr = <Record *>&mv[0]
    ret.size = int(buf.nbytes / RECORD_SIZE)
    return ret


cdef ConstArray[Record] _to_const_record_array(buf):
    cdef ConstArray[Record] ret
    cdef const uint8_t[:] mv = buf
    ret.ptr = <const Record*>&mv[0]
    ret.size = int(buf.nbytes / RECORD_SIZE)
    return ret


def sort_and_partition(part, boundaries):
    arr = _to_record_array(part.getbuffer())
    chunks = SortAndPartition(arr, boundaries)
    return [(c.offset, c.size) for c in chunks]


def merge_partitions(chunks):
    """
    Returns: io.BytesIO.
    """
    cdef vector[ConstArray[Record]] record_arrays
    record_arrays.reserve(len(chunks))
    total_records = 0
    for chunk in chunks:
        ra = _to_const_record_array(chunk.getbuffer())
        record_arrays.push_back(ra)
        total_records += ra.size
    
    total_bytes = total_records * RECORD_SIZE
    ret = io.BytesIO(b"0" * total_bytes)
    cdef uint8_t[:] mv = ret.getbuffer()
    ptr = <Record*>&mv[0]
    MergePartitions(record_arrays, ptr)
    ret.seek(0)
    return ret
