#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <vector>

#include "sortlib.h"

using namespace sortlib;

void PrintRecord(const Record& rec) {
    for (size_t i = 0; i < HEADER_SIZE; ++i) {
        printf("%02x ", rec.header[i]);
    }
    printf("\n");
}

void AssertSorted(const Array<Record>& array) {
    for (size_t i = 0; i < array.size - 1; ++i) {
        const auto& a = array.ptr[i];
        const auto& b = array.ptr[i + 1];
        assert(std::memcmp(a.header, b.header, HEADER_SIZE) <= 0);
    }
}

std::vector<Array<Record>> MakeRecordArrays(
    Record* const records,
    const std::vector<Partition>& parts) {
    std::vector<Array<Record>> ret;
    ret.reserve(parts.size());
    for (const auto& part : parts) {
        ret.emplace_back(Array<Record>{records + part.offset, part.size});
    }
    return ret;
}

void test() {
    // Populate some records.
    const size_t num_records = 8;
    const uint8_t keys[num_records] = {8, 5, 1, 7, 6, 5, 7, 5};
    Record records[num_records];
    memset(records, 0, sizeof(Record) * num_records);
    for (size_t i = 0; i < num_records; ++i) {
        records[i].header[KEY_SIZE - 1] = keys[i];
        assert(records[i].key() == keys[i]);
    }
    const std::vector<Key> boundaries({0, 3, 5, 7});

    // Call and verify SortAndPartition().
    const auto& parts = SortAndPartition({records, num_records}, boundaries);

    AssertSorted({records, num_records});
    assert(parts == std::vector<Partition>({{{0, 1}, {1, 0}, {1, 4}, {5, 3}}}));

    const auto& record_arrays = MakeRecordArrays(records, parts);

    // Call and verify MergePartitions().
    const auto& merged_array = MergePartitions(record_arrays);
    AssertSorted(merged_array);
}

int main() {
    printf("Hello, world!\n");

    test();

    printf("A-OK!\n");

    const size_t num_reducers = 1;
    const auto& boundaries = GetBoundaries(num_reducers);

    const size_t num_records = 10000;
    const size_t buf_size = RECORD_SIZE * (num_records + 1);
    Record* records = new Record[num_records + 1];

    FILE* fin;
    size_t file_size = 0;
    fin = fopen("/var/tmp/raysort/input/input-000000", "r");
    if (fin == NULL) {
        perror("Failed to open file");
    } else {
        file_size = fread(records, RECORD_SIZE, num_records, fin);
        printf("Read %lu bytes.\n", file_size);
        fclose(fin);
    }

    const auto& parts = SortAndPartition({records, num_records}, boundaries);
    const auto& record_arrays = MakeRecordArrays(records, parts);
    const auto output = MergePartitions(record_arrays);

    FILE* fout;
    fout = fopen("/var/tmp/raysort/output/test-output", "w");
    if (fout == NULL) {
        perror("Failed to open file");
    } else {
        size_t writecount = fwrite(output.ptr, RECORD_SIZE, output.size, fout);
        printf("Wrote %lu bytes.\n", writecount);
        fclose(fout);
    }

    return 0;
}