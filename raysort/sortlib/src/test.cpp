#include <cassert>
#include <chrono>
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

std::vector<ConstArray<Record>> MakeConstRecordArrays(
    Record* const records,
    const std::vector<Partition>& parts) {
    std::vector<ConstArray<Record>> ret;
    ret.reserve(parts.size());
    for (const auto& part : parts) {
        ret.emplace_back(ConstArray<Record>{records + part.offset, part.size});
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

    const auto& record_arrays = MakeConstRecordArrays(records, parts);

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

    const size_t num_records = 1000000;
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

    const auto start1 = std::chrono::high_resolution_clock::now();
    const auto& parts = SortAndPartition({records, num_records}, boundaries);
    const auto stop1 = std::chrono::high_resolution_clock::now();
    const auto& record_arrays = MakeConstRecordArrays(records, parts);
    const auto start2 = std::chrono::high_resolution_clock::now();
    const auto output = MergePartitions(record_arrays);
    const auto stop2 = std::chrono::high_resolution_clock::now();

    FILE* fout;
    fout = fopen("/var/tmp/raysort/output/test-output", "w");
    if (fout == NULL) {
        perror("Failed to open file");
    } else {
        size_t writecount = fwrite(output.ptr, RECORD_SIZE, output.size, fout);
        printf("Wrote %lu bytes.\n", writecount);
        fclose(fout);
    }
    printf("Execution time (ms):\n");
    printf("SortAndPartition,%ld\n",
           std::chrono::duration_cast<std::chrono::milliseconds>(stop1 - start1)
               .count());
    printf("MergePartitions,%ld\n",
           std::chrono::duration_cast<std::chrono::milliseconds>(stop2 - start2)
               .count());
    printf("Total,%ld\n",
           std::chrono::duration_cast<std::chrono::milliseconds>(stop2 - start1)
               .count());

    return 0;
}