#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <vector>

#include "sortlib.h"

using namespace sortlib;

void print_record(const Record& rec) {
    for (size_t i = 0; i < KEY_SIZE; ++i) {
        printf("%02x ", rec.key[i]);
    }
    printf("\n");
}

void assert_sorted(const RecordArray& record_array) {
    for (size_t i = 0; i < record_array.size - 1; ++i) {
        const auto& a = record_array.ptr[i];
        const auto& b = record_array.ptr[i + 1];
        assert(std::memcmp(a.key, b.key, KEY_SIZE) <= 0);
    }
}

size_t total_size(const std::vector<RecordArray>& parts) {
    size_t ret = 0;
    for (const auto& part : parts) {
        ret += part.size;
    }
    return ret;
}

void test() {
    // Populate some records.
    const size_t num_records = 8;
    const uint8_t headers[num_records] = {8, 5, 1, 7, 6, 5, 7, 5};
    Record records[num_records];
    memset(records, 0, sizeof(Record) * num_records);
    for (size_t i = 0; i < num_records; ++i) {
        records[i].key[HEADER_SIZE - 1] = headers[i];
        assert(records[i].header() == headers[i]);
    }
    const std::vector<Header> boundaries({0, 3, 5, 7});

    // Call and verify PartitionAndSort().
    const auto& parts = PartitionAndSort({records, num_records}, boundaries);

    assert_sorted({records, num_records});
    assert(parts == std::vector<RecordArray>({{{records, 1},
                                               {records + 1, 0},
                                               {records + 1, 4},
                                               {records + 5, 3}}}));

    // Call and verify MergePartitions().
    const auto& merged_array = MergePartitions(parts);
    assert_sorted(merged_array);
}

int main() {
    printf("Hello, world!\n");

    test();

    const size_t num_reducers = 1;
    const auto& boundaries = GetBoundaries(num_reducers);

    const size_t num_records = 10000;
    const size_t buf_size = RECORD_SIZE * (num_records + 1);
    void* buffer = malloc(buf_size);

    FILE* fin;
    size_t file_size = 0;
    fin = fopen("/var/tmp/raysort/input/input-0", "r");
    if (fin == NULL) {
        perror("Failed to open file");
    } else {
        file_size = fread(buffer, RECORD_SIZE, num_records, fin);
        printf("Read %lu bytes.\n", file_size);
        fclose(fin);
    }

    const auto& record_arrays =
        PartitionAndSort({(Record*)buffer, num_records}, boundaries);
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