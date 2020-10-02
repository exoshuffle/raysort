#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <vector>

#include "sortlib.h"

using namespace sortlib;

void print_record(const Record& rec) {
    for (int i = 0; i < KEY_SIZE; ++i) {
        printf("%x ", rec.key[i]);
    }
    printf("\n");
}

void assert_sorted(const RecordArray& record_array) {
    for (int i = 0; i < record_array.size - 1; ++i) {
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
    const int headers[num_records] = {8, 5, 1, 7, 6, 5, 7, 5};
    Record records[num_records];
    memset(records, 0, sizeof(Record) * num_records);
    for (int i = 0; i < num_records; ++i) {
        records[i].key[0] = headers[i];
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

    const auto& boundaries = GetBoundaries(4);
    for (const auto& b : boundaries) {
        printf("boundary %llu\n", b);
    }

    size_t num_records = 1000;
    size_t buf_size = RECORD_SIZE * (num_records + 1);
    void* buffer = malloc(buf_size);

    // FILE* fin;
    // size_t file_size = 0;
    // fin = fopen("../../../gensort/64/part1", "r");
    // if (fin == NULL) {
    //     perror("Failed to open file");
    // } else {
    //     file_size = fread(buffer, sizeof(char), buf_size, fin);
    //     printf("Read %lu bytes.\n", file_size);
    //     fclose(fin);
    // }

    // const auto& partition_ptrs =
    //     PartitionAndSort({(Record*)buffer, num_records}, boundaries);

    // verify_sort();

    // FILE* fout;
    // fout = fopen("../gensort/64/part1.sorted", "w");
    // if (fout == NULL) {
    //     perror("Failed to open file");
    // } else {
    //     size_t writecount = fwrite(buffer, sizeof(char), file_size, fout);
    //     printf("Wrote %lu bytes.\n", writecount);
    //     fclose(fout);
    // }

    return 0;
}