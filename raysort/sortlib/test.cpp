#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <vector>

#include "sortlib.h"

void print_record(const Record& rec) {
    for (int i = 0; i < KEY_SIZE; ++i) {
        printf("%x ", rec.key[i]);
    }
    printf("\n");
}

void assert_sorted(Record* records, const size_t num_records) {
    for (int i = 0; i < num_records - 1; ++i) {
        assert(RecordComparator()(records[i], records[i + 1]));
    }
}

void assert_sorted(std::vector<Record> records) {
    for (int i = 0; i < records.size() - 1; ++i) {
        assert(RecordComparator()(records[i], records[i + 1]));
    }
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
    const std::vector<Header> boundaries({1, 3, 5, 7});
    std::vector<Record*> parts;

    // Call and verify partition_and_sort().
    partition_and_sort(records, num_records, boundaries, parts);

    assert_sorted(records, num_records);
    for (int i = 0; i < num_records - 1; ++i) {
        assert(records[i].key < records[i + 1].key);
    }
    assert(parts == std::vector<Record*>(
                        {{records, records + 1, records + 1, records + 5}}));

    // Create partition size vectors
    std::vector<size_t> part_sizes;
    part_sizes.reserve(parts.size());
    for (int i = 0; i < parts.size() - 1; ++i) {
        part_sizes.emplace_back(parts[i + 1] - parts[i]);
    }
    part_sizes.emplace_back(records + num_records - parts.back());
    assert(part_sizes == std::vector<size_t>({{1, 0, 4, 3}}));

    // Call and verify merge_partitions().
    std::vector<Record> merged = merge_partitions(parts, part_sizes);
    assert(merged.size() == num_records);
    assert_sorted(merged);
}

int main() {
    printf("Hello, world!\n");

    test();

    const auto& boundaries = get_boundaries(4);
    for (const auto& b : boundaries) {
        printf("boundary %llu\n", b);
    }

    size_t num_records = 1000;
    size_t buf_size = RECORD_SIZE * (num_records + 1);
    void* buffer = malloc(buf_size);

    FILE* fin;
    size_t file_size = 0;
    fin = fopen("../../gensort/64/part1", "r");
    if (fin == NULL) {
        perror("Failed to open file");
    } else {
        file_size = fread(buffer, sizeof(char), buf_size, fin);
        printf("Read %lu bytes.\n", file_size);
        fclose(fin);
    }

    std::vector<Record*> partition_ptrs;
    partition_and_sort((Record*)buffer, num_records, boundaries,
                       partition_ptrs);

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