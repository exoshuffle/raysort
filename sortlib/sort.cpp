#include <algorithm>
#include <array>
#include <cstdio>
#include <cstring>

const size_t KEY_SIZE = 10;
const size_t RECORD_SIZE = 100;
const size_t N_RECORDS = 1000;
const size_t BUF_SIZE = RECORD_SIZE * (N_RECORDS + 1);

size_t file_size = 0;
char buffer[BUF_SIZE];

struct Record {
    char key[KEY_SIZE];
    char data[RECORD_SIZE - KEY_SIZE];
};

bool compare(const Record& a, const Record& b) {
    return std::memcmp(a.key, b.key, KEY_SIZE) < 0;
}

void do_sort() {
    Record* records = (Record*)buffer;
    std::sort(records, records + N_RECORDS, compare);
}

void print_record(const Record& a) {
    for (int i = 0; i < KEY_SIZE; ++i) {
        printf("%d ", a.key[i]);
    }
    printf("\n");
}

void verify_sort() {
    Record* records = (Record*)buffer;
    const Record* prev = NULL;
    for (int i = 0; i < N_RECORDS; ++i) {
        const Record* const rec = records + i;
        if (prev != NULL) {
            if (!compare(*prev, *rec)) {
                printf("BAD! %d\n", i);
                print_record(*prev);
                print_record(*rec);
            }
        }
        prev = rec;
    }
}

int main() {
    printf("Hello, world!\n");

    FILE* fin;
    fin = fopen("../gensort/64/part1", "r");
    if (fin == NULL) {
        perror("Failed to open file");
    } else {
        file_size = fread(buffer, sizeof(char), BUF_SIZE, fin);
        printf("Read %lu bytes.\n", file_size);
        fclose(fin);
    }

    do_sort();
    // verify_sort();

    FILE* fout;
    fout = fopen("../gensort/64/part1.sorted", "w");
    if (fout == NULL) {
        perror("Failed to open file");
    } else {
        size_t writecount = fwrite(buffer, sizeof(char), file_size, fout);
        printf("Wrote %lu bytes.\n", writecount);
        fclose(fout);
    }

    return 0;
}