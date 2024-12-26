#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <string>
#include <unordered_map>
#include <bit>

#include "../kvdb.hpp"
#include "VoxelIndex.h"

#define TEST_FILE1 "test1.dat"
#define TEST_FILE2 "test2.dat"

struct TTT {
    double T1 = 0;
    double T2 = 0;
    double T3 = 0;
    double T4 = 0;

    uint64_t L1 = 0;
    uint64_t L2 = 0;
    uint64_t L3 = 0;
    uint64_t L4 = 0;

    bool operator==(const TTT &other) const {
        return (T1 == other.T1 && T2 == other.T2 && T3 == other.T3 &&
                L1 == other.L1 && L2 == other.L2 && L3 == other.L3 && L4 == other.L4);
    }
};

struct TTestStructItem {
    TVoxelIndex key;
    TTT value;
    uint16 flags;
};

struct TTestDataItem {
    TVoxelIndex key;
    TValueData data;
    uint16 flags;
};

void print_test_name(std::string name, std::string description) {
    printf("\033[3;104;30m %s \033[0m   ", name.c_str());
    printf("%s\n\n", description.c_str());
}

void print_assert(bool b, std::string text) {
    if (b) {
        printf("\033[3;42;30m PASS \033[0m ");
    } else {
        printf("\033[3;41;30m FAIL \033[0m ");
    }

    printf("%s \n", text.c_str());

    if (!b)
        exit(-1);
}

//=====================================================================================

void test1(std::unordered_map<TVoxelIndex, TTestStructItem> &test_data_map) {
    print_test_name("Test#1", "Basic create, add, get operation...");

    std::string file_name = TEST_FILE1;
    std::remove(file_name.c_str());

    printf("Create new empty file\n");
    const std::unordered_map<TVoxelIndex, TTT> empty;
    kvdb::KvFile<TVoxelIndex, TTT>::create(file_name, empty);

    printf("Open file...\n");
    kvdb::KvFile<TVoxelIndex, TTT> kv_file1;
    bool is_exist = (kv_file1.open(file_name) == KVDB_OK);
    print_assert(is_exist, "Open file");

    int s = kv_file1.size();
    print_assert(s == 0, "Empty file size");

    printf("Add 2 values\n");

    TVoxelIndex key1(0, 1, 2);
    TVoxelIndex key2(0, -1, -1);
    TTT test1{1, 2, 3, 4};
    TTT test2{1, 2, 3, 4};

    kv_file1.save(key1, test1);
    kv_file1.save(key2, test2, 100);

    test_data_map.insert({key1, TTestStructItem{key1, test1, 0}});
    test_data_map.insert({key2, TTestStructItem{key2, test2, 100}});

    print_assert(s == 0, "Check file size");

    auto ptr = kv_file1.load(TVoxelIndex(0, 1, 2));
    print_assert(ptr != nullptr, "Get value#1 by key");

    TTT check = *ptr;
    print_assert(check == test1, "Check value#1");

    // kv_file1.forEachKey([](TVoxelIndex Index) {
    //     printf("%d %d %d\n", Index.X, Index.Y, Index.Z);
    // });

    auto f1 = kv_file1.k_flags(key1);
    print_assert(f1 == 0, "Check zero key#1 flag");

    print_assert(kv_file1.reserved() == 998, "Check reserved keys");
    print_assert(kv_file1.deleted() == 0, "Check deleted keys");

    printf("=========================== \n\n");
}

//=====================================================================================

void test2(std::unordered_map<TVoxelIndex, TTestStructItem> &test_data_map) {
    print_test_name("Test#2", "Open and read file...");

    std::string file_name = TEST_FILE1;

    kvdb::KvFile<TVoxelIndex, TTT> kv_file;
    bool is_exist = (kv_file.open(file_name) == KVDB_OK);
    print_assert(is_exist, "Open file");

    int s = kv_file.size();
    print_assert(s == 2, "File size");

    int i = 0;
    for (const auto &test_pair : test_data_map) {
        printf("Check pair: %d\n", i);

        const auto &ti = test_pair.second;

        const auto &key = ti.key;
        const auto &value = ti.value;
        const auto flags = ti.flags;

        auto ptr = kv_file.load(key);
        auto f = kv_file.k_flags(key);
        TTT c = *ptr;

        print_assert(ptr != nullptr, "Get value by key");
        print_assert(f == flags, "Check key flag");
        print_assert(c == value, "Check value");

        i++;
    }

    print_assert(kv_file.reserved() == 998, "Check reserved keys");
    print_assert(kv_file.deleted() == 0, "Check deleted keys");

    printf("=========================== \n\n");
}

//=====================================================================================

void test3(std::unordered_map<TVoxelIndex, TTestStructItem> &test_data_map) {
    print_test_name("Test#3", "Open and add...");

    std::string file_name = TEST_FILE1;

    kvdb::KvFile<TVoxelIndex, TTT> kv_file;
    bool is_exist = (kv_file.open(file_name) == KVDB_OK);

    print_assert(is_exist, "Open file");
    print_assert(kv_file.size() == 2, "File size");

    int n = 10;
    int i = 0;

    printf("Add %d pairs \n", n * n * n);
    for (int x = 0; x < n; x++) {
        for (int y = 0; y < n; y++) {
            for (int z = 0; z < n; z++) {
                uint16 f = i % 1000;
                TVoxelIndex index(x, y, z);
                TTT test{1.f / (float)x, 2 / (float)x, 3 / (float)x, 4 / (float)x};

                test_data_map[index] = TTestStructItem{index, test, f};

                kv_file.save(index, test, f);

                auto ptr = kv_file.load(index);
                auto ff = kv_file.k_flags(index);

                /*
                                if(ff != f){
                                    printf("ERROR %d\n", i);
                                    exit(-1);
                                }

                                if(ptr == nullptr){
                                    printf("ERROR %d\n", i);
                                    exit(-1);
                                } else {
                                    TTT c = *ptr;
                                    if(!(c == test)){
                                        printf("ERROR %d\n", i);
                                        exit(-1);
                                    } else {
                                        printf("key: %f %f %f \n", c.T1, c.T2, c.T3);
                                    }
                                }
                */
                i++;
            }
        }
    }

    print_assert(kv_file.size() == test_data_map.size(), "File size");
    print_assert(kv_file.reserved() == 999, "Check reserved keys");

    printf("=========================== \n\n");
}

//=====================================================================================

void checkWithMap(std::string file_name, const std::unordered_map<TVoxelIndex, TTestStructItem> &test_map) {
    kvdb::KvFile<TVoxelIndex, TTT> kv_file;
    bool is_exist = (kv_file.open(file_name) == KVDB_OK);

    print_assert(is_exist, "Open file");

    printf("Check %d pairs \n", (int)test_map.size());

    bool ok = true;
    int i = 0;
    for (const auto &test_pair : test_map) {
        const auto &ti = test_pair.second;

        const auto &key = ti.key;

        const auto &value = ti.value;
        const auto flags = ti.flags;
        auto ptr = kv_file.load(key);
        auto f = kv_file.k_flags(key);

        if (ptr == nullptr) {
            printf("fail: %d\n", i);
            printf("key: %d %d %d \n", key.X, key.Y, key.Z);
            printf("flag: %d %d\n", f, flags);
            ok = false;
            break;
        }

        TTT c = *ptr;

        ok = (ptr != nullptr) && (f == flags) && (c == value);
        if (!ok) {
            printf("fail: %d\n", i);
            printf("key: %d %d %d \n", key.X, key.Y, key.Z);
            printf("flag: %d %d\n", f, flags);
            break;
        }

        i++;
    }

    print_assert(ok, "Check values");
}

void test4(std::unordered_map<TVoxelIndex, TTestStructItem> &test_data_map) {
    print_test_name("Test#4", "Open and check...");

    // TVoxelIndex iii(9, 9, 9);
    // test_data_map.insert({iii, TTestStructItem{iii, TTT(), 0}});

    std::string file_name = TEST_FILE1;
    checkWithMap(file_name, test_data_map);

    printf("=========================== \n\n");
}

//=====================================================================================
// test null values
//=====================================================================================

bool createTestFile(std::string file_name, std::unordered_map<TVoxelIndex, TTestDataItem> &test_set, size_t &created_elements, int size) {
    std::remove(file_name.c_str());

    printf("Create new empty file\n");
    const std::unordered_map<TVoxelIndex, TValueData> empty;
    kvdb::KvFile<TVoxelIndex, TValueData>::create(file_name, empty);

    kvdb::KvFile<TVoxelIndex, TValueData> kv_file;
    bool is_exist = (kv_file.open(file_name) == KVDB_OK);
    print_assert(is_exist, "File created");

    TValueData zero_data;
    zero_data.resize(0);

    size_t n = 0;
    for (int x = 0; x < size; x++) {
        for (int y = 0; y < size; y++) {
            for (int z = 0; z < size; z++) {
                TVoxelIndex index(x, y, z);

                int flag = n % 65536;

                if (n % 200 == 0) {
                    double p = (double)n / (double)(size * size * size);
                    printf("\rFill test file: %.2f%%", p * 100);
                }

                test_set[index] = TTestDataItem{index, zero_data, (uint16)flag};
                kv_file.save(index, zero_data, (uint16)flag);

                const auto ff = kv_file.k_flags(index);

                if (ff != flag) {
                    return false;
                }

                n++;
            }
        }
    }

    printf("\nTest file filled\n");

    print_assert(kv_file.size() == n, "File size");

    created_elements = n;
    kv_file.close();

    return true;
}

void make_test_data(TValueData &data, int size) {

    data.resize(size);

    for (int i = 0; i < size; i++) {
        auto v = rand() % 256;
        data.push_back(v);
    }
}

bool check_values(const kvdb::KvFile<TVoxelIndex, TValueData> &kv_file, const std::unordered_map<TVoxelIndex, TTestDataItem> &test_data_map) {

    int i = 0;

    for (const auto &a : test_data_map) {
        const auto &ti = a.second;

        const auto ptr = kv_file.load(ti.key);
        const auto f = kv_file.k_flags(ti.key);

        bool ok = f == ti.flags;
        if (!ok) {
            printf("fail: %d\n", i);
            printf("key: %d %d %d \n", ti.key.X, ti.key.Y, ti.key.Z);
            printf("flag: %d %d\n", f, ti.flags);
            return false;
        }

        // TODO asset values

        i++;
    }

    return true;
}

void test_null_val1(std::unordered_map<TVoxelIndex, TTestDataItem> test_data_map) {
    print_test_name("Test#5", "Zero length values...");

    std::string file_name = TEST_FILE2;
    size_t n;

    createTestFile(file_name, test_data_map, n, 3);

    kvdb::KvFile<TVoxelIndex, TValueData> kv_file;
    bool is_exist = (kv_file.open(file_name) == KVDB_OK);
    print_assert(is_exist, "Open file");

    printf("File size: %d \n", (int)kv_file.size());

    print_assert(kv_file.size() == n, "File size");
    print_assert(kv_file.reserved() == (size_t)(KVDB_RESERVED_TABLE_SIZE - n), "Reserved");

    print_assert(check_values(kv_file, test_data_map), "Check values");

    // add new keys
    printf("Add new kev/value pairs with flag\n");

    TVoxelIndex key1(10, 8, -9);
    TValueData data1;
    make_test_data(data1, 100);

    kv_file.save(key1, data1, 10);
    test_data_map.insert({key1, TTestDataItem{key1, data1, (uint16)10}});

    TVoxelIndex key2(0, -4, 11);
    TValueData data2;
    make_test_data(data1, 300);

    kv_file.save(key2, data2, 255);
    test_data_map.insert({key2, TTestDataItem{key2, data2, (uint16)255}});

    print_assert(kv_file.reserved() == (size_t)(KVDB_RESERVED_TABLE_SIZE - n - 2), "Reserved");
    print_assert(check_values(kv_file, test_data_map), "Check values");

    printf("=========================== \n\n");
}

void test_big1(std::unordered_map<TVoxelIndex, TTestDataItem> test_data_map) {
    print_test_name("Test#6", "Big data test...");

    std::string file_name = TEST_FILE2;

    const int s = 50;

    size_t n;
    print_assert(createTestFile(file_name, test_data_map, n, s), "Create new file");

    kvdb::KvFile<TVoxelIndex, TValueData> kv_file;
    bool is_exist = (kv_file.open(file_name) == KVDB_OK);
    print_assert(is_exist, "Open file");

    printf("File size: %d \n", (int)kv_file.size());

    print_assert(kv_file.size() == n, "File size");

    print_assert(check_values(kv_file, test_data_map), "Check values");

    // add new keys
    printf("Add new kev/value pairs with flag\n");

    const int s2 = s + 2;

    bool ok = true;

    size_t n2 = 0;
    for (int x = 0; x < s2; x += 2) {
        for (int y = 0; y < s2; y += 2) {
            for (int z = 0; z < s2; z += 2) {
                TVoxelIndex index(x, y, z);

                int flag = n2 % 65536;

                if (n2 % 100 == 0) {
                    double p = (double)n2 / (double)(s2 * s2 * s2);
                    printf("\rRefill test file: %.2f%%", p * 100);
                }

                TValueData data;
                make_test_data(data, 100);

                test_data_map[index] = TTestDataItem{index, data, (uint16)flag};
                kv_file.save(index, data, (uint16)flag);

                const auto ff = kv_file.k_flags(index);

                if (flag != ff) {
                    ok = false;
                    break;
                }

                n2++;
            }

            if (!ok) {
                break;
            }
        }

        if (!ok) {
            break;
        }
    }

    printf("\n");

    print_assert(ok, "Change data");

    printf("Refilled\n");

    /*
    TVoxelIndex key1(10, 8, -9);
    TValueData data1;
    make_test_data(data1, 100);

    kv_file.save(key1, data1, 10);
    test_data_map.insert({key1, TTestDataItem{key1, data1, (uint16)10}});

    TVoxelIndex key2(0, -4, 11);
    TValueData data2;
    make_test_data(data1, 300);

    kv_file.save(key2, data2, 255);
    test_data_map.insert({key2, TTestDataItem{key2, data2, (uint16)255}});
    */

    print_assert(check_values(kv_file, test_data_map), "Check values");

    printf("=========================== \n\n");
}

//=====================================================================================

int main() {

    if constexpr (std::endian::native == std::endian::big)
        std::cout << "big-endian\n";
    else if constexpr (std::endian::native == std::endian::little)
        std::cout << "little-endian\n";
    else
        std::cout << "mixed-endian\n";

    printf("\nRun KVDB tests... \n\n");

    // basic functionality
    std::unordered_map<TVoxelIndex, TTestStructItem> test_data_map;
    test1(test_data_map);
    test2(test_data_map);
    test3(test_data_map);
    test4(test_data_map);

    // keys with zero length values
    std::unordered_map<TVoxelIndex, TTestDataItem> test_data_map2;
    test_null_val1(test_data_map2);

    std::unordered_map<TVoxelIndex, TTestDataItem> test_data_map3;
    test_big1(test_data_map3);

    printf("\n");
}
