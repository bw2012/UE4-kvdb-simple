#include <iostream>

#include "VoxelIndex.h"
#include "../kvdb.hpp"
#include <cstdio>
#include <unordered_map>
#include <string>

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
                L1 == other.L1 && L2 == other.L2 && L3 == other.L3 && L4 == other.L4 );
    }
};

struct TTestItem {
    TVoxelIndex key;
    TTT value;
    uint16 flags;
};

std::unordered_map<TVoxelIndex, TTestItem> global_test_map;

void print_test_name(std::string name, std::string description) {
    printf("\033[3;104;30m %s \033[0m\t\t \n", name.c_str());
    printf("%s\n", description.c_str());
}


void print_assert(bool b, std::string text) {
    if(b){ 
        printf("\033[3;42;30m PASS \033[0m ");
    } else {
        printf("\033[3;41;30m FAIL \033[0m ");    
    }
    
    printf("%s \n", text.c_str());    
    
    if(!b) exit(-1);
}

//=====================================================================================

void test1() {
    print_test_name("Test#1", "Basic create, add, get operation...");

    std::string file_name = "test1.dat";
    std::remove(file_name.c_str());

    printf("Create new empty file\n");
    const std::unordered_map<TVoxelIndex, TTT> empty;
    kvdb::KvFile<TVoxelIndex, TTT>::create(file_name, empty);

    kvdb::KvFile<TVoxelIndex, TTT> kv_file1;
    bool is_exist = kv_file1.open(file_name);
    print_assert(is_exist, "Open file");
    
    int s = kv_file1.size();
    print_assert(s == 0, "Empty file size");

    printf("Add 2 values\n");
        
    TVoxelIndex key1(0, 1, 2);
    TVoxelIndex key2(0, -1, -1);
    TTT test1{ 1, 2, 3, 4 };
    TTT test2{ 1, 2, 3, 4 };
      
    kv_file1.save(key1, test1);
    kv_file1.save(key2, test2, 100);
    
    global_test_map.insert({key1, TTestItem{ key1, test1, 0}});
    global_test_map.insert({key2, TTestItem{ key2, test2, 100}});
        
    print_assert(s == 0, "Check file size");

    auto ptr = kv_file1.load(TVoxelIndex(0, 1, 2));
    print_assert(ptr != nullptr, "Get value#1 by key");
    
    TTT check = *ptr;
    print_assert(check == test1, "Check value#1");

    //kv_file1.forEachKey([](TVoxelIndex Index) {
    //    printf("%d %d %d\n", Index.X, Index.Y, Index.Z);
    //});

    auto f1 = kv_file1.k_flags(key1);
    print_assert(f1 == 0, "Check zero key#1 flag");
    
    print_assert(kv_file1.reserved() == 998, "Check reserved keys");
    print_assert(kv_file1.deleted() == 0, "Check deleted keys");
    
    printf("=========================== \n\n");
}

//=====================================================================================

void test2() {
    print_test_name("Test#2", "Open and read file...");

    std::string file_name = "test1.dat";

    kvdb::KvFile<TVoxelIndex, TTT> kv_file;
    bool is_exist = kv_file.open(file_name);
    print_assert(is_exist, "Open file");
    
    int s = kv_file.size();
    print_assert(s == 2, "File size");
    
    int i = 0;
    for(const auto& test_pair : global_test_map){
        printf("Check pair: %d\n", i);
        
        const auto&ti = test_pair.second;
        
        const auto& key = ti.key;
        const auto& value = ti.value;
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

void test3() {
    print_test_name("Test#3", "Open and add...");

    std::string file_name = "test1.dat";

    kvdb::KvFile<TVoxelIndex, TTT> kv_file;
    bool is_exist = kv_file.open(file_name);
    
    print_assert(is_exist, "Open file");
    print_assert(kv_file.size() == 2, "File size");
    
    int n = 10;
    int i = 0;
    
    printf("Add %d pairs \n", n * n * n);
    for(int x = 0; x < n; x++){
        for(int y = 0; y < n; y++){
            for(int z = 0; z < n; z++){
                uint16 f = i % 1000;
                TVoxelIndex index(x, y, z);
                TTT test{ 1.f / (float)x, 2 / (float)x, 3 / (float)x, 4 / (float)x };
                
                global_test_map[index] = TTestItem{index, test, f};
                
                kv_file.save(index, test, f);
                i++;  
            }
        }
    }
    
    print_assert(kv_file.size() == global_test_map.size(), "File size");
    print_assert(kv_file.reserved() == 999, "Check reserved keys");
    
    printf("=========================== \n\n");
}

//=====================================================================================

void checkWithMap(std::string file_name, const std::unordered_map<TVoxelIndex, TTestItem>& test_map){
    kvdb::KvFile<TVoxelIndex, TTT> kv_file;
    bool is_exist = kv_file.open(file_name);
   
    print_assert(is_exist, "Open file");

    printf("Check %d pairs \n", (int)test_map.size());

    bool ok = true;
    int i = 0;
    for(const auto& test_pair : test_map){       
        const auto&ti = test_pair.second;
        
        const auto& key = ti.key;
        const auto& value = ti.value;
        const auto flags = ti.flags;
        
        auto ptr = kv_file.load(key);
        auto f = kv_file.k_flags(key);
        TTT c = *ptr;
               
        ok = (ptr != nullptr) && (f == flags) && (c == value);
 
        if(!ok){
            printf("fail: %d\n", i);
            printf("key: %d %d %d \n", key.X, key.Y, key.Z);
            printf("flag: %d %d\n", f, flags);            
            break;
        }
                        
        i++;
    }
    
    print_assert(ok, "Check values");
}

void test4() {
    print_test_name("Test#3", "Open and check...");
    
    std::string file_name = "test1.dat";
    checkWithMap(file_name, global_test_map);
    
    printf("=========================== \n\n");
}

//=====================================================================================

void createTestFile(std::string file_name, std::unordered_map<TVoxelIndex, TTT>& test_set, int& created_elements){
    std::remove(file_name.c_str());

    printf("Create new empty file\n");
    const std::unordered_map<TVoxelIndex, TTT> empty;
    kvdb::KvFile<TVoxelIndex, TTT>::create(file_name, empty);

    kvdb::KvFile<TVoxelIndex, TTT> kv_file;
    bool is_exist = kv_file.open(file_name);
    //print_assert(is_exist, "Open file");

    int n = 10;
    for(int x = 0; x < n; x++){
        TVoxelIndex index(x, 0, 0);
        TTT test{ 1.f / (float)x, 2 / (float)x, 3 / (float)x, 4 / (float)x };
        test_set.insert({index, test});
        kv_file.save(index, test);
    }
    
    created_elements = n;
    
    kv_file.close();
}

 void test_erase() {
    print_test_name("Test#", "Erase elements...");

    std::string file_name = "test2.dat";
    std::unordered_map<TVoxelIndex, TTT> test_set;
    int n;

    createTestFile(file_name, test_set, n);

    kvdb::KvFile<TVoxelIndex, TTT> kv_file;
    bool is_exist = kv_file.open(file_name);
    print_assert(is_exist, "Open file");
    
    print_assert(kv_file.size() == n, "File size");

    printf("=========================== \n\n");
}

//=====================================================================================

int main() {
    test1();
    test2();
    test3();
    test4();
    
    printf("\n");
}
