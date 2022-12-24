#include <iostream>

#include "VoxelIndex.h"
#include "../kvdb.hpp"
#include <cstdio>

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


void print_assert(bool b, std::string text) {
    if(b){ 
        printf("\033[3;42;30m PASS \033[0m ");
    } else {
		printf("\033[3;41;30m FAIL \033[0m ");    
    }
    
    printf("%s \n", text.c_str());    
    
    if(!b) exit(-1);
}

void test1() {
    printf("\033[3;104;30m Test#1 \033[0m\t\t \n");
    printf("Basic create, add, get operation...\n");

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
    kv_file1.save(key2, test2);
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

    printf("=========================== \n\n");
}

int main() {
    test1();
    printf("\n");
}
