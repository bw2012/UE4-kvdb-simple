// kvdb_test.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"
#include "../../kvdb.h"
#include <cstring>

//-----------------------------------------------------------------------------

int intFromVector(TValueDataPtr dataPtr) {
	int temp = 0;
	memcpy(&temp, dataPtr->data(), sizeof(int));

	return temp;
}

void printPair(KvFile<int, int>& db, int key) {
	std::shared_ptr<int> dataPtr = db.getVal(key);
	if (dataPtr != nullptr) {
		printf("key -> %d --- val -> %d\n", key, *dataPtr.get());
	} else {
		printf("key -> %d --- not found\n", key);
	}
}

int main() {

	printf("TEST\n\n");

	//TKeyData ttt = IntKey(1)();

	std::string file = "test.dat";

	std::unordered_map<int, int> test;
	test.insert({ 1, 1000 });
	test.insert({ 3, 2000 });
	test.insert({ 5, 3000 });

	KvFile<int, int>::create(file, test);

	KvFile<int, int> testDb;
	testDb.open(file);


	printPair(testDb, 5);

	testDb.erase(5);
	
	printPair(testDb, 5);

	testDb.put(5, 777000);

	printPair(testDb, 5);



	testDb.put(3, 999);
	printPair(testDb, 3);

	testDb.put(111, 999);
	printPair(testDb, 111);

	testDb.put(222, 999);
	printPair(testDb, 222);

	testDb.put(333, 999);
	printPair(testDb, 333);
	

	testDb.close();

    return 0;

}

