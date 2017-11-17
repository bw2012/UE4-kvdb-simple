// kvdb_test.cpp : Defines the entry point for the console application.
//

#include "stdafx.h"
#include "../../kvdb.h"
#include <cstring>

//-----------------------------------------------------------------------------

TKeyData intToBytes(const int paramInt) {
	byte temp[KVDB_KEY_SIZE];
	for (int i = 0; i < KVDB_KEY_SIZE; i++) {
		temp[i] = 0;
	}

	memcpy(&temp, &paramInt, sizeof(int));

	TKeyData arrayOfByte;
	for (int i = 0; i < KVDB_KEY_SIZE; i++) {
		arrayOfByte[i] = temp[i];
	}

	return arrayOfByte;
}

int intFromBytes(const TKeyData& arrayOfByte) {
	int temp = 0;
	memcpy(&temp, &arrayOfByte, sizeof(int));

	return temp;
}

std::vector<byte> intToVector(int test) {
	std::vector<byte> temp;

	if (temp.size() < sizeof(test)) temp.resize(sizeof(test));
	std::memcpy(temp.data(), &test, sizeof(test));

	return temp;
}

int intFromVector(TValueDataPtr dataPtr) {
	int temp = 0;
	memcpy(&temp, dataPtr->data(), sizeof(int));

	return temp;
}

typedef struct IntKey {
	int key;

	IntKey(int k) : key(k) {};

	TKeyData operator()() const {
		return intToBytes(key);
	};

} IntKey;



void printPair(KvFile<int, int>& db, int key) {
	TValueDataPtr dataPtr = db.get(key);
	if (dataPtr != nullptr) {
		printf("key -> %d --- val -> %d\n", key, intFromVector(dataPtr));
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

	/*
	testDb.put(intToBytes(3), intToVector(999));
	printPair(testDb, 3);

	testDb.put(intToBytes(111), intToVector(999));
	printPair(testDb, 111);

	testDb.put(intToBytes(222), intToVector(999));
	printPair(testDb, 222);

	testDb.put(intToBytes(333), intToVector(999));
	printPair(testDb, 333);
	*/

	//testDb.close();

    return 0;

}

