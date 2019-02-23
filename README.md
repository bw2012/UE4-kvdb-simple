# kvdb

Small and simple key-value binary storage. 

* key-value storage
* based on unordered_map and random access file
* one small file (~500 code lines)
* no external dependency
* all data stored in one file on disk
* all value stored in file and can be loaded by request
* no memory cache

# Warning

C++17 required

# Usage


```
#include "kvdb.h"

int main() {
  	// storage file name
	std::string file = "test.dat"; 

  	// create inital kye-value map with data (can be empty)
	std::unordered_map<int, int> test;
	test.insert({ 1, 1000 });
	test.insert({ 3, 2000 });
	test.insert({ 5, 3000 });

  	// save map on disk
	KvFile<int, int>::create(file, test);

  	// just create db
	KvFile<int, int> testDb;
  
  	// open db file (returns false if error)
	bool ret = testDb.open(file);

  	// get value by key
	std::shared_ptr<int> dataPtr = testDb[key];
	if (dataPtr != nullptr) {
		printf("key -> %d --- val -> %d\n", key, *dataPtr.get());
	} else {
		printf("key -> %d --- not found\n", key);
	}
	
  	// remove key-value pair
	testDb.erase(5);

  	// add/edit key-value paid to db file
	testDb.save(5, 777000);
	
  	// close db file
	testDb.close();
  
	return 0;
}
```
