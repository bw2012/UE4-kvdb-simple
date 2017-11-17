
#include <iostream>
#include <stdio.h>
#include <fstream>
#include <stdint.h>
#include <unordered_map>
#include <array>
#include <vector>
#include <memory>
#include <string>
#include <list>
#include <set>
#include <mutex>
#include <cassert>


#define KVDB_KEY_SIZE 64
#define KVDB_RESERVED_TABLE_SIZE 5

typedef uint32_t uint32;
typedef unsigned long long ulong64;
typedef unsigned char byte;

typedef std::array<byte, KVDB_KEY_SIZE> TKeyData;
typedef std::vector<byte> TValueData;
typedef std::shared_ptr<TValueData> TValueDataPtr;

//============================================================================
// Base IO
//============================================================================
template <typename T>
void write(std::ostream& os, const T& obj) {
    os.write((char*)&obj, sizeof(obj));
}

template <typename T>
void read(std::istream& is, const T& obj) {
    is.read((char*)&obj, sizeof(obj));
}

template <typename T>
void write(std::ostream* os, const T& obj) {
    if(os != nullptr) {
        os->write((char*)&obj, sizeof(obj));
    }
}

template <typename T>
void read(std::istream* is, const T& obj) {
    if(is != nullptr) {
        is->read((char*)&obj, sizeof(obj));
    }
}

//============================================================================
// File header
//============================================================================

typedef struct TFileHeader {
    uint32  version = 1;
    
    ulong64 timestamp = 0;
    
    uint32  endOfHeaderOffset = 0;
    
} TFileHeader;

std::ostream& operator << (std::ostream& os, const TFileHeader& obj) {
    write(os, obj.version);
    write(os, obj.timestamp);
    write(os, obj.endOfHeaderOffset);
    return os;
}

std::istream& operator >> (std::istream& is, TFileHeader& obj) {
    read(is, obj.version);
    read(is, obj.timestamp);
    read(is, obj.endOfHeaderOffset);
    return is;
}

std::ostream* operator << (std::ostream* os, const TFileHeader& obj) {
    write(os, obj.version);
    write(os, obj.timestamp);
    write(os, obj.endOfHeaderOffset);
    return os;
}

std::istream* operator >> (std::istream* is, TFileHeader& obj) {
    read(is, obj.version);
    read(is, obj.timestamp);
    read(is, obj.endOfHeaderOffset);
    return is;
}

//============================================================================
// Table header
//============================================================================

typedef struct TTableHeader {

    ulong64 recordCount = 0;
    
    ulong64 nextTable = 0;
    
    //TTableHeader() {};
    
    //TTableHeader(TTableHeader& t) : recordCount(t.recordCount), nextTable(t.nextTable) {};
    
} TTableHeader;

typedef struct TTableHeaderInfo {
    TTableHeader table;
    
    //TTableHeaderInfo() {};
    
    //TTableHeaderInfo(TTableHeader t) : table(t) {};
    
    long entryPos = 0;
    
} TTableHeaderInfo;

std::ostream& operator << (std::ostream& os, const TTableHeader& obj) {
    write(os, obj.recordCount);
    write(os, obj.nextTable);
    return os;
}

std::istream& operator >> (std::istream& is, TTableHeader& obj) {
    read(is, obj.recordCount);
    read(is, obj.nextTable);
    return is;
}

std::ostream* operator << (std::ostream* os, const TTableHeader& obj) {
    write(os, obj.recordCount);
    write(os, obj.nextTable);
    return os;
}

std::istream* operator >> (std::istream* is, TTableHeader& obj) {
    read(is, obj.recordCount);
    read(is, obj.nextTable);
    return is;
}

//============================================================================
// Key Entry
//============================================================================

typedef struct TKeyEntry {
    ulong64 dataPos = 0;
    ulong64 dataLength = 0;
    ulong64 initialDataLength = 0;
    
    TKeyData freeKeyData;
    
    TKeyEntry(){
        for(int i = 0; i < KVDB_KEY_SIZE; i++){
            freeKeyData[i] = 0;
        }
    }
        
} TKeyEntry;

typedef struct TKeyEntryInfo {
    TKeyEntry key;
    
    long entryPos = 0;
    
} TKeyEntryInfo;


struct TKeyInfoComparatorByInitialLength {
    bool operator() (const TKeyEntryInfo &lhs, const TKeyEntryInfo &rhs) const {
         return (lhs.key.initialDataLength > rhs.key.initialDataLength);
    }
};

std::ostream& operator << (std::ostream& os, const TKeyEntry& obj) {
    write(os, obj.dataPos);
    write(os, obj.dataLength);
    write(os, obj.initialDataLength);
    write(os, obj.freeKeyData);
    return os;
}

std::istream& operator >> (std::istream& is, TKeyEntry& obj) {
    read(is, obj.dataPos);
    read(is, obj.dataLength);
    read(is, obj.initialDataLength);
    read(is, obj.freeKeyData);
    return is;
}

std::ostream* operator << (std::ostream* os, const TKeyEntry& obj) {
    write(os, obj.dataPos);
    write(os, obj.dataLength);
    write(os, obj.initialDataLength);
    write(os, obj.freeKeyData);
    return os;
}

std::istream* operator >> (std::istream* is, TKeyEntry& obj) {
    read(is, obj.dataPos);
    read(is, obj.dataLength);
    read(is, obj.initialDataLength);
    read(is, obj.freeKeyData);
    return is;
}

//============================================================================
// 
//============================================================================

namespace std {
    template <>
    struct hash<TKeyData> {
        std::size_t operator()(const TKeyData& keyData) const {
            std::size_t h = 0;
            for (auto elem : keyData) {
                h ^= std::hash<int>{}(elem)  + 0x9e3779b9 + (h << 6) + (h >> 2); 
            }
        return h;
        }
    };
}

//============================================================================
// File db
//============================================================================

template <typename K, typename V>
class KvFile {
    
private:
    
    std::unordered_map<TKeyData, TKeyEntryInfo> dataMap;
    
    std::fstream* filePtr = nullptr;
    
    std::list<TKeyEntryInfo> reservedKeyList;
    
    std::set<TKeyEntryInfo, TKeyInfoComparatorByInitialLength> deletedKeyList;
    
    std::list<TTableHeaderInfo> tableList;

	std::mutex fileMutex;
    
private:

	static TKeyData toKeyData(K key) {
		byte temp[KVDB_KEY_SIZE];
		for (int i = 0; i < KVDB_KEY_SIZE; i++) {
			temp[i] = 0;
		}

		memcpy(&temp, &key, sizeof(K));

		TKeyData arrayOfByte;
		for (int i = 0; i < KVDB_KEY_SIZE; i++) {
			arrayOfByte[i] = temp[i];
		}

		return arrayOfByte;
	}

	static TValueData toValueData(V value) {
		std::vector<byte> temp;

		if (temp.size() < sizeof(value)) temp.resize(sizeof(value));
		std::memcpy(temp.data(), &value, sizeof(value));

		return temp;
	}

    void rewritePair(TKeyEntryInfo& keyInfo, const TValueData& valueData){                
        // rewrite value data
        filePtr->seekg(keyInfo.key.dataPos);
        filePtr->write((char*)valueData.data(), valueData.size());
                
        // rewrite key data
        keyInfo.key.dataLength = valueData.size(); // new length
        filePtr->seekg(keyInfo.entryPos);
        filePtr << keyInfo.key;
    }
    
    void earsePair(TKeyEntryInfo& keyInfo){                                
        // rewrite key data
        keyInfo.key.dataLength = 0; // new length
        filePtr->seekg(keyInfo.entryPos);
        filePtr << keyInfo.key;
        
        deletedKeyList.insert(keyInfo);
        dataMap.erase(keyInfo.key.freeKeyData);
    }
    
    void newPairFromReserved(const TKeyData& keyData, const TValueData& valueData){                
        // has reserved key slots
        TKeyEntryInfo& keyInfo = reservedKeyList.front();
        reservedKeyList.pop_front();
                
        // rewrite value data
        filePtr->seekg(0, std::ios::end); // to end-of-file
        int endFile =  (int)(filePtr->tellp());
        filePtr->write((char*)valueData.data(), valueData.size());
                
        // fill key data
        keyInfo.key.dataLength = valueData.size(); // length
        keyInfo.key.initialDataLength = valueData.size(); // length
        keyInfo.key.dataPos = endFile;
        keyInfo.key.freeKeyData = keyData;
                
        filePtr->seekg(keyInfo.entryPos);
        filePtr << keyInfo.key;
                
        // add new pair to table 
        dataMap.insert({keyInfo.key.freeKeyData, keyInfo});   
    }
    
    ulong64 readTable(){
        int tablePos = (int)filePtr->tellp();
        
        TTableHeader tableHeader;
        filePtr >> tableHeader;
        
        printf("read table -> %d -> %d\n", tablePos, tableHeader.nextTable);
    
        for(int i = 0; i < tableHeader.recordCount; i++){
            int pos = filePtr->tellp();
            
            TKeyEntry keyEntry;
            filePtr >> keyEntry;
            
            TKeyEntryInfo keyInfo;
            keyInfo.key = keyEntry;
            keyInfo.entryPos = pos;
            
            if(keyInfo.key.dataLength > 0){
                printf("key -> %d\n", keyInfo.entryPos);
                dataMap.insert({keyEntry.freeKeyData, keyInfo});                
            } else {
                if(keyInfo.key.initialDataLength == 0 ){
                    // reserved key slot
                    printf("reserved key slot -> %d\n", keyInfo.entryPos);
                    reservedKeyList.push_back(keyInfo);
                } else {
                    // marked as deleted pair
                    printf("deleted key -> %d\n", keyInfo.entryPos);
                    deletedKeyList.insert(keyInfo);
                }
            }
        }
        
        
        TTableHeaderInfo tableInfo;
        tableInfo.table = tableHeader;
        tableInfo.entryPos = tablePos;
        
        tableList.push_back(tableInfo);
        
        printf("next table -> %d\n", tableHeader.nextTable);
        
        return tableHeader.nextTable;
    }
    
    void createNewTable(){
        filePtr->seekg(0, std::ios::end); // to end-of-file
        int newTablePos = (int)filePtr->tellp();
        
        // write new table
        TTableHeader newTable;
        newTable.recordCount = KVDB_RESERVED_TABLE_SIZE;
        newTable.nextTable = 0;
        filePtr << newTable;
        
        // write reserved keys
        for(int i = 0; i < KVDB_RESERVED_TABLE_SIZE; i++){
            int newReservedKeyPos = (int)filePtr->tellp();
            
            TKeyEntry newReservedKey;
            filePtr << newReservedKey;
            
            TKeyEntryInfo keyInfo;
            keyInfo.key = newReservedKey;
            keyInfo.entryPos = newReservedKeyPos;
            
            reservedKeyList.push_back(keyInfo);
        }
        
        // read previous last table 
        TTableHeaderInfo& lastTable = tableList.back();        
        filePtr->seekg(lastTable.entryPos);
        
        // add link to new table
        lastTable.table.nextTable = newTablePos;

        // rewrite previous last table
        filePtr << lastTable.table;
        
        // add new table to internal list
        TTableHeaderInfo newTableInfo;
        newTableInfo.table = newTable;
        newTableInfo.entryPos = newTablePos;
        tableList.push_back(newTableInfo);
    }
    
    bool hasReserved(){
        return reservedKeyList.size();
    }
    
    bool tryWriteToSuitableDeletedPair(const TKeyData& keyData, const TValueData& valueData){
        for (auto itr = deletedKeyList.begin(); itr != deletedKeyList.end();) {
            if (itr->key.initialDataLength >= valueData.size()){
                TKeyEntryInfo keyInfo = *itr;
                rewritePair(keyInfo, valueData);
                dataMap.insert({keyInfo.key.freeKeyData, keyInfo});
                itr = deletedKeyList.erase(itr); 
                
                return true;
            } else {
                ++itr;
            }
        }
        
        return false;
    }
    
    void addNew(const TKeyData& keyData, const TValueData& valueData){
        if(valueData.size() == 0){
            return;
        }

        if(!tryWriteToSuitableDeletedPair(keyData, valueData)){    
            if(hasReserved()){
                printf("new pair from reserved\n");
                newPairFromReserved(keyData, valueData);
            } else {
                printf("create new table\n");
                createNewTable();
                newPairFromReserved(keyData, valueData);
            } 
        }
    }
    
    void change(const TKeyData& keyData, const TValueData& valueData){
        TKeyEntryInfo keyInfo = dataMap[keyData];
        if(valueData.size() > 0){
            if(keyInfo.key.dataLength >= valueData.size()){
                printf("rewrite\n");
                rewritePair(keyInfo, valueData);
            } else {
                //remove old and create new
                printf("remove old and create new\n");
                earsePair(keyInfo);
                addNew(keyData, valueData);
            }                   
        } else {
            // erase
            printf("erase\n");
            earsePair(keyInfo);
        }  
    }        
            
public:
    
	KvFile() {
		assert(sizeof(K) <= KVDB_KEY_SIZE);
	}

	void close() {
		if (!filePtr->is_open()) {
			return;
		}

		filePtr->close();
		dataMap.clear();
		reservedKeyList.clear();
		deletedKeyList.clear();
		tableList.clear();
	}

    void open(std::string& file){
        filePtr = new std::fstream(file, std::ios::in | std::ios::out | std::ios::binary);
        
        if (!filePtr->is_open()) {
            return;
        }
 
		fileMutex.lock();

        TFileHeader fileHeader;
        filePtr >> fileHeader;
                
        ulong64 nextTablePos = readTable();
        while(nextTablePos > 0){
            filePtr->seekg(nextTablePos);
            nextTablePos = readTable();
        }

		fileMutex.unlock();
    }
    
    TValueDataPtr get(const K& k){
		TKeyData& keyData = toKeyData(k);

        if (!filePtr->is_open()) {
            return nullptr;
        }

		fileMutex.lock();

        std::unordered_map<TKeyData, TKeyEntryInfo>::const_iterator got = dataMap.find(keyData);
        if(got == dataMap.end()) {
			fileMutex.unlock();
            return nullptr;
        }

        TKeyEntryInfo i = dataMap[keyData];
        TKeyEntry e = i.key;
        
        filePtr->seekg(e.dataPos);
        
        TValueDataPtr dataPtr = TValueDataPtr(new TValueData);
        for (int i = 0; i < e.dataLength; i++) {
            byte tmp;
            read(filePtr, tmp);
            dataPtr->push_back(tmp);
        }
    
		fileMutex.unlock();
        return dataPtr;
    }
    
    void erase(const K& k){
		TKeyData& keyData = toKeyData(k);

        if (!filePtr->is_open()) {
            return;
        }
        
		fileMutex.lock();
        std::unordered_map<TKeyData, TKeyEntryInfo>::const_iterator got = dataMap.find(keyData);
        if(got != dataMap.end()) {
            TKeyEntryInfo keyInfo = dataMap[keyData];
            earsePair(keyInfo);
        }

		fileMutex.unlock();
    }
    
    
    void put(const K& k, const V& v){
		TKeyData& keyData = toKeyData(k);
		TValueData& valueData = toValueData(v);

        if (!filePtr->is_open()) {
            return;
        }
        
		fileMutex.lock();

        std::unordered_map<TKeyData, TKeyEntryInfo>::const_iterator got = dataMap.find(keyData);
        if(got == dataMap.end()) {
            // pair not found  
            addNew(keyData, valueData);
        } else {        
            // pair found  
            change(keyData, valueData);
        }

		fileMutex.unlock();
    }
    
    static void create(std::string& file, const std::unordered_map<K, V>& test) {
        std::ofstream outf(file, std::ios::out | std::ios::binary);
    
        if (!outf) {
            return;
        }
    
        static const int max_key_records = KVDB_RESERVED_TABLE_SIZE;
        const int keyRecords = (test.size() > max_key_records) ? test.size() : max_key_records;
    
        // save file header
        TFileHeader fileHeader;
        fileHeader.endOfHeaderOffset = sizeof(fileHeader);
    
        outf << fileHeader;
    
        TTableHeader tableHeader;
        tableHeader.recordCount = keyRecords;
        outf << tableHeader;
    
        int bodyDataOffset = (int)(outf.tellp()) + sizeof(TKeyEntry) * keyRecords;
    
        std::vector<byte> dataBody;
    
        for(auto& e : test) {
            TKeyEntry entry;
            entry.freeKeyData = toKeyData(e.first);
            entry.dataPos = dataBody.size() + bodyDataOffset;
            
            TValueData valueData = toValueData(e.second);
            entry.dataLength = valueData.size();
            entry.initialDataLength = valueData.size();
            
            dataBody.insert(std::end(dataBody), std::begin(valueData), std::end(valueData));
        
            outf << entry;
        }
    
        if(test.size() < max_key_records){
            // add empty records
            const int emptyRecords = max_key_records - test.size();
            for(int i = 0; i < emptyRecords; i++){
                TKeyEntry entry;
                outf << entry;
            }
        }
                
        outf.write((char*)dataBody.data(), dataBody.size());
        outf.close();
    }
};

//-----------------------------------------------------------------------------
