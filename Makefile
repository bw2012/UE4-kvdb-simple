
CC = g++
CFLAGS = -std=c++20 -Wall
CLIBS = -lstdc++ 

all: clean_data tests 

tests: test_kvdb
	./test_kvdb

test_kvdb: test/test.cpp kvdb.hpp
	$(CC) $(CFLAGS) -o test_kvdb test/test.cpp $(CLIBS) 

clean_data:
	rm -f *.dat1
	
clean: clean_data
	rm -f test_kvdb

