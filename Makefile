
all: clean
	g++ -std=c++20 -o test_kvdb test/test.cpp -lstdc++ 
	./test_kvdb

clean:
	rm -f test_kvdb
	rm -f *.dat
