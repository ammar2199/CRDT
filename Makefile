CXX = g++
CXXFLAGS = -std=c++17

.PHONY: clean

crdt: crdt.cpp
	$(CXX) -o $@.out $< $(CXXFLAGS)

clean:
	rm -f *.out
