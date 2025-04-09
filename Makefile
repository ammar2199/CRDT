CXX = g++
LDFLAGS = -std=c++17

.PHONY: clean

crdt: crdt.cpp
	$(CXX) -o $@.out $< $(LDFLAGS)

clean:
	rm -f *.out
