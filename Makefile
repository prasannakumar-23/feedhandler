CXX ?= g++
CXXFLAGS ?= -std=c++17 -O2 -Wall -Wextra -Iinclude
LDFLAGS ?= -lpthread
all: feedsim tests
feedsim: src/feedsim.cpp include/*.h
	$(CXX) $(CXXFLAGS) src/feedsim.cpp -o feedsim $(LDFLAGS)
tests: tests/tests.cpp include/*.h
	$(CXX) $(CXXFLAGS) tests/tests.cpp -o tests_bin $(LDFLAGS)
check: tests
	./tests_bin
tsan:
	$(CXX) -std=c++17 -O1 -g -fsanitize=thread -Iinclude tests/tests.cpp -o tests_tsan $(LDFLAGS) && ./tests_tsan
asan:
	$(CXX) -std=c++17 -O1 -g -fsanitize=address,undefined -Iinclude tests/tests.cpp -o tests_asan $(LDFLAGS) && ./tests_asan
trace: feedsim
	./feedsim trace --packets 300 --drop 0.05 --dup 0.03 --reorder 0.04 --seed 5 > docs/trace.json
clean:
	rm -f feedsim tests_bin tests_tsan tests_asan
