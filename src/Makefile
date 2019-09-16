CXX         = g++
CXXFLAGS    = -O3 -pthread -Wall -std=gnu++14 -static-libstdc++ -static-libgcc -g
DEPS        = common.h
COBJ        = main.o
TARGET      = BDCI19.out

${TARGET}: $(COBJ)
	$(CXX) -o $@ $^ $(CXXFLAGS)

%.o: %.cpp $(DEPS)
	$(CXX) -c $< $(CXXFLAGS)

.PHONY: clean

clean :
	rm *.o $(TARGET)
