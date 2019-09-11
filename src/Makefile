CXX         = g++
CXXFLAGS    = -O3 -lpthread -Wall -std=gnu++14 -static-libstdc++
DEPS        = common.h
COBJ        = main.o load_files.o 
TARGET      = BDCI19.out

${TARGET}: $(COBJ)
	$(CXX) -o $@ $^ $(CXXFLAGS)

%.o: %.cpp $(DEPS)
	$(CXX) -c $< $(CXXFLAGS)

.PHONY: clean

clean :
	rm *.o $(TARGET)
