CXX         = g++
CXXFLAGS    = -O3 -pthread -Wall -std=gnu++14 -static-libstdc++ -static-libgcc
OBJS        = main.o
TARGET      = BDCI19.out

${TARGET}: $(OBJS)
	$(CXX) -o $@ $^ $(CXXFLAGS)

%.o: %.cpp common.h
	$(CXX) -c $< -g $(CXXFLAGS)

.PHONY: fastest
fastest: main.cpp common.h
	$(CXX) -o ${TARGET} main.cpp common.h -DMAKE_FASTEST=1 $(CXXFLAGS)

.PHONY: clean
clean:
	rm *.o $(TARGET)
