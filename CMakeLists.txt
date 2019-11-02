cmake_minimum_required(VERSION 3.9.0 FATAL_ERROR)

set(CMAKE_CXX_FLAGS_RELEASE "")  # default: -O3 -DNDEBUG
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-g")  # default: -O2 -g -DNDEBUG
set(CMAKE_CUDA_FLAGS_RELEASE "")  # default: -O3 -DNDEBUG
set(CMAKE_CUDA_FLAGS_RELWITHDEBINFO "-g")  # default: -O2 -g -DNDEBUG

project(BDCI19
    LANGUAGES C CXX CUDA
    VERSION 0.2.0)


set(CMAKE_VERBOSE_MAKEFILE ON)


file(GLOB CPP_SRC_FILES
    CONFIGURE_DEPENDS
    RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
    src/*.cpp)
message(STATUS "CPP_SRC_FILES: ${CPP_SRC_FILES}")

file(GLOB CUDA_SRC_FILES
    CONFIGURE_DEPENDS
    RELATIVE ${CMAKE_CURRENT_SOURCE_DIR}
    src/*.cu)
message(STATUS "CUDA_SRC_FILES: ${CUDA_SRC_FILES}")


set(CPP_FLAGS "")
string(APPEND CPP_FLAGS " -std=gnu++17 -Ofast")
string(APPEND CPP_FLAGS " -flto -mtune=broadwell -march=broadwell")
string(APPEND CPP_FLAGS " -funsafe-loop-optimizations -fstack-protector-explicit")
string(APPEND CPP_FLAGS " -Wunsafe-loop-optimizations -Wall -Wextra -Weffc++ -Winline -Wformat=2")
set_source_files_properties(${CPP_SRC_FILES} PROPERTIES COMPILE_FLAGS "${CPP_FLAGS}")

set(CUDA_FLAGS "")
string(APPEND CUDA_FLAGS " -std=c++14 -O3")
string(APPEND CUDA_FLAGS " -gencode arch=compute_60,code=sm_60")
#string(APPEND CUDA_FLAGS " -gencode arch=compute_70,code=sm_70")
set_source_files_properties(${CUDA_SRC_FILES} PROPERTIES COMPILE_FLAGS "${CUDA_FLAGS}")


add_executable(BDCI19 ${CPP_SRC_FILES} ${CUDA_SRC_FILES})

if (MAKE_FASTEST)
    target_compile_definitions(BDCI19 PUBLIC MAKE_FASTEST=1)
endif()

find_package(Threads REQUIRED)
target_link_libraries(BDCI19 PRIVATE
    -static-libgcc -static-libstdc++
    Threads::Threads)
