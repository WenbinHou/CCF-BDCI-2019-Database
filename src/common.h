#if !defined(_BDCI19_COMMON_H_INCLUDED_)
#define _BDCI19_COMMON_H_INCLUDED_


//==============================================================================
// Standard C++ / System Headers
//==============================================================================
#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstring>
#include <cstdio>
#include <chrono>
#include <cstdint>
#include <cinttypes>
#include <climits>
#include <functional>
#include <linux/futex.h>
#include <mutex>
#include <pthread.h>
#include <random>
//#include <semaphore.h>  // use custom semaphore
#include <shared_mutex>
#include <sys/sysctl.h>
#include <sys/syscall.h>
#include <sys/fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <thread>
#include <unistd.h>
#include <vector>

typedef __int128 int128_t;
typedef unsigned __int128 uint128_t;

typedef uint32_t index32_t;
typedef uint64_t index64_t;


#include "config.h"
#include "macros.h"

#include "futex.h"
#include "sem.h"


#endif  // !defined(_BDCI19_COMMON_H_INCLUDED_)
