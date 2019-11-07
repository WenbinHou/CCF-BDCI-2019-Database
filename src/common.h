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
#include <sys/resource.h>
#include <sys/shm.h>
#include <thread>
#include <unistd.h>
#include <vector>

typedef __int128 int128_t;
typedef unsigned __int128 uint128_t;

typedef uint32_t index32_t;
typedef uint64_t index64_t;

#if !defined(PAGE_SIZE)
#define PAGE_SIZE   (4096)
#endif


#include "config.h"
#include "macros.h"

#include "str.h"
#include "fs.h"
#include "futex.h"
#include "sem.h"
#include "mm.h"
#include "mapper.h"
#include "spin_lock.h"
#include "sync_barrier.h"
#include "date.h"
#include "queue.h"


//==============================================================================
// Structures
//==============================================================================


//==============================================================================
// Global Constants
//==============================================================================
#define ACTION_DROP_PAGE_CACHE      "drop_page_cache"

#define SHMKEY_TXT_CUSTOMER         ((key_t)0x19491001)
#define SHMKEY_TXT_ORDERS           ((key_t)0x19491002)
#define SHMKEY_TXT_LINEITEM         ((key_t)0x19491003)
#define SHMKEY_CUSTKEY_TO_MKTID     ((key_t)0x19491004)
#define SHMKEY_ORDERKEY_TO_ORDER    ((key_t)0x19491005)

//==============================================================================
// Global Variables
//==============================================================================
struct shared_information_t {
public:
    DISABLE_COPY_MOVE_CONSTRUCTOR(shared_information_t);
    shared_information_t() noexcept = default;

public:
    std::atomic_uint64_t customer_file_shared_offset { 0 };
    std::atomic_uint64_t orders_file_shared_offset { 0 };
    std::atomic_uint64_t lineitem_file_shared_offset { 0 };

    sync_barrier loader_sync_barrier { };
    sync_barrier worker_sync_barrier { };

    volatile uint8_t mktid_count = 0;
    struct {
        uint8_t length;
        char name[12];
    } all_mktsegments[8] { };  // only used in create_index
    pthread_mutex<process_shared> all_mktsegments_insert_mutex { };  // only used in create_index

    bool sched_fifo_failed { false };
#if ENABLE_ASSERTION
    std::atomic_uint64_t customer_file_loaded_parts { 0 };
#endif
};

inline shared_information_t* g_shared = nullptr;
inline bool g_use_multi_process = false;
inline uint32_t g_active_cpu_cores = 0;  // number of CPU cores
inline uint32_t g_total_process_count = 0;  // process or thread count
inline uint32_t g_id = 0;

inline load_file_context g_customer_file { };
inline load_file_context g_orders_file { };
inline load_file_context g_lineitem_file { };

inline int g_index_directory_fd = -1;
inline bool g_is_creating_index = false;
inline bool g_is_preparing_page_cache = true;

inline uint32_t g_query_count = 0;
inline const char* const* g_argv_queries = nullptr;



//==============================================================================
// Functions in create_index.cpp
//==============================================================================
void fn_loader_thread_create_index(const uint32_t tid) noexcept;
void fn_worker_thread_create_index(const uint32_t tid) noexcept;
void fn_unloader_thread_create_index() noexcept;
void create_index_initialize_before_fork() noexcept;
void create_index_initialize_after_fork() noexcept;


//==============================================================================
// Functions in use_index.cpp
//==============================================================================
void fn_loader_thread_use_index(const uint32_t tid) noexcept;
void fn_worker_thread_use_index(const uint32_t tid) noexcept;
void fn_unloader_thread_use_index() noexcept;
void use_index_initialize_before_fork() noexcept;
void use_index_initialize_after_fork() noexcept;


//==============================================================================
// Very common routines
//==============================================================================
__always_inline
void pin_thread_to_cpu_core(const uint32_t core) noexcept
{
    cpu_set_t cpu_set;
    CPU_ZERO(&cpu_set);
    CPU_SET(core, &cpu_set);
    PTHREAD_CALL_NO_PANIC(pthread_setaffinity_np(pthread_self(), sizeof(cpu_set), &cpu_set));
}

__always_inline
void set_thread_fifo_scheduler(const uint32_t nice_from_max_priority) noexcept
{
    static const uint32_t __max_priority = C_CALL(sched_get_priority_max(SCHED_FIFO));
    ASSERT(nice_from_max_priority < __max_priority);

    ASSERT(g_shared != nullptr);
    if (g_shared->sched_fifo_failed) {
        return;
    }

    sched_param param { };
    param.sched_priority = (int)(__max_priority - nice_from_max_priority);
    const int err = PTHREAD_CALL_NO_PANIC(pthread_setschedparam(pthread_self(), SCHED_FIFO, &param));
    if (err != 0) {
        g_shared->sched_fifo_failed = true;
    }
}


#endif  // !defined(_BDCI19_COMMON_H_INCLUDED_)
