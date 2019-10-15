#if !defined(_BDCI19_COMMON_H_INCLUDED_)
#define _BDCI19_COMMON_H_INCLUDED_

//==============================================================================
// Standard C++ / System Headers
//==============================================================================
#include <atomic>
#include <cerrno>
#include <cstring>
#include <cstdio>
#include <chrono>
#include <cstdint>
#include <cinttypes>
#include <climits>
#include <vector>
#include <algorithm>
#include <functional>
#include <sys/sysctl.h>
#include <sys/syscall.h>
#include <sys/fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <semaphore.h>
#include <unistd.h>
#include <thread>
#include <pthread.h>
#include <mutex>
#include <shared_mutex>


//==============================================================================
// My Headers
//==============================================================================
#include "config.h"

#include "macros.h"
#include "spin_lock.h"
#include "date.h"
#include "sync_barrier.h"
#include "queue.h"
#include "mapper.h"
#include "done_event.h"


//==============================================================================
// Structures
//==============================================================================
struct load_file_context
{
    int fd = -1;
    uint64_t file_size = 0;
    std::atomic_uint64_t curr_offset { 0 };
};


//==============================================================================
// Global Variables
//==============================================================================
inline int g_index_directory_fd = -1;

inline load_file_context g_customer_file;
inline load_file_context g_orders_file;
inline load_file_context g_lineitem_file;

inline std::vector<std::thread> g_worker_threads;
inline sync_barrier g_worker_sync_barrier;

inline std::vector<std::thread> g_loader_threads;
inline sync_barrier g_loader_sync_barrier;

inline uint32_t g_query_count = 0;
inline const char* const* g_argv_queries = nullptr;

[[maybe_unused]] inline struct {
    uint32_t partial_index_count;
    uint32_t max_shipdate_orderdate_diff;
    uint32_t max_bucket_actual_size;
    uint32_t max_bucket_actual_size_up_aligned;
} g_meta;

#endif  // !defined(_BDCI19_COMMON_H_INCLUDED_)



//==============================================================================
// Functions in create_index.cpp
//==============================================================================
void fn_loader_thread_create_index(const uint32_t tid) noexcept;
void fn_worker_thread_create_index(const uint32_t tid) noexcept;
void create_index_initialize() noexcept;


//==============================================================================
// Functions in use_index.cpp
//==============================================================================
void fn_loader_thread_use_index(const uint32_t tid) noexcept;
void fn_worker_thread_use_index(const uint32_t tid) noexcept;
void use_index_initialize() noexcept;


//==============================================================================
// Very common routines
//==============================================================================
FORCEINLINE void openat_file(const char *const path, int *const fd, uint64_t *const file_size) noexcept
{
    ASSERT(g_index_directory_fd >= 0);
    *fd = C_CALL(openat(g_index_directory_fd, path, O_RDONLY | O_CLOEXEC));

    struct stat64 st;
    C_CALL(fstat64(*fd, &st));
    *file_size = st.st_size;

    DEBUG("openat %s: fd = %d, size = %lu", path, *fd, *file_size);
};