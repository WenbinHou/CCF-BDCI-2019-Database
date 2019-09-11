#if !defined(_COMMON_H_INCLUDED_)
#define _COMMON_H_INCLUDED_

#if !defined(_GNU_SOURCE)
#define _GNU_SOURCE
#endif

#include <cerrno>
#include <chrono>
#include <cinttypes>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <map>
#include <queue>
#include <set>
#include <thread>
#include <type_traits>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <syscall.h>
#include <unistd.h>


//==============================================================
// Compilation flags
//==============================================================
#define ENABLE_PROFILING        1
#define ENABLE_LOGGING_TRACE    1
#define ENABLE_LOGGING_DEBUG    1
#define ENABLE_LOGGING_INFO     1
#define ENABLE_ASSERTION        1



//==============================================================
// Global variables (declared in main.cpp)
//==============================================================
extern const std::chrono::steady_clock::time_point g_startup_time;
extern volatile uint64_t g_dummy_prevent_optimize;



//==============================================================
// Widely-used macros
//==============================================================
#define LIKELY(_What_)      (__builtin_expect((_What_), 1))
#define UNLIKELY(_What_)    (__builtin_expect((_What_), 0))

#define __LOG_INTERNAL(_Level_, _Msg_, ...) \
    do { \
        const auto _now_ = std::chrono::steady_clock::now(); \
        const uint64_t _elapsed_msec_ = (uint64_t)std::chrono::duration<double, std::milli>(_now_ - g_startup_time).count(); \
        \
        fprintf(stderr, "[%02d:%02d.%03d] %-5s [%s:%d] " _Msg_ "\n", \
            (int)(_elapsed_msec_ / 60000), (int)((_elapsed_msec_ / 1000) % 60), (int)(_elapsed_msec_ % 1000), \
            (_Level_), \
            (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__), \
            __LINE__, \
            ##__VA_ARGS__); \
        /*fflush(stderr);*/ \
    } while(false)


#if ENABLE_LOGGING_TRACE
#define TRACE(_Msg_, ...)   __LOG_INTERNAL("TRACE", _Msg_, ##__VA_ARGS__)
#else
#define TRACE(_Msg_, ...)
#endif

#if ENABLE_LOGGING_DEBUG
#define DEBUG(_Msg_, ...)   __LOG_INTERNAL("DEBUG", _Msg_, ##__VA_ARGS__)
#else
#define DEBUG(_Msg_, ...)
#endif

#if ENABLE_LOGGING_INFO
#define INFO(_Msg_, ...)    __LOG_INTERNAL("INFO", _Msg_, ##__VA_ARGS__)
#else
#define INFO(_Msg_, ...)
#endif


#define PANIC(_Msg_, ...) \
    do { \
        __LOG_INTERNAL("PANIC", _Msg_, ##__VA_ARGS__); \
        fflush(stderr); \
        std::abort(); \
    } while(false)


#if ENABLE_ASSERTION
#define ASSERT(_What_, _Msg_, ...) \
    do { \
        if (UNLIKELY(!(_What_))) { \
            PANIC("ASSERT(%s) failed. " _Msg_, #_What_, ##__VA_ARGS__); \
        } \
    } while(false);
#else
#define ASSERT(_What_, _Msg_, ...)
#endif



#define C_CALL(_Call_) \
    ([&]() { \
        auto const _ret_ = (_Call_); \
        static_assert(std::is_signed<decltype(_ret_)>::value, "Should return signed type"); \
        if (UNLIKELY(_ret_ < 0)) { \
            PANIC("%s failed with %" PRId64 ". errno: %d (%s)", #_Call_, (int64_t)_ret_, errno, strerror(errno)); \
        } \
        return _ret_; \
    })();




//==============================================================
// Structures
//==============================================================
union date_t
{
    struct {
        uint8_t day;
        uint8_t month;
        uint16_t year;
    };
    uint32_t value;
};
static_assert(sizeof(date_t) == 4, "date_t should be 4 bytes");

static inline __attribute__((always_inline))
date_t parse_date(const char* const str) noexcept
{
    // str: yyyy-MM-dd
    ASSERT(str[10] == '\0' || str[10] == '\n', "Unexpected date format: %s", str);

    date_t date;
    date.year = (uint16_t)((uint32_t)(str[0] - '0') * 1000 + (uint32_t)(str[1] - '0') * 100 + (uint32_t)(str[2] - '0') * 10 + (uint32_t)(str[3] - '0') * 1);
    date.month = (uint8_t)((uint32_t)(str[5] - '0') * 10 + (uint32_t)(str[6] - '0') * 1);
    date.day = (uint8_t)((uint32_t)(str[8] - '0') * 10 + (uint32_t)(str[9] - '0') * 1);
    return date;
}

struct result_t
{
    uint32_t order_key;
    date_t order_date;
    uint32_t total_expend_cent;

    result_t(const uint32_t order_key, const date_t order_date, const uint32_t total_expend_cent) noexcept
        : order_key(order_key), order_date(order_date), total_expend_cent(total_expend_cent)
    { }

    bool operator >(const result_t& other) const noexcept {
        return total_expend_cent > other.total_expend_cent;
    }
};


static constexpr size_t MAX_THREAD_COUNT = 16;

struct query_t
{
    date_t order_date;
    date_t ship_date;
    uint32_t limit_count;
    uint8_t mktsegment_id;
    uint8_t query_id;

    uint32_t tmp_total_expend_cent[MAX_THREAD_COUNT] { };
    std::priority_queue<result_t, std::vector<result_t>, std::greater<result_t>> top_n[MAX_THREAD_COUNT];
};


static inline __attribute__((always_inline))
void preload_memory_range(const iovec vec)
{
    // TODO: The files are mapped with page size = 2 MB
    // So pre-fault every 2 MB
    constexpr uint64_t step = 4096;  // 1024 * 1024 * 2

    static uint64_t dummy = 0;
    for (uint64_t off = 0; off < vec.iov_len; off += step) {
        dummy += *(uint8_t*)((uintptr_t)vec.iov_base + off);
    }
    g_dummy_prevent_optimize = dummy;
}


//==============================================================
// load_files.cpp
//==============================================================

std::thread preload_files(
    const char* const file_customer,
    const char* const file_orders,
    const char* const file_lineitem,
    /*out*/ iovec* const mapped_customer,
    /*out*/ iovec* const mapped_orders,
    /*out*/ iovec* const mapped_lineitem);


#endif  // !defined(_COMMON_H_INCLUDED_)
