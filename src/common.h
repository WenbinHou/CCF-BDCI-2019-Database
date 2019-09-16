#if !defined(_COMMON_H_INCLUDED_)
#define _COMMON_H_INCLUDED_

#if !defined(_GNU_SOURCE)
#define _GNU_SOURCE
#endif

// ReSharper disable CppUnusedIncludeDirective
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cinttypes>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <future>
#include <map>
#include <pthread.h>
#include <semaphore.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/fcntl.h>
#include <sys/uio.h>
#include <thread>
#include <type_traits>
#include <vector>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>
// ReSharper restore CppUnusedIncludeDirective



//==============================================================
// Compilation flags
//==============================================================
#if !defined(MAKE_FASTEST)
#define MAKE_FASTEST            0
#endif

#if !MAKE_FASTEST
#define ENABLE_PROFILING        1
#define ENABLE_LOGGING_TRACE    1
#define ENABLE_LOGGING_DEBUG    1
#define ENABLE_LOGGING_INFO     1
#define ENABLE_ASSERTION        1
#endif

#if !defined(MAX_WORKER_THREADS)
#define MAX_WORKER_THREADS      16
#endif
#if !defined(MAX_LOADER_THREADS)
#define MAX_LOADER_THREADS      4
#endif



//==============================================================
// Global variables (declared in main.cpp)
//==============================================================
extern const std::chrono::steady_clock::time_point g_startup_time;
extern uint32_t g_workers_thread_count;
extern volatile uint64_t g_dummy_prevent_optimize;



//==============================================================
// Widely-used macros
//==============================================================
#define FORCEINLINE         inline __attribute__((always_inline))

#define LIKELY(_What_)      (__builtin_expect((_What_), 1))
#define UNLIKELY(_What_)    (__builtin_expect((_What_), 0))

#define __LOG_INTERNAL(_Level_, _Msg_, ...) \
    do { \
        const auto _now_ = std::chrono::steady_clock::now(); \
        const uint64_t _elapsed_usec_ = (uint64_t)std::chrono::duration<double, std::micro>(_now_ - g_startup_time).count(); \
        \
        fprintf(stderr, "[%02d:%02d.%06d] %-5s [%s:%d] " _Msg_ "\n", \
            (int)(_elapsed_usec_ / 60000000), (int)((_elapsed_usec_ / 1000000) % 60), (int)(_elapsed_usec_ % 1000000), \
            (_Level_), \
            (strrchr(__FILE__, '/') ? strrchr(__FILE__, '/') + 1 : __FILE__), \
            __LINE__, \
            ##__VA_ARGS__); \
        fflush(stderr); \
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
    (([&]() { \
        auto const _ret_ = (_Call_); \
        static_assert(std::is_signed<decltype(_ret_)>::value, "Should return signed type"); \
        if (UNLIKELY(_ret_ < 0)) { \
            PANIC("%s failed with %" PRId64 ". errno: %d (%s)", #_Call_, (int64_t)_ret_, errno, strerror(errno)); \
        } \
        return _ret_; \
    })())

#define PTHREAD_CALL(_Call_) \
    do { \
        const int _err_ = (_Call_); \
        if (UNLIKELY(_err_ != 0)) { \
            PANIC("%s failed with %d (%s)", #_Call_, _err_, strerror(_err_)); \
        } \
    } while(false)



//==============================================================
// Structures
//==============================================================
struct c_string_less
{
    bool operator()(char const* const a, char const* const b) const noexcept { return strcmp(a, b) < 0; }
};

struct date_t
{
    FORCEINLINE void parse(const char* const str, const uint8_t mktsegment) noexcept
    {
        // NOTE: for query's orderdate, mktsegment should be 0x00
        //       because we require o_orderdate < q_orderdate 
        // NOTE: for query's shipdate, mktsegment should be 0xff
        //       because we require o_shipdate > q_shipdate 

        // `str` looks like "yyyy-MM-dd"
        //ASSERT(str[10] == '\n' || str[10] == '\0', "Unexpected char in date_t: (0x%02x) '%c'", str[10], str[10]);
        _value =
            ((str[0] - '0') * 1000 + (str[1] - '0') * 100 + (str[2] - '0') * 10 + (str[3] - '0')) << 18 |
            ((str[5] - '0') * 10 + (str[6] - '0')) << 14 |
            ((str[8] - '0') * 10 + (str[9] - '0')) << 8 |
            mktsegment;
    }

    FORCEINLINE uint32_t year() const noexcept { return (_value >> 18); }

    FORCEINLINE uint32_t month() const noexcept { return (_value >> 14) & 0b1111; }

    FORCEINLINE uint32_t day() const noexcept { return (_value >> 8) & 0b111111; }

    FORCEINLINE uint8_t mktsegment() const noexcept { return (uint8_t)(_value & 0b11111111); }

    // NOTE: we don't compare mktsegment here! (see `parse()` function)
    FORCEINLINE bool operator < (const date_t& date) const noexcept { return (_value <  date._value); }
    FORCEINLINE bool operator <=(const date_t& date) const noexcept { return (_value <= date._value); }
    FORCEINLINE bool operator > (const date_t& date) const noexcept { return (_value >  date._value); }
    FORCEINLINE bool operator >=(const date_t& date) const noexcept { return (_value >= date._value); }
    FORCEINLINE bool operator ==(const date_t& date) const noexcept { return (_value == date._value); }
    FORCEINLINE bool operator !=(const date_t& date) const noexcept { return (_value != date._value); }


private:
    // (14 bits) 18-31: year
    //  (4 bits) 14-17: month
    //  (6 bits)  8-13: day
    //  (8 bits)  0-7 : mktsegment

    // ReSharper disable once CppUninitializedNonStaticDataMember    
    uint32_t _value;
};
static_assert(sizeof(date_t) == 4, "date_t should be 4 bytes");


template<typename T>
struct mpmc_queue_t
{
    static_assert(std::is_trivial<T>::value, "T must be trivial type");

    mpmc_queue_t(const mpmc_queue_t&) noexcept = delete;
    mpmc_queue_t& operator =(const mpmc_queue_t&) noexcept = delete;
    mpmc_queue_t(mpmc_queue_t&&) noexcept = delete;
    mpmc_queue_t& operator =(mpmc_queue_t&&) noexcept = delete;

    mpmc_queue_t() noexcept
    {
        C_CALL(sem_init(&_sem, /*pshared*/0, 0));
    }

    void init(const size_t capacity) noexcept
    {
        //_capacity = capacity;
        _items = (T*)malloc(sizeof(T) * capacity);
        ASSERT(_items != nullptr, "malloc() failed");
    }

    void mark_finish_push() noexcept
    {
        for (uint32_t t = 0; t < g_workers_thread_count; ++t) {
            C_CALL(sem_post(&_sem));
        }
    }

    void push(const T& item) noexcept
    {
        // NOTE: this lock is necessary!
        //
        // Consider this situation:
        //      Thread_1: tmp1=_tail++
        //                      Thread_2: tmp2=_tail++
        //                      Thread_2: _items[tmp2]=...
        //                      Thread_2: sem_post
        //                                      Thread_3: pop()  // will get _items[tmp1]
        //      Thread_1: _items[tmp1]=...
        //      Thread_1: sem_post
        //
        // THIS WILL BE A CONTENTION!
        std::lock_guard<std::mutex> lock(_mutex);

        ASSERT(_items != nullptr, "mpmc_queue_t init() not called");
        _items[_tail++] = item;

        C_CALL(sem_post(&_sem));
    }

    bool pop(/*out*/ T& item) noexcept
    {
        C_CALL(sem_wait(&_sem));

        const uint64_t head = _head++;
        if (head >= _tail) return false;
        ASSERT(_items != nullptr, "mpmc_queue_t init() not called");
        item = _items[head];
        return true;
    }

    ~mpmc_queue_t() noexcept
    {
        if (_items) {
            free(_items);
            _items = nullptr;
        }
        C_CALL(sem_destroy(&_sem));
    }

private:
    //size_t _capacity;
    sem_t _sem;
    T* _items = nullptr;
    std::atomic_uint64_t _tail { 0 } ;
    std::atomic_uint64_t _head { 0 };
    std::mutex _mutex;
};



struct query_result_t
{
    uint32_t total_expend_cent;
    uint32_t orderkey;
    date_t orderdate;

    query_result_t() noexcept = default;
    query_result_t(const uint32_t total_expend_cent, const uint32_t orderkey, const date_t orderdate) noexcept
        : total_expend_cent(total_expend_cent), orderkey(orderkey), orderdate(orderdate)
    { }

    FORCEINLINE bool operator >(const query_result_t& other) const noexcept {
        if (total_expend_cent > other.total_expend_cent) return true;
        if (total_expend_cent < other.total_expend_cent) return false;
        return (orderkey > other.orderkey);
    }
};

struct query_t
{
    uint8_t q_mktsegment;
    date_t q_orderdate;
    date_t q_shipdate;
    uint32_t q_limit;

    uint32_t query_index;
    std::vector<query_result_t> results[MAX_WORKER_THREADS];
    std::vector<query_result_t> final_result;
};



struct mapped_file_part_t
{
    bool is_first;
    bool is_last;
    const char* start;
    const char* desired_end;
    uint64_t map_size;
};


struct timer
{
public:
    FORCEINLINE timer() noexcept { reset(); }

    FORCEINLINE void reset() noexcept
    {
        _start_time = std::chrono::steady_clock::now();
    }

    FORCEINLINE double elapsed_msec() const noexcept
    {
        return std::chrono::duration<double, std::milli>(std::chrono::steady_clock::now() - _start_time).count();
    }

private:
    std::chrono::steady_clock::time_point _start_time;
};


struct threads_barrier_t
{
    void init(const uint32_t thread_count) noexcept
    {
        _thread_count = thread_count;

        PTHREAD_CALL(pthread_barrier_init(&_barrier, nullptr, thread_count));
    }

    void sync() noexcept
    {
        [[maybe_unused]] const int ret = pthread_barrier_wait(&_barrier);
        ASSERT(ret == 0 || ret == PTHREAD_BARRIER_SERIAL_THREAD, "pthread_barrier_wait() failed");
    }

private:
    uint32_t _thread_count;
    pthread_barrier_t _barrier;
};




FORCEINLINE void preload_memory_range(const void* base, const uint64_t length)
{
    constexpr uint64_t step = 4096;

    static uint64_t dummy = 0;
    for (uint64_t off = 0; off < length; off += step) {
        dummy += *(uint8_t*)((uintptr_t)base + off);
    }
    g_dummy_prevent_optimize = dummy;
}

FORCEINLINE void preload_memory_range_2M(const void* base, const uint64_t length)
{
    // The files are mapped with page size = 2 MB
    constexpr uint64_t step = 1024 * 1024 * 2;

    static uint64_t dummy = 0;
    for (uint64_t off = 0; off < length; off += step) {
        dummy += *(uint8_t*)((uintptr_t)base + off);
    }
    g_dummy_prevent_optimize = dummy;
}


FORCEINLINE void open_file(
    /*in*/ const char* const file,
    /*out*/ uint64_t* filesize,
    /*out*/ int* fd)
{
    // TODO: Specify (O_DIRECT | O_SYNC) to disable system cache?
    *fd = C_CALL(open(file, O_RDONLY | O_CLOEXEC));

    struct stat st { };
    C_CALL(fstat(*fd, &st));
    *filesize = st.st_size;

    DEBUG("open_file: %s (fd=%d, size=%" PRIu64 ")", file, *fd, *filesize);
}


FORCEINLINE void open_file_mapping(
    const char* const file,
    /*out*/ iovec* const mapped)
{
    // TODO: Specify (O_DIRECT | O_SYNC) to disable system cache?
    //const int fd = C_CALL(open(file, O_RDONLY | O_DIRECT | O_SYNC | O_CLOEXEC));
    const int fd = C_CALL(open(file, O_RDONLY | O_CLOEXEC));

    struct stat st { };
    C_CALL(fstat(fd, &st));
    mapped->iov_len = st.st_size;

#if !defined(MAP_HUGE_2MB)
#define MAP_HUGE_2MB    (21 << MAP_HUGE_SHIFT)
#endif  // !defined(MAP_HUGE_2MB)

    mapped->iov_base = mmap(
        nullptr,
        mapped->iov_len,
        PROT_READ,
        MAP_PRIVATE | MAP_POPULATE /* | MAP_HUGETLB | MAP_HUGE_2MB*/,  // don't use MAP_POPULATE: we do pre-fault later
        fd,
        0);
    if (UNLIKELY(mapped->iov_base == MAP_FAILED)) {
        PANIC("mmap() failed. errno = %d (%s)", errno, strerror(errno));
    }
    DEBUG("%s mapped to %p (size: %" PRIu64 ")", file, mapped->iov_base, (uint64_t)mapped->iov_len);

    C_CALL(close(fd));
}


#endif  // !defined(_COMMON_H_INCLUDED_)
