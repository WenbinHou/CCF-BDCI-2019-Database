#if !defined(_CONFIG_H_INCLUDED_)
#define _CONFIG_H_INCLUDED_


//==============================================================
// Configuration macros
//==============================================================

#if !defined(MAKE_FASTEST)
//#define MAKE_FASTEST
#endif

#define ENABLE_UNLOADER_FOR_MUNMAP      1  // suggested to be 1
#define ENABLE_CHECK_INDEX_VALIDITY     0

#if !defined(MAKE_FASTEST)
#define ENABLE_LOGGING_TRACE            1
#define ENABLE_LOGGING_DEBUG            1
#define ENABLE_LOGGING_INFO             1
#define ENABLE_ASSERTION                1
#endif



#if !defined(MAX_WORKER_THREADS)
#define MAX_WORKER_THREADS              64  // A reasonable large value is OK...
#endif

#if !defined(MAX_QUERY_TOPN)
#define MAX_QUERY_TOPN                  10000
#endif



//==============================================================
// Widely-used macros
//==============================================================
#define EMPTY               /*empty macro as placeholder*/
#define JUST(...)           __VA_ARGS__

#define FORCEINLINE         inline __attribute__((always_inline))

#define LIKELY(_What_)      (__builtin_expect((_What_), 1))
#define UNLIKELY(_What_)    (__builtin_expect((_What_), 0))



inline const std::chrono::steady_clock::time_point __startup_time = std::chrono::steady_clock::now();

#define __LOG_INTERNAL(_Level_, _Msg_, ...) \
    do { \
        const auto _now_ = std::chrono::steady_clock::now(); \
        const uint64_t _elapsed_usec_ = (uint64_t)std::chrono::duration<double, std::micro>(_now_ - __startup_time).count(); \
        \
        fprintf(stderr, "\033[32;1m[%02d:%02d.%03d,%03d]\033[0m\033[34;1m %-5s [%s:%d]\033[0m " _Msg_ "\n", \
            (int)(_elapsed_usec_ / 60000000), (int)((_elapsed_usec_ / 1000000) % 60), \
            (int)(_elapsed_usec_ % 1000000) / 1000, (int)(_elapsed_usec_ % 1000), \
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
    } while(false)
#else
#define ASSERT(_What_, _Msg_, ...)
#endif



#define C_CALL(_Call_) \
    (([&]() { \
        auto const _ret_ = (_Call_); \
        static_assert(std::is_signed<decltype(_ret_)>::value, "Should return signed type"); \
        if (UNLIKELY(_ret_ < 0)) { \
            PANIC("%s failed with %ld. errno: %d (%s)", #_Call_, (int64_t)_ret_, errno, strerror(errno)); \
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


#endif  // !defined(_CONFIG_H_INCLUDED_)
