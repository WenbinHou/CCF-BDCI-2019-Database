#if !defined(_BDCI19_SYNC_BARRIER_H_INCLUDED_)
#define _BDCI19_SYNC_BARRIER_H_INCLUDED_

class sync_barrier
{
public:
    __always_inline
    void init(const uint32_t thread_count, const bool process_shared) noexcept
    {
        ASSERT(thread_count > 0, "sync_barrier init() with thread_count == 0");
        _thread_count = thread_count;

        pthread_barrierattr_t attr;
        PTHREAD_CALL(pthread_barrierattr_init(&attr));
        if (process_shared) {
            PTHREAD_CALL(pthread_barrierattr_setpshared(&attr, PTHREAD_PROCESS_SHARED));
        }
        else {
            //PTHREAD_CALL(pthread_barrierattr_setpshared(&attr, PTHREAD_PROCESS_PRIVATE));
        }

        PTHREAD_CALL(pthread_barrier_init(&_barrier, &attr, thread_count));

        PTHREAD_CALL(pthread_barrierattr_destroy(&attr));
    }

    __always_inline
    void sync() noexcept
    {
        ASSERT(_thread_count > 0, "sync_barrier init() not called?");

        [[maybe_unused]] const int ret = pthread_barrier_wait(&_barrier);
        CHECK(ret == 0 || ret == PTHREAD_BARRIER_SERIAL_THREAD, "pthread_barrier_wait() failed");
    }

    template<typename TFn, typename... TArgs>
    __always_inline
    void sync_and_run_once(TFn fn, TArgs&&... args) noexcept
    {
        ASSERT(_thread_count > 0, "sync_barrier init() not called?");

        const int ret = pthread_barrier_wait(&_barrier);
        CHECK(ret == 0 || ret == PTHREAD_BARRIER_SERIAL_THREAD, "pthread_barrier_wait() failed");
        if (ret == PTHREAD_BARRIER_SERIAL_THREAD) {
            fn(std::forward<TArgs>(args)...);
        }
    }

    template<typename TFn, typename... TArgs>
    __always_inline
    void run_once_and_sync(TFn fn, TArgs&&... args) noexcept
    {
        ASSERT(_thread_count > 0, "sync_barrier init() not called?");

        if (_run_once_and_sync_counter.fetch_add(1, std::memory_order_acq_rel) % _thread_count == 0) {
            fn(std::forward<TArgs>(args)...);
        }
        
        const int ret = pthread_barrier_wait(&_barrier);
        CHECK(ret == 0 || ret == PTHREAD_BARRIER_SERIAL_THREAD, "pthread_barrier_wait() failed");
    }

    ~sync_barrier() noexcept
    {
        if (_thread_count > 0) {
            PTHREAD_CALL(pthread_barrier_destroy(&_barrier));
            _thread_count = 0;
        }
    }

    [[nodiscard]]
    __always_inline
    uint32_t thread_count() const noexcept
    {
        return _thread_count;
    }

private:
    std::atomic_uint64_t _run_once_and_sync_counter { 0 };
    uint32_t _thread_count = 0;
    pthread_barrier_t _barrier { };
};

#endif  // !defined(_BDCI19_SYNC_BARRIER_H_INCLUDED_)
