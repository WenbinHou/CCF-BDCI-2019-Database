#if !defined(_BDCI19_SPIN_LOCK_H_INCLUDED_)
#define _BDCI19_SPIN_LOCK_H_INCLUDED_

class spin_lock
{
public:
    DISABLE_COPY_MOVE_CONSTRUCTOR(spin_lock);

    __always_inline
    explicit spin_lock(const bool locked = false) noexcept
        : _locked((locked ? 1 : 0))
    { }

    [[nodiscard]]
    __always_inline
    bool is_locked() const noexcept
    {
        return _locked.load(std::memory_order_acquire);
    }

    [[nodiscard]]
    __always_inline
    bool try_lock() noexcept
    {
        size_t expected = 0;
        return _locked.compare_exchange_strong(expected, 1, std::memory_order_acq_rel);
    }

    __always_inline
    void lock() noexcept
    {
        while (!try_lock()) {
            while (is_locked()) { }
        }
    }

    __always_inline
    void unlock() noexcept
    {
        _locked.store(0, std::memory_order_release);
    }

private:
    std::atomic_size_t _locked;
};


template<typename TType>
class pthread_mutex
{
    static_assert(
        std::is_same<TType, process_private>::value ||
        std::is_same<TType, process_shared>::value);

public:
    DISABLE_COPY_MOVE_CONSTRUCTOR(pthread_mutex);

    __always_inline
    pthread_mutex() noexcept
    {
        pthread_mutexattr_t attr;

        PTHREAD_CALL(pthread_mutexattr_init(&attr));
        //PTHREAD_CALL(pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_NORMAL));
        if constexpr (std::is_same<TType, process_shared>::value) {
            PTHREAD_CALL(pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED));
        }
        else {
            //PTHREAD_CALL(pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_PRIVATE));
        }

        PTHREAD_CALL(pthread_mutex_init(&_mutex, &attr));

        PTHREAD_CALL(pthread_mutexattr_destroy(&attr));
    }

    __always_inline
    void lock() noexcept
    {
        PTHREAD_CALL(pthread_mutex_lock(&_mutex));
    }

    __always_inline
    void unlock() noexcept
    {
        PTHREAD_CALL(pthread_mutex_unlock(&_mutex));
    }

    [[nodiscard]]
    __always_inline
    bool try_lock() noexcept
    {
        const int err = pthread_mutex_trylock(&_mutex);
        if (__likely(err == 0)) {
            return true;
        }
        else if (err == EBUSY) {
            return false;
        }
        else {
            PANIC("pthread_mutex_trylock() failed: %d (%s)", err, strerror(err));
        }
    }

    __always_inline
    ~pthread_mutex() noexcept
    {
        PTHREAD_CALL(pthread_mutex_destroy(&_mutex));
    }

private:
    pthread_mutex_t _mutex { };
};


#endif  // !defined(_BDCI19_SPIN_LOCK_H_INCLUDED_)
