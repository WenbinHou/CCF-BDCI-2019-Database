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


#endif  // !defined(_BDCI19_SPIN_LOCK_H_INCLUDED_)
