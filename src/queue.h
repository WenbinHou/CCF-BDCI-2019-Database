#if !defined(_BDCI19_QUEUE_H_INCLUDED_)
#define _BDCI19_QUEUE_H_INCLUDED_

template<typename T>
class mpmc_queue
{
    static_assert(std::is_trivial<T>::value, "T must be trivial type");

public:
    FORCEINLINE mpmc_queue() noexcept
    {
        C_CALL(sem_init(&_sem, /*pshared*/0, 0));
    }

    [[nodiscard]]
    FORCEINLINE size_t max_push() const noexcept
    {
        ASSERT(_max_push > 0, "_max_push == 0. init() not called yet?");
        return _max_push;
    }

    FORCEINLINE void init(const size_t max_push, const size_t max_consumer_threads) noexcept
    {
        ASSERT(max_push > 0, "BUG: max_push == 0");
        ASSERT(_max_push == 0, "BUG: _max_push = %lu. init() called more than once?", _max_push);
        _max_push = max_push;

        ASSERT(max_consumer_threads > 0, "BUG: max_consumer_threads == 0");
        _max_consumer_threads = max_consumer_threads;

        _items = (T*)malloc(max_push * sizeof(T));
        ASSERT(_items != nullptr, "malloc() failed");
    }

    FORCEINLINE ~mpmc_queue() noexcept
    {
        if (_items != nullptr) {
            free(_items);
            _items = nullptr;
        }

        C_CALL(sem_destroy(&_sem));
    }

    FORCEINLINE void push(const T& item) noexcept
    {
        ASSERT(_items != nullptr, "BUG: _items == nullptr. init() not called?");

        _lock.lock();
        const size_t tail = _tail++;
        ASSERT(tail < _max_push, "BUG: tail >= _max_push (%lu >= %lu): too many push()?", tail, _max_push);
        _items[tail] = item;
        _lock.unlock();

        C_CALL(sem_post(&_sem));
    }

    template<typename... Args>
    FORCEINLINE void emplace(Args&&... args) noexcept
    {
        ASSERT(_items != nullptr, "BUG: _items == nullptr. init() not called?");

        _lock.lock();
        const size_t tail = _tail++;
        ASSERT(tail < _max_push, "BUG: tail >= _max_push (%lu >= %lu): too many push()?", tail, _max_push);
        new (&_items[tail]) T(std::forward<Args>(args)...);
        _lock.unlock();

        C_CALL(sem_post(&_sem));
    }

    FORCEINLINE bool pop(/*out*/ T* item) noexcept
    {
        C_CALL(sem_wait(&_sem));

        // NOTE:
        //  We explicitly allow calling pop() before calling init()
        //  Thus, this assertion should go after sem_wait()
        ASSERT(_items != nullptr, "BUG: _items == nullptr. init() not called? head=%lu, tail=%lu",
            _head.load(), _tail.load());

        const size_t head = _head++;
        const size_t tail = _tail.load();
        if (UNLIKELY(head >= tail)) {
            return false;
        }

        *item = _items[head];

        return true;
    }

    FORCEINLINE void mark_push_finish() noexcept
    {
        ASSERT(_items != nullptr, "BUG: _items == nullptr. init() not called?");

        for (size_t i = 0; i < _max_consumer_threads; ++i) {
            C_CALL(sem_post(&_sem));
        }
    }

private:
    sem_t _sem;
    size_t _max_push = 0;
    size_t _max_consumer_threads = 0;
    std::atomic_size_t _head { 0 };
    std::atomic_size_t _tail { 0 };
    spin_lock _lock { false };
    T* _items = nullptr;
};


#endif  // !defined(_BDCI19_QUEUE_H_INCLUDED_)
