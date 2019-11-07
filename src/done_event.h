#if !defined(_BDCI19_DONE_EVENT_H_INCLUDED_)
#define _BDCI19_DONE_EVENT_H_INCLUDED_

class done_event
{
public:
    DISABLE_COPY_MOVE_CONSTRUCTOR(done_event);

    FORCEINLINE done_event() noexcept = default;

    FORCEINLINE void wait_done() noexcept
    {
        if (is_done()) return;

        std::unique_lock<std::mutex> lock(_mutex);
        while (!is_done()) {
            _cond.wait(lock);
        }
    }

    [[nodiscard]]
    FORCEINLINE bool is_done() noexcept
    {
        return ((bool)_done.load());
    }

    FORCEINLINE void mark_done() noexcept
    {
        if (is_done()) return;

        {
            std::unique_lock<std::mutex> lock(_mutex);
            _done.store(/*true*/1);
        }
        _cond.notify_all();
    }

private:
    std::mutex _mutex { };
    std::condition_variable _cond { };
    std::atomic_uint32_t _done { /*false*/0 };
};

#endif  // !defined(_BDCI19_DONE_EVENT_H_INCLUDED_)
