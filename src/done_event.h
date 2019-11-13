#if !defined(_BDCI19_DONE_EVENT_H_INCLUDED_)
#define _BDCI19_DONE_EVENT_H_INCLUDED_

class done_event
{
public:
    DISABLE_COPY_MOVE_CONSTRUCTOR(done_event);
    done_event() noexcept = default;

    __always_inline
    void wait_done() noexcept
    {
        if (is_done()) return;

        _sem.wait();
    }

    [[nodiscard]]
    __always_inline
    bool is_done() const noexcept
    {
        return (_sem.approx_value() > 0);
    }

    __always_inline
    void mark_done() noexcept
    {
        if (is_done()) return;

        _sem.post(INT_MAX);
        std::atomic_thread_fence(std::memory_order_seq_cst);
    }

private:
    process_shared_semaphore _sem { 0 };
};

#endif  // !defined(_BDCI19_DONE_EVENT_H_INCLUDED_)
