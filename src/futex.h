#if !defined(_BDCI19_FUTEX_H_INCLUDED_)
#define _BDCI19_FUTEX_H_INCLUDED_


#if !defined(__NR_futex)
#define __NR_futex 202
#else
static_assert(__NR_futex == 202, "Unexpected __NR_futex");
#endif


__always_inline
int __futex(int* uaddr, int futex_op, int val, const struct timespec* timeout, int* uaddr2, int val3)
{
    return (int)syscall(__NR_futex, uaddr, futex_op, val, timeout, uaddr2, val3);
}

__always_inline
int __futex(int* uaddr, int futex_op, int val, uint32_t val2, int* uaddr2, int val3)
{
    return __futex(uaddr, futex_op, val, (const struct timespec*)(uintptr_t)val2, uaddr2, val3);
}

template<uint32_t _PrivateFlag>
__always_inline
int __futex_wait(int* uaddr, int val)
{
    // Tests (*uaddr == val)
    // If so, then sleeps waiting for a FUTEX_WAKE operation on the futex word.
    const int res =  __futex(uaddr, FUTEX_WAIT | _PrivateFlag, val, nullptr, nullptr, 0);

    return res;
}

template<uint32_t _PrivateFlag>
__always_inline
int __futex_wake(int* uaddr, int nr_waiter)
{
    const int res = __futex(uaddr, FUTEX_WAKE | _PrivateFlag, nr_waiter, nullptr, nullptr, 0);
    CHECK(res >= 0);
    return res;
}


#endif  // !defined(_BDCI19_FUTEX_H_INCLUDED_)
