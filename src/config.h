#if !defined(_BDCI19_CONFIG_H_INCLUDED_)
#define _BDCI19_CONFIG_H_INCLUDED_

#if !defined(MAKE_FASTEST)
//#define MAKE_FASTEST
#endif

#if defined(MAKE_FASTEST)
#define ENABLE_ASSERTION                    0
#define ENABLE_LOGGING_TRACE                0
#define ENABLE_LOGGING_DEBUG                0
#define ENABLE_LOGGING_INFO                 0
#else  // !defined(MAKE_FASTEST)
#define ENABLE_ASSERTION                    0
#define ENABLE_LOGGING_TRACE                0
#define ENABLE_LOGGING_DEBUG                1
#define ENABLE_LOGGING_INFO                 1
#endif


// Do we use spin_lock in bounded_bag<T> and mpmc_queue<T>?
//  0 - Use std::mutex
//  1 - Use spin_lock
#define ENABLE_QUEUE_USE_SPIN_LOCK          1


// Do we pin worker and loader threads to its corresponding CPU core?
//  0 - Don't pin
//  1 - Pin
#define ENABLE_PIN_THREAD_TO_CPU            1


// Do we try to use SCHED_FIFO for better performance?
//  1 - Try to set scheduler to SCHED_FIFO (fall back to not changed (SCHED_OTHER) if failed)
//  0 - Don't try to set scheduler to SCHED_FIFO
#define ENABLE_ATTEMPT_SCHED_FIFO           0
#if ENABLE_ATTEMPT_SCHED_FIFO
#define CONFIG_SCHED_FIFO_LOADER_NICE       (0)
#define CONFIG_SCHED_FIFO_WORKER_NICE       (1)
#define CONFIG_SCHED_FIFO_PWRITE_NICE       (CONFIG_SCHED_FIFO_WORKER_NICE)
#define CONFIG_SCHED_FIFO_UNLOADER_NICE     (CONFIG_SCHED_FIFO_LOADER_NICE)
#endif


// How many byte per mmap() when loading original text files?
// Note, to deal with unaligned line-breaks, we have to overlap a little between each call
#define CONFIG_PART_OVERLAPPED_SIZE         (4096U)
#define CONFIG_CUSTOMER_PART_BODY_SIZE      (1048576U * 8 - CONFIG_PART_OVERLAPPED_SIZE)
#define CONFIG_ORDERS_PART_BODY_SIZE        (1048576U * 16 - CONFIG_PART_OVERLAPPED_SIZE)
#define CONFIG_LINEITEM_PART_BODY_SIZE      (1048576U * 32 - CONFIG_PART_OVERLAPPED_SIZE)

// Number of buffers when we load the 3 text files
#define CONFIG_LOAD_BUFFER_COUNT            (3)


#endif  // !defined(_BDCI19_CONFIG_H_INCLUDED_)
