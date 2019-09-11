#include "common.h"

void open_file_mapping(
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
        MAP_PRIVATE /* | MAP_HUGETLB | MAP_HUGE_2MB*/,  // don't use MAP_POPULATE: we do pre-fault later
        fd,
        0);
    if (UNLIKELY(mapped->iov_base == nullptr)) {
        PANIC("mmap() failed. errno = %d (%s)", errno, strerror(errno));
    }
    DEBUG("%s mapped to %p (size: %" PRIu64 ")", file, mapped->iov_base,(uint64_t)mapped->iov_len);

    C_CALL(close(fd));
}


std::thread preload_files(
    const char* const file_customer,
    const char* const file_orders,
    const char* const file_lineitem,
    /*out*/ iovec* const mapped_customer,
    /*out*/ iovec* const mapped_orders,
    /*out*/ iovec* const mapped_lineitem)
{
    open_file_mapping(file_customer, mapped_customer);
    open_file_mapping(file_orders, mapped_orders);
    open_file_mapping(file_lineitem, mapped_lineitem);

    const auto thr_prefault = [](iovec mapped_customer, iovec mapped_orders, iovec mapped_lineitem) {

#if ENABLE_PROFILING
        auto start_time = std::chrono::steady_clock::now();
#endif
        preload_memory_range(mapped_customer);

        //preload_memory_range(mapped_lineitem);

#if ENABLE_PROFILING
        auto end_time = std::chrono::steady_clock::now();
        std::chrono::duration<double > sec = end_time - start_time;
        DEBUG("pre-fault customer: %.3lf sec", sec.count());
        start_time = std::chrono::steady_clock::now();
#endif
    };
    return std::thread(thr_prefault, *mapped_customer, *mapped_orders, *mapped_lineitem);
}
