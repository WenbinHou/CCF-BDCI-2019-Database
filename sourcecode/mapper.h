#if !defined(_BDCI19_MAPPER_H_INCLUDED_)
#define _BDCI19_MAPPER_H_INCLUDED_

#if !defined(MAP_HUGE_2MB)
#define MAP_HUGE_2MB    (21 << MAP_HUGE_SHIFT)
#endif

#if !defined(MAP_HUGE_1GB)
#define MAP_HUGE_1GB    (30 << MAP_HUGE_SHIFT)
#endif


#define MMAP_ALIGNMENT          (1024 * 4)  // Align to 4 KiB boundary
#define MMAP_ALIGN_DOWN(_Ptr_)  ((void*)((uintptr_t)(_Ptr_) & ~(uintptr_t)(MMAP_ALIGNMENT - 1)))
#define MMAP_ALIGN_UP(_Ptr_)    ((void*)MMAP_ALIGN_DOWN((uintptr_t)(_Ptr_) + MMAP_ALIGNMENT - 1))


struct mapped_file_part_overlapped_t
{
    void* ptr;
    uint64_t file_offset;  // TODO: optimize this out to save memory
    uint32_t desired_size;
    uint32_t map_size;

    [[nodiscard]] FORCEINLINE bool is_first() const noexcept { return file_offset == 0; }
    [[nodiscard]] FORCEINLINE bool is_last() const noexcept { return (desired_size == map_size); }
};



FORCEINLINE uintptr_t __mapper_initialize() noexcept
{
    void* const ptr = mmap(
        nullptr,
        MMAP_ALIGNMENT,
        PROT_READ | PROT_WRITE,
        MAP_PRIVATE | MAP_ANONYMOUS,
        -1,
        0);
    if (UNLIKELY(ptr == MAP_FAILED)) {
        PANIC("mmap() failed. errno = %d (%s)", errno, strerror(errno));
    }

    uintptr_t __mmap_ptr = (uintptr_t)MMAP_ALIGN_DOWN(ptr) - (uintptr_t)1024 * 1024 * 1024 * 1024;  // subtract 1 TB in VA
    DEBUG("__mmap_ptr: %p", (void*)__mmap_ptr);

    C_CALL(munmap(ptr, MMAP_ALIGNMENT));

    return __mmap_ptr;
}

inline std::atomic_uintptr_t __mmap_ptr { __mapper_initialize() };


FORCEINLINE void* mmap_allocate(const size_t size) noexcept
{
    const size_t aligned_size = (size_t)MMAP_ALIGN_UP(size);
    void* const fixed_address = (void*)(__mmap_ptr -= aligned_size);
    void* const ptr = mmap(
        fixed_address,
        size,
        PROT_READ | PROT_WRITE,
        MAP_FIXED | MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE,
        -1,
        0);
    if (UNLIKELY(ptr == MAP_FAILED)) {
        PANIC("mmap() failed. errno = %d (%s)", errno, strerror(errno));
    }
    ASSERT(ptr == fixed_address, "BUG: expect %p == %p", ptr, fixed_address);
    return ptr;
}


FORCEINLINE void* mmap_parallel(
    const size_t size,
    const uint32_t num_threads,
    const int prot = (PROT_READ | PROT_WRITE),
    const int flags = (MAP_PRIVATE | MAP_ANONYMOUS),
    const int fd = -1) noexcept
{
    const size_t aligned_size = (size_t)MMAP_ALIGN_UP(size);
    void* const fixed_address = (void*)(__mmap_ptr -= aligned_size);

    constexpr const uint32_t MAX_MMAP_STEP_SIZE = 1048576 * 16;  // limit max size per mmap()
    constexpr const uint32_t MIN_MMAP_STEP_SIZE = 1024 * 64;  // limit min size per mmap()
    static_assert(MAX_MMAP_STEP_SIZE % MMAP_ALIGNMENT == 0);
    static_assert(MIN_MMAP_STEP_SIZE % MMAP_ALIGNMENT == 0);

    ASSERT(num_threads > 0, "BUG: num_threads == 0");
    size_t step_size = (size_t)MMAP_ALIGN_UP(size / num_threads);
    uint32_t real_num_threads = num_threads;
    if (step_size < MIN_MMAP_STEP_SIZE) {
        step_size = MIN_MMAP_STEP_SIZE;
        real_num_threads = (size + MIN_MMAP_STEP_SIZE - 1) / MIN_MMAP_STEP_SIZE;
    }
    else if (step_size > MAX_MMAP_STEP_SIZE) {
        step_size = MAX_MMAP_STEP_SIZE;
        // real_num_threads not changed
    }

    DEBUG("mmap_parallel(): fd=%d, size=%lu, step_size=%lu, real_num_threads=%u, ptr=%p",
          fd, size, step_size, real_num_threads, fixed_address);

    std::atomic_size_t shared_offset { 0 };
    #pragma omp parallel num_threads(real_num_threads)
    {
        while (true) {
            const size_t curr_offset = shared_offset.fetch_add(step_size);
            if (curr_offset >= size) break;

            const size_t curr_size = std::min<size_t>(step_size, size - curr_offset);
            void* const curr_address = (void*)((uintptr_t)fixed_address + curr_offset);
            void* const ptr = mmap(
                curr_address,
                curr_size,
                prot,
                MAP_FIXED | MAP_POPULATE | flags,
                fd,
                curr_offset);
            if (UNLIKELY(ptr == MAP_FAILED)) {
                PANIC("mmap() failed. errno = %d (%s)", errno, strerror(errno));
            }
            ASSERT(ptr == curr_address, "BUG: expect %p == %p", ptr, curr_address);
            DEBUG("mmap_parallel(): fd=%d, size=%lu, step_size=%lu, num_threads=%u, offset=%lu -> %p",
                fd, size, step_size, num_threads, curr_offset, curr_address);

            std::this_thread::yield();
        }
    }

    DEBUG("mmap_parallel() done: fd=%d, size=%lu, step_size=%lu, num_threads=%u, ptr=%p",
        fd, size, step_size, num_threads, fixed_address);
    return fixed_address;
}



FORCEINLINE void* my_mmap(
    const size_t size,
    const int prot,
    const int flags,
    const int fd,
    const off_t offset) noexcept
{
    const size_t aligned_size = (size_t)MMAP_ALIGN_UP(size);
    void* const fixed_address = (void*)(__mmap_ptr -= aligned_size);

    void* const ptr = mmap(fixed_address, size, prot, MAP_FIXED | flags, fd, offset);
    if (UNLIKELY(ptr == MAP_FAILED)) {
        PANIC("mmap() failed. errno = %d (%s)", errno, strerror(errno));
    }
    ASSERT(ptr == fixed_address, "BUG: expect %p == %p", ptr, fixed_address);

    return ptr;
}


template<uint32_t _PartBodySize, uint32_t _PartOverlap>
FORCEINLINE void mmap_file_overlapped(
    /*in*/ const uint32_t num_threads,
    /*in*/ const int fd,
    /*in*/ const uint64_t total_size,
    /*inout*/ mpmc_queue<mapped_file_part_overlapped_t>& queue)
{
    DEBUG("mmap_file_overlapped(): fd=%d", fd);

    ASSERT(num_threads > 0, "BUG: num_threads == 0");

    std::atomic_uint64_t shared_offset { 0 };

    #pragma omp parallel num_threads(num_threads)
    {
        while (true) {
            const uint64_t off = shared_offset.fetch_add(_PartBodySize);
            if (off >= total_size) {
                break;
            }

            mapped_file_part_overlapped_t part;
            part.file_offset = off;
            if (UNLIKELY(off + _PartBodySize + _PartOverlap >= total_size)) {  // this is last
                part.desired_size = total_size - off;
                part.map_size = total_size - off;
            }
            else {
                part.desired_size = _PartBodySize;
                part.map_size = _PartBodySize + _PartOverlap;
            }

            part.ptr = my_mmap(
                part.map_size,
                PROT_READ,
                MAP_PRIVATE | MAP_POPULATE,
                fd,
                off);
            ASSERT(part.ptr != nullptr, "BUG");
            ASSERT(part.ptr != MAP_FAILED, "BUG");

            DEBUG("mmap_file_overlapped(): fd=%d mapped to %p (total_size=%lu, offset=%lu, _DesiredSize=%u, map_size=%u)",
                fd, part.ptr, total_size, off, part.desired_size, part.map_size);
            queue.push(part);
        }
    }

    DEBUG("mmap_file_overlapped() done: fd=%d", fd);
}


#endif  // !defined(_BDCI19_MAPPER_H_INCLUDED_)
