#include "common.h"

//==============================================================================
// Structures
//==============================================================================
struct mapped_file_part_overlapped_t
{
    uint64_t file_offset;
    uint32_t desired_size;
    uint32_t map_size;
    int fd;

    [[nodiscard]] __always_inline bool is_first() const noexcept { return file_offset == 0; }
    [[nodiscard]] __always_inline bool is_last() const noexcept { return (desired_size == map_size); }
};

struct order_t
{
    std::make_unsigned_t<date_t> orderdate : 12;
    uint8_t mktid : 4;
};
static_assert(sizeof(order_t) == 2);


//==============================================================================
// Global Variables
//==============================================================================
namespace
{
    constexpr const uint32_t TXT_MAPPING_BUFFER_SIZE = std::max({
        CONFIG_CUSTOMER_PART_BODY_SIZE + CONFIG_PART_OVERLAPPED_SIZE,
        CONFIG_ORDERS_PART_BODY_SIZE + CONFIG_PART_OVERLAPPED_SIZE,
        CONFIG_LINEITEM_PART_BODY_SIZE + CONFIG_PART_OVERLAPPED_SIZE
    });

    void* g_txt_mapping_buffer_start_ptr = nullptr;  // [CONFIG_LOAD_TXT_BUFFER_COUNT][TXT_MAPPING_BUFFER_SIZE]
    mapped_file_part_overlapped_t g_txt_mapping_buffers[CONFIG_LOAD_TXT_BUFFER_COUNT];

    spsc_bounded_bag<index32_t, CONFIG_LOAD_TXT_BUFFER_COUNT> g_txt_mapping_bag { };

    spsc_queue<index32_t, CONFIG_LOAD_TXT_BUFFER_COUNT> g_customer_mapping_queue { };
    spsc_queue<index32_t, CONFIG_LOAD_TXT_BUFFER_COUNT> g_orders_mapping_queue { };
    spsc_queue<index32_t, CONFIG_LOAD_TXT_BUFFER_COUNT> g_lineitem_mapping_queue { };

    uint32_t g_max_custkey = 0;
    uint32_t g_max_orderkey = 0;

    posix_shm_t<uint8_t> g_custkey_to_mktid { };
    posix_shm_t<order_t> g_orderkey_to_order { };
    posix_shm_t<uint32_t> g_orderkey_to_custkey { };

    void* g_items_buffer_1_start_ptr = nullptr;  // [index_tls_buffer_count][CONFIG_INDEX_TLS_BUFFER_SIZE_1]
    void* g_items_buffer_2_start_ptr = nullptr;  // [index_tls_buffer_count][CONFIG_INDEX_TLS_BUFFER_SIZE_2]

    std::atomic_uint64_t* g_buckets_endoffset_1 = nullptr;  // [g_shared->total_buckets]
    std::atomic_uint64_t* g_buckets_endoffset_2 = nullptr;  // [g_shared->total_buckets]
}



template<uint32_t _PartBodySize, uint32_t _PartOverlap, bool _ClearPageCache, size_t _LoadBufferCount>
static void load_file_overlapped(
    /*in*/ [[maybe_unused]] sync_barrier& barrier,
    /*inout*/ std::atomic_size_t& shared_offset,  // it should be 0!
    /*in*/ [[maybe_unused]] const uint32_t tid,
    /*in*/ const size_t size,
    /*in*/ const int fd,
    /*inout*/ spsc_queue<index32_t, _LoadBufferCount>& queue,
    const uint64_t max_clear_cache_offset)
{
    DEBUG("[loader:%u] load_file_overlapped() starts: fd=%d", tid, fd);

    ASSERT(fd > 0);

#if ENABLE_ASSERTION
    barrier.run_once_and_sync([&]() {
        ASSERT(queue.capacity() > 0);
        ASSERT(shared_offset.load() == 0);
    });
#endif

    while (true) {
        const uint64_t off = shared_offset.fetch_add(_PartBodySize);
        if (off >= size) {
            queue.mark_push_finish(/*max_consumer_threads=1*/);
            break;
        }

        index32_t part_index;
        g_txt_mapping_bag.take(&part_index);
        ASSERT(part_index < CONFIG_LOAD_TXT_BUFFER_COUNT);
        mapped_file_part_overlapped_t& part = g_txt_mapping_buffers[part_index];

        ASSERT(g_txt_mapping_buffer_start_ptr != nullptr);
        void* const ptr = (void*)((uintptr_t)g_txt_mapping_buffer_start_ptr + (uintptr_t)part_index * TXT_MAPPING_BUFFER_SIZE);

        [[maybe_unused]] uint64_t clear_cache_file_offset = 0;
        [[maybe_unused]] uint64_t clear_cache_size = 0;
        if constexpr (_ClearPageCache) {
            if (part.fd == fd && off < max_clear_cache_offset) {
                clear_cache_file_offset = part.file_offset;
                clear_cache_size = part.desired_size;
            }
            part.fd = fd;
        }

        part.file_offset = off;
        if (__unlikely(off + _PartBodySize + _PartOverlap >= size)) {  // this is last
            part.desired_size = size - off;
            part.map_size = size - off;
        }
        else {
            part.desired_size = _PartBodySize;
            part.map_size = _PartBodySize + _PartOverlap;
        }

        void* const return_ptr = mmap(
            ptr,
            part.map_size,
            PROT_READ,
            MAP_FIXED | MAP_PRIVATE | MAP_POPULATE,
            fd,
            off);
        CHECK(return_ptr != MAP_FAILED);
        ASSERT(return_ptr == ptr);

        TRACE("[loader:%u] load_file_overlapped(): fd=%d mapped to %p (size=%lu, offset=%lu, _DesiredSize=%u, map_size=%u)",
            tid, fd, ptr, size, off, part.desired_size, part.map_size);
        queue.push(part_index);

        if constexpr (_ClearPageCache) {
            if (clear_cache_size > 0) {
                __fadvice_dont_need(fd, clear_cache_file_offset, clear_cache_size);
            }
        }

        //C_CALL(sched_yield());
    }

    DEBUG("[loader:%u] load_file_overlapped() done: fd=%d", tid, fd);
}


template<char _Delimiter>
__always_inline
uint32_t __parse_u32(const char* s) noexcept
{
    uint32_t result = 0;
    do {
        ASSERT(*s >= '0' && *s <= '9', "Unexpected char: %c", *s);
        result = result * 10 + (*s - '0');
        ++s;
    } while (*s != _Delimiter);
    return result;
}

__always_inline
constexpr uint32_t __u32_length(uint32_t n) noexcept
{
    uint32_t len = 0;
    do {
        ++len;
        n /= 10;
    } while (n > 0);
    return len;
}
static_assert(__u32_length(0) == 1);
static_assert(__u32_length(1) == 1);
static_assert(__u32_length(2) == 1);
static_assert(__u32_length(9) == 1);
static_assert(__u32_length(10) == 2);
static_assert(__u32_length(99) == 2);
static_assert(__u32_length(100) == 3);
static_assert(__u32_length(999) == 3);
static_assert(__u32_length(1000) == 4);


void fn_loader_thread_create_index([[maybe_unused]] const uint32_t tid) noexcept
{
    DEBUG("[loader:%u] fn_loader_thread_create_index() starts", tid);

    // Load customer file to memory
    {
#if ENABLE_SHM_CACHE_TXT
        // Do nothing
        ASSERT(g_customer_shm.shmid >= 0);
        ASSERT(g_customer_shm.ptr != nullptr);

#else
        load_file_overlapped<
                CONFIG_CUSTOMER_PART_BODY_SIZE,
                CONFIG_PART_OVERLAPPED_SIZE,
                /*_ClearPageCache*/false>(
            g_shared->loader_sync_barrier,
            g_shared->customer_file_shared_offset,
            tid,
            g_customer_file.file_size,
            g_customer_file.fd,
            g_customer_mapping_queue,
            /*dummy*/ 0);
#endif
    }


    // Load orders file to memory
    {
#if ENABLE_SHM_CACHE_TXT
        // Do nothing
        ASSERT(g_orders_shm.shmid >= 0);
        ASSERT(g_orders_shm.ptr != nullptr);

#else
        uint64_t clear_size = std::min<uint64_t>(
            g_orders_file.file_size,
            sizeof(uint32_t) * g_max_orderkey * 6);
        //TODO: const uint32_t free_mem = mem_get_free_bytes();
        // Make use of free memory to reduce page cache clearing?

        load_file_overlapped<
            CONFIG_ORDERS_PART_BODY_SIZE,
            CONFIG_PART_OVERLAPPED_SIZE,
            /*_ClearPageCache*/true>(
            g_shared->loader_sync_barrier,
            g_shared->orders_file_shared_offset,
            tid,
            g_orders_file.file_size,
            g_orders_file.fd,
            g_orders_mapping_queue,
            clear_size);
#endif
    }


    // Load lineitem file to memory
    {
#if ENABLE_SHM_CACHE_TXT
        // Do nothing
        ASSERT(g_lineitem_shm.shmid >= 0);
        ASSERT(g_lineitem_shm.ptr != nullptr);

#else
        load_file_overlapped<
            CONFIG_LINEITEM_PART_BODY_SIZE,
            CONFIG_PART_OVERLAPPED_SIZE,
            /*_ClearPageCache*/false>(
            g_shared->loader_sync_barrier,
            g_shared->lineitem_file_shared_offset,
            tid,
            g_lineitem_file.file_size,
            g_lineitem_file.fd,
            g_lineitem_mapping_queue,
            /*dummy*/ 0);
#endif
    }


    // Save mktsegment to index
    {
        if (tid == 0) {
            ASSERT(g_shared->mktid_count > 0);

            std::string buf;
            buf.reserve(64);
            buf += (char)g_shared->mktid_count;
            for (uint8_t mktid = 0; mktid < g_shared->mktid_count; ++mktid) {
                buf += (char)g_shared->all_mktsegments[mktid].length;
                buf += std::string_view(g_shared->all_mktsegments[mktid].name, g_shared->all_mktsegments[mktid].length);
            }

            const int fd = C_CALL(openat(
                g_index_directory_fd,
                "mktsegment",
                O_RDWR | O_CLOEXEC | O_CREAT | O_EXCL,
                0666));
            size_t cnt = C_CALL(pwrite(fd, buf.data(), buf.length(), 0));
            CHECK(cnt == buf.length());

            C_CALL(close(fd));
        }
    }
}



static void worker_load_customer_multi_part([[maybe_unused]] const uint32_t tid) noexcept
{
#if ENABLE_SHM_CACHE_TXT
    std::atomic_uint64_t& shared_offset = g_shared->customer_file_shared_offset;
    #if ENABLE_ASSERTION
    g_shared->worker_sync_barrier.run_once_and_sync([&]() {
        ASSERT(shared_offset.load() == 0);
    });
    #endif

    ASSERT(g_customer_shm.shmid >= 0);

    ASSERT(g_customer_shm.size_in_byte == g_customer_file.file_size);
    const uint64_t file_size = g_customer_shm.size_in_byte;

    void* const base_ptr = g_customer_shm.ptr;
    ASSERT(base_ptr != nullptr);

#endif

    while (true) {

#if ENABLE_SHM_CACHE_TXT
        const uint64_t off = shared_offset.fetch_add(CONFIG_CUSTOMER_PART_BODY_SIZE);
        if (off >= file_size) break;

        mapped_file_part_overlapped_t part; // NOLINT(cppcoreguidelines-pro-type-member-init,hicpp-member-init)
        part.file_offset = off;
        if (__unlikely(off + CONFIG_CUSTOMER_PART_BODY_SIZE + CONFIG_PART_OVERLAPPED_SIZE >= file_size)) {  // this is last
            part.desired_size = file_size - off;
            part.map_size = file_size - off;
        }
        else {
            part.desired_size = CONFIG_CUSTOMER_PART_BODY_SIZE;
            part.map_size = CONFIG_CUSTOMER_PART_BODY_SIZE + CONFIG_PART_OVERLAPPED_SIZE;
        }

        void* const ptr = (void*)((uintptr_t)base_ptr + off);

#else  // !ENABLE_SHM_CACHE_TXT
        index32_t part_index;
        if (!g_customer_mapping_queue.pop(&part_index)) break;
        ASSERT(part_index < CONFIG_LOAD_TXT_BUFFER_COUNT);
        const mapped_file_part_overlapped_t& part = g_txt_mapping_buffers[part_index];

        ASSERT(g_txt_mapping_buffer_start_ptr != nullptr);
        void* const ptr = (void*)((uintptr_t)g_txt_mapping_buffer_start_ptr + (uintptr_t)part_index * TXT_MAPPING_BUFFER_SIZE);
#endif
        const char* p = (const char*)ptr;
        const char* end = p + part.desired_size;

        // Get beginning custkey -> last_custkey
        uint32_t last_custkey;
        uint32_t last_custkey_length;
        if (!part.is_first()) {
            while (*p != '\n') ++p;
            ++p;  // skip '\n'

            last_custkey = __parse_u32<'|'>(p);
            if (last_custkey % 3 == 0) {
                while (*p != '\n') ++p;
                ++p;  // skip '\n'
                ++last_custkey;
            }
            else if (last_custkey % 3 == 2) {
                while (*p != '\n') ++p;
                ++p;  // skip '\n'
                ++last_custkey;

                while (*p != '\n') ++p;
                ++p;  // skip '\n'
                ++last_custkey;
            }
            else {  // last_custkey % 3 == 1
                // Do nothing
            }

            last_custkey_length = __u32_length(last_custkey);
        }
        else {  // part.is_first()
            last_custkey = 1;
            last_custkey_length = 1;
        }
        ASSERT(last_custkey_length > 0);

        ASSERT(last_custkey % 3 == 1);
        ASSERT(last_custkey > 0);
        ASSERT(last_custkey <= g_max_custkey, "Unexpected last_custkey (too large): %u", last_custkey);
        --last_custkey;

        if (!part.is_last()) {
            while (*end != '\n') ++end;
            ++end;  // skip '\n'

            const uint32_t to_custkey = __parse_u32<'|'>(end);
            if (to_custkey % 3 == 1) {
                while (*end != '\n') ++end;
                ++end;  // skip '\n'
                while (*end != '\n') ++end;
                ++end;  // skip '\n'
            }
            else if (to_custkey % 3 == 2) {
                while (*end != '\n') ++end;
                ++end;  // skip '\n'
            }
            else {  // to_custkey % 3 == 0
                // Do nothing
            }
        }

        TRACE("[%u] load customer: [%p, %p)", tid, p, end);
#if ENABLE_ASSERTION
        ++g_shared->customer_file_loaded_parts;
#endif

        uint32_t write_offset = last_custkey / 3;
        while (p < end) {
            ASSERT(last_custkey % 3 == 0);
            uint8_t write_value = 0;

            #pragma GCC unroll 3
            for (uint32_t inner = 0; inner < 3; ++inner) {
                if (__unlikely(p >= end)) {
                    ASSERT(p == end);
                    break;
                }

                const uint32_t custkey = ++last_custkey;
                ASSERT(custkey >= 1);
                ASSERT(custkey <= g_max_custkey, "Unexpected custkey (too large): %u", custkey);
#if ENABLE_ASSERTION
                const char* tmp_p = p;
                uint32_t tmp_custkey = 0;
                while (*tmp_p != '|') {
                    ASSERT(*tmp_p >= '0' && *tmp_p <= '9', "Unexpected char: %c", *tmp_p);
                    tmp_custkey = tmp_custkey * 10 + (*tmp_p - '0');
                    ++tmp_p;
                }
                ASSERT(custkey == tmp_custkey);
#endif
                p += last_custkey_length;
                if (__unlikely(*p != '|')) {
                    ++p;
                    ASSERT(*p == '|', "custkey: %u, *p = %c", custkey, *p);
                    ++last_custkey_length;
                }
                ++p;  // skip '|'

                const char* const mktsegment_start = p;
                while (*p != '\n') {
                    ++p;
                }
                const char* const mktsegment_end = p;
                ++p;  // skip '\n'

                //std::string_view mktsegment_view(mktsegment_start, mktsegment_end - mktsegment_start);

                const auto try_find_mktsegment = [&]() -> bool {
                    for (int8_t mktid = g_shared->mktid_count - 1; mktid >= 0; --mktid) {
                        if (g_shared->all_mktsegments[mktid].name[0] != *mktsegment_start) continue;
                        //if (std::string_view(g_shared->all_mktsegments[mktid].name, g_shared->all_mktsegments[mktid].length) == mktsegment_view) {
                            ASSERT(mktid < (1 << 4));
                            //write_value |= mktid << (inner * 4);
                            write_value = write_value * 5 + mktid;
                            return true;
                        //}
                    }
                    return false;
                };

                if (!try_find_mktsegment()) {
                    std::unique_lock<decltype(g_shared->all_mktsegments_insert_mutex)> lock(
                        g_shared->all_mktsegments_insert_mutex);
                    if (!try_find_mktsegment()) {
                        const uint32_t new_mktid = g_shared->mktid_count++;
                        std::atomic_thread_fence(std::memory_order_seq_cst);
                        ASSERT(new_mktid < (1 << 4));
                        //write_value |= new_mktid << (inner * 4);
                        write_value = write_value * 5 + new_mktid;

                        g_shared->all_mktsegments[new_mktid].length = (uint8_t)(mktsegment_end - mktsegment_start);
                        strncpy(
                            g_shared->all_mktsegments[new_mktid].name,
                            mktsegment_start,
                            mktsegment_end - mktsegment_start);
                        INFO("found new mktsegment: custkey = %u, %.*s -> %u",
                             custkey,
                             (int)g_shared->all_mktsegments[new_mktid].length,
                             g_shared->all_mktsegments[new_mktid].name,
                             new_mktid);

                        g_shared->total_buckets = g_shared->mktid_count * BUCKETS_PER_MKTID;
                        g_shared->buckets_per_holder = __div_up(g_shared->total_buckets, CONFIG_INDEX_HOLDER_COUNT);
                    }
                }
            }

            //ASSERT(last_custkey % 3 == 0);
            ASSERT(write_offset == (last_custkey - 1) / 3);
            g_custkey_to_mktid.ptr[write_offset++] = write_value;
        }

#if ENABLE_SHM_CACHE_TXT
#else  // !ENABLE_SHM_CACHE_TXT
        g_txt_mapping_bag.return_back(part_index);
#endif  // ENABLE_SHM_CACHE_TXT
    }

    INFO("[%u] done worker_load_customer_multi_part()", tid);
}


static void worker_load_orders_multi_part([[maybe_unused]] const uint32_t tid) noexcept
{
    [[maybe_unused]] const uint8_t mktid_count = g_shared->mktid_count;

#if ENABLE_SHM_CACHE_TXT
    std::atomic_uint64_t& shared_offset = g_shared->orders_file_shared_offset;
    #if ENABLE_ASSERTION
    g_shared->worker_sync_barrier.run_once_and_sync([&]() {
        ASSERT(shared_offset.load() == 0);
    });
    #endif

    ASSERT(g_orders_shm.shmid >= 0);

    void* const base_ptr = g_orders_shm.ptr;
    ASSERT(base_ptr != nullptr);

    ASSERT(g_orders_shm.size_in_byte == g_orders_file.file_size);

    const uint64_t file_size = g_orders_shm.size_in_byte;
#endif

    while (true) {

#if ENABLE_SHM_CACHE_TXT
        const uint64_t off = shared_offset.fetch_add(CONFIG_ORDERS_PART_BODY_SIZE);
        if (off >= file_size) break;

        mapped_file_part_overlapped_t part; // NOLINT(cppcoreguidelines-pro-type-member-init,hicpp-member-init)
        part.file_offset = off;
        if (__unlikely(off + CONFIG_ORDERS_PART_BODY_SIZE + CONFIG_PART_OVERLAPPED_SIZE >= file_size)) {  // this is last
            part.desired_size = file_size - off;
            part.map_size = file_size - off;
        }
        else {
            part.desired_size = CONFIG_ORDERS_PART_BODY_SIZE;
            part.map_size = CONFIG_ORDERS_PART_BODY_SIZE + CONFIG_PART_OVERLAPPED_SIZE;
        }

        void* const ptr = (void*)((uintptr_t)base_ptr + off);

#else  // !ENABLE_SHM_CACHE_TXT
        index32_t part_index;
        if (!g_orders_mapping_queue.pop(&part_index)) break;
        ASSERT(part_index < CONFIG_LOAD_TXT_BUFFER_COUNT);
        const mapped_file_part_overlapped_t& part = g_txt_mapping_buffers[part_index];

        ASSERT(g_txt_mapping_buffer_start_ptr != nullptr);
        void* const ptr = (void*)((uintptr_t)g_txt_mapping_buffer_start_ptr + (uintptr_t)part_index * TXT_MAPPING_BUFFER_SIZE);
#endif  // ENABLE_SHM_CACHE_TXT

        const char* p = (const char*)ptr;
        const char* end = p + part.desired_size;

        if (!part.is_first()) {
            while (*p != '\n') ++p;
            ++p;
        }
        if (!part.is_last()) {
            while (*end != '\n') ++end;
            ++end;
        }

        TRACE("[%u] load orders: [%p, %p)", tid, p, end);

        // Get beginning orderkey -> last_orderkey
        uint32_t last_orderkey = 0;
        const char* tmp_p = p;
        while (*tmp_p != '|') {
            ASSERT(*tmp_p >= '0' && *tmp_p <= '9', "Unexpected char: %c", *tmp_p);
            last_orderkey = last_orderkey * 10 + (*tmp_p - '0');
            ++tmp_p;
        }
        ASSERT((last_orderkey & 0b11000) == 0);
        last_orderkey = (last_orderkey & 0b111) | ((last_orderkey >> 2) & ~0b1);
        ASSERT(last_orderkey > 0);
        ASSERT(last_orderkey <= g_max_orderkey, "Unexpected last_orderkey (too large): %u", last_orderkey);

        uint32_t last_orderkey_length = (tmp_p - p);
        ASSERT(last_orderkey_length > 0);

        --last_orderkey;

        while (p < end) {
            const uint32_t orderkey = ++last_orderkey;
            ASSERT(orderkey >= 1);
            ASSERT(orderkey <= g_max_orderkey, "Unexpected orderkey (too large): %u", orderkey);
#if ENABLE_ASSERTION
            const char* tmp_p = p;
            uint32_t tmp_orderkey = 0;
            while (*tmp_p != '|') {
                ASSERT(*tmp_p >= '0' && *tmp_p <= '9', "Unexpected char: %c", *tmp_p);
                tmp_orderkey = tmp_orderkey * 10 + (*tmp_p - '0');
                ++tmp_p;
            }
            ASSERT((tmp_orderkey & 0b11000) == 0);
            tmp_orderkey = (tmp_orderkey & 0b111) | ((tmp_orderkey >> 2) & ~0b1);
            ASSERT(orderkey == tmp_orderkey);
#endif
            p += last_orderkey_length;
            if (__unlikely(*p != '|')) {
                ++p;
                ASSERT(*p == '|', "orderkey: %u, *p = %c", orderkey, *p);
                ++last_orderkey_length;
            }
            ++p;  // skip '|'

            uint32_t custkey = 0;
            while (*p != '|') {
                ASSERT(*p >= '0' && *p <= '9', "Unexpected char: %c", *p);
                custkey = custkey * 10 + (*p - '0');
                ++p;
            }
            ++p;  // skip '|'
            ASSERT(custkey >= 1);
            ASSERT(custkey <= g_max_custkey, "Unexpected custkey (too large): %u", custkey);
            g_orderkey_to_custkey.ptr[orderkey] = custkey;

            const date_t orderdate = date_from_string<true>(p);
            ASSERT(orderdate >= MIN_TABLE_DATE);
            ASSERT(orderdate <= MAX_TABLE_DATE);
            p += 10;  // skip 'yyyy-MM-dd'

            // Save to orderkey_to_order
            ASSERT(orderdate < (1 << 12), "BUG: orderdate too large?");
#if ENABLE_ASSERTION
            g_orderkey_to_order.ptr[orderkey].orderdate = (std::make_unsigned_t<date_t>)(orderdate);
            g_orderkey_to_order.ptr[orderkey].mktid = mktid_count;  // Set a posion value
#else
            g_orderkey_to_order.ptr[orderkey] = { (std::make_unsigned_t<date_t>)(orderdate), /*dummy*/0 };
#endif

            ASSERT(*p == '\n', "Expect EOL");
            p += 1;  // skip '\n'
        }

#if ENABLE_ASSERTION
        if (part.is_last()) {
            ASSERT(last_orderkey == g_max_orderkey);
        }
#endif

#if ENABLE_SHM_CACHE_TXT
#else
        g_txt_mapping_bag.return_back(part_index);
#endif
    }

    INFO("[%u] done worker_load_orders_multi_part()", tid);
}


static void worker_load_orders_custkey_from_orderkey(
    /*in*/ [[maybe_unused]] const uint32_t tid,
    /*inout*/ std::atomic_uint64_t& shared_counter) noexcept
{
#if ENABLE_ASSERTION
    g_shared->worker_sync_barrier.run_once_and_sync([&]() {
        ASSERT(shared_counter.load() == 0);
    });
#endif

    static constexpr const uint32_t STEP_SIZE = 1048576;
    while (true) {
        const uint32_t from_orderkey = std::max<uint32_t>(shared_counter.fetch_add(STEP_SIZE), 1);
        if (from_orderkey > g_max_orderkey) break;
        const uint32_t to_orderkey = std::min<uint32_t>(from_orderkey + STEP_SIZE - 1, g_max_orderkey);

        //TODO: #pragma GCC unroll 4
        for (uint32_t orderkey = from_orderkey; orderkey <= to_orderkey; ++orderkey) {
            const uint32_t custkey = g_orderkey_to_custkey.ptr[orderkey];
            ASSERT(custkey >= 1);
            ASSERT(custkey <= g_max_custkey);

            const uint8_t mktid_embed = g_custkey_to_mktid.ptr[(custkey - 1) / 3];

            uint8_t mktid;
            if (custkey % 3 == 1) {
                mktid = mktid_embed % 5;
            }
            else if (custkey % 3 == 2) {
                mktid = (mktid_embed / 5) % 5;
            }
            else {  // custkey % 3 == 0
                mktid = mktid_embed / 25;
            }
            ASSERT(mktid < 5, "Boom! mktid %u too large", mktid);
            ASSERT(mktid < g_shared->mktid_count,
                "Expect mktid < g_shared->mktid_count (%u < %u)", mktid, g_shared->mktid_count);

            g_orderkey_to_order.ptr[orderkey].mktid = mktid;
        }
    }

    INFO("[%u] done worker_load_orders_custkey_from_orderkey()", tid);
}


void debug_check_orders()
{
    INFO("[%u] debug_check_orders() starts", g_id);

    const uint8_t mktid_count = g_shared->mktid_count;

    for (uint32_t orderkey = 1; orderkey <= g_max_orderkey; ++orderkey) {
        const order_t order = g_orderkey_to_order.ptr[orderkey];
        CHECK(order.orderdate >= MIN_TABLE_DATE);
        CHECK(order.orderdate <= MAX_TABLE_DATE);

        CHECK(order.mktid < mktid_count, "orderkey: %u, mktid: %u", orderkey, order.mktid);
    }

    INFO("[%u] done debug_check_orders()", g_id);
}




__always_inline
const char* skip_one_orderkey_in_lineitem(const char* p) noexcept
{
    uint32_t first_orderkey = (uint32_t)-1;
    while (true) {
        const char* const saved_p = p;
        uint32_t orderkey = 0;
        while (*p != '|') {
            ASSERT(*p >= '0' && *p <= '9', "Unexpected char: %c", *p);
            orderkey = orderkey * 10 + (*p - '0');
            ++p;
        }
        //++p;  // skip '|' (not necessary)
        ASSERT((orderkey & 0b11000) == 0);
        orderkey = (orderkey & 0b111) | ((orderkey >> 2) & ~0b1);
        ASSERT(orderkey > 0);
        ASSERT(orderkey <= g_max_orderkey, "Unexpected orderkey (too large): %u", orderkey);

        if (first_orderkey == (uint32_t)-1) {
            first_orderkey = orderkey;
        }
        else {
            ASSERT(orderkey >= first_orderkey, "lineitem not sorted? orderkey=%u, first_orderkey=%u", orderkey, first_orderkey);
            if (orderkey > first_orderkey) return saved_p;
        }

        while (*p != '\n') ++p;
        ++p;  // skip '\n'
    }
}


void worker_load_lineitem_multi_part(const uint32_t tid) noexcept
{
    ASSERT(g_orderkey_to_order.ptr != nullptr);
    const order_t* const orderkey_to_order = g_orderkey_to_order.ptr;

    ASSERT(g_shared->mktid_count > 0);
    const uint32_t mktid_count = g_shared->mktid_count;

    uint32_t max_orderdate_shipdate_diff = 0;


    // Allocate for g_items_buffer_1_start_ptr, g_items_buffer_2_start_ptr
    {
        ASSERT(g_shared->total_buckets > 0);
        const uint64_t index_tls_buffer_count =
            (uint64_t)g_shared->mktid_count *
            __div_up(MAX_TABLE_DATE - MIN_TABLE_DATE + 1, CONFIG_ORDERDATES_PER_BUCKET);
        DEBUG("index_tls_buffer_count: %lu", index_tls_buffer_count);

        g_items_buffer_1_start_ptr = mmap_allocate_page2m(index_tls_buffer_count * CONFIG_INDEX_TLS_BUFFER_SIZE_1);
        ASSERT(g_items_buffer_1_start_ptr != nullptr);
        DEBUG("[%u] g_items_buffer_1_start_ptr: %p", tid, g_items_buffer_1_start_ptr);

        // Allocate for g_items_buffer_2_start_ptr
        g_items_buffer_2_start_ptr = mmap_allocate_page2m(index_tls_buffer_count * CONFIG_INDEX_TLS_BUFFER_SIZE_2);
        ASSERT(g_items_buffer_2_start_ptr != nullptr);
        DEBUG("[%u] g_items_buffer_2_start_ptr: %p", tid, g_items_buffer_2_start_ptr);
    }


    // Truncate for g_endoffset_file
    {
        ASSERT(g_shared->total_buckets > 0);

        ASSERT(g_endoffset_file_1.fd > 0);
        ASSERT(g_endoffset_file_2.fd > 0);

        g_endoffset_file_1.file_size = sizeof(uint64_t) * g_shared->total_buckets;
        g_endoffset_file_2.file_size = sizeof(uint64_t) * g_shared->total_buckets;
        ASSERT(g_endoffset_file_1.file_size > 0);
        ASSERT(g_endoffset_file_2.file_size > 0);

        g_shared->worker_sync_barrier.run_once_and_sync([]() {
            C_CALL(ftruncate(g_endoffset_file_1.fd, g_endoffset_file_1.file_size));
            C_CALL(ftruncate(g_endoffset_file_2.fd, g_endoffset_file_2.file_size));
        });

        g_buckets_endoffset_1 = (std::atomic_uint64_t*)my_mmap(
            g_endoffset_file_1.file_size,
            PROT_READ | PROT_WRITE,
            MAP_SHARED | MAP_POPULATE,
            g_endoffset_file_1.fd,
            0);
        DEBUG("[%u] g_buckets_endoffset_1: %p", tid, g_buckets_endoffset_1);

        g_buckets_endoffset_2 = (std::atomic_uint64_t*)my_mmap(
            g_endoffset_file_2.file_size,
            PROT_READ | PROT_WRITE,
            MAP_SHARED | MAP_POPULATE,
            g_endoffset_file_2.fd,
            0);
        DEBUG("[%u] g_buckets_endoffset_2: %p", tid, g_buckets_endoffset_2);
    }

    // Truncate for holder files
    {
        ASSERT(g_shared->buckets_per_holder > 0);

        const uint64_t holder_1_file_size = (uint64_t)CONFIG_INDEX_SPARSE_BUCKET_SIZE_1 * g_shared->buckets_per_holder;
        while (true) {
            const uint32_t holder_id = g_shared->next_truncate_holder_1_id++;
            if (holder_id >= CONFIG_INDEX_HOLDER_COUNT) break;

            ASSERT(g_holder_files_1_fd[holder_id] > 0);
            C_CALL(ftruncate(g_holder_files_1_fd[holder_id], holder_1_file_size));

            const uint32_t begin_bucket_id = holder_id * g_shared->buckets_per_holder;
            const uint32_t end_bucket_id = (holder_id + 1) * g_shared->buckets_per_holder;
            for (uint32_t bucket_id = begin_bucket_id; bucket_id < end_bucket_id; ++bucket_id) {
                g_buckets_endoffset_1[bucket_id] = (uint64_t)CONFIG_INDEX_SPARSE_BUCKET_SIZE_1 * (bucket_id - begin_bucket_id);
            }
        }

        const uint64_t holder_2_file_size = (uint64_t)CONFIG_INDEX_SPARSE_BUCKET_SIZE_2 * g_shared->buckets_per_holder;
        while (true) {
            const uint32_t holder_id = g_shared->next_truncate_holder_2_id++;
            if (holder_id >= CONFIG_INDEX_HOLDER_COUNT) break;

            ASSERT(g_holder_files_2_fd[holder_id] > 0);
            C_CALL(ftruncate(g_holder_files_2_fd[holder_id], holder_2_file_size));

            const uint32_t begin_bucket_id = holder_id * g_shared->buckets_per_holder;
            const uint32_t end_bucket_id = (holder_id + 1) * g_shared->buckets_per_holder;
            for (uint32_t bucket_id = begin_bucket_id; bucket_id < end_bucket_id; ++bucket_id) {
                g_buckets_endoffset_2[bucket_id] = (uint64_t)CONFIG_INDEX_SPARSE_BUCKET_SIZE_2 * (bucket_id - begin_bucket_id);
            }
        }
    }


#define _CALC_START_PTR_1(_BufferIndex) \
    ((void*)((uintptr_t)g_items_buffer_1_start_ptr + (size_t)(_BufferIndex) * CONFIG_INDEX_TLS_BUFFER_SIZE_1))
#define _CALC_START_PTR_2(_BufferIndex) \
    ((void*)((uintptr_t)g_items_buffer_2_start_ptr + (size_t)(_BufferIndex) * CONFIG_INDEX_TLS_BUFFER_SIZE_2))

    iovec* const bucket_data_1 = new iovec[g_shared->total_buckets];
    for (uint32_t bucket_id = 0; bucket_id < g_shared->total_buckets; ++bucket_id) {
        bucket_data_1[bucket_id].iov_len = 0;
        bucket_data_1[bucket_id].iov_base = _CALC_START_PTR_1(bucket_id);
    }
    iovec* const bucket_data_2 = new iovec[g_shared->total_buckets];
    for (uint32_t bucket_id = 0; bucket_id < g_shared->total_buckets; ++bucket_id) {
        bucket_data_2[bucket_id].iov_len = 0;
        bucket_data_2[bucket_id].iov_base = _CALC_START_PTR_2(bucket_id);
    }


    const auto maybe_submit_for_pwrite_1 = [&](/*inout*/ iovec& vec, /*in*/ uint32_t bucket_id) {
        //ASSERT(bucket_id < index_tls_buffer_count, "bucket_id: %u", bucket_id);
        ASSERT(vec.iov_len <= CONFIG_INDEX_TLS_BUFFER_SIZE_1);
        ASSERT(vec.iov_len > 0);

        // Do pwrite
        const uint64_t file_offset = g_buckets_endoffset_1[bucket_id].fetch_add(vec.iov_len);
        // If not final writing:
        //  ASSERT(vec.iov_len == CONFIG_INDEX_TLS_BUFFER_SIZE_1);
        //  ASSERT(file_offset % CONFIG_INDEX_TLS_BUFFER_SIZE_1 == 0, "bucket_id: %u, file_offset: %lu", bucket_id, file_offset);
        const uint32_t holder_id = bucket_id / g_shared->buckets_per_holder;
        const size_t cnt = C_CALL(pwrite(
            g_holder_files_1_fd[holder_id],
            vec.iov_base,
            vec.iov_len,
            file_offset));
        CHECK(cnt == vec.iov_len);

        // Fetch new last_data
        vec.iov_len = 0;
    };

    const auto maybe_submit_for_pwrite_2 = [&](/*inout*/ iovec& vec, /*in*/ uint32_t bucket_id) {
        //ASSERT(bucket_id < index_tls_buffer_count, "bucket_id: %u", bucket_id);
        ASSERT(vec.iov_len <= CONFIG_INDEX_TLS_BUFFER_SIZE_2);
        ASSERT(vec.iov_len > 0);

        // Do pwrite
        const uint64_t file_offset = g_buckets_endoffset_2[bucket_id].fetch_add(vec.iov_len);
        const uint32_t holder_id = bucket_id / g_shared->buckets_per_holder;
        const size_t cnt = C_CALL(pwrite(
            g_holder_files_2_fd[holder_id],
            vec.iov_base,
            vec.iov_len,
            file_offset));
        CHECK(cnt == vec.iov_len);

        // Fetch new last_data
        vec.iov_len = 0;
    };




    
#if ENABLE_SHM_CACHE_TXT
    std::atomic_uint64_t& shared_offset = g_shared->lineitem_file_shared_offset;
    #if ENABLE_ASSERTION
    g_shared->worker_sync_barrier.run_once_and_sync([&]() {
        ASSERT(shared_offset.load() == 0);
    });
    #endif

    ASSERT(g_lineitem_shm.shmid >= 0);

    ASSERT(g_lineitem_shm.size_in_byte == g_lineitem_file.file_size);
    const uint64_t file_size = g_lineitem_shm.size_in_byte;

    void* const base_ptr = g_lineitem_shm.ptr;
    ASSERT(base_ptr != nullptr);
#endif  // ENABLE_SHM_CACHE_TXT

    while (true) {

#if ENABLE_SHM_CACHE_TXT
        const uint64_t off = shared_offset.fetch_add(CONFIG_LINEITEM_PART_BODY_SIZE);
        if (off >= file_size) break;

        mapped_file_part_overlapped_t part; // NOLINT(cppcoreguidelines-pro-type-member-init,hicpp-member-init)
        part.file_offset = off;
        if (__unlikely(off + CONFIG_LINEITEM_PART_BODY_SIZE + CONFIG_PART_OVERLAPPED_SIZE >= file_size)) {  // this is last
            part.desired_size = file_size - off;
            part.map_size = file_size - off;
        }
        else {
            part.desired_size = CONFIG_LINEITEM_PART_BODY_SIZE;
            part.map_size = CONFIG_LINEITEM_PART_BODY_SIZE + CONFIG_PART_OVERLAPPED_SIZE;
        }

        void* const ptr = (void*)((uintptr_t)base_ptr + off);

#else  // !ENABLE_SHM_CACHE_TXT
        index32_t part_index;
        if (!g_lineitem_mapping_queue.pop(&part_index)) break;
        ASSERT(part_index < CONFIG_LOAD_TXT_BUFFER_COUNT);
        const mapped_file_part_overlapped_t& part = g_txt_mapping_buffers[part_index];

        ASSERT(g_txt_mapping_buffer_start_ptr != nullptr);
        void* const ptr = (void*)((uintptr_t)g_txt_mapping_buffer_start_ptr + (uintptr_t)part_index * TXT_MAPPING_BUFFER_SIZE);
#endif
        const char* p = (const char*)ptr;
        const char* valid_end = p + part.desired_size;

        if (!part.is_first()) {
            while (*p != '\n') ++p;
            ++p;

            p = skip_one_orderkey_in_lineitem(p);
        }

        if (!part.is_last()) {
            while (*valid_end != '\n') ++valid_end;
            ++valid_end;

            valid_end = skip_one_orderkey_in_lineitem(valid_end);
        }

        TRACE("[%u] load lineitem: [%p, %p)", tid, p, valid_end);

        uint32_t last_items[8];
        uint32_t last_item_count;
        uint32_t last_orderkey;
        uint32_t last_bucket_id;
        date_t last_orderdate;
        date_t last_bucket_base_orderdate;


        const auto append_current_order_to_index = [&]() {
            ASSERT(last_item_count >= 2);
            ASSERT(last_item_count <= 8);

            if (last_item_count == 8) {
                iovec& vec = bucket_data_1[last_bucket_id];
                ASSERT(vec.iov_len < CONFIG_INDEX_TLS_BUFFER_SIZE_1);
                ASSERT(vec.iov_base == _CALC_START_PTR_1(last_bucket_id));
                static_assert(CONFIG_INDEX_TLS_BUFFER_SIZE_1 % (8 * sizeof(uint32_t)) == 0);

                memcpy(
                    (void*)((uintptr_t)vec.iov_base + vec.iov_len),
                    last_items,
                    8 * sizeof(uint32_t));
                vec.iov_len += 8 * sizeof(uint32_t);

                ASSERT(vec.iov_len <= CONFIG_INDEX_TLS_BUFFER_SIZE_1);
                if (vec.iov_len == CONFIG_INDEX_TLS_BUFFER_SIZE_1) {
                    maybe_submit_for_pwrite_1(vec, last_bucket_id);
                }
            }
            else {
                iovec& vec = bucket_data_2[last_bucket_id];
                ASSERT(vec.iov_len <= CONFIG_INDEX_TLS_BUFFER_SIZE_2 - CONFIG_INDEX_BUFFER_GRACE_SIZE_2);
                ASSERT(vec.iov_base == _CALC_START_PTR_2(last_bucket_id));

                memcpy(
                    (void*)((uintptr_t)vec.iov_base + vec.iov_len),
                    last_items,
                    last_item_count * sizeof(uint32_t));
                vec.iov_len += last_item_count * sizeof(uint32_t);

                ASSERT(vec.iov_len <= CONFIG_INDEX_TLS_BUFFER_SIZE_2);
                if (vec.iov_len > CONFIG_INDEX_TLS_BUFFER_SIZE_2 - CONFIG_INDEX_BUFFER_GRACE_SIZE_2) {
                    maybe_submit_for_pwrite_2(vec, last_bucket_id);
                }
            }
        };


        // Get first item
        {
            last_orderkey = 0;
            while (*p != '|') {
                ASSERT(*p >= '0' && *p <= '9', "Unexpected char: %c", *p);
                last_orderkey = last_orderkey * 10 + (*p - '0');
                ++p;
            }
            ++p;  // skip '|'
            ASSERT((last_orderkey & 0b11000) == 0);
            last_orderkey = (last_orderkey & 0b111) | ((last_orderkey >> 2) & ~0b1);
            ASSERT(last_orderkey > 0);
            ASSERT(last_orderkey < (1 << 30));
            ASSERT(last_orderkey <= g_max_orderkey, "Unexpected orderkey (too large): %u", last_orderkey);

            ASSERT(orderkey_to_order != nullptr);
            const order_t last_order = orderkey_to_order[last_orderkey];
            last_orderdate = last_order.orderdate;
            const uint8_t mktid = last_order.mktid;

            ASSERT(mktid < mktid_count, "BUG: expect mktid < mktid_count (%u < %u)", mktid, mktid_count);

            last_bucket_base_orderdate = calc_base_orderdate(last_orderdate);
            ASSERT(last_orderdate >= last_bucket_base_orderdate);
            ASSERT(last_orderdate < last_bucket_base_orderdate + CONFIG_ORDERDATES_PER_BUCKET);
            //last_orderkey_with_orderdate_diff = last_orderkey | (uint32_t)(last_orderdate - last_bucket_base_orderdate) << 30;

            last_bucket_id = calc_bucket_index(mktid, last_orderdate);

            uint32_t expend_cent = 0;
            while (*p != '.') {
                ASSERT(*p >= '0' && *p <= '9', "Unexpected char: %c", *p);
                expend_cent = expend_cent * 10 + (*p - '0');
                ++p;
            }
            ++p;  // skip '.'
            ASSERT(*p >= '0' && *p <= '9', "Unexpected char: %c", *p);
            expend_cent = expend_cent * 10 + (*(p++) - '0');
            ASSERT(*p >= '0' && *p <= '9', "Unexpected char: %c", *p);
            expend_cent = expend_cent * 10 + (*(p++) - '0');
            ASSERT(*p == '|');
            ++p;  // skip '|'
            ASSERT(expend_cent < (1 << 24), "Boom! expend_cent too large");

            const date_t shipdate = date_from_string<true>(p);
            p += 10;  // skip 'yyyy-MM-dd'
            ASSERT(shipdate > last_orderdate, "Expect shipdate > orderdate");
            ASSERT((shipdate - last_orderdate) < (1 << 7),
                "shipdate - orderdate Boom! shipdate=%d, orderdate=%d", (int32_t)shipdate, (int32_t)last_orderdate);
            ASSERT((shipdate - last_bucket_base_orderdate) < (1 << 7),
                "shipdate - orderdate Boom! shipdate=%d, base_orderdate=%d", (int32_t)shipdate, (int32_t)last_bucket_base_orderdate);
            ASSERT(shipdate >= MIN_TABLE_DATE, "Expect shipdate >= MIN_TABLE_DATE");
            ASSERT(shipdate <= MAX_TABLE_DATE, "Expect shipdate <= MAX_TABLE_DATE");
            if (__unlikely(shipdate - last_orderdate) > max_orderdate_shipdate_diff) {
                max_orderdate_shipdate_diff = shipdate - last_orderdate;
            }

            ASSERT(*p == '\n', "Expect EOL");
            p += 1;  // skip '\n'

            ASSERT(last_orderdate - last_bucket_base_orderdate < CONFIG_ORDERDATES_PER_BUCKET);
            last_items[0] = (last_orderdate - last_bucket_base_orderdate) << 30 | last_orderkey;
            last_items[1] = ((uint32_t)(shipdate - last_bucket_base_orderdate) << 24) | expend_cent;
            last_item_count = 2;
        }


        while (true) {
            uint32_t orderkey = 0;
            while (*p != '|') {
                ASSERT(*p >= '0' && *p <= '9', "Unexpected char: %c", *p);
                orderkey = orderkey * 10 + (*p - '0');
                ++p;
            }
            ++p;  // skip '|'
            ASSERT((orderkey & 0b11000) == 0);
            orderkey = (orderkey & 0b111) | ((orderkey >> 2) & ~0b1);
            ASSERT(orderkey > 0);
            ASSERT(orderkey < (1 << 30));
            ASSERT(orderkey <= g_max_orderkey, "Unexpected orderkey (too large): %u", orderkey);


            uint32_t expend_cent = 0;
            while (*p != '.') {
                ASSERT(*p >= '0' && *p <= '9', "Unexpected char: %c", *p);
                expend_cent = expend_cent * 10 + (*p - '0');
                ++p;
            }
            ++p;  // skip '.'
            ASSERT(*p >= '0' && *p <= '9', "Unexpected char: %c", *p);
            expend_cent = expend_cent * 10 + (*(p++) - '0');
            ASSERT(*p >= '0' && *p <= '9', "Unexpected char: %c", *p);
            expend_cent = expend_cent * 10 + (*(p++) - '0');
            ASSERT(*p == '|');
            ++p;  // skip '|'
            ASSERT(expend_cent < (1 << 24), "Boom! expend_cent too large");

            const date_t shipdate = date_from_string<true>(p);
            p += 10;  // skip 'yyyy-MM-dd'

            ASSERT(*p == '\n', "Expect EOL");
            p += 1;  // skip '\n'

            if (orderkey == last_orderkey) {
                ASSERT(shipdate > last_orderdate, "Expect shipdate > orderdate");
                ASSERT((shipdate - last_orderdate) < (1 << 7),
                    "shipdate - orderdate Boom! shipdate=%d, orderdate=%d", (int32_t)shipdate, (int32_t)last_orderdate);
                ASSERT((shipdate - last_bucket_base_orderdate) < (1 << 7),
                    "shipdate - orderdate Boom! shipdate=%d, base_orderdate=%d", (int32_t)shipdate, (int32_t)last_bucket_base_orderdate);
                ASSERT(shipdate >= MIN_TABLE_DATE, "Expect shipdate >= MIN_TABLE_DATE");
                ASSERT(shipdate <= MAX_TABLE_DATE, "Expect shipdate <= MAX_TABLE_DATE");

                if (__unlikely(shipdate - last_orderdate) > max_orderdate_shipdate_diff) {
                    max_orderdate_shipdate_diff = shipdate - last_orderdate;
                }

                ASSERT(last_item_count >= 2);
                ASSERT(last_item_count < 8);
                last_items[last_item_count++] = ((uint32_t)(shipdate - last_bucket_base_orderdate) << 24) | expend_cent;
            }
            else {  // orderkey != last_orderkey
                // Save current items to index buffer
                ASSERT(last_item_count >= 2);
                ASSERT(last_item_count <= 8);
                append_current_order_to_index();

                ASSERT(orderkey == last_orderkey + 1);
                last_orderkey = orderkey;

                ASSERT(orderkey_to_order != nullptr);
                const order_t last_order = orderkey_to_order[last_orderkey];
                last_orderdate = last_order.orderdate;

                const uint8_t mktid = last_order.mktid;
                ASSERT(mktid < mktid_count, "BUG: expect mktid < mktid_count (%u < %u)", mktid, mktid_count);

                last_bucket_base_orderdate = calc_base_orderdate(last_orderdate);
                ASSERT(last_orderdate >= last_bucket_base_orderdate);
                ASSERT(last_orderdate < last_bucket_base_orderdate + CONFIG_ORDERDATES_PER_BUCKET);
                //last_orderkey_with_orderdate_diff = last_orderkey | (uint32_t)(last_orderdate - last_bucket_base_orderdate) << 30;

                last_bucket_id = calc_bucket_index(mktid, last_orderdate);

                ASSERT(shipdate > last_orderdate, "Expect shipdate > orderdate");
                ASSERT((shipdate - last_orderdate) < (1 << 7),
                    "shipdate - orderdate Boom! shipdate=%d, orderdate=%d", (int32_t)shipdate, (int32_t)last_orderdate);
                ASSERT((shipdate - last_bucket_base_orderdate) < (1 << 7),
                    "shipdate - orderdate Boom! shipdate=%d, base_orderdate=%d", (int32_t)shipdate, (int32_t)last_bucket_base_orderdate);
                ASSERT(shipdate >= MIN_TABLE_DATE, "Expect shipdate >= MIN_TABLE_DATE");
                ASSERT(shipdate <= MAX_TABLE_DATE, "Expect shipdate <= MAX_TABLE_DATE");
                if (__unlikely(shipdate - last_orderdate) > max_orderdate_shipdate_diff) {
                    max_orderdate_shipdate_diff = shipdate - last_orderdate;
                }

                ASSERT(last_orderdate - last_bucket_base_orderdate < CONFIG_ORDERDATES_PER_BUCKET);
                last_items[0] = (last_orderdate - last_bucket_base_orderdate) << 30 | last_orderkey;
                last_items[1] = ((uint32_t)(shipdate - last_bucket_base_orderdate) << 24) | expend_cent;
                last_item_count = 2;
            }


            if (__unlikely(p >= valid_end)) {
                ASSERT(p == valid_end);

                // Save current items to index buffer
                append_current_order_to_index();

                break;
            }
        }

#if ENABLE_SHM_CACHE_TXT
#else
        g_txt_mapping_bag.return_back(part_index);
#endif
    }

    // For each bucket, write remaining buffer (if exists) to file
    uint32_t bucket_ids_to_shuffle[g_shared->total_buckets];
    for (uint32_t i = 0; i < g_shared->total_buckets; ++i) {
        bucket_ids_to_shuffle[i] = i;
    }
    std::mt19937 g(tid);
    std::shuffle(bucket_ids_to_shuffle, bucket_ids_to_shuffle + g_shared->total_buckets, g);
    for (uint32_t i = 0; i < g_shared->total_buckets; ++i) {
        const uint32_t bucket_id = bucket_ids_to_shuffle[i];
        iovec& vec_1 = bucket_data_1[bucket_id];
        if (vec_1.iov_len > 0) {
            maybe_submit_for_pwrite_1(vec_1, bucket_id);
        }

        iovec& vec_2 = bucket_data_2[bucket_id];
        if (vec_2.iov_len > 0) {
            maybe_submit_for_pwrite_2(vec_2, bucket_id);
        }
    }

#if ENABLE_ASSERTION
    for (uint32_t i = 0; i < g_shared->total_buckets; ++i) {
        const uint32_t bucket_id = bucket_ids_to_shuffle[i];
        //TODO: ASSERT(bucket_data_1[bucket_id].iov_len == 0);
        ASSERT(bucket_data_2[bucket_id].iov_len == 0);
    }
#endif


    // Update my max_orderdate_shipdate_diff
    {
        DEBUG("[%u] max_orderdate_shipdate_diff: %u", tid, max_orderdate_shipdate_diff);
        if (*(volatile uint32_t*)&g_shared->meta.max_shipdate_orderdate_diff < max_orderdate_shipdate_diff) {
            std::unique_lock<decltype(g_shared->meta_update_mutex)> lock(g_shared->meta_update_mutex);
            if (*(volatile uint32_t*)&g_shared->meta.max_shipdate_orderdate_diff < max_orderdate_shipdate_diff) {
                g_shared->meta.max_shipdate_orderdate_diff = max_orderdate_shipdate_diff;
            }
        }
    }

    INFO("[%u] done worker_load_lineitem_multi_part()", tid);
}



void fn_worker_thread_create_index(const uint32_t tid) noexcept
{
    DEBUG("[%u] fn_worker_thread_create_index() starts", tid);

    // Parse loaded customer table
    {
        worker_load_customer_multi_part(tid);
        g_shared->worker_sync_barrier.sync();
        if (tid == 0) {
            INFO("g_shared->total_buckets: %u", g_shared->total_buckets);
            INFO("g_shared->buckets_per_holder: %u", g_shared->buckets_per_holder);

#if ENABLE_ASSERTION
            INFO("g_shared->customer_file_loaded_parts: %lu", g_shared->customer_file_loaded_parts.load());
#endif
        }
    }


    // Parse loaded orders table
    {
        worker_load_orders_multi_part(tid);
#if ENABLE_SHM_CACHE_TXT
        ASSERT(g_orders_shm.ptr != nullptr);
        g_orders_shm.detach();  // then g_orders_shm should have been removed
        ASSERT(g_orders_shm.ptr == nullptr);

        g_shared->worker_sync_barrier.sync_and_run_once([]() {
            // Release huge pages for orders txt
            const uint64_t require_nr_2mb =
                __div_up(g_customer_file.file_size, 1024 * 1024 * 2) +
                __div_up(g_lineitem_file.file_size, 1024 * 1024 * 2) +
                CONFIG_EXTRA_HUGE_PAGES;

            const bool success = mem_set_nr_hugepages_2048kB(require_nr_2mb);
            CHECK(success, "Can't adjust nr_hugepages to %lu", require_nr_2mb);
            INFO("adjusted nr_hugepages to %lu", require_nr_2mb);

            #if ENABLE_ASSERTION
            const uint64_t actual_nr_2mb = mem_get_nr_hugepages_2048kB();
            ASSERT(actual_nr_2mb == require_nr_2mb,
                "Expect actual_nr_2mb == require_nr_2mb: %lu == %lu", actual_nr_2mb, require_nr_2mb);
            #endif

            //system("free -h");
        });

#else  // !ENABLE_SHM_CACHE_TXT
        g_shared->worker_sync_barrier.sync_and_run_once([]() {
            ASSERT(g_orders_file.fd > 0);
            ASSERT(g_orders_file.file_size > 0);

            /*
            uint64_t clear_size = std::min<uint64_t>(
                g_orders_file.file_size,
                sizeof(uint32_t) * g_max_orderkey * 6);
            //TODO: const uint32_t free_mem = mem_get_free_bytes();
            // Make use of free memory to reduce page cache clearing?

            __fadvice_dont_need(g_orders_file.fd, 0, clear_size);
            INFO("__fadvice_dont_need page cache for orders file (clear_size: %lu)", clear_size);
             */

            //system("free -h");
        });
#endif  // ENABLE_SHM_CACHE_TXT

        worker_load_orders_custkey_from_orderkey(tid, g_shared->orderkey_custkey_shared_counter);
        g_shared->worker_sync_barrier.sync();

#if ENABLE_ASSERTION
        if (tid == 0) {
            debug_check_orders();
        }
#endif
    }

    // We must do shmdt()
    // This is to save space for g_items_buffer_{1/2}_start_ptr
    {
        // Detach g_custkey_to_mktid
        g_custkey_to_mktid.detach();

        // Detach g_orderkey_to_custkey
        g_orderkey_to_custkey.detach();

        g_shared->worker_sync_barrier.sync();  // This sync is necessary
    }


    // Parse loaded lineitem table
    {
        worker_load_lineitem_multi_part(tid);
        g_shared->worker_sync_barrier.sync();  // TODO: comment this out?

#if ENABLE_ASSERTION
        if (tid == 0) {
            uint64_t max_bucket_size_1 = 0;
            uint64_t max_bucket_size_2 = 0;

            for (uint32_t bucket_id = 0; bucket_id < g_shared->total_buckets; ++bucket_id) {
                const uint32_t holder_id = bucket_id / g_shared->buckets_per_holder;
                const uint32_t begin_bucket_id = holder_id * g_shared->buckets_per_holder;

                const uintptr_t bucket_start_offset_1 = (uint64_t)CONFIG_INDEX_SPARSE_BUCKET_SIZE_1 * (bucket_id - begin_bucket_id);
                const uint64_t bucket_size_1 = g_buckets_endoffset_1[bucket_id] - bucket_start_offset_1;
                if (max_bucket_size_1 < bucket_size_1) {
                    max_bucket_size_1 = bucket_size_1;
                }

                const uintptr_t bucket_start_offset_2 = (uint64_t)CONFIG_INDEX_SPARSE_BUCKET_SIZE_2 * (bucket_id - begin_bucket_id);
                const uint64_t bucket_size_2 = g_buckets_endoffset_2[bucket_id] - bucket_start_offset_2;
                if (max_bucket_size_2 < bucket_size_2) {
                    max_bucket_size_2 = bucket_size_2;
                }
            }

            INFO("max_bucket_size_1: %lu", max_bucket_size_1);
            INFO("max_bucket_size_2: %lu", max_bucket_size_2);

            ASSERT(max_bucket_size_1 < CONFIG_INDEX_SPARSE_BUCKET_SIZE_1);
            ASSERT(max_bucket_size_2 < CONFIG_INDEX_SPARSE_BUCKET_SIZE_2);
        }
#endif
    }
}


void fn_unloader_thread_create_index() noexcept
{

}

void create_index_initialize_before_fork() noexcept
{
    //
    // Open holder files & endoffset file
    //
    {
        for (uint32_t holder_id = 0; holder_id < CONFIG_INDEX_HOLDER_COUNT; ++holder_id) {
            char filename[32];

            snprintf(filename, std::size(filename), "holder1_%04u", holder_id);
            g_holder_files_1_fd[holder_id] = C_CALL(openat(
                g_index_directory_fd,
                filename,
                O_WRONLY | O_CLOEXEC | O_CREAT | O_EXCL,
                0666));

            snprintf(filename, std::size(filename), "holder2_%04u", holder_id);
            g_holder_files_2_fd[holder_id] = C_CALL(openat(
                g_index_directory_fd,
                filename,
                O_WRONLY | O_CLOEXEC | O_CREAT | O_EXCL,
                0666));
        }

        g_endoffset_file_1.fd = C_CALL(openat(
            g_index_directory_fd,
            "endoffset_1",
            O_RDWR | O_CLOEXEC | O_CREAT | O_EXCL,
            0666));

        g_endoffset_file_2.fd = C_CALL(openat(
            g_index_directory_fd,
            "endoffset_2",
            O_RDWR | O_CLOEXEC | O_CREAT | O_EXCL,
            0666));
    }


    //
    // Calculate g_max_custkey
    //
    g_max_custkey = 75000000;
    if (false) {  // TODO
        char buffer[PAGE_SIZE * 2];
        C_CALL(pread(
            g_customer_file.fd,
            buffer,
            std::size(buffer),
            __align_down(g_customer_file.file_size, PAGE_SIZE) - PAGE_SIZE));
        const char* p = &buffer[std::size(buffer) - 1];
        while (*(p - 1) != '\n') --p;
        g_max_custkey = 0;
        while (*p != '|') {
            ASSERT(*p >= '0' && *p <= '9');
            g_max_custkey = g_max_custkey * 10 + (*p - '0');
            ++p;
        }
        INFO("g_max_custkey: %u", g_max_custkey);
    }


    // Calculate g_max_orderkey
    g_max_orderkey = 750000000;
    if (false) {  // TODO
        char buffer[PAGE_SIZE * 2];
        C_CALL(pread(
            g_orders_file.fd,
            buffer,
            std::size(buffer),
            __align_down(g_orders_file.file_size, PAGE_SIZE) - PAGE_SIZE));
        const char* p = &buffer[std::size(buffer) - 1];
        while (*(p - 1) != '\n') --p;
        g_max_orderkey = 0;
        while (*p != '|') {
            ASSERT(*p >= '0' && *p <= '9');
            g_max_orderkey = g_max_orderkey * 10 + (*p - '0');
            ++p;
        }
        ASSERT((g_max_orderkey & 0b11000) == 0);
        g_max_orderkey = (g_max_orderkey & 0b111) | ((g_max_orderkey >> 2) & ~0b1);
        INFO("g_max_orderkey: %u", g_max_orderkey);
    }


    // Initialize g_customer_shm, g_orders_shm, g_lineitem_shm
    if (g_is_preparing_page_cache) {
#if ENABLE_SHM_CACHE_TXT
        PANIC("TODO!");
#else  // !ENABLE_SHM_CACHE_TXT
        // Do nothing
#endif  // ENABLE_SHM_CACHE_TXT
    }
    else {
#if ENABLE_SHM_CACHE_TXT
        ASSERT(g_customer_file.file_size > 0);
        ASSERT(g_customer_shm.size_in_byte == g_customer_file.file_size);
        ASSERT(g_customer_shm.shmid >= 0);
        ASSERT(g_customer_shm.ptr != nullptr);
        INFO("g_customer_shm: shmid=%d, ptr=%p, size0x%lx", g_customer_shm.shmid, g_customer_shm.ptr, g_customer_shm.size_in_byte);

        ASSERT(g_orders_file.file_size > 0);
        ASSERT(g_orders_shm.size_in_byte == g_orders_file.file_size);
        ASSERT(g_orders_shm.shmid >= 0);
        ASSERT(g_orders_shm.ptr != nullptr);
        INFO("g_orders_shm: shmid=%d, ptr=%p, size0x%lx", g_orders_shm.shmid, g_orders_shm.ptr, g_orders_shm.size_in_byte);

        ASSERT(g_lineitem_file.file_size > 0);
        ASSERT(g_lineitem_shm.size_in_byte == g_lineitem_file.file_size);
        ASSERT(g_lineitem_shm.shmid >= 0);
        ASSERT(g_lineitem_shm.ptr != nullptr);
        INFO("g_lineitem_shm: shmid=%d, ptr=%p, size0x%lx", g_lineitem_shm.shmid, g_lineitem_shm.ptr, g_lineitem_shm.size_in_byte);
        
#else  // !ENABLE_SHM_CACHE_TXT
        // Do nothing
#endif  // ENABLE_SHM_CACHE_TXT
    }

    // Initialize g_custkey_to_mktid, g_orderkey_to_order
    {
        const bool success = g_custkey_to_mktid.init_fixed(
            SHMKEY_CUSTKEY_TO_MKTID,
            sizeof(g_custkey_to_mktid.ptr[0]) * (g_max_custkey / 3 + 1),
            true);
        CHECK(success);
        INFO("g_custkey_to_mktid: %p (size: %lu)", g_custkey_to_mktid.ptr, g_custkey_to_mktid.size_in_byte);
    }

    // Initialize g_orderkey_to_order
    {
        const bool success = g_orderkey_to_order.init_fixed(
            SHMKEY_ORDERKEY_TO_ORDER,
            sizeof(g_orderkey_to_order.ptr[0]) * (g_max_orderkey + 1),
            true);
        CHECK(success);
        INFO("g_orderkey_to_order: %p (size: %lu)", g_orderkey_to_order.ptr, g_orderkey_to_order.size_in_byte);
    }

    // Initialize g_orderkey_to_custkey
    {
        const bool success = g_orderkey_to_custkey.init_fixed(
            SHMKEY_ORDERKEY_TO_CUSTKEY,
            sizeof(g_orderkey_to_custkey.ptr[0]) * (g_max_orderkey + 1),
            true);
        CHECK(success);
        INFO("g_orderkey_to_custkey: %p (size: %lu)", g_orderkey_to_custkey.ptr, g_orderkey_to_custkey.size_in_byte);
    }
}

void create_index_initialize_after_fork() noexcept
{
    // Attach g_customer_shmat_ptr, g_orders_shmat_ptr, g_lineitem_shmat_ptr
    if (g_is_preparing_page_cache) {
#if ENABLE_SHM_CACHE_TXT
        PANIC("TODO!");
#else  // !ENABLE_SHM_CACHE_TXT
        // Do nothing
#endif // ENABLE_SHM_CACHE_TXT
    }
    else {
#if ENABLE_SHM_CACHE_TXT
        g_customer_shm.attach_fixed(false);
        g_orders_shm.attach_fixed((g_id == 0));
        g_lineitem_shm.attach_fixed(false);
#else  // !ENABLE_SHM_CACHE_TXT
        // Do nothing
#endif // ENABLE_SHM_CACHE_TXT
    }

    // Initialize g_custkey_to_mktid
    g_custkey_to_mktid.attach_fixed((g_id == 0));
#if ENABLE_ASSERTION
    memset(g_custkey_to_mktid.ptr, 0x00, g_custkey_to_mktid.size_in_byte);
#endif

    // Initialize g_orderkey_to_order
    g_orderkey_to_order.attach_fixed((g_id == 0));
#if ENABLE_ASSERTION
    memset(g_orderkey_to_order.ptr, 0x00, g_orderkey_to_order.size_in_byte);
#endif

    // Initialize g_orderkey_to_custkey
    g_orderkey_to_custkey.attach_fixed((g_id == 0));
#if ENABLE_ASSERTION
    memset(g_orderkey_to_custkey.ptr, 0x00, g_orderkey_to_custkey.size_in_byte);
#endif


    // Initialize g_mapping_buffers
    {
        DEBUG("CONFIG_LOAD_TXT_BUFFER_COUNT: %u", CONFIG_LOAD_TXT_BUFFER_COUNT);

        DEBUG("TXT_MAPPING_BUFFER_SIZE: 0x%x (%u)", TXT_MAPPING_BUFFER_SIZE, TXT_MAPPING_BUFFER_SIZE);
        ASSERT(TXT_MAPPING_BUFFER_SIZE % PAGE_SIZE == 0);

        g_txt_mapping_buffer_start_ptr = mmap_reserve_space((size_t)TXT_MAPPING_BUFFER_SIZE * CONFIG_LOAD_TXT_BUFFER_COUNT);
        DEBUG("g_txt_mapping_buffer_start_ptr: %p", g_txt_mapping_buffer_start_ptr);

        static_assert(g_txt_mapping_bag.capacity() == CONFIG_LOAD_TXT_BUFFER_COUNT);
        g_txt_mapping_bag.init([](const size_t idx) -> index32_t {
            ASSERT(idx < CONFIG_LOAD_TXT_BUFFER_COUNT);
            return idx;
        }, CONFIG_LOAD_TXT_BUFFER_COUNT);
    }
}
