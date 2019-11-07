#include "common.h"

//==============================================================================
// Structures
//==============================================================================
struct mapped_file_part_overlapped_t
{
    uint64_t file_offset;  // TODO: optimize this out to save memory
    uint32_t desired_size;
    uint32_t map_size;

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
        CONFIG_CUSTOMER_PART_BODY_SIZE,
        CONFIG_ORDERS_PART_BODY_SIZE,
        CONFIG_LINEITEM_PART_BODY_SIZE
    }) + CONFIG_PART_OVERLAPPED_SIZE;
    void* g_txt_mapping_buffer_start_ptr = nullptr;  // [CONFIG_LOAD_BUFFER_COUNT][TXT_MAPPING_BUFFER_SIZE]
    spsc_bounded_bag<index32_t, CONFIG_LOAD_BUFFER_COUNT> g_txt_mapping_buffers { };
    mapped_file_part_overlapped_t g_txt_overlapped_parts[CONFIG_LOAD_BUFFER_COUNT];

    spsc_queue<index32_t, CONFIG_LOAD_BUFFER_COUNT> g_customer_mapping_queue { };
    spsc_queue<index32_t, CONFIG_LOAD_BUFFER_COUNT> g_orders_mapping_queue { };
    spsc_queue<index32_t, CONFIG_LOAD_BUFFER_COUNT> g_lineitem_mapping_queue { };

    uint32_t g_max_custkey = 0;
    int g_custkey_to_mktid_shmid = -1;
    uint8_t* g_custkey_to_mktid = nullptr;  // [g_max_custkey+1]

    uint32_t g_max_orderkey = 0;
    int g_orderkey_to_order_shmid = -1;
    order_t* g_orderkey_to_order = nullptr;  // [g_max_orderkey+1]
}



template<uint32_t _PartBodySize, uint32_t _PartOverlap, bool _UseMmap, size_t _LoadBufferCount>
static void load_file_overlapped(
    /*in*/ [[maybe_unused]] sync_barrier& barrier,
    /*inout*/ std::atomic_size_t& shared_offset,  // it should be 0!
    /*in*/ [[maybe_unused]] const uint32_t tid,
    /*in*/ const size_t size,
    /*in*/ const int fd,
    /*inout*/ spsc_queue<index32_t, _LoadBufferCount>& queue)
{
    DEBUG("[loader:%u] load_file_overlapped() starts: fd=%d", tid, fd);

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
        g_txt_mapping_buffers.take(&part_index);
        ASSERT(part_index < _LoadBufferCount);

        mapped_file_part_overlapped_t& part = g_txt_overlapped_parts[part_index];
        part.file_offset = off;
        if (__unlikely(off + _PartBodySize + _PartOverlap >= size)) {  // this is last
            part.desired_size = size - off;
            part.map_size = size - off;
        }
        else {
            part.desired_size = _PartBodySize;
            part.map_size = _PartBodySize + _PartOverlap;
        }

        void* const ptr = (void*)((uintptr_t)g_txt_mapping_buffer_start_ptr + (uintptr_t)part_index * TXT_MAPPING_BUFFER_SIZE);
        if constexpr (_UseMmap) {
            void* const return_ptr = mmap(
                ptr,
                part.map_size,
                PROT_READ,
                MAP_FIXED | MAP_PRIVATE | MAP_POPULATE,
                fd,
                off);
            CHECK(return_ptr != MAP_FAILED);
            ASSERT(return_ptr == ptr);
        }
        else {
            const ssize_t read_cnt = C_CALL(pread(fd, ptr, part.map_size, off));
            CHECK(read_cnt == part.map_size);
        }

        //DEBUG("[loader:%u] load_file_overlapped(): fd=%d mapped to %p (size=%lu, offset=%lu, _DesiredSize=%u, map_size=%u, part_index=%lu)",
        //    tid, fd, ptr, size, off, part.desired_size, part.map_size, part_index);
        queue.push(part_index);

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


void fn_loader_thread_create_index(const uint32_t tid) noexcept
{
    DEBUG("[loader:%u] fn_loader_thread_create_index() starts", tid);

    // Load customer file to memory
    {
        load_file_overlapped<CONFIG_CUSTOMER_PART_BODY_SIZE, CONFIG_PART_OVERLAPPED_SIZE, /*_UseMmap*/true>(
            g_shared->loader_sync_barrier,
            g_shared->customer_file_shared_offset,
            tid,
            g_customer_file.file_size,
            g_customer_file.fd,
            g_customer_mapping_queue);
    }


    // Load orders file to memory
    {
        load_file_overlapped<CONFIG_ORDERS_PART_BODY_SIZE, CONFIG_PART_OVERLAPPED_SIZE, /*_UseMmap*/true>(
            g_shared->loader_sync_barrier,
            g_shared->orders_file_shared_offset,
            tid,
            g_orders_file.file_size,
            g_orders_file.fd,
            g_orders_mapping_queue);
    }

}





void worker_load_customer_multi_part([[maybe_unused]] const uint32_t tid) noexcept
{
    index32_t part_index;
    while (g_customer_mapping_queue.pop(&part_index)) {
        ASSERT(part_index < CONFIG_LOAD_BUFFER_COUNT);
        const mapped_file_part_overlapped_t& part = g_txt_overlapped_parts[part_index];
        const char* p = (const char*)((uintptr_t)g_txt_mapping_buffer_start_ptr + (uintptr_t)part_index * TXT_MAPPING_BUFFER_SIZE);
        const char* end = p + part.desired_size;

        // Get beginning custkey -> last_custkey
        uint32_t last_custkey = 0;
        if (!part.is_first()) {
            while (*p != '\n') ++p;
            ++p;  // skip '\n'

            last_custkey = __parse_u32<'|'>(p);
            if (last_custkey % 2 == 0) {
                while (*p != '\n') ++p;
                ++p;  // skip '\n'
                ++last_custkey;
            }
        }
        else {  // part.is_first()
            last_custkey = 1;
        }
        ASSERT(last_custkey % 2 == 1);
        ASSERT(last_custkey > 0);
        ASSERT(last_custkey <= g_max_custkey, "Unexpected last_custkey (too large): %u", last_custkey);
        --last_custkey;

        if (!part.is_last()) {
            while (*end != '\n') ++end;
            ++end;  // skip '\n'

            const uint32_t to_custkey = __parse_u32<'|'>(end);
            if (to_custkey % 2 == 1) {
                while (*end != '\n') ++end;
                ++end;  // skip '\n'
            }
        }

        //DEBUG("[%u] load customer: [%p, %p)", tid, p, end);
#if ENABLE_ASSERTION
        ++g_shared->customer_file_loaded_parts;
#endif

        uint32_t write_offset = last_custkey / 2;
        while (p < end) {
            ASSERT(last_custkey % 2 == 0);
            uint8_t write_value = 0;

            //TODO: #pragma GCC unroll 2
            for (uint32_t inner = 0; inner < 2; ++inner) {
                ++last_custkey;
#if ENABLE_ASSERTION
                uint32_t custkey = 0;
                while (*p != '|') {
                    ASSERT(*p >= '0' && *p <= '9', "Unexpected char: %c", *p);
                    custkey = custkey * 10 + (*p - '0');
                    ++p;
                }
                ASSERT(custkey == last_custkey);
#else
                const uint32_t custkey = last_custkey;
                while (*p != '|') {
                    ++p;
                }
#endif
                ++p;  // skip '|'
                ASSERT(custkey <= g_max_custkey, "Unexpected custkey (too large): %u", custkey);

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
                            write_value |= mktid << (inner * 4);
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
                        write_value |= new_mktid << (inner * 4);

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

                        // TODO:
                        //g_shared->total_buckets = g_shared->mktid_count * BUCKETS_PER_MKTID;
                        //g_shared->buckets_per_holder = DIV_UP(g_shared->total_buckets, CONFIG_INDEX_HOLDER_COUNT);
                    }
                }
            }

            ASSERT(last_custkey % 2 == 0);
            ASSERT(write_offset == (last_custkey - 1) / 2);
            g_custkey_to_mktid[write_offset++] = write_value;
        }

        g_txt_mapping_buffers.return_back(part_index);
    }

    INFO("[%u] done worker_load_customer_multi_part()", tid);
}



void worker_load_orders_multi_part([[maybe_unused]] const uint32_t tid) noexcept
{
    index32_t part_index;
    while (g_orders_mapping_queue.pop(&part_index)) {
        ASSERT(part_index < CONFIG_LOAD_BUFFER_COUNT);
        const mapped_file_part_overlapped_t& part = g_txt_overlapped_parts[part_index];
        const char* p = (const char*)((uintptr_t)g_txt_mapping_buffer_start_ptr + (uintptr_t)part_index * TXT_MAPPING_BUFFER_SIZE);
        const char* end = p + part.desired_size;

        if (!part.is_first()) {
            while (*p != '\n') ++p;
            ++p;
        }
        if (!part.is_last()) {
            while (*end != '\n') ++end;
            ++end;
        }

        //DEBUG("[%u] load orders: [%p, %p)", tid, p, end);

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
        --last_orderkey;

        while (p < end) {
            ++last_orderkey;
#if ENABLE_ASSERTION
            uint32_t orderkey = 0;
            while (*p != '|') {
                ASSERT(*p >= '0' && *p <= '9', "Unexpected char: %c", *p);
                orderkey = orderkey * 10 + (*p - '0');
                ++p;
            }
            ASSERT((orderkey & 0b11000) == 0);
            orderkey = (orderkey & 0b111) | ((orderkey >> 2) & ~0b1);
            ASSERT(orderkey == last_orderkey);
#else
            const uint32_t orderkey = last_orderkey;
            while (*p != '|') {
                ++p;
            }
#endif
            ++p;  // skip '|'
            ASSERT(orderkey <= g_max_orderkey, "Unexpected orderkey (too large): %u", orderkey);
            
            uint32_t custkey = 0;
            while (*p != '|') {
                ASSERT(*p >= '0' && *p <= '9', "Unexpected char: %c", *p);
                custkey = custkey * 10 + (*p - '0');
                ++p;
            }
            ++p;  // skip '|'
            ASSERT(custkey >= 1);
            ASSERT(custkey <= g_max_custkey, "Unexpected custkey (too large): %u", custkey);
            const uint8_t mktid_embed = g_custkey_to_mktid[(custkey - 1) / 2];

            //if (custkey % 2 == 1) {
            //    mktid = mktid_embed & 0b00001111;
            //}
            //else {  // custkey % 2 == 0
            //    mktid = (mktid_embed & 0b11110000) >> 4;
            //}
            const uint8_t mktid = ((uint32_t)mktid_embed << (24 + ((custkey & 1) << 2))) >> 28;  // this saves 100 ms
            ASSERT(mktid < g_shared->mktid_count, "Expect mktid < g_shared->mktid_count (%u < %u)", mktid, g_shared->mktid_count);

            const date_t orderdate = date_from_string(p);
            ASSERT(orderdate >= MIN_TABLE_DATE);
            ASSERT(orderdate <= MAX_TABLE_DATE);
            p += 10;  // skip 'yyyy-MM-dd'

            // Save to orderkey_to_order
            ASSERT(mktid < (1 << 4), "Boom! mktid %u too large", mktid);
            ASSERT(orderdate < (1 << 12), "BUG: orderdate too large?");
            g_orderkey_to_order[orderkey] = { (std::make_unsigned_t<date_t>)(orderdate), mktid };

            ASSERT(*p == '\n', "Expect EOL");
            p += 1;  // skip '\n'
        }

        g_txt_mapping_buffers.return_back(part_index);
    }

    INFO("[%u] done worker_load_orders_multi_part()", tid);
}




void fn_worker_thread_create_index(const uint32_t tid) noexcept
{
    DEBUG("[%u] fn_worker_thread_create_index() starts", tid);

    // Parse loaded customer table
    {
        worker_load_customer_multi_part(tid);
        g_shared->worker_sync_barrier.sync();
        if (tid == 0) {
            // TODO:
            //INFO("g_shared->total_buckets: %u", g_shared->total_buckets);
            //INFO("g_shared->buckets_per_holder: %u", g_shared->buckets_per_holder);

#if ENABLE_ASSERTION
            INFO("g_shared->customer_file_loaded_parts: %lu", g_shared->customer_file_loaded_parts.load());
#endif
        }
    }

    // Parse loaded orders table
    {
        worker_load_orders_multi_part(tid);
        g_shared->worker_sync_barrier.sync();

        // Detach g_custkey_to_mktid
        ASSERT(g_custkey_to_mktid != nullptr);
        C_CALL(shmdt(g_custkey_to_mktid));
        g_custkey_to_mktid = nullptr;
    }
}


void fn_unloader_thread_create_index() noexcept
{

}

void create_index_initialize_before_fork() noexcept
{
    // Calculate g_max_custkey
    {
        char buffer[256];
        C_CALL(pread(
            g_customer_file.fd,
            buffer,
            std::size(buffer),
            g_customer_file.file_size - std::size(buffer)));
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
    {
        char buffer[256];
        C_CALL(pread(
            g_orders_file.fd,
            buffer,
            std::size(buffer),
            g_orders_file.file_size - std::size(buffer)));
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


    // Initialize g_custkey_to_mktid_shmid
    {
        const uint64_t mem_size = sizeof(g_custkey_to_mktid[0]) * (g_max_custkey / 2 + 1);

        g_custkey_to_mktid = (uint8_t*)mmap_reserve_space(mem_size);
        INFO("g_custkey_to_mktid: %p", g_custkey_to_mktid);

        g_custkey_to_mktid_shmid = C_CALL(shmget(
            SHMKEY_CUSTKEY_TO_MKTID,
            mem_size,
            IPC_CREAT | 0666 | SHM_HUGETLB | SHM_HUGE_2MB));
        INFO("g_custkey_to_mktid_shmid: %d", g_custkey_to_mktid_shmid);

        INFO("g_custkey_to_mktid: %p (size: 0x%lx)", g_custkey_to_mktid, mem_size);
    }

    // Initialize g_orderkey_to_order_shmid
    {
        g_orderkey_to_order = (order_t*)mmap_reserve_space(
            sizeof(g_orderkey_to_order[0]) * (g_max_orderkey + 1));
        INFO("g_orderkey_to_order: %p", g_orderkey_to_order);

        g_orderkey_to_order_shmid = C_CALL(shmget(
            SHMKEY_ORDERKEY_TO_ORDER,
            sizeof(g_orderkey_to_order[0]) * (g_max_orderkey + 1),
            IPC_CREAT | 0666 | SHM_HUGETLB | SHM_HUGE_2MB));
        INFO("g_orderkey_to_order_shmid: %d", g_orderkey_to_order_shmid);

        INFO("g_orderkey_to_order: %p (size: 0x%lx)",
            g_orderkey_to_order, sizeof(g_orderkey_to_order[0]) * (g_max_orderkey + 1));
    }
}

void create_index_initialize_after_fork() noexcept
{
    // Initialize g_custkey_to_mktid
    {
        ASSERT(g_custkey_to_mktid != nullptr);
        ASSERT(g_custkey_to_mktid_shmid >= 0);
        void* const ptr = (uint8_t*)shmat(
            g_custkey_to_mktid_shmid,
            g_custkey_to_mktid,
            0);
        CHECK(ptr != (void*)-1,
            "shmat(g_custkey_to_mktid_shmid=%d) failed. errno = %d (%s)", g_custkey_to_mktid_shmid, errno, strerror(errno));
        if (g_id == 0) {
            C_CALL(shmctl(g_custkey_to_mktid_shmid, IPC_RMID, nullptr));
        }

        ASSERT(g_custkey_to_mktid == ptr);
        DEBUG("[%u] g_custkey_to_mktid: %p", g_id, g_custkey_to_mktid);
    }

    // Initialize g_orderkey_to_order
    {
        ASSERT(g_orderkey_to_order != nullptr);
        ASSERT(g_orderkey_to_order_shmid >= 0);
        void* const ptr = (order_t*)shmat(
            g_orderkey_to_order_shmid,
            g_orderkey_to_order,
            0);
        CHECK(ptr != (void*)-1,
            "shmat(g_orderkey_to_order_shmid=%d) failed. errno = %d (%s)", g_orderkey_to_order_shmid, errno, strerror(errno));
        if (g_id == 0) {
            C_CALL(shmctl(g_orderkey_to_order_shmid, IPC_RMID, nullptr));
        }

        ASSERT(g_orderkey_to_order == ptr);
        DEBUG("[%u] g_orderkey_to_order: %p", g_id, g_orderkey_to_order);
    }

    // Initialize g_mapping_buffers
    {
        DEBUG("CONFIG_LOAD_BUFFER_COUNT: %u", CONFIG_LOAD_BUFFER_COUNT);

        DEBUG("TXT_MAPPING_BUFFER_SIZE: 0x%x (%u)", TXT_MAPPING_BUFFER_SIZE, TXT_MAPPING_BUFFER_SIZE);
        ASSERT(TXT_MAPPING_BUFFER_SIZE % PAGE_SIZE == 0);

#if ENABLE_LOAD_FILE_USE_PREAD  // TODO: decide by using CPU or GPU?
        g_txt_mapping_buffer_start_ptr = mmap_allocate((size_t)TXT_MAPPING_BUFFER_SIZE * CONFIG_LOAD_BUFFER_COUNT);
#else
        g_txt_mapping_buffer_start_ptr = mmap_reserve_space((size_t)TXT_MAPPING_BUFFER_SIZE * CONFIG_LOAD_BUFFER_COUNT);
#endif
        DEBUG("g_txt_mapping_buffer_start_ptr: %p", g_txt_mapping_buffer_start_ptr);

        g_txt_mapping_buffers.init([](const size_t idx) -> index32_t {
            ASSERT(idx < CONFIG_LOAD_BUFFER_COUNT);
            return idx;
        }, g_txt_mapping_buffers.capacity());
    }
}
