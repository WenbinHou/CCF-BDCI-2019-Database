#include "common.h"

struct order_t
{
    date_t orderdate : 12;
    uint8_t mktid : 4;
};
static_assert(sizeof(order_t) == 2);

struct lineitem_file_part_t
{
    void* map_ptr;
    const char* valid_start;
    const char* valid_end;
    uint16_t* part_item_offset;  // by (mktid,orderdate)
    uint32_t map_size;
    uint32_t offset_from_worker_tid;
};


namespace
{
    constexpr const uint32_t PART_OVERLAP = 4096;
    constexpr const uint32_t CUSTOMER_PART_BODY_SIZE = 1048576 * 4/* - PART_OVERLAP*/;
    constexpr const uint32_t ORDERS_PART_BODY_SIZE = 1048576 * 8/* - PART_OVERLAP*/;
    constexpr const uint32_t LINEITEM_PART_BODY_SIZE = 1048576 * 24/* - PART_OVERLAP*/;

    mpmc_queue<mapped_file_part_overlapped_t> g_customer_mapping_queue { };
    std::string g_all_mktsegments[256] { };
    std::mutex g_all_mktsegments_insert_mutex { };
    uint8_t g_mktid_count = 0;
    uint32_t g_max_custkey = 0;
    uint8_t* g_custkey_to_mktid = nullptr;

    mpmc_queue<mapped_file_part_overlapped_t> g_orders_mapping_queue { };
    uint32_t g_max_orderkey = 0;
    order_t* g_orderkey_to_order = nullptr;

    mpmc_queue<mapped_file_part_overlapped_t> g_lineitem_mapping_queue { };
    uint32_t g_lineitem_total_count_upbound = 0;
    uint32_t g_lineitem_total_buckets = 0;
    uint16_t* g_lineitem_per_worker_per_bucket_item_count[MAX_WORKER_THREADS] { };  // [tid][bucket]
    done_event g_lineitem_per_worker_per_bucket_item_count_alloc_done { };
    uint32_t* g_lineitem_per_bucket_by_worker_prefixsum[MAX_WORKER_THREADS] { };  // [tid][bucket]
    done_event g_lineitem_per_bucket_by_worker_prefixsum_alloc_done { };
    uint32_t* g_lineitem_by_bucket_prefixsum = nullptr;  // [bucket]
    done_event g_lineitem_by_bucket_prefixsum_alloc_done { };
    lineitem_file_part_t* g_lineitem_parts = nullptr;
    std::atomic_uint32_t g_lineitem_parts_count { 0 };
    std::atomic_uint32_t g_lineitem_parts_build_index_curr { 0 };

    uint64_t* g_index_items = nullptr;
    done_event g_index_items_alloc_done { };

#if ENABLE_UNLOADER_FOR_MUNMAP
    mpmc_queue<iovec> g_unmap_queue { };
#endif
}


FORCEINLINE size_t calc_bucket_index(const uint8_t mktid, const date_t orderdate) noexcept
{
    ASSERT(mktid < g_mktid_count, "BUG");
    ASSERT(orderdate >= MIN_TABLE_DATE, "BUG");
    ASSERT(orderdate <= MAX_TABLE_DATE, "BUG");

    return (size_t)(mktid - 0) * (MAX_TABLE_DATE - MIN_TABLE_DATE + 1) + (orderdate - MIN_TABLE_DATE);
}



void create_index_initialize() noexcept
{
    // Get max custkey
    {
        char buf[256];
        C_CALL(pread(g_customer_fd, buf, std::size(buf), g_customer_file_size - std::size(buf)));
        const char* p = buf + std::size(buf) - 1;
        while (*(p - 1) != '\n') --p;
        while (*p != '|') {
            ASSERT(*p >= '0' && *p <= '9', "Unexpected char: %c (0x%02x)", *p, *p);
            g_max_custkey = g_max_custkey * 10 + (*(p++) - '0');
        }
        DEBUG("g_max_custkey: %u", g_max_custkey);
    }

    // Get max orderkey
    {
        char buf[256];
        C_CALL(pread(g_orders_fd, buf, std::size(buf), g_orders_file_size - std::size(buf)));
        const char* p = buf + std::size(buf) - 1;
        while (*(p - 1) != '\n') --p;
        while (*p != '|') {
            ASSERT(*p >= '0' && *p <= '9', "Unexpected char: %c (0x%02x)", *p, *p);
            g_max_orderkey = g_max_orderkey * 10 + (*(p++) - '0');
        }
        DEBUG("g_max_orderkey: %u", g_max_orderkey);
    }

    // Estimate g_lineitem_total_count_upbound
    {
        g_lineitem_total_count_upbound = (uint32_t)((uint64_t)g_max_orderkey * 1001 / 1000);
        DEBUG("estimated g_lineitem_total_count_upbound: %u", g_lineitem_total_count_upbound);
    }

#if ENABLE_UNLOADER_FOR_MUNMAP
    // Initialize g_unmap_queue
    {
        uint32_t max_push = 0;

        ASSERT(g_customer_file_size > 0, "BUG: g_customer_file_size == 0");
        max_push += (g_customer_file_size / CUSTOMER_PART_BODY_SIZE + 1);

        ASSERT(g_orders_file_size > 0, "BUG: g_orders_file_size == 0");
        max_push += (g_orders_file_size / ORDERS_PART_BODY_SIZE + 1);

        ASSERT(g_lineitem_file_size > 0, "BUG: g_lineitem_file_size == 0");
        max_push += (g_lineitem_file_size / LINEITEM_PART_BODY_SIZE + 1);  // for part

        DEBUG("g_unmap_queue max_push: %u", max_push);

        ASSERT(g_loader_thread_count > 0, "BUG: g_loader_thread_count == 0");
        g_unmap_queue.init(max_push, g_loader_thread_count);
    }
#endif
}



void worker_load_customer_multi_part(const uint32_t tid) noexcept
{
    mapped_file_part_overlapped_t part;
    while (g_customer_mapping_queue.pop(&part)) {
        const char* p = (const char*)part.ptr;
        const char* end = (const char*)part.ptr + part.desired_size;
        if (!part.is_first()) {
            while (*p != '\n') ++p;
            ++p;
        }
        if (!part.is_last()) {
            while (*end != '\n') ++end;
            ++end;
        }
        DEBUG("[%u] load customer: [%p, %p)", tid, p, end);

        while (p < end) {
            uint32_t custkey = 0;
            while (*p != '|') {
                ASSERT(*p >= '0' && *p <= '9', "Unexpected char: %c", *p);
                custkey = custkey * 10 + (*p - '0');
                ++p;
            }
            ++p;  // skip '|'
            ASSERT(custkey <= g_max_custkey, "Unexpected custkey (too large): %u", custkey);

            const char* const mktsegment_start = p;
            while (*p != '\n') {
                ++p;
            }

            std::string_view mktsegment_view(mktsegment_start, p - mktsegment_start);
            ++p;  // skip '\n'

            const auto try_find_mktsegment = [&]() -> bool {
                for (int8_t i = g_mktid_count - 1; i >= 0; --i) {
                    if (g_all_mktsegments[i][0] != mktsegment_view[0]) continue;
                    if (g_all_mktsegments[i] == mktsegment_view) {
                        g_custkey_to_mktid[custkey] = (uint8_t)i;
                        return true;
                    }
                }
                return false;
            };

            if (!try_find_mktsegment()) {
                std::unique_lock<std::mutex> lock(g_all_mktsegments_insert_mutex);
                if (!try_find_mktsegment()) {
                    g_custkey_to_mktid[custkey] = g_mktid_count;
                    g_all_mktsegments[g_mktid_count] = mktsegment_view;
                    DEBUG("found new mktsegment: %s -> %u", g_all_mktsegments[g_mktid_count].c_str(), g_mktid_count);
                    ++g_mktid_count;
                    g_lineitem_total_buckets = g_mktid_count * (MAX_TABLE_DATE - MIN_TABLE_DATE + 1);
                }
            }
        }

#if ENABLE_UNLOADER_FOR_MUNMAP
        g_unmap_queue.emplace(iovec { part.ptr, part.map_size });
#endif
    }

    DEBUG("[%u] done worker_load_customer_multi_part()", tid);
}


void worker_load_orders_multi_part(const uint32_t tid) noexcept
{
    mapped_file_part_overlapped_t part;
    while (g_orders_mapping_queue.pop(&part)) {
        const char* p = (const char*)part.ptr;
        const char* end = (const char*)part.ptr + part.desired_size;
        if (!part.is_first()) {
            while (*p != '\n') ++p;
            ++p;
        }
        if (!part.is_last()) {
            while (*end != '\n') ++end;
            ++end;
        }

        DEBUG("[%u] load orders: [%p, %p)", tid, p, end);
        while (p < end) {
            uint32_t orderkey = 0;
            while (*p != '|') {
                ASSERT(*p >= '0' && *p <= '9', "Unexpected char: %c", *p);
                orderkey = orderkey * 10 + (*p - '0');
                ++p;
            }
            ++p;  // skip '|'
            ASSERT(orderkey <= g_max_orderkey, "Unexpected orderkey (too large): %u", orderkey);

            uint32_t custkey = 0;
            while (*p != '|') {
                ASSERT(*p >= '0' && *p <= '9', "Unexpected char: %c", *p);
                custkey = custkey * 10 + (*p - '0');
                ++p;
            }
            ++p;  // skip '|'
            const uint8_t mktid = g_custkey_to_mktid[custkey];
            ASSERT(mktid < g_mktid_count, "Expect mktid < g_mktid_count (%u < %u)", mktid, g_mktid_count);

            const date_t orderdate = date_from_string(p);
            p += 10;  // skip 'yyyy-MM-dd'

            // Save to orderkey_to_order
            ASSERT(mktid < (1 << 4), "Boom! mktid %u too large", mktid);
            ASSERT(orderdate < (1 << 12), "BUG: orderdate too large?");
            g_orderkey_to_order[orderkey] = { orderdate, mktid };

            ASSERT(*p == '\n', "Expect EOL");
            p += 1;  // skip '\n'
        }

#if ENABLE_UNLOADER_FOR_MUNMAP
        g_unmap_queue.emplace(iovec { part.ptr, part.map_size });
#endif
    }

    DEBUG("[%u] done worker_load_orders_multi_part()", tid);
}




FORCEINLINE const char* skip_one_orderkey_in_lineitem(const char* p) noexcept
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

void worker_load_lineitem_multi_part_count_part(const uint32_t tid) noexcept
{
    ASSERT(g_lineitem_total_buckets > 0, "BUG: g_lineitem_total_buckets == 0");

    g_lineitem_per_worker_per_bucket_item_count_alloc_done.wait_done();
    ASSERT(g_lineitem_per_worker_per_bucket_item_count[tid] != nullptr,
        "BUG: g_lineitem_per_worker_per_bucket_item_count[tid] == nullptr");
    uint16_t* const curr_worker_per_bucket_item_count = g_lineitem_per_worker_per_bucket_item_count[tid];

#if ENABLE_ASSERTION
    for (uint32_t bucket = 0; bucket < g_lineitem_total_buckets; ++bucket) {
        ASSERT(curr_worker_per_bucket_item_count[bucket] == 0, "[%u] bucket %u not 0 ?!", tid, bucket);
    }
#endif

    mapped_file_part_overlapped_t part;
    while (g_lineitem_mapping_queue.pop(&part)) {

        const uint32_t pos = g_lineitem_parts_count++;

        // Copy from current curr_worker_per_bucket_item_count to the new part_item_offset
        // part_item_offset is not modified any more!
        uint16_t* const part_item_offset = g_lineitem_parts[pos].part_item_offset;
        ASSERT(part_item_offset != nullptr, "BUG: part_item_offset == nullptr");
        memcpy(part_item_offset, curr_worker_per_bucket_item_count, sizeof(uint16_t) * g_lineitem_total_buckets);

        const char* p = (const char*)part.ptr;
        const char* valid_end = (const char*)part.ptr + part.desired_size;
        if (!part.is_first()) {
            while (*p != '\n') ++p;
            ++p;

            p = skip_one_orderkey_in_lineitem(p);
        }
        const char* const valid_start = p;

        if (!part.is_last()) {
            while (*valid_end != '\n') ++valid_end;
            ++valid_end;

            valid_end = skip_one_orderkey_in_lineitem(valid_end);
        }

        g_lineitem_parts[pos].map_ptr = part.ptr;
        g_lineitem_parts[pos].map_size = part.map_size;
        g_lineitem_parts[pos].valid_start = valid_start;
        g_lineitem_parts[pos].valid_end = valid_end;
        g_lineitem_parts[pos].offset_from_worker_tid = tid;

        DEBUG("[%u] load lineitem: [%p, %p)", tid, p, valid_end);
        while (p < valid_end) {
            uint32_t orderkey = 0;
            while (*p != '|') {
                ASSERT(*p >= '0' && *p <= '9', "Unexpected char: %c", *p);
                orderkey = orderkey * 10 + (*p - '0');
                ++p;
            }
            ++p;  // skip '|'
            ASSERT(orderkey <= g_max_orderkey, "Unexpected orderkey (too large): %u", orderkey);

            const order_t& order = g_orderkey_to_order[orderkey];

            const uint8_t mktid = order.mktid;
            const date_t orderdate = order.orderdate;
            ASSERT(mktid < g_mktid_count, "BUG: expect mktid < g_mktid_count (%u < %u)", mktid, g_mktid_count);

            const size_t bucket = calc_bucket_index(mktid, orderdate);
            ++curr_worker_per_bucket_item_count[bucket];
            ASSERT(curr_worker_per_bucket_item_count[bucket] < UINT16_MAX, "Boom!");

            // Skip expend_cent, shipdate
            while (*p != '\n') {
                ++p;
            }
            ++p;  // skip '\n'
        }
    }
}



void worker_load_lineitem_multi_part_count_all(const uint32_t tid) noexcept
{
    ASSERT(g_lineitem_total_buckets > 0, "BUG: g_lineitem_total_buckets == 0");

    g_worker_sync_barrier.sync_and_run_once([]() {
        // Compute g_lineitem_per_bucket_by_worker_prefixsum
        // TODO: Very cache-unfriendly memory access!
        g_lineitem_per_bucket_by_worker_prefixsum_alloc_done.wait_done();
        for (uint32_t i = 0; i < g_lineitem_total_buckets; ++i) {
            // Compute g_lineitem_per_bucket_by_worker_prefixsum[...][i]
            g_lineitem_per_bucket_by_worker_prefixsum[0][i] = g_lineitem_per_worker_per_bucket_item_count[0][i];
            for (uint32_t t = 1; t < g_worker_thread_count; ++t) {
                g_lineitem_per_bucket_by_worker_prefixsum[t][i] = g_lineitem_per_bucket_by_worker_prefixsum[t-1][i] + g_lineitem_per_worker_per_bucket_item_count[t][i];
            }
        }

        // Compute g_lineitem_by_bucket_prefixsum
        g_lineitem_by_bucket_prefixsum_alloc_done.wait_done();
        g_lineitem_by_bucket_prefixsum[0] = g_lineitem_per_bucket_by_worker_prefixsum[g_worker_thread_count-1][0];
        for (uint32_t i = 1; i < g_lineitem_total_buckets; ++i) {
            g_lineitem_by_bucket_prefixsum[i] = g_lineitem_by_bucket_prefixsum[i-1] + g_lineitem_per_bucket_by_worker_prefixsum[g_worker_thread_count-1][i];
            //DEBUG("g_lineitem_by_bucket_prefixsum[%u] = %u", i, g_lineitem_by_bucket_prefixsum[i]);
        }
    });

    g_worker_sync_barrier.sync();
    DEBUG("[%u] all_lineitem_total_count: %u", tid, g_lineitem_by_bucket_prefixsum[g_lineitem_total_buckets-1]);
}


void worker_load_lineitem_multi_part_build_index(const uint32_t tid) noexcept
{
    g_worker_sync_barrier.run_once_and_sync([]() {
        g_index_items_alloc_done.wait_done();
    });
    ASSERT(g_index_items != nullptr, "BUG: g_index_items == nullptr");

    // TODO: push final_offset for munmap
    uint32_t* final_offset = (uint32_t*)mmap_allocate(sizeof(uint32_t) * g_lineitem_total_buckets);

    while (true) {
        const uint32_t pos = g_lineitem_parts_build_index_curr++;
        if (pos >= g_lineitem_parts_count) break;
        
        const lineitem_file_part_t& part = g_lineitem_parts[pos];

        if (part.offset_from_worker_tid == 0) {
            final_offset[0] = 0 + part.part_item_offset[0];
            for (uint32_t i = 1; i < g_lineitem_total_buckets; ++i) {
                final_offset[i] = g_lineitem_by_bucket_prefixsum[i-1] + part.part_item_offset[i];
            }
        }
        else {  // part.offset_from_worker_tid > 0
            final_offset[0] = 0 + g_lineitem_per_bucket_by_worker_prefixsum[part.offset_from_worker_tid-1][0] + part.part_item_offset[0];
            for (uint32_t i = 1; i < g_lineitem_total_buckets; ++i) {
                final_offset[i] = g_lineitem_by_bucket_prefixsum[i-1] + g_lineitem_per_bucket_by_worker_prefixsum[part.offset_from_worker_tid-1][i] + part.part_item_offset[i];
            }
        }

        //for (uint32_t i = 0; i < g_lineitem_total_buckets; ++i) {
        //    DEBUG("[%u] part_pos %u, bucket %u: final_offset=%u, count=?", tid, pos, i, final_offset[i]);
        //}

        const char* p = part.valid_start;
        const char* valid_end = part.valid_end;
        DEBUG("[%u] build index lineitem part #%u: [%p, %p)", tid, pos, p, valid_end);

        while (p < valid_end) {
            uint32_t orderkey = 0;
            while (*p != '|') {
                ASSERT(*p >= '0' && *p <= '9', "Unexpected char: %c", *p);
                orderkey = orderkey * 10 + (*p - '0');
                ++p;
            }
            ++p;  // skip '|'
            ASSERT(orderkey <= g_max_orderkey, "Unexpected orderkey (too large): %u", orderkey);

            const order_t& order = g_orderkey_to_order[orderkey];

            const uint8_t mktid = order.mktid;
            const date_t orderdate = order.orderdate;
            ASSERT(mktid < g_mktid_count, "BUG: expect mktid < g_mktid_count (%u < %u)", mktid, g_mktid_count);

            uint32_t expend_cent = 0;
            while (*p != '.') {
                ASSERT(*p >= '0' && *p <= '9', "Unexpected char: %c", *p);
                expend_cent = expend_cent * 10 + (*p - '0');
                ++p;
            }
            ++p;  // skip '.'
            while (*p != '|') {
                ASSERT(*p >= '0' && *p <= '9', "Unexpected char: %c", *p);
                expend_cent = expend_cent * 10 + (*p - '0');
                ++p;
            }
            ++p;  // skip '|'

            const date_t shipdate = date_from_string(p);
            p += 10;  // skip 'yyyy-MM-dd'
            ASSERT(shipdate > orderdate, "Expect shipdate > orderdate");
            ASSERT((shipdate - orderdate) < (1 << 8), "shipdate - orderdate Boom!");
            ASSERT(shipdate >= MIN_TABLE_DATE, "Expect shipdate >= MIN_TABLE_DATE");
            ASSERT(shipdate <= MAX_TABLE_DATE, "Expect shipdate <= MAX_TABLE_DATE");

            ASSERT(*p == '\n', "Expect EOL");
            p += 1;  // skip '\n'

            const uint64_t value = (uint64_t)(shipdate - orderdate) << 56 | (uint64_t)expend_cent << 32 | orderkey;

            const uint32_t bucket = calc_bucket_index(mktid, orderdate);
            g_index_items[(uint64_t)final_offset[bucket]++] = value;
        }

        DEBUG("[%u] done write index for lineitem part #%u", tid, pos);

#if ENABLE_UNLOADER_FOR_MUNMAP
        // TODO:? g_unmap_queue.emplace(iovec { part.map_ptr, part.map_size });
#endif
    }
}



void check_index_validity() noexcept
{
    ASSERT(g_index_items != nullptr, "BUG: g_index_items == nullptr");
    ASSERT(g_lineitem_by_bucket_prefixsum != nullptr, "BUG: g_lineitem_by_bucket_prefixsum == nullptr");
    ASSERT(g_orderkey_to_order != nullptr, "BUG: g_orderkey_to_order == nullptr");

    #pragma omp parallel for
    for (uint32_t bucket = 0; bucket < g_lineitem_total_buckets; ++bucket) {
        const uint64_t final_offset = (bucket == 0)
            ? 0
            : g_lineitem_by_bucket_prefixsum[bucket-1];
        const uint32_t final_count = (bucket == 0)
            ? g_lineitem_by_bucket_prefixsum[bucket]
            : g_lineitem_by_bucket_prefixsum[bucket] - g_lineitem_by_bucket_prefixsum[bucket-1];
        DEBUG("checking bucket %u: offset=%lu, count=%u", bucket, final_offset, final_count);

        for (uint64_t i = final_offset; i < final_offset + final_count; ++i) {
            const uint64_t value = g_index_items[i];

            //const uint8_t shipdate_diff = (uint8_t)(value >> 56);
            //const uint32_t expend_cent = (uint32_t)(value >> 32) & 0x00FFFFFF;
            const uint32_t orderkey = (uint32_t)(value);

            ASSERT(orderkey <= g_max_orderkey, "Unexpected orderkey (too large): %u", orderkey);

            const order_t& order = g_orderkey_to_order[orderkey];

            [[maybe_unused]] const uint8_t mktid = order.mktid;
            [[maybe_unused]] const date_t orderdate = order.orderdate;
            ASSERT(mktid < g_mktid_count, "BUG: expect mktid < g_mktid_count (%u < %u)", mktid, g_mktid_count);

            ASSERT(calc_bucket_index(mktid, orderdate) == bucket, "BUG! Build index error!");
        }
    }
}


void fn_worker_thread_create_index(const uint32_t tid) noexcept
{
    DEBUG("[%u] fn_worker_thread_create_index() starts", tid);

    // Set affinity: bound worker thread to current CPU core
//    {
//        cpu_set_t set;
//        CPU_ZERO(&set);
//        CPU_SET(tid, &set);
//        PTHREAD_CALL(pthread_setaffinity_np(pthread_self(), sizeof(set), &set));
//        DEBUG("[%u] set affinity to #%u", tid, tid);
//    }

    // Parse loaded customer table
    {
        worker_load_customer_multi_part(tid);
    }

    // Parse loaded orders table
    {
        worker_load_orders_multi_part(tid);
    }

    // Parse loaded lineitem table
    {
        worker_load_lineitem_multi_part_count_part(tid);

        worker_load_lineitem_multi_part_count_all(tid);

        worker_load_lineitem_multi_part_build_index(tid);

#if ENABLE_CHECK_INDEX_VALIDITY
        g_worker_sync_barrier.sync_and_run_once([]() {
            check_index_validity();
        });
#endif
    }

#if ENABLE_UNLOADER_FOR_MUNMAP
    // Mark g_unmap_queue as finished
    g_worker_sync_barrier.sync_and_run_once([]() {
        g_unmap_queue.mark_push_finish();
    });
#endif

    DEBUG("[%u] fn_worker_thread_create_index() done", tid);
}

void main_thread_create_index() noexcept
{
    // Load customer table to memory
    {
        /*
        g_custkey_to_mktid = (uint8_t*)mmap_allocate(
            sizeof(g_custkey_to_mktid[0]) * (g_max_custkey + 1));
        */

        g_custkey_to_mktid = (uint8_t*)mmap_parallel(
            sizeof(g_custkey_to_mktid[0]) * (g_max_custkey + 1),
            g_loader_thread_count,
            PROT_READ | PROT_WRITE,
            MAP_PRIVATE | MAP_ANONYMOUS);

        g_customer_mapping_queue.init(
            g_customer_file_size / CUSTOMER_PART_BODY_SIZE + 1, g_worker_thread_count);
        mmap_file_overlapped<CUSTOMER_PART_BODY_SIZE, PART_OVERLAP>(
            g_loader_thread_count, g_customer_fd, g_customer_file_size, g_customer_mapping_queue);
        g_customer_mapping_queue.mark_push_finish();
    }
    
    // Load orders table to memory
    {
        DEBUG("now allocating g_orderkey_to_order");
        g_orderkey_to_order = (order_t*)mmap_parallel(
            sizeof(g_orderkey_to_order[0]) * (g_max_orderkey + 1),
            g_loader_thread_count,
            PROT_READ | PROT_WRITE,
            MAP_PRIVATE | MAP_ANONYMOUS);
        DEBUG("allocated g_orderkey_to_order = %p", g_orderkey_to_order);

        g_orders_mapping_queue.init(
            g_orders_file_size / ORDERS_PART_BODY_SIZE + 1, g_worker_thread_count);
        mmap_file_overlapped<ORDERS_PART_BODY_SIZE, PART_OVERLAP>(
            g_loader_thread_count, g_orders_fd, g_orders_file_size, g_orders_mapping_queue);
        g_orders_mapping_queue.mark_push_finish();
    }
    
    // Load lineitem table to memory
    {
        // Initialize g_lineitem_per_worker_per_bucket_item_count
        {
            DEBUG("now allocating for g_lineitem_per_worker_per_bucket_item_count");
            ASSERT(g_lineitem_total_buckets > 0, "BUG: g_lineitem_total_buckets == 0");
            DEBUG("g_lineitem_total_buckets: %u", g_lineitem_total_buckets);
            /*
            uint16_t* const storage = (uint16_t*)mmap_parallel(
                sizeof(uint16_t) * g_worker_thread_count * g_lineitem_total_buckets,
                LINEITEM_LOADER_THREADS,
                PROT_READ | PROT_WRITE,
                MAP_PRIVATE | MAP_ANONYMOUS);
            */
            uint16_t* const storage = (uint16_t*)mmap_allocate(
                sizeof(uint16_t) * g_worker_thread_count * g_lineitem_total_buckets);
            for (uint32_t tid = 0; tid < g_worker_thread_count; ++tid) {
                g_lineitem_per_worker_per_bucket_item_count[tid] = storage + g_lineitem_total_buckets * tid;
            }
            DEBUG("allocated for g_lineitem_per_worker_per_bucket_item_count. storage = %p", storage);

            g_lineitem_per_worker_per_bucket_item_count_alloc_done.mark_done();
        }

        // Allocate g_lineitem_parts
        {
            const uint64_t lineitem_parts_max_push = (g_lineitem_file_size / LINEITEM_PART_BODY_SIZE + 1);
            DEBUG("lineitem_parts_max_push: %lu", lineitem_parts_max_push);
            DEBUG("now allocating g_lineitem_parts");
            g_lineitem_parts = (lineitem_file_part_t*)mmap_allocate(sizeof(lineitem_file_part_t) * lineitem_parts_max_push);
            DEBUG("allocated g_lineitem_parts = %p", g_lineitem_parts);

            // Allocate for g_lineitem_parts.part_item_offset
            uint16_t* const part_item_offsets = (uint16_t*)mmap_allocate(
                sizeof(uint16_t) * lineitem_parts_max_push * g_lineitem_total_buckets);
            for (uint32_t idx = 0; idx < lineitem_parts_max_push; ++idx) {
                g_lineitem_parts[idx].part_item_offset = part_item_offsets + g_lineitem_total_buckets * idx;
            }
        }

        g_lineitem_mapping_queue.init(
            g_lineitem_file_size / LINEITEM_PART_BODY_SIZE + 1, g_worker_thread_count);
        mmap_file_overlapped<LINEITEM_PART_BODY_SIZE, PART_OVERLAP>(
            g_loader_thread_count, g_lineitem_fd, g_lineitem_file_size, g_lineitem_mapping_queue);
        g_lineitem_mapping_queue.mark_push_finish();
    }


    // Allocate g_lineitem_per_bucket_by_worker_prefixsum
    {
        DEBUG("now allocating for g_lineitem_per_bucket_by_worker_prefixsum");
        ASSERT(g_lineitem_total_buckets > 0, "BUG: g_lineitem_total_buckets == 0");
        /* TODO: use multi-thread version?
        uint32_t* const storage = (uint32_t*)mmap_parallel(
            sizeof(uint32_t) * g_worker_thread_count * g_lineitem_total_buckets,
            g_loader_thread_count,
            PROT_READ | PROT_WRITE,
            MAP_PRIVATE | MAP_ANONYMOUS);
        */
        uint32_t* const storage = (uint32_t*)mmap_allocate(
            sizeof(uint32_t) * g_worker_thread_count * g_lineitem_total_buckets);
        for (uint32_t tid = 0; tid < g_worker_thread_count; ++tid) {
            g_lineitem_per_bucket_by_worker_prefixsum[tid] = storage + g_lineitem_total_buckets * tid;
        }
        DEBUG("allocated for g_lineitem_per_bucket_by_worker_prefixsum. storage = %p", storage);

        g_lineitem_per_bucket_by_worker_prefixsum_alloc_done.mark_done();
    }


    // Allocate g_lineitem_by_bucket_prefixsum
    {
        DEBUG("now map g_lineitem_by_bucket_prefixsum");
        ASSERT(g_lineitem_total_buckets > 0, "BUG: g_lineitem_total_buckets == 0");

        const int fd = C_CALL(openat(
            g_index_directory_fd,
            "prefixsum",
            O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC,
            0666));
        C_CALL(ftruncate(fd, sizeof(uint32_t) * g_lineitem_total_buckets));

        g_lineitem_by_bucket_prefixsum = (uint32_t*)my_mmap(
            sizeof(uint32_t) * g_lineitem_total_buckets,
            PROT_READ | PROT_WRITE,
            MAP_SHARED,
            fd,
            0);
        DEBUG("mapped g_lineitem_by_bucket_prefixsum = %p", g_lineitem_by_bucket_prefixsum);

        g_lineitem_by_bucket_prefixsum_alloc_done.mark_done();

        C_CALL(close(fd));
    }


    // Initialize mapping for g_index_items (this is a huge part!)
    {
        const int fd = C_CALL(openat(
            g_index_directory_fd,
            "items",
            O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC,
            0666));

        const uint64_t index_items_size = (uint64_t)g_lineitem_total_count_upbound * sizeof(uint64_t);
        DEBUG("truncate index items...");
        C_CALL(ftruncate64(fd, index_items_size));

        DEBUG("now allocating index items...");
        g_index_items = (uint64_t*)mmap_parallel(
            index_items_size,
            g_loader_thread_count,
            PROT_READ | PROT_WRITE,
            MAP_SHARED,
            fd);
        g_index_items_alloc_done.mark_done();
        DEBUG("now allocated index items...");

        C_CALL(madvise(g_index_items, index_items_size, MADV_WILLNEED | MADV_RANDOM));

        C_CALL(close(fd));
    }


    // Save mktsegment to index directory
    {
        const int fd = C_CALL(openat(
            g_index_directory_fd,
            "mktsegment",
            O_CREAT | O_EXCL | O_RDWR | O_CLOEXEC,
            0666));

        // Very limited number of mktsegments, so we just write() them to index file...
        C_CALL(write(fd, &g_mktid_count, sizeof(uint8_t)));
        for (uint8_t mktid = 0; mktid < g_mktid_count; ++mktid) {
            const std::string& mktsegment = g_all_mktsegments[mktid];
            ASSERT(mktsegment.length() <= 255, "BUG?!");
            const uint8_t mktsegment_length = (uint8_t)mktsegment.length();
            C_CALL(write(fd, &mktsegment_length, sizeof(uint8_t)));
            C_CALL(write(fd, mktsegment.c_str(), mktsegment.length()));
        }

        C_CALL(close(fd));
    }


#if ENABLE_UNLOADER_FOR_MUNMAP
    // Do unmapping...
    {
        #pragma omp parallel num_threads(g_loader_thread_count)
        //#pragma omp parallel num_threads(4)
        {
            {
                cpu_set_t set;
                CPU_ZERO(&set);
                CPU_SET(0, &set);
                PTHREAD_CALL(pthread_setaffinity_np(pthread_self(), sizeof(set), &set));
                DEBUG("unloader: set affinity to a single core");
            }

            iovec vec;
            while (g_unmap_queue.pop(&vec)) {
                DEBUG("munmap(%p, %lu)", vec.iov_base, vec.iov_len);
                C_CALL(munmap(vec.iov_base, vec.iov_len));
                //std::this_thread::yield();
            }
        }
    }
#endif
}
