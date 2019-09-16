#include "common.h"

//==============================================================
// Global variables
//==============================================================
const std::chrono::steady_clock::time_point g_startup_time = std::chrono::steady_clock::now();
volatile uint64_t g_dummy_prevent_optimize; 

uint8_t g_max_mktsegment = 0;
std::map<const char*, uint8_t, c_string_less> g_string_to_mktsegment;

uint32_t g_query_count;
std::atomic_uint32_t g_synthesis_query_count { 0 };
std::pair<uint32_t /*mktsegment*/, uint32_t /*bucket_index*/> g_all_queries[256];
std::vector<query_t> g_queries_by_mktsegment[256];

std::vector<std::thread> g_workers;
uint32_t g_workers_thread_count;
threads_barrier_t g_workers_barrier;
threads_barrier_t g_workers_barrier_external;

std::vector<std::thread> g_loaders;
uint32_t g_loaders_thread_count;
threads_barrier_t g_loaders_barrier;

//iovec g_mapped_customer, g_mapped_orders, g_mapped_lineitem;
int g_customer_fd, g_orders_fd, g_lineitem_fd;
uint64_t g_customer_filesize, g_orders_filesize, g_lineitem_filesize;
const char *g_customer_filepath, *g_orders_filepath, *g_lineitem_filepath;
mpmc_queue_t<mapped_file_part_t> g_mapped_customer_queue, g_mapped_orders_queue, g_mapped_lineitem_queue;

uint64_t g_upbound_custkey;
uint8_t* g_custkey_to_mktsegment;

uint64_t g_upbound_orderkey;
date_t* g_orderkey_to_order;



void load_queries(
    [[maybe_unused]] const int argc,
    char* const argv[])
{
    ASSERT(argc >= 5, "Unexpected argc too small: %d", argc);

    g_query_count = (uint32_t)strtol(argv[4], nullptr, 10);
    DEBUG("g_query_count: %u", g_query_count);

    for (uint32_t query_idx = 0; query_idx < g_query_count; ++query_idx) {
        uint8_t mktsegment;
        const char* const str_mktsegment = argv[5 + query_idx * 4 + 0];
        const auto it = g_string_to_mktsegment.find(str_mktsegment);
        if (it != g_string_to_mktsegment.end()) {
            mktsegment = it->second;
        }
        else {
            g_string_to_mktsegment[str_mktsegment] = ++g_max_mktsegment;
            mktsegment = g_max_mktsegment;
            DEBUG("// found new mktsegment: %s -> %u", str_mktsegment, g_max_mktsegment);
        }

        const size_t bucket_index = g_queries_by_mktsegment[mktsegment].size();
        g_queries_by_mktsegment[mktsegment].resize(bucket_index + 1);  // usually O(1)
        query_t& query = g_queries_by_mktsegment[mktsegment][bucket_index];
        query.query_index = query_idx;

        query.q_mktsegment = mktsegment;
        query.q_orderdate.parse(argv[5 + query_idx * 4 + 1], 0x00);
        query.q_shipdate.parse(argv[5 + query_idx * 4 + 2], 0xff);
        query.q_limit = (uint32_t)strtol(argv[5 + query_idx * 4 + 3], nullptr, 10);

        TRACE("query#%u: q_mktsegment=%u, q_orderdate=%04u-%02u-%02u, q_shipdate=%04u-%02u-%02u, q_limit=%u",
            query_idx, query.q_mktsegment,
            query.q_orderdate.year(), query.q_orderdate.month(), query.q_orderdate.day(),
            query.q_shipdate.year(), query.q_shipdate.month(), query.q_shipdate.day(),
            query.q_limit);
    }

    // Sort queries in each bucket by their q_orderdate (descending)
    for (uint32_t mktsegment = 1; mktsegment <= g_max_mktsegment; ++mktsegment) {
        std::sort(
            g_queries_by_mktsegment[mktsegment].begin(),
            g_queries_by_mktsegment[mktsegment].end(),
            [](const query_t& a, const query_t& b) noexcept {
                if (a.q_orderdate > b.q_orderdate) return true;
                if (a.q_orderdate < b.q_orderdate) return false;
                return (a.query_index < b.query_index);
            });

        TRACE("------------------------------------------------");
        TRACE("total queries on mktsegment#%u: %" PRIu64, mktsegment, (uint64_t)g_queries_by_mktsegment[mktsegment].size());
        for (size_t bucket_index = 0; bucket_index < g_queries_by_mktsegment[mktsegment].size(); ++bucket_index) {
            const query_t& query = g_queries_by_mktsegment[mktsegment][bucket_index];
            TRACE("    query#%u: q_mktsegment=%u, q_orderdate=%04u-%02u-%02u, q_shipdate=%04u-%02u-%02u, q_limit=%u",
                query.query_index, query.q_mktsegment,
                query.q_orderdate.year(), query.q_orderdate.month(), query.q_orderdate.day(),
                query.q_shipdate.year(), query.q_shipdate.month(), query.q_shipdate.day(),
                query.q_limit);

            // Fill g_all_queries
            g_all_queries[query.query_index] = std::make_pair((uint32_t)mktsegment, (uint32_t)bucket_index);
        }
    }
}



FORCEINLINE void open_file_mapping_multi_part(
    /*in*/[[maybe_unused]] const char* const file,
    /*in*/ const uint32_t tid,
    /*inout*/ std::atomic_uint64_t& shared_offset,
    /*in*/ const int fd,
    /*in*/ const uint64_t total_size,
    /*in*/ const uint64_t step_size,
    /*inout*/ mpmc_queue_t<mapped_file_part_t>& queue)
{
    DEBUG("[loader:%u] %s starts mapping", tid, file);

    while (true) {
        const uint64_t off = shared_offset.fetch_add(step_size);
        if (off >= total_size) {
            //INFO("=============== total_size: %lu, off: %lu", total_size, off);
            break;
        }

        mapped_file_part_t part;
        part.is_first = (off == 0);
        part.is_last = false;
        uint64_t desired_size = step_size;
        uint64_t map_size = step_size + 4096;
        if (off + desired_size >= total_size) {
            map_size = total_size - off;
            desired_size = total_size - off;
            part.is_last = true;
        }
        part.map_size = map_size;

        part.start = (const char*)mmap(
            nullptr,
            map_size,
            PROT_READ,
            MAP_PRIVATE | MAP_POPULATE,
            fd,
            off);
        if (UNLIKELY(part.start == MAP_FAILED)) {
            PANIC("mmap() failed. errno = %d (%s)", errno, strerror(errno));
        }
        part.desired_end = part.start + desired_size;

        DEBUG("[loader:%u] %s mapped to %p (total_size=%" PRIu64 ", offset=%" PRIu64 ", desired_size=%" PRIu64 ", map_size=%" PRIu64 ")",
            tid, file, part.start, total_size, off, desired_size, map_size);
        ASSERT(part.start != nullptr, "BUG: push: part.start == nullptr");
        ASSERT(part.desired_end != nullptr, "BUG: push: part.desired_end == nullptr");
        queue.push(part);
    }
    DEBUG("[loader:%u] %s all mapped", tid, file);

}


/*
// worker_load_customer: split to g_workers_thread_count workers
// But it turns out that the workloads are not evenly distributed!

void worker_load_customer(const uint32_t tid)
{
    static const char* __customer_starts[MAX_WORKER_THREADS + 1];

    const char* start = (const char*)g_mapped_customer.iov_base + g_mapped_customer.iov_len / g_workers_thread_count * tid;
    while (start > (const char*)g_mapped_customer.iov_base) {
        if (start[-1] == '\n') break;
        --start;
    }
    __customer_starts[tid] = start;
    if (tid == 0) __customer_starts[g_workers_thread_count] = (const char*)g_mapped_customer.iov_base + g_mapped_customer.iov_len;

    g_workers_barrier.sync();
    //if (tid == 0) {
    //    // Launch a thread for pre-fault mapped file
    //    std::thread([]() {
    //        const char* customer_ends[MAX_WORKER_THREADS];
    //        uint64_t max_length = 0;
    //        for (uint32_t t = 0; t < g_workers_thread_count; ++t) {
    //            customer_ends[t] = __customer_starts[t + 1];
    //            max_length = std::max(max_length, (uint64_t)(customer_ends[t] - __customer_starts[t]));
    //        }

    //        uint32_t dummy = 0;
    //        const uint64_t STEP_SIZE = 4096;
    //        for (uint64_t off = 0; off < max_length; off += STEP_SIZE) {
    //            for (uint32_t t = 0; t < g_workers_thread_count; ++t) {
    //                if (__customer_starts[t] + off < customer_ends[t]) {
    //                    dummy += *(__customer_starts[t] + off);
    //                }
    //            }
    //        }
    //        g_dummy_prevent_optimize = dummy;

    //        DEBUG("pre-fault customer done. dummy = %u", dummy);
    //    }).detach();
    //}

    const char* const end = __customer_starts[tid + 1];
    TRACE("[%u] customer [%p,%p)", tid, start, end);

    uint32_t last_custkey = 0;
    const char* p = start;
    while (p < end) {
        uint32_t custkey = 0;
        while (*p != '|') {
            ASSERT(*p >= '0' && *p <= '9', "Unexpected char: %c", *p);
            custkey = custkey * 10 + (*p - '0');
            ++p;
        }
        ++p;  // skip '|'
        ASSERT(custkey < g_custkey_to_mktsegment.size(), "Unexpected custkey (too large): %u", custkey);

        char str_mktsegment[16];
        int mktsegment_pos = 0;
        while (*p != '\n') {
            str_mktsegment[mktsegment_pos++] = *p;
            ++p;
        }
        ++p;  // skip '\n'
        str_mktsegment[mktsegment_pos] = '\0';

        const auto& it = g_string_to_mktsegment.find(str_mktsegment);
        if (it != g_string_to_mktsegment.end()) {
            g_custkey_to_mktsegment[custkey] = it->second;
        }
        else {
            g_custkey_to_mktsegment[custkey] = 0;
        }

        //DEBUG("%lu -> %s", custkey, mktsegment);
        //TODO: remove these 2 lines...
        ASSERT(last_custkey == 0 || custkey == last_custkey + 1, "custkey: %u, last_custkey: %u", custkey, last_custkey);
        last_custkey = custkey;
    }
}
*/



void worker_load_customer_multi_part(const uint32_t tid)
{
    mapped_file_part_t part;
    while (g_mapped_customer_queue.pop(part)) {
        const char* p = part.start;
        const char* end = part.desired_end;
        if (!part.is_first) {
            while (*p != '\n') ++p;
            ++p;
        }
        if (!part.is_last) {
            while (*end != '\n') ++end;
            ++end;
        }

        //uint32_t last_custkey = 0;
        while (p < end) {
            uint32_t custkey = 0;
            while (*p != '|') {
                ASSERT(*p >= '0' && *p <= '9', "Unexpected char: %c", *p);
                custkey = custkey * 10 + (*p - '0');
                ++p;
            }
            ++p;  // skip '|'
            ASSERT(custkey < g_upbound_custkey, "Unexpected custkey (too large): %u", custkey);

            char str_mktsegment[16];
            int mktsegment_pos = 0;
            while (*p != '\n') {
                str_mktsegment[mktsegment_pos++] = *p;
                ++p;
            }
            ++p;  // skip '\n'
            str_mktsegment[mktsegment_pos] = '\0';

            const auto& it = g_string_to_mktsegment.find(str_mktsegment);
            if (it != g_string_to_mktsegment.end()) {
                g_custkey_to_mktsegment[custkey] = it->second;
            }
            else {
                g_custkey_to_mktsegment[custkey] = 0;
            }

            //DEBUG("%lu -> %s", custkey, mktsegment);
            //TODO: remove these 2 lines...
            //ASSERT(last_custkey == 0 || custkey == last_custkey + 1, "custkey: %u, last_custkey: %u", custkey, last_custkey);
            //last_custkey = custkey;
        }

        //C_CALL(munmap((void*)part.start, part.map_size));
    }
}



void worker_load_orders_multi_part(const uint32_t tid)
{
    mapped_file_part_t part;
    while (g_mapped_orders_queue.pop(part)) {
        const char* p = part.start;
        const char* end = part.desired_end;
        if (!part.is_first) {
            while (*p != '\n') ++p;
            ++p;
        }
        if (!part.is_last) {
            while (*end != '\n') ++end;
            ++end;
        }

        while (p < end) {
            uint32_t orderkey = 0;
            while (*p != '|') {
                ASSERT(*p >= '0' && *p <= '9', "Unexpected char: %c", *p);
                orderkey = orderkey * 10 + (*p - '0');
                ++p;
            }
            ++p;  // skip '|'
            ASSERT(orderkey < g_upbound_orderkey, "Unexpected orderkey (too large): %u", orderkey);

            uint32_t custkey = 0;
            while (*p != '|') {
                ASSERT(*p >= '0' && *p <= '9', "Unexpected char: %c", *p);
                custkey = custkey * 10 + (*p - '0');
                ++p;
            }
            ++p;  // skip '|'

            const uint8_t mktsegment = g_custkey_to_mktsegment[custkey];
            g_orderkey_to_order[orderkey].parse(p, mktsegment);
            p += 10;  // skip 'yyyy-MM-dd'

            ASSERT(*p == '\n', "Expect EOL");
            p += 1;  // skip '\n'
        }

        //C_CALL(munmap((void*)part.start, part.map_size));
    }
}



FORCEINLINE const char* skip_one_orderkey_in_lineitem(const char* p)
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
        ASSERT(orderkey < g_upbound_orderkey, "Unexpected orderkey (too large): %u", orderkey);

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


void worker_load_lineitem_multi_part(const uint32_t tid)
{
    mapped_file_part_t part;
    while (g_mapped_lineitem_queue.pop(part)) {
        ASSERT(part.start != nullptr, "[%u] BUG: pop: part.start == nullptr", tid);
        ASSERT(part.desired_end != nullptr, "[%u] BUG: pop: part.desired_end == nullptr", tid);

        const char* p = part.start;
        const char* end = part.desired_end;
        DEBUG("[%u] load lineitem: (original) [%p, %p)", tid, p, end);
        if (!part.is_first) {
            while (*p != '\n') ++p;
            ++p;

            p = skip_one_orderkey_in_lineitem(p);
        }
        if (!part.is_last) {
            while (*end != '\n') ++end;
            ++end;

            end = skip_one_orderkey_in_lineitem(end);
        }

        DEBUG("[%u] load lineitem: [%p, %p)", tid, p, end);

        struct lineitem_t {
            uint32_t expend_cent;
            date_t shipdate;
        };
        lineitem_t curr_items[32];
        uint32_t curr_item_count = 0;
        uint32_t curr_orderkey = (uint32_t)-1;
        date_t curr_orderdate;


        const auto query_by_same_orderkey = [&]() {
            if (LIKELY(curr_orderkey != (uint32_t)-1)) {
                ASSERT(curr_item_count > 0, "BUG... curr_item_count is 0");

                const uint8_t mktsegment = curr_orderdate.mktsegment();
                for (query_t& query : g_queries_by_mktsegment[mktsegment]) {
                    if (!(curr_orderdate < query.q_orderdate)) break;
                    uint32_t total_expend_cent = 0;
                    for (uint32_t i = 0; i < curr_item_count; ++i) {
                        if (curr_items[i].shipdate > query.q_shipdate) {
                            total_expend_cent += curr_items[i].expend_cent;
                        }
                    }

                    if (total_expend_cent > 0) {  // TODO: suppose no order will have price = 0.00
                        std::vector<query_result_t>& results = query.results[tid];
                        if (results.size() < query.q_limit) {
                            results.emplace_back(total_expend_cent, curr_orderkey, curr_orderdate);
                            if (results.size() == query.q_limit) {
                                std::make_heap(results.begin(), results.end(), std::greater<query_result_t>());
                            }
                        }
                        else {
                            if (UNLIKELY(total_expend_cent > results.begin()->total_expend_cent)) {
                                std::pop_heap(results.begin(), results.end(), std::greater<query_result_t>());
                                *results.rbegin() = query_result_t(total_expend_cent, curr_orderkey, curr_orderdate);
                                std::push_heap(results.begin(), results.end(), std::greater<query_result_t>());
                            }
                        }
                    }
                }
            }
        };

        while (p < end) {
            uint32_t orderkey = 0;
            while (*p != '|') {
                ASSERT(*p >= '0' && *p <= '9', "Unexpected char: %c", *p);
                orderkey = orderkey * 10 + (*p - '0');
                ++p;
            }
            ++p;  // skip '|'
            ASSERT(orderkey < g_upbound_orderkey, "Unexpected orderkey (too large): %u", orderkey);

            if (orderkey != curr_orderkey) {
                query_by_same_orderkey();

                curr_orderkey = orderkey;
                curr_orderdate = g_orderkey_to_order[orderkey];
                curr_item_count = 0;
            }

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
            curr_items[curr_item_count].expend_cent = expend_cent;

            date_t shipdate;
            shipdate.parse(p, /*dummy*/0x00);
            p += 10;  // skip 'yyyy-MM-dd'
            curr_items[curr_item_count].shipdate = shipdate;

            ASSERT(*p == '\n', "Expect EOL");
            p += 1;  // skip '\n'
            ++curr_item_count;
        }
        query_by_same_orderkey();

        //C_CALL(munmap((void*)part.start, part.map_size));
    }
}


void worker_final_synthesis(const uint32_t /*tid*/)
{
    while (true) {
        const uint32_t query_idx = g_synthesis_query_count++;
        if (query_idx >= g_query_count) return;

        const uint32_t mktsegment = g_all_queries[query_idx].first;
        const uint32_t bucket_index = g_all_queries[query_idx].second;
        query_t& query = g_queries_by_mktsegment[mktsegment][bucket_index];
        auto& final_result = query.final_result;

        for (uint32_t t = 0; t < g_workers_thread_count; ++t) {
            for (const query_result_t& result : query.results[t]) {
                if (final_result.size() < query.q_limit) {
                    final_result.emplace_back(result);
                    if (final_result.size() == query.q_limit) {
                        std::make_heap(final_result.begin(), final_result.end(), std::greater<query_result_t>());
                    }
                }
                else {
                    if (UNLIKELY(result > *final_result.begin())) {
                        std::pop_heap(final_result.begin(), final_result.end(), std::greater<query_result_t>());
                        *final_result.rbegin() = result;
                        std::push_heap(final_result.begin(), final_result.end(), std::greater<query_result_t>());
                    }
                }
            }
        }

        std::sort(final_result.begin(), final_result.end(), std::greater<query_result_t>());
    }
}



void fn_worker_thread(const uint32_t tid)
{
    DEBUG("[%u] worker started", tid);

    //g_workers_barrier_external.sync();  // sync.ext@0
    {
        timer tmr;
        DEBUG("[%u] worker_load_customer(): starts", tid);
        worker_load_customer_multi_part(tid);
        DEBUG("[%u] worker_load_customer(): %.3lf msec", tid, tmr.elapsed_msec());
    }

    g_workers_barrier.sync();  // sync
    {
        timer tmr;
        DEBUG("[%u] worker_load_orders(): starts", tid);
        worker_load_orders_multi_part(tid);
        DEBUG("[%u] worker_load_orders(): %.3lf msec", tid, tmr.elapsed_msec());
    }

    g_workers_barrier.sync();  // sync
    {
        timer tmr;
        DEBUG("[%u] worker_load_lineitem(): starts", tid);
        worker_load_lineitem_multi_part(tid);
        DEBUG("[%u] worker_load_lineitem(): %.3lf msec", tid, tmr.elapsed_msec());
    }

    g_workers_barrier.sync();  // sync
    {
        timer tmr;
        DEBUG("[%u] worker final synthesis: starts", tid);
        worker_final_synthesis(tid);
        DEBUG("[%u] worker final synthesis: %.3lf msec", tid, tmr.elapsed_msec());
    }

    DEBUG("[%u] worker done", tid);
}



static std::atomic_bool __orders_allocated { false };
static std::atomic_uint64_t __customer_offset { 0 };
static std::atomic_uint64_t __orders_offset { 0 };
static std::atomic_uint64_t __lineitem_offset { 0 };

#if !defined(MAP_UNINITIALIZED)
#define MAP_UNINITIALIZED 0x4000000
#endif


void fn_loader_thread(const uint32_t tid)
{
    DEBUG("[%u] loader started", tid);

    constexpr uint64_t customer_step_size = 1048576 * 4;
    constexpr uint64_t orders_step_size = 1048576 * 16;
    constexpr uint64_t lineitem_step_size = 1048576 * 16;

    if (tid == 0) {
        // Open files
        open_file(g_customer_filepath, &g_customer_filesize, &g_customer_fd);
        open_file(g_orders_filepath, &g_orders_filesize, &g_orders_fd);
        open_file(g_lineitem_filepath, &g_lineitem_filesize, &g_lineitem_fd);

        // Table customer queue init
        g_mapped_customer_queue.init(g_customer_filesize / customer_step_size + 1);

        // Table orders queue init
        g_mapped_orders_queue.init(g_orders_filesize / orders_step_size + 1);

        // Table lineitem queue init
        g_mapped_lineitem_queue.init(g_lineitem_filesize / lineitem_step_size + 1);

        // Allocate memory for table customer
        g_upbound_custkey = g_customer_filesize / 15;  // TODO: estimated max customer key
        g_custkey_to_mktsegment = (uint8_t*)mmap(
            nullptr,
            g_upbound_custkey * sizeof(uint8_t),
            PROT_READ | PROT_WRITE,
            MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE | MAP_UNINITIALIZED,
            -1,
            0);
        ASSERT(g_custkey_to_mktsegment != MAP_FAILED, "g_custkey_to_mktsegment mmap() failed");
        DEBUG("g_custkey_to_mktsegment: %p", g_custkey_to_mktsegment);
        //preload_memory_range(g_custkey_to_mktsegment, g_upbound_custkey * sizeof(uint8_t));

        //g_workers_barrier_external.sync();  // sync.ext@0
    }
    g_loaders_barrier.sync();

    open_file_mapping_multi_part(
        g_customer_filepath,
        tid,
        __customer_offset,
        g_customer_fd,
        g_customer_filesize,
        customer_step_size,
        g_mapped_customer_queue);

    // Allocate memory for table orders
    bool expected = false;
    if (__orders_allocated.compare_exchange_strong(expected, true)) {
        g_upbound_orderkey = g_lineitem_filesize / 20;  // TODO: estimated max order key by lineitem file
        const uint64_t alloc_size = g_upbound_orderkey * sizeof(date_t);
        DEBUG("[%u] allocating g_upbound_orderkey... (size: %" PRIu64 ")", tid, alloc_size);

        g_orderkey_to_order = (date_t*)mmap(
            nullptr,
            alloc_size,
            PROT_READ | PROT_WRITE,
            MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE,  // MAP_UNINITIALIZED must NOT be specified
            -1,
            0);
        ASSERT(g_orderkey_to_order != MAP_FAILED, "g_orderkey_to_order mmap() failed");

        //g_orderkey_to_order = (date_t*)aligned_alloc(1024 * 1024 * 2, alloc_size);
        //ASSERT(g_orderkey_to_order != nullptr, "g_orderkey_to_order aligned_alloc() failed");
        //C_CALL(madvise(
        //    g_orderkey_to_order,
        //    alloc_size,
        //    MADV_HUGEPAGE | MADV_SEQUENTIAL | MADV_WILLNEED));
        //preload_memory_range/*_2M*/(g_orderkey_to_order, alloc_size);

        DEBUG("[%u] allocated g_orderkey_to_order: %p", tid, g_orderkey_to_order);
    }

    g_loaders_barrier.sync();
    if (tid == 0) {
        g_mapped_customer_queue.mark_finish_push();
    }

    open_file_mapping_multi_part(
        g_orders_filepath,
        tid,
        __orders_offset,
        g_orders_fd,
        g_orders_filesize,
        orders_step_size,
        g_mapped_orders_queue);
    g_loaders_barrier.sync();
    if (tid == 0) {
        g_mapped_orders_queue.mark_finish_push();
    }

    open_file_mapping_multi_part(
        g_lineitem_filepath,
        tid,
        __lineitem_offset,
        g_lineitem_fd,
        g_lineitem_filesize,
        lineitem_step_size,
        g_mapped_lineitem_queue);
    g_loaders_barrier.sync();
    if (tid == 0) {
        g_mapped_lineitem_queue.mark_finish_push();
    }

    DEBUG("[%u] loader done", tid);
}


int main(int argc, char* argv[])
{
    //C_CALL(mlockall(MCL_FUTURE));

    //
    // Initializations
    //
    g_customer_filepath = argv[1];
    g_orders_filepath = argv[2];
    g_lineitem_filepath = argv[3];

    g_workers_thread_count = std::thread::hardware_concurrency();
    if (g_workers_thread_count > MAX_WORKER_THREADS) g_workers_thread_count = MAX_WORKER_THREADS;
    INFO("g_workers_thread_count: %u", g_workers_thread_count);

    g_workers_barrier.init(g_workers_thread_count);
    g_workers_barrier_external.init(g_workers_thread_count + 1);

    g_loaders_thread_count = std::thread::hardware_concurrency();
    if (g_loaders_thread_count > MAX_LOADER_THREADS) g_loaders_thread_count = MAX_LOADER_THREADS;
    INFO("g_loaders_thread_count: %u", g_loaders_thread_count);

    g_loaders_barrier.init(g_loaders_thread_count);


    //
    // Load queries
    //
    {
        [[maybe_unused]] timer tmr;
        load_queries(argc, argv);
        DEBUG("all query loaded (%.3lf msec)", tmr.elapsed_msec());
    }


    //
    // Create loader threads
    //
    for (uint32_t tid = 0; tid < g_loaders_thread_count; ++tid) {
        g_loaders.emplace_back(fn_loader_thread, tid);
        //g_loaders.rbegin()->detach();
    }

    //
    // Create worker threads
    //
    for (uint32_t tid = 0; tid < g_workers_thread_count; ++tid) {
        g_workers.emplace_back(fn_worker_thread, tid);
    }


    //
    // Wait for all loader and worker threads
    //
    for (std::thread& thr : g_loaders) thr.join();
    INFO("all %u loaders done", g_workers_thread_count);

    for (std::thread& thr : g_workers) thr.join();
    INFO("all %u workers done", g_workers_thread_count);


    //
    // Print results
    //
    for (uint32_t query_idx = 0; query_idx < g_query_count; ++query_idx) {
        const uint32_t mktsegment = g_all_queries[query_idx].first;
        const uint32_t bucket_index = g_all_queries[query_idx].second;
        query_t& query = g_queries_by_mktsegment[mktsegment][bucket_index];

        TRACE("printing query#%u: mktsegment=%u", query.query_index, query.q_mktsegment);
        fprintf(stdout, "l_orderkey|o_orderdate|revenue\n");
        for (const query_result_t& result : query.final_result) {
            fprintf(stdout, "%u|%04u-%02u-%02u|%u.%02u\n",
                result.orderkey,
                result.orderdate.year(), result.orderdate.month(), result.orderdate.day(),
                result.total_expend_cent / 100, result.total_expend_cent % 100);
        }
    }
    fflush(stdout);

    return 0;
}
