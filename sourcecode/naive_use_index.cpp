#include "common.h"

struct query_result_t
{
    date_t orderdate;
    uint32_t orderkey;
    uint32_t total_expend_cent;

    FORCEINLINE bool operator >(const query_result_t& other) const noexcept {
        if (total_expend_cent > other.total_expend_cent) return true;
        if (total_expend_cent < other.total_expend_cent) return false;
        return (orderkey > other.orderkey);
    }
};

struct query_t
{
    uint8_t q_mktid;
    date_t q_orderdate;
    date_t q_shipdate;
    uint32_t q_topn;

    std::vector<query_result_t> result;
};


namespace
{
    int g_mktsegment_fd = -1;
    uint64_t g_mktsegment_file_size = 0;
    uint8_t g_mktid_count = 0;
    std::unordered_map<std::string, uint8_t> g_mktsegment_to_mktid { };

    int g_prefixsum_fd = -1;
    uint64_t g_prefixsum_file_size = 0;

    int g_items_fd = -1;
    uint64_t g_items_file_size = 0;

    uint64_t* g_index_items = nullptr;
    uint32_t* g_index_prefixsum = nullptr;

    std::vector<query_t> g_queries { };
    done_event* g_queries_done = nullptr;
    std::atomic_uint32_t g_queries_curr { 0 };
}



FORCEINLINE size_t calc_bucket_index(const uint8_t mktid, const date_t orderdate) noexcept
{
    ASSERT(mktid < g_mktid_count, "BUG");
    ASSERT(orderdate >= MIN_TABLE_DATE, "BUG");
    ASSERT(orderdate <= MAX_TABLE_DATE, "BUG");

    return (size_t)(mktid - 0) * (MAX_TABLE_DATE - MIN_TABLE_DATE + 1) + (orderdate - MIN_TABLE_DATE);
}


template <typename DifferenceT>
DifferenceT heap_parent(DifferenceT k)
{
    return (k - 1) / 2;
}

template <typename DifferenceT>
DifferenceT heap_left(DifferenceT k)
{
    return 2 * k + 1;
}

template<typename RandomIt, typename Compare = std::less<>>
void replace_heap(RandomIt first, RandomIt last, Compare comp = Compare())
{
    auto const size = last - first;
    if (size <= 1)
        return;
    typename std::iterator_traits<RandomIt>::difference_type k = 0;
    auto e = std::move(first[k]);
    auto const max_k = heap_parent(size - 1);
    while (k <= max_k) {
        auto max_child = heap_left(k);
        if (max_child < size - 1 && comp(first[max_child], first[max_child + 1]))
            ++max_child; // Go to right sibling.
        if (!comp(e, first[max_child]))
            break;
        first[k] = std::move(first[max_child]);
        k = max_child;
    }

    first[k] = std::move(e);
}


void use_index_initialize() noexcept
{
    // Open index files
    {
        const auto &open_file = [](const char *const path, int *const fd, uint64_t *const file_size) noexcept {
            *fd = C_CALL(openat(g_index_directory_fd, path, O_RDONLY | O_CLOEXEC));
            struct stat64 st;
            C_CALL(fstat64(*fd, &st));
            *file_size = st.st_size;
            TRACE("openat %s: fd = %d, size = %lu", path, *fd, *file_size);
        };

        open_file("mktsegment", &g_mktsegment_fd, &g_mktsegment_file_size);
        open_file("prefixsum", &g_prefixsum_fd, &g_prefixsum_file_size);
        open_file("items", &g_items_fd, &g_items_file_size);
    }

    // Load index files: load mktsegment
    {
        const void* ptr = mmap_parallel(g_mktsegment_file_size, 1, PROT_READ, MAP_PRIVATE, g_mktsegment_fd);
        const char* p = (const char*)ptr;
        g_mktid_count = *(uint8_t*)(p++);
        for (uint8_t mktid = 0; mktid < g_mktid_count; ++mktid) {
            const uint8_t length = *(uint8_t*)(p++);
            g_mktsegment_to_mktid[std::string(p, length)] = mktid;
            DEBUG("loaded mktsegment: %.*s -> %u", length, p, mktid);
            p += length;
        }
    }

    // Load queries
    {
        g_queries_done = new done_event[g_query_count];

        g_queries.resize(g_query_count);
        for (uint32_t q = 0; q < g_query_count; ++q) {
            const auto it = g_mktsegment_to_mktid.find(g_argv_queries[4 * q + 0]);
            ASSERT(it != g_mktsegment_to_mktid.end(), "TODO: deal with unknown mktsegment in query");  // TODO

            query_t& query = g_queries[q];
            query.q_mktid = it->second;
            query.q_orderdate = date_from_string(g_argv_queries[4 * q + 1]);
            query.q_shipdate = date_from_string(g_argv_queries[4 * q + 2]);
            query.q_topn = (uint32_t)std::strtoul(g_argv_queries[4 * q + 3], nullptr, 10);

            DEBUG("query #%u: q_mktid=%u,q_orderdate=%u,q_shipdate=%u,q_topn=%u",
                q, query.q_mktid, query.q_orderdate, query.q_shipdate, query.q_topn);

            query.result.reserve(query.q_topn);
        }
    }


    // Load index files: load prefixsum
    {
        ASSERT(g_prefixsum_file_size == sizeof(uint32_t) * g_mktid_count * (MAX_TABLE_DATE - MIN_TABLE_DATE + 1),
            "BUG: broken index?");

        g_index_prefixsum = (uint32_t*)mmap_parallel(
            g_prefixsum_file_size,
            1,
            PROT_READ,
            MAP_PRIVATE,
            g_prefixsum_fd);
        DEBUG("g_index_prefixsum: %p", g_index_prefixsum);
    }

    // Load index files: load items
    {
        ASSERT(g_items_file_size >= sizeof(uint64_t) * g_index_prefixsum[g_mktid_count * (MAX_TABLE_DATE - MIN_TABLE_DATE + 1) - 1],
            "BUG: broken index?");

        /*
        g_index_items = (uint64_t*)mmap_parallel(
            g_items_file_size,
            g_loader_thread_count,
            PROT_READ,
            MAP_PRIVATE,
            g_items_fd);
        */

        g_index_items = (uint64_t*)my_mmap(
            g_items_file_size,
            PROT_READ,
            MAP_PRIVATE,
            g_items_fd,
            0);

        DEBUG("g_index_items: %p", g_index_items);
    }

}


void fn_worker_thread_use_index(const uint32_t tid) noexcept
{
    while (true) {
        const uint32_t query_id = g_queries_curr++;
        if (query_id >= g_query_count) break;
        query_t &query = g_queries[query_id];

        // NOTE: very carefully deal with q_orderdate
        if (LIKELY(query.q_orderdate > MIN_TABLE_DATE)) {
            const date_t scan_from_orderdate = date_subtract_bounded_to_min_table_date(query.q_shipdate, 121 - 1);
            const date_t scan_to_orderdate = date_subtract_bounded_to_min_table_date(query.q_orderdate, 1);
            DEBUG("[%u] query #%u: scan_from_orderdate=%u, scan_from_orderdate=%u",
                  tid, query_id, scan_from_orderdate, scan_to_orderdate);

            for (date_t orderdate = scan_from_orderdate; orderdate <= scan_to_orderdate; ++orderdate) {
                const uint32_t bucket = calc_bucket_index(query.q_mktid, orderdate);

                const uint32_t scan_begin_index = (bucket == 0) ? 0 : g_index_prefixsum[bucket - 1];
                const uint32_t scan_end_index = g_index_prefixsum[bucket];
                DEBUG("[%u] query #%u: orderdate=%u, scan_begin_index=%u, scan_end_index=%u",
                      tid, query_id, orderdate, scan_begin_index, scan_end_index);

                uint32_t last_orderkey = (uint32_t) g_index_items[scan_begin_index];  // low 32 bit: orderkey
                uint32_t last_total_expend_cent = 0;
                const auto maybe_update_topn = [&]() {
                    if (UNLIKELY(last_total_expend_cent == 0)) return;

                    query_result_t tmp;
                    tmp.orderdate = orderdate;
                    tmp.orderkey = last_orderkey;
                    tmp.total_expend_cent = last_total_expend_cent;

                    if (query.result.size() < query.q_topn) {
                        query.result.emplace_back(std::move(tmp));
                        if (query.result.size() == query.q_topn) {
                            std::make_heap(query.result.begin(), query.result.end(), std::greater<query_result_t>());
                        }
                    }
                    else {
                        if (UNLIKELY(last_total_expend_cent > query.result.begin()->total_expend_cent)) {
                            //std::pop_heap(query.result.begin(), query.result.end(), std::greater<query_result_t>());
                            //*query.result.rbegin() = tmp;
                            //std::push_heap(query.result.begin(), query.result.end(), std::greater<query_result_t>());
                            *query.result.begin() = tmp;
                            replace_heap(query.result.begin(), query.result.end(), std::greater<query_result_t>());
                        }
                    }
                };

                for (uint32_t i = scan_begin_index; i < scan_end_index; ++i) {
                    const uint64_t value = g_index_items[i];
                    const date_t shipdate = orderdate + (value >> 56);
                    if (shipdate > query.q_shipdate) {
                        const uint32_t expend_cent = (value >> 32) & 0x00FFFFFF;
                        const uint32_t orderkey = (uint32_t) value;
                        if (orderkey == last_orderkey) {
                            last_total_expend_cent += expend_cent;
                        }
                        else {  // orderkey != last_orderkey
                            maybe_update_topn();
                            last_orderkey = orderkey;
                            last_total_expend_cent = expend_cent;
                        }
                    }
                }
                maybe_update_topn();
            }
        }

        DEBUG("[%u] query #%u done", tid, query_id);
        g_queries_done[query_id].mark_done();
    }
}


void main_thread_use_index() noexcept
{
    for (uint32_t query_id = 0; query_id < g_query_count; ++query_id) {
        g_queries_done[query_id].wait_done();

        // print query
        query_t& query = g_queries[query_id];
        std::sort(query.result.begin(), query.result.end(), std::greater<query_result_t>());
        printf("l_orderkey|o_orderdate|revenue\n");
        for (const query_result_t& line : query.result) {
            const auto ymd = date_get_ymd(line.orderdate);
            printf("%u|%u-%02u-%02u|%u.%02u\n",
                line.orderkey,
                std::get<0>(ymd), std::get<1>(ymd), std::get<2>(ymd),
                line.total_expend_cent / 100, line.total_expend_cent % 100);
        }
    }
}
