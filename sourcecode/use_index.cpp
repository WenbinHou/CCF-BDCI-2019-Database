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

    bool is_unknown_mktsegment;
    std::vector<query_result_t> result;
};

struct partial_index_t
{
    int items_fd = -1;
    uint64_t items_file_size = 0;
    void* items_ptr = nullptr;

    int endoffset_fd = -1;
    uint64_t endoffset_file_size = 0;
    uint64_t* endoffset_ptr = nullptr;
};


namespace
{
    int g_mktsegment_fd = -1;
    uint64_t g_mktsegment_file_size = 0;
    uint8_t g_mktid_count = 0;
    std::unordered_map<std::string, uint8_t> g_mktsegment_to_mktid { };

    std::vector<query_t> g_queries { };
    done_event* g_queries_done = nullptr;
    std::atomic_uint32_t g_queries_curr { 0 };

    partial_index_t* g_partial_indices = nullptr;
    done_event g_partial_index_loaded { };
}



FORCEINLINE size_t calc_bucket_index(const uint8_t mktid, const date_t orderdate) noexcept
{
    ASSERT(mktid < g_mktid_count);
    ASSERT(orderdate >= MIN_TABLE_DATE);
    ASSERT(orderdate <= MAX_TABLE_DATE);

    return (size_t)(mktid - 0) * (MAX_TABLE_DATE - MIN_TABLE_DATE + 1) + (orderdate - MIN_TABLE_DATE);
}



void use_index_initialize() noexcept
{
    // Open index files
    {
        {
            int count_fd;
            uint64_t count_file_size;
            openat_file("meta", &count_fd, &count_file_size);
            const size_t cnt = C_CALL(read(count_fd, &g_meta, sizeof(g_meta)));
            CHECK(cnt == sizeof(g_meta));
            C_CALL(close(count_fd));

            INFO("g_meta.partial_index_count: %u", g_meta.partial_index_count);
            INFO("g_meta.max_shipdate_orderdate_diff: %u", g_meta.max_shipdate_orderdate_diff);
            g_partial_indices = new partial_index_t[g_meta.partial_index_count];
        }

        openat_file("mktsegment", &g_mktsegment_fd, &g_mktsegment_file_size);
    }

    // Load index files: load mktsegment
    {
        char buffer[g_mktsegment_file_size];
        const size_t cnt = C_CALL(read(g_mktsegment_fd, buffer, g_mktsegment_file_size));
        CHECK(cnt == g_mktsegment_file_size);

        const char* p = (const char*)buffer;
        g_mktid_count = *(uint8_t*)(p++);
        for (uint8_t mktid = 0; mktid < g_mktid_count; ++mktid) {
            const uint8_t length = *(uint8_t*)(p++);
            INFO("length: %u", length);
            g_mktsegment_to_mktid[std::string(p, length)] = mktid;
            INFO("loaded mktsegment: %.*s -> %u", length, p, mktid);
            p += length;
        }
    }

    // Load queries
    {
        g_queries_done = new done_event[g_query_count];

        g_queries.resize(g_query_count);
        for (uint32_t q = 0; q < g_query_count; ++q) {
            query_t& query = g_queries[q];

            const auto it = g_mktsegment_to_mktid.find(g_argv_queries[4 * q + 0]);
            if (UNLIKELY(it == g_mktsegment_to_mktid.end())) {
                query.is_unknown_mktsegment = true;
                DEBUG("query #%u: unknown mktsegment: %s", q, g_argv_queries[4 * q + 0]);
                continue;
            }

            query.q_mktid = it->second;
            query.q_orderdate = date_from_string(g_argv_queries[4 * q + 1]);
            query.q_shipdate = date_from_string(g_argv_queries[4 * q + 2]);
            query.q_topn = (uint32_t)std::strtoul(g_argv_queries[4 * q + 3], nullptr, 10);

            DEBUG("query #%u: q_mktid=%u,q_orderdate=%u,q_shipdate=%u,q_topn=%u",
                  q, query.q_mktid, query.q_orderdate, query.q_shipdate, query.q_topn);

            query.result.reserve(query.q_topn);
        }
    }
}


void fn_worker_thread_use_index([[maybe_unused]] const uint32_t tid) noexcept
{
    g_partial_index_loaded.wait_done();

    while (true) {
        const uint32_t query_id = g_queries_curr++;
        if (query_id >= g_query_count) break;
        query_t &query = g_queries[query_id];

        // NOTE: very carefully deal with q_orderdate
        if (LIKELY(!query.is_unknown_mktsegment && query.q_orderdate > MIN_TABLE_DATE)) {
            const date_t scan_from_orderdate = date_subtract_bounded_to_min_table_date(query.q_shipdate, g_meta.max_shipdate_orderdate_diff - 1);
            const date_t scan_to_orderdate = date_subtract_bounded_to_min_table_date(query.q_orderdate, 1);
            DEBUG("[%u] query #%u: scan_from_orderdate=%u, scan_from_orderdate=%u",
                  tid, query_id, scan_from_orderdate, scan_to_orderdate);

            for (date_t orderdate = scan_from_orderdate; orderdate <= scan_to_orderdate; ++orderdate) {
                const uint32_t bucket = calc_bucket_index(query.q_mktid, orderdate);

                for (uint32_t partial_id = 0; partial_id < g_meta.partial_index_count; ++partial_id) {

                    const partial_index_t& partial = g_partial_indices[partial_id];
                    const uintptr_t scan_begin_offset = (uintptr_t)bucket * CONFIG_INDEX_SPARSE_SIZE_PER_BUCKET;
                    const uintptr_t scan_end_offset = (uintptr_t)partial.endoffset_ptr[bucket];
                    DEBUG("[%u] query #%u: orderdate=%u, partial_id=%u,scan_begin_offset=0x%lx, scan_end_offset=0x%lx",
                        tid, query_id, orderdate, partial_id, scan_begin_offset, scan_end_offset);

                    uint32_t total_expend_cent = 0;
                    const auto maybe_update_topn = [&](const uint32_t orderkey) {
                        if (total_expend_cent == 0) {
                            return;
                        }

                        query_result_t tmp;
                        tmp.orderdate = orderdate;
                        tmp.orderkey = orderkey;
                        tmp.total_expend_cent = total_expend_cent;

                        if (query.result.size() < query.q_topn) {
                            query.result.emplace_back(std::move(tmp));
                            if (query.result.size() == query.q_topn) {
                                std::make_heap(query.result.begin(), query.result.end(), std::greater<>());
                            }
                        }
                        else {
                            if (UNLIKELY(total_expend_cent > query.result.begin()->total_expend_cent)) {
                                std::pop_heap(query.result.begin(), query.result.end(), std::greater<>());
                                *query.result.rbegin() = tmp;
                                std::push_heap(query.result.begin(), query.result.end(), std::greater<>());
                            }
                        }
                    };

                    for (uint64_t off = scan_begin_offset; off < scan_end_offset; off += 4) {
                        const uint32_t value = *(uint32_t*)((uintptr_t)partial.items_ptr + off);
                        if (value & 0x80000000) {  // This is orderkey
                            const uint32_t orderkey = value & ~0x80000000;
                            maybe_update_topn(orderkey);
                            total_expend_cent = 0;
                        }
                        else {
                            const date_t shipdate = orderdate + (value >> 24);
                            if (shipdate > query.q_shipdate) {
                                const uint32_t expend_cent = value & 0x00FFFFFF;
                                ASSERT(expend_cent > 0);
                                total_expend_cent += expend_cent;
                            }
                        }
                    }
                }
            }
        }

        DEBUG("[%u] query #%u done", tid, query_id);
        g_queries_done[query_id].mark_done();
    }
}


void fn_loader_thread_use_index([[maybe_unused]] const uint32_t tid) noexcept
{
    // Load partial index
    {
        static std::atomic_uint32_t __curr_partial_id { 0 };
        while (true) {
            const uint32_t partial_id = __curr_partial_id++;
            if (partial_id >= g_meta.partial_index_count) break;

            char filename[32];
            partial_index_t& partial = g_partial_indices[partial_id];

            snprintf(filename, std::size(filename), "items_%u", partial_id);
            openat_file(filename, &partial.items_fd, &partial.items_file_size);
            partial.items_ptr = my_mmap(
                partial.items_file_size,
                PROT_READ,
                MAP_PRIVATE,  // DO NOT specify MAP_POPULATE!!
                partial.items_fd,
                0);
            DEBUG("g_partial_indices[%u].items_ptr = %p", partial_id, partial.items_ptr);

            snprintf(filename, std::size(filename), "endoffset_%u", partial_id);
            openat_file(filename, &partial.endoffset_fd, &partial.endoffset_file_size);
            partial.endoffset_ptr = (uint64_t*)my_mmap(
                partial.endoffset_file_size,
                PROT_READ,
                MAP_PRIVATE | MAP_POPULATE,
                partial.endoffset_fd,
                0);
            DEBUG("g_partial_indices[%u].endoffset_ptr = %p", partial_id, partial.endoffset_ptr);
        }
    }

    g_loader_sync_barrier.sync_and_run_once([]() {
        g_partial_index_loaded.mark_done();

    });

    g_loader_sync_barrier.sync_and_run_once([]() {
        for (uint32_t query_id = 0; query_id < g_query_count; ++query_id) {
            g_queries_done[query_id].wait_done();

            // print query
            query_t& query = g_queries[query_id];
            std::sort(query.result.begin(), query.result.end(), std::greater<>());
            printf("l_orderkey|o_orderdate|revenue\n");
            for (const query_result_t& line : query.result) {
                const auto ymd = date_get_ymd(line.orderdate);
                printf("%u|%u-%02u-%02u|%u.%02u\n",
                       line.orderkey,
                       std::get<0>(ymd), std::get<1>(ymd), std::get<2>(ymd),
                       line.total_expend_cent / 100, line.total_expend_cent % 100);
            }
        }
    });

}
