#include "common.h"


//==============================================================
// Global variables
//==============================================================
const std::chrono::steady_clock::time_point g_startup_time = std::chrono::steady_clock::now();
volatile uint64_t g_dummy_prevent_optimize;

std::vector<query_t> g_queries[256];  // query bucket by mktsegment
std::vector<query_t*> g_queries_by_id;
std::vector<uint8_t> g_custkey_to_mktsegment;
size_t g_thread_count;
iovec g_mapped_customer, g_mapped_orders, g_mapped_lineitem;


struct cstring_less
{
    bool operator() (const char* const str1, const char* const str2) const noexcept
    {
        return strcmp(str1, str2) < 0;
    }
};


struct lineitem_state_t
{
    const char* ptr = nullptr;
};

struct lineitem_t
{
    uint32_t order_key;
    uint32_t expend_cent;
    date_t ship_date;
};

static inline __attribute__((always_inline))
bool forward_lineitem_state(
    /*inout*/ lineitem_state_t& state,
    /*out*/ lineitem_t* item,
    const uint32_t expected_order_key)
{
    ASSERT(state.ptr != nullptr, "BUG...");
    const char* const end = (const char*)g_mapped_lineitem.iov_base + g_mapped_lineitem.iov_len;
    while (state.ptr < end) {
        //DEBUG(">>>>>>>>>>>> (expected_order_key=%u) %.*s", expected_order_key, 16, state.ptr);
        const char* const old_state_ptr = state.ptr;
        uint32_t order_key = 0;
        while (*state.ptr != '|') {
            ASSERT(*state.ptr >= '0' && *state.ptr <= '9', "Unexpected char: 0x%02x (%c)", *state.ptr, *state.ptr);
            order_key = order_key * 10 + (*state.ptr - '0');
            ++state.ptr;
        }
        ++state.ptr;  // skip '|'

        if (order_key < expected_order_key) {  // just skip this line
            while (*state.ptr != '\n') {
                ++state.ptr;
            }
            ++state.ptr;  // skip '\n'
        }
        else if (order_key == expected_order_key) {
            uint32_t expend_cent = 0;
            while (*state.ptr != '|') {
                if (*state.ptr != '.') {
                    ASSERT(*state.ptr >= '0' && *state.ptr <= '9', "Unexpected char: 0x%02x (%c)", *state.ptr, *state.ptr);
                    expend_cent = expend_cent * 10 + (*state.ptr - '0');
                }
                ++state.ptr;
            }
            ++state.ptr;  // skip '|'

            const date_t ship_date = parse_date(state.ptr);  // read yyyy-MM-dd
            state.ptr += 10;  // skip yyyy-MM-dd
            ASSERT(*state.ptr == '\n', "Expects EOL, but got %c", *state.ptr);
            ++state.ptr;

            item->order_key = order_key;
            item->expend_cent = expend_cent;
            item->ship_date = ship_date;

            return true;
        }
        else {  // order_key > expected_order_key
            // TODO: optimize out this! (save this in a space for future use)
            state.ptr = old_state_ptr;
            return false;
        }
    }
    return false;  // reached EOF
}


void find_start_address_in_lineitem(
    /*inout*/ lineitem_state_t& state,
    const uint32_t order_key)
{
    // Find first row in lineitem table, whose row.order_key >= order_key
    // Set `state.ptr` to start address of that line
    const char* const p = (const char*)g_mapped_lineitem.iov_base;
    
    const auto align_to_line = [&](const char* s) -> const char* {
        while (s > p) {
            if (s[-1] == '\n') break;
            --s;
        }
        return s;
    };
    
    const auto read_order_key = [](const char* s) -> uint32_t {
        uint32_t order_key = 0;
        while (*s != '|') {
            ASSERT(*s >= '0' && *s <= '9', "Unexpected char: 0x%02x (%c)", *s, *s);
            order_key = order_key * 10 + (*s - '0');
            ++s;
        }
        return order_key;
    };

    const char* p_start = (const char*)g_mapped_lineitem.iov_base;
    const char* p_end = (const char*)g_mapped_lineitem.iov_base + g_mapped_lineitem.iov_len;
    while (true) {
        const char* p_mid = align_to_line(p_start + (p_end - p_start) / 2);
        if (p_mid == p_start) break;
        const uint32_t mid_order_key = read_order_key(p_mid);
        if (mid_order_key >= order_key) {
            p_end = p_mid;
        }
        else {  // mid_order_key < order_key
            p_start = p_mid;
        }
    }

    DEBUG("found starting address in lineitem for order_key %u: %p >>> %.*s...", order_key, p_start, 20, p_start);
    state.ptr = p_start;


    // Create a thread to preload current part of lineitem
    iovec vec;
    vec.iov_base = (void*)p_start;
    vec.iov_len = std::min<size_t>(
        g_mapped_lineitem.iov_len / g_thread_count,
        g_mapped_lineitem.iov_len - ((uintptr_t)p_start - (uintptr_t)g_mapped_lineitem.iov_base));
    std::thread(preload_memory_range, vec).detach();
}


void load_order_part(const iovec part, const size_t thread_idx)
{
#if ENABLE_PROFILING
    const auto start_time = std::chrono::steady_clock::now();
#endif

    lineitem_state_t state { };

    /* The table looks like:
        order_key|cust_key|order_date

        599999943|454411|1996-03-31
        599999968|2271617|1995-12-14
        599999969|4063478|1994-04-20
        599999970|14864260|1993-10-19
        599999971|13602718|1992-02-18
        599999972|5864876|1994-03-09
        599999973|5956910|1996-03-04
        599999974|7780136|1997-06-24
        599999975|5730910|1994-04-05
        600000000|4400869|1997-12-02
    */
    const char* const p = (const char*)part.iov_base;

    for (uint64_t i = 0; i < part.iov_len; ++i) {
        uint32_t order_key = 0;
        while (p[i] != '|') {
            ASSERT(p[i] >= '0' && p[i] <= '9', "Unexpected char: 0x%02x (%c) (i=%llu)", p[i], p[i], i);
            order_key = order_key * 10 + (p[i] - '0');
            ++i;
        }
        ++i;  // skip '|'

        uint32_t custkey = 0;
        while (p[i] != '|') {
            ASSERT(p[i] >= '0' && p[i] <= '9', "Unexpected char: 0x%02x (%c) (i=%llu)", p[i], p[i], i);
            custkey = custkey * 10 + (p[i] - '0');
            ++i;
        }
        ++i;  // skip '|'

        const date_t order_date = parse_date(&p[i]);  // read yyyy-MM-dd
        i += 10;  // skip yyyy-MM-dd
        ASSERT(p[i] == '\n', "Expects EOL, but got %c", p[i]);

        //DEBUG("%u %u", order_key, cust_key);
        const uint8_t mktsegment_id = g_custkey_to_mktsegment[custkey];
        if (UNLIKELY(mktsegment_id == 0)) {  // we don't care about this customer (not queried)
            continue;
        }

        // (One time) init in lineitem
        if (UNLIKELY(state.ptr == nullptr)) {
            find_start_address_in_lineitem(state, order_key);
            ASSERT(state.ptr != nullptr, "BUG... find_start_address_in_lineitem() not found?!");
        }

        // Forward state.ptr until reaching lineitem.order_key > orders.order_key
        lineitem_t item;
        while (forward_lineitem_state(state, &item, order_key)) {
            // Found an item matching order_key
            //INFO("item: order_key=%u, expend_cent=%u", item.order_key, item.expend_cent);
            for (query_t& query : g_queries[mktsegment_id]) {
                ASSERT(query.mktsegment_id == mktsegment_id, "BUG...");
                if (order_date.value < query.order_date.value) {
                    //DEBUG("order_date: %04u-%02u-%02u < query_order_date: %04u-%02u-%02u",
                    //    order_date.year, order_date.month, order_date.day,
                    //    query.order_date.year, query.order_date.month, query.order_date.day);

                    if (item.ship_date.value > query.ship_date.value) {
                        query.tmp_total_expend_cent[thread_idx] += item.expend_cent;
                    }
                }
            }
        }

        for (query_t& query : g_queries[mktsegment_id]) {
            if (query.tmp_total_expend_cent[thread_idx] > 0) {
                bool do_insert = false;
                if (query.top_n[thread_idx].size() < query.limit_count) {
                    do_insert = true;
                }
                else {  // query.top_n[thread_idx].size() >= query.limit_count
                    if (query.top_n[thread_idx].top().total_expend_cent < query.tmp_total_expend_cent[thread_idx]) {
                        query.top_n[thread_idx].pop();
                        do_insert = true;
                    }
                }
                if (do_insert) {
                    query.top_n[thread_idx].emplace(order_key, order_date, query.tmp_total_expend_cent[thread_idx]);
                    //DEBUG("update query_%u top_n: total_expend_cent=%u", query.query_id, query.tmp_total_expend_cent[thread_idx]);
                }
                query.tmp_total_expend_cent[thread_idx] = 0;
            }
        }
    }

#if ENABLE_PROFILING
    const auto end_time = std::chrono::steady_clock::now();
    std::chrono::duration<double> sec = end_time - start_time;
    DEBUG("load order part_%u done: %.3lf sec", (unsigned)thread_idx, sec.count());
#endif
}


int main(int argc, char* argv[])
{
    //
    // Load queries
    //
    std::map<const char*, uint8_t, cstring_less> all_mktsegment;
    uint8_t max_mktsegment_id = 0;  // mktsegment: 1, 2, 3, ..., max_mktsegment_id
    const int query_count = std::atoi(argv[4]);
    {
        ASSERT(query_count >= 1, "Unexpected query count: %d", query_count);
        DEBUG("totally query count: %d", query_count);

        for (int query_id = 0; query_id < query_count; ++query_id) {
            const char* const mktsegment = argv[5 + query_id * 4 + 0];
            const char* const order_date = argv[5 + query_id * 4 + 1];
            const char* const ship_date = argv[5 + query_id * 4 + 2];
            const char* const limit_count = argv[5 + query_id * 4 + 3];

            uint8_t& mktsegment_id = all_mktsegment[mktsegment];
            if (mktsegment_id == 0) {
                mktsegment_id = ++max_mktsegment_id;
                DEBUG("found new mktsegment: %s (id: %u)", mktsegment, mktsegment_id);
            }

            query_t query;
            query.query_id = (uint8_t)query_id;
            query.mktsegment_id = mktsegment_id;
            query.order_date = parse_date(order_date);
            query.ship_date = parse_date(ship_date);
            query.limit_count = std::stoi(limit_count);
            TRACE("query: mktsegment_id=%u, order_date=%u-%u-%u, ship_date=%u-%u-%u, limit=%u",
                query.mktsegment_id,
                query.order_date.year, query.order_date.month, query.order_date.day,
                query.ship_date.year, query.ship_date.month, query.ship_date.day,
                query.limit_count);

            g_queries[mktsegment_id].emplace_back(std::move(query));
            g_queries_by_id.emplace_back(&g_queries[mktsegment_id].back());
        }
    }
    DEBUG("queried mktsegment count: %d", (int)all_mktsegment.size());


    //
    // Map input files to memory
    //
    std::thread thr = preload_files(argv[1], argv[2], argv[3], &g_mapped_customer, &g_mapped_orders, &g_mapped_lineitem);
    //TODO: thr.detach();


    //
    // Load table: customers
    //
    {
#if ENABLE_PROFILING
        const auto start_time = std::chrono::steady_clock::now();
#endif

        const char* const p = (const char*)g_mapped_customer.iov_base;
        const uint64_t max_custkey = g_mapped_customer.iov_len / 10;  // estimated max customer key
        g_custkey_to_mktsegment.resize(max_custkey);

        uint64_t last_custkey = 0;
        for (uint64_t i = 0; i < g_mapped_customer.iov_len; ++i) {
            uint64_t custkey = 0;
            while (p[i] != '|') {
                ASSERT(p[i] >= '0' && p[i] <= '9', "Unexpected char: %c", p[i]);
                custkey = custkey * 10 + (p[i] - '0');
                ++i;
            }
            ++i;
            ASSERT(custkey < max_custkey, "Unexpected custkey (too large): %" PRIu64, custkey);

            char mktsegment[16];
            int mktsegment_pos = 0;
            while (p[i] != '\n') {
                mktsegment[mktsegment_pos++] = p[i];
                ++i;
            }
            mktsegment[mktsegment_pos] = '\0';

            const auto& it = all_mktsegment.find(mktsegment);
            if (it != all_mktsegment.end()) {
                g_custkey_to_mktsegment[custkey] = it->second;
            }
            else {
                g_custkey_to_mktsegment[custkey] = 0;
            }

            //DEBUG("%llu -> %s", custkey, mktsegment);
            //TODO: remove these 2 lines...
            ASSERT(custkey == last_custkey + 1, "custkey: %llu, last_custkey: %llu", custkey, last_custkey);
            last_custkey = custkey;
        }

#if ENABLE_PROFILING
        const auto end_time = std::chrono::steady_clock::now();
        std::chrono::duration<double> sec = end_time - start_time;
        DEBUG("load g_custkey_to_mktsegment done: %.3lf sec", sec.count());
#endif
    }


    //
    // Load table: orders
    //
    g_thread_count = std::thread::hardware_concurrency();
    if (g_thread_count > MAX_THREAD_COUNT) g_thread_count = MAX_THREAD_COUNT;
    DEBUG("g_thread_count: %u", (unsigned)g_thread_count);
    std::vector<std::thread> threads;
    threads.resize(g_thread_count);
    {
        std::vector<iovec> parts;
        parts.resize(g_thread_count);
        void* last_p = g_mapped_orders.iov_base;
        const char* max_p = (const char*)((uintptr_t)g_mapped_orders.iov_base + g_mapped_orders.iov_len - 1);

        for (size_t t = 0; t < g_thread_count; ++t) {
            parts[t].iov_base = last_p;
            parts[t].iov_len = (g_mapped_orders.iov_len / g_thread_count);
            const char* p = (const char*)((uintptr_t)parts[t].iov_base + parts[t].iov_len);
            if (p > max_p) {
                p = max_p;
            }
            else {
                while (*p != '\n') ++p;
                ++p;  // skip '\n'
            }
            parts[t].iov_len = (uintptr_t)p - (uintptr_t)parts[t].iov_base + 1;
            last_p = (void*)p;
        }

        for (size_t t = 0; t < g_thread_count; ++t) {
            DEBUG("orders part_%u: base=%p, len=0x%llx", (unsigned)t, parts[t].iov_base, (uint64_t)parts[t].iov_len);
        }

        for (size_t t = 0; t < g_thread_count; ++t) {
            std::thread(preload_memory_range, parts[t]).detach();
            threads[t] = std::thread(
                [&](const iovec part, const size_t thread_idx) {
                    load_order_part(part, thread_idx);
                },
                parts[t], t);
        }
        for (size_t t = 0; t < g_thread_count; ++t) {
            threads[t].join();
        }
    }


    //
    // Print results
    //
    INFO("finally, print results...");
    std::vector<result_t> results;
    for (uint8_t i = 1; !g_queries[i].empty(); ++i) {        
        for (auto& query : g_queries[i]) {
            std::priority_queue<result_t, std::vector<result_t>, std::greater<result_t>> final_top_n;
            for (size_t t = 0; t < g_thread_count; ++t) {
                while (!query.top_n[t].empty()) {
                    bool do_insert = false;
                    if (final_top_n.size() < query.limit_count) {
                        do_insert = true;
                    }
                    else {  // final_top_n.size() >= query.limit_count
                        if (final_top_n.top().total_expend_cent < query.top_n[t].top().total_expend_cent) {
                            final_top_n.pop();
                            do_insert = true;
                        }
                    }
                    if (do_insert) {
                        final_top_n.emplace(query.top_n[t].top());
                    }
                    query.top_n[t].pop();
                }
            }

            results.clear();
            while (!final_top_n.empty()) {
                results.emplace_back(final_top_n.top());
                final_top_n.pop();
            }

            fprintf(stdout, "l_orderkey|o_orderdate|revenue\n");
            for (size_t idx = results.size() - 1; idx != (size_t)-1; --idx) {
                const result_t* it = &results[idx];
                fprintf(stdout, "%u|%u-%02u-%02u|%u.%02u\n",
                    it->order_key, it->order_date.year, it->order_date.month, it->order_date.day,
                    it->total_expend_cent / 100, it->total_expend_cent % 100);
            }
        }
    }
    fflush(stdout);

    INFO("all done!");
    thr.join();  // TODO: remove this
    return 0;
}
