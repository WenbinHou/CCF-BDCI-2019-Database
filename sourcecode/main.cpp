#include "common.h"

int main(int argc, char* argv[])
{
    //
    // Print configurations
    //
    {
#define _PRINT_CONFIG(_Name_) \
            INFO("---- " #_Name_ ": %u (0x%x)", (_Name_), (_Name_))
        INFO("Configurations:");
        _PRINT_CONFIG(ENABLE_ASSERTION);
        _PRINT_CONFIG(ENABLE_LOGGING_DEBUG);
        _PRINT_CONFIG(ENABLE_LOGGING_INFO);
        _PRINT_CONFIG(ENABLE_PIN_THREAD_TO_CPU);
        _PRINT_CONFIG(ENABLE_QUEUE_USE_SPIN_LOCK);
        _PRINT_CONFIG(ENABLE_LOAD_FILE_USE_PREAD);
        _PRINT_CONFIG(ENABLE_ATTEMPT_HUGETLB);
        _PRINT_CONFIG(ENABLE_ATTEMPT_SCHED_FIFO);
        _PRINT_CONFIG(CONFIG_LOAD_BUFFER_COUNT);
        _PRINT_CONFIG(CONFIG_MMAP_MAX_STEP_SIZE);
        _PRINT_CONFIG(CONFIG_MMAP_MIN_STEP_SIZE);
        _PRINT_CONFIG(CONFIG_MMAP_MAX_STEP_SIZE_HUGETLB);
        _PRINT_CONFIG(CONFIG_MMAP_MIN_STEP_SIZE_HUGETLB);
        _PRINT_CONFIG(CONFIG_PART_OVERLAPPED_SIZE);
        _PRINT_CONFIG(CONFIG_CUSTOMER_PART_BODY_SIZE);
        _PRINT_CONFIG(CONFIG_ORDERS_PART_BODY_SIZE);
        _PRINT_CONFIG(CONFIG_LINEITEM_PART_BODY_SIZE);
#undef _PRINT_CONFIG
    }

    ASSERT(argc >= 5, "%s <customer_txt> <orders_txt> <lineitem_txt> <query_count> ...", argv[0]);

    //
    // Open text files
    //
    {
        const char* const customer_text_path = argv[1];
        const char* const orders_text_path = argv[2];
        const char* const lineitem_text_path = argv[3];

        const auto& open_file = [](/*in*/const char* const path, /*out*/load_file_context* const ctx) noexcept {
            ctx->fd = C_CALL(open(path, O_RDONLY | O_CLOEXEC));
            struct stat64 st;
            C_CALL(fstat64(ctx->fd, &st));
            ctx->file_size = st.st_size;
            DEBUG("open %s: fd = %d, size = %lu", path, ctx->fd, ctx->file_size);
        };

        open_file(customer_text_path, &g_customer_file);
        open_file(orders_text_path, &g_orders_file);
        open_file(lineitem_text_path, &g_lineitem_file);
    }


    //
    // Using index or creating index?
    //
    bool is_creating_index;
    {
        uint64_t tmp64 = g_customer_file.file_size ^ g_orders_file.file_size ^ g_lineitem_file.file_size;
        uint32_t index_hash_u32 = (uint32_t)(tmp64) ^(uint32_t)(tmp64 >> 32);

        char index_dir[32];
        snprintf(index_dir, 32, "./index_%08x", index_hash_u32);
        DEBUG("use current working directory for index files: index_dir: %s", index_dir);

        g_index_directory_fd = open(index_dir, O_DIRECTORY | O_PATH | O_CLOEXEC);
        if (g_index_directory_fd >= 0) {
            is_creating_index = false;
            DEBUG("index directory found... using created index");
        }
        else if (errno == ENOENT) {
            is_creating_index = true;
            DEBUG("index directory not found... now creating index");
            C_CALL(mkdir(index_dir, 0755));
            g_index_directory_fd = C_CALL(open(index_dir, O_DIRECTORY | O_PATH | O_CLOEXEC));
        }
        else {
            PANIC("open() %s failed with %d (%s)", index_dir, errno, strerror(errno));
        }
        DEBUG("g_index_directory_fd: %d", g_index_directory_fd);
    }

    //
    // A few common initializations
    //
    {
        // Get query count
        g_query_count = (uint32_t)std::strtoul(argv[4], nullptr, 10);
        DEBUG("g_query_count: %u", g_query_count);
        g_argv_queries = &argv[5];

        // How many CPU cores do we use?
        cpu_set_t original_cpu_set;
        CPU_ZERO(&original_cpu_set);
        PTHREAD_CALL(pthread_getaffinity_np(pthread_self(), sizeof(original_cpu_set), &original_cpu_set));
        const uint32_t active_cpu_cores = CPU_COUNT(&original_cpu_set);
        INFO("active_cpu_cores: %u", active_cpu_cores);

        // How many workers do we use?
        const uint32_t worker_thread_count = active_cpu_cores;
        DEBUG("worker_thread_count: %u", worker_thread_count);
        g_worker_sync_barrier.init(worker_thread_count);

        // How many loaders do we use?
        const uint32_t loader_thread_count = active_cpu_cores;
        DEBUG("loader_thread_count: %u", loader_thread_count);
        g_loader_sync_barrier.init(loader_thread_count);
    }

    //
    // Custom initializations
    //
    if (is_creating_index) {
        create_index_initialize();
    }
    else {
        use_index_initialize();
    }


    //
    // Create loader threads and worker threads
    //
    {
        [[maybe_unused]]
        const auto pin_thread_to_cpu_core = [](const uint32_t tid) {
            cpu_set_t cpu_set;
            CPU_ZERO(&cpu_set);
            CPU_SET(tid, &cpu_set);
            PTHREAD_CALL_NO_PANIC(pthread_setaffinity_np(pthread_self(), sizeof(cpu_set), &cpu_set));
        };

        g_worker_threads.reserve(g_worker_sync_barrier.thread_count());
        g_loader_threads.reserve(g_loader_sync_barrier.thread_count());

        for (uint32_t tid = 0; tid < g_worker_sync_barrier.thread_count(); ++tid) {
            g_worker_threads.emplace_back(
                [&, tid]() {
#if ENABLE_PIN_THREAD_TO_CPU
                    pin_thread_to_cpu_core(tid);
#endif
                    //sched_param param { };
                    //param.sched_priority = sched_get_priority_max(SCHED_FIFO) - 1;  // TODO: slightly lower than loader threads
                    //PTHREAD_CALL(pthread_setschedparam(pthread_self(), SCHED_FIFO, &param));
                    //DEBUG("[%u] pthread_setschedparam SCHED_FIFO", tid);

                    (is_creating_index ? fn_worker_thread_create_index : fn_worker_thread_use_index)(tid);
                });
        }

        for (uint32_t tid = 0; tid < g_loader_sync_barrier.thread_count(); ++tid) {
            g_loader_threads.emplace_back(
                [&, tid]() {
#if ENABLE_PIN_THREAD_TO_CPU
                    pin_thread_to_cpu_core(tid);
#endif
                    //sched_param param { };
                    //param.sched_priority = sched_get_priority_max(SCHED_FIFO);
                    //PTHREAD_CALL(pthread_setschedparam(pthread_self(), SCHED_FIFO, &param));
                    //DEBUG("[%u] pthread_setschedparam SCHED_FIFO", tid);

                    (is_creating_index ? fn_loader_thread_create_index : fn_loader_thread_use_index)(tid);
                });
        }
    }


    //
    // Wait for worker and loader threads
    //
    for (std::thread& thr : g_loader_threads) thr.join();
    for (std::thread& thr : g_worker_threads) thr.join();


    //
    // Exit now
    //
    if (is_creating_index && g_query_count > 0) {
        char* args[argc + 1];
        for (int i = 0; i < argc; ++i) {
            args[i] = argv[i];
        }
        args[argc] = nullptr;
        INFO("================ now exec! ================");
        C_CALL(execv(argv[0], args));
    }
    else {
        INFO("================ now exit! ================");
    }

    return 0;
}
