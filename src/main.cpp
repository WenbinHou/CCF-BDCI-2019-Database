#include "common.h"

static void maybe_take_action_and_exit() noexcept
{
    const char* const action = getenv("__BDCI19_TAKE_ACTION");
    if (__likely(action == nullptr)) {
        return;
    }

    if (strcmp(action, ACTION_DROP_PAGE_CACHE) == 0) {
        exit(mem_drop_cache() ? EXIT_SUCCESS : EXIT_FAILURE);
    }

    WARN("unknown action: %s (do nothing)", action);
}


static void detect_preparing_page_cache() noexcept
{
    constexpr const uint64_t FINCORE_TEST_SIZE = 1024 * 1024 * 2;  // Test 2MB

    void* const base_ptr = mmap_reserve_space(FINCORE_TEST_SIZE);

    g_is_preparing_page_cache = !__fincore(
        base_ptr,
        g_customer_file.fd,
        __align_down(g_customer_file.file_size - FINCORE_TEST_SIZE, PAGE_SIZE),
        FINCORE_TEST_SIZE);
    if (!g_is_preparing_page_cache) {
        g_is_preparing_page_cache = !__fincore(
            base_ptr,
            g_customer_file.fd,
            0,
            FINCORE_TEST_SIZE);
    }
    INFO("fincore: customer_file -> %s", (g_is_preparing_page_cache ? "no" : "yes"));

    if (!g_is_preparing_page_cache) {
        g_is_preparing_page_cache = !__fincore(
            base_ptr,
            g_orders_file.fd,
            __align_down(g_orders_file.file_size - FINCORE_TEST_SIZE, PAGE_SIZE),
            FINCORE_TEST_SIZE);
        if (!g_is_preparing_page_cache) {
            g_is_preparing_page_cache = !__fincore(
                base_ptr,
                g_orders_file.fd,
                0,
                FINCORE_TEST_SIZE);
        }
        INFO("fincore: orders_file -> %s", (g_is_preparing_page_cache ? "no" : "yes"));
    }

    if (!g_is_preparing_page_cache) {
        g_is_preparing_page_cache = !__fincore(
            base_ptr,
            g_lineitem_file.fd,
            __align_down(g_lineitem_file.file_size - FINCORE_TEST_SIZE, PAGE_SIZE),
            FINCORE_TEST_SIZE);
        if (!g_is_preparing_page_cache) {
            g_is_preparing_page_cache = !__fincore(
                base_ptr,
                g_lineitem_file.fd,
                0,
                FINCORE_TEST_SIZE);
        }
        INFO("fincore: lineitem_file -> %s", (g_is_preparing_page_cache ? "no" : "yes"));
    }

    C_CALL(munmap(base_ptr, FINCORE_TEST_SIZE));

    mmap_return_space(base_ptr, FINCORE_TEST_SIZE);

    INFO("g_is_preparing_page_cache: %d", (int)g_is_preparing_page_cache);
}


static void do_multi_process() noexcept
{
    //
    // Custom initializations (before fork)
    //
    if (g_is_creating_index) {
        create_index_initialize_before_fork();
    }
    else {
        use_index_initialize_before_fork();
    }


    //
    // Do fork or create threads!
    //
    g_id = -1;
    for (uint32_t child_id = 0; child_id < g_total_process_count; ++child_id) {
        if (C_CALL(fork()) == 0) {  // child
            g_id = child_id;
            break;
        }
        else {  // parent
        }
    }

    //
    // Main process:
    // Wait for child processes to exit
    //
    if (g_id == (uint32_t)-1) {
        for (uint32_t i = 0; i < g_total_process_count; ++i) {
            int status;
            C_CALL(wait(&status));
            CHECK(WIFEXITED(status), "One of child process not normally exits");
            CHECK(WEXITSTATUS(status) == 0, "One of child process exits with %d", WEXITSTATUS(status));
        }
        INFO("all child processes exits normally");
        return;
    }

    INFO("[%u] child process: g_id = %u, pid = %d", g_id, g_id, getpid());

    // Custom initializations (after fork)
    if (g_is_creating_index) {
        create_index_initialize_after_fork();
    }
    else {
        use_index_initialize_after_fork();
    }


    //
    // Create worker thread, unloader thread
    //
    std::thread worker_thread;
    std::thread unloader_thread;
    {
        worker_thread = std::thread([&]() {
#if ENABLE_PIN_THREAD_TO_CPU
            pin_thread_to_cpu_core(g_id);
#endif
#if ENABLE_ATTEMPT_SCHED_FIFO
            set_thread_fifo_scheduler(CONFIG_SCHED_FIFO_WORKER_NICE);
#endif
            (g_is_creating_index ? fn_worker_thread_create_index : fn_worker_thread_use_index)(g_id);
        });

        unloader_thread = std::thread([&]() {
#if ENABLE_PIN_THREAD_TO_CPU
            pin_thread_to_cpu_core(g_id);
#endif
#if ENABLE_ATTEMPT_SCHED_FIFO
            set_thread_fifo_scheduler(CONFIG_SCHED_FIFO_UNLOADER_NICE);
#endif
            (g_is_creating_index ? fn_unloader_thread_create_index : fn_unloader_thread_use_index)();
        });
    }


    //
    // Do like loader thread...
    //
    {
#if ENABLE_PIN_THREAD_TO_CPU
        pin_thread_to_cpu_core(g_id);
#endif
#if ENABLE_ATTEMPT_SCHED_FIFO
        set_thread_fifo_scheduler(CONFIG_SCHED_FIFO_LOADER_NICE);
#endif
        (g_is_creating_index ? fn_loader_thread_create_index : fn_loader_thread_use_index)(g_id);
    }


    //
    // Wait for loader thread
    // ...as well as unloader thread
    //
    worker_thread.join();
    unloader_thread.join();
}


static void do_multi_thread() noexcept
{
    PANIC("TODO: do_multi_thread() not implemented yet");
}



int main(int argc, char* argv[])
{
    debug_print_cgroup();

    //
    // Maybe take some action and then exit...
    //
    maybe_take_action_and_exit();

    //
    // Initialize mapper
    //
    __mapper_initialize();


    //
    // Detect whether we are creating index or using index
    //
    {
        const char index_dir[32] { "./index" };
        DEBUG("use current working directory for index files: index_dir: %s", index_dir);

        g_index_directory_fd = open(index_dir, O_DIRECTORY | O_PATH | O_CLOEXEC);
        if (g_index_directory_fd >= 0) {
            g_is_creating_index = false;
            DEBUG("index directory found... using created index");
        }
        else if (errno == ENOENT) {
            g_is_creating_index = true;
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
    // Open text files if creating index
    //
    CHECK(argc >= 5, "%s <customer_txt> <orders_txt> <lineitem_txt> <query_count>", argv[0]);
    if (g_is_creating_index) {
        const char* const customer_text_path = argv[1];
        const char* const orders_text_path = argv[2];
        const char* const lineitem_text_path = argv[3];

        __open_file_read(customer_text_path, &g_customer_file);
        if (euidaccess(customer_text_path, W_OK) != 0) {
            WARN("%s is not writeable by current process. mincore() will mal-function", customer_text_path);
            WARN("please grant write permission to %s (although it will not be written)", customer_text_path);
        }

        __open_file_read(orders_text_path, &g_orders_file);
        if (euidaccess(orders_text_path, W_OK) != 0) {
            WARN("%s is not writeable by current process. mincore() will mal-function", orders_text_path);
            WARN("please grant write permission to %s (although it will not be written)", orders_text_path);
        }

        __open_file_read(lineitem_text_path, &g_lineitem_file);
        if (euidaccess(lineitem_text_path, W_OK) != 0) {
            WARN("%s is not writeable by current process. mincore() will mal-function", lineitem_text_path);
            WARN("please grant write permission to %s (although it will not be written)", lineitem_text_path);
        }

        detect_preparing_page_cache();
    }


    //
    // Initialize g_shared
    //
    {
        //g_use_multi_process = g_is_creating_index ? !g_is_preparing_page_cache : false;
        g_use_multi_process = g_is_creating_index;
        INFO("g_use_multi_process: %d", (int)g_use_multi_process);

        if (g_use_multi_process) {
            g_shared = (shared_information_t*)my_mmap(
                sizeof(shared_information_t),
                PROT_READ | PROT_WRITE,
                MAP_SHARED | MAP_ANONYMOUS | MAP_POPULATE,
                -1,
                0);
        }
        else {
            g_shared = (shared_information_t*)malloc(sizeof(shared_information_t));
            CHECK(g_shared != nullptr, "malloc() failed");
        }
        DEBUG("g_shared: %p", g_shared);

        new (g_shared) shared_information_t;
    }


    //
    // A few common initializations
    //
    {
        // Get query count
        g_query_count = (uint32_t) std::strtoul(argv[4], nullptr, 10);
        DEBUG("g_query_count: %u", g_query_count);
        g_argv_queries = &argv[5];
        if (!g_is_creating_index && __unlikely(g_query_count == 0)) {
            INFO("!is_creating_index && g_query_count == 0: fast exit!");
            return 0;
        }

        // How many CPU cores do we use?
        cpu_set_t original_cpu_set;
        CPU_ZERO(&original_cpu_set);
        PTHREAD_CALL(pthread_getaffinity_np(pthread_self(), sizeof(original_cpu_set), &original_cpu_set));
        g_active_cpu_cores = CPU_COUNT(&original_cpu_set);
        INFO("g_active_cpu_cores: %u", g_active_cpu_cores);

        // How many processes do we use?
        g_total_process_count = g_active_cpu_cores;
        DEBUG("g_total_process_count: %u", g_total_process_count);
        g_shared->worker_sync_barrier.init(g_total_process_count, g_use_multi_process);
        g_shared->loader_sync_barrier.init(g_total_process_count, g_use_multi_process);
    }


    if (g_use_multi_process) {
        do_multi_process();
    }
    else {
        do_multi_thread();
    }


    return 0;
}
