#include "../common.h"


int main(int argc, char* argv[])
{
    CHECK(argc == 2, "debug_sysv_perf <txt_file>");

    debug_print_cgroup();

    const char* const file_path = argv[1];

    int shmkey;
    uint32_t hugetlb_flags;
    uint32_t step;
    if (strstr(file_path, "customer")) {
        shmkey = SHMKEY_TXT_CUSTOMER;
        hugetlb_flags = SHM_HUGETLB | SHM_HUGE_2MB;
        step = 1024 * 1024 * 2;
    }
    else if (strstr(file_path, "orders")) {
        shmkey = SHMKEY_TXT_ORDERS;
        hugetlb_flags = SHM_HUGETLB | SHM_HUGE_2MB;
        step = 1024 * 1024 * 2;
    }
    else if (strstr(file_path, "lineitem")) {
        shmkey = SHMKEY_TXT_LINEITEM;
        hugetlb_flags = SHM_HUGETLB | SHM_HUGE_2MB;
        step = 1024 * 1024 * 2;
    }
    else {
        PANIC("unknown file %s: customer? orders? lineitem?", file_path);
    }
    INFO("shmkey: %d", shmkey);

    load_file_context ctx;
    __open_file_read_direct(file_path, &ctx);
    INFO("file opened: %s", file_path);
    INFO("file size: %lu (0x%lx)", ctx.file_size, ctx.file_size);

    // NOTE: We do NOT use huge page here!
    const int shmid = C_CALL(shmget(
        shmkey,
        ctx.file_size,
        0666 | hugetlb_flags));
    INFO("shmid: %d", shmid);

    void* const ptr = (uint8_t*)shmat(shmid, nullptr, 0);
    INFO("shmat got %p", ptr);
    CHECK(ptr != (void*)-1,
        "shmat(shmid=0x%x) failed. errno = %d (%s)", shmid, errno, strerror(errno));

    uint64_t dummy = 0;
    for (uint64_t i = 0; i < ctx.file_size; i += step) {
        dummy += ((uint8_t*)ptr)[i];
    }
    CHECK(dummy < (uint64_t)-1);
    INFO("pre-faulting done");

    C_CALL(shmdt(ptr));
    INFO("shmdt() done");

    return 0;
}
