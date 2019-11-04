#include "common.h"

static void maybe_take_action_and_exit(const char* const action) noexcept
{
    ASSERT(action != nullptr);

    if (strcmp(action, ACTION_DROP_PAGE_CACHE) == 0) {
        exit(mem_drop_cache() ? EXIT_SUCCESS : EXIT_FAILURE);
    }

    WARN("unknown action: %s (do nothing)", action);
}


int main(int argc, char* argv[])
{
    {
        const char* const action = getenv("__BDCI19_TAKE_ACTION");
        if (__unlikely(action != nullptr)) {
            maybe_take_action_and_exit(action);
        }
    }

    mem_info mem { };
    mem_get_usage(&mem);
    INFO("mem_get_available_bytes: %lu MB", mem_get_available_bytes() >> 20);
    INFO("mem_get_free_bytes: %lu MB", mem_get_free_bytes() >> 20);

    rlimit64 lim;
    C_CALL(getrlimit64(RLIMIT_FSIZE, &lim));
    INFO("RLIMIT_FSIZE: soft=0x%lx, hard=0x%lx", lim.rlim_cur, lim.rlim_max);
    C_CALL(getrlimit64(RLIMIT_MEMLOCK, &lim));
    INFO("RLIMIT_MEMLOCK: soft=0x%lx, hard=0x%lx", lim.rlim_cur, lim.rlim_max);
    C_CALL(getrlimit64(RLIMIT_NOFILE, &lim));
    INFO("RLIMIT_NOFILE: soft=0x%lx, hard=0x%lx", lim.rlim_cur, lim.rlim_max);
    C_CALL(getrlimit64(RLIMIT_RTPRIO, &lim));
    INFO("RLIMIT_RTPRIO: soft=0x%lx, hard=0x%lx", lim.rlim_cur, lim.rlim_max);
    C_CALL(getrlimit64(RLIMIT_RTTIME, &lim));
    INFO("RLIMIT_RTTIME: soft=0x%lx, hard=0x%lx", lim.rlim_cur, lim.rlim_max);

    return 0;
}
