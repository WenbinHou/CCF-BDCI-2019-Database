#if !defined(_BDCI19_MEM_H_INCLUDED_)
#define _BDCI19_MEM_H_INCLUDED_


struct mem_info
{
    uint64_t mem_total;
    uint64_t mem_free;
    uint64_t mem_available;
    uint64_t buffers;
    uint64_t cached;
};


__always_inline
void mem_get_usage(/*out*/mem_info* mem) noexcept
{
    ASSERT(mem != nullptr);
    //memset(mem, 0x00, sizeof(*mem));

    FILE* const file = fopen("/proc/meminfo", "rb");
    CHECK(file != nullptr, "fopen() failed");

    TRACE("mem_get_usage:");
    while (true) {
        char key[64];
        key[0] = '\0';
        uint64_t value = 0;

#if ENABLE_LOGGING_TRACE
        char suffix[8];
        suffix[0] = '\0';
        const int res = fscanf(file, " %[^ \r\t\n:]: %lu%[^\n]\n", key, &value, suffix); // NOLINT(cert-err34-c)
        if (res == EOF) break;
        ASSERT(res == 2 || res == 3);
        TRACE("  %s: %lu%s", key, value, suffix);
#else
        const int res = fscanf(file, " %[^ \r\t\n:]: %lu%*[^\n]\n", key, &value); // NOLINT(cert-err34-c)
        if (res == EOF) break;
        ASSERT(res == 2);
#endif

        if (strcmp(key, "MemTotal") == 0) {
            mem->mem_total = value * 1024;  // unit in /proc/meminfo: kB
        }
        else if (strcmp(key, "MemFree") == 0) {
            mem->mem_free = value * 1024;  // unit in /proc/meminfo: kB
        }
        else if (strcmp(key, "MemAvailable") == 0) {
            mem->mem_available = value * 1024;  // unit in /proc/meminfo: kB
        }
        else if (strcmp(key, "Buffers") == 0) {
            mem->buffers = value * 1024;  // unit in /proc/meminfo: kB
        }
        else if (strcmp(key, "Cached") == 0) {
            mem->cached = value * 1024;  // unit in /proc/meminfo: kB
        }
    }

    DEBUG("mem_info: mem_total=%lu MB", mem->mem_total >> 20);
    DEBUG("mem_info: mem_free=%lu MB", mem->mem_free >> 20);
    DEBUG("mem_info: mem_available=%lu MB", mem->mem_available >> 20);
    DEBUG("mem_info: buffers=%lu MB", mem->buffers >> 20);
    DEBUG("mem_info: cached=%lu MB", mem->cached >> 20);

    fclose(file);
}


template<size_t _N>
__always_inline
uint64_t __mem_get_kb_to_bytes(const string<_N>& key_name) noexcept
{
    FILE* const file = fopen("/proc/meminfo", "rb");
    CHECK(file != nullptr, "fopen() failed");

    uint64_t result = 0ULL;
    while (true) {
        char key[64];
        key[0] = '\0';
        uint64_t value = 0;
        const int res = fscanf(file, "%[^:]: %lu%*[^\n]\n", key, &value); // NOLINT(cert-err34-c)
        if (res == EOF) {
            PANIC("__mem_get_kb_to_bytes: key not found: %s", key_name.value);
            break;
        }
        ASSERT(res == 2);

        if (key_name == key) {
            result = value * 1024ULL;  // unit in /proc/meminfo: kB
            break;
        }
    }

    fclose(file);
    return result;
}

__always_inline
uint64_t mem_get_available_bytes() noexcept
{
    static constexpr const auto key = make_string("MemAvailable");
    return __mem_get_kb_to_bytes(key);
}

__always_inline
uint64_t mem_get_free_bytes() noexcept
{
    static constexpr const auto key = make_string("MemFree");
    return __mem_get_kb_to_bytes(key);
}


__always_inline
bool  mem_drop_cache() noexcept
{
    // To free page cache:
    //   echo 1 > /proc/sys/vm/drop_caches
    // To free reclaimable slab objects (includes dentries and inodes):
    //   echo 2 > /proc/sys/vm/drop_caches
    // To free slab objects and page cache:
    //   echo 3 > /proc/sys/vm/drop_caches
    const int fd = C_CALL_NO_PANIC(open("/proc/sys/vm/drop_caches", O_RDWR | O_CLOEXEC));
    if (fd >= 0) {
        const char content = '3';
        const size_t cnt = (size_t)C_CALL(write(fd, &content, sizeof(content)));
        CHECK(cnt == sizeof(content));
        return true;
    }
    return false;
}

#endif  // !defined(_BDCI19_MEM_H_INCLUDED_)
