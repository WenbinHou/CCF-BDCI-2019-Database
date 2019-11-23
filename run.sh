#!/bin/bash

#
# Try to allocate 2500 2MB huge pages
#
CONFIG_EXTRA_HUGE_PAGES=2500

if [ "$(cat /proc/sys/vm/nr_hugepages)" != "$CONFIG_EXTRA_HUGE_PAGES" ]; then
    sudo sysctl -w vm.nr_hugepages="$CONFIG_EXTRA_HUGE_PAGES"
    # Check whether sysctl is successful
    # Linux may allocate less than required huge pages (if no enough memory)
    if [ "$(cat /proc/sys/vm/nr_hugepages)" != "$CONFIG_EXTRA_HUGE_PAGES" ]; then
        # We try to drop page cache
        sync
        sudo sysctl -w vm.compact_memory=1
        sync
        sudo sysctl -w vm.drop_caches=3

        # Now try to allocate huge pages again, and check
        sudo sysctl -w vm.nr_hugepages="$CONFIG_EXTRA_HUGE_PAGES"
        if [ "$(cat /proc/sys/vm/nr_hugepages)" != "$CONFIG_EXTRA_HUGE_PAGES" ]; then
            echo "ERROR: Can't allocate $CONFIG_EXTRA_HUGE_PAGES huge pages. Actually allocated $(cat /proc/sys/vm/nr_hugepages) huge pages."
            exit 1
        fi
    fi
fi


#
# Run BDCI19 executable
#
# We grant the program root permission, cause it may:
#   - test whether a file is in page cache (which requires the file is writable by the program since Linux kernel 5.0+)
#   - decide to drop some page caches (which requries root permission)
# In real-world applications, programs should drop this dangerous privilege after initialization.
#
exec sudo "$(dirname "$0")/BDCI19" "$@"
