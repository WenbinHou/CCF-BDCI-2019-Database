
## System Configurations

**To use io_uring**

Use Linux kernel >= 5.1 (We used [v5.1.21](https://kernel.ubuntu.com/~kernel-ppa/mainline/v5.1.21/))

---

**Fix NVIDIA driver for kernel 5.1**

NVIDIA driver should be slightly modified for Linux kernel 5.1+

This is mainly caused by:
- Kernel 5.1 uses `vm_fault_t` type as return type for fault handler (not `int`)
- Kernel 5.1 moved some functions from `drm/drm_crtc_helper.h` to `drm/drm_probe_helper.h` in kernel headers

Refer to
- https://gitlab.com/redcore/redcore-desktop/commit/a85038eed25ba9627a30fd11260c54d94539e9b5
- https://devtalk.nvidia.com/default/topic/1055534/cuda-setup-and-installation/nvidia-driver-418-67-installation-on-kernel-5-1-9-1-el7-fail-implicit-declaration-of-function-lsquo-drm_kms_helper_poll_init-rsquo-/

---

**To allow unlimited memlock, RT scheduling:**

In `/etc/security/limits.conf`

````text
#==============================================================================
# Added by Wenbin Hou
#==============================================================================

* soft nofile 65535
* hard nofile 65535

* soft memlock unlimited
* hard memlock unlimited

* soft rtprio 99
* hard rtprio 99
````

---

**To take advantage of huge pages:**

In `/etc/sysctl.conf`

````text
#==============================================================================
# Added by Wenbin Hou
#==============================================================================

# Allocate 1.6GB 2MB-hugepages
# Pity that we cannot make use of 1GB-hugepages, due to the KVM virtualization
vm.nr_hugepages = 800
````

---

**Misc.**

Configuration script `/usr/local/bin/sys-configure`

This script should be (manually) invoked after each reboot.

This script is idempotent.

```bash
#!/bin/bash

#
# Set clocksource to tsc
#
# cat /sys/devices/system/clocksource/clocksource0/available_clocksource
#   kvm-clock tsc acpi_pm
#
sudo sh -c 'echo tsc >/sys/devices/system/clocksource/clocksource0/current_clocksource'
```

**# Intel PEB**

// TODO: Intel Performance and Energy Bias Hint

## TODOs

https://kernelnewbies.org/Linux_5.2#File_systems

Revert https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/commit/?id=10eeadf3045c35fc83649ac586973eb28255add9, https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/commit/?id=39186cbe652d4f09d9494292fb77dfbc113bdf59

Revert https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/commit/?id=134fca9063ad4851de767d1768180e5dede9a881

````bash
fakeroot debian/rules clean  # this is necessary to create some files!
fakeroot debian/rules binary-lowlatency
fakeroot debian/rules binary-indep
````
