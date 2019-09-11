# Tesla P100 Spec

## Specs

- SM: 56 (64 CUDA Cores/SM, totally 3584 CUDA Cores/GPU)

- Max Warps/SM: 64
- Max Threads/SM: 2048
- Max CTA/SM: 32
- Max Reg4B/SM: 64 K
- Max Reg4B/CTA: 64 K
- Max Reg4B/Thread: 255


- Register: 256 KB/SM (allocation unit size: 256, warp-granularity)
- SharedMem: 64 KB/SM (allocation unit size: 256)  // requires >= 2 CTA to make full use
- L1 Cache: ?


- DRAM: 16 GB (4096-bit HBM2)
- L2 Cache: 4 MB


## CUDA Occupancy Calculator

Online version: https://xmartlabs.github.io/cuda-calculator
