#!/bin/bash

source ../../shared.sh

arr_aifm_heap_size=( 1250 2500 3750 5000 6250 7500 8750 10000 11250 12500 13750 15000 )

# 确保之前的进程已经终止
sudo pkill -9 main

for((i=0; i<${#arr_aifm_heap_size[@]}; ++i)); do
    cur_heap_size=${arr_aifm_heap_size[i]}
    sed "s/constexpr static uint64_t kCacheSize = .*/constexpr size_t kCacheSize = $cur_heap_size * Region::kSize;/g" main.cpp -i
    make clean
    make -j
    rerun_local_iokerneld
    rerun_mem_server
    run_program ./main | grep "=" 1>log.$cur_heap_size 2>&1
done

kill_local_iokerneld
