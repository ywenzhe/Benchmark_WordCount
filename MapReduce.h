//
// Created by lenovo on 2025/8/4.
//

#ifndef UNTITLED25_MAPREDUCE_H
#define UNTITLED25_MAPREDUCE_H


#include <list>
#include <vector>
#include <thread>
#include <chrono>
#include <stdexcept>
#include <cstring>
#include <iostream>
#include <pthread.h>
#include <algorithm> // 为了使用 std::max

#include "BaseAllocator.h"

// 线程同步屏障
extern pthread_barrier_t barrier_map, barrier_reduce;

// Map任务参数
struct map_parameter {
    void* map_data;
    size_t length;
    int task_id;
};

// Reduce任务参数
struct reduce_parameter {
    int task_id;
};

// 中间数据块结构
struct imm_data {
    void* data;
    int count;
};

const static int imm_data_size = 512; // 每个中间数据块的默认大小

class MapReduce {
protected:
    BaseAllocator* allocator;

public:
    int map_num;
    int reduce_num;
    std::vector<std::list<imm_data>*>* vec;

    inline int get_vec_index(int map_id, int reduce_id) {
        return map_id * reduce_num + reduce_id;
    }

    MapReduce(int map_num_, int reduce_num_, BaseAllocator* alloc)
            : map_num(map_num_), reduce_num(reduce_num_), allocator(alloc) {
        if (!allocator) {
            throw std::runtime_error("Allocator cannot be null.");
        }
        vec = new std::vector<std::list<imm_data>*>();
        for (int i = 0; i < map_num * reduce_num; i++) {
            vec->push_back(new std::list<imm_data>());
        }
        pthread_barrier_init(&barrier_map, NULL, map_num + reduce_num);
        pthread_barrier_init(&barrier_reduce, NULL, reduce_num);
    }

    virtual ~MapReduce() {
        for (auto& list_ptr : *vec) {
            for(auto const& data_block : *list_ptr) {
                allocator->deallocate(data_block.data);
            }
            delete list_ptr;
        }
        delete vec;
        pthread_barrier_destroy(&barrier_map);
        pthread_barrier_destroy(&barrier_reduce);
    }

    virtual void map_func(void* map_data, int task_id, size_t data_length) = 0;
    virtual void reduce_func(int task_id) = 0;
    virtual void splice(char** data_arr, size_t* data_dis, char* map_data, size_t data_length) = 0;


    // [已修复] 修正了缓冲区溢出的逻辑
    void emit_intermediate(std::list<imm_data>* inter, char* data, int len) {
        // 场景1: 当前块有足够空间，直接拷贝
        if (!inter->empty() && (inter->back().count + len <= imm_data_size)) {
            memcpy((char*)inter->back().data + inter->back().count, data, len);
            inter->back().count += len;
            return;
        }

        // 场景2: 当前块空间不足或没有块，需要分配新块
        // 新块的大小必须能容纳下当前数据。取默认大小和当前数据长度中的较大者。
        size_t new_block_size = std::max((size_t)len, (size_t)imm_data_size);

        struct imm_data inter_en;
        inter_en.data = allocator->allocate(new_block_size);
        if (!inter_en.data) {
            throw std::runtime_error("Failed to allocate intermediate data block.");
        }

        // 将数据拷贝到这个全新的块中
        memcpy(inter_en.data, data, len);
        inter_en.count = len; // 这个块已使用的字节数就是刚拷贝进来的数据长度
        inter->push_back(inter_en);
    }

    void mapper(void* arg) {
        map_parameter* para = (map_parameter*)arg;
        printf("[Mapper %d Start]\n", para->task_id);
        map_func(para->map_data, para->task_id, para->length);
        pthread_barrier_wait(&barrier_map);
        delete para;
    }

    void reducer(void* arg) {
        reduce_parameter* para = (reduce_parameter*)arg;
        pthread_barrier_wait(&barrier_map);
        printf("[Reducer %d Start]\n", para->task_id);
        reduce_func(para->task_id);
        pthread_barrier_wait(&barrier_reduce);
        delete para;
    }

    void run_mr(char* map_data, size_t data_length) {
        printf("--- Running Benchmark ---\n");
        printf("Allocator: %s\n", allocator->get_name());
        printf("Map Tasks: %d, Reduce Tasks: %d\n", map_num, reduce_num);

        allocator->init();

        std::vector<std::thread*> map_threads, reduce_threads;

        char** map_data_arr = (char**)malloc(sizeof(char*) * map_num);
        size_t* map_data_dis = (size_t*)malloc(sizeof(size_t) * map_num);
        splice(map_data_arr, map_data_dis, map_data, data_length);

        auto start = std::chrono::system_clock::now();

        for (int i = 0; i < map_num; i++) {
            map_parameter* mp = new map_parameter();
            mp->task_id = i;
            mp->length = map_data_dis[i];
            mp->map_data = map_data_arr[i];
            map_threads.push_back(new std::thread(&MapReduce::mapper, this, mp));
        }

        for (int i = 0; i < reduce_num; i++) {
            reduce_parameter* rp = new reduce_parameter();
            rp->task_id = i;
            reduce_threads.push_back(new std::thread(&MapReduce::reducer, this, rp));
        }

        for (auto th : map_threads) {
            th->join();
            delete th;
        }
        for (auto th : reduce_threads) {
            th->join();
            delete th;
        }

        auto end = std::chrono::system_clock::now();
        std::chrono::duration<double> diff = end - start;
        printf("[TOTAL TIME] %f seconds\n", diff.count());

        free(map_data_arr);
        free(map_data_dis);
        allocator->shutdown();
    }
};

#endif //UNTITLED25_MAPREDUCE_H