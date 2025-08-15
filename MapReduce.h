#pragma once

// shenango
extern "C" {
#include <runtime/runtime.h>
}
#include "thread.h"
#include "sync.h"

#include <list>
#include <vector>
#include <thread>
#include <chrono>
#include <stdexcept>
#include <cstring>
#include <iostream>
#include <algorithm> // 为了使用 std::max

// FarMemory
#include "manager.hpp"
#include "helpers.hpp"
#include "list.hpp"
#include "array.hpp"
#include "deref_scope.hpp"
#include "BaseDataManager.h"

using namespace far_memory;

// 线程同步屏障
barrier_t barrier_map;
barrier_t barrier_reduce;

// Map任务参数
template <typename T>
struct map_parameter {
    DataFrameVector<T>* map_data;
    size_t length;
    size_t offset;  // 添加偏移量字段
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

template <typename T>
class MapReduce {
    protected:
    FarMemManager* manager;

    public:
    int map_num;
    int reduce_num;
    std::vector<std::list<imm_data>*> vec;

    inline int get_vec_index(int map_id, int reduce_id) {
        return map_id * reduce_num + reduce_id;
    }

    MapReduce(int map_num_, int reduce_num_, FarMemManager* manager_)
        : map_num(map_num_), reduce_num(reduce_num_), manager(manager_) {

        vec = new std::vector<std::list<imm_data>*>();
        for (int i = 0; i < map_num * reduce_num; i++) {
            vec->push_back(new std::list<imm_data>());
        }
        barrier_init(&barrier_map, map_num + reduce_num);
        barrier_init(&barrier_reduce, reduce_num);
    }

    virtual ~MapReduce() {
        if (vec) {
            for (auto& list_ptr : *vec) {
                if (list_ptr) {
                    for (auto const& data_block : *list_ptr) {
                        if (data_block.data) {
                            free(data_block.data);
                        }
                    }
                    delete list_ptr;
                }
            }
            delete vec;
        }
    }

    virtual void map_func(DataFrameVector<T>* map_data, int task_id, size_t offset, size_t data_length) = 0;
    virtual void reduce_func(int task_id) = 0;
    virtual void splice(DataFrameVector<T>** data_arr, size_t* data_dis, DataFrameVector<T>* map_data, size_t data_length) = 0;


    // [已修复] 修正了缓冲区溢出的逻辑
    void emit_intermediate(std::list<imm_data>* inter, const T* data, int len) {
        // 场景1: 当前块有足够空间，直接拷贝
        if (!inter->empty() && (inter->back().count + len <= imm_data_size)) {
            memcpy((T*)inter->back().data + inter->back().count, data, len * sizeof(T));
            inter->back().count += len;
            return;
        }

        // 场景2: 当前块空间不足或没有块，需要分配新块
        // 新块的大小必须能容纳下当前数据。取默认大小和当前数据长度中的较大者。
        size_t new_block_size = std::max((size_t)len, (size_t)imm_data_size) * sizeof(T);

        struct imm_data inter_en;
        inter_en.data = malloc(new_block_size); // check
        if (!inter_en.data) {
            throw std::runtime_error("Failed to allocate intermediate data block.");
        }

        // 将数据拷贝到这个全新的块中
        memcpy(inter_en.data, data, len * sizeof(T));
        inter_en.count = len; // 这个块已使用的元素数量
        inter->push_back(inter_en);
    }

    void mapper(void* arg) {
        map_parameter<T>* para = (map_parameter<T>*)arg;
        printf("[Mapper %d Start] offset=%zu, length=%zu\n", para->task_id, para->offset, para->length);
        map_func(para->map_data, para->task_id, para->offset, para->length);
        barrier_wait(&barrier_map);
        // para由创建线程的人负责释放
    }

    void reducer(void* arg) {
        reduce_parameter* para = (reduce_parameter*)arg;
        barrier_wait(&barrier_map);
        printf("[Reducer %d Start]\n", para->task_id);
        reduce_func(para->task_id);
        barrier_wait(&barrier_reduce);
        // para由创建线程的人负责释放
    }

    void run_mr(DataFrameVector<T>* map_data, size_t data_length) {
        printf("--- Running Benchmark ---\n");
        printf("Map Tasks: %d, Reduce Tasks: %d\n", map_num, reduce_num);

        std::vector<rt::Thread> map_threads, reduce_threads;

        DataFrameVector<T>** map_data_arr = (DataFrameVector<T>**)malloc(sizeof(DataFrameVector<T>*) * map_num);
        size_t* map_data_dis = (size_t*)malloc(sizeof(size_t) * map_num);
        splice(map_data_arr, map_data_dis, map_data, data_length); // check

        auto start = std::chrono::system_clock::now();

        size_t current_offset = 0;
        for (int i = 0; i < map_num; i++) {
            map_parameter<T>* mp = new map_parameter<T>();
            mp->task_id = i;
            mp->length = map_data_dis[i];
            mp->offset = current_offset;
            mp->map_data = map_data_arr[i];
            
            current_offset += map_data_dis[i];

            map_threads.emplace_back(rt::Thread([this, mp]() {
                mapper(mp);
                delete mp; // 在线程完成后释放参数
                }));
        }

        for (int i = 0; i < reduce_num; i++) {
            reduce_parameter* rp = new reduce_parameter();
            rp->task_id = i;

            reduce_threads.emplace_back(rt::Thread([this, rp]() {
                reducer(rp);
                delete rp; // 在线程完成后释放参数
                }));
        }

        for (auto& th : map_threads) {
            th.Join();
        }
        for (auto& th : reduce_threads) {
            th.Join();
        }

        auto end = std::chrono::system_clock::now();
        std::chrono::duration<double> diff = end - start;
        printf("[TOTAL TIME] %f seconds\n", diff.count());

        free(map_data_arr);
        free(map_data_dis);
    }
};