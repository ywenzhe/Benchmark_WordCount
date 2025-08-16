#pragma once

#include "MapReduce.h"
#include <string>
#include <unordered_map>
#include <cctype>
#include <vector> // 用于临时缓冲区

#include "concurrent_hopscotch.hpp"
#include "dataframe_vector.hpp"
#include "deref_scope.hpp"

using namespace far_memory;

class WordCount : public MapReduce<char> {
public:
    WordCount(int map_num, int reduce_num, FarMemManager* manager)
            : MapReduce<char>(map_num, reduce_num, manager) {}

    // DJB2哈希函数，用于shuffle
    uint64_t djb_hash(const char* cp) const {
        uint64_t hash = 5381;
        while (*cp)
            hash = 33 * hash ^ (unsigned char)*cp++;
        return hash;
    }

    // Shuffle阶段：根据单词哈希值决定它应被哪个Reducer处理
    int shuffle_func(char* word) {
        return djb_hash(word) % reduce_num;
    }

    // 实现Map功能  
    void map_func(DataFrameVector<char>* map_data, int task_id, size_t offset, size_t data_length) override {
        // 使用批量读取提高性能，但保持与原版本相同的处理逻辑
        constexpr size_t BUFFER_SIZE = 8192;
        std::vector<char> buffer(BUFFER_SIZE);
        std::vector<char> word_buffer;
        
        size_t end_pos = offset + data_length;
        size_t current_pos = offset;
        
        printf("[Mapper %d] Processing range [%zu, %zu), length=%zu\n", 
               task_id, offset, end_pos, data_length);
        
        while (current_pos < end_pos) {
            DerefScope scope;
            size_t chunk_size = std::min(BUFFER_SIZE, end_pos - current_pos);
            
            // 批量读取数据到本地缓冲区
            for (size_t i = 0; i < chunk_size; i++) {
                buffer[i] = map_data->at(scope, current_pos + i);
            }
            
            // 使用与原版本相同的逐字符处理逻辑
            size_t i = 0;
            bool has_cross_boundary_word = false;
            
            while (i < chunk_size) {
                // 跳过非字母字符
                while (i < chunk_size && !isalpha(buffer[i])) {
                    i++;
                }
                
                if (i >= chunk_size) break;
                
                // 记录单词起点
                size_t word_start = i;
                
                // 找到单词终点
                while (i < chunk_size && (isalpha(buffer[i]) || buffer[i] == '\'')) {
                    i++;
                }
                
                // 如果单词完整在缓冲区内
                if (i < chunk_size || current_pos + i >= end_pos) {
                    if (i > word_start) {
                        size_t word_len = i - word_start;
                        word_buffer.resize(word_len + 1);
                        
                        // 复制单词并转为小写
                        for (size_t k = 0; k < word_len; ++k) {
                            word_buffer[k] = tolower(buffer[word_start + k]);
                        }
                        word_buffer[word_len] = '\0';
                        
                        // 发送到reducer
                        int reduce_id = shuffle_func(word_buffer.data());
                        emit_intermediate(vec->at(get_vec_index(task_id, reduce_id)), 
                                         word_buffer.data(), word_len + 1);
                    }
                } else {
                    // 单词跨越缓冲区边界，需要特殊处理
                    // 先处理缓冲区内的部分
                    word_buffer.clear();
                    for (size_t k = word_start; k < chunk_size; ++k) {
                        word_buffer.push_back(tolower(buffer[k]));
                    }
                    
                    // 继续读取单词的剩余部分
                    scope.renew();
                    size_t next_pos = current_pos + chunk_size;
                    while (next_pos < end_pos && 
                           (isalpha(map_data->at(scope, next_pos)) || map_data->at(scope, next_pos) == '\'')) {
                        word_buffer.push_back(tolower(map_data->at(scope, next_pos)));
                        next_pos++;
                    }
                    
                    if (!word_buffer.empty()) {
                        word_buffer.push_back('\0');
                        
                        // 发送到reducer
                        int reduce_id = shuffle_func(word_buffer.data());
                        emit_intermediate(vec->at(get_vec_index(task_id, reduce_id)), 
                                         word_buffer.data(), word_buffer.size());
                    }
                    
                    // 调整位置：下一个缓冲区从单词结束后开始
                    current_pos = next_pos;
                    has_cross_boundary_word = true;
                    break; // 跳出内层循环，读取下一个缓冲区
                }
            }
            
            // 如果没有跨边界单词，正常前进到下一个缓冲区
            if (!has_cross_boundary_word) {
                current_pos += chunk_size;
            }
        }
    }

    // 实现Reduce功能
    void reduce_func(int task_id) override {
        std::unordered_map<std::string, int> word_counts;

        for (int map_id = 0; map_id < map_num; map_id++) {
            std::list<imm_data>* inter = vec->at(get_vec_index(map_id, task_id));
            for (auto const& data_block : *inter) {
                char* current_word = (char*)data_block.data;
                char* end_of_block = current_word + data_block.count;

                while(current_word < end_of_block) {
                    // 确保我们不会处理一个空的或损坏的字符串
                    if (*current_word == '\0') {
                        current_word++;
                        continue;
                    }
                    word_counts[current_word]++;
                    current_word += strlen(current_word) + 1;
                }
            }
        }
        printf("[Reducer %d] Processed %zu unique words.\n", task_id, word_counts.size());
    }

    // 实现数据分片逻辑
    void splice(DataFrameVector<char>** data_arr, size_t* data_dis, DataFrameVector<char>* map_data, size_t data_length) override {
        DerefScope scope;
        size_t avg_len_per_map = data_length / map_num;
        size_t current_offset = 0;

        printf("[Splice] Total data length: %zu, Map tasks: %d, Avg per map: %zu\n", 
               data_length, map_num, avg_len_per_map);

        // 对于DataFrameVector类型，每个mapper都使用相同的数据源
        for (int i = 0; i < map_num; ++i) {
            data_arr[i] = map_data;

            if (i == map_num - 1) {
                // 最后一个mapper处理剩余的所有数据
                data_dis[i] = data_length - current_offset;
            } else {
                size_t boundary = current_offset + avg_len_per_map;
                if (boundary >= data_length) {
                    boundary = data_length;
                } else {
                    // 确保边界不会切断单词 - 找到下一个非字母字符
                    while (boundary < data_length && isalpha(map_data->at(scope, boundary))) {
                        boundary++;
                    }
                }
                
                data_dis[i] = boundary - current_offset;
            }
            
            printf("[Splice] Mapper %d: offset=%zu, length=%zu\n", i, current_offset, data_dis[i]);
            current_offset += data_dis[i];
        }
    }
};