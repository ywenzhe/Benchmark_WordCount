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
        // 使用较大的缓冲区批量读取数据，减少远程访问
        constexpr size_t BUFFER_SIZE = 512;
        char buffer[BUFFER_SIZE];
        std::vector<char> word_buffer; // 安全的临时缓冲区

        size_t end_pos = offset + data_length;
        size_t current_pos = offset;
        
        while (current_pos < end_pos) {
            // 每次处理一个缓冲区的数据
            DerefScope scope;
            size_t chunk_size = std::min(BUFFER_SIZE, end_pos - current_pos);
            
            // 批量读取数据到本地缓冲区
            for (size_t i = 0; i < chunk_size; i++) {
                buffer[i] = map_data->at(scope, current_pos + i);
            }
            
            // 在本地缓冲区中处理单词
            size_t i = 0;
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
                
                // 如果单词被缓冲区边界切断
                if (i == chunk_size && current_pos + i < end_pos) {
                    // 处理跨越缓冲区边界的单词
                    word_buffer.clear();
                    
                    // 先复制当前缓冲区中的部分
                    word_buffer.insert(word_buffer.end(), buffer + word_start, buffer + i);
                    
                    // 然后继续读取剩余部分
                    size_t next_pos = current_pos + i;
                    scope.renew();
                    
                    while (next_pos < end_pos && 
                           (isalpha(map_data->at(scope, next_pos)) || map_data->at(scope, next_pos) == '\'')) {
                        word_buffer.push_back(map_data->at(scope, next_pos));
                        next_pos++;
                    }
                    
                    // 添加终止符
                    word_buffer.push_back('\0');
                    
                    // 转为小写
                    for (size_t k = 0; k < word_buffer.size() - 1; ++k) {
                        word_buffer[k] = tolower(word_buffer[k]);
                    }
                    
                    // 发送到reducer
                    int reduce_id = shuffle_func(word_buffer.data());
                    emit_intermediate(vec->at(get_vec_index(task_id, reduce_id)), 
                                     word_buffer.data(), word_buffer.size());
                    
                } else if (i > word_start) {
                    // 处理完整的单词
                    int word_len = i - word_start;
                    word_buffer.resize(word_len + 1); // +1 for null terminator
                    
                    // 复制单词到缓冲区
                    memcpy(word_buffer.data(), buffer + word_start, word_len);
                    word_buffer.back() = '\0'; // 添加null终止符
                    
                    // 转为小写
                    for (int k = 0; k < word_len; ++k) {
                        word_buffer[k] = tolower(word_buffer[k]);
                    }
                    
                    // 发送到reducer
                    int reduce_id = shuffle_func(word_buffer.data());
                    emit_intermediate(vec->at(get_vec_index(task_id, reduce_id)), 
                                     word_buffer.data(), word_len + 1);
                }
            }
            // 移动到下一个缓冲区
            current_pos += chunk_size;
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
                if (boundary >= data_length) boundary = data_length;
                
                // 确保边界不会切断单词 - 找到下一个非字母字符
                while (boundary < data_length && isalpha(map_data->at(scope, boundary))) {
                    boundary++;
                }
                
                data_dis[i] = boundary - current_offset;
            }
            
            printf("[Splice] Mapper %d: offset=%zu, length=%zu\n", i, current_offset, data_dis[i]);
            current_offset += data_dis[i];
        }
    }
};