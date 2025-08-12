#ifndef UNTITLED25_WORDCOUNT_H
#define UNTITLED25_WORDCOUNT_H

#include "MapReduce.h"
#include <string>
#include <unordered_map>
#include <cctype>
#include <vector> // 用于临时缓冲区

class WordCount : public MapReduce {
public:
    WordCount(int map_num, int reduce_num, BaseAllocator* alloc)
            : MapReduce(map_num, reduce_num, alloc) {}

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
    // [已修复] data_length 从 int 改为 size_t 以匹配基类
    void map_func(void* map_data, int task_id, size_t data_length) override {
        char* str_data = (char*)map_data;
        std::vector<char> word_buffer; // 安全的临时缓冲区

        for (uint64_t i = 0; i < data_length; ) {
            // 跳过非字母字符
            while (i < data_length && !isalpha(str_data[i])) {
                i++;
            }
            
            // 记录单词起点
            uint64_t start = i;
            
            // 找到单词终点
            while (i < data_length && (isalpha(str_data[i]) || str_data[i] == '\'')) {
                i++;
            }

            if (i > start) {
                // 将找到的单词复制到安全缓冲区
                int word_len = i - start;
                word_buffer.assign(str_data + start, str_data + i);
                word_buffer.push_back('\0'); // 添加null终止符

                char* word = word_buffer.data();

                // 转为小写以统一计数
                for(int k = 0; k < word_len; ++k) {
                    word[k] = tolower(word[k]);
                }

                int reduce_id = shuffle_func(word);
                
                emit_intermediate(vec->at(get_vec_index(task_id, reduce_id)), word, word_len + 1);
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
    // [已修复] data_length 从 int 改为 size_t 以匹配基类
    void splice(char** data_arr, size_t* data_dis, char* map_data, size_t data_length) override {
        size_t avg_len_per_map = data_length / map_num;
        size_t current_offset = 0;

        for (int i = 0; i < map_num; ++i) {
            data_arr[i] = map_data + current_offset;

            if (i == map_num - 1) {
                data_dis[i] = data_length - current_offset;
            } else {
                size_t boundary = current_offset + avg_len_per_map;
                if (boundary >= data_length) boundary = data_length;
                
                // 确保边界不会切断单词
                while (boundary < data_length && isalpha(map_data[boundary])) {
                    boundary++;
                }
                
                data_dis[i] = boundary - current_offset;
                current_offset = boundary;
            }
        }
    }
};
#endif //UNTITLED25_WORDCOUNT_H