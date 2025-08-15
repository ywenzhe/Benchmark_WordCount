#pragma once

#include <string>
#include <fstream>
#include <stdexcept>
#include <cstddef>

#include "dataframe_vector.hpp"
#include "manager.hpp"
#include "deref_scope.hpp"
#include "BaseDataManager.h"

using namespace far_memory;

class AIFMDataManager : public BaseDataManager<char> {
    private:
    FarMemManager* manager;
    std::string file_path;
    size_t length = 0;

    DataFrameVector<char>* data_vec;

    public:
    AIFMDataManager(FarMemManager* manager_, const std::string& path)
        : manager(manager_), file_path(path) {
            data_vec = manager->allocate_dataframe_vector_heap<char>();
    }

    ~AIFMDataManager() {}

    void load() override {
        std::cout << "Reading data from file: " << file_path << "..." << std::endl;
        std::ifstream file(file_path, std::ios::binary | std::ios::ate);
        if (!file.is_open()) {
            throw std::runtime_error("Failed to open file: " + file_path);
        }

        length = file.tellg(); // bytes
        std::cout << "File size: " << length << " bytes" << std::endl;
        
        // 预先调整DataFrameVector大小，避免多次扩容
        data_vec->resize(length);

        file.seekg(0, std::ios::beg);

        // 使用更大的块来读取文件，减少远程访问次数
        const size_t block_size = 4 * 1024 * 1024; // 4MB块
        char* buffer = new char[block_size];
        size_t total_read = 0;

        std::cout << "check read file" << std::endl;
        
        while (total_read < length && file.good()) {
            size_t to_read = std::min(block_size, length - total_read);
            file.read(buffer, to_read);
            size_t actual_read = file.gcount();

            DerefScope scope;
            
            // 批量写入数据到DataFrameVector
            for (size_t i = 0; i < actual_read; i += 4096) {
                size_t batch_size = std::min((size_t)4096, actual_read - i);
                
                // 每4096个字符更新一次DerefScope以避免超时
                if (i > 0) scope.renew();
                
                // 批量写入数据
                for (size_t j = 0; j < batch_size; j++) {
                    data_vec->at_mut(scope, total_read + i + j) = buffer[i + j];
                }
            }

            total_read += actual_read;
            std::cout << "Read " << total_read << "/" << length << " bytes." << std::endl;
        }
        
        delete[] buffer;
        std::cout << "check read file done" << std::endl;
        file.close();
    }

    DataFrameVector<char>* get_data() override {
        return data_vec;
    }

    size_t get_size() override {
        return length;
    }
};