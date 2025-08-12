#ifndef UNTITLED25_GENERATEDDATASOURCE_H
#define UNTITLED25_GENERATEDDATASOURCE_H

#include "BaseDataManager.h"
#include <string>
#include <stdexcept>

class GeneratedDataSource : public IDataSource {
private:
    BaseAllocator* allocator;
    char* buffer;
    size_t length;

public:
    // 构造函数直接接收数据大小
    GeneratedDataSource(size_t data_size_mb, BaseAllocator* alloc)
        : allocator(alloc), buffer(nullptr), length(data_size_mb * 1024 * 1024) {
        if (!allocator) throw std::runtime_error("Allocator cannot be null for GeneratedDataSource.");
    }

    ~GeneratedDataSource() override {
        if (buffer) {
            allocator->deallocate(buffer);
        }
    }

    void load() override {
        std::cout << "Generating " << length / (1024*1024) << "MB of random text data..." << std::endl;
        buffer = (char*)allocator->allocate(length, 4096);
        if (!buffer) throw std::runtime_error("Failed to allocate workload buffer.");
        
        srand(time(0));
        const char charset[] = "abcdefghijklmnopqrstuvwxyz ";
        const int charset_size = sizeof(charset) - 1;
        for (size_t i = 0; i < length - 1; ++i) buffer[i] = charset[rand() % charset_size];
        buffer[length - 1] = '\0';
    }

    void* get_data() override { return buffer; }
    size_t get_size() override { return length; }
};

#endif //UNTITLED25_GENERATEDDATASOURCE_H