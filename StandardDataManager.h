// FileDataSource.h
#ifndef UNTITLED25_FILEDATASOURCE_H
#define UNTITLED25_FILEDATASOURCE_H

#include "BaseDataManager.h"
#include <string>
#include <fstream>
#include <stdexcept>

class FileDataSource : public IDataSource {
private:
    BaseAllocator* allocator;
    std::string filepath;
    char* buffer;
    size_t length;

public:
    FileDataSource(const std::string& path, BaseAllocator* alloc)
        : allocator(alloc), filepath(path), buffer(nullptr), length(0) {
        if (!allocator) throw std::runtime_error("Allocator cannot be null for FileDataSource.");
    }

    ~FileDataSource() override {
        if (buffer) {
            allocator->deallocate(buffer);
        }
    }

    void load() override {
        std::cout << "Reading data from file: " << filepath << "..." << std::endl;
        std::ifstream file(filepath, std::ios::binary | std::ios::ate);
        if (!file.is_open()) {
            throw std::runtime_error("Failed to open file: " + filepath);
        }
        std::streamsize size = file.tellg();
        file.seekg(0, std::ios::beg);
        
        buffer = (char*)allocator->allocate(size + 1, 4096);
        if (!buffer) {
            throw std::runtime_error("Failed to allocate buffer for file content.");
        }
        if (!file.read(buffer, size)) {
            allocator->deallocate(buffer);
            buffer = nullptr;
            throw std::runtime_error("Failed to read file into buffer: " + filepath);
        }
        buffer[size] = '\0';
        length = size;
        file.close();
        std::cout << "Read " << length / (1024.0 * 1024.0) << " MB." << std::endl;
    }

    void* get_data() override { return buffer; }
    size_t get_size() override { return length; }
};

#endif