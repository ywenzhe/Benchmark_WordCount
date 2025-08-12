// IDataSource.h
#ifndef UNTITLED25_IDATASOURCE_H
#define UNTITLED25_IDATASOURCE_H

#include "BaseAllocator.h"
#include <cstddef>

// 数据源接口
class IDataSource {
public:
    virtual ~IDataSource() = default;

    // 获取数据缓冲区的指针
    virtual void* get_data() = 0;

    // 获取数据的大小
    virtual size_t get_size() = 0;

    // 一个准备/加载数据的可选方法
    virtual void load() = 0;
};

#endif //UNTITLED25_IDATASOURCE_H