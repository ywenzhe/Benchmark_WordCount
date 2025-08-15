#pragma once
#include <cstddef>
#include "dataframe_vector.hpp"
#include "manager.hpp"

namespace far_memory {
    template <typename T>
    class BaseDataManager {
    public:
        virtual ~BaseDataManager() = default;

        // 获取数据向量的指针
        virtual DataFrameVector<T>* get_data() = 0;

        // 获取数据的实际大小
        virtual size_t get_size() = 0;

        // 加载数据的方法
        virtual void load() = 0;
    };
}
