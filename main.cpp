// main.cpp (重构后)

#include <iostream>
#include <string>
#include <memory>
#include <stdexcept>
#include <cstring>

#include "WordCount.h"
#include "StandardAllocator.h"
#include "StandardDataManager.h"
#include "RondomGeneratedDataManager.h" // 根据您的文件命名
pthread_barrier_t barrier_map;
pthread_barrier_t barrier_reduce;
// 更新用法说明以反映简化的参数
void print_usage(const char* prog_name) {
    std::cerr << "用法: " << prog_name << " <map_tasks> <reduce_tasks> <allocator> <data_source> [source_spec]" << std::endl;
    std::cerr << std::endl;
    std::cerr << "参数说明:" << std::endl;
    std::cerr << "  <map_tasks>         Map任务的数量。" << std::endl;
    std::cerr << "  <reduce_tasks>      Reduce任务的数量。" << std::endl;
    std::cerr << "  <allocator>         使用的内存分配器。可选: 'standard'。" << std::endl;
    std::cerr << "  <data_source>       数据来源。可选: 'file', 'generated'。" << std::endl;
    std::cerr << "  [source_spec]       数据源的具体参数:" << std::endl;
    std::cerr << "                        - 如果 data_source 是 'file', 这里是输入文件的路径。" << std::endl;
    std::cerr << "                        - 如果 data_source 是 'generated', 这里是需要生成的随机数据大小 (MB)。" << std::endl;
    std::cerr << std::endl;
    std::cerr << "示例:" << std::endl;
    std::cerr << "  # 使用4个mapper, 2个reducer, 标准分配器, 从文件读取:" << std::endl;
    std::cerr << "  " << prog_name << " 4 2 standard file ./sample.txt" << std::endl;
    std::cerr << std::endl;
    std::cerr << "  # 使用8个mapper, 4个reducer, 标准分配器, 生成512MB数据:" << std::endl;
    std::cerr << "  " << prog_name << " 8 4 standard generated 512" << std::endl;
}

int main(int argc, char **argv) {
    // 至少需要5个参数 (程序名 + 4个主要参数)
    if (argc < 5) {
        print_usage(argv[0]);
        return 1;
    }

    try {
        // 1. 解析命令行参数
        int map_num = std::stoi(argv[1]);
        int reduce_num = std::stoi(argv[2]);
        std::string allocator_name = argv[3];
        std::string source_type = argv[4];
        
        // 检查 [source_spec] 参数是否存在
        if (argc < 6) {
            std::cerr << "错误: 缺少数据源 '" << source_type << "' 所需的 [source_spec] 参数。" << std::endl;
            print_usage(argv[0]);
            return 1;
        }
        std::string source_spec = argv[5];

        // 2. 创建分配器 (Allocator)
        std::unique_ptr<BaseAllocator> allocator;
        if (allocator_name == "standard") {
            allocator = std::make_unique<StandardAllocator>();
        } else {
            throw std::runtime_error("未知的分配器类型: " + allocator_name);
        }

        // 3. 创建数据源 (Data Source)
        std::unique_ptr<IDataSource> dataSource;
        if (source_type == "file") {
            dataSource = std::make_unique<FileDataSource>(source_spec, allocator.get());
        } else if (source_type == "generated") {
            size_t data_size_mb = std::stoull(source_spec);
            dataSource = std::make_unique<GeneratedDataSource>(data_size_mb, allocator.get());
        } else {
            throw std::runtime_error("未知的数据源类型: " + source_type);
        }

        // 4. 创建并运行 WordCount 任务
         auto mr_job = std::make_unique<WordCount>(map_num, reduce_num, allocator.get());
        if (mr_job && dataSource) {
            // 首先，必须调用 load() 来实际加载或生成数据
            dataSource->load(); 
            
            // 然后，获取数据指针和大小，并调用正确的 run_mr 方法
            mr_job->run_mr(static_cast<char*>(dataSource->get_data()), dataSource->get_size());

        } else {
            throw std::runtime_error("初始化MapReduce任务或数据源失败。");
        }

    } catch (const std::exception& e) {
        std::cerr << "发生错误: " << e.what() << std::endl;
        print_usage(argv[0]); // 在出错时打印用法提示
        return 1;
    }

    return 0;
}