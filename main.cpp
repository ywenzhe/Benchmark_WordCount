extern "C" {
#include <runtime/runtime.h>
}

// main.cpp (重构后)
#include <iostream>
#include <string>
#include <memory>
#include <stdexcept>
#include <cstring>
#include <cstddef>

#include "WordCount.h"
#include "AIFMDataManager.h"

// FarMemory
#include "helpers.hpp"
#include "manager.hpp"
#include "device.hpp"

constexpr unsigned long long kCacheSize = 1024 * Region::kSize;
constexpr unsigned long long kFarMemSize = 7ULL << 30;
constexpr unsigned long kNumGCThreads = 15;
constexpr unsigned long long kNumConnections = 800;

constexpr unsigned long kMapTasks = 20;
constexpr unsigned long kReduceTasks = 20;
constexpr const char* kFilePath = "./news.2022.en.shuffled.deduped";

void do_work(FarMemManager* manager) {
    auto data_manager = std::make_unique<AIFMDataManager>(manager, kFilePath);
    data_manager->load();

    auto map_reduce = std::make_unique<WordCount>(kMapTasks, kReduceTasks, manager);
    map_reduce->run_mr(data_manager->get_data(), data_manager->get_size());
}

void run(netaddr raddr) {
    auto manager = std::unique_ptr<FarMemManager>(
        FarMemManagerFactory::build(
            kCacheSize,
            kNumGCThreads,
            new TCPDevice(raddr, kNumConnections, kFarMemSize)
        )
    );

    do_work(manager.get());
}

int argc;
void _main(void* arg) {
    char** argv = (char**)arg;
    std::string ip_addr_port(argv[1]);
    run(helpers::str_to_netaddr(ip_addr_port));
}

int main(int _argc, char* argv[]) {
    int ret;

    if (_argc < 3) {
        std::cerr << "usage: [cfg_file] [ip_addr:port]" << std::endl;
        return -EINVAL;
    }

    char conf_path[strlen(argv[1]) + 1];
    strcpy(conf_path, argv[1]);
    for (int i = 2; i < _argc; i++) {
        argv[i - 1] = argv[i];
    }
    argc = _argc - 1;

    ret = runtime_init(conf_path, _main, argv);
    if (ret) {
        std::cerr << "failed to start runtime" << std::endl;
        return ret;
    }

    return 0;
}