// shenango
extern "C" {
#include <runtime/runtime.h>
}
#include "thread.h"
#include "sync.h"

#include "deref_scope.hpp"
#include "device.hpp"
#include "manager.hpp"

#include <fstream>
#include <iostream>
#include <map>
#include <cstring>
#include <list>
#include <vector>
#include <thread>
#include <chrono>
#include <unordered_map>

using namespace far_memory;
using namespace std;

template <uint64_t N, typename T>
struct map_parameter {
    int block_index;
    Array<T, N>* data_array;
    size_t block_num;
    int task_id;
};

struct reduce_parameter {
    int task_id;
};

const static int imm_data_size = 512;

struct imm_data_block {
    char data[imm_data_size];
};

struct imm_data {
    UniquePtr<imm_data_block> data; // 用unique_ptr来管理内存
    int count;                      // 表示存了多少了
};

constexpr size_t kCacheSize = 1250 * Region::kSize;
constexpr size_t kFarMemSize = 20ULL << 30;
constexpr unsigned long kNumGCThreads = 15;
constexpr unsigned long long kNumConnections = 800;

constexpr unsigned long kMapTasks = 10;
constexpr size_t kArrayBlockSize = 8192;
constexpr unsigned long long kArrayBlockNum = (6430559567 / kArrayBlockSize) + 10; // 总数据集size/ArrayBlockSize，需要每次手动改！！！
constexpr unsigned long kReduceTasks = 10;
constexpr const char* kFilePath = "./news.2022.en.shuffled.deduped";

struct array_block {
    char data[kArrayBlockSize];
    int count; // 用于记录当前block中存储的字节数
};

class WordCount {
    public:
    int map_num;
    int reduce_num;
    FarMemManager* manager;

    std::vector<std::list<imm_data>*>* vec;
    inline int get_vec_index(int map_id, int reduce_id) {
        return map_id * reduce_num + reduce_id;
    }

    WordCount(int map_num_ = 1, int reduce_num_ = 1, FarMemManager* manager_ = nullptr) : manager(manager_), map_num(map_num_), reduce_num(reduce_num_) {
        vec = new std::vector<std::list<imm_data> *>();
        for (int i = 0; i < map_num * reduce_num; i++) {
            vec->push_back(new std::list<imm_data>());
        }
    }
    ~WordCount() {}

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

    template <uint64_t N, typename T>
    void map_func(int block_index, Array<T, N>* map_data, int task_id, size_t block_num) {

        //char* buffer = new char[kArrayBlockSize]; // 安全的临时缓冲区
        vector<char> word_buffer;
        vector<char> overflow_word; // 用于存储跨块的单词片段

        for (size_t block_i = 0; block_i < block_num; block_i++) {
            // 获取当前块的数据和大小
            int count;
            const char* buffer;
            {
                DerefScope scope;
                count = map_data->at(scope, block_index + block_i).count;
                buffer = map_data->at(scope, block_index + block_i).data;
            }

            // 处理上一个块末尾可能的单词片段
            int process_start = 0;
            if (!overflow_word.empty()) {
                // 查找当前块中单词的结束位置
                int i = 0;
                while (i < count && (isalpha(buffer[i]) || buffer[i] == '\'')) {
                    i++;
                }

                // 合并单词片段
                word_buffer.clear();
                word_buffer.insert(word_buffer.end(), overflow_word.begin(), overflow_word.end());
                word_buffer.insert(word_buffer.end(), buffer, buffer + i);

                if (i < count) { // 找到了单词的结束
                    // 处理完整单词
                    int word_len = word_buffer.size();
                    word_buffer.push_back('\0');

                    char* word = word_buffer.data();

                    // 转为小写
                    for (int k = 0; k < word_len; ++k) {
                        word[k] = tolower(word[k]);
                    }

                    int reduce_id = shuffle_func(word);
                    emit_intermediate(vec->at(get_vec_index(task_id, reduce_id)), word, word_len + 1);
                }

                overflow_word.clear();
                process_start = i;
            }

            // 处理当前块中的单词
            for (int i = process_start; i < count;) {
                // 跳过非字母字符
                while (i < count && !isalpha(buffer[i])) {
                    i++;
                }

                int start = i;

                // 找到单词的结束位置
                while (i < count && (isalpha(buffer[i]) || buffer[i] == '\'')) {
                    i++;
                }

                // 如果单词在块的末尾被截断
                if (i == count && start < count && block_i < block_num - 1) {
                    // 保存单词片段到overflow_word，等待下一个块处理
                    overflow_word.assign(buffer + start, buffer + i);
                    break;
                }

                // 处理完整单词
                if (i > start) {
                    int word_len = i - start;
                    word_buffer.clear();
                    word_buffer.assign(buffer + start, buffer + i);
                    word_buffer.push_back('\0');

                    char* word = word_buffer.data();

                    // 转为小写以统一计数
                    for (int k = 0; k < word_len; ++k) {
                        word[k] = tolower(word[k]);
                    }

                    int reduce_id = shuffle_func(word);
                    emit_intermediate(vec->at(get_vec_index(task_id, reduce_id)), word, word_len + 1);
                }
            }
        }
        //delete[] buffer;
    };


    void mapper(void* arg) {
        struct map_parameter<kArrayBlockNum, array_block>* para = (struct map_parameter<kArrayBlockNum, array_block>*)arg;

        printf("[Mapper %d Start] block_index=%d, block_num=%zu\n", para->task_id, para->block_index, para->block_num);
        map_func(para->block_index, para->data_array, para->task_id, para->block_num);
    }

    void reduce_func(int task_id) {
        std::unordered_map<std::string, int> word_counts;

        for (int map_id = 0; map_id < map_num; map_id++) {
            std::list<imm_data>* inter = vec->at(get_vec_index(map_id, task_id));
            for (auto& data_block : *inter) {
                DerefScope scope;
                const char* current_word = data_block.data.deref(scope)->data;
                const char* end_of_block = current_word + data_block.count;

                while (current_word < end_of_block) {
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

    void reducer(void* arg) {
        reduce_parameter* para = (reduce_parameter*)arg;
        printf("[Reducer %d Start]\n", para->task_id);
        reduce_func(para->task_id);
    }

    void splice(int* data_arr, int* data_dis, size_t block_num) {
        size_t avg_block_per_map = block_num / map_num;
        int index = 0;
        for (int i = 0; i < map_num; ++i) {
            data_arr[i] = index;
            if (index + avg_block_per_map < block_num) {
                data_dis[i] = avg_block_per_map;
            } else {
                data_dis[i] = block_num - index; // 最后一个map处理剩余的所有block
            }
            index += data_dis[i];
        }
    };

    void emit_intermediate(std::list<imm_data>* inter, const void* data, int len) {
        if (inter->empty() || inter->back().count + len + 1 > imm_data_size) {
            struct imm_data inter_en;
            inter_en.count = 0;
            inter_en.data = manager->allocate_unique_ptr<imm_data_block>(); // 用block 512 (unique ptr)来存
            // printf("SHIT\n");
            inter->push_back(std::move(inter_en));
        }
        {
            DerefScope scope;
            auto imm_data_block_ptr = inter->back().data.deref_mut(scope);
            memcpy(imm_data_block_ptr->data + inter->back().count, data, len);
        }
        inter->back().count += len;
        return;
    }

    template <uint64_t N, typename T>
    void run_mr(Array<T, N>* data_array) {
        printf("--- Running Benchmark ---\n");
        printf("Map Tasks: %d, Reduce Tasks: %d\n", map_num, reduce_num);
        std::vector<rt::Thread> map_threads, reduce_threads;

        int* map_data_arr = (int*)malloc(sizeof(int) * map_num); // 表示从data_array中开始处理的index
        int* map_data_dis = (int*)malloc(sizeof(int) * map_num); // 表示每个map需要处理的array_block数量
        splice(map_data_arr, map_data_dis, kArrayBlockNum);

        auto start = std::chrono::system_clock::now();
        for (int i = 0; i < map_num; i++) {
            map_parameter<N, T>* mp = new map_parameter<N, T>();
            mp->task_id = i;
            mp->block_num = map_data_dis[i];
            mp->data_array = data_array;
            mp->block_index = map_data_arr[i];

            map_threads.emplace_back(rt::Thread([this, mp]()
                {
                    mapper(mp);
                    delete mp; // 在线程完成后释放参数
                }));
        }

        for (auto& th : map_threads) {
            th.Join();
        }

        for (int i = 0; i < reduce_num; i++) {
            reduce_parameter* rp = new reduce_parameter();
            rp->task_id = i;

            reduce_threads.emplace_back(rt::Thread([this, rp]()
                {
                    reducer(rp);
                    delete rp; // 在线程完成后释放参数
                }));
        }

        for (auto& th : reduce_threads) {
            th.Join();
        }
        auto end = std::chrono::system_clock::now();
        std::chrono::duration<double> diff = end - start;
        printf("[TOTAL TIME] %f seconds\n", diff.count());

        free(map_data_arr);
        free(map_data_dis);
    };
};

void do_work(FarMemManager* manager) {
    // 加载数据
    // auto data_manager = std::make_unique<AIFMDataManager>(manager, kFilePath);
    // data_manager->load();
    std::cout << "Reading data from file: " << *kFilePath << "..." << std::endl;
    std::ifstream file(kFilePath, std::ios::binary | std::ios::ate);
    if (!file.is_open()) {
        throw std::runtime_error("Failed to open file: " + *kFilePath);
    }

    size_t length = file.tellg(); // bytes
    std::cout << "File size: " << length << " bytes" << std::endl;
    file.seekg(0, std::ios::beg);

    // 创建数据容器：data array
    auto data_array = manager->allocate_array<array_block, kArrayBlockNum>();

    char* buffer = new char[kArrayBlockSize];
    size_t total_read = 0;
    int block_index = 0;

    while (total_read < length && file.good()) {
        // 读取一个array_block的数据并写入data array
        size_t to_read = min(kArrayBlockSize, length - total_read);
        DerefScope scope;
        file.read(data_array.at_mut(scope, block_index).data, to_read);
        size_t actual_read = file.gcount();
        data_array.at_mut(scope, block_index).count = actual_read; // 更新当前block的字节数

        // TODO:这里还需要做断词检测处理，避免array_block中最后存储的单词被切割

        total_read += actual_read;
        block_index++;
    }

    std::cout << "check read file done" << std::endl;
    file.close();

    auto map_reduce = std::make_unique<WordCount>(kMapTasks, kReduceTasks, manager);
    map_reduce->run_mr(&data_array);
}

int argc;
void _main(void* arg) {
    char** argv = (char**)arg;
    std::string ip_addr_port(argv[1]);

    auto manager = std::unique_ptr<FarMemManager>(
        FarMemManagerFactory::build(
            kCacheSize, // 只使用IMMCache
            kNumGCThreads,
            new TCPDevice(helpers::str_to_netaddr(ip_addr_port), kNumConnections, kFarMemSize)));
    do_work(manager.get());
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
