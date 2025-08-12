# 服务器配置

#### 硬件环境
- 两颗 32 核 Intel Xeon GOLD 6430 CPU (主频 2.10GHz)
- 256GB 内存
- 以及一块支持CXL的Supermicro x13dei 主板。
- CXL-DSM 启用了超线程，但根据先前工作的建议，禁用了CPUC-states、动态 CPU 频率调整和透明大页。

#### 软件环境
- Linux Kernel >= 6.9.0-smdk
- OS version >= Ubuntu 22.04.5 LTS
- CMake >= 3.22.1
- Jemalloc >= 5.2.1-4ubuntu
- Intel TBB: tbb >= 2021.5.0-7ubuntu2, tbb-devel >= 2021.5.0-7ubuntu2
- gcc >= 11.4.0

# 数据集——The News Crawl Corpus

- 容量：压缩包约24GB，解压缩后预估扩展一倍，之后全部下载完成后会给出精确数值。

![img](https://jianmucloud.feishu.cn/space/api/box/stream/download/asynccode/?code=MGY1YTM5ZjVjMDdiMjk1NzNlODIyMWE0ZjY3NWFhMjJfZldkVnFIamNHdVBrSG10UzRLN0Q2WERONjRyTnB3UmxfVG9rZW46V0RFSmJDeEFIb2w0SHJ4T0pPRmNDQVpXbkljXzE3NTQ5NjU2NTc6MTc1NDk2OTI1N19WNA)

- 来源：https://data.statmt.org/news-crawl/en/
- 格式：解压后，为文本文档
- 内容摘要：

![img](https://jianmucloud.feishu.cn/space/api/box/stream/download/asynccode/?code=ZDU0ZTU0MTRmYmQ0MjgxOGYzNjZhOGEyMTMwYTVkN2JfdEd2QU9EaHQ3ckg2dU9kNnRMN0tMUFpvU1NCR2wyektfVG9rZW46R2JhQ2J4YnJQb3BCa3Z4cDBoNmNmcHowbjJlXzE3NTQ5NjU2NTc6MTc1NDk2OTI1N19WNA)

# WordCount使用方法


1. 准备/下载数据集

WordCount程序需要一个纯文本文件作为输入。任何标准的UTF-8或ASCII编码的文本文档，其中单词以空格、换行符等标准分隔符隔开，均可作为本项目的数据集，这里推荐使用上文提到的The News Crawl Corpus数据集，下载方式如下：

```
 aria2c -c -x 16 -s 16 "https://data.statmt.org/news-crawl/en/news.2022.en.shuffled.deduped.gz"
```

这个命令的2022可以替换为2007——2024，可以根据需求将文档合并为自己适合的大小。

2. 解压

```bash
gunzip news.2022.en.shuffled.deduped.gz
```

3. 编译

```
g++ -std=c++17 -Wall -O2 main.cpp -o wordcount_df -pthread
```

4. 运行

运行程序需要提供一系列命令行参数来指定任务配置和数据源。

- 命令格式:

  ```
  ./<可执行文件名> <map任务数> <reduce任务数> <内存分配器> <数据源类型> <数据源路径或大小>
  ```

- 参数详解:

  1. <map任务数> (argv[1]): 指定Map阶段并发运行的任务数量（线程数）。
  2. <reduce任务数> (argv[2]): 指定Reduce阶段并发运行的任务数量（线程数）。
  3. <内存分配器> (argv[3]): 指定使用的内存分配器。目前只支持 **standard**，如有需要可以自定义Allocator，并按照代码结构及框架使用说明加入到此MapReduce框架中。
  4. <数据源类型> (argv[4]): 指定数据从哪里来。
     - **file**: 从文件读取。
     - **generated**: 程序内部生成随机数据（用于测试）。
  5. <数据源路径或大小> (argv[5]):
     - 如果数据源类型是 file，这里应提供 **文件的完整路径**。
     - 如果数据源类型是 generated，这里应提供一个 **整数**，代表要生成的随机数据大小（单位：MB）。

- 运行示例:

  - **示例1：从文件运行**
    假设您想用8个Map线程、4个Reduce线程处理名为 sample.txt 的文件：

  ```bash
  ./wordcount_runner 8 4 standard file ./sample.txt
  ```

  - **示例2：使用内部生成的数据运行**
    假设您想测试程序性能，使用16个Map线程和8个Reduce线程处理动态生成的1024MB数据：

  ```bash
  ./wordcount_runner 16 8 standard generated 1024
  ```

- 推荐运行命令:
  如果使用项目中的标准数据集和wordcount_df可执行文件，一个典型的运行命令如下（假设使用20个线程）：

  ```bash
  ./wordcount_df 20 20 standard file ./datasets/news.2024.en.shuffled.deduped
  ```

5. （可选）如果想要进行TPP，Weighted Interleaving对Kmeans的评估，可以执行total_experiment.sh脚本

```bash
sudo bash total_experiment.sh
```

# 代码结构及框架使用说明

## 一. 基本接口

### 1.1 内存分配器（BaseAllocator.h与StandardAllocator.h）

#### 1.1.1 简介

BaseAllocator.h为内存分配器的接口文件，一般无需改动。

如果使用此benchmark的应用API有涉及到与malloc，free不同的内存分配与回收方案时，需要继承此接口，来实现个性化的Allocator。

这里给出了一个样例StandardAllocator.h，其本质上是对标准库 `malloc` 和 `free` 的一层封装。

#### 1.1.2 实际使用

1. 继承BaseAllocator.h，写一个单独的exampleAllocator.h，格式参考StandardAllocator.h。
2. 修改main.cpp文件

![img](https://jianmucloud.feishu.cn/space/api/box/stream/download/asynccode/?code=MTUzZjlkMWUwOTdjN2ZhN2ZhYWRmOGMxOWI0YTlhNjdfWnpKQWFyeXhiQktvcUJOZWNuRlU3aGdISnFoR2ZISVBfVG9rZW46RG16UGJXeVo4b0tHbXp4UkJ5bWN6MHZJbk1jXzE3NTQ5Njc2Nzc6MTc1NDk3MTI3N19WNA)

在这里加入一个else分支，让allocator初始化为自定义的exampleAllocator。

![img](https://jianmucloud.feishu.cn/space/api/box/stream/download/asynccode/?code=MjI5YzU5NzNiOTQ1YzQ5OWVkYjdkOGExZjg3ZmI1MTFfelFTeW1kQmN5Uk1VbXluTE9zbWR3V0MzWk41ZHVSak1fVG9rZW46WFZvdmJsVnFxb0tyWXp4aW5jM2N1eWJ0bndmXzE3NTQ5Njc2Nzc6MTc1NDk3MTI3N19WNA)

为了美观，也可以修改上图main.cpp中的print_usage函数的第22行，加入用户自定义的Allocator名。

1. 在执行可执行文件时，命令需要加上图中自定义的名称。

如原来的命令是

```Bash
./wordcount 4 2 standard file ./sample.txt
```

现在应该换成

```Bash
./wordcount 4 2 example file ./sample.txt
```

## 1.2 数据源管理器 (BaseDataManager.h, StandardDataManager.h, RondomGeneratedDataManager.h)

BaseDataManager.h是数据读写的基本接口，用来管理数据的加载。

这里给了两种继承BaseDataManager.h接口的样例，其中StandardDataManager.h为从文件读写的类，RondomGeneratedDataManager.h是随机生成数据的类。

本来这个接口是为了AIFM的Dataframe准备的，因为这个应用里实现了数据读写的API，但最后没有使用，但也保留了这个接口，一般应该不需要改动。

如果改动的话，需要在main.cpp中配合修改：

1. 首先在main.cpp中加入用户写好的头文件，如exmapleDataSource.h。
2. 在main.cpp创建数据源部分加入你的else if分支，将dataSource示例化为你创建的exmapleDataSource。

![img](https://jianmucloud.feishu.cn/space/api/box/stream/download/asynccode/?code=NmNlODQ0MGQ2NjZmOTlhYjQxZjdhZTJlNmEzOWQyYjNfSTN6RWhLS2gyeXNHNnNGaE9XeXU4ZURrQmVWYVVUS3lfVG9rZW46TlQzbmJQbWl4b3BPdER4bDByZGNyZmlMbjllXzE3NTQ5Njc2Nzc6MTc1NDk3MTI3N19WNA)

# 二. MapReduce接口(MapReduce.h)

## 2.1 简介

此接口是benchmark的核心业务接口，定义了`MapReduce` 基类，封装了所有通用的、与具体业务无关的逻辑，包括：

- 线程的创建、管理与同步（使用 `pthread_barrier`）。
- 启动 Mapper 和 Reducer 任务。
- 测量并报告总执行时间。
- 管理中间数据的存储结构。

## 2.2 执行流程（run_mr函数）

run_mr_single_iteration/run_mr函数是驱动单次MapReduce计算流程的核心。此函数的执行流程几乎等同于整个MapReduce任务的完整流程，其流程如下：

1. **初始化内存分配器 (****`allocator->init()`****)**:

默认是一个空操作，除非用户实现了自己的Allocator示例。

2. **数据分片 (****`splice(...)`****)**:

`splice` 函数的职责是将原始的、连续的输入数据块（`map_data`）分割成多个小的数据片段。

实际执行的是子类实现的 `splice` 纯虚函数。

3. **启动** **`Mapper`** **和** **`Reducer`** **线程**

4. **Map阶段：**

执行map_func任务，其中必要的一步是调用 `emit_intermediate` 函数，将处理得到的中间结果（键值对）存入共享的中间数据结构 `vec` 中。

每个 `Mapper` 线程完成其 `map_func` 后，会在 `pthread_barrier_wait(&barrier_map)` 处阻塞等待。

5. **Reduce阶段**:

通过第一次同步后，所有 `Reducer` 线程开始并发执行。每个线程调用由子类实现的 `reduce_func`。

`reduce_func` 会根据自身的 `task_id` 从中间数据结构 `vec` 中拉取（pull）所有`Mapper`为其生成的中间数据，并进行汇总计算。

每个 `Reducer` 线程完成其 `reduce_func` 后，会在 `pthread_barrier_wait(&barrier_reduce)` 处阻塞等待。

6. **线程汇合与资源清理 (****`join`** **and** **`free`****)：**

主线程通过调用每个 `std::thread` 对象的 `join()` 方法，等待所有 `Mapper` 和 `Reducer` 线程执行完毕。

释放为线程参数动态分配的内存（`map_parameter` 和 `reduce_parameter`）以及为数据分片分配的辅助数组（`map_data_arr` 和 `map_data_dis`）。

7. **关闭内存分配器 (****`allocator->shutdown()`****)**:

通知底层内存分配器本次计算已结束，可以进行资源回收等清理工作（例如，解除共享内存段的附加）。

## 三. MapReduce的示例任务 ——WordCount

WordCount的任务是统计给定文本中每个单词出现的次数。我们将通过`WordCount.h`（业务实现）和`main.cpp`（驱动程序）两个文件来展示如何利用我们的框架完成这个任务。

### 3.1 WordCount.h

`WordCount`类继承自`MapReduce`基类，它继承了 `MapReduce` 类的所有通用并行框架逻辑，并在此基础上，通过重写三个核心的纯虚函数（`map_func`, `reduce_func`, `splice`），注入了K-均值算法的特定业务逻辑。这里简要介绍一下这几个重写函数的主要工作：

1. **`splice`** **- 文本分片** 此函数负责将大块的文本数据分割成`map_num`份，分配给各个`Mapper`。为了避免将一个完整的单词从中间切断，它在切分时会智能地寻找单词边界（如空格或标点符号），确保每个`Mapper`收到的都是完整的文本片段。
2. **`map_func`** **- 单词提取与发射** 每个`Mapper`线程在此函数中处理它分到的文本片段。
   1. **提取**: 它会遍历文本，识别出一个个独立的单词，并跳过空格和标点。
   2. **标准化**: 为了统一计数，所有单词都会被转换成小写。
   3. **发射 (Emit)**: 对于提取出的每一个单词，它会调用`emit_intermediate`函数。与KMeans不同的是，它需要先通过一个`shuffle_func`（基于DJB2哈希算法）计算出这个单词应该由哪个`Reducer`来处理，然后将单词作为中间"键"（Key）发送给该`Reducer`。
3. **`reduce_func`** **- 单词汇总统计** 每个`Reducer`线程负责处理一部分哈希值相同的单词。
   1. **收集**: 它会从所有`Mapper`那里收集被分配给自己的单词列表。
   2. **统计**: 使用一个哈希表（`std::unordered_map`）来存储单词和其对应的计数值。每当收到一个单词，就在哈希表中将其计数值加一。
   3. **输出**: 当所有单词处理完毕后，（在这个示例中）它会打印出自己处理了多少个**独立不重复**的单词。在实际应用中，这里通常会将最终的统计结果写入到输出文件。

### 3.2 main.cpp

`main.cpp`是专为WordCount任务设计的驱动程序，主要依靠调用上层接口来完成整体任务，会将数据源、内存分配器、MapReduce任务组合在一起，并按照预定的逻辑顺序（加载数据 -> 运行计算 -> 输出结果）来驱动整个流程。所有实现MapReduce任务的main.cpp都可以参考本文件的基本流程：

1. 参数解析：

从命令行中读取了运行参数（如输入输出的文件路径），也可以自定义一些自己需要的参数，其中详细参数如下：

- **`<map_tasks>`** **(argv)**: `Map`任务的数量。决定启动多少个`Mapper`线程并行处理文本。通过 `std::stoi(argv[1])` 解析。
- **`<reduce_tasks>`** **(argv)**: `Reduce`任务的数量。决定启动多少个`Reducer`线程并行统计单词。通过 `std::stoi(argv[2])` 解析。
- **`<allocator>`** **(argv)**: 指定要使用的**内存分配器类型**。这是一个字符串（直接读取`argv[3]`），程序会根据这个字符串的值（例如，`"standard"`）来决定后续创建哪一种具体的分配器对象。这使得更换内存管理策略无需重新编译代码。
- **`<data_source>`** **(argv)**: 指定**数据来源的类型**。这是一个字符串（直接读取`argv[4]`），用于告诉程序数据是从文件中读取（例如，`"file"`）还是需要动态生成（例如，`"generated"`）。
- **`[source_spec]`** **(argv)**: 这是一个**依赖于前一个参数的**特定于数据源的参数。它的含义会根据`<data_source>`的值而改变：
  - **如果****`<data_source>`****是****`"file"`**: 那么`[source_spec]`（即`argv[5]`）应为一个**字符串**，代表**输入文件的路径**。
  - **如果****`<data_source>`****是****`"generated"`**: 那么`[source_spec]`（即`argv[5]`）应为一个**数字**，代表需要**随机生成的文本数据的大小**（单位为MB）。程序会使用 `std::stoull` 将其转换为数值。

1. 组件的动态创建
   1. **分配器**: 根据用户输入的字符串（如`"standard"`），使用`if-else`语句创建出对应的分配器对象（`StandardAllocator`）。
   2. **数据源**: 同样地，根据`"file"`或`"generated"`来创建`FileDataSource`或`GeneratedDataSource`对象，实现了数据来源的动态选择。
2. 创建WordCount MapReduce任务
3. 执行任务

在所有组件都准备好之后，实例化`WordCount`任务，并将之前创建的分配器注入进去。 与KMeans不同，WordCount任务通常**只需要执行一次**，不需要迭代。因此，这里直接调用了`run_mr`函数（而不是`run_mr_single_iteration`），由它来完成一次完整的MapReduce流程，并处理内部的计时和信息打印。

### 3.3 total_experiment.sh

前面的WordCount主体文件是一个独立完整的Benchmark，这个脚本仅是为了评估TPP，Weighted Interleaving更改对WordCount任务的影响而编写的，不适用于其他目的，也不适合移植到其他实验。

**如果同样想评估以上功能，**此脚本中的**超参数**应该是用户最应当关注和修改的部分，以适配不同的硬件环境和测试目标。

#### 3.3.1 主要功能

本实验旨在探索不同的TPP，Weighted Interleaving等变量设置下，会怎样影响页面调度以及程序的执行情况。详细实验目的，实验变量设置，实验数据，数据分析，注意事项等请参考以下笔记。

[TPP，Weighted Interleaving对WordCount的影响](https://jianmucloud.feishu.cn/wiki/UPX9wwpH1iHHikk4a61cisptnxc?from=from_copylink)

#### 3.3.2 超参数列举与说明

- **`EXECUTABLE`**: 指定要运行的WordCount C++程序的可执行文件名。当前值为`./wordcount_df`。
- **`DATASET_PATH`**: 定义用作输入的文本文件的完整路径。
- **`FIXED_THREADS_FOR_RATIO_TEST`**: 在第一阶段（内存配比测试）中，固定的线程数量。
- **`THREAD_LIST_FOR_SCALING_TEST`**: 在第二阶段（线程扩展性测试）中，要遍历的线程数列表。
- **`FIXED_RATIO_FOR_SCALING_TEST`**: 在第二阶段（线程扩展性测试）中，固定的内存分配策略。
- **`CPU_NODE_BIND`**: 将程序的所有线程绑定到指定的CPU节点（NUMA Node）上。
- **`LOCAL_MEM_NODE`**: 定义哪个NUMA节点ID被视为“本地内存”（通常是DRAM所在的节点）。
- **`CXL_MEM_NODE`**: 定义哪个NUMA节点ID被视为“CXL内存”。
- **`scenarios`**: 定义不同的系统级内存管理场景，主要用于开启（`TPP_ON`）或关闭（`TPP_OFF`）内核的自动内存页升降级功能。
- **`NUM_RUNS`**: 定义每种测试配置的重复执行次数，用于减少偶然误差。
- **`OUTPUT_CSV`**: 指定保存所有测试结果的CSV数据文件的名称。
- **`policy_ord
