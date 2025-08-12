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
