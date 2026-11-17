# CFMDC 存储设计

## 1. 设计目标

- 行情回调线程不阻塞
- 数据可落盘、可追溯
- CSV 与 Parquet 模式可切换
- 多实例运行时不会覆盖已有文件

## 2. 存储模式

### 2.1 CSV

- 每个合约一个文件
- 文件名：`{InstrumentID}_{TradingDay}.csv`
- 文件路径：`History.CSVPath` 指定目录

示例：

```
./data/csv/20250110/
  IF2501_20250110.csv
  IC2501_20250110.csv
```

### 2.2 Parquet

- 每次运行生成一个文件
- 文件名：`YYYYMMDD_HHMMSS_mmm.parquet`
- 文件路径：`History.ParquetPath` 指定目录

示例：

```
./data/parquet/20250110/
  20250110_093015_123.parquet
  20250110_210030_789.parquet
```

### 2.3 Hybrid

同时写入 CSV 与 Parquet。

## 3. 路径占位符

`CSVPath` / `ParquetPath` 支持占位符：

- `{tradingday}`
- `{year}`
- `{month}`
- `{day}`

示例：

```toml
[History]
CSVPath = "./data/{year}/{month}/csv/{tradingday}"
ParquetPath = "./data/{year}/{month}/parquet/{tradingday}"
```

## 4. 异步写入管线

```
OnRtnDepthMarketData
  -> 采集 RecvTimeNs，构造紧凑 MarketTick
  -> LockFreeQueue<MarketTick> (SPSC)
  -> AsyncFileManager 处理线程 (安全校验 / UpdateTime 过滤 / 日期修正)
       -> CsvWriter
       -> ParquetQueue (SPSC) -> Parquet 写线程
```

特性：

- `MarketTick` 为紧凑、可平凡复制的行情快照结构体，减少队列内存占用与拷贝开销
- 行情队列与 Parquet 队列均提供 16384 条可用槽位，`overflow_count_` 独立缓存行对齐以避免伪共享
- CSV/Hybrid 模式下将进程文件句柄上限 `FILE_HANDLE_LIMIT` 设为 `8192`
- 队列满时直接丢弃并限频告警
- 线程支持 cpuset 感知的绑核（`WorkerThreadCore` / `ParquetWorkerThreadCore`）与自适应退避，降低 CPU 空转
- 统计指标（每分钟记录一次）：`stored`、`csv`、`parquet`、`ingress_queue`、`parquet_queue`、`filtered`（窗口外被过滤）、`invalid`（字段或合约 ID 非法）、`dropped`（队列满丢弃）、`write_failures`、`csv_flushes`

## 5. TradingDay / ActionDay 与过滤策略

### 5.1 时区无关计算

启动窗口判定与 `base_action_day` / `next_action_day` 均基于固定 `UTC+8`（`Asia/Shanghai`，无夏令时）的纯 C++ `<chrono>` 整数历法逻辑计算，不依赖系统本地时区或 `TZ` 环境变量，在默认 UTC 的 Docker 容器内也可正确工作。

### 5.2 TradingDay 修正

TradingDay 来自 TraderSpi 的登录结果。写盘前会覆盖行情原始 TradingDay。

### 5.3 ActionDay 规则

在写盘前进行修正：

- `UpdateTime <= 07:00:00` -> 使用 `next_action_day`
- 否则使用 `base_action_day`

`base_action_day` / `next_action_day` 由固定 `UTC+8` 启动时间计算。

### 5.4 UpdateTime 过滤窗口

根据固定 `UTC+8` 启动时间决定保留区间（被过滤的数据计入 `filtered`）：

| 启动窗口 (`UTC+8`) | 保留 UpdateTime |
| --- | --- |
| `< 07:00` | `00:00:00 - 07:00:00` |
| `07:00 - 17:00` | `08:55:00 - 15:30:00` |
| `> 17:00` | `20:55:00 - 23:59:59` 和 `00:00:00 - 03:00:00` |

## 6. 数据列、清洗与类型精度

### 6.1 44 列输出结构

CSV 与 Parquet 均输出 44 列：在原 43 个 CTP 行情字段（`TradingDay`, `InstrumentID`, `ExchangeID`, `LastPrice`, ..., `AveragePrice`, `ActionDay`）之后，新增第 44 列 **`RecvTimeNs`**（在 `OnRtnDepthMarketData` 入口采集的纳秒级 Unix Epoch 时间戳，Parquet 中类型为 `int64`）。

### 6.2 无效价格与精度处理

- **无效 CTP 价格**：通过 `is_valid_ctp_price` 识别 `DBL_MAX`、`-DBL_MAX`、`NaN` 和 `±Inf` 等无效值；在 CSV 中写出空字段（`""`），在 Parquet 中写入 `NULL`
- **保留真实 `0.0` 与负价差**：合法的 `0.0` 以及合法负价差均原样保留，不与缺失值混淆
- **`Turnover` 全精度**：CSV 中 `Turnover` 以完整浮点小数精度输出（不截断为整数），Parquet 中以 `float64` 存储

## 7. Parquet 参数

当前参数：

- `compression`: `zstd`
- `compression_level`: `3`
- `row_group_size`: 由 `History.ParquetRowGroupSize` 配置，默认 `100000`

Parquet 写入按 `row_group_size` 缓冲并写出 row group。

Parquet 使用独立队列和线程，Row Group 构建与压缩不会阻塞主处理线程或 CSV writer。

## 8. 构建兼容性

若构建时关闭 `CFMDC_ENABLE_PARQUET`，仅允许 CSV 模式；配置为 Parquet/Hybrid 会在启动阶段明确失败，避免静默改变输出格式。

