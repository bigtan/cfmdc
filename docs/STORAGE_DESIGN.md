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
  -> LockFreeQueue (SPSC)
  -> AsyncFileManager worker
  -> CsvWriter / ParquetBatchWriter
```

特性：

- 队列容量固定为 16384
- 队列满时直接丢弃并限频告警
- 线程自适应退避，降低 CPU 空转

## 5. TradingDay / ActionDay 与过滤策略

### 5.1 TradingDay 修正

TradingDay 来自 TraderSpi 的登录结果。写盘前会覆盖行情原始 TradingDay。

### 5.2 ActionDay 规则

在写盘前进行修正：

- UpdateTime <= 07:00:00 -> 使用 `next_action_day`
- 否则使用 `base_action_day`

`base_action_day` / `next_action_day` 由启动时间计算。

### 5.3 UpdateTime 过滤窗口

根据启动时间决定保留区间：

| 启动窗口 | 保留 UpdateTime |
| --- | --- |
| < 07:00 | 00:00:00 - 07:00:00 |
| 07:00 - 17:00 | 08:55:00 - 15:30:00 |
| > 17:00 | 20:55:00 - 23:59:59 和 00:00:00 - 03:00:00 |

## 6. Parquet 参数（当前编译内置）

当前版本未从配置文件读取 Parquet 参数，默认值在代码中固定：

- compression: `zstd`
- compression_level: `3`
- batch_size: `10000`
- row_group_size: `100000`

如需调整，请修改 `src/utils/FileManager.cpp` 中 `ParquetMarketDataWriter::Config` 的初始化配置。

## 7. 构建兼容性

若构建时关闭 `CFMDC_ENABLE_PARQUET`，配置为 Parquet/Hybrid 将自动降级为 CSV。

