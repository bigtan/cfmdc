# CFMDC 架构设计

## 1. 项目定位

CFMDC 是面向期货行情的高性能采集器，使用 CTP 行情与交易接口。
系统目标是稳定、低延迟地接收行情，并以异步方式落盘，支持 CSV/Parquet 等存储模式。

## 2. 模块划分

### Core

- `Application`: 生命周期管理、信号处理、初始化流程、流目录拆分、合约过滤与订阅流程
- `TraderSpi`: 认证、登录、结算确认、全市场合约查询（完成后立即释放以归还 TD 会话槽位）
- `MdSpi`: 行情登录、订阅、行情回调与 `RecvTimeNs` 采集

### Config

- `Config`: TOML 加载与校验，支持环境变量凭据覆盖（`CFMDC_BROKER_ID`、`CFMDC_USER_ID`、`CFMDC_PASSWORD`、`CFMDC_AUTH_CODE`、`CFMDC_APP_ID`），Front/History/Application 配置解析

### Utils

- `AsyncFileManager`: 异步写入管线（无锁队列 + 后台处理/压缩线程 + 统计计数）
- `CsvWriter`: CSV 写入（每合约一文件，44 列格式，空值表示无效 CTP 价格）
- `ParquetBatchWriter`: Parquet 写入（每次运行一个文件，44 列 Schema，`NULL` 表示无效 CTP 价格）
- `LockFreeQueue`: SPSC 无锁队列（`overflow_count_` 独立缓存行对齐，避免伪共享）
- `MarketDataTimePolicy`: UpdateTime 过滤与 ActionDay 规则
- `TimeUtils`: 固定 `UTC+8`（`Asia/Shanghai`）纯整数历法时间辅助函数（不依赖宿主机 `TZ`）
- `Helpers` / `Error`: 异常体系、CTP 响应检查、有界字符串工具、CTP 价格校验、`is_safe_instrument_id` 安全校验与 cpuset 感知核心解析

### Types

- `MarketTick`: 紧凑且可平凡复制的行情快照结构体（包含 `recv_time_ns`）
- `FrontServer`: 前置配置与连接凭据封装（支持 `clear_secrets()` 内存擦除）
- `StorageMode`: CSV / Parquet / Hybrid 存储模式枚举

## 3. 启动与初始化流程

1. `main` 解析命令行，创建 `Application`（加载 `Config` 并应用 `CFMDC_*` 环境变量凭据覆盖）
2. `Application::run` 注册信号，在 CSV/Hybrid 模式下将进程文件句柄上限提升至 `FILE_HANDLE_LIMIT = 8192`
3. 准备流目录：将 `Application.FlowPath` 拆分为 `<FlowPath>/td` 与 `<FlowPath>/md`，避免 TD/MD 流文件相互干扰
4. 初始化 `TraderSpi`（使用 `<FlowPath>/td`）：多前置重试，完成认证、登录与结算确认，获取权威 `TradingDay`
5. 发起单次全市场 `ReqQryInstrument("")` 查询；在本地按 `Application.SubList`、`Application.IncludeOptions`（bool，默认 `false`）及 `is_safe_instrument_id` 进行过滤与安全校验，随后立即销毁 `TraderSpi` 以释放 TD 会话槽位
6. 基于固定 `UTC+8`（`Asia/Shanghai`）计算启动时间窗口及 `base_action_day` / `next_action_day`
7. 初始化 `MdSpi`（使用 `<FlowPath>/md`）：多前置重试，配置 `AsyncFileManager` 并订阅过滤后的合约列表

## 4. 行情数据流

```
CTP 行情推送
   │
   ▼
MdSpi::OnRtnDepthMarketData
   │  (采集 RecvTimeNs，转为紧凑 MarketTick，仅入队，无锁)
   ▼
LockFreeQueue<MarketTick> (SPSC)
   │
   ▼
AsyncFileManager::worker_loop
   │  (安全校验[invalid]、过滤 UpdateTime[filtered]、修正 TradingDay/ActionDay)
   ├──────────────► CsvWriter ──────────────► 磁盘 (44 列，无效价格为空，全精度 Turnover)
   │
   └► ParquetQueue ─► Parquet writer thread ─► 磁盘 (44 列，无效价格为 NULL)
```

## 5. 时间与交易日策略

### 固定 UTC+8 时区计算

启动窗口与 `ActionDay` 计算均采用纯 C++20 `<chrono>` 整数历法运算，固定使用 `UTC+8`（`Asia/Shanghai`，无夏令时），不依赖宿主机 `localtime`、`TZ` 环境变量或外部 `tzdata`/ICU。即使运行在未设置 `TZ` 的 UTC Docker 容器中，也能保证交易时段判定完全一致。

### TradingDay 来源

- 以 `TraderSpi` 登录成功返回的 `TradingDay` 作为唯一权威值
- `MdSpi` 登录返回值仅用于日志，不参与存储

### ActionDay 规则

启动时由 `Application` 按固定 `UTC+8` 时间计算：

- 启动时间 < 07:00:00
  - `base_action_day` = 昨天（`UTC+8`）
  - `next_action_day` = 今天（`UTC+8`）
- 启动时间 >= 07:00:00
  - `base_action_day` = 今天（`UTC+8`）
  - `next_action_day` = 明天（`UTC+8`）

在 `AsyncFileManager` 中：

- `UpdateTime <= 07:00:00` 使用 `next_action_day`
- 其他时间使用 `base_action_day`

### UpdateTime 过滤窗口

由 `MarketDataTimePolicy` 决定（同样基于固定 `UTC+8` 启动时间）：

- 启动时间 < 07:00:00: 仅保留 00:00:00 - 07:00:00
- 07:00:00 - 17:00:00: 仅保留 08:55:00 - 15:30:00
- 启动时间 > 17:00:00: 仅保留 20:55:00 - 23:59:59 和 00:00:00 - 03:00:00

该过滤在消费线程执行，避免阻塞行情回调；被过滤的快照会累计到统计指标 `filtered` 中。

## 6. 并发与性能

- 行情回调线程仅记录 `RecvTimeNs` 并构造紧凑的 `MarketTick` 入队，无锁且不直接拷贝冗余 CTP 字段
- `LockFreeQueue` 将 `overflow_count_` 放在独立缓存行（`alignas(kCacheLineSize)`），避免与生产者 `tail_`/`head_cache_` 发生伪共享
- 异步处理线程批量出队并执行校验、过滤与 CSV 写盘
- Parquet 使用独立 `MarketTick` SPSC 队列和写线程，隔离 Row Group 构建及压缩暂停
- 支持 cpuset 感知的线程绑核（`Application.WorkerThreadCore` 与 `Application.ParquetWorkerThreadCore`）：当为 `"auto"` 时自动跳过核心 0 并为采集与 Parquet 压缩分配不同的可用核心
- 队列满时丢弃数据并限频告警；处理管线每分钟输出统计指标（`stored`、`csv`、`parquet`、`ingress_queue`、`parquet_queue`、`filtered`、`invalid`、`dropped`、`write_failures`、`csv_flushes`）
- 自适应退避（自旋 -> yield -> sleep）降低空转开销

## 7. 可靠性与可用性

- Trader/Md 初始化支持多前置顺序重试
- `FlowPath` 拆分为 `td` 与 `md` 独立目录，启动查询结束后立即释放 `TraderSpi` 以归还 TD 会话槽位
- 支持 `FrontServer::clear_secrets()` 擦除内存敏感凭据，并通过环境变量覆写与 `0600` 安装权限降低凭据泄露风险
- Trader/Md 初始化使用 `Pending -> Ready/Failed` 显式状态，通过回调发布完成结果
- 认证、登录或结算确认明确失败时立即切换下一前置，未收到终态时由超时机制兜底
- 初始化超时由 `Application.InitTimeout` 控制
- 支持 SIGINT/SIGTERM 优雅退出

## 8. 扩展点

- 新存储格式：新增 `StorageMode`、实现 writer，并在 `AsyncFileManager` 中分发
- 新配置项：扩展 `Config` 校验与解析
- 新数据处理逻辑：在 `AsyncFileManager` 处理中插入清洗或派生字段

## 9. 版本管理

- 项目版本由根目录 `CMakeLists.txt` 中的 `CFMDC_PROJECT_VERSION` 统一定义
- CMake 通过编译定义 `CFMDC_VERSION` 将版本注入目标
- 程序运行时通过 `cfmdc::APP_VERSION` 暴露版本字符串，并用于 `--version`

