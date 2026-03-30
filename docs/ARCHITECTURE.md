# CFMDC 架构设计

## 1. 项目定位

CFMDC 是面向期货行情的高性能采集器，使用 CTP 行情与交易接口。
系统目标是稳定、低延迟地接收行情，并以异步方式落盘，支持 CSV/Parquet 等存储模式。

## 2. 模块划分

### Core

- `Application`: 生命周期管理、信号处理、初始化流程、订阅流程
- `TraderSpi`: 认证、登录、结算确认、合约查询
- `MdSpi`: 行情登录、订阅、行情回调

### Config

- `Config`: TOML 加载与校验，Front/History/Application 配置解析

### Utils

- `AsyncFileManager`: 异步写入管线（无锁队列 + 后台线程）
- `CsvWriter`: CSV 写入（每合约一文件）
- `ParquetBatchWriter`: Parquet 写入（每次运行一个文件）
- `LockFreeQueue`: SPSC 无锁队列
- `MarketDataTimePolicy`: UpdateTime 过滤与 ActionDay 规则
- `Error`: 异常体系与 CTP 响应检查

### Types

- `FrontServer`, `StorageMode` 等类型封装

## 3. 启动与初始化流程

1. `main` 解析命令行，创建 `Application`
2. `Application::run` 注册信号、设置文件句柄上限（CSV/Hybrid）
3. 初始化 Trader：多前置重试，等待结算确认完成
4. 解析订阅列表，查询合约
5. 初始化 Md：多前置重试
6. 读取 Trader 的 TradingDay，计算 ActionDay 相关信息
7. 初始化 `AsyncFileManager` 并订阅行情

## 4. 行情数据流

```
CTP 行情推送
   │
   ▼
MdSpi::OnRtnDepthMarketData
   │  (仅入队，无锁)
   ▼
LockFreeQueue (SPSC)
   │
   ▼
AsyncFileManager::worker_loop
   │  (过滤 UpdateTime，修正 TradingDay/ActionDay)
   ▼
CsvWriter / ParquetBatchWriter
   ▼
磁盘
```

## 5. 时间与交易日策略

### TradingDay 来源

- 以 TraderSpi 登录成功返回的 TradingDay 作为唯一权威值
- MdSpi 登录返回值仅用于日志，不参与存储

### ActionDay 规则

启动时由 `Application` 计算：

- 启动时间 < 07:00:00
  - base_action_day = 昨天
  - next_action_day = 今天
- 启动时间 >= 07:00:00
  - base_action_day = 今天
  - next_action_day = 明天

在 `AsyncFileManager` 中：

- UpdateTime <= 07:00:00 使用 `next_action_day`
- 其他时间使用 `base_action_day`

### UpdateTime 过滤窗口

由 `MarketDataTimePolicy` 决定：

- 启动时间 < 07:00:00: 仅保留 00:00:00 - 07:00:00
- 07:00:00 - 17:00:00: 仅保留 08:55:00 - 15:30:00
- 启动时间 > 17:00:00: 仅保留 20:55:00 - 23:59:59 和 00:00:00 - 03:00:00

该过滤在消费线程执行，避免阻塞行情回调。

## 6. 并发与性能

- 行情线程只做入队操作
- 异步线程批量出队并写盘
- 队列满时丢弃数据并限频告警
- 自适应退避（自旋 -> yield -> sleep）降低空转开销

## 7. 可靠性与可用性

- Trader/Md 初始化支持多前置轮询重试
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

