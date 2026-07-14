# CFMDC 配置说明

配置文件为 TOML 格式，默认文件名为 `config.toml`。
模板见 `config.toml.example`。

## 1. 完整示例

```toml
[[Front]]
MD_Url = "tcp://180.168.146.187:10131"
TD_Url = "tcp://180.168.146.187:10130"
BrokerID = "9999"
UserID = "your_user_id"
Password = "your_password"
UserProductInfo = "your_product_info"
AuthCode = "your_auth_code"
AppID = "your_app_id"

[History]
StorageMode = "Parquet"
CSVPath = "./data/csv/{tradingday}"
ParquetPath = "./data/parquet/{tradingday}"
ParquetRowGroupSize = 100000

[Application]
InitTimeout = 60
FlowPath = "./flow"
SubList = "null"
```

## 2. Front（前置服务器）

### 2.1 形式

`Front` 支持两种形式：

- 单服务器（Table）：`[Front]`
- 多服务器（Array of Tables）：`[[Front]]`

多服务器会按顺序重试，直到成功。
各项表示同一账户的冗余 TD/MD 接入地址，TD 与 MD 会分别选择首个可用前置；
账户信息应保持一致，订阅策略统一由 `Application.SubList` 配置。

### 2.2 必填字段

以下字段必须存在（单服务器或多服务器每项均需具备）：

- `MD_Url`
- `TD_Url`
- `BrokerID`
- `UserID`
- `Password`
- `UserProductInfo`
- `AuthCode`
- `AppID`

## 3. History（存储配置）

### 3.1 StorageMode

`StorageMode` 可取：

- `CSV`
- `Parquet`
- `Hybrid`

配置大小写不敏感。未知值或非字符串值会导致启动失败，避免因拼写错误写出非预期格式。

### 3.2 CSVPath / ParquetPath

只需提供当前模式使用的路径：CSV 需要 `CSVPath`，Parquet 需要 `ParquetPath`，Hybrid 两者都需要。

支持的占位符：

- `{tradingday}`: 交易日（YYYYMMDD）
- `{year}`: 年（YYYY）
- `{month}`: 月（MM）
- `{day}`: 日（DD）

示例：

```toml
[History]
CSVPath = "./data/{year}/{month}/csv/{tradingday}"
ParquetPath = "./data/{year}/{month}/parquet/{tradingday}"
```

### 3.3 ParquetRowGroupSize

Parquet 每个 Row Group 的行数，默认 `100000`，有效范围 `1` 到 `1000000`。
较小的值可缩短单次构建和压缩造成的尾延迟，较大的值通常有利于压缩率和批量吞吐。

## 4. Application（应用配置）

### 4.1 InitTimeout

初始化超时时间（秒），默认 `60`。

### 4.2 FlowPath

CTP 流文件路径，默认 `./flow`。若目录不存在会自动创建。

### 4.3 SubList

应用全局订阅合约列表，不属于单个 Front：

- `"null"` 或省略：订阅全部期货合约
- `"rb2505|IC2501"`：订阅指定合约（`|` 分隔）

旧版配置中的 `Front.SubList` 暂时仍可读取，但会产生迁移告警；新配置应统一使用
`Application.SubList`。

## 5. 版本与配置边界

- 应用版本号不从配置文件读取，而是由根目录 `CMakeLists.txt` 中的 `CFMDC_PROJECT_VERSION` 统一管理。
- `config.toml` 只负责运行期行为，不负责构建期版本信息。

## 6. 当前未启用的配置

代码当前未读取日志配置或 Parquet 压缩算法参数。当前固定值为：

- `compression = "zstd"`
- `compression_level = 3`

Row Group 大小通过 `History.ParquetRowGroupSize` 配置。Parquet 拥有独立队列和写线程，
Row Group 构建及压缩不会阻塞 CSV 写入或行情清洗线程。
