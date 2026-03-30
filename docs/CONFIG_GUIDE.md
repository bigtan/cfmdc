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
SubList = "null"

[History]
StorageMode = "Parquet"
CSVPath = "./data/csv/{tradingday}"
ParquetPath = "./data/parquet/{tradingday}"

[Application]
InitTimeout = 60
FlowPath = "./flow"
```

## 2. Front（前置服务器）

### 2.1 形式

`Front` 支持两种形式：

- 单服务器（Table）：`[Front]`
- 多服务器（Array of Tables）：`[[Front]]`

多服务器会按顺序重试，直到成功。

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

### 2.3 可选字段

- `SubList`: 订阅合约列表
  - `"null"`: 订阅全部（默认）
  - `"rb2505|IC2501"`: 订阅指定合约（`|` 分隔）
  - `""`: 不订阅任何合约

## 3. History（存储配置）

### 3.1 StorageMode

`StorageMode` 可取：

- `CSV`
- `Parquet`
- `Hybrid`

配置大小写不敏感，未知值会回退为 CSV。

### 3.2 CSVPath / ParquetPath

两者为必填字段，即使某个模式当前不使用也需要提供。

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

## 4. Application（应用配置）

### 4.1 InitTimeout

初始化超时时间（秒），默认 `60`。

### 4.2 FlowPath

CTP 流文件路径，默认 `./flow`。若目录不存在会自动创建。

## 5. 版本与配置边界

- 应用版本号不从配置文件读取，而是由根目录 `CMakeLists.txt` 中的 `CFMDC_PROJECT_VERSION` 统一管理。
- `config.toml` 只负责运行期行为，不负责构建期版本信息。

## 6. 当前未启用的配置

代码当前未读取日志配置或 Parquet 参数（压缩、批量大小等）。
当前 Parquet 默认值定义在 `src/utils/FileManager.cpp`：

- `compression = "zstd"`
- `compression_level = 3`
- `batch_size = 10000`
- `row_group_size = 100000`

如需调整，请修改该文件中的 `ParquetMarketDataWriter::Config` 初始化逻辑。
