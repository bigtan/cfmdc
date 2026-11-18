# CFMDC - CTP Market Data Recorder

CFMDC is a C++23 market data recorder for China futures using the CTP API.
It focuses on high-throughput ingestion, async storage, and production-friendly
operations.

Current project version: `0.1.8`.

## Highlights

- Multi-front retry for Trader and Market Data sessions, with split `flow/td` & `flow/md` directories and immediate `TraderSpi` teardown after startup query to release the TD session slot
- Single-shot full-market `ReqQryInstrument("")` with local filtering (`SubList`, `IncludeOptions`) and instrument ID safety validation
- Environment variable credential overrides (`CFMDC_BROKER_ID`, `CFMDC_USER_ID`, `CFMDC_PASSWORD`, `CFMDC_AUTH_CODE`, `CFMDC_APP_ID`)
- Async storage pipeline with compact `MarketTick` snapshots, lock-free SPSC queues, and cpuset-aware thread affinity (`WorkerThreadCore`, `ParquetWorkerThreadCore`)
- CSV / Parquet / Hybrid storage modes with the exact 43 CTP columns (ending with `ActionDay`), empty CSV / Parquet `NULL` for invalid CTP prices, preserved `0.0`, and full-precision decimal `Turnover`
- Fixed `UTC+8` (`Asia/Shanghai`) startup window and `ActionDay` calculation (safe in UTC Docker containers without `TZ`)
- Modern CMake (`cfmdc_storage` / `cfmdc_core` static targets) + vcpkg manifest workflow

## Quick Start

### Prerequisites

- CMake 3.31+
- C++23 compiler (MSVC 2022+, GCC 13+, Clang 16+)
- vcpkg (for spdlog, tomlplusplus, Taywee/args, Arrow/Parquet, Catch2)
- CTP SDK (already vendored under `3rd/ctp`)

### Build (Windows)

```powershell
git clone https://github.com/microsoft/vcpkg C:\vcpkg
cd C:\vcpkg
.\bootstrap-vcpkg.bat
$env:VCPKG_ROOT = "C:\vcpkg"

cmake -S . -B build -G Ninja `
  -DCMAKE_BUILD_TYPE=Release `
  -DCMAKE_C_COMPILER=clang-cl `
  -DCMAKE_CXX_COMPILER=clang-cl `
  -DCMAKE_TOOLCHAIN_FILE="$env:VCPKG_ROOT\scripts\buildsystems\vcpkg.cmake" `
  -DVCPKG_TARGET_TRIPLET=x64-windows-static

cmake --build build
```

### Build (Linux)

```bash
git clone https://github.com/microsoft/vcpkg ~/vcpkg
cd ~/vcpkg
./bootstrap-vcpkg.sh
export VCPKG_ROOT=~/vcpkg

cmake -S . -B build -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_TOOLCHAIN_FILE="$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake"

cmake --build build
```

### Notes

- GitHub CI and Release Linux archives (`cfmdc-*-x64-linux.tar.gz`) are built
  and tested in Debian 13 containers with Clang and the system libstdc++.
  The packaged binary is also checked in a clean Debian 13 container before upload.
  Run these archives on Debian 13 amd64 with `libstdc++6` and `libgcc-s1`
  installed; keep the bundled CTP `.so` files beside `cfmdc`.
- On Linux, CMake only defaults to `clang`/`clang++` if `CMAKE_C_COMPILER`/`CMAKE_CXX_COMPILER`
  and `CC`/`CXX` are unset, without forcing overrides.
- On Windows (`x64-windows-static`), `vcpkg.json` builds Arrow without `mimalloc`,
  and CMake no longer explicitly finds or links `mimalloc` (`Arrow::arrow_static`
  manages its own transitive allocator dependencies).
- Output binary: `build/bin/cfmdc` (`build\bin\cfmdc.exe` on Windows)
- Default build enables x86-64-v3 (AVX2). Disable if you need older CPUs:
  `-DCFMDC_ENABLE_ARCH_V3=OFF`
- Disable Parquet if Arrow is not available: `-DCFMDC_ENABLE_PARQUET=OFF`
  (only `StorageMode=CSV` is accepted in that build)

## Configuration

Create `config.toml` from the example and edit it.

Windows:

```powershell
Copy-Item config.toml.example config.toml
```

Linux:

```bash
cp config.toml.example config.toml
```

Minimal example:

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
ParquetRowGroupSize = 200000

[Application]
InitTimeout = 60
FlowPath = "./flow" # Splits internally into <FlowPath>/td and <FlowPath>/md
SubList = "null" # "null" = all queried contracts, or "rb2505|IC2501"
IncludeOptions = false # Set true to include options alongside futures
WorkerThreadCore = "auto" # -1, "auto", or core index (default: -1)
ParquetWorkerThreadCore = "auto" # Defaults to "auto" (-2) if WorkerThreadCore="auto", else -1
```

Placeholders supported in `CSVPath` and `ParquetPath`:
`{tradingday}`, `{year}`, `{month}`, `{day}`.

Environment variable credential overrides (take precedence over `Front` fields when set):
`CFMDC_BROKER_ID`, `CFMDC_USER_ID`, `CFMDC_PASSWORD`, `CFMDC_AUTH_CODE`, `CFMDC_APP_ID`.

## Run

```bash
# Default config.toml
./build/bin/cfmdc

# Custom config
./build/bin/cfmdc -c /path/to/config.toml

# Help and version
./build/bin/cfmdc --help
./build/bin/cfmdc --version
```

Version metadata is managed from a single source in `CMakeLists.txt` via
`CFMDC_PROJECT_VERSION`, then propagated to the binary and tests through CMake.

## Deployment Notes

- The application is designed to run for a single trading session: start it
  before the session begins and restart it for the next one (TradingDay and
  storage paths are fixed at startup).
- Startup session window and `ActionDay` calculations use fixed `UTC+8`
  (`Asia/Shanghai`) calendar math independent of host timezone, making UTC
  Docker containers safe without setting `TZ`.
- `FlowPath` is split into `<FlowPath>/td` and `<FlowPath>/md`, and `TraderSpi`
  is torn down immediately after the startup instrument query to release the TD
  session slot.
- Front server retry walks the configured server list once. Run cfmdc under a
  process supervisor (systemd `Restart=on-failure`, NSSM, etc.) for automatic
  recovery after a failed start or a crash.
- `config.toml` contains account credentials. It is excluded from version
  control via `.gitignore`; restrict file permissions (e.g. `chmod 600` —
  `cmake --install` installs `config.toml` with owner-only read/write
  permissions), or supply credentials via `CFMDC_*` environment variables.
- CSV/Hybrid modes raise the process file handle limit (`FILE_HANDLE_LIMIT = 8192`).
- Pipeline statistics (`stored`, `csv`, `parquet`, `ingress_queue`,
  `parquet_queue`, `filtered`, `invalid`, `dropped`, `write_failures`,
  `csv_flushes`) are logged once per minute; non-zero `invalid`, `dropped`, or
  `write_failures` values need investigation (`filtered` tracks expected
  outside-window ticks).
- Logs are written to the console and to `logs/cfmdc_YYYY-MM-DD.log` (daily
  files, rotated at 06:00 so a night session stays in one file, 30 days kept).

## Storage Behavior

- CSV: one file per instrument, named `{InstrumentID}_{TradingDay}.csv`
- Parquet: single timestamped file per run in `ParquetPath`
- 43 columns: preserves the exact original 43 CTP market data fields (ending
  with `ActionDay`)
- Invalid CTP prices (`DBL_MAX`, `-DBL_MAX`, `NaN`, `±Inf`) are written as empty
  fields in CSV and `NULL` in Parquet, while genuine `0.0` (and valid negative
  spread prices) are preserved; `Turnover` retains full decimal precision in CSV
- TradingDay is taken from Trader SPI and applied to all stored ticks
- UpdateTime filtering, safety validation, and ActionDay correction happen in
  the async worker over compact `MarketTick` records
- Parquet uses a dedicated writer queue/thread so compression does not block CSV
  or ingestion processing (`ParquetWorkerThreadCore` defaults to `-2`/`"auto"`
  when `WorkerThreadCore = "auto"`, otherwise `-1`)
- Parquet uses zstd level 3, sorts each row group by `InstrumentID` (preserving
  per-instrument order), and applies `BYTE_STREAM_SPLIT` on float64 columns;
  `History.ParquetRowGroupSize` (default `200000`) controls the
  throughput/tail-latency tradeoff

## Tests

```bash
cmake -S . -B build -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_TOOLCHAIN_FILE="$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake" \
  -DCFMDC_BUILD_TESTS=ON

cmake --build build
ctest --test-dir build --output-on-failure
```

Useful test commands:

```bash
# Run only config-related tests
ctest --test-dir build -R config --output-on-failure

# Run the Catch2 binary directly
./build/bin/cfmdc_tests
```

## Documentation

- `docs/ARCHITECTURE.md`
- `docs/BUILD_GUIDE.md`
- `docs/CONFIG_GUIDE.md`
- `docs/STORAGE_DESIGN.md`
