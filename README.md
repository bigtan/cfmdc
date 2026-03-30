# CFMDC - CTP Market Data Recorder

CFMDC is a C++23 market data recorder for China futures using the CTP API.
It focuses on high-throughput ingestion, async storage, and production-friendly
operations.

Current project version: `0.1.0`.

## Highlights

- Multi-front retry for both Trader and Market Data sessions
- Async file pipeline with lock-free SPSC queue
- CSV / Parquet / Hybrid storage modes
- TradingDay and ActionDay correction based on startup window
- Time-window filtering by UpdateTime to keep expected sessions only
- Modern CMake + vcpkg manifest workflow

## Quick Start

### Prerequisites

- CMake 3.31+
- C++23 compiler (MSVC 2022+, GCC 13+, Clang 16+)
- vcpkg (for spdlog, tomlplusplus, cxxopts, Arrow/Parquet, Catch2)
- CTP SDK (already vendored under `3rd/ctp`)

### Build (Windows)

```powershell
git clone https://github.com/microsoft/vcpkg C:\vcpkg
cd C:\vcpkg
.\bootstrap-vcpkg.bat
$env:VCPKG_ROOT = "C:\vcpkg"

cmake -S . -B build -G Ninja `
  -DCMAKE_BUILD_TYPE=Release `
  -DCMAKE_TOOLCHAIN_FILE="$env:VCPKG_ROOT\scripts\buildsystems\vcpkg.cmake"

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

- Output binary: `build/bin/cfmdc`
- Default build enables x86-64-v3 (AVX2). Disable if you need older CPUs:
  `-DCFMDC_ENABLE_ARCH_V3=OFF`
- Disable Parquet if Arrow is not available: `-DCFMDC_ENABLE_PARQUET=OFF`

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
SubList = "null" # "null" = all, or "rb2505|IC2501"

[History]
StorageMode = "Parquet"
CSVPath = "./data/csv/{tradingday}"
ParquetPath = "./data/parquet/{tradingday}"

[Application]
InitTimeout = 60
FlowPath = "./flow"
```

Placeholders supported in `CSVPath` and `ParquetPath`:
`{tradingday}`, `{year}`, `{month}`, `{day}`.

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

## Storage Behavior (Current)

- CSV: one file per instrument, named `{InstrumentID}_{TradingDay}.csv`
- Parquet: single timestamped file per run in `ParquetPath`
- TradingDay is taken from Trader SPI and applied to all stored ticks
- UpdateTime filtering and ActionDay correction happen in the async worker
- Parquet writer parameters are compiled-in (zstd, batch 10000, row-group 100000)

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
