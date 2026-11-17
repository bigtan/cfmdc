# CFMDC 编译指南

## 1. 环境要求

- CMake 3.31+
- C++23 编译器（MSVC 2022+ / GCC 13+ / Clang 16+）
- vcpkg（依赖管理）
- CTP SDK（项目已提供 `3rd/ctp`）

## 2. vcpkg 准备

项目使用 vcpkg manifest 模式（`vcpkg.json`），依赖包含：
`args`, `spdlog`, `tomlplusplus`, `catch2`，以及可选 `parquet` 特性依赖 `arrow`（含 `parquet`、`filesystem`）。

> 说明：在 Windows `x64-windows-static`（`staticcrt`）下，`vcpkg.json` 自动禁用 `arrow` 的 `mimalloc` 特性；CMake 中已移除对 `mimalloc` 的显式 `find_package` 与链接，`Arrow::arrow_static` 会自行管理其传递分配器依赖，从而避免 Windows 静态构建时的 `mimalloc` 查找失败问题。

### Windows

```powershell
git clone https://github.com/microsoft/vcpkg C:\vcpkg
cd C:\vcpkg
.\bootstrap-vcpkg.bat
$env:VCPKG_ROOT = "C:\vcpkg"
```

### Linux

```bash
git clone https://github.com/microsoft/vcpkg ~/vcpkg
cd ~/vcpkg
./bootstrap-vcpkg.sh
export VCPKG_ROOT=~/vcpkg
```

## 3. 构建步骤

### Windows (Ninja + clang-cl / MSVC)

```powershell
cmake -S . -B build -G Ninja `
  -DCMAKE_BUILD_TYPE=Release `
  -DCMAKE_C_COMPILER=clang-cl `
  -DCMAKE_CXX_COMPILER=clang-cl `
  -DCMAKE_TOOLCHAIN_FILE="$env:VCPKG_ROOT\scripts\buildsystems\vcpkg.cmake" `
  -DVCPKG_TARGET_TRIPLET=x64-windows-static

cmake --build build
```

### Linux (Ninja)

```bash
cmake -S . -B build -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_TOOLCHAIN_FILE="$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake"

cmake --build build
```

在 Linux 下，仅当 `CMAKE_C_COMPILER`/`CMAKE_CXX_COMPILER` 与环境变量 `CC`/`CXX` 均未指定时，CMake 才会默认选择 `clang`/`clang++`（不使用 `FORCE` 覆盖）；如需使用 GCC，可通过 `CC=gcc CXX=g++` 或 `-DCMAKE_C_COMPILER=gcc -DCMAKE_CXX_COMPILER=g++` 指定。

构建产物位于 `build/bin/cfmdc`（Windows 为 `build\bin\cfmdc.exe`），CTP 运行时库会被自动拷贝到相同目录。
测试二进制默认也位于 `build/bin/cfmdc_tests`。
CMake 目标拆分为 `cfmdc_storage`（存储静态库）与 `cfmdc_core`（核心静态库），主程序 `cfmdc` 与测试 `cfmdc_tests` 共用静态库，避免重复编译实现文件。

## 4. 关键选项

| 选项 | 默认值 | 说明 |
| --- | --- | --- |
| `CFMDC_ENABLE_PARQUET` | `ON` | 启用 Parquet（依赖 Arrow/Parquet） |
| `CFMDC_BUILD_TESTS` | `ON` | 构建单元测试 |
| `CFMDC_ENABLE_ARCH_V3` | `ON` | 启用 x86-64-v3（AVX2） |
| `VCPKG_TARGET_TRIPLET` | `x64-windows-static` / `x64-linux` | vcpkg 架构 |

### CPU 兼容性

默认开启 x86-64-v3（AVX2）。若需要兼容旧 CPU，请在配置时关闭：

```bash
-DCFMDC_ENABLE_ARCH_V3=OFF
```

## 5. 关闭 Parquet（可选）

如果环境中不需要 Parquet 或缺少 Arrow，可关闭：

```bash
-DCFMDC_ENABLE_PARQUET=OFF
```

此时仅允许 `StorageMode=CSV`；配置为 Parquet/Hybrid 会在启动阶段明确失败。
vcpkg 也不会安装 Arrow/Parquet 依赖。

## 6. 运行与安装

```bash
./build/bin/cfmdc
./build/bin/cfmdc -c /path/to/config.toml
./build/bin/cfmdc --version
```

项目版本号统一由根目录 `CMakeLists.txt` 中的 `CFMDC_PROJECT_VERSION` 管理。

安装目标（可选）：

```bash
cmake --install build
```

安装会将可执行文件放入 `bin/cfmdc`，并将 `config.toml.example` 复制为 `etc/cfmdc/config.toml`（权限限制为 `OWNER_READ OWNER_WRITE` / `0600`）。

## 7. 测试

```bash
cmake -S . -B build -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_TOOLCHAIN_FILE="$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake" \
  -DCFMDC_BUILD_TESTS=ON

cmake --build build
ctest --test-dir build --output-on-failure
```

如需按测试名过滤：

```bash
ctest --test-dir build -R config --output-on-failure
ctest --test-dir build -R constants --output-on-failure
```

