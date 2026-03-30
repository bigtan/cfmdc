# CFMDC 编译指南

## 1. 环境要求

- CMake 3.31+
- C++23 编译器（MSVC 2022+ / GCC 13+ / Clang 16+）
- vcpkg（依赖管理）
- CTP SDK（项目已提供 `3rd/ctp`）

## 2. vcpkg 准备

项目使用 vcpkg manifest 模式（`vcpkg.json`），依赖包含：
`spdlog`, `tomlplusplus`, `cxxopts`, `arrow`, `parquet`, `catch2`。

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

### Windows (Ninja)

```powershell
cmake -S . -B build -G Ninja `
  -DCMAKE_BUILD_TYPE=Release `
  -DCMAKE_TOOLCHAIN_FILE="$env:VCPKG_ROOT\scripts\buildsystems\vcpkg.cmake"

cmake --build build
```

### Linux (Ninja)

```bash
cmake -S . -B build -G Ninja \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_TOOLCHAIN_FILE="$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake"

cmake --build build
```

构建产物位于 `build/bin/cfmdc`，CTP 运行时库会被自动拷贝到相同目录。
测试二进制默认也位于 `build/bin/cfmdc_tests`。

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

此时配置中的 `StorageMode=Parquet/Hybrid` 会被降级为 CSV。

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

