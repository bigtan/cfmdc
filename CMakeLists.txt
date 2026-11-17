cmake_minimum_required(VERSION 3.31)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake")

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    if(NOT DEFINED CMAKE_C_COMPILER AND "$ENV{CC}" STREQUAL "")
        set(CMAKE_C_COMPILER clang CACHE STRING "C compiler")
    endif()
    if(NOT DEFINED CMAKE_CXX_COMPILER AND "$ENV{CXX}" STREQUAL "")
        set(CMAKE_CXX_COMPILER clang++ CACHE STRING "C++ compiler")
    endif()
endif()

if(NOT DEFINED VCPKG_TARGET_TRIPLET)
    if(WIN32)
        set(VCPKG_TARGET_TRIPLET "x64-windows-static" CACHE STRING "vcpkg triplet")
    elseif(UNIX AND NOT APPLE)
        set(VCPKG_TARGET_TRIPLET "x64-linux" CACHE STRING "vcpkg triplet")
    endif()
    message(STATUS "Using vcpkg triplet: ${VCPKG_TARGET_TRIPLET}")
endif()

include(FindVCPKG)

set(CFMDC_PROJECT_VERSION "0.1.5")
option(CFMDC_ENABLE_PARQUET "Enable Parquet storage support" ON)
if(CFMDC_ENABLE_PARQUET)
    list(APPEND VCPKG_MANIFEST_FEATURES parquet)
endif()

project(cfmdc VERSION ${CFMDC_PROJECT_VERSION} LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)

include(CheckIPOSupported)
check_ipo_supported(RESULT IPO_SUPPORTED OUTPUT IPO_ERROR)
if(IPO_SUPPORTED)
    if(NOT CMAKE_BUILD_TYPE MATCHES "Debug")
        message(STATUS "LTO/IPO enabled for Release builds")
        set(CMAKE_INTERPROCEDURAL_OPTIMIZATION TRUE)
    endif()
else()
    message(WARNING "LTO not supported: ${IPO_ERROR}")
endif()

if(UNIX AND NOT APPLE)
    if(CMAKE_BUILD_TYPE STREQUAL "Release" OR CMAKE_BUILD_TYPE STREQUAL "MinSizeRel")
        # 链接时去除符号表，显著减小体积
        add_link_options("-Wl,--strip-all")
        # 移除未使用的段
        add_link_options("-Wl,--gc-sections")
        add_compile_options("-ffunction-sections" "-fdata-sections")
        message(STATUS "Release build: Stripping symbols and unused sections")
    endif()
endif()

if(MSVC)
    set(CMAKE_MSVC_RUNTIME_LIBRARY "MultiThreaded$<$<CONFIG:Debug>:Debug>")
    add_compile_options(/utf-8 /W4 /permissive- /EHsc)
    add_compile_definitions(_CRT_SECURE_NO_WARNINGS)
    
else()
    add_compile_options(-Wall -Wextra -Wpedantic)
    if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
        add_compile_options(-fPIC -pthread)
        set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--as-needed")
        if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
            add_link_options(-fuse-ld=lld)
            message(STATUS "Linux toolchain: clang/clang++ with lld and system libstdc++")
        endif()
        
        set(CMAKE_SKIP_BUILD_RPATH FALSE)
        set(CMAKE_BUILD_WITH_INSTALL_RPATH TRUE)
        set(CMAKE_INSTALL_RPATH "$ORIGIN")
        set(CMAKE_BUILD_RPATH "$ORIGIN")
    endif()
endif()

add_compile_definitions(
    "$<$<NOT:$<CONFIG:Debug>>:SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_INFO>"
    "$<$<CONFIG:Debug>:SPDLOG_ACTIVE_LEVEL=SPDLOG_LEVEL_TRACE>"
)

option(CFMDC_ENABLE_ARCH_V3 "Enable x86-64-v3 (AVX2) optimization" ON)

if(CFMDC_ENABLE_ARCH_V3)
    if(MSVC)
        if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
            add_compile_options(/clang:-march=x86-64-v3)
            message(STATUS "Build Architecture: x86-64-v3 (via /clang:-march=x86-64-v3 for clang-cl)")
        else()
            add_compile_options(/arch:AVX2)
            message(STATUS "Build Architecture: x86-64-v3 (via /arch:AVX2 for MSVC)")
        endif()
        
    else()
        include(CheckCXXCompilerFlag)
        check_cxx_compiler_flag("-march=x86-64-v3" COMPILER_SUPPORTS_ARCH_V3)

        if(COMPILER_SUPPORTS_ARCH_V3)
            add_compile_options(-march=x86-64-v3)
            message(STATUS "Build Architecture: x86-64-v3 (Supported by compiler)")
        else()
            message(STATUS "Build Architecture: x86-64-v3 (Legacy fallback: -mavx2 -mfma)")
            add_compile_options(-mavx2 -mfma -mbmi -mbmi2)
        endif()
    endif()
else()
    message(STATUS "Build Architecture: Default (Generic x86-64)")
endif()

function(cfmdc_apply_linux_clang_cxx_runtime target_name)
    if(CMAKE_SYSTEM_NAME STREQUAL "Linux" AND CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        # Some environments configure CMake with `clang` as the CXX driver.
        # Link libstdc++ explicitly so C++ symbols resolve even in that case.
        target_link_libraries(${target_name} PRIVATE stdc++)
    endif()
endfunction()

function(cfmdc_apply_version_metadata target_name)
    set(visibility PRIVATE)
    if(ARGC GREATER 1)
        set(visibility "${ARGV1}")
    endif()
    target_compile_definitions(${target_name} ${visibility} CFMDC_VERSION="${PROJECT_VERSION}")
endfunction()


# ==============================================================================
# 7. 依赖查找
# ==============================================================================
find_package(spdlog CONFIG REQUIRED)
find_package(tomlplusplus CONFIG REQUIRED)
find_package(args CONFIG REQUIRED)

# 可选：Parquet 支持
if(CFMDC_ENABLE_PARQUET)
    find_package(Arrow CONFIG REQUIRED)
    find_package(Parquet CONFIG REQUIRED)
endif()

# 查找 CTP
find_package(CTP MODULE REQUIRED)

# 加载辅助函数
include(CopyCTP)

# ==============================================================================
# 8. 构建目标
# ==============================================================================
set(CFMDC_STORAGE_SOURCES
    src/utils/CsvWriter.cpp
    src/utils/FileManager.cpp
)

if(CFMDC_ENABLE_PARQUET)
    list(APPEND CFMDC_STORAGE_SOURCES src/utils/ParquetWriter.cpp)
endif()

add_library(cfmdc_storage STATIC ${CFMDC_STORAGE_SOURCES})

target_include_directories(cfmdc_storage PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

cfmdc_apply_version_metadata(cfmdc_storage PRIVATE)

target_link_libraries(cfmdc_storage PUBLIC
    spdlog::spdlog
    CTP::ctp
)

if(CFMDC_ENABLE_PARQUET)
    target_compile_definitions(cfmdc_storage PUBLIC CFMDC_ENABLE_PARQUET)
    target_link_libraries(cfmdc_storage PUBLIC
        Arrow::arrow_static
        Parquet::parquet_static
    )
    if(MSVC)
        target_compile_definitions(cfmdc_storage PUBLIC ARROW_STATIC PARQUET_STATIC)
    endif()
endif()

add_library(cfmdc_core STATIC
    src/config/Config.cpp
    src/utils/ApiWrapper.cpp
    src/core/TraderSpi.cpp
    src/core/MdSpi.cpp
    src/core/Application.cpp
)

cfmdc_apply_version_metadata(cfmdc_core PUBLIC)

target_link_libraries(cfmdc_core PUBLIC
    cfmdc_storage
    tomlplusplus::tomlplusplus
    CTP::ctp
)

add_executable(cfmdc src/main.cpp)

target_link_libraries(cfmdc PRIVATE
    cfmdc_core
    taywee::args
)

cfmdc_apply_linux_clang_cxx_runtime(cfmdc)

# 拷贝 CTP 运行时库
copy_ctp_runtime_libs(cfmdc)

# 安装
install(TARGETS cfmdc RUNTIME DESTINATION bin)
install(FILES config.toml.example DESTINATION etc/cfmdc RENAME config.toml PERMISSIONS OWNER_READ OWNER_WRITE)

# 测试
option(CFMDC_BUILD_TESTS "Build unit tests" ON)
if(CFMDC_BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()
