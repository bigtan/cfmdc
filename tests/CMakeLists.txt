# tests/CMakeLists.txt

message(STATUS "Configuring tests...")

# 1. 查找测试框架 Catch2
find_package(Catch2 3 CONFIG REQUIRED)

# 2. 定义测试源文件
set(TEST_SOURCES
    test_constants.cpp
    test_config.cpp
    test_error.cpp
    test_lock_free_queue.cpp
    test_market_data_time_policy.cpp
)

# 3. 引入测试依赖的实现文件
# (注意：保持原有的文件列表，避免重复编译整个 Core)
set(TEST_IMPL_SOURCES
    ${CMAKE_SOURCE_DIR}/src/config/Config.cpp
    ${CMAKE_SOURCE_DIR}/src/utils/ApiWrapper.cpp
    ${CMAKE_SOURCE_DIR}/src/utils/FileManager.cpp
    ${CMAKE_SOURCE_DIR}/src/utils/CsvWriter.cpp
)

# 如果启用 Parquet，加入对应实现
if(CFMDC_ENABLE_PARQUET)
    list(APPEND TEST_IMPL_SOURCES
        ${CMAKE_SOURCE_DIR}/src/utils/ParquetWriter.cpp
    )
endif()

# 4. 创建测试可执行文件
add_executable(cfmdc_tests ${TEST_SOURCES} ${TEST_IMPL_SOURCES})

# 5. 包含路径
target_include_directories(cfmdc_tests PRIVATE
    ${CMAKE_SOURCE_DIR}/include
)

cfmdc_apply_version_metadata(cfmdc_tests)

# 6. 链接依赖库
target_link_libraries(cfmdc_tests PRIVATE
    Catch2::Catch2WithMain
    spdlog::spdlog
    tomlplusplus::tomlplusplus
    CTP::ctp  # 使用 FindCTP 提供的统一接口
)

cfmdc_apply_linux_clang_cxx_runtime(cfmdc_tests)

# Parquet 链接配置
if(CFMDC_ENABLE_PARQUET)
    target_link_libraries(cfmdc_tests PRIVATE
        Arrow::arrow_static
        Parquet::parquet_static
        mimalloc
    )
    # Windows 静态链接宏
    if(MSVC)
        target_compile_definitions(cfmdc_tests PRIVATE ARROW_STATIC PARQUET_STATIC)
    endif()
endif()

# 7. 编译器选项微调 (大部分已由根目录继承)
if(MSVC)
    target_compile_options(cfmdc_tests PRIVATE /W4 /permissive- /utf-8)
    # Sanitizers (如果开启)
    if(CFMDC_ENABLE_ASAN)
        target_compile_options(cfmdc_tests PRIVATE /fsanitize=address)
    endif()
else()
    target_compile_options(cfmdc_tests PRIVATE -Wall -Wextra -Wpedantic)
    # Sanitizers (如果开启)
    if(CFMDC_ENABLE_ASAN)
        target_compile_options(cfmdc_tests PRIVATE -fsanitize=address -fno-omit-frame-pointer)
        target_link_options(cfmdc_tests PRIVATE -fsanitize=address)
    endif()
    if(CFMDC_ENABLE_TSAN)
        target_compile_options(cfmdc_tests PRIVATE -fsanitize=thread -fno-omit-frame-pointer)
        target_link_options(cfmdc_tests PRIVATE -fsanitize=thread)
    endif()
endif()

# 8. 关键步骤：拷贝 CTP 运行时库到测试程序目录
# 这使用了根目录加载的 cmake/CopyCTP.cmake 中的函数
copy_ctp_runtime_libs(cfmdc_tests)

# 9. 集成到 CTest
include(Catch)
catch_discover_tests(cfmdc_tests)

message(STATUS "Tests configured successfully")
