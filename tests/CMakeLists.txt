# tests/CMakeLists.txt

message(STATUS "Configuring tests...")

# 1. 查找测试框架 Catch2
find_package(Catch2 3 CONFIG REQUIRED)

# 2. 定义测试源文件
set(TEST_SOURCES
    test_constants.cpp
    test_config.cpp
    test_error.cpp
    test_file_manager.cpp
    test_initialization_tracker.cpp
    test_lock_free_queue.cpp
    test_market_data_time_policy.cpp
    test_subscription_tracker.cpp
)

# 3. 创建测试可执行文件（复用 cfmdc_core 与 cfmdc_storage 静态库，避免重复编译）
add_executable(cfmdc_tests ${TEST_SOURCES})

# 4. 链接依赖库
target_link_libraries(cfmdc_tests PRIVATE
    cfmdc_core
    Catch2::Catch2WithMain
)

cfmdc_apply_linux_clang_cxx_runtime(cfmdc_tests)

# 5. 编译器选项微调 (大部分已由根目录继承)
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

# 6. 关键步骤：拷贝 CTP 运行时库到测试程序目录
# 这使用了根目录加载的 cmake/CopyCTP.cmake 中的函数
copy_ctp_runtime_libs(cfmdc_tests)

# 7. 集成到 CTest
include(Catch)
catch_discover_tests(cfmdc_tests)

message(STATUS "Tests configured successfully")
