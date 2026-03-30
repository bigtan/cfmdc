# cmake/find_vcpkg.cmake
#
# 目的:
#   在 'project()' 命令之前找到并设置 vcpkg CMAKE_TOOLCHAIN_FILE。
#   此脚本基于用户提供的健壮的、跨平台的检测逻辑。
#
# 查找顺序:
#   1. 如果 CMAKE_TOOLCHAIN_FILE 已被设置: 什么都不做。
#   2. 检查一系列候选路径 (环境变量, 相对路径, Windows/Linux 上的常见安装路径)。
#   3. 如果找到: 设置 CMAKE_TOOLCHAIN_FILE 并缓存。
#   4. 如果未找到: 抛出 FATAL_ERROR (因为 vcpkg.json 暗示 vcpkg 是必需的)。

if(DEFINED CMAKE_TOOLCHAIN_FILE)
    message(STATUS "VCPKG: CMAKE_TOOLCHAIN_FILE 已由用户设置: ${CMAKE_TOOLCHAIN_FILE}")
    return()
endif()

# --- (以下逻辑来自您提供的脚本) ---

# 1. 定义所有可能的候选路径
set(VCPKG_CMAKE_CANDIDATES
    # 环境变量 (最优先)
    "$ENV{VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake"
    
    # 项目相对路径 (CMAKE_SOURCE_DIR 在 include() 时指向项目根目录)
    "${CMAKE_SOURCE_DIR}/vcpkg/scripts/buildsystems/vcpkg.cmake"
    "${CMAKE_SOURCE_DIR}/3rd/vcpkg/scripts/buildsystems/vcpkg.cmake" # (添加 3rd/ 目录作为候选项)
    "${CMAKE_SOURCE_DIR}/../vcpkg/scripts/buildsystems/vcpkg.cmake"
)

# 2. 添加 Windows 特定的常见路径
if(WIN32)
    list(APPEND VCPKG_CMAKE_CANDIDATES
        "C:/vcpkg/scripts/buildsystems/vcpkg.cmake"
        "C:/tools/vcpkg/scripts/buildsystems/vcpkg.cmake"
        "C:/dev/vcpkg/scripts/buildsystems/vcpkg.cmake"
    )
endif()

# 3. 添加 Linux 特定的常见路径
if(UNIX AND NOT APPLE)
    list(APPEND VCPKG_CMAKE_CANDIDATES
        "/usr/local/vcpkg/scripts/buildsystems/vcpkg.cmake"
        "/opt/vcpkg/scripts/buildsystems/vcpkg.cmake"
        "$ENV{HOME}/vcpkg/scripts/buildsystems/vcpkg.cmake"
    )
endif()

# 4. 循环查找
foreach(VCPKG_CMAKE_FILE ${VCPKG_CMAKE_CANDIDATES})
    if(EXISTS ${VCPKG_CMAKE_FILE})
        set(CMAKE_TOOLCHAIN_FILE ${VCPKG_CMAKE_FILE} CACHE FILEPATH "vcpkg toolchain file")
        message(STATUS "VCPKG: 自动找到 vcpkg 工具链: ${CMAKE_TOOLCHAIN_FILE}")
        break() # 找到即停止
    endif()
endforeach()

# 5. 最终检查 (使用您原始 CMakeLists.txt 的严格模式)
if(NOT CMAKE_TOOLCHAIN_FILE)
    message(FATAL_ERROR "VCPKG: CMAKE_TOOLCHAIN_FILE 未设置, 且自动检测失败。"
                      "请设置 CMAKE_TOOLCHAIN_FILE 命令行变量, 或设置 VCPKG_ROOT 环境变量。")
endif()