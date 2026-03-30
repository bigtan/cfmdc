include(FindPackageHandleStandardArgs)

set(CTP_ROOT_DIR "${CMAKE_SOURCE_DIR}/3rd/ctp")

# 1. 查找头文件
find_path(CTP_INCLUDE_DIR ThostFtdcTraderApi.h
    HINTS "${CTP_ROOT_DIR}/include"
    DOC "CTP include 目录")

# 确定库搜索目录
if(WIN32)
    set(CTP_LIB_SEARCH_PATH "${CTP_ROOT_DIR}/lib/win64")
elseif(UNIX)
    set(CTP_LIB_SEARCH_PATH "${CTP_ROOT_DIR}/lib/linux64")
else()
    message(FATAL_ERROR "CTP: Unsupported platform.")
endif()

# ----------------------------------------------------------
# 2. 查找文件
# ----------------------------------------------------------
# 查找运行时文件 (DLL/SO)
find_file(CTP_MD_FILE 
    NAMES thostmduserapi_se.dll libthostmduserapi_se.so thostmduserapi_se.so
    HINTS "${CTP_LIB_SEARCH_PATH}" "${CTP_ROOT_DIR}/bin"
    DOC "CTP MD File")

find_file(CTP_TD_FILE 
    NAMES thosttraderapi_se.dll libthosttraderapi_se.so thosttraderapi_se.so
    HINTS "${CTP_LIB_SEARCH_PATH}" "${CTP_ROOT_DIR}/bin"
    DOC "CTP TD File")

# 查找导入库 (Windows .lib)
if(WIN32)
    find_library(CTP_MD_LIB NAMES thostmduserapi_se HINTS "${CTP_LIB_SEARCH_PATH}")
    find_library(CTP_TD_LIB NAMES thosttraderapi_se HINTS "${CTP_LIB_SEARCH_PATH}")
endif()

# ----------------------------------------------------------
# 3. 验证
# ----------------------------------------------------------
set(CTP_INCLUDE_DIRS ${CTP_INCLUDE_DIR})
find_package_handle_standard_args(CTP
    FOUND_VAR CTP_FOUND
    REQUIRED_VARS CTP_INCLUDE_DIRS CTP_MD_FILE CTP_TD_FILE)

# ----------------------------------------------------------
# 4. 创建目标
# ----------------------------------------------------------
if(CTP_FOUND AND NOT TARGET CTP::ctp)
    
    # === Linux: 使用 INTERFACE_LINK_LIBRARIES 传递 Raw Flags ===
    if(UNIX)
        # --- MD ---
        add_library(CTP::ctp_md INTERFACE IMPORTED)
        set_target_properties(CTP::ctp_md PROPERTIES
            INTERFACE_INCLUDE_DIRECTORIES "${CTP_INCLUDE_DIRS}"
            INTERFACE_LINK_DIRECTORIES    "${CTP_LIB_SEARCH_PATH}"
            # [修正] 直接传递链接器标志，绕过 CMake 对库名的文件名检查
            INTERFACE_LINK_LIBRARIES      "-l:thostmduserapi_se.so"
        )
        # 设置自定义属性供 CopyCTP 使用
        set_property(TARGET CTP::ctp_md PROPERTY CTP_FILE "${CTP_MD_FILE}")

        # --- TD ---
        add_library(CTP::ctp_td INTERFACE IMPORTED)
        set_target_properties(CTP::ctp_td PROPERTIES
            INTERFACE_INCLUDE_DIRECTORIES "${CTP_INCLUDE_DIRS}"
            INTERFACE_LINK_DIRECTORIES    "${CTP_LIB_SEARCH_PATH}"
            INTERFACE_LINK_LIBRARIES      "-l:thosttraderapi_se.so"
        )
        set_property(TARGET CTP::ctp_td PROPERTY CTP_FILE "${CTP_TD_FILE}")

    # === Windows: 保持 SHARED 目标 ===
    elseif(WIN32)
        # --- MD ---
        add_library(CTP::ctp_md SHARED IMPORTED)
        set_target_properties(CTP::ctp_md PROPERTIES
            INTERFACE_INCLUDE_DIRECTORIES "${CTP_INCLUDE_DIRS}"
            IMPORTED_IMPLIB               "${CTP_MD_LIB}"
            IMPORTED_LOCATION             "${CTP_MD_FILE}"
        )
        set_property(TARGET CTP::ctp_md PROPERTY CTP_FILE "${CTP_MD_FILE}")

        # --- TD ---
        add_library(CTP::ctp_td SHARED IMPORTED)
        set_target_properties(CTP::ctp_td PROPERTIES
            INTERFACE_INCLUDE_DIRECTORIES "${CTP_INCLUDE_DIRS}"
            IMPORTED_IMPLIB               "${CTP_TD_LIB}"
            IMPORTED_LOCATION             "${CTP_TD_FILE}"
        )
        set_property(TARGET CTP::ctp_td PROPERTY CTP_FILE "${CTP_TD_FILE}")
    endif()

    # --- 聚合接口 ---
    add_library(CTP::ctp INTERFACE IMPORTED)
    target_link_libraries(CTP::ctp INTERFACE CTP::ctp_md CTP::ctp_td)

    message(STATUS "Found CTP: MD=${CTP_MD_FILE}")
endif()