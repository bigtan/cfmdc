if(NOT TARGET CTP::ctp_md OR NOT TARGET CTP::ctp_td)
    message(WARNING "CTP targets not found, cannot copy runtime libs.")
    return()
endif()

function(copy_ctp_runtime_libs target_name)
    message(STATUS "CTP: Configuring copy step for ${target_name}...")

    # 使用生成器表达式读取自定义属性 CTP_FILE
    # 这比 $<TARGET_FILE:...> 更通用，支持 Linux 下的 INTERFACE 目标
    add_custom_command(TARGET ${target_name} POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_if_different
            $<TARGET_PROPERTY:CTP::ctp_md,CTP_FILE>
            $<TARGET_PROPERTY:CTP::ctp_td,CTP_FILE>
            $<TARGET_FILE_DIR:${target_name}>
        COMMENT "Copying CTP runtime libraries..."
        VERBATIM
    )
endfunction()