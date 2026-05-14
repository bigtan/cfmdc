#pragma once

#include <cstring>
#include <limits>
#include <string_view>
#include <thread>

#ifdef _WIN32
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX
#endif
#include <windows.h>
#else
#include <pthread.h>
#include <sched.h>
#endif

#include "ThostFtdcUserApiDataType.h"
#include "ThostFtdcUserApiStruct.h"
#include "cfmdc/utils/Constants.h"
#include "cfmdc/utils/Error.h"

namespace cfmdc
{

/// @brief Set thread affinity to a specific CPU core
/// @param thread Thread handle
/// @param core_index CPU core index
/// @return true if successful
inline bool set_thread_affinity(std::thread &thread, int core_index)
{
    if (core_index < 0)
    {
        return true;
    }

#ifdef _WIN32
    // Windows affinity mask is limited to the current processor group (max 64 cores)
    if (static_cast<size_t>(core_index) >= sizeof(DWORD_PTR) * 8)
    {
        return false;
    }
    DWORD_PTR mask = (static_cast<DWORD_PTR>(1) << core_index);
    return SetThreadAffinityMask(thread.native_handle(), mask) != 0;
#else
    // Linux CPU_SET supports larger number of cores, but we still do a basic check
    if (core_index >= CPU_SETSIZE)
    {
        return false;
    }
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_index, &cpuset);
    int rc = pthread_setaffinity_np(thread.native_handle(), sizeof(cpu_set_t), &cpuset);
    return rc == 0;
#endif
}

/// @brief Safely copy string to fixed-size char array
/// @tparam N Size of destination array
/// @param dest Destination array
/// @param src Source string
template <size_t N> void safe_strcpy(char (&dest)[N], std::string_view src) noexcept
{
    size_t copy_len = src.copy(dest, N - 1);
    dest[copy_len] = '\0';
}

/// @brief Clean invalid price values
/// @param price Raw price value
/// @return Cleaned price (0.0 if invalid)
inline double clean_price(double price) noexcept
{
    return (price == INVALID_PRICE) ? 0.0 : price;
}

/// @brief Check CTP response and create error object
/// @param pRspInfo Response info from CTP
/// @return CtpError object
inline CtpError check_response(CThostFtdcRspInfoField *pRspInfo)
{
    if (pRspInfo && pRspInfo->ErrorID != 0)
    {
        return CtpError{pRspInfo->ErrorID, pRspInfo->ErrorMsg};
    }
    return CtpError{0, ""};
}

} // namespace cfmdc
