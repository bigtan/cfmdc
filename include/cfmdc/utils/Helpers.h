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
#include <iconv.h>
#include <pthread.h>
#include <sched.h>
#endif

#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86)
#include <immintrin.h>
#endif

#include "ThostFtdcUserApiDataType.h"
#include "ThostFtdcUserApiStruct.h"
#include "cfmdc/utils/Constants.h"
#include "cfmdc/utils/Error.h"

namespace cfmdc
{

/// @brief Emit a CPU pause/yield hint for spin-wait loops
inline void cpu_relax() noexcept
{
#if defined(__x86_64__) || defined(_M_X64) || defined(__i386__) || defined(_M_IX86)
    _mm_pause();
#elif defined(__aarch64__) || defined(_M_ARM64)
#ifdef _MSC_VER
    __yield();
#else
    asm volatile("yield" ::: "memory");
#endif
#else
    std::atomic_signal_fence(std::memory_order_acquire);
#endif
}

/// @brief Set thread affinity to a specific CPU core
/// @tparam ThreadType Thread type (std::thread or std::jthread)
/// @param thread Thread handle
/// @param core_index CPU core index
/// @return true if successful
template <typename ThreadType>
inline bool set_thread_affinity(ThreadType &thread, int core_index)
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

/// @brief Check whether a byte sequence is already valid UTF-8
inline bool is_valid_utf8(std::string_view input) noexcept
{
    const auto *bytes = reinterpret_cast<const unsigned char *>(input.data());
    size_t i = 0;
    const size_t len = input.size();
    while (i < len)
    {
        const unsigned char b0 = bytes[i];
        if (b0 <= 0x7F)
        {
            ++i;
            continue;
        }
        if (b0 >= 0xC2 && b0 <= 0xDF)
        {
            if (i + 1 >= len || (bytes[i + 1] & 0xC0) != 0x80)
            {
                return false;
            }
            i += 2;
        }
        else if (b0 >= 0xE0 && b0 <= 0xEF)
        {
            if (i + 2 >= len || (bytes[i + 1] & 0xC0) != 0x80 || (bytes[i + 2] & 0xC0) != 0x80)
            {
                return false;
            }
            if (b0 == 0xE0 && bytes[i + 1] < 0xA0)
            {
                return false;
            }
            if (b0 == 0xED && bytes[i + 1] >= 0xA0)
            {
                return false;
            }
            i += 3;
        }
        else if (b0 >= 0xF0 && b0 <= 0xF4)
        {
            if (i + 3 >= len || (bytes[i + 1] & 0xC0) != 0x80 || (bytes[i + 2] & 0xC0) != 0x80 ||
                (bytes[i + 3] & 0xC0) != 0x80)
            {
                return false;
            }
            if (b0 == 0xF0 && bytes[i + 1] < 0x90)
            {
                return false;
            }
            if (b0 == 0xF4 && bytes[i + 1] >= 0x90)
            {
                return false;
            }
            i += 4;
        }
        else
        {
            return false;
        }
    }
    return true;
}

/// @brief Convert GBK/GB2312 string from CTP API into UTF-8
inline std::string gbk_to_utf8(std::string_view input)
{
    if (input.empty() || is_valid_utf8(input))
    {
        return std::string(input);
    }

#ifdef _WIN32
    constexpr UINT kCodePageGBK = 936;
    const int wide_len = MultiByteToWideChar(kCodePageGBK, 0, input.data(), static_cast<int>(input.size()), nullptr, 0);
    if (wide_len <= 0)
    {
        return std::string(input);
    }
    std::wstring wide_str(static_cast<size_t>(wide_len), L'\0');
    MultiByteToWideChar(kCodePageGBK, 0, input.data(), static_cast<int>(input.size()), wide_str.data(), wide_len);

    const int utf8_len =
        WideCharToMultiByte(CP_UTF8, 0, wide_str.data(), wide_len, nullptr, 0, nullptr, nullptr);
    if (utf8_len <= 0)
    {
        return std::string(input);
    }
    std::string utf8_str(static_cast<size_t>(utf8_len), '\0');
    WideCharToMultiByte(CP_UTF8, 0, wide_str.data(), wide_len, utf8_str.data(), utf8_len, nullptr, nullptr);
    return utf8_str;
#else
    iconv_t cd = iconv_open("UTF-8", "GBK");
    if (cd == reinterpret_cast<iconv_t>(-1))
    {
        return std::string(input);
    }
    std::string output(input.size() * 3 + 1, '\0');
    char *in_buf = const_cast<char *>(input.data());
    size_t in_bytes_left = input.size();
    char *out_buf = output.data();
    size_t out_bytes_left = output.size();

    const size_t res = iconv(cd, &in_buf, &in_bytes_left, &out_buf, &out_bytes_left);
    iconv_close(cd);
    if (res == static_cast<size_t>(-1))
    {
        return std::string(input);
    }
    output.resize(output.size() - out_bytes_left);
    return output;
#endif
}

/// @brief Check CTP response and create error object
/// @param pRspInfo Response info from CTP
/// @return CtpError object
inline CtpError check_response(CThostFtdcRspInfoField *pRspInfo)
{
    if (pRspInfo && pRspInfo->ErrorID != 0)
    {
        return CtpError{pRspInfo->ErrorID, gbk_to_utf8(pRspInfo->ErrorMsg)};
    }
    return CtpError{0, ""};
}

} // namespace cfmdc
