#pragma once

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstddef>
#include <cstring>
#include <limits>
#include <string>
#include <string_view>
#include <thread>
#include <utility>
#include <vector>

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

/// @brief Set the current thread name for diagnostics and profilers
inline void set_current_thread_name(std::string_view name) noexcept
{
    if (name.empty())
    {
        return;
    }

#ifdef _WIN32
    wchar_t wide_name[128]{};
    const int in_len = static_cast<int>((std::min)(name.size(), size_t{127}));
    if (MultiByteToWideChar(CP_UTF8, 0, name.data(), in_len, wide_name, 127) > 0)
    {
        (void)SetThreadDescription(GetCurrentThread(), wide_name);
    }
#elif defined(__linux__)
    char buf[16]{};
    const size_t len = (std::min)(name.size(), sizeof(buf) - 1);
    std::memcpy(buf, name.data(), len);
    buf[len] = '\0';
    (void)pthread_setname_np(pthread_self(), buf);
#else
    (void)name;
#endif
}

/// @brief Query CPU core indices allowed for the current process/thread (cpuset-aware)
inline std::vector<int> available_cpu_cores() noexcept
{
    std::vector<int> cores;

#ifdef _WIN32
    DWORD_PTR process_mask = 0;
    DWORD_PTR system_mask = 0;
    if (GetProcessAffinityMask(GetCurrentProcess(), &process_mask, &system_mask) != 0 && process_mask != 0)
    {
        constexpr int kMaxBits = static_cast<int>(sizeof(DWORD_PTR) * 8);
        for (int cpu = 0; cpu < kMaxBits; ++cpu)
        {
            if ((process_mask & (static_cast<DWORD_PTR>(1) << cpu)) != 0)
            {
                cores.push_back(cpu);
            }
        }
    }
#elif defined(__linux__)
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    if (pthread_getaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset) == 0)
    {
        for (int cpu = 0; cpu < CPU_SETSIZE; ++cpu)
        {
            if (CPU_ISSET(cpu, &cpuset))
            {
                cores.push_back(cpu);
            }
        }
    }
#endif

    if (cores.empty())
    {
        const unsigned int hw = std::thread::hardware_concurrency();
        cores.reserve(hw);
        for (unsigned int cpu = 0; cpu < hw; ++cpu)
        {
            cores.push_back(static_cast<int>(cpu));
        }
    }

    return cores;
}

/// @brief Resolve worker thread core indices against an explicit available-cores list
/// @param ingest_cfg Configured ingest worker core (-1 = no affinity, -2 = auto, >=0 = manual)
/// @param parquet_cfg Configured Parquet worker core (-1 = no affinity, -2 = auto, >=0 = manual)
/// @param available Sorted list of available CPU core indices
/// @return Resolved (ingest_core, parquet_core) pair
inline std::pair<int, int> resolve_worker_cores_for_available(int ingest_cfg, int parquet_cfg,
                                                              const std::vector<int> &available) noexcept
{
    const auto is_available = [&](int core) noexcept {
        return core >= 0 && std::find(available.begin(), available.end(), core) != available.end();
    };

    int ingest_core = -1;
    int parquet_core = -1;

    if (ingest_cfg >= 0)
    {
        ingest_core = is_available(ingest_cfg) ? ingest_cfg : -1;
    }
    if (parquet_cfg >= 0)
    {
        parquet_core = is_available(parquet_cfg) ? parquet_cfg : -1;
    }

    const auto pick_auto_core = [&](int excluded_core) noexcept -> int {
        for (auto it = available.rbegin(); it != available.rend(); ++it)
        {
            if (*it > 0 && *it != excluded_core)
            {
                return *it;
            }
        }
        return -1;
    };

    if (ingest_cfg == -2)
    {
        ingest_core = pick_auto_core(parquet_core);
    }
    if (parquet_cfg == -2)
    {
        parquet_core = pick_auto_core(ingest_core);
    }

    return {ingest_core, parquet_core};
}

/// @brief Resolve configured ingest and Parquet worker cores against current process affinity
inline std::pair<int, int> resolve_worker_cores(int ingest_cfg, int parquet_cfg) noexcept
{
    return resolve_worker_cores_for_available(ingest_cfg, parquet_cfg, available_cpu_cores());
}

/// @brief Set thread affinity to a specific CPU core
/// @tparam ThreadType Thread type (std::thread or std::jthread)
/// @param thread Thread handle
/// @param core_index CPU core index
/// @return true if successful
template <typename ThreadType> inline bool set_thread_affinity(ThreadType &thread, int core_index)
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

/// @brief Create a bounded std::string_view from a fixed-size char array without reading past N bytes
template <size_t N> constexpr std::string_view bounded_sv(const char (&buf)[N]) noexcept
{
    const char *nul = std::char_traits<char>::find(buf, N, '\0');
    return std::string_view(buf, nul ? static_cast<size_t>(nul - buf) : N);
}

/// @brief Safely copy string to fixed-size char array
/// @tparam N Size of destination array
/// @param dest Destination array
/// @param src Source string
template <size_t N> void safe_strcpy(char (&dest)[N], std::string_view src) noexcept
{
    static_assert(N > 0, "Destination buffer must have non-zero size");
    size_t copy_len = src.copy(dest, N - 1);
    dest[copy_len] = '\0';
}

/// @brief Safely copy a potentially unterminated fixed-size char buffer into a null-terminated destination buffer
template <size_t DestN, size_t SrcN>
inline void safe_copy_bounded(char (&dest)[DestN], const char (&src)[SrcN]) noexcept
{
    static_assert(DestN > 0, "Destination buffer must have non-zero size");
    safe_strcpy(dest, bounded_sv(src));
}

/// @brief Check whether a CTP floating-point price/value is valid (finite and not sentinel DBL_MAX)
inline bool is_valid_ctp_price(double price) noexcept
{
    return std::isfinite(price) && price < (INVALID_PRICE * 0.5) && price > -(INVALID_PRICE * 0.5);
}

/// @brief Clean invalid price values
/// @param price Raw price value
/// @return Cleaned price (0.0 if invalid)
inline double clean_price(double price) noexcept
{
    return is_valid_ctp_price(price) ? price : 0.0;
}

/// @brief Validate that a trading day string is exactly 8 ASCII digits (YYYYMMDD)
inline bool is_valid_trading_day(std::string_view s) noexcept
{
    if (s.size() != 8)
    {
        return false;
    }
    for (const char ch : s)
    {
        if (ch < '0' || ch > '9')
        {
            return false;
        }
    }
    return true;
}

/// @brief Validate that an instrument ID is safe for filenames, logs, and CSV cells
inline bool is_safe_instrument_id(std::string_view s) noexcept
{
    if (s.empty() || s.size() > 63)
    {
        return false;
    }

    const auto is_ascii_alnum = [](unsigned char c) noexcept {
        return (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9');
    };

    if (!is_ascii_alnum(static_cast<unsigned char>(s[0])))
    {
        return false;
    }

    for (size_t i = 1; i < s.size(); ++i)
    {
        const unsigned char c = static_cast<unsigned char>(s[i]);
        if (!is_ascii_alnum(c) && c != '.' && c != '_' && c != '&' && c != ' ' && c != '-')
        {
            return false;
        }
    }

    return s.find("..") == std::string_view::npos;
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
        return CtpError{pRspInfo->ErrorID, gbk_to_utf8(bounded_sv(pRspInfo->ErrorMsg))};
    }
    return CtpError{0, ""};
}

} // namespace cfmdc
