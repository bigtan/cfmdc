#pragma once

#include <ctime>
#include <format>
#include <string>

namespace cfmdc::time_utils
{

inline std::tm to_local_tm(std::time_t t)
{
    std::tm tm{};
#ifdef _WIN32
    localtime_s(&tm, &t);
#else
    localtime_r(&t, &tm);
#endif
    return tm;
}

inline std::string format_yyyymmdd(const std::tm &tm)
{
    return std::format("{:04}{:02}{:02}", tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday);
}

inline std::string format_hhmmss(const std::tm &tm)
{
    return std::format("{:02}:{:02}:{:02}", tm.tm_hour, tm.tm_min, tm.tm_sec);
}

} // namespace cfmdc::time_utils
