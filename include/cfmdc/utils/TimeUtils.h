#pragma once

#include <chrono>
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

inline std::string offset_days_yyyymmdd(const std::tm &tm, int day_offset)
{
    const std::chrono::year_month_day current{
        std::chrono::year{tm.tm_year + 1900},
        std::chrono::month{static_cast<unsigned>(tm.tm_mon + 1)},
        std::chrono::day{static_cast<unsigned>(tm.tm_mday)},
    };
    const std::chrono::year_month_day shifted{std::chrono::sys_days{current} + std::chrono::days{day_offset}};
    return std::format("{:04}{:02}{:02}", static_cast<int>(shifted.year()), static_cast<unsigned>(shifted.month()),
                       static_cast<unsigned>(shifted.day()));
}

inline std::string format_hhmmss(const std::tm &tm)
{
    return std::format("{:02}:{:02}:{:02}", tm.tm_hour, tm.tm_min, tm.tm_sec);
}

} // namespace cfmdc::time_utils
