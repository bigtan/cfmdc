#pragma once

#include <chrono>
#include <ctime>
#include <format>
#include <string>

namespace cfmdc::time_utils
{

inline std::tm to_china_tm(std::chrono::system_clock::time_point tp) noexcept
{
    const auto china_sec = std::chrono::floor<std::chrono::seconds>(tp) + std::chrono::hours{8};
    const auto day_tp = std::chrono::floor<std::chrono::days>(china_sec);
    const std::chrono::year_month_day ymd{day_tp};
    const std::chrono::hh_mm_ss<std::chrono::seconds> tod{china_sec - day_tp};

    std::tm tm{};
    tm.tm_year = static_cast<int>(ymd.year()) - 1900;
    tm.tm_mon = static_cast<int>(static_cast<unsigned>(ymd.month())) - 1;
    tm.tm_mday = static_cast<int>(static_cast<unsigned>(ymd.day()));
    tm.tm_hour = static_cast<int>(tod.hours().count());
    tm.tm_min = static_cast<int>(tod.minutes().count());
    tm.tm_sec = static_cast<int>(tod.seconds().count());
    tm.tm_wday = static_cast<int>(std::chrono::weekday{day_tp}.c_encoding());
    const auto jan1 = std::chrono::sys_days{ymd.year() / std::chrono::January / std::chrono::day{1}};
    tm.tm_yday = static_cast<int>((day_tp - jan1).count());
    tm.tm_isdst = 0;
    return tm;
}

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

struct SessionDates
{
    std::string base_action_day;
    std::string next_action_day;
    std::string startup_time_hms;
};

inline SessionDates compute_china_session_dates(
    std::chrono::system_clock::time_point tp = std::chrono::system_clock::now())
{
    const std::tm china_tm = to_china_tm(tp);
    SessionDates dates;
    dates.startup_time_hms = format_hhmmss(china_tm);

    if (china_tm.tm_hour < 7)
    {
        dates.base_action_day = offset_days_yyyymmdd(china_tm, -1);
        dates.next_action_day = format_yyyymmdd(china_tm);
    }
    else
    {
        dates.base_action_day = format_yyyymmdd(china_tm);
        dates.next_action_day = offset_days_yyyymmdd(china_tm, 1);
    }
    return dates;
}

} // namespace cfmdc::time_utils
