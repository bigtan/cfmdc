#pragma once

#include <cstring>
#include <string>

namespace cfmdc::market_data_time
{

// All time strings are expected to be fixed-width "HH:MM:SS".

struct TimeRange
{
    const char *begin;
    const char *end;
};

inline bool in_range_inclusive(const char *value, TimeRange range) noexcept
{
    return value && value[0] != '\0' && std::strcmp(value, range.begin) >= 0 && std::strcmp(value, range.end) <= 0;
}

// Startup time windows
constexpr const char *kStartupCutoff0700 = "07:00:00";
constexpr const char *kStartupCutoff1700 = "17:00:00";

// Storage filter ranges
constexpr TimeRange kKeepPre0700 = {"00:00:00", "07:00:00"};
constexpr TimeRange kKeepDay = {"08:55:00", "15:30:00"};
constexpr TimeRange kKeepNight1 = {"20:55:00", "23:59:59"};
constexpr TimeRange kKeepNight2 = {"00:00:00", "03:00:00"};

// ActionDay cutoff (inclusive): UpdateTime <= 07:00:00 -> next_action_day
constexpr const char *kActionDayCutoff = "07:00:00";

enum class StartupWindow
{
    Pre0700, // startup < 07:00:00
    Day,     // 07:00:00 <= startup <= 17:00:00
    Night    // startup > 17:00:00
};

inline const char *to_string(StartupWindow window) noexcept
{
    switch (window)
    {
    case StartupWindow::Pre0700:
        return "pre0700";
    case StartupWindow::Day:
        return "day";
    case StartupWindow::Night:
        return "night";
    default:
        return "unknown";
    }
}

inline StartupWindow classify_startup_window(const std::string &startup_time_hms) noexcept
{
    if (std::strcmp(startup_time_hms.c_str(), kStartupCutoff0700) < 0)
    {
        return StartupWindow::Pre0700;
    }

    if (std::strcmp(startup_time_hms.c_str(), kStartupCutoff1700) <= 0)
    {
        return StartupWindow::Day;
    }

    return StartupWindow::Night;
}

inline bool should_store_by_update_time(StartupWindow startup_window, const char *update_time) noexcept
{
    switch (startup_window)
    {
    case StartupWindow::Pre0700:
        return in_range_inclusive(update_time, kKeepPre0700);
    case StartupWindow::Day:
        return in_range_inclusive(update_time, kKeepDay);
    case StartupWindow::Night:
        return in_range_inclusive(update_time, kKeepNight1) || in_range_inclusive(update_time, kKeepNight2);
    default:
        return false;
    }
}

inline bool should_use_next_action_day(const char *update_time) noexcept
{
    return update_time && update_time[0] != '\0' && std::strcmp(update_time, kActionDayCutoff) <= 0;
}

} // namespace cfmdc::market_data_time
