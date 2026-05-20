#pragma once

#include <cstdint>
#include <string>

namespace cfmdc::market_data_time
{

// All time strings are expected to be fixed-width "HH:MM:SS".

namespace detail
{

using PackedTime = uint64_t;

struct TimeRange
{
    PackedTime begin;
    PackedTime end;

    constexpr bool contains(PackedTime value) const noexcept
    {
        return value >= begin && value <= end;
    }
};

constexpr PackedTime pack_hhmmss_unchecked(const char *time_str) noexcept
{
    return (static_cast<uint64_t>(static_cast<unsigned char>(time_str[0])) << 56) |
           (static_cast<uint64_t>(static_cast<unsigned char>(time_str[1])) << 48) |
           (static_cast<uint64_t>(static_cast<unsigned char>(time_str[2])) << 40) |
           (static_cast<uint64_t>(static_cast<unsigned char>(time_str[3])) << 32) |
           (static_cast<uint64_t>(static_cast<unsigned char>(time_str[4])) << 24) |
           (static_cast<uint64_t>(static_cast<unsigned char>(time_str[5])) << 16) |
           (static_cast<uint64_t>(static_cast<unsigned char>(time_str[6])) << 8) |
           static_cast<uint64_t>(static_cast<unsigned char>(time_str[7]));
}

inline bool parse_hhmmss(const char *time_str, PackedTime &packed_time) noexcept
{
    if (!time_str)
    {
        return false;
    }

    size_t len = 0;
    while (len < 9 && time_str[len] != '\0')
    {
        ++len;
    }
    if (len != 8 || time_str[2] != ':' || time_str[5] != ':')
    {
        return false;
    }

    const char h0 = time_str[0];
    const char h1 = time_str[1];
    const char m0 = time_str[3];
    const char m1 = time_str[4];
    const char s0 = time_str[6];
    const char s1 = time_str[7];

    const bool valid = h0 >= '0' && h0 <= '2' && h1 >= '0' && h1 <= (h0 == '2' ? '3' : '9') && m0 >= '0' && m0 <= '5' &&
                       m1 >= '0' && m1 <= '9' && s0 >= '0' && s0 <= '5' && s1 >= '0' && s1 <= '9';
    if (!valid)
    {
        return false;
    }

    packed_time = pack_hhmmss_unchecked(time_str);
    return true;
}

} // namespace detail

inline bool is_valid_hhmmss(const char *time_str) noexcept
{
    detail::PackedTime ignored = 0;
    return detail::parse_hhmmss(time_str, ignored);
}

// Startup time windows
constexpr const char *kStartupCutoff0700 = "07:00:00";
constexpr const char *kStartupCutoff1700 = "17:00:00";

// Storage filter ranges
constexpr detail::TimeRange kKeepPre0700 = {detail::pack_hhmmss_unchecked("00:00:00"),
                                            detail::pack_hhmmss_unchecked("07:00:00")};
constexpr detail::TimeRange kKeepDay = {detail::pack_hhmmss_unchecked("08:55:00"),
                                        detail::pack_hhmmss_unchecked("15:30:00")};
constexpr detail::TimeRange kKeepNight1 = {detail::pack_hhmmss_unchecked("20:55:00"),
                                           detail::pack_hhmmss_unchecked("23:59:59")};
constexpr detail::TimeRange kKeepNight2 = {detail::pack_hhmmss_unchecked("00:00:00"),
                                           detail::pack_hhmmss_unchecked("03:00:00")};

// ActionDay cutoff (inclusive): UpdateTime <= 07:00:00 -> next_action_day
constexpr detail::PackedTime kStartupCutoff0700Packed = detail::pack_hhmmss_unchecked(kStartupCutoff0700);
constexpr detail::PackedTime kStartupCutoff1700Packed = detail::pack_hhmmss_unchecked(kStartupCutoff1700);
constexpr detail::PackedTime kActionDayCutoffPacked = detail::pack_hhmmss_unchecked("07:00:00");

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
    detail::PackedTime startup_time = 0;
    if (!detail::parse_hhmmss(startup_time_hms.c_str(), startup_time))
    {
        return StartupWindow::Night;
    }

    if (startup_time < kStartupCutoff0700Packed)
    {
        return StartupWindow::Pre0700;
    }

    if (startup_time <= kStartupCutoff1700Packed)
    {
        return StartupWindow::Day;
    }

    return StartupWindow::Night;
}

inline bool should_store_by_update_time(StartupWindow startup_window, const char *update_time) noexcept
{
    detail::PackedTime update = 0;
    if (!detail::parse_hhmmss(update_time, update))
    {
        return false;
    }

    switch (startup_window)
    {
    case StartupWindow::Pre0700:
        return kKeepPre0700.contains(update);
    case StartupWindow::Day:
        return kKeepDay.contains(update);
    case StartupWindow::Night:
        return kKeepNight1.contains(update) || kKeepNight2.contains(update);
    default:
        return false;
    }
}

inline bool should_use_next_action_day(const char *update_time) noexcept
{
    detail::PackedTime update = 0;
    return detail::parse_hhmmss(update_time, update) && update <= kActionDayCutoffPacked;
}

} // namespace cfmdc::market_data_time
