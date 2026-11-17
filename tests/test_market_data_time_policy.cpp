#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <cstring>
#include <format>
#include <string>
#include <string_view>

#include "cfmdc/utils/MarketDataTimePolicy.h"
#include "cfmdc/utils/TimeUtils.h"

using namespace cfmdc::market_data_time;

TEST_CASE("Startup window classification", "[time_policy]")
{
    REQUIRE(classify_startup_window("06:59:59") == StartupWindow::Pre0700);
    REQUIRE(classify_startup_window("07:00:00") == StartupWindow::Day);
    REQUIRE(classify_startup_window("17:00:00") == StartupWindow::Day);
    REQUIRE(classify_startup_window("17:00:01") == StartupWindow::Night);
}

TEST_CASE("UpdateTime filtering", "[time_policy]")
{
    SECTION("Pre0700 keeps early window")
    {
        REQUIRE(should_store_by_update_time(StartupWindow::Pre0700, "00:00:00"));
        REQUIRE(should_store_by_update_time(StartupWindow::Pre0700, "06:59:59"));
        REQUIRE(should_store_by_update_time(StartupWindow::Pre0700, "07:00:00"));
        REQUIRE_FALSE(should_store_by_update_time(StartupWindow::Pre0700, "07:00:01"));
        REQUIRE_FALSE(should_store_by_update_time(StartupWindow::Pre0700, "08:00:00"));
    }

    SECTION("Day keeps main session window")
    {
        REQUIRE(should_store_by_update_time(StartupWindow::Day, "08:55:00"));
        REQUIRE(should_store_by_update_time(StartupWindow::Day, "15:20:00"));
        REQUIRE(should_store_by_update_time(StartupWindow::Day, "15:30:00"));
        REQUIRE_FALSE(should_store_by_update_time(StartupWindow::Day, "08:00:00"));
        REQUIRE_FALSE(should_store_by_update_time(StartupWindow::Day, "16:00:00"));
    }

    SECTION("Night keeps night sessions")
    {
        REQUIRE(should_store_by_update_time(StartupWindow::Night, "20:55:00"));
        REQUIRE(should_store_by_update_time(StartupWindow::Night, "23:59:59"));
        REQUIRE(should_store_by_update_time(StartupWindow::Night, "00:00:00"));
        REQUIRE(should_store_by_update_time(StartupWindow::Night, "02:30:00"));
        REQUIRE(should_store_by_update_time(StartupWindow::Night, "03:00:00"));
        REQUIRE_FALSE(should_store_by_update_time(StartupWindow::Night, "03:00:01"));
        REQUIRE_FALSE(should_store_by_update_time(StartupWindow::Night, "15:00:00"));
    }
}

TEST_CASE("ActionDay decision", "[time_policy]")
{
    REQUIRE(should_use_next_action_day("00:00:00"));
    REQUIRE(should_use_next_action_day("07:00:00"));
    REQUIRE_FALSE(should_use_next_action_day("07:00:01"));
}

TEST_CASE("UpdateTime rejects malformed HHMMSS strings", "[time_policy]")
{
    REQUIRE_FALSE(should_store_by_update_time(StartupWindow::Day, nullptr));
    REQUIRE_FALSE(should_store_by_update_time(StartupWindow::Day, ""));
    REQUIRE_FALSE(should_store_by_update_time(StartupWindow::Day, "12"));
    REQUIRE_FALSE(should_store_by_update_time(StartupWindow::Day, "08:55"));
    REQUIRE_FALSE(should_store_by_update_time(StartupWindow::Day, "08a55b00"));
    REQUIRE_FALSE(should_store_by_update_time(StartupWindow::Day, "24:00:00"));
    REQUIRE_FALSE(should_store_by_update_time(StartupWindow::Day, "08:60:00"));
    REQUIRE_FALSE(should_store_by_update_time(StartupWindow::Day, "08:55:60"));
    REQUIRE_FALSE(should_use_next_action_day(nullptr));
    REQUIRE_FALSE(should_use_next_action_day("12"));
}

TEST_CASE("Packed HHMMSS comparisons match strcmp semantics for every valid second", "[time_policy]")
{
    auto in_range_by_strcmp = [](const std::string &value, const char *begin, const char *end) {
        return std::strcmp(value.c_str(), begin) >= 0 && std::strcmp(value.c_str(), end) <= 0;
    };

    for (int seconds = 0; seconds < 24 * 60 * 60; ++seconds)
    {
        const int hour = seconds / 3600;
        const int minute = (seconds / 60) % 60;
        const int second = seconds % 60;
        const std::string hms = std::format("{:02d}:{:02d}:{:02d}", hour, minute, second);

        const auto expected_startup_window =
            std::strcmp(hms.c_str(), "07:00:00") < 0
                ? StartupWindow::Pre0700
                : (std::strcmp(hms.c_str(), "17:00:00") <= 0 ? StartupWindow::Day : StartupWindow::Night);
        REQUIRE(classify_startup_window(hms) == expected_startup_window);

        REQUIRE(should_store_by_update_time(StartupWindow::Pre0700, hms.c_str()) ==
                in_range_by_strcmp(hms, "00:00:00", "07:00:00"));
        REQUIRE(should_store_by_update_time(StartupWindow::Day, hms.c_str()) ==
                in_range_by_strcmp(hms, "08:55:00", "15:30:00"));
        REQUIRE(should_store_by_update_time(StartupWindow::Night, hms.c_str()) ==
                (in_range_by_strcmp(hms, "20:55:00", "23:59:59") || in_range_by_strcmp(hms, "00:00:00", "03:00:00")));
        REQUIRE(should_use_next_action_day(hms.c_str()) == (std::strcmp(hms.c_str(), "07:00:00") <= 0));
    }
}

TEST_CASE("Calendar date offset handles month, year, and leap-year boundaries", "[time_utils]")
{
    std::tm jan1{};
    jan1.tm_year = 2025 - 1900;
    jan1.tm_mon = 0; // January
    jan1.tm_mday = 1;
    REQUIRE(cfmdc::time_utils::offset_days_yyyymmdd(jan1, -1) == "20241231");
    REQUIRE(cfmdc::time_utils::offset_days_yyyymmdd(jan1, 1) == "20250102");

    std::tm feb28_leap{};
    feb28_leap.tm_year = 2024 - 1900;
    feb28_leap.tm_mon = 1; // February
    feb28_leap.tm_mday = 28;
    REQUIRE(cfmdc::time_utils::offset_days_yyyymmdd(feb28_leap, 1) == "20240229");
    REQUIRE(cfmdc::time_utils::offset_days_yyyymmdd(feb28_leap, 2) == "20240301");
}

TEST_CASE("StringView overloads match const char* and std::string without ambiguity", "[time_policy]")
{
    const char *cstr = "09:30:00";
    const std::string str = "09:30:00";
    const std::string backed = "09:30:00_trailing";
    const std::string_view sv = std::string_view(backed).substr(0, 8);

    REQUIRE(is_valid_hhmmss(cstr));
    REQUIRE(is_valid_hhmmss(str));
    REQUIRE(is_valid_hhmmss(sv));
    REQUIRE_FALSE(is_valid_hhmmss(std::string_view("09:30:00\0", 9)));
    REQUIRE_FALSE(is_valid_hhmmss(std::string_view{}));
    REQUIRE_FALSE(is_valid_hhmmss(nullptr));

    REQUIRE(classify_startup_window("06:59:59") == StartupWindow::Pre0700);
    REQUIRE(classify_startup_window(std::string("07:00:00")) == StartupWindow::Day);
    REQUIRE(classify_startup_window(std::string_view("17:00:01_x").substr(0, 8)) == StartupWindow::Night);
    REQUIRE(classify_startup_window(nullptr) == StartupWindow::Night);

    REQUIRE(should_store_by_update_time(StartupWindow::Day, cstr));
    REQUIRE(should_store_by_update_time(StartupWindow::Day, str));
    REQUIRE(should_store_by_update_time(StartupWindow::Day, sv));

    REQUIRE(should_use_next_action_day("06:59:59"));
    REQUIRE(should_use_next_action_day(std::string("07:00:00")));
    REQUIRE_FALSE(should_use_next_action_day(sv));
}

TEST_CASE("China Standard Time UTC+8 helpers handle day/night, year, and leap-year boundaries", "[time_utils]")
{
    auto make_utc_tp = [](int year, unsigned month, unsigned day, int hour, int minute, int second) {
        const std::chrono::year_month_day ymd{std::chrono::year{year}, std::chrono::month{month},
                                              std::chrono::day{day}};
        return std::chrono::sys_days{ymd} + std::chrono::hours{hour} + std::chrono::minutes{minute} +
               std::chrono::seconds{second};
    };

    SECTION("Pre-07:00 CST vs 07:00 CST startup cutoff")
    {
        // 2025-01-01 22:59:59 UTC == 2025-01-02 06:59:59 CST
        const auto pre0700 = cfmdc::time_utils::compute_china_session_dates(make_utc_tp(2025, 1, 1, 22, 59, 59));
        REQUIRE(pre0700.startup_time_hms == "06:59:59");
        REQUIRE(pre0700.base_action_day == "20250101");
        REQUIRE(pre0700.next_action_day == "20250102");

        // 2025-01-01 23:00:00 UTC == 2025-01-02 07:00:00 CST
        const auto at0700 = cfmdc::time_utils::compute_china_session_dates(make_utc_tp(2025, 1, 1, 23, 0, 0));
        REQUIRE(at0700.startup_time_hms == "07:00:00");
        REQUIRE(at0700.base_action_day == "20250102");
        REQUIRE(at0700.next_action_day == "20250103");
    }

    SECTION("Night session across CST midnight")
    {
        // 2025-01-02 15:59:59 UTC == 2025-01-02 23:59:59 CST
        const auto before_mid = cfmdc::time_utils::compute_china_session_dates(make_utc_tp(2025, 1, 2, 15, 59, 59));
        REQUIRE(before_mid.startup_time_hms == "23:59:59");
        REQUIRE(before_mid.base_action_day == "20250102");
        REQUIRE(before_mid.next_action_day == "20250103");

        // 2025-01-02 16:00:00 UTC == 2025-01-03 00:00:00 CST
        const auto at_mid = cfmdc::time_utils::compute_china_session_dates(make_utc_tp(2025, 1, 2, 16, 0, 0));
        REQUIRE(at_mid.startup_time_hms == "00:00:00");
        REQUIRE(at_mid.base_action_day == "20250102");
        REQUIRE(at_mid.next_action_day == "20250103");
    }

    SECTION("Year boundary")
    {
        // 2024-12-31 16:00:00 UTC == 2025-01-01 00:00:00 CST
        const auto ny_midnight = cfmdc::time_utils::compute_china_session_dates(make_utc_tp(2024, 12, 31, 16, 0, 0));
        REQUIRE(ny_midnight.startup_time_hms == "00:00:00");
        REQUIRE(ny_midnight.base_action_day == "20241231");
        REQUIRE(ny_midnight.next_action_day == "20250101");

        // 2024-12-31 23:00:00 UTC == 2025-01-01 07:00:00 CST
        const auto ny_morning = cfmdc::time_utils::compute_china_session_dates(make_utc_tp(2024, 12, 31, 23, 0, 0));
        REQUIRE(ny_morning.startup_time_hms == "07:00:00");
        REQUIRE(ny_morning.base_action_day == "20250101");
        REQUIRE(ny_morning.next_action_day == "20250102");
    }

    SECTION("Leap-year and non-leap-year February boundary")
    {
        // 2024-02-28 16:00:00 UTC == 2024-02-29 00:00:00 CST (leap year)
        const auto leap_tp = make_utc_tp(2024, 2, 28, 16, 0, 0);
        const std::tm leap_tm = cfmdc::time_utils::to_china_tm(leap_tp);
        REQUIRE(leap_tm.tm_year == 2024 - 1900);
        REQUIRE(leap_tm.tm_mon == 1);
        REQUIRE(leap_tm.tm_mday == 29);
        REQUIRE(leap_tm.tm_hour == 0);
        REQUIRE(leap_tm.tm_min == 0);
        REQUIRE(leap_tm.tm_sec == 0);
        REQUIRE(leap_tm.tm_wday == 4); // Thursday
        REQUIRE(leap_tm.tm_yday == 59);
        REQUIRE(leap_tm.tm_isdst == 0);

        const auto leap_mid = cfmdc::time_utils::compute_china_session_dates(leap_tp);
        REQUIRE(leap_mid.startup_time_hms == "00:00:00");
        REQUIRE(leap_mid.base_action_day == "20240228");
        REQUIRE(leap_mid.next_action_day == "20240229");

        const auto leap_day = cfmdc::time_utils::compute_china_session_dates(make_utc_tp(2024, 2, 28, 23, 0, 0));
        REQUIRE(leap_day.startup_time_hms == "07:00:00");
        REQUIRE(leap_day.base_action_day == "20240229");
        REQUIRE(leap_day.next_action_day == "20240301");

        // 2025-02-28 16:00:00 UTC == 2025-03-01 00:00:00 CST (non-leap year)
        const auto non_leap_mid = cfmdc::time_utils::compute_china_session_dates(make_utc_tp(2025, 2, 28, 16, 0, 0));
        REQUIRE(non_leap_mid.startup_time_hms == "00:00:00");
        REQUIRE(non_leap_mid.base_action_day == "20250228");
        REQUIRE(non_leap_mid.next_action_day == "20250301");
    }
}
