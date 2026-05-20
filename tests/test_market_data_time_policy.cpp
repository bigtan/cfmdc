#include <catch2/catch_test_macros.hpp>
#include <cstring>
#include <format>
#include <string>

#include "cfmdc/utils/MarketDataTimePolicy.h"

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
