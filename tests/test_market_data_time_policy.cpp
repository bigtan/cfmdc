#include <catch2/catch_test_macros.hpp>

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
        REQUIRE(should_store_by_update_time(StartupWindow::Night, "02:30:00"));
        REQUIRE_FALSE(should_store_by_update_time(StartupWindow::Night, "15:00:00"));
    }
}

TEST_CASE("ActionDay decision", "[time_policy]")
{
    REQUIRE(should_use_next_action_day("07:00:00"));
    REQUIRE_FALSE(should_use_next_action_day("07:00:01"));
}
