#include <catch2/catch_test_macros.hpp>

#include <string_view>

#include "cfmdc/utils/Constants.h"

TEST_CASE("Application version is wired from build metadata", "[constants]")
{
    REQUIRE_FALSE(cfmdc::APP_VERSION.empty());
    REQUIRE(cfmdc::APP_VERSION != std::string_view{"unknown"});
    REQUIRE(cfmdc::APP_VERSION == std::string_view{CFMDC_VERSION});
    REQUIRE(cfmdc::APP_VERSION.find(' ') == std::string_view::npos);
    REQUIRE(cfmdc::APP_VERSION.find('.') != std::string_view::npos);
}
