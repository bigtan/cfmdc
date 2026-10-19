#include <catch2/catch_test_macros.hpp>

#include "cfmdc/utils/Error.h"
#include "cfmdc/utils/Helpers.h"

using namespace cfmdc;

TEST_CASE("CtpError basic functionality", "[error]")
{
    SECTION("No error")
    {
        CtpError error(0, "");
        REQUIRE_FALSE(error.is_error());
        REQUIRE(error.error_id() == 0);
    }

    SECTION("With error")
    {
        CtpError error(100, "Test error message");
        REQUIRE(error.is_error());
        REQUIRE(error.error_id() == 100);
        REQUIRE(error.error_msg() == "Test error message");
    }
}

TEST_CASE("CtpError throw_if_error", "[error]")
{
    SECTION("No error - should not throw")
    {
        CtpError error(0, "");
        REQUIRE_NOTHROW(error.throw_if_error());
    }

    SECTION("With error - should throw")
    {
        CtpError error(100, "Test error");
        REQUIRE_THROWS_AS(error.throw_if_error(), CtpException);
    }

    SECTION("Throw specific exception type")
    {
        CtpError error(100, "Connection failed");
        REQUIRE_THROWS_AS(error.throw_if_error<ConnectionException>(), ConnectionException);
    }
}

TEST_CASE("Exception hierarchy", "[error]")
{
    SECTION("CtpException contains error code")
    {
        CtpException ex(100, "Test error");
        REQUIRE(ex.error_code() == 100);
        REQUIRE(std::string(ex.what()).contains("100"));
        REQUIRE(std::string(ex.what()).contains("Test error"));
    }

    SECTION("Derived exceptions work correctly")
    {
        ConnectionException conn_ex(101, "Connection error");
        REQUIRE(conn_ex.error_code() == 101);

        AuthenticationException auth_ex(102, "Auth error");
        REQUIRE(auth_ex.error_code() == 102);

        LoginException login_ex(103, "Login error");
        REQUIRE(login_ex.error_code() == 103);
    }

    SECTION("ConfigException")
    {
        ConfigException config_ex("Invalid configuration");
        REQUIRE(std::string(config_ex.what()) == "Invalid configuration");
    }

    SECTION("ApiException")
    {
        ApiException api_ex("API initialization failed");
        REQUIRE(std::string(api_ex.what()) == "API initialization failed");
    }
}

TEST_CASE("check_response converts GBK error messages to UTF-8", "[error][encoding]")
{
    CThostFtdcRspInfoField rsp{};
    rsp.ErrorID = 3;
    // "CTP:不合法的登录" encoded in GBK
    constexpr char kGbkMsg[] = "CTP:\xb2\xbb\xba\xcf\xb7\xa8\xb5\xc4\xb5\xc7\xc2\xbc";
    safe_strcpy(rsp.ErrorMsg, kGbkMsg);

    const auto err = check_response(&rsp);
    REQUIRE(err.is_error());
    REQUIRE(err.error_id() == 3);
    REQUIRE(is_valid_utf8(err.error_msg()));
    REQUIRE(err.error_msg() == "CTP:不合法的登录");
}
