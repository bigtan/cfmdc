#include <algorithm>
#include <catch2/catch_test_macros.hpp>
#include <cfloat>
#include <cmath>
#include <cstring>
#include <limits>
#include <string>
#include <string_view>
#include <type_traits>
#include <vector>

#include "cfmdc/types/FrontServer.h"
#include "cfmdc/types/MarketTick.h"
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

TEST_CASE("bounded_sv and safe_copy_bounded prevent over-reads on unterminated buffers", "[helpers][safety]")
{
    constexpr char kEmpty[8]{};
    static_assert(bounded_sv(kEmpty).empty());

    constexpr char kUnterminated[8] = {'2', '0', '2', '5', '0', '1', '0', '2'};
    static_assert(bounded_sv(kUnterminated) == "20250102");

    struct GuardedBuffer
    {
        char buf[8];
        char guard[8];
    } guarded{{'2', '0', '2', '5', '0', '1', '0', '2'}, {'X', 'X', 'X', 'X', 'X', 'X', 'X', '\0'}};

    const std::string_view sv = bounded_sv(guarded.buf);
    REQUIRE(sv.size() == 8);
    REQUIRE(sv == "20250102");

    char embedded[8] = {'r', 'b', '\0', '9', '9', '9', '9', '9'};
    REQUIRE(bounded_sv(embedded) == "rb");

    char exact_dest[9]{};
    safe_copy_bounded(exact_dest, guarded.buf);
    REQUIRE(std::string_view(exact_dest) == "20250102");
    REQUIRE(exact_dest[8] == '\0');

    char short_dest[5]{};
    safe_copy_bounded(short_dest, guarded.buf);
    REQUIRE(std::string_view(short_dest) == "2025");
    REQUIRE(short_dest[4] == '\0');

    CThostFtdcRspInfoField rsp{};
    rsp.ErrorID = 42;
    std::memset(rsp.ErrorMsg, 'E', sizeof(rsp.ErrorMsg));
    const auto err = check_response(&rsp);
    REQUIRE(err.is_error());
    REQUIRE(err.error_id() == 42);
    REQUIRE(err.error_msg().size() == sizeof(rsp.ErrorMsg));
}

TEST_CASE("is_valid_trading_day enforces 8 ASCII digits", "[helpers][validation]")
{
    REQUIRE(is_valid_trading_day("20250102"));
    REQUIRE(is_valid_trading_day("20240229"));
    REQUIRE(is_valid_trading_day("00000000"));

    REQUIRE_FALSE(is_valid_trading_day(""));
    REQUIRE_FALSE(is_valid_trading_day("2025010"));
    REQUIRE_FALSE(is_valid_trading_day("202501020"));
    REQUIRE_FALSE(is_valid_trading_day("2025-01-02"));
    REQUIRE_FALSE(is_valid_trading_day("2025010a"));
    REQUIRE_FALSE(is_valid_trading_day("2025010 "));
    REQUIRE_FALSE(is_valid_trading_day("../20250"));
}

TEST_CASE("is_safe_instrument_id validates futures/options/spreads and blocks dangerous IDs", "[helpers][validation]")
{
    SECTION("Valid futures, options, and spread IDs")
    {
        REQUIRE(is_safe_instrument_id("rb2505"));
        REQUIRE(is_safe_instrument_id("IF2506"));
        REQUIRE(is_safe_instrument_id("a"));
        REQUIRE(is_safe_instrument_id("m2505-C-3000"));
        REQUIRE(is_safe_instrument_id("IO2505-P-3800"));
        REQUIRE(is_safe_instrument_id("SP rb2505&rb2509"));
        REQUIRE(is_safe_instrument_id("SPD MA505&MA509"));
        REQUIRE(is_safe_instrument_id("rb2505.SHFE"));
        REQUIRE(is_safe_instrument_id("rb_2505"));

        const std::string max_len_id = "A" + std::string(62, '1');
        REQUIRE(max_len_id.size() == 63);
        REQUIRE(is_safe_instrument_id(max_len_id));
    }

    SECTION("Invalid length and leading characters")
    {
        REQUIRE_FALSE(is_safe_instrument_id(""));
        REQUIRE_FALSE(is_safe_instrument_id(std::string(64, 'a')));
        REQUIRE_FALSE(is_safe_instrument_id(".rb2505"));
        REQUIRE_FALSE(is_safe_instrument_id("-rb2505"));
        REQUIRE_FALSE(is_safe_instrument_id("_rb2505"));
        REQUIRE_FALSE(is_safe_instrument_id("&rb2505"));
        REQUIRE_FALSE(is_safe_instrument_id(" rb2505"));
        REQUIRE_FALSE(is_safe_instrument_id("=CMD"));
        REQUIRE_FALSE(is_safe_instrument_id("=1+1"));
        REQUIRE_FALSE(is_safe_instrument_id("+CMD"));
        REQUIRE_FALSE(is_safe_instrument_id("@SUM(A1)"));
    }

    SECTION("Traversal, path separators, CSV delimiters, and control chars")
    {
        REQUIRE_FALSE(is_safe_instrument_id("../rb2505"));
        REQUIRE_FALSE(is_safe_instrument_id("rb2505/../etc"));
        REQUIRE_FALSE(is_safe_instrument_id("a..b"));
        REQUIRE_FALSE(is_safe_instrument_id("rb2505/x"));
        REQUIRE_FALSE(is_safe_instrument_id("rb2505\\x"));
        REQUIRE_FALSE(is_safe_instrument_id("rb2505:x"));
        REQUIRE_FALSE(is_safe_instrument_id("rb,2505"));
        REQUIRE_FALSE(is_safe_instrument_id("rb\"2505"));
        REQUIRE_FALSE(is_safe_instrument_id("rb'2505"));
        REQUIRE_FALSE(is_safe_instrument_id("rb\t2505"));
        REQUIRE_FALSE(is_safe_instrument_id("rb\n2505"));
        REQUIRE_FALSE(is_safe_instrument_id(std::string_view("rb\0 2505", 8)));
        REQUIRE_FALSE(is_safe_instrument_id("\xC3\xA9rb"));
    }
}

TEST_CASE("is_valid_ctp_price and clean_price filter sentinels while preserving zero and negative spreads",
          "[helpers][price]")
{
    REQUIRE(is_valid_ctp_price(0.0));
    REQUIRE(is_valid_ctp_price(-0.0));
    REQUIRE(is_valid_ctp_price(3500.5));
    REQUIRE(is_valid_ctp_price(-12.5));
    REQUIRE(is_valid_ctp_price(1e100));
    REQUIRE(is_valid_ctp_price(-1e100));

    REQUIRE(clean_price(0.0) == 0.0);
    REQUIRE(clean_price(3500.5) == 3500.5);
    REQUIRE(clean_price(-12.5) == -12.5);

    REQUIRE_FALSE(is_valid_ctp_price(DBL_MAX));
    REQUIRE_FALSE(is_valid_ctp_price(-DBL_MAX));
    REQUIRE_FALSE(is_valid_ctp_price(INVALID_PRICE));
    REQUIRE_FALSE(is_valid_ctp_price(-INVALID_PRICE));
    REQUIRE_FALSE(is_valid_ctp_price(INVALID_PRICE * 0.5));
    REQUIRE_FALSE(is_valid_ctp_price(-(INVALID_PRICE * 0.5)));
    REQUIRE_FALSE(is_valid_ctp_price(std::numeric_limits<double>::quiet_NaN()));
    REQUIRE_FALSE(is_valid_ctp_price(std::numeric_limits<double>::infinity()));
    REQUIRE_FALSE(is_valid_ctp_price(-std::numeric_limits<double>::infinity()));

    REQUIRE(clean_price(DBL_MAX) == 0.0);
    REQUIRE(clean_price(-DBL_MAX) == 0.0);
    REQUIRE(clean_price(std::numeric_limits<double>::quiet_NaN()) == 0.0);
    REQUIRE(clean_price(std::numeric_limits<double>::infinity()) == 0.0);
    REQUIRE(clean_price(-std::numeric_limits<double>::infinity()) == 0.0);
}

TEST_CASE("MarketTick::from_ctp safely converts raw CTP market data", "[market_tick]")
{
    static_assert(std::is_trivially_copyable_v<MarketTick>);

    CThostFtdcDepthMarketDataField raw{};
    std::memcpy(raw.TradingDay, "20250102", 8); // Full 8 bytes without relying on NUL inside 8 chars
    std::memcpy(raw.ActionDay, "20250101", 8);
    std::memcpy(raw.UpdateTime, "21:05:30", 8);
    std::memcpy(raw.ExchangeID, "SHFE", 4);
    const std::string long_id(80, 'X');
    std::memcpy(raw.InstrumentID, long_id.data(), 80); // Completely unterminated 81-byte field? Fill 80 bytes + 1
    raw.InstrumentID[80] = 'Y';

    raw.LastPrice = -12.5; // Negative spread price
    raw.PreSettlementPrice = 100.0;
    raw.PreClosePrice = 0.0;
    raw.PreOpenInterest = 5000.0;
    raw.OpenPrice = 101.0;
    raw.HighestPrice = 105.0;
    raw.LowestPrice = 99.0;
    raw.Turnover = 1234567.89;
    raw.OpenInterest = 5100.0;
    raw.ClosePrice = DBL_MAX; // Sentinel must be preserved raw in MarketTick
    raw.SettlementPrice = DBL_MAX;
    raw.UpperLimitPrice = 120.0;
    raw.LowerLimitPrice = 80.0;
    raw.PreDelta = 0.1;
    raw.CurrDelta = 0.2;
    raw.AveragePrice = 102.5;
    raw.Volume = 42;
    raw.UpdateMillisec = 500;

    raw.BidPrice1 = 100.1;
    raw.BidVolume1 = 11;
    raw.AskPrice1 = 100.2;
    raw.AskVolume1 = 21;
    raw.BidPrice2 = 100.0;
    raw.BidVolume2 = 12;
    raw.AskPrice2 = 100.3;
    raw.AskVolume2 = 22;
    raw.BidPrice3 = 99.9;
    raw.BidVolume3 = 13;
    raw.AskPrice3 = 100.4;
    raw.AskVolume3 = 23;
    raw.BidPrice4 = 99.8;
    raw.BidVolume4 = 14;
    raw.AskPrice4 = 100.5;
    raw.AskVolume4 = 24;
    raw.BidPrice5 = 99.7;
    raw.BidVolume5 = 15;
    raw.AskPrice5 = 100.6;
    raw.AskVolume5 = 25;

    constexpr int64_t kRecvNs = 1700000000123456789LL;
    const MarketTick tick = MarketTick::from_ctp(raw, kRecvNs);

    REQUIRE(tick.recv_time_ns == kRecvNs);
    REQUIRE(std::string_view(tick.trading_day) == "20250102");
    REQUIRE(std::string_view(tick.action_day) == "20250101");
    REQUIRE(std::string_view(tick.update_time) == "21:05:30");
    REQUIRE(std::string_view(tick.exchange_id) == "SHFE");
    REQUIRE(std::string_view(tick.instrument_id).size() == 63);
    REQUIRE(std::string_view(tick.instrument_id) == std::string(63, 'X'));

    REQUIRE(tick.last_price == -12.5);
    REQUIRE(tick.pre_settlement_price == 100.0);
    REQUIRE(tick.pre_close_price == 0.0);
    REQUIRE(tick.pre_open_interest == 5000.0);
    REQUIRE(tick.open_price == 101.0);
    REQUIRE(tick.highest_price == 105.0);
    REQUIRE(tick.lowest_price == 99.0);
    REQUIRE(tick.turnover == 1234567.89);
    REQUIRE(tick.open_interest == 5100.0);
    REQUIRE(tick.close_price == DBL_MAX);
    REQUIRE(tick.settlement_price == DBL_MAX);
    REQUIRE(tick.upper_limit_price == 120.0);
    REQUIRE(tick.lower_limit_price == 80.0);
    REQUIRE(tick.pre_delta == 0.1);
    REQUIRE(tick.curr_delta == 0.2);
    REQUIRE(tick.average_price == 102.5);
    REQUIRE(tick.volume == 42);
    REQUIRE(tick.update_millisec == 500);

    REQUIRE(tick.bid_price == std::array<double, 5>{100.1, 100.0, 99.9, 99.8, 99.7});
    REQUIRE(tick.ask_price == std::array<double, 5>{100.2, 100.3, 100.4, 100.5, 100.6});
    REQUIRE(tick.bid_volume == std::array<int32_t, 5>{11, 12, 13, 14, 15});
    REQUIRE(tick.ask_volume == std::array<int32_t, 5>{21, 22, 23, 24, 25});

    REQUIRE(now_epoch_ns() > 0);
    const MarketTick default_ts_tick = MarketTick::from_ctp(raw);
    REQUIRE(default_ts_tick.recv_time_ns > 0);
}

TEST_CASE("FrontServer::clear_secrets scrubs password and auth_code", "[front_server][security]")
{
    const std::string long_password(64, 'P');
    const std::string long_auth_code(64, 'A');

    FrontServer server = FrontServer::Builder()
                             .md_url("tcp://127.0.0.1:41213")
                             .td_url("tcp://127.0.0.1:41205")
                             .broker_id("9999")
                             .user_id("123456")
                             .password(long_password)
                             .user_product_info("cfmdc")
                             .auth_code(long_auth_code)
                             .app_id("simnow_client_test")
                             .build();

    REQUIRE(server.validate());
    server.clear_secrets();

    REQUIRE(server.password().empty());
    REQUIRE(server.auth_code().empty());
    REQUIRE_FALSE(server.validate());
    REQUIRE(server.md_url() == "tcp://127.0.0.1:41213");
    REQUIRE(server.td_url() == "tcp://127.0.0.1:41205");
    REQUIRE(server.broker_id() == "9999");
    REQUIRE(server.user_id() == "123456");
    REQUIRE(server.user_product_info() == "cfmdc");
    REQUIRE(server.app_id() == "simnow_client_test");
}

TEST_CASE("available_cpu_cores, resolve_worker_cores, and set_current_thread_name", "[helpers][cpu_affinity]")
{
    set_current_thread_name("cfmdc-test");
    set_current_thread_name("cfmdc-very-long-thread-name-truncated");

    const auto cores = available_cpu_cores();
    REQUIRE_FALSE(cores.empty());
    REQUIRE(std::is_sorted(cores.begin(), cores.end()));
    REQUIRE(cores.front() >= 0);

    REQUIRE(resolve_worker_cores(-1, -1) == std::pair<int, int>{-1, -1});
    REQUIRE(resolve_worker_cores(999999, 999999) == std::pair<int, int>{-1, -1});

    const auto [auto_ingest, auto_parquet] = resolve_worker_cores(-2, -2);
    int non_zero_count = 0;
    for (int core : cores)
    {
        if (core > 0)
        {
            ++non_zero_count;
        }
    }
    if (non_zero_count >= 2)
    {
        REQUIRE(auto_ingest > 0);
        REQUIRE(auto_parquet > 0);
        REQUIRE(auto_ingest != auto_parquet);
    }

    SECTION("Explicit available-core lists")
    {
        REQUIRE(resolve_worker_cores_for_available(-2, -2, {}) == std::pair<int, int>{-1, -1});
        REQUIRE(resolve_worker_cores_for_available(0, 0, {}) == std::pair<int, int>{-1, -1});
        REQUIRE(resolve_worker_cores_for_available(-2, -2, {0}) == std::pair<int, int>{-1, -1});

        REQUIRE(resolve_worker_cores_for_available(-2, -2, {0, 1}) == std::pair<int, int>{1, -1});
        REQUIRE(resolve_worker_cores_for_available(-2, -1, {0, 1}) == std::pair<int, int>{1, -1});
        REQUIRE(resolve_worker_cores_for_available(-1, -2, {0, 1}) == std::pair<int, int>{-1, 1});

        const auto [i4, p4] = resolve_worker_cores_for_available(-2, -2, {0, 1, 2, 3});
        REQUIRE(i4 > 0);
        REQUIRE(p4 > 0);
        REQUIRE(i4 != p4);

        const auto [isparse, psparse] = resolve_worker_cores_for_available(-2, -2, {2, 5});
        REQUIRE(isparse > 0);
        REQUIRE(psparse > 0);
        REQUIRE(isparse != psparse);
        REQUIRE((isparse == 2 || isparse == 5));
        REQUIRE((psparse == 2 || psparse == 5));

        REQUIRE(resolve_worker_cores_for_available(0, 1, {2, 5}) == std::pair<int, int>{-1, -1});
        REQUIRE(resolve_worker_cores_for_available(2, 5, {2, 5}) == std::pair<int, int>{2, 5});
        REQUIRE(resolve_worker_cores_for_available(5, -2, {2, 5}) == std::pair<int, int>{5, 2});
        REQUIRE(resolve_worker_cores_for_available(-2, 5, {2, 5}) == std::pair<int, int>{2, 5});
    }
}
