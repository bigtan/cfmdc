#pragma once

#include <array>
#include <chrono>
#include <cstdint>
#include <type_traits>

#include "ThostFtdcUserApiStruct.h"
#include "cfmdc/utils/Helpers.h"

namespace cfmdc
{

/// @brief Return current wall-clock timestamp as nanoseconds since Unix epoch
inline int64_t now_epoch_ns() noexcept
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::system_clock::now().time_since_epoch())
        .count();
}

/// @brief Compact, trivially copyable market tick decoupled from raw CTP struct padding and reserved fields
struct MarketTick
{
    int64_t recv_time_ns{0};

    double last_price{0.0};
    double pre_settlement_price{0.0};
    double pre_close_price{0.0};
    double pre_open_interest{0.0};
    double open_price{0.0};
    double highest_price{0.0};
    double lowest_price{0.0};
    double turnover{0.0};
    double open_interest{0.0};
    double close_price{0.0};
    double settlement_price{0.0};
    double upper_limit_price{0.0};
    double lower_limit_price{0.0};
    double pre_delta{0.0};
    double curr_delta{0.0};
    double average_price{0.0};
    std::array<double, 5> bid_price{};
    std::array<double, 5> ask_price{};

    int32_t volume{0};
    int32_t update_millisec{0};
    std::array<int32_t, 5> bid_volume{};
    std::array<int32_t, 5> ask_volume{};

    char trading_day[9]{};
    char action_day[9]{};
    char update_time[9]{};
    char exchange_id[9]{};
    char instrument_id[64]{};

    /// @brief Construct a MarketTick from a raw CTP depth market data field
    static MarketTick from_ctp(const CThostFtdcDepthMarketDataField &raw, int64_t recv_ns = now_epoch_ns()) noexcept
    {
        MarketTick tick;
        tick.recv_time_ns = recv_ns;

        tick.last_price = raw.LastPrice;
        tick.pre_settlement_price = raw.PreSettlementPrice;
        tick.pre_close_price = raw.PreClosePrice;
        tick.pre_open_interest = raw.PreOpenInterest;
        tick.open_price = raw.OpenPrice;
        tick.highest_price = raw.HighestPrice;
        tick.lowest_price = raw.LowestPrice;
        tick.turnover = raw.Turnover;
        tick.open_interest = raw.OpenInterest;
        tick.close_price = raw.ClosePrice;
        tick.settlement_price = raw.SettlementPrice;
        tick.upper_limit_price = raw.UpperLimitPrice;
        tick.lower_limit_price = raw.LowerLimitPrice;
        tick.pre_delta = raw.PreDelta;
        tick.curr_delta = raw.CurrDelta;
        tick.average_price = raw.AveragePrice;
        tick.bid_price = {raw.BidPrice1, raw.BidPrice2, raw.BidPrice3, raw.BidPrice4, raw.BidPrice5};
        tick.ask_price = {raw.AskPrice1, raw.AskPrice2, raw.AskPrice3, raw.AskPrice4, raw.AskPrice5};

        tick.volume = static_cast<int32_t>(raw.Volume);
        tick.update_millisec = static_cast<int32_t>(raw.UpdateMillisec);
        tick.bid_volume = {
            static_cast<int32_t>(raw.BidVolume1), static_cast<int32_t>(raw.BidVolume2),
            static_cast<int32_t>(raw.BidVolume3), static_cast<int32_t>(raw.BidVolume4),
            static_cast<int32_t>(raw.BidVolume5),
        };
        tick.ask_volume = {
            static_cast<int32_t>(raw.AskVolume1), static_cast<int32_t>(raw.AskVolume2),
            static_cast<int32_t>(raw.AskVolume3), static_cast<int32_t>(raw.AskVolume4),
            static_cast<int32_t>(raw.AskVolume5),
        };

        safe_copy_bounded(tick.trading_day, raw.TradingDay);
        safe_copy_bounded(tick.action_day, raw.ActionDay);
        safe_copy_bounded(tick.update_time, raw.UpdateTime);
        safe_copy_bounded(tick.exchange_id, raw.ExchangeID);
        safe_copy_bounded(tick.instrument_id, raw.InstrumentID);

        return tick;
    }
};

static_assert(std::is_trivially_copyable_v<MarketTick>);

} // namespace cfmdc
