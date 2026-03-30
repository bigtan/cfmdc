#pragma once

#include <spdlog/spdlog.h>

#include <atomic>
#include <filesystem>
#include <memory>
#include <span>
#include <string>

#include "cfmdc/config/Config.h"
#include "cfmdc/types/FrontServer.h"
#include "cfmdc/utils/ApiWrapper.h"
#include "cfmdc/utils/FileManager.h"
#include "cfmdc/utils/Helpers.h"

namespace cfmdc
{

/// @brief CTP Market Data SPI implementation
/// @details Handles market data callbacks and async file writing
/// @thread_safety Callbacks may be invoked from different threads.
///                File writing is async and lock-free.
class MdSpi : public CThostFtdcMdSpi
{
  public:
    /// @brief Constructor
    /// @param server Front server configuration
    /// @param config Configuration object for path resolution
    /// @param flow_path Path to flow directory for CTP API
    explicit MdSpi(const FrontServer &server, const Config &config, const std::filesystem::path &flow_path);

    /// @brief Destructor
    ~MdSpi();

    // Disable copy/move - API objects shouldn't be copied
    MdSpi(const MdSpi &) = delete;
    MdSpi &operator=(const MdSpi &) = delete;
    MdSpi(MdSpi &&) = delete;
    MdSpi &operator=(MdSpi &&) = delete;

    /// @brief Initialize the API
    void init();

    /// @brief Subscribe to market data
    /// @param instrument_ids Span of instrument IDs
    /// @return Request result (0 = success)
    int subscribe_market_data(std::span<char *> instrument_ids);

    /// @brief Check if market data connection is ready
    /// @return true if logged in
    bool is_ready() const noexcept
    {
        return is_ready_.load(std::memory_order_acquire);
    }

    /// @brief Set the trading day (from Trader API) and pre-calculated action days
    /// @param trading_day The authorized trading day
    /// @param base_action_day The startup physical date (YYYYMMDD)
    /// @param next_action_day The next physical date (YYYYMMDD)
    /// @param startup_time_hms Local startup time HH:MM:SS (used for UpdateTime filtering)
    void set_trading_day_and_action_days(const std::string &trading_day, const std::string &base_action_day,
                                         const std::string &next_action_day, const std::string &startup_time_hms);

    // CTP API callbacks
    void OnFrontConnected() override;
    void OnRspUserLogin(CThostFtdcRspUserLoginField *pRspUserLogin, CThostFtdcRspInfoField *pRspInfo, int nRequestID,
                        bool bIsLast) override;
    void OnRspSubMarketData(CThostFtdcSpecificInstrumentField *pSpecificInstrument, CThostFtdcRspInfoField *pRspInfo,
                            int nRequestID, bool bIsLast) override;
    void OnRtnDepthMarketData(CThostFtdcDepthMarketDataField *pDepthMarketData) override;

  private:
    MdApiWrapper md_api_;
    FrontServer server_;
    const Config &config_;
    std::unique_ptr<AsyncFileManager> async_file_manager_;

    std::string trading_day_;
    std::string action_day_base_;
    std::string action_day_next_;
    std::atomic<bool> is_ready_{false};
};

} // namespace cfmdc
