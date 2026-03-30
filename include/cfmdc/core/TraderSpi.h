#pragma once

#include <spdlog/spdlog.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <mutex>
#include <shared_mutex>
#include <vector>

#include "cfmdc/types/FrontServer.h"
#include "cfmdc/utils/ApiWrapper.h"
#include "cfmdc/utils/Helpers.h"

namespace cfmdc
{

/// @brief CTP Trader SPI implementation
/// @details Handles trader API callbacks for authentication and instrument
/// queries
/// @thread_safety Callbacks may be invoked from different threads.
///                Use atomic operations for state flags and mutex for shared
///                data.
class TraderSpi : public CThostFtdcTraderSpi
{
  public:
    /// @brief Constructor
    /// @param server Front server configuration
    /// @param flow_path Path to flow directory for CTP API
    explicit TraderSpi(const FrontServer &server, const std::filesystem::path &flow_path);

    /// @brief Destructor
    ~TraderSpi() = default;

    // Disable copy/move - API objects shouldn't be copied
    TraderSpi(const TraderSpi &) = delete;
    TraderSpi &operator=(const TraderSpi &) = delete;
    TraderSpi(TraderSpi &&) = delete;
    TraderSpi &operator=(TraderSpi &&) = delete;

    /// @brief Initialize the API
    void init();

    /// @brief Request to query instrument information
    /// @param instrument_id Instrument ID (empty for all instruments)
    /// @return Request result (0 = success)
    int request_query_instrument(std::string_view instrument_id);

    /// @brief Check if trader is ready
    /// @return true if authenticated and logged in
    bool is_ready() const noexcept
    {
        return is_ready_.load(std::memory_order_acquire);
    }

    /// @brief Get instrument IDs (thread-safe)
    /// @return Vector of instrument IDs
    std::vector<std::string> get_instrument_ids() const;

    /// @brief Get the current trading day
    /// @return Trading day string (YYYYMMDD)
    std::string get_trading_day() const
    {
        std::shared_lock lock(trading_day_mutex_);
        return trading_day_;
    }

    // CTP API callbacks
    void OnFrontConnected() override;
    void OnRspAuthenticate(CThostFtdcRspAuthenticateField *pRspAuthenticateField, CThostFtdcRspInfoField *pRspInfo,
                           int nRequestID, bool bIsLast) override;
    void OnRspUserLogin(CThostFtdcRspUserLoginField *pRspUserLogin, CThostFtdcRspInfoField *pRspInfo, int nRequestID,
                        bool bIsLast) override;
    void OnRspUserLogout(CThostFtdcUserLogoutField *pUserLogout, CThostFtdcRspInfoField *pRspInfo, int nRequestID,
                         bool bIsLast) override;
    void OnRspSettlementInfoConfirm(CThostFtdcSettlementInfoConfirmField *pSettlementInfoConfirm,
                                    CThostFtdcRspInfoField *pRspInfo, int nRequestID, bool bIsLast) override;
    void OnRspQryInstrument(CThostFtdcInstrumentField *pInstrument, CThostFtdcRspInfoField *pRspInfo, int nRequestID,
                            bool bIsLast) override;

    /// @brief Wait for current instrument query request to complete
    /// @param timeout Maximum time to wait
    /// @return true if the current query completed before timeout
    bool wait_for_instrument_query_completion(std::chrono::seconds timeout);

  private:
    TraderApiWrapper trader_api_;
    FrontServer server_;

    // Thread-safe state
    std::atomic<bool> is_ready_{false};
    std::atomic<int> request_id_{0};

    // Protected by mutex
    mutable std::shared_mutex instrument_mutex_;
    std::vector<std::string> instrument_ids_;
    std::mutex instrument_query_mutex_;
    std::condition_variable instrument_query_cv_;
    bool instrument_query_in_flight_{false};
    bool instrument_query_completed_{false};

    mutable std::shared_mutex trading_day_mutex_;
    std::string trading_day_;
};

} // namespace cfmdc
