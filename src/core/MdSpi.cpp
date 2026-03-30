#include "cfmdc/core/MdSpi.h"

#include <chrono>

#include "cfmdc/utils/Constants.h"

namespace cfmdc
{

MdSpi::MdSpi(const FrontServer &server, const Config &config, const std::filesystem::path &flow_path)
    : md_api_(flow_path.string().c_str()), server_(server), config_(config)
{
    md_api_->RegisterSpi(this);
    md_api_->RegisterFront(const_cast<char *>(server_.md_url().c_str()));

    auto storage_mode = config_.storage_mode();
    spdlog::info("MdSpi initialized with front: {}, storage mode: {}, flow path: {}", server_.md_url(),
                 to_string(storage_mode), flow_path.string());
}

MdSpi::~MdSpi()
{
    spdlog::info("MdSpi destructor called - stopping async file manager...");
    if (async_file_manager_)
    {
        async_file_manager_->stop();
        async_file_manager_->flush_all();
    }
}

void MdSpi::init()
{
    md_api_->Init();
}

int MdSpi::subscribe_market_data(std::span<char *> instrument_ids)
{
    if (instrument_ids.empty())
    {
        spdlog::error("SubscribeMarketData called with empty instrument list");
        return -1;
    }
    return md_api_->SubscribeMarketData(instrument_ids.data(), static_cast<int>(instrument_ids.size()));
}

void MdSpi::set_trading_day_and_action_days(const std::string &trading_day, const std::string &base_action_day,
                                            const std::string &next_action_day, const std::string &startup_time_hms)
{
    trading_day_ = trading_day;
    action_day_base_ = base_action_day;
    action_day_next_ = next_action_day;

    spdlog::info("MdSpi configured with TradingDay: {}, BaseActionDay: {}, NextActionDay: {}", trading_day_,
                 action_day_base_, action_day_next_);

    // Resolve paths with actual trading day (from Trader SPI)
    auto csv_path = config_.csv_path(trading_day_);
    auto parquet_path = config_.parquet_path(trading_day_);
    auto storage_mode = config_.storage_mode();

    spdlog::info("Resolved storage paths - CSV: {}, Parquet: {}", csv_path.string(), parquet_path.string());

    // Initialize async file manager with lock-free queue for high-frequency trading
    // Re-initialization is safe here as this is called before subscription
    async_file_manager_ = std::make_unique<AsyncFileManager>(csv_path, parquet_path, trading_day_, action_day_base_,
                                                             action_day_next_, startup_time_hms, storage_mode);
}

void MdSpi::OnFrontConnected()
{
    spdlog::info("Front connection successful...");

    CThostFtdcReqUserLoginField login_req{};
    safe_strcpy(login_req.BrokerID, server_.broker_id());
    safe_strcpy(login_req.UserID, server_.user_id());
    safe_strcpy(login_req.Password, server_.password());

    int rt = md_api_->ReqUserLogin(&login_req, DEFAULT_REQUEST_ID);
    if (rt == 0)
    {
        spdlog::info("Login request sent successfully...");
    }
    else
    {
        spdlog::error("Failed to send login request, error code: {}", rt);
    }
}

void MdSpi::OnRspUserLogin(CThostFtdcRspUserLoginField *pRspUserLogin, CThostFtdcRspInfoField *pRspInfo,
                           int /*nRequestID*/, bool /*bIsLast*/)
{
    auto error = check_response(pRspInfo);
    if (!error.is_error())
    {
        if (!pRspUserLogin)
        {
            spdlog::error("Market data login response succeeded but payload is null");
            return;
        }

        spdlog::info("Login response received...");
        spdlog::info("Trading Day: {}", pRspUserLogin->TradingDay);
        spdlog::info("Login Time: {}", pRspUserLogin->LoginTime);
        spdlog::info("Broker: {}", pRspUserLogin->BrokerID);
        spdlog::info("Account: {}", pRspUserLogin->UserID);

        // Note: trading_day_ and async_file_manager_ will be set by Application
        // using the authoritative TradingDay from TraderSpi via set_trading_day_and_action_days()
        is_ready_.store(true, std::memory_order_release);
    }
    else
    {
        spdlog::error("Login error - ErrorID: {}, ErrorMsg: {}", error.error_id(), error.error_msg());
    }
}

void MdSpi::OnRspSubMarketData(CThostFtdcSpecificInstrumentField *pSpecificInstrument, CThostFtdcRspInfoField *pRspInfo,
                               int /*nRequestID*/, bool /*bIsLast*/)
{
    auto error = check_response(pRspInfo);
    if (!error.is_error())
    {
        if (pSpecificInstrument)
        {
            spdlog::info("Market data subscription successful...");
            spdlog::info("Instrument code: {} subscribed successfully", pSpecificInstrument->InstrumentID);
        }
        else
        {
            spdlog::info("Market data subscription callback completed without instrument payload");
        }
    }
    else
    {
        spdlog::error("Subscription error - ErrorID: {}, ErrorMsg: {}", error.error_id(), error.error_msg());
    }
}

void MdSpi::OnRtnDepthMarketData(CThostFtdcDepthMarketDataField *pDepthMarketData)
{
    if (pDepthMarketData && async_file_manager_)
    {
        // High-performance: just enqueue data
        // Data correction (TradingDay/ActionDay) is now handled in AsyncFileManager consumer thread
        if (!async_file_manager_->write_market_data_async(*pDepthMarketData))
        {
            // Optional: log queue full condition, but avoid frequent logging
            static thread_local auto last_warning = std::chrono::steady_clock::now();
            auto now = std::chrono::steady_clock::now();
            if (now - last_warning > std::chrono::seconds(1))
            {
                spdlog::warn("Market data queue is full, dropping data for {}", pDepthMarketData->InstrumentID);
                last_warning = now;
            }
        }
    }
}

} // namespace cfmdc
