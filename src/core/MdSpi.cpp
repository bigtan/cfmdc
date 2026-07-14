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
    spdlog::info("MdSpi destructor called - releasing API and stopping async file manager...");
    if (!shutdown())
    {
        spdlog::critical("Market data storage did not shut down cleanly");
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
    subscription_tracker_.begin(instrument_ids);
    return md_api_->SubscribeMarketData(instrument_ids.data(), static_cast<int>(instrument_ids.size()));
}

bool MdSpi::wait_for_subscription_completion(std::chrono::seconds timeout)
{
    return subscription_tracker_.wait_for_completion(timeout);
}

SubscriptionTracker::Result MdSpi::subscription_result() const
{
    return subscription_tracker_.result();
}

void MdSpi::set_trading_day_and_action_days(const std::string &trading_day, const std::string &base_action_day,
                                            const std::string &next_action_day, const std::string &startup_time_hms)
{
    trading_day_ = trading_day;
    action_day_base_ = base_action_day;
    action_day_next_ = next_action_day;

    spdlog::info("MdSpi configured with TradingDay: {}, BaseActionDay: {}, NextActionDay: {}", trading_day_,
                 action_day_base_, action_day_next_);

    // Resolve only the paths required by the configured storage mode
    auto storage_mode = config_.storage_mode();
    bool use_csv = storage_mode == StorageMode::CSV || storage_mode == StorageMode::HYBRID;
    bool use_parquet = storage_mode == StorageMode::PARQUET || storage_mode == StorageMode::HYBRID;
#ifndef CFMDC_ENABLE_PARQUET
    if (use_parquet)
    {
        // AsyncFileManager falls back to CSV when Parquet support is not compiled in
        use_csv = true;
        use_parquet = false;
    }
#endif

    auto csv_path = use_csv ? config_.csv_path(trading_day_) : std::filesystem::path{};
    auto parquet_path = use_parquet ? config_.parquet_path(trading_day_) : std::filesystem::path{};

    spdlog::info("Resolved storage paths - CSV: {}, Parquet: {}", csv_path.string(), parquet_path.string());

    // Initialize async file manager with lock-free queue for high-frequency trading
    // Re-initialization is safe here as this is called before subscription
    file_manager_.store(nullptr, std::memory_order_release);
    async_file_manager_ = std::make_unique<AsyncFileManager>(
        csv_path, parquet_path, trading_day_, action_day_base_, action_day_next_, startup_time_hms, storage_mode,
        config_.worker_thread_core());
    // Publish the fully-constructed manager to the CTP callback thread
    file_manager_.store(async_file_manager_.get(), std::memory_order_release);
}

void MdSpi::log_statistics() const
{
    auto *file_manager = file_manager_.load(std::memory_order_acquire);
    if (!file_manager)
    {
        return;
    }

    const auto stats = file_manager->get_statistics();
    spdlog::info("Market data pipeline: stored={}, queue={}, dropped={}, write_failures={}", stats.total_records,
                 stats.queue_size, stats.dropped_records, stats.write_failures);
}

bool MdSpi::has_fatal_pipeline_error() const noexcept
{
    auto *file_manager = file_manager_.load(std::memory_order_acquire);
    return file_manager && file_manager->has_fatal_error();
}

bool MdSpi::shutdown()
{
    if (shutdown_started_.exchange(true, std::memory_order_acq_rel))
    {
        return shutdown_succeeded_.load(std::memory_order_acquire);
    }

    // Release the API first so callbacks cannot race with queue teardown.
    md_api_.reset();
    file_manager_.store(nullptr, std::memory_order_release);

    bool success = true;
    if (async_file_manager_)
    {
        async_file_manager_->stop();
        success = async_file_manager_->flush_all() && !async_file_manager_->has_fatal_error();
    }
    shutdown_succeeded_.store(success, std::memory_order_release);
    return success;
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
                               int /*nRequestID*/, bool bIsLast)
{
    auto error = check_response(pRspInfo);
    const std::string_view instrument_id = pSpecificInstrument ? pSpecificInstrument->InstrumentID : "";
    subscription_tracker_.record(instrument_id, !error.is_error(), bIsLast);

    if (!error.is_error())
    {
        if (pSpecificInstrument)
        {
            spdlog::debug("Instrument {} subscribed successfully", pSpecificInstrument->InstrumentID);
        }
        else if (!bIsLast)
        {
            spdlog::warn("Subscription callback succeeded without instrument payload");
        }
    }
    else
    {
        spdlog::error("Subscription error for {} - ErrorID: {}, ErrorMsg: {}",
                      instrument_id.empty() ? "<unknown>" : instrument_id, error.error_id(), error.error_msg());
    }
}

void MdSpi::OnFrontDisconnected(int nReason)
{
    // The CTP API reconnects and re-subscribes automatically; just make it visible.
    spdlog::warn("Market data front disconnected, reason: {:#x} (API will auto-reconnect)", nReason);
}

void MdSpi::OnHeartBeatWarning(int nTimeLapse)
{
    spdlog::warn("Market data heartbeat warning, {}s since last message", nTimeLapse);
}

void MdSpi::OnRtnDepthMarketData(CThostFtdcDepthMarketDataField *pDepthMarketData)
{
    auto *file_manager = file_manager_.load(std::memory_order_acquire);
    if (pDepthMarketData && file_manager)
    {
        // High-performance: just enqueue data
        // Data correction (TradingDay/ActionDay) is now handled in AsyncFileManager consumer thread
        if (!file_manager->write_market_data_async(*pDepthMarketData))
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
