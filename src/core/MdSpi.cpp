#include "cfmdc/core/MdSpi.h"

#include <chrono>
#include <format>

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
    const int rt = md_api_->SubscribeMarketData(instrument_ids.data(), static_cast<int>(instrument_ids.size()));
    if (rt == 0)
    {
        subscription_tracker_.mark_request_sent(instrument_ids);
    }
    return rt;
}

int MdSpi::send_resubscribe_request(std::span<char *> instruments, size_t attempt)
{
    if (!md_api_ || shutdown_started_.load(std::memory_order_acquire))
    {
        return -1;
    }

    const int rt = md_api_->SubscribeMarketData(instruments.data(), static_cast<int>(instruments.size()));
    if (rt == 0)
    {
        spdlog::info("Re-subscribed {} instruments after front reconnection (attempt {}/{})", instruments.size(),
                     attempt, SubscriptionTracker::kDefaultResubscribeAttempts);
    }
    else
    {
        spdlog::warn("Failed to re-subscribe {} instruments on attempt {}/{} (error code: {})", instruments.size(),
                     attempt, SubscriptionTracker::kDefaultResubscribeAttempts, rt);
    }
    return rt;
}

void MdSpi::resubscribe_market_data()
{
    // Try once immediately without blocking/sleeping on CTP's network callback thread.
    // Further retries and response-timeout handling are driven by check_pipeline_health().
    (void)subscription_tracker_.begin_reconnect_resubscribe(
        [this](std::span<char *> instruments, size_t attempt) { return send_resubscribe_request(instruments, attempt); });
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

    auto csv_path = use_csv ? config_.csv_path(trading_day_) : std::filesystem::path{};
    auto parquet_path = use_parquet ? config_.parquet_path(trading_day_) : std::filesystem::path{};

    spdlog::info("Resolved storage paths - CSV: {}, Parquet: {}", csv_path.string(), parquet_path.string());

    // Initialize async file manager with lock-free queue for high-frequency trading
    // Re-initialization is safe here as this is called before subscription
    file_manager_.store(nullptr, std::memory_order_release);
    AsyncFileManager::Options file_manager_options;
    file_manager_options.worker_core = config_.worker_thread_core();
    if (use_parquet)
    {
        file_manager_options.parquet_row_group_size = config_.parquet_row_group_size();
    }
    async_file_manager_ =
        std::make_unique<AsyncFileManager>(csv_path, parquet_path, trading_day_, action_day_base_, action_day_next_,
                                           startup_time_hms, storage_mode, file_manager_options);
    // Publish the fully-constructed manager to the CTP callback thread
    file_manager_.store(async_file_manager_.get(), std::memory_order_release);
}

void MdSpi::check_pipeline_health()
{
    if (auto *file_manager = file_manager_.load(std::memory_order_acquire))
    {
        const auto stats = file_manager->get_statistics();
        if (stats.dropped_records > last_reported_dropped_)
        {
            const auto now = std::chrono::steady_clock::now();
            if (!last_drop_warning_time_.has_value() || now - *last_drop_warning_time_ >= std::chrono::seconds(1))
            {
                const size_t newly_dropped = stats.dropped_records - last_reported_dropped_;
                spdlog::warn("Market data ingestion queue full: {} record(s) dropped since last report "
                             "(total dropped: {}, queue depth: {})",
                             newly_dropped, stats.dropped_records, stats.queue_size);
                last_reported_dropped_ = stats.dropped_records;
                last_drop_warning_time_ = now;
            }
        }
    }

    const bool stopping = shutdown_started_.load(std::memory_order_acquire) || !md_api_;
    const auto prev_state = subscription_tracker_.resubscribe_state();
    const size_t prev_attempt = subscription_tracker_.resubscribe_attempt();
    const auto state = subscription_tracker_.poll_resubscribe(
        [this](std::span<char *> instruments, size_t attempt) { return send_resubscribe_request(instruments, attempt); },
        stopping);

    if (prev_state == SubscriptionTracker::ResubscribeState::WaitingResponse &&
        state == SubscriptionTracker::ResubscribeState::WaitingRetry)
    {
        spdlog::warn("Market data re-subscription attempt {}/{} did not complete cleanly within deadline; "
                     "scheduling retry",
                     prev_attempt, SubscriptionTracker::kDefaultResubscribeAttempts);
    }

    if (state == SubscriptionTracker::ResubscribeState::Failed && !resubscribe_failure_logged_)
    {
        resubscribe_failure_logged_ = true;
        spdlog::critical("Exhausted all {} attempts to re-subscribe instruments after reconnection; "
                         "marking market data pipeline as failed",
                         SubscriptionTracker::kDefaultResubscribeAttempts);
    }
}

void MdSpi::log_statistics() const
{
    auto *file_manager = file_manager_.load(std::memory_order_acquire);
    if (!file_manager)
    {
        return;
    }

    const auto stats = file_manager->get_statistics();
    spdlog::info("Market data pipeline: stored={}, csv={}, parquet={}, ingress_queue={}, parquet_queue={}, dropped={}, "
                 "write_failures={}, csv_flushes={}",
                 stats.total_records, stats.csv_records, stats.parquet_records, stats.queue_size,
                 stats.parquet_queue_size, stats.dropped_records, stats.write_failures, stats.periodic_flushes);
}

bool MdSpi::has_fatal_pipeline_error() const noexcept
{
    if (subscription_tracker_.has_resubscription_failure())
    {
        return true;
    }
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
    server_.clear_secrets();
    file_manager_.store(nullptr, std::memory_order_release);

    bool success = !subscription_tracker_.has_resubscription_failure();
    if (async_file_manager_)
    {
        async_file_manager_->stop();
        const bool flushed = async_file_manager_->flush_all();
        const bool closed = async_file_manager_->close_all();
        success = success && flushed && closed && !async_file_manager_->has_fatal_error();
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

    const int rt = md_api_->ReqUserLogin(&login_req, DEFAULT_REQUEST_ID);
    if (rt == 0)
    {
        spdlog::info("Login request sent successfully...");
    }
    else
    {
        spdlog::error("Failed to send login request, error code: {}", rt);
        initialization_tracker_.mark_failed(rt, std::format("Failed to send market data login request ({})", rt));
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
            initialization_tracker_.mark_failed(-1, "Market data login response payload is null");
            return;
        }

        const std::string_view trading_day = bounded_sv(pRspUserLogin->TradingDay);
        const std::string_view login_time = bounded_sv(pRspUserLogin->LoginTime);
        const std::string_view broker_id = bounded_sv(pRspUserLogin->BrokerID);
        const std::string_view user_id = bounded_sv(pRspUserLogin->UserID);

        spdlog::info("Login response received...");
        spdlog::info("Trading Day: {}", trading_day);
        spdlog::info("Login Time: {}", login_time);
        spdlog::info("Broker: {}", broker_id);
        spdlog::info("Account: {}", user_id);

        // Note: trading_day_ and async_file_manager_ will be set by Application
        // using the authoritative TradingDay from TraderSpi via set_trading_day_and_action_days()
        if (!initialization_tracker_.mark_ready())
        {
            resubscribe_market_data();
        }
    }
    else
    {
        spdlog::error("Login error - ErrorID: {}, ErrorMsg: {}", error.error_id(), error.error_msg());
        initialization_tracker_.mark_failed(error.error_id(),
                                            std::format("Market data login failed: {}", error.error_msg()));
    }
}

void MdSpi::OnRspSubMarketData(CThostFtdcSpecificInstrumentField *pSpecificInstrument, CThostFtdcRspInfoField *pRspInfo,
                               int /*nRequestID*/, bool bIsLast)
{
    auto error = check_response(pRspInfo);
    const std::string_view instrument_id =
        pSpecificInstrument ? bounded_sv(pSpecificInstrument->InstrumentID) : std::string_view{};
    subscription_tracker_.record(instrument_id, !error.is_error(), bIsLast);

    if (!error.is_error())
    {
        if (pSpecificInstrument)
        {
            spdlog::debug("Instrument {} subscribed successfully", instrument_id);
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
    // The CTP API reconnects automatically; MdSpi will re-login and re-subscribe upon reconnection.
    subscription_tracker_.on_disconnected();
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
        // High-performance lock-free/I/O-free hot path: overflow count is tracked atomically
        // in LockFreeQueue and warned from the main loop via check_pipeline_health().
        (void)file_manager->write_market_data_async(*pDepthMarketData);
    }
}

} // namespace cfmdc
