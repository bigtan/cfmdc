#include "cfmdc/core/TraderSpi.h"

#include <algorithm>
#include <format>
#include <ranges>
#include <stdexcept>
#include <unordered_set>

#include "cfmdc/utils/Constants.h"

namespace cfmdc
{

TraderSpi::TraderSpi(const FrontServer &server, const std::filesystem::path &flow_path)
    : trader_api_(flow_path.string().c_str()), server_(server)
{
    trader_api_->RegisterSpi(this);
    trader_api_->SubscribePublicTopic(THOST_TERT_QUICK);
    trader_api_->SubscribePrivateTopic(THOST_TERT_QUICK);
    trader_api_->RegisterFront(const_cast<char *>(server_.td_url().c_str()));

    spdlog::info("TraderSpi initialized with front: {}, flow path: {}", server_.td_url(), flow_path.string());
}

TraderSpi::~TraderSpi()
{
    spdlog::info("TraderSpi destructor called - releasing API...");
    // Release the API before the members the callbacks touch (mutexes, maps)
    // are destroyed, so no in-flight callback can race with member destruction.
    trader_api_.reset();
    server_.clear_secrets();
}

void TraderSpi::init()
{
    trader_api_->Init();
}

int TraderSpi::request_query_instrument(std::string_view instrument_id)
{
    {
        std::unique_lock cat_lock(instrument_mutex_);
        instrument_catalog_.clear();
    }
    {
        std::lock_guard lock(instrument_query_mutex_);
        instrument_query_completed_ = false;
        instrument_query_in_flight_ = true;
    }

    CThostFtdcQryInstrumentField instrument_req{};
    safe_strcpy(instrument_req.InstrumentID, instrument_id);
    const int result = trader_api_->ReqQryInstrument(&instrument_req, ++request_id_);
    if (result != 0)
    {
        std::lock_guard lock(instrument_query_mutex_);
        instrument_query_in_flight_ = false;
        instrument_query_completed_ = true;
        instrument_query_cv_.notify_all();
    }
    return result;
}

std::vector<std::string> TraderSpi::get_instrument_ids() const
{
    return resolve_subscription_instruments({}, false);
}

std::vector<std::string> TraderSpi::resolve_subscription_instruments(const std::vector<std::string> &requested,
                                                                   bool include_options) const
{
    std::shared_lock lock(instrument_mutex_);
    std::vector<std::string> resolved;

    if (requested.empty())
    {
        resolved.reserve(instrument_catalog_.size());
        for (const auto &[instrument_id, product_class] : instrument_catalog_)
        {
            const bool is_future = (product_class == THOST_FTDC_PC_Futures);
            const bool is_option =
                (product_class == THOST_FTDC_PC_Options || product_class == THOST_FTDC_PC_SpotOption);
            if (is_future || (include_options && is_option))
            {
                resolved.push_back(instrument_id);
            }
        }
        std::ranges::sort(resolved);
        const auto [first, last] = std::ranges::unique(resolved);
        resolved.erase(first, last);
        return resolved;
    }

    std::unordered_set<std::string> seen;
    seen.reserve(requested.size());
    resolved.reserve(requested.size());
    std::vector<std::string> missing;

    for (const auto &instrument_id : requested)
    {
        if (!seen.insert(instrument_id).second)
        {
            continue;
        }
        if (!instrument_catalog_.contains(instrument_id))
        {
            missing.push_back(instrument_id);
        }
        else
        {
            resolved.push_back(instrument_id);
        }
    }

    if (!missing.empty())
    {
        std::string missing_list;
        for (size_t i = 0; i < missing.size(); ++i)
        {
            if (i > 0)
            {
                missing_list += ", ";
            }
            missing_list += missing[i];
        }
        throw std::runtime_error(
            std::format("Requested instrument(s) not found in queried CTP catalog: {}", missing_list));
    }

    return resolved;
}

void TraderSpi::OnFrontDisconnected(int nReason)
{
    // The CTP API reconnects and re-authenticates automatically; just make it visible.
    spdlog::warn("Trading front disconnected, reason: {:#x} (API will auto-reconnect)", nReason);
}

void TraderSpi::OnHeartBeatWarning(int nTimeLapse)
{
    spdlog::warn("Trading front heartbeat warning, {}s since last message", nTimeLapse);
}

void TraderSpi::OnFrontConnected()
{
    spdlog::info("Trading front connection successful...");

    CThostFtdcReqAuthenticateField authenticate_field{};
    safe_strcpy(authenticate_field.BrokerID, server_.broker_id());
    safe_strcpy(authenticate_field.UserID, server_.user_id());
    safe_strcpy(authenticate_field.UserProductInfo, server_.user_product_info());
    safe_strcpy(authenticate_field.AuthCode, server_.auth_code());
    safe_strcpy(authenticate_field.AppID, server_.app_id());

    const int rt = trader_api_->ReqAuthenticate(&authenticate_field, ++request_id_);
    if (rt == 0)
    {
        spdlog::info("Authentication request sent successfully...");
    }
    else
    {
        spdlog::error("Authentication request failed, error code: {}", rt);
        initialization_tracker_.mark_failed(rt, std::format("Failed to send trader authentication request ({})", rt));
    }
}

void TraderSpi::OnRspAuthenticate(CThostFtdcRspAuthenticateField * /*pRspAuthenticateField*/,
                                  CThostFtdcRspInfoField *pRspInfo, int /*nRequestID*/, bool /*bIsLast*/)
{
    auto error = check_response(pRspInfo);
    if (!error.is_error())
    {
        spdlog::info("Authentication successful...");

        CThostFtdcReqUserLoginField login_req{};
        safe_strcpy(login_req.BrokerID, server_.broker_id());
        safe_strcpy(login_req.UserID, server_.user_id());
        safe_strcpy(login_req.Password, server_.password());

        const int rt = trader_api_->ReqUserLogin(&login_req, ++request_id_);
        if (rt == 0)
        {
            spdlog::info("Trading login request sent successfully...");
        }
        else
        {
            spdlog::error("Trading login request failed, error code: {}", rt);
            initialization_tracker_.mark_failed(rt, std::format("Failed to send trader login request ({})", rt));
        }
    }
    else
    {
        spdlog::error("Authentication error - ErrorID: {}, ErrorMsg: {}", error.error_id(), error.error_msg());
        initialization_tracker_.mark_failed(error.error_id(),
                                            std::format("Trader authentication failed: {}", error.error_msg()));
    }
}

void TraderSpi::OnRspUserLogin(CThostFtdcRspUserLoginField *pRspUserLogin, CThostFtdcRspInfoField *pRspInfo,
                               int /*nRequestID*/, bool /*bIsLast*/)
{
    auto error = check_response(pRspInfo);
    if (!error.is_error())
    {
        if (!pRspUserLogin)
        {
            spdlog::error("Trading login response succeeded but payload is null");
            initialization_tracker_.mark_failed(-1, "Trader login response payload is null");
            return;
        }

        const std::string_view trading_day = bounded_sv(pRspUserLogin->TradingDay);
        spdlog::info("Account login successful...");
        spdlog::info("Trading Day: {}", trading_day);
        spdlog::info("Broker ID: {}", server_.broker_id());

        {
            std::unique_lock lock(trading_day_mutex_);
            trading_day_.assign(trading_day);
        }

        CThostFtdcSettlementInfoConfirmField settlement_confirm_req{};
        safe_strcpy(settlement_confirm_req.BrokerID, server_.broker_id());
        safe_strcpy(settlement_confirm_req.InvestorID, server_.user_id());

        const int rt = trader_api_->ReqSettlementInfoConfirm(&settlement_confirm_req, ++request_id_);
        if (rt == 0)
        {
            spdlog::info("Settlement confirmation request sent successfully...");
        }
        else
        {
            spdlog::error("Settlement confirmation request failed, error code: {}", rt);
            initialization_tracker_.mark_failed(rt,
                                                std::format("Failed to send settlement confirmation request ({})", rt));
        }
    }
    else
    {
        spdlog::error("Login error - ErrorID: {}, ErrorMsg: {}", error.error_id(), error.error_msg());
        initialization_tracker_.mark_failed(error.error_id(),
                                            std::format("Trader login failed: {}", error.error_msg()));
    }
}

void TraderSpi::OnRspUserLogout(CThostFtdcUserLogoutField * /*pUserLogout*/, CThostFtdcRspInfoField * /*pRspInfo*/,
                                int /*nRequestID*/, bool /*bIsLast*/)
{
    spdlog::info("User logout response received");
}

void TraderSpi::OnRspSettlementInfoConfirm(CThostFtdcSettlementInfoConfirmField * /*pSettlementInfoConfirm*/,
                                           CThostFtdcRspInfoField *pRspInfo, int /*nRequestID*/, bool /*bIsLast*/)
{
    auto error = check_response(pRspInfo);
    if (!error.is_error())
    {
        spdlog::info("Investor settlement confirmation successful...");
        initialization_tracker_.mark_ready();
    }
    else
    {
        spdlog::error("Settlement confirmation error - ErrorID: {}, ErrorMsg: {}", error.error_id(), error.error_msg());
        initialization_tracker_.mark_failed(error.error_id(),
                                            std::format("Settlement confirmation failed: {}", error.error_msg()));
    }
}

void TraderSpi::OnRspQryInstrument(CThostFtdcInstrumentField *pInstrument, CThostFtdcRspInfoField *pRspInfo,
                                   int /*nRequestID*/, bool bIsLast)
{
    auto error = check_response(pRspInfo);
    if (error.is_error())
    {
        spdlog::error("Instrument query error - ErrorID: {}, ErrorMsg: {}", error.error_id(), error.error_msg());
    }

    if (pInstrument)
    {
        const std::string_view instrument_id = bounded_sv(pInstrument->InstrumentID);
        if (!is_safe_instrument_id(instrument_id))
        {
            spdlog::warn("Ignoring instrument with unsafe ID (length: {}) from CTP query", instrument_id.size());
        }
        else
        {
            spdlog::debug("Instrument {} query successful...", instrument_id);

            std::unique_lock lock(instrument_mutex_);
            instrument_catalog_.insert_or_assign(std::string(instrument_id), pInstrument->ProductClass);
        }
    }

    if (bIsLast)
    {
        {
            std::shared_lock lock(instrument_mutex_);
            spdlog::info("Instrument query completed, {} valid instruments collected", instrument_catalog_.size());
        }

        std::lock_guard lock(instrument_query_mutex_);
        instrument_query_in_flight_ = false;
        instrument_query_completed_ = true;
        instrument_query_cv_.notify_all();
    }
}

} // namespace cfmdc
