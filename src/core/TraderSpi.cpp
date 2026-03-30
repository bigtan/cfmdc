#include "cfmdc/core/TraderSpi.h"

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

void TraderSpi::init()
{
    trader_api_->Init();
}

int TraderSpi::request_query_instrument(std::string_view instrument_id)
{
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
    std::shared_lock lock(instrument_mutex_);
    return instrument_ids_; // Return copy for thread safety
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

    int rt = trader_api_->ReqAuthenticate(&authenticate_field, ++request_id_);
    if (rt == 0)
    {
        spdlog::info("Authentication request sent successfully...");
    }
    else
    {
        spdlog::error("Authentication request failed, error code: {}", rt);
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

        int rt = trader_api_->ReqUserLogin(&login_req, ++request_id_);
        if (rt == 0)
        {
            spdlog::info("Trading login request sent successfully...");
        }
        else
        {
            spdlog::error("Trading login request failed, error code: {}", rt);
        }
    }
    else
    {
        spdlog::error("Authentication error - ErrorID: {}, ErrorMsg: {}", error.error_id(), error.error_msg());
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
            return;
        }

        spdlog::info("Account login successful...");
        spdlog::info("Trading Day: {}", pRspUserLogin->TradingDay);
        spdlog::info("Broker ID: {}", server_.broker_id());

        {
            std::unique_lock lock(trading_day_mutex_);
            trading_day_ = pRspUserLogin->TradingDay;
        }

        CThostFtdcSettlementInfoConfirmField settlement_confirm_req{};
        safe_strcpy(settlement_confirm_req.BrokerID, server_.broker_id());
        safe_strcpy(settlement_confirm_req.InvestorID, server_.user_id());

        int rt = trader_api_->ReqSettlementInfoConfirm(&settlement_confirm_req, ++request_id_);
        if (rt == 0)
        {
            spdlog::info("Settlement confirmation request sent successfully...");
        }
        else
        {
            spdlog::error("Settlement confirmation request failed, error code: {}", rt);
        }
    }
    else
    {
        spdlog::error("Login error - ErrorID: {}, ErrorMsg: {}", error.error_id(), error.error_msg());
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
        is_ready_.store(true, std::memory_order_release);
    }
    else
    {
        spdlog::error("Settlement confirmation error - ErrorID: {}, ErrorMsg: {}", error.error_id(), error.error_msg());
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

    if (pInstrument && pInstrument->ProductClass == THOST_FTDC_APC_FutureSingle)
    {
        spdlog::info("Instrument {} query successful...", pInstrument->InstrumentID);

        std::unique_lock lock(instrument_mutex_);
        instrument_ids_.emplace_back(pInstrument->InstrumentID);
    }

    if (bIsLast)
    {
        std::lock_guard lock(instrument_query_mutex_);
        instrument_query_in_flight_ = false;
        instrument_query_completed_ = true;
        instrument_query_cv_.notify_all();
    }
}

bool TraderSpi::wait_for_instrument_query_completion(std::chrono::seconds timeout)
{
    std::unique_lock lock(instrument_query_mutex_);
    if (!instrument_query_in_flight_)
    {
        return instrument_query_completed_;
    }

    return instrument_query_cv_.wait_for(lock, timeout, [this]() { return instrument_query_completed_; });
}

} // namespace cfmdc
