#include "cfmdc/utils/CsvWriter.h"

#include <spdlog/spdlog.h>

#include <format>

#include "cfmdc/utils/Helpers.h"

namespace cfmdc
{

CsvWriter::CsvWriter(const std::filesystem::path &base_path, const std::string &trading_day)
    : base_path_(base_path), trading_day_(trading_day)
{
    spdlog::info("CsvWriter initialized: base_path={}, trading_day={}", base_path_.string(), trading_day_);
}

CsvWriter::~CsvWriter()
{
    close_all();
}

bool CsvWriter::write(const CThostFtdcDepthMarketDataField &data)
{
    const std::string instrument_id = data.InstrumentID;
    std::ofstream *file = get_or_create_file(instrument_id);

    if (!file || !file->is_open())
    {
        return false;
    }

    auto csv_line = std::format(
        "{},{},{},{},{},{},{},{},{},{},{},{:.0f},{},{},{},{},{},{},{},{},"
        "{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        data.TradingDay, data.InstrumentID, data.ExchangeID, clean_price(data.LastPrice),
        clean_price(data.PreSettlementPrice), clean_price(data.PreClosePrice), data.PreOpenInterest,
        clean_price(data.OpenPrice), clean_price(data.HighestPrice), clean_price(data.LowestPrice), data.Volume,
        clean_price(data.Turnover), data.OpenInterest, clean_price(data.ClosePrice), clean_price(data.SettlementPrice),
        clean_price(data.UpperLimitPrice), clean_price(data.LowerLimitPrice), clean_price(data.PreDelta),
        clean_price(data.CurrDelta), data.UpdateTime, data.UpdateMillisec, clean_price(data.BidPrice1), data.BidVolume1,
        clean_price(data.AskPrice1), data.AskVolume1, clean_price(data.BidPrice2), data.BidVolume2,
        clean_price(data.AskPrice2), data.AskVolume2, clean_price(data.BidPrice3), data.BidVolume3,
        clean_price(data.AskPrice3), data.AskVolume3, clean_price(data.BidPrice4), data.BidVolume4,
        clean_price(data.AskPrice4), data.AskVolume4, clean_price(data.BidPrice5), data.BidVolume5,
        clean_price(data.AskPrice5), data.AskVolume5, clean_price(data.AveragePrice), data.ActionDay);

    *file << csv_line << '\n';
    return true;
}

void CsvWriter::close_all()
{
    for (auto &[instrument_id, file] : file_handles_)
    {
        if (file && file->is_open())
        {
            file->close();
        }
    }
    file_handles_.clear();
}

void CsvWriter::flush_all()
{
    for (auto &[instrument_id, file] : file_handles_)
    {
        if (file && file->is_open())
        {
            file->flush();
        }
    }
}

std::ofstream *CsvWriter::get_or_create_file(const std::string &instrument_id)
{
    // Check if we already have a handle for this instrument
    auto it = file_handles_.find(instrument_id);
    if (it != file_handles_.end() && it->second && it->second->is_open())
    {
        return it->second.get();
    }

    // Create directory structure: base_path/trading_day/
    // std::filesystem::path trading_day_path = base_path_ / trading_day_;
    if (!std::filesystem::exists(base_path_))
    {
        std::error_code ec;
        std::filesystem::create_directories(base_path_, ec);
        if (ec)
        {
            spdlog::error("Failed to create directory {}: {}", base_path_.string(), ec.message());
            return nullptr;
        }
    }

    // File name format: instrumentID_tradingday.csv
    std::string filename = std::format("{}_{}.csv", instrument_id, trading_day_);
    std::filesystem::path file_path = base_path_ / filename;

    // Check if file exists to determine if we need CSV header
    bool need_header = !std::filesystem::exists(file_path);

    // Create new file handle
    auto file = std::make_unique<std::ofstream>(file_path, std::ios::app);
    if (!file->is_open())
    {
        spdlog::error("Failed to open file {}", file_path.string());
        return nullptr;
    }

    // Write CSV header if new file
    if (need_header)
    {
        write_csv_header(*file);
    }

    // Store the file handle
    std::ofstream *file_ptr = file.get();
    file_handles_[instrument_id] = std::move(file);

    spdlog::debug("Created CSV file for instrument {}: {}", instrument_id, file_path.string());

    return file_ptr;
}

void CsvWriter::write_csv_header(std::ofstream &file)
{
    file << "TradingDay,InstrumentID,ExchangeID,LastPrice,PreSettlementPrice,"
            "PreClosePrice,"
         << "PreOpenInterest,OpenPrice,HighestPrice,LowestPrice,Volume,Turnover,"
            "OpenInterest,"
         << "ClosePrice,SettlementPrice,UpperLimitPrice,LowerLimitPrice,PreDelta,"
            "CurrDelta,"
         << "UpdateTime,UpdateMillisec,BidPrice1,BidVolume1,AskPrice1,AskVolume1,"
         << "BidPrice2,BidVolume2,AskPrice2,AskVolume2,BidPrice3,BidVolume3,"
            "AskPrice3,AskVolume3,"
         << "BidPrice4,BidVolume4,AskPrice4,AskVolume4,BidPrice5,BidVolume5,"
            "AskPrice5,AskVolume5,"
         << "AveragePrice,ActionDay" << std::endl;
}

} // namespace cfmdc
