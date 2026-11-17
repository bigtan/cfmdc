#include "cfmdc/utils/CsvWriter.h"

#include <spdlog/spdlog.h>

#include <charconv>
#include <cstdint>
#include <format>
#include <system_error>

#include "cfmdc/utils/Error.h"
#include "cfmdc/utils/Helpers.h"

namespace cfmdc
{

namespace
{

std::filesystem::path normalize_base_path(const std::filesystem::path &base_path)
{
    auto normalized = base_path.lexically_normal();
    if (normalized.filename().empty() && normalized != normalized.root_path())
    {
        normalized = normalized.parent_path();
    }
    return normalized;
}

void append_int32(std::string &out, int32_t value)
{
    char buf[16];
    const auto [ptr, ec] = std::to_chars(buf, buf + sizeof(buf), value);
    if (ec == std::errc{})
    {
        out.append(buf, static_cast<size_t>(ptr - buf));
    }
}

void append_ctp_double(std::string &out, double value)
{
    if (!is_valid_ctp_price(value))
    {
        return;
    }
    if (value == 0.0)
    {
        value = 0.0;
    }
    char buf[64];
    const auto [ptr, ec] = std::to_chars(buf, buf + sizeof(buf), value);
    if (ec == std::errc{})
    {
        out.append(buf, static_cast<size_t>(ptr - buf));
    }
}

} // namespace

CsvWriter::CsvWriter(const std::filesystem::path &base_path, const std::string &trading_day)
    : base_path_(normalize_base_path(base_path)), trading_day_(trading_day)
{
    if (!is_valid_trading_day(trading_day_))
    {
        throw FileException(std::format("Invalid trading day '{}': expected 8 digits (YYYYMMDD)", trading_day_));
    }

    // Reserve space for a typical CSV line (~500 bytes) to avoid reallocations
    csv_buffer_.reserve(1024);
    spdlog::info("CsvWriter initialized: base_path={}, trading_day={}", base_path_.string(), trading_day_);
}

CsvWriter::~CsvWriter()
{
    if (!close_all())
    {
        spdlog::critical("Failed to close one or more CSV files");
    }
}

bool CsvWriter::write(const MarketTick &tick)
{
    const std::string_view instrument_id = bounded_sv(tick.instrument_id);
    FileEntry *entry = get_or_create_file(instrument_id);

    if (!entry || !entry->stream || !entry->stream->is_open())
    {
        return false;
    }

    // Clear and reuse buffer to minimize allocations
    csv_buffer_.clear();

    const auto append_field_sv = [this](std::string_view sv) {
        csv_buffer_.push_back(',');
        csv_buffer_.append(sv);
    };
    const auto append_field_double = [this](double v) {
        csv_buffer_.push_back(',');
        append_ctp_double(csv_buffer_, v);
    };
    const auto append_field_int = [this](int32_t v) {
        csv_buffer_.push_back(',');
        append_int32(csv_buffer_, v);
    };

    csv_buffer_.append(bounded_sv(tick.trading_day));
    append_field_sv(instrument_id);
    append_field_sv(bounded_sv(tick.exchange_id));
    append_field_double(tick.last_price);
    append_field_double(tick.pre_settlement_price);
    append_field_double(tick.pre_close_price);
    append_field_double(tick.pre_open_interest);
    append_field_double(tick.open_price);
    append_field_double(tick.highest_price);
    append_field_double(tick.lowest_price);
    append_field_int(tick.volume);
    append_field_double(tick.turnover);
    append_field_double(tick.open_interest);
    append_field_double(tick.close_price);
    append_field_double(tick.settlement_price);
    append_field_double(tick.upper_limit_price);
    append_field_double(tick.lower_limit_price);
    append_field_double(tick.pre_delta);
    append_field_double(tick.curr_delta);
    append_field_sv(bounded_sv(tick.update_time));
    append_field_int(tick.update_millisec);

    for (size_t i = 0; i < 5; ++i)
    {
        append_field_double(tick.bid_price[i]);
        append_field_int(tick.bid_volume[i]);
        append_field_double(tick.ask_price[i]);
        append_field_int(tick.ask_volume[i]);
    }

    append_field_double(tick.average_price);
    append_field_sv(bounded_sv(tick.action_day));
    csv_buffer_.push_back('\n');

    entry->stream->write(csv_buffer_.data(), static_cast<std::streamsize>(csv_buffer_.size()));
    entry->dirty = true;
    // Surface stream errors (disk full, closed handle) instead of silently dropping data
    return entry->stream->good();
}

bool CsvWriter::close_all()
{
    bool success = true;
    for (auto &[instrument_id, entry] : file_handles_)
    {
        if (entry.stream && entry.stream->is_open())
        {
            entry.stream->close();
            entry.dirty = false;
            success = entry.stream->good() && success;
        }
    }
    file_handles_.clear();
    return success;
}

bool CsvWriter::flush()
{
    bool success = true;
    for (auto &[instrument_id, entry] : file_handles_)
    {
        if (entry.stream && entry.stream->is_open() && entry.dirty)
        {
            entry.stream->flush();
            entry.dirty = false;
            success = entry.stream->good() && success;
        }
    }
    return success;
}

size_t CsvWriter::dirty_file_count() const noexcept
{
    size_t count = 0;
    for (const auto &[instrument_id, entry] : file_handles_)
    {
        if (entry.dirty)
        {
            ++count;
        }
    }
    return count;
}

CsvWriter::FileEntry *CsvWriter::get_or_create_file(std::string_view instrument_id)
{
    // Check if we already have a handle for this instrument
    auto it = file_handles_.find(instrument_id);
    if (it != file_handles_.end() && it->second.stream && it->second.stream->is_open())
    {
        return &it->second;
    }

    if (!is_safe_instrument_id(instrument_id))
    {
        spdlog::warn("Rejected unsafe instrument ID in CsvWriter: '{}'", instrument_id);
        return nullptr;
    }

    if (!base_dir_ensured_)
    {
        if (!base_path_.empty())
        {
            std::error_code ec;
            std::filesystem::create_directories(base_path_, ec);
            if (ec)
            {
                spdlog::error("Failed to create directory {}: {}", base_path_.string(), ec.message());
                return nullptr;
            }
        }
        base_dir_ensured_ = true;
    }

    // File name format: instrumentID_tradingday.csv
    std::string filename = std::format("{}_{}.csv", instrument_id, trading_day_);
    std::filesystem::path file_path = base_path_ / filename;
    if (file_path.parent_path() != base_path_)
    {
        spdlog::error("CSV path {} escaped base directory {}", file_path.string(), base_path_.string());
        return nullptr;
    }

    // Check if file exists to determine if we need CSV header
    std::error_code exists_ec;
    const bool file_exists = std::filesystem::exists(file_path, exists_ec);
    const bool need_header = !exists_ec && !file_exists;

    // Attach 32 KiB stream buffer before opening file so all writes use the larger buffer
    auto buffer = std::make_unique<char[]>(kStreamBufferSize);
    auto file = std::make_unique<std::ofstream>();
    file->rdbuf()->pubsetbuf(buffer.get(), static_cast<std::streamsize>(kStreamBufferSize));
    file->open(file_path, std::ios::app);
    if (!file->is_open())
    {
        spdlog::error("Failed to open file {}", file_path.string());
        return nullptr;
    }

    // Write CSV header if new file
    if (need_header && !write_csv_header(*file))
    {
        spdlog::error("Failed to write CSV header to {}", file_path.string());
        return nullptr;
    }

    // Store the file handle (buffer moved alongside stream in FileEntry)
    auto [emplaced_it, inserted] = file_handles_.insert_or_assign(
        std::string(instrument_id), FileEntry{std::move(buffer), std::move(file), need_header});

    spdlog::debug("Created CSV file for instrument {}: {}", instrument_id, file_path.string());

    return &emplaced_it->second;
}

bool CsvWriter::write_csv_header(std::ofstream &file)
{
    static constexpr std::string_view kCsvHeader =
        "TradingDay,InstrumentID,ExchangeID,LastPrice,PreSettlementPrice,"
        "PreClosePrice,PreOpenInterest,OpenPrice,HighestPrice,LowestPrice,Volume,Turnover,"
        "OpenInterest,ClosePrice,SettlementPrice,UpperLimitPrice,LowerLimitPrice,PreDelta,"
        "CurrDelta,UpdateTime,UpdateMillisec,BidPrice1,BidVolume1,AskPrice1,AskVolume1,"
        "BidPrice2,BidVolume2,AskPrice2,AskVolume2,BidPrice3,BidVolume3,AskPrice3,AskVolume3,"
        "BidPrice4,BidVolume4,AskPrice4,AskVolume4,BidPrice5,BidVolume5,AskPrice5,AskVolume5,"
        "AveragePrice,ActionDay\n";
    file.write(kCsvHeader.data(), static_cast<std::streamsize>(kCsvHeader.size()));
    return file.good();
}

} // namespace cfmdc
