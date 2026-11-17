#include "cfmdc/utils/ParquetWriter.h"

#ifdef CFMDC_ENABLE_PARQUET

#include <arrow/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <format>
#include <stdexcept>
#include <string_view>
#include <vector>

#include "cfmdc/utils/Error.h"
#include "cfmdc/utils/Helpers.h"
#include "cfmdc/utils/TimeUtils.h"

namespace cfmdc
{

namespace
{

inline void check_arrow_status(const arrow::Status &status)
{
    if (!status.ok())
    {
        throw std::runtime_error(status.ToString());
    }
}

inline void append_bounded_string(arrow::StringBuilder &builder, std::string_view sv)
{
    check_arrow_status(builder.Append(sv.data(), static_cast<int32_t>(sv.size())));
}

inline void append_ctp_double(arrow::DoubleBuilder &builder, double value)
{
    check_arrow_status(is_valid_ctp_price(value) ? builder.Append(value) : builder.AppendNull());
}

} // namespace

// Implementation class to hide Arrow/Parquet details
class ParquetMarketDataWriter::Impl
{
  public:
    std::shared_ptr<arrow::Schema> schema;
    std::shared_ptr<arrow::io::FileOutputStream> outfile;
    std::unique_ptr<parquet::arrow::FileWriter> writer;

    // Buffers for batch writing
    arrow::StringBuilder trading_day_builder;
    arrow::StringBuilder instrument_id_builder;
    arrow::StringBuilder exchange_id_builder;
    arrow::DoubleBuilder last_price_builder;
    arrow::DoubleBuilder pre_settlement_price_builder;
    arrow::DoubleBuilder pre_close_price_builder;
    arrow::DoubleBuilder pre_open_interest_builder;
    arrow::DoubleBuilder open_price_builder;
    arrow::DoubleBuilder highest_price_builder;
    arrow::DoubleBuilder lowest_price_builder;
    arrow::Int32Builder volume_builder;
    arrow::DoubleBuilder turnover_builder;
    arrow::DoubleBuilder open_interest_builder;
    arrow::DoubleBuilder close_price_builder;
    arrow::DoubleBuilder settlement_price_builder;
    arrow::DoubleBuilder upper_limit_price_builder;
    arrow::DoubleBuilder lower_limit_price_builder;
    arrow::DoubleBuilder pre_delta_builder;
    arrow::DoubleBuilder curr_delta_builder;
    arrow::StringBuilder update_time_builder;
    arrow::Int32Builder update_millisec_builder;

    // Bid/Ask arrays (5 levels each)
    arrow::DoubleBuilder bid_price_builders[5];
    arrow::Int32Builder bid_volume_builders[5];
    arrow::DoubleBuilder ask_price_builders[5];
    arrow::Int32Builder ask_volume_builders[5];

    arrow::DoubleBuilder average_price_builder;
    arrow::StringBuilder action_day_builder;

    // Row-group buffer. write() only copies rows here; Arrow builders are touched in flush_buffer().
    std::vector<MarketTick> row_buffer;
};

ParquetMarketDataWriter::ParquetMarketDataWriter(const std::filesystem::path &file_path, const Config &config)
    : impl_(std::make_unique<Impl>()), config_(config)
{
    try
    {
        impl_->row_buffer.reserve(std::max<size_t>(1, config_.row_group_size));

        // Create schema
        initialize_schema();

        // Open output file
        auto result = arrow::io::FileOutputStream::Open(file_path.string());
        if (!result.ok())
        {
            throw std::runtime_error("Failed to open file: " + result.status().ToString());
        }
        impl_->outfile = *result;

        // Configure Parquet properties
        parquet::WriterProperties::Builder props_builder;
        props_builder.max_row_group_length(config_.row_group_size);

        // Set compression
        parquet::Compression::type compression_type = parquet::Compression::SNAPPY;
        if (config_.compression == "gzip")
        {
            compression_type = parquet::Compression::GZIP;
        }
        else if (config_.compression == "zstd")
        {
            compression_type = parquet::Compression::ZSTD;
        }
        else if (config_.compression == "lz4")
        {
            compression_type = parquet::Compression::LZ4;
        }
        else if (config_.compression == "none")
        {
            compression_type = parquet::Compression::UNCOMPRESSED;
        }
        props_builder.compression(compression_type);

        if (config_.compression_level >= 0)
        {
            props_builder.compression_level(config_.compression_level);
        }

        if (config_.use_dictionary)
        {
            props_builder.enable_dictionary();
        }
        else
        {
            props_builder.disable_dictionary();
        }

        props_builder.version(config_.writer_version == 1 ? parquet::ParquetVersion::PARQUET_1_0
                                                          : parquet::ParquetVersion::PARQUET_2_6);

        auto props = props_builder.build();

        // Create Arrow writer properties
        auto arrow_props = parquet::ArrowWriterProperties::Builder().build();

        // Create the Parquet writer
        auto writer_result = parquet::arrow::FileWriter::Open(*impl_->schema, arrow::default_memory_pool(),
                                                              impl_->outfile, props, arrow_props);
        if (!writer_result.ok())
        {
            throw std::runtime_error("Failed to create Parquet writer: " + writer_result.status().ToString());
        }
        impl_->writer = std::move(*writer_result);

        spdlog::info("Parquet writer created: {}, compression: {}, row_group_size: {}", file_path.string(),
                     config_.compression, config_.row_group_size);
    }
    catch (const std::exception &ex)
    {
        spdlog::error("Failed to create Parquet writer: {}", ex.what());
        throw;
    }
}

ParquetMarketDataWriter::~ParquetMarketDataWriter()
{
    if (!close())
    {
        spdlog::critical("Parquet writer did not close cleanly");
    }
}

bool ParquetMarketDataWriter::close()
{
    if (closed_)
    {
        return true;
    }

    bool success = true;
    try
    {
        flush();
    }
    catch (const std::exception &ex)
    {
        spdlog::error("Failed to flush Parquet writer during close: {}", ex.what());
        success = false;
    }

    if (impl_->writer)
    {
        auto status = impl_->writer->Close();
        if (!status.ok())
        {
            spdlog::error("Failed to close Parquet writer: {}", status.ToString());
            success = false;
        }
        impl_->writer.reset();
    }
    if (impl_->outfile)
    {
        auto status = impl_->outfile->Close();
        if (!status.ok())
        {
            spdlog::error("Failed to close output file: {}", status.ToString());
            success = false;
        }
        impl_->outfile.reset();
    }
    closed_ = true;
    return success;
}

void ParquetMarketDataWriter::initialize_schema()
{
    // Define schema for market data
    std::vector<std::shared_ptr<arrow::Field>> fields;

    fields.push_back(arrow::field("TradingDay", arrow::utf8()));
    fields.push_back(arrow::field("InstrumentID", arrow::utf8()));
    fields.push_back(arrow::field("ExchangeID", arrow::utf8()));
    fields.push_back(arrow::field("LastPrice", arrow::float64()));
    fields.push_back(arrow::field("PreSettlementPrice", arrow::float64()));
    fields.push_back(arrow::field("PreClosePrice", arrow::float64()));
    fields.push_back(arrow::field("PreOpenInterest", arrow::float64()));
    fields.push_back(arrow::field("OpenPrice", arrow::float64()));
    fields.push_back(arrow::field("HighestPrice", arrow::float64()));
    fields.push_back(arrow::field("LowestPrice", arrow::float64()));
    fields.push_back(arrow::field("Volume", arrow::int32()));
    fields.push_back(arrow::field("Turnover", arrow::float64()));
    fields.push_back(arrow::field("OpenInterest", arrow::float64()));
    fields.push_back(arrow::field("ClosePrice", arrow::float64()));
    fields.push_back(arrow::field("SettlementPrice", arrow::float64()));
    fields.push_back(arrow::field("UpperLimitPrice", arrow::float64()));
    fields.push_back(arrow::field("LowerLimitPrice", arrow::float64()));
    fields.push_back(arrow::field("PreDelta", arrow::float64()));
    fields.push_back(arrow::field("CurrDelta", arrow::float64()));
    fields.push_back(arrow::field("UpdateTime", arrow::utf8()));
    fields.push_back(arrow::field("UpdateMillisec", arrow::int32()));

    // Five levels of bid/ask
    for (int i = 1; i <= 5; ++i)
    {
        fields.push_back(arrow::field("BidPrice" + std::to_string(i), arrow::float64()));
        fields.push_back(arrow::field("BidVolume" + std::to_string(i), arrow::int32()));
        fields.push_back(arrow::field("AskPrice" + std::to_string(i), arrow::float64()));
        fields.push_back(arrow::field("AskVolume" + std::to_string(i), arrow::int32()));
    }

    fields.push_back(arrow::field("AveragePrice", arrow::float64()));
    fields.push_back(arrow::field("ActionDay", arrow::utf8()));

    impl_->schema = arrow::schema(fields);
}

bool ParquetMarketDataWriter::write(const MarketTick &tick)
{
    try
    {
        append_to_buffer(tick);
        record_count_++;

        // Flush one row group at a time to avoid building and concatenating many small tables.
        if (impl_->row_buffer.size() >= std::max<size_t>(1, config_.row_group_size))
        {
            flush_buffer();
        }

        return true;
    }
    catch (const std::exception &ex)
    {
        spdlog::error("Failed to write market data: {}", ex.what());
        return false;
    }
}

void ParquetMarketDataWriter::append_to_buffer(const MarketTick &tick)
{
    impl_->row_buffer.push_back(tick);
}

void ParquetMarketDataWriter::flush_buffer()
{
    auto &rows = impl_->row_buffer;
    if (rows.empty())
    {
        return; // Nothing to flush
    }

    try
    {
        const auto row_count = static_cast<int64_t>(rows.size());

#define RESERVE_BUILDER(builder) check_arrow_status((builder).Reserve(row_count))
#define RESERVE_STRING_BUILDER(builder, bytes)                                                                         \
    do                                                                                                                 \
    {                                                                                                                  \
        check_arrow_status((builder).Reserve(row_count));                                                              \
        check_arrow_status((builder).ReserveData(bytes));                                                              \
    } while (0)

        RESERVE_STRING_BUILDER(impl_->trading_day_builder, row_count * 8);
        RESERVE_STRING_BUILDER(impl_->instrument_id_builder, row_count * 16);
        RESERVE_STRING_BUILDER(impl_->exchange_id_builder, row_count * 6);
        RESERVE_BUILDER(impl_->last_price_builder);
        RESERVE_BUILDER(impl_->pre_settlement_price_builder);
        RESERVE_BUILDER(impl_->pre_close_price_builder);
        RESERVE_BUILDER(impl_->pre_open_interest_builder);
        RESERVE_BUILDER(impl_->open_price_builder);
        RESERVE_BUILDER(impl_->highest_price_builder);
        RESERVE_BUILDER(impl_->lowest_price_builder);
        RESERVE_BUILDER(impl_->volume_builder);
        RESERVE_BUILDER(impl_->turnover_builder);
        RESERVE_BUILDER(impl_->open_interest_builder);
        RESERVE_BUILDER(impl_->close_price_builder);
        RESERVE_BUILDER(impl_->settlement_price_builder);
        RESERVE_BUILDER(impl_->upper_limit_price_builder);
        RESERVE_BUILDER(impl_->lower_limit_price_builder);
        RESERVE_BUILDER(impl_->pre_delta_builder);
        RESERVE_BUILDER(impl_->curr_delta_builder);
        RESERVE_STRING_BUILDER(impl_->update_time_builder, row_count * 8);
        RESERVE_BUILDER(impl_->update_millisec_builder);

        for (int i = 0; i < 5; ++i)
        {
            RESERVE_BUILDER(impl_->bid_price_builders[i]);
            RESERVE_BUILDER(impl_->bid_volume_builders[i]);
            RESERVE_BUILDER(impl_->ask_price_builders[i]);
            RESERVE_BUILDER(impl_->ask_volume_builders[i]);
        }

        RESERVE_BUILDER(impl_->average_price_builder);
        RESERVE_STRING_BUILDER(impl_->action_day_builder, row_count * 8);

#undef RESERVE_STRING_BUILDER
#undef RESERVE_BUILDER

        for (const auto &row : rows)
        {
            append_bounded_string(impl_->trading_day_builder, bounded_sv(row.trading_day));
            append_bounded_string(impl_->instrument_id_builder, bounded_sv(row.instrument_id));
            append_bounded_string(impl_->exchange_id_builder, bounded_sv(row.exchange_id));
            append_ctp_double(impl_->last_price_builder, row.last_price);
            append_ctp_double(impl_->pre_settlement_price_builder, row.pre_settlement_price);
            append_ctp_double(impl_->pre_close_price_builder, row.pre_close_price);
            append_ctp_double(impl_->pre_open_interest_builder, row.pre_open_interest);
            append_ctp_double(impl_->open_price_builder, row.open_price);
            append_ctp_double(impl_->highest_price_builder, row.highest_price);
            append_ctp_double(impl_->lowest_price_builder, row.lowest_price);
            impl_->volume_builder.UnsafeAppend(row.volume);
            append_ctp_double(impl_->turnover_builder, row.turnover);
            append_ctp_double(impl_->open_interest_builder, row.open_interest);
            append_ctp_double(impl_->close_price_builder, row.close_price);
            append_ctp_double(impl_->settlement_price_builder, row.settlement_price);
            append_ctp_double(impl_->upper_limit_price_builder, row.upper_limit_price);
            append_ctp_double(impl_->lower_limit_price_builder, row.lower_limit_price);
            append_ctp_double(impl_->pre_delta_builder, row.pre_delta);
            append_ctp_double(impl_->curr_delta_builder, row.curr_delta);
            append_bounded_string(impl_->update_time_builder, bounded_sv(row.update_time));
            impl_->update_millisec_builder.UnsafeAppend(row.update_millisec);

            for (size_t i = 0; i < 5; ++i)
            {
                append_ctp_double(impl_->bid_price_builders[i], row.bid_price[i]);
                impl_->bid_volume_builders[i].UnsafeAppend(row.bid_volume[i]);
                append_ctp_double(impl_->ask_price_builders[i], row.ask_price[i]);
                impl_->ask_volume_builders[i].UnsafeAppend(row.ask_volume[i]);
            }

            append_ctp_double(impl_->average_price_builder, row.average_price);
            append_bounded_string(impl_->action_day_builder, bounded_sv(row.action_day));
        }

        // Finish all builders and create arrays
        std::vector<std::shared_ptr<arrow::Array>> arrays;
        arrays.reserve(impl_->schema->num_fields());

#define FINISH_BUILDER(builder)                                                                                        \
    do                                                                                                                 \
    {                                                                                                                  \
        auto result = builder.Finish();                                                                                \
        if (!result.ok())                                                                                              \
        {                                                                                                              \
            throw std::runtime_error(result.status().ToString());                                                      \
        }                                                                                                              \
        if ((*result)->length() != row_count)                                                                          \
        {                                                                                                              \
            throw std::runtime_error("Unexpected Arrow array length after finishing builder");                         \
        }                                                                                                              \
        arrays.push_back(*result);                                                                                     \
    } while (0)

        FINISH_BUILDER(impl_->trading_day_builder);
        FINISH_BUILDER(impl_->instrument_id_builder);
        FINISH_BUILDER(impl_->exchange_id_builder);
        FINISH_BUILDER(impl_->last_price_builder);
        FINISH_BUILDER(impl_->pre_settlement_price_builder);
        FINISH_BUILDER(impl_->pre_close_price_builder);
        FINISH_BUILDER(impl_->pre_open_interest_builder);
        FINISH_BUILDER(impl_->open_price_builder);
        FINISH_BUILDER(impl_->highest_price_builder);
        FINISH_BUILDER(impl_->lowest_price_builder);
        FINISH_BUILDER(impl_->volume_builder);
        FINISH_BUILDER(impl_->turnover_builder);
        FINISH_BUILDER(impl_->open_interest_builder);
        FINISH_BUILDER(impl_->close_price_builder);
        FINISH_BUILDER(impl_->settlement_price_builder);
        FINISH_BUILDER(impl_->upper_limit_price_builder);
        FINISH_BUILDER(impl_->lower_limit_price_builder);
        FINISH_BUILDER(impl_->pre_delta_builder);
        FINISH_BUILDER(impl_->curr_delta_builder);
        FINISH_BUILDER(impl_->update_time_builder);
        FINISH_BUILDER(impl_->update_millisec_builder);

        for (int i = 0; i < 5; ++i)
        {
            FINISH_BUILDER(impl_->bid_price_builders[i]);
            FINISH_BUILDER(impl_->bid_volume_builders[i]);
            FINISH_BUILDER(impl_->ask_price_builders[i]);
            FINISH_BUILDER(impl_->ask_volume_builders[i]);
        }

        FINISH_BUILDER(impl_->average_price_builder);
        FINISH_BUILDER(impl_->action_day_builder);

#undef FINISH_BUILDER

        // Create table from arrays
        auto table = arrow::Table::Make(impl_->schema, arrays);

        auto status = impl_->writer->WriteTable(*table, table->num_rows());
        if (!status.ok())
        {
            throw std::runtime_error("Failed to write table: " + status.ToString());
        }

        spdlog::debug("Wrote row group with {} records", table->num_rows());

        rows.clear();
    }
    catch (const std::exception &ex)
    {
        spdlog::error("Failed to flush buffer: {}", ex.what());
        throw;
    }
}

void ParquetMarketDataWriter::flush()
{
    flush_buffer();
}

size_t ParquetMarketDataWriter::file_size() const
{
    try
    {
        if (impl_->outfile)
        {
            auto result = impl_->outfile->Tell();
            if (result.ok())
            {
                return static_cast<size_t>(*result);
            }
        }
    }
    catch (...)
    {
    }
    return 0;
}

// ============================================================================
// ParquetBatchWriter implementation
// ============================================================================

ParquetBatchWriter::ParquetBatchWriter(const std::filesystem::path &base_path, const std::string &trading_day,
                                       const ParquetMarketDataWriter::Config &config)
    : base_path_(base_path), trading_day_(trading_day), config_(config)
{
    if (!is_valid_trading_day(trading_day_))
    {
        throw FileException(std::format("Invalid trading day '{}': expected 8 digits (YYYYMMDD)", trading_day_));
    }

    try
    {
        if (!base_path_.empty())
        {
            std::error_code ec;
            const bool created = std::filesystem::create_directories(base_path_, ec);
            if (ec)
            {
                spdlog::error("Failed to create directory {}: {}", base_path_.string(), ec.message());
                throw std::runtime_error("Failed to create directory: " + ec.message());
            }
            if (created)
            {
                spdlog::info("Created trading day directory: {}", base_path_.string());
            }
        }

        // Generate timestamped filename for this session in fixed China Standard Time (UTC+8)
        const auto now = std::chrono::system_clock::now();
        const auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;
        const std::tm tm = time_utils::to_china_tm(now);

        // Create timestamped file: YYYYMMDD_HHMMSS_mmm.parquet
        std::string filename =
            std::format("{:04d}{:02d}{:02d}_{:02d}{:02d}{:02d}_{:03d}.parquet", tm.tm_year + 1900, tm.tm_mon + 1,
                        tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec, static_cast<int>(ms.count()));

        // Full path: base_path / timestamp.parquet
        current_file_path_ = base_path_ / filename;

        writer_ = std::make_unique<ParquetMarketDataWriter>(current_file_path_, config_);
        spdlog::info("Created Parquet writer in trading day folder: {}", current_file_path_.string());
        spdlog::info("Trading day: {}, all files will be stored in: {}", trading_day_, base_path_.string());
    }
    catch (const std::exception &ex)
    {
        spdlog::error("Failed to create Parquet writer: {}", ex.what());
        throw;
    }
}

ParquetBatchWriter::~ParquetBatchWriter()
{
    if (!close_all())
    {
        spdlog::critical("Parquet batch writer did not close cleanly");
    }
    spdlog::info("ParquetBatchWriter closed. Data saved to: {}", current_file_path_.string());
}

bool ParquetBatchWriter::write(const MarketTick &tick)
{
    try
    {
        if (writer_)
        {
            return writer_->write(tick);
        }
        return false;
    }
    catch (const std::exception &ex)
    {
        spdlog::error("Failed to write market data: {}", ex.what());
        return false;
    }
}

bool ParquetBatchWriter::flush()
{
    try
    {
        if (writer_)
        {
            writer_->flush();
        }
        return true;
    }
    catch (const std::exception &ex)
    {
        spdlog::error("Failed to flush writer: {}", ex.what());
        return false;
    }
}

bool ParquetBatchWriter::close_all()
{
    if (!writer_)
    {
        return true;
    }
    const bool success = writer_->close();
    writer_.reset();
    return success;
}

size_t ParquetBatchWriter::total_record_count() const
{
    if (writer_)
    {
        return writer_->record_count();
    }
    return 0;
}

size_t ParquetBatchWriter::writer_count() const
{
    return writer_ ? 1 : 0;
}

} // namespace cfmdc

#endif // CFMDC_ENABLE_PARQUET
