#include "cfmdc/utils/ParquetWriter.h"

#ifdef CFMDC_ENABLE_PARQUET

#include <arrow/api.h>
#include <arrow/compute/api.h>
#include <arrow/io/file.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>
#include <parquet/exception.h>
#include <spdlog/spdlog.h>

#include <chrono>
#include <format>

#include "cfmdc/utils/Helpers.h"

namespace cfmdc
{

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

    // Accumulated tables for forming row groups
    std::vector<std::shared_ptr<arrow::Table>> accumulated_tables;
    size_t accumulated_rows{0};
};

ParquetMarketDataWriter::ParquetMarketDataWriter(const std::filesystem::path &file_path, const Config &config)
    : impl_(std::make_unique<Impl>()), config_(config)
{
    try
    {
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

        spdlog::info("Parquet writer created: {}, compression: {}, batch_size: {}", file_path.string(),
                     config_.compression, config_.batch_size);
    }
    catch (const std::exception &ex)
    {
        spdlog::error("Failed to create Parquet writer: {}", ex.what());
        throw;
    }
}

ParquetMarketDataWriter::~ParquetMarketDataWriter()
{
    try
    {
        flush();
        if (impl_->writer)
        {
            auto status = impl_->writer->Close();
            if (!status.ok())
            {
                spdlog::error("Failed to close Parquet writer: {}", status.ToString());
            }
        }
        if (impl_->outfile)
        {
            auto status = impl_->outfile->Close();
            if (!status.ok())
            {
                spdlog::error("Failed to close output file: {}", status.ToString());
            }
        }
    }
    catch (const std::exception &ex)
    {
        spdlog::error("Error in Parquet writer destructor: {}", ex.what());
    }
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

bool ParquetMarketDataWriter::write(const CThostFtdcDepthMarketDataField &data)
{
    try
    {
        append_to_buffer(data);
        record_count_++;
        buffer_index_++;

        // Flush when buffer is full
        if (buffer_index_ >= config_.batch_size)
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

void ParquetMarketDataWriter::append_to_buffer(const CThostFtdcDepthMarketDataField &data)
{
    // Append to each builder
    auto status = impl_->trading_day_builder.Append(data.TradingDay);
    if (!status.ok())
        throw std::runtime_error("Failed to append TradingDay");

    status = impl_->instrument_id_builder.Append(data.InstrumentID);
    if (!status.ok())
        throw std::runtime_error("Failed to append InstrumentID");

    status = impl_->exchange_id_builder.Append(data.ExchangeID);
    if (!status.ok())
        throw std::runtime_error("Failed to append ExchangeID");

    status = impl_->last_price_builder.Append(clean_price(data.LastPrice));
    if (!status.ok())
        throw std::runtime_error("Failed to append LastPrice");

    status = impl_->pre_settlement_price_builder.Append(clean_price(data.PreSettlementPrice));
    if (!status.ok())
        throw std::runtime_error("Failed to append PreSettlementPrice");

    status = impl_->pre_close_price_builder.Append(clean_price(data.PreClosePrice));
    if (!status.ok())
        throw std::runtime_error("Failed to append PreClosePrice");

    status = impl_->pre_open_interest_builder.Append(data.PreOpenInterest);
    if (!status.ok())
        throw std::runtime_error("Failed to append PreOpenInterest");

    status = impl_->open_price_builder.Append(clean_price(data.OpenPrice));
    if (!status.ok())
        throw std::runtime_error("Failed to append OpenPrice");

    status = impl_->highest_price_builder.Append(clean_price(data.HighestPrice));
    if (!status.ok())
        throw std::runtime_error("Failed to append HighestPrice");

    status = impl_->lowest_price_builder.Append(clean_price(data.LowestPrice));
    if (!status.ok())
        throw std::runtime_error("Failed to append LowestPrice");

    status = impl_->volume_builder.Append(data.Volume);
    if (!status.ok())
        throw std::runtime_error("Failed to append Volume");

    status = impl_->turnover_builder.Append(clean_price(data.Turnover));
    if (!status.ok())
        throw std::runtime_error("Failed to append Turnover");

    status = impl_->open_interest_builder.Append(data.OpenInterest);
    if (!status.ok())
        throw std::runtime_error("Failed to append OpenInterest");

    status = impl_->close_price_builder.Append(clean_price(data.ClosePrice));
    if (!status.ok())
        throw std::runtime_error("Failed to append ClosePrice");

    status = impl_->settlement_price_builder.Append(clean_price(data.SettlementPrice));
    if (!status.ok())
        throw std::runtime_error("Failed to append SettlementPrice");

    status = impl_->upper_limit_price_builder.Append(clean_price(data.UpperLimitPrice));
    if (!status.ok())
        throw std::runtime_error("Failed to append UpperLimitPrice");

    status = impl_->lower_limit_price_builder.Append(clean_price(data.LowerLimitPrice));
    if (!status.ok())
        throw std::runtime_error("Failed to append LowerLimitPrice");

    status = impl_->pre_delta_builder.Append(clean_price(data.PreDelta));
    if (!status.ok())
        throw std::runtime_error("Failed to append PreDelta");

    status = impl_->curr_delta_builder.Append(clean_price(data.CurrDelta));
    if (!status.ok())
        throw std::runtime_error("Failed to append CurrDelta");

    status = impl_->update_time_builder.Append(data.UpdateTime);
    if (!status.ok())
        throw std::runtime_error("Failed to append UpdateTime");

    status = impl_->update_millisec_builder.Append(data.UpdateMillisec);
    if (!status.ok())
        throw std::runtime_error("Failed to append UpdateMillisec");

    // Five levels of bid/ask
    const double *bid_prices[] = {&data.BidPrice1, &data.BidPrice2, &data.BidPrice3, &data.BidPrice4, &data.BidPrice5};
    const int *bid_volumes[] = {&data.BidVolume1, &data.BidVolume2, &data.BidVolume3, &data.BidVolume4,
                                &data.BidVolume5};
    const double *ask_prices[] = {&data.AskPrice1, &data.AskPrice2, &data.AskPrice3, &data.AskPrice4, &data.AskPrice5};
    const int *ask_volumes[] = {&data.AskVolume1, &data.AskVolume2, &data.AskVolume3, &data.AskVolume4,
                                &data.AskVolume5};

    for (int i = 0; i < 5; ++i)
    {
        status = impl_->bid_price_builders[i].Append(clean_price(*bid_prices[i]));
        if (!status.ok())
            throw std::runtime_error("Failed to append BidPrice");

        status = impl_->bid_volume_builders[i].Append(*bid_volumes[i]);
        if (!status.ok())
            throw std::runtime_error("Failed to append BidVolume");

        status = impl_->ask_price_builders[i].Append(clean_price(*ask_prices[i]));
        if (!status.ok())
            throw std::runtime_error("Failed to append AskPrice");

        status = impl_->ask_volume_builders[i].Append(*ask_volumes[i]);
        if (!status.ok())
            throw std::runtime_error("Failed to append AskVolume");
    }

    status = impl_->average_price_builder.Append(clean_price(data.AveragePrice));
    if (!status.ok())
        throw std::runtime_error("Failed to append AveragePrice");

    status = impl_->action_day_builder.Append(data.ActionDay);
    if (!status.ok())
        throw std::runtime_error("Failed to append ActionDay");
}

void ParquetMarketDataWriter::write_accumulated_tables()
{
    if (impl_->accumulated_rows == 0)
    {
        return; // Nothing to write
    }

    try
    {
        // Concatenate all accumulated tables
        auto concat_result = arrow::ConcatenateTables(impl_->accumulated_tables);
        if (!concat_result.ok())
        {
            throw std::runtime_error("Failed to concatenate tables: " + concat_result.status().ToString());
        }
        auto combined_table = *concat_result;

        // Write the combined table as one row group
        auto status = impl_->writer->WriteTable(*combined_table, combined_table->num_rows());
        if (!status.ok())
        {
            throw std::runtime_error("Failed to write table: " + status.ToString());
        }

        spdlog::info("Wrote row group with {} records", combined_table->num_rows());

        // Clear accumulated tables
        impl_->accumulated_tables.clear();
        impl_->accumulated_rows = 0;
    }
    catch (const std::exception &ex)
    {
        spdlog::error("Failed to write accumulated tables: {}", ex.what());
        throw;
    }
}

void ParquetMarketDataWriter::flush_buffer()
{
    if (buffer_index_ == 0)
    {
        return; // Nothing to flush
    }

    try
    {
        // Finish all builders and create arrays
        std::vector<std::shared_ptr<arrow::Array>> arrays;

        std::shared_ptr<arrow::Array> array;

#define FINISH_BUILDER(builder)                                                                                        \
    do                                                                                                                 \
    {                                                                                                                  \
        auto result = builder.Finish();                                                                                \
        if (!result.ok())                                                                                              \
        {                                                                                                              \
            throw std::runtime_error(result.status().ToString());                                                      \
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
        size_t batch_rows = arrays[0]->length();

        // Accumulate tables until we reach row_group_size
        impl_->accumulated_tables.push_back(table);
        impl_->accumulated_rows += batch_rows;

        spdlog::debug("Accumulated {} records (total: {} / {})", batch_rows, impl_->accumulated_rows,
                      config_.row_group_size);

        // Write when accumulated rows reach row_group_size
        if (impl_->accumulated_rows >= config_.row_group_size)
        {
            write_accumulated_tables();
        }

        buffer_index_ = 0;
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

    // Write any remaining accumulated data
    write_accumulated_tables();
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
    try
    {
        // Create trading day directory: base_path / trading_day
        // trading_day_dir_ = base_path_ / trading_day_;

        // Ensure trading day directory exists
        if (!std::filesystem::exists(base_path_))
        {
            std::error_code ec;
            std::filesystem::create_directories(base_path_, ec);
            if (ec)
            {
                spdlog::error("Failed to create directory {}: {}", base_path_.string(), ec.message());
                throw std::runtime_error("Failed to create directory: " + ec.message());
            }
            spdlog::info("Created trading day directory: {}", base_path_.string());
        }

        // Generate timestamped filename for this session
        auto now = std::chrono::system_clock::now();
        auto time_t = std::chrono::system_clock::to_time_t(now);
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()) % 1000;

        std::tm tm;
#ifdef _WIN32
        localtime_s(&tm, &time_t);
#else
        localtime_r(&time_t, &tm);
#endif

        // Create timestamped file: YYYYMMDD_HHMMSS_mmm.parquet
        std::string filename =
            std::format("{:04d}{:02d}{:02d}_{:02d}{:02d}{:02d}_{:03d}.parquet", tm.tm_year + 1900, tm.tm_mon + 1,
                        tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec, static_cast<int>(ms.count()));

        // Full path: base_path / trading_day / timestamp.parquet
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
    close_all();
    spdlog::info("ParquetBatchWriter closed. Data saved to: {}", current_file_path_.string());
}

bool ParquetBatchWriter::write(const CThostFtdcDepthMarketDataField &data)
{
    try
    {
        if (writer_)
        {
            return writer_->write(data);
        }
        return false;
    }
    catch (const std::exception &ex)
    {
        spdlog::error("Failed to write market data: {}", ex.what());
        return false;
    }
}

void ParquetBatchWriter::flush_all()
{
    try
    {
        if (writer_)
        {
            writer_->flush();
        }
    }
    catch (const std::exception &ex)
    {
        spdlog::error("Failed to flush writer: {}", ex.what());
    }
}

void ParquetBatchWriter::close_all()
{
    writer_.reset(); // Destructor will close and flush
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
