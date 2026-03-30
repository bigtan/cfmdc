#pragma once

#ifdef CFMDC_ENABLE_PARQUET

#include <filesystem>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "ThostFtdcUserApiStruct.h"

namespace cfmdc
{

/// @brief High-performance Parquet writer for market data
/// @details Uses Apache Arrow/Parquet for columnar storage with compression
/// @thread_safety Not thread-safe. Should be used from single thread or with
/// external synchronization
class ParquetMarketDataWriter
{
  public:
    /// @brief Configuration for Parquet writer
    struct Config
    {
        size_t batch_size;       ///< Number of records to buffer before flush
        std::string compression; ///< Compression: snappy, gzip, zstd, lz4
        int compression_level;   ///< Compression level (-1 = default)
        size_t row_group_size;   ///< Rows per row group
        bool use_dictionary;     ///< Use dictionary encoding for strings
        int writer_version;      ///< Parquet version (1 or 2)

        /// @brief Constructor with default values
        Config()
            : batch_size(10000), compression("snappy"), compression_level(-1), row_group_size(200000),
              use_dictionary(true), writer_version(2)
        {
        }
    };

    /// @brief Create Parquet writer for a specific instrument
    /// @param file_path Output file path
    /// @param config Writer configuration
    explicit ParquetMarketDataWriter(const std::filesystem::path &file_path, const Config &config = Config{});

    /// @brief Destructor - flushes remaining data
    ~ParquetMarketDataWriter();

    // Non-copyable, movable
    ParquetMarketDataWriter(const ParquetMarketDataWriter &) = delete;
    ParquetMarketDataWriter &operator=(const ParquetMarketDataWriter &) = delete;
    ParquetMarketDataWriter(ParquetMarketDataWriter &&) = default;
    ParquetMarketDataWriter &operator=(ParquetMarketDataWriter &&) = default;

    /// @brief Write a single market data record
    /// @param data Market data to write
    /// @return true if successful, false otherwise
    bool write(const CThostFtdcDepthMarketDataField &data);

    /// @brief Flush buffered data to file
    void flush();

    /// @brief Get number of records written
    size_t record_count() const
    {
        return record_count_;
    }

    /// @brief Get file size in bytes
    size_t file_size() const;

  private:
    void initialize_schema();
    void append_to_buffer(const CThostFtdcDepthMarketDataField &data);
    void flush_buffer();
    void write_accumulated_tables();

    class Impl;
    std::unique_ptr<Impl> impl_;

    Config config_;
    size_t record_count_{0};
    size_t buffer_index_{0};
};

/// @brief Batch writer for multiple instruments
/// @details Writes all instruments to a single Parquet file
class ParquetBatchWriter
{
  public:
    /// @brief Create batch writer
    /// @param base_path Base directory for Parquet files
    /// @param trading_day Trading day string
    /// @param config Writer configuration
    explicit ParquetBatchWriter(const std::filesystem::path &base_path, const std::string &trading_day,
                                const ParquetMarketDataWriter::Config &config = ParquetMarketDataWriter::Config{});

    ~ParquetBatchWriter();

    // Non-copyable, movable
    ParquetBatchWriter(const ParquetBatchWriter &) = delete;
    ParquetBatchWriter &operator=(const ParquetBatchWriter &) = delete;
    ParquetBatchWriter(ParquetBatchWriter &&) = default;
    ParquetBatchWriter &operator=(ParquetBatchWriter &&) = default;

    /// @brief Write market data (all instruments to single file)
    /// @param data Market data to write
    /// @return true if successful, false otherwise
    bool write(const CThostFtdcDepthMarketDataField &data);

    /// @brief Flush writer
    void flush_all();

    /// @brief Close writer
    void close_all();

    /// @brief Get total records written
    size_t total_record_count() const;

    /// @brief Get number of active writers (always 1)
    size_t writer_count() const;

  private:
    std::filesystem::path base_path_; ///< Base directory for all data
    std::string trading_day_;         ///< Trading day string
    // std::filesystem::path                      trading_day_dir_;    ///<
    // Directory for this trading day
    ParquetMarketDataWriter::Config config_;
    std::unique_ptr<ParquetMarketDataWriter> writer_;
    std::filesystem::path current_file_path_; ///< Path to current timestamped file
};

} // namespace cfmdc

#endif // CFMDC_ENABLE_PARQUET
