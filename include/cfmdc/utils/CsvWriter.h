#pragma once

#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <unordered_map>

#include "ThostFtdcUserApiStruct.h"

namespace cfmdc
{

/// @brief CSV writer for market data
/// @details Manages CSV file handles and writes market data in CSV format
/// @thread_safety Not thread-safe. Should be used from a single thread or with
/// external synchronization
class CsvWriter
{
  public:
    /// @brief Create CSV writer
    /// @param base_path Base directory for CSV files
    /// @param trading_day Trading day string
    explicit CsvWriter(const std::filesystem::path &base_path, const std::string &trading_day);

    /// @brief Destructor - closes all open files
    ~CsvWriter();

    // Non-copyable but movable
    CsvWriter(const CsvWriter &) = delete;
    CsvWriter &operator=(const CsvWriter &) = delete;
    CsvWriter(CsvWriter &&) = default;
    CsvWriter &operator=(CsvWriter &&) = default;

    /// @brief Write market data to CSV file
    /// @param data Market data to write
    /// @return true if written successfully, false otherwise
    bool write(const CThostFtdcDepthMarketDataField &data);

    /// @brief Close all open files
    void close_all();

    /// @brief Flush all open files
    void flush_all();

    /// @brief Get number of open CSV files
    size_t file_count() const
    {
        return file_handles_.size();
    }

  private:
    /// @brief Get or create file handle for an instrument
    /// @param instrument_id Instrument identifier
    /// @return Pointer to ofstream, or nullptr if failed
    std::ofstream *get_or_create_file(const std::string &instrument_id);

    /// @brief Write CSV header to file
    /// @param file Output stream to write header to
    void write_csv_header(std::ofstream &file);

    // Configuration
    std::filesystem::path base_path_;
    std::string trading_day_;

    // File handles
    std::unordered_map<std::string, std::unique_ptr<std::ofstream>> file_handles_;
};

} // namespace cfmdc
