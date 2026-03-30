#pragma once

#include <atomic>
#include <filesystem>
#include <memory>
#include <string>
#include <thread>

#include "ThostFtdcUserApiStruct.h"
#include "cfmdc/types/StorageMode.h"
#include "cfmdc/utils/CsvWriter.h"
#include "cfmdc/utils/LockFreeQueue.h"
#include "cfmdc/utils/MarketDataTimePolicy.h"

#ifdef CFMDC_ENABLE_PARQUET
#include "cfmdc/utils/ParquetWriter.h"
#endif

namespace cfmdc
{

/// @brief High-performance async file manager for market data
/// @details Uses lock-free queue and background thread for async I/O
/// @thread_safety Thread-safe. Write operations are async and lock-free.
class AsyncFileManager
{
  public:
    /// @brief Constructor
    /// @param csv_path Path for CSV output
    /// @param parquet_path Path for Parquet output
    /// @param trading_day Current trading day
    /// @param base_action_day Startup physical date
    /// @param next_action_day Next physical date
    /// @param startup_time_hms Local startup time in HH:MM:SS (used for UpdateTime filtering)
    /// @param mode Storage mode
    explicit AsyncFileManager(const std::filesystem::path &csv_path, const std::filesystem::path &parquet_path,
                              const std::string &trading_day, const std::string &base_action_day,
                              const std::string &next_action_day, const std::string &startup_time_hms,
                              StorageMode mode);

    /// @brief Destructor
    ~AsyncFileManager();

    // Non-copyable but movable
    AsyncFileManager(const AsyncFileManager &) = delete;
    AsyncFileManager &operator=(const AsyncFileManager &) = delete;
    AsyncFileManager(AsyncFileManager &&) = default;
    AsyncFileManager &operator=(AsyncFileManager &&) = default;

    /// @brief Async write market data (lock-free, high-performance)
    /// @param data Market data to write
    /// @return true if enqueued successfully, false if queue is full
    bool write_market_data_async(const CThostFtdcDepthMarketDataField &data);

    /// @brief Synchronous write (fallback method)
    /// @param data Market data to write
    void write_market_data_sync(const CThostFtdcDepthMarketDataField &data);

    /// @brief Stop the worker thread
    void stop();

    /// @brief Close all open files
    void close_all();

    /// @brief Flush all open files
    void flush_all();

    /// @brief Get current storage mode
    StorageMode storage_mode() const
    {
        return storage_mode_;
    }

    /// @brief Get statistics
    struct Statistics
    {
        size_t total_records{0};
        size_t csv_files{0};
        size_t parquet_files{0};
        size_t queue_size{0};
    };
    Statistics get_statistics() const;

  private:
    void worker_loop();
    void write_market_data_to_csv(const CThostFtdcDepthMarketDataField &data);
    void write_market_data_to_parquet(const CThostFtdcDepthMarketDataField &data);

    bool process_market_data(CThostFtdcDepthMarketDataField &data) const;
    void write_processed_market_data(const CThostFtdcDepthMarketDataField &data);
    void commit_processed_market_data(const CThostFtdcDepthMarketDataField &data);

    // Configuration
    std::filesystem::path csv_path_;
    std::filesystem::path parquet_path_;
    std::string trading_day_;
    std::string action_day_base_;
    std::string action_day_next_;
    std::string startup_time_hms_;
    market_data_time::StartupWindow startup_window_{market_data_time::StartupWindow::Day};
    StorageMode storage_mode_;

    std::unique_ptr<CsvWriter> csv_writer_;
#ifdef CFMDC_ENABLE_PARQUET
    std::unique_ptr<ParquetBatchWriter> parquet_writer_;
#endif
    // Lock-free queue
    static constexpr size_t LOCKFREE_QUEUE_SIZE = 16384; // Must be power of 2
    LockFreeQueue<CThostFtdcDepthMarketDataField, LOCKFREE_QUEUE_SIZE> queue_;

    // Worker thread
    std::thread worker_thread_;
    std::atomic<bool> stop_flag_;

    // Statistics
    std::atomic<size_t> total_records_{0};
};

} // namespace cfmdc
