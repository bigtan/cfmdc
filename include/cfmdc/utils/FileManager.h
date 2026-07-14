#pragma once

#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

#include "IMarketDataWriter.h"
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
                              const std::string &next_action_day, const std::string &startup_time_hms, StorageMode mode,
                              int worker_core = -1,
                              std::chrono::milliseconds csv_flush_interval = std::chrono::seconds(5));

    /// @brief Destructor
    ~AsyncFileManager();

    // Non-copyable and non-movable due to std::atomic members in queue
    AsyncFileManager(const AsyncFileManager &) = delete;
    AsyncFileManager &operator=(const AsyncFileManager &) = delete;
    AsyncFileManager(AsyncFileManager &&) = delete;
    AsyncFileManager &operator=(AsyncFileManager &&) = delete;

    /// @brief Async write market data (lock-free, high-performance)
    /// @param data Market data to write
    /// @return true if enqueued successfully, false if queue is full
    bool write_market_data_async(const CThostFtdcDepthMarketDataField &data);

    /// @brief Stop the worker thread
    void stop();

    /// @brief Close all open files
    void close_all();

    /// @brief Flush all open files
    /// @return true if every configured writer flushed successfully
    bool flush_all();

    /// @brief Check whether the pipeline stopped because storage failed
    bool has_fatal_error() const noexcept
    {
        return fatal_error_.load(std::memory_order_acquire);
    }

    /// @brief Get current storage mode
    StorageMode storage_mode() const
    {
        return storage_mode_;
    }

    /// @brief Pipeline statistics (all fields safe to read from any thread)
    struct Statistics
    {
        size_t total_records{0};   ///< Records written to storage
        size_t queue_size{0};      ///< Current queue depth
        size_t dropped_records{0}; ///< Records dropped because the queue was full
        size_t write_failures{0};  ///< Writer-level write failures
        size_t periodic_flushes{0}; ///< Successful periodic CSV flushes
    };
    Statistics get_statistics() const;

  private:
    void worker_loop(std::stop_token st);

    bool process_market_data(CThostFtdcDepthMarketDataField &data) const;
    bool write_processed_market_data(const CThostFtdcDepthMarketDataField &data);
    bool commit_processed_market_data(const CThostFtdcDepthMarketDataField &data);
    void mark_write_failure(std::string_view operation, std::string_view instrument_id = {});

    // Configuration
    std::filesystem::path csv_path_;
    std::filesystem::path parquet_path_;
    std::string trading_day_;
    std::string action_day_base_;
    std::string action_day_next_;
    std::string startup_time_hms_;
    market_data_time::StartupWindow startup_window_{market_data_time::StartupWindow::Day};
    StorageMode storage_mode_;
    std::chrono::milliseconds csv_flush_interval_;

    std::unique_ptr<CsvWriter> csv_writer_;
#ifdef CFMDC_ENABLE_PARQUET
    std::unique_ptr<ParquetBatchWriter> parquet_writer_;
#endif
    // Storage writers
    std::vector<IMarketDataWriter *> writers_;

    // Lock-free queue. CN futures peak at ~1000 ticks per 500ms snapshot slice,
    // so 16384 slots cover several seconds of full-market backlog.
    static constexpr size_t LOCKFREE_QUEUE_SIZE = 16384; // Must be power of 2
    LockFreeQueue<CThostFtdcDepthMarketDataField, LOCKFREE_QUEUE_SIZE> queue_;

    // Worker thread
    std::jthread worker_thread_;

    // Statistics
    std::atomic<size_t> total_records_{0};
    std::atomic<size_t> write_failures_{0};
    std::atomic<size_t> periodic_flushes_{0};
    std::atomic<bool> fatal_error_{false};
};

} // namespace cfmdc
