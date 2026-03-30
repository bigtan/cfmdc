#include "cfmdc/utils/FileManager.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>

#include "cfmdc/utils/Helpers.h"
#include "cfmdc/utils/MarketDataTimePolicy.h"

namespace cfmdc
{

AsyncFileManager::AsyncFileManager(const std::filesystem::path &csv_path, const std::filesystem::path &parquet_path,
                                   const std::string &trading_day, const std::string &base_action_day,
                                   const std::string &next_action_day, const std::string &startup_time_hms,
                                   StorageMode mode)
    : csv_path_(csv_path), parquet_path_(parquet_path), trading_day_(trading_day), action_day_base_(base_action_day),
      action_day_next_(next_action_day), storage_mode_(mode), stop_flag_(false)
{
    startup_time_hms_ = startup_time_hms;
    if (startup_time_hms_.empty())
    {
        startup_time_hms_ = "00:00:00";
    }

    startup_window_ = market_data_time::classify_startup_window(startup_time_hms_);

    // Initialize CSV writer if needed
    if (mode == StorageMode::CSV || mode == StorageMode::HYBRID)
    {
        csv_writer_ = std::make_unique<CsvWriter>(csv_path, trading_day);
        spdlog::info("CSV writer initialized at path: {}", csv_path.string());
    }

#ifdef CFMDC_ENABLE_PARQUET
    if (mode == StorageMode::PARQUET || mode == StorageMode::HYBRID)
    {
        ParquetMarketDataWriter::Config config;
        config.batch_size = 10000;
        config.compression = "zstd";  // Better compression than snappy
        config.compression_level = 3; // Balanced speed/compression
        config.row_group_size = 100000;

        parquet_writer_ = std::make_unique<ParquetBatchWriter>(parquet_path, trading_day, config);
        spdlog::info("Parquet writer initialized at path: {} with ZSTD compression", parquet_path.string());
    }
#else
    if (mode == StorageMode::PARQUET || mode == StorageMode::HYBRID)
    {
        spdlog::warn("Parquet storage requested but not enabled in build. Using CSV only.");
        storage_mode_ = StorageMode::CSV;
        if (!csv_writer_)
        {
            csv_writer_ = std::make_unique<CsvWriter>(csv_path, trading_day);
            spdlog::info("CSV writer initialized at path: {}", csv_path.string());
        }
    }
#endif

    const auto mode_str = to_string(storage_mode_);

    const char *window_str = market_data_time::to_string(startup_window_);
    spdlog::info("AsyncFileManager initialized: mode={}, csv_path={}, parquet_path={}, "
                 "trading_day={}, base_action_day={}, next_action_day={}",
                 mode_str, csv_path_.string(), parquet_path_.string(), trading_day_, action_day_base_,
                 action_day_next_);

    spdlog::info("AsyncFileManager UpdateTime filter configured: startup_time={}, window={}", startup_time_hms_,
                 window_str);

    worker_thread_ = std::thread(&AsyncFileManager::worker_loop, this);
}

AsyncFileManager::~AsyncFileManager()
{
    stop();
    close_all();
}

bool AsyncFileManager::write_market_data_async(const CThostFtdcDepthMarketDataField &data)
{
    // Enqueue only: no filtering or field rewriting here.
    return queue_.try_enqueue(data);
}

void AsyncFileManager::write_market_data_sync(const CThostFtdcDepthMarketDataField &data)
{
    auto copy = data;
    if (!process_market_data(copy))
    {
        return;
    }
    commit_processed_market_data(copy);
}

bool AsyncFileManager::process_market_data(CThostFtdcDepthMarketDataField &data) const
{
    // Filter first: do NOT rewrite TradingDay/ActionDay for dropped data.
    if (!market_data_time::should_store_by_update_time(startup_window_, data.UpdateTime))
    {
        return false;
    }

    // Override TradingDay with the one from TraderSpi (authoritative)
    safe_strcpy(data.TradingDay, trading_day_);

    // Calculate ActionDay based on UpdateTime
    if (market_data_time::should_use_next_action_day(data.UpdateTime))
    {
        safe_strcpy(data.ActionDay, action_day_next_);
    }
    else
    {
        safe_strcpy(data.ActionDay, action_day_base_);
    }

    return true;
}

void AsyncFileManager::write_processed_market_data(const CThostFtdcDepthMarketDataField &data)
{
    // Write based on storage mode
    if (storage_mode_ == StorageMode::CSV)
    {
        write_market_data_to_csv(data);
    }
    else if (storage_mode_ == StorageMode::PARQUET)
    {
        write_market_data_to_parquet(data);
    }
    else
    { // HYBRID
        write_market_data_to_csv(data);
        write_market_data_to_parquet(data);
    }
}

void AsyncFileManager::commit_processed_market_data(const CThostFtdcDepthMarketDataField &data)
{
    write_processed_market_data(data);
    total_records_.fetch_add(1, std::memory_order_relaxed);
}

void AsyncFileManager::stop()
{
    stop_flag_.store(true, std::memory_order_release);
    if (worker_thread_.joinable())
    {
        worker_thread_.join();
    }
}

void AsyncFileManager::close_all()
{
    if (csv_writer_)
    {
        csv_writer_->close_all();
    }

#ifdef CFMDC_ENABLE_PARQUET
    if (parquet_writer_)
    {
        parquet_writer_->close_all();
    }
#endif
}

void AsyncFileManager::flush_all()
{
    if (csv_writer_)
    {
        csv_writer_->flush_all();
    }

#ifdef CFMDC_ENABLE_PARQUET
    if (parquet_writer_)
    {
        parquet_writer_->flush_all();
    }
#endif
}

AsyncFileManager::Statistics AsyncFileManager::get_statistics() const
{
    Statistics stats;
    stats.total_records = total_records_.load();
    stats.csv_files = csv_writer_ ? csv_writer_->file_count() : 0;
    stats.queue_size = queue_.size();

#ifdef CFMDC_ENABLE_PARQUET
    if (parquet_writer_)
    {
        stats.parquet_files = parquet_writer_->writer_count();
    }
#endif

    return stats;
}

void AsyncFileManager::worker_loop()
{
    CThostFtdcDepthMarketDataField data;
    using namespace std::chrono_literals;

    // Time-budgeted adaptive backoff with jitter to reduce CPU while keeping latency low.
    constexpr auto kSpinBudget = 8us;
    constexpr auto kYieldBudget = 80us;
    constexpr auto kSleepMin = 50us;
    constexpr auto kSleepMax = 2000us;
    constexpr int kJitterPercent = 10;

    using clock = std::chrono::steady_clock;
    auto spin_deadline = clock::now() + kSpinBudget;
    auto yield_deadline = spin_deadline + kYieldBudget;
    auto sleep_duration = kSleepMin;
    std::uint32_t jitter_state = 0x9e3779b9u;

    auto reset_backoff = [&]() {
        auto now = clock::now();
        spin_deadline = now + kSpinBudget;
        yield_deadline = spin_deadline + kYieldBudget;
        sleep_duration = kSleepMin;
    };

    auto jittered_sleep = [&](std::chrono::microseconds base) {
        jitter_state = jitter_state * 1664525u + 1013904223u;
        int jitter = static_cast<int>(jitter_state % (2 * kJitterPercent + 1)) - kJitterPercent;
        auto delta = std::chrono::microseconds(static_cast<int64_t>(base.count()) * jitter / 100);
        auto adjusted = base + delta;
        return adjusted < 1us ? 1us : adjusted;
    };

    auto process_data = [&](CThostFtdcDepthMarketDataField &data) {
        if (!process_market_data(data))
        {
            return;
        }
        commit_processed_market_data(data);
    };

    while (!stop_flag_.load(std::memory_order_acquire))
    {
        bool processed_any = false;
        while (queue_.try_dequeue(data))
        {
            process_data(data);
            processed_any = true;
        }

        if (processed_any)
        {
            reset_backoff();
            continue;
        }

        auto now = clock::now();
        if (now < spin_deadline)
        {
            std::atomic_signal_fence(std::memory_order_acquire);
            continue;
        }

        if (now < yield_deadline)
        {
            std::this_thread::yield();
            continue;
        }

        std::this_thread::sleep_for(jittered_sleep(sleep_duration));
        sleep_duration = std::min(sleep_duration * 2, kSleepMax);
    }
    // Process remaining items
    while (queue_.try_dequeue(data))
    {
        process_data(data);
    }
}

void AsyncFileManager::write_market_data_to_csv(const CThostFtdcDepthMarketDataField &data)
{
    if (csv_writer_)
    {
        csv_writer_->write(data);
    }
}

void AsyncFileManager::write_market_data_to_parquet(const CThostFtdcDepthMarketDataField &data)
{
#ifdef CFMDC_ENABLE_PARQUET
    if (parquet_writer_)
    {
        parquet_writer_->write(data);
    }
#else
    (void)data; // Suppress unused parameter warning
#endif
}

} // namespace cfmdc
