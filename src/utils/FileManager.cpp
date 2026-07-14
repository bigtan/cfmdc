#include "cfmdc/utils/FileManager.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iterator>

#include "cfmdc/utils/Helpers.h"
#include "cfmdc/utils/MarketDataTimePolicy.h"

namespace cfmdc
{

AsyncFileManager::AsyncFileManager(const std::filesystem::path &csv_path, const std::filesystem::path &parquet_path,
                                   const std::string &trading_day, const std::string &base_action_day,
                                   const std::string &next_action_day, const std::string &startup_time_hms,
                                   StorageMode mode, Options options)
    : csv_path_(csv_path), parquet_path_(parquet_path), trading_day_(trading_day), action_day_base_(base_action_day),
      action_day_next_(next_action_day), storage_mode_(mode),
      csv_flush_interval_(options.csv_flush_interval > std::chrono::milliseconds::zero() ? options.csv_flush_interval
                                                                                         : std::chrono::seconds(5))
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
        config.compression = "zstd";  // Better compression than snappy
        config.compression_level = 3; // Balanced speed/compression
        config.row_group_size = (std::max)(size_t{1}, options.parquet_row_group_size);

        parquet_writer_ = std::make_unique<ParquetBatchWriter>(parquet_path, trading_day, config);
        parquet_queue_ = std::make_unique<MarketDataQueue>();
        spdlog::info("Parquet writer initialized at path: {} with ZSTD compression and {} rows per group",
                     parquet_path.string(), config.row_group_size);
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

#ifdef CFMDC_ENABLE_PARQUET
    if (parquet_writer_)
    {
        parquet_worker_thread_ = std::jthread([this](std::stop_token st) { parquet_worker_loop(st); });
    }
#endif
    worker_thread_ = std::jthread([this](std::stop_token st) { worker_loop(st); });

    // Apply thread affinity
    int actual_core = options.worker_core;
    unsigned int hardware_threads = std::thread::hardware_concurrency();

    if (options.worker_core == -2)
    {
        // Auto-select core: skip core 0 if possible as it handles most interrupts
        if (hardware_threads > 1)
        {
            actual_core = static_cast<int>(hardware_threads - 1); // Use the last core
        }
        else
        {
            actual_core = -1; // Single core, no affinity
        }
    }
    else if (options.worker_core >= static_cast<int>(hardware_threads))
    {
        spdlog::warn("Configured WorkerThreadCore ({}) exceeds available hardware threads ({}), affinity disabled",
                     options.worker_core, hardware_threads);
        actual_core = -1;
    }

    if (actual_core >= 0)
    {
        if (set_thread_affinity(worker_thread_, actual_core))
        {
            spdlog::info("Worker thread pinned to CPU core: {} (mode: {})", actual_core,
                         options.worker_core == -2 ? "auto" : "manual");
        }
        else
        {
            spdlog::warn("Failed to pin worker thread to CPU core: {}", actual_core);
        }
    }
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

bool AsyncFileManager::write_processed_market_data(const CThostFtdcDepthMarketDataField &data)
{
    if (fatal_error_.load(std::memory_order_acquire))
    {
        return false;
    }

    if (csv_writer_)
    {
        if (!csv_writer_->write(data))
        {
            mark_write_failure("CSV write", data.InstrumentID);
            return false;
        }
        csv_records_.fetch_add(1, std::memory_order_relaxed);
    }

#ifdef CFMDC_ENABLE_PARQUET
    if (parquet_queue_)
    {
        while (!parquet_queue_->try_enqueue(data))
        {
            if (fatal_error_.load(std::memory_order_acquire))
            {
                return false;
            }
            std::this_thread::yield();
        }
    }
#endif
    return true;
}

bool AsyncFileManager::commit_processed_market_data(const CThostFtdcDepthMarketDataField &data)
{
    if (!write_processed_market_data(data))
    {
        return false;
    }
    return true;
}

void AsyncFileManager::mark_write_failure(std::string_view operation, std::string_view instrument_id)
{
    const auto failures = write_failures_.fetch_add(1, std::memory_order_relaxed) + 1;
    fatal_error_.store(true, std::memory_order_release);
    if (instrument_id.empty())
    {
        spdlog::critical("Market data storage {} failed (total failures: {}); stopping pipeline", operation, failures);
    }
    else
    {
        spdlog::critical("Market data storage {} failed for {} (total failures: {}); stopping pipeline", operation,
                         instrument_id, failures);
    }
}

void AsyncFileManager::stop()
{
    worker_thread_.request_stop();
    if (worker_thread_.joinable())
    {
        worker_thread_.join();
    }
#ifdef CFMDC_ENABLE_PARQUET
    parquet_worker_thread_.request_stop();
    if (parquet_worker_thread_.joinable())
    {
        parquet_worker_thread_.join();
    }
#endif
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

bool AsyncFileManager::flush_all()
{
    bool success = true;
    if (csv_writer_ && !csv_writer_->flush())
    {
        success = false;
        mark_write_failure("CSV flush");
    }
#ifdef CFMDC_ENABLE_PARQUET
    if (parquet_writer_ && !parquet_writer_->flush())
    {
        success = false;
        mark_write_failure("Parquet flush");
    }
#endif
    return success;
}

AsyncFileManager::Statistics AsyncFileManager::get_statistics() const
{
    // Reads atomics only, safe to call from any thread while the worker runs.
    Statistics stats;
    stats.csv_records = csv_records_.load(std::memory_order_relaxed);
    stats.parquet_records = parquet_records_.load(std::memory_order_relaxed);
    stats.queue_size = queue_.size();
#ifdef CFMDC_ENABLE_PARQUET
    stats.parquet_queue_size = parquet_queue_ ? parquet_queue_->size() : 0;
#endif
    stats.dropped_records = queue_.overflow_count();
    stats.write_failures = write_failures_.load(std::memory_order_relaxed);
    stats.periodic_flushes = periodic_flushes_.load(std::memory_order_relaxed);
    switch (storage_mode_)
    {
    case StorageMode::CSV:
        stats.total_records = stats.csv_records;
        break;
    case StorageMode::PARQUET:
        stats.total_records = stats.parquet_records;
        break;
    case StorageMode::HYBRID:
        stats.total_records = (std::min)(stats.csv_records, stats.parquet_records);
        break;
    }
    return stats;
}

void AsyncFileManager::worker_loop(std::stop_token st)
{
    using namespace std::chrono_literals;

    // Time-budgeted adaptive backoff with jitter to reduce CPU while keeping latency low.
    constexpr auto kSpinBudget = 8us;
    constexpr auto kYieldBudget = 80us;
    constexpr auto kSleepMin = 50us;
    constexpr auto kSleepMax = 2000us;
    constexpr int kJitterPercent = 10;
    constexpr size_t kBatchSize = 64;

    // Durability: bound CSV data lost on an abnormal exit (cheap ofstream flush).
    // Parquet is intentionally NOT flushed periodically: it would fragment row
    // groups, and without the file footer (written on close) the file is
    // unreadable anyway - Parquet durability relies on graceful shutdown.
    using clock = std::chrono::steady_clock;
    auto spin_deadline = clock::now() + kSpinBudget;
    auto yield_deadline = spin_deadline + kYieldBudget;
    auto sleep_duration = kSleepMin;
    auto next_csv_flush = clock::now() + csv_flush_interval_;
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

    auto flush_csv_if_due = [&](clock::time_point now) {
        if (!csv_writer_ || now < next_csv_flush)
        {
            return true;
        }
        if (!csv_writer_->flush())
        {
            mark_write_failure("periodic flush");
            return false;
        }
        periodic_flushes_.fetch_add(1, std::memory_order_relaxed);
        next_csv_flush = now + csv_flush_interval_;
        return true;
    };

    auto process_data = [&](CThostFtdcDepthMarketDataField &data) {
        if (!process_market_data(data))
        {
            return true;
        }
        return commit_processed_market_data(data);
    };

    std::array<CThostFtdcDepthMarketDataField, kBatchSize> batch;

    while (!st.stop_requested())
    {
        size_t count = 0;
        while (count < kBatchSize && queue_.try_dequeue(batch[count]))
        {
            count++;
        }

        if (count > 0)
        {
            for (size_t i = 0; i < count; ++i)
            {
                if (!process_data(batch[i]))
                {
                    return;
                }
            }
            if (!flush_csv_if_due(clock::now()))
            {
                return;
            }
            reset_backoff();
            continue;
        }

        auto now = clock::now();

        if (!flush_csv_if_due(now))
        {
            return;
        }

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
        sleep_duration = (std::min)(sleep_duration * 2, kSleepMax);
    }

    // Process remaining items
    CThostFtdcDepthMarketDataField data;
    while (queue_.try_dequeue(data))
    {
        if (!process_data(data))
        {
            return;
        }
    }
}

#ifdef CFMDC_ENABLE_PARQUET
void AsyncFileManager::parquet_worker_loop(std::stop_token st)
{
    using namespace std::chrono_literals;
    constexpr size_t kBatchSize = 64;
    std::array<CThostFtdcDepthMarketDataField, kBatchSize> batch;

    auto write_batch = [&](size_t count) {
        for (size_t i = 0; i < count; ++i)
        {
            if (!parquet_writer_->write(batch[i]))
            {
                mark_write_failure("Parquet write", batch[i].InstrumentID);
                return false;
            }
            parquet_records_.fetch_add(1, std::memory_order_relaxed);
        }
        return true;
    };

    while (!st.stop_requested() && !fatal_error_.load(std::memory_order_acquire))
    {
        size_t count = 0;
        while (count < kBatchSize && parquet_queue_->try_dequeue(batch[count]))
        {
            ++count;
        }
        if (count == 0)
        {
            std::this_thread::sleep_for(200us);
            continue;
        }
        if (!write_batch(count))
        {
            return;
        }
    }

    if (fatal_error_.load(std::memory_order_acquire))
    {
        return;
    }

    size_t count = 0;
    while (parquet_queue_->try_dequeue(batch[count]))
    {
        if (++count == kBatchSize)
        {
            if (!write_batch(count))
            {
                return;
            }
            count = 0;
        }
    }
    if (count > 0)
    {
        (void)write_batch(count);
    }
}
#endif

} // namespace cfmdc
