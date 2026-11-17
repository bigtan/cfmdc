#include "cfmdc/utils/FileManager.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <format>
#include <stdexcept>

#include "cfmdc/utils/Error.h"
#include "cfmdc/utils/Helpers.h"
#include "cfmdc/utils/MarketDataTimePolicy.h"

namespace cfmdc
{

namespace
{

/// @brief Time-budgeted adaptive backoff (spin -> yield -> jittered sleep) for worker polling loops
class AdaptiveBackoff
{
  public:
    using clock = std::chrono::steady_clock;

    static constexpr auto kSpinBudget = std::chrono::microseconds(8);
    static constexpr auto kYieldBudget = std::chrono::microseconds(80);
    static constexpr auto kSleepMin = std::chrono::microseconds(50);
    static constexpr auto kSleepMax = std::chrono::microseconds(2000);
    static constexpr int kJitterPercent = 10;

    AdaptiveBackoff() noexcept
    {
        reset(clock::now());
    }

    void reset(clock::time_point now = clock::now()) noexcept
    {
        spin_deadline_ = now + kSpinBudget;
        yield_deadline_ = spin_deadline_ + kYieldBudget;
        sleep_duration_ = kSleepMin;
    }

    void wait_once(clock::time_point now = clock::now()) noexcept
    {
        if (now < spin_deadline_)
        {
            cpu_relax();
            return;
        }
        if (now < yield_deadline_)
        {
            std::this_thread::yield();
            return;
        }
        std::this_thread::sleep_for(jittered_sleep(sleep_duration_));
        sleep_duration_ = (std::min)(sleep_duration_ * 2, kSleepMax);
    }

  private:
    std::chrono::microseconds jittered_sleep(std::chrono::microseconds base) noexcept
    {
        jitter_state_ = jitter_state_ * 1664525u + 1013904223u;
        const int jitter = static_cast<int>(jitter_state_ % (2 * kJitterPercent + 1)) - kJitterPercent;
        const auto delta = std::chrono::microseconds(static_cast<int64_t>(base.count()) * jitter / 100);
        const auto adjusted = base + delta;
        return adjusted < std::chrono::microseconds(1) ? std::chrono::microseconds(1) : adjusted;
    }

    clock::time_point spin_deadline_{};
    clock::time_point yield_deadline_{};
    std::chrono::microseconds sleep_duration_{kSleepMin};
    std::uint32_t jitter_state_{0x9e3779b9u};
};

} // namespace

AsyncFileManager::AsyncFileManager(const std::filesystem::path &csv_path, const std::filesystem::path &parquet_path,
                                   const std::string &trading_day, const std::string &base_action_day,
                                   const std::string &next_action_day, const std::string &startup_time_hms,
                                   StorageMode mode, Options options)
    : csv_path_(csv_path), parquet_path_(parquet_path), trading_day_(trading_day), action_day_base_(base_action_day),
      action_day_next_(next_action_day), storage_mode_(mode),
      csv_flush_interval_(options.csv_flush_interval > std::chrono::milliseconds::zero() ? options.csv_flush_interval
                                                                                         : std::chrono::seconds(5)),
      queue_(std::make_unique<MarketDataQueue>())
{
#ifndef CFMDC_ENABLE_PARQUET
    if (mode == StorageMode::PARQUET || mode == StorageMode::HYBRID)
    {
        throw std::invalid_argument("Parquet storage requires a build with Parquet support");
    }
#endif

    if (!is_valid_trading_day(trading_day_))
    {
        throw FileException(std::format("Invalid trading day '{}': expected 8 digits (YYYYMMDD)", trading_day_));
    }

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
#endif

    const auto mode_str = to_string(storage_mode_);
    const char *window_str = market_data_time::to_string(startup_window_);
    spdlog::info("AsyncFileManager initialized: mode={}, csv_path={}, parquet_path={}, "
                 "trading_day={}, base_action_day={}, next_action_day={}",
                 mode_str, csv_path_.string(), parquet_path_.string(), trading_day_, action_day_base_,
                 action_day_next_);

    spdlog::info("AsyncFileManager UpdateTime filter configured: startup_time={}, window={}", startup_time_hms_,
                 window_str);

    bool has_parquet_worker = false;
#ifdef CFMDC_ENABLE_PARQUET
    if (parquet_writer_)
    {
        has_parquet_worker = true;
        parquet_worker_active_.store(true, std::memory_order_release);
        parquet_worker_thread_ = std::jthread([this](std::stop_token st) { parquet_worker_loop(st); });
    }
#endif
    worker_thread_ = std::jthread([this](std::stop_token st) { worker_loop(st); });

    const int effective_parquet_cfg = has_parquet_worker ? options.parquet_worker_core : -1;
    const auto [ingest_core, parquet_core] = resolve_worker_cores(options.worker_core, effective_parquet_cfg);

    auto apply_affinity = [](std::jthread &thread, int requested_cfg, int resolved_core, std::string_view role) {
        if (requested_cfg >= 0 && resolved_core < 0)
        {
            spdlog::warn("Configured {} core ({}) is unavailable in current process affinity; affinity disabled", role,
                         requested_cfg);
            return;
        }
        if (resolved_core >= 0)
        {
            if (set_thread_affinity(thread, resolved_core))
            {
                spdlog::info("{} worker thread pinned to CPU core: {} (mode: {})", role, resolved_core,
                             requested_cfg == -2 ? "auto" : "manual");
            }
            else
            {
                spdlog::warn("Failed to pin {} worker thread to CPU core: {}", role, resolved_core);
            }
        }
    };

    apply_affinity(worker_thread_, options.worker_core, ingest_core, "Ingest");
#ifdef CFMDC_ENABLE_PARQUET
    if (has_parquet_worker)
    {
        apply_affinity(parquet_worker_thread_, options.parquet_worker_core, parquet_core, "Parquet");
    }
#endif
}

AsyncFileManager::~AsyncFileManager()
{
    stop();
    (void)close_all();
}

bool AsyncFileManager::write_market_data_async(const MarketTick &tick)
{
    // Enqueue only: no filtering or field rewriting here.
    return queue_->try_enqueue(tick);
}

bool AsyncFileManager::write_market_data_async(const CThostFtdcDepthMarketDataField &raw)
{
    return write_market_data_async(MarketTick::from_ctp(raw));
}

bool AsyncFileManager::process_market_data(MarketTick &tick)
{
    if (!is_safe_instrument_id(bounded_sv(tick.instrument_id)))
    {
        invalid_records_.fetch_add(1, std::memory_order_relaxed);
        return false;
    }

    // Filter before rewriting TradingDay/ActionDay.
    const std::string_view update_time = bounded_sv(tick.update_time);
    if (!market_data_time::should_store_by_update_time(startup_window_, update_time))
    {
        filtered_records_.fetch_add(1, std::memory_order_relaxed);
        return false;
    }

    // Override TradingDay with the one from TraderSpi (authoritative)
    safe_strcpy(tick.trading_day, trading_day_);

    // Calculate ActionDay based on UpdateTime
    if (market_data_time::should_use_next_action_day(update_time))
    {
        safe_strcpy(tick.action_day, action_day_next_);
    }
    else
    {
        safe_strcpy(tick.action_day, action_day_base_);
    }

    return true;
}

bool AsyncFileManager::write_processed_market_data(const MarketTick &tick, std::stop_token st)
{
    if (fatal_error_.load(std::memory_order_acquire))
    {
        return false;
    }

    if (csv_writer_)
    {
        if (!csv_writer_->write(tick))
        {
            mark_write_failure(FailureSource::Csv, "CSV write", bounded_sv(tick.instrument_id));
            return false;
        }
        csv_records_.fetch_add(1, std::memory_order_relaxed);
    }

#ifdef CFMDC_ENABLE_PARQUET
    if (parquet_queue_)
    {
        if (!parquet_queue_->try_enqueue(tick))
        {
            using clock = std::chrono::steady_clock;
            constexpr auto kSpinBudget = std::chrono::microseconds(8);
            constexpr auto kYieldBudget = std::chrono::microseconds(80);
            constexpr auto kSleepStep = std::chrono::microseconds(50);

            const auto start = clock::now();
            const auto spin_deadline = start + kSpinBudget;
            const auto yield_deadline = spin_deadline + kYieldBudget;

            do
            {
                if (st.stop_requested() || parquet_fatal_.load(std::memory_order_acquire) ||
                    !parquet_worker_active_.load(std::memory_order_acquire))
                {
                    return false;
                }

                const auto now = clock::now();
                if (now < spin_deadline)
                {
                    cpu_relax();
                }
                else if (now < yield_deadline)
                {
                    std::this_thread::yield();
                }
                else
                {
                    std::this_thread::sleep_for(kSleepStep);
                }
            } while (!parquet_queue_->try_enqueue(tick));
        }
    }
#else
    (void)st;
#endif
    return true;
}

void AsyncFileManager::mark_write_failure(FailureSource source, std::string_view operation,
                                            std::string_view instrument_id)
{
    const auto failures = write_failures_.fetch_add(1, std::memory_order_relaxed) + 1;
    if (source == FailureSource::Csv)
    {
        csv_fatal_.store(true, std::memory_order_release);
    }
    else
    {
        parquet_fatal_.store(true, std::memory_order_release);
    }
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

bool AsyncFileManager::close_all()
{
    bool success = true;
    if (csv_writer_ && !csv_writer_->close_all())
    {
        success = false;
        mark_write_failure(FailureSource::Csv, "CSV close");
    }

#ifdef CFMDC_ENABLE_PARQUET
    if (parquet_writer_ && !parquet_writer_->close_all())
    {
        success = false;
        mark_write_failure(FailureSource::Parquet, "Parquet close");
    }
#endif
    return success;
}

bool AsyncFileManager::flush_all()
{
    bool success = true;
    if (csv_writer_ && !csv_writer_->flush())
    {
        success = false;
        mark_write_failure(FailureSource::Csv, "CSV flush");
    }
#ifdef CFMDC_ENABLE_PARQUET
    if (parquet_writer_ && !parquet_writer_->flush())
    {
        success = false;
        mark_write_failure(FailureSource::Parquet, "Parquet flush");
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
    stats.queue_size = queue_->size();
#ifdef CFMDC_ENABLE_PARQUET
    stats.parquet_queue_size = parquet_queue_ ? parquet_queue_->size() : 0;
#endif
    stats.dropped_records = queue_->overflow_count();
    stats.filtered_records = filtered_records_.load(std::memory_order_relaxed);
    stats.invalid_records = invalid_records_.load(std::memory_order_relaxed);
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
    set_current_thread_name("cfmdc-ingest");

    constexpr size_t kBatchSize = 64;

    // Durability: bound CSV data lost on an abnormal exit (cheap ofstream flush).
    // Parquet is intentionally NOT flushed periodically: it would fragment row
    // groups, and without the file footer (written on close) the file is
    // unreadable anyway - Parquet durability relies on graceful shutdown.
    using clock = std::chrono::steady_clock;
    AdaptiveBackoff backoff;
    auto next_csv_flush = clock::now() + csv_flush_interval_;

    auto flush_csv_if_due = [&](clock::time_point now) {
        if (!csv_writer_ || now < next_csv_flush)
        {
            return true;
        }
        if (!csv_writer_->flush())
        {
            mark_write_failure(FailureSource::Csv, "periodic flush");
            return false;
        }
        periodic_flushes_.fetch_add(1, std::memory_order_relaxed);
        next_csv_flush = now + csv_flush_interval_;
        return true;
    };

    auto process_slot = [&](MarketTick &slot) {
        if (!process_market_data(slot))
        {
            return true;
        }
        return write_processed_market_data(slot, st);
    };

    while (!st.stop_requested())
    {
        size_t count = 0;
        while (count < kBatchSize)
        {
            auto *slot = queue_->front();
            if (!slot)
            {
                break;
            }
            const bool ok = process_slot(*slot);
            queue_->pop();
            if (!ok)
            {
                return;
            }
            ++count;
        }

        const auto now = clock::now();
        if (count > 0)
        {
            if (!flush_csv_if_due(now))
            {
                return;
            }
            backoff.reset(now);
            continue;
        }

        if (!flush_csv_if_due(now))
        {
            return;
        }

        backoff.wait_once(now);
    }

    // Process remaining items in-place
    while (auto *slot = queue_->front())
    {
        const bool ok = process_slot(*slot);
        queue_->pop();
        if (!ok)
        {
            return;
        }
    }
}

#ifdef CFMDC_ENABLE_PARQUET
void AsyncFileManager::parquet_worker_loop(std::stop_token st)
{
    set_current_thread_name("cfmdc-parquet");

    constexpr size_t kBatchSize = 64;
    AdaptiveBackoff backoff;

    struct ActiveGuard
    {
        std::atomic<bool> &flag;
        ~ActiveGuard()
        {
            flag.store(false, std::memory_order_release);
        }
    } active_guard{parquet_worker_active_};

    auto write_slot = [&](const MarketTick &slot) {
        if (!parquet_writer_->write(slot))
        {
            mark_write_failure(FailureSource::Parquet, "Parquet write", bounded_sv(slot.instrument_id));
            return false;
        }
        parquet_records_.fetch_add(1, std::memory_order_relaxed);
        return true;
    };

    while (!st.stop_requested() && !parquet_fatal_.load(std::memory_order_acquire))
    {
        size_t count = 0;
        while (count < kBatchSize)
        {
            const auto *slot = parquet_queue_->front();
            if (!slot)
            {
                break;
            }
            const bool ok = write_slot(*slot);
            parquet_queue_->pop();
            if (!ok)
            {
                return;
            }
            ++count;
        }
        if (count > 0)
        {
            backoff.reset();
            continue;
        }
        backoff.wait_once();
    }

    if (parquet_fatal_.load(std::memory_order_acquire))
    {
        return;
    }

    while (const auto *slot = parquet_queue_->front())
    {
        const bool ok = write_slot(*slot);
        parquet_queue_->pop();
        if (!ok)
        {
            return;
        }
    }
}
#endif

} // namespace cfmdc
