#include "cfmdc/core/Application.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <csignal>
#include <cstring>
#include <filesystem>
#include <format>
#include <sstream>
#include <stdexcept>
#include <thread>

#ifdef _WIN32
#include <io.h>
#else
#include <sys/resource.h>
#endif

#include "cfmdc/utils/Helpers.h"
#include "cfmdc/utils/TimeUtils.h"

namespace cfmdc
{

namespace
{

constexpr auto kInterruptiblePollSlice = std::chrono::milliseconds(100);

// Global flag for graceful shutdown
std::atomic<bool> g_shutdown_requested{false};
static_assert(std::atomic<bool>::is_always_lock_free, "Signal flag must be lock-free");

// Signal handler for graceful shutdown
void signal_handler(int signal)
{
    (void)signal;
    g_shutdown_requested.store(true, std::memory_order_release);
}

} // namespace

Application::Application(std::string_view config_file)
{
    config_ = std::make_unique<Config>(std::string(config_file));
}

void Application::run()
{
    // Install signal handlers for graceful shutdown
    std::signal(SIGINT, signal_handler);  // Ctrl+C
    std::signal(SIGTERM, signal_handler); // Termination request
#ifdef _WIN32
    std::signal(SIGBREAK, signal_handler); // Ctrl+Break on Windows
#else
    std::signal(SIGHUP, signal_handler); // Terminal hangup on POSIX
#endif

    // Capture startup session dates in fixed UTC+8 (Asia/Shanghai) at the very beginning
    // before TD/MD connection retries so session windows stay accurate across slow startups.
    const auto session_dates = time_utils::compute_china_session_dates(std::chrono::system_clock::now());

    // Only set file limits for CSV mode (which may open many files simultaneously)
    const auto storage_mode = config_->storage_mode();
    if (storage_mode == StorageMode::CSV || storage_mode == StorageMode::HYBRID)
    {
        setup_file_limits();
    }

    ensure_flow_directory();

    // Initialize trader SPI with retry across all front servers
    if (!init_trader_with_retry())
    {
        trader_spi_.reset();
        if (g_shutdown_requested.load(std::memory_order_acquire))
        {
            return;
        }
        throw std::runtime_error("All trader front servers failed");
    }

    // Parse subscription list, issue one full-market query, resolve locally,
    // and immediately release the TD session before initializing MdSpi.
    std::vector<std::string> requested_instruments;
    parse_subscription_list(requested_instruments);
    const bool query_ok = query_instruments(requested_instruments);
    trader_spi_.reset();
    if (!query_ok)
    {
        return;
    }

    // Initialize market data SPI with retry across all front servers
    if (!init_md_with_retry())
    {
        md_spi_.reset();
        if (g_shutdown_requested.load(std::memory_order_acquire))
        {
            return;
        }
        throw std::runtime_error("All market data front servers failed");
    }

    // Configure MdSpi with authoritative TradingDay from TraderSpi and startup session dates
    md_spi_->set_trading_day_and_action_days(trading_day_, session_dates.base_action_day,
                                             session_dates.next_action_day, session_dates.startup_time_hms);

    // Subscribe to market data
    if (!subscribe_market_data())
    {
        (void)md_spi_->shutdown();
        md_spi_.reset();
        return;
    }

    spdlog::info("Market data subscription successful, starting to receive data...");
    spdlog::info("Press Ctrl+C to stop the application");

    // Main event loop with graceful shutdown support
    constexpr auto kHealthCheckInterval = std::chrono::milliseconds(100);
    constexpr auto kStatsInterval = std::chrono::minutes(1);
    auto next_stats = std::chrono::steady_clock::now() + kStatsInterval;
    while (!g_shutdown_requested.load(std::memory_order_acquire))
    {
        std::this_thread::sleep_for(kHealthCheckInterval);
        md_spi_->check_pipeline_health();
        if (md_spi_->has_fatal_pipeline_error())
        {
            (void)md_spi_->shutdown();
            md_spi_.reset();
            throw std::runtime_error("Market data storage pipeline failed");
        }

        const auto now = std::chrono::steady_clock::now();
        if (now >= next_stats)
        {
            md_spi_->log_statistics();
            next_stats = now + kStatsInterval;
        }
    }

    spdlog::info("Shutdown signal received, cleaning up SPIs and file managers...");

    // Explicitly reset in reverse order of initialization
    const bool storage_shutdown_succeeded = md_spi_->shutdown();
    md_spi_.reset();
    trader_spi_.reset();

    if (!storage_shutdown_succeeded)
    {
        throw std::runtime_error("Market data storage failed during shutdown");
    }

    spdlog::info("Cleanup completed, exiting application.");
}

void Application::setup_file_limits()
{
#ifdef _WIN32
    if (_setmaxstdio(FILE_HANDLE_LIMIT) == -1)
    {
        spdlog::warn("Failed to set file handle limit to {} on Windows", FILE_HANDLE_LIMIT);
    }
    else
    {
        spdlog::info("Set file handle limit to {} on Windows", FILE_HANDLE_LIMIT);
    }
#else
    struct rlimit rl{};
    if (getrlimit(RLIMIT_NOFILE, &rl) != 0)
    {
        spdlog::warn("Failed to query file descriptor limit (RLIMIT_NOFILE) on Linux: {}", std::strerror(errno));
        return;
    }

    const rlim_t target = static_cast<rlim_t>(FILE_HANDLE_LIMIT);
    if (rl.rlim_cur >= target)
    {
        spdlog::info("File descriptor limit ({}) already meets target ({}) on Linux", rl.rlim_cur, target);
        return;
    }

    const rlim_t desired_cur = (std::min)(target, rl.rlim_max);
    if (desired_cur <= rl.rlim_cur)
    {
        spdlog::warn("File descriptor soft limit ({}) is capped by hard limit ({}) below target ({})", rl.rlim_cur,
                     rl.rlim_max, target);
        return;
    }

    rl.rlim_cur = desired_cur;
    if (setrlimit(RLIMIT_NOFILE, &rl) != 0)
    {
        spdlog::warn("Failed to raise file descriptor limit to {} on Linux: {}", desired_cur, std::strerror(errno));
    }
    else
    {
        spdlog::info("Raised file descriptor soft limit to {} (hard limit: {}) on Linux", desired_cur, rl.rlim_max);
    }
#endif
}

void Application::ensure_flow_directory()
{
    const std::filesystem::path flow_dir = config_->flow_path();
    for (const auto &dir : {flow_dir, flow_dir / "td", flow_dir / "md"})
    {
        if (!std::filesystem::exists(dir))
        {
            std::error_code ec;
            std::filesystem::create_directories(dir, ec);
            if (ec)
            {
                throw std::runtime_error("Failed to create flow directory '" + dir.string() + "': " + ec.message());
            }
            spdlog::info("Created flow directory: {}", dir.string());
        }
        else if (!std::filesystem::is_directory(dir))
        {
            throw std::runtime_error("Flow path exists but is not a directory: " + dir.string());
        }
    }
}

void Application::parse_subscription_list(std::vector<std::string> &instruments)
{
    const std::string &sub_list = config_->subscription_list();
    if (sub_list != "null" && !sub_list.empty())
    {
        std::stringstream ss(sub_list);
        std::string token;
        while (std::getline(ss, token, '|'))
        {
            if (!token.empty())
            {
                instruments.push_back(token);
            }
        }
        spdlog::info("Parsed {} instruments from subscription list", instruments.size());
    }
}

bool Application::query_instruments(const std::vector<std::string> &requested)
{
    const auto timeout = std::chrono::seconds(config_->init_timeout());

    {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (!g_shutdown_requested.load(std::memory_order_acquire))
        {
            if (trader_spi_->request_query_instrument("") == 0)
            {
                break;
            }

            const auto now = std::chrono::steady_clock::now();
            if (now >= deadline)
            {
                throw std::runtime_error("Instrument query request timed out");
            }

            const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
            std::this_thread::sleep_for((std::min)(remaining, kInterruptiblePollSlice));
        }
    }

    if (g_shutdown_requested.load(std::memory_order_acquire))
    {
        spdlog::info("Shutdown requested during instrument query");
        return false;
    }

    bool query_completed = false;
    {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (!g_shutdown_requested.load(std::memory_order_acquire))
        {
            const auto now = std::chrono::steady_clock::now();
            if (now >= deadline)
            {
                query_completed = trader_spi_->wait_for_instrument_query_completion(std::chrono::milliseconds::zero());
                break;
            }
            const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
            if (trader_spi_->wait_for_instrument_query_completion((std::min)(remaining, kInterruptiblePollSlice)))
            {
                query_completed = true;
                break;
            }
        }
    }

    if (g_shutdown_requested.load(std::memory_order_acquire))
    {
        spdlog::info("Shutdown requested while waiting for instrument query");
        return false;
    }

    if (!query_completed)
    {
        throw std::runtime_error("Timed out while waiting for full instrument list");
    }
    spdlog::info("Full-market instrument query completed");

    trading_day_ = trader_spi_->get_trading_day();
    if (!is_valid_trading_day(trading_day_))
    {
        throw std::runtime_error(std::format("Invalid trading day received from trader front: '{}'", trading_day_));
    }

    subscribed_instruments_ = trader_spi_->resolve_subscription_instruments(requested, config_->include_options());
    if (subscribed_instruments_.empty())
    {
        throw std::runtime_error("Instrument query returned no subscribable contracts");
    }

    spdlog::info("Resolved {} subscribable instrument(s) for trading day {}", subscribed_instruments_.size(),
                 trading_day_);
    return true;
}

bool Application::subscribe_market_data()
{
    if (subscribed_instruments_.empty())
    {
        throw std::runtime_error("No instrument IDs available for market data subscription");
    }

    // Convert to char* array for CTP API
    std::vector<char *> c_strings;
    c_strings.reserve(subscribed_instruments_.size());
    for (const auto &id : subscribed_instruments_)
    {
        c_strings.push_back(const_cast<char *>(id.c_str()));
    }

    const auto timeout = std::chrono::seconds(config_->init_timeout());
    {
        const auto deadline = std::chrono::steady_clock::now() + timeout;
        while (!g_shutdown_requested.load(std::memory_order_acquire))
        {
            if (md_spi_->subscribe_market_data(c_strings) == 0)
            {
                break;
            }

            const auto now = std::chrono::steady_clock::now();
            if (now >= deadline)
            {
                throw std::runtime_error("Market data subscription request timed out");
            }

            const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
            std::this_thread::sleep_for((std::min)(remaining, kInterruptiblePollSlice));
        }
    }

    if (g_shutdown_requested.load(std::memory_order_acquire))
    {
        spdlog::info("Shutdown requested during market data subscription");
        return false;
    }

    bool completed = false;
    {
        const auto wait_deadline = std::chrono::steady_clock::now() + timeout;
        while (!g_shutdown_requested.load(std::memory_order_acquire))
        {
            const auto now = std::chrono::steady_clock::now();
            if (now >= wait_deadline)
            {
                completed = md_spi_->wait_for_subscription_completion(std::chrono::milliseconds::zero());
                break;
            }
            const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(wait_deadline - now);
            if (md_spi_->wait_for_subscription_completion((std::min)(remaining, kInterruptiblePollSlice)))
            {
                completed = true;
                break;
            }
        }
    }

    if (g_shutdown_requested.load(std::memory_order_acquire))
    {
        spdlog::info("Shutdown requested while waiting for market data subscription responses");
        return false;
    }

    const auto result = md_spi_->subscription_result();
    if (!completed && !result.completed)
    {
        throw std::runtime_error(std::format(
            "Timed out while waiting for market data subscription responses: expected={}, succeeded={}, failed={}, "
            "missing={}",
            result.expected, result.succeeded, result.failed, result.missing));
    }

    if (!result.successful())
    {
        throw std::runtime_error(
            std::format("Market data subscription incomplete: expected={}, succeeded={}, failed={}, missing={}",
                        result.expected, result.succeeded, result.failed, result.missing));
    }

    spdlog::info("Confirmed market data subscriptions: {} instruments", result.succeeded);
    return true;
}

WaitStatus Application::wait_for_initialization(
    const std::function<InitializationResult(std::chrono::milliseconds)> &wait_for_result, std::string_view operation)
{
    const auto timeout = std::chrono::seconds(config_->init_timeout());
    const auto start_time = std::chrono::steady_clock::now();
    const auto deadline = start_time + timeout;

    spdlog::info("Waiting for {} to complete (timeout: {}s)...", operation, timeout.count());

    while (!g_shutdown_requested.load(std::memory_order_acquire))
    {
        const auto now = std::chrono::steady_clock::now();
        if (now >= deadline)
        {
            const auto elapsed = now - start_time;
            spdlog::error("{} timed out after {}s, service may be unavailable", operation,
                          std::chrono::duration_cast<std::chrono::seconds>(elapsed).count());
            return WaitStatus::Timeout;
        }

        const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now);
        const auto wait_slice = (std::min)(remaining, kInterruptiblePollSlice);
        const auto result = wait_for_result(wait_slice);
        if (result.ready())
        {
            const auto elapsed = std::chrono::steady_clock::now() - start_time;
            spdlog::info("{} completed successfully (took {:.2f}s)", operation,
                         std::chrono::duration<double>(elapsed).count());
            return WaitStatus::Success;
        }
        if (result.failed())
        {
            spdlog::error("{} failed (error {}): {}", operation, result.error_code, result.message);
            return WaitStatus::Failed;
        }
    }

    spdlog::info("Shutdown requested during {}", operation);
    return WaitStatus::Interrupted;
}

bool Application::init_trader_with_retry()
{
    const auto &servers = config_->front_servers();
    const auto td_flow_dir = config_->flow_path() / "td";

    for (size_t i = 0; i < servers.size(); ++i)
    {
        if (g_shutdown_requested.load(std::memory_order_acquire))
        {
            spdlog::info("Shutdown requested, stopping trader initialization");
            return false;
        }

        const auto &server = servers[i];
        spdlog::info("Attempting to initialize trader with server {}/{}: {}", i + 1, servers.size(), server.td_url());

        try
        {
            // Create new trader SPI with current server
            trader_spi_ = std::make_unique<TraderSpi>(server, td_flow_dir);
            trader_spi_->init();

            // Wait for initialization
            auto status = wait_for_initialization(
                [this](std::chrono::milliseconds timeout) { return trader_spi_->wait_for_initialization(timeout); },
                std::format("trader initialization (server {})", i + 1));

            if (status == WaitStatus::Success)
            {
                spdlog::info("Trader initialized successfully with server {}: {}", i + 1, server.td_url());
                return true;
            }
            else if (status == WaitStatus::Interrupted)
            {
                return false;
            }

            // Timeout or explicit failure - try next server
            spdlog::warn("Trader initialization failed with server {}: {}, trying next "
                         "server...",
                         i + 1, server.td_url());
            trader_spi_.reset();
        }
        catch (const std::exception &e)
        {
            spdlog::error("Exception during trader initialization with server {}: {}", i + 1, e.what());
            trader_spi_.reset();
        }
    }

    return false;
}

bool Application::init_md_with_retry()
{
    const auto &servers = config_->front_servers();
    const auto md_flow_dir = config_->flow_path() / "md";

    for (size_t i = 0; i < servers.size(); ++i)
    {
        if (g_shutdown_requested.load(std::memory_order_acquire))
        {
            spdlog::info("Shutdown requested, stopping market data initialization");
            return false;
        }

        const auto &server = servers[i];
        spdlog::info("Attempting to initialize market data with server {}/{}: {}", i + 1, servers.size(),
                     server.md_url());

        try
        {
            // Create new MD SPI with current server
            // Paths will be resolved in MdSpi after trading day is received
            md_spi_ = std::make_unique<MdSpi>(server, *config_, md_flow_dir);
            md_spi_->init();

            // Wait for initialization
            auto status = wait_for_initialization(
                [this](std::chrono::milliseconds timeout) { return md_spi_->wait_for_initialization(timeout); },
                std::format("market data initialization (server {})", i + 1));

            if (status == WaitStatus::Success)
            {
                spdlog::info("Market data initialized successfully with server {}: {}", i + 1, server.md_url());
                return true;
            }
            else if (status == WaitStatus::Interrupted)
            {
                return false;
            }

            // Timeout or explicit failure - try next server
            spdlog::warn("Market data initialization failed with server {}: {}, trying next "
                         "server...",
                         i + 1, server.md_url());
            md_spi_.reset();
        }
        catch (const std::exception &e)
        {
            spdlog::error("Exception during market data initialization with server {}: {}", i + 1, e.what());
            md_spi_.reset();
        }
    }

    return false;
}

} // namespace cfmdc
