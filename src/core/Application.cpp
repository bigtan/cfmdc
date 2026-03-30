#include "cfmdc/core/Application.h"

#include <spdlog/spdlog.h>

#include <atomic>
#include <csignal>
#include <format>
#include <sstream>
#include <thread>

#ifdef _WIN32
#include <io.h>
#else
#include <sys/resource.h>
#endif

#include "cfmdc/utils/TimeUtils.h"

namespace cfmdc
{

// Global flag for graceful shutdown
static std::atomic<bool> g_shutdown_requested{false};

// Signal handler for graceful shutdown
void signal_handler(int signal)
{
    (void)signal;
    g_shutdown_requested.store(true, std::memory_order_release);
}

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
#endif

    // Only set file limits for CSV mode (which may open many files
    // simultaneously)
    auto storage_mode = config_->storage_mode();
    if (storage_mode == StorageMode::CSV || storage_mode == StorageMode::HYBRID)
    {
        setup_file_limits();
    }

    ensure_flow_directory();

    // Initialize trader SPI with retry across all front servers
    if (!init_trader_with_retry())
    {
        spdlog::error("All trader front servers failed, exiting...");
        return;
    }

    // Parse subscription list and query instruments
    std::vector<std::string> instruments;
    parse_subscription_list(instruments);
    query_instruments(instruments);

    // Initialize market data SPI with retry across all front servers
    if (!init_md_with_retry())
    {
        spdlog::error("All market data front servers failed, exiting...");
        return;
    }

    // Configure MdSpi with TradingDay from TraderSpi and calculated ActionDays
    {
        auto now = std::chrono::system_clock::now();
        auto now_c = std::chrono::system_clock::to_time_t(now);
        std::tm now_tm = time_utils::to_local_tm(now_c);

        std::string base_action_day;
        std::string next_action_day;

        std::string startup_time_hms = time_utils::format_hhmmss(now_tm);

        // Startup time windows:
        // - < 07:00:00: base=t-1, next=t
        // - >= 07:00:00: base=t, next=t+1
        // (MdSpi/AsyncFileManager uses UpdateTime <= 07:00:00 ? next : base for ActionDay)
        if (startup_time_hms < "07:00:00")
        {
            // Set next as today
            next_action_day = time_utils::format_yyyymmdd(now_tm);

            // Set base as yesterday
            auto yesterday = now - std::chrono::hours(24);
            auto yesterday_c = std::chrono::system_clock::to_time_t(yesterday);
            std::tm yesterday_tm = time_utils::to_local_tm(yesterday_c);
            base_action_day = time_utils::format_yyyymmdd(yesterday_tm);
        }
        else
        {
            // Normal startup (>= 07:00:00), base is today, next is tomorrow
            base_action_day = time_utils::format_yyyymmdd(now_tm);

            auto tomorrow = now + std::chrono::hours(24);
            auto tomorrow_c = std::chrono::system_clock::to_time_t(tomorrow);
            std::tm tomorrow_tm = time_utils::to_local_tm(tomorrow_c);
            next_action_day = time_utils::format_yyyymmdd(tomorrow_tm);
        }

        std::string trading_day = trader_spi_->get_trading_day();
        md_spi_->set_trading_day_and_action_days(trading_day, base_action_day, next_action_day, startup_time_hms);
    }

    // Subscribe to market data
    subscribe_market_data();

    spdlog::info("Market data subscription successful, starting to receive data...");
    spdlog::info("Press Ctrl+C to stop the application");

    // Main event loop with graceful shutdown support
    while (!g_shutdown_requested.load(std::memory_order_acquire))
    {
        std::this_thread::sleep_for(SLEEP_DURATION);
    }

    spdlog::info("Shutdown signal received, cleaning up...");
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
    struct rlimit rl;
    rl.rlim_cur = FILE_HANDLE_LIMIT;
    rl.rlim_max = FILE_HANDLE_LIMIT;
    if (setrlimit(RLIMIT_NOFILE, &rl) == -1)
    {
        spdlog::warn("Failed to set file descriptor limit to {} on Linux", FILE_HANDLE_LIMIT);
    }
    else
    {
        spdlog::info("Set file descriptor limit to {} on Linux", FILE_HANDLE_LIMIT);
    }
#endif
}

void Application::ensure_flow_directory()
{
    std::filesystem::path flow_dir = config_->flow_path();

    // Ensure the path represents a directory (not a file)
    // If it doesn't end with a separator, filesystem will treat it correctly
    if (!std::filesystem::exists(flow_dir))
    {
        std::error_code ec;
        std::filesystem::create_directories(flow_dir, ec);
        if (ec)
        {
            throw std::runtime_error("Failed to create flow directory: " + ec.message());
        }
        spdlog::info("Created flow directory: {}", flow_dir.string());
    }
    else if (!std::filesystem::is_directory(flow_dir))
    {
        throw std::runtime_error("Flow path exists but is not a directory: " + flow_dir.string());
    }
}

void Application::parse_subscription_list(std::vector<std::string> &instruments)
{
    const std::string sub_list = config_->subscription_list();
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

void Application::query_instruments(const std::vector<std::string> &instruments)
{
    const auto timeout = std::chrono::seconds(config_->init_timeout());
    auto request_with_retry = [this, timeout](std::string_view instrument) {
        const auto start_time = std::chrono::steady_clock::now();
        while (!g_shutdown_requested.load(std::memory_order_acquire))
        {
            if (trader_spi_->request_query_instrument(instrument) == 0)
            {
                return;
            }

            if (std::chrono::steady_clock::now() - start_time >= timeout)
            {
                throw std::runtime_error(std::format("Instrument query request timed out for '{}'", instrument));
            }

            std::this_thread::sleep_for(SLEEP_DURATION);
        }

        throw std::runtime_error("Shutdown requested during instrument query");
    };

    if (instruments.empty())
    {
        // Query all instruments
        request_with_retry("");
        if (!trader_spi_->wait_for_instrument_query_completion(timeout))
        {
            throw std::runtime_error("Timed out while waiting for full instrument list");
        }
        spdlog::info("Query all instruments completed");
    }
    else
    {
        // Query specific instruments
        for (const auto &instrument : instruments)
        {
            spdlog::info("Querying instrument: {}", instrument);
            request_with_retry(instrument);
            if (!trader_spi_->wait_for_instrument_query_completion(timeout))
            {
                throw std::runtime_error(std::format("Timed out while waiting for instrument '{}'", instrument));
            }
            spdlog::info("Query instrument {} completed", instrument);
        }
    }

    if (trader_spi_->get_instrument_ids().empty())
    {
        throw std::runtime_error("Instrument query returned no subscribable futures contracts");
    }
}

void Application::subscribe_market_data()
{
    const auto instrument_ids = trader_spi_->get_instrument_ids();
    if (instrument_ids.empty())
    {
        throw std::runtime_error("No instrument IDs available for market data subscription");
    }

    // Convert to char* array for CTP API
    std::vector<char *> c_strings;
    c_strings.reserve(instrument_ids.size());

    for (const auto &id : instrument_ids)
    {
        c_strings.push_back(const_cast<char *>(id.c_str()));
    }

    const auto timeout = std::chrono::seconds(config_->init_timeout());
    const auto start_time = std::chrono::steady_clock::now();
    while (!g_shutdown_requested.load(std::memory_order_acquire))
    {
        if (md_spi_->subscribe_market_data(c_strings) == 0)
        {
            return;
        }

        if (std::chrono::steady_clock::now() - start_time >= timeout)
        {
            throw std::runtime_error("Market data subscription request timed out");
        }

        std::this_thread::sleep_for(SLEEP_DURATION);
    }

    throw std::runtime_error("Shutdown requested during market data subscription");
}

WaitStatus Application::wait_for_ready(const std::function<bool()> &ready_check, std::string_view operation)
{
    const auto timeout = std::chrono::seconds(config_->init_timeout());
    const auto start_time = std::chrono::steady_clock::now();

    spdlog::info("Waiting for {} to complete (timeout: {}s)...", operation, timeout.count());

    while (!ready_check() && !g_shutdown_requested.load(std::memory_order_acquire))
    {
        std::this_thread::sleep_for(SLEEP_DURATION);

        // Check if timeout has been reached
        auto elapsed = std::chrono::steady_clock::now() - start_time;
        if (elapsed >= timeout)
        {
            spdlog::error("{} timed out after {}s, service may be unavailable", operation,
                          std::chrono::duration_cast<std::chrono::seconds>(elapsed).count());
            return WaitStatus::Timeout;
        }
    }

    if (g_shutdown_requested.load(std::memory_order_acquire))
    {
        spdlog::info("Shutdown requested during {}", operation);
        return WaitStatus::Interrupted;
    }

    auto elapsed = std::chrono::steady_clock::now() - start_time;
    spdlog::info("{} completed successfully (took {:.2f}s)", operation, std::chrono::duration<double>(elapsed).count());
    return WaitStatus::Success;
}

bool Application::init_trader_with_retry()
{
    const auto &servers = config_->front_servers();

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
            trader_spi_ = std::make_unique<TraderSpi>(server, config_->flow_path());
            trader_spi_->init();

            // Wait for initialization
            auto status = wait_for_ready([this]() { return trader_spi_->is_ready(); },
                                         std::format("trader initialization (server {})", i + 1));

            if (status == WaitStatus::Success)
            {
                spdlog::info("Trader initialized successfully with server {}: {}", i + 1, server.td_url());
                current_server_index_ = i;
                return true;
            }
            else if (status == WaitStatus::Interrupted)
            {
                return false;
            }

            // Timeout - try next server
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
            md_spi_ = std::make_unique<MdSpi>(server, *config_, config_->flow_path());
            md_spi_->init();

            // Wait for initialization
            auto status = wait_for_ready([this]() { return md_spi_->is_ready(); },
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

            // Timeout - try next server
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
