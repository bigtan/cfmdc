#pragma once

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "cfmdc/config/Config.h"
#include "cfmdc/core/MdSpi.h"
#include "cfmdc/core/TraderSpi.h"
#include "cfmdc/utils/Constants.h"

namespace cfmdc
{

/// @brief Wait status for asynchronous operations
enum class WaitStatus
{
    Success,    ///< Operation completed successfully
    Timeout,    ///< Operation timed out
    Interrupted ///< Operation interrupted by shutdown signal
};

/// @brief Main application class for market data collector
/// @details Manages lifecycle of trader and market data connections
/// @thread_safety Not thread-safe. Should only be used from main thread.
class Application
{
  public:
    /// @brief Constructor
    /// @param config_file Path to configuration file
    explicit Application(std::string_view config_file = CONFIG_FILE);

    /// @brief Destructor
    ~Application() = default;

    // Disable copy/move
    Application(const Application &) = delete;
    Application &operator=(const Application &) = delete;
    Application(Application &&) = delete;
    Application &operator=(Application &&) = delete;

    /// @brief Run the application main loop
    void run();

  private:
    void setup_file_limits();
    void ensure_flow_directory();
    void parse_subscription_list(std::vector<std::string> &instruments);
    void query_instruments(const std::vector<std::string> &instruments);
    void subscribe_market_data();
    WaitStatus wait_for_ready(const std::function<bool()> &ready_check, std::string_view operation);

    // Multi-server support
    bool init_trader_with_retry();
    bool init_md_with_retry();

    std::unique_ptr<Config> config_;
    std::unique_ptr<TraderSpi> trader_spi_;
    std::unique_ptr<MdSpi> md_spi_;
    size_t current_server_index_ = 0;
};

} // namespace cfmdc
