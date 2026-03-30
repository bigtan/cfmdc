#pragma once

#include <toml++/toml.h>

#include <filesystem>
#include <string>
#include <vector>

#include "cfmdc/types/FrontServer.h"
#include "cfmdc/types/StorageMode.h"

namespace cfmdc
{

/// @brief Configuration manager for CTP market data collector
/// @details Loads and validates configuration from TOML file
///          Supports both single and multiple front servers
/// @thread_safety Not thread-safe. Should be initialized once at startup.
class Config
{
  public:
    /// @brief Load configuration from file
    /// @param config_file Path to TOML configuration file
    /// @throws ConfigException if file cannot be loaded or validation fails
    explicit Config(const std::string &config_file);

    // Getters

    /// @brief Get all configured front servers
    /// @return Vector of front server configurations
    const std::vector<FrontServer> &front_servers() const noexcept
    {
        return front_servers_;
    }

    /// @brief Get subscription list from first server (for backward
    /// compatibility)
    /// @return Subscription list or "null" if not specified
    std::string subscription_list() const;

    /// @brief Get CSV storage path with trading day placeholders resolved
    /// @param trading_day Trading day in YYYYMMDD format
    /// @return Path to CSV data directory
    std::filesystem::path csv_path(const std::string &trading_day) const;

    /// @brief Get Parquet storage path with trading day placeholders resolved
    /// @param trading_day Trading day in YYYYMMDD format
    /// @return Path to Parquet data directory
    std::filesystem::path parquet_path(const std::string &trading_day) const;

    /// @brief Get flow file path for CTP API
    /// @return Path to flow directory (default: "./flow")
    std::filesystem::path flow_path() const;

    /// @brief Get initialization timeout in seconds
    /// @return Timeout value (default: 60 seconds)
    int init_timeout() const;

    /// @brief Get storage mode
    /// @return Storage mode (CSV, Parquet, or Hybrid)
    StorageMode storage_mode() const;

    /// @brief Get raw TOML configuration
    /// @return Reference to TOML table
    const toml::table &raw_config() const noexcept
    {
        return config_;
    }

  private:
    void validate_config();
    void parse_front_servers();
    std::string resolve_path_placeholders(const std::string &path_template, const std::string &trading_day) const;

    toml::table config_;
    std::vector<FrontServer> front_servers_;
};

} // namespace cfmdc
