#pragma once

#include <cstddef>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>

#include "cfmdc/types/FrontServer.h"
#include "cfmdc/types/StorageMode.h"

namespace cfmdc
{

/// @brief Configuration manager for CTP market data collector
/// @details Eagerly loads and validates configuration from TOML file into typed members.
///          Supports both single ([Front]) and multiple ([[Front]]) front servers and
///          environment-variable credential overrides (non-empty, non-whitespace env vars
///          take precedence over TOML values; when set, the corresponding TOML key may be
///          omitted or empty, and when unset or blank, loading falls back to TOML).
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

    /// @brief Get the application-wide instrument subscription list
    /// @details Reads Application.SubList. Front.SubList is accepted as a
    ///          deprecated fallback for backward compatibility.
    /// @return Subscription list or "null" if not specified
    const std::string &subscription_list() const noexcept
    {
        return subscription_list_;
    }

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
    const std::filesystem::path &flow_path() const noexcept
    {
        return flow_path_;
    }

    /// @brief Get initialization timeout in seconds
    /// @return Timeout value in [1, 3600] (default: 60 seconds)
    int init_timeout() const noexcept
    {
        return init_timeout_;
    }

    /// @brief Get CPU core for worker thread affinity (-1 for none, -2 for auto)
    /// @return CPU core index, -1, or -2 (default: -1)
    int worker_thread_core() const noexcept
    {
        return worker_thread_core_;
    }

    /// @brief Get CPU core for Parquet compression/writer thread affinity (-1 for none, -2 for auto)
    /// @return CPU core index, -1, or -2 (default: -2 if WorkerThreadCore is "auto", else -1)
    int parquet_worker_thread_core() const noexcept
    {
        return parquet_worker_thread_core_;
    }

    /// @brief Whether option contracts are included when subscribing to full market
    /// @return true if options are included (default: false)
    bool include_options() const noexcept
    {
        return include_options_;
    }

    /// @brief Get storage mode
    /// @return Storage mode (CSV, Parquet, or Hybrid)
    StorageMode storage_mode() const noexcept
    {
        return storage_mode_;
    }

    /// @brief Get Parquet row-group size
    /// @return Rows per group (default: 200000)
    size_t parquet_row_group_size() const noexcept
    {
        return parquet_row_group_size_;
    }

  private:
    static std::string resolve_path_placeholders(const std::string &path_template, const std::string &trading_day);

    std::vector<FrontServer> front_servers_;
    std::string subscription_list_{"null"};
    std::optional<std::string> csv_path_template_;
    std::optional<std::string> parquet_path_template_;
    std::filesystem::path flow_path_{"./flow"};
    int init_timeout_{60};
    int worker_thread_core_{-1};
    int parquet_worker_thread_core_{-1};
    bool include_options_{false};
    StorageMode storage_mode_{StorageMode::CSV};
    size_t parquet_row_group_size_{200000};
};

} // namespace cfmdc
