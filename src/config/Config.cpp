#include "cfmdc/config/Config.h"

#include <spdlog/spdlog.h>
#include <toml++/toml.h>

#include <algorithm>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <format>
#include <limits>
#include <memory>
#include <ranges>
#include <utility>

#ifndef _WIN32
#include <sys/stat.h>
#endif

#include "cfmdc/utils/Error.h"
#include "cfmdc/utils/Helpers.h"

namespace cfmdc
{

namespace
{

void scrub_string(std::string &value) noexcept
{
    if (!value.empty())
    {
        volatile char *ptr = value.data();
        for (size_t i = 0; i < value.size(); ++i)
        {
            ptr[i] = '\0';
        }
    }
    value.clear();
}

std::optional<std::string> read_non_empty_env(const char *name)
{
#ifdef _WIN32
    char *raw = nullptr;
    size_t len = 0;
    if (_dupenv_s(&raw, &len, name) != 0 || raw == nullptr)
    {
        return std::nullopt;
    }
    const auto free_and_scrub = [](char *ptr) noexcept {
        if (ptr)
        {
            const size_t n = std::strlen(ptr);
            volatile char *vptr = ptr;
            for (size_t i = 0; i < n; ++i)
            {
                vptr[i] = '\0';
            }
            std::free(ptr);
        }
    };
    std::unique_ptr<char, decltype(free_and_scrub)> guard(raw, free_and_scrub);
    std::string value(raw);
#else
    const char *raw = std::getenv(name);
    if (raw == nullptr)
    {
        return std::nullopt;
    }
    std::string value(raw);
#endif

    if (value.empty())
    {
        return std::nullopt;
    }
    return value;
}

struct EnvCredentialOverrides
{
    std::optional<std::string> broker_id;
    std::optional<std::string> user_id;
    std::optional<std::string> password;
    std::optional<std::string> auth_code;
    std::optional<std::string> app_id;

    ~EnvCredentialOverrides()
    {
        if (password)
        {
            scrub_string(*password);
        }
        if (auth_code)
        {
            scrub_string(*auth_code);
        }
    }
};

EnvCredentialOverrides load_env_credential_overrides()
{
    EnvCredentialOverrides overrides;
    overrides.broker_id = read_non_empty_env("CFMDC_BROKER_ID");
    overrides.user_id = read_non_empty_env("CFMDC_USER_ID");
    overrides.password = read_non_empty_env("CFMDC_PASSWORD");
    overrides.auth_code = read_non_empty_env("CFMDC_AUTH_CODE");
    overrides.app_id = read_non_empty_env("CFMDC_APP_ID");
    return overrides;
}

StorageMode parse_storage_mode(const toml::table &history_table)
{
    if (!history_table.contains("StorageMode"))
    {
        return StorageMode::CSV;
    }

    const toml::node *mode_node = history_table.get("StorageMode");
    if (!mode_node || !mode_node->is_string())
    {
        throw ConfigException("History.StorageMode must be a string");
    }

    const std::string &raw_mode = mode_node->as_string()->get();
    std::string mode_str = raw_mode;
    std::ranges::transform(mode_str, mode_str.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });

    if (mode_str == "parquet")
    {
        return StorageMode::PARQUET;
    }
    if (mode_str == "hybrid")
    {
        return StorageMode::HYBRID;
    }
    if (mode_str == "csv")
    {
        return StorageMode::CSV;
    }

    throw ConfigException(std::format("Unsupported History.StorageMode: {}", raw_mode));
}

size_t parse_parquet_row_group_size(const toml::table &history_table)
{
    constexpr int64_t kDefaultRowGroupSize = 100000;
    constexpr int64_t kMaxRowGroupSize = 1000000;

    if (!history_table.contains("ParquetRowGroupSize"))
    {
        return kDefaultRowGroupSize;
    }

    const toml::node *node = history_table.get("ParquetRowGroupSize");
    if (!node || !node->is_integer())
    {
        throw ConfigException(
            std::format("History.ParquetRowGroupSize must be an integer between 1 and {}", kMaxRowGroupSize));
    }

    const int64_t value = node->as_integer()->get();
    if (value <= 0 || value > kMaxRowGroupSize)
    {
        throw ConfigException(
            std::format("History.ParquetRowGroupSize must be an integer between 1 and {}", kMaxRowGroupSize));
    }
    return static_cast<size_t>(value);
}

int parse_init_timeout(const toml::table *app_table)
{
    constexpr int64_t kDefaultInitTimeout = 60;
    constexpr int64_t kMinInitTimeout = 1;
    constexpr int64_t kMaxInitTimeout = 3600;

    if (!app_table || !app_table->contains("InitTimeout"))
    {
        return static_cast<int>(kDefaultInitTimeout);
    }

    const toml::node *node = app_table->get("InitTimeout");
    if (!node || !node->is_integer())
    {
        throw ConfigException(
            std::format("Application.InitTimeout must be an integer between {} and {}", kMinInitTimeout, kMaxInitTimeout));
    }

    const int64_t value = node->as_integer()->get();
    if (value < kMinInitTimeout || value > kMaxInitTimeout)
    {
        throw ConfigException(std::format("Application.InitTimeout must be an integer between {} and {}, got {}",
                                          kMinInitTimeout, kMaxInitTimeout, value));
    }
    return static_cast<int>(value);
}

int parse_worker_core(const toml::node &node, std::string_view key_name)
{
    if (node.is_string())
    {
        const std::string &raw = node.as_string()->get();
        std::string lower = raw;
        std::ranges::transform(lower, lower.begin(), [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        if (lower == "auto")
        {
            return -2;
        }
        throw ConfigException(std::format("{} must be \"auto\" or an integer >= -1, got '{}'", key_name, raw));
    }

    if (node.is_integer())
    {
        const int64_t value = node.as_integer()->get();
        if (value < -1 || value > static_cast<int64_t>((std::numeric_limits<int>::max)()))
        {
            throw ConfigException(std::format("{} integer value must be >= -1, got {}", key_name, value));
        }
        return static_cast<int>(value);
    }

    throw ConfigException(std::format("{} must be \"auto\" or an integer >= -1", key_name));
}

bool parse_include_options(const toml::table *app_table)
{
    if (!app_table || !app_table->contains("IncludeOptions"))
    {
        return false;
    }

    const toml::node *node = app_table->get("IncludeOptions");
    if (!node || !node->is_boolean())
    {
        throw ConfigException("Application.IncludeOptions must be a boolean");
    }
    return node->as_boolean()->get();
}

std::filesystem::path parse_flow_path(const toml::table *app_table)
{
    if (!app_table || !app_table->contains("FlowPath"))
    {
        return std::filesystem::path("./flow");
    }

    const toml::node *node = app_table->get("FlowPath");
    if (!node || !node->is_string())
    {
        throw ConfigException("Application.FlowPath must be a string");
    }
    return std::filesystem::path(node->as_string()->get());
}

std::string parse_subscription_list(const toml::table *app_table, toml::node_view<const toml::node> front_node)
{
    if (app_table && app_table->contains("SubList"))
    {
        const toml::node *node = app_table->get("SubList");
        if (!node || !node->is_string())
        {
            throw ConfigException("Application.SubList must be a string");
        }
        return node->as_string()->get();
    }

    const toml::table *legacy_front = nullptr;
    if (const auto *front_array = front_node.as_array(); front_array && !front_array->empty())
    {
        legacy_front = (*front_array)[0].as_table();
    }
    else
    {
        legacy_front = front_node.as_table();
    }

    if (legacy_front && legacy_front->contains("SubList"))
    {
        const toml::node *node = legacy_front->get("SubList");
        if (!node || !node->is_string())
        {
            throw ConfigException("Front.SubList must be a string");
        }
        spdlog::warn("Front.SubList is deprecated; move it to Application.SubList");
        return node->as_string()->get();
    }

    return "null";
}

FrontServer build_front_server(const toml::table &table, std::string_view prefix,
                               const EnvCredentialOverrides &overrides)
{
    auto get_required_toml = [&table, prefix](std::string_view key) -> std::string {
        if (!table.contains(key))
        {
            throw ConfigException(std::format("Missing required field: {}.{}", prefix, key));
        }
        const toml::node *node = table.get(key);
        if (!node || !node->is_string())
        {
            throw ConfigException(std::format("Invalid field type for {}.{}: expected string", prefix, key));
        }
        return node->as_string()->get();
    };

    auto get_overridable = [&](std::string_view key, const std::optional<std::string> &override_val) -> std::string {
        if (override_val.has_value())
        {
            return *override_val;
        }
        return get_required_toml(key);
    };

    return FrontServer::Builder()
        .md_url(get_required_toml("MD_Url"))
        .td_url(get_required_toml("TD_Url"))
        .broker_id(get_overridable("BrokerID", overrides.broker_id))
        .user_id(get_overridable("UserID", overrides.user_id))
        .password(get_overridable("Password", overrides.password))
        .user_product_info(get_required_toml("UserProductInfo"))
        .auth_code(get_overridable("AuthCode", overrides.auth_code))
        .app_id(get_overridable("AppID", overrides.app_id))
        .build();
}

std::vector<FrontServer> parse_front_servers(toml::node_view<const toml::node> front_node,
                                             const EnvCredentialOverrides &overrides)
{
    std::vector<FrontServer> servers;

    if (const auto *front_array = front_node.as_array())
    {
        if (front_array->empty())
        {
            throw ConfigException("Front server list is empty");
        }

        servers.reserve(front_array->size());
        for (size_t i = 0; i < front_array->size(); ++i)
        {
            const auto *server_table = (*front_array)[i].as_table();
            if (!server_table)
            {
                throw ConfigException(std::format("Invalid Front[{}] configuration format", i));
            }

            try
            {
                auto server = build_front_server(*server_table, std::format("Front[{}]", i), overrides);
                if (!servers.empty())
                {
                    const auto &baseline = servers.front();
                    const bool same_account =
                        server.broker_id() == baseline.broker_id() && server.user_id() == baseline.user_id() &&
                        server.password() == baseline.password() &&
                        server.user_product_info() == baseline.user_product_info() &&
                        server.auth_code() == baseline.auth_code() && server.app_id() == baseline.app_id();
                    if (!same_account)
                    {
                        throw ConfigException(std::format(
                            "Front[{}] account and authentication fields must match Front[0] because Front entries "
                            "are redundant endpoints",
                            i));
                    }
                }
                servers.push_back(std::move(server));
            }
            catch (const ConfigException &e)
            {
                throw ConfigException(std::format("Failed to build front server [{}]: {}", i, e.what()));
            }
        }
        return servers;
    }

    if (const auto *front_table = front_node.as_table())
    {
        try
        {
            servers.push_back(build_front_server(*front_table, "Front", overrides));
        }
        catch (const ConfigException &e)
        {
            throw ConfigException(std::format("Failed to build front server: {}", e.what()));
        }
        return servers;
    }

    throw ConfigException("Invalid Front configuration format");
}

} // namespace

Config::Config(const std::string &config_file)
{
    try
    {
        // Keep the parsed TOML table local so plaintext secrets do not remain in memory.
        const toml::table config = toml::parse_file(config_file);

        const auto front = config["Front"];
        if (!front)
        {
            throw ConfigException("Missing 'Front' section in configuration");
        }

        const auto history = config["History"];
        if (!history || !history.is_table())
        {
            throw ConfigException("Missing 'History' section in configuration");
        }
        const auto &history_table = *history.as_table();

        const auto application = config["Application"];
        if (application && !application.is_table())
        {
            throw ConfigException("Invalid 'Application' section in configuration");
        }
        const toml::table *app_table = application ? application.as_table() : nullptr;

        // Parse and validate History settings
        storage_mode_ = parse_storage_mode(history_table);
        const bool needs_csv = storage_mode_ == StorageMode::CSV || storage_mode_ == StorageMode::HYBRID;
        const bool needs_parquet = storage_mode_ == StorageMode::PARQUET || storage_mode_ == StorageMode::HYBRID;

#ifndef CFMDC_ENABLE_PARQUET
        if (needs_parquet)
        {
            throw ConfigException("History.StorageMode requires a build with Parquet support");
        }
#endif

        if (history_table.contains("CSVPath"))
        {
            const toml::node *node = history_table.get("CSVPath");
            if (!node || !node->is_string())
            {
                throw ConfigException("History.CSVPath must be a string");
            }
            csv_path_template_ = node->as_string()->get();
        }
        if (needs_csv && !csv_path_template_.has_value())
        {
            throw ConfigException("Missing required field: History.CSVPath");
        }

        if (history_table.contains("ParquetPath"))
        {
            const toml::node *node = history_table.get("ParquetPath");
            if (!node || !node->is_string())
            {
                throw ConfigException("History.ParquetPath must be a string");
            }
            parquet_path_template_ = node->as_string()->get();
        }
        if (needs_parquet && !parquet_path_template_.has_value())
        {
            throw ConfigException("Missing required field: History.ParquetPath");
        }

        parquet_row_group_size_ = parse_parquet_row_group_size(history_table);

        // Parse and validate Application settings
        flow_path_ = parse_flow_path(app_table);
        init_timeout_ = parse_init_timeout(app_table);
        include_options_ = parse_include_options(app_table);

        if (app_table && app_table->contains("WorkerThreadCore"))
        {
            worker_thread_core_ =
                parse_worker_core(*app_table->get("WorkerThreadCore"), "Application.WorkerThreadCore");
        }
        else
        {
            worker_thread_core_ = -1;
        }

        if (app_table && app_table->contains("ParquetWorkerThreadCore"))
        {
            parquet_worker_thread_core_ =
                parse_worker_core(*app_table->get("ParquetWorkerThreadCore"), "Application.ParquetWorkerThreadCore");
        }
        else
        {
            parquet_worker_thread_core_ = (worker_thread_core_ == -2) ? -2 : -1;
        }

        subscription_list_ = parse_subscription_list(app_table, front);

        // Parse and validate Front servers with environment-variable credential overrides
        const EnvCredentialOverrides overrides = load_env_credential_overrides();
        front_servers_ = parse_front_servers(front, overrides);

        spdlog::info("Configuration loaded from: {}", config_file);
        spdlog::info("Found {} front server(s)", front_servers_.size());

#ifndef _WIN32
        // The config file may hold account credentials - warn if other users can read it
        struct stat st{};
        if (::stat(config_file.c_str(), &st) == 0 && (st.st_mode & (S_IRGRP | S_IWGRP | S_IROTH | S_IWOTH)) != 0)
        {
            spdlog::warn("Config file {} is accessible by other users (mode {:o}); it contains credentials, "
                         "consider 'chmod 600'",
                         config_file, st.st_mode & 0777);
        }
#endif
    }
    catch (const toml::parse_error &e)
    {
        throw ConfigException(std::format("Failed to parse TOML config file {}: {}", config_file, e.description()));
    }
    catch (const std::exception &e)
    {
        throw ConfigException(std::format("Failed to load config file {}: {}", config_file, e.what()));
    }
}

std::string Config::resolve_path_placeholders(const std::string &path_template, const std::string &trading_day)
{
    if (!is_valid_trading_day(trading_day))
    {
        throw ConfigException(
            std::format("Invalid trading day '{}' for path '{}': expected YYYYMMDD", trading_day, path_template));
    }

    std::string result = path_template;
    const bool has_placeholder = result.contains("{tradingday}") || result.contains("{year}") ||
                                 result.contains("{month}") || result.contains("{day}");
    if (!has_placeholder)
    {
        return result;
    }

    auto replace_all = [&result](std::string_view placeholder, std::string_view value) {
        size_t pos = 0;
        while ((pos = result.find(placeholder, pos)) != std::string::npos)
        {
            result.replace(pos, placeholder.size(), value);
            pos += value.size();
        }
    };

    replace_all("{tradingday}", trading_day);
    replace_all("{year}", std::string_view(trading_day).substr(0, 4));
    replace_all("{month}", std::string_view(trading_day).substr(4, 2));
    replace_all("{day}", std::string_view(trading_day).substr(6, 2));

    return result;
}

std::filesystem::path Config::csv_path(const std::string &trading_day) const
{
    if (!is_valid_trading_day(trading_day))
    {
        throw ConfigException(std::format("Invalid trading day '{}': expected YYYYMMDD", trading_day));
    }
    if (!csv_path_template_.has_value())
    {
        throw ConfigException("Missing or invalid required field: History.CSVPath");
    }
    return std::filesystem::path(resolve_path_placeholders(*csv_path_template_, trading_day));
}

std::filesystem::path Config::parquet_path(const std::string &trading_day) const
{
    if (!is_valid_trading_day(trading_day))
    {
        throw ConfigException(std::format("Invalid trading day '{}': expected YYYYMMDD", trading_day));
    }
    if (!parquet_path_template_.has_value())
    {
        throw ConfigException("Missing or invalid required field: History.ParquetPath");
    }
    return std::filesystem::path(resolve_path_placeholders(*parquet_path_template_, trading_day));
}

} // namespace cfmdc
