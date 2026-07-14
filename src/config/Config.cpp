#include "cfmdc/config/Config.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cctype>
#include <format>
#include <optional>
#include <ranges>

#ifndef _WIN32
#include <sys/stat.h>
#endif

#include "cfmdc/utils/Error.h"

namespace cfmdc
{

Config::Config(const std::string &config_file)
{
    try
    {
        config_ = toml::parse_file(config_file);
        validate_config();
        parse_front_servers();
        spdlog::info("Configuration loaded from: {}", config_file);
        spdlog::info("Found {} front server(s)", front_servers_.size());

#ifndef _WIN32
        // The config file holds account credentials - warn if other users can read it
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

void Config::validate_config()
{
    // Validate required sections
    auto front = config_["Front"];
    if (!front)
    {
        throw ConfigException("Missing 'Front' section in configuration");
    }

    auto history = config_["History"];
    if (!history || !history.is_table())
    {
        throw ConfigException("Missing 'History' section in configuration");
    }

    auto application = config_["Application"];
    if (application && !application.is_table())
    {
        throw ConfigException("Invalid 'Application' section in configuration");
    }
    if (auto app = application.as_table(); app && app->contains("SubList") && !(*app)["SubList"].is_string())
    {
        throw ConfigException("Application.SubList must be a string");
    }

    // Check history paths - only the ones the configured storage mode actually uses
    auto history_table = *history.as_table();
    const auto mode = storage_mode();
    bool needs_csv = mode == StorageMode::CSV || mode == StorageMode::HYBRID;
    bool needs_parquet = mode == StorageMode::PARQUET || mode == StorageMode::HYBRID;
#ifndef CFMDC_ENABLE_PARQUET
    if (needs_parquet)
    {
        // AsyncFileManager falls back to CSV when Parquet support is not compiled in
        needs_csv = true;
        needs_parquet = false;
    }
#endif

    if (needs_csv && !history_table.contains("CSVPath"))
    {
        throw ConfigException("Missing required field: History.CSVPath");
    }

    if (needs_parquet && !history_table.contains("ParquetPath"))
    {
        throw ConfigException("Missing required field: History.ParquetPath");
    }
    if (needs_parquet)
    {
        (void)parquet_row_group_size();
    }

    // Validate Front configuration - support both single and multiple servers
    auto required_fields = std::vector<std::string>{"MD_Url",   "TD_Url",          "BrokerID", "UserID",
                                                    "Password", "UserProductInfo", "AuthCode", "AppID"};

    // Front can be an array of tables (multiple servers)
    if (auto front_array = front.as_array())
    {
        if (front_array->empty())
        {
            throw ConfigException("Front server list is empty");
        }

        for (size_t i = 0; i < front_array->size(); ++i)
        {
            auto *server = (*front_array)[i].as_table();
            if (!server)
            {
                throw ConfigException(std::format("Invalid Front[{}] configuration format", i));
            }

            for (const auto &field : required_fields)
            {
                if (!server->contains(field))
                {
                    throw ConfigException(std::format("Missing required field: Front[{}].{}", i, field));
                }
            }
        }
    }
    // Front can also be a single table (backward compatibility)
    else if (auto front_table = front.as_table())
    {
        for (const auto &field : required_fields)
        {
            if (!front_table->contains(field))
            {
                throw ConfigException(std::format("Missing required field: Front.{}", field));
            }
        }
    }
    else
    {
        throw ConfigException("Invalid Front configuration format");
    }
}

void Config::parse_front_servers()
{
    auto front = config_["Front"];
    auto build_server = [](const toml::table &table) {
        auto get_required = [&table](std::string_view key) {
            auto value = table[key].value<std::string>();
            if (!value)
            {
                throw ConfigException(std::format("Missing or invalid required field: {}", key));
            }
            return *value;
        };

        return FrontServer::Builder()
            .md_url(get_required("MD_Url"))
            .td_url(get_required("TD_Url"))
            .broker_id(get_required("BrokerID"))
            .user_id(get_required("UserID"))
            .password(get_required("Password"))
            .user_product_info(get_required("UserProductInfo"))
            .auth_code(get_required("AuthCode"))
            .app_id(get_required("AppID"))
            .build();
    };

    if (auto front_array = front.as_array())
    {
        for (size_t i = 0; i < front_array->size(); ++i)
        {
            auto *server_table = (*front_array)[i].as_table();
            if (!server_table)
            {
                throw ConfigException(std::format("Invalid Front[{}] configuration format", i));
            }

            try
            {
                front_servers_.push_back(build_server(*server_table));
            }
            catch (const ConfigException &e)
            {
                throw ConfigException(std::format("Failed to build front server [{}]: {}", i, e.what()));
            }
        }
        return;
    }

    auto *front_table = front.as_table();
    if (!front_table)
    {
        throw ConfigException("Invalid Front configuration format");
    }

    try
    {
        front_servers_.push_back(build_server(*front_table));
    }
    catch (const ConfigException &e)
    {
        throw ConfigException(std::format("Failed to build front server: {}", e.what()));
    }
}

std::string Config::subscription_list() const
{
    if (auto app = config_["Application"].as_table())
    {
        if (auto sub_list = (*app)["SubList"].value<std::string>())
        {
            return *sub_list;
        }
        if ((*app).contains("SubList"))
        {
            throw ConfigException("Application.SubList must be a string");
        }
    }

    // Backward-compatible migration path for configurations created before
    // SubList became an application-wide option.
    const toml::table *legacy_front = nullptr;
    auto front = config_["Front"];
    if (auto front_array = front.as_array(); front_array && !front_array->empty())
    {
        legacy_front = (*front_array)[0].as_table();
    }
    else
    {
        legacy_front = front.as_table();
    }

    if (legacy_front && legacy_front->contains("SubList"))
    {
        if (auto sub_list = (*legacy_front)["SubList"].value<std::string>())
        {
            spdlog::warn("Front.SubList is deprecated; move it to Application.SubList");
            return *sub_list;
        }
        throw ConfigException("Front.SubList must be a string");
    }

    return "null";
}

std::string Config::resolve_path_placeholders(const std::string &path_template, const std::string &trading_day) const
{
    std::string result = path_template;

    const bool has_placeholder = result.contains("{tradingday}") || result.contains("{year}") ||
                                 result.contains("{month}") || result.contains("{day}");
    if (!has_placeholder)
    {
        return result;
    }

    // A path with placeholders requires a valid YYYYMMDD trading day; failing loudly
    // beats silently creating a directory literally named "{tradingday}".
    const bool valid_trading_day =
        trading_day.length() == 8 &&
        std::ranges::all_of(trading_day, [](unsigned char c) { return std::isdigit(c) != 0; });
    if (!valid_trading_day)
    {
        throw ConfigException(
            std::format("Cannot resolve placeholders in '{}': invalid trading day '{}'", path_template, trading_day));
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
    auto history = config_["History"].as_table();
    auto path_template = history ? (*history)["CSVPath"].value<std::string>() : std::nullopt;
    if (!path_template)
    {
        throw ConfigException("Missing or invalid required field: History.CSVPath");
    }
    return std::filesystem::path(resolve_path_placeholders(*path_template, trading_day));
}

std::filesystem::path Config::parquet_path(const std::string &trading_day) const
{
    auto history = config_["History"].as_table();
    auto path_template = history ? (*history)["ParquetPath"].value<std::string>() : std::nullopt;
    if (!path_template)
    {
        throw ConfigException("Missing or invalid required field: History.ParquetPath");
    }
    return std::filesystem::path(resolve_path_placeholders(*path_template, trading_day));
}

std::filesystem::path Config::flow_path() const
{
    // Return configured path or default to "./flow"
    if (auto app = config_["Application"].as_table())
    {
        if (auto flow = (*app)["FlowPath"].value<std::string>())
        {
            return std::filesystem::path(*flow);
        }
    }
    return std::filesystem::path("./flow");
}

int Config::init_timeout() const
{
    // Default timeout is 60 seconds if not specified
    if (auto app = config_["Application"].as_table())
    {
        if (auto timeout = (*app)["InitTimeout"].value<int64_t>())
        {
            return static_cast<int>(*timeout);
        }
    }
    return 60;
}

int Config::worker_thread_core() const
{
    // Default is -1 (no affinity)
    if (auto app = config_["Application"].as_table())
    {
        auto node = (*app)["WorkerThreadCore"];
        if (node.is_string())
        {
            std::string val = *node.value<std::string>();
            std::ranges::transform(val, val.begin(), [](unsigned char c) { return std::tolower(c); });
            if (val == "auto")
            {
                return -2; // Sentinel for auto
            }
        }
        else if (auto core = node.value<int64_t>())
        {
            return static_cast<int>(*core);
        }
    }
    return -1;
}

StorageMode Config::storage_mode() const
{
    // Default to CSV if not specified
    auto history = config_["History"].as_table();
    if (!history || !(*history)["StorageMode"])
    {
        return StorageMode::CSV;
    }

    auto mode_value = (*history)["StorageMode"].value<std::string>();
    if (!mode_value)
    {
        return StorageMode::CSV;
    }
    std::string mode_str = *mode_value;

    // Convert to lowercase for case-insensitive comparison
    std::ranges::transform(mode_str, mode_str.begin(), [](unsigned char c) { return std::tolower(c); });

    if (mode_str == "parquet")
    {
        return StorageMode::PARQUET;
    }
    else if (mode_str == "hybrid")
    {
        return StorageMode::HYBRID;
    }
    else
    {
        if (mode_str != "csv")
        {
            spdlog::warn("Unknown storage mode '{}', defaulting to CSV", mode_str);
        }
        return StorageMode::CSV;
    }
}

size_t Config::parquet_row_group_size() const
{
    constexpr int64_t kDefaultRowGroupSize = 100000;
    constexpr int64_t kMaxRowGroupSize = 1000000;

    auto history = config_["History"].as_table();
    if (!history || !history->contains("ParquetRowGroupSize"))
    {
        return kDefaultRowGroupSize;
    }

    auto value = (*history)["ParquetRowGroupSize"].value<int64_t>();
    if (!value || *value <= 0 || *value > kMaxRowGroupSize)
    {
        throw ConfigException(
            std::format("History.ParquetRowGroupSize must be an integer between 1 and {}", kMaxRowGroupSize));
    }
    return static_cast<size_t>(*value);
}

} // namespace cfmdc
