#include "cfmdc/config/Config.h"

#include <spdlog/spdlog.h>

#include <algorithm>
#include <cctype>
#include <format>
#include <optional>

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

    // Check history path
    auto history_table = *history.as_table();
    if (!history_table.contains("CSVPath"))
    {
        throw ConfigException("Missing required field: History.CSVPath");
    }

    if (!history_table.contains("ParquetPath"))
    {
        throw ConfigException("Missing required field: History.ParquetPath");
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

        auto sublist = table["SubList"].value<std::string>().value_or("null");
        return FrontServer::Builder()
            .md_url(get_required("MD_Url"))
            .td_url(get_required("TD_Url"))
            .broker_id(get_required("BrokerID"))
            .user_id(get_required("UserID"))
            .password(get_required("Password"))
            .user_product_info(get_required("UserProductInfo"))
            .auth_code(get_required("AuthCode"))
            .app_id(get_required("AppID"))
            .subscription_list(sublist)
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
    if (!front_servers_.empty())
    {
        return front_servers_[0].subscription_list();
    }
    return "null";
}

std::string Config::resolve_path_placeholders(const std::string &path_template, const std::string &trading_day) const
{
    std::string result = path_template;

    // Validate trading_day format (should be YYYYMMDD)
    if (trading_day.length() == 8)
    {
        std::string year = trading_day.substr(0, 4);
        std::string month = trading_day.substr(4, 2);
        std::string day = trading_day.substr(6, 2);

        // Replace placeholders
        size_t pos = 0;
        while ((pos = result.find("{tradingday}", pos)) != std::string::npos)
        {
            result.replace(pos, 12, trading_day);
            pos += trading_day.length();
        }

        pos = 0;
        while ((pos = result.find("{year}", pos)) != std::string::npos)
        {
            result.replace(pos, 6, year);
            pos += year.length();
        }

        pos = 0;
        while ((pos = result.find("{month}", pos)) != std::string::npos)
        {
            result.replace(pos, 7, month);
            pos += month.length();
        }

        pos = 0;
        while ((pos = result.find("{day}", pos)) != std::string::npos)
        {
            result.replace(pos, 5, day);
            pos += day.length();
        }
    }

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
    std::transform(mode_str.begin(), mode_str.end(), mode_str.begin(), [](unsigned char c) { return std::tolower(c); });

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

} // namespace cfmdc
