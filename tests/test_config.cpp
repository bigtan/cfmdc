#include <atomic>
#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <format>
#include <fstream>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#ifdef _WIN32
#include <process.h>
#else
#include <unistd.h>
#endif

#include "cfmdc/config/Config.h"
#include "cfmdc/utils/Error.h"

using namespace cfmdc;

namespace
{

class TempConfigFile
{
  public:
    explicit TempConfigFile(std::string_view content, std::string_view tag = "cfg")
    {
        static std::atomic<uint64_t> counter{0};
        const uint64_t seq = counter.fetch_add(1, std::memory_order_relaxed);
#ifdef _WIN32
        const unsigned long pid = static_cast<unsigned long>(_getpid());
#else
        const unsigned long pid = static_cast<unsigned long>(::getpid());
#endif
        const uint64_t ticks = static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count());
        path_ = std::filesystem::temp_directory_path() /
                std::format("cfmdc_test_{}_{}_{}_{}.toml", tag, pid, seq, ticks);

        std::ofstream out(path_, std::ios::binary | std::ios::trunc);
        REQUIRE(out.is_open());
        out << content;
        out.flush();
        REQUIRE(out.good());
    }

    ~TempConfigFile()
    {
        std::error_code ec;
        std::filesystem::remove(path_, ec);
    }

    TempConfigFile(const TempConfigFile &) = delete;
    TempConfigFile &operator=(const TempConfigFile &) = delete;

    std::string path_string() const
    {
        return path_.string();
    }

  private:
    std::filesystem::path path_;
};

class ScopedEnvOverrides
{
  public:
    explicit ScopedEnvOverrides(std::vector<const char *> names)
    {
        saved_.reserve(names.size());
        for (const char *name : names)
        {
            saved_.emplace_back(name, read_env(name));
        }
    }

    ~ScopedEnvOverrides()
    {
        for (const auto &[name, original] : saved_)
        {
            if (original.has_value())
            {
                apply_env(name.c_str(), original->c_str());
            }
            else
            {
                clear_env(name.c_str());
            }
        }
    }

    ScopedEnvOverrides(const ScopedEnvOverrides &) = delete;
    ScopedEnvOverrides &operator=(const ScopedEnvOverrides &) = delete;

    void set(const char *name, const char *value)
    {
        apply_env(name, value);
    }

    void unset(const char *name)
    {
        clear_env(name);
    }

  private:
    static std::optional<std::string> read_env(const char *name)
    {
#ifdef _WIN32
        char *raw = nullptr;
        size_t len = 0;
        if (_dupenv_s(&raw, &len, name) != 0 || raw == nullptr)
        {
            return std::nullopt;
        }
        std::unique_ptr<char, decltype(&std::free)> guard(raw, &std::free);
        return std::string(raw);
#else
        const char *raw = std::getenv(name);
        if (raw == nullptr)
        {
            return std::nullopt;
        }
        return std::string(raw);
#endif
    }

    static void apply_env(const char *name, const char *value)
    {
#ifdef _WIN32
        REQUIRE(_putenv_s(name, value) == 0);
#else
        REQUIRE(setenv(name, value, 1) == 0);
#endif
    }

    static void clear_env(const char *name)
    {
#ifdef _WIN32
        REQUIRE(_putenv_s(name, "") == 0);
#else
        REQUIRE(unsetenv(name) == 0);
#endif
    }

    std::vector<std::pair<std::string, std::optional<std::string>>> saved_;
};

} // namespace

TEST_CASE("Config loads valid TOML", "[config]")
{
    TempConfigFile test_config(R"(
[Front]
MD_Url = "tcp://test.com:10131"
TD_Url = "tcp://test.com:10130"
BrokerID = "9999"
UserID = "test_user"
Password = "test_pass"
UserProductInfo = "test_product"
AuthCode = "test_auth"
AppID = "test_app"

[History]
CSVPath = "./test_data/csv"
ParquetPath = "./test_data/parquet"
StorageMode = "CSV"

[Application]
InitTimeout = 30
SubList = "null"
)");

    SECTION("Valid config loads successfully")
    {
        Config config(test_config.path_string());
        REQUIRE(config.front_servers().size() == 1);
        REQUIRE(config.front_servers()[0].md_url() == "tcp://test.com:10131");
        REQUIRE(config.front_servers()[0].broker_id() == "9999");
        REQUIRE(config.init_timeout() == 30);
    }
}

TEST_CASE("Config validates required fields", "[config]")
{
    SECTION("Missing Front section")
    {
        TempConfigFile test_config(R"(
[History]
CSVPath = "./test_data/csv"
ParquetPath = "./test_data/parquet"
)");
        REQUIRE_THROWS_AS(Config(test_config.path_string()), ConfigException);
    }

    SECTION("Missing required field")
    {
        TempConfigFile test_config(R"(
[Front]
MD_Url = "tcp://test.com:10131"

[History]
CSVPath = "./test_data/csv"
ParquetPath = "./test_data/parquet"
)");
        REQUIRE_THROWS_AS(Config(test_config.path_string()), ConfigException);
    }

    SECTION("Application SubList must be a string")
    {
        TempConfigFile test_config(R"(
[Front]
MD_Url = "tcp://test.com:10131"
TD_Url = "tcp://test.com:10130"
BrokerID = "9999"
UserID = "test_user"
Password = "test_pass"
UserProductInfo = "test_product"
AuthCode = "test_auth"
AppID = "test_app"

[History]
CSVPath = "./test_data/csv"
StorageMode = "CSV"

[Application]
SubList = 123
)");
        REQUIRE_THROWS_AS(Config(test_config.path_string()), ConfigException);
    }
}

TEST_CASE("Config supports multiple servers", "[config]")
{
    TempConfigFile test_config(R"(
[[Front]]
MD_Url = "tcp://server1.com:10131"
TD_Url = "tcp://server1.com:10130"
BrokerID = "9999"
UserID = "test_user"
Password = "test_pass"
UserProductInfo = "test_product"
AuthCode = "test_auth"
AppID = "test_app"

[[Front]]
MD_Url = "tcp://server2.com:10131"
TD_Url = "tcp://server2.com:10130"
BrokerID = "9999"
UserID = "test_user"
Password = "test_pass"
UserProductInfo = "test_product"
AuthCode = "test_auth"
AppID = "test_app"

[History]
CSVPath = "./test_data/csv"
ParquetPath = "./test_data/parquet"
StorageMode = "CSV"

[Application]
SubList = "rb2505|IC2501"
)");

    Config config(test_config.path_string());
    REQUIRE(config.front_servers().size() == 2);
    REQUIRE(config.front_servers()[0].md_url() == "tcp://server1.com:10131");
    REQUIRE(config.front_servers()[1].md_url() == "tcp://server2.com:10131");
    REQUIRE(config.subscription_list() == "rb2505|IC2501");
}

TEST_CASE("Config rejects different accounts across redundant fronts", "[config]")
{
    TempConfigFile test_config(R"(
[[Front]]
MD_Url = "tcp://server1.com:10131"
TD_Url = "tcp://server1.com:10130"
BrokerID = "9999"
UserID = "account_a"
Password = "test_pass"
UserProductInfo = "test_product"
AuthCode = "test_auth"
AppID = "test_app"

[[Front]]
MD_Url = "tcp://server2.com:10131"
TD_Url = "tcp://server2.com:10130"
BrokerID = "9999"
UserID = "account_b"
Password = "test_pass"
UserProductInfo = "test_product"
AuthCode = "test_auth"
AppID = "test_app"

[History]
CSVPath = "./test_data/csv"
StorageMode = "CSV"
)");

    REQUIRE_THROWS_AS(Config(test_config.path_string()), ConfigException);
}

TEST_CASE("Config supports environment variable credential overrides", "[config][env]")
{
    ScopedEnvOverrides env_guard(
        {"CFMDC_BROKER_ID", "CFMDC_USER_ID", "CFMDC_PASSWORD", "CFMDC_AUTH_CODE", "CFMDC_APP_ID"});
    env_guard.unset("CFMDC_BROKER_ID");
    env_guard.unset("CFMDC_USER_ID");
    env_guard.unset("CFMDC_PASSWORD");
    env_guard.unset("CFMDC_AUTH_CODE");
    env_guard.unset("CFMDC_APP_ID");

    SECTION("Overrides allow TOML to omit or leave empty overridden credential fields")
    {
        TempConfigFile test_config(R"(
[Front]
MD_Url = "tcp://test.com:10131"
TD_Url = "tcp://test.com:10130"
BrokerID = ""
UserProductInfo = "test_product"
AuthCode = ""

[History]
CSVPath = "./data/csv"
StorageMode = "CSV"
)");

        env_guard.set("CFMDC_BROKER_ID", "8888");
        env_guard.set("CFMDC_USER_ID", "env_user");
        env_guard.set("CFMDC_PASSWORD", "env_secret_pw");
        env_guard.set("CFMDC_AUTH_CODE", "env_auth");
        env_guard.set("CFMDC_APP_ID", "env_app");

        Config config(test_config.path_string());
        REQUIRE(config.front_servers().size() == 1);
        const auto &server = config.front_servers()[0];
        REQUIRE(server.broker_id() == "8888");
        REQUIRE(server.user_id() == "env_user");
        REQUIRE(server.password() == "env_secret_pw");
        REQUIRE(server.auth_code() == "env_auth");
        REQUIRE(server.app_id() == "env_app");
    }

    SECTION("Overrides apply across all Front entries before validation and account consistency checks")
    {
        TempConfigFile test_config(R"(
[[Front]]
MD_Url = "tcp://server1.com:10131"
TD_Url = "tcp://server1.com:10130"
BrokerID = "9999"
UserID = "user_a"
Password = ""
UserProductInfo = "test_product"
AuthCode = "toml_auth"
AppID = "toml_app"

[[Front]]
MD_Url = "tcp://server2.com:10131"
TD_Url = "tcp://server2.com:10130"
BrokerID = "9999"
UserID = "user_b"
UserProductInfo = "test_product"
AuthCode = "toml_auth"
AppID = "toml_app"

[History]
CSVPath = "./data/csv"
StorageMode = "CSV"
)");

        env_guard.set("CFMDC_USER_ID", "unified_user");
        env_guard.set("CFMDC_PASSWORD", "unified_secret");

        Config config(test_config.path_string());
        REQUIRE(config.front_servers().size() == 2);
        for (const auto &server : config.front_servers())
        {
            REQUIRE(server.user_id() == "unified_user");
            REQUIRE(server.password() == "unified_secret");
            REQUIRE(server.broker_id() == "9999");
        }
    }

#ifndef _WIN32
    SECTION("Empty environment variable does not override TOML on POSIX")
    {
        TempConfigFile test_config(R"(
[Front]
MD_Url = "tcp://test.com:10131"
TD_Url = "tcp://test.com:10130"
BrokerID = "9999"
UserID = "toml_user"
Password = "toml_password"
UserProductInfo = "test_product"
AuthCode = "toml_auth"
AppID = "toml_app"

[History]
CSVPath = "./data/csv"
StorageMode = "CSV"
)");

        env_guard.set("CFMDC_PASSWORD", "");
        Config config(test_config.path_string());
        REQUIRE(config.front_servers()[0].password() == "toml_password");
    }
#endif
}

TEST_CASE("Config resolves path placeholders and validates trading_day unconditionally", "[config]")
{
    SECTION("Resolves valid trading_day with placeholders")
    {
        TempConfigFile test_config(R"(
[Front]
MD_Url = "tcp://test.com:10131"
TD_Url = "tcp://test.com:10130"
BrokerID = "9999"
UserID = "test_user"
Password = "test_pass"
UserProductInfo = "test_product"
AuthCode = "test_auth"
AppID = "test_app"

[History]
CSVPath = "./data/{year}/{month}/csv/{tradingday}"
ParquetPath = "./data/{year}/{month}/parquet/{tradingday}"
StorageMode = "CSV"
)");

        Config config(test_config.path_string());
        auto csv_path = config.csv_path("20250110");
        auto parquet_path = config.parquet_path("20250110");
        REQUIRE(csv_path.generic_string() == "./data/2025/01/csv/20250110");
        REQUIRE(parquet_path.generic_string() == "./data/2025/01/parquet/20250110");
    }

    SECTION("Rejects malformed trading_day even when path templates contain no placeholders")
    {
        TempConfigFile test_config(R"(
[Front]
MD_Url = "tcp://test.com:10131"
TD_Url = "tcp://test.com:10130"
BrokerID = "9999"
UserID = "test_user"
Password = "test_pass"
UserProductInfo = "test_product"
AuthCode = "test_auth"
AppID = "test_app"

[History]
CSVPath = "./data/csv"
ParquetPath = "./data/parquet"
StorageMode = "CSV"
)");

        Config config(test_config.path_string());
        REQUIRE(config.csv_path("20250110").generic_string() == "./data/csv");
        REQUIRE(config.parquet_path("20250110").generic_string() == "./data/parquet");

        for (const std::string bad_day : {"", "2025011", "202501101", "2025-01-10", "abcd1234", "../20250"})
        {
            REQUIRE_THROWS_AS(config.csv_path(bad_day), ConfigException);
            REQUIRE_THROWS_AS(config.parquet_path(bad_day), ConfigException);
        }
    }
}

TEST_CASE("Config validates InitTimeout, WorkerThreadCore, ParquetWorkerThreadCore, and IncludeOptions", "[config]")
{
    SECTION("Valid InitTimeout boundaries")
    {
        TempConfigFile min_cfg(R"(
[Front]
MD_Url = "tcp://test.com:10131"
TD_Url = "tcp://test.com:10130"
BrokerID = "9999"
UserID = "test_user"
Password = "test_pass"
UserProductInfo = "test_product"
AuthCode = "test_auth"
AppID = "test_app"

[History]
CSVPath = "./data/csv"
StorageMode = "CSV"

[Application]
InitTimeout = 1
)");
        REQUIRE(Config(min_cfg.path_string()).init_timeout() == 1);

        TempConfigFile max_cfg(R"(
[Front]
MD_Url = "tcp://test.com:10131"
TD_Url = "tcp://test.com:10130"
BrokerID = "9999"
UserID = "test_user"
Password = "test_pass"
UserProductInfo = "test_product"
AuthCode = "test_auth"
AppID = "test_app"

[History]
CSVPath = "./data/csv"
StorageMode = "CSV"

[Application]
InitTimeout = 3600
)");
        REQUIRE(Config(max_cfg.path_string()).init_timeout() == 3600);
    }

    SECTION("Invalid InitTimeout throws ConfigException")
    {
        for (const std::string_view value : {"0", "-5", "3601", "\"60\"", "30.5", "true"})
        {
            TempConfigFile bad_cfg(std::format(R"(
[Front]
MD_Url = "tcp://test.com:10131"
TD_Url = "tcp://test.com:10130"
BrokerID = "9999"
UserID = "test_user"
Password = "test_pass"
UserProductInfo = "test_product"
AuthCode = "test_auth"
AppID = "test_app"

[History]
CSVPath = "./data/csv"
StorageMode = "CSV"

[Application]
InitTimeout = {}
)",
                                               value));
            REQUIRE_THROWS_AS(Config(bad_cfg.path_string()), ConfigException);
        }
    }

    SECTION("WorkerThreadCore and ParquetWorkerThreadCore defaults and valid values")
    {
        TempConfigFile auto_both(R"(
[Front]
MD_Url = "tcp://test.com:10131"
TD_Url = "tcp://test.com:10130"
BrokerID = "9999"
UserID = "test_user"
Password = "test_pass"
UserProductInfo = "test_product"
AuthCode = "test_auth"
AppID = "test_app"

[History]
CSVPath = "./data/csv"
StorageMode = "CSV"

[Application]
WorkerThreadCore = "AuTo"
)");
        Config cfg_auto(auto_both.path_string());
        REQUIRE(cfg_auto.worker_thread_core() == -2);
        REQUIRE(cfg_auto.parquet_worker_thread_core() == -2);

        TempConfigFile auto_ingest_disabled_parquet(R"(
[Front]
MD_Url = "tcp://test.com:10131"
TD_Url = "tcp://test.com:10130"
BrokerID = "9999"
UserID = "test_user"
Password = "test_pass"
UserProductInfo = "test_product"
AuthCode = "test_auth"
AppID = "test_app"

[History]
CSVPath = "./data/csv"
StorageMode = "CSV"

[Application]
WorkerThreadCore = "auto"
ParquetWorkerThreadCore = -1
)");
        Config cfg_override(auto_ingest_disabled_parquet.path_string());
        REQUIRE(cfg_override.worker_thread_core() == -2);
        REQUIRE(cfg_override.parquet_worker_thread_core() == -1);

        TempConfigFile manual_cores(R"(
[Front]
MD_Url = "tcp://test.com:10131"
TD_Url = "tcp://test.com:10130"
BrokerID = "9999"
UserID = "test_user"
Password = "test_pass"
UserProductInfo = "test_product"
AuthCode = "test_auth"
AppID = "test_app"

[History]
CSVPath = "./data/csv"
StorageMode = "CSV"

[Application]
WorkerThreadCore = 2
ParquetWorkerThreadCore = "auto"
)");
        Config cfg_manual(manual_cores.path_string());
        REQUIRE(cfg_manual.worker_thread_core() == 2);
        REQUIRE(cfg_manual.parquet_worker_thread_core() == -2);

        TempConfigFile manual_both(R"(
[Front]
MD_Url = "tcp://test.com:10131"
TD_Url = "tcp://test.com:10130"
BrokerID = "9999"
UserID = "test_user"
Password = "test_pass"
UserProductInfo = "test_product"
AuthCode = "test_auth"
AppID = "test_app"

[History]
CSVPath = "./data/csv"
StorageMode = "CSV"

[Application]
WorkerThreadCore = 1
ParquetWorkerThreadCore = 3
)");
        Config cfg_manual_both(manual_both.path_string());
        REQUIRE(cfg_manual_both.worker_thread_core() == 1);
        REQUIRE(cfg_manual_both.parquet_worker_thread_core() == 3);
    }

    SECTION("Invalid WorkerThreadCore or ParquetWorkerThreadCore throws ConfigException")
    {
        for (const std::string_view value : {"\"atuo\"", "\"\"", "-2", "-99", "1.5", "true"})
        {
            TempConfigFile bad_worker(std::format(R"(
[Front]
MD_Url = "tcp://test.com:10131"
TD_Url = "tcp://test.com:10130"
BrokerID = "9999"
UserID = "test_user"
Password = "test_pass"
UserProductInfo = "test_product"
AuthCode = "test_auth"
AppID = "test_app"

[History]
CSVPath = "./data/csv"
StorageMode = "CSV"

[Application]
WorkerThreadCore = {}
)",
                                                  value));
            REQUIRE_THROWS_AS(Config(bad_worker.path_string()), ConfigException);

            TempConfigFile bad_parquet_worker(std::format(R"(
[Front]
MD_Url = "tcp://test.com:10131"
TD_Url = "tcp://test.com:10130"
BrokerID = "9999"
UserID = "test_user"
Password = "test_pass"
UserProductInfo = "test_product"
AuthCode = "test_auth"
AppID = "test_app"

[History]
CSVPath = "./data/csv"
StorageMode = "CSV"

[Application]
ParquetWorkerThreadCore = {}
)",
                                                          value));
            REQUIRE_THROWS_AS(Config(bad_parquet_worker.path_string()), ConfigException);
        }
    }

    SECTION("IncludeOptions parses bool and rejects non-bool values")
    {
        TempConfigFile enabled_cfg(R"(
[Front]
MD_Url = "tcp://test.com:10131"
TD_Url = "tcp://test.com:10130"
BrokerID = "9999"
UserID = "test_user"
Password = "test_pass"
UserProductInfo = "test_product"
AuthCode = "test_auth"
AppID = "test_app"

[History]
CSVPath = "./data/csv"
StorageMode = "CSV"

[Application]
IncludeOptions = true
)");
        REQUIRE(Config(enabled_cfg.path_string()).include_options() == true);

        for (const std::string_view bad_val : {"\"true\"", "1", "0"})
        {
            TempConfigFile bad_opt(std::format(R"(
[Front]
MD_Url = "tcp://test.com:10131"
TD_Url = "tcp://test.com:10130"
BrokerID = "9999"
UserID = "test_user"
Password = "test_pass"
UserProductInfo = "test_product"
AuthCode = "test_auth"
AppID = "test_app"

[History]
CSVPath = "./data/csv"
StorageMode = "CSV"

[Application]
IncludeOptions = {}
)",
                                               bad_val));
            REQUIRE_THROWS_AS(Config(bad_opt.path_string()), ConfigException);
        }
    }
}

TEST_CASE("Config defaults and storage mode parsing", "[config]")
{
    SECTION("Defaults when Application section is missing")
    {
        TempConfigFile test_config(R"(
[Front]
MD_Url = "tcp://test.com:10131"
TD_Url = "tcp://test.com:10130"
BrokerID = "9999"
UserID = "test_user"
Password = "test_pass"
UserProductInfo = "test_product"
AuthCode = "test_auth"
AppID = "test_app"

[History]
CSVPath = "./data/csv"
ParquetPath = "./data/parquet"
)");

        Config config(test_config.path_string());
        REQUIRE(config.init_timeout() == 60);
        REQUIRE(config.flow_path().generic_string() == "./flow");
        REQUIRE(config.subscription_list() == "null");
        REQUIRE(config.worker_thread_core() == -1);
        REQUIRE(config.parquet_worker_thread_core() == -1);
        REQUIRE(config.include_options() == false);
        REQUIRE(config.parquet_row_group_size() == 200000);
    }

    SECTION("Application options are read when explicitly configured")
    {
        TempConfigFile test_config(R"(
[Front]
MD_Url = "tcp://test.com:10131"
TD_Url = "tcp://test.com:10130"
BrokerID = "9999"
UserID = "test_user"
Password = "test_pass"
UserProductInfo = "test_product"
AuthCode = "test_auth"
AppID = "test_app"

[History]
CSVPath = "./data/csv"
ParquetPath = "./data/parquet"
StorageMode = "CSV"
ParquetRowGroupSize = 4096

[Application]
FlowPath = "./custom-flow"
InitTimeout = 90
SubList = "rb2505|IC2501"
IncludeOptions = true
WorkerThreadCore = 2
)");

        Config config(test_config.path_string());
        REQUIRE(config.flow_path().generic_string() == "./custom-flow");
        REQUIRE(config.init_timeout() == 90);
        REQUIRE(config.subscription_list() == "rb2505|IC2501");
        REQUIRE(config.include_options() == true);
        REQUIRE(config.worker_thread_core() == 2);
        REQUIRE(config.parquet_worker_thread_core() == -1);
        REQUIRE(config.storage_mode() == StorageMode::CSV);
        REQUIRE(config.parquet_row_group_size() == 4096);
    }

    SECTION("Legacy Front SubList remains a compatibility fallback")
    {
        TempConfigFile test_config(R"(
[Front]
MD_Url = "tcp://test.com:10131"
TD_Url = "tcp://test.com:10130"
BrokerID = "9999"
UserID = "test_user"
Password = "test_pass"
UserProductInfo = "test_product"
AuthCode = "test_auth"
AppID = "test_app"
SubList = "rb2505|IC2501"

[History]
CSVPath = "./data/csv"
ParquetPath = "./data/parquet"
StorageMode = "CSV"
)");

        Config config(test_config.path_string());
        REQUIRE(config.subscription_list() == "rb2505|IC2501");
    }

    SECTION("StorageMode is case-insensitive and rejects unknown values")
    {
        TempConfigFile test_config(R"(
[Front]
MD_Url = "tcp://test.com:10131"
TD_Url = "tcp://test.com:10130"
BrokerID = "9999"
UserID = "test_user"
Password = "test_pass"
UserProductInfo = "test_product"
AuthCode = "test_auth"
AppID = "test_app"

[History]
CSVPath = "./data/csv"
ParquetPath = "./data/parquet"
StorageMode = "CsV"
)");
        REQUIRE(Config(test_config.path_string()).storage_mode() == StorageMode::CSV);

        TempConfigFile test_config_unknown(R"(
[Front]
MD_Url = "tcp://test.com:10131"
TD_Url = "tcp://test.com:10130"
BrokerID = "9999"
UserID = "test_user"
Password = "test_pass"
UserProductInfo = "test_product"
AuthCode = "test_auth"
AppID = "test_app"

[History]
CSVPath = "./data/csv"
ParquetPath = "./data/parquet"
StorageMode = "not-a-mode"
)");
        REQUIRE_THROWS_AS(Config(test_config_unknown.path_string()), ConfigException);

        TempConfigFile test_config_invalid_type(R"(
[Front]
MD_Url = "tcp://test.com:10131"
TD_Url = "tcp://test.com:10130"
BrokerID = "9999"
UserID = "test_user"
Password = "test_pass"
UserProductInfo = "test_product"
AuthCode = "test_auth"
AppID = "test_app"

[History]
CSVPath = "./data/csv"
StorageMode = 1
)");
        REQUIRE_THROWS_AS(Config(test_config_invalid_type.path_string()), ConfigException);
    }

    SECTION("Parquet row group size is validated")
    {
        TempConfigFile test_config(R"(
[Front]
MD_Url = "tcp://test.com:10131"
TD_Url = "tcp://test.com:10130"
BrokerID = "9999"
UserID = "test_user"
Password = "test_pass"
UserProductInfo = "test_product"
AuthCode = "test_auth"
AppID = "test_app"

[History]
ParquetPath = "./data/parquet"
StorageMode = "Parquet"
ParquetRowGroupSize = 0
)");
        REQUIRE_THROWS_AS(Config(test_config.path_string()), ConfigException);
    }

#ifndef CFMDC_ENABLE_PARQUET
    SECTION("Parquet mode requires a Parquet-enabled build")
    {
        TempConfigFile test_config(R"(
[Front]
MD_Url = "tcp://test.com:10131"
TD_Url = "tcp://test.com:10130"
BrokerID = "9999"
UserID = "test_user"
Password = "test_pass"
UserProductInfo = "test_product"
AuthCode = "test_auth"
AppID = "test_app"

[History]
ParquetPath = "./data/parquet"
StorageMode = "Parquet"
)");
        REQUIRE_THROWS_AS(Config(test_config.path_string()), ConfigException);
    }
#endif
}
