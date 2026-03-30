#include <catch2/catch_test_macros.hpp>
#include <filesystem>
#include <fstream>

#include "cfmdc/config/Config.h"
#include "cfmdc/utils/Error.h"

using namespace cfmdc;

namespace
{
void write_config_file(const std::string &path, const std::string &content)
{
    std::ofstream out(path);
    out << content;
}
} // namespace

TEST_CASE("Config loads valid TOML", "[config]")
{
    // Create a temporary test config file
    const std::string test_config = "test_config_temp.toml";
    write_config_file(test_config, R"(
[Front]
MD_Url = "tcp://test.com:10131"
TD_Url = "tcp://test.com:10130"
BrokerID = "9999"
UserID = "test_user"
Password = "test_pass"
UserProductInfo = "test_product"
AuthCode = "test_auth"
AppID = "test_app"
SubList = "null"

[History]
CSVPath = "./test_data/csv"
ParquetPath = "./test_data/parquet"
StorageMode = "CSV"

[Application]
InitTimeout = 30
)");

    SECTION("Valid config loads successfully")
    {
        Config config(test_config);
        REQUIRE(config.front_servers().size() == 1);
        REQUIRE(config.front_servers()[0].md_url() == "tcp://test.com:10131");
        REQUIRE(config.front_servers()[0].broker_id() == "9999");
        REQUIRE(config.init_timeout() == 30);
    }

    // Cleanup
    std::filesystem::remove(test_config);
}

TEST_CASE("Config validates required fields", "[config]")
{
    const std::string test_config = "test_config_invalid.toml";

    SECTION("Missing Front section")
    {
        write_config_file(test_config, R"(
[History]
CSVPath = "./test_data/csv"
ParquetPath = "./test_data/parquet"
)");

        REQUIRE_THROWS_AS(Config(test_config), ConfigException);
        std::filesystem::remove(test_config);
    }

    SECTION("Missing required field")
    {
        write_config_file(test_config, R"(
[Front]
MD_Url = "tcp://test.com:10131"

[History]
CSVPath = "./test_data/csv"
ParquetPath = "./test_data/parquet"
)");

        REQUIRE_THROWS_AS(Config(test_config), ConfigException);
        std::filesystem::remove(test_config);
    }
}

TEST_CASE("Config supports multiple servers", "[config]")
{
    const std::string test_config = "test_config_multi.toml";
    write_config_file(test_config, R"(
[[Front]]
MD_Url = "tcp://server1.com:10131"
TD_Url = "tcp://server1.com:10130"
BrokerID = "9999"
UserID = "test_user"
Password = "test_pass"
UserProductInfo = "test_product"
AuthCode = "test_auth"
AppID = "test_app"
SubList = "null"

[[Front]]
MD_Url = "tcp://server2.com:10131"
TD_Url = "tcp://server2.com:10130"
BrokerID = "8888"
UserID = "test_user2"
Password = "test_pass2"
UserProductInfo = "test_product2"
AuthCode = "test_auth2"
AppID = "test_app2"
SubList = "null"

[History]
CSVPath = "./test_data/csv"
ParquetPath = "./test_data/parquet"
StorageMode = "Parquet"
)");

    Config config(test_config);
    REQUIRE(config.front_servers().size() == 2);
    REQUIRE(config.front_servers()[0].md_url() == "tcp://server1.com:10131");
    REQUIRE(config.front_servers()[1].md_url() == "tcp://server2.com:10131");

    std::filesystem::remove(test_config);
}

TEST_CASE("Config resolves path placeholders", "[config]")
{
    const std::string test_config = "test_config_paths.toml";
    write_config_file(test_config, R"(
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

    Config config(test_config);
    auto csv_path = config.csv_path("20250110");
    auto parquet_path = config.parquet_path("20250110");
    REQUIRE(csv_path.generic_string() == "./data/2025/01/csv/20250110");
    REQUIRE(parquet_path.generic_string() == "./data/2025/01/parquet/20250110");

    std::filesystem::remove(test_config);
}

TEST_CASE("Config defaults and storage mode parsing", "[config]")
{
    SECTION("Defaults when Application section is missing")
    {
        const std::string test_config = "test_config_defaults.toml";
        write_config_file(test_config, R"(
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

        Config config(test_config);
        REQUIRE(config.init_timeout() == 60);
        REQUIRE(config.flow_path().generic_string() == "./flow");
        REQUIRE(config.subscription_list() == "null");

        std::filesystem::remove(test_config);
    }

    SECTION("FlowPath and SubList are read when explicitly configured")
    {
        const std::string test_config = "test_config_app_options.toml";
        write_config_file(test_config, R"(
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
StorageMode = "Hybrid"

[Application]
FlowPath = "./custom-flow"
InitTimeout = 90
)");

        Config config(test_config);
        REQUIRE(config.flow_path().generic_string() == "./custom-flow");
        REQUIRE(config.init_timeout() == 90);
        REQUIRE(config.subscription_list() == "rb2505|IC2501");
        REQUIRE(config.storage_mode() == StorageMode::HYBRID);

        std::filesystem::remove(test_config);
    }

    SECTION("StorageMode is case-insensitive and defaults to CSV on unknown")
    {
        const std::string test_config = "test_config_storage.toml";
        write_config_file(test_config, R"(
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
StorageMode = "ParQuEt"
)");

        Config config(test_config);
        REQUIRE(config.storage_mode() == StorageMode::PARQUET);

        std::filesystem::remove(test_config);

        const std::string test_config_unknown = "test_config_storage_unknown.toml";
        write_config_file(test_config_unknown, R"(
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

        Config config_unknown(test_config_unknown);
        REQUIRE(config_unknown.storage_mode() == StorageMode::CSV);

        std::filesystem::remove(test_config_unknown);
    }
}
