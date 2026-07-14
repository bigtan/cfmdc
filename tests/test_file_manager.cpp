#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <thread>

#include "cfmdc/utils/FileManager.h"
#include "cfmdc/utils/Helpers.h"

using namespace cfmdc;
using namespace std::chrono_literals;

namespace
{
CThostFtdcDepthMarketDataField make_tick()
{
    CThostFtdcDepthMarketDataField tick{};
    safe_strcpy(tick.InstrumentID, "rb2505");
    safe_strcpy(tick.UpdateTime, "09:30:00");
    tick.LastPrice = 3500.0;
    return tick;
}
} // namespace

TEST_CASE("AsyncFileManager marks storage failures as fatal", "[file_manager][failure]")
{
    const auto invalid_directory = std::filesystem::current_path() / "test_storage_path_is_a_file.tmp";
    {
        std::ofstream marker(invalid_directory);
        REQUIRE(marker.is_open());
    }

    {
        auto manager = std::make_unique<AsyncFileManager>(invalid_directory, std::filesystem::path{}, "20250102",
                                                          "20250101", "20250102", "09:00:00", StorageMode::CSV);
        REQUIRE(manager->write_market_data_async(make_tick()));

        const auto deadline = std::chrono::steady_clock::now() + 2s;
        while (!manager->has_fatal_error() && std::chrono::steady_clock::now() < deadline)
        {
            std::this_thread::sleep_for(1ms);
        }

        REQUIRE(manager->has_fatal_error());
        const auto stats = manager->get_statistics();
        REQUIRE(stats.total_records == 0);
        REQUIRE(stats.write_failures >= 1);
    }

    std::filesystem::remove(invalid_directory);
}

TEST_CASE("AsyncFileManager periodically flushes CSV during sustained writes", "[file_manager][flush]")
{
    const auto output_directory = std::filesystem::current_path() / "test_periodic_flush_output";
    std::filesystem::remove_all(output_directory);

    auto manager = std::make_unique<AsyncFileManager>(output_directory, std::filesystem::path{}, "20250102", "20250101",
                                                      "20250102", "09:00:00", StorageMode::CSV, -1, 1ms);

    constexpr size_t kRecordCount = 20000;
    const auto tick = make_tick();
    for (size_t i = 0; i < kRecordCount; ++i)
    {
        while (!manager->write_market_data_async(tick))
        {
            REQUIRE_FALSE(manager->has_fatal_error());
            std::this_thread::yield();
        }
    }

    const auto deadline = std::chrono::steady_clock::now() + 5s;
    while (manager->get_statistics().total_records < kRecordCount && std::chrono::steady_clock::now() < deadline)
    {
        std::this_thread::sleep_for(1ms);
    }

    manager->stop();
    const auto stats = manager->get_statistics();
    REQUIRE(stats.total_records == kRecordCount);
    REQUIRE(stats.write_failures == 0);
    REQUIRE(stats.periodic_flushes > 0);

    manager.reset();
    std::filesystem::remove_all(output_directory);
}
