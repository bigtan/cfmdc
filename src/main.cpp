#include <spdlog/sinks/daily_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include <chrono>
#include <cxxopts.hpp>
#include <exception>
#include <memory>
#include <print>
#include <string>

#include "cfmdc/core/Application.h"
#include "cfmdc/utils/Constants.h"

namespace
{

/// @brief Log to console and a rotating file so incidents can be analyzed after the fact
void setup_logging()
{
    try
    {
        auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
        // Daily file (logs/cfmdc_YYYY-MM-DD.log), 30 days retention. Rotate at 06:00
        // so a night session crossing midnight stays in one file, grouped with the
        // following day session (matches the trading day).
        auto file_sink = std::make_shared<spdlog::sinks::daily_file_sink_mt>("logs/cfmdc.log", 6, 0, false, 30);
        auto logger =
            std::make_shared<spdlog::logger>("cfmdc", spdlog::sinks_init_list{console_sink, file_sink});
        logger->flush_on(spdlog::level::warn);
        spdlog::set_default_logger(std::move(logger));
        spdlog::flush_every(std::chrono::seconds(3));
    }
    catch (const spdlog::spdlog_ex &ex)
    {
        spdlog::warn("File logging unavailable ({}), using console only", ex.what());
    }
}

} // namespace

int main(int argc, char *argv[])
{
    try
    {
        // Setup command line options using cxxopts
        cxxopts::Options options("cfmdc", std::string("CTP Market Data Recorder v") +
                                              std::string(cfmdc::APP_VERSION));

        options.add_options()("c,config", "Path to configuration file",
                              cxxopts::value<std::string>()->default_value(std::string(cfmdc::CONFIG_FILE)))(
            "h,help", "Print usage information")("v,version", "Print version information");

        // Allow positional argument for config file
        options.parse_positional({"config"});
        options.positional_help("[config_file]");

        auto result = options.parse(argc, argv);

        // Handle help
        if (result.count("help"))
        {
            std::println("{}", options.help());
            return 0;
        }

        // Handle version
        if (result.count("version"))
        {
            std::println("CFMDC - CTP Market Data Recorder v{}", cfmdc::APP_VERSION);
            return 0;
        }

        // Get config file
        std::string config_file = result["config"].as<std::string>();

        setup_logging();

        spdlog::info("Starting CFMDC - CTP Market Data Recorder v{}", cfmdc::APP_VERSION);
        spdlog::info("Using configuration file: {}", config_file);

        cfmdc::Application app(config_file);
        app.run();

        spdlog::info("Application shutdown complete");
    }
    catch (const std::exception &e)
    {
        spdlog::error("Application error: {}", e.what());
        std::println(stderr, "Error: {}", e.what());
        return 1;
    }
    catch (...)
    {
        spdlog::error("Unknown error occurred");
        std::println(stderr, "Unknown error occurred");
        return 1;
    }

    return 0;
}
