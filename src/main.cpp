#include <spdlog/sinks/daily_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

#include <args.hxx>
#include <chrono>
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
        args::ArgumentParser parser(std::string("CTP Market Data Recorder v") + std::string(cfmdc::APP_VERSION));
        args::HelpFlag help(parser, "help", "Print usage information", {'h', "help"});
        args::Flag version(parser, "version", "Print version information", {'v', "version"});
        args::ValueFlag<std::string> config_option(parser, "config_file", "Path to configuration file",
                                                   {'c', "config"});
        args::Positional<std::string> config_positional(parser, "config_file", "Path to configuration file");

        try
        {
            parser.ParseCLI(argc, argv);
        }
        catch (const args::Help &)
        {
            std::print("{}", parser.Help());
            return 0;
        }
        catch (const args::ParseError &e)
        {
            std::println(stderr, "Error: {}", e.what());
            std::print(stderr, "{}", parser.Help());
            return 1;
        }

        if (version)
        {
            std::println("CFMDC - CTP Market Data Recorder v{}", cfmdc::APP_VERSION);
            return 0;
        }

        std::string config_file(cfmdc::CONFIG_FILE);
        if (config_positional)
        {
            config_file = args::get(config_positional);
        }
        if (config_option)
        {
            config_file = args::get(config_option);
        }

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
