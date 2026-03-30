#include <spdlog/spdlog.h>

#include <cxxopts.hpp>
#include <exception>
#include <print>
#include <string>

#include "cfmdc/core/Application.h"
#include "cfmdc/utils/Constants.h"

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
