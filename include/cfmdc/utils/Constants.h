#pragma once

#include <chrono>
#include <limits>
#include <string_view>

namespace cfmdc
{

#ifndef CFMDC_VERSION
#define CFMDC_VERSION "unknown"
#endif

// Constants
constexpr int DEFAULT_REQUEST_ID = 0;
constexpr int FILE_HANDLE_LIMIT = 2048;
constexpr auto SLEEP_DURATION = std::chrono::milliseconds(1000);
constexpr double INVALID_PRICE = std::numeric_limits<double>::max();
constexpr std::string_view APP_VERSION = CFMDC_VERSION;

// File paths
constexpr std::string_view FLOW_DIR = "./flow/";
constexpr std::string_view CONFIG_FILE = "config.toml";

} // namespace cfmdc
