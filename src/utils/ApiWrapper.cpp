#include "cfmdc/utils/ApiWrapper.h"

#include <filesystem>
#include <format>
#include <string>

namespace cfmdc
{

namespace
{
/// @brief Ensure path ends with directory separator for CTP API
/// @param path Input path
/// @return Path with trailing separator
std::string ensure_trailing_separator(std::string_view path)
{
    std::filesystem::path p(path);
    // Add trailing separator if not present
    std::string result = p.string();
    if (!result.empty() && !result.ends_with('/') && !result.ends_with('\\'))
    {
        result += std::filesystem::path::preferred_separator;
    }
    return result;
}
} // namespace

MdApiWrapper::MdApiWrapper(std::string_view flow_path)
    : api_(CThostFtdcMdApi::CreateFtdcMdApi(ensure_trailing_separator(flow_path).c_str()))
{
    if (!api_)
    {
        throw ApiException("Failed to create Market Data API");
    }
}

MdApiWrapper::~MdApiWrapper() = default;

TraderApiWrapper::TraderApiWrapper(std::string_view flow_path)
    : api_(CThostFtdcTraderApi::CreateFtdcTraderApi(ensure_trailing_separator(flow_path).c_str()))
{
    if (!api_)
    {
        throw ApiException("Failed to create Trader API");
    }
}

TraderApiWrapper::~TraderApiWrapper() = default;

} // namespace cfmdc
