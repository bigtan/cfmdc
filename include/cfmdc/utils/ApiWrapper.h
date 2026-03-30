#pragma once

#include <memory>
#include <string_view>

#include "ThostFtdcMdApi.h"
#include "ThostFtdcTraderApi.h"
#include "cfmdc/utils/Error.h"

namespace cfmdc
{

/// @brief RAII wrapper for CTP Market Data API
/// @details Manages lifecycle of CThostFtdcMdApi with proper cleanup
/// @thread_safety Not thread-safe. API callbacks may be called from different
/// threads.
class MdApiWrapper
{
  public:
    /// @brief Create market data API instance
    /// @param flow_path Path for flow files
    /// @throws ApiException if API creation fails
    explicit MdApiWrapper(std::string_view flow_path);

    /// @brief Destructor - ensures proper cleanup
    ~MdApiWrapper();

    // Non-copyable but movable
    MdApiWrapper(const MdApiWrapper &) = delete;
    MdApiWrapper &operator=(const MdApiWrapper &) = delete;
    MdApiWrapper(MdApiWrapper &&) noexcept = default;
    MdApiWrapper &operator=(MdApiWrapper &&) noexcept = default;

    /// @brief Get raw API pointer
    /// @return Pointer to CThostFtdcMdApi
    CThostFtdcMdApi *get() noexcept
    {
        return api_.get();
    }

    /// @brief Get raw API pointer (const version)
    /// @return Pointer to CThostFtdcMdApi
    const CThostFtdcMdApi *get() const noexcept
    {
        return api_.get();
    }

    /// @brief Arrow operator for convenient access
    /// @return Pointer to CThostFtdcMdApi
    CThostFtdcMdApi *operator->() noexcept
    {
        return api_.get();
    }

    /// @brief Arrow operator for convenient access (const version)
    /// @return Pointer to CThostFtdcMdApi
    const CThostFtdcMdApi *operator->() const noexcept
    {
        return api_.get();
    }

    /// @brief Check if API is valid
    /// @return true if API is initialized
    explicit operator bool() const noexcept
    {
        return api_ != nullptr;
    }

  private:
    struct Deleter
    {
        void operator()(CThostFtdcMdApi *api) const noexcept
        {
            if (api)
            {
                api->RegisterSpi(nullptr);
                api->Release();
            }
        }
    };

    std::unique_ptr<CThostFtdcMdApi, Deleter> api_;
};

/// @brief RAII wrapper for CTP Trader API
/// @details Manages lifecycle of CThostFtdcTraderApi with proper cleanup
/// @thread_safety Not thread-safe. API callbacks may be called from different
/// threads.
class TraderApiWrapper
{
  public:
    /// @brief Create trader API instance
    /// @param flow_path Path for flow files
    /// @throws ApiException if API creation fails
    explicit TraderApiWrapper(std::string_view flow_path);

    /// @brief Destructor - ensures proper cleanup
    ~TraderApiWrapper();

    // Non-copyable but movable
    TraderApiWrapper(const TraderApiWrapper &) = delete;
    TraderApiWrapper &operator=(const TraderApiWrapper &) = delete;
    TraderApiWrapper(TraderApiWrapper &&) noexcept = default;
    TraderApiWrapper &operator=(TraderApiWrapper &&) noexcept = default;

    /// @brief Get raw API pointer
    /// @return Pointer to CThostFtdcTraderApi
    CThostFtdcTraderApi *get() noexcept
    {
        return api_.get();
    }

    /// @brief Get raw API pointer (const version)
    /// @return Pointer to CThostFtdcTraderApi
    const CThostFtdcTraderApi *get() const noexcept
    {
        return api_.get();
    }

    /// @brief Arrow operator for convenient access
    /// @return Pointer to CThostFtdcTraderApi
    CThostFtdcTraderApi *operator->() noexcept
    {
        return api_.get();
    }

    /// @brief Arrow operator for convenient access (const version)
    /// @return Pointer to CThostFtdcTraderApi
    const CThostFtdcTraderApi *operator->() const noexcept
    {
        return api_.get();
    }

    /// @brief Check if API is valid
    /// @return true if API is initialized
    explicit operator bool() const noexcept
    {
        return api_ != nullptr;
    }

  private:
    struct Deleter
    {
        void operator()(CThostFtdcTraderApi *api) const noexcept
        {
            if (api)
            {
                api->RegisterSpi(nullptr);
                api->Release();
            }
        }
    };

    std::unique_ptr<CThostFtdcTraderApi, Deleter> api_;
};

} // namespace cfmdc
