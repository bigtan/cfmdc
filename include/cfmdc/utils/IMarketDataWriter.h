#pragma once

#include "ThostFtdcUserApiStruct.h"

namespace cfmdc
{

/// @brief Abstract base class for all market data writers
class IMarketDataWriter
{
  public:
    virtual ~IMarketDataWriter() = default;

    /// @brief Write a single market data record
    /// @param data Market data to write
    /// @return true if successful, false otherwise
    virtual bool write(const CThostFtdcDepthMarketDataField &data) = 0;

    /// @brief Flush buffered data (if any)
    /// @return true if all buffered data was flushed successfully
    virtual bool flush() = 0;
};

} // namespace cfmdc
