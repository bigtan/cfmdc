#pragma once

#include <cstring>
#include <string_view>

#include "ThostFtdcUserApiDataType.h"
#include "ThostFtdcUserApiStruct.h"
#include "cfmdc/utils/Constants.h"
#include "cfmdc/utils/Error.h"

namespace cfmdc
{

/// @brief Safely copy string to fixed-size char array
/// @tparam N Size of destination array
/// @param dest Destination array
/// @param src Source string
template <size_t N> void safe_strcpy(char (&dest)[N], std::string_view src) noexcept
{
    size_t copy_len = src.copy(dest, N - 1);
    dest[copy_len] = '\0';
}

/// @brief Clean invalid price values
/// @param price Raw price value
/// @return Cleaned price (0.0 if invalid)
inline double clean_price(double price) noexcept
{
    return (price == INVALID_PRICE) ? 0.0 : price;
}

/// @brief Check CTP response and create error object
/// @param pRspInfo Response info from CTP
/// @return CtpError object
inline CtpError check_response(CThostFtdcRspInfoField *pRspInfo)
{
    if (pRspInfo && pRspInfo->ErrorID != 0)
    {
        return CtpError{pRspInfo->ErrorID, pRspInfo->ErrorMsg};
    }
    return CtpError{0, ""};
}

} // namespace cfmdc
