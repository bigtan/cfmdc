#pragma once

#include <string_view>
#include <utility>

namespace cfmdc
{

/// @brief Storage mode for market data
enum class StorageMode
{
    CSV,     ///< CSV text format (human-readable, larger files)
    PARQUET, ///< Parquet columnar format (compressed, high performance)
    HYBRID   ///< Both CSV and Parquet (for migration/backup)
};

/// @brief Convert StorageMode to string representation
constexpr std::string_view to_string(StorageMode mode)
{
    switch (mode)
    {
        using enum StorageMode;
    case CSV:
        return "CSV";
    case PARQUET:
        return "Parquet";
    case HYBRID:
        return "Hybrid";
    }
    std::unreachable();
}

} // namespace cfmdc
