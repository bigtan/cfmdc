#pragma once

#include <format>
#include <stdexcept>
#include <string>
#include <string_view>

namespace cfmdc
{

/// @brief Base exception class for CTP-related errors
/// @details Provides error code and message for all CTP operations
class CtpException : public std::runtime_error
{
  public:
    /// @brief Construct a CTP exception with error code and message
    /// @param error_code The CTP error code
    /// @param msg The error message
    explicit CtpException(int error_code, std::string_view msg)
        : std::runtime_error(std::format("[Error {}] {}", error_code, msg)), error_code_(error_code)
    {
    }

    /// @brief Get the error code
    /// @return The error code
    int error_code() const noexcept
    {
        return error_code_;
    }

  private:
    int error_code_;
};

/// @brief Exception thrown when connection to CTP server fails
class ConnectionException : public CtpException
{
  public:
    using CtpException::CtpException;
};

/// @brief Exception thrown when authentication fails
class AuthenticationException : public CtpException
{
  public:
    using CtpException::CtpException;
};

/// @brief Exception thrown when login fails
class LoginException : public CtpException
{
  public:
    using CtpException::CtpException;
};

/// @brief Exception thrown for configuration errors
class ConfigException : public std::runtime_error
{
  public:
    using std::runtime_error::runtime_error;
};

/// @brief Exception thrown for API initialization errors
class ApiException : public std::runtime_error
{
  public:
    using std::runtime_error::runtime_error;
};

/// @brief Exception thrown for file I/O errors
class FileException : public std::runtime_error
{
  public:
    using std::runtime_error::runtime_error;
};

/// @brief Helper class to check CTP response and convert to exception if needed
/// @details This class provides a convenient way to check CTP responses
///          and either throw exceptions or return error information
class CtpError
{
  public:
    /// @brief Construct an error object
    /// @param error_id The error ID from CTP
    /// @param error_msg The error message from CTP
    CtpError(int error_id, std::string_view error_msg) : error_id_(error_id), error_msg_(error_msg)
    {
    }

    /// @brief Get the error ID
    /// @return The error ID
    int error_id() const noexcept
    {
        return error_id_;
    }

    /// @brief Get the error message
    /// @return The error message
    const std::string &error_msg() const noexcept
    {
        return error_msg_;
    }

    /// @brief Check if this represents an error
    /// @return true if error_id is non-zero
    bool is_error() const noexcept
    {
        return error_id_ != 0;
    }

    /// @brief Throw an exception if this is an error
    /// @tparam ExceptionType The type of exception to throw (default:
    /// CtpException)
    template <typename ExceptionType = CtpException> void throw_if_error() const
    {
        if (is_error())
        {
            throw ExceptionType(error_id_, error_msg_);
        }
    }

  private:
    int error_id_;
    std::string error_msg_;
};

} // namespace cfmdc
