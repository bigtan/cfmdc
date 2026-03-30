#pragma once

#include <string>
#include <utility>

#include "cfmdc/utils/Error.h"

namespace cfmdc
{

/// @brief Front server configuration with builder pattern
/// @details Encapsulates all configuration needed to connect to a CTP front
/// server
///          Uses builder pattern for flexible and validated construction
class FrontServer
{
  public:
    /// @brief Builder for FrontServer with validation
    class Builder
    {
      public:
        Builder() = default;

        Builder &md_url(std::string url)
        {
            md_url_ = std::move(url);
            return *this;
        }

        Builder &td_url(std::string url)
        {
            td_url_ = std::move(url);
            return *this;
        }

        Builder &broker_id(std::string id)
        {
            broker_id_ = std::move(id);
            return *this;
        }

        Builder &user_id(std::string id)
        {
            user_id_ = std::move(id);
            return *this;
        }

        Builder &password(std::string pwd)
        {
            password_ = std::move(pwd);
            return *this;
        }

        Builder &user_product_info(std::string info)
        {
            user_product_info_ = std::move(info);
            return *this;
        }

        Builder &auth_code(std::string code)
        {
            auth_code_ = std::move(code);
            return *this;
        }

        Builder &app_id(std::string id)
        {
            app_id_ = std::move(id);
            return *this;
        }

        Builder &subscription_list(std::string list)
        {
            subscription_list_ = std::move(list);
            return *this;
        }

        /// @brief Build and validate the FrontServer
        /// @return Validated FrontServer instance
        /// @throws ConfigException if validation fails
        FrontServer build() const
        {
            validate();
            return FrontServer(*this);
        }

      private:
        void validate() const
        {
            if (md_url_.empty())
            {
                throw ConfigException("MD_Url cannot be empty");
            }
            if (td_url_.empty())
            {
                throw ConfigException("TD_Url cannot be empty");
            }
            if (broker_id_.empty())
            {
                throw ConfigException("BrokerID cannot be empty");
            }
            if (user_id_.empty())
            {
                throw ConfigException("UserID cannot be empty");
            }
            if (password_.empty())
            {
                throw ConfigException("Password cannot be empty");
            }
            if (user_product_info_.empty())
            {
                throw ConfigException("UserProductInfo cannot be empty");
            }
            if (auth_code_.empty())
            {
                throw ConfigException("AuthCode cannot be empty");
            }
            if (app_id_.empty())
            {
                throw ConfigException("AppID cannot be empty");
            }
        }

        std::string md_url_;
        std::string td_url_;
        std::string broker_id_;
        std::string user_id_;
        std::string password_;
        std::string user_product_info_;
        std::string auth_code_;
        std::string app_id_;
        std::string subscription_list_;

        friend class FrontServer;
    };

    // Getters
    const std::string &md_url() const noexcept
    {
        return md_url_;
    }
    const std::string &td_url() const noexcept
    {
        return td_url_;
    }
    const std::string &broker_id() const noexcept
    {
        return broker_id_;
    }
    const std::string &user_id() const noexcept
    {
        return user_id_;
    }
    const std::string &password() const noexcept
    {
        return password_;
    }
    const std::string &user_product_info() const noexcept
    {
        return user_product_info_;
    }
    const std::string &auth_code() const noexcept
    {
        return auth_code_;
    }
    const std::string &app_id() const noexcept
    {
        return app_id_;
    }
    const std::string &subscription_list() const noexcept
    {
        return subscription_list_;
    }

    /// @brief Validate the server configuration
    /// @return true if all required fields are non-empty
    bool validate() const noexcept
    {
        return !md_url_.empty() && !td_url_.empty() && !broker_id_.empty() && !user_id_.empty() && !password_.empty() &&
               !user_product_info_.empty() && !auth_code_.empty() && !app_id_.empty();
    }

  private:
    explicit FrontServer(const Builder &builder)
        : md_url_(builder.md_url_), td_url_(builder.td_url_), broker_id_(builder.broker_id_),
          user_id_(builder.user_id_), password_(builder.password_), user_product_info_(builder.user_product_info_),
          auth_code_(builder.auth_code_), app_id_(builder.app_id_), subscription_list_(builder.subscription_list_)
    {
    }

    std::string md_url_;
    std::string td_url_;
    std::string broker_id_;
    std::string user_id_;
    std::string password_;
    std::string user_product_info_;
    std::string auth_code_;
    std::string app_id_;
    std::string subscription_list_;
};

} // namespace cfmdc
