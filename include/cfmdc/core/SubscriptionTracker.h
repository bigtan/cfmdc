#pragma once

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <span>
#include <string>
#include <string_view>
#include <unordered_set>
#include <vector>

namespace cfmdc
{

/// @brief Tracks asynchronous CTP market-data subscription responses.
class SubscriptionTracker
{
  public:
    struct Result
    {
        bool completed{false};
        size_t expected{0};
        size_t succeeded{0};
        size_t failed{0};
        size_t missing{0};
        std::vector<std::string> failed_instruments;

        bool successful() const noexcept
        {
            return completed && expected == succeeded && failed == 0 && missing == 0;
        }
    };

    void begin(std::span<char *> instrument_ids)
    {
        std::lock_guard lock(mutex_);
        expected_.clear();
        succeeded_.clear();
        failed_.clear();
        unattributed_failures_ = 0;
        completed_ = false;
        for (const char *instrument_id : instrument_ids)
        {
            if (instrument_id && *instrument_id != '\0')
            {
                expected_.emplace(instrument_id);
            }
        }
    }

    void record(std::string_view instrument_id, bool success, bool is_last)
    {
        bool notify_completion = false;
        {
            std::lock_guard lock(mutex_);
            if (instrument_id.empty())
            {
                if (!success)
                {
                    ++unattributed_failures_;
                }
            }
            else
            {
                const std::string instrument_key{instrument_id};
                if (!expected_.contains(instrument_key))
                {
                    // CTP can replay responses from subscriptions restored through
                    // its flow files. They do not belong to the active request.
                }
                else if (success)
                {
                    failed_.erase(instrument_key);
                    succeeded_.emplace(instrument_key);
                }
                else
                {
                    succeeded_.erase(instrument_key);
                    failed_.emplace(instrument_key);
                }
            }

            const auto accounted = succeeded_.size() + failed_.size();
            if (accounted == expected_.size() || (is_last && unattributed_failures_ != 0))
            {
                completed_ = true;
                notify_completion = true;
            }
        }
        if (notify_completion)
        {
            completion_cv_.notify_all();
        }
    }

    template <typename Rep, typename Period> bool wait_for_completion(std::chrono::duration<Rep, Period> timeout)
    {
        std::unique_lock lock(mutex_);
        return completion_cv_.wait_for(lock, timeout, [this]() { return completed_; });
    }

    Result result() const
    {
        std::lock_guard lock(mutex_);
        Result result;
        result.completed = completed_;
        result.expected = expected_.size();
        result.failed = failed_.size() + unattributed_failures_;
        result.failed_instruments.assign(failed_.begin(), failed_.end());
        std::ranges::sort(result.failed_instruments);

        for (const auto &instrument_id : expected_)
        {
            if (succeeded_.contains(instrument_id))
            {
                ++result.succeeded;
            }
            else if (!failed_.contains(instrument_id))
            {
                ++result.missing;
            }
        }
        return result;
    }

  private:
    mutable std::mutex mutex_;
    std::condition_variable completion_cv_;
    std::unordered_set<std::string> expected_;
    std::unordered_set<std::string> succeeded_;
    std::unordered_set<std::string> failed_;
    size_t unattributed_failures_{0};
    bool completed_{false};
};

} // namespace cfmdc
