#pragma once

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <span>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_set>
#include <vector>

namespace cfmdc
{

/// @brief Tracks asynchronous CTP market-data subscription responses and reconnect re-subscriptions.
class SubscriptionTracker
{
  public:
    static constexpr size_t kDefaultResubscribeAttempts = 3;
    static constexpr auto kDefaultResubscribeDelay = std::chrono::milliseconds(200);
    static constexpr auto kDefaultResubscribeResponseTimeout = std::chrono::seconds(5);

    enum class ResubscribeState
    {
        Idle,
        WaitingResponse,
        WaitingRetry,
        Failed
    };

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
        reset_active_tracking_locked(instrument_ids);
    }

    /// @brief Record that the initial SubscribeMarketData call was accepted by the API
    void mark_request_sent(std::span<char *> instrument_ids)
    {
        std::lock_guard lock(mutex_);
        subscribed_instruments_.clear();
        subscribed_instruments_.reserve(instrument_ids.size());
        for (const char *instrument_id : instrument_ids)
        {
            if (instrument_id && *instrument_id != '\0')
            {
                subscribed_instruments_.emplace_back(instrument_id);
            }
        }
        ++resubscribe_generation_;
        resubscribe_state_ = ResubscribeState::Idle;
        resubscribe_attempt_ = 0;
        resubscription_failed_.store(false, std::memory_order_release);
    }

    /// @brief Cancel any in-flight reconnect resubscription wait upon disconnect (preserving Failed state)
    void on_disconnected()
    {
        std::lock_guard lock(mutex_);
        if (resubscribe_state_ == ResubscribeState::WaitingResponse ||
            resubscribe_state_ == ResubscribeState::WaitingRetry)
        {
            ++resubscribe_generation_;
            resubscribe_state_ = ResubscribeState::Idle;
        }
    }

    /// @brief Start a non-blocking reconnect re-subscription attempt without sleeping on the caller thread
    /// @param subscribe_fn Callable `int(std::span<char *>, size_t attempt)` returning 0 on success
    /// @param max_attempts Total maximum attempts including this initial attempt (clamped to >= 1)
    /// @param retry_delay Delay before scheduling the next retry after a failed attempt or response timeout
    /// @param response_timeout Deadline to wait for OnRspSubMarketData completion before retrying
    /// @param now Current steady_clock time point (injectable for deterministic tests)
    /// @return true if initial send returned 0 or no instruments were registered; false if send failed
    template <typename SubscribeFn>
    bool begin_reconnect_resubscribe(
        SubscribeFn &&subscribe_fn, size_t max_attempts = kDefaultResubscribeAttempts,
        std::chrono::milliseconds retry_delay = kDefaultResubscribeDelay,
        std::chrono::milliseconds response_timeout = kDefaultResubscribeResponseTimeout,
        std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now())
    {
        std::vector<std::string> instruments_copy;
        std::vector<char *> c_strings;
        uint64_t gen = 0;

        {
            std::lock_guard lock(mutex_);
            if (subscribed_instruments_.empty())
            {
                resubscribe_state_ = ResubscribeState::Idle;
                resubscription_failed_.store(false, std::memory_order_release);
                return true;
            }

            instruments_copy = subscribed_instruments_;
            c_strings.reserve(instruments_copy.size());
            for (auto &id : instruments_copy)
            {
                c_strings.push_back(id.data());
            }

            gen = ++resubscribe_generation_;
            resubscribe_max_attempts_ = (std::max)(size_t{1}, max_attempts);
            resubscribe_retry_delay_ = (std::max)(std::chrono::milliseconds::zero(), retry_delay);
            resubscribe_response_timeout_ = (std::max)(std::chrono::milliseconds::zero(), response_timeout);
            resubscribe_attempt_ = 1;
            resubscription_failed_.store(false, std::memory_order_release);

            reset_active_tracking_locked(c_strings);
            resubscribe_state_ = ResubscribeState::WaitingResponse;
            resubscribe_response_deadline_ = now + resubscribe_response_timeout_;
        }

        const int rt = subscribe_fn(std::span<char *>(c_strings), 1);

        std::lock_guard lock(mutex_);
        if (gen != resubscribe_generation_)
        {
            return rt == 0;
        }

        if (rt == 0)
        {
            if (completed_)
            {
                if (is_active_result_successful_locked())
                {
                    resubscribe_state_ = ResubscribeState::Idle;
                    resubscription_failed_.store(false, std::memory_order_release);
                }
                else
                {
                    handle_resubscribe_failure_locked(now);
                }
            }
            return true;
        }

        handle_resubscribe_failure_locked(now);
        return false;
    }

    /// @brief Advance the non-blocking reconnect re-subscription state machine from the main health-check loop
    /// @param subscribe_fn Callable `int(std::span<char *>, size_t attempt)` returning 0 on success
    /// @param should_stop When true, cancels active pending retries cleanly without marking fatal failure
    /// @param now Current steady_clock time point (injectable for deterministic tests)
    /// @return Current ResubscribeState after polling
    template <typename SubscribeFn>
    ResubscribeState poll_resubscribe(
        SubscribeFn &&subscribe_fn, bool should_stop = false,
        std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now())
    {
        std::vector<std::string> instruments_copy;
        std::vector<char *> c_strings;
        size_t attempt = 0;
        uint64_t gen = 0;

        {
            std::lock_guard lock(mutex_);
            if (should_stop)
            {
                if (resubscribe_state_ == ResubscribeState::WaitingResponse ||
                    resubscribe_state_ == ResubscribeState::WaitingRetry)
                {
                    ++resubscribe_generation_;
                    resubscribe_state_ = ResubscribeState::Idle;
                }
                return resubscribe_state_;
            }

            if (resubscribe_state_ == ResubscribeState::Idle || resubscribe_state_ == ResubscribeState::Failed)
            {
                return resubscribe_state_;
            }

            if (resubscribe_state_ == ResubscribeState::WaitingResponse)
            {
                if (completed_)
                {
                    if (is_active_result_successful_locked())
                    {
                        resubscribe_state_ = ResubscribeState::Idle;
                        resubscription_failed_.store(false, std::memory_order_release);
                        return resubscribe_state_;
                    }
                    handle_resubscribe_failure_locked(now);
                }
                else if (now >= resubscribe_response_deadline_)
                {
                    handle_resubscribe_failure_locked(now);
                }
                else
                {
                    return resubscribe_state_;
                }
            }

            if (resubscribe_state_ != ResubscribeState::WaitingRetry)
            {
                return resubscribe_state_;
            }

            if (now < resubscribe_next_retry_at_)
            {
                return resubscribe_state_;
            }

            if (subscribed_instruments_.empty())
            {
                resubscribe_state_ = ResubscribeState::Idle;
                return resubscribe_state_;
            }

            instruments_copy = subscribed_instruments_;
            c_strings.reserve(instruments_copy.size());
            for (auto &id : instruments_copy)
            {
                c_strings.push_back(id.data());
            }

            attempt = ++resubscribe_attempt_;
            gen = ++resubscribe_generation_;
            reset_active_tracking_locked(c_strings);
            resubscribe_state_ = ResubscribeState::WaitingResponse;
            resubscribe_response_deadline_ = now + resubscribe_response_timeout_;
        }

        const int rt = subscribe_fn(std::span<char *>(c_strings), attempt);

        std::lock_guard lock(mutex_);
        if (gen != resubscribe_generation_)
        {
            return resubscribe_state_;
        }

        if (rt == 0)
        {
            if (completed_)
            {
                if (is_active_result_successful_locked())
                {
                    resubscribe_state_ = ResubscribeState::Idle;
                    resubscription_failed_.store(false, std::memory_order_release);
                }
                else
                {
                    handle_resubscribe_failure_locked(now);
                }
            }
        }
        else
        {
            handle_resubscribe_failure_locked(now);
        }

        return resubscribe_state_;
    }

    /// @brief Re-subscribe remembered instruments with bounded retries after reconnection (synchronous helper)
    /// @param subscribe_fn Callable `int(std::span<char *>, size_t attempt)` returning 0 on success
    /// @param max_attempts Maximum number of attempts (must be >= 1)
    /// @param retry_delay Delay between failed attempts
    /// @param should_stop Callable `bool()` checked before sleeping/retrying
    /// @return true if re-subscription succeeded or no instruments were registered
    template <typename SubscribeFn, typename StopPredicate>
    bool resubscribe_with_retry(SubscribeFn &&subscribe_fn, size_t max_attempts,
                                std::chrono::milliseconds retry_delay, StopPredicate &&should_stop)
    {
        std::vector<std::string> instruments_copy;
        {
            std::lock_guard lock(mutex_);
            if (subscribed_instruments_.empty())
            {
                return true;
            }
            instruments_copy = subscribed_instruments_;
        }

        std::vector<char *> c_strings;
        c_strings.reserve(instruments_copy.size());
        for (auto &id : instruments_copy)
        {
            c_strings.push_back(id.data());
        }

        const size_t bounded_attempts = (std::max)(size_t{1}, max_attempts);
        for (size_t attempt = 1; attempt <= bounded_attempts; ++attempt)
        {
            if (should_stop())
            {
                return false;
            }

            {
                std::lock_guard lock(mutex_);
                reset_active_tracking_locked(c_strings);
            }

            if (subscribe_fn(std::span<char *>(c_strings), attempt) == 0)
            {
                resubscription_failed_.store(false, std::memory_order_release);
                return true;
            }

            if (attempt < bounded_attempts)
            {
                if (should_stop())
                {
                    return false;
                }
                if (retry_delay > std::chrono::milliseconds::zero())
                {
                    std::this_thread::sleep_for(retry_delay);
                }
            }
        }

        resubscription_failed_.store(true, std::memory_order_release);
        return false;
    }

    bool has_resubscription_failure() const noexcept
    {
        return resubscription_failed_.load(std::memory_order_acquire);
    }

    ResubscribeState resubscribe_state() const
    {
        std::lock_guard lock(mutex_);
        return resubscribe_state_;
    }

    size_t resubscribe_attempt() const
    {
        std::lock_guard lock(mutex_);
        return resubscribe_attempt_;
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

                if (resubscribe_state_ == ResubscribeState::WaitingResponse)
                {
                    if (is_active_result_successful_locked())
                    {
                        resubscribe_state_ = ResubscribeState::Idle;
                        resubscription_failed_.store(false, std::memory_order_release);
                    }
                    else if (resubscribe_attempt_ >= resubscribe_max_attempts_)
                    {
                        resubscribe_state_ = ResubscribeState::Failed;
                        resubscription_failed_.store(true, std::memory_order_release);
                    }
                }
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
    void reset_active_tracking_locked(std::span<char *> instrument_ids)
    {
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

    bool is_active_result_successful_locked() const noexcept
    {
        return completed_ && failed_.empty() && unattributed_failures_ == 0 && succeeded_.size() == expected_.size();
    }

    void handle_resubscribe_failure_locked(std::chrono::steady_clock::time_point now) noexcept
    {
        if (resubscribe_attempt_ < resubscribe_max_attempts_)
        {
            resubscribe_state_ = ResubscribeState::WaitingRetry;
            resubscribe_next_retry_at_ = now + resubscribe_retry_delay_;
        }
        else
        {
            resubscribe_state_ = ResubscribeState::Failed;
            resubscription_failed_.store(true, std::memory_order_release);
        }
    }

    mutable std::mutex mutex_;
    std::condition_variable completion_cv_;
    std::unordered_set<std::string> expected_;
    std::unordered_set<std::string> succeeded_;
    std::unordered_set<std::string> failed_;
    std::vector<std::string> subscribed_instruments_;
    size_t unattributed_failures_{0};
    bool completed_{false};

    ResubscribeState resubscribe_state_{ResubscribeState::Idle};
    size_t resubscribe_attempt_{0};
    size_t resubscribe_max_attempts_{kDefaultResubscribeAttempts};
    std::chrono::milliseconds resubscribe_retry_delay_{kDefaultResubscribeDelay};
    std::chrono::milliseconds resubscribe_response_timeout_{kDefaultResubscribeResponseTimeout};
    std::chrono::steady_clock::time_point resubscribe_next_retry_at_{};
    std::chrono::steady_clock::time_point resubscribe_response_deadline_{};
    uint64_t resubscribe_generation_{0};
    std::atomic<bool> resubscription_failed_{false};
};

} // namespace cfmdc
