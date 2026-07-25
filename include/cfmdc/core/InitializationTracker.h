#pragma once

#include <chrono>
#include <condition_variable>
#include <mutex>
#include <string>
#include <utility>

namespace cfmdc
{

/// @brief Terminal state of a one-shot asynchronous initialization.
enum class InitializationState
{
    Pending,
    Ready,
    Failed
};

/// @brief Snapshot of an asynchronous initialization outcome.
struct InitializationResult
{
    InitializationState state{InitializationState::Pending};
    int error_code{0};
    std::string message;

    bool ready() const noexcept
    {
        return state == InitializationState::Ready;
    }

    bool failed() const noexcept
    {
        return state == InitializationState::Failed;
    }
};

/// @brief Publishes and waits for a one-shot asynchronous initialization result.
/// @details The first terminal transition wins. Later callback activity cannot
///          overwrite the result observed by the application.
class InitializationTracker
{
  public:
    InitializationTracker() = default;

    InitializationTracker(const InitializationTracker &) = delete;
    InitializationTracker &operator=(const InitializationTracker &) = delete;
    InitializationTracker(InitializationTracker &&) = delete;
    InitializationTracker &operator=(InitializationTracker &&) = delete;

    bool mark_ready()
    {
        return complete(InitializationResult{InitializationState::Ready, 0, {}});
    }

    bool mark_failed(int error_code, std::string message)
    {
        return complete(InitializationResult{InitializationState::Failed, error_code, std::move(message)});
    }

    InitializationResult result() const
    {
        std::lock_guard lock(mutex_);
        return result_;
    }

    template <typename Rep, typename Period> InitializationResult wait_for(std::chrono::duration<Rep, Period> timeout)
    {
        std::unique_lock lock(mutex_);
        completion_cv_.wait_for(lock, timeout, [this]() { return result_.state != InitializationState::Pending; });
        return result_;
    }

  private:
    bool complete(InitializationResult result)
    {
        {
            std::lock_guard lock(mutex_);
            if (result_.state != InitializationState::Pending)
            {
                return false;
            }
            result_ = std::move(result);
        }
        completion_cv_.notify_all();
        return true;
    }

    mutable std::mutex mutex_;
    std::condition_variable completion_cv_;
    InitializationResult result_;
};

} // namespace cfmdc
