#pragma once

#include <atomic>
#include <cstddef>
#include <type_traits>
#include <utility>

namespace cfmdc
{

#ifdef _MSC_VER
#pragma warning(push)
#pragma warning(disable : 4324) // structure was padded due to alignment specifier
#endif

/// @brief Lock-free SPSC (Single Producer Single Consumer) queue
/// @tparam T Element type
/// @tparam Size Queue size (must be power of 2)
/// @thread_safety Thread-safe for single producer and single consumer
template <typename T, size_t Size> class LockFreeQueue
{
    static_assert((Size & (Size - 1)) == 0, "Size must be power of 2");
    static_assert(Size > 0, "Size must be greater than 0");

  public:
    LockFreeQueue() = default;

    /// @brief Try to enqueue an item (producer side)
    /// @param item Item to enqueue
    /// @return true if successfully enqueued, false if queue is full
    bool try_enqueue(const T &item) noexcept(std::is_nothrow_copy_assignable_v<T>)
    {
        const size_t current_tail = tail_.load(std::memory_order_relaxed);
        if (current_tail - head_cache_ >= Size)
        {
            head_cache_ = head_.load(std::memory_order_acquire);
            if (current_tail - head_cache_ >= Size)
            {
                overflow_count_.fetch_add(1, std::memory_order_relaxed);
                return false; // Queue is full
            }
        }

        buffer_[current_tail & (Size - 1)] = item;
        tail_.store(current_tail + 1, std::memory_order_release);
        return true;
    }

    /// @brief Try to enqueue an item using move semantics (producer side)
    /// @param item Item to enqueue
    /// @return true if successfully enqueued, false if queue is full
    bool try_enqueue(T &&item) noexcept(std::is_nothrow_move_assignable_v<T>)
    {
        const size_t current_tail = tail_.load(std::memory_order_relaxed);
        if (current_tail - head_cache_ >= Size)
        {
            head_cache_ = head_.load(std::memory_order_acquire);
            if (current_tail - head_cache_ >= Size)
            {
                overflow_count_.fetch_add(1, std::memory_order_relaxed);
                return false; // Queue is full
            }
        }

        buffer_[current_tail & (Size - 1)] = std::move(item);
        tail_.store(current_tail + 1, std::memory_order_release);
        return true;
    }

    /// @brief Get total number of overflow events
    size_t overflow_count() const noexcept
    {
        return overflow_count_.load(std::memory_order_relaxed);
    }

    /// @brief Reset overflow counter
    void reset_overflow_count() noexcept
    {
        overflow_count_.store(0, std::memory_order_relaxed);
    }

    /// @brief Inspect the front element in-place without copying (consumer side)
    /// @return Pointer to front element, or nullptr if queue is empty
    T *front() noexcept
    {
        const size_t current_head = head_.load(std::memory_order_relaxed);
        if (current_head == tail_cache_)
        {
            tail_cache_ = tail_.load(std::memory_order_acquire);
            if (current_head == tail_cache_)
            {
                return nullptr;
            }
        }
        return &buffer_[current_head & (Size - 1)];
    }

    /// @brief Advance consumer head after inspecting front() (consumer side)
    /// @pre front() must have returned non-null
    void pop() noexcept
    {
        const size_t current_head = head_.load(std::memory_order_relaxed);
        head_.store(current_head + 1, std::memory_order_release);
    }

    /// @brief Try to dequeue an item (consumer side)
    /// @param item Output parameter for dequeued item
    /// @return true if successfully dequeued, false if queue is empty
    bool try_dequeue(T &item) noexcept(std::is_nothrow_move_assignable_v<T>)
    {
        T *slot = front();
        if (!slot)
        {
            return false; // Queue is empty
        }

        item = std::move(*slot);
        pop();
        return true;
    }

    /// @brief Get current queue size
    /// @return Number of elements in queue
    size_t size() const noexcept
    {
        const size_t head = head_.load(std::memory_order_acquire);
        const size_t tail = tail_.load(std::memory_order_acquire);
        return tail - head;
    }

    /// @brief Check if queue is empty
    /// @return true if queue is empty
    bool empty() const noexcept
    {
        return head_.load(std::memory_order_acquire) == tail_.load(std::memory_order_acquire);
    }

    /// @brief Get the usable queue capacity
    static constexpr size_t capacity() noexcept
    {
        return Size;
    }

  private:
    // 128-byte alignment avoids false sharing across adjacent L2 spatial prefetcher lines.
    static constexpr size_t kCacheLineSize = 128;

    // Consumer-owned cache line
    alignas(kCacheLineSize) std::atomic<size_t> head_{0};
    size_t tail_cache_{0};

    // Producer-owned cache line
    alignas(kCacheLineSize) std::atomic<size_t> tail_{0};
    size_t head_cache_{0};
    std::atomic<size_t> overflow_count_{0};

    alignas(kCacheLineSize) T buffer_[Size];
};

#ifdef _MSC_VER
#pragma warning(pop)
#endif

} // namespace cfmdc
