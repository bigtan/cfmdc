#pragma once

#include <atomic>
#include <cstddef>

namespace cfmdc
{

/// @brief Lock-free SPSC (Single Producer Single Consumer) queue
/// @tparam T Element type
/// @tparam Size Queue size (must be power of 2)
/// @thread_safety Thread-safe for single producer and single consumer
template <typename T, size_t Size> class LockFreeQueue
{
    static_assert((Size & (Size - 1)) == 0, "Size must be power of 2");
    static_assert(Size > 0, "Size must be greater than 0");

  public:
    LockFreeQueue() : head_(0), tail_(0)
    {
    }

    /// @brief Try to enqueue an item (producer side)
    /// @param item Item to enqueue
    /// @return true if successfully enqueued, false if queue is full
    bool try_enqueue(const T &item) noexcept
    {
        const size_t current_tail = tail_.load(std::memory_order_relaxed);
        const size_t next_tail = (current_tail + 1) & (Size - 1);

        if (next_tail == head_.load(std::memory_order_acquire))
        {
            return false; // Queue is full
        }

        buffer_[current_tail] = item;
        tail_.store(next_tail, std::memory_order_release);
        return true;
    }

    /// @brief Try to dequeue an item (consumer side)
    /// @param item Output parameter for dequeued item
    /// @return true if successfully dequeued, false if queue is empty
    bool try_dequeue(T &item) noexcept
    {
        const size_t current_head = head_.load(std::memory_order_relaxed);

        if (current_head == tail_.load(std::memory_order_acquire))
        {
            return false; // Queue is empty
        }

        item = buffer_[current_head];
        head_.store((current_head + 1) & (Size - 1), std::memory_order_release);
        return true;
    }

    /// @brief Get current queue size
    /// @return Number of elements in queue
    size_t size() const noexcept
    {
        const size_t head = head_.load(std::memory_order_acquire);
        const size_t tail = tail_.load(std::memory_order_acquire);
        return (tail - head) & (Size - 1);
    }

    /// @brief Check if queue is empty
    /// @return true if queue is empty
    bool empty() const noexcept
    {
        return head_.load(std::memory_order_acquire) == tail_.load(std::memory_order_acquire);
    }

  private:
    alignas(64) std::atomic<size_t> head_; // Consumer index (cache line aligned)
    alignas(64) std::atomic<size_t> tail_; // Producer index (cache line aligned)
    T buffer_[Size];
};

} // namespace cfmdc
