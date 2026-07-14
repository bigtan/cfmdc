#include <atomic>
#include <catch2/catch_message.hpp>
#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <cstdint>
#include <thread>

#include "cfmdc/utils/LockFreeQueue.h"

using namespace cfmdc;

TEST_CASE("LockFreeQueue basic operations", "[lockfreequeue]")
{
    LockFreeQueue<int, 8> queue;

    SECTION("Initial state")
    {
        REQUIRE(queue.empty());
        REQUIRE(queue.size() == 0);
    }

    SECTION("Enqueue and dequeue")
    {
        REQUIRE(queue.try_enqueue(1));
        REQUIRE(queue.try_enqueue(2));
        REQUIRE(queue.try_enqueue(3));

        REQUIRE_FALSE(queue.empty());
        REQUIRE(queue.size() == 3);

        int value;
        REQUIRE(queue.try_dequeue(value));
        REQUIRE(value == 1);

        REQUIRE(queue.try_dequeue(value));
        REQUIRE(value == 2);

        REQUIRE(queue.try_dequeue(value));
        REQUIRE(value == 3);

        REQUIRE(queue.empty());
    }

    SECTION("Queue full")
    {
        for (int i = 0; i < 8; ++i)
        {
            REQUIRE(queue.try_enqueue(i));
        }

        REQUIRE(queue.size() == queue.capacity());
        REQUIRE_FALSE(queue.try_enqueue(999)); // Should fail when full
        REQUIRE(queue.overflow_count() == 1);
    }

    SECTION("Queue empty")
    {
        int value;
        REQUIRE_FALSE(queue.try_dequeue(value)); // Should fail when empty
    }
}

TEST_CASE("LockFreeQueue with complex types", "[lockfreequeue]")
{
    struct TestData
    {
        int id;
        double value;
        char name[32];

        bool operator==(const TestData &other) const
        {
            return id == other.id && value == other.value;
        }
    };

    LockFreeQueue<TestData, 16> queue;

    SECTION("Enqueue and dequeue complex data")
    {
        TestData data1{1, 100.5, "test1"};
        TestData data2{2, 200.7, "test2"};

        REQUIRE(queue.try_enqueue(data1));
        REQUIRE(queue.try_enqueue(data2));

        TestData result;
        REQUIRE(queue.try_dequeue(result));
        REQUIRE(result == data1);

        REQUIRE(queue.try_dequeue(result));
        REQUIRE(result == data2);

        REQUIRE(queue.empty());
    }
}

TEST_CASE("LockFreeQueue thread safety (SPSC)", "[lockfreequeue][concurrency]")
{
    LockFreeQueue<int, 1024> queue;
    constexpr int num_items = 250000;
    std::atomic<bool> done{false};
    std::atomic<bool> sequence_valid{true};

    SECTION("Single producer, single consumer")
    {
        // Producer thread
        std::thread producer([&]() {
            for (int i = 0; i < num_items; ++i)
            {
                while (!queue.try_enqueue(i))
                {
                    std::this_thread::yield();
                }
            }
            done.store(true);
        });

        // Consumer thread
        std::thread consumer([&]() {
            int count = 0;
            int value;
            while (count < num_items)
            {
                if (queue.try_dequeue(value))
                {
                    if (value != count)
                    {
                        sequence_valid.store(false, std::memory_order_relaxed);
                    }
                    ++count;
                }
                else
                {
                    std::this_thread::yield();
                }
            }
        });

        producer.join();
        consumer.join();

        REQUIRE(queue.empty());
        REQUIRE(done.load());
        REQUIRE(sequence_valid.load());
    }
}

TEST_CASE("LockFreeQueue performance characteristics", "[lockfreequeue][benchmark]")
{
    LockFreeQueue<int, 4096> queue;

    SECTION("Sequential enqueue/dequeue performance")
    {
        constexpr int iterations = 1000000;
        bool operations_succeeded = true;
        std::int64_t checksum = 0;

        const auto start = std::chrono::steady_clock::now();

        for (int i = 0; i < iterations; ++i)
        {
            operations_succeeded = queue.try_enqueue(i) && operations_succeeded;
            int value = 0;
            operations_succeeded = queue.try_dequeue(value) && operations_succeeded;
            checksum += value;
        }

        const auto elapsed = std::chrono::steady_clock::now() - start;
        const auto seconds = std::chrono::duration<double>(elapsed).count();
        const auto operations_per_second = static_cast<double>(iterations) * 2.0 / seconds;
        INFO("sequential queue operations per second: " << operations_per_second);

        REQUIRE(operations_succeeded);
        REQUIRE(checksum == static_cast<std::int64_t>(iterations - 1) * iterations / 2);
        REQUIRE(queue.empty());
        REQUIRE(seconds > 0.0);
    }
}
