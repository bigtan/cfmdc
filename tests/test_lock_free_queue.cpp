#include <catch2/catch_test_macros.hpp>
#include <thread>
#include <vector>

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
        // Size is 8, but can only hold 7 items (ring buffer implementation)
        for (int i = 0; i < 7; ++i)
        {
            REQUIRE(queue.try_enqueue(i));
        }

        REQUIRE_FALSE(queue.try_enqueue(999)); // Should fail when full
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
    const int num_items = 1000;
    std::atomic<bool> done{false};

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
                    REQUIRE(value == count);
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
    }
}

TEST_CASE("LockFreeQueue performance characteristics", "[lockfreequeue][benchmark]")
{
    LockFreeQueue<int, 4096> queue;

    SECTION("Sequential enqueue/dequeue performance")
    {
        const int iterations = 10000;

        auto start = std::chrono::high_resolution_clock::now();

        for (int i = 0; i < iterations; ++i)
        {
            queue.try_enqueue(i);
        }

        int value;
        for (int i = 0; i < iterations; ++i)
        {
            queue.try_dequeue(value);
        }

        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);

        // Just verify it completed successfully
        REQUIRE(queue.empty());
        REQUIRE(duration.count() > 0); // Sanity check
    }
}
