#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <thread>

#include "cfmdc/core/InitializationTracker.h"

using namespace cfmdc;
using namespace std::chrono_literals;

TEST_CASE("InitializationTracker publishes one terminal outcome", "[initialization]")
{
    InitializationTracker tracker;

    SECTION("initial state is pending")
    {
        const auto result = tracker.result();
        REQUIRE(result.state == InitializationState::Pending);
        REQUIRE_FALSE(result.ready());
        REQUIRE_FALSE(result.failed());
    }

    SECTION("ready state wakes a waiter")
    {
        std::jthread completer([&tracker]() {
            std::this_thread::sleep_for(1ms);
            (void)tracker.mark_ready();
        });

        const auto result = tracker.wait_for(100ms);
        REQUIRE(result.ready());
        REQUIRE(result.error_code == 0);
        REQUIRE(result.message.empty());
    }

    SECTION("failure preserves diagnostic details")
    {
        REQUIRE(tracker.mark_failed(7, "authentication rejected"));

        const auto result = tracker.wait_for(1ms);
        REQUIRE(result.failed());
        REQUIRE(result.error_code == 7);
        REQUIRE(result.message == "authentication rejected");
    }

    SECTION("first terminal transition wins")
    {
        REQUIRE(tracker.mark_failed(3, "login failed"));
        REQUIRE_FALSE(tracker.mark_ready());
        REQUIRE_FALSE(tracker.mark_failed(4, "later failure"));

        const auto result = tracker.result();
        REQUIRE(result.failed());
        REQUIRE(result.error_code == 3);
        REQUIRE(result.message == "login failed");
    }
}
