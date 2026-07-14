#include <array>
#include <catch2/catch_test_macros.hpp>
#include <chrono>

#include "cfmdc/core/SubscriptionTracker.h"

using namespace cfmdc;
using namespace std::chrono_literals;

TEST_CASE("SubscriptionTracker requires complete successful responses", "[subscription]")
{
    std::array<char, 8> rb{"rb2505"};
    std::array<char, 8> ag{"ag2506"};
    std::array<char *, 2> instruments{rb.data(), ag.data()};

    SECTION("all expected instruments succeed")
    {
        SubscriptionTracker tracker;
        tracker.begin(instruments);
        tracker.record("rb2505", true, false);
        tracker.record("ag2506", true, true);

        REQUIRE(tracker.wait_for_completion(10ms));
        const auto result = tracker.result();
        REQUIRE(result.successful());
        REQUIRE(result.expected == 2);
        REQUIRE(result.succeeded == 2);
    }

    SECTION("a final callback does not hide missing instruments")
    {
        SubscriptionTracker tracker;
        tracker.begin(instruments);
        tracker.record("rb2505", true, true);

        const auto result = tracker.result();
        REQUIRE_FALSE(result.successful());
        REQUIRE(result.succeeded == 1);
        REQUIRE(result.missing == 1);
    }

    SECTION("subscription errors are retained")
    {
        SubscriptionTracker tracker;
        tracker.begin(instruments);
        tracker.record("rb2505", true, false);
        tracker.record("ag2506", false, true);

        const auto result = tracker.result();
        REQUIRE_FALSE(result.successful());
        REQUIRE(result.failed == 1);
        REQUIRE(result.failed_instruments == std::vector<std::string>{"ag2506"});
    }
}
