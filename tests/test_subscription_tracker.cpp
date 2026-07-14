#include <array>
#include <catch2/catch_test_macros.hpp>
#include <chrono>
#include <string>
#include <vector>

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

    SECTION("an intermediate final callback does not complete the full request")
    {
        SubscriptionTracker tracker;
        tracker.begin(instruments);
        tracker.record("rb2505", true, true);

        REQUIRE_FALSE(tracker.wait_for_completion(1ms));
        const auto partial_result = tracker.result();
        REQUIRE_FALSE(partial_result.completed);
        REQUIRE(partial_result.succeeded == 1);
        REQUIRE(partial_result.missing == 1);

        tracker.record("ag2506", true, true);
        REQUIRE(tracker.wait_for_completion(10ms));
        REQUIRE(tracker.result().successful());
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

    SECTION("responses restored from flow files do not affect the active request")
    {
        SubscriptionTracker tracker;
        tracker.begin(instruments);
        tracker.record("old2501", true, true);

        REQUIRE_FALSE(tracker.wait_for_completion(1ms));
        tracker.record("rb2505", true, false);
        tracker.record("ag2506", true, true);

        const auto result = tracker.result();
        REQUIRE(result.successful());
        REQUIRE(result.succeeded == 2);
    }

    SECTION("a terminal request-level error completes with failure")
    {
        SubscriptionTracker tracker;
        tracker.begin(instruments);
        tracker.record("", false, true);

        REQUIRE(tracker.wait_for_completion(10ms));
        const auto result = tracker.result();
        REQUIRE_FALSE(result.successful());
        REQUIRE(result.failed == 1);
        REQUIRE(result.missing == 2);
    }

    SECTION("internal response batches do not complete an 865-instrument request early")
    {
        constexpr size_t instrument_count = 865;
        constexpr size_t response_batch_size = 34;
        std::vector<std::string> instrument_ids;
        instrument_ids.reserve(instrument_count);
        for (size_t i = 0; i < instrument_count; ++i)
        {
            instrument_ids.push_back("instrument" + std::to_string(i));
        }

        std::vector<char *> instrument_ptrs;
        instrument_ptrs.reserve(instrument_ids.size());
        for (auto &instrument_id : instrument_ids)
        {
            instrument_ptrs.push_back(instrument_id.data());
        }

        SubscriptionTracker tracker;
        tracker.begin(instrument_ptrs);
        for (size_t i = 0; i < instrument_ids.size(); ++i)
        {
            const bool is_batch_end = (i + 1) % response_batch_size == 0 || i + 1 == instrument_ids.size();
            tracker.record(instrument_ids[i], true, is_batch_end);
            if (i + 1 == response_batch_size)
            {
                REQUIRE_FALSE(tracker.result().completed);
            }
        }

        const auto result = tracker.result();
        REQUIRE(result.successful());
        REQUIRE(result.succeeded == instrument_count);
    }
}
