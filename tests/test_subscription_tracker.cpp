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

    SECTION("re-subscription responses after reconnect complete or preserve state")
    {
        SubscriptionTracker tracker;
        tracker.begin(instruments);
        tracker.mark_request_sent(instruments);
        tracker.record("rb2505", true, false);

        // Simulate reconnect re-sending both instruments
        tracker.record("rb2505", true, false);
        tracker.record("ag2506", true, true);

        REQUIRE(tracker.wait_for_completion(10ms));
        const auto result = tracker.result();
        REQUIRE(result.successful());
        REQUIRE(result.succeeded == 2);
    }

    SECTION("re-subscription retries when first attempt fails and succeeds on subsequent attempt")
    {
        SubscriptionTracker tracker;
        tracker.begin(instruments);
        tracker.mark_request_sent(instruments);
        tracker.record("rb2505", true, false);
        tracker.record("ag2506", true, true);
        REQUIRE(tracker.result().successful());

        size_t call_count = 0;
        const bool ok = tracker.resubscribe_with_retry(
            [&](std::span<char *> req_instruments, size_t attempt) {
                ++call_count;
                REQUIRE(req_instruments.size() == 2);
                REQUIRE(std::string_view(req_instruments[0]) == "rb2505");
                REQUIRE(std::string_view(req_instruments[1]) == "ag2506");
                return attempt == 1 ? -1 : 0;
            },
            3, 1ms, []() { return false; });

        REQUIRE(ok);
        REQUIRE(call_count == 2);
        REQUIRE_FALSE(tracker.has_resubscription_failure());

        tracker.record("rb2505", true, false);
        tracker.record("ag2506", true, true);
        REQUIRE(tracker.wait_for_completion(10ms));
        REQUIRE(tracker.result().successful());
    }

    SECTION("re-subscription marks fatal failure after exhausting bounded retries")
    {
        SubscriptionTracker tracker;
        tracker.begin(instruments);
        tracker.mark_request_sent(instruments);

        size_t call_count = 0;
        const bool ok = tracker.resubscribe_with_retry(
            [&](std::span<char *>, size_t) {
                ++call_count;
                return -2;
            },
            3, 1ms, []() { return false; });

        REQUIRE_FALSE(ok);
        REQUIRE(call_count == 3);
        REQUIRE(tracker.has_resubscription_failure());
    }
}

TEST_CASE("SubscriptionTracker non-blocking reconnect re-subscription state machine", "[subscription][reconnect]")
{
    std::array<char, 8> rb{"rb2505"};
    std::array<char, 8> ag{"ag2506"};
    std::array<char *, 2> instruments{rb.data(), ag.data()};

    const auto t0 = std::chrono::steady_clock::now();

    SECTION("initial attempt succeeds and completes on callback without retries")
    {
        SubscriptionTracker tracker;
        tracker.begin(instruments);
        tracker.mark_request_sent(instruments);

        size_t calls = 0;
        auto send_fn = [&](std::span<char *> req, size_t attempt) {
            ++calls;
            REQUIRE(req.size() == 2);
            REQUIRE(attempt == 1);
            return 0;
        };

        REQUIRE(tracker.begin_reconnect_resubscribe(send_fn, 3, 200ms, 5s, t0));
        REQUIRE(calls == 1);
        REQUIRE(tracker.resubscribe_state() == SubscriptionTracker::ResubscribeState::WaitingResponse);

        tracker.record("rb2505", true, false);
        tracker.record("ag2506", true, true);
        REQUIRE(tracker.resubscribe_state() == SubscriptionTracker::ResubscribeState::Idle);
        REQUIRE_FALSE(tracker.has_resubscription_failure());

        // Polling afterwards does not trigger extra calls
        REQUIRE(tracker.poll_resubscribe(send_fn, false, t0 + 10s) == SubscriptionTracker::ResubscribeState::Idle);
        REQUIRE(calls == 1);
    }

    SECTION("retries after retry_delay when initial SubscribeMarketData send fails")
    {
        SubscriptionTracker tracker;
        tracker.begin(instruments);
        tracker.mark_request_sent(instruments);

        std::vector<size_t> attempts_seen;
        auto send_fn = [&](std::span<char *> req, size_t attempt) {
            REQUIRE(req.size() == 2);
            attempts_seen.push_back(attempt);
            return attempt == 1 ? -1 : 0;
        };

        REQUIRE_FALSE(tracker.begin_reconnect_resubscribe(send_fn, 3, 200ms, 5s, t0));
        REQUIRE(attempts_seen == std::vector<size_t>{1});
        REQUIRE(tracker.resubscribe_state() == SubscriptionTracker::ResubscribeState::WaitingRetry);
        REQUIRE_FALSE(tracker.has_resubscription_failure());

        // Before 200ms retry delay elapses, poll must NOT fire attempt 2
        REQUIRE(tracker.poll_resubscribe(send_fn, false, t0 + 100ms) ==
                SubscriptionTracker::ResubscribeState::WaitingRetry);
        REQUIRE(attempts_seen == std::vector<size_t>{1});

        // At 200ms, poll fires attempt 2 which returns 0 and awaits responses
        REQUIRE(tracker.poll_resubscribe(send_fn, false, t0 + 200ms) ==
                SubscriptionTracker::ResubscribeState::WaitingResponse);
        REQUIRE(attempts_seen == std::vector<size_t>{1, 2});
        REQUIRE_FALSE(tracker.has_resubscription_failure());

        tracker.record("rb2505", true, false);
        tracker.record("ag2506", true, true);
        REQUIRE(tracker.resubscribe_state() == SubscriptionTracker::ResubscribeState::Idle);
        REQUIRE(tracker.result().successful());
    }

    SECTION("retries when OnRspSubMarketData does not complete before response timeout")
    {
        SubscriptionTracker tracker;
        tracker.begin(instruments);
        tracker.mark_request_sent(instruments);

        std::vector<size_t> attempts_seen;
        auto send_fn = [&](std::span<char *>, size_t attempt) {
            attempts_seen.push_back(attempt);
            return 0;
        };

        REQUIRE(tracker.begin_reconnect_resubscribe(send_fn, 3, 200ms, 5s, t0));
        REQUIRE(attempts_seen == std::vector<size_t>{1});
        REQUIRE(tracker.resubscribe_state() == SubscriptionTracker::ResubscribeState::WaitingResponse);

        // Partial response does not complete and should still time out after 5s
        tracker.record("rb2505", true, false);
        REQUIRE(tracker.poll_resubscribe(send_fn, false, t0 + 4900ms) ==
                SubscriptionTracker::ResubscribeState::WaitingResponse);
        REQUIRE(attempts_seen == std::vector<size_t>{1});

        // Response timeout at t0 + 5s transitions to WaitingRetry
        REQUIRE(tracker.poll_resubscribe(send_fn, false, t0 + 5s) ==
                SubscriptionTracker::ResubscribeState::WaitingRetry);
        REQUIRE(attempts_seen == std::vector<size_t>{1});

        // Before retry delay (t0 + 5s + 100ms), does not fire attempt 2 yet
        REQUIRE(tracker.poll_resubscribe(send_fn, false, t0 + 5100ms) ==
                SubscriptionTracker::ResubscribeState::WaitingRetry);
        REQUIRE(attempts_seen == std::vector<size_t>{1});

        // After retry delay (t0 + 5s + 200ms), fires attempt 2
        REQUIRE(tracker.poll_resubscribe(send_fn, false, t0 + 5200ms) ==
                SubscriptionTracker::ResubscribeState::WaitingResponse);
        REQUIRE(attempts_seen == std::vector<size_t>{1, 2});

        // Attempt 2 receives all callbacks and succeeds
        tracker.record("rb2505", true, false);
        tracker.record("ag2506", true, true);
        REQUIRE(tracker.resubscribe_state() == SubscriptionTracker::ResubscribeState::Idle);
        REQUIRE_FALSE(tracker.has_resubscription_failure());
    }

    SECTION("marks fatal after exhausting max attempts on send failures")
    {
        SubscriptionTracker tracker;
        tracker.begin(instruments);
        tracker.mark_request_sent(instruments);

        size_t calls = 0;
        auto send_fn = [&](std::span<char *>, size_t) {
            ++calls;
            return -2;
        };

        REQUIRE_FALSE(tracker.begin_reconnect_resubscribe(send_fn, 3, 200ms, 5s, t0));
        REQUIRE(calls == 1);
        REQUIRE(tracker.resubscribe_state() == SubscriptionTracker::ResubscribeState::WaitingRetry);

        REQUIRE(tracker.poll_resubscribe(send_fn, false, t0 + 200ms) ==
                SubscriptionTracker::ResubscribeState::WaitingRetry);
        REQUIRE(calls == 2);
        REQUIRE_FALSE(tracker.has_resubscription_failure());

        REQUIRE(tracker.poll_resubscribe(send_fn, false, t0 + 400ms) ==
                SubscriptionTracker::ResubscribeState::Failed);
        REQUIRE(calls == 3);
        REQUIRE(tracker.has_resubscription_failure());

        // Subsequent polls remain Failed with no further calls
        REQUIRE(tracker.poll_resubscribe(send_fn, false, t0 + 10s) ==
                SubscriptionTracker::ResubscribeState::Failed);
        REQUIRE(calls == 3);
    }

    SECTION("marks fatal after exhausting max attempts on response timeouts")
    {
        SubscriptionTracker tracker;
        tracker.begin(instruments);
        tracker.mark_request_sent(instruments);

        size_t calls = 0;
        auto send_fn = [&](std::span<char *>, size_t) {
            ++calls;
            return 0;
        };

        REQUIRE(tracker.begin_reconnect_resubscribe(send_fn, 3, 200ms, 5s, t0));
        REQUIRE(calls == 1);

        // Attempt 1 times out at t0 + 5s -> retry at t0 + 5.2s
        REQUIRE(tracker.poll_resubscribe(send_fn, false, t0 + 5s) ==
                SubscriptionTracker::ResubscribeState::WaitingRetry);
        REQUIRE(tracker.poll_resubscribe(send_fn, false, t0 + 5200ms) ==
                SubscriptionTracker::ResubscribeState::WaitingResponse);
        REQUIRE(calls == 2);

        // Attempt 2 times out at t0 + 10.2s -> retry at t0 + 10.4s
        REQUIRE(tracker.poll_resubscribe(send_fn, false, t0 + 10200ms) ==
                SubscriptionTracker::ResubscribeState::WaitingRetry);
        REQUIRE(tracker.poll_resubscribe(send_fn, false, t0 + 10400ms) ==
                SubscriptionTracker::ResubscribeState::WaitingResponse);
        REQUIRE(calls == 3);

        // Attempt 3 times out at t0 + 15.4s -> terminal Failed
        REQUIRE(tracker.poll_resubscribe(send_fn, false, t0 + 15400ms) ==
                SubscriptionTracker::ResubscribeState::Failed);
        REQUIRE(calls == 3);
        REQUIRE(tracker.has_resubscription_failure());
    }

    SECTION("disconnect cancels pending timeout and stop predicate cancels cleanly without fatal failure")
    {
        SubscriptionTracker tracker;
        tracker.begin(instruments);
        tracker.mark_request_sent(instruments);

        size_t calls = 0;
        auto send_fn = [&](std::span<char *>, size_t) {
            ++calls;
            return 0;
        };

        REQUIRE(tracker.begin_reconnect_resubscribe(send_fn, 3, 200ms, 5s, t0));
        REQUIRE(tracker.resubscribe_state() == SubscriptionTracker::ResubscribeState::WaitingResponse);

        // Disconnect clears pending wait without marking fatal
        tracker.on_disconnected();
        REQUIRE(tracker.resubscribe_state() == SubscriptionTracker::ResubscribeState::Idle);
        REQUIRE_FALSE(tracker.has_resubscription_failure());

        // Subsequent reconnect starts fresh
        REQUIRE(tracker.begin_reconnect_resubscribe(send_fn, 3, 200ms, 5s, t0 + 6s));
        REQUIRE(calls == 2);
        REQUIRE(tracker.poll_resubscribe(send_fn, true, t0 + 7s) == SubscriptionTracker::ResubscribeState::Idle);
        REQUIRE_FALSE(tracker.has_resubscription_failure());
    }
}
