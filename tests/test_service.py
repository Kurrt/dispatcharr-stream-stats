import json
import os
import tempfile
import unittest

from .harness import World
from stream_stats.service import render_csv, render_html


class Reporting(unittest.TestCase):
    """Two providers, with known events, so every number can be checked by hand."""

    def setUp(self):
        w = self.w = World()
        w.provider(1, "Big", 2)
        w.provider(2, "Small", 1)
        for stream_id in range(11, 21):
            w.stream(stream_id, 1)
        w.stream(29, 1, stale=1)
        w.stream(21, 2)
        w.stream(22, 2)
        # Channel 1: Big first. Channel 2: Small first. Channel 3: Big only. Channel 4: Small only.
        w.channel(1, [11, 12, 21])
        w.channel(2, [22, 13])
        w.channel(3, [14, 15, 16, 17, 18, 19])
        w.channel(4, [21])

    def known_events(self):
        w = self.w
        # Big: stream 11 dies, the failover lands on 12 on the same account, which holds.
        w.start(1, 11)
        w.tick(1)
        w.fail(1, 11, 12)
        w.tick(100)
        w.stop(1)
        w.tick(5)
        # Big: 14 works for 200 s.
        w.start(3, 14)
        w.tick(200)
        w.stop(3)
        w.tick(5)
        # Big: 15 stalls, failing over to Big's 16, which fails too, onto 17, which holds 40 s.
        w.start(3, 15)
        w.tick(10)
        w.fail(3, 15, 16, reason="health_monitor")
        w.tick(1)
        w.fail(3, 16, 17)
        w.tick(40)
        w.stop(3)
        w.tick(5)
        # Small: 22 works for 300 s.
        w.start(2, 22)
        w.tick(300)
        w.stop(2)
        w.tick(5)
        # Small: 22 fails and is rescued by Big's 13 for only 10 s, too short to be a rescue.
        w.start(2, 22)
        w.tick(1)
        w.fail(2, 22, 13)
        w.tick(10)
        w.stop(2)

    def test_counts_match_the_known_events(self):
        self.known_events()
        big = self.w.report_row(1)
        small = self.w.report_row(2)
        self.assertEqual(big["tries"], 3 + 4)  # tune-ins 11, 14, 15. Failover onto 12, 16, 17, 13.
        self.assertEqual(big["failures"], 3)
        self.assertEqual((big["fail_hard"], big["fail_stall"], big["fail_slow"]), (2, 1, 0))
        self.assertEqual(big["failures_per_100_tries"], round(100 * 3 / 7, 1))
        self.assertEqual(big["skipped"], 3)
        self.assertEqual(big["skipped_pct"], round(100 * 3 / 7, 1))
        self.assertEqual(big["streams_checked"], 7)  # 11, 12, 13, 14, 15, 16, 17
        self.assertEqual(big["streams_failed"], 3)  # 11, 15, 16
        self.assertEqual(big["streams_failed_pct"], round(100 * 3 / 7, 1))
        self.assertEqual(big["good_streams"], 4)  # 12, 13, 14, 17
        self.assertEqual(big["rescues"], 2)  # 12 held 100 s, 17 held 40 s. 13 held 10 s.
        self.assertEqual(big["channels_rescued"], 2)
        self.assertEqual(small["tries"], 2)
        self.assertEqual(small["failures"], 1)
        self.assertEqual(small["streams_checked"], 1)
        self.assertEqual(small["good_streams"], 0, "one failure in two tries isn't good")
        self.assertEqual(small["rescues"], 0)
        self.assertEqual(self.w.service.build_report()["channels_watched"], 3)

    def test_use_among_the_channels_people_watched(self):
        self.known_events()
        big = self.w.report_row(1)
        small = self.w.report_row(2)
        # Channels 1, 2 and 3 were watched. Big is in all three, Small in 1 and 2.
        self.assertEqual((big["watched_channels_served"], big["watched_channels_share_pct"]), (3, 100.0))
        self.assertEqual((small["watched_channels_served"], small["watched_channels_share_pct"]), (2, 66.7))
        # Stream 15 stalled after 10 s, and the last 8 of those had no data.
        self.assertEqual(big["hours_share_pct"], round(100 * 354 / (354 + 301), 1))
        self.assertEqual(small["hours_share_pct"], round(100 * 301 / (354 + 301), 1))

    def test_time_on_air_is_added_to_the_right_stream(self):
        self.known_events()
        self.assertAlmostEqual(self.w.report_row(1)["hours_on_air"], round((1 + 100 + 200 + 10 + 1 + 40 + 10) / 3600, 2))
        self.assertAlmostEqual(self.w.report_row(2)["hours_on_air"], round((300 + 1) / 3600, 2))
        self.assertAlmostEqual(self.w.report_row(1)["hours_per_failure"], round(362 / 3600 / 3, 2))

    def test_rank_counts(self):
        big = self.w.report_row(1)
        small = self.w.report_row(2)
        self.assertEqual((big["ranked_first"], big["ranked_top3"], big["ranked_top5"]), (2, 6, 8))
        self.assertEqual((small["ranked_first"], small["ranked_top3"], small["ranked_top5"]), (2, 3, 3))
        self.assertEqual(big["ranked_first_pct"], 50.0)
        self.assertEqual(small["ranked_top3_pct"], 33.3)
        self.assertEqual(big["ranked_top5_pct"], round(100 * 8 / 11, 1))

    def test_median_place(self):
        self.assertEqual(self.w.report_row(2)["median_position"], 1)
        # Big's places: 1, 2, 2, 1, 2, 3, 4, 5, 6.
        self.assertEqual(self.w.report_row(1)["median_position"], 2)

    def test_reach(self):
        big = self.w.report_row(1)
        small = self.w.report_row(2)
        self.assertEqual((big["streams_total"], big["streams_gone_stale"], big["streams_in_channels"]), (11, 1, 9))
        self.assertEqual((small["streams_total"], small["streams_in_channels"]), (2, 2))
        self.assertEqual((big["channels_served"], small["channels_served"]), (3, 3))
        self.assertEqual((big["channels_only_here"], small["channels_only_here"]), (1, 1))
        self.assertEqual(big["channels_share_pct"], 75.0)

    def test_a_big_broken_provider_can_still_bring_more_good_streams(self):
        self.known_events()
        report = self.w.service.build_report()
        first = report["providers"][0]
        self.assertEqual(first["provider"], "Big", "sorted by the good streams it brings")
        self.assertGreater(first["failures_per_100_tries"], 0)
        self.assertEqual(first["good_streams_estimate"], round(4 / 7 * 9))

    def test_little_data_is_marked_unproven(self):
        self.known_events()
        self.assertEqual(self.w.report_row(2)["confidence"], "unproven")

    def test_a_provider_nobody_has_watched_has_no_score(self):
        row = self.w.report_row(2)
        self.assertEqual(row["tries"], 0)
        self.assertIsNone(row["failures_per_100_tries"])
        self.assertIsNone(row["streams_failed_pct"])
        self.assertIsNone(row["good_streams_estimate"])

    def test_works_with_no_data_at_all(self):
        w = World()
        self.assertEqual(w.service.build_report()["providers"], [])
        self.assertEqual(w.service.summary()[1], "No providers found yet.")

    def test_a_deleted_account_keeps_its_history(self):
        self.known_events()
        del self.w.dp.names[2]
        row = self.w.report_row(2)
        self.assertEqual(row["provider"], "Removed account 2")
        self.assertEqual(row["tries"], 2)


class KnockDowns(unittest.TestCase):
    def setUp(self):
        w = self.w = World()
        w.provider(1, "A")
        for stream_id in (11, 12, 13):
            w.stream(stream_id, 1)
        w.channel(1, [11, 12, 13])
        w.service.take_snapshot()

    def next_day(self):
        self.w.tick(86400)

    def test_found_by_comparing_days_with_no_other_plugin(self):
        self.next_day()
        self.w.set_order(1, [12, 13, 11])
        self.assertEqual(self.w.service.take_snapshot()["knocked_down"], 1)
        row = self.w.report_row(1)
        self.assertEqual(row["streams_knocked_down"], 1)

    def test_snapshot_runs_by_itself_once_a_day(self):
        self.next_day()
        self.w.set_order(1, [12, 13, 11])
        self.w.start(1, 12)
        self.w.tick(5)
        self.w.stop(1)
        self.assertEqual(self.w.report_row(1)["streams_knocked_down"], 1)
        self.w.set_order(1, [13, 11, 12])
        self.w.start(1, 13)
        self.w.tick(5)
        self.w.stop(1)
        self.assertEqual(self.w.store._rows("SELECT COUNT(*) AS n FROM plugin_stream_stats_knockdown")[0]["n"], 1, "only once a day")

    def test_shared_row_counts_without_waiting_for_a_snapshot(self):
        self.w.service.on_shared_row("stream_demoted", self.w.uuid(1), {"stream_id": 11, "cause": "max_retries_exceeded"})
        self.assertEqual(self.w.report_row(1)["streams_knocked_down"], 1)

    def test_shared_row_and_snapshot_do_not_count_twice(self):
        self.w.set_order(1, [12, 13, 11])
        self.w.service.on_shared_row("stream_demoted", self.w.uuid(1), {"stream_id": 11, "cause": "flag"})
        self.next_day()
        self.assertEqual(self.w.service.take_snapshot()["knocked_down"], 0)
        rows = self.w.store._rows("SELECT * FROM plugin_stream_stats_knockdown")
        self.assertEqual(len(rows), 1)
        self.assertEqual((rows[0]["source"], rows[0]["cause"]), ("event", "flag"))

    def test_restored_is_counted_and_clears_good(self):
        self.w.start(1, 11)
        self.w.tick(60)
        self.w.stop(1)
        self.w.service.on_shared_row("stream_demoted", self.w.uuid(1), {"stream_id": 11, "cause": "flag"})
        self.assertEqual(self.w.report_row(1)["good_streams"], 0)
        self.w.service.on_shared_row("stream_restored", self.w.uuid(1), {"stream_id": 11})
        row = self.w.report_row(1)
        self.assertEqual((row["streams_restored"], row["good_streams"]), (1, 1))

    def test_percent_counts_only_streams_viewers_tried(self):
        # Moved by hand and never watched: it counts as knocked down, but not in the percentage.
        self.w.service.on_shared_row("stream_demoted", self.w.uuid(1), {"stream_id": 12, "cause": "flag"})
        self.w.service.on_shared_row("stream_demoted", self.w.uuid(1), {"stream_id": 13, "cause": "flag"})
        self.w.start(1, 11)
        self.w.tick(60)
        self.w.stop(1)
        row = self.w.report_row(1)
        self.assertEqual((row["streams_knocked_down"], row["streams_knocked_down_pct"]), (2, 0.0))

    def test_shared_row_with_rubbish_is_ignored(self):
        self.w.service.on_shared_row("stream_demoted", "not-a-uuid", {"stream_id": 11})
        self.w.service.on_shared_row("stream_demoted", self.w.uuid(1), {"stream_id": "x"})
        self.w.service.on_shared_row("stream_demoted", self.w.uuid(1), None)
        self.assertEqual(self.w.report_row(1)["streams_knocked_down"], 0)

    def test_ranked_first_change_over_the_window(self):
        self.w.provider(2, "B")
        self.w.stream(21, 2)
        self.w.channel(2, [21, 12])
        self.next_day()
        self.w.service.take_snapshot()
        self.assertEqual(self.w.report_row(1)["ranked_first_change"], 0)


class EventQuirks(unittest.TestCase):
    def setUp(self):
        w = self.w = World()
        w.provider(1, "A")
        for stream_id in (11, 12):
            w.stream(stream_id, 1)
        w.channel(1, [11, 12])

    def row(self):
        return self.w.report_row(1)

    def test_the_same_start_delivered_twice_counts_once(self):
        self.w.start(1, 11)
        self.w.start(1, 11)
        self.assertEqual(self.row()["tries"], 1)

    def test_the_same_switch_delivered_twice_counts_once(self):
        self.w.start(1, 11)
        self.w.fail(1, 11, 12)
        self.w.fail(1, 11, 12)
        self.assertEqual(self.row()["failures"], 1)

    def test_a_manual_switch_is_not_a_failure(self):
        self.w.start(1, 11)
        self.w.tick(5)
        self.w.fail(1, 11, 12, reason="manual")
        row = self.row()
        self.assertEqual((row["failures"], row["skipped"]), (0, 0))

    def test_false_reconnect_after_a_manual_switch_is_ignored(self):
        self.w.start(1, 11)
        self.w.fail(1, 11, 12, reason="manual")
        self.w.send("channel_reconnect", 1, stream_id=12)
        self.assertEqual(self.w.store._rows("SELECT SUM(reconnects) AS n FROM plugin_stream_stats_stream_day")[0]["n"], 0)

    def test_a_real_reconnect_counts(self):
        self.w.start(1, 11)
        self.w.tick(30)
        self.w.send("channel_reconnect", 1, stream_id=11)
        self.assertEqual(self.w.store._rows("SELECT SUM(reconnects) AS n FROM plugin_stream_stats_stream_day")[0]["n"], 1)

    def test_a_stop_with_no_start_is_harmless(self):
        self.w.stop(1)
        self.assertEqual(self.row()["tries"], 0)

    def test_unknown_channel_is_ignored(self):
        self.w.service.handle_event("channel_start", {"channel_id": "nope", "stream_id": 11})
        self.assertEqual(self.row()["tries"], 0)

    def test_buffering_and_errors_are_stored(self):
        self.w.start(1, 11)
        self.w.send("channel_buffering", 1, stream_id=11, speed=0.8)
        self.w.send("channel_error", 1, stream_id=11, error_type="connection_failed")
        rows = self.w.store._rows("SELECT buffering, errors FROM plugin_stream_stats_stream_day")
        self.assertEqual((rows[0]["buffering"], rows[0]["errors"]), (1, 1))


class Privacy(unittest.TestCase):
    def test_no_urls_names_or_viewers_in_the_data_or_exports(self):
        w = World()
        w.provider(1, "Provider")
        w.stream(11, 1)
        w.stream(12, 1)
        w.channel(1, [11, 12])
        secret = "http://provider.example/live/USERNAME/PASSWORD/1.ts"
        noisy = {
            "channel_id": w.uuid(1), "stream_id": 11, "stream_url": secret, "channel_url": secret, "url": secret,
            "new_url": secret, "user_agent": "Player/1", "client_ip": "192.0.2.9", "username": "viewer-name", "ip": "192.0.2.9",
            "stream_name": "Secret stream", "channel_name": "Secret channel", "provider_name": "Secret provider",
            "previous_stream_name": "Secret", "previous_provider_name": "Secret",
        }
        w.service.handle_event("channel_start", noisy)
        w.service.handle_event("channel_error", dict(noisy, error_type="connection_failed", error_message=secret))
        w.service.handle_event("channel_reconnect", noisy)
        w.service.handle_event("channel_buffering", dict(noisy, speed=0.5))
        w.service.handle_event("stream_switch", dict(noisy, previous_stream_id=11, stream_id=12, reason="health_monitor"))
        w.service.handle_event("channel_stop", noisy)
        w.service.handle_event("m3u_refresh", noisy)
        w.service.on_shared_row("stream_demoted", w.uuid(1), {"stream_id": 11, "cause": "flag", "url": secret, "username": "viewer-name"})
        report = w.service.build_report()
        everything = w.dump_all_tables() + json.dumps(report) + render_csv(report) + render_html(report)
        for needle in ("provider.example", "USERNAME", "PASSWORD", "192.0.2.9", "viewer-name", "Player/1", "Secret"):
            self.assertNotIn(needle, everything)

    def test_formula_like_names_are_defused_in_csv(self):
        w = World()
        w.provider(1, "=HYPERLINK(1)")
        w.stream(11, 1)
        self.assertIn("'=HYPERLINK(1)", render_csv(w.service.build_report()))

    def test_html_names_are_escaped(self):
        w = World()
        w.provider(1, "<script>x</script>")
        w.stream(11, 1)
        page = render_html(w.service.build_report())
        self.assertNotIn("<script>", page)
        self.assertIn("&lt;script&gt;", page)


class Files(unittest.TestCase):
    def test_writes_html_csv_and_json(self):
        w = World()
        w.provider(1, "A")
        w.stream(11, 1)
        with tempfile.TemporaryDirectory() as folder:
            path, _report = w.service.write_report(folder)
            self.assertEqual(os.path.basename(path), "provider-report.html")
            for name in ("provider-report.html", "provider-report.csv", "provider-report.json"):
                self.assertTrue(os.path.getsize(os.path.join(folder, name)) > 0)


class Housekeeping(unittest.TestCase):
    def test_old_data_is_dropped(self):
        w = World({"retention_days": 10})
        w.provider(1, "A")
        w.stream(11, 1)
        w.channel(1, [11])
        w.start(1, 11)
        w.tick(5)
        w.stop(1)
        w.tick(11 * 86400)
        w.send("m3u_refresh")
        self.assertEqual(w.store._rows("SELECT COUNT(*) AS n FROM plugin_stream_stats_stream_day")[0]["n"], 0)

    def test_full_orders_are_kept_for_less_time_than_the_counts(self):
        w = World({"retention_days": 90})
        w.provider(1, "A")
        w.stream(11, 1)
        w.channel(1, [11])
        w.service.take_snapshot()
        w.start(1, 11)
        w.tick(5)
        w.stop(1)
        w.tick(20 * 86400)
        w.send("m3u_refresh")
        days = w.store._rows("SELECT COUNT(*) AS n FROM plugin_stream_stats_rank")[0]["n"]
        self.assertEqual(days, 1, "only the new order remains")
        self.assertEqual(w.store._rows("SELECT COUNT(*) AS n FROM plugin_stream_stats_stream_day")[0]["n"], 1)
        self.assertEqual(w.store._rows("SELECT COUNT(*) AS n FROM plugin_stream_stats_rank_day")[0]["n"], 2)

    def test_erase_leaves_nothing(self):
        w = World()
        w.provider(1, "A")
        w.stream(11, 1)
        w.channel(1, [11])
        w.start(1, 11)
        w.store.erase()
        self.assertEqual(w.dump_all_tables(), "")


class FromTheCheck(unittest.TestCase):
    def test_a_late_event_for_a_deleted_stream_keeps_its_provider(self):
        w = World()
        w.provider(1, "P")
        w.provider(2, "Q")
        w.stream(1, 1)
        w.stream(2, 2)
        w.channel(1, [1, 2])
        w.start(1, 1)
        w.tick(60)
        w.conn.execute("DELETE FROM dispatcharr_channels_stream WHERE id = 1")
        w.conn.execute("DELETE FROM dispatcharr_channels_channelstream WHERE stream_id = 1")
        w.fail(1, 1, 2)
        self.assertEqual(w.report_row(1)["tries"], 1)
        self.assertEqual(w.report_row(1)["failures"], 1)

    def test_checked_cant_pass_100_when_streams_leave_their_channels(self):
        w = World()
        w.provider(1, "P")
        for stream in (1, 2, 3):
            w.stream(stream, 1)
        w.channel(1, [1, 2, 3])
        for stream in (1, 2, 3):
            w.start(1, stream)
            w.tick(40)
            w.stop(1)
            w.tick(40)
        w.conn.execute("DELETE FROM dispatcharr_channels_channelstream WHERE stream_id IN (2, 3)")
        self.assertEqual(w.report_row(1)["streams_checked_pct"], 100.0)

    def test_skipped_cant_pass_100(self):
        w = World()
        w.provider(1, "P")
        w.stream(1, 1)
        w.stream(2, 1)
        w.channel(1, [1, 2])
        w.start(1, 2)
        w.tick(10)
        for _ in range(3):
            w.fail(1, 1, 2)
            w.tick(10)
        row = w.report_row(1)
        self.assertLessEqual(row["skipped_pct"], 100.0)
        self.assertLessEqual(row["streams_failed_pct"], 100.0)

    def test_one_stream_tried_often_isnt_solid_and_gets_no_estimate(self):
        w = World()
        w.provider(1, "P")
        for stream in range(1, 1001):
            w.stream(stream, 1)
        for channel in range(1, 501):
            w.channel(channel, [channel * 2 - 1, channel * 2])
        for _ in range(100):
            w.start(1, 1)
            w.tick(40)
            w.stop(1)
            w.tick(40)
        row = w.report_row(1)
        self.assertEqual(row["confidence"], "unproven")
        self.assertIsNone(row["good_streams_estimate"])

    def test_time_on_air_leaves_out_the_silence(self):
        w = World()
        w.provider(1, "P")
        w.stream(1, 1)
        w.stream(2, 1)
        w.channel(1, [1, 2])
        uuid = w.uuid(1)
        started = w.clock.now
        w.start(1, 1)
        w.tick(10)
        # Nothing ever arrived on this stream: the last data predates it.
        w.dp.last_data[uuid] = started - 5
        w.fail(1, 1, 2, reason="health_monitor")
        del w.dp.last_data[uuid]
        w.tick(100)
        w.stop(1)
        rows = dict(w.conn.execute("SELECT stream_id, seconds FROM plugin_stream_stats_stream_day").fetchall())
        self.assertEqual(rows[1], 0.0)
        self.assertEqual(rows[2], 100.0)

    def test_bytes_from_the_next_stream_dont_count_for_the_silent_one(self):
        w = World()
        w.provider(1, "P")
        w.stream(1, 1)
        w.stream(2, 1)
        w.channel(1, [1, 2])
        w.start(1, 1)
        w.tick(8)
        # By the time this event is handled, the stream it moved to has started delivering.
        w.fail(1, 1, 2, reason="health_monitor")
        rows = dict(w.conn.execute("SELECT stream_id, seconds FROM plugin_stream_stats_stream_day").fetchall())
        self.assertEqual(rows[1], 0.0)

    def test_a_stall_with_nothing_on_record_still_takes_off_the_wait(self):
        w = World()
        w.provider(1, "P")
        w.stream(1, 1)
        w.stream(2, 1)
        w.channel(1, [1, 2])
        uuid = w.uuid(1)
        w.start(1, 1)
        w.tick(100)
        w.dp.last_data[uuid] = None
        w.fail(1, 1, 2, reason="health_monitor")
        rows = dict(w.conn.execute("SELECT stream_id, seconds FROM plugin_stream_stats_stream_day").fetchall())
        self.assertEqual(rows[1], 92.0)

    def test_time_on_air_stops_when_the_data_did(self):
        w = World()
        w.provider(1, "P")
        w.stream(1, 1)
        w.stream(2, 1)
        w.channel(1, [1, 2])
        uuid = w.uuid(1)
        w.start(1, 1)
        w.tick(100)
        w.dp.last_data[uuid] = w.clock.now
        w.tick(25)
        w.fail(1, 1, 2, reason="health_monitor")
        rows = dict(w.conn.execute("SELECT stream_id, seconds FROM plugin_stream_stats_stream_day").fetchall())
        self.assertEqual(rows[1], 100.0)

    def test_a_stop_after_dispatcharr_has_forgotten_counts_the_whole_time(self):
        w = World()
        w.provider(1, "P")
        w.stream(1, 1)
        w.channel(1, [1])
        uuid = w.uuid(1)
        w.start(1, 1)
        w.tick(100)
        w.dp.last_data[uuid] = None
        w.stop(1)
        rows = dict(w.conn.execute("SELECT stream_id, seconds FROM plugin_stream_stats_stream_day").fetchall())
        self.assertEqual(rows[1], 100.0)

    def test_the_seven_day_windows_are_seven_days(self):
        w = World()
        calls = []
        real = w.store.failure_window
        w.store.failure_window = lambda first, last: calls.append((first, last)) or real(first, last)
        w.provider(1, "P")
        w.service.build_report()
        import time

        days = [time.strptime(item, "%Y-%m-%d") for pair in calls for item in pair]
        spans = [(time.mktime(days[i + 1]) - time.mktime(days[i])) / 86400 for i in (0, 2)]
        self.assertEqual(spans, [6.0, 6.0], "seven calendar days each, counting both ends")
