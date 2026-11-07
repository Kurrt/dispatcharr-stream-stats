import unittest

from .harness import engine


class MovedDown(unittest.TestCase):
    def test_no_change(self):
        self.assertEqual(engine.moved_down([1, 2, 3], [1, 2, 3]), [])

    def test_one_stream_dropped_to_the_bottom(self):
        self.assertEqual(engine.moved_down([1, 2, 3], [2, 3, 1]), [(1, 0, 2)])

    def test_others_rising_is_not_a_fall(self):
        self.assertEqual(engine.moved_down([1, 2, 3, 4], [1, 3, 4, 2]), [(2, 1, 3)])

    def test_a_stream_jumping_to_the_top_knocks_nobody_down(self):
        self.assertEqual(engine.moved_down([1, 2, 3], [3, 1, 2]), [])

    def test_two_dropped(self):
        self.assertEqual(sorted(engine.moved_down([1, 2, 3, 4], [3, 4, 1, 2])), [(1, 0, 2), (2, 1, 3)])

    def test_added_and_removed_streams_are_ignored(self):
        self.assertEqual(engine.moved_down([1, 2, 3], [2, 9, 3]), [])
        self.assertEqual(engine.moved_down([1, 2, 3], [9, 2, 3, 1]), [(1, 0, 2)])

    def test_empty(self):
        self.assertEqual(engine.moved_down([], []), [])
        self.assertEqual(engine.moved_down([1], [2]), [])


class Good(unittest.TestCase):
    def test_worked_more_than_it_failed(self):
        self.assertTrue(engine.is_good(10, 1, 0, 0))

    def test_failed_half_the_time_is_not_good(self):
        self.assertFalse(engine.is_good(2, 1, 0, 0))

    def test_never_tried_is_not_good(self):
        self.assertFalse(engine.is_good(0, 0, 0, 0))

    def test_knocked_down_is_not_good(self):
        self.assertFalse(engine.is_good(10, 0, 1, 0))

    def test_knocked_down_then_restored_is_good(self):
        self.assertTrue(engine.is_good(10, 0, 1, 1))


class Sums(unittest.TestCase):
    def test_percent(self):
        self.assertEqual(engine.percent(1, 3), 33.3)
        self.assertIsNone(engine.percent(1, 0))

    def test_change(self):
        self.assertEqual(engine.rate_change(5, 100, 10, 100), -5.0)
        self.assertIsNone(engine.rate_change(5, 0, 10, 100))

    def test_confidence(self):
        cfg = engine.Settings()
        self.assertEqual(engine.confidence(0, 50, 100, cfg), "unproven")
        self.assertEqual(engine.confidence(20, 50, 100, cfg), "some")
        self.assertEqual(engine.confidence(100, 50, 100, cfg), "solid")

    def test_one_stream_tried_a_hundred_times_proves_little(self):
        cfg = engine.Settings()
        self.assertEqual(engine.confidence(100, 1, 1000, cfg), "unproven")
        self.assertEqual(engine.confidence(100, 5, 1000, cfg), "some")
        self.assertEqual(engine.confidence(100, 20, 1000, cfg), "solid")

    def test_a_small_provider_can_still_be_solid(self):
        self.assertEqual(engine.confidence(100, 3, 3, engine.Settings()), "solid")

    def test_a_share_cant_pass_100(self):
        self.assertEqual(engine.share(3, 1), 100.0)
        self.assertEqual(engine.share(1, 4), 25.0)
        self.assertIsNone(engine.share(1, 0))

    def test_settings_fall_back(self):
        cfg = engine.Settings({"report_days": "lots", "retention_days": -1, "thin_tries": float("nan")})
        self.assertEqual((cfg.report_days, cfg.retention_days, cfg.thin_tries), (30, 90, 20))
