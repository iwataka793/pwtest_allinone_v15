import sys
import types
import unittest

if "playwright.sync_api" not in sys.modules:
    sync_api = types.ModuleType("playwright.sync_api")

    class DummyTimeoutError(Exception):
        pass

    def sync_playwright():
        raise RuntimeError("playwright not available in test environment")

    sync_api.sync_playwright = sync_playwright
    sync_api.TimeoutError = DummyTimeoutError
    playwright = types.ModuleType("playwright")
    playwright.sync_api = sync_api
    sys.modules["playwright"] = playwright
    sys.modules["playwright.sync_api"] = sync_api

from scrape_core import _calc_scrape_health, _calc_signal_strength, _row_quality_diag_from_stats


class ScoreHealthTests(unittest.TestCase):
    def test_scrape_health_all_dash_not_penalized(self):
        stats = {
            "ok": True,
            "header_dates": 7,
            "total_slots": 100,
            "dash": 100,
            "bell": 0,
            "maru": 0,
            "tel": 0,
            "other": 0,
        }
        diag = _row_quality_diag_from_stats(stats, frame_url="https://example.com/frame")
        score, grade, reasons, core_missing = _calc_scrape_health(diag)
        self.assertEqual(score, 100)
        self.assertEqual(grade, "OK")
        self.assertFalse(reasons)
        self.assertFalse(core_missing)

    def test_signal_strength_counts_service_days(self):
        stats = {"bell": 2, "maru": 1, "tel": 0}
        stats_by_date = {
            "2024-01-01": {"bell": 1, "maru": 0, "tel": 0},
            "2024-01-02": {"bell": 0, "maru": 1, "tel": 0},
            "2024-01-03": {"bell": 0, "maru": 0, "tel": 0},
        }
        strength, detail = _calc_signal_strength(stats, stats_by_date=stats_by_date)
        self.assertGreater(strength, 0)
        self.assertEqual(detail["signal_service_days"], 2)
        self.assertFalse(detail["all_dash"])

    def test_signal_strength_all_dash_zero(self):
        stats = {
            "total_slots": 80,
            "dash": 80,
            "bell": 0,
            "maru": 0,
            "tel": 0,
        }
        strength, detail = _calc_signal_strength(stats, stats_by_date=None)
        self.assertEqual(strength, 0.0)
        self.assertTrue(detail["all_dash"])


if __name__ == "__main__":
    unittest.main()
