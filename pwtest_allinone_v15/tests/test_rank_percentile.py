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

from scrape_core import _assign_rank_percentiles, _calc_rank_score_detail


class RankPercentileTests(unittest.TestCase):
    def test_percentile_uses_unique_rank(self):
        rows = [
            {"rank_score_raw": 0.0},
            {"rank_score_raw": 0.0},
            {"rank_score_raw": 0.5},
            {"rank_score_raw": 1.0},
        ]
        _assign_rank_percentiles(rows)
        self.assertEqual(rows[0]["rank_percentile"], 0.0)
        self.assertEqual(rows[1]["rank_percentile"], 0.0)
        self.assertEqual(rows[2]["rank_percentile"], 0.5)
        self.assertEqual(rows[3]["rank_percentile"], 1.0)

    def test_percentile_top_tie_is_max(self):
        rows = [
            {"rank_score_raw": 1.0},
            {"rank_score_raw": 1.0},
            {"rank_score_raw": 0.5},
        ]
        _assign_rank_percentiles(rows)
        self.assertEqual(rows[0]["rank_percentile"], 1.0)
        self.assertEqual(rows[1]["rank_percentile"], 1.0)
        self.assertEqual(rows[2]["rank_percentile"], 0.0)

    def test_rank_lower_not_above_raw(self):
        raw, detail = _calc_rank_score_detail({"bell": 1, "maru": 5, "tel": 0}, [])
        self.assertLessEqual(detail["rank_score_lower"], raw)


if __name__ == "__main__":
    unittest.main()
