import sys
import types
import unittest

if "playwright" not in sys.modules:
    playwright = types.ModuleType("playwright")
    sync_api = types.ModuleType("playwright.sync_api")

    def _stub_sync_playwright(*_args, **_kwargs):
        raise RuntimeError("playwright is not available in tests")

    class DummyTimeoutError(Exception):
        pass

    sync_api.sync_playwright = _stub_sync_playwright
    sync_api.TimeoutError = DummyTimeoutError
    playwright.sync_api = sync_api
    sys.modules["playwright"] = playwright
    sys.modules["playwright.sync_api"] = sync_api

from scrape_core import _assign_rank_percentiles, _calc_rank_score_detail


class TestRankPercentile(unittest.TestCase):
    def test_rank_percentile_bottom_ties_zero(self):
        rows = [
            {"rank_score_raw": 0.0},
            {"rank_score_raw": 0.0},
            {"rank_score_raw": 0.0},
        ]
        _assign_rank_percentiles(rows)
        for row in rows:
            self.assertEqual(row.get("rank_percentile"), 0.0)

    def test_rank_percentile_top_is_one(self):
        rows = [
            {"rank_score_raw": 0.0},
            {"rank_score_raw": 0.0},
            {"rank_score_raw": 1.0},
            {"rank_score_raw": 2.0},
            {"rank_score_raw": 2.0},
        ]
        _assign_rank_percentiles(rows)
        self.assertEqual(rows[0].get("rank_percentile"), 0.0)
        self.assertEqual(rows[1].get("rank_percentile"), 0.0)
        self.assertEqual(rows[3].get("rank_percentile"), 1.0)
        self.assertEqual(rows[4].get("rank_percentile"), 1.0)
        self.assertAlmostEqual(rows[2].get("rank_percentile"), 0.5)

    def test_rank_lower_never_exceeds_raw(self):
        raw, detail = _calc_rank_score_detail({"bell": 3, "maru": 1, "tel": 0}, [])
        self.assertLessEqual(detail.get("rank_score_lower"), raw)
