"""Execution entry points for pwtest.

These are thin wrappers around scrape_core to keep compatibility while
providing a stable module path for future refactors.
"""
from scrape_core import run_auto_once, run_job

__all__ = ["run_auto_once", "run_job"]
