import json
import os
import time
from typing import Any, Dict, Optional

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA_ROOT = os.path.join(BASE_DIR, "score_data")
STATE_DIR = os.path.join(DATA_ROOT, "state")

JOB_FILE_NAME = "job.json"
PROGRESS_FILE_NAME = "progress.json"
STOP_FILE_NAME = "stop.flag"


def _ensure_state_dir() -> None:
    os.makedirs(STATE_DIR, exist_ok=True)


def job_state_path(path: Optional[str] = None) -> str:
    return path or os.path.join(STATE_DIR, JOB_FILE_NAME)


def progress_state_path(path: Optional[str] = None) -> str:
    return path or os.path.join(STATE_DIR, PROGRESS_FILE_NAME)


def stop_flag_path(path: Optional[str] = None) -> str:
    return path or os.path.join(STATE_DIR, STOP_FILE_NAME)


def write_job_state(payload: Dict[str, Any], path: Optional[str] = None) -> str:
    _ensure_state_dir()
    out_path = job_state_path(path)
    payload = dict(payload or {})
    payload.setdefault("updated_at", time.time())
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return out_path


def write_progress_state(payload: Dict[str, Any], path: Optional[str] = None) -> str:
    _ensure_state_dir()
    out_path = progress_state_path(path)
    payload = dict(payload or {})
    payload.setdefault("updated_at", time.time())
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return out_path


def read_job_state(path: Optional[str] = None) -> Optional[Dict[str, Any]]:
    in_path = job_state_path(path)
    if not os.path.exists(in_path):
        return None
    try:
        with open(in_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def stop_requested(path: Optional[str] = None) -> bool:
    return os.path.exists(stop_flag_path(path))


def clear_stop_flag(path: Optional[str] = None) -> None:
    stop_path = stop_flag_path(path)
    if os.path.exists(stop_path):
        try:
            os.remove(stop_path)
        except Exception:
            pass


def write_stop_flag(reason: str = "", path: Optional[str] = None) -> str:
    _ensure_state_dir()
    out_path = stop_flag_path(path)
    try:
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(reason or "stop")
    except Exception:
        pass
    return out_path
