import re, sys, json, time, threading, webbrowser, os, shutil, math
import datetime
import calendar
from collections import deque
from dataclasses import dataclass
from urllib.parse import urlparse, urljoin
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError
from core.state import (
    clear_stop_flag,
    job_state_path,
    progress_state_path,
    read_job_state,
    stop_requested,
    write_job_state,
    write_progress_state,
)

# ======== 内部設定（基本いじらなくてOK）========
NAV_TIMEOUT_MS = 45000
CAL_WAIT_MS = 20000
CAL_WAIT_STEP_MS = 500
AFTER_GOTO_WAIT_MS = 600
CAL_WAIT_SHORT_MS = 2000
CAL_WAIT_LONG_MS = 10000
TEXT_HIT_REPROBE_MS = 1800
_ALL_DASH_RATIO = 0.95
_ALL_DASH_MIN_SLOTS = 40
_ALL_DASH_GRACE_MS = 2500
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
PRESETS_FILE = os.path.join(BASE_DIR, "presets.json")
PROFILE_DIR = os.path.join(BASE_DIR, "pw_profile")  # ここにCookie等を保持（存在すれば自動で使用）
# ==============================================
# ---- 通信高速化（必要に応じて調整OK） ----
# 画像/フォント/メディア + 主要トラッカー類をブロックして高速化します。
ENABLE_NET_BLOCK = True
BLOCK_RESOURCE_TYPES = {"image", "media", "font"}
BLOCK_URL_SUBSTR = [
    "googletagmanager", "google-analytics", "doubleclick", "g.doubleclick",
    "rubiconproject", "adnxs", "gsspat", "sc-analytics", "d-markets",
    "translate.google.com",
]

DATA_ROOT = os.path.join(BASE_DIR, "score_data")
STATE_DIR = os.path.join(DATA_ROOT, "state")
RUNS_DIR = os.path.join(DATA_ROOT, "runs")
# ==============================================
ANALYTICS_DIR = os.path.join(DATA_ROOT, "analytics")
HISTORY_DIR = os.path.join(DATA_ROOT, "history")
# ==============================================

# ==============================================
LOG_DIR = os.path.join(DATA_ROOT, "logs")
DAILY_DIR = os.path.join(DATA_ROOT, "daily")
NOTIFY_DIR = os.path.join(DATA_ROOT, "notifications")
CONFIG_PATH = os.path.join(DATA_ROOT, "config.json")
# ==============================================

# ---- 精密ログ（PowerShell/ファイル/GUI） ----
_CURRENT_RUN_DIR = None
_DETAIL_LOG_ENV_KEYS = ("DETAIL_LOG", "PW_DETAIL_LOG", "SCRAPE_DETAIL_LOG")
_CAL_EXPECT_TOTAL_SLOTS = 238
_CAL_EXPECT_TIME_ROWS = 34
_CAL_EXPECT_COLS = 7
_CAL_MIN_TIME_ROWS = 20
_CAL_MAX_TIME_ROWS = 60
_CAL_MIN_COLS = 5
_CAL_MAX_COLS = 8
_CAL_TD_MIN_RATIO = 0.30
_CAL_TD_MAX_RATIO = 2.50
_QUALITY_HEADER_DATES_MIN = 3
_QUALITY_OK_MIN = 80
_QUALITY_WARN_MIN = 60
_QUALITY_ALL_DASH_PENALTY = 8
_RESERVATION_BLOCK_TEXTS = [
    "予約できません",
    "予約できない",
    "予約不可",
    "指名できません",
    "別の女の子",
    "お手数ですが",
    "お店の空き状況を見る",
]
_LOGIN_IFRAME_KEYWORDS = (
    "ReservationLogin",
    "ShareToReservationLogin",
    "S6ShareToReservationLogin",
)
_SUSPICIOUS_SCAN_MAX_BYTES = 200000
_SUSPICIOUS_EXCERPT_CHARS = 180
_SUSPICIOUS_DEFAULT_MAX_PER_RUN = 4
_SUSPICIOUS_DEFAULT_MAX_PER_PRESET = 2
_SUSPICIOUS_DEFAULT_MAX_HTML_BYTES = 200000
_SUSPICIOUS_DUMP_COUNTS = {}
_SUSPICIOUS_DUMP_PRESET_COUNTS = {}
_SUSPICIOUS_DUMP_STATS = {}
_SUSPICIOUS_DUMP_LOCK = threading.Lock()
_SUSPICIOUS_MARKERS = (
    {"label": "cf-chl", "token": "cf-chl", "vendor": "cloudflare", "strength": "strong"},
    {"label": "cf-turnstile", "token": "cf-turnstile", "vendor": "cloudflare", "strength": "strong"},
    {"label": "challenges.cloudflare.com", "token": "challenges.cloudflare.com", "vendor": "cloudflare", "strength": "strong"},
    {"label": "turnstile", "token": "turnstile", "vendor": "turnstile", "strength": "strong"},
    {"label": "hcaptcha", "token": "hcaptcha", "vendor": "hcaptcha", "strength": "strong"},
    {"label": "recaptcha", "token": "recaptcha", "vendor": "recaptcha", "strength": "weak"},
    {"label": "captcha", "token": "captcha", "vendor": None, "strength": "weak"},
    {"label": "access denied", "token": "access denied", "vendor": None, "strength": "strong"},
    {"label": "forbidden", "token": "forbidden", "vendor": None, "strength": "strong"},
    {"label": "too many requests", "token": "too many requests", "vendor": None, "strength": "strong"},
    {"label": "blocked", "token": "blocked", "vendor": None, "strength": "strong"},
    {"label": "enable javascript", "token": "enable javascript", "vendor": None, "strength": "weak"},
    {"label": "please enable cookies", "token": "please enable cookies", "vendor": None, "strength": "weak"},
    {"label": "attention required", "token": "attention required", "vendor": "cloudflare", "strength": "strong"},
    {"label": "verify you are human", "token": "verify you are human", "vendor": None, "strength": "strong"},
    {"label": "unusual traffic", "token": "unusual traffic", "vendor": None, "strength": "strong"},
    {"label": "datadome", "token": "datadome", "vendor": "datadome", "strength": "strong"},
    {"label": "perimeterx", "token": "perimeterx", "vendor": "perimeterx", "strength": "strong"},
    {
        "label": "robot check",
        "regex": re.compile(r"are you a robot|not a robot|robot check", re.I),
        "vendor": None,
        "strength": "strong",
    },
)

_SUSPICIOUS_LIGHT_TITLE_TOKENS = (
    "attention required",
    "just a moment",
    "verify you are human",
    "access denied",
)
_SUSPICIOUS_LIGHT_PROBE_MAX_CHARS = 80000
_SUSPICIOUS_LIGHT_SELECTORS = (
    ("cf_challenge_iframe", "iframe[src*='challenges.cloudflare.com']", "cloudflare"),
    ("cf_turnstile", "[name='cf-turnstile-response']", "cloudflare"),
    ("cf_challenge_dom", "#cf-challenge", "cloudflare"),
    ("recaptcha", "iframe[src*='recaptcha']", "recaptcha"),
    ("hcaptcha", "iframe[src*='hcaptcha']", "hcaptcha"),
)

def _probe_suspicious_selectors_sync(page, fr):
    for label, selector, vendor in _SUSPICIOUS_LIGHT_SELECTORS:
        try:
            if page.query_selector(selector):
                return label, vendor
        except Exception:
            pass
        if fr:
            try:
                if fr.query_selector(selector):
                    return label, vendor
            except Exception:
                pass
    return None, None

async def _probe_suspicious_selectors_async(page, fr):
    for label, selector, vendor in _SUSPICIOUS_LIGHT_SELECTORS:
        try:
            if await page.query_selector(selector):
                return label, vendor
        except Exception:
            pass
        if fr:
            try:
                if await fr.query_selector(selector):
                    return label, vendor
            except Exception:
                pass
    return None, None

def _probe_suspicious_title_sync(page):
    try:
        title = (page.title() or "").strip()
    except Exception:
        title = ""
    lower = title.lower()
    for token in _SUSPICIOUS_LIGHT_TITLE_TOKENS:
        if token in lower:
            return token
    return ""

async def _probe_suspicious_title_async(page):
    try:
        title = (await page.title()) or ""
    except Exception:
        title = ""
    lower = title.strip().lower()
    for token in _SUSPICIOUS_LIGHT_TITLE_TOKENS:
        if token in lower:
            return token
    return ""

def _probe_suspicious_snippet_sync(target):
    try:
        return target.evaluate(
            f"""() => {{
  const el = document.documentElement;
  if (!el) return "";
  const html = el.outerHTML || "";
  return html.slice(0, {_SUSPICIOUS_LIGHT_PROBE_MAX_CHARS});
}}"""
        )
    except Exception:
        return ""

async def _probe_suspicious_snippet_async(target):
    try:
        return await target.evaluate(
            f"""() => {{
  const el = document.documentElement;
  if (!el) return "";
  const html = el.outerHTML || "";
  return html.slice(0, {_SUSPICIOUS_LIGHT_PROBE_MAX_CHARS});
}}"""
        )
    except Exception:
        return ""

def _light_probe_suspicious_sync(page, fr):
    label, vendor = _probe_suspicious_selectors_sync(page, fr)
    if label:
        return {"hit": True, "label": label, "vendor": vendor, "strength": "strong"}
    title_token = _probe_suspicious_title_sync(page)
    if title_token:
        return {"hit": True, "label": f"title:{title_token}", "vendor": None, "strength": "strong"}
    page_snip = _probe_suspicious_snippet_sync(page)
    frame_snip = _probe_suspicious_snippet_sync(fr) if fr else ""
    detect = _detect_suspicious_markers(page_snip, frame_snip)
    if detect.get("suspicious_hit"):
        return {"hit": True, "label": "snippet", "vendor": None, "strength": detect.get("strength") or "weak"}
    return {"hit": False}

async def _light_probe_suspicious_async(page, fr):
    label, vendor = await _probe_suspicious_selectors_async(page, fr)
    if label:
        return {"hit": True, "label": label, "vendor": vendor, "strength": "strong"}
    title_token = await _probe_suspicious_title_async(page)
    if title_token:
        return {"hit": True, "label": f"title:{title_token}", "vendor": None, "strength": "strong"}
    page_snip = await _probe_suspicious_snippet_async(page)
    frame_snip = await _probe_suspicious_snippet_async(fr) if fr else ""
    detect = _detect_suspicious_markers(page_snip, frame_snip)
    if detect.get("suspicious_hit"):
        return {"hit": True, "label": "snippet", "vendor": None, "strength": detect.get("strength") or "weak"}
    return {"hit": False}

def _probe_login_like_iframe_sync(page):
    iframe_src = ""
    iframe_h = 0.0
    iframe_frame_url = ""
    login_like = False
    try:
        ih = page.query_selector("iframe[name='pcreserveiframe'], iframe#pcreserveiframe, iframe[src*='yoyaku.cityheaven.net'], iframe[src*='A6ShopReservation']")
    except Exception:
        ih = None
    if ih:
        try:
            iframe_src = ih.get_attribute("src") or ""
        except Exception:
            iframe_src = ""
        try:
            iframe_h = float(ih.evaluate("el => el.getBoundingClientRect().height") or 0.0)
        except Exception:
            iframe_h = 0.0
        try:
            fr = ih.content_frame()
        except Exception:
            fr = None
        if fr:
            try:
                iframe_frame_url = fr.url or ""
            except Exception:
                iframe_frame_url = ""
        if iframe_src and any(kw in iframe_src for kw in _LOGIN_IFRAME_KEYWORDS) and iframe_h <= 300:
            login_like = True
    return login_like, iframe_src, iframe_h, iframe_frame_url

async def _probe_login_like_iframe_async(page):
    iframe_src = ""
    iframe_h = 0.0
    iframe_frame_url = ""
    login_like = False
    try:
        ih = await page.query_selector("iframe[name='pcreserveiframe'], iframe#pcreserveiframe, iframe[src*='yoyaku.cityheaven.net'], iframe[src*='A6ShopReservation']")
    except Exception:
        ih = None
    if ih:
        try:
            iframe_src = (await ih.get_attribute("src")) or ""
        except Exception:
            iframe_src = ""
        try:
            iframe_h = float(await ih.evaluate("el => el.getBoundingClientRect().height") or 0.0)
        except Exception:
            iframe_h = 0.0
        try:
            fr = await ih.content_frame()
        except Exception:
            fr = None
        if fr:
            try:
                iframe_frame_url = fr.url or ""
            except Exception:
                iframe_frame_url = ""
        if iframe_src and any(kw in iframe_src for kw in _LOGIN_IFRAME_KEYWORDS) and iframe_h <= 300:
            login_like = True
    return login_like, iframe_src, iframe_h, iframe_frame_url

def _detail_log_enabled() -> bool:
    for key in _DETAIL_LOG_ENV_KEYS:
        val = os.environ.get(key, "")
        if val and str(val).strip().lower() not in ("0", "false", "no", "off"):
            return True
    return False

def _env_flag(key: str) -> bool:
    val = os.environ.get(key, "")
    return bool(val and str(val).strip().lower() not in ("0", "false", "no", "off"))

def _valid_url(url: str) -> bool:
    if not url or not str(url).strip():
        return False
    raw = str(url).strip()
    if raw.lower() in ("about:blank", "about:"):
        return False
    try:
        u = urlparse(raw)
    except Exception:
        return False
    if u.scheme not in ("http", "https") or not u.netloc:
        return False
    return True

def _limit_text_bytes(text: str, max_bytes: int) -> str:
    if not text:
        return ""
    try:
        raw = text.encode("utf-8")
    except Exception:
        return text[:max_bytes] if max_bytes else text
    if max_bytes and len(raw) > max_bytes:
        return raw[:max_bytes].decode("utf-8", errors="ignore")
    return text

def _make_excerpt(text: str, span: tuple[int, int], max_chars: int) -> str:
    if not text or not span or max_chars <= 0:
        return ""
    start, end = span
    half = max(10, max_chars // 2)
    left = max(0, start - half)
    right = min(len(text), end + half)
    excerpt = text[left:right]
    excerpt = re.sub(r"\s+", " ", excerpt).strip()
    if len(excerpt) > max_chars:
        excerpt = excerpt[:max_chars].rstrip()
    return excerpt

def _detect_suspicious_markers(page_html: str, frame_html: str) -> dict:
    combined = (page_html or "")[:_SUSPICIOUS_SCAN_MAX_BYTES] + "\n" + (frame_html or "")[:_SUSPICIOUS_SCAN_MAX_BYTES]
    if not combined.strip():
        return {
            "suspicious_hit": False,
            "markers_hit": [],
            "vendors": [],
            "strength": None,
            "excerpt": "",
        }
    lower = combined.lower()
    markers = []
    vendors = []
    strengths = []
    first_span = None
    for marker in _SUSPICIOUS_MARKERS:
        label = marker.get("label")
        strength = marker.get("strength")
        vendor = marker.get("vendor")
        if marker.get("token"):
            token = marker["token"]
            idx = lower.find(token)
            if idx >= 0:
                if label and label not in markers:
                    markers.append(label)
                    strengths.append(strength)
                    if vendor and vendor not in vendors:
                        vendors.append(vendor)
                if first_span is None:
                    first_span = (idx, idx + len(token))
        elif marker.get("regex"):
            m = marker["regex"].search(combined)
            if m:
                if label and label not in markers:
                    markers.append(label)
                    strengths.append(strength)
                    if vendor and vendor not in vendors:
                        vendors.append(vendor)
                if first_span is None:
                    first_span = m.span()
    hit = bool(markers)
    strength = None
    if hit:
        if strengths and all(s == "weak" for s in strengths):
            strength = "weak"
        else:
            strength = "strong"
    excerpt = _make_excerpt(combined, first_span, _SUSPICIOUS_EXCERPT_CHARS) if first_span else ""
    return {
        "suspicious_hit": hit,
        "markers_hit": markers,
        "vendors": vendors,
        "strength": strength,
        "excerpt": excerpt,
    }

def _current_run_id() -> str:
    run_dir = _CURRENT_RUN_DIR or ""
    if run_dir:
        base = os.path.basename(run_dir.rstrip("/\\"))
        if base.startswith("run_"):
            return base.replace("run_", "", 1)
        if base:
            return base
    return _now_ts()

def _get_suspicious_debug_config() -> dict:
    cfg = load_config()
    debug_cfg = cfg.get("debug", {}) if isinstance(cfg, dict) else {}
    suspicious_cfg = debug_cfg.get("suspicious", {}) if isinstance(debug_cfg, dict) else {}
    enabled = suspicious_cfg.get("enabled", True)
    max_per_run = int(suspicious_cfg.get("max_per_run", _SUSPICIOUS_DEFAULT_MAX_PER_RUN) or 0)
    max_per_preset = int(suspicious_cfg.get("max_per_preset", _SUSPICIOUS_DEFAULT_MAX_PER_PRESET) or 0)
    max_html_bytes = int(suspicious_cfg.get("max_html_bytes", _SUSPICIOUS_DEFAULT_MAX_HTML_BYTES) or 0)
    full_page = bool(suspicious_cfg.get("full_page", False))
    return {
        "enabled": bool(enabled),
        "max_per_run": max_per_run,
        "max_per_preset": max_per_preset,
        "max_html_bytes": max_html_bytes,
        "full_page": full_page,
    }

def _get_suspicious_dump_stats(run_id: str) -> dict:
    return _SUSPICIOUS_DUMP_STATS.setdefault(run_id, {"saved": 0, "suppressed": 0, "presets": {}})

def _record_suspicious_dump_saved(run_id: str, preset: str):
    stats = _get_suspicious_dump_stats(run_id)
    stats["saved"] += 1
    preset_stats = stats["presets"].setdefault(preset, {"saved": 0, "suppressed": 0})
    preset_stats["saved"] += 1

def _record_suspicious_dump_suppressed(run_id: str, preset: str):
    stats = _get_suspicious_dump_stats(run_id)
    stats["suppressed"] += 1
    preset_stats = stats["presets"].setdefault(preset, {"saved": 0, "suppressed": 0})
    preset_stats["suppressed"] += 1

def _claim_suspicious_dump_slot(run_id: str, preset: str, max_per_run: int, max_per_preset: int) -> bool:
    if max_per_run <= 0 or max_per_preset <= 0:
        _record_suspicious_dump_suppressed(run_id, preset)
        return False
    with _SUSPICIOUS_DUMP_LOCK:
        count = _SUSPICIOUS_DUMP_COUNTS.get(run_id, 0)
        preset_counts = _SUSPICIOUS_DUMP_PRESET_COUNTS.setdefault(run_id, {})
        preset_count = preset_counts.get(preset, 0)
        if count >= max_per_run or preset_count >= max_per_preset:
            _record_suspicious_dump_suppressed(run_id, preset)
            return False
        _SUSPICIOUS_DUMP_COUNTS[run_id] = count + 1
        preset_counts[preset] = preset_count + 1
        _record_suspicious_dump_saved(run_id, preset)
        return True

def _get_suspicious_dump_summary(run_id: str, preset: str = None) -> dict:
    stats = _get_suspicious_dump_stats(run_id)
    if preset:
        preset_stats = stats.get("presets", {}).get(preset, {"saved": 0, "suppressed": 0})
        return {"saved": preset_stats.get("saved", 0), "suppressed": preset_stats.get("suppressed", 0)}
    return {"saved": stats.get("saved", 0), "suppressed": stats.get("suppressed", 0)}

def _suspicious_debug_dir(run_id: str, preset: str, gid: str) -> str:
    ensure_data_dirs()
    base = os.path.join(DATA_ROOT, "debug", "suspicious", run_id)
    safe_preset = _safe_name(preset or "preset")
    safe_gid = _safe_name(gid or "gid")
    path = os.path.join(base, safe_preset, safe_gid)
    os.makedirs(path, exist_ok=True)
    return path

def _dump_suspicious_debug_sync(page, fr, preset: str, gid: str, page_html: str, frame_html: str, meta: dict):
    try:
        cfg = _get_suspicious_debug_config()
        if not cfg.get("enabled"):
            return None
        run_id = _current_run_id()
        if not _claim_suspicious_dump_slot(
            run_id,
            preset or "preset",
            cfg.get("max_per_run", 0),
            cfg.get("max_per_preset", 0),
        ):
            return None
        out_dir = _suspicious_debug_dir(run_id, preset, gid)
        ts = _now_ts()
        prefix = os.path.join(out_dir, ts)
        paths = {}
        max_html_bytes = cfg.get("max_html_bytes", _SUSPICIOUS_DEFAULT_MAX_HTML_BYTES)

        try:
            html = _limit_text_bytes(page_html or "", max_html_bytes)
            if html:
                p_html = prefix + "_page.html"
                with open(p_html, "w", encoding="utf-8") as w:
                    w.write(html)
                paths["page_html"] = p_html
        except Exception:
            pass

        try:
            html = _limit_text_bytes(frame_html or "", max_html_bytes)
            if html:
                p_html = prefix + "_frame.html"
                with open(p_html, "w", encoding="utf-8") as w:
                    w.write(html)
                paths["frame_html"] = p_html
        except Exception:
            pass

        try:
            p_png = prefix + "_page.png"
            page.screenshot(path=p_png, full_page=cfg.get("full_page", False))
            paths["page_png"] = p_png
        except Exception:
            pass

        try:
            if fr:
                el = fr.frame_element()
                p_png = prefix + "_frame.png"
                el.screenshot(path=p_png)
                paths["frame_png"] = p_png
        except Exception:
            pass

        try:
            meta_path = prefix + "_meta.json"
            with open(meta_path, "w", encoding="utf-8") as w:
                json.dump(meta or {}, w, ensure_ascii=False, indent=2)
            paths["meta_json"] = meta_path
        except Exception:
            pass

        log_event(
            "DBG",
            "suspicious dump saved",
            preset=preset,
            gid=gid,
            run_id=run_id,
            files=paths,
        )
        return paths
    except Exception:
        return None

async def _dump_suspicious_debug_async(page, fr, preset: str, gid: str, page_html: str, frame_html: str, meta: dict):
    try:
        cfg = _get_suspicious_debug_config()
        if not cfg.get("enabled"):
            return None
        run_id = _current_run_id()
        if not _claim_suspicious_dump_slot(
            run_id,
            preset or "preset",
            cfg.get("max_per_run", 0),
            cfg.get("max_per_preset", 0),
        ):
            return None
        out_dir = _suspicious_debug_dir(run_id, preset, gid)
        ts = _now_ts()
        prefix = os.path.join(out_dir, ts)
        paths = {}
        max_html_bytes = cfg.get("max_html_bytes", _SUSPICIOUS_DEFAULT_MAX_HTML_BYTES)

        try:
            html = _limit_text_bytes(page_html or "", max_html_bytes)
            if html:
                p_html = prefix + "_page.html"
                with open(p_html, "w", encoding="utf-8") as w:
                    w.write(html)
                paths["page_html"] = p_html
        except Exception:
            pass

        try:
            html = _limit_text_bytes(frame_html or "", max_html_bytes)
            if html:
                p_html = prefix + "_frame.html"
                with open(p_html, "w", encoding="utf-8") as w:
                    w.write(html)
                paths["frame_html"] = p_html
        except Exception:
            pass

        try:
            p_png = prefix + "_page.png"
            await page.screenshot(path=p_png, full_page=cfg.get("full_page", False))
            paths["page_png"] = p_png
        except Exception:
            pass

        try:
            if fr:
                el = await fr.frame_element()
                p_png = prefix + "_frame.png"
                await el.screenshot(path=p_png)
                paths["frame_png"] = p_png
        except Exception:
            pass

        try:
            meta_path = prefix + "_meta.json"
            with open(meta_path, "w", encoding="utf-8") as w:
                json.dump(meta or {}, w, ensure_ascii=False, indent=2)
            paths["meta_json"] = meta_path
        except Exception:
            pass

        log_event(
            "DBG",
            "suspicious dump saved",
            preset=preset,
            gid=gid,
            run_id=run_id,
            files=paths,
        )
        return paths
    except Exception:
        return None

def _detail_log_skip(preset: str = None, gid: str = None, reason: str = ""):
    if not _detail_log_enabled():
        return
    log_event("INFO", "bd/skip", preset=preset, gid=gid, reason=reason)

def _detail_log_iframe_wait(preset: str = None, gid: str = None, short_ms: int = 0, long_ms: int = 0, used: str = ""):
    if not _detail_log_enabled():
        return
    log_event(
        "INFO",
        "iframe_wait",
        preset=preset,
        gid=gid,
        short_ms=short_ms,
        long_ms=long_ms,
        used=used,
    )

def _detail_log_probe(preset: str = None, gid: str = None, stage: str = "", **kv):
    if not _detail_log_enabled():
        return
    data = {"preset": preset, "gid": gid, "stage": stage}
    data.update(kv)
    log_event("INFO", "reservation probe", **data)

def _should_reprobe_text_hit(probe: dict) -> bool:
    if not isinstance(probe, dict):
        return False
    if not probe.get("text_hit"):
        return False
    if probe.get("has_calendar_table"):
        return False
    if probe.get("error_page") or probe.get("login_like"):
        return False
    return True

def _login_frame_url_like(url: str) -> bool:
    if not url:
        return False
    return any(kw in url for kw in _LOGIN_IFRAME_KEYWORDS)

def _should_skip_login_like(probe: dict) -> bool:
    if not isinstance(probe, dict):
        return False
    if not probe.get("login_like"):
        return False
    if probe.get("has_calendar_table"):
        return False
    frame_url = probe.get("iframe_frame_url") or ""
    return _login_frame_url_like(frame_url)

def _effective_not_reservable_without_login(probe: dict) -> bool:
    if not isinstance(probe, dict):
        return False
    return ((probe.get("text_hit") and not probe.get("has_calendar_table")) or probe.get("error_page"))

def _looks_like_all_dash(stats: dict) -> bool:
    if not isinstance(stats, dict):
        return False
    total = int(stats.get("total_slots") or 0)
    if total < _ALL_DASH_MIN_SLOTS:
        return False
    dash = int(stats.get("dash") or 0)
    bookable = int(stats.get("bell") or 0) + int(stats.get("maru") or 0) + int(stats.get("tel") or 0)
    if bookable != 0:
        return False
    return (dash / total) >= _ALL_DASH_RATIO

def _is_not_reservable_page_sync(page) -> tuple[bool, str]:
    """
    例外ページ（「該当の女の子は予約できません」等）を検出して、カレンダー待ちの無駄なタイムアウトを回避する。
    注意: エラーメッセージが iframe 内に出るケースがあるため、page だけでなく frames も走査する。
    """
    def _check_ctx(ctx, label: str, require_reserve_ui: bool) -> tuple[bool, str]:
        try:
            cur_url = (ctx.url or "")
        except Exception:
            cur_url = ""
        if cur_url and ("yoyaku.cityheaven.net/error" in cur_url or "EFRESV" in cur_url):
            return True, f"{label}:url_error"

        try:
            err = ctx.query_selector("div.error-msg")
        except Exception:
            err = None

        msg = ""
        if err:
            try:
                msg = ctx.locator("div.error-msg").first.inner_text(timeout=500)
            except Exception:
                try:
                    msg = err.text_content() or ""
                except Exception:
                    msg = ""
            msg = (msg or "").strip()

        # 念のため、div.error-msg が無い場合も本文キーワードで拾う
        body_txt = ""
        if not msg:
            try:
                body_txt = ctx.locator("body").inner_text(timeout=300)
            except Exception:
                body_txt = ""
            body_txt = (body_txt or "").strip()

        hit_txt = msg or body_txt
        if not hit_txt:
            return False, ""

        if "予約できません" not in hit_txt and "予約できない" not in hit_txt and "予約不可" not in hit_txt:
            return False, ""

        if require_reserve_ui:
            try:
                has_reserve_ui = bool(page.query_selector("#shop-reservation"))
            except Exception:
                has_reserve_ui = False
            if not has_reserve_ui:
                try:
                    has_reserve_ui = bool(page.query_selector("iframe[name='pcreserveiframe'], iframe#pcreserveiframe"))
                except Exception:
                    has_reserve_ui = False
            if not has_reserve_ui:
                return False, ""

        helpers = []
        for kw in ("別の女の子", "お手数ですが", "該当の女の子"):
            if kw in hit_txt:
                helpers.append(kw)
        reason = f"{label}:予約できません"
        if helpers:
            reason = f"{reason}/" + ",".join(helpers)
        return True, reason

    ok, reason = _check_ctx(page, "page", True)
    if ok:
        if _detail_log_enabled():
            try:
                log_event("DBG", "skip not-reservable", reason=reason, url=(page.url or ""))
            except Exception:
                pass
        return True, reason

    # iframe 内エラー（frame url が /error/ や EFRESV など、または div.error-msg が出るケース）を拾う
    try:
        frames = list(page.frames)
    except Exception:
        frames = []

    for fr in frames:
        try:
            fr_url = (fr.url or "")
            fr_name = (fr.name or "")
        except Exception:
            fr_url = ""
            fr_name = ""
        # 無関係な frame をむやみに叩かない（速度と安定性）
        if fr_url and ("yoyaku.cityheaven.net" in fr_url or "/A6ShopReservation" in fr_url or "/calendar/" in fr_url or "/error/" in fr_url or "EFRESV" in fr_url):
            ok, reason = _check_ctx(fr, f"frame[{fr_name or 'noname'}]", False)
        elif fr_name and ("pcreserveiframe" in fr_name):
            ok, reason = _check_ctx(fr, f"frame[{fr_name}]", False)
        else:
            continue

        if ok:
            if _detail_log_enabled():
                try:
                    log_event("DBG", "skip not-reservable(in-frame)", reason=reason, url=(page.url or ""), frame_url=fr_url, frame_name=fr_name)
                except Exception:
                    pass
            return True, reason

    return False, ""

async def _is_not_reservable_page_async(page) -> tuple[bool, str]:
    """
    非同期版: 例外ページ（「該当の女の子は予約できません」等）を検出して、カレンダー待ちタイムアウトを回避する。
    エラーメッセージが iframe 内に出るケースがあるため、frames も走査する。
    """
    async def _check_ctx(ctx, label: str, require_reserve_ui: bool) -> tuple[bool, str]:
        try:
            cur_url = (ctx.url or "")
        except Exception:
            cur_url = ""
        if cur_url and ("yoyaku.cityheaven.net/error" in cur_url or "EFRESV" in cur_url):
            return True, f"{label}:url_error"

        try:
            err = await ctx.query_selector("div.error-msg")
        except Exception:
            err = None

        msg = ""
        if err:
            try:
                msg = await ctx.locator("div.error-msg").first.inner_text(timeout=500)
            except Exception:
                try:
                    msg = (await err.text_content()) or ""
                except Exception:
                    msg = ""
            msg = (msg or "").strip()

        body_txt = ""
        if not msg:
            try:
                body_txt = await ctx.locator("body").inner_text(timeout=300)
            except Exception:
                body_txt = ""
            body_txt = (body_txt or "").strip()

        hit_txt = msg or body_txt
        if not hit_txt:
            return False, ""

        if "予約できません" not in hit_txt and "予約できない" not in hit_txt and "予約不可" not in hit_txt:
            return False, ""

        if require_reserve_ui:
            try:
                has_reserve_ui = bool(await page.query_selector("#shop-reservation"))
            except Exception:
                has_reserve_ui = False
            if not has_reserve_ui:
                try:
                    has_reserve_ui = bool(await page.query_selector("iframe[name='pcreserveiframe'], iframe#pcreserveiframe"))
                except Exception:
                    has_reserve_ui = False
            if not has_reserve_ui:
                return False, ""

        helpers = []
        for kw in ("別の女の子", "お手数ですが", "該当の女の子"):
            if kw in hit_txt:
                helpers.append(kw)
        reason = f"{label}:予約できません"
        if helpers:
            reason = f"{reason}/" + ",".join(helpers)
        return True, reason

    ok, reason = await _check_ctx(page, "page", True)
    if ok:
        if _detail_log_enabled():
            try:
                log_event("DBG", "skip not-reservable", reason=reason, url=(page.url or ""))
            except Exception:
                pass
        return True, reason

    try:
        frames = list(page.frames)
    except Exception:
        frames = []

    for fr in frames:
        try:
            fr_url = (fr.url or "")
            fr_name = (fr.name or "")
        except Exception:
            fr_url = ""
            fr_name = ""
        if fr_url and ("yoyaku.cityheaven.net" in fr_url or "/A6ShopReservation" in fr_url or "/calendar/" in fr_url or "/error/" in fr_url or "EFRESV" in fr_url):
            ok, reason = await _check_ctx(fr, f"frame[{fr_name or 'noname'}]", False)
        elif fr_name and ("pcreserveiframe" in fr_name):
            ok, reason = await _check_ctx(fr, f"frame[{fr_name}]", False)
        else:
            continue

        if ok:
            if _detail_log_enabled():
                try:
                    log_event("DBG", "skip not-reservable(in-frame)", reason=reason, url=(page.url or ""), frame_url=fr_url, frame_name=fr_name)
                except Exception:
                    pass
            return True, reason

    return False, ""

def _probe_calendar_table_sync(target) -> bool:
    try:
        return bool(target.evaluate(r"""
(() => {
  const timeRe = /^\s*\d{1,2}:\d{2}/;
  const tables = Array.from(document.querySelectorAll("table"));
  if (!tables.length) return false;
  for (const t of tables) {
    const rows = Array.from(t.querySelectorAll("tr"));
    if (rows.some(r => timeRe.test((r.querySelector("td,th")?.innerText || "").trim()))) return true;
  }
  return false;
})()
"""))
    except Exception:
        return False

def _reservation_page_probe_sync(page):
    iframe = False
    link = False
    text_hit = False
    iframe_hint = False
    error_page = False
    has_calendar_table = False
    login_like = False
    iframe_src = ""
    iframe_h = 0.0
    iframe_frame_url = ""
    try:
        cur_url = page.url or ""
        if "yoyaku.cityheaven.net/error" in cur_url:
            error_page = True
    except Exception:
        cur_url = ""
    try:
        iframe = bool(page.query_selector("iframe[name='pcreserveiframe'], iframe#pcreserveiframe, iframe[src*='yoyaku.cityheaven.net'], iframe[src*='A6ShopReservation']"))
    except Exception:
        iframe = False
    login_like, iframe_src, iframe_h, iframe_frame_url = _probe_login_like_iframe_sync(page)
    try:
        link = bool(page.query_selector("a[href*='A6ShopReservation'], a[href*='Reservation'], a[href*='reserve'], a[href*='calendar'], a[href*='yoyaku'], a:has-text('ネット予約'), a:has-text('WEB予約'), a:has-text('予約')"))
    except Exception:
        link = False
    if not iframe and not link:
        try:
            html = page.content() or ""
            iframe_hint = "pcreserveiframe" in html
        except Exception:
            iframe_hint = False
    try:
        body_txt = page.locator("body").inner_text(timeout=500)
    except Exception:
        body_txt = ""
    if body_txt:
        for kw in _RESERVATION_BLOCK_TEXTS:
            if kw in body_txt:
                text_hit = True
                break
    if iframe:
        try:
            fr = page.frame(name="pcreserveiframe")
        except Exception:
            fr = None
        if not fr:
            try:
                ih = page.query_selector("iframe[name='pcreserveiframe']") or page.query_selector("iframe#pcreserveiframe")
                if ih:
                    fr = ih.content_frame()
            except Exception:
                fr = None
        if fr:
            has_calendar_table = _probe_calendar_table_sync(fr)
            try:
                iframe_frame_url = fr.url or iframe_frame_url
                if iframe_frame_url and ("yoyaku.cityheaven.net/error" in iframe_frame_url or "EFRESV" in iframe_frame_url):
                    error_page = True
            except Exception:
                pass
    if not has_calendar_table:
        has_calendar_table = _probe_calendar_table_sync(page)
    not_reservable = ((text_hit and not has_calendar_table) or error_page or login_like)
    return {
        "iframe": iframe,
        "link": link,
        "iframe_hint": iframe_hint,
        "text_hit": text_hit,
        "error_page": error_page,
        "not_reservable": not_reservable,
        "has_calendar_table": has_calendar_table,
        "login_like": login_like,
        "iframe_src": iframe_src,
        "iframe_h": iframe_h,
        "iframe_frame_url": iframe_frame_url,
    }

async def _reservation_page_probe_async(page):
    iframe = False
    link = False
    text_hit = False
    iframe_hint = False
    error_page = False
    has_calendar_table = False
    login_like = False
    iframe_src = ""
    iframe_h = 0.0
    iframe_frame_url = ""
    try:
        cur_url = page.url or ""
        if "yoyaku.cityheaven.net/error" in cur_url:
            error_page = True
    except Exception:
        cur_url = ""
    try:
        iframe = bool(await page.query_selector("iframe[name='pcreserveiframe'], iframe#pcreserveiframe, iframe[src*='yoyaku.cityheaven.net'], iframe[src*='A6ShopReservation']"))
    except Exception:
        iframe = False
    login_like, iframe_src, iframe_h, iframe_frame_url = await _probe_login_like_iframe_async(page)
    try:
        link = bool(await page.query_selector("a[href*='A6ShopReservation'], a[href*='Reservation'], a[href*='reserve'], a[href*='calendar'], a[href*='yoyaku'], a:has-text('ネット予約'), a:has-text('WEB予約'), a:has-text('予約')"))
    except Exception:
        link = False
    if not iframe and not link:
        try:
            html = await page.content()
            iframe_hint = "pcreserveiframe" in (html or "")
        except Exception:
            iframe_hint = False
    try:
        body_txt = await page.locator("body").inner_text(timeout=500)
    except Exception:
        body_txt = ""
    if body_txt:
        for kw in _RESERVATION_BLOCK_TEXTS:
            if kw in body_txt:
                text_hit = True
                break
    if iframe:
        try:
            fr = page.frame(name="pcreserveiframe")
        except Exception:
            fr = None
        if not fr:
            try:
                ih = await page.query_selector("iframe[name='pcreserveiframe']") or await page.query_selector("iframe#pcreserveiframe")
                if ih:
                    fr = await ih.content_frame()
            except Exception:
                fr = None
        if fr:
            try:
                has_calendar_table = bool(await fr.evaluate(r"""
(() => {
  const timeRe = /^\s*\d{1,2}:\d{2}/;
  const tables = Array.from(document.querySelectorAll("table"));
  if (!tables.length) return false;
  for (const t of tables) {
    const rows = Array.from(t.querySelectorAll("tr"));
    if (rows.some(r => timeRe.test((r.querySelector("td,th")?.innerText || "").trim()))) return true;
  }
  return false;
})()
"""))
            except Exception:
                has_calendar_table = False
            try:
                iframe_frame_url = fr.url or iframe_frame_url
                if iframe_frame_url and ("yoyaku.cityheaven.net/error" in iframe_frame_url or "EFRESV" in iframe_frame_url):
                    error_page = True
            except Exception:
                pass
    if not has_calendar_table:
        try:
            has_calendar_table = bool(await page.evaluate(r"""
(() => {
  const timeRe = /^\s*\d{1,2}:\d{2}/;
  const tables = Array.from(document.querySelectorAll("table"));
  if (!tables.length) return false;
  for (const t of tables) {
    const rows = Array.from(t.querySelectorAll("tr"));
    if (rows.some(r => timeRe.test((r.querySelector("td,th")?.innerText || "").trim()))) return true;
  }
  return false;
})()
"""))
        except Exception:
            has_calendar_table = False
    not_reservable = ((text_hit and not has_calendar_table) or error_page or login_like)
    return {
        "iframe": iframe,
        "link": link,
        "iframe_hint": iframe_hint,
        "text_hit": text_hit,
        "error_page": error_page,
        "not_reservable": not_reservable,
        "has_calendar_table": has_calendar_table,
        "login_like": login_like,
        "iframe_src": iframe_src,
        "iframe_h": iframe_h,
        "iframe_frame_url": iframe_frame_url,
    }

def _calendar_signature(stats: dict):
    return (
        stats.get("total_slots"),
        stats.get("bookable_slots"),
        stats.get("bell"),
        stats.get("tel"),
        stats.get("dash"),
        stats.get("other"),
        stats.get("excluded_slots"),
        stats.get("time_rows"),
        stats.get("max_cols"),
        stats.get("td_count"),
    )

def _calendar_sanity(stats: dict):
    if not isinstance(stats, dict) or not stats.get("ok"):
        return False, "stats_ng"
    total_slots = int(stats.get("total_slots", 0) or 0)
    excluded_slots = int(stats.get("excluded_slots", 0) or 0)
    time_rows = int(stats.get("time_rows", 0) or 0)
    max_cols = int(stats.get("max_cols", 0) or 0)
    td_count = int(stats.get("td_count", 0) or 0)
    if time_rows < _CAL_MIN_TIME_ROWS or time_rows > _CAL_MAX_TIME_ROWS:
        return False, "time_rows_range"
    if max_cols < _CAL_MIN_COLS or max_cols > _CAL_MAX_COLS:
        return False, "max_cols_range"
    expected_total = time_rows * max_cols
    if expected_total <= 0 or (total_slots + excluded_slots) != expected_total:
        return False, "total_slots_mismatch"
    if td_count <= 0:
        return False, "td_count_missing"
    td_min = max(3, int(expected_total * _CAL_TD_MIN_RATIO))
    td_max = int(expected_total * _CAL_TD_MAX_RATIO)
    if td_count < td_min:
        return False, "td_count_too_small"
    if td_count > td_max:
        return False, "td_count_too_large"
    return True, "ok"

def _calendar_debug_dir():
    ensure_data_dirs()
    base = _CURRENT_RUN_DIR or LOG_DIR
    path = os.path.join(base, "calendar_debug")
    os.makedirs(path, exist_ok=True)
    return path

def _dump_calendar_debug_sync(page, fr, stats: dict, reason: str, frame_url: str):
    if not _detail_log_enabled():
        return None
    try:
        base_dir = _calendar_debug_dir()
        ts = _now_ts()
        safe_reason = _safe_name(reason or "debug")
        prefix = f"{ts}_{safe_reason}"
        paths = {}
        try:
            html = fr.content()
            p_html = os.path.join(base_dir, f"{prefix}_frame.html")
            with open(p_html, "w", encoding="utf-8") as w:
                w.write(html or "")
            paths["frame_html"] = p_html
        except Exception:
            pass
        try:
            el = fr.frame_element()
            p_png = os.path.join(base_dir, f"{prefix}_frame.png")
            el.screenshot(path=p_png)
            paths["frame_png"] = p_png
        except Exception:
            pass
        try:
            cells = fr.evaluate(r"""
(() => {
  const timeRe = /^\s*\d{1,2}:\d{2}/;
  const tables = Array.from(document.querySelectorAll("table"));
  if (!tables.length) return [];
  let table = null;
  for (const t of tables) {
    const rows = Array.from(t.querySelectorAll("tr"));
    if (rows.some(r => timeRe.test((r.querySelector("td,th")?.innerText || "").trim()))) { table = t; break; }
  }
  table = table || tables[0];
  const rows = Array.from(table.querySelectorAll("tr"));
  const dataRows = rows.filter(r => {
    const first = r.querySelector("td,th");
    if (!first) return false;
    return timeRe.test(first.innerText.trim());
  });
  function classifyCell(td) {
    if (!td) return "other";
    const dn = (td.getAttribute("data-name") || "").trim().toUpperCase();
    const rawTxt = (td.innerText || "").trim();
    const msg = (td.getAttribute("data-name_message") || "").trim();
    const txt = rawTxt.toUpperCase();
    const aria = (td.getAttribute("aria-label") || "").trim().toUpperCase();
    const title = (td.getAttribute("title") || "").trim().toUpperCase();
    const cls = (td.className || "").toString().toUpperCase();
    const combined = `${rawTxt} ${msg}`;
    if (combined.includes("お電話にてお問い合わせください")) return "excluded_notice_big";
    if (dn === "TEL" || txt === "TEL" || aria === "TEL" || title === "TEL") return "tel";
    if (txt === "×" || aria.includes("×") || title.includes("×")) return "bell";
    if (rawTxt.includes("〇先行") || rawTxt.includes("○先行")) return "maru";
    if (td.querySelector("span[data-mark='○']") || txt === "○") return "maru";
    if (cls.includes("BELL") || cls.includes("CROSS")) return "bell";
    const bg = (getComputedStyle(td).backgroundImage || "").toLowerCase();
    if (bg.includes("bell")) return "bell";
    const img = td.querySelector("img");
    if (img) {
      const alt = (img.getAttribute("alt") || "").toLowerCase();
      const src = (img.getAttribute("src") || "").toLowerCase();
      if (alt.includes("bell") || src.includes("bell")) return "bell";
    }
    const raw = (td.innerText || "").trim();
    if (raw === "―" || raw === "‐" || raw === "-" || raw === "–" || raw === "—" || raw === "ー") return "dash";
    return "other";
  }
  const out = [];
  dataRows.forEach((r, rowIdx) => {
    const cells = Array.from(r.children).filter(el => el.tagName === "TD" || el.tagName === "TH");
    const tds = cells.slice(1);
    tds.forEach((td, colIdx) => {
      const img = td.querySelector("img");
      const style = getComputedStyle(td);
      out.push({
        row: rowIdx,
        col: colIdx,
        type: classifyCell(td),
        text: (td.innerText || "").trim(),
        class: td.className || "",
        aria_label: td.getAttribute("aria-label") || "",
        title: td.getAttribute("title") || "",
        href: td.querySelector("a")?.getAttribute("href") || "",
        data_name: td.getAttribute("data-name") || "",
        data_mark: td.getAttribute("data-mark") || "",
        rowspan: td.getAttribute("rowspan") || "",
        colspan: td.getAttribute("colspan") || "",
        img_alt: img ? (img.getAttribute("alt") || "") : "",
        img_src: img ? (img.getAttribute("src") || "") : "",
        bg_image: style ? (style.backgroundImage || "") : "",
      });
    });
  });
  return out;
})()
""")
            p_json = os.path.join(base_dir, f"{prefix}_cells.json")
            with open(p_json, "w", encoding="utf-8") as w:
                json.dump(cells or [], w, ensure_ascii=False, indent=2)
            paths["cells_json"] = p_json
        except Exception:
            pass
        log_event(
            "DBG",
            "calendar debug dump",
            frame_url=frame_url,
            reason=reason,
            signature=_calendar_signature(stats) if isinstance(stats, dict) else None,
            files=paths,
        )
        return paths
    except Exception:
        return None

async def _dump_calendar_debug_async(page, fr, stats: dict, reason: str, frame_url: str):
    if not _detail_log_enabled():
        return None
    try:
        base_dir = _calendar_debug_dir()
        ts = _now_ts()
        safe_reason = _safe_name(reason or "debug")
        prefix = f"{ts}_{safe_reason}"
        paths = {}
        try:
            html = await fr.content()
            p_html = os.path.join(base_dir, f"{prefix}_frame.html")
            with open(p_html, "w", encoding="utf-8") as w:
                w.write(html or "")
            paths["frame_html"] = p_html
        except Exception:
            pass
        try:
            el = await fr.frame_element()
            p_png = os.path.join(base_dir, f"{prefix}_frame.png")
            await el.screenshot(path=p_png)
            paths["frame_png"] = p_png
        except Exception:
            pass
        try:
            cells = await fr.evaluate(r"""
(() => {
  const timeRe = /^\s*\d{1,2}:\d{2}/;
  const tables = Array.from(document.querySelectorAll("table"));
  if (!tables.length) return [];
  let table = null;
  for (const t of tables) {
    const rows = Array.from(t.querySelectorAll("tr"));
    if (rows.some(r => timeRe.test((r.querySelector("td,th")?.innerText || "").trim()))) { table = t; break; }
  }
  table = table || tables[0];
  const rows = Array.from(table.querySelectorAll("tr"));
  const dataRows = rows.filter(r => {
    const first = r.querySelector("td,th");
    if (!first) return false;
    return timeRe.test(first.innerText.trim());
  });
  function classifyCell(td) {
    if (!td) return "other";
    const dn = (td.getAttribute("data-name") || "").trim().toUpperCase();
    const rawTxt = (td.innerText || "").trim();
    const msg = (td.getAttribute("data-name_message") || "").trim();
    const txt = rawTxt.toUpperCase();
    const aria = (td.getAttribute("aria-label") || "").trim().toUpperCase();
    const title = (td.getAttribute("title") || "").trim().toUpperCase();
    const cls = (td.className || "").toString().toUpperCase();
    const combined = `${rawTxt} ${msg}`;
    if (combined.includes("お電話にてお問い合わせください")) return "excluded_notice_big";
    if (dn === "TEL" || txt === "TEL" || aria === "TEL" || title === "TEL") return "tel";
    if (txt === "×" || aria.includes("×") || title.includes("×")) return "bell";
    if (rawTxt.includes("〇先行") || rawTxt.includes("○先行")) return "maru";
    if (td.querySelector("span[data-mark='○']") || txt === "○") return "maru";
    if (cls.includes("BELL") || cls.includes("CROSS")) return "bell";
    const bg = (getComputedStyle(td).backgroundImage || "").toLowerCase();
    if (bg.includes("bell")) return "bell";
    const img = td.querySelector("img");
    if (img) {
      const alt = (img.getAttribute("alt") || "").toLowerCase();
      const src = (img.getAttribute("src") || "").toLowerCase();
      if (alt.includes("bell") || src.includes("bell")) return "bell";
    }
    const raw = (td.innerText || "").trim();
    if (raw === "―" || raw === "‐" || raw === "-" || raw === "–" || raw === "—" || raw === "ー") return "dash";
    return "other";
  }
  const out = [];
  dataRows.forEach((r, rowIdx) => {
    const cells = Array.from(r.children).filter(el => el.tagName === "TD" || el.tagName === "TH");
    const tds = cells.slice(1);
    tds.forEach((td, colIdx) => {
      const img = td.querySelector("img");
      const style = getComputedStyle(td);
      out.push({
        row: rowIdx,
        col: colIdx,
        type: classifyCell(td),
        text: (td.innerText || "").trim(),
        class: td.className || "",
        aria_label: td.getAttribute("aria-label") || "",
        title: td.getAttribute("title") || "",
        href: td.querySelector("a")?.getAttribute("href") || "",
        data_name: td.getAttribute("data-name") || "",
        data_mark: td.getAttribute("data-mark") || "",
        rowspan: td.getAttribute("rowspan") || "",
        colspan: td.getAttribute("colspan") || "",
        img_alt: img ? (img.getAttribute("alt") || "") : "",
        img_src: img ? (img.getAttribute("src") || "") : "",
        bg_image: style ? (style.backgroundImage || "") : "",
      });
    });
  });
  return out;
})()
""")
            p_json = os.path.join(base_dir, f"{prefix}_cells.json")
            with open(p_json, "w", encoding="utf-8") as w:
                json.dump(cells or [], w, ensure_ascii=False, indent=2)
            paths["cells_json"] = p_json
        except Exception:
            pass
        log_event(
            "DBG",
            "calendar debug dump",
            frame_url=frame_url,
            reason=reason,
            signature=_calendar_signature(stats) if isinstance(stats, dict) else None,
            files=paths,
        )
        return paths
    except Exception:
        return None
def _now_iso_ms():
    # 例: 2025-12-31T06:12:34.123+09:00
    try:
        return datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=9))).isoformat(timespec="milliseconds")
    except Exception:
        return datetime.datetime.now().isoformat(timespec="milliseconds")

def set_current_run_dir(run_dir):
    global _CURRENT_RUN_DIR
    _CURRENT_RUN_DIR = run_dir

def log_event(level: str, msg: str, **kv):
    ts = _now_iso_ms()
    # PowerShell/コンソールにも出す（最重要）
    try:
        extra = (" " + json.dumps(kv, ensure_ascii=False)) if kv else ""
        line = f"{ts} [{level}] {msg}{extra}"
        print(line, flush=True)
    except Exception:
        pass
    # ファイルにも残す（フォルダ内完結）
    try:
        ensure_data_dirs()
        os.makedirs(LOG_DIR, exist_ok=True)
        p_txt = os.path.join(LOG_DIR, "app.log")
        p_json = os.path.join(LOG_DIR, "app.jsonl")
        rec = {"ts": ts, "level": level, "msg": msg}
        if kv:
            rec.update(kv)
        with open(p_txt, "a", encoding="utf-8") as w:
            w.write(f"{ts} [{level}] {msg}\n")
        with open(p_json, "a", encoding="utf-8") as w:
            w.write(json.dumps(rec, ensure_ascii=False) + "\n")
        if _CURRENT_RUN_DIR:
            p_run = os.path.join(_CURRENT_RUN_DIR, "run_log.jsonl")
            with open(p_run, "a", encoding="utf-8") as w:
                w.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception:
        pass

# --- 音（Windows標準） ---
def beep_ok():
    try:
        import winsound
        winsound.MessageBeep(winsound.MB_OK)
    except Exception:
        pass

def beep_err():
    try:
        import winsound
        winsound.MessageBeep(winsound.MB_ICONHAND)
    except Exception:
        pass

# -------------------------
# スクレイピング部（中核）
# -------------------------
def store_base_from_list_url(list_url: str) -> str:
    u = urlparse(list_url)
    m = re.search(r"^(.+?)/girllist(?:/|$)", u.path)
    if not m:
        raise ValueError("LIST_URL から store_base を作れませんでした。/girllist/ を含むURLにしてください。")
    store_path = m.group(1)
    return f"{u.scheme}://{u.netloc}{store_path}"

def abs_url_from_href(page_url: str, href: str) -> str:
    return urljoin(page_url, href)

def goto_retry(page, url: str, wait_until="domcontentloaded", tries=2, preset: str = None, gid: str = None):
    if not _valid_url(url):
        if _detail_log_enabled():
            log_event(
                "INFO",
                "skip invalid url",
                preset=preset,
                gid=gid,
                url=(url or ""),
            )
            _detail_log_skip(preset, gid, "invalid_url")
        return False
    last_err = None
    for _ in range(tries):
        try:
            page.goto(url, wait_until=wait_until, timeout=NAV_TIMEOUT_MS)
            return True
        except PWTimeoutError as e:
            last_err = e
            try:
                page.wait_for_timeout(700)
                page.reload(wait_until=wait_until, timeout=NAV_TIMEOUT_MS)
                return True
            except Exception:
                pass
    return False

def get_next_list_url(page):
    loc = page.locator("a.next")
    if loc.count() == 0:
        return None
    href = loc.first.get_attribute("href")
    if not href:
        return None
    return abs_url_from_href(page.url, href)

# ---- 一覧DOM差を吸収して ID+名前を集める（強化版）----
def collect_girls_from_list(page, need: int):
    """
    店によってDOMが違うので複数候補をまとめて拾う。
    - 1) td.second a.profileLink（あなたが貼った構造）
    - 2) a[href*='girlid-']（一般）
    strong優先 → text fallback
    """
    js = r"""
(() => {
  const out = [];
  const seen = new Set();
  const cleanName = (s) => {
    if (!s) return "";
    s = s.replace(/（\s*\d+\s*歳\s*）/g, "");
    s = s.replace(/\s+/g, " ").trim();
    // 長すぎるのは切る（GUIの見やすさ）
    if (s.length > 30) s = s.slice(0, 30).trim();
    return s;
  };

  function pushFromAnchor(a){
    if (!a) return;
    const href = a.getAttribute("href") || a.href || "";
    const m = href.match(/girlid-(\d+)/);
    if (!m) return;
    const gid = m[1];
    if (seen.has(gid)) return;

    let name = "";
    const strongs = a.querySelectorAll("strong");
    if (strongs && strongs.length) name = (strongs[0].innerText || "").trim();
    if (!name) name = cleanName(a.innerText || "");
    if (!name) {
      const p = a.closest("td,div,li");
      if (p) name = cleanName(p.innerText || "");
    }
    if (!name) name = "（名前不明）";

    seen.add(gid);
    out.push([gid, name]);
  }

  // (1) まず「td.second a.profileLink」を優先
  document.querySelectorAll("td.second a.profileLink").forEach(a => pushFromAnchor(a));

  // (2) それでも足りなければ一般の girlid リンク
  if (out.length < 3) {
    document.querySelectorAll("a[href*='girlid-']").forEach(a => pushFromAnchor(a));
  }

  return out;
})()
"""
    try:
        items = page.evaluate(js) or []
        return items[:need]
    except Exception:
        return []

# ---- 予約ページ側から名前を軽く拾う（保険）----
def try_get_name_from_res_page(page):
    candidates = [
        "strong",
        ".castName", ".cast_name",
        "h1", "h2",
        "a.profileLink strong",
        "a.profileLink",
    ]
    for sel in candidates:
        try:
            loc = page.locator(sel).first
            if loc.count() > 0:
                txt = (loc.text_content() or "").strip()
                txt = re.sub(r"\s+", " ", txt)
                if 2 <= len(txt) <= 40:
                    txt = re.sub(r"（\s*\d+\s*歳\s*）", "", txt).strip()
                    if txt:
                        return txt[:30]
        except Exception:
            pass
    return ""

# ---- DETAILページから名前補完（別ページで安全に）----
def try_get_name_from_detail_page(name_page, detail_url):
    if not goto_retry(name_page, detail_url, tries=2):
        return ""
    try:
        name_page.wait_for_load_state("networkidle", timeout=12000)
    except Exception:
        pass
    name_page.wait_for_timeout(300)

    selectors = [
        "h1", "h2",
        "strong",
        ".profile .name", ".castName", ".cast_name",
        "a.profileLink strong",
    ]
    for sel in selectors:
        try:
            loc = name_page.locator(sel).first
            if loc.count():
                txt = (loc.text_content() or "").strip()
                txt = re.sub(r"\s+", " ", txt)
                txt = re.sub(r"（\s*\d+\s*歳\s*）", "", txt).strip()
                if 2 <= len(txt) <= 40:
                    return txt[:30]
        except Exception:
            pass
    return ""

def _count_calendar_stats_by_slots_core(page, stop_evt: threading.Event, progress_cb=None, max_wait_ms: int = None, preset: str = None, gid: str = None):
    """
    iframe内のテーブルを「枠（スロット）」として再構築し、
    rowspan を展開して bell / ○ / TEL / ― / × を“枠数”で集計する。
    ※ × は bell 扱いでOK
    """
    waited = 0
    if max_wait_ms is None:
        max_wait_ms = min(CAL_WAIT_MS, CAL_WAIT_SHORT_MS)
    fast_wait_ms = 500
    slow_step_ms = CAL_WAIT_STEP_MS
    stable_attempts = 0
    stable_hits = 0
    last_light_sig = None
    last_stats = None
    last_frame = None
    last_frame_url = ""
    last_invalid_stats = None
    last_invalid_reason = None
    attempt = 0
    sanity_retry_count = 0
    sanity_last_reason = None
    frame_names = []
    frame_urls = []
    suspicious_dumped = False
    while waited <= max_wait_ms:
        if stop_evt.is_set():
            return None, None

        fr = None
        frame_url = ""
        # 1) 予約カレンダーiframe（name/idが固定のことが多い）
        try:
            fr = page.frame(name="pcreserveiframe")
        except Exception:
            fr = None

        # 1.5) name/idが違う or 動的に差し込まれるケース：
        #      topだけでなく「全フレーム内」のiframeも走査して、src/name/id から予約フレーム候補を拾う
        if not fr:
            try:
                sel = "iframe[name='pcreserveiframe'], iframe#pcreserveiframe, iframe[src*='yoyaku.cityheaven.net'], iframe[src*='A6ShopReservation'], iframe[src*='ShopReservation']"
                for host_fr in page.frames:
                    try:
                        iframes = host_fr.query_selector_all(sel) or []
                    except Exception:
                        continue
                    for ih in iframes:
                        try:
                            src = (ih.get_attribute("src") or "")
                            nm = (ih.get_attribute("name") or "")
                            iid = (ih.get_attribute("id") or "")
                            # iframeが error に飛ばされているケース（枠なし）を早期検出
                            if ("/error/" in src) or ("EFRESV" in src) or ("yoyaku.cityheaven.net/error" in src):
                                return {"ok": False, "reason": "not_reservable(iframe_src_error)", "iframe_src": src}, src
                            hay = f"{src} {nm} {iid}"
                        except Exception:
                            continue
                        if ("yoyaku.cityheaven.net" in hay) or ("A6ShopReservation" in hay) or ("ShopReservation" in hay) or ("calendar" in src and "cityheaven" in src):
                            try:
                                fr = ih.content_frame()
                            except Exception:
                                fr = None
                            if fr:
                                break
                    if fr:
                        break
            except Exception:
                fr = None


        # 2) URLで拾う（calendar / error どちらも拾う）
        if not fr:
            try:
                frames = [f for f in page.frames if "yoyaku.cityheaven.net/" in (f.url or "")]
                if frames:
                    def _rank(u: str):
                        u = u or ""
                        if "yoyaku.cityheaven.net/calendar" in u:
                            return 0
                        if "yoyaku.cityheaven.net/error" in u:
                            return 1
                        return 2
                    frames.sort(key=lambda f: _rank(f.url or ""))
                    fr = frames[0]
            except Exception:
                fr = None
                if _detail_log_enabled():
                    log_event("WARN", "calendar frame scan failed", reason="frames_list", error=str(e))
        # 3) 最後の保険：calendar系フレームを拾う
        if not fr:
            try:
                frames = [f for f in page.frames if ("/calendar/" in (f.url or "")) or ("calendar" in (f.url or ""))]
                if frames:
                    fr = frames[0]
            except Exception as e:
                if _detail_log_enabled():
                    log_event("WARN", "calendar frame scan failed", reason="frames_calendar", error=str(e))

        # 3.5) iframe枠だけ存在して src が空のまま（=結局ロードされない）場合は、無駄な待ちを避けて早期終了
        if not fr:
            try:
                ih0 = page.query_selector("iframe[name='pcreserveiframe'], iframe#pcreserveiframe")
            except Exception:
                ih0 = None
            if ih0:
                try:
                    src0 = (ih0.get_attribute("src") or "").strip()
                except Exception:
                    src0 = ""
                # error src は即スキップ
                if ("/error/" in src0) or ("EFRESV" in src0) or ("yoyaku.cityheaven.net/error" in src0):
                    return {"ok": False, "reason": "not_reservable(iframe_src_error)", "iframe_src": src0}, src0
                # src が空で、yoyakuフレームも存在しない状態が続くなら「カレンダーなし」と扱う
                if (not src0):
                    try:
                        has_yoyaku = any("yoyaku.cityheaven.net/" in (f.url or "") for f in page.frames)
                    except Exception:
                        has_yoyaku = False
                    if (not has_yoyaku) and (waited >= min(4000, max_wait_ms)):
                        return {"ok": False, "reason": "calendar_not_present(iframe_no_src)"}, None


        if fr:
            frame_url = fr.url or ""
            # 例外（予約不可）ページは iframe 内で表示されることがある（例: /error/ や EFRESV、div.error-msg）
            try:
                if frame_url and ("yoyaku.cityheaven.net/error" in frame_url or "/error/" in frame_url or "EFRESV" in frame_url):
                    return {"ok": False, "reason": "not_reservable(frame_url_error)", "frame_url": frame_url}, frame_url
            except Exception:
                pass
            try:
                err_in_fr = fr.query_selector("div.error-msg")
            except Exception:
                err_in_fr = None
            if err_in_fr:
                try:
                    msg = fr.locator("div.error-msg").first.inner_text(timeout=300)
                except Exception:
                    try:
                        msg = err_in_fr.text_content() or ""
                    except Exception:
                        msg = ""
                msg = (msg or "").strip()
                if "予約できません" in msg:
                    return {"ok": False, "reason": "not_reservable(error_msg_in_frame)", "frame_url": frame_url, "msg": msg[:160]}, frame_url

            try:
                fr.wait_for_load_state("domcontentloaded", timeout=2500)
            except Exception:
                pass
            try:
                attempt += 1
                try:
                    fr.wait_for_selector("table, td", timeout=fast_wait_ms)
                except Exception:
                    if _detail_log_enabled():
                        log_event(
                            "DBG",
                            "calendar retry",
                            frame_url=frame_url,
                            td_count=0,
                            time_rows=0,
                            signature=None,
                            attempt=attempt,
                            reason="td_wait_timeout",
                        )
                    skip, why = _is_not_reservable_page_sync(page)
                    if skip:
                        return {"ok": False, "reason": f"not_reservable({why})"}, None
                    waited += slow_step_ms
                    if progress_cb:
                        progress_cb(waited, max_wait_ms)
                    page.wait_for_timeout(slow_step_ms)
                    continue
                stats = fr.evaluate(r"""
(() => {
  const timeRe = /^\s*\d{1,2}:\d{2}/;
  const tables = Array.from(document.querySelectorAll("table"));
  if (!tables.length) return { ok:false, reason:"no table" };

  let table = null;
  for (const t of tables) {
    const rows = Array.from(t.querySelectorAll("tr"));
    if (rows.some(r => timeRe.test((r.querySelector("td,th")?.innerText || "").trim()))) { table = t; break; }
  }
  table = table || tables[0];

  const rows = Array.from(table.querySelectorAll("tr"));
  const dataRows = rows.filter(r => {
    const first = r.querySelector("td,th");
    if (!first) return false;
    return timeRe.test(first.innerText.trim());
  });
  if (!dataRows.length) return { ok:false, reason:"no time rows" };

  let maxCols = 0;
  for (const r of dataRows) {
    const cells = Array.from(r.children).filter(el => el.tagName === "TD" || el.tagName === "TH").slice(1);
    let cols = 0;
    for (const td of cells) {
      const cs = parseInt(td.getAttribute("colspan") || "1", 10) || 1;
      cols += cs;
    }
    if (cols > maxCols) maxCols = cols;
  }
  if (maxCols <= 0) return { ok:false, reason:"no cols" };

  const firstDataRowIndex = rows.findIndex(r => dataRows.includes(r));
  const headerRows = firstDataRowIndex > 0 ? rows.slice(0, firstDataRowIndex) : [];
  const now = new Date();
  function pad2(n) { return String(n).padStart(2, "0"); }
  function findBaseYearMonth() {
    let year = now.getFullYear();
    let month = now.getMonth() + 1;
    const bodyText = (document.body && document.body.innerText) ? document.body.innerText : "";
    let m = bodyText.match(/(\d{4})\s*[年\/\.]\s*(\d{1,2})\s*月/);
    if (m) {
      year = parseInt(m[1], 10) || year;
      month = parseInt(m[2], 10) || month;
      return { year, month };
    }
    m = bodyText.match(/(\d{1,2})\s*月/);
    if (m) {
      month = parseInt(m[1], 10) || month;
    }
    return { year, month };
  }
  const baseYM = findBaseYearMonth();
  function parseDateText(txt) {
    if (!txt) return null;
    const clean = String(txt).replace(/\s+/g, "");
    let month = null;
    let day = null;
    let m = clean.match(/(\d{1,2})[\/\.](\d{1,2})/);
    if (m) {
      month = parseInt(m[1], 10);
      day = parseInt(m[2], 10);
    } else {
      m = clean.match(/(\d{1,2})月(\d{1,2})/);
      if (m) {
        month = parseInt(m[1], 10);
        day = parseInt(m[2], 10);
      } else {
        m = clean.match(/(\d{1,2})\([月火水木金土日]\)/);
        if (m) {
          day = parseInt(m[1], 10);
        } else {
          m = clean.match(/(\d{1,2})日/);
          if (m) day = parseInt(m[1], 10);
        }
      }
    }
    if (!day || day < 1 || day > 31) return null;
    if (!month || month < 1 || month > 12) month = baseYM.month;
    if (!month) return null;
    let year = baseYM.year;
    if (baseYM.month === 12 && month === 1) year += 1;
    if (baseYM.month === 1 && month === 12) year -= 1;
    return `${year}-${pad2(month)}-${pad2(day)}`;
  }
  let headerRow = null;
  let headerMatches = 0;
  for (const r of headerRows) {
    const cells = Array.from(r.children).filter(el => el.tagName === "TD" || el.tagName === "TH").slice(1);
    let hits = 0;
    for (const td of cells) {
      if (parseDateText(td.innerText || "")) hits += 1;
    }
    if (hits > headerMatches) {
      headerMatches = hits;
      headerRow = r;
    }
  }
  const columnDateKeys = Array(maxCols).fill(null);
  if (headerRow && headerMatches > 0) {
    const cells = Array.from(headerRow.children).filter(el => el.tagName === "TD" || el.tagName === "TH").slice(1);
    let col = 0;
    for (const td of cells) {
      const cs = parseInt(td.getAttribute("colspan") || "1", 10) || 1;
      const dateKey = parseDateText(td.innerText || "");
      for (let cc = col; cc < Math.min(maxCols, col + cs); cc++) {
        if (dateKey) columnDateKeys[cc] = dateKey;
      }
      col += cs;
      if (col >= maxCols) break;
    }
  }
  const headerDateKeySet = new Set();
  for (const key of columnDateKeys) {
    if (key) headerDateKeySet.add(key);
  }
  const headerDateCount = headerDateKeySet.size;

  const rowCount = dataRows.length;
  const grid = Array.from({ length: rowCount }, () => Array(maxCols).fill(null));
  const out = { ok:true, bell:0, maru:0, tel:0, dash:0, other:0, total_slots:0, bookable_slots:0, excluded_slots:0, time_rows: rowCount, max_cols: maxCols, td_count:0, header_dates: headerDateCount, slots_unique: 0 };
  const statsByDate = {};
  function ensureDateKey(key) {
    if (!statsByDate[key]) statsByDate[key] = { bell:0, maru:0, tel:0, other:0 };
    return statsByDate[key];
  }

  function classifyCell(td) {
    if (!td) return "other";
    const dn = (td.getAttribute("data-name") || "").trim().toUpperCase();
    const dataMark = (td.getAttribute("data-mark") || "").trim().toUpperCase();
    const dataStatus = (td.getAttribute("data-status") || "").trim().toUpperCase();
    const rawTxt = (td.innerText || "").trim();
    const msg = (td.getAttribute("data-name_message") || "").trim();
    const txt = rawTxt;
    const txtUpper = txt.toUpperCase();
    const aria = (td.getAttribute("aria-label") || "").trim().toUpperCase();
    const title = (td.getAttribute("title") || "").trim().toUpperCase();
    const cls = (td.className || "").toString().toUpperCase();
    const combined = `${rawTxt} ${msg}`;
    if (combined.includes("お電話にてお問い合わせください")) return "excluded_notice_big";
    if (dn === "TEL" || dataStatus.includes("TEL") || txtUpper === "TEL" || aria.includes("TEL") || title.includes("TEL") || cls.includes("TEL") || cls.includes("PHONE")) return "tel";
    // × は bell 扱い
    if (txt === "×" || txt === "✕" || txt === "✖" || txt.includes("不可") || aria.includes("×") || title.includes("×") || cls.includes("NG")) return "bell";
    if (rawTxt.includes("〇先行") || rawTxt.includes("○先行")) return "maru";
    if (dataMark === "○" || dataMark === "MARU" || td.querySelector("span[data-mark='○']") || txt === "○" || txt === "〇" || cls.includes("MARU") || cls.includes("CIRCLE") || cls.includes("OK") || aria.includes("○")) return "maru";
    if (cls.includes("BELL") || cls.includes("CROSS") || dataStatus.includes("NG")) return "bell";
    const bg = (getComputedStyle(td).backgroundImage || "").toLowerCase();
    if (bg.includes("bell") || bg.includes("cross") || bg.includes("ng")) return "bell";
    const img = td.querySelector("img");
    if (img) {
      const alt = (img.getAttribute("alt") || "").toLowerCase();
      const titleImg = (img.getAttribute("title") || "").toLowerCase();
      const ariaImg = (img.getAttribute("aria-label") || "").toLowerCase();
      const src = (img.getAttribute("src") || "").toLowerCase();
      const imgCls = (img.className || "").toString().toLowerCase();
      const imgData = (img.getAttribute("data-name") || "").toLowerCase();
      if (alt.includes("bell") || titleImg.includes("bell") || ariaImg.includes("bell") || src.includes("bell") || imgCls.includes("bell") || imgData.includes("bell")) return "bell";
      if (alt.includes("cross") || titleImg.includes("cross") || ariaImg.includes("cross") || src.includes("cross") || imgCls.includes("cross") || imgData.includes("cross")) return "bell";
      if (alt.includes("tel") || titleImg.includes("tel") || ariaImg.includes("tel") || src.includes("tel") || imgCls.includes("tel") || imgData.includes("tel")) return "tel";
    }
    const raw = txt.trim();
    if (raw === "―" || raw === "‐" || raw === "-" || raw === "–" || raw === "—" || raw === "ー") return "dash";
    return "other";
  }

  function countSlot(r, c, t) {
    if (grid[r][c]) return;
    grid[r][c] = t;
    if (t === "excluded_notice_big") {
      out.excluded_slots += 1;
      const dateKey = columnDateKeys[c];
      if (dateKey) {
        const dst = ensureDateKey(dateKey);
        dst.other += 1;
      }
      return;
    }
    out[t] = (out[t] || 0) + 1;
    out.total_slots += 1;
    if (t === "bell" || t === "maru" || t === "tel") out.bookable_slots += 1;
    const dateKey = columnDateKeys[c];
    if (dateKey) {
      const dst = ensureDateKey(dateKey);
      if (t === "bell" || t === "maru" || t === "tel") {
        dst[t] += 1;
      } else {
        dst.other += 1;
      }
    }
  }

  for (let r = 0; r < rowCount; r++) {
    const cells = Array.from(dataRows[r].children).filter(el => el.tagName === "TD" || el.tagName === "TH").slice(1);
    let col = 0;
    for (const td of cells) {
      out.td_count += 1;
      while (col < maxCols && grid[r][col]) col++;
      if (col >= maxCols) break;
      const rs = parseInt(td.getAttribute("rowspan") || "1", 10) || 1;
      const cs = parseInt(td.getAttribute("colspan") || "1", 10) || 1;
      const t = classifyCell(td);
      const rLimit = Math.min(rowCount, r + rs);
      const cLimit = Math.min(maxCols, col + cs);
      for (let rr = r; rr < rLimit; rr++) {
        for (let cc = col; cc < cLimit; cc++) {
          countSlot(rr, cc, t);
        }
      }
      col += cs;
    }
  }

  let uniqueSlots = 0;
  for (let r = 0; r < rowCount; r++) {
    for (let c = 0; c < maxCols; c++) {
      if (grid[r][c] && grid[r][c] !== "excluded_notice_big") uniqueSlots += 1;
    }
  }
  out.slots_unique = uniqueSlots;
  out.bell_rate_total = out.total_slots ? (out.bell / out.total_slots) : null;
  out.bell_rate_bookable = out.bookable_slots ? (out.bell / out.bookable_slots) : null;
  out.stats_by_date = statsByDate;
  return out;
})()
""")
                suspicious_hit = False
                suspicious_meta = None
                page_html = ""
                frame_html = ""
                try:
                    probe = _light_probe_suspicious_sync(page, fr)
                    if probe.get("hit"):
                        page_html = (page.content() or "")
                        frame_html = (fr.content() or "")
                        detect = _detect_suspicious_markers(page_html, frame_html)
                        if detect.get("suspicious_hit"):
                            suspicious_hit = True
                            suspicious_meta = detect
                        else:
                            suspicious_hit = True
                            suspicious_meta = {
                                "suspicious_hit": True,
                                "markers_hit": [probe.get("label")] if probe.get("label") else [],
                                "vendors": [probe.get("vendor")] if probe.get("vendor") else [],
                                "strength": probe.get("strength"),
                                "excerpt": "",
                            }
                    else:
                        suspicious_hit = False
                        suspicious_meta = {
                            "suspicious_hit": False,
                            "markers_hit": [],
                            "vendors": [],
                            "strength": None,
                            "excerpt": "",
                        }
                except Exception:
                    pass
                if isinstance(stats, dict):
                    stats["suspicious_hit"] = suspicious_hit
                    markers_hit = (suspicious_meta or {}).get("markers_hit") or []
                    vendors = (suspicious_meta or {}).get("vendors") or []
                    strength = (suspicious_meta or {}).get("strength")
                    excerpt = (suspicious_meta or {}).get("excerpt") or ""
                    if suspicious_hit and not vendors:
                        vendor_value = "unknown"
                    elif len(vendors) == 1:
                        vendor_value = vendors[0]
                    elif vendors:
                        vendor_value = vendors
                    else:
                        vendor_value = None
                    stats["suspicious_markers_hit"] = markers_hit
                    stats["suspicious_vendor"] = vendor_value
                    stats["suspicious_excerpt"] = excerpt
                    stats["suspicious_strength"] = strength
                    should_dump = _should_dump_suspicious(stats, suspicious_meta or {}, frame_url=frame_url or getattr(fr, "url", ""))
                    if should_dump and not suspicious_dumped:
                        suspicious_dumped = True
                        _dump_suspicious_debug_sync(
                            page,
                            fr,
                            preset,
                            gid,
                            page_html,
                            frame_html,
                            {
                                "preset": preset,
                                "gid": gid,
                                "page_url": getattr(page, "url", ""),
                                "frame_url": frame_url or getattr(fr, "url", ""),
                                "markers_hit": markers_hit,
                                "vendors": vendors,
                                "strength": strength,
                                "excerpt": excerpt,
                            },
                        )
                    last_frame_url = frame_url or fr.url
                    if stats.get("ok"):
                        last_frame = fr
                        sanity_ok, sanity_reason = _calendar_sanity(stats)
                        if not sanity_ok:
                            last_invalid_stats = stats
                            last_invalid_reason = sanity_reason
                            sanity_retry_count += 1
                            sanity_last_reason = sanity_reason
                            if _detail_log_enabled():
                                sig = _calendar_signature(stats)
                                expected_total = int(stats.get("time_rows", 0) or 0) * int(stats.get("max_cols", 0) or 0)
                                log_event(
                                    "DBG",
                                    "calendar retry",
                                    frame_url=frame_url,
                                    td_count=stats.get("td_count", 0),
                                    time_rows=stats.get("time_rows", 0),
                                    max_cols=stats.get("max_cols", 0),
                                    expected_total=expected_total,
                                    total_slots=stats.get("total_slots", 0),
                                    signature=sig,
                                    attempt=attempt,
                                    reason=sanity_reason,
                                )
                                _dump_calendar_debug_sync(page, fr, stats, sanity_reason, frame_url)
                            skip, why = _is_not_reservable_page_sync(page)
                            if skip:
                                return {"ok": False, "reason": f"not_reservable({why})"}, None
                            waited += slow_step_ms
                            if progress_cb:
                                progress_cb(waited, max_wait_ms)
                            page.wait_for_timeout(slow_step_ms)
                            continue
                        if isinstance(stats, dict):
                            stats["_detail"] = {
                                "sanity_retries": sanity_retry_count,
                                "sanity_last_reason": sanity_last_reason,
                            }
                        stable_attempts += 1
                        light_sig = (stats.get("time_rows", 0), stats.get("max_cols", 0), stats.get("td_count", 0))
                        if light_sig == last_light_sig:
                            stable_hits += 1
                        else:
                            stable_hits = 1
                        last_light_sig = light_sig
                        last_stats = stats
                        if _detail_log_enabled():
                            log_event(
                                "DBG",
                                "calendar stabilize probe",
                                attempt=stable_attempts,
                                frame_url=frame_url,
                                td_count=stats.get("td_count", 0),
                                time_rows=stats.get("time_rows", 0),
                                max_cols=stats.get("max_cols", 0),
                                signature=_calendar_signature(stats),
                                stable_hits=stable_hits,
                            )
                        if stable_hits >= 2:
                            return stats, (frame_url or fr.url)
                        waited += 200
                        if progress_cb:
                            progress_cb(waited, max_wait_ms)
                        page.wait_for_timeout(200)
                        continue
                    else:
                        stable_hits = 0
                        last_light_sig = None
                last_stats = stats if isinstance(stats, dict) else last_stats
            except Exception:
                pass

        skip, why = _is_not_reservable_page_sync(page)
        if skip:
            return {"ok": False, "reason": f"not_reservable({why})"}, None
        waited += slow_step_ms
        if progress_cb:
            progress_cb(waited, max_wait_ms)
        page.wait_for_timeout(slow_step_ms)

        try:
            frame_names = []
            frame_urls = []
            for f in page.frames:
                frame_names.append(f.name or "")
                u = (f.url or "")
                if u:
                    frame_urls.append(u[:200])
            frame_names = frame_names[:20]
            frame_urls = frame_urls[:20]
        except Exception:
            frame_names = []
            frame_urls = []

    if last_stats:
        if isinstance(last_stats, dict):
            last_stats["_detail"] = {
                "sanity_retries": sanity_retry_count,
                "sanity_last_reason": sanity_last_reason,
            }
        if _detail_log_enabled():
            expected_total = int(last_stats.get("time_rows", 0) or 0) * int(last_stats.get("max_cols", 0) or 0)
            log_event(
                "DBG",
                "calendar stats unstable",
                attempt=stable_attempts,
                frame_url=last_frame_url,
                td_count=last_stats.get("td_count", 0),
                time_rows=last_stats.get("time_rows", 0),
                max_cols=last_stats.get("max_cols", 0),
                expected_total=expected_total,
                total_slots=last_stats.get("total_slots", 0),
                signature=_calendar_signature(last_stats),
                reason="unstable_timeout",
            )
            if last_frame:
                _dump_calendar_debug_sync(page, last_frame, last_stats, "unstable_timeout", last_frame_url)
        return last_stats, (last_frame_url or None)

    if last_invalid_stats:
        if isinstance(last_invalid_stats, dict):
            last_invalid_stats["_detail"] = {
                "sanity_retries": sanity_retry_count,
                "sanity_last_reason": sanity_last_reason,
            }
        if _detail_log_enabled():
            sig = _calendar_signature(last_invalid_stats)
            expected_total = int(last_invalid_stats.get("time_rows", 0) or 0) * int(last_invalid_stats.get("max_cols", 0) or 0)
            log_event(
                "DBG",
                "calendar stats sanity invalid",
                attempt=attempt,
                frame_url=last_frame_url,
                td_count=last_invalid_stats.get("td_count", 0),
                time_rows=last_invalid_stats.get("time_rows", 0),
                max_cols=last_invalid_stats.get("max_cols", 0),
                expected_total=expected_total,
                total_slots=last_invalid_stats.get("total_slots", 0),
                signature=sig,
                reason=last_invalid_reason or "sanity_fail",
            )
            if last_frame:
                _dump_calendar_debug_sync(page, last_frame, last_invalid_stats, last_invalid_reason or "sanity_fail", last_frame_url)
        last_invalid_stats = dict(last_invalid_stats)
        last_invalid_stats["ok"] = False
        last_invalid_stats["reason"] = last_invalid_reason or "sanity_fail"
        return last_invalid_stats, (last_frame_url or None)

    return {"ok": False, "reason": "calendar iframe not detected (timeout)", "frame_names": frame_names, "frame_urls": frame_urls}, None

def count_calendar_stats_by_slots(page, stop_evt: threading.Event, progress_cb=None, preset: str = None, gid: str = None):
    short_ms = min(CAL_WAIT_MS, CAL_WAIT_SHORT_MS)
    long_ms = min(CAL_WAIT_MS, CAL_WAIT_LONG_MS)

    if stop_evt.is_set():
        return None, None

    skip, why = _is_not_reservable_page_sync(page)
    if skip:
        _detail_log_skip(preset, gid, f"not_reservable({why})")
        _detail_log_iframe_wait(preset, gid, short_ms, long_ms, "skip")
        return {"ok": False, "reason": f"not_reservable({why})"}, None

    probe = _reservation_page_probe_sync(page)
    _detail_log_probe(
        preset,
        gid,
        "initial",
        not_reservable=probe.get("not_reservable"),
        has_iframe=probe.get("iframe"),
        has_calendar_table=probe.get("has_calendar_table"),
        has_link=probe.get("link"),
        login_like=probe.get("login_like"),
        text_hit=probe.get("text_hit"),
        error_page=probe.get("error_page"),
        iframe_hint=probe.get("iframe_hint"),
        iframe_src=probe.get("iframe_src"),
        iframe_h=probe.get("iframe_h"),
        iframe_frame_url=probe.get("iframe_frame_url"),
    )
    if probe.get("login_like"):
        if _detail_log_enabled():
            try:
                log_event("DBG", "login_like_reprobe", preset=preset, gid=gid, wait_ms=short_ms)
            except Exception:
                pass
        page.wait_for_timeout(short_ms)
        probe = _reservation_page_probe_sync(page)
        _detail_log_probe(
            preset,
            gid,
            "login_like_recheck",
            not_reservable=probe.get("not_reservable"),
            has_iframe=probe.get("iframe"),
            has_calendar_table=probe.get("has_calendar_table"),
            has_link=probe.get("link"),
            login_like=probe.get("login_like"),
            text_hit=probe.get("text_hit"),
            error_page=probe.get("error_page"),
            iframe_hint=probe.get("iframe_hint"),
            iframe_src=probe.get("iframe_src"),
            iframe_h=probe.get("iframe_h"),
            iframe_frame_url=probe.get("iframe_frame_url"),
        )
        if _should_skip_login_like(probe):
            _detail_log_skip(preset, gid, "not_reservable(login_iframe_src)")
            _detail_log_iframe_wait(preset, gid, short_ms, long_ms, "skip")
            return {"ok": False, "reason": "not_reservable(login_iframe_src)"}, None
    probe_not_reservable = probe.get("not_reservable")
    if probe.get("login_like"):
        probe_not_reservable = _effective_not_reservable_without_login(probe)
        # --- 追加：iframe/link が見当たらない例外ページの早期判定（無駄な待ちを避ける） ---
        # 予約枠が「クリック後に生成」「スクロール後にlazy load」されるケースに備えて、
        # まず短時間だけスクロール→再プローブして、それでも無ければ「カレンダーなし」としてスキップする。
        if (not probe.get("iframe")) and (not probe.get("link")) and (not probe.get("iframe_hint")) and (not probe.get("has_calendar_table")) and (not probe.get("text_hit")) and (not probe.get("error_page")):
            found = False
            for _ in range(3):
                try:
                    page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                except Exception:
                    pass
                try:
                    page.wait_for_timeout(400)
                except Exception:
                    pass
                p2 = _reservation_page_probe_sync(page)
                if p2.get("iframe") or p2.get("link") or p2.get("has_calendar_table"):
                    probe = p2
                    found = True
                    break
            if not found:
                _detail_log_skip(preset, gid, "calendar_not_present")
                _detail_log_iframe_wait(preset, gid, short_ms, long_ms, "skip")
                return {"ok": False, "reason": "calendar_not_present"}, None
    if probe_not_reservable:
        if probe.get("error_page") and _detail_log_enabled():
            try:
                log_event("INFO", "skip not_reservable/error", preset=preset, gid=gid, url=(page.url or ""), reason="error_page")
            except Exception:
                pass
        if _should_reprobe_text_hit(probe):
            if _detail_log_enabled():
                try:
                    log_event("DBG", "text_hit_reprobe", preset=preset, gid=gid, wait_ms=TEXT_HIT_REPROBE_MS)
                except Exception:
                    pass
            page.wait_for_timeout(TEXT_HIT_REPROBE_MS)
            probe = _reservation_page_probe_sync(page)
            _detail_log_probe(
                preset,
                gid,
                "text_hit_recheck",
                not_reservable=probe.get("not_reservable"),
                has_iframe=probe.get("iframe"),
                has_calendar_table=probe.get("has_calendar_table"),
                has_link=probe.get("link"),
                login_like=probe.get("login_like"),
                text_hit=probe.get("text_hit"),
                error_page=probe.get("error_page"),
                iframe_hint=probe.get("iframe_hint"),
                iframe_src=probe.get("iframe_src"),
                iframe_h=probe.get("iframe_h"),
                iframe_frame_url=probe.get("iframe_frame_url"),
            )
        probe_not_reservable = probe.get("not_reservable")
        if probe.get("login_like"):
            probe_not_reservable = _effective_not_reservable_without_login(probe)
        if probe_not_reservable:
            if probe.get("error_page"):
                _detail_log_skip(preset, gid, "not_reservable(error_page)")
                _detail_log_iframe_wait(preset, gid, short_ms, long_ms, "skip")
                return {"ok": False, "reason": "not_reservable(error_page)"}, None
            if _should_reprobe_text_hit(probe):
                _detail_log_skip(preset, gid, "not_reservable(text_hit)")
                _detail_log_iframe_wait(preset, gid, short_ms, long_ms, "skip")
                return {"ok": False, "reason": "not_reservable(text_hit)"}, None
            _detail_log_skip(preset, gid, "not_reservable")
            _detail_log_iframe_wait(preset, gid, short_ms, long_ms, "skip")
            return {"ok": False, "reason": "not_reservable"}, None

    stats, frame_url = _count_calendar_stats_by_slots_core(page, stop_evt, progress_cb, short_ms, preset=preset, gid=gid)
    if stats is None and stop_evt.is_set():
        return None, None
    if stats and isinstance(stats, dict) and stats.get("ok"):
        _detail_log_iframe_wait(preset, gid, short_ms, long_ms, "short")
        return stats, frame_url

    if stats and isinstance(stats, dict):
        reason = (stats.get("reason") or "")
        if reason and "iframe not detected" not in reason and "calendar_placeholders" not in reason:
            _detail_log_iframe_wait(preset, gid, short_ms, long_ms, "short")
            return stats, frame_url

    probe = _reservation_page_probe_sync(page)
    _detail_log_probe(
        preset,
        gid,
        "recheck",
        not_reservable=probe.get("not_reservable"),
        has_iframe=probe.get("iframe"),
        has_calendar_table=probe.get("has_calendar_table"),
        has_link=probe.get("link"),
        login_like=probe.get("login_like"),
        text_hit=probe.get("text_hit"),
        error_page=probe.get("error_page"),
        iframe_hint=probe.get("iframe_hint"),
        iframe_src=probe.get("iframe_src"),
        iframe_h=probe.get("iframe_h"),
        iframe_frame_url=probe.get("iframe_frame_url"),
    )
    if probe.get("login_like"):
        if _detail_log_enabled():
            try:
                log_event("DBG", "login_like_reprobe", preset=preset, gid=gid, wait_ms=short_ms)
            except Exception:
                pass
        page.wait_for_timeout(short_ms)
        probe = _reservation_page_probe_sync(page)
        _detail_log_probe(
            preset,
            gid,
            "login_like_recheck",
            not_reservable=probe.get("not_reservable"),
            has_iframe=probe.get("iframe"),
            has_calendar_table=probe.get("has_calendar_table"),
            has_link=probe.get("link"),
            login_like=probe.get("login_like"),
            text_hit=probe.get("text_hit"),
            error_page=probe.get("error_page"),
            iframe_hint=probe.get("iframe_hint"),
            iframe_src=probe.get("iframe_src"),
            iframe_h=probe.get("iframe_h"),
            iframe_frame_url=probe.get("iframe_frame_url"),
        )
        if _should_skip_login_like(probe):
            _detail_log_skip(preset, gid, "not_reservable(login_iframe_src)")
            _detail_log_iframe_wait(preset, gid, short_ms, long_ms, "skip")
            return {"ok": False, "reason": "not_reservable(login_iframe_src)"}, None
    probe_not_reservable = probe.get("not_reservable")
    if probe.get("login_like"):
        probe_not_reservable = _effective_not_reservable_without_login(probe)
    if probe_not_reservable:
        if probe.get("error_page") and _detail_log_enabled():
            try:
                log_event("INFO", "skip not_reservable/error", preset=preset, gid=gid, url=(page.url or ""), reason="error_page")
            except Exception:
                pass
        if _should_reprobe_text_hit(probe):
            if _detail_log_enabled():
                try:
                    log_event("DBG", "text_hit_reprobe", preset=preset, gid=gid, wait_ms=TEXT_HIT_REPROBE_MS)
                except Exception:
                    pass
            page.wait_for_timeout(TEXT_HIT_REPROBE_MS)
            probe = _reservation_page_probe_sync(page)
            _detail_log_probe(
                preset,
                gid,
                "text_hit_recheck",
                not_reservable=probe.get("not_reservable"),
                has_iframe=probe.get("iframe"),
                has_calendar_table=probe.get("has_calendar_table"),
                has_link=probe.get("link"),
                login_like=probe.get("login_like"),
                text_hit=probe.get("text_hit"),
                error_page=probe.get("error_page"),
                iframe_hint=probe.get("iframe_hint"),
                iframe_src=probe.get("iframe_src"),
                iframe_h=probe.get("iframe_h"),
                iframe_frame_url=probe.get("iframe_frame_url"),
            )
        probe_not_reservable = probe.get("not_reservable")
        if probe.get("login_like"):
            probe_not_reservable = _effective_not_reservable_without_login(probe)
        if probe_not_reservable:
            if probe.get("error_page"):
                _detail_log_skip(preset, gid, "not_reservable(error_page)")
                _detail_log_iframe_wait(preset, gid, short_ms, long_ms, "skip")
                return {"ok": False, "reason": "not_reservable(error_page)"}, None
            if _should_reprobe_text_hit(probe):
                _detail_log_skip(preset, gid, "not_reservable(text_hit)")
                _detail_log_iframe_wait(preset, gid, short_ms, long_ms, "skip")
                return {"ok": False, "reason": "not_reservable(text_hit)"}, None
            _detail_log_skip(preset, gid, "not_reservable")
            _detail_log_iframe_wait(preset, gid, short_ms, long_ms, "skip")
            return {"ok": False, "reason": "not_reservable"}, None

    if not (probe.get("iframe") or probe.get("link") or probe.get("iframe_hint") or probe.get("has_calendar_table")):
        _detail_log_skip(preset, gid, "iframe_missing")
        _detail_log_iframe_wait(preset, gid, short_ms, long_ms, "skip")
        return {"ok": False, "reason": "iframe_missing"}, None

    if long_ms <= short_ms:
        _detail_log_iframe_wait(preset, gid, short_ms, long_ms, "short")
        return stats, frame_url

    stats, frame_url = _count_calendar_stats_by_slots_core(page, stop_evt, progress_cb, long_ms, preset=preset, gid=gid)
    _detail_log_iframe_wait(preset, gid, short_ms, long_ms, "long")
    return stats, frame_url

# -------------------------
# スコア（率×ベル数×母数ペナルティ）
# -------------------------
_SCORE_PARAMS_CACHE = None
_SCORE_MODEL_NAME = "v2_fill_evidence"
_BD_MODEL_NAME = "v4_bd_fill_volume_decay"
_RANK_MODEL_NAME = "rank_v1_quality_momentum"

def _get_score_params():
    global _SCORE_PARAMS_CACHE
    if _SCORE_PARAMS_CACHE is None:
        cfg = load_config()
        score_cfg = cfg.get("score", {}) if isinstance(cfg, dict) else {}
        bell_sat = float(score_cfg.get("bell_sat", 18) or 18)
        trust_day_sat = float(score_cfg.get("trust_day_sat", score_cfg.get("bd_day_sat", 18)) or 18)
        bd_day_sat = float(score_cfg.get("bd_day_sat", 14) or 14)
        bd_total_sat = float(score_cfg.get("bd_total_sat", 40) or 40)
        bd_half_life = float(score_cfg.get("bd_half_life", 28) or 28)
        bd_prior_bell = float(score_cfg.get("bd_prior_bell", 1.0) or 1.0)
        bd_prior_open = float(score_cfg.get("bd_prior_open", 1.0) or 1.0)
        _SCORE_PARAMS_CACHE = {
            "bell_sat": bell_sat if bell_sat > 0 else 18,
            "trust_day_sat": trust_day_sat if trust_day_sat > 0 else 18,
            "bd_day_sat": bd_day_sat if bd_day_sat > 0 else 14,
            "bd_total_sat": bd_total_sat if bd_total_sat > 0 else 40,
            "bd_half_life": bd_half_life if bd_half_life > 0 else 28,
            "bd_prior_bell": bd_prior_bell if bd_prior_bell > 0 else 1.0,
            "bd_prior_open": bd_prior_open if bd_prior_open > 0 else 1.0,
        }
    return _SCORE_PARAMS_CACHE

_RANK_PARAMS_CACHE = None

def _get_rank_params():
    global _RANK_PARAMS_CACHE
    if _RANK_PARAMS_CACHE is None:
        cfg = load_config()
        rank_cfg = cfg.get("rank", {}) if isinstance(cfg, dict) else {}
        quality_half_life = float(rank_cfg.get("quality_half_life", 60) or 60)
        momentum_half_life = float(rank_cfg.get("momentum_half_life", 14) or 14)
        momentum_bell_sat = float(rank_cfg.get("momentum_bell_sat", 20) or 20)
        quality_prior_bell = float(rank_cfg.get("quality_prior_bell", 1.0) or 1.0)
        quality_prior_open = float(rank_cfg.get("quality_prior_open", 1.0) or 1.0)
        quality_lower_z = float(rank_cfg.get("quality_lower_z", 1.96) or 1.96)
        rank_sort_mode = str(rank_cfg.get("rank_sort_mode", "raw") or "raw").lower()
        zero_total_weight = float(rank_cfg.get("zero_total_weight", 0.35) or 0.35)
        rank_momentum_base = float(rank_cfg.get("rank_momentum_base", 0.2) or 0.2)
        quality_power = float(rank_cfg.get("quality_power", 1.0) or 1.0)
        momentum_power = float(rank_cfg.get("momentum_power", 1.0) or 1.0)
        max_window = int(rank_cfg.get("rank_max_window", 112) or 112)
        min_conf = int(rank_cfg.get("rank_input_min_confidence", 0) or 0)
        _RANK_PARAMS_CACHE = {
            "quality_half_life": quality_half_life if quality_half_life > 0 else 60,
            "momentum_half_life": momentum_half_life if momentum_half_life > 0 else 14,
            "momentum_bell_sat": momentum_bell_sat if momentum_bell_sat > 0 else 20,
            "quality_prior_bell": quality_prior_bell if quality_prior_bell > 0 else 1.0,
            "quality_prior_open": quality_prior_open if quality_prior_open > 0 else 1.0,
            "quality_lower_z": quality_lower_z if quality_lower_z > 0 else 1.96,
            "rank_sort_mode": rank_sort_mode if rank_sort_mode in ("raw", "lower") else "raw",
            "zero_total_weight": zero_total_weight if zero_total_weight > 0 else 0.35,
            "rank_momentum_base": max(0.0, min(rank_momentum_base, 1.0)),
            "quality_power": quality_power if quality_power > 0 else 1.0,
            "momentum_power": momentum_power if momentum_power > 0 else 1.0,
            "rank_max_window": max(1, max_window),
            "rank_input_min_confidence": max(0, min_conf),
        }
    return _RANK_PARAMS_CACHE

def _clamp01(value: float) -> float:
    if value < 0.0:
        return 0.0
    if value > 1.0:
        return 1.0
    return value

def _wilson_lower_bound(successes: float, total: float, z: float) -> float:
    if total <= 0 or z <= 0:
        return 0.0
    z2 = z * z
    phat = successes / total
    denom = 1.0 + (z2 / total)
    center = phat + (z2 / (2.0 * total))
    margin = z * math.sqrt((phat * (1.0 - phat) + (z2 / (4.0 * total))) / total)
    return _clamp01((center - margin) / denom)

def score_v2(stats: dict, bell_sat: float = None) -> float:
    bell = int((stats or {}).get("bell", 0) or 0)
    maru = int((stats or {}).get("maru", 0) or 0)
    tel = int((stats or {}).get("tel", 0) or 0)
    denom = bell + maru + tel
    fill = (bell / denom) if denom > 0 else 0.0
    if bell_sat is None:
        bell_sat = _get_score_params().get("bell_sat", 18)
    bell_strength = 1.0 - math.exp(-bell / float(bell_sat)) if bell_sat > 0 else 0.0
    return _clamp01(fill * bell_strength)

def calc_score(stats, max_bell_in_batch: int):
    """
    キャスト人気スコア（0.0〜1.0）
    ※「空き(○/TEL)が多いほど人気が低い」「ベル(×)が多いほど人気が高い」前提。
    ※ TEL は予約手段が電話なだけで空き扱い（○と同等）。
    ※ このスコアにはサイト側の信頼度(Confidence)は一切加算しない（別軸）。
    """
    _ = max_bell_in_batch  # v2では未使用（相対最大×は使わない）
    params = _get_score_params()
    return score_v2(stats, bell_sat=params.get("bell_sat", 18))

def calc_delta_popularity(prev_stats, cur_stats):
    """
    変化スコア（-1.0〜+1.0）
    + 予約が埋まる方向（bell↑ / bookable↓）＝人気が上がった（+）
    - 空きが増える方向（bookable↑ / bell↓）＝人気が下がった（-）
    ※ TEL は○と同等（bookableに含む）
    """
    if not prev_stats:
        return None
    pb = int(prev_stats.get("bell", 0) or 0)
    pm = int(prev_stats.get("maru", 0) or 0)
    pt = int(prev_stats.get("tel", 0) or 0)
    pbook = pm + pt

    cb = int(cur_stats.get("bell", 0) or 0)
    cm = int(cur_stats.get("maru", 0) or 0)
    ct = int(cur_stats.get("tel", 0) or 0)
    cbook = cm + ct

    total = int(cur_stats.get("total_slots", 0) or 0)
    denom = max(1, total)

    raw = (cb - pb) - (cbook - pbook)
    d = raw / denom

    if d < -1.0: d = -1.0
    if d >  1.0: d =  1.0
    return d

def _row_quality_diag_from_stats(stats: dict, frame_url: str = None, parse_errors: list = None) -> dict:
    stats = stats or {}
    stats_by_date = stats.get("stats_by_date") if isinstance(stats.get("stats_by_date"), dict) else {}
    header_dates = len(stats_by_date or {})
    total_slots = int(stats.get("total_slots", 0) or 0)
    counts_sum = (
        int(stats.get("bell", 0) or 0)
        + int(stats.get("maru", 0) or 0)
        + int(stats.get("tel", 0) or 0)
        + int(stats.get("dash", 0) or 0)
        + int(stats.get("other", 0) or 0)
    )
    slots_unique = int(stats.get("slots_unique", 0) or 0) or counts_sum
    has_calendar_table = bool(stats.get("ok")) or bool(stats.get("time_rows", 0)) or bool(stats.get("max_cols", 0)) or bool(stats.get("td_count", 0))
    return {
        "has_calendar_table": has_calendar_table,
        "header_dates": int(stats.get("header_dates", header_dates) or 0),
        "slots_total": total_slots,
        "counts_sum": counts_sum,
        "parse_errors": parse_errors or stats.get("parse_errors") or [],
        "has_iframe": bool(frame_url),
        "has_time_axis": int(stats.get("time_rows", 0) or 0) > 0,
        "slots_unique": slots_unique,
        "all_dash": _looks_like_all_dash(stats),
    }

def _calc_scrape_health(diag: dict) -> tuple[int, str, list, bool]:
    score = 100
    reasons = []
    core_missing = False

    if not diag.get("has_calendar_table"):
        score -= 40
        reasons.append("missing_calendar_table")
        core_missing = True
    if int(diag.get("header_dates", 0) or 0) < _QUALITY_HEADER_DATES_MIN:
        score -= 25
        reasons.append("header_dates_low")
        core_missing = True
    if int(diag.get("slots_total", 0) or 0) <= 0:
        score -= 40
        reasons.append("slots_total_zero")
        core_missing = True
    if int(diag.get("counts_sum", 0) or 0) != int(diag.get("slots_total", 0) or 0):
        score -= 30
        reasons.append("slot_count_mismatch")
        core_missing = True
    if diag.get("parse_errors"):
        score -= 30
        reasons.append("parse_errors")
        core_missing = True

    if score < 0:
        score = 0
    if score > 100:
        score = 100

    if score >= _QUALITY_OK_MIN:
        grade = "OK"
    elif score >= _QUALITY_WARN_MIN:
        grade = "WARN"
    else:
        grade = "BAD"
    return score, grade, reasons, core_missing

def _apply_scrape_health_fields(row: dict, score: int, grade: str, reasons: list, core_missing: bool) -> None:
    if not isinstance(row, dict):
        return
    row["row_quality_score"] = score
    row["row_quality_grade"] = grade
    row["row_quality_reasons"] = reasons
    row["row_quality_core_missing"] = core_missing
    row["scrape_health"] = score
    row["scrape_health_grade"] = grade
    row["scrape_health_reasons"] = reasons
    row["scrape_health_core_missing"] = core_missing
    row["site_confidence"] = score
    row.setdefault("conf", row.get("site_confidence"))

def _calc_row_quality(diag: dict) -> tuple[int, str, list, bool]:
    """
    互換用: row_quality = scrape_health と同義
    """
    return _calc_scrape_health(diag)

def _should_dump_suspicious(stats: dict, detect: dict, frame_url: str = None) -> bool:
    if not detect or not detect.get("suspicious_hit"):
        return False
    diag = _row_quality_diag_from_stats(stats, frame_url=frame_url)
    _, grade, _, core_missing = _calc_scrape_health(diag)
    strength = detect.get("strength")
    if strength == "strong" and grade == "BAD":
        return True
    if strength == "weak" and core_missing:
        return True
    return False

def calc_scrape_health(stats: dict, frame_url: str = None, parse_errors: list = None):
    """
    取得/解析の健全性（0〜100）
    ※ 予約シグナル(all_dash等)とは分離
    """
    diag = _row_quality_diag_from_stats(stats, frame_url=frame_url, parse_errors=parse_errors)
    score, _grade, reasons, _core_missing = _calc_scrape_health(diag)
    return score, reasons

def _calc_signal_strength(stats: dict, stats_by_date: dict = None):
    """
    需要/予約シグナルの強さ（0〜100）
    - all_dashは異常ではなく、信号が弱い状態として扱う
    """
    stats = stats or {}
    params = _get_score_params()
    total_sat = float(params.get("bd_total_sat", 40) or 40)
    day_sat = float(params.get("bd_day_sat", 14) or 14)
    bell = int(stats.get("bell", 0) or 0)
    maru = int(stats.get("maru", 0) or 0)
    tel = int(stats.get("tel", 0) or 0)
    total = bell + maru + tel
    service_days = 0
    if isinstance(stats_by_date, dict):
        for st in stats_by_date.values():
            if not isinstance(st, dict):
                continue
            day_total = int(st.get("bell", 0) or 0) + int(st.get("maru", 0) or 0) + int(st.get("tel", 0) or 0)
            if day_total > 0:
                service_days += 1
    if service_days <= 0 and total > 0:
        service_days = 1
    total_factor = 1.0 - math.exp(-total / total_sat) if total_sat > 0 else 1.0
    day_factor = 1.0 - math.exp(-service_days / day_sat) if day_sat > 0 else 1.0
    strength = _clamp01(total_factor * day_factor) * 100.0
    detail = {
        "signal_total": total,
        "signal_service_days": service_days,
        "signal_total_sat": total_sat,
        "signal_day_sat": day_sat,
        "signal_total_factor": total_factor,
        "signal_day_factor": day_factor,
        "all_dash": _looks_like_all_dash(stats),
    }
    return strength, detail

def calc_site_confidence(diag):
    """
    互換: site_confidence = scrape_health
    旧来のrow_quality互換も維持する。
    """
    if isinstance(diag, dict) and any(k in diag for k in ("has_calendar_table", "header_dates", "slots_total", "counts_sum")):
        score, _grade, reasons, _core_missing = _calc_scrape_health(diag or {})
        return score, reasons
    if isinstance(diag, dict):
        score, reasons = calc_scrape_health(diag)
        return score, reasons
    score, reasons = calc_scrape_health({})
    return score, reasons

def _safe_name(s: str):
    s = re.sub(r"[\\/:*?\"<>|\s]+", "_", s or "").strip("_")
    return s[:80] if len(s) > 80 else s

def normalize_preset_name(name: str) -> str:
    name = (name or "").strip()
    return re.sub(r"^\d+_", "", name)


class BlockedBySiteError(RuntimeError):
    pass

def _now_ts():
    return time.strftime("%Y%m%d_%H%M%S")

def ensure_data_dirs():
    os.makedirs(DATA_ROOT, exist_ok=True)
    os.makedirs(STATE_DIR, exist_ok=True)
    os.makedirs(RUNS_DIR, exist_ok=True)
    os.makedirs(ANALYTICS_DIR, exist_ok=True)
    os.makedirs(HISTORY_DIR, exist_ok=True)
    os.makedirs(LOG_DIR, exist_ok=True)
    os.makedirs(DAILY_DIR, exist_ok=True)
    os.makedirs(NOTIFY_DIR, exist_ok=True)


# -------------------------
# 保持期間メンテ（6ヶ月など）
# A) 日付で削除 + B) 行数で制限（おすすめ）
# -------------------------
def _days_in_month(y: int, m: int) -> int:
    try:
        return calendar.monthrange(y, m)[1]
    except Exception:
        # fallback
        if m == 2:
            # leap
            leap = (y % 4 == 0 and (y % 100 != 0 or y % 400 == 0))
            return 29 if leap else 28
        return 30 if m in (4,6,9,11) else 31

def _subtract_months(d: datetime.date, months: int) -> datetime.date:
    if months <= 0:
        return d
    y = d.year
    m = d.month - months
    while m <= 0:
        m += 12
        y -= 1
    day = min(d.day, _days_in_month(y, m))
    return datetime.date(y, m, day)

def _parse_dt_any(s: str):
    if not s:
        return None
    s = str(s).strip()
    fmts = [
        "%Y%m%d_%H%M%S",
        "%Y%m%d%H%M%S",
        "%Y-%m-%d",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M:%S.%f",
    ]
    for f in fmts:
        try:
            return datetime.datetime.strptime(s, f)
        except Exception:
            pass
    # 末尾のZやTZオフセット等を雑に落として再挑戦
    try:
        if s.endswith("Z"):
            return _parse_dt_any(s[:-1])
    except Exception:
        pass
    return None

def _parse_run_dir_dt(name: str):
    # run_YYYYMMDD_HHMMSS
    try:
        m = re.match(r"^run_(\d{8}_\d{6})$", name)
        if m:
            return _parse_dt_any(m.group(1))
    except Exception:
        pass
    return None

def _parse_daily_dir_dt(name: str):
    # YYYY-MM-DD
    try:
        if re.match(r"^\d{4}-\d{2}-\d{2}$", name):
            d = datetime.date.fromisoformat(name)
            return datetime.datetime.combine(d, datetime.time.min)
    except Exception:
        pass
    return None

def _json_dt(obj: dict):
    if not isinstance(obj, dict):
        return None
    # まずはこのツールの標準（ts: YYYYMMDD_HHMMSS）
    for k in ("ts", "run_ts", "timestamp", "time", "datetime", "date"):
        v = obj.get(k)
        dt = _parse_dt_any(v) if v else None
        if dt:
            return dt
    return None

def _prune_jsonl_file(path: str, cutoff_dt: datetime.datetime, max_lines: int):
    # A) cutoff_dt より古い行は捨てる
    # B) 残りは末尾 max_lines に制限
    kept = deque(maxlen=max(10, int(max_lines or 0) or 0) if (max_lines and max_lines > 0) else None)
    total = 0
    kept_count = 0

    # max_linesが未指定/0のときは「日付フィルタだけ」or「何もしない」になりがちなので、
    # 事故防止として最低10行は保持。
    if max_lines is None or max_lines <= 0:
        kept = deque(maxlen=None)

    try:
        with open(path, "r", encoding="utf-8") as r:
            for line in r:
                total += 1
                s = line.strip()
                if not s:
                    continue
                try:
                    obj = json.loads(s)
                except Exception:
                    # JSONでなければ、とりあえず保持（ただしBの上限は効く）
                    kept.append(line)
                    kept_count += 1
                    continue
                dt = _json_dt(obj)
                if dt and cutoff_dt and dt < cutoff_dt:
                    continue
                kept.append(json.dumps(obj, ensure_ascii=False) + "\n")
                kept_count += 1
    except Exception:
        return {"ok": False, "total": total, "kept": kept_count, "changed": False}

    new_lines = list(kept)

    # 変更判定（同じなら書き換えない）
    try:
        with open(path, "r", encoding="utf-8") as r:
            old_lines = r.readlines()
        changed = (old_lines != new_lines)
    except Exception:
        changed = True

    if changed:
        tmp = path + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as w:
                w.writelines(new_lines)
            os.replace(tmp, path)
        except Exception:
            try:
                if os.path.exists(tmp):
                    os.remove(tmp)
            except Exception:
                pass

    return {"ok": True, "total": total, "kept": len(new_lines), "changed": changed}

def _tail_text_file(path: str, max_lines: int):
    if not max_lines or max_lines <= 0:
        return {"ok": True, "total": 0, "kept": 0, "changed": False}
    dq = deque(maxlen=max_lines)
    total = 0
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as r:
            for line in r:
                total += 1
                dq.append(line)
    except Exception:
        return {"ok": False, "total": total, "kept": 0, "changed": False}

    new_lines = list(dq)
    changed = False
    try:
        # サイズが超えていないなら変更なし
        if total > len(new_lines):
            changed = True
            tmp = path + ".tmp"
            with open(tmp, "w", encoding="utf-8") as w:
                w.writelines(new_lines)
            os.replace(tmp, path)
    except Exception:
        pass

    return {"ok": True, "total": total, "kept": len(new_lines), "changed": changed}

def retention_cleanup(retention_months: int = 6, retention_max_lines: int = 200, log_gui=None):
    """保持期間メンテ。run/dailyは日付で削除、history/logは日付+行数でトリムする。"""
    ensure_data_dirs()

    # cutoff
    try:
        months = int(retention_months or 0)
    except Exception:
        months = 6
    if months <= 0:
        return {"ok": True, "skipped": True}

    cut_date = _subtract_months(datetime.date.today(), months)
    cutoff_dt = datetime.datetime.combine(cut_date, datetime.time.min)

    # 1) runs を日付で削除
    removed_runs = 0
    try:
        names = [d for d in os.listdir(RUNS_DIR) if os.path.isdir(os.path.join(RUNS_DIR, d))]
        # 直近は必ず残す（事故防止）
        names_sorted = sorted(names)
        keep_last = names_sorted[-1:] if names_sorted else []
        for d in names:
            if d in keep_last:
                continue
            dt = _parse_run_dir_dt(d)
            if dt and dt < cutoff_dt:
                try:
                    shutil.rmtree(os.path.join(RUNS_DIR, d), ignore_errors=True)
                    removed_runs += 1
                except Exception:
                    pass
    except Exception:
        pass

    # 2) daily を日付で削除
    removed_daily = 0
    try:
        names = [d for d in os.listdir(DAILY_DIR) if os.path.isdir(os.path.join(DAILY_DIR, d))]
        # 直近は残す
        names_sorted = sorted([d for d in names if re.match(r"^\d{4}-\d{2}-\d{2}$", d)])
        keep_last = names_sorted[-1:] if names_sorted else []
        for d in names:
            if d in keep_last:
                continue
            dt = _parse_daily_dir_dt(d)
            if dt and dt < cutoff_dt:
                try:
                    shutil.rmtree(os.path.join(DAILY_DIR, d), ignore_errors=True)
                    removed_daily += 1
                except Exception:
                    pass
    except Exception:
        pass

    # 3) history（jsonl）を日付+行数でトリム
    trimmed_hist = 0
    hist_changed = 0
    try:
        for fn in os.listdir(HISTORY_DIR):
            if not fn.lower().endswith(".jsonl"):
                continue
            path = os.path.join(HISTORY_DIR, fn)
            st = _prune_jsonl_file(path, cutoff_dt, int(retention_max_lines or 200))
            trimmed_hist += 1
            if st.get("changed"):
                hist_changed += 1
    except Exception:
        pass

    # 4) logs（jsonl/テキスト）を日付+行数でトリム
    log_changed = 0
    try:
        # jsonl系
        for fn in os.listdir(LOG_DIR):
            path = os.path.join(LOG_DIR, fn)
            if os.path.isdir(path):
                continue
            low = fn.lower()
            if low.endswith(".jsonl"):
                st = _prune_jsonl_file(path, cutoff_dt, max(20000, int(retention_max_lines or 200) * 200))
                if st.get("changed"):
                    log_changed += 1
            elif low.endswith(".log") or low.endswith(".txt"):
                st = _tail_text_file(path, max(20000, int(retention_max_lines or 200) * 200))
                if st.get("changed"):
                    log_changed += 1
    except Exception:
        pass

    msg = f"[RETENTION] cutoff={cut_date.isoformat()} runs_del={removed_runs} daily_del={removed_daily} hist_trim={hist_changed}/{trimmed_hist} logs_trim={log_changed}"
    try:
        log_event("INFO", "retention_cleanup", cutoff=cut_date.isoformat(), runs_deleted=removed_runs, daily_deleted=removed_daily, hist_files_changed=hist_changed, hist_files=trimmed_hist, logs_changed=log_changed)
    except Exception:
        pass
    if callable(log_gui):
        try:
            log_gui(msg + "\n")
        except Exception:
            pass

    return {"ok": True, "cutoff": cut_date.isoformat(), "runs_deleted": removed_runs, "daily_deleted": removed_daily, "hist_files_changed": hist_changed, "hist_files": trimmed_hist, "logs_changed": log_changed}

def make_run_dir():
    ensure_data_dirs()
    d = os.path.join(RUNS_DIR, f"run_{_now_ts()}")
    os.makedirs(d, exist_ok=True)
    os.makedirs(os.path.join(d, "jobs"), exist_ok=True)
    return d

def load_state_snapshot(gid: str):
    ensure_data_dirs()
    path = os.path.join(STATE_DIR, f"{gid}.json")
    if not os.path.exists(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as r:
            return json.load(r)
    except Exception:
        return None

def save_state_snapshot(gid: str, snap: dict):
    ensure_data_dirs()
    path = os.path.join(STATE_DIR, f"{gid}.json")
    try:
        _atomic_write_json(path, snap)
    except Exception as e:
        log_event("ERR", "save_state_snapshot failed", path=path, err=str(e)[:200])

def _atomic_write_text(path: str, text: str):
    tmp_path = f"{path}.tmp{os.getpid()}_{int(time.time() * 1000)}"
    try:
        with open(tmp_path, "w", encoding="utf-8") as w:
            w.write(text)
            w.flush()
            os.fsync(w.fileno())
        os.replace(tmp_path, path)
        return True
    except Exception as e:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass
        log_event("ERR", "atomic write failed", path=path, err=str(e)[:200])
        return False

def _atomic_write_json(path: str, obj):
    tmp_path = f"{path}.tmp{os.getpid()}_{int(time.time() * 1000)}"
    try:
        with open(tmp_path, "w", encoding="utf-8") as w:
            json.dump(obj, w, ensure_ascii=False, indent=2)
            w.flush()
            os.fsync(w.fileno())
        os.replace(tmp_path, path)
        return True
    except Exception as e:
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass
        log_event("ERR", "atomic json write failed", path=path, err=str(e)[:200])
        return False

def write_run_file(run_dir: str, relpath: str, obj):
    path = os.path.join(run_dir, relpath)
    if obj is None:
        log_event("WARN", "write_run_file skipped (obj is None)", path=path)
        return False
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        if isinstance(obj, str):
            return _atomic_write_text(path, obj)
        return _atomic_write_json(path, obj)
    except Exception as e:
        log_event("ERR", "write_run_file failed", path=path, err=str(e)[:200])
        return False

def save_job_outputs(run_dir: str, job, job_i: int, current_rows, prev_rows):
    safe_job = _safe_name(job.name)
    cur_path = f"jobs/{job_i:02d}_{safe_job}_current.json"
    prev_path = f"jobs/{job_i:02d}_{safe_job}_prev.json"
    cur_rows = current_rows if current_rows is not None else []
    prev_rows = prev_rows if prev_rows is not None else []
    write_run_file(run_dir, cur_path, cur_rows)
    write_run_file(run_dir, prev_path, prev_rows)
    log_event("INFO", "job outputs saved", preset=job.name, current=cur_path, prev=prev_path, rows=len(cur_rows))
    return cur_path

def build_history_summary(all_rows: list, run_ts: str, run_dir: str):
    summary_lines = []
    summary_lines.append(f"run_ts={run_ts}")
    summary_lines.append(f"run_dir={os.path.basename(run_dir)}")
    summary_lines.append(f"rows={len(all_rows)}")
    summary_lines.append("")
    if not all_rows:
        summary_lines.append("no rows")
        return "\n".join(summary_lines)

    by_preset = {}
    for row in all_rows:
        if not isinstance(row, dict):
            continue
        preset = str(row.get("preset", "") or "")
        by_preset.setdefault(preset, []).append(row)

    def _avg(vals):
        return (sum(vals) / len(vals)) if vals else None

    def _fmt(val, scale=1.0, suffix=""):
        if val is None:
            return "N/A"
        try:
            return f"{float(val) * scale:.2f}{suffix}"
        except Exception:
            return str(val)

    summary_lines.append("=== per preset ===")
    for preset, rows in sorted(by_preset.items(), key=lambda x: x[0]):
        scores = []
        bigs = []
        confs = []
        for r in rows:
            sc = r.get("score")
            if isinstance(sc, (int, float)):
                scores.append(float(sc))
            bd = r.get("big_score", r.get("score"))
            if isinstance(bd, (int, float)):
                bigs.append(float(bd))
            cf = r.get("site_confidence")
            if isinstance(cf, (int, float)):
                confs.append(float(cf))
        summary_lines.append(f"- preset={preset or '(none)'} rows={len(rows)} avg_score={_fmt(_avg(scores), 100, '%')} avg_bd={_fmt(_avg(bigs), 100, '%')} avg_conf={_fmt(_avg(confs), 1, '')}")

    return "\n".join(summary_lines)

def save_run_outputs(run_dir: str, run_ts: str, all_rows: list, force_today: bool=False, cfg: dict=None):
    cfg = cfg or {}
    _assign_rank_percentiles(all_rows or [])
    write_run_file(run_dir, "all_current.json", all_rows if all_rows is not None else [])
    try:
        build_analytics(all_rows or [], run_ts, run_dir)
    except Exception as e:
        log_event("WARN", "build_analytics failed", err=str(e)[:200])
    try:
        summary_text = build_history_summary(all_rows or [], run_ts, run_dir)
        if summary_text:
            write_run_file(run_dir, "history_summary.txt", summary_text)
    except Exception as e:
        log_event("WARN", "build_history_summary failed", err=str(e)[:200])
    update_daily_snapshot(all_rows or [], run_dir, run_ts, force_today=force_today, cfg=cfg)
    return True

def update_daily_snapshot(all_rows: list, run_dir: str, run_ts: str, force_today: bool=False, cfg: dict=None):
    cfg = cfg or {}
    today = datetime.date.today().isoformat()
    day_dir = os.path.join(DAILY_DIR, today)
    snap_path = os.path.join(day_dir, "daily_snapshot.json")

    if os.path.exists(snap_path) and (not force_today) and (not bool(cfg.get("auto",{}).get("force_overwrite_today", False))):
        log_event("INFO", "daily snapshot exists -> skip", path=snap_path)
        return False

    os.makedirs(day_dir, exist_ok=True)

    def _calc_avg_big_score(rows):
        vals = []
        for r in rows or []:
            if not isinstance(r, dict):
                continue
            v = r.get("big_score", None)
            if v is None:
                v = r.get("score", None)
            if isinstance(v, (int, float)):
                vals.append(float(v))
        return (sum(vals) / len(vals)) if vals else None

    def _row_key(row):
        if not isinstance(row, dict):
            return None
        preset = row.get("preset", "")
        gid = row.get("gid", "")
        if preset or gid:
            return f"{preset}::{gid}"
        name = row.get("name", "")
        detail = row.get("detail", "") or row.get("res", "")
        list_url = row.get("list_url", "")
        if name or detail or list_url:
            return f"{preset}::{gid}::{name}::{detail}::{list_url}"
        return None

    def _calc_delta(cur, prev):
        if isinstance(cur, (int, float)) and isinstance(prev, (int, float)):
            return cur - prev
        return None

    def _calc_row_big_score(row):
        if not isinstance(row, dict):
            return None
        v = row.get("big_score", None)
        if v is None:
            v = row.get("score", None)
        return v if isinstance(v, (int, float)) else None

    prev_day = None
    prev_snapshot = None
    prev_avg_big_score = None
    gap_days = None
    ma_samples = []

    days = []
    try:
        days = [d for d in os.listdir(DAILY_DIR) if re.match(r"^\d{4}-\d{2}-\d{2}$", d)]
        days.sort()
    except Exception:
        days = []

    for d in reversed(days):
        if d >= today:
            continue
        ppath = os.path.join(DAILY_DIR, d, "daily_snapshot.json")
        if not os.path.exists(ppath):
            continue
        try:
            with open(ppath, "r", encoding="utf-8") as r:
                psnap = json.load(r) or {}
        except Exception:
            continue
        if not isinstance(psnap, dict):
            continue
        if prev_day is None:
            prev_day = d
            prev_snapshot = psnap
        bd_prev = psnap.get("bd_daily", {}) if isinstance(psnap.get("bd_daily", {}), dict) else {}
        avg_sample = bd_prev.get("avg_big_score", None)
        if avg_sample is None:
            avg_sample = _calc_avg_big_score(psnap.get("all_current", []) or [])
        if isinstance(avg_sample, (int, float)):
            ma_samples.append(float(avg_sample))
        if len(ma_samples) >= 112:
            break

    if prev_day:
        try:
            gap_days = (datetime.date.fromisoformat(today) - datetime.date.fromisoformat(prev_day)).days
        except Exception:
            gap_days = None

    avg_big_score = _calc_avg_big_score(all_rows)

    if prev_snapshot and isinstance(prev_snapshot, dict):
        bd_prev = prev_snapshot.get("bd_daily", {}) if isinstance(prev_snapshot.get("bd_daily", {}), dict) else {}
        prev_avg_big_score = bd_prev.get("avg_big_score", None)
        if not isinstance(prev_avg_big_score, (int, float)):
            prev_avg_big_score = _calc_avg_big_score(prev_snapshot.get("all_current", []) or [])

    delta_avg_big_score = None
    delta_avg_big_score_per_day = None
    if isinstance(avg_big_score, (int, float)) and isinstance(prev_avg_big_score, (int, float)):
        delta_avg_big_score = avg_big_score - prev_avg_big_score
        if isinstance(gap_days, int) and gap_days >= 1:
            delta_avg_big_score_per_day = delta_avg_big_score / gap_days

    ma3 = ma14 = ma28 = ma56 = ma84 = ma112 = None
    samples = []
    if isinstance(avg_big_score, (int, float)):
        samples.append(float(avg_big_score))
    samples.extend(ma_samples)
    if samples:
        ma3 = sum(samples[:3]) / len(samples[:3])
        ma14 = sum(samples[:14]) / len(samples[:14])
        ma28 = sum(samples[:28]) / len(samples[:28])
        ma56 = sum(samples[:56]) / len(samples[:56])
        ma84 = sum(samples[:84]) / len(samples[:84])
        ma112 = sum(samples[:112]) / len(samples[:112])

    prev_rows_map = {}
    if prev_snapshot and isinstance(prev_snapshot, dict):
        for prow in prev_snapshot.get("all_current", []) or []:
            key = _row_key(prow)
            if key and key not in prev_rows_map:
                prev_rows_map[key] = prow

    for row in all_rows:
        key = _row_key(row)
        prow = prev_rows_map.get(key) if key else None
        if prow:
            row["prev_seen_day"] = prev_day
            row["gap_days"] = gap_days
        else:
            row["prev_seen_day"] = None
            row["gap_days"] = None

        cur_bs = _calc_row_big_score(row)
        prev_bs = _calc_row_big_score(prow) if prow else None
        delta_bs = _calc_delta(cur_bs, prev_bs)
        row["delta_big_score"] = delta_bs
        if delta_bs is not None and isinstance(gap_days, int) and gap_days >= 1:
            row["delta_big_score_per_day"] = delta_bs / gap_days
        else:
            row["delta_big_score_per_day"] = None

        cur_stats = row.get("stats", {}) if isinstance(row, dict) else {}
        prev_stats = (prow.get("stats", {}) if isinstance(prow, dict) else {}) if prow else {}
        row["delta_bell"] = _calc_delta(cur_stats.get("bell"), prev_stats.get("bell"))
        row["delta_bookable"] = _calc_delta(cur_stats.get("bookable_slots"), prev_stats.get("bookable_slots"))
        row["delta_total"] = _calc_delta(cur_stats.get("total_slots"), prev_stats.get("total_slots"))

    bd_daily = {
        "prev_day": prev_day,
        "gap_days": gap_days,
        "avg_big_score": avg_big_score,
        "delta_avg_big_score": delta_avg_big_score,
        "delta_avg_big_score_per_day": delta_avg_big_score_per_day,
        "ma3_samples_avg_big_score": ma3,
        "ma14_samples_avg_big_score": ma14,
        "ma28_samples_avg_big_score": ma28,
        "ma56_samples_avg_big_score": ma56,
        "ma84_samples_avg_big_score": ma84,
        "ma112_samples_avg_big_score": ma112,
    }

    if _detail_log_enabled():
        log_event("INFO", "bd", today=today, prev_day=prev_day, gap_days=gap_days)
        log_event("INFO", "bd", avg_big_score=avg_big_score, prev_avg=prev_avg_big_score, delta=delta_avg_big_score, per_day=delta_avg_big_score_per_day)
        log_event("INFO", "bd", ma3=ma3, ma14=ma14, ma28=ma28, ma56=ma56, ma84=ma84, ma112=ma112)

    daily = {
        "date": today,
        "run_ts": run_ts,
        "run_dir": os.path.basename(run_dir),
        "rows": len(all_rows),
        "score_model": _SCORE_MODEL_NAME,
        "all_current": all_rows,
        "bd_daily": bd_daily,
    }

    wrote = _atomic_write_json(snap_path, daily)
    _atomic_write_text(os.path.join(day_dir, "latest_run.txt"), os.path.basename(run_dir))
    if wrote:
        log_event("INFO", "daily snapshot saved", path=snap_path)
    return wrote
# -------------------------
# ビッグデータ（履歴/分析）
# -------------------------
def _hist_path(gid: str):
    ensure_data_dirs()
    return os.path.join(HISTORY_DIR, f"{gid}.jsonl")

def load_history(gid: str, limit: int = 200):
    """
    1キャスト（gid）の過去履歴（jsonl）を新しい順に返す。
    limit: 読み込み上限（重くならないよう制限）
    """
    path = _hist_path(gid)
    if not os.path.exists(path):
        return []
    rows = []
    try:
        with open(path, "r", encoding="utf-8") as r:
            for line in r:
                line = line.strip()
                if not line:
                    continue
                try:
                    rows.append(json.loads(line))
                except Exception:
                    continue
    except Exception:
        return []
    if not rows:
        return []
    # tsは "YYYYMMDD_HHMMSS" 形式想定。無ければ末尾順。
    rows.sort(key=lambda x: (x.get("ts",""), x.get("run_ts","")), reverse=True)
    return rows[:limit]

def append_history(gid: str, rec: dict):
    """
    履歴を追記（jsonl）。フォルダ内完結：score_data/history/
    """
    path = _hist_path(gid)
    try:
        with open(path, "a", encoding="utf-8") as w:
            w.write(json.dumps(rec, ensure_ascii=False) + "\n")
    except Exception as e:
        log_event("ERR", "append_history failed", path=path, err=str(e)[:200])

def _percentile_of(value: float, arr):
    if not arr:
        return None
    unique = sorted(set(arr))
    if not unique:
        return None
    if len(unique) == 1:
        return 0.0
    denom = len(unique) - 1
    for i, v in enumerate(unique):
        if v == value:
            return i / denom
    return None

def _collect_service_date_series(cur_stats: dict, hist: list, cur_stats_by_date: dict, min_conf: int, include_zero: bool=False):
    def _parse_iso_date(val):
        if not val:
            return None
        try:
            return datetime.date.fromisoformat(str(val))
        except Exception:
            return None

    def _obs_date_from_entry(h):
        if not isinstance(h, dict):
            return None
        ts = h.get("ts") or h.get("run_ts") or h.get("date") or h.get("datetime") or h.get("time")
        if ts:
            dt = _parse_dt_any(ts)
            if dt:
                return dt.date()
            m = re.match(r"^(\d{8})", str(ts))
            if m:
                try:
                    dt = datetime.datetime.strptime(m.group(1), "%Y%m%d")
                    return dt.date()
                except Exception:
                    pass
        return None

    def _stats_by_date_from_entry(h):
        if not isinstance(h, dict):
            return None
        sbd = h.get("stats_by_date")
        if isinstance(sbd, dict):
            return sbd
        st = h.get("stats")
        if isinstance(st, dict) and isinstance(st.get("stats_by_date"), dict):
            return st.get("stats_by_date")
        return None

    def _update_best(best, service_date, obs_date, st):
        if not service_date:
            return
        if obs_date and obs_date > service_date:
            return
        bell = int((st or {}).get("bell", 0) or 0)
        maru = int((st or {}).get("maru", 0) or 0)
        tel = int((st or {}).get("tel", 0) or 0)
        if bell + maru + tel <= 0 and not include_zero:
            return
        key = service_date.isoformat()
        prev = best.get(key)
        if not prev:
            best[key] = {
                "service_date": service_date,
                "obs_date": obs_date,
                "bell": bell,
                "maru": maru,
                "tel": tel,
            }
            return
        prev_obs = prev.get("obs_date")
        if prev_obs is None and obs_date is not None:
            prev.update(obs_date=obs_date, bell=bell, maru=maru, tel=tel)
            return
        if obs_date is not None and prev_obs is not None and obs_date > prev_obs:
            prev.update(obs_date=obs_date, bell=bell, maru=maru, tel=tel)

    best = {}
    obs_dates = set()
    today = datetime.date.today()
    obs_dates.add(today)
    if isinstance(cur_stats_by_date, dict):
        for dkey, st in cur_stats_by_date.items():
            service_date = _parse_iso_date(dkey)
            _update_best(best, service_date, today, st)
    if not best:
        _update_best(best, today, today, cur_stats or {})

    for h in hist or []:
        if not isinstance(h, dict):
            continue
        if min_conf > 0:
            conf = int(h.get("site_confidence", 0) or 0)
            if conf < min_conf:
                continue
        obs_date = _obs_date_from_entry(h)
        if obs_date:
            obs_dates.add(obs_date)
        sbd = _stats_by_date_from_entry(h)
        if isinstance(sbd, dict):
            for dkey, st in sbd.items():
                service_date = _parse_iso_date(dkey)
                _update_best(best, service_date, obs_date, st)
        else:
            st = h.get("stats")
            if isinstance(st, dict) and obs_date:
                _update_best(best, obs_date, obs_date, st)

    series = sorted(best.values(), key=lambda x: x.get("service_date") or today, reverse=True)
    return series, obs_dates

def _calc_bigdata_score_detail_legacy_v3(cur_stats: dict, hist: list, cur_stats_by_date: dict = None):
    """
    旧BD（v3）ロジック：互換用
    """
    params = _get_score_params()
    bell_sat = params.get("bell_sat", 18)
    trust_day_sat = params.get("trust_day_sat", 18)
    cfg = load_config()
    score_cfg = cfg.get("score", {}) if isinstance(cfg.get("score"), dict) else {}
    min_conf = int(score_cfg.get("bd_input_min_confidence", 0) or 0)

    series, obs_dates = _collect_service_date_series(cur_stats, hist, cur_stats_by_date, min_conf)
    today = datetime.date.today()
    series_desc = [
        score_v2({"bell": s.get("bell"), "maru": s.get("maru"), "tel": s.get("tel")}, bell_sat=bell_sat)
        for s in series
    ]
    if not series_desc:
        series_desc = [score_v2(cur_stats or {}, bell_sat=bell_sat)]

    def _ma(window):
        if not series_desc:
            return None
        w = max(1, int(window))
        subset = series_desc[:min(w, len(series_desc))]
        return (sum(subset) / len(subset)) if subset else None

    n_service = len(series) if series else len(series_desc)
    n_obs = len(obs_dates)
    ma3 = _ma(3)
    ma14 = _ma(14)
    ma28 = _ma(28)
    ma56 = _ma(56)
    ma84 = _ma(84)
    ma112 = _ma(112)

    bd_window = None
    if n_service >= 112:
        level = ma112
        bd_window = 112
    elif n_service >= 84:
        level = ma84
        bd_window = 84
    elif n_service >= 56:
        level = ma56
        bd_window = 56
    elif n_service >= 28:
        level = ma28
        bd_window = 28
    elif n_service >= 14:
        level = ma14
        bd_window = 14
    elif n_service >= 3:
        level = ma3
        bd_window = 3
    else:
        level = series_desc[0] if series_desc else 0.0

    if trust_day_sat <= 0:
        trust = 1.0
    else:
        trust = 1.0 - math.exp(-n_obs / float(trust_day_sat))

    base = float(level or 0.0)
    big_score = _clamp01(base * trust)
    detail = {
        "big_score": big_score,
        "bd_level": base,
        "bd_trust": trust,
        "bd_days": n_service,
        "bd_window": bd_window,
        "bd_service_days": n_service,
        "bd_obs_days": n_obs,
        "ma3": ma3,
        "ma14": ma14,
        "ma28": ma28,
        "ma56": ma56,
        "ma84": ma84,
        "ma112": ma112,
        "unique_dates": [s.get("service_date").isoformat() for s in series if s.get("service_date")],
    }
    return big_score, detail

def _calc_bigdata_score_detail(cur_stats: dict, hist: list, cur_stats_by_date: dict = None):
    """
    ビッグデータ版スコア（0.0〜1.0）
    - サービス日(YYYY-MM-DD)ごとの“ユニーク日付系列”から最新データだけ採用
    - bell/(bell+maru+tel) を平滑化して長期の埋まりを評価
    - 観測量は飽和関数で頭打ちし、古いデータは減衰させる
    - Confidence（サイト信頼度）は別軸なので、ここでは加点しない
    """
    params = _get_score_params()
    cfg = load_config()
    score_cfg = cfg.get("score", {}) if isinstance(cfg.get("score"), dict) else {}
    min_conf = int(score_cfg.get("bd_input_min_confidence", 0) or 0)
    total_sat = float(params.get("bd_total_sat", 40) or 40)
    half_life = float(params.get("bd_half_life", 28) or 28)
    prior_bell = float(params.get("bd_prior_bell", 1.0) or 1.0)
    prior_open = float(params.get("bd_prior_open", 1.0) or 1.0)

    series, obs_dates = _collect_service_date_series(cur_stats, hist, cur_stats_by_date, min_conf)
    today = datetime.date.today()
    max_window = 112
    series = series[:max_window]

    # 重みは idx ではなく「今日からの残日数」を基準にする（欠け日付でも意味がぶれない）
    service_dates = [
        s.get("service_date") for s in series
        if isinstance(s, dict) and isinstance(s.get("service_date"), datetime.date)
    ]
    if service_dates:
        max_days_until = max((d - today).days for d in service_dates)
    else:
        max_days_until = 0

    entries = []
    for idx, s in enumerate(series):
        _ = idx  # idxは参照しない（days_untilベースの重み付けに変更）
        bell = int(s.get("bell", 0) or 0)
        maru = int(s.get("maru", 0) or 0)
        tel = int(s.get("tel", 0) or 0)
        total = bell + maru + tel
        if total <= 0:
            continue
        smooth_fill = (bell + prior_bell) / (total + prior_bell + prior_open)
        service_date = s.get("service_date") or today
        if isinstance(service_date, datetime.datetime):
            service_date = service_date.date()
        if isinstance(service_date, datetime.date):
            days_until = (service_date - today).days
        else:
            days_until = 0
            service_date = today
        age = float(max(0, max_days_until - days_until))
        weight = math.exp(-age / half_life) if half_life > 0 else 1.0
        entries.append({
            "service_date": service_date,
            "bell": bell,
            "maru": maru,
            "tel": tel,
            "total": total,
            "fill": smooth_fill,
            "weight": weight,
        })

    if not entries:
        bell = int((cur_stats or {}).get("bell", 0) or 0)
        maru = int((cur_stats or {}).get("maru", 0) or 0)
        tel = int((cur_stats or {}).get("tel", 0) or 0)
        total = bell + maru + tel
        smooth_fill = (bell + prior_bell) / (total + prior_bell + prior_open) if total > 0 else 0.0
        entries.append({
            "service_date": today,
            "bell": bell,
            "maru": maru,
            "tel": tel,
            "total": total,
            "fill": smooth_fill,
            "weight": 1.0,
        })

    def _calc_window_stats(window):
        subset = entries[:min(int(window), len(entries))]
        if not subset:
            return None, None
        wsum = sum(e["weight"] for e in subset) or 0.0
        fill_sum = sum(e["fill"] * e["weight"] for e in subset)
        total_sum = sum(e["total"] * e["weight"] for e in subset)
        fill_avg = (fill_sum / wsum) if wsum > 0 else None
        volume = total_sum
        return fill_avg, volume

    fill_avg, weighted_total = _calc_window_stats(len(entries))
    if fill_avg is None:
        fill_avg = 0.0
    if weighted_total is None:
        weighted_total = 0.0

    volume_factor = 1.0 - math.exp(-weighted_total / float(total_sat)) if total_sat > 0 else 1.0
    base = float(fill_avg or 0.0)
    big_score = _clamp01(base * volume_factor)

    ma3, _ = _calc_window_stats(3)
    ma14, _ = _calc_window_stats(14)
    ma28, _ = _calc_window_stats(28)
    ma56, _ = _calc_window_stats(56)
    ma84, _ = _calc_window_stats(84)
    ma112, _ = _calc_window_stats(112)

    n_service = len(series)
    n_obs = len(obs_dates)
    old_score, old_detail = _calc_bigdata_score_detail_legacy_v3(cur_stats, hist, cur_stats_by_date=cur_stats_by_date)

    detail = {
        "big_score": big_score,
        "big_score_old": old_score,
        "bd_model_version": _BD_MODEL_NAME,
        "bd_level": base,
        "bd_trust": volume_factor,
        "bd_volume_factor": volume_factor,
        "bd_total_weighted": weighted_total,
        "bd_total_sat": total_sat,
        "bd_half_life": half_life,
        "bd_weight_basis": "days_until",
        "bd_prior_bell": prior_bell,
        "bd_prior_open": prior_open,
        "bd_days": n_service,
        "bd_window": min(max_window, n_service) if n_service else None,
        "bd_service_days": n_service,
        "bd_obs_days": n_obs,
        "ma3": ma3,
        "ma14": ma14,
        "ma28": ma28,
        "ma56": ma56,
        "ma84": ma84,
        "ma112": ma112,
        "unique_dates": [s.get("service_date").isoformat() for s in series if s.get("service_date")],
        "legacy_detail": {
            "bd_level": old_detail.get("bd_level"),
            "bd_trust": old_detail.get("bd_trust"),
            "bd_window": old_detail.get("bd_window"),
            "bd_service_days": old_detail.get("bd_service_days"),
            "bd_obs_days": old_detail.get("bd_obs_days"),
        },
    }
    return big_score, detail

def _calc_rank_score_detail(cur_stats: dict, hist: list, cur_stats_by_date: dict = None):
    """
    ランキング向けスコア（Quality + Momentum）
    - Quality: 長期の埋まり率（bell / (bell+maru+tel)）を減衰平均
    - Momentum: 直近のベル量を減衰 + 飽和で評価
    - 0観測は弱い重みでQualityを下げる方向へ寄与
    """
    params = _get_rank_params()
    min_conf = int(params.get("rank_input_min_confidence", 0) or 0)
    quality_half_life = float(params.get("quality_half_life", 60) or 60)
    momentum_half_life = float(params.get("momentum_half_life", 14) or 14)
    momentum_bell_sat = float(params.get("momentum_bell_sat", 20) or 20)
    prior_bell = float(params.get("quality_prior_bell", 1.0) or 1.0)
    prior_open = float(params.get("quality_prior_open", 1.0) or 1.0)
    quality_lower_z = float(params.get("quality_lower_z", 1.96) or 1.96)
    zero_total_weight = float(params.get("zero_total_weight", 0.35) or 0.35)
    rank_momentum_base = float(params.get("rank_momentum_base", 0.2) or 0.2)
    quality_power = float(params.get("quality_power", 1.0) or 1.0)
    momentum_power = float(params.get("momentum_power", 1.0) or 1.0)
    max_window = int(params.get("rank_max_window", 112) or 112)

    series, obs_dates = _collect_service_date_series(cur_stats, hist, cur_stats_by_date, min_conf, include_zero=True)
    today = datetime.date.today()
    series = series[:max_window]

    entries = []
    quality_success_wsum = 0.0
    quality_total_wsum = 0.0
    for idx, s in enumerate(series):
        bell = int(s.get("bell", 0) or 0)
        maru = int(s.get("maru", 0) or 0)
        tel = int(s.get("tel", 0) or 0)
        total = bell + maru + tel
        obs_date = s.get("obs_date")
        if isinstance(obs_date, datetime.datetime):
            obs_date = obs_date.date()
        if isinstance(obs_date, datetime.date):
            age = (today - obs_date).days
            if age < 0:
                age = 0
        else:
            age = int(idx)

        if total > 0:
            fill = (bell + prior_bell) / (total + prior_bell + prior_open)
            weight = math.exp(-age / quality_half_life) if quality_half_life > 0 else 1.0
            effective_success = bell + prior_bell
            effective_total = total + prior_bell + prior_open
        else:
            fill = 0.0
            weight = (math.exp(-age / quality_half_life) if quality_half_life > 0 else 1.0) * zero_total_weight
            effective_success = 0.0
            effective_total = 0.0

        momentum_weight = math.exp(-age / momentum_half_life) if momentum_half_life > 0 else 1.0

        quality_success_wsum += effective_success * weight
        quality_total_wsum += effective_total * weight
        entries.append({
            "bell": bell,
            "maru": maru,
            "tel": tel,
            "total": total,
            "fill": fill,
            "quality_weight": weight,
            "momentum_weight": momentum_weight,
            "age": age,
        })

    if not entries:
        bell = int((cur_stats or {}).get("bell", 0) or 0)
        maru = int((cur_stats or {}).get("maru", 0) or 0)
        tel = int((cur_stats or {}).get("tel", 0) or 0)
        total = bell + maru + tel
        fill = (bell + prior_bell) / (total + prior_bell + prior_open) if total > 0 else 0.0
        effective_success = (bell + prior_bell) if total > 0 else 0.0
        effective_total = (total + prior_bell + prior_open) if total > 0 else 0.0
        quality_success_wsum += effective_success
        quality_total_wsum += effective_total
        entries.append({
            "bell": bell,
            "maru": maru,
            "tel": tel,
            "total": total,
            "fill": fill,
            "quality_weight": 1.0,
            "momentum_weight": 1.0,
            "age": 0,
        })

    quality_wsum = sum(e["quality_weight"] for e in entries) or 0.0
    quality_num = sum(e["fill"] * e["quality_weight"] for e in entries) if quality_wsum > 0 else 0.0
    quality = (quality_num / quality_wsum) if quality_wsum > 0 else 0.0

    quality_lower = _wilson_lower_bound(quality_success_wsum, quality_total_wsum, quality_lower_z)
    momentum_bell = sum(e["bell"] * e["momentum_weight"] for e in entries)
    momentum = 1.0 - math.exp(-momentum_bell / float(momentum_bell_sat)) if momentum_bell_sat > 0 else 0.0

    quality = _clamp01(quality)
    quality_lower = _clamp01(quality_lower)
    momentum = _clamp01(momentum)

    rank_raw = (quality ** quality_power) * (rank_momentum_base + (1.0 - rank_momentum_base) * (momentum ** momentum_power))
    rank_raw = _clamp01(rank_raw)
    rank_lower = (quality_lower ** quality_power) * (rank_momentum_base + (1.0 - rank_momentum_base) * (momentum ** momentum_power))
    rank_lower = _clamp01(rank_lower)
    if rank_lower > rank_raw:
        rank_lower = rank_raw

    detail = {
        "rank_model_version": _RANK_MODEL_NAME,
        "quality_score": quality,
        "quality_lower_bound": quality_lower,
        "momentum_score": momentum,
        "rank_score_raw": rank_raw,
        "rank_score_lower": rank_lower,
        "quality_half_life": quality_half_life,
        "momentum_half_life": momentum_half_life,
        "momentum_bell_sat": momentum_bell_sat,
        "quality_prior_bell": prior_bell,
        "quality_prior_open": prior_open,
        "quality_lower_z": quality_lower_z,
        "zero_total_weight": zero_total_weight,
        "rank_momentum_base": rank_momentum_base,
        "quality_power": quality_power,
        "momentum_power": momentum_power,
        "rank_max_window": max_window,
        "rank_service_days": len(series),
        "rank_obs_days": len(obs_dates),
        "rank_weighted_bell": momentum_bell,
        "rank_weighted_quality_sum": quality_wsum,
        "rank_weighted_quality_success": quality_success_wsum,
        "rank_weighted_quality_total": quality_total_wsum,
    }
    return rank_raw, detail

def _assign_rank_percentiles(rows: list, score_key: str = "rank_score_raw", percentile_key: str = "rank_percentile"):
    scores = []
    for row in rows or []:
        val = row.get(score_key) if isinstance(row, dict) else None
        if isinstance(val, (int, float)):
            scores.append(float(val))
    if not scores:
        return
    for row in rows or []:
        if not isinstance(row, dict):
            continue
        val = row.get(score_key)
        if isinstance(val, (int, float)):
            row[percentile_key] = _percentile_of(float(val), scores)
        else:
            row[percentile_key] = None
        rank_detail = row.get("rank_detail")
        if isinstance(rank_detail, dict):
            rank_detail[percentile_key] = row.get(percentile_key)

def calc_bigdata_score(cur_score: float, cur_stats: dict, hist: list, cur_stats_by_date: dict = None):
    _, detail = _calc_bigdata_score_detail(cur_stats, hist, cur_stats_by_date=cur_stats_by_date)
    return detail.get("big_score", 0.0)

def build_analytics(all_rows: list, run_ts: str, run_dir: str):
    """
    解析結果を score_data/analytics に保存する（フォルダ内完結）。
    - last_run_summary.json : 今回runの要点
    - global_summary.json   : 履歴全体の概況（軽量）
    """
    ensure_data_dirs()
    def _safe_float(v, default=0.0):
        try:
            return float(v)
        except Exception:
            return default

    # runサマリ（軽量）
    try:
        suspicious = 0
        confs = []
        scrape_healths = []
        signals = []
        scores = []
        bigs = []
        ranks = []
        qualities = []
        lower_bounds = []
        rank_lowers = []
        momenta = []
        for r in all_rows:
            st = r.get("stats", {}) or {}
            if st.get("suspicious_hit"):
                suspicious += 1
            confs.append(int(r.get("site_confidence", 0) or 0))
            scrape_healths.append(int(r.get("scrape_health", r.get("site_confidence", 0)) or 0))
            sv = r.get("signal_strength")
            if isinstance(sv, (int, float)):
                signals.append(float(sv))
            scores.append(float(r.get("score", 0) or 0))
            bigs.append(float(r.get("big_score", r.get("score",0)) or 0))
            rank_raw = r.get("rank_score_raw")
            if isinstance(rank_raw, (int, float)):
                ranks.append(float(rank_raw))
            qv = r.get("quality_score")
            if isinstance(qv, (int, float)):
                qualities.append(float(qv))
            qlb = r.get("quality_lower_bound")
            if isinstance(qlb, (int, float)):
                lower_bounds.append(float(qlb))
            mv = r.get("momentum_score")
            if isinstance(mv, (int, float)):
                momenta.append(float(mv))
            rl = r.get("rank_score_lower")
            if isinstance(rl, (int, float)):
                rank_lowers.append(float(rl))

        rows = all_rows if isinstance(all_rows, list) else []
        sorted_rows = sorted(
            rows,
            key=lambda r: (
                -_safe_float(r.get("score")),
                -_safe_float(r.get("delta")),
                str(r.get("name") or ""),
            ),
        )
        summary = {
            "run_ts": run_ts,
            "run_dir": os.path.basename(run_dir or ""),
            "rows": len(rows),
            "suspicious_rows": suspicious,
            "avg_confidence": (sum(confs) / len(confs)) if confs else None,
            "avg_scrape_health": (sum(scrape_healths) / len(scrape_healths)) if scrape_healths else None,
            "avg_signal_strength": (sum(signals) / len(signals)) if signals else None,
            "avg_score": (sum(scores) / len(scores)) if scores else None,
            "avg_big_score": (sum(bigs) / len(bigs)) if bigs else None,
            "avg_rank_score_raw": (sum(ranks) / len(ranks)) if ranks else None,
            "avg_quality_score": (sum(qualities) / len(qualities)) if qualities else None,
            "avg_quality_lower_bound": (sum(lower_bounds) / len(lower_bounds)) if lower_bounds else None,
            "avg_momentum_score": (sum(momenta) / len(momenta)) if momenta else None,
            "avg_rank_score_lower": (sum(rank_lowers) / len(rank_lowers)) if rank_lowers else None,
            "top10": [
                {
                    "gid": r.get("gid"),
                    "name": r.get("name"),
                    "score": r.get("score"),
                    "big_score": r.get("big_score"),
                    "rank_score_raw": r.get("rank_score_raw"),
                    "rank_score_lower": r.get("rank_score_lower"),
                    "rank_percentile": r.get("rank_percentile"),
                    "rank_percentile_lower": r.get("rank_percentile_lower"),
                    "quality_score": r.get("quality_score"),
                    "quality_lower_bound": r.get("quality_lower_bound"),
                    "momentum_score": r.get("momentum_score"),
                    "delta": r.get("delta"),
                    "conf": r.get("site_confidence"),
                    "scrape_health": r.get("scrape_health", r.get("site_confidence")),
                    "signal_strength": r.get("signal_strength"),
                    "bell": (r.get("stats",{}) or {}).get("bell"),
                    "maru": (r.get("stats",{}) or {}).get("maru"),
                    "tel": (r.get("stats",{}) or {}).get("tel"),
                }
                for r in sorted_rows[:10]
            ],
        }
        _atomic_write_json(os.path.join(ANALYTICS_DIR, "last_run_summary.json"), summary)
        # run_dir側にも同じものを置く（後でrunだけ見ても分かる）
        write_run_file(run_dir, "analytics_summary.json", summary)
    except Exception:
        pass

    # global（軽量）：履歴ファイル数など（※重くならないよう行数は数えない）
    try:
        gids = []
        total_bytes = 0
        for fn in os.listdir(HISTORY_DIR):
            if fn.endswith(".jsonl"):
                gids.append(fn[:-5])
                try:
                    total_bytes += os.path.getsize(os.path.join(HISTORY_DIR, fn))
                except Exception:
                    pass

        global_sum = {
            "updated": _now_ts(),
            "history_files": len(gids),
            "history_total_bytes": total_bytes,
            "runs_dir": os.path.basename(RUNS_DIR),
        }
        _atomic_write_json(os.path.join(ANALYTICS_DIR, "global_summary.json"), global_sum)
    except Exception:
        pass

# -------------------------
# プリセット管理（JSON）
# -------------------------
def load_presets():
    if not os.path.exists(PRESETS_FILE):
        data = {"presets": [{"name": "サンプル1", "url": "https://www.cityheaven.net/ibaraki/A0802/A080202/mirror/girllist/?lo=1", "max": 100}], "queue_selected": []}
        save_presets(data)
        return data
    try:
        with open(PRESETS_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
            data.setdefault("presets", [])
            data.setdefault("queue_selected", [])
            return data
    except Exception:
        data = {"presets": [], "queue_selected": []}
        save_presets(data)
        return data

def save_presets(data):
    with open(PRESETS_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

# -------------------------
# Shared dataclass
# -------------------------
@dataclass
class Job:
    name: str
    url: str
    max_items: int

def build_jobs_from_presets(preset_names=None, presets_data=None):
    preset_names = preset_names or []
    preset_names = [x.strip() for x in preset_names if x and str(x).strip()]

    if presets_data is None:
        presets_data = load_presets()

    # load_presets() returns a dict: {"presets":[...], "queue_selected":[...]}.
    # Normalize to a list[dict] for auto mode, and be tolerant to older formats.
    if isinstance(presets_data, dict):
        presets = presets_data.get("presets", [])
    else:
        presets = presets_data

    # If presets somehow becomes a dict (e.g., name->url), convert to list[dict]
    if isinstance(presets, dict):
        tmp = []
        for k, v in presets.items():
            if isinstance(v, dict):
                d = dict(v)
                d.setdefault("name", str(k))
            else:
                d = {"name": str(k), "url": str(v)}
            tmp.append(d)
        presets = tmp

    # Filter only dict entries
    if not isinstance(presets, list):
        presets = []
    presets = [x for x in presets if isinstance(x, dict)]

    raw_set = set(preset_names)
    norm_set = set([normalize_preset_name(x) for x in preset_names])

    def _match_preset_name(name: str) -> bool:
        n = str(name or "")
        nn = normalize_preset_name(n)
        return (n in raw_set) or (nn in raw_set) or (nn in norm_set)

    if preset_names:
        targets = [x for x in presets if _match_preset_name(x.get("name", ""))]
    else:
        # default: respect queue_selected if present
        qsel = []
        if isinstance(presets_data, dict):
            qsel = presets_data.get("queue_selected", []) or []
        if qsel:
            qset = set([str(x) for x in qsel])
            qnorm = set([normalize_preset_name(x) for x in qsel])

            def _match_queue(name: str) -> bool:
                n = str(name or "")
                nn = normalize_preset_name(n)
                return (n in qset) or (nn in qset) or (nn in qnorm)

            targets = [x for x in presets if _match_queue(x.get("name", ""))]
        else:
            targets = presets

    jobs = []
    for p in targets:
        try:
            mx = p.get("max", None)
            if mx is None:
                mx = p.get("max_items", None)
            if mx is None:
                mx = 60
            jobs.append(Job(name=p.get("name", ""), url=p.get("url", ""), max_items=int(mx or 60)))
        except Exception as e:
            log_event("ERR", "preset parse failed", preset=p, err=str(e)[:200])

    if preset_names and not jobs:
        log_event("WARN", "no matched presets", requested=preset_names)

    return jobs

# ============================================================
# 追加: 1日1回の自動スナップショット + 安全なasync（同時2〜3）
#  - GUIは従来通り（sync）
#  - 自動実行はCLI(--auto)で headless + async を使う
# ============================================================

import argparse
import asyncio
import subprocess

def load_config():
    """
    score_data/config.json があれば読む。なければデフォルトを返す。
    （フォルダ内完結）
    """
    ensure_data_dirs()
    default = {
        "auto": {
            "presets": "",
            "headful": False,
            "concurrency": 3,
            "min_nav_interval_ms": 650,
            "force_overwrite_today": False,
            "once_per_day": True,
            "minimize_browser": False,
        },
        "notify": {
            "enabled": True,
            "min_confidence": 50,
            "top_n": 5
        },
        "score": {
            "bell_sat": 18,
            "bd_day_sat": 14,
            "trust_day_sat": 18,
            "bd_input_min_confidence": 0
        },
        "rank": {
            "quality_half_life": 60,
            "momentum_half_life": 14,
            "momentum_bell_sat": 20,
            "quality_prior_bell": 1.0,
            "quality_prior_open": 1.0,
            "quality_lower_z": 1.96,
            "zero_total_weight": 0.35,
            "rank_momentum_base": 0.2,
            "quality_power": 1.0,
            "momentum_power": 1.0,
            "rank_max_window": 112,
            "rank_input_min_confidence": 0,
            "rank_sort_mode": "raw"
        },
        "retention": {
            "months": 6,
            "max_lines": 200
        },
        "debug": {
            "suspicious": {
                "enabled": True,
                "max_per_run": _SUSPICIOUS_DEFAULT_MAX_PER_RUN,
                "max_per_preset": _SUSPICIOUS_DEFAULT_MAX_PER_PRESET,
                "max_html_bytes": _SUSPICIOUS_DEFAULT_MAX_HTML_BYTES,
                "full_page": False
            }
        }
    }
    try:
        if os.path.exists(CONFIG_PATH):
            with open(CONFIG_PATH, "r", encoding="utf-8") as r:
                userc = json.load(r) or {}
            for k, v in userc.items():
                if isinstance(v, dict) and isinstance(default.get(k), dict):
                    default[k].update(v)
                else:
                    default[k] = v
        else:
            with open(CONFIG_PATH, "w", encoding="utf-8") as w:
                json.dump(default, w, ensure_ascii=False, indent=2)
    except Exception:
        pass
    return default

def save_config(cfg: dict) -> None:
    ensure_data_dirs()
    try:
        with open(CONFIG_PATH, "w", encoding="utf-8") as w:
            json.dump(cfg or {}, w, ensure_ascii=False, indent=2)
    except Exception:
        pass

def _ps_escape(s: str) -> str:
    if s is None:
        return ""
    return str(s).replace('"', '`"')

def notify_windows(title: str, body: str):
    """
    Windows標準(.NET)で軽量通知（追加インストール不要）
    ※ タスクスケジューラは「ユーザーがログオンしている時」にしておくと確実
    """
    ensure_data_dirs()
    try:
        fn = os.path.join(NOTIFY_DIR, f"notify_{_now_ts()}.txt")
        with open(fn, "w", encoding="utf-8") as w:
            w.write((title or "") + "\n" + (body or "") + "\n")
    except Exception:
        pass

    try:
        t = _ps_escape(title)[:70]
        b = _ps_escape(body)[:2200]
        ps = f'''
Add-Type -AssemblyName System.Windows.Forms
Add-Type -AssemblyName System.Drawing
$ni = New-Object System.Windows.Forms.NotifyIcon
$ni.Icon = [System.Drawing.SystemIcons]::Information
$ni.BalloonTipTitle = "{t}"
$ni.BalloonTipText  = "{b}"
$ni.Visible = $true
$ni.ShowBalloonTip(6000)
Start-Sleep -Seconds 7
$ni.Dispose()
'''
        subprocess.run(
            ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command", ps],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False
        )
    except Exception:
        pass

class AsyncNavLimiter:
    """
    全体で「goto間隔」を守る（bot対策の安全弁）
    """
    def __init__(self, min_interval_ms: int = 650):
        self.min_interval = max(0.05, float(min_interval_ms) / 1000.0)
        self._lock = asyncio.Lock()
        self._t_last = 0.0

    async def wait_turn(self):
        async with self._lock:
            now = time.perf_counter()
            dt = now - self._t_last
            if dt < self.min_interval:
                await asyncio.sleep(self.min_interval - dt)
            self._t_last = time.perf_counter()

async def async_goto_retry(page, url: str, wait_until="domcontentloaded", tries=2, preset: str = None, gid: str = None):
    if not _valid_url(url):
        if _detail_log_enabled():
            log_event(
                "INFO",
                "skip invalid url",
                preset=preset,
                gid=gid,
                url=(url or ""),
            )
            _detail_log_skip(preset, gid, "invalid_url")
        return False
    for _ in range(tries):
        try:
            await page.goto(url, wait_until=wait_until, timeout=NAV_TIMEOUT_MS)
            return True
        except Exception:
            try:
                await page.wait_for_timeout(700)
                await page.reload(wait_until=wait_until, timeout=NAV_TIMEOUT_MS)
            except Exception:
                pass
    return False



async def _maybe_accept_interstitial(page):
    """年齢確認/同意/クッキー同意などの中間画面をできるだけ自動で抜ける。
    何もしなければ False、クリック等を行えば True を返す。
    """
    try:
        cur = page.url or ""
    except Exception:
        cur = ""
    # まずページ内テキストの軽いチェック
    try:
        body_txt = await page.locator("body").inner_text(timeout=1500)
    except Exception:
        body_txt = ""

    keywords = ["年齢確認", "18歳", "同意", "利用規約", "Cookie", "クッキー"]
    if not any(k in (body_txt or "") for k in keywords) and all(x not in cur for x in ["Age", "age", "consent"]):
        return False

    # クリック候補（順序が大事）
    labels = ["同意する", "同意", "はい", "OK", "確認", "入場", "ENTER", "Enter", "I Agree", "AGREE", "Accept"]
    selectors = []
    for t in labels:
        selectors += [
            f'button:has-text("{t}")',
            f'a:has-text("{t}")',
            f'input[type="submit"][value*="{t}"]',
            f'input[type="button"][value*="{t}"]',
        ]

    for sel in selectors:
        try:
            loc = page.locator(sel)
            if await loc.count() <= 0:
                continue
            el = loc.first
            # visible でなければスキップ
            try:
                if not await el.is_visible():
                    continue
            except Exception:
                pass

            await el.click(timeout=1500)
            # 遷移する/しない両方あるので軽く待つ
            try:
                await page.wait_for_load_state("domcontentloaded", timeout=8000)
            except Exception:
                pass
            await page.wait_for_timeout(800)
            return True
        except Exception:
            continue

    return False


async def _wait_list_ready(page):
    """girllist の一覧がDOMに出るのを少し待つ（headlessで間に合わない対策）"""
    # networkidle はサイトによって終わらないことがあるので例外は無視
    try:
        await page.wait_for_load_state("networkidle", timeout=8000)
    except Exception:
        pass
    try:
        await page.wait_for_selector('a[href*="girlid-"]', timeout=8000)
    except Exception:
        pass
async def async_get_next_list_url(page):
    try:
        loc = page.locator("a.next")
        if await loc.count() == 0:
            return None
        href = await loc.first.get_attribute("href")
        if not href:
            return None
        return abs_url_from_href(page.url, href)
    except Exception:
        return None

async def async_collect_girls_from_list(page, need: int):
    js = r"""
(() => {
  const out = [];
  const seen = new Set();
  const cleanName = (s) => {
    if (!s) return "";
    s = s.replace(/（\s*\d+\s*歳\s*）/g, "");
    s = s.replace(/\s+/g, " ").trim();
    if (s.length > 30) s = s.slice(0, 30).trim();
    return s;
  };

  function pushFromAnchor(a){
    if (!a) return;
    const href = a.getAttribute("href") || a.href || "";
    const m = href.match(/girlid-(\d+)/);
    if (!m) return;
    const gid = m[1];
    if (seen.has(gid)) return;

    let name = "";
    const strongs = a.querySelectorAll("strong");
    if (strongs && strongs.length) name = (strongs[0].innerText || "").trim();
    if (!name) name = cleanName(a.innerText || "");
    if (!name) {
      const p = a.closest("td,div,li");
      if (p) name = cleanName(p.innerText || "");
    }
    if (!name) name = "（名前不明）";

    seen.add(gid);
    out.push([gid, name]);
  }

  document.querySelectorAll("td.second a.profileLink").forEach(a => pushFromAnchor(a));
  if (out.length < 3) {
    document.querySelectorAll("a[href*='girlid-']").forEach(a => pushFromAnchor(a));
  }

  return out;
})()
"""
    try:
        items = await page.evaluate(js) or []
        return items[:need]
    except Exception:
        return []


def _extract_girlids_from_html(html: str):
    # fallback: HTML中の girlid-12345 を拾う
    try:
        ids = re.findall(r"girlid-(\d+)", html or "")
        out = []
        seen = set()
        for x in ids:
            if x not in seen:
                seen.add(x)
                out.append(x)
        return out
    except Exception:
        return []

async def _dump_list_debug_async(page, preset_name: str, list_url: str, note: str = ""):
    # no girl_ids などの時にHTML/スクショを run_dir/jobs に落とす（自動実行の再現性アップ）
    try:
        run_dir = _CURRENT_RUN_DIR or ""
        if not run_dir:
            return
        jobs_dir = os.path.join(run_dir, "jobs")
        os.makedirs(jobs_dir, exist_ok=True)
        safe = _safe_name(preset_name or "preset")
        base = os.path.join(jobs_dir, f"{safe}_LIST_FAIL")

        # info
        try:
            title = await page.title()
        except Exception:
            title = ""
        try:
            cur_url = page.url
        except Exception:
            cur_url = ""

        info_path = base + ".txt"
        with open(info_path, "w", encoding="utf-8") as f:
            f.write(f"preset={preset_name}\n")
            f.write(f"list_url={list_url}\n")
            f.write(f"page.url={cur_url}\n")
            f.write(f"title={title}\n")
            if note:
                f.write(f"note={note}\n")

        # html
        try:
            html = await page.content()
        except Exception:
            html = ""
        if html:
            with open(base + ".html", "w", encoding="utf-8") as f:
                f.write(html)

        # screenshot（失敗してもOK）
        try:
            await page.screenshot(path=base + ".png", full_page=True)
        except Exception:
            pass
    except Exception:
        pass


async def _count_calendar_stats_by_slots_async_core(page, max_wait_ms: int = None, preset: str = None, gid: str = None):
    waited = 0
    if max_wait_ms is None:
        max_wait_ms = min(CAL_WAIT_MS, CAL_WAIT_LONG_MS)
    fast_wait_ms = 500
    slow_step_ms = CAL_WAIT_STEP_MS
    slow_mode = False
    stable_attempts = 0
    light_stable_hits = 0
    light_stable_required = 2
    last_light_sig = None
    last_stats = None
    last_frame = None
    last_invalid_stats = None
    last_invalid_reason = None
    last_frame_url = ""
    attempt = 0
    sanity_retry_count = 0
    sanity_last_reason = None
    frame_names = []
    frame_urls = []
    suspicious_dumped = False
    while waited <= max_wait_ms:
        fr = None
        frame_url = ""
        try:
            fr = page.frame(name="pcreserveiframe")
            if fr:
                frame_url = fr.url or ""
                # 例外（予約不可）ページは iframe 内で表示されることがある（例: /error/ や EFRESV、div.error-msg）
                try:
                    if frame_url and ("yoyaku.cityheaven.net/error" in frame_url or "/error/" in frame_url or "EFRESV" in frame_url):
                        return {"ok": False, "reason": "not_reservable(frame_url_error)", "frame_url": frame_url}, frame_url
                except Exception:
                    pass
                try:
                    err_in_fr = await fr.query_selector("div.error-msg")
                except Exception:
                    err_in_fr = None
                if err_in_fr:
                    try:
                        msg = await fr.locator("div.error-msg").first.inner_text(timeout=300)
                    except Exception:
                        try:
                            msg = (await err_in_fr.text_content()) or ""
                        except Exception:
                            msg = ""
                    msg = (msg or "").strip()
                    if "予約できません" in msg:
                        return {"ok": False, "reason": "not_reservable(error_msg_in_frame)", "frame_url": frame_url, "msg": msg[:160]}, frame_url

        except Exception:
            fr = None

        # 1.5) 全iframeを走査（src/name/id）: topだけでなく全フレーム内も走査
        if not fr:
            sel = "iframe[name='pcreserveiframe'], iframe#pcreserveiframe, iframe[src*='yoyaku.cityheaven.net'], iframe[src*='A6ShopReservation'], iframe[src*='ShopReservation']"
            for host_fr in page.frames:
                try:
                    iframes = await host_fr.query_selector_all(sel)
                except Exception:
                    iframes = []
                for ih in iframes or []:
                    try:
                        src = (await ih.get_attribute("src") or "")
                        nm = (await ih.get_attribute("name") or "")
                        iid = (await ih.get_attribute("id") or "")
                        if ("/error/" in src) or ("EFRESV" in src) or ("yoyaku.cityheaven.net/error" in src):
                            return {"ok": False, "reason": "not_reservable(iframe_src_error)", "iframe_src": src}, src
                        hay = f"{src} {nm} {iid}"
                    except Exception:
                        continue
                    if ("yoyaku.cityheaven.net" in hay) or ("A6ShopReservation" in hay) or ("ShopReservation" in hay) or ("calendar" in src and "cityheaven" in src):
                        try:
                            fr = await ih.content_frame()
                        except Exception:
                            fr = None
                        if fr:
                            break
                if fr:
                    break

# 2) frame URL から拾う（calendar / error）
        if not fr:
            try:
                frames = [f for f in page.frames if "yoyaku.cityheaven.net/" in (f.url or "")]
                if frames:
                    def _rank(u: str):
                        u = u or ""
                        if "yoyaku.cityheaven.net/calendar" in u:
                            return 0
                        if "yoyaku.cityheaven.net/error" in u:
                            return 1
                        return 2
                    frames.sort(key=lambda f: _rank(f.url or ""))
                    fr = frames[0]
                    try:
                        frame_url = fr.url or ""
                    except Exception:
                        frame_url = ""
            except Exception:
                fr = None
        if _detail_log_enabled():
            try:
                frame_names = []
                frame_urls = []
                for f in page.frames:
                    frame_names.append(f.name or "")
                    u = (f.url or "")
                    if u:
                        frame_urls.append(u[:200])
                log_event(
                    "DBG",
                    "calendar iframe probe",
                    frame_url=frame_url,
                    frame_names=frame_names[:20],
                    frame_urls=frame_urls[:20],
                )
            except Exception:
                pass

        # 3.5) iframe枠だけ存在して src が空のまま（=結局ロードされない）場合は、無駄な待ちを避けて早期終了
        if not fr:
            try:
                ih0 = await page.query_selector("iframe[name='pcreserveiframe'], iframe#pcreserveiframe")
            except Exception:
                ih0 = None
            if ih0:
                try:
                    src0 = (await ih0.get_attribute("src") or "").strip()
                except Exception:
                    src0 = ""
                if ("/error/" in src0) or ("EFRESV" in src0) or ("yoyaku.cityheaven.net/error" in src0):
                    return {"ok": False, "reason": "not_reservable(iframe_src_error)", "iframe_src": src0}, src0
                if (not src0):
                    try:
                        has_yoyaku = any("yoyaku.cityheaven.net/" in (f.url or "") for f in page.frames)
                    except Exception:
                        has_yoyaku = False
                    if (not has_yoyaku) and (waited >= min(4000, max_wait_ms)):
                        return {"ok": False, "reason": "calendar_not_present(iframe_no_src)"}, None


        if fr:
            try:
                attempt += 1
                try:
                    await fr.wait_for_load_state("domcontentloaded", timeout=2500)
                except Exception:
                    pass
                try:
                    await fr.wait_for_selector("table, td", timeout=fast_wait_ms)
                except Exception:
                    if _detail_log_enabled():
                        log_event(
                            "DBG",
                            "calendar retry",
                            frame_url=frame_url,
                            td_count=0,
                            time_rows=0,
                            signature=None,
                            attempt=attempt,
                            reason="td_wait_timeout",
                        )
                    slow_mode = True
                    skip, why = await _is_not_reservable_page_async(page)
                    if skip:
                        return {"ok": False, "reason": f"not_reservable({why})"}, None
                    await page.wait_for_timeout(slow_step_ms)
                    waited += slow_step_ms
                    continue
                light = await fr.evaluate(r"""
(() => {
  const timeRe = /^\s*\d{1,2}:\d{2}/;
  const tables = Array.from(document.querySelectorAll("table"));
  if (!tables.length) return { ok:false, reason:"no table" };

  let table = null;
  for (const t of tables) {
    const rows = Array.from(t.querySelectorAll("tr"));
    if (rows.some(r => timeRe.test((r.querySelector("td,th")?.innerText || "").trim()))) { table = t; break; }
  }
  table = table || tables[0];

  const rows = Array.from(table.querySelectorAll("tr"));
  const dataRows = rows.filter(r => {
    const first = r.querySelector("td,th");
    if (!first) return false;
    return timeRe.test(first.innerText.trim());
  });
  if (!dataRows.length) return { ok:false, reason:"no time rows" };

  let maxCols = 0;
  let tdCount = 0;
  for (const r of dataRows) {
    const tds = Array.from(r.children).filter(el => el.tagName === "TD" || el.tagName === "TH").slice(1);
    let cols = 0;
    tdCount += tds.length;
    for (const td of tds) {
      const cs = parseInt(td.getAttribute("colspan") || "1", 10) || 1;
      cols += cs;
    }
    if (cols > maxCols) maxCols = cols;
  }
  if (maxCols <= 0) return { ok:false, reason:"no cols" };

  return { ok:true, time_rows:dataRows.length, td_count:tdCount, max_cols:maxCols };
})()
""")
                if not (isinstance(light, dict) and light.get("ok")):
                    if _detail_log_enabled():
                        log_event(
                            "DBG",
                            "calendar retry",
                            frame_url=frame_url,
                            td_count=(light or {}).get("td_count", 0) if isinstance(light, dict) else 0,
                            time_rows=(light or {}).get("time_rows", 0) if isinstance(light, dict) else 0,
                            signature=None,
                            attempt=attempt,
                            reason=(light or {}).get("reason", "light_probe_failed") if isinstance(light, dict) else "light_probe_failed",
                        )
                    slow_mode = True
                    skip, why = await _is_not_reservable_page_async(page)
                    if skip:
                        return {"ok": False, "reason": f"not_reservable({why})"}, None
                    await page.wait_for_timeout(slow_step_ms)
                    waited += slow_step_ms
                    continue
                stable_attempts += 1
                light_sig = (light.get("td_count", 0), light.get("time_rows", 0), light.get("max_cols", 0))
                if light_sig == last_light_sig:
                    light_stable_hits += 1
                else:
                    light_stable_hits = 1
                last_light_sig = light_sig
                if _detail_log_enabled():
                    log_event(
                        "DBG",
                        "calendar stabilize probe",
                        attempt=stable_attempts,
                        frame_url=frame_url,
                        td_count=light.get("td_count", 0),
                        time_rows=light.get("time_rows", 0),
                        max_cols=light.get("max_cols", 0),
                        signature=light_sig,
                        stable_hits=light_stable_hits,
                        stable_required=light_stable_required,
                    )
                if light_stable_hits < light_stable_required:
                    if not slow_mode:
                        await page.wait_for_timeout(200)
                        waited += 200
                        continue
                    skip, why = await _is_not_reservable_page_async(page)
                    if skip:
                        return {"ok": False, "reason": f"not_reservable({why})"}, None
                    await page.wait_for_timeout(slow_step_ms)
                    waited += slow_step_ms
                    continue

                stats = await fr.evaluate(r"""
(() => {
  const timeRe = /^\s*\d{1,2}:\d{2}/;
  const tables = Array.from(document.querySelectorAll("table"));
  if (!tables.length) return { ok:false, reason:"no table" };

  let table = null;
  for (const t of tables) {
    const rows = Array.from(t.querySelectorAll("tr"));
    if (rows.some(r => timeRe.test((r.querySelector("td,th")?.innerText || "").trim()))) { table = t; break; }
  }
  table = table || tables[0];

  const rows = Array.from(table.querySelectorAll("tr"));
  const dataRows = rows.filter(r => {
    const first = r.querySelector("td,th");
    if (!first) return false;
    return timeRe.test(first.innerText.trim());
  });
  if (!dataRows.length) return { ok:false, reason:"no time rows" };

  let maxCols = 0;
  for (const r of dataRows) {
    const tds = Array.from(r.children).filter(el => el.tagName === "TD" || el.tagName === "TH").slice(1);
    let cols = 0;
    for (const td of tds) {
      const cs = parseInt(td.getAttribute("colspan") || "1", 10) || 1;
      cols += cs;
    }
    if (cols > maxCols) maxCols = cols;
  }
  if (maxCols <= 0) return { ok:false, reason:"no cols" };

  const firstDataRowIndex = rows.findIndex(r => dataRows.includes(r));
  const headerRows = firstDataRowIndex > 0 ? rows.slice(0, firstDataRowIndex) : [];
  const now = new Date();
  function pad2(n) { return String(n).padStart(2, "0"); }
  function findBaseYearMonth() {
    let year = now.getFullYear();
    let month = now.getMonth() + 1;
    const bodyText = (document.body && document.body.innerText) ? document.body.innerText : "";
    let m = bodyText.match(/(\d{4})\s*[年\/\.]\s*(\d{1,2})\s*月/);
    if (m) {
      year = parseInt(m[1], 10) || year;
      month = parseInt(m[2], 10) || month;
      return { year, month };
    }
    m = bodyText.match(/(\d{1,2})\s*月/);
    if (m) {
      month = parseInt(m[1], 10) || month;
    }
    return { year, month };
  }
  const baseYM = findBaseYearMonth();
  function parseDateText(txt) {
    if (!txt) return null;
    const clean = String(txt).replace(/\s+/g, "");
    let month = null;
    let day = null;
    let m = clean.match(/(\d{1,2})[\/\.](\d{1,2})/);
    if (m) {
      month = parseInt(m[1], 10);
      day = parseInt(m[2], 10);
    } else {
      m = clean.match(/(\d{1,2})月(\d{1,2})/);
      if (m) {
        month = parseInt(m[1], 10);
        day = parseInt(m[2], 10);
      } else {
        m = clean.match(/(\d{1,2})\([月火水木金土日]\)/);
        if (m) {
          day = parseInt(m[1], 10);
        } else {
          m = clean.match(/(\d{1,2})日/);
          if (m) day = parseInt(m[1], 10);
        }
      }
    }
    if (!day || day < 1 || day > 31) return null;
    if (!month || month < 1 || month > 12) month = baseYM.month;
    if (!month) return null;
    let year = baseYM.year;
    if (baseYM.month === 12 && month === 1) year += 1;
    if (baseYM.month === 1 && month === 12) year -= 1;
    return `${year}-${pad2(month)}-${pad2(day)}`;
  }
  let headerRow = null;
  let headerMatches = 0;
  for (const r of headerRows) {
    const cells = Array.from(r.children).filter(el => el.tagName === "TD" || el.tagName === "TH").slice(1);
    let hits = 0;
    for (const td of cells) {
      if (parseDateText(td.innerText || "")) hits += 1;
    }
    if (hits > headerMatches) {
      headerMatches = hits;
      headerRow = r;
    }
  }
  const columnDateKeys = Array(maxCols).fill(null);
  if (headerRow && headerMatches > 0) {
    const cells = Array.from(headerRow.children).filter(el => el.tagName === "TD" || el.tagName === "TH").slice(1);
    let col = 0;
    for (const td of cells) {
      const cs = parseInt(td.getAttribute("colspan") || "1", 10) || 1;
      const dateKey = parseDateText(td.innerText || "");
      for (let cc = col; cc < Math.min(maxCols, col + cs); cc++) {
        if (dateKey) columnDateKeys[cc] = dateKey;
      }
      col += cs;
      if (col >= maxCols) break;
    }
  }
  const headerDateKeySet = new Set();
  for (const key of columnDateKeys) {
    if (key) headerDateKeySet.add(key);
  }
  const headerDateCount = headerDateKeySet.size;

  const rowCount = dataRows.length;
  const grid = Array.from({ length: rowCount }, () => Array(maxCols).fill(null));
  const out = { ok:true, bell:0, maru:0, tel:0, dash:0, other:0, total_slots:0, bookable_slots:0, excluded_slots:0, time_rows:rowCount, max_cols:maxCols, td_count:0, header_dates: headerDateCount, slots_unique: 0, symbols:{} };
  const statsByDate = {};
  function ensureDateKey(key) {
    if (!statsByDate[key]) statsByDate[key] = { bell:0, maru:0, tel:0, other:0 };
    return statsByDate[key];
  }

  function cellType(td){
    const rawTxt = (td.innerText || "").trim();
    const msg = (td.getAttribute("data-name_message") || "").trim();
    const txt = rawTxt;
    const dn = (td.getAttribute("data-name") || "").trim().toUpperCase();
    const dataMark = (td.getAttribute("data-mark") || "").trim().toUpperCase();
    const dataStatus = (td.getAttribute("data-status") || "").trim().toUpperCase();
    const aria = (td.getAttribute("aria-label") || "").trim().toUpperCase();
    const title = (td.getAttribute("title") || "").trim().toUpperCase();
    const cls = (td.className || "").toString().toUpperCase();
    const combined = `${rawTxt} ${msg}`;
    if (combined.includes("お電話にてお問い合わせください")) return "excluded_notice_big";
    if (dn === "TEL" || dataStatus.includes("TEL") || txt.toUpperCase() === "TEL" || aria.includes("TEL") || title.includes("TEL") || cls.includes("TEL") || cls.includes("PHONE")) return "tel";
    if (txt === "×" || txt === "✕" || txt === "✖" || txt.includes("不可") || aria.includes("×") || title.includes("×") || cls.includes("NG")) return "bell";
    if (rawTxt.includes("〇先行") || rawTxt.includes("○先行")) return "maru";
    if (dataMark === "○" || dataMark === "MARU" || td.querySelector("span[data-mark='○']") || txt === "○" || txt === "〇" || cls.includes("MARU") || cls.includes("CIRCLE") || cls.includes("OK") || aria.includes("○")) return "maru";
    if (cls.includes("BELL") || cls.includes("CROSS") || dataStatus.includes("NG")) return "bell";
    const bg = (getComputedStyle(td).backgroundImage || "").toLowerCase();
    if (bg.includes("bell") || bg.includes("cross") || bg.includes("ng")) return "bell";
    const img = td.querySelector("img");
    if (img) {
      const alt = (img.getAttribute("alt") || "").toLowerCase();
      const titleImg = (img.getAttribute("title") || "").toLowerCase();
      const ariaImg = (img.getAttribute("aria-label") || "").toLowerCase();
      const src = (img.getAttribute("src") || "").toLowerCase();
      const imgCls = (img.className || "").toString().toLowerCase();
      const imgData = (img.getAttribute("data-name") || "").toLowerCase();
      if (alt.includes("bell") || titleImg.includes("bell") || ariaImg.includes("bell") || src.includes("bell") || imgCls.includes("bell") || imgData.includes("bell")) return "bell";
      if (alt.includes("cross") || titleImg.includes("cross") || ariaImg.includes("cross") || src.includes("cross") || imgCls.includes("cross") || imgData.includes("cross")) return "bell";
      if (alt.includes("tel") || titleImg.includes("tel") || ariaImg.includes("tel") || src.includes("tel") || imgCls.includes("tel") || imgData.includes("tel")) return "tel";
    }
    if (txt === "―" || txt === "‐" || txt === "-" || txt === "–" || txt === "—" || txt === "ー") return "dash";
    return "other";
  }

  function countSlot(r, c, t) {
    if (grid[r][c]) return;
    grid[r][c] = t;
    if (t === "excluded_notice_big") {
      out.excluded_slots += 1;
      const dateKey = columnDateKeys[c];
      if (dateKey) {
        const dst = ensureDateKey(dateKey);
        dst.other += 1;
      }
      return;
    }
    out[t] = (out[t] || 0) + 1;
    out.total_slots += 1;
    if (t === "bell" || t === "maru" || t === "tel") out.bookable_slots += 1;
    const dateKey = columnDateKeys[c];
    if (dateKey) {
      const dst = ensureDateKey(dateKey);
      if (t === "bell" || t === "maru" || t === "tel") {
        dst[t] += 1;
      } else {
        dst.other += 1;
      }
    }
  }

  for (let r = 0; r < rowCount; r++) {
    const cells = Array.from(dataRows[r].children).filter(el => el.tagName === "TD" || el.tagName === "TH").slice(1);
    let col = 0;
    for (const td of cells) {
      out.td_count += 1;
      while (col < maxCols && grid[r][col]) col++;
      if (col >= maxCols) break;

      const t = cellType(td);
      const rs = parseInt(td.getAttribute("rowspan") || "1", 10) || 1;
      const cs = parseInt(td.getAttribute("colspan") || "1", 10) || 1;
      const rLimit = Math.min(rowCount, r + rs);
      const cLimit = Math.min(maxCols, col + cs);
      for (let rr = r; rr < rLimit; rr++) {
        for (let cc = col; cc < cLimit; cc++) {
          countSlot(rr, cc, t);
        }
      }

      const key = (td.innerText || "").trim() || t;
      const slots = (rLimit - r) * (cLimit - col);
      out.symbols[key] = (out.symbols[key] || 0) + slots;

      col += cs;
    }
  }

  let uniqueSlots = 0;
  for (let r = 0; r < rowCount; r++) {
    for (let c = 0; c < maxCols; c++) {
      if (grid[r][c] && grid[r][c] !== "excluded_notice_big") uniqueSlots += 1;
    }
  }
  out.slots_unique = uniqueSlots;
  const denom = out.total_slots || 0;
  out.other_ratio = denom > 0 ? (out.other / denom) : null;
  out.bell_rate_total = out.total_slots ? (out.bell / out.total_slots) : null;
  out.bell_rate_bookable = out.bookable_slots ? (out.bell / out.bookable_slots) : null;
  out.stats_by_date = statsByDate;

  return out;
})()
""")
                suspicious_hit = False
                suspicious_meta = None
                page_html = ""
                frame_html = ""
                try:
                    probe = await _light_probe_suspicious_async(page, fr)
                    if probe.get("hit"):
                        page_html = (await page.content()) or ""
                        frame_html = (await fr.content()) or ""
                        detect = _detect_suspicious_markers(page_html, frame_html)
                        if detect.get("suspicious_hit"):
                            suspicious_hit = True
                            suspicious_meta = detect
                        else:
                            suspicious_hit = True
                            suspicious_meta = {
                                "suspicious_hit": True,
                                "markers_hit": [probe.get("label")] if probe.get("label") else [],
                                "vendors": [probe.get("vendor")] if probe.get("vendor") else [],
                                "strength": probe.get("strength"),
                                "excerpt": "",
                            }
                    else:
                        suspicious_hit = False
                        suspicious_meta = {
                            "suspicious_hit": False,
                            "markers_hit": [],
                            "vendors": [],
                            "strength": None,
                            "excerpt": "",
                        }
                except Exception:
                    pass
                if isinstance(stats, dict):
                    stats["suspicious_hit"] = suspicious_hit
                    markers_hit = (suspicious_meta or {}).get("markers_hit") or []
                    vendors = (suspicious_meta or {}).get("vendors") or []
                    strength = (suspicious_meta or {}).get("strength")
                    excerpt = (suspicious_meta or {}).get("excerpt") or ""
                    if suspicious_hit and not vendors:
                        vendor_value = "unknown"
                    elif len(vendors) == 1:
                        vendor_value = vendors[0]
                    elif vendors:
                        vendor_value = vendors
                    else:
                        vendor_value = None
                    stats["suspicious_markers_hit"] = markers_hit
                    stats["suspicious_vendor"] = vendor_value
                    stats["suspicious_excerpt"] = excerpt
                    stats["suspicious_strength"] = strength
                    should_dump = _should_dump_suspicious(stats, suspicious_meta or {}, frame_url=frame_url or getattr(fr, "url", ""))
                    if should_dump and not suspicious_dumped:
                        suspicious_dumped = True
                        await _dump_suspicious_debug_async(
                            page,
                            fr,
                            preset,
                            gid,
                            page_html,
                            frame_html,
                            {
                                "preset": preset,
                                "gid": gid,
                                "page_url": getattr(page, "url", ""),
                                "frame_url": frame_url or getattr(fr, "url", ""),
                                "markers_hit": markers_hit,
                                "vendors": vendors,
                                "strength": strength,
                                "excerpt": excerpt,
                            },
                        )
                    last_frame_url = frame_url or fr.url
                    if stats.get("ok"):
                        last_frame = fr
                        sanity_ok, sanity_reason = _calendar_sanity(stats)
                        if not sanity_ok:
                            last_invalid_stats = stats
                            last_invalid_reason = sanity_reason
                            sanity_retry_count += 1
                            sanity_last_reason = sanity_reason
                            if _detail_log_enabled():
                                sig = _calendar_signature(stats)
                                expected_total = int(stats.get("time_rows", 0) or 0) * int(stats.get("max_cols", 0) or 0)
                                log_event(
                                    "DBG",
                                    "calendar retry",
                                    frame_url=frame_url,
                                    td_count=stats.get("td_count", 0),
                                    time_rows=stats.get("time_rows", 0),
                                    max_cols=stats.get("max_cols", 0),
                                    expected_total=expected_total,
                                    total_slots=stats.get("total_slots", 0),
                                    signature=sig,
                                    attempt=attempt,
                                    reason=sanity_reason,
                                )
                                await _dump_calendar_debug_async(page, fr, stats, sanity_reason, frame_url)
                            slow_mode = True
                            skip, why = await _is_not_reservable_page_async(page)
                            if skip:
                                return {"ok": False, "reason": f"not_reservable({why})"}, None
                            await page.wait_for_timeout(slow_step_ms)
                            waited += slow_step_ms
                            continue
                        if _looks_like_all_dash(stats):
                            if isinstance(stats, dict):
                                stats["ok"] = True
                                stats["empty_calendar"] = True
                                stats["reason"] = "empty_calendar(all_dash)"
                                stats["_detail"] = {
                                    "sanity_retries": sanity_retry_count,
                                    "sanity_last_reason": sanity_last_reason,
                                }
                            if _detail_log_enabled():
                                log_event(
                                    "DBG",
                                    "calendar_all_dash_confirmed",
                                    preset=None,
                                    gid=None,
                                    waited_ms=waited,
                                    total_slots=stats.get("total_slots", 0),
                                    dash=stats.get("dash", 0),
                                )
                            return stats, last_frame_url
                        if isinstance(stats, dict):
                            stats["_detail"] = {
                                "sanity_retries": sanity_retry_count,
                                "sanity_last_reason": sanity_last_reason,
                            }
                        last_stats = stats
                        if _detail_log_enabled():
                            log_event(
                                "DBG",
                                "calendar stats confirmed",
                                attempt=stable_attempts,
                                frame_url=frame_url,
                                td_count=stats.get("td_count", 0),
                                time_rows=stats.get("time_rows", 0),
                                max_cols=stats.get("max_cols", 0),
                                signature=_calendar_signature(stats),
                                stable_hits=light_stable_hits,
                                stable_required=light_stable_required,
                                reason="light_stable_confirmed",
                            )
                        return stats, last_frame_url
                    else:
                        light_stable_hits = 0
                        last_light_sig = None
            except Exception:
                pass

        skip, why = await _is_not_reservable_page_async(page)
        if skip:
            return {"ok": False, "reason": f"not_reservable({why})"}, None
        await page.wait_for_timeout(slow_step_ms)
        waited += slow_step_ms

    if last_stats:
        if isinstance(last_stats, dict):
            last_stats["_detail"] = {
                "sanity_retries": sanity_retry_count,
                "sanity_last_reason": sanity_last_reason,
            }
        if _detail_log_enabled():
            expected_total = int(last_stats.get("time_rows", 0) or 0) * int(last_stats.get("max_cols", 0) or 0)
            log_event(
                "DBG",
                "calendar stats unstable",
                attempt=stable_attempts,
                frame_url=last_frame_url,
                td_count=last_stats.get("td_count", 0),
                time_rows=last_stats.get("time_rows", 0),
                max_cols=last_stats.get("max_cols", 0),
                expected_total=expected_total,
                total_slots=last_stats.get("total_slots", 0),
                signature=_calendar_signature(last_stats),
                reason="light_stable_timeout",
                stable_hits=light_stable_hits,
                stable_required=light_stable_required,
            )
            if last_frame:
                await _dump_calendar_debug_async(page, last_frame, last_stats, "unstable_timeout", last_frame_url)
        return last_stats, (last_frame_url or None)

    if last_invalid_stats:
        if isinstance(last_invalid_stats, dict):
            last_invalid_stats["_detail"] = {
                "sanity_retries": sanity_retry_count,
                "sanity_last_reason": sanity_last_reason,
            }
        if _detail_log_enabled():
            sig = _calendar_signature(last_invalid_stats)
            expected_total = int(last_invalid_stats.get("time_rows", 0) or 0) * int(last_invalid_stats.get("max_cols", 0) or 0)
            log_event(
                "DBG",
                "calendar stats sanity invalid",
                attempt=attempt,
                frame_url=last_frame_url,
                td_count=last_invalid_stats.get("td_count", 0),
                time_rows=last_invalid_stats.get("time_rows", 0),
                max_cols=last_invalid_stats.get("max_cols", 0),
                expected_total=expected_total,
                total_slots=last_invalid_stats.get("total_slots", 0),
                signature=sig,
                reason=last_invalid_reason or "sanity_fail",
            )
            if last_frame:
                await _dump_calendar_debug_async(page, last_frame, last_invalid_stats, last_invalid_reason or "sanity_fail", last_frame_url)
        last_invalid_stats = dict(last_invalid_stats)
        last_invalid_stats["ok"] = False
        last_invalid_stats["reason"] = last_invalid_reason or "sanity_fail"
        return last_invalid_stats, (last_frame_url or None)

    frame_names = []
    frame_urls = []
    try:
        for f in page.frames:
            frame_names.append(f.name or "")
            u = (f.url or "")
            if u:
                frame_urls.append(u[:200])
        frame_names = frame_names[:20]
        frame_urls = frame_urls[:20]
    except Exception:
        pass
    return {"ok": False, "reason": "calendar iframe not detected(timeout)", "frame_names": frame_names, "frame_urls": frame_urls}, None

async def count_calendar_stats_by_slots_async(page, preset: str = None, gid: str = None):
    short_ms = min(CAL_WAIT_MS, CAL_WAIT_SHORT_MS)
    long_ms = min(CAL_WAIT_MS, CAL_WAIT_LONG_MS)

    skip, why = await _is_not_reservable_page_async(page)
    if skip:
        _detail_log_skip(preset, gid, f"not_reservable({why})")
        _detail_log_iframe_wait(preset, gid, short_ms, long_ms, "skip")
        return {"ok": False, "reason": f"not_reservable({why})"}, None

    probe = await _reservation_page_probe_async(page)
    _detail_log_probe(
        preset,
        gid,
        "initial",
        not_reservable=probe.get("not_reservable"),
        has_iframe=probe.get("iframe"),
        has_calendar_table=probe.get("has_calendar_table"),
        has_link=probe.get("link"),
        login_like=probe.get("login_like"),
        text_hit=probe.get("text_hit"),
        error_page=probe.get("error_page"),
        iframe_hint=probe.get("iframe_hint"),
        iframe_src=probe.get("iframe_src"),
        iframe_h=probe.get("iframe_h"),
        iframe_frame_url=probe.get("iframe_frame_url"),
    )
    if probe.get("login_like"):
        if _detail_log_enabled():
            try:
                log_event("DBG", "login_like_reprobe", preset=preset, gid=gid, wait_ms=short_ms)
            except Exception:
                pass
        await page.wait_for_timeout(short_ms)
        probe = await _reservation_page_probe_async(page)
        _detail_log_probe(
            preset,
            gid,
            "login_like_recheck",
            not_reservable=probe.get("not_reservable"),
            has_iframe=probe.get("iframe"),
            has_calendar_table=probe.get("has_calendar_table"),
            has_link=probe.get("link"),
            login_like=probe.get("login_like"),
            text_hit=probe.get("text_hit"),
            error_page=probe.get("error_page"),
            iframe_hint=probe.get("iframe_hint"),
            iframe_src=probe.get("iframe_src"),
            iframe_h=probe.get("iframe_h"),
            iframe_frame_url=probe.get("iframe_frame_url"),
        )
        if _should_skip_login_like(probe):
            _detail_log_skip(preset, gid, "not_reservable(login_iframe_src)")
            _detail_log_iframe_wait(preset, gid, short_ms, long_ms, "skip")
            return {"ok": False, "reason": "not_reservable(login_iframe_src)"}, None
    probe_not_reservable = probe.get("not_reservable")
    if probe.get("login_like"):
        probe_not_reservable = _effective_not_reservable_without_login(probe)
        # --- 追加：iframe/link が見当たらない例外ページの早期判定（無駄な待ちを避ける） ---
        if (not probe.get("iframe")) and (not probe.get("link")) and (not probe.get("iframe_hint")) and (not probe.get("has_calendar_table")) and (not probe.get("text_hit")) and (not probe.get("error_page")):
            found = False
            for _ in range(3):
                try:
                    await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                except Exception:
                    pass
                try:
                    await page.wait_for_timeout(400)
                except Exception:
                    pass
                p2 = await _reservation_page_probe_async(page)
                if p2.get("iframe") or p2.get("link") or p2.get("has_calendar_table"):
                    probe = p2
                    found = True
                    break
            if not found:
                _detail_log_skip(preset, gid, "calendar_not_present")
                _detail_log_iframe_wait(preset, gid, short_ms, long_ms, "skip")
                return {"ok": False, "reason": "calendar_not_present"}, None
    if probe_not_reservable:
        if probe.get("error_page") and _detail_log_enabled():
            try:
                log_event("INFO", "skip not_reservable/error", preset=preset, gid=gid, url=(page.url or ""), reason="error_page")
            except Exception:
                pass
        if _should_reprobe_text_hit(probe):
            if _detail_log_enabled():
                try:
                    log_event("DBG", "text_hit_reprobe", preset=preset, gid=gid, wait_ms=TEXT_HIT_REPROBE_MS)
                except Exception:
                    pass
            await page.wait_for_timeout(TEXT_HIT_REPROBE_MS)
            probe = await _reservation_page_probe_async(page)
            _detail_log_probe(
                preset,
                gid,
                "text_hit_recheck",
                not_reservable=probe.get("not_reservable"),
                has_iframe=probe.get("iframe"),
                has_calendar_table=probe.get("has_calendar_table"),
                has_link=probe.get("link"),
                login_like=probe.get("login_like"),
                text_hit=probe.get("text_hit"),
                error_page=probe.get("error_page"),
                iframe_hint=probe.get("iframe_hint"),
                iframe_src=probe.get("iframe_src"),
                iframe_h=probe.get("iframe_h"),
                iframe_frame_url=probe.get("iframe_frame_url"),
            )
        probe_not_reservable = probe.get("not_reservable")
        if probe.get("login_like"):
            probe_not_reservable = _effective_not_reservable_without_login(probe)
        if probe_not_reservable:
            if probe.get("error_page"):
                _detail_log_skip(preset, gid, "not_reservable(error_page)")
                _detail_log_iframe_wait(preset, gid, short_ms, long_ms, "skip")
                return {"ok": False, "reason": "not_reservable(error_page)"}, None
            if _should_reprobe_text_hit(probe):
                _detail_log_skip(preset, gid, "not_reservable(text_hit)")
                _detail_log_iframe_wait(preset, gid, short_ms, long_ms, "skip")
                return {"ok": False, "reason": "not_reservable(text_hit)"}, None
            _detail_log_skip(preset, gid, "not_reservable")
            _detail_log_iframe_wait(preset, gid, short_ms, long_ms, "skip")
            return {"ok": False, "reason": "not_reservable"}, None

    stats, frame_url = await _count_calendar_stats_by_slots_async_core(page, short_ms, preset=preset, gid=gid)
    if stats and isinstance(stats, dict) and stats.get("ok"):
        _detail_log_iframe_wait(preset, gid, short_ms, long_ms, "short")
        return stats, frame_url

    if stats and isinstance(stats, dict):
        reason = (stats.get("reason") or "")
        if reason and "iframe not detected" not in reason:
            _detail_log_iframe_wait(preset, gid, short_ms, long_ms, "short")
            return stats, frame_url

    probe = await _reservation_page_probe_async(page)
    _detail_log_probe(
        preset,
        gid,
        "recheck",
        not_reservable=probe.get("not_reservable"),
        has_iframe=probe.get("iframe"),
        has_calendar_table=probe.get("has_calendar_table"),
        has_link=probe.get("link"),
        login_like=probe.get("login_like"),
        text_hit=probe.get("text_hit"),
        error_page=probe.get("error_page"),
        iframe_hint=probe.get("iframe_hint"),
        iframe_src=probe.get("iframe_src"),
        iframe_h=probe.get("iframe_h"),
        iframe_frame_url=probe.get("iframe_frame_url"),
    )
    if probe.get("login_like"):
        if _detail_log_enabled():
            try:
                log_event("DBG", "login_like_reprobe", preset=preset, gid=gid, wait_ms=short_ms)
            except Exception:
                pass
        await page.wait_for_timeout(short_ms)
        probe = await _reservation_page_probe_async(page)
        _detail_log_probe(
            preset,
            gid,
            "login_like_recheck",
            not_reservable=probe.get("not_reservable"),
            has_iframe=probe.get("iframe"),
            has_calendar_table=probe.get("has_calendar_table"),
            has_link=probe.get("link"),
            login_like=probe.get("login_like"),
            text_hit=probe.get("text_hit"),
            error_page=probe.get("error_page"),
            iframe_hint=probe.get("iframe_hint"),
            iframe_src=probe.get("iframe_src"),
            iframe_h=probe.get("iframe_h"),
            iframe_frame_url=probe.get("iframe_frame_url"),
        )
        if _should_skip_login_like(probe):
            _detail_log_skip(preset, gid, "not_reservable(login_iframe_src)")
            _detail_log_iframe_wait(preset, gid, short_ms, long_ms, "skip")
            return {"ok": False, "reason": "not_reservable(login_iframe_src)"}, None
    probe_not_reservable = probe.get("not_reservable")
    if probe.get("login_like"):
        probe_not_reservable = _effective_not_reservable_without_login(probe)
    if probe_not_reservable:
        if probe.get("error_page") and _detail_log_enabled():
            try:
                log_event("INFO", "skip not_reservable/error", preset=preset, gid=gid, url=(page.url or ""), reason="error_page")
            except Exception:
                pass
        if _should_reprobe_text_hit(probe):
            if _detail_log_enabled():
                try:
                    log_event("DBG", "text_hit_reprobe", preset=preset, gid=gid, wait_ms=TEXT_HIT_REPROBE_MS)
                except Exception:
                    pass
            await page.wait_for_timeout(TEXT_HIT_REPROBE_MS)
            probe = await _reservation_page_probe_async(page)
            _detail_log_probe(
                preset,
                gid,
                "text_hit_recheck",
                not_reservable=probe.get("not_reservable"),
                has_iframe=probe.get("iframe"),
                has_calendar_table=probe.get("has_calendar_table"),
                has_link=probe.get("link"),
                login_like=probe.get("login_like"),
                text_hit=probe.get("text_hit"),
                error_page=probe.get("error_page"),
                iframe_hint=probe.get("iframe_hint"),
                iframe_src=probe.get("iframe_src"),
                iframe_h=probe.get("iframe_h"),
                iframe_frame_url=probe.get("iframe_frame_url"),
            )
        probe_not_reservable = probe.get("not_reservable")
        if probe.get("login_like"):
            probe_not_reservable = _effective_not_reservable_without_login(probe)
        if probe_not_reservable:
            if probe.get("error_page"):
                _detail_log_skip(preset, gid, "not_reservable(error_page)")
                _detail_log_iframe_wait(preset, gid, short_ms, long_ms, "skip")
                return {"ok": False, "reason": "not_reservable(error_page)"}, None
            if _should_reprobe_text_hit(probe):
                _detail_log_skip(preset, gid, "not_reservable(text_hit)")
                _detail_log_iframe_wait(preset, gid, short_ms, long_ms, "skip")
                return {"ok": False, "reason": "not_reservable(text_hit)"}, None
            _detail_log_skip(preset, gid, "not_reservable")
            _detail_log_iframe_wait(preset, gid, short_ms, long_ms, "skip")
            return {"ok": False, "reason": "not_reservable"}, None

    if not (probe.get("iframe") or probe.get("link") or probe.get("iframe_hint") or probe.get("has_calendar_table")):
        _detail_log_skip(preset, gid, "iframe_missing")
        _detail_log_iframe_wait(preset, gid, short_ms, long_ms, "skip")
        return {"ok": False, "reason": "iframe_missing"}, None

    if long_ms <= short_ms:
        _detail_log_iframe_wait(preset, gid, short_ms, long_ms, "short")
        return stats, frame_url

    stats, frame_url = await _count_calendar_stats_by_slots_async_core(page, long_ms, preset=preset, gid=gid)
    _detail_log_iframe_wait(preset, gid, short_ms, long_ms, "long")
    return stats, frame_url

async def _make_async_context(headless: bool, minimize_browser: bool = False):
    """Playwright context を作る。
    - ルート直下に pw_profile/ があれば persistent profile を使い、Cookie/同意状態を維持する
    - なければ通常の一時コンテキスト
    """
    from playwright.async_api import async_playwright
    apw = await async_playwright().start()

    launch_args = []
    if (not headless) and minimize_browser:
        # keep it headful (anti-block) but visually minimized
        launch_args += ["--start-minimized", "--disable-infobars"]

    profile_dir = PROFILE_DIR
    context = None
    browser = None
    context_opts = {
        "locale": "ja-JP",
        "timezone_id": "Asia/Tokyo",
        "viewport": {"width": 1400, "height": 900},
        "user_agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/123.0.0.0 Safari/537.36"
        ),
    }
    try:
        if profile_dir and os.path.isdir(profile_dir):
            # persistent context（Cookie/ローカルストレージを維持）
            context = await apw.chromium.launch_persistent_context(
                user_data_dir=profile_dir,
                headless=headless,
                args=launch_args,
                **context_opts,
            )
            browser = context.browser
        else:
            browser = await apw.chromium.launch(headless=headless, args=launch_args)
            context = await browser.new_context(**context_opts)

        allow_images = str(os.environ.get("ALLOW_IMAGES", "")).strip().lower() not in ("", "0", "false", "no", "off")
        effective_block_types = set(BLOCK_RESOURCE_TYPES)
        if allow_images:
            effective_block_types.discard("image")

        async def _route(route, request):
            try:
                rt = request.resource_type
                url = (request.url or "").lower()
                if rt in effective_block_types:
                    await route.abort()
                    return
                for s in BLOCK_URL_SUBSTR:
                    if s in url:
                        await route.abort()
                        return
            except Exception:
                pass
            try:
                await route.continue_()
            except Exception:
                pass

        try:
            await context.route("**/*", _route)
        except Exception:
            pass

        try:
            await context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")
        except Exception:
            pass

        return apw, browser, context
    except Exception:
        # 途中で失敗したら後片付け
        try:
            if context:
                await context.close()
        except Exception:
            pass
        try:
            if browser:
                await browser.close()
        except Exception:
            pass
        try:
            await apw.stop()
        except Exception:
            pass
        raise

def _merge_rows_to_map(rows):
    m = {}
    for r in rows:
        key = f'{r.get("preset","")}::{r.get("gid","")}'
        m[key] = r
    return m

def _delta_report(prev_rows, cur_rows, top_n=5, min_conf=0):
    pm = _merge_rows_to_map(prev_rows or [])
    cm = _merge_rows_to_map(cur_rows or [])
    opens = []
    fills = []
    bad_conf = 0

    for k, cur in cm.items():
        conf = int(cur.get("site_confidence", 0) or 0)
        if conf < min_conf:
            bad_conf += 1
        prev = pm.get(k)
        if not prev:
            continue
        ps = prev.get("stats", {}) or {}
        cs = cur.get("stats", {}) or {}
        pb = int(ps.get("bell",0) or 0)
        cb = int(cs.get("bell",0) or 0)
        pbook = int(ps.get("maru",0) or 0) + int(ps.get("tel",0) or 0)
        cbook = int(cs.get("maru",0) or 0) + int(cs.get("tel",0) or 0)
        db = cb - pb
        dbook = cbook - pbook
        if dbook != 0:
            opens.append((abs(dbook), dbook, cur))
        if db != 0:
            fills.append((abs(db), db, cur))

    opens.sort(reverse=True, key=lambda x: x[0])
    fills.sort(reverse=True, key=lambda x: x[0])

    return {
        "opens": opens[:top_n],
        "fills": fills[:top_n],
        "bad_conf_skipped": bad_conf,
        "prev_rows": len(prev_rows or []),
        "cur_rows": len(cur_rows or []),
    }

def _summarize_row_quality(rows: list) -> dict:
    counts = {"OK": 0, "WARN": 0, "BAD": 0}
    for r in rows or []:
        grade = r.get("row_quality_grade")
        if grade not in counts:
            conf = int(r.get("scrape_health", r.get("site_confidence", 0)) or 0)
            if conf >= _QUALITY_OK_MIN:
                grade = "OK"
            elif conf >= _QUALITY_WARN_MIN:
                grade = "WARN"
            else:
                grade = "BAD"
        counts[grade] += 1
    return counts

async def async_scrape_job(job, headless: bool, minimize_browser: bool, concurrency: int, nav_limiter: AsyncNavLimiter):
    store_base = store_base_from_list_url(job.url)
    apw, browser, context = await _make_async_context(headless=headless, minimize_browser=minimize_browser)
    try:
        page = await context.new_page()
        page.set_default_navigation_timeout(NAV_TIMEOUT_MS)
        perf_records = [] if _detail_log_enabled() else None

        girl_ids = []
        girl_name = {}

        cur = job.url
        while cur and len(girl_ids) < job.max_items:
            await nav_limiter.wait_turn()
            ok = await async_goto_retry(page, cur, wait_until="domcontentloaded", tries=2)
            if not ok:
                log_event("WARN", "list goto failed", url=cur)
                break

            try:
                await page.wait_for_selector("a[href*='girlid-']", timeout=12000)
            except Exception:
                pass
            await page.wait_for_timeout(AFTER_GOTO_WAIT_MS)
            await _maybe_accept_interstitial(page)
            await _wait_list_ready(page)

            need = job.max_items - len(girl_ids)
            pairs = await async_collect_girls_from_list(page, need)

            if not pairs:
                # fallback: evaluate失敗/DOM違い対策（HTMLから girlid- を拾う）
                try:
                    html = await page.content()
                except Exception:
                    html = ""
                ids = _extract_girlids_from_html(html)
                if ids:
                    pairs = [[gid, "（名前不明）"] for gid in ids[:need]]
            for gid, name in pairs:
                if gid not in girl_name:
                    girl_name[gid] = name
                if gid not in girl_ids:
                    girl_ids.append(gid)
                    if len(girl_ids) >= job.max_items:
                        break

            nxt = await async_get_next_list_url(page)
            if not nxt or nxt == cur:
                break
            cur = nxt

        if not girl_ids:
            log_event("ERR", "no girl_ids", list_url=job.url, preset=job.name)
            # capture debug artifacts
            try:
                await _dump_list_debug_async(page, job.name, job.url, note="no girl_ids")
            except Exception:
                pass
            # if the site blocks automation, stop early to avoid hammering
            try:
                html = await page.content()
                if ("detected as abnormal" in html) or ("Please try again later" in html) or ("Your current behavior" in html):
                    raise BlockedBySiteError("blocked_by_site")
            except BlockedBySiteError:
                raise
            except Exception:
                pass
            return [], []

        sem = asyncio.Semaphore(max(1, int(concurrency)))

        async def _worker(gid: str):
            name = girl_name.get(gid, "（名前不明）")
            detail_url = f"{store_base}/girlid-{gid}/"
            res_url = f"{store_base}/A6ShopReservation/?girl_id={gid}"
            start_total = time.monotonic()
            goto_s = 0.0
            iframe_wait_s = 0.0
            count_s = 0.0
            stats = None
            frame_url = None

            async with sem:
                p2 = await context.new_page()
                p2.set_default_navigation_timeout(NAV_TIMEOUT_MS)
                try:
                    await nav_limiter.wait_turn()
                    goto_start = time.monotonic()
                    ok = await async_goto_retry(p2, res_url, wait_until="domcontentloaded", tries=2, preset=job.name, gid=gid)
                    goto_s = time.monotonic() - goto_start
                    if not ok:
                        return None
                    skip, why = await _is_not_reservable_page_async(p2)
                    if skip:
                        stats = {"ok": False, "reason": f"not_reservable({why})"}
                        return None
                    iframe_start = time.monotonic()
                    await p2.wait_for_timeout(AFTER_GOTO_WAIT_MS)
                    iframe_wait_s = time.monotonic() - iframe_start
                    count_start = time.monotonic()
                    stats, frame_url = await count_calendar_stats_by_slots_async(p2, preset=job.name, gid=gid)
                    count_s = time.monotonic() - count_start
                    if not stats or not isinstance(stats, dict):
                        return None
                    reason = stats.get("reason") if isinstance(stats, dict) else ""
                    if (not stats.get("ok")) and (reason == "iframe_missing" or str(reason).startswith("not_reservable")):
                        return None
                    return {
                        "gid": gid,
                        "name": name,
                        "detail": detail_url,
                        "res": res_url,
                        "frame_url": frame_url,
                        "stats": stats,
                        "preset": job.name,
                        "list_url": job.url,
                    }
                finally:
                    total_s = time.monotonic() - start_total
                    if _detail_log_enabled():
                        detail = stats.get("_detail", {}) if isinstance(stats, dict) else {}
                        sanity_retries = int(detail.get("sanity_retries", 0) or 0)
                        sanity_reason = detail.get("sanity_last_reason")
                        time_rows = stats.get("time_rows") if isinstance(stats, dict) else None
                        max_cols = stats.get("max_cols") if isinstance(stats, dict) else None
                        total_slots = stats.get("total_slots") if isinstance(stats, dict) else None
                        td_count = stats.get("td_count") if isinstance(stats, dict) else None
                        segments = {
                            "goto": goto_s,
                            "iframe_wait": iframe_wait_s,
                            "count": count_s,
                        }
                        slowest_segment = max(segments, key=segments.get) if segments else "unknown"
                        if sanity_retries:
                            slowest_segment = f"{slowest_segment}+sanity_retry"
                        log_event(
                            "INFO",
                            "calendar perf",
                            preset=job.name,
                            gid=gid,
                            url=res_url,
                            total_s=round(total_s, 3),
                            goto_s=round(goto_s, 3),
                            iframe_wait_s=round(iframe_wait_s, 3),
                            count_s=round(count_s, 3),
                            slowest_segment=slowest_segment,
                            sanity_retries=sanity_retries,
                            sanity_reason=sanity_reason,
                            sanity_ok=bool(stats.get("ok")) if isinstance(stats, dict) else False,
                            time_rows=time_rows,
                            max_cols=max_cols,
                            total_slots=total_slots,
                            td_count=td_count,
                        )
                        if perf_records is not None:
                            perf_records.append({
                                "gid": gid,
                                "url": res_url,
                                "total_s": round(total_s, 3),
                                "slowest_segment": slowest_segment,
                            })
                    try:
                        await p2.close()
                    except Exception:
                        pass

        tasks = [asyncio.create_task(_worker(gid)) for gid in girl_ids]
        results = []
        for t in asyncio.as_completed(tasks):
            r = await t
            if r and isinstance(r, dict):
                results.append(r)

        prev_rows = []
        for r in results:
            gid = r.get("gid","")
            prev = load_state_snapshot(gid) if gid else None
            prev_rows.append(prev)

        if perf_records is not None and perf_records:
            top5 = sorted(perf_records, key=lambda x: x.get("total_s", 0), reverse=True)[:5]
            log_event(
                "INFO",
                "calendar slowest top5",
                preset=job.name,
                count=len(perf_records),
                top5=top5,
            )

        return results, prev_rows
    finally:
        try:
            await context.close()
        except Exception:
            pass
        try:
            if browser:
                await browser.close()
        except Exception:
            pass
        try:
            await apw.stop()
        except Exception:
            pass

def finalize_rows(collected_rows: list, prev_rows: list, run_dir: str, job, job_i: int):
    if not collected_rows:
        log_event("WARN", "finalize_rows empty", preset=job.name, run_dir=os.path.basename(run_dir))
        save_job_outputs(run_dir, job, job_i, [], prev_rows or [])
        return []

    max_bell = 0
    for r in collected_rows:
        b = (r.get("stats", {}) or {}).get("bell", 0) or 0
        if b > max_bell:
            max_bell = b

    hist_cache = {}
    out = []
    for r in collected_rows:
        gid = r.get("gid","")
        stats = r.get("stats", {}) or {}
        stats_by_date = None
        if isinstance(stats, dict):
            stats_by_date = stats.get("stats_by_date")
        if not isinstance(stats_by_date, dict):
            stats_by_date = r.get("stats_by_date") if isinstance(r.get("stats_by_date"), dict) else None
        if isinstance(stats_by_date, dict):
            r["stats_by_date"] = stats_by_date

        prev = load_state_snapshot(gid) if gid else None
        prev_stats = (prev.get("stats") if isinstance(prev, dict) else None) if prev else None
        r["prev_stats"] = prev_stats
        r["delta_pop"] = calc_delta_popularity(prev_stats, stats)
        r.setdefault("delta", r.get("delta_pop"))

        diag = _row_quality_diag_from_stats(
            stats,
            frame_url=r.get("frame_url"),
            parse_errors=r.get("parse_errors") if isinstance(r.get("parse_errors"), list) else None,
        )
        conf, grade, reasons, core_missing = _calc_scrape_health(diag)
        _apply_scrape_health_fields(r, conf, grade, reasons, core_missing)
        r["site_issues"] = reasons
        r["scrape_issues"] = reasons

        signal_strength, signal_detail = _calc_signal_strength(stats, stats_by_date=stats_by_date)
        r["signal_strength"] = signal_strength
        r["signal_detail"] = signal_detail

        r["score"] = calc_score(stats, max_bell)
        r["score_model"] = _SCORE_MODEL_NAME

        try:
            if gid:
                if gid in hist_cache:
                    hist = hist_cache[gid]
                else:
                    hist = load_history(gid, limit=200)
                    hist_cache[gid] = hist
            else:
                hist = []
            big_score, detail = _calc_bigdata_score_detail(stats, hist, cur_stats_by_date=stats_by_date)
            r["big_score"] = big_score
            r["big_score_old"] = detail.get("big_score_old")
            r["bd_detail"] = detail
            r["bd_level"] = detail.get("bd_level")
            r["bd_trust"] = detail.get("bd_trust")
            r["bd_days"] = detail.get("bd_days")
            r["bd_window"] = detail.get("bd_window")
            r["bd_service_days"] = detail.get("bd_service_days")
            r["bd_obs_days"] = detail.get("bd_obs_days")
            r["bd_model"] = _BD_MODEL_NAME
            r["bd_model_version"] = detail.get("bd_model_version")
            rank_raw, rank_detail = _calc_rank_score_detail(stats, hist, cur_stats_by_date=stats_by_date)
            r["quality_score"] = rank_detail.get("quality_score")
            r["quality_lower_bound"] = rank_detail.get("quality_lower_bound")
            r["momentum_score"] = rank_detail.get("momentum_score")
            r["rank_score_raw"] = rank_detail.get("rank_score_raw")
            r["rank_score_lower"] = rank_detail.get("rank_score_lower")
            r["rank_model_version"] = rank_detail.get("rank_model_version")
            r["rank_detail"] = rank_detail
        except Exception as e:
            r["big_score"] = r.get("score",0)
            log_event("WARN", "calc_bigdata_score failed", preset=job.name, gid=gid, err=str(e)[:200])
        r["spike"] = r.get("score", 0) - r.get("big_score", r.get("score", 0))

        out.append(r)

    _assign_rank_percentiles(out)
    _assign_rank_percentiles(out, score_key="rank_score_lower", percentile_key="rank_percentile_lower")

    for r in out:
        gid = r.get("gid","")
        stats = r.get("stats", {}) or {}
        stats_by_date = r.get("stats_by_date") if isinstance(r.get("stats_by_date"), dict) else None
        if gid:
            try:
                append_history(gid, {
                    "ts": _now_ts(),
                    "preset": job.name,
                    "gid": gid,
                    "name": r.get("name",""),
                    "score": r.get("score",0),
                    "big_score": r.get("big_score", r.get("score",0)),
                    "big_score_old": r.get("big_score_old"),
                    "score_model": _SCORE_MODEL_NAME,
                    "bd_model": r.get("bd_model"),
                    "bd_model_version": r.get("bd_model_version"),
                    "bd_level": r.get("bd_level"),
                    "bd_trust": r.get("bd_trust"),
                    "bd_days": r.get("bd_days"),
                    "bd_window": r.get("bd_window"),
                    "bd_service_days": r.get("bd_service_days"),
                    "bd_obs_days": r.get("bd_obs_days"),
                    "quality_score": r.get("quality_score"),
                    "quality_lower_bound": r.get("quality_lower_bound"),
                    "momentum_score": r.get("momentum_score"),
                    "rank_score_raw": r.get("rank_score_raw"),
                    "rank_score_lower": r.get("rank_score_lower"),
                    "rank_percentile": r.get("rank_percentile"),
                    "rank_percentile_lower": r.get("rank_percentile_lower"),
                    "rank_model_version": r.get("rank_model_version"),
                    "rank_detail": r.get("rank_detail"),
                    "spike": r.get("spike"),
                    "site_confidence": r.get("site_confidence", 0),
                    "scrape_health": r.get("scrape_health", r.get("site_confidence", 0)),
                    "signal_strength": r.get("signal_strength"),
                    "stats": stats,
                    "stats_by_date": stats_by_date,
                })
            except Exception as e:
                log_event("ERR", "append_history failed", preset=job.name, gid=gid, err=str(e)[:200])

        if gid:
            try:
                save_state_snapshot(gid, {
                    "ts": _now_ts(),
                    "gid": gid,
                    "name": r.get("name",""),
                    "preset": job.name,
                    "stats": stats,
                    "stats_by_date": stats_by_date,
                    "score": r.get("score"),
                    "big_score": r.get("big_score"),
                    "big_score_old": r.get("big_score_old"),
                    "bd_window": r.get("bd_window"),
                    "quality_score": r.get("quality_score"),
                    "quality_lower_bound": r.get("quality_lower_bound"),
                    "momentum_score": r.get("momentum_score"),
                    "rank_score_raw": r.get("rank_score_raw"),
                    "rank_score_lower": r.get("rank_score_lower"),
                    "rank_percentile": r.get("rank_percentile"),
                    "rank_percentile_lower": r.get("rank_percentile_lower"),
                    "rank_model_version": r.get("rank_model_version"),
                    "scrape_health": r.get("scrape_health", r.get("site_confidence", 0)),
                    "signal_strength": r.get("signal_strength"),
                })
            except Exception as e:
                log_event("ERR", "save_state_snapshot failed", preset=job.name, gid=gid, err=str(e)[:200])

    save_job_outputs(run_dir, job, job_i, out, prev_rows or [])

    return out

async def run_job(preset_names=None, jobs=None, headless=True, minimize_browser=True, concurrency=3, trigger_context="auto", force_today=False, job_state_file=None, progress_file=None, stop_file=None):
    cfg = load_config()
    preset_names = preset_names or []
    preset_names = [x.strip() for x in preset_names if x.strip()]

    ensure_data_dirs()
    run_dir = make_run_dir()
    set_current_run_dir(run_dir)
    run_ts = os.path.basename(run_dir).replace("run_", "")

    if _env_flag("CAL_DEBUG_ONE"):
        concurrency = 1

    if jobs is None:
        jobs = build_jobs_from_presets(preset_names, presets_data=None)

    clear_stop_flag(stop_file)

    job_payload = {
        "created_at": _now_ts(),
        "trigger": trigger_context,
        "preset_names": preset_names,
        "jobs": [{"name": j.name, "url": j.url, "max_items": j.max_items} for j in jobs],
        "headless": headless,
        "minimize_browser": minimize_browser,
        "concurrency": concurrency,
        "run_dir": run_dir,
        "run_ts": run_ts,
    }
    write_job_state(job_payload, job_state_file)

    total_jobs = len(jobs)
    settings_payload = {
        "preset_names": preset_names,
        "headless": headless,
        "minimize_browser": minimize_browser,
        "concurrency": concurrency,
        "trigger": trigger_context,
    }
    write_progress_state({
        "status": "running",
        "trigger": trigger_context,
        "run_dir": run_dir,
        "run_ts": run_ts,
        "completed": 0,
        "total": total_jobs,
        "rows": 0,
        "current_job": None,
        "settings": settings_payload,
    }, progress_file)

    log_event("INFO", "job run start", run_dir=os.path.basename(run_dir), headless=headless, concurrency=concurrency, presets=preset_names, trigger=trigger_context)

    nav_limiter = AsyncNavLimiter(int(cfg.get("auto", {}).get("min_nav_interval_ms", 650) or 650))
    all_rows = []
    completed = 0
    stop_reason = ""
    for i, job in enumerate(jobs, start=1):
        if stop_requested(stop_file):
            stop_reason = "stop_flag"
            log_event("INFO", "stop flag detected", preset=job.name, run_dir=os.path.basename(run_dir))
            break
        write_progress_state({
            "status": "running",
            "trigger": trigger_context,
            "run_dir": run_dir,
            "run_ts": run_ts,
            "completed": completed,
            "total": total_jobs,
            "rows": len(all_rows),
            "current_job": {"index": i, "name": job.name, "url": job.url, "max_items": job.max_items},
            "settings": settings_payload,
        }, progress_file)
        log_event("INFO", "job start", preset=job.name, url=job.url, max_items=job.max_items)
        try:
            cur_rows, prev_rows = await async_scrape_job(job, headless=headless, minimize_browser=minimize_browser, concurrency=concurrency, nav_limiter=nav_limiter)
        except BlockedBySiteError:
            log_event("ERR", "blocked_by_site", preset=job.name, url=job.url)
            err_row = {
                "preset": job.name,
                "list_url": job.url,
                "name": "[ERROR] blocked_by_site",
                "error": "blocked_by_site",
                "stats": {"bell": 0, "maru": 0, "tel": 0, "bookable_slots": 0, "total_slots": 0, "excluded_slots": 0, "bell_rate_bookable": None},
            }
            save_job_outputs(run_dir, job, i, [err_row], [])
            stop_reason = "blocked_by_site"
            break
        except Exception as e:
            log_event("ERR", "job failed", preset=job.name, url=job.url, err=str(e)[:200])
            err_row = {
                "preset": job.name,
                "list_url": job.url,
                "name": f"[ERROR] {str(e)[:120]}",
                "error": str(e),
                "stats": {"bell": 0, "maru": 0, "tel": 0, "bookable_slots": 0, "total_slots": 0, "excluded_slots": 0, "bell_rate_bookable": None},
            }
            save_job_outputs(run_dir, job, i, [err_row], [])
            completed += 1
            continue
        rows = finalize_rows(cur_rows, prev_rows, run_dir, job, i)
        all_rows.extend(rows)
        completed += 1
        log_event("INFO", "job done", preset=job.name, got=len(rows))
        try:
            summary = _get_suspicious_dump_summary(_current_run_id(), job.name)
            log_event(
                "INFO",
                "suspicious dump summary",
                preset=job.name,
                saved=summary.get("saved", 0),
                suppressed=summary.get("suppressed", 0),
            )
        except Exception:
            pass

    save_run_outputs(run_dir, run_ts, all_rows, force_today=force_today, cfg=cfg)

    status = "stopped" if stop_reason == "stop_flag" else ("blocked" if stop_reason == "blocked_by_site" else "done")
    write_progress_state({
        "status": status,
        "trigger": trigger_context,
        "run_dir": run_dir,
        "run_ts": run_ts,
        "completed": completed,
        "total": total_jobs,
        "rows": len(all_rows),
        "current_job": None,
        "stop_reason": stop_reason or None,
        "settings": settings_payload,
    }, progress_file)

    log_event("INFO", "job run done", rows=len(all_rows), run_dir=os.path.basename(run_dir), status=status)
    return {"run_dir": run_dir, "run_ts": run_ts, "rows": all_rows, "jobs": jobs, "status": status}


async def run_auto_once(preset_names=None, headless=True, minimize_browser=True, concurrency=3, do_notify=True, force_today=False, retention_months=None, retention_max_lines=None, retention_disabled=False, trigger_context="auto"):
    cfg = load_config()
    result = await run_job(
        preset_names=preset_names,
        headless=headless,
        minimize_browser=minimize_browser,
        concurrency=concurrency,
        trigger_context=trigger_context,
        force_today=force_today,
    )

    run_dir = result["run_dir"]
    all_rows = result["rows"]
    today = datetime.date.today().isoformat()

    if do_notify and bool(cfg.get("notify",{}).get("enabled", True)):
        try:
            prev_day = None
            try:
                days = [d for d in os.listdir(DAILY_DIR) if re.match(r"^\d{4}-\d{2}-\d{2}$", d)]
                days.sort()
                for d in reversed(days):
                    if d != today:
                        prev_day = d
                        break
            except Exception:
                prev_day = None

            prev_rows = []
            if prev_day:
                ppath = os.path.join(DAILY_DIR, prev_day, "daily_snapshot.json")
                if os.path.exists(ppath):
                    with open(ppath, "r", encoding="utf-8") as r:
                        prev_rows = (json.load(r) or {}).get("all_current", []) or []

            min_conf = int(cfg.get("notify",{}).get("min_confidence", 0) or 0)
            rep = _delta_report(prev_rows, all_rows,
                                top_n=int(cfg.get("notify",{}).get("top_n", 5) or 5),
                                min_conf=min_conf)

            quality = _summarize_row_quality(all_rows)
            total_rows = len(all_rows)
            bad_conf = int(rep.get("bad_conf_skipped", 0) or 0)
            run_id = _current_run_id()
            suspicious_summary = _get_suspicious_dump_summary(run_id)
            summary_line = (
                f"抽出:{total_rows}件 OK:{quality['OK']} WARN:{quality['WARN']} BAD:{quality['BAD']} "
                f"低信頼:{bad_conf} (min_conf:{min_conf}) "
                f"suspicious: 保存{suspicious_summary.get('saved', 0)} / 抑止{suspicious_summary.get('suppressed', 0)}"
            )

            lines = [summary_line]
            if rep["opens"]:
                lines.append("開き増（bookable↑）Top:")
                for _, dbook, r in rep["opens"]:
                    nm = r.get("name","")
                    pr = r.get("preset","")
                    lines.append(f" {dbook:+d}  {pr} {nm}")
            if rep["fills"]:
                lines.append("埋まり増（bell↑）Top:")
                for _, db, r in rep["fills"]:
                    nm = r.get("name","")
                    pr = r.get("preset","")
                    lines.append(f" {db:+d}  {pr} {nm}")
            if not rep["opens"] and not rep["fills"]:
                lines.append("変化なし")

            title = f"予約スナップショット {today}"
            body = "\n".join(lines)
            notify_windows(title, body)
            log_event("INFO", "notify", title=title, body_preview=body[:180])
        except Exception as e:
            log_event("WARN", "notify failed", err=str(e)[:200])

    try:
        if not retention_disabled:
            rm = retention_months if retention_months is not None else int(cfg.get("retention", {}).get("months", 6) or 6)
            rl = retention_max_lines if retention_max_lines is not None else int(cfg.get("retention", {}).get("max_lines", 200) or 200)
            retention_cleanup(rm, rl)
    except Exception as e:
        log_event("WARN", "retention_cleanup failed", err=str(e)[:200])

    log_event("INFO", "auto done", rows=len(all_rows), run_dir=os.path.basename(run_dir))
    return run_dir

def _debug_score_print():
    params = _get_score_params()
    bell_sat = params.get("bell_sat", 18)
    bd_total_sat = params.get("bd_total_sat", 40)

    def _score_of(bell, maru, tel):
        return score_v2({"bell": bell, "maru": maru, "tel": tel}, bell_sat=bell_sat)

    score_a = _score_of(40, 0, 0)
    score_b = _score_of(39, 0, 0)
    score_c = _score_of(40, 40, 0)

    def _volume_factor(total):
        return 1.0 - math.exp(-total / float(bd_total_sat)) if bd_total_sat > 0 else 1.0

    print("[debug-score] params:", f"bell_sat={bell_sat}", f"bd_total_sat={bd_total_sat}")
    print("[debug-score] A bell=40 maru=0 tel=0 ->", f"{score_a:.6f}")
    print("[debug-score] B bell=39 maru=0 tel=0 ->", f"{score_b:.6f}")
    print("[debug-score] C bell=40 maru=40 tel=0 ->", f"{score_c:.6f}")
    print("[debug-score] BD volume factor total=10 ->", f"{_volume_factor(10):.6f}")
    print("[debug-score] BD volume factor total=40 ->", f"{_volume_factor(40):.6f}")
    print("[debug-score] BD volume factor total=200 ->", f"{_volume_factor(200):.6f}")

def _debug_bd_print():
    today = datetime.date.today()
    hist = [
        {
            "ts": (today - datetime.timedelta(days=2)).strftime("%Y%m%d_090000"),
            "stats_by_date": {
                (today + datetime.timedelta(days=1)).isoformat(): {"bell": 6, "maru": 4, "tel": 0},
                (today + datetime.timedelta(days=2)).isoformat(): {"bell": 5, "maru": 5, "tel": 0},
            },
        },
        {
            "ts": (today - datetime.timedelta(days=1)).strftime("%Y%m%d_090000"),
            "stats_by_date": {
                (today + datetime.timedelta(days=1)).isoformat(): {"bell": 8, "maru": 2, "tel": 0},
                (today + datetime.timedelta(days=3)).isoformat(): {"bell": 4, "maru": 6, "tel": 0},
            },
        },
    ]
    cur_stats_by_date = {
        (today + datetime.timedelta(days=1)).isoformat(): {"bell": 9, "maru": 1, "tel": 0},
        (today + datetime.timedelta(days=4)).isoformat(): {"bell": 3, "maru": 7, "tel": 0},
    }
    _, detail = _calc_bigdata_score_detail({"bell": 9, "maru": 1, "tel": 0}, hist, cur_stats_by_date=cur_stats_by_date)
    print("[debug-bd] unique_dates:", detail.get("unique_dates"))
    print("[debug-bd] days:", detail.get("bd_days"), "window:", detail.get("bd_window"), "service_days:", detail.get("bd_service_days"), "obs_days:", detail.get("bd_obs_days"))
    print("[debug-bd] level:", f"{detail.get('bd_level', 0):.6f}", "volume:", f"{detail.get('bd_volume_factor', 0):.6f}", "big_score:", f"{detail.get('big_score', 0):.6f}")
    print("[debug-bd] ma28/ma56/ma84/ma112:", f"{detail.get('ma28')}", f"{detail.get('ma56')}", f"{detail.get('ma84')}", f"{detail.get('ma112')}")

def _debug_rank_print():
    today = datetime.date.today()

    def _hist(days_ago, bell, maru, tel):
        day = today - datetime.timedelta(days=days_ago)
        return {
            "ts": day.strftime("%Y%m%d_090000"),
            "stats_by_date": {
                day.isoformat(): {"bell": bell, "maru": maru, "tel": tel},
            },
        }

    print("[debug-rank] T1: same input/history -> stable")
    hist = [_hist(1, 5, 5, 0), _hist(2, 5, 5, 0)]
    cur = {"bell": 5, "maru": 5, "tel": 0}
    r1, d1 = _calc_rank_score_detail(cur, hist)
    r2, d2 = _calc_rank_score_detail(cur, hist)
    print("[debug-rank] T1 quality:", f"{d1.get('quality_score', 0):.6f}", "momentum:", f"{d1.get('momentum_score', 0):.6f}", "rank:", f"{r1:.6f}")
    print("[debug-rank] T1 repeat rank:", f"{r2:.6f}")

    print("[debug-rank] T2: bell固定でmaru/tel増 -> quality低下")
    r_low, d_low = _calc_rank_score_detail({"bell": 5, "maru": 5, "tel": 0}, [])
    r_high, d_high = _calc_rank_score_detail({"bell": 5, "maru": 15, "tel": 0}, [])
    print("[debug-rank] T2 quality base:", f"{d_low.get('quality_score', 0):.6f}", "vs", f"{d_high.get('quality_score', 0):.6f}")

    print("[debug-rank] T3: quality同等でも最近bell増 -> momentum上昇")
    r_a, d_a = _calc_rank_score_detail({}, [_hist(1, 5, 5, 0)])
    r_b, d_b = _calc_rank_score_detail({}, [_hist(1, 10, 10, 0)])
    print("[debug-rank] T3 quality:", f"{d_a.get('quality_score', 0):.6f}", "vs", f"{d_b.get('quality_score', 0):.6f}")
    print("[debug-rank] T3 momentum:", f"{d_a.get('momentum_score', 0):.6f}", "vs", f"{d_b.get('momentum_score', 0):.6f}")
    print("[debug-rank] T3 rank:", f"{r_a:.6f}", "vs", f"{r_b:.6f}")

    print("[debug-rank] T4: 需要停止(古いbell) -> momentum低下")
    r_recent, d_recent = _calc_rank_score_detail({}, [_hist(1, 10, 0, 0)])
    r_old, d_old = _calc_rank_score_detail({}, [_hist(14, 10, 0, 0)])
    print("[debug-rank] T4 momentum recent:", f"{d_recent.get('momentum_score', 0):.6f}", "vs", f"{d_old.get('momentum_score', 0):.6f}")
    print("[debug-rank] T4 rank recent:", f"{r_recent:.6f}", "vs", f"{r_old:.6f}")

    print("[debug-rank] T5: 同率でも観測多い方がLB高い")
    _r_small, d_small = _calc_rank_score_detail({"bell": 1, "maru": 1, "tel": 0}, [])
    _r_large, d_large = _calc_rank_score_detail({"bell": 10, "maru": 10, "tel": 0}, [])
    print(
        "[debug-rank] T5 quality/lb:",
        f"{d_small.get('quality_score', 0):.6f}",
        f"{d_small.get('quality_lower_bound', 0):.6f}",
        "vs",
        f"{d_large.get('quality_lower_bound', 0):.6f}",
    )

    print("[debug-rank] T6: 0観測でもLBが破綻しない")
    _r_zero, d_zero = _calc_rank_score_detail({"bell": 0, "maru": 0, "tel": 0}, [])
    print("[debug-rank] T6 quality/lb:", f"{d_zero.get('quality_score', 0):.6f}", f"{d_zero.get('quality_lower_bound', 0):.6f}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--debug-score", action="store_true", help="print score/bd debug samples")
    parser.add_argument("--debug-bd", action="store_true", help="print BD unique-date debug samples")
    parser.add_argument("--debug-rank", action="store_true", help="print rank (quality/momentum) debug samples")
    args = parser.parse_args()
    if args.debug_score:
        _debug_score_print()
    if args.debug_bd:
        _debug_bd_print()
    if args.debug_rank:
        _debug_rank_print()
