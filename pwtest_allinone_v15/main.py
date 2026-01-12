# Entry point that supports both GUI and CLI automation.
# Usage:
#   .\.venv\Scripts\python.exe main.py            -> GUI
#   .\.venv\Scripts\python.exe main.py --auto ... -> headless automation
from scrape_core import *
from scrape_ui import App

LAST_RUN_STAMP = os.path.join(DATA_ROOT, "last_run_date.txt")

def _parse_args(argv):
    ap = argparse.ArgumentParser(add_help=False)
    ap.add_argument("--auto", action="store_true")
    ap.add_argument("--headless", action="store_true")
    ap.add_argument("--concurrency", type=int, default=None)
    ap.add_argument("--preset", type=str, default="")  # backward compat (comma-separated)
    ap.add_argument("--presets", type=str, default="")  # preferred (comma-separated)
    ap.add_argument("--headful", action="store_true")  # run with visible browser (may help if headless blocked)
    ap.add_argument("--minimize-browser", dest="minimize_browser", action="store_true", default=None, help="(headful only) minimize browser window while running")
    ap.add_argument("--no-minimize-browser", dest="minimize_browser", action="store_false", default=None, help="(headful only) do not minimize browser window")
    ap.add_argument("--notify", action="store_true")
    ap.add_argument("--force-today", action="store_true")
    ap.add_argument("--run-job", action="store_true")
    ap.add_argument("--job-file", type=str, default="")
    ap.add_argument("--help", action="store_true")
    ap.add_argument("--retention-months", type=int, default=None)
    ap.add_argument("--retention-max-lines", type=int, default=None)
    ap.add_argument("--no-retention", action="store_true")
    args, _ = ap.parse_known_args(argv)
    return args

def _read_last_run_date(path: str) -> str:
    if not os.path.exists(path):
        return ""
    try:
        with open(path, "r", encoding="utf-8-sig", errors="ignore") as f:
            return (f.read() or "").strip()
    except Exception:
        return ""

def _write_last_run_date(path: str, value: str) -> None:
    try:
        with open(path, "w", encoding="utf-8", newline="\n") as f:
            f.write((value or "").strip())
    except Exception:
        pass


def _resolve_options(args, job_state, cfg):
    auto_cfg = cfg.get("auto", {}) or {}
    notify_cfg = cfg.get("notify", {}) or {}

    preset_arg = (args.presets or args.preset or "").strip()
    if preset_arg:
        preset_names = [x.strip() for x in preset_arg.split(",") if x.strip()]
    elif job_state and job_state.get("preset_names"):
        preset_names = [x.strip() for x in (job_state.get("preset_names") or []) if x.strip()]
    else:
        auto_presets = str(auto_cfg.get("presets", "") or "")
        preset_names = [x.strip() for x in auto_presets.split(",") if x.strip()]

    jobs = []
    if job_state:
        for j in job_state.get("jobs", []) or []:
            try:
                jobs.append(Job(name=j.get("name", ""), url=j.get("url", ""), max_items=int(j.get("max_items", 60) or 60)))
            except Exception:
                continue
    if not jobs:
        jobs = None

    if args.concurrency is not None:
        concurrency = int(args.concurrency)
    elif job_state and job_state.get("concurrency") is not None:
        concurrency = int(job_state.get("concurrency") or auto_cfg.get("concurrency", 3) or 3)
    else:
        concurrency = int(auto_cfg.get("concurrency", 3) or 3)

    if args.headful:
        headless = False
    elif args.headless:
        headless = True
    elif job_state and job_state.get("headless") is not None:
        headless = bool(job_state.get("headless"))
    else:
        headless = not bool(auto_cfg.get("headful", False))

    if args.minimize_browser is not None:
        minimize_browser = bool(args.minimize_browser)
    elif job_state and job_state.get("minimize_browser") is not None:
        minimize_browser = bool(job_state.get("minimize_browser"))
    else:
        minimize_browser = bool(auto_cfg.get("minimize_browser", False))

    if headless:
        minimize_browser = False

    once_per_day = bool(auto_cfg.get("once_per_day", True))
    if job_state and job_state.get("once_per_day") is not None:
        once_per_day = bool(job_state.get("once_per_day"))

    if args.notify:
        do_notify = True
    elif job_state and job_state.get("notify_enabled") is not None:
        do_notify = bool(job_state.get("notify_enabled"))
    else:
        do_notify = bool(notify_cfg.get("enabled", True))

    retention_months = args.retention_months if args.retention_months is not None else None
    retention_max_lines = args.retention_max_lines if args.retention_max_lines is not None else None

    trigger_context = "auto" if args.auto else "job"
    if job_state and job_state.get("trigger"):
        trigger_context = str(job_state.get("trigger"))

    return {
        "preset_names": preset_names,
        "jobs": jobs,
        "concurrency": concurrency,
        "headless": headless,
        "minimize_browser": minimize_browser,
        "once_per_day": once_per_day,
        "do_notify": do_notify,
        "retention_months": retention_months,
        "retention_max_lines": retention_max_lines,
        "trigger_context": trigger_context,
    }


def _maybe_once_per_day(once_per_day: bool, force_today: bool) -> None:
    if not once_per_day or force_today:
        return
    ensure_data_dirs()
    today = datetime.date.today().isoformat()
    last = _read_last_run_date(LAST_RUN_STAMP)
    if last == today:
        print(f"already ran today: {today}")
        raise SystemExit(0)
    _write_last_run_date(LAST_RUN_STAMP, today)


if __name__ == "__main__":
    args = _parse_args(sys.argv[1:])
    if args.help:
        print(
            "使い方:\n"
            "  GUI起動:\n"
            "    python main.py\n\n"
            "  自動スナップショット（1回）:\n"
            "    python main.py --auto --headless --concurrency 3 --notify\n\n"
            "  プリセット指定（カンマ区切り）:\n"
            "    python main.py --auto --headless --preset \"A店,B店\" --concurrency 2 --notify\n\n"
            "  自動設定の既定値は score_data/config.json の auto.* で指定できます。\n"
            "    auto.presets / auto.headful / auto.concurrency / auto.once_per_day\n\n"
            "※ タスクスケジューラで『毎日1回』起動すれば、score_data/daily/YYYY-MM-DD に1日1回のスナップが残ります。\n※ 6ヶ月以上前のruns/daily/history/logは自動で削除/圧縮(行数制限)されます（--no-retentionで無効化可）。\n"
            "※ 通知を確実に出すには、タスクは『ユーザーがログオンしている場合のみ実行』推奨。"
        )
        raise SystemExit(0)

    if args.run_job:
        cfg = load_config()
        job_path = args.job_file or job_state_path()
        job_state = read_job_state(job_path)
        if not job_state:
            print(f"job ファイルが見つかりません: {job_path}")
            raise SystemExit(1)

        options = _resolve_options(args, job_state, cfg)
        if not options["jobs"] and not options["preset_names"]:
            print("[ERR] presets または jobs が指定されていません。")
            raise SystemExit(2)
        trigger = options["trigger_context"]
        if trigger.startswith("auto"):
            _maybe_once_per_day(options["once_per_day"], args.force_today)
            asyncio.run(
                run_auto_once(
                    preset_names=options["preset_names"],
                    headless=options["headless"],
                    minimize_browser=options["minimize_browser"],
                    concurrency=options["concurrency"],
                    do_notify=options["do_notify"],
                    force_today=args.force_today,
                    retention_months=options["retention_months"],
                    retention_max_lines=options["retention_max_lines"],
                    retention_disabled=args.no_retention,
                    trigger_context=trigger,
                )
            )
        else:
            asyncio.run(
                run_job(
                    preset_names=options["preset_names"],
                    jobs=options["jobs"],
                    headless=options["headless"],
                    minimize_browser=options["minimize_browser"],
                    concurrency=options["concurrency"],
                    trigger_context=trigger,
                    force_today=args.force_today,
                )
            )
    elif args.auto:
        cfg = load_config()
        options = _resolve_options(args, None, cfg)

        if not options["preset_names"]:
            print("[ERR] auto.presets が空です。score_data/config.json の auto.presets を設定するか、GUIの「設定」から登録してください。")
            raise SystemExit(2)

        _maybe_once_per_day(options["once_per_day"], args.force_today)

        asyncio.run(
            run_auto_once(
                preset_names=options["preset_names"],
                headless=options["headless"],
                minimize_browser=options["minimize_browser"],
                concurrency=options["concurrency"],
                do_notify=options["do_notify"],
                force_today=args.force_today,
                retention_months=options["retention_months"],
                retention_max_lines=options["retention_max_lines"],
                retention_disabled=args.no_retention,
                trigger_context=options["trigger_context"],
            )
        )
    else:
        App().mainloop()
