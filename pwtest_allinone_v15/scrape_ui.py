# GUI layer (Tkinter) - separated from core so CLI automation can run without Tkinter.
import tkinter as tk
from tkinter import ttk, messagebox, simpledialog
import json
import os
import subprocess
import sys

# Core logic (Playwright, scoring, IO, presets, auto runner...)
from scrape_core import *  # noqa: F401,F403
from scrape_core import _now_ts, _safe_name, _detail_log_enabled  # '_' names are not imported by '*'

HEADER_LABELS = {
    "bell": "🔔",
    "maru": "○",
    "tel": "📞",
}


class CastDetailPanel(ttk.Frame):
    def __init__(self, app, master, show_close=False, on_close=None):
        super().__init__(master)
        self.app = app
        self.show_close = show_close
        self.on_close = on_close
        self.day = None
        self.snap = None
        self.rows = []
        self.row_map = {}
        self.tree = None
        self.col_defs = []
        self.var_show_delta = tk.BooleanVar(value=False)
        self.var_day = tk.StringVar(value="")
        self.var_cast_samples = tk.StringVar(value="N/A")
        self.var_cast_ma3 = tk.StringVar(value="N/A")
        self.var_cast_ma14 = tk.StringVar(value="N/A")
        self.var_cast_ma28 = tk.StringVar(value="N/A")
        self.var_cast_ma_display = tk.StringVar(value=str(self.app.bd_ma_windows[0]))
        self.var_cast_ma_labels = [
            tk.StringVar(value="ma3_cast_big_score"),
            tk.StringVar(value="ma14_cast_big_score"),
            tk.StringVar(value="ma28_cast_big_score"),
        ]
        self.var_summary_ma_labels = [
            tk.StringVar(value="ma3_samples_avg_big_score"),
            tk.StringVar(value="ma14_samples_avg_big_score"),
            tk.StringVar(value="ma28_samples_avg_big_score"),
        ]
        self.var_summary_ma_values = [
            tk.StringVar(value="N/A"),
            tk.StringVar(value="N/A"),
            tk.StringVar(value="N/A"),
        ]
        self.ma_samples = []
        self.cast_samples = []
        self.cast_series_desc = []
        self.app.bd_ma_display_var.trace_add("write", lambda *_: self._on_summary_ma_display_change())
        self.var_cast_ma_display.trace_add("write", lambda *_: self._on_cast_ma_display_change())
        self._build_ui()

    def _build_ui(self):
        top = ttk.Frame(self)
        top.pack(fill="x", padx=6, pady=(6, 2))
        ttk.Label(top, textvariable=self.var_day).pack(side="left")

        self.summary_panel = ttk.Frame(self)
        self.summary_panel.pack(fill="x", padx=6, pady=(0, 6))

        self.summary_blocks = ttk.Frame(self.summary_panel)
        self.summary_blocks.pack(side="left", fill="x", expand=True)

        self.ma_graph_frame = ttk.LabelFrame(self.summary_panel, text="MA推移 (avg_big_score / 履歴)")
        self.ma_graph_frame.pack(side="right", fill="x", padx=6, pady=4)
        ma_content = ttk.Frame(self.ma_graph_frame)
        ma_content.pack(fill="both", padx=6, pady=6)
        ma_opts = ttk.Frame(ma_content)
        ma_opts.pack(side="left", fill="y", padx=(0, 6))
        ttk.Checkbutton(
            ma_opts,
            text="MA: 長期(56/84/112)",
            variable=self.app.bd_ma_long_mode,
            command=self._on_toggle_ma,
        ).pack(anchor="w")
        self.ma_canvas = tk.Canvas(ma_content, width=360, height=120, bg="white", highlightthickness=1, highlightbackground="#ddd")
        self.ma_canvas.pack(side="left", fill="both", expand=True)

        self.toggle_frame = ttk.Frame(self)
        self.toggle_frame.pack(fill="x", padx=6, pady=(0, 4))
        self.delta_toggle = ttk.Checkbutton(self.toggle_frame, text="Δ列を表示", variable=self.var_show_delta, command=self._on_toggle_delta)
        self.delta_toggle.pack(side="left")
        self.delta_note = ttk.Label(self.toggle_frame, text="")
        self.delta_note.pack(side="left", padx=8)

        cast_panel = ttk.Frame(self)
        cast_panel.pack(fill="x", padx=6, pady=(0, 6))

        cast_left = ttk.Frame(cast_panel)
        cast_left.pack(side="left", fill="x", expand=True)

        cast_btns = ttk.Frame(cast_left)
        cast_btns.pack(fill="x", pady=(0, 4))
        self.btn_cast_ma = ttk.Button(cast_btns, text="選択キャストMAを見る")
        self.btn_cast_ma.pack(side="left")
        self.btn_cast_open = ttk.Button(cast_btns, text="キャストページを開く")
        self.btn_cast_open.pack(side="left", padx=6)

        cast_info = ttk.LabelFrame(cast_left, text="MA（選択キャスト）")
        cast_info.pack(fill="x", padx=6, pady=(0, 4))
        ttk.Label(cast_info, text="samples_used").grid(row=0, column=0, sticky="w", padx=6, pady=2)
        ttk.Label(cast_info, textvariable=self.var_cast_samples).grid(row=0, column=1, sticky="w", padx=6, pady=2)
        cast_display_frame = ttk.Frame(cast_info)
        cast_display_frame.grid(row=0, column=2, sticky="e", padx=6, pady=2)
        cast_display_label = ttk.Label(cast_display_frame, text="表示MA")
        cast_display_label.pack(side="left")
        cast_display_combo = ttk.Combobox(
            cast_display_frame,
            width=4,
            state="disabled",
            textvariable=self.var_cast_ma_display,
            values=[str(w) for w in self.app.bd_ma_windows],
        )
        cast_display_combo.pack(side="left", padx=(4, 0))
        cast_display_label.pack_forget()
        cast_display_combo.pack_forget()
        cast_value_vars = [self.var_cast_ma3, self.var_cast_ma14, self.var_cast_ma28]
        for idx, (label_var, value_var) in enumerate(zip(self.var_cast_ma_labels, cast_value_vars), start=1):
            ttk.Label(cast_info, textvariable=label_var).grid(row=idx, column=0, sticky="w", padx=6, pady=2)
            ttk.Label(cast_info, textvariable=value_var).grid(row=idx, column=1, sticky="w", padx=6, pady=2)
        cast_info.columnconfigure(2, weight=1)
        cast_info.columnconfigure(1, weight=1)

        cast_graph_frame = ttk.LabelFrame(cast_panel, text="選択キャスト big_score 履歴")
        cast_graph_frame.pack(side="right", fill="x", padx=6, pady=4)
        self.cast_canvas = tk.Canvas(cast_graph_frame, width=360, height=120, bg="white", highlightthickness=1, highlightbackground="#ddd")
        self.cast_canvas.pack(fill="both", padx=6, pady=6)

        self.tree_frame = ttk.Frame(self)
        self.tree_frame.pack(fill="both", expand=True, padx=6, pady=(0, 6))

        bottom = ttk.Frame(self)
        bottom.pack(fill="both", expand=False, padx=6, pady=(0, 6))
        ttk.Label(bottom, text="Top Δbig_score").pack(anchor="w")
        self.delta_text = tk.Text(bottom, height=6, wrap="none")
        self.delta_text.pack(fill="x", expand=False, pady=(2, 6))

        btns = ttk.Frame(self)
        btns.pack(fill="x", padx=6, pady=(0, 6))
        ttk.Button(btns, text="コピー", command=self.copy_all).pack(side="left")
        if self.show_close:
            ttk.Button(btns, text="閉じる", command=self._on_close).pack(side="left", padx=8)

    def _on_close(self):
        if self.on_close:
            self.on_close()

    def load_day(self, day: str):
        if not day:
            return
        snap = self.app._load_daily_snapshot(day)
        if not snap:
            self._render_empty(day, "daily_snapshot.json を読み込めませんでした")
            return
        rows = snap.get("all_current", []) or []
        if not rows:
            self._render_empty(day, "all_current がありません")
            return
        self.update_snapshot(snap, day)

    def _render_empty(self, day: str, message: str):
        self.day = day
        self.snap = None
        self.rows = []
        self.var_day.set(f"日付: {day}")
        self.var_show_delta.set(False)
        self.ma_samples = []
        self.cast_samples = []
        self.cast_series_desc = []
        self.app._draw_ma_graph(self.ma_canvas, self.ma_samples)
        self.app._draw_ma_graph(self.cast_canvas, self.cast_samples)
        for child in self.summary_blocks.winfo_children():
            child.destroy()
        self._update_summary_ma_display({})
        self._update_cast_ma_display()
        self.delta_note.configure(text="")
        self.delta_toggle.state(["disabled"])
        self._clear_tree()
        self.delta_text.configure(state="normal")
        self.delta_text.delete("1.0", "end")
        self.delta_text.insert("end", message)
        self.delta_text.configure(state="disabled")

    def update_snapshot(self, snap: dict, day: str):
        self.day = day
        self.snap = snap
        self.rows = snap.get("all_current", []) or []
        self.var_day.set(f"日付: {snap.get('date', day)}")

        bd, has_bd_daily, summary = self.app._get_bd_daily_summary(snap, day)

        for child in self.summary_blocks.winfo_children():
            child.destroy()
        self.app._build_summary_block(self.summary_blocks, "--- 日付 ---", [
            ("date", summary["date"]),
            ("prev_day", summary["prev_day"]),
            ("gap_days", summary["gap_days"]),
        ])
        self.app._build_summary_block(self.summary_blocks, "--- BD Daily ---", [
            ("avg_big_score", summary["avg_big_score"]),
            ("delta_avg_big_score", summary["delta_avg_big_score"]),
            ("delta_avg_big_score_per_day", summary["delta_avg_big_score_per_day"]),
        ])
        self.app._build_ma_summary_block(
            self.summary_blocks,
            self.var_summary_ma_labels,
            self.var_summary_ma_values,
            self.app.bd_ma_display_var,
        )

        self.ma_samples = self.app._get_avg_big_score_series(day)
        self.app._draw_ma_graph(self.ma_canvas, self.ma_samples)
        self._update_summary_ma_display(summary)

        extra_keys = [
            "prev_seen_day",
            "gap_days",
        ]
        delta_keys = [
            "delta_big_score",
            "delta_big_score_per_day",
            "delta_bell",
            "delta_bookable",
            "delta_total",
        ]
        base_extra_cols = [k for k in extra_keys if self._col_present(k)]
        delta_available = any(self._col_present(k) for k in delta_keys)
        if delta_available:
            self.delta_note.configure(text="")
            self.delta_toggle.state(["!disabled"])
        elif not has_bd_daily:
            self.delta_note.configure(text="※bd_dailyが無いためΔ列はN/A")
            self.delta_toggle.state(["disabled"])
        else:
            self.delta_note.configure(text="")
            self.delta_toggle.state(["disabled"])

        self.base_extra_cols = base_extra_cols
        self.delta_keys = delta_keys
        self._populate_tree(self.var_show_delta.get())
        self._render_delta_top()
        self._bind_cast_actions()

    def _clear_tree(self):
        for child in self.tree_frame.winfo_children():
            child.destroy()
        self.tree = None
        self.row_map = {}

    def _col_present(self, key):
        for row in self.rows:
            if isinstance(row, dict) and row.get(key) is not None:
                return True
        return False

    def _build_columns(self, show_delta):
        extra_cols = list(self.base_extra_cols)
        if show_delta:
            extra_cols += [k for k in self.delta_keys if self._col_present(k)]
        col_defs = [
            ("rank", "Rank", 60),
            ("name", "Name", 200),
            ("gid", "GID", 110),
            ("score", "Score", 80),
            ("big_score", "BD", 80),
            ("rank_percentile", "Rank%", 80),
            ("quality_score", "Qual", 70),
            ("conf", "Conf", 70),
            ("bell", HEADER_LABELS["bell"], 60),
            ("maru", HEADER_LABELS["maru"], 60),
            ("tel", HEADER_LABELS["tel"], 60),
            ("bookable", "Bookable", 80),
            ("total", "Total", 70),
        ]
        col_defs += [(k, k, 120) for k in extra_cols]
        return col_defs

    def _get_stat(self, row, key):
        st = row.get("stats") if isinstance(row, dict) else {}
        if not isinstance(st, dict):
            st = {}
        return st.get(key)

    def _populate_tree(self, show_delta):
        self._clear_tree()
        self.col_defs = self._build_columns(show_delta)
        cols = [c[0] for c in self.col_defs]
        tree = ttk.Treeview(self.tree_frame, columns=cols, show="headings", height=18)
        tree.pack(fill="both", expand=True)
        for col_id, label, width in self.col_defs:
            tree.heading(col_id, text=label)
            anchor = "w" if col_id == "name" else "center"
            tree.column(col_id, width=width, anchor=anchor)
        row_map = {}
        for idx, row in enumerate(self.rows, 1):
            if not isinstance(row, dict):
                continue
            score_val = row.get("score")
            big_score_val = row.get("big_score", score_val)
            conf_val = row.get("site_confidence", row.get("conf"))
            row_values = {
                "rank": idx,
                "name": self.app._format_cast_name(row.get("name", "")),
                "gid": row.get("gid", ""),
                "score": self.app._format_score_percent(score_val),
                "big_score": self.app._format_score_percent(big_score_val),
                "rank_percentile": self.app._format_score_percent(row.get("rank_percentile")),
                "quality_score": self.app._format_score_percent(row.get("quality_score")),
                "conf": self.app._format_plain(conf_val),
                "bell": self.app._format_plain(self._get_stat(row, "bell")),
                "maru": self.app._format_plain(self._get_stat(row, "maru")),
                "tel": self.app._format_plain(self._get_stat(row, "tel")),
                "bookable": self.app._format_plain(self._get_stat(row, "bookable_slots")),
                "total": self.app._format_plain(self._get_stat(row, "total_slots")),
            }
            if "prev_seen_day" in cols:
                row_values["prev_seen_day"] = self.app._format_plain(row.get("prev_seen_day"))
            if "gap_days" in cols:
                row_values["gap_days"] = self.app._format_plain(row.get("gap_days"))
            if "delta_big_score" in cols:
                row_values["delta_big_score"] = self.app._format_delta_percent(row.get("delta_big_score"))
            if "delta_big_score_per_day" in cols:
                row_values["delta_big_score_per_day"] = self.app._format_delta_percent(row.get("delta_big_score_per_day"))
            if "delta_bell" in cols:
                row_values["delta_bell"] = self.app._format_delta_int(row.get("delta_bell"))
            if "delta_bookable" in cols:
                row_values["delta_bookable"] = self.app._format_delta_int(row.get("delta_bookable"))
            if "delta_total" in cols:
                row_values["delta_total"] = self.app._format_delta_int(row.get("delta_total"))

            iid = str(idx)
            tree.insert("", "end", iid=iid, values=[row_values.get(c, "") for c in cols])
            row_map[iid] = row

        def update_rank_column():
            if "rank" not in cols:
                return
            for index, iid in enumerate(tree.get_children(), 1):
                tree.set(iid, "rank", index)

        def sort_key_for(col_id, row):
            if col_id == "rank":
                return row.get("rank")
            if col_id == "name":
                return row.get("name", "")
            if col_id == "gid":
                return row.get("gid", "")
            if col_id == "score":
                return row.get("score")
            if col_id == "big_score":
                return row.get("big_score", row.get("score"))
            if col_id == "rank_percentile":
                return row.get("rank_percentile", row.get("rank_score_raw"))
            if col_id == "quality_score":
                return row.get("quality_score")
            if col_id == "conf":
                return row.get("site_confidence", row.get("conf"))
            if col_id == "bell":
                return self._get_stat(row, "bell")
            if col_id == "maru":
                return self._get_stat(row, "maru")
            if col_id == "tel":
                return self._get_stat(row, "tel")
            if col_id == "bookable":
                return self._get_stat(row, "bookable_slots")
            if col_id == "total":
                return self._get_stat(row, "total_slots")
            return row.get(col_id)

        def sort_tree(col_id, reverse=False):
            data = []
            for iid, row in row_map.items():
                val = sort_key_for(col_id, row)
                data.append((val, iid))
            def sort_key(item):
                val = item[0]
                if val is None:
                    return (True, 0)
                if isinstance(val, str):
                    return (False, val.lower())
                return (False, val)
            data.sort(key=sort_key, reverse=reverse)
            for index, (_, iid) in enumerate(data):
                tree.move(iid, "", index)
            update_rank_column()
            tree.heading(col_id, command=lambda c=col_id: sort_tree(c, not reverse))

        for col_id in cols:
            tree.heading(col_id, command=lambda c=col_id: sort_tree(c, False))

        sort_tree("score", True)
        update_rank_column()

        self.tree = tree
        self.row_map = row_map

    def _render_delta_top(self):
        deltas = []
        for row in self.rows:
            if not isinstance(row, dict):
                continue
            val = row.get("delta_big_score")
            if isinstance(val, (int, float)):
                deltas.append((float(val), row))
        self.delta_text.configure(state="normal")
        self.delta_text.delete("1.0", "end")
        if deltas:
            deltas.sort(key=lambda x: x[0], reverse=True)
            for i, (val, row) in enumerate(deltas[:10], 1):
                name = self.app._format_cast_name(row.get("name", "?"))
                self.delta_text.insert("end", f"{i:02d}. {val * 100:+.2f}  {name}\n")
        else:
            self.delta_text.insert("end", "N/A\n")
        self.delta_text.configure(state="disabled")

    def _get_selected_row(self):
        if not self.tree:
            return None
        sel = self.tree.selection()
        if not sel:
            return None
        return self.row_map.get(sel[0])

    def _bind_cast_actions(self):
        if not self.tree:
            return
        self.btn_cast_ma.configure(command=lambda: self._render_cast_ma(show_errors=True))
        self.btn_cast_open.configure(command=self._open_selected_cast)
        self.tree.bind("<Double-1>", lambda _e: self._open_selected_cast())
        self.tree.bind("<<TreeviewSelect>>", lambda _e: self._render_cast_ma(show_errors=False))

    def _open_selected_cast(self):
        row = self._get_selected_row()
        if not row:
            messagebox.showinfo("キャストページ", "行が選択されていません")
            return
        url = self.app._build_profile_url_from_row(row)
        if not url:
            messagebox.showinfo("キャストページ", "URL作成に失敗しました")
            return
        webbrowser.open(url)

    def _render_cast_ma(self, show_errors=True):
        row = self._get_selected_row()
        if not row:
            if show_errors:
                messagebox.showinfo("MA", "行が選択されていません")
            return
        gid = row.get("gid")
        if not gid:
            if show_errors:
                messagebox.showinfo("MA", "gid が見つかりません")
            return

        series_desc = self.app._get_cast_score_series(gid, self.day)
        samples_used = len(series_desc)

        self.var_cast_samples.set(str(samples_used))
        self.cast_series_desc = series_desc
        self._update_cast_ma_display()

        series = list(reversed(series_desc))
        self.cast_samples = series
        self.app._draw_ma_graph(self.cast_canvas, self.cast_samples)

    def _on_toggle_delta(self):
        self._populate_tree(self.var_show_delta.get())
        self._bind_cast_actions()

    def _on_toggle_ma(self):
        self.app._apply_bd_ma_mode(self.app.bd_ma_long_mode.get())
        self.var_cast_ma_display.set(self.app.bd_ma_display_var.get())
        summary = self.app._get_bd_daily_summary(self.snap or {}, self.day or "")[2]
        self._update_summary_ma_display(summary)
        self._update_cast_ma_display()
        self._redraw_ma_graphs()

    def _on_summary_ma_display_change(self):
        summary = self.app._get_bd_daily_summary(self.snap or {}, self.day or "")[2]
        self._update_summary_ma_display(summary)

    def _on_cast_ma_display_change(self):
        self._update_cast_ma_display()

    def _update_summary_ma_display(self, summary):
        self.app._update_ma_summary_values(
            self.var_summary_ma_labels,
            self.var_summary_ma_values,
            summary,
            self.ma_samples,
            self.app.bd_ma_display_var.get(),
        )

    def _update_cast_ma_display(self):
        window_values = self.app._get_display_windows(self.var_cast_ma_display.get())
        cast_value_vars = [self.var_cast_ma3, self.var_cast_ma14, self.var_cast_ma28]
        for idx, window in enumerate(window_values):
            label = f"ma{window}_cast_big_score" if window else "ma--_cast_big_score"
            self.var_cast_ma_labels[idx].set(label)
            cast_value_vars[idx].set(self._calc_cast_ma_value(window))

    def _calc_cast_ma_value(self, window):
        if not window or not self.cast_series_desc or len(self.cast_series_desc) < window:
            return "N/A"
        vals = [v for _, v in self.cast_series_desc[:window] if isinstance(v, (int, float))]
        if len(vals) < window:
            return "N/A"
        return self.app._format_score_percent(sum(vals) / window)

    def _redraw_ma_graphs(self):
        self.app._draw_ma_graph(self.ma_canvas, self.ma_samples)
        self.app._draw_ma_graph(self.cast_canvas, self.cast_samples)

    def copy_all(self):
        if not self.tree:
            return
        try:
            header = "\t".join([label for _, label, _ in self.col_defs])
            lines = [header]
            for iid in self.tree.get_children():
                vals = self.tree.item(iid, "values")
                lines.append("\t".join([str(v) for v in vals]))
            self.app.clipboard_clear()
            self.app.clipboard_append("\n".join(lines))
        except Exception:
            pass

# -------------------------
# GUI
# -------------------------
class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("予約カレンダー集計ツール")
        self.geometry("1200x820")

        self.stop_evt = threading.Event()
        self.worker = None
        self._run_active = False
        self._poll_after_id = None
        self._run_start_ts = None
        self._last_run_trigger = None
        self._beeped_for_run = False
        self._saw_running_for_run = False
        self._is_starting = True

        self.presets = load_presets()
        cfg = load_config()
        auto_cfg = cfg.get("auto", {}) or {}
        self.jobs = []
        self.results = []
        self.cache_day_snapshot = {}
        self.cache_gid_series = {}
        self._manual_url_lock = False
        self._manual_max_lock = False
        self._last_applied_preset = None

        self.var_showlog = tk.BooleanVar(value=False)
        self.var_beep = tk.BooleanVar(value=True)
        self.bd_ma_windows = (3, 14, 28, 56, 84, 112)
        self.bd_ma_long_mode = tk.BooleanVar(value=False)
        self.bd_ma_vars = {
            3: tk.BooleanVar(value=True),
            14: tk.BooleanVar(value=True),
            28: tk.BooleanVar(value=True),
            56: tk.BooleanVar(value=False),
            84: tk.BooleanVar(value=False),
            112: tk.BooleanVar(value=False),
        }
        self.bd_ma_display_var = tk.StringVar(value=str(self.bd_ma_windows[0]))
        self._bd_summary_ma_trace_id = None

        self._build_ui()
        self._refresh_preset_combo()
        self._refresh_preset_listbox()
        self._toggle_log()
        self.after(100, self._preload_results_from_latest_daily_snapshot)
        self.after(0, self._finish_startup)

    def _build_ui(self):
        dashboard = ttk.LabelFrame(self, text="ダッシュボード")
        dashboard.pack(fill="x", padx=10, pady=(8, 0))
        dash_row = ttk.Frame(dashboard)
        dash_row.pack(fill="x", padx=8, pady=6)
        ttk.Button(dash_row, text="手動実行", command=self.start_run, width=14).pack(side="left", padx=4)
        ttk.Button(dash_row, text="停止", command=self.stop_run, width=10).pack(side="left", padx=4)
        ttk.Button(dash_row, text="前回結果", command=self.view_prev_preset, width=12).pack(side="left", padx=4)
        ttk.Button(dash_row, text="履歴サマリ", command=self.view_history_summary, width=12).pack(side="left", padx=4)
        ttk.Button(dash_row, text="BDサマリ", command=self.view_bd_summary, width=12).pack(side="left", padx=4)
        ttk.Button(dash_row, text="自動(1回)", command=self.start_auto_once, width=12).pack(side="left", padx=4)
        ttk.Button(dash_row, text="設定", command=self.edit_auto_settings, width=12).pack(side="left", padx=4)
        ttk.Button(dash_row, text="ログ表示切替", command=self.toggle_log_visibility, width=12).pack(side="left", padx=4)

        top = ttk.Frame(self)
        top.pack(fill="x", padx=10, pady=8)

        ttk.Label(top, text="プリセット").grid(row=0, column=0, sticky="w")

        self.combo = ttk.Combobox(top, width=28, state="normal")
        self.combo.grid(row=0, column=1, sticky="w", padx=6)
        self.combo.bind("<<ComboboxSelected>>", lambda _e: self.on_preset_selected())
        self.combo.bind("<Return>", lambda _e: self.on_preset_selected())
        self.combo.bind("<FocusOut>", lambda _e: self.on_preset_selected())

        ttk.Button(top, text="一括キュー設定", command=self.apply_preset).grid(row=0, column=2, sticky="w", padx=4)
        ttk.Button(top, text="追加/更新", command=self.upsert_preset).grid(row=0, column=3, sticky="w", padx=4)
        ttk.Button(top, text="削除", command=self.delete_preset).grid(row=0, column=4, sticky="w", padx=4)

        ttk.Label(top, text="MAX").grid(row=0, column=5, sticky="e")
        self.ent_max = ttk.Entry(top, width=8)
        self.ent_max.grid(row=0, column=6, sticky="w", padx=6)
        self.ent_max.bind("<KeyRelease>", lambda _e: self._mark_manual_max())

        ttk.Checkbutton(top, text="完了音", variable=self.var_beep).grid(row=0, column=7, sticky="w", padx=6)

        ttk.Label(top, text="LIST_URL").grid(row=1, column=0, sticky="w", pady=(8,0))
        self.ent_url = ttk.Entry(top, width=105)
        self.ent_url.grid(row=1, column=1, columnspan=8, sticky="we", padx=6, pady=(8,0))
        self.ent_url.bind("<KeyRelease>", lambda _e: self._mark_manual_url())

        ttk.Button(top, text="キューに追加", command=self.add_to_queue).grid(row=2, column=1, sticky="w", padx=6, pady=(10,0))

        ttk.Label(top, text="ソート").grid(row=2, column=5, sticky="e", pady=(10,0))
        self.combo_sort = ttk.Combobox(top, width=14, state="readonly",
                                       values=["総合スコア","ランキング(新)","ビッグデータ","bell率","ベル数","空き(○)","TEL多い","bookable多い"])
        self.combo_sort.grid(row=2, column=6, sticky="w", padx=6, pady=(10,0))
        self.combo_sort.set("総合スコア")
        self.combo_sort.bind("<<ComboboxSelected>>", lambda e: self.resort_view())

        mid = ttk.Panedwindow(self, orient="horizontal")
        mid.pack(fill="both", expand=True, padx=10, pady=8)

        left = ttk.Frame(mid)
        right = ttk.Frame(mid)
        mid.add(left, weight=1)
        mid.add(right, weight=3)

        preset_panel = ttk.LabelFrame(left, text="プリセット選択（チェック）")
        preset_panel.pack(fill="x", pady=(0, 6))

        self.preset_canvas = tk.Canvas(preset_panel, height=180, highlightthickness=0)
        self.preset_canvas.pack(side="left", fill="both", expand=True)
        self.preset_scroll = ttk.Scrollbar(preset_panel, orient="vertical", command=self.preset_canvas.yview)
        self.preset_scroll.pack(side="right", fill="y")
        self.preset_canvas.configure(yscrollcommand=self.preset_scroll.set)

        self.preset_checks_frame = ttk.Frame(self.preset_canvas)
        self.preset_canvas.create_window((0, 0), window=self.preset_checks_frame, anchor="nw")
        self.preset_checks_frame.bind(
            "<Configure>",
            lambda _e: self.preset_canvas.configure(scrollregion=self.preset_canvas.bbox("all")),
        )
        def _on_preset_mousewheel(e):
            try:
                delta = int(-1 * (e.delta / 120))
            except Exception:
                delta = -1
            self.preset_canvas.yview_scroll(delta, "units")
            return "break"
        def _on_preset_mousewheel_linux(e):
            if getattr(e, 'num', None) == 4:
                self.preset_canvas.yview_scroll(-1, "units")
            elif getattr(e, 'num', None) == 5:
                self.preset_canvas.yview_scroll(1, "units")
            return "break"
        self._preset_mousewheel_handler = _on_preset_mousewheel
        self._preset_mousewheel_handler_linux = _on_preset_mousewheel_linux
        for _w in (self.preset_canvas, self.preset_checks_frame):
            _w.bind("<MouseWheel>", _on_preset_mousewheel)
            _w.bind("<Button-4>", _on_preset_mousewheel_linux)
            _w.bind("<Button-5>", _on_preset_mousewheel_linux)

        preset_btns = ttk.Frame(left)
        preset_btns.pack(fill="x")
        ttk.Button(preset_btns, text="全選択", command=lambda: self._select_all_presets(True)).pack(side="left", padx=2)
        ttk.Button(preset_btns, text="全解除", command=lambda: self._select_all_presets(False)).pack(side="left", padx=2)
        ttk.Button(preset_btns, text="選択を保存", command=self.save_selected_presets_to_auto).pack(side="left", padx=6)

        ttk.Label(left, text="処理キュー").pack(anchor="w", pady=(12, 0))
        self.lb_queue = tk.Listbox(left, height=12)
        self.lb_queue.pack(fill="x", pady=6)

        btns = ttk.Frame(left); btns.pack(fill="x")
        ttk.Button(btns, text="上へ", command=lambda: self.move_queue(-1)).pack(side="left", padx=2)
        ttk.Button(btns, text="下へ", command=lambda: self.move_queue(1)).pack(side="left", padx=2)
        ttk.Button(btns, text="削除", command=self.remove_queue).pack(side="left", padx=10)
        ttk.Button(btns, text="クリア", command=self.clear_queue).pack(side="left", padx=2)

        ttk.Label(left, text="使い方").pack(anchor="w", pady=(14,0))
        ttk.Label(left, text="・プリセット名は直接打ち替えOK\n・チェックを選択→一括キュー設定\n・結果はダブルクリックでDETAIL\n・Shift+ダブルクリックでRES",
                  justify="left").pack(anchor="w", pady=6)

        ttk.Label(right, text="結果（ダブルクリックでDETAIL / Shift+ダブルクリックでRES）").pack(anchor="w")
        cols = ("rank","score","big","rnk","qual","delta","conf","rate","bell","maru","tel","bookable","total","name")
        self.tree = ttk.Treeview(right, columns=cols, show="headings", height=18)
        self.tree.pack(fill="both", expand=True, pady=6)

        headings = {"rank":"Rank","score":"Score","big":"BD","rnk":"Rank%","qual":"Qual","delta":"Δpop","conf":"Conf","rate":"bell%","bell":HEADER_LABELS["bell"],"maru":HEADER_LABELS["maru"],"tel":HEADER_LABELS["tel"],"bookable":"Bookable","total":"Total","name":"Name"}
        widths = {"rank":60,"score":80,"big":80,"rnk":70,"qual":70,"delta":75,"conf":70,"rate":80,"bell":60,"maru":60,"tel":60,"bookable":95,"total":75,"name":270}
        for c in cols:
            self.tree.heading(c, text=headings[c])
            self.tree.column(c, width=widths[c], anchor="center", stretch=False)
        self.tree.column("name", anchor="w", stretch=False)

        self._tree_cols = cols
        self._tree_base_widths = widths
        self._tree_min_widths = {
            "rank":40,
            "score":55,
            "big":55,
            "rnk":55,
            "qual":55,
            "delta":55,
            "conf":50,
            "rate":55,
            "bell":40,
            "maru":40,
            "tel":40,
            "bookable":70,
            "total":55,
            "name":140,
        }
        self._resize_after_id = None
        self._last_root_size = None

        self.tree.tag_configure("top", font=("Meiryo", 10, "bold"))
        self.tree.tag_configure("muted", foreground="#777")

        self.tree.bind("<Double-1>", self.on_double_click)
        self.tree.bind("<Button-3>", self.on_right_click)

        self.menu = tk.Menu(self, tearoff=0)
        self.menu.add_command(label="RESを開く", command=lambda: self.open_selected("res"))
        self.menu.add_command(label="DETAILを開く", command=lambda: self.open_selected("detail"))
        self.menu.add_separator()
        self.menu.add_command(label="RES URLをコピー", command=lambda: self.copy_selected("res"))
        self.menu.add_command(label="DETAIL URLをコピー", command=lambda: self.copy_selected("detail"))
        self.menu.add_command(label="名前をコピー", command=lambda: self.copy_selected("name"))

        self.frame_log = ttk.Frame(self)
        self.txt = tk.Text(self.frame_log, height=10, wrap="none")
        self.txt.pack(fill="both", expand=True)

        bottom = ttk.Frame(self)
        bottom.pack(fill="x", padx=10, pady=(0,10))
        self.var_status = tk.StringVar(value="待機中")
        ttk.Label(bottom, textvariable=self.var_status).pack(side="left")

        self.pb = ttk.Progressbar(bottom, length=320, mode="determinate")
        self.pb.pack(side="right")
        self.var_time = tk.StringVar(value="")
        ttk.Label(bottom, textvariable=self.var_time).pack(side="right", padx=12)

        self.bind("<Configure>", self._on_root_configure)
        self.after(0, self._apply_tree_layout)

    def start_auto_once(self):
        if self._run_active:
            messagebox.showinfo("実行中", "すでに実行中です")
            return

        preset_names = self._get_selected_presets()
        if not preset_names:
            messagebox.showinfo("未選択", "左のプリセット一覧から1つ以上選択してください。")
            return

        payload = self._build_job_payload(
            trigger="auto_once",
            preset_names=preset_names,
            jobs=None,
            headless=None,
            include_auto_flags=True,
        )
        self._launch_job(payload, status_message="自動実行(1回)を起動しました")

    def edit_auto_settings(self):
        cfg = load_config()
        auto_cfg = cfg.get("auto", {}) or {}
        notify_cfg = cfg.get("notify", {}) or {}

        win = tk.Toplevel(self)
        win.title("設定")
        win.geometry("420x360")
        win.transient(self)
        win.grab_set()

        frm = ttk.Frame(win)
        frm.pack(fill="both", expand=True, padx=12, pady=12)

        var_presets = tk.StringVar(value=str(auto_cfg.get("presets", "") or ""))
        var_concurrency = tk.StringVar(value=str(auto_cfg.get("concurrency", 3) or 3))
        var_headful = tk.BooleanVar(value=bool(auto_cfg.get("headful", False)))
        var_once = tk.BooleanVar(value=bool(auto_cfg.get("once_per_day", True)))
        var_minimize = tk.BooleanVar(value=bool(auto_cfg.get("minimize_browser", False)))
        var_notify = tk.BooleanVar(value=bool(notify_cfg.get("enabled", True)))

        ttk.Label(frm, text="auto.presets (カンマ区切り)").pack(anchor="w")
        ttk.Entry(frm, textvariable=var_presets, width=48).pack(fill="x", pady=(0, 8))

        ttk.Label(frm, text="auto.concurrency").pack(anchor="w")
        ttk.Entry(frm, textvariable=var_concurrency, width=12).pack(anchor="w", pady=(0, 8))

        ttk.Checkbutton(frm, text="auto.headful (表示あり)", variable=var_headful).pack(anchor="w")
        ttk.Checkbutton(frm, text="auto.once_per_day (1日1回)", variable=var_once).pack(anchor="w")
        ttk.Checkbutton(frm, text="auto.minimize_browser (最小化)", variable=var_minimize).pack(anchor="w")
        ttk.Checkbutton(frm, text="notify.enabled (通知)", variable=var_notify).pack(anchor="w")

        btns = ttk.Frame(frm)
        btns.pack(fill="x", pady=12)

        def on_save():
            try:
                concurrency = int(var_concurrency.get().strip())
                if concurrency <= 0:
                    raise ValueError
            except Exception:
                messagebox.showerror("入力エラー", "auto.concurrency は1以上の数字で入力してください。")
                return

            auto_cfg["presets"] = var_presets.get().strip()
            auto_cfg["concurrency"] = concurrency
            auto_cfg["headful"] = bool(var_headful.get())
            auto_cfg["once_per_day"] = bool(var_once.get())
            auto_cfg["minimize_browser"] = bool(var_minimize.get())
            cfg["auto"] = auto_cfg

            notify_cfg["enabled"] = bool(var_notify.get())
            cfg["notify"] = notify_cfg

            save_config(cfg)
            self._refresh_preset_listbox()
            messagebox.showinfo("設定保存", "設定を保存しました。")
            win.destroy()

        ttk.Button(btns, text="保存", command=on_save).pack(side="right")
        ttk.Button(btns, text="キャンセル", command=win.destroy).pack(side="right", padx=6)

    def _resolve_python_exe(self, base_dir: str) -> str:
        candidates = [
            os.path.join(base_dir, ".venv", "Scripts", "python.exe"),
            os.path.join(base_dir, "..", ".venv", "Scripts", "python.exe"),
            sys.executable,
        ]
        for path in candidates:
            if path and os.path.exists(path):
                return path
        return "python"

    def _build_job_payload(self, trigger: str, preset_names=None, jobs=None, headless=None, include_auto_flags=False):
        cfg = load_config()
        auto_cfg = cfg.get("auto", {}) or {}

        if headless is None:
            headless = not bool(auto_cfg.get("headful", False))

        concurrency = int(auto_cfg.get("concurrency", 3) or 3)
        minimize_browser = bool(auto_cfg.get("minimize_browser", False))
        if headless:
            minimize_browser = False

        payload = {
            "created_at": _now_ts(),
            "trigger": trigger,
            "preset_names": preset_names or [],
            "jobs": [{"name": j.name, "url": j.url, "max_items": j.max_items} for j in (jobs or [])],
            "headless": bool(headless),
            "concurrency": concurrency,
            "minimize_browser": bool(minimize_browser),
        }

        if include_auto_flags:
            payload["once_per_day"] = bool(auto_cfg.get("once_per_day", True))
            payload["notify_enabled"] = bool(cfg.get("notify", {}).get("enabled", True))

        return payload

    def _launch_job(self, payload: dict, status_message: str):
        base_dir = os.path.abspath(os.path.dirname(__file__))
        py = self._resolve_python_exe(base_dir)
        job_path = write_job_state(payload, job_state_path())
        cmd = [py, os.path.join(base_dir, "main.py"), "--run-job", "--job-file", job_path]
        try:
            self._last_run_trigger = payload.get("trigger")
            self._beeped_for_run = False
            self._saw_running_for_run = False
            try:
                progress_path = progress_state_path()
                if os.path.exists(progress_path):
                    os.remove(progress_path)
            except Exception:
                pass
            if self._poll_after_id is not None:
                try:
                    self.after_cancel(self._poll_after_id)
                except Exception:
                    pass
                self._poll_after_id = None
            subprocess.Popen(cmd)
            self._run_active = True
            self._run_start_ts = time.time()
            self._poll_progress()
            self.var_status.set(status_message)
        except Exception as e:
            self._run_active = False
            messagebox.showerror("起動失敗", f"起動に失敗しました: {e}")

    def _read_progress_state(self):
        path = progress_state_path()
        if not os.path.exists(path):
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return None

    def _poll_progress(self):
        if not self._run_active:
            return

        prog = self._read_progress_state()
        if prog:
            status = prog.get("status", "running")
            completed = int(prog.get("completed", 0) or 0)
            total = int(prog.get("total", 0) or 0)
            current_job = prog.get("current_job")

            if total > 0:
                self.pb["maximum"] = total
                self.pb["value"] = min(completed, total)
            else:
                self.pb["maximum"] = 100
                self.pb["value"] = 0

            if self._run_start_ts:
                elapsed = time.time() - self._run_start_ts
                mm = int(elapsed // 60)
                ss = int(elapsed % 60)
                self.var_time.set(f"経過 {mm:02d}:{ss:02d}")

            if status == "running":
                self._saw_running_for_run = True
                if current_job:
                    idx = current_job.get("index")
                    name = current_job.get("name", "")
                    total_jobs = total if total > 0 else "?"
                    self.var_status.set(f"実行中: {idx}/{total_jobs} {name}")
                else:
                    self.var_status.set("実行中…")
            elif status == "stopped":
                self.var_status.set("停止しました")
            elif status == "blocked":
                self.var_status.set("ブロック検知で停止しました")
            else:
                self.var_status.set("完了しました")

            if status in ("done", "stopped", "blocked", "error"):
                if not self._beeped_for_run and self._last_run_trigger == "manual" and self._saw_running_for_run:
                    self._beeped_for_run = True
                    self.after(0, self._notify_manual_run_done)
                self._run_active = False
                self._poll_after_id = None
                return

        self._poll_after_id = self.after(1000, self._poll_progress)

    def toggle_log_visibility(self):
        self.var_showlog.set(not self.var_showlog.get())
        self._toggle_log()

    def _toggle_log(self):
        if self.var_showlog.get():
            self.frame_log.pack(fill="both", expand=False, padx=10, pady=(0,10))
        else:
            self.frame_log.pack_forget()

    def _clear_caches(self):
        self.cache_day_snapshot.clear()
        self.cache_gid_series.clear()

    def _on_root_configure(self, event=None):
        if event is not None and event.widget is not self:
            return
        size = (self.winfo_width(), self.winfo_height())
        if self._last_root_size == size:
            return
        self._last_root_size = size
        if self._resize_after_id is not None:
            try:
                self.after_cancel(self._resize_after_id)
            except Exception:
                pass
        if _detail_log_enabled():
            self.log("[INFO] ui: resize event debounced")
        self._resize_after_id = self.after(120, self._apply_tree_layout)

    def _apply_tree_layout(self):
        if self._resize_after_id is not None:
            try:
                self.after_cancel(self._resize_after_id)
            except Exception:
                pass
            self._resize_after_id = None
        if not self.winfo_exists():
            return
        t0 = time.perf_counter()
        try:
            self._update_tree_columns()
        finally:
            if _detail_log_enabled():
                dt_ms = (time.perf_counter() - t0) * 1000
                self.log(
                    f"[INFO] ui: apply_layout width={self.winfo_width()} height={self.winfo_height()} "
                    f"dt_ms={dt_ms:.1f}"
                )

    def _update_tree_columns(self):
        if not self.tree.winfo_exists():
            return
        available = self.tree.winfo_width()
        if available <= 1:
            return
        base = self._tree_base_widths
        total = sum(base.values())
        if total <= 0:
            return
        usable = max(available - 2, 1)
        new_widths = {}
        if usable >= total:
            extra = usable - total
            used = 0
            for idx, col in enumerate(self._tree_cols):
                if idx == len(self._tree_cols) - 1:
                    new_widths[col] = base[col] + (extra - used)
                else:
                    add = int(extra * (base[col] / total))
                    new_widths[col] = base[col] + add
                    used += add
        else:
            scale = usable / total
            for col in self._tree_cols:
                min_w = self._tree_min_widths.get(col, 20)
                new_widths[col] = max(min_w, int(base[col] * scale))
            over = sum(new_widths.values()) - usable
            if over > 0:
                last = self._tree_cols[-1]
                min_last = self._tree_min_widths.get(last, 20)
                new_widths[last] = max(min_last, new_widths[last] - over)
        for col in self._tree_cols:
            self.tree.column(col, width=new_widths[col], stretch=False)

    def log(self, s: str):
        """
        精密ログ:
        - PowerShell/コンソールに必ず出す
        - score_data/logs に保存
        - GUIの「詳細ログ」ONなら画面にも出す（スレッド安全）
        """
        txt = s if isinstance(s, str) else str(s)
        # level推定（既存の [INFO]/[WARN]/[ERR] を維持）
        m = re.match(r"\[(INFO|WARN|ERR|DBG)\]\s*(.*)", txt.strip())
        if m:
            lvl = m.group(1)
            msg0 = m.group(2)
            # 1行目はパースしたmsg0を優先、以降はそのまま
            parts = [msg0] + [ln for ln in txt.splitlines()[1:] if ln.strip() != ""]
        else:
            lvl = "INFO"
            parts = [ln for ln in txt.splitlines() if ln.strip() != ""]
        for ln in parts:
            log_event(lvl, ln)
        if self.var_showlog.get():
            def append():
                try:
                    self.txt.insert("end", txt)
                    if not txt.endswith("\n"):
                        self.txt.insert("end", "\n")
                    self.txt.see("end")
                except Exception:
                    pass
            try:
                self.after(0, append)
            except Exception:
                pass

    # ---------- Presets ----------
    def _refresh_preset_combo(self):
        names = [p["name"] for p in self.presets.get("presets", [])]
        if not names:
            names = ["(なし)"]
        self.combo["values"] = names
        self.combo.set(names[0])
        if self.presets.get("presets"):
            self._apply_preset_to_inputs(names[0], force=True)

    def _refresh_preset_listbox(self):
        if not hasattr(self, "preset_checks_frame"):
            return
        for child in self.preset_checks_frame.winfo_children():
            child.destroy()
        names = [p.get("name", "") for p in self.presets.get("presets", []) if p.get("name")]
        cfg = load_config()
        auto_cfg = cfg.get("auto", {}) or {}
        preset_raw = str(auto_cfg.get("presets", "") or "")
        preset_names = [x.strip() for x in preset_raw.split(",") if x.strip()]
        preset_set = set(preset_names)
        self.preset_vars = {}
        if not names:
            ttk.Label(self.preset_checks_frame, text="(プリセットなし)").pack(anchor="w")
            return
        for name in names:
            var = tk.BooleanVar(value=name in preset_set)
            cb = ttk.Checkbutton(self.preset_checks_frame, text=name, variable=var)
            cb.pack(anchor="w")
            try:
                h = getattr(self, "_preset_mousewheel_handler", None)
                if h:
                    cb.bind("<MouseWheel>", h)
                hl = getattr(self, "_preset_mousewheel_handler_linux", None)
                if hl:
                    cb.bind("<Button-4>", hl)
                    cb.bind("<Button-5>", hl)
            except Exception:
                pass
            self.preset_vars[name] = var

    def _select_presets_by_name(self, names):
        if not hasattr(self, "preset_vars"):
            return
        name_set = set(names)
        for name, var in self.preset_vars.items():
            var.set(name in name_set)

    def _select_all_presets(self, select: bool):
        if not hasattr(self, "preset_vars"):
            return
        for var in self.preset_vars.values():
            var.set(select)

    def _get_selected_presets(self):
        if not hasattr(self, "preset_vars"):
            return []
        return [name for name, var in self.preset_vars.items() if var.get()]

    def _mark_manual_url(self):
        self._manual_url_lock = True

    def _mark_manual_max(self):
        self._manual_max_lock = True

    def _apply_preset_to_inputs(self, name: str, force: bool=False):
        if not name or name == "(なし)":
            return
        for p in self.presets.get("presets", []):
            if p.get("name") == name:
                url_val = p.get("url", "")
                max_val = str(p.get("max", 100))
                cur_url = self.ent_url.get()
                cur_max = self.ent_max.get()
                if force or (not self._manual_url_lock) or (not cur_url):
                    self.ent_url.delete(0, "end")
                    self.ent_url.insert(0, url_val)
                    self._manual_url_lock = False
                if force or (not self._manual_max_lock) or (not cur_max):
                    self.ent_max.delete(0, "end")
                    self.ent_max.insert(0, max_val)
                    self._manual_max_lock = False
                self._last_applied_preset = name
                return

    def on_preset_selected(self):
        name = (self.combo.get() or "").strip()
        self._apply_preset_to_inputs(name)

    def apply_preset(self):
        """プリセット選択からキューを一括設定（選択は記憶）。"""
        ps = self.presets.get("presets", [])
        if not ps:
            messagebox.showinfo("プリセットなし", "プリセットがありません")
            return

        selected = self._get_selected_presets()
        if not selected:
            messagebox.showinfo("未選択", "左のプリセット一覧から1つ以上選択してください")
            return

        self.presets["queue_selected"] = selected
        save_presets(self.presets)

        self.jobs.clear()
        for nm in selected:
            p = next((x for x in ps if x.get("name") == nm), None)
            if not p:
                continue
            url = (p.get("url") or "").strip()
            mx = int(p.get("max", 100) or 100)
            if url:
                self.jobs.append(Job(name=nm, url=url, max_items=mx))
        self._render_queue()

    def save_selected_presets_to_auto(self):
        selected = self._get_selected_presets()
        if not selected:
            messagebox.showinfo("未選択", "左のプリセット一覧から1つ以上選択してください")
            return
        cfg = load_config()
        auto_cfg = cfg.get("auto", {}) or {}
        auto_cfg["presets"] = ",".join(selected)
        cfg["auto"] = auto_cfg
        save_config(cfg)
        messagebox.showinfo("設定保存", "選択したプリセットを auto.presets に保存しました。")

    def upsert_preset(self):
        name = (self.combo.get() or "").strip()
        url = self.ent_url.get().strip()
        try:
            mx = int(self.ent_max.get().strip())
        except:
            messagebox.showerror("入力エラー","MAXは数字で入れてください")
            return
        if not name or name == "(なし)":
            name = f"Preset {len(self.presets.get('presets',[]))+1}"
        if not url:
            messagebox.showerror("入力エラー","URLが空です")
            return

        ps = self.presets.get("presets", [])
        for p in ps:
            if p["name"] == name:
                p["url"] = url
                p["max"] = mx
                save_presets(self.presets)
                self._refresh_preset_combo()
                self._refresh_preset_listbox()
                self.combo.set(name)
                messagebox.showinfo("保存","プリセットを更新しました")
                return

        ps.append({"name": name, "url": url, "max": mx})
        self.presets["presets"] = ps
        save_presets(self.presets)
        self._refresh_preset_combo()
        self._refresh_preset_listbox()
        self.combo.set(name)
        messagebox.showinfo("保存","プリセットを追加しました")

    def delete_preset(self):
        name = (self.combo.get() or "").strip()
        ps = self.presets.get("presets", [])
        hit = None
        for i,p in enumerate(ps):
            if p["name"] == name:
                hit = i
                break
        if hit is None:
            return
        if messagebox.askyesno("削除", f"プリセット「{name}」を削除しますか？"):
            ps.pop(hit)
            self.presets["presets"] = ps
            save_presets(self.presets)
            self._refresh_preset_combo()
            self._refresh_preset_listbox()

    # ---------- Queue ----------
    def add_to_queue(self):
        url = self.ent_url.get().strip()
        if not url:
            messagebox.showerror("入力エラー","URLが空です")
            return
        try:
            mx = int(self.ent_max.get().strip())
        except:
            messagebox.showerror("入力エラー","MAXは数字で入れてください")
            return
        name = (self.combo.get() or "").strip()
        if not name or name == "(なし)":
            name = (urlparse(url).path or "job")[:25]
        job = Job(name=name, url=url, max_items=mx)
        self.jobs.append(job)
        self._render_queue()

    def _render_queue(self):
        self.lb_queue.delete(0, "end")
        for i, j in enumerate(self.jobs, 1):
            self.lb_queue.insert("end", f"{i}. {j.name} | MAX={j.max_items}")

    def move_queue(self, delta):
        sel = self.lb_queue.curselection()
        if not sel: return
        i = sel[0]
        j = i + delta
        if j < 0 or j >= len(self.jobs): return
        self.jobs[i], self.jobs[j] = self.jobs[j], self.jobs[i]
        self._render_queue()
        self.lb_queue.selection_set(j)

    def remove_queue(self):
        sel = self.lb_queue.curselection()
        if not sel: return
        i = sel[0]
        self.jobs.pop(i)
        self._render_queue()

    def clear_queue(self):
        self.jobs.clear()
        self._render_queue()

    # ---------- Run/Stop ----------
    def start_run(self):
        if self._run_active:
            messagebox.showinfo("実行中", "すでに実行中です")
            return
        preset_names = self._get_selected_presets()
        if not preset_names and not self.jobs:
            messagebox.showinfo("未選択", "左のプリセット一覧から1つ以上選択するか、キューにURLを追加してください")
            return
        clear_stop_flag()
        self.pb["value"] = 0
        self.pb["maximum"] = 100
        self.var_status.set("起動中…")
        self.var_time.set("")

        headless = None
        payload = self._build_job_payload(
            trigger="manual",
            preset_names=preset_names,
            jobs=self.jobs if self.jobs else None,
            headless=headless,
            include_auto_flags=True,
        )
        self._launch_job(payload, status_message="手動実行を起動しました")

    def stop_run(self):
        write_stop_flag("manual_stop")
        self.var_status.set("停止要求…（安全に止めています）")

    def _ui_status(self, msg, cur, total, start_all, done=False):
        def fmt_time():
            elapsed = time.time() - start_all
            mm = int(elapsed // 60)
            ss = int(elapsed % 60)
            return f"経過 {mm:02d}:{ss:02d}"
        def upd():
            self.var_status.set(msg)
            self.var_time.set(fmt_time())
            if total > 0:
                self.pb["maximum"] = total
                self.pb["value"] = cur
        self.after(0, upd)

    # --- Playwright起動（モード別） ---
    def _make_browser_context(self, p, headless: bool, start_minimized: bool):
        args = [
            "--disable-blink-features=AutomationControlled",
            "--disable-dev-shm-usage",
            "--no-sandbox",
            "--disable-features=site-per-process",
        ]
        if (not headless) and start_minimized:
            args.append("--start-minimized")

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
        profile_dir = PROFILE_DIR
        if profile_dir and os.path.isdir(profile_dir):
            context = p.chromium.launch_persistent_context(
                user_data_dir=profile_dir,
                headless=headless,
                args=args,
                **context_opts,
            )
            browser = context.browser
        else:
            browser = p.chromium.launch(headless=headless, args=args)
            context = browser.new_context(**context_opts)
        context.add_init_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined});")
        # --- 通信高速化：不要なリソースをブロック（画像/フォント/トラッカー） ---
        if ENABLE_NET_BLOCK:
            def _route_handler(route, request):
                try:
                    rt = request.resource_type
                    url = request.url.lower()
                    if rt in BLOCK_RESOURCE_TYPES:
                        return route.abort()
                    for s in BLOCK_URL_SUBSTR:
                        if s in url:
                            return route.abort()
                except Exception:
                    pass
                return route.continue_()
            try:
                context.route("**/*", _route_handler)
            except Exception:
                pass


        page = context.new_page()
        page.set_default_navigation_timeout(NAV_TIMEOUT_MS)

        name_page = context.new_page()
        name_page.set_default_navigation_timeout(NAV_TIMEOUT_MS)

        return browser, context, page, name_page

    def _run_all_jobs_with_mode(self, p, start_all, headless: bool, start_minimized: bool=False):
        browser, context, page, name_page = self._make_browser_context(p, headless=headless, start_minimized=start_minimized)
        ok_all = True
        try:
            total_jobs = len(self.jobs)
            for job_i, job in enumerate(self.jobs, 1):
                if self.stop_evt.is_set():
                    break
                self._ui_status(f"URL {job_i}/{total_jobs} 実行開始…  {job.name}", 0, 100, start_all)

                results_sorted = self._run_one_job(page, name_page, job, start_all, job_i, total_jobs)
                if results_sorted is None:
                    ok_all = False
                    continue

                # ★合算（スコア0は除外）
                self.results.extend([r for r in results_sorted if r is not None])
                self.results = self.sort_rows(self.results)
                self.after(0, self.populate_tree)

            # run全体保存
            try:
                run_ts = os.path.basename(self.run_dir).replace("run_","")
                cfg2 = load_config()
                save_run_outputs(self.run_dir, run_ts, self.results, cfg=cfg2)

                # 人間用の軽いサマリ（上位50件）
                lines = []
                mode_s = "headless" if headless else "headful"
                lines.append(f"mode={mode_s}")
                lines.append(f"jobs={total_jobs} results={len(self.results)}")
                lines.append("rank	score	bd	Δpop	conf	bell	maru	tel	name")
                for i, r in enumerate(self.results[:50], 1):
                    st = r.get("stats", {}) or {}
                    ds = self._format_delta(r)
                    conf = int(self._get_confidence_value(r) or 0)
                    sc = (r.get("score", 0) or 0) * 100.0
                    bd = (r.get("big_score", r.get("score",0)) or 0) * 100.0
                    bell = int(st.get("bell", 0) or 0)
                    maru = int(st.get("maru", 0) or 0)
                    tel  = int(st.get("tel", 0) or 0)
                    name = r.get("name","")
                    lines.append(f"{i}	{sc:0.1f}	{bd:0.1f}	{ds}	{conf}	{bell}	{maru}	{tel}	{name}")
                write_run_file(self.run_dir, "summary.txt", "\n".join(lines))
                self._clear_caches()
            except Exception as e:
                self.log(f"[WARN] save_run_outputs failed: {e}\n")

            return ok_all
        finally:
            try:
                context.close()
            except Exception:
                pass
            try:
                if browser:
                    browser.close()
            except Exception:
                pass

            set_current_run_dir(None)
    # --- 1URL分 ---
    def _run_one_job(self, page, name_page, job: Job, start_all, job_i, total_jobs):
        list_url = job.url
        max_items = job.max_items
        t_job0 = time.perf_counter()
        t_list0 = time.perf_counter()

        store_base = store_base_from_list_url(list_url)

        girl_ids = []
        girl_name = {}
        visited = set()
        cur = list_url

        while len(girl_ids) < max_items and not self.stop_evt.is_set():
            self._ui_status(f"URL {job_i}/{total_jobs} 一覧取得中… 取得 {len(girl_ids)}/{max_items}", len(girl_ids), max_items, start_all)

            if cur in visited:
                self.log(f"[WARN] 一覧URLループ検知: {cur}\n")
                break
            visited.add(cur)

            if not goto_retry(page, cur):
                self.log(f"[ERR] 一覧ページへ遷移失敗: {cur}\n")
                break

            try:
                page.wait_for_load_state("networkidle", timeout=15000)
            except Exception:
                pass

            try:
                page.wait_for_selector("a[href*='girlid-']", timeout=12000)
            except Exception:
                self.log(f"[WARN] girlidリンクが見つかりません: {cur}\n")

            page.wait_for_timeout(AFTER_GOTO_WAIT_MS)

            need = max_items - len(girl_ids)
            pairs = collect_girls_from_list(page, need)

            for gid, name in pairs:
                if gid not in girl_name:
                    girl_name[gid] = name
                if gid not in girl_ids:
                    girl_ids.append(gid)
                    if len(girl_ids) >= max_items:
                        break

            nxt = get_next_list_url(page)
            if not nxt:
                break
            cur = nxt

        if not girl_ids:
            self.log(f"[ERR] girl_id が 0 件でした（headless/最小化でDOMが変化の可能性）: {list_url}\n")
            return None

        if self.stop_evt.is_set():
            return None

        collected = []
        t_list1 = time.perf_counter()
        self.log(f"[INFO] 一覧取得: preset={job.name} ids={len(girl_ids)} time={(t_list1-t_list0):.2f}s\n")
        failures = 0
        for i, gid in enumerate(girl_ids, 1):
            if self.stop_evt.is_set():
                break

            name = girl_name.get(gid, "（名前不明）")
            detail_url = f"{store_base}/girlid-{gid}/"
            res_url = f"{store_base}/A6ShopReservation/?girl_id={gid}"

            self._ui_status(f"URL {job_i}/{total_jobs} 予約集計中… {i}/{len(girl_ids)}  {name}", i, len(girl_ids), start_all)

            if not goto_retry(page, res_url, preset=job.name, gid=gid):
                failures += 1
                self.log(f"[WARN] 予約ページ遷移失敗: {res_url}\n")
                continue

            try:
                page.wait_for_load_state("networkidle", timeout=15000)
            except Exception:
                pass

            page.wait_for_timeout(AFTER_GOTO_WAIT_MS)

            if (not name) or (name == "（名前不明）"):
                nm = try_get_name_from_res_page(page)
                if nm:
                    name = nm
                    girl_name[gid] = nm

            if (not name) or (name == "（名前不明）"):
                nm2 = try_get_name_from_detail_page(name_page, detail_url)
                if nm2:
                    name = nm2
                    girl_name[gid] = nm2

            def iframe_progress(waited, total):
                self._ui_status(
                    f"URL {job_i}/{total_jobs} iframe待ち… {i}/{len(girl_ids)} {int(waited/1000)}s/{int(total/1000)}s  {name}",
                    i, len(girl_ids), start_all
                )

            t_cast0 = time.perf_counter()
            stats, frame_url = count_calendar_stats_by_slots(page, self.stop_evt, iframe_progress, preset=job.name, gid=gid)
            if not stats or not stats.get("ok"):
                failures += 1
                reason = stats.get("reason") if isinstance(stats, dict) else "iframe not found"
                t_cast1 = time.perf_counter()
                self.log(f"[WARN] cast {i}/{len(girl_ids)} gid={gid} fail_t={(t_cast1-t_cast0):.2f}s\n")
                self.log(f"[WARN] 集計失敗: {name} / {gid} / reason={reason}\n")
                continue

            t_cast1 = time.perf_counter()
            if (i % 5) == 0:
                try:
                    bell = int(stats.get("bell",0) or 0)
                    maru = int(stats.get("maru",0) or 0)
                    tel  = int(stats.get("tel",0) or 0)
                except Exception:
                    bell = maru = tel = 0
                self.log(f"[INFO] cast {i}/{len(girl_ids)} gid={gid} t={(t_cast1-t_cast0):.2f}s bell={bell} maru={maru} tel={tel}\n")
            row = {
                "gid": gid,
                "name": name,
                "stats": stats,
                "detail": detail_url,
                "res": res_url,
                "frame": frame_url,
            }
            collected.append(row)

        if not collected:
            self.log(f"[ERR] 集計結果が 0 件でした: {list_url}\n")
            return None

        max_bell = max((r["stats"].get("bell", 0) or 0) for r in collected)

        # 前回スナップショットと比較（Delta / Confidence）しつつ、今回スコア算出
        prev_rows = []
        hist_cache = {}
        for r in collected:
            gid = r.get("gid","")
            prev = load_state_snapshot(gid) if gid else None
            prev_stats = (prev.get("stats") if isinstance(prev, dict) else None) if prev else None
            r["prev_stats"] = prev_stats
            r["delta"] = calc_delta_popularity(prev_stats, r["stats"])
            stats_by_date = None
            if isinstance(r.get("stats"), dict):
                stats_by_date = r["stats"].get("stats_by_date")
            if isinstance(stats_by_date, dict):
                r["stats_by_date"] = stats_by_date

            # 信頼度（サイト側評価：キャスト評価に加えない）
            diag = {
                "iframe_ok": True,
                "time_rows": r["stats"].get("time_rows", 0) or 0,
                "max_cols": r["stats"].get("max_cols", 0) or 0,
                "other_ratio": r["stats"].get("other_ratio", None),
                "suspicious_hit": bool(r["stats"].get("suspicious_hit")),
                "min_rows": 20,
                "min_cols": 7,
                "max_other_ratio": 0.35,
            }
            conf, issues = calc_site_confidence(diag)
            r["site_confidence"] = conf
            r["site_issues"] = issues

            r["score"] = calc_score(r["stats"], max_bell)
            # ビッグデータ版（過去履歴を使って安定化したスコア）
            try:
                if gid:
                    if gid in hist_cache:
                        hist = hist_cache[gid]
                    else:
                        hist = load_history(gid, limit=200)
                        hist_cache[gid] = hist
                else:
                    hist = []
                big_score, detail = _calc_bigdata_score_detail(r.get("stats", {}), hist, cur_stats_by_date=stats_by_date)
                r["big_score"] = big_score
                r["big_score_old"] = detail.get("big_score_old")
                r["bd_detail"] = detail
                r["bd_level"] = detail.get("bd_level")
                r["bd_trust"] = detail.get("bd_trust")
                r["bd_days"] = detail.get("bd_days")
                r["bd_model"] = _BD_MODEL_NAME
                r["bd_model_version"] = detail.get("bd_model_version")
                rank_raw, rank_detail = _calc_rank_score_detail(r.get("stats", {}), hist, cur_stats_by_date=stats_by_date)
                r["quality_score"] = rank_detail.get("quality_score")
                r["momentum_score"] = rank_detail.get("momentum_score")
                r["rank_score_raw"] = rank_detail.get("rank_score_raw")
                r["rank_model_version"] = rank_detail.get("rank_model_version")
                r["rank_detail"] = rank_detail
            except Exception:
                r["big_score"] = r.get("score",0)

            if prev:
                prev_rows.append(prev)

        _assign_rank_percentiles(collected)
        save_job_outputs(self.run_dir, job, job_i, collected, prev_rows)
        for r in collected:
            gid = r.get("gid","")
            if not gid:
                continue
            stats_by_date = r.get("stats_by_date") if isinstance(r.get("stats_by_date"), dict) else None
            # 履歴追記（フォルダ内完結：score_data/history）
            try:
                run_ts = os.path.basename(self.run_dir).replace("run_","")
                append_history(gid, {
                    "ts": _now_ts(),
                    "run_ts": run_ts,
                    "job": getattr(job, "name", ""),
                    "gid": gid,
                    "name": r.get("name",""),
                    "score": r.get("score",0),
                    "big_score": r.get("big_score", r.get("score",0)),
                    "big_score_old": r.get("big_score_old"),
                    "delta": r.get("delta"),
                    "site_confidence": r.get("site_confidence",0),
                    "stats": r.get("stats",{}),
                    "stats_by_date": stats_by_date,
                    "bd_model": r.get("bd_model"),
                    "bd_model_version": r.get("bd_model_version"),
                    "bd_level": r.get("bd_level"),
                    "bd_trust": r.get("bd_trust"),
                    "bd_days": r.get("bd_days"),
                    "quality_score": r.get("quality_score"),
                    "momentum_score": r.get("momentum_score"),
                    "rank_score_raw": r.get("rank_score_raw"),
                    "rank_percentile": r.get("rank_percentile"),
                    "rank_model_version": r.get("rank_model_version"),
                    "rank_detail": r.get("rank_detail"),
                })
            except Exception as e:
                self.log(f"[ERR] append_history failed gid={gid} err={e}\n")

            # 状態保存（次回比較用）
            snap = {
                "ts": _now_ts(),
                "gid": gid,
                "name": r.get("name",""),
                "stats": r.get("stats",{}),
                "score": r.get("score",0),
                "big_score": r.get("big_score", r.get("score",0)),
                "big_score_old": r.get("big_score_old"),
                "site_confidence": r.get("site_confidence",0),
                "quality_score": r.get("quality_score"),
                "momentum_score": r.get("momentum_score"),
                "rank_score_raw": r.get("rank_score_raw"),
                "rank_percentile": r.get("rank_percentile"),
                "rank_model_version": r.get("rank_model_version"),
            }
            save_state_snapshot(gid, snap)

        t_job1 = time.perf_counter()
        ok_cnt = len(collected)
        self.log(f"[INFO] job done preset={job.name} ok={ok_cnt} fail={failures} total_time={(t_job1-t_job0):.2f}s\n")
        rows = self.sort_rows(collected)
        return rows

    # ---------- Tree / UX ----------
    def _clear_tree(self):
        for it in self.tree.get_children():
            self.tree.delete(it)

    def sort_rows(self, rows):
        keyname = self.combo_sort.get()
        if keyname == "ランキング(新)":
            rows.sort(
                key=lambda r: (
                    r.get("rank_percentile") is not None,
                    r.get("rank_percentile") if r.get("rank_percentile") is not None else r.get("rank_score_raw", 0),
                ),
                reverse=True,
            )
        elif keyname == "ビッグデータ":
            rows.sort(key=lambda r: (r.get("big_score") if r.get("big_score") is not None else r.get("score",0)), reverse=True)
        elif keyname == "bell率":
            rows.sort(key=lambda r: (r["stats"].get("bell_rate_bookable") is not None, r["stats"].get("bell_rate_bookable") or 0), reverse=True)
        elif keyname == "ベル数":
            rows.sort(key=lambda r: (r["stats"].get("bell",0) or 0), reverse=True)
        elif keyname == "空き(○)":
            rows.sort(key=lambda r: (r["stats"].get("maru",0) or 0), reverse=True)
        elif keyname == "TEL多い":
            rows.sort(key=lambda r: (r["stats"].get("tel",0) or 0), reverse=True)
        elif keyname == "bookable多い":
            rows.sort(key=lambda r: (r["stats"].get("bookable_slots",0) or 0), reverse=True)
        else:
            rows.sort(key=lambda r: r.get("score",0), reverse=True)
        return rows

    # ---------- 閲覧（通信なし） ----------
    def _get_selected_preset_name(self) -> str:
        n = (self.combo.get() or "").strip()
        if not n or n == "(なし)":
            return ""
        return n

    def _list_run_dirs(self):
        if not os.path.isdir(RUNS_DIR):
            return []
        try:
            run_dirs = [d for d in os.listdir(RUNS_DIR) if d.startswith("run_") and os.path.isdir(os.path.join(RUNS_DIR, d))]
            run_dirs.sort(reverse=True)
            return run_dirs
        except Exception:
            return []

    def _list_presets_in_run(self, run_dir: str):
        job_dir = os.path.join(run_dir, "jobs")
        if not os.path.isdir(job_dir):
            return []
        presets = []
        pat = re.compile(r"^\d{2}_(.+)_current\.json$")
        try:
            for fn in os.listdir(job_dir):
                if not fn.endswith("_current.json"):
                    continue
                path = os.path.join(job_dir, fn)
                preset = None
                data = self._load_json_safe(path)
                if isinstance(data, list):
                    for row in data:
                        if isinstance(row, dict) and row.get("preset"):
                            preset = str(row.get("preset"))
                            break
                if preset is None:
                    m = pat.match(fn)
                    if m:
                        preset = m.group(1)
                if preset and preset not in presets:
                    presets.append(preset)
        except Exception:
            return presets
        return presets

    def _pick_preset_in_run(self, run_dir: str, default_preset: str):
        presets = self._list_presets_in_run(run_dir)
        if not presets:
            return default_preset
        win = tk.Toplevel(self)
        win.title("プリセット選択")
        win.geometry("420x360")
        ttk.Label(win, text="このrun内のプリセットを選んでください").pack(anchor="w", padx=10, pady=(10,0))
        lb = tk.Listbox(win, height=12)
        lb.pack(fill="both", expand=True, padx=10, pady=10)
        for name in presets:
            lb.insert("end", name)
        try:
            idx = presets.index(default_preset)
        except ValueError:
            idx = 0
        lb.selection_set(idx)
        chosen = {"preset": None}
        btns = ttk.Frame(win); btns.pack(fill="x", padx=10, pady=(0,10))
        def ok():
            try:
                i = int(lb.curselection()[0])
            except Exception:
                return
            chosen["preset"] = presets[i]
            win.destroy()
        def cancel():
            win.destroy()
        ttk.Button(btns, text="開く", command=ok).pack(side="left")
        ttk.Button(btns, text="キャンセル", command=cancel).pack(side="left", padx=8)
        win.grab_set()
        self.wait_window(win)
        return chosen["preset"]

    def _find_preset_file_in_run(self, run_dir: str, preset_name: str):
        safe = _safe_name(preset_name)
        safe_norm = _safe_name(normalize_preset_name(preset_name))
        job_dir = os.path.join(run_dir, "jobs")
        if not os.path.isdir(job_dir):
            return None
        pat = re.compile(rf"^\d{{2}}_{re.escape(safe)}_current\.json$")
        pat_norm = re.compile(rf"^\d{{2}}_{re.escape(safe_norm)}_current\.json$")
        try:
            for fn in os.listdir(job_dir):
                if pat.match(fn) or pat_norm.match(fn):
                    return os.path.join(job_dir, fn)
        except Exception:
            return None
        return None

    def _load_json_safe(self, path: str):
        try:
            with open(path, "r", encoding="utf-8") as r:
                return json.load(r)
        except Exception:
            return None

    def _load_rows_for_preset(self, run_dir: str, preset_name: str):
        preset_path = self._find_preset_file_in_run(run_dir, preset_name)
        rows = None
        if preset_path:
            data = self._load_json_safe(preset_path)
            if isinstance(data, list):
                rows = data
            else:
                rows = None
        if rows is not None:
            return rows, preset_path, False

        run_ts = os.path.basename(run_dir).replace("run_", "")
        messagebox.showinfo("プリセットファイルなし", f"このpresetの個別ファイルが見つかりません。\nジョブ失敗 or 保存失敗の可能性があります。\n\npreset={preset_name}\nrun={run_ts}")

        fallback_path = os.path.join(run_dir, "all_current.json")
        data = self._load_json_safe(fallback_path)
        if isinstance(data, list):
            filtered = [
                r for r in data
                if isinstance(r, dict)
                and (
                    str(r.get("preset","")) == preset_name
                    or normalize_preset_name(str(r.get("preset",""))) == normalize_preset_name(preset_name)
                )
            ]
            return filtered, fallback_path, True
        return [], fallback_path, True

    def _pick_run_dir_dialog(self, preset_name: str, title: str):
        run_dirs = self._list_run_dirs()
        if not run_dirs:
            messagebox.showinfo("データなし", f"run データが見つかりません\n\n※まず一度キュー実行して run を作ってください")
            return None
        win = tk.Toplevel(self)
        win.title(title)
        win.geometry("520x360")
        ttk.Label(win, text=f"プリセット: {preset_name}").pack(anchor="w", padx=10, pady=(10,0))
        lb = tk.Listbox(win, height=12)
        lb.pack(fill="both", expand=True, padx=10, pady=10)
        for run_name in run_dirs:
            lb.insert("end", run_name.replace("run_", ""))
        lb.selection_set(0)
        chosen = {"dir": None}
        btns = ttk.Frame(win); btns.pack(fill="x", padx=10, pady=(0,10))
        def ok():
            try:
                i = int(lb.curselection()[0])
            except Exception:
                return
            chosen["dir"] = os.path.join(RUNS_DIR, run_dirs[i])
            win.destroy()
        def cancel():
            win.destroy()
        ttk.Button(btns, text="開く", command=ok).pack(side="left")
        ttk.Button(btns, text="キャンセル", command=cancel).pack(side="left", padx=8)
        if "前回結果" in title:
            def reload_and_close():
                self._reload_prev_result_from_window()
                win.destroy()
            ttk.Button(btns, text="リロード", command=reload_and_close, width=8).pack(side="right")
        win.grab_set()
        self.wait_window(win)
        return chosen["dir"]

    def _reload_prev_result_from_window(self):
        prev_results = self.results
        self.results = []
        self._preload_results_from_latest_daily_snapshot()
        if not self.results:
            self.results = prev_results
            if prev_results:
                self.populate_tree()
            self.var_status.set("前回結果の再読み込みに失敗しました")
        else:
            self.var_status.set("前回結果を再読み込みしました")

    def view_prev_preset(self):
        """プリセット単位で過去run（jobs/*_current.json）を読み込み、結果テーブルに表示。"""
        preset = self._get_selected_preset_name()
        if not preset:
            messagebox.showinfo("プリセット", "プリセットを選択してください")
            return
        run_dir = self._pick_run_dir_dialog(preset, "前回結果を見る（run選択）")
        if not run_dir:
            return
        preset = self._pick_preset_in_run(run_dir, preset)
        if not preset:
            return
        rows, path, used_fallback = self._load_rows_for_preset(run_dir, preset)
        self.results = rows or []
        self.results = self.sort_rows(self.results)
        self.populate_tree()
        run_ts = os.path.basename(run_dir).replace("run_", "")
        self.var_status.set(f"閲覧: {preset} / {run_ts}")
        self.var_time.set("閲覧モード")
        self.log(f"[INFO] VIEW prev preset={preset} file={path} fallback={used_fallback}")

    def view_history_summary(self):
        """プリセット単位の履歴サマリ（BD/件数/期間/上位）を表示。"""
        preset = self._get_selected_preset_name()
        if not preset:
            messagebox.showinfo("プリセット", "プリセットを選択してください")
            return
        run_dir = self._pick_run_dir_dialog(preset, "履歴サマリ（run選択）")
        if not run_dir:
            return
        preset = self._pick_preset_in_run(run_dir, preset)
        if not preset:
            return
        rows, path, used_fallback = self._load_rows_for_preset(run_dir, preset)
        # 集計
        rows = rows or []
        gids = [str(x.get("gid","")) for x in rows if x.get("gid") is not None]
        cur_rates = []
        cur_bds = []
        cur_scores = []
        cur_confs = []
        suspicious = 0
        span_new = None
        span_old = None
        for r0 in rows:
            st = r0.get("stats") or {}
            rt = st.get("bell_rate_bookable")
            if isinstance(rt, (int,float)):
                cur_rates.append(float(rt))
            bd = r0.get("big_score")
            if isinstance(bd, (int,float)):
                cur_bds.append(float(bd))
            sc = r0.get("score")
            if isinstance(sc, (int,float)):
                cur_scores.append(float(sc))
            cf = r0.get("site_confidence")
            if isinstance(cf, (int,float)):
                cur_confs.append(float(cf))
            issues = r0.get("site_issues") or []
            if issues:
                suspicious += 1
        # 期間（このプリセットに出てくるgidの履歴から最大範囲を推定）
        for gid in gids[:80]:  # 重くしないため上限
            hist = load_history(gid, limit=500)
            if not hist:
                continue
            newest = hist[0].get("ts")
            oldest = hist[-1].get("ts")
            if newest and (span_new is None or newest > span_new):
                span_new = newest
            if oldest and (span_old is None or oldest < span_old):
                span_old = oldest
        run_ts = os.path.basename(run_dir).replace("run_", "")
        # 表示ウィンドウ
        win = tk.Toplevel(self)
        win.title("履歴サマリ")
        win.geometry("760x560")
        txt = tk.Text(win, wrap="none")
        txt.pack(fill="both", expand=True, padx=10, pady=10)
        def avg(a):
            return (sum(a)/len(a)) if a else None
        txt.insert("end", f"プリセット: {preset}\n")
        txt.insert("end", f"run: {run_ts}\n")
        txt.insert("end", f"rows: {len(rows)}\n")
        if span_old or span_new:
            txt.insert("end", f"history span: {span_old or 'N/A'} 〜 {span_new or 'N/A'}\n")
        ar = avg(cur_rates)
        ab = avg(cur_bds)
        asc = avg(cur_scores)
        acf = avg(cur_confs)
        if ar is not None:
            txt.insert("end", f"avg bell_rate(bookable): {ar*100:.2f}%\n")
        if ab is not None:
            txt.insert("end", f"avg BD: {ab*100:.2f}\n")
        if asc is not None:
            txt.insert("end", f"avg Score: {asc*100:.2f}\n")
        if acf is not None:
            txt.insert("end", f"avg Confidence: {acf:.1f}\n")
        txt.insert("end", f"suspicious rows: {suspicious}\n")
        txt.insert("end", "\n--- Top10 BD ---\n")
        top_bd = sorted(rows, key=lambda x: (x.get("big_score") or -1), reverse=True)[:10]
        for i, r0 in enumerate(top_bd, 1):
            name = r0.get("name","?")
            bd = (r0.get("big_score") or 0)*100
            rt = (r0.get("stats") or {}).get("bell_rate_bookable")
            rt_s = "N/A" if rt is None else f"{float(rt)*100:.1f}%"
            txt.insert("end", f"{i:02d}. {bd:6.1f}  rate={rt_s}  {name}\n")
        txt.insert("end", "\n--- Top10 Δpop ---\n")
        top_d = sorted(rows, key=lambda x: (self._get_delta_value(x) or 0), reverse=True)[:10]
        for i, r0 in enumerate(top_d, 1):
            name = r0.get("name","?")
            d = self._get_delta_value(r0)
            d_s = "N/A" if d is None else f"{(d*100):6.1f}"
            txt.insert("end", f"{i:02d}. {d_s}  {name}\n")
        txt.insert("end", "\n--- Bottom10 Δpop ---\n")
        bot_d = sorted(rows, key=lambda x: (self._get_delta_value(x) or 0))[:10]
        for i, r0 in enumerate(bot_d, 1):
            name = r0.get("name","?")
            d = self._get_delta_value(r0)
            d_s = "N/A" if d is None else f"{(d*100):6.1f}"
            txt.insert("end", f"{i:02d}. {d_s}  {name}\n")
        # コピー
        btns = ttk.Frame(win); btns.pack(fill="x", padx=10, pady=(0,10))
        def copy_all():
            try:
                self.clipboard_clear()
                self.clipboard_append(txt.get("1.0","end").strip())
            except Exception:
                pass
        ttk.Button(btns, text="コピー", command=copy_all).pack(side="left")
        ttk.Button(btns, text="閉じる", command=win.destroy).pack(side="left", padx=8)
        txt.configure(state="disabled")
        self.log(f"[INFO] VIEW history_summary preset={preset} file={path} fallback={used_fallback}")

    def _list_daily_dates(self):
        if not os.path.isdir(DAILY_DIR):
            return []
        try:
            days = [d for d in os.listdir(DAILY_DIR) if re.match(r"^\d{4}-\d{2}-\d{2}$", d)]
            days.sort()
            return days
        except Exception:
            return []

    def _load_daily_snapshot(self, day):
        if day in self.cache_day_snapshot:
            return self.cache_day_snapshot[day]
        path = os.path.join(DAILY_DIR, day, "daily_snapshot.json")
        if not os.path.exists(path):
            self.cache_day_snapshot[day] = None
            return None
        try:
            with open(path, "r", encoding="utf-8") as r:
                snap = json.load(r)
            snap = snap if isinstance(snap, dict) else None
            self.cache_day_snapshot[day] = snap
            return snap
        except Exception:
            self.cache_day_snapshot[day] = None
            return None

    def _format_nullable(self, val, fmt):
        if val is None:
            return "N/A"
        try:
            return fmt(val)
        except Exception:
            return str(val)

    def _format_bd_score(self, val):
        return self._format_nullable(val, lambda v: f"{float(v) * 100:.2f}")

    def _format_bd_delta(self, val):
        return self._format_nullable(val, lambda v: f"{float(v) * 100:+.2f}")

    def _format_cast_name(self, name):
        if name is None:
            return "?"
        return str(name)

    def _finish_startup(self):
        self._is_starting = False

    def _preload_results_from_latest_daily_snapshot(self):
        if self.results:
            return
        try:
            days = self._list_daily_dates()
            if not days:
                return
            latest_day = sorted(days)[-1]
            snap = self._load_daily_snapshot(latest_day)
            if not snap:
                return
            rows = snap.get("all_current", []) or []
            if not rows:
                return
            normalized = []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                new_row = dict(row)
                stats = new_row.get("stats")
                if not isinstance(stats, dict):
                    new_row["stats"] = {}
                if not new_row.get("res"):
                    res_url = self._build_res_url_from_row(new_row)
                    if res_url:
                        new_row["res"] = res_url
                if not new_row.get("detail"):
                    detail_url = self._build_profile_url_from_row(new_row)
                    if detail_url:
                        new_row["detail"] = detail_url
                normalized.append(new_row)
            if not normalized:
                return
            self.results = normalized
            self.resort_view()
        except Exception as e:
            try:
                self.log(f"[INFO] preload daily snapshot skipped: {e}")
            except Exception:
                pass

    def _notify_manual_run_done(self):
        if self._is_starting:
            return
        if not self.var_beep.get():
            return
        self._play_beep()

    def _play_beep(self):
        try:
            import winsound
            try:
                winsound.MessageBeep(winsound.MB_OK)
                return
            except Exception:
                try:
                    winsound.Beep(1000, 200)
                    return
                except Exception:
                    pass
        except Exception:
            pass
        try:
            self.bell()
        except Exception:
            pass

    def _format_plain(self, val):
        if val is None:
            return "N/A"
        return str(val)

    def _format_score_percent(self, val):
        return self._format_nullable(val, lambda v: f"{float(v) * 100:.2f}")

    def _format_delta_percent(self, val):
        return self._format_nullable(val, lambda v: f"{float(v) * 100:+.2f}")

    def _format_delta_int(self, val):
        if val is None:
            return "N/A"
        try:
            return f"{int(val):+d}"
        except Exception:
            return str(val)

    def _get_bd_daily_summary(self, snap, day):
        bd = snap.get("bd_daily")
        has_bd_daily = isinstance(bd, dict) and bool(bd)
        if not isinstance(bd, dict):
            bd = {}
        today = snap.get("date", day)
        summary = {
            "date": self._format_plain(today),
            "prev_day": self._format_plain(bd.get("prev_day")),
            "gap_days": self._format_plain(bd.get("gap_days")),
            "avg_big_score": self._format_bd_score(bd.get("avg_big_score")),
            "delta_avg_big_score": self._format_bd_delta(bd.get("delta_avg_big_score")),
            "delta_avg_big_score_per_day": self._format_bd_delta(bd.get("delta_avg_big_score_per_day")),
            "ma3_samples_avg_big_score": self._format_bd_score(bd.get("ma3_samples_avg_big_score")),
            "ma14_samples_avg_big_score": self._format_bd_score(bd.get("ma14_samples_avg_big_score")),
            "ma28_samples_avg_big_score": self._format_bd_score(bd.get("ma28_samples_avg_big_score")),
            "ma56_samples_avg_big_score": self._format_bd_score(bd.get("ma56_samples_avg_big_score")),
            "ma84_samples_avg_big_score": self._format_bd_score(bd.get("ma84_samples_avg_big_score")),
            "ma112_samples_avg_big_score": self._format_bd_score(bd.get("ma112_samples_avg_big_score")),
        }
        return bd, has_bd_daily, summary

    def _build_summary_block(self, parent, title, rows):
        frame = ttk.LabelFrame(parent, text=title)
        frame.pack(side="left", fill="x", expand=True, padx=6, pady=4)
        for idx, (label, value) in enumerate(rows):
            ttk.Label(frame, text=label).grid(row=idx, column=0, sticky="w", padx=6, pady=2)
            ttk.Label(frame, text=value).grid(row=idx, column=1, sticky="w", padx=6, pady=2)
        frame.columnconfigure(1, weight=1)
        return frame

    def _get_display_windows(self, window_value):
        short_windows = [3, 14, 28]
        long_windows = [56, 84, 112]
        return long_windows if self.bd_ma_long_mode.get() else short_windows

    def _format_ma_summary_value(self, window, summary, samples):
        if not window:
            return "N/A"
        key = f"ma{window}_samples_avg_big_score"
        if key in summary:
            val = summary.get(key)
            if val not in (None, ""):
                return val
        values = [v for _, v in samples]
        ma_values = self._calc_moving_average(values, window)
        if not ma_values:
            return "N/A"
        last_val = ma_values[-1]
        if last_val is None:
            return "N/A"
        return self._format_bd_score(last_val)

    def _update_ma_summary_values(self, label_vars, value_vars, summary, samples, window_value):
        window_values = self._get_display_windows(window_value)
        for idx, window in enumerate(window_values):
            label = f"ma{window}_samples_avg_big_score" if window else "ma--_samples_avg_big_score"
            label_vars[idx].set(label)
            value_vars[idx].set(self._format_ma_summary_value(window, summary, samples))

    def _build_ma_summary_block(self, parent, label_vars, value_vars, combo_var):
        frame = ttk.LabelFrame(parent, text="--- MA ---")
        frame.pack(side="left", fill="x", expand=True, padx=6, pady=4)
        label_frame = ttk.Frame(frame)
        label_frame.grid(row=0, column=0, sticky="w", padx=6, pady=2)
        ttk.Label(label_frame, textvariable=label_vars[0]).pack(side="left")
        display_label = ttk.Label(label_frame, text="表示MA")
        display_label.pack(side="left", padx=(8, 0))
        combo = ttk.Combobox(
            label_frame,
            width=4,
            state="disabled",
            textvariable=combo_var,
            values=[str(w) for w in self.bd_ma_windows],
        )
        combo.pack(side="left", padx=(4, 0))
        display_label.pack_forget()
        combo.pack_forget()
        for idx in range(1, 3):
            ttk.Label(frame, textvariable=label_vars[idx]).grid(row=idx, column=0, sticky="w", padx=6, pady=2)
        for idx, value_var in enumerate(value_vars):
            ttk.Label(frame, textvariable=value_var).grid(row=idx, column=1, sticky="w", padx=6, pady=2)
        frame.columnconfigure(1, weight=1)
        return frame

    def _get_avg_big_score_series(self, upto_day):
        days = self._list_daily_dates()
        samples = []
        if days:
            for d in days:
                if d > upto_day:
                    continue
                snap_d = self._load_daily_snapshot(d)
                if not snap_d:
                    continue
                bd_d, _, _ = self._get_bd_daily_summary(snap_d, d)
                val = bd_d.get("avg_big_score") if isinstance(bd_d, dict) else None
                if isinstance(val, (int, float)):
                    samples.append((d, float(val)))
        return samples

    def _build_summary_panel(self, panel, bd, has_bd_daily, summary, day):
        for child in panel.winfo_children():
            child.destroy()
        if not has_bd_daily:
            ttk.Label(panel, text="bd_dailyがありません（古い日付でもN/Aで表示）", foreground="#777").pack(anchor="w")
        elif bd.get("prev_day") is None:
            ttk.Label(panel, text="※prev_dayが無いためdelta系はN/A（サンプル不足）", foreground="#777").pack(anchor="w")
        blocks = ttk.Frame(panel)
        blocks.pack(fill="x")
        self._build_summary_block(blocks, "--- 日付 ---", [
            ("date", summary["date"]),
            ("prev_day", summary["prev_day"]),
            ("gap_days", summary["gap_days"]),
        ])
        self._build_summary_block(blocks, "--- BD Daily ---", [
            ("avg_big_score", summary["avg_big_score"]),
            ("delta_avg_big_score", summary["delta_avg_big_score"]),
            ("delta_avg_big_score_per_day", summary["delta_avg_big_score_per_day"]),
        ])
        ma_label_vars = [
            tk.StringVar(value="ma3_samples_avg_big_score"),
            tk.StringVar(value="ma14_samples_avg_big_score"),
            tk.StringVar(value="ma28_samples_avg_big_score"),
        ]
        ma_value_vars = [
            tk.StringVar(value="N/A"),
            tk.StringVar(value="N/A"),
            tk.StringVar(value="N/A"),
        ]
        self._build_ma_summary_block(blocks, ma_label_vars, ma_value_vars, self.bd_ma_display_var)
        samples = self._get_avg_big_score_series(day)
        self._update_ma_summary_values(ma_label_vars, ma_value_vars, summary, samples, self.bd_ma_display_var.get())
        if self._bd_summary_ma_trace_id:
            self.bd_ma_display_var.trace_remove("write", self._bd_summary_ma_trace_id)
        self._bd_summary_ma_trace_id = self.bd_ma_display_var.trace_add(
            "write",
            lambda *_: self._update_ma_summary_values(
                ma_label_vars,
                ma_value_vars,
                summary,
                samples,
                self.bd_ma_display_var.get(),
            ),
        )

    def _calc_moving_average(self, values, window):
        if window <= 0:
            return []
        count = len(values)
        if count < window:
            return []
        result = [None] * (window - 1)
        running_sum = sum(values[:window])
        result.append(running_sum / window)
        for i in range(window, count):
            running_sum += values[i] - values[i - window]
            result.append(running_sum / window)
        return result

    def _draw_ma_graph(self, canvas, samples):
        canvas.delete("all")
        width = int(canvas["width"])
        height = int(canvas["height"])
        padding = 12
        if not samples:
            canvas.create_text(width // 2, height // 2, text="N/A", fill="#777")
            return
        values = [v for _, v in samples]
        min_v = min(values)
        max_v = max(values)
        span = max_v - min_v if max_v != min_v else 1.0
        count = len(samples)
        usable_w = max(width - padding * 2, 1)
        usable_h = max(height - padding * 2, 1)
        points = []
        x_positions = []
        for i, (_, val) in enumerate(samples):
            x = padding if count == 1 else padding + (usable_w * i / (count - 1))
            y = padding + (usable_h * (1 - (val - min_v) / span))
            points.append((x, y))
            x_positions.append(x)
        if len(points) > 1:
            canvas.create_line(points, fill="#2b6cb0", width=2)
        for x, y in points:
            canvas.create_oval(x - 3, y - 3, x + 3, y + 3, fill="#2b6cb0", outline="")

        ma_colors = {
            3: "#d53f8c",
            14: "#38a169",
            28: "#dd6b20",
            56: "#4a5568",
            84: "#718096",
            112: "#2d3748",
        }
        for window in self.bd_ma_windows:
            var = self.bd_ma_vars.get(window)
            if not var or not var.get():
                continue
            ma_values = self._calc_moving_average(values, window)
            if not ma_values:
                continue
            ma_points = []
            for i, val in enumerate(ma_values):
                if val is None:
                    continue
                y = padding + (usable_h * (1 - (val - min_v) / span))
                ma_points.append((x_positions[i], y))
            if len(ma_points) > 1:
                canvas.create_line(ma_points, fill=ma_colors.get(window, "#555"), width=1)

    def _apply_bd_ma_mode(self, long_mode):
        short_windows = (3, 14, 28)
        long_windows = (56, 84, 112)
        active = long_windows if long_mode else short_windows
        for window in self.bd_ma_windows:
            var = self.bd_ma_vars.get(window)
            if var is not None:
                var.set(window in active)
        self.bd_ma_display_var.set(str(active[0]))

    def _get_cast_score_series(self, gid, upto_day):
        if not gid or not upto_day:
            return []
        cache_key = (str(gid), upto_day)
        cached = self.cache_gid_series.get(cache_key)
        if cached is not None:
            return cached
        days = self._list_daily_dates()
        series = []
        if days:
            for d in reversed(days):
                if d > upto_day:
                    continue
                snap = self._load_daily_snapshot(d)
                if not snap:
                    continue
                rows = snap.get("all_current", []) or []
                val = None
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    if str(row.get("gid", "")) != str(gid):
                        continue
                    val = row.get("big_score", row.get("score"))
                    break
                if isinstance(val, (int, float)):
                    series.append((d, float(val)))
        self.cache_gid_series[cache_key] = series
        return series

    def _build_res_url_from_row(self, row):
        if not isinstance(row, dict):
            return None
        res_url = row.get("res")
        if res_url:
            return res_url
        gid = row.get("gid")
        if not gid:
            return None
        list_url = row.get("list_url") or ""
        if list_url:
            try:
                base = store_base_from_list_url(list_url)
                return f"{base}/A6ShopReservation/?girl_id={gid}"
            except Exception:
                pass
        detail_url = row.get("detail") or ""
        m = re.search(r"^(.+?)/girlid-[^/]+/?$", detail_url)
        if m:
            return f"{m.group(1)}/A6ShopReservation/?girl_id={gid}"
        return None

    def _build_profile_url_from_row(self, row):
        if not isinstance(row, dict):
            return None
        gid = row.get("gid")
        if not gid:
            return None
        detail_url = row.get("detail") or ""
        if detail_url:
            base = re.sub(r"/girlid-[^/]+/?$", "", detail_url.rstrip("/"))
            if base:
                return f"{base}/girlid-{gid}"
        list_url = row.get("list_url") or ""
        if list_url:
            try:
                base = store_base_from_list_url(list_url)
                return f"{base}/girlid-{gid}"
            except Exception:
                return None
        return None

    def view_bd_summary(self):
        days = self._list_daily_dates()
        if not days:
            messagebox.showinfo("BDサマリ", "daily_snapshot.json が見つかりません")
            return

        win = tk.Toplevel(self)
        win.title("BDサマリ")
        win.geometry("1200x700")

        top = ttk.Frame(win)
        top.pack(fill="x", padx=10, pady=(10, 4))
        ttk.Label(top, text="日付").pack(side="left")
        combo = ttk.Combobox(top, width=18, state="readonly", values=sorted(days, reverse=True))
        combo.pack(side="left", padx=8)
        var_hide_copy = tk.BooleanVar(value=True)
        # チェックON=コピー用テキスト(左ペイン)を隠す（デフォルト）
        chk_hide = ttk.Checkbutton(top, text="コピー欄を隠す", variable=var_hide_copy)
        chk_hide.pack(side="left", padx=8)

        main = ttk.Panedwindow(win, orient="horizontal")
        main.pack(fill="both", expand=True, padx=10, pady=(4, 10))

        left = ttk.Frame(main)
        right = ttk.Frame(main)
        main.add(left, weight=2)
        main.add(right, weight=3)

        summary_panel = ttk.Frame(left)
        summary_panel.pack(fill="x", padx=6, pady=(0, 6))

        txt = tk.Text(left, wrap="none")
        txt.pack(fill="both", expand=True, padx=6, pady=(0, 6))

        cast_panel = CastDetailPanel(self, right)
        cast_panel.pack(fill="both", expand=True, padx=6, pady=6)

        def _pane_present(pw, w):
            try:
                return str(w) in (pw.panes() or [])
            except Exception:
                return False

        def apply_copy_visibility():
            # チェックONで左ペイン（コピー用）を非表示にし、別窓(DETAIL)と同じレイアウト感にする
            try:
                if var_hide_copy.get():
                    if _pane_present(main, left):
                        main.forget(left)
                else:
                    if not _pane_present(main, left):
                        try:
                            main.insert(0, left, weight=2)
                        except Exception:
                            main.add(left, weight=2)

                # コピー欄に付随する下部（コピー/閉じる）も同時に隠す
                try:
                    _ = btns  # btns が未作成なら NameError
                    if var_hide_copy.get():
                        try:
                            if btns.winfo_manager():
                                btns.pack_forget()
                        except Exception:
                            pass
                    else:
                        try:
                            if not btns.winfo_manager():
                                btns.pack(fill="x", padx=10, pady=(0, 10))
                        except Exception:
                            pass
                except Exception:
                    pass

            except Exception:
                pass

        chk_hide.configure(command=apply_copy_visibility)
        apply_copy_visibility()


        def render(day):
            snap = self._load_daily_snapshot(day)
            if not snap:
                txt.configure(state="normal")
                txt.delete("1.0", "end")
                txt.insert("end", f"{day}\n\n")
                txt.insert("end", "daily_snapshot.json を読み込めませんでした\n")
                txt.configure(state="disabled")
                cast_panel._render_empty(day, "daily_snapshot.json を読み込めませんでした")
                return

            bd, has_bd_daily, summary = self._get_bd_daily_summary(snap, day)

            txt.configure(state="normal")
            txt.delete("1.0", "end")
            self._build_summary_panel(summary_panel, bd, has_bd_daily, summary, day)
            cast_panel.update_snapshot(snap, day)
            if not has_bd_daily:
                txt.insert("end", "bd_dailyがありません（BD導入前のsnapshot、または既存snapshotがskipされ更新されていない可能性）\n")
            elif bd.get("prev_day") is None:
                txt.insert("end", "※prev_dayが無いためdelta系はN/A（サンプル不足）\n")
            txt.insert("end", "\n--- 日付 ---\n")
            txt.insert("end", f"date: {summary['date']}\n")
            txt.insert("end", f"prev_day: {summary['prev_day']}\n")
            txt.insert("end", f"gap_days: {summary['gap_days']}\n")
            txt.insert("end", "\n--- BD Daily ---\n")
            txt.insert("end", f"avg_big_score: {summary['avg_big_score']}\n")
            txt.insert("end", f"delta_avg_big_score: {summary['delta_avg_big_score']}\n")
            txt.insert("end", f"delta_avg_big_score_per_day: {summary['delta_avg_big_score_per_day']}\n")
            txt.insert("end", "\n--- MA ---\n")
            txt.insert("end", "※MAは存在するサンプルのみ平均（0は0 / 無いものだけN/A）\n")
            txt.insert("end", f"ma3_samples_avg_big_score: {summary['ma3_samples_avg_big_score']}\n")
            txt.insert("end", f"ma14_samples_avg_big_score: {summary['ma14_samples_avg_big_score']}\n")
            txt.insert("end", f"ma28_samples_avg_big_score: {summary['ma28_samples_avg_big_score']}\n")

            rows = snap.get("all_current", []) or []
            deltas = []
            for row in rows:
                if not isinstance(row, dict):
                    continue
                val = row.get("delta_big_score")
                if isinstance(val, (int, float)):
                    deltas.append((float(val), row))

            txt.insert("end", "\n--- Top Δbig_score ---\n")
            if deltas:
                deltas.sort(key=lambda x: x[0], reverse=True)
                for i, (val, row) in enumerate(deltas[:10], 1):
                    name = self._format_cast_name(row.get("name", "?"))
                    preset = row.get("preset", "")
                    label = f"{preset} {name}".strip()
                    txt.insert("end", f"{i:02d}. {val * 100:+.2f}  {label}\n")
            else:
                txt.insert("end", "N/A\n")
            txt.configure(state="disabled")

        def on_change(_e=None):
            sel = combo.get()
            if sel:
                render(sel)

        combo.bind("<<ComboboxSelected>>", on_change)
        combo.set(sorted(days, reverse=True)[0])
        render(combo.get())

        btns = ttk.Frame(win)
        btns.pack(fill="x", padx=10, pady=(0, 10))
        def copy_all():
            try:
                self.clipboard_clear()
                self.clipboard_append(txt.get("1.0", "end").strip())
            except Exception:
                pass
        ttk.Button(btns, text="コピー", command=copy_all).pack(side="left")
        ttk.Button(btns, text="閉じる", command=win.destroy).pack(side="left", padx=8)
        # 初期状態（デフォルト: コピー欄を隠す）を下部ボタン列にも反映
        try:
            apply_copy_visibility()
        except Exception:
            pass
        self.log("[INFO] VIEW bd_summary")

    def view_cast_detail(self, day):
        if not day:
            return
        win = tk.Toplevel(self)
        win.title(f"キャスト詳細: {day}")
        win.geometry("1180x720")
        panel = CastDetailPanel(self, win, show_close=True, on_close=win.destroy)
        panel.pack(fill="both", expand=True, padx=10, pady=(6, 10))
        panel.load_day(day)

    def resort_view(self):
        if not self.results:
            return
        self.results = self.sort_rows(self.results)
        self.populate_tree()

    def _get_delta_value(self, row):
        return row.get("delta", row.get("delta_pop"))

    def _format_delta(self, row):
        d = self._get_delta_value(row)
        if d is None:
            return "N/A"
        return f"{(d*100):+.1f}"

    def _get_confidence_value(self, row):
        return row.get("site_confidence", row.get("conf", 0))

    def populate_tree(self):
        self._clear_tree()
        for idx, r in enumerate(self.results, 1):
            st = r["stats"]
            rate = st.get("bell_rate_bookable")
            rate_s = "N/A" if rate is None else f"{rate*100:.1f}%"
            score_s = f"{r.get('score',0)*100:.1f}"
            big_s = f"{(r.get('big_score', r.get('score',0)) or 0)*100:.1f}"
            rank_s = self._format_score_percent(r.get("rank_percentile"))
            qual_s = self._format_score_percent(r.get("quality_score"))
            delta_s = self._format_delta(r)
            conf_s = str(int(self._get_confidence_value(r) or 0))
            tags = []
            if idx <= 5:
                tags.append("top")
            if (st.get("bookable_slots",0) or 0) < 5:
                tags.append("muted")

            self.tree.insert("", "end", iid=str(idx), values=(
                idx,
                score_s,
                big_s,
                rank_s,
                qual_s,
                delta_s,
                conf_s,
                rate_s,
                st.get("bell",0) or 0,
                st.get("maru",0) or 0,
                st.get("tel",0) or 0,
                st.get("bookable_slots",0) or 0,
                st.get("total_slots",0) or 0,
                r.get("name",""),
            ), tags=tuple(tags))

        self.var_status.set(f"結果表示：{len(self.results)}件（ダブルクリックでDETAIL）")

    def _get_selected_row(self):
        sel = self.tree.selection()
        if not sel:
            return None
        try:
            rank = int(sel[0])
            return self.results[rank-1]
        except Exception:
            return None

    def on_double_click(self, event):
        if not self.results:
            return
        is_shift = (event.state & 0x0001) != 0
        self.open_selected("res" if is_shift else "detail")

    def on_right_click(self, event):
        try:
            iid = self.tree.identify_row(event.y)
            if iid:
                self.tree.selection_set(iid)
            self.menu.tk_popup(event.x_root, event.y_root)
        finally:
            self.menu.grab_release()

    def open_selected(self, kind):
        r = self._get_selected_row()
        if not r:
            return
        url = r["res"] if kind == "res" else r["detail"]
        webbrowser.open(url)

    def copy_selected(self, kind):
        r = self._get_selected_row()
        if not r:
            return
        if kind == "res":
            s = r["res"]
        elif kind == "detail":
            s = r["detail"]
        else:
            s = r.get("name","")
        self.clipboard_clear()
        self.clipboard_append(s)
