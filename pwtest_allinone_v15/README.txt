pwtest (Playwright 自動取得ツール)

■ フォルダ完結の考え方
- このフォルダの中だけで「設定・ログ・履歴・画像」を増やすようにしています。
- 生成物: score_data/ (runs, daily, history, logs, analytics, state など)
- Cookie等のブラウザプロファイル: pw_profile/

■ 起動（手動/自動）
1) Start.bat（推奨）
   - ダブルクリック: GUI
   - タスクスケジューラ: Start.bat auto
   - ※タスク登録前に score_data/config.json の auto.presets を設定してください
2) 既存BAT（互換）
   Start-GUI.bat / Start-AUTO*.bat は Start.bat のラッパーです。
3) ジョブファイルから実行（拡張）
   score_data\state\job.json を作成してから:
     python main.py --run-job

■ GUI ダッシュボード（入口）
- 上部の「ダッシュボード」に主要操作を集約しました。
  - 手動実行 / 停止 / 前回結果 / 履歴サマリ / BDサマリ / 自動(1回)
  - 設定 / ログ表示切替
  - 既存の詳細操作（プリセット編集、キュー、詳細ログ等）は従来通り利用できます。
  - 手動/自動(1回)は job.json を生成して main.py --run-job で実行します。
    （GUIはガワ専用・run_job/async_scrape_job に統一）

■ 起動（1日1回だけ）
- score_data/config.json の auto.once_per_day を true にすると、Python側で1日1回の判定を行います。
  その日の初回だけ実行し、2回目以降は「already ran today」で終了します。
- 判定用ファイル: score_data\last_run_date.txt （yyyy-MM-dd）

■ 進捗/ジョブの状態ファイル（拡張）
- GUI/CLI から使える共通の状態ファイルを追加しました。
  - score_data\state\job.json : 実行ジョブ定義
  - score_data\state\progress.json : 進捗更新
  - score_data\state\stop.flag : 停止要求
  ※既存の出力・保存先は維持したまま拡張しています。

■ タスクスケジューラ設定例（“ログインしたら” で 1日1回）
1) 「タスクの作成」
2) [トリガー] → 新規 → 「ログオン時」
3) [操作] → 新規
   - プログラム/スクリプト: Start.bat
   - 引数の追加: auto
   - 開始 (オプション): C:\Users\iwata\Desktop\pwtest
     ※ “開始” が空でも動くように作ってありますが、入れておくと安心です。
4) [条件] → 「コンピューターをAC電源で使用中のみ…」などは必要に応じてOFF
5) [設定] → 「タスクを停止する…」は長めに（例: 2時間）推奨

■ 6か月より古いデータの削除（retention）
- main.py / GUI 実行後に retention_cleanup が走ります。
- 月数は score_data/config.json の retention.months で変更可能（デフォルト6）

■ 自動実行の設定（config.json）
score_data/config.json の auto.* を自動/手動の既定値として使います。
例:
{
  "auto": {
    "presets": "fortune,natural-swim,megapalace",
    "headful": false,
    "concurrency": 3,
    "once_per_day": true,
    "minimize_browser": false
  },
  "notify": { "enabled": true, "min_confidence": 50, "top_n": 5 },
  "retention": { "months": 6, "max_lines": 200 }
}
※ --presets / --concurrency / --headful/--headless などのCLIは上書きとして利用できます。
※ GUIの「設定」で auto.presets / auto.concurrency / auto.headful / auto.once_per_day / auto.minimize_browser / notify.enabled を編集できます。
※ --force-today で once_per_day を無視して実行できます。




---

【重要】.venv の場所

- このフォルダ内に .venv が無い場合でも、親フォルダに .venv があれば自動で使います。

  例: C:\Users\iwata\Desktop\pwtest\.venv を使って、pwtest\pwtest_allinone_v9 から起動できます。

- どちらにも無い場合は PATH 上の python を使います。



---
[NEW] Start-AUTO-Detached.bat: starts auto in a separate minimized console and returns immediately (useful when running from PowerShell).

---

■ 簡易テスト手順（GUI/自動の確認）
1) presets を3件用意して自動実行（複数プリセット）
2) GUIでプリセット切替 → LIST_URL が即座に切り替わる
3) 「前回結果」「履歴サマリ」→ run を選択して各presetが表示される
   - 該当presetのファイルが無い場合は明示メッセージが出る
4) score_data/runs/.../jobs に per-preset の *_current.json が生成される（nullファイル無し）
5) キャスト詳細: 1クリックでMA/履歴グラフ更新、ダブルクリックで girlid ページが開く
