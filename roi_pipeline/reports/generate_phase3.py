"""
Phase 3 Walk-Forward検証レポート生成スクリプト

使用方法:
    cd E:\\jraroi1219
    py -3.12 -m roi_pipeline.reports.generate_phase3

出力:
    roi_pipeline/reports/phase3/walk_forward_report.md

前提条件:
    - PostgreSQL (127.0.0.1:5432, database: pckeiba) に接続可能であること
    - Phase 1 / Phase 2 の全テストがパス済みであること
    - pip install -r roi_pipeline/requirements.txt 済みであること

Walk-Forward 設計:
    学習窓: 36か月（ローリング、初期学習: 2016-2018の3年分）
    検証窓: 1か月
    検証期間: 2019-01 ～ 2025-12（84か月 = 7年）
    使用ファクター: Phase 2で確認された13エッジファクター
"""
import os
import sys
from datetime import datetime
from typing import List, Optional

import numpy as np
import pandas as pd

from roi_pipeline.config.db import DBConfig, get_connection
from roi_pipeline.engine.phase3_walk_forward import (
    MonthlyP3Result,
    betting_sharpe,
    max_drawdown,
    run_phase3_walk_forward,
)
from roi_pipeline.factors.definitions import FACTOR_DEFINITIONS, FactorDefinition

# ─────────────────────────────────────────────
# 設定
# ─────────────────────────────────────────────

REPORT_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "phase3"
)

# Walk-Forward 期間設定
DATA_START = "20160101"   # 学習データ開始（初期学習: 2016-2018の3年分）
DATA_END   = "20251231"   # 検証データ終了
VAL_START  = "2019-01"    # 検証開始年月（学習窓36か月 = 2016-01〜2018-12）
VAL_END    = "2025-12"    # 検証終了年月
TRAIN_MONTHS = 36         # 学習窓: 3年（36か月）


# ─────────────────────────────────────────────
# 最適化SQLローダー（Phase 3専用）
# ─────────────────────────────────────────────

# JRA場コード（01-10）フィルタ
_JRA_CODES = "('01','02','03','04','05','06','07','08','09','10')"

# 8byte JRDBレースキー合成式（PostgreSQL）
# jvd_seの各カラムから生成。CTEで一度だけ計算してJOINに再利用する。
_RACE_KEY_EXPR = """
    TRIM(se.keibajo_code)
    || SUBSTRING(se.kaisai_nen, 3, 2)
    || CAST(CAST(NULLIF(TRIM(se.kaisai_kai), '') AS INTEGER) AS TEXT)
    || CASE
        WHEN CAST(NULLIF(TRIM(se.kaisai_nichime), '') AS INTEGER) <= 9
            THEN CAST(CAST(NULLIF(TRIM(se.kaisai_nichime), '') AS INTEGER) AS TEXT)
        WHEN CAST(NULLIF(TRIM(se.kaisai_nichime), '') AS INTEGER) = 10 THEN 'a'
        WHEN CAST(NULLIF(TRIM(se.kaisai_nichime), '') AS INTEGER) = 11 THEN 'b'
        WHEN CAST(NULLIF(TRIM(se.kaisai_nichime), '') AS INTEGER) = 12 THEN 'c'
        ELSE CAST(CAST(NULLIF(TRIM(se.kaisai_nichime), '') AS INTEGER) AS TEXT)
       END
    || LPAD(CAST(CAST(NULLIF(TRIM(se.race_bango), '') AS INTEGER) AS TEXT), 2, '0')
"""

# Phase 3 Walk-Forward で必要なカラムのみ（se.* を使わない）
_PHASE3_QUERY = """
WITH race_keys AS (
    -- JRDB 8byte レースキーを一度だけ計算（4つのJOINで再利用）
    SELECT
        se.keibajo_code,
        se.kaisai_nen,
        se.kaisai_tsukihi,
        se.kaisai_kai,
        se.kaisai_nichime,
        se.race_bango,
        se.umaban,
        se.tansho_odds,
        se.kakutei_chakujun,
        se.barei,
        se.blinker_shiyo_kubun,
        (se.kaisai_nen || se.kaisai_tsukihi) AS race_date,
        ({race_key_expr}) AS rk8
    FROM jvd_se AS se
    WHERE (se.kaisai_nen || se.kaisai_tsukihi) >= '{date_from}'
      AND (se.kaisai_nen || se.kaisai_tsukihi) <= '{date_to}'
      AND TRIM(se.keibajo_code) IN {jra_codes}
      AND NULLIF(TRIM(se.tansho_odds), '') IS NOT NULL
      AND TRIM(se.tansho_odds) != '0'
)
SELECT
    rk.keibajo_code,
    rk.kaisai_nen,
    rk.kaisai_tsukihi,
    rk.race_bango,
    rk.umaban,
    rk.tansho_odds,
    rk.kakutei_chakujun,
    rk.barei,
    rk.blinker_shiyo_kubun,
    rk.race_date,
    -- jvd_ra: track_code と babajotai コード
    ra.track_code,
    ra.babajotai_code_shiba,
    ra.babajotai_code_dirt,
    -- jrd_kyi_fixed: 能力・適性・関係者指数
    kyi.idm,
    kyi.sogo_shisu,
    kyi.agari_shisu,
    kyi.pace_shisu,
    kyi.kyori_tekisei        AS kyori_tekisei_code,
    kyi.shiba_tekisei_code   AS course_tekisei,
    kyi.omo_tekisei_code     AS baba_tekisei,
    kyi.chokyo_yajirushi_code,
    kyi.soho,
    kyi.kishu_shisu,
    kyi.chokyo_shisu,
    kyi.kyusha_shisu,
    -- jrd_cyb_fixed: 調教評価
    cyb.chokyo_hyoka,
    -- jrd_joa_fixed: LS指数
    joa.ls_shisu,
    -- jrd_bac_fixed: 負担重量種別
    bac.juryo_shubetsu_code
FROM race_keys AS rk
LEFT JOIN jvd_ra AS ra
    ON rk.keibajo_code  = ra.keibajo_code
   AND rk.kaisai_nen    = ra.kaisai_nen
   AND rk.kaisai_tsukihi = ra.kaisai_tsukihi
   AND rk.kaisai_kai    = ra.kaisai_kai
   AND rk.kaisai_nichime = ra.kaisai_nichime
   AND rk.race_bango    = ra.race_bango
LEFT JOIN jrd_kyi_fixed AS kyi
    ON rk.rk8 = kyi.jrdb_race_key8
   AND TRIM(rk.umaban) = TRIM(kyi.umaban)
LEFT JOIN jrd_cyb_fixed AS cyb
    ON rk.rk8 = cyb.jrdb_race_key8
   AND TRIM(rk.umaban) = TRIM(cyb.umaban)
LEFT JOIN jrd_joa_fixed AS joa
    ON rk.rk8 = joa.jrdb_race_key8
   AND TRIM(rk.umaban) = TRIM(joa.umaban)
LEFT JOIN jrd_bac_fixed AS bac
    ON rk.rk8 = bac.jrdb_race_key8
"""


def _load_one_year(year: int, config: Optional[DBConfig] = None) -> pd.DataFrame:
    """
    1年分のデータを最適化クエリで取得する。

    最適化ポイント:
        - se.* を使わず必要カラムのみ SELECT（未使用列を排除）
        - fukusho CTE（UNION ALL×5）を完全除去（最重量処理を排除）
        - JRDB race_key 式を CTE で一度だけ計算（4回の繰り返し計算を排除）
        - ORDER BY を除去（DB ソートコストを排除）
        - 年単位クエリで処理単位を分割

    Args:
        year: 取得年（例: 2016）
        config: DB接続設定（None でデフォルト）

    Returns:
        前処理前の生DataFrame
    """
    date_from = f"{year}0101"
    date_to   = f"{year}1231"

    query = _PHASE3_QUERY.format(
        race_key_expr=_RACE_KEY_EXPR,
        date_from=date_from,
        date_to=date_to,
        jra_codes=_JRA_CODES,
    )

    conn = get_connection(config)
    try:
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            df = pd.read_sql_query(query, conn)
    finally:
        conn.close()

    return df


def _preprocess(df: pd.DataFrame) -> pd.DataFrame:
    """
    取得済み DataFrame を Walk-Forward 用に前処理する。

    処理内容:
        - tansho_odds_val: tansho_odds / 10（JRA-VANは10倍単位格納）
        - is_hit: 単勝的中フラグ（kakutei_chakujun == 1）
        - is_hit_fukusho: 複勝的中フラグ（kakutei_chakujun ∈ {1,2,3}）
        - race_year: 年度文字列（year_weight 計算用）
        - race_id: ユニークレース識別子
    """
    df = df.copy()

    # ── オッズ変換（JRA-VANは10倍単位格納） ──
    tansho_raw = pd.to_numeric(df["tansho_odds"], errors="coerce")
    df["tansho_odds_val"] = tansho_raw / 10.0
    # fukusho は今回除去済み → NaN で統一（Walk-Forward は gracefully handle）
    df["fukusho_odds_val"] = float("nan")

    # ── 的中フラグ ──
    chakujun = pd.to_numeric(df["kakutei_chakujun"], errors="coerce")
    df["is_hit"]         = (chakujun == 1).astype(int)
    df["is_hit_fukusho"] = (chakujun <= 3).astype(int)

    # ── 年度 ──
    df["race_year"] = df["race_date"].astype(str).str[:4]

    # ── ユニークレース識別子 ──
    df["race_id"] = (
        df["keibajo_code"].astype(str).str.strip()
        + df["race_date"].astype(str)
        + df["race_bango"].astype(str).str.strip().str.zfill(2)
    )

    # ── 無効オッズを除去 ──
    df = df[df["tansho_odds_val"] > 1.0].copy()
    return df


def load_and_preprocess(
    date_from: str = DATA_START,
    date_to: str = DATA_END,
) -> pd.DataFrame:
    """
    年単位の分割クエリでデータを取得し、Phase 3 Walk-Forward 用に前処理する。

    最適化戦略:
        1. 1年分ずつ個別クエリ → 一括50万行クエリを回避
        2. 必要カラムのみ SELECT → se.* による未使用列を排除
        3. fukusho CTE（UNION ALL×5）を除去 → 最重量処理を排除
        4. race_key 式を CTE で一度計算 → 4回の繰り返し計算を排除
        5. ORDER BY 除去 → DB ソートコスト排除

    Args:
        date_from: 開始日（YYYYMMDD）
        date_to: 終了日（YYYYMMDD）

    Returns:
        前処理済み DataFrame
    """
    year_from = int(date_from[:4])
    year_to   = int(date_to[:4])

    chunks: List[pd.DataFrame] = []
    for year in range(year_from, year_to + 1):
        t0 = datetime.now()
        print(f"  [{year}] クエリ実行中...", end="", flush=True)
        chunk = _load_one_year(year)
        chunk = _preprocess(chunk)
        elapsed = (datetime.now() - t0).total_seconds()
        print(f" {len(chunk):,} 行 ({elapsed:.1f}s)")
        chunks.append(chunk)

    df = pd.concat(chunks, ignore_index=True)
    print(f"  全期間合計: {len(df):,} 行, レース数: {df['race_id'].nunique():,}")
    return df


# ─────────────────────────────────────────────
# レポート生成
# ─────────────────────────────────────────────

def render_report(
    results: List[MonthlyP3Result],
    report_path: str,
    generated_at: Optional[str] = None,
) -> str:
    """
    Walk-Forward 結果を Markdown レポートとして生成する。

    Args:
        results: run_phase3_walk_forward の戻り値
        report_path: 出力ファイルパス
        generated_at: 生成日時文字列（None なら現在時刻）

    Returns:
        生成されたレポートの文字列
    """
    if generated_at is None:
        generated_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if not results:
        report = "# Phase 3 Walk-Forward検証レポート\n\n結果なし（データ不足）\n"
        with open(report_path, "w", encoding="utf-8") as f:
            f.write(report)
        return report

    # ── 集計 ──
    monthly_returns = np.array([r.monthly_return for r in results])
    cumulative_bk   = np.array([r.cumulative_bankroll for r in results])
    cumulative_ret  = (cumulative_bk - 1.0) * 100.0   # %表示

    sharpe   = betting_sharpe(monthly_returns)
    max_dd   = max_drawdown(cumulative_bk) * 100.0
    total_ret = (cumulative_bk[-1] - 1.0) * 100.0
    avg_bets  = np.mean([r.n_bets for r in results])
    bs_model  = np.mean([r.brier_model for r in results])
    bs_market = np.mean([r.brier_market for r in results])
    n_months  = len(results)

    # 年間リターン > 0% の月の割合
    positive_months = np.sum(monthly_returns > 0)
    positive_ratio  = positive_months / n_months if n_months > 0 else 0.0

    # ── 合格判定 ──
    pass_sharpe  = sharpe > 1.0
    pass_dd      = max_dd < 30.0
    pass_pos     = positive_ratio >= 0.70
    pass_brier   = bs_model < bs_market

    # ── 年次サマリー集計 ──
    annual: dict = {}
    for r in results:
        yr = r.year_month[:4]
        if yr not in annual:
            annual[yr] = {
                "returns": [], "n_bets": 0, "bk_start": None, "bk_end": None,
            }
        annual[yr]["returns"].append(r.monthly_return)
        annual[yr]["n_bets"] += r.n_bets
        if annual[yr]["bk_start"] is None:
            annual[yr]["bk_start"] = r.cumulative_bankroll / (1.0 + r.monthly_return)
        annual[yr]["bk_end"] = r.cumulative_bankroll

    # ── Markdown 構築 ──
    lines = [
        "# Phase 3 Walk-Forward検証レポート",
        "",
        f"> 生成日時: {generated_at}",
        "",
        "## サマリー",
        "",
        f"- 検証期間: {results[0].year_month} ～ {results[-1].year_month}（{n_months}か月）",
        f"- 年間Betting Sharpe Ratio: {sharpe:.4f}",
        f"- 最大ドローダウン: {max_dd:.2f}%",
        f"- 累積リターン: {total_ret:+.2f}%",
        f"- 平均月次ベット数: {avg_bets:.1f}",
        f"- Brier Score（モデル）: {bs_model:.6f}",
        f"- Brier Score（市場）: {bs_market:.6f}",
        "",
        "## 月次リターン推移",
        "",
        "| 年月 | ベット数 | 的中数 | 月次リターン | 累積リターン | DD |",
        "|------|---------|--------|------------|------------|-----|",
    ]

    # 月次テーブル（累積バンクロールからDD計算）
    peak = 1.0
    for r in results:
        bk = r.cumulative_bankroll
        if bk > peak:
            peak = bk
        dd_pct = (peak - bk) / peak * 100.0 if peak > 0 else 0.0
        cum_ret_pct = (bk - 1.0) * 100.0
        sign = "+" if r.monthly_return >= 0 else ""
        lines.append(
            f"| {r.year_month} | {r.n_bets} | {r.n_hits} | "
            f"{sign}{r.monthly_return*100:.2f}% | "
            f"{cum_ret_pct:+.2f}% | "
            f"{dd_pct:.1f}% |"
        )

    # 年次サマリー
    lines += [
        "",
        "## 年次サマリー",
        "",
        "| 年 | 年間リターン | Sharpe | 最大DD | ベット数 |",
        "|----|------------|--------|--------|---------|",
    ]
    for yr, data in sorted(annual.items()):
        yr_rets = np.array(data["returns"])
        yr_ret_pct = np.sum(yr_rets) * 100.0
        yr_sharpe = betting_sharpe(yr_rets)
        # 年内 cumulative_bankroll からDD計算
        yr_bks = np.array([
            r.cumulative_bankroll for r in results if r.year_month.startswith(yr)
        ])
        yr_dd = max_drawdown(yr_bks) * 100.0 if len(yr_bks) > 0 else 0.0
        lines.append(
            f"| {yr} | {yr_ret_pct:+.2f}% | {yr_sharpe:.3f} | "
            f"{yr_dd:.1f}% | {data['n_bets']} |"
        )

    # 合格判定
    lines += [
        "",
        "## 合格判定",
        "",
        f"- [{'x' if pass_sharpe  else ' '}] Sharpe > 1.0 （実績: {sharpe:.4f}）",
        f"- [{'x' if pass_dd      else ' '}] 最大DD < 30% （実績: {max_dd:.2f}%）",
        f"- [{'x' if pass_pos     else ' '}] 年間リターン > 0% が検証期間の70%以上"
        f" （実績: {positive_ratio*100:.1f}%）",
        f"- [{'x' if pass_brier   else ' '}] Brier Score（モデル） < Brier Score（市場）"
        f" （{bs_model:.6f} vs {bs_market:.6f}）",
        "",
        f"**合格数: {sum([pass_sharpe, pass_dd, pass_pos, pass_brier])}/4**",
        "",
    ]

    report = "\n".join(lines)
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report)

    return report


# ─────────────────────────────────────────────
# エントリポイント
# ─────────────────────────────────────────────

def main() -> None:
    """Phase 3 Walk-Forward検証を実行してレポートを生成する。"""
    print("=" * 60)
    print("  Phase 3 Walk-Forward検証レポート生成")
    print(f"  データ期間: {DATA_START} ～ {DATA_END}")
    print(f"  検証期間: {VAL_START} ～ {VAL_END}（84か月）")
    print(f"  学習窓: {TRAIN_MONTHS}か月（ローリング、初期学習: 2016-2018）")
    print("=" * 60)

    os.makedirs(REPORT_DIR, exist_ok=True)

    # ── データ取得・前処理 ──
    print("\n[1] データ取得・前処理")
    df = load_and_preprocess(DATA_START, DATA_END)

    # ── Walk-Forward 実行 ──
    print("\n[2] Walk-Forward実行中（84か月分: 2019-01 ～ 2025-12）...")
    results = run_phase3_walk_forward(
        df=df,
        factor_defs=FACTOR_DEFINITIONS,
        val_start_ym=VAL_START,
        val_end_ym=VAL_END,
        train_months=TRAIN_MONTHS,  # 36か月
        date_col="race_date",
        race_id_col="race_id",
        tansho_odds_col="tansho_odds_val",
        fukusho_odds_col="fukusho_odds_val",
        hit_col="is_hit",
        hit_fuku_col="is_hit_fukusho",
        year_col="race_year",
        umaban_col="umaban",
        verbose=True,
    )

    print(f"\n  検証完了: {len(results)} か月（目標: 84か月）")

    # ── レポート生成 ──
    print("\n[3] レポート生成中...")
    report_path = os.path.join(REPORT_DIR, "walk_forward_report.md")
    report = render_report(results, report_path)

    print(f"\n  出力先: {report_path}")
    print("\n" + "=" * 60)
    print("  サマリー（抜粋）")
    print("=" * 60)

    if results:
        monthly_returns = np.array([r.monthly_return for r in results])
        cumulative_bk   = np.array([r.cumulative_bankroll for r in results])
        sharpe  = betting_sharpe(monthly_returns)
        max_dd  = max_drawdown(cumulative_bk) * 100.0
        total   = (cumulative_bk[-1] - 1.0) * 100.0
        print(f"  Sharpe Ratio: {sharpe:.4f}")
        print(f"  最大DD:       {max_dd:.2f}%")
        print(f"  累積リターン: {total:+.2f}%")
        print(f"  検証月数:     {len(results)}")
    else:
        print("  結果なし")


if __name__ == "__main__":
    main()
