"""
325ファクター × 3セグメント 全量エッジ分析レポート生成スクリプト

出力ファイル:
    roi_pipeline/reports/factor_analysis/
      full_325_factor_report.md        全体サマリー（採用判断用）
      by_segment/GLOBAL.md             GLOBALセグメント詳細
      by_segment/SURFACE_2.md          SURFACE_2セグメント詳細
      by_segment/COURSE_27.md          COURSE_27セグメント詳細
      raw_data/factor_scores.csv       全数値データ

使用方法:
    cd E:\\jraroi1219
    py -3.12 -m roi_pipeline.reports.generate_factor_analysis_325

オプション:
    --year-from YYYY     開始年（デフォルト: 2016）
    --year-to   YYYY     終了年（デフォルト: 2025）
    --dry-run            DBに接続せずにテスト実行（モックデータ）
    --factors fid,...    特定ファクターIDのみ（例: --factors 210,211,212）
"""
import argparse
import os
import sys
from datetime import datetime
from typing import List, Optional

import numpy as np
import pandas as pd

from roi_pipeline.engine.factor_catalog_325 import ALL_FACTORS_325, Factor325
from roi_pipeline.engine.factor_analysis_engine import (
    analyze_all_factors,
    FactorResult,
    BASELINE,
)
from roi_pipeline.engine.full_factor_loader import load_all_years

# =============================================================================
# 出力先ディレクトリ
# =============================================================================
REPORT_BASE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "factor_analysis")
BY_SEGMENT_DIR = os.path.join(REPORT_BASE, "by_segment")
RAW_DATA_DIR = os.path.join(REPORT_BASE, "raw_data")


def ensure_dirs() -> None:
    """出力ディレクトリを作成する。"""
    for d in [REPORT_BASE, BY_SEGMENT_DIR, RAW_DATA_DIR]:
        os.makedirs(d, exist_ok=True)


# =============================================================================
# CSV 出力
# =============================================================================

def save_csv(results: List[FactorResult]) -> str:
    """全ビン結果をCSVに保存する。"""
    rows = []
    for r in results:
        rows.extend(r.to_csv_rows())

    df = pd.DataFrame(rows)
    path = os.path.join(RAW_DATA_DIR, "factor_scores.csv")
    df.to_csv(path, index=False, encoding="utf-8-sig")
    return path


# =============================================================================
# セグメント別Markdown
# =============================================================================

def _format_table_header() -> List[str]:
    return [
        "| ファクター名 | テーブル | セグメント | セグメント値 | ビン | N | 単勝的中率 | 単勝補正ROI | 単勝信頼度 | 単勝調整済 | 複勝的中率 | 複勝補正ROI | 複勝信頼度 | 複勝調整済 | 単勝✓ | 複勝✓ |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]


def _format_bin_row(r: FactorResult, b) -> str:
    t_pass = "✅" if b.tansho_pass else ""
    f_pass = "✅" if b.fukusho_pass else ""
    return (
        f"| {r.factor.description or r.factor.alias} "
        f"| {r.factor.table} "
        f"| {b.segment_type} "
        f"| {b.segment_value} "
        f"| {b.bin_value} "
        f"| {b.n:,} "
        f"| {b.tansho_hit_rate:.1%} "
        f"| {b.tansho_roi:.1f} "
        f"| {b.tansho_confidence:.3f} "
        f"| **{b.tansho_adjusted:.1f}** "
        f"| {b.fukusho_hit_rate:.1%} "
        f"| {b.fukusho_roi:.1f} "
        f"| {b.fukusho_confidence:.3f} "
        f"| **{b.fukusho_adjusted:.1f}** "
        f"| {t_pass} "
        f"| {f_pass} |"
    )


def generate_segment_report(
    results: List[FactorResult],
    segment_type: str,
    title: str,
) -> str:
    """特定セグメントのMarkdownレポートを生成する。"""
    lines = [
        f"# {title}",
        "",
        f"**生成日時**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"**セグメント**: {segment_type}",
        f"**基準値**: 補正回収率 {BASELINE}%（JRA控除率基準点）",
        "",
    ]

    # サマリー統計
    active = [r for r in results if not r.skipped]
    t_pass_count = sum(1 for r in active for b in r.bins if b.segment_type == segment_type and b.tansho_pass)
    f_pass_count = sum(1 for r in active for b in r.bins if b.segment_type == segment_type and b.fukusho_pass)
    lines += [
        "## サマリー",
        "",
        f"- 分析ファクター数: {len(active)}",
        f"- 単勝合格ビン総数: {t_pass_count}",
        f"- 複勝合格ビン総数: {f_pass_count}",
        "",
        "---",
        "",
        "## ビン別詳細テーブル",
        "",
    ]
    lines += _format_table_header()

    for r in results:
        if r.skipped:
            continue
        seg_bins = [b for b in r.bins if b.segment_type == segment_type]
        for b in sorted(seg_bins, key=lambda x: (-x.tansho_adjusted, x.segment_value, x.bin_value)):
            lines.append(_format_bin_row(r, b))

    return "\n".join(lines)


# =============================================================================
# 全体サマリーレポート
# =============================================================================

def generate_full_summary(results: List[FactorResult]) -> str:
    """全体サマリーレポート（CEO採用判断テーブル）を生成する。"""
    lines = [
        "# 325ファクター 全量エッジ分析 最終サマリー",
        "",
        f"**生成日時**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"**データ期間**: 2016-2025年（10年間）",
        f"**基準値**: 補正回収率 {BASELINE}%",
        "",
        "## 採用基準",
        "- **S**: 単勝・複勝両方で信頼度調整済み回収率 > 80% のビンが2個以上",
        "- **A**: 単勝または複勝で合格ビンが2個以上",
        "- **B**: 単勝または複勝で合格ビンが1個",
        "- **C**: 合格ビンなし（不採用）",
        "",
        "---",
        "",
        "## 採用推奨ファクター（S/A/B）",
        "",
        "| ファクター名 | テーブル | カラム | 最適セグメント | 単勝合格ビン数 | 複勝合格ビン数 | 最高単勝調整済 | 最高複勝調整済 | 評価 |",
        "|---|---|---|---|---|---|---|---|---|",
    ]

    grade_order = {"S": 0, "A": 1, "B": 2, "C": 3}
    active = [r for r in results if not r.skipped and r.bins]
    active_sorted = sorted(active, key=lambda r: (grade_order.get(r.grade, 9), -r.best_tansho_adjusted))

    recommend = [r for r in active_sorted if r.grade in ("S", "A", "B")]
    for r in recommend:
        lines.append(
            f"| {r.factor.description or r.factor.alias} "
            f"| {r.factor.table} "
            f"| {r.factor.column} "
            f"| {r.best_segment} "
            f"| {len(r.tansho_pass_bins)} "
            f"| {len(r.fukusho_pass_bins)} "
            f"| **{r.best_tansho_adjusted:.1f}** "
            f"| **{r.best_fukusho_adjusted:.1f}** "
            f"| **{r.grade}** |"
        )

    lines += [
        "",
        "---",
        "",
        "## 全325ファクター一覧（スキップ含む）",
        "",
        "| # | ファクター名 | テーブル | カラム | 種別 | NULL率 | 単勝合格数 | 複勝合格数 | 最高単勝 | 最高複勝 | 評価 | 備考 |",
        "|---|---|---|---|---|---|---|---|---|---|---|---|",
    ]

    for r in results:
        if r.skipped:
            skip_note = r.skip_reason or r.factor.skip_reason
            lines.append(
                f"| {r.factor.fid} "
                f"| {r.factor.description or r.factor.alias} "
                f"| {r.factor.table} "
                f"| {r.factor.column} "
                f"| {r.factor.kind} "
                f"| {r.null_rate:.1%} "
                f"| - | - | - | - "
                f"| **SKIP** "
                f"| {skip_note} |"
            )
        else:
            lines.append(
                f"| {r.factor.fid} "
                f"| {r.factor.description or r.factor.alias} "
                f"| {r.factor.table} "
                f"| {r.factor.column} "
                f"| {r.factor.kind} "
                f"| {r.null_rate:.1%} "
                f"| {len(r.tansho_pass_bins)} "
                f"| {len(r.fukusho_pass_bins)} "
                f"| {r.best_tansho_adjusted:.1f} "
                f"| {r.best_fukusho_adjusted:.1f} "
                f"| **{r.grade}** "
                f"|  |"
            )

    # 統計サマリー
    total = len(results)
    skipped = sum(1 for r in results if r.skipped)
    analyzed = total - skipped
    grade_s = sum(1 for r in results if not r.skipped and r.grade == "S")
    grade_a = sum(1 for r in results if not r.skipped and r.grade == "A")
    grade_b = sum(1 for r in results if not r.skipped and r.grade == "B")
    grade_c = sum(1 for r in results if not r.skipped and r.grade == "C")
    skip_by_reason: dict = {}
    for r in results:
        if r.skipped:
            reason = r.skip_reason or r.factor.skip_reason
            skip_by_reason[reason] = skip_by_reason.get(reason, 0) + 1

    lines += [
        "",
        "---",
        "",
        "## 統計サマリー",
        "",
        f"- **総ファクター数**: {total}",
        f"- **分析実施**: {analyzed}",
        f"- **スキップ**: {skipped}",
        f"- **S評価**: {grade_s}",
        f"- **A評価**: {grade_a}",
        f"- **B評価**: {grade_b}",
        f"- **C評価（不採用）**: {grade_c}",
        "",
        "### スキップ理由内訳",
        "",
    ]
    for reason, cnt in sorted(skip_by_reason.items(), key=lambda x: -x[1]):
        lines.append(f"- {reason}: {cnt}件")

    return "\n".join(lines)


# =============================================================================
# メイン
# =============================================================================

def run_analysis(
    year_from: int = 2016,
    year_to: int = 2025,
    dry_run: bool = False,
    factor_ids: Optional[List[int]] = None,
) -> None:
    """分析を実行してレポートを生成する。"""
    ensure_dirs()
    print(f"[325ファクター全量エッジ分析]", flush=True)
    print(f"  期間: {year_from}-{year_to}", flush=True)

    # 対象ファクター
    if factor_ids:
        factors = [f for f in ALL_FACTORS_325 if f.fid in set(factor_ids)]
        print(f"  対象: {len(factors)} ファクター（指定）", flush=True)
    else:
        factors = ALL_FACTORS_325
        print(f"  対象: 全{len(factors)} ファクター", flush=True)

    if dry_run:
        print("  [DRY RUN] モックデータで実行します", flush=True)
        df = _make_mock_df()
    else:
        # データ取得
        print("  データ取得中...", flush=True)
        years = list(range(year_from, year_to + 1))
        df = load_all_years(years=years, verbose=True)
        print(f"  取得完了: {len(df):,} 行", flush=True)

    if len(df) == 0:
        print("ERROR: データが0行です", file=sys.stderr)
        sys.exit(1)

    # 分析実行
    print("  分析実行中...", flush=True)
    results = analyze_all_factors(df, factors=factors, verbose=True)

    # CSV保存
    csv_path = save_csv(results)
    print(f"  CSV保存: {csv_path}", flush=True)

    # セグメント別Markdown
    for seg, title, fname in [
        ("GLOBAL",    "GLOBAL セグメント ビン別補正回収率",          "GLOBAL.md"),
        ("SURFACE_2", "SURFACE_2（芝/ダート）セグメント ビン別補正回収率", "SURFACE_2.md"),
        ("COURSE_27", "COURSE_27（27コース）セグメント ビン別補正回収率",  "COURSE_27.md"),
    ]:
        content = generate_segment_report(results, seg, title)
        path = os.path.join(BY_SEGMENT_DIR, fname)
        with open(path, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"  レポート保存: {path}", flush=True)

    # 全体サマリー
    summary = generate_full_summary(results)
    summary_path = os.path.join(REPORT_BASE, "full_325_factor_report.md")
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write(summary)
    print(f"  サマリー保存: {summary_path}", flush=True)

    # 採用ファクター集計表示
    grade_s = [r for r in results if not r.skipped and r.grade == "S"]
    grade_a = [r for r in results if not r.skipped and r.grade == "A"]
    grade_b = [r for r in results if not r.skipped and r.grade == "B"]
    skipped = [r for r in results if r.skipped]
    print(f"\n  ===== 採用判定結果 =====", flush=True)
    print(f"  S: {len(grade_s)}  A: {len(grade_a)}  B: {len(grade_b)}  "
          f"C: {len([r for r in results if not r.skipped and r.grade=='C'])}  "
          f"SKIP: {len(skipped)}", flush=True)
    if grade_s or grade_a:
        print(f"\n  --- S/A 採用推奨 ---", flush=True)
        for r in grade_s + grade_a:
            print(f"  [{r.grade}] {r.factor.description or r.factor.alias}"
                  f" ({r.factor.table}.{r.factor.column})"
                  f"  最高単勝調整: {r.best_tansho_adjusted:.1f}", flush=True)

    print("\n  完了。", flush=True)


# =============================================================================
# モックデータ（dry-run用）
# =============================================================================

def _make_mock_df(n_rows: int = 50000) -> pd.DataFrame:
    """テスト用のモックDataFrameを生成する。"""
    rng = np.random.default_rng(42)
    years = rng.choice(list(range(2016, 2026)), size=n_rows)
    n_horses = rng.integers(8, 18, size=n_rows)
    chakujun = rng.integers(1, 19, size=n_rows)
    tansho_odds = rng.exponential(scale=15.0, size=n_rows) + 1.5

    df = pd.DataFrame({
        "race_date": [f"{y}{rng.integers(1,13):02d}{rng.integers(1,28):02d}" for y in years],
        "race_year": years.astype(str),
        "keibajo_code": rng.choice(["01","02","03","04","05","06","07","08","09","10"], size=n_rows),
        "kaisai_nen": years.astype(str),
        "kaisai_tsukihi": [f"{rng.integers(1,13):02d}{rng.integers(1,28):02d}" for _ in range(n_rows)],
        "kakutei_chakujun": chakujun.astype(str),
        "tansho_odds": tansho_odds,
        "fukusho_odds": np.where(chakujun <= 3, rng.uniform(1.1, 8.0, n_rows), np.nan),
        "ra_track_code": rng.choice(["10","11","12","20","21","22"], size=n_rows),
        "ra_kyori": rng.choice([1200,1400,1600,1800,2000,2400], size=n_rows).astype(str),
        "track_code_for_course": rng.choice(["10","11","12","20","21","22"], size=n_rows),
        "kyori_for_course": rng.choice([1200,1400,1600,1800,2000,2400], size=n_rows).astype(str),
        "surface_2": rng.choice(["芝","ダ"], size=n_rows),
    })

    # ファクターカラムを追加（正規分布の数値 or カテゴリ）
    from roi_pipeline.engine.factor_catalog_325 import ACTIVE_FACTORS
    for f in ACTIVE_FACTORS:
        if f.kind == "NUMERIC":
            vals = rng.normal(50, 15, n_rows)
            df[f.alias] = vals.astype(str)
        elif f.kind in ("CATEGORY", "ORDINAL"):
            df[f.alias] = rng.choice(["1","2","3","4","5",""], size=n_rows)
        # SKIPは不要

    df["is_hit"] = (pd.to_numeric(df["kakutei_chakujun"], errors="coerce") == 1).astype(int)
    df["is_fukusho_hit"] = df["fukusho_odds"].notna().astype(int)
    return df


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="325ファクター全量エッジ分析")
    parser.add_argument("--year-from", type=int, default=2016)
    parser.add_argument("--year-to",   type=int, default=2025)
    parser.add_argument("--dry-run",   action="store_true")
    parser.add_argument("--factors",   type=str, default=None,
                        help="カンマ区切りのfactor ID (例: 210,211,212)")
    args = parser.parse_args()

    factor_ids = None
    if args.factors:
        factor_ids = [int(x.strip()) for x in args.factors.split(",")]

    run_analysis(
        year_from=args.year_from,
        year_to=args.year_to,
        dry_run=args.dry_run,
        factor_ids=factor_ids,
    )


if __name__ == "__main__":
    main()
