"""
組み合わせファクター分析レポート生成スクリプト

CEOが指定した全39組み合わせの補正回収率テーブルをMarkdown形式で出力する。
スキップ組み合わせ（jrd_kyi rawのみ対応カラム）はスキップ理由を明記する。

使用方法:
    cd E:\\jraroi1219
    py -3.12 -m roi_pipeline.reports.generate_combination_report

    特定の組み合わせのみ:
    py -3.12 -m roi_pipeline.reports.generate_combination_report --combo course27_01
    py -3.12 -m roi_pipeline.reports.generate_combination_report --combo surface2_02

出力:
    roi_pipeline/reports/combination_analysis/{combo_id}.md （各組み合わせ1ファイル）
    roi_pipeline/reports/combination_analysis/summary.md   （全組み合わせサマリー）

前提条件:
    - PostgreSQL (127.0.0.1:5432, database: pckeiba) に接続可能であること
    - 分析期間: 2016-01-01 〜 2025-12-31
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime
from typing import Optional

import pandas as pd

from roi_pipeline.config.db import get_connection
from roi_pipeline.engine.combination_analysis import (
    COMBINATIONS,
    run_combination_analysis,
    load_combination_dataset,
    _compute_roi_table,
)

# =============================================================================
# 設定
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)

REPORT_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "combination_analysis"
)
os.makedirs(REPORT_DIR, exist_ok=True)

# 分析期間
DATA_START = "20160101"
DATA_END   = "20251231"

# ROI がこの値以上のビンを「エッジあり」と判定
EDGE_THRESHOLD = 80.0


# =============================================================================
# ユーティリティ
# =============================================================================

def _roi_emoji(roi: float) -> str:
    """補正回収率に対応した絵文字を返す。"""
    if roi >= 90:
        return "🟢"
    elif roi >= 80:
        return "🟡"
    else:
        return "🔴"


def _format_roi_table(table: pd.DataFrame) -> str:
    """
    _compute_roi_table の出力 DataFrame を Markdown テーブル文字列に変換する。

    テーブル列: ビン | 単勝件数 | 単勝的中率(%) | 複勝件数 | 複勝的中率(%) |
                単勝補正回収率 | 複勝補正回収率
    """
    if table.empty:
        return "_サンプル不足（最低30件未満）のためデータなし_\n"

    lines = []
    # ヘッダ
    lines.append("| ビン | 単勝件数 | 単勝的中率(%) | 複勝件数 | 複勝的中率(%) | 単勝補正回収率 | 複勝補正回収率 |")
    lines.append("|------|---------|------------|---------|------------|-----------|-----------|")

    for _, row in table.iterrows():
        t_roi = row.get("単勝補正回収率", 0.0)
        f_roi = row.get("複勝補正回収率", 0.0)
        t_icon = _roi_emoji(t_roi)
        f_icon = _roi_emoji(f_roi)
        lines.append(
            f"| {row['ビン']} "
            f"| {int(row.get('単勝件数', 0)):,} "
            f"| {row.get('単勝的中率(%)', 0.0):.1f} "
            f"| {int(row.get('複勝件数', 0)):,} "
            f"| {row.get('複勝的中率(%)', 0.0):.1f} "
            f"| {t_icon} {t_roi:.1f}% "
            f"| {f_icon} {f_roi:.1f}% |"
        )

    return "\n".join(lines) + "\n"


def _count_edges(table: pd.DataFrame) -> tuple[int, int]:
    """
    テーブル内のエッジあり行数（単勝 >= 80% / 複勝 >= 80%）をカウントする。

    Returns:
        (tansho_edge_count, fukusho_edge_count)
    """
    if table.empty:
        return 0, 0
    t_edges = int((table["単勝補正回収率"] >= EDGE_THRESHOLD).sum())
    f_edges = int((table["複勝補正回収率"] >= EDGE_THRESHOLD).sum())
    return t_edges, f_edges


# =============================================================================
# レポート生成
# =============================================================================

def _write_combo_report(
    combo: dict,
    seg_results: dict[str, pd.DataFrame],
    output_path: str,
) -> dict:
    """
    1組み合わせ分のMarkdownレポートを書き出す。

    Args:
        combo:        COMBINATIONS の1エントリ
        seg_results:  segment_label -> DataFrame の辞書
        output_path:  出力ファイルパス

    Returns:
        サマリー用の統計辞書
    """
    cid = combo["id"]
    name = combo["name"]
    segment = combo["segment"]
    is_skip = combo.get("skip", False)

    lines = []
    lines.append(f"# {cid}: {name}")
    lines.append("")
    lines.append(f"- **セグメント**: {segment}")
    lines.append(f"- **ファクター**: `{'`, `'.join(combo.get('_override_factors', combo['factors']))}`")
    lines.append(f"- **分析期間**: {DATA_START} 〜 {DATA_END}")
    lines.append(f"- **生成日時**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")

    total_t_edges = 0
    total_f_edges = 0
    total_rows = 0

    if is_skip:
        skip_reason = combo.get("skip_reason", "不明")
        lines.append("## ⚠️ スキップ")
        lines.append("")
        lines.append(f"**理由**: {skip_reason}")
        lines.append("")
        lines.append("このカラムは `jrd_kyi`（raw）テーブルにのみ存在するが、")
        lines.append("当該テーブルは 2026-03-14 の 517 行しかデータが存在せず、")
        lines.append("2016〜2025 年の分析には使用不可である。")
        lines.append("")
    else:
        if not seg_results:
            lines.append("_結果なし（データ不足またはエラー）_")
            lines.append("")
        else:
            for seg_label, table in sorted(seg_results.items()):
                lines.append(f"## {seg_label}")
                lines.append("")

                if isinstance(table, pd.DataFrame) and "reason" in table.columns:
                    # SKIP マーカー（通常は is_skip で捌くが念のため）
                    lines.append(f"_スキップ: {table['reason'].iloc[0]}_")
                    lines.append("")
                    continue

                lines.append(_format_roi_table(table))

                if not table.empty:
                    t_e, f_e = _count_edges(table)
                    total_t_edges += t_e
                    total_f_edges += f_e
                    total_rows += len(table)
                    lines.append(
                        f"> 単勝エッジあり: **{t_e}ビン** / {len(table)}ビン  |  "
                        f"複勝エッジあり: **{f_e}ビン** / {len(table)}ビン"
                    )
                    lines.append("")

        # サマリー
        lines.append("---")
        lines.append("")
        lines.append("## 全セグメント集計")
        lines.append("")
        lines.append(f"| 指標 | 値 |")
        lines.append(f"|------|-----|")
        lines.append(f"| 単勝エッジビン合計 | **{total_t_edges}** |")
        lines.append(f"| 複勝エッジビン合計 | **{total_f_edges}** |")
        lines.append(f"| 有効ビン数合計 | {total_rows} |")
        lines.append("")

    content = "\n".join(lines)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(content)

    logger.info("  -> %s （単勝エッジ %d / 複勝エッジ %d）", output_path, total_t_edges, total_f_edges)

    return {
        "id": cid,
        "name": name,
        "segment": segment,
        "skip": is_skip,
        "total_t_edges": total_t_edges,
        "total_f_edges": total_f_edges,
        "total_bins": total_rows,
    }


def _write_summary(stats_list: list[dict], output_path: str) -> None:
    """
    全組み合わせのサマリーレポートを書き出す。

    Args:
        stats_list:  各組み合わせのサマリー統計リスト
        output_path: 出力ファイルパス
    """
    lines = []
    lines.append("# 組み合わせファクター分析 サマリー")
    lines.append("")
    lines.append(f"- **分析期間**: {DATA_START} 〜 {DATA_END}")
    lines.append(f"- **生成日時**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append(f"- **組み合わせ総数**: {len(stats_list)}")
    active = [s for s in stats_list if not s["skip"]]
    skipped = [s for s in stats_list if s["skip"]]
    lines.append(f"  - 実行: {len(active)}  |  スキップ: {len(skipped)}")
    lines.append("")

    # セグメント別グループ
    lines.append("## 実行組み合わせ（エッジビン降順）")
    lines.append("")
    lines.append("| ID | 名称 | セグメント | 単勝エッジビン | 複勝エッジビン | 有効ビン数 |")
    lines.append("|---|---|---|---|---|---|")
    for s in sorted(active, key=lambda x: x["total_t_edges"], reverse=True):
        lines.append(
            f"| [{s['id']}]({s['id']}.md) "
            f"| {s['name']} "
            f"| {s['segment']} "
            f"| **{s['total_t_edges']}** "
            f"| **{s['total_f_edges']}** "
            f"| {s['total_bins']} |"
        )
    lines.append("")

    if skipped:
        lines.append("## スキップ組み合わせ")
        lines.append("")
        lines.append("| ID | 名称 | セグメント | スキップ理由 |")
        lines.append("|---|---|---|---|")
        for s in skipped:
            combo = next((c for c in COMBINATIONS if c["id"] == s["id"]), {})
            reason = combo.get("skip_reason", "不明")
            lines.append(
                f"| {s['id']} | {s['name']} | {s['segment']} | {reason} |"
            )
        lines.append("")

    content = "\n".join(lines)
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(content)
    logger.info("サマリー: %s", output_path)


# =============================================================================
# メイン
# =============================================================================

def main(target_combo_id: Optional[str] = None) -> None:
    """
    全組み合わせ（または指定組み合わせ）の分析レポートを生成する。

    Args:
        target_combo_id: 特定の combo_id のみ実行する場合に指定。None = 全実行。
    """
    logger.info("=== 組み合わせファクター分析レポート生成 開始 ===")
    logger.info("分析期間: %s 〜 %s", DATA_START, DATA_END)
    logger.info("出力先: %s", REPORT_DIR)

    # 実行対象の絞り込み
    target_combos = COMBINATIONS
    if target_combo_id:
        target_combos = [c for c in COMBINATIONS if c["id"] == target_combo_id]
        if not target_combos:
            logger.error("指定された combo_id '%s' が見つかりません。", target_combo_id)
            logger.error("有効な ID: %s", [c["id"] for c in COMBINATIONS])
            sys.exit(1)
        logger.info("対象組み合わせ: %s", target_combo_id)

    # DB 接続
    logger.info("DB 接続中...")
    conn = get_connection()

    try:
        # --- データセット構築（全組み合わせ共通で1回のみ）---
        if target_combo_id is None or True:
            # 常に全データセットをロードし、各コンボでフィルタ
            logger.info("組み合わせ分析実行中（%d 組み合わせ）...", len(target_combos))
            all_results = run_combination_analysis(conn, DATA_START, DATA_END)
        else:
            all_results = {}

        # --- 各組み合わせのレポート生成 ---
        stats_list = []
        for combo in target_combos:
            cid = combo["id"]
            seg_results = all_results.get(cid, {})
            out_path = os.path.join(REPORT_DIR, f"{cid}.md")
            stats = _write_combo_report(combo, seg_results, out_path)
            stats_list.append(stats)

        # --- サマリー生成（全実行時のみ）---
        if target_combo_id is None:
            summary_path = os.path.join(REPORT_DIR, "summary.md")
            _write_summary(stats_list, summary_path)

        # --- 完了メッセージ ---
        active_stats = [s for s in stats_list if not s["skip"]]
        total_t = sum(s["total_t_edges"] for s in active_stats)
        total_f = sum(s["total_f_edges"] for s in active_stats)
        logger.info("=== 完了 ===")
        logger.info("  実行組み合わせ : %d", len(active_stats))
        logger.info("  単勝エッジビン合計: %d", total_t)
        logger.info("  複勝エッジビン合計: %d", total_f)
        logger.info("  出力先: %s", REPORT_DIR)

    finally:
        conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="組み合わせファクター分析レポート生成")
    parser.add_argument(
        "--combo",
        type=str,
        default=None,
        help="特定の組み合わせ ID のみ実行する（例: course27_01）",
    )
    args = parser.parse_args()
    main(target_combo_id=args.combo)
