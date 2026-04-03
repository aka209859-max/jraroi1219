"""
Phase 2 交互作用分析レポート生成スクリプト

使用方法:
    cd /path/to/jraroi1219
    python -m roi_pipeline.reports.generate_phase2

    特定レポートのみ:
    python -m roi_pipeline.reports.generate_phase2 --report umaban
    python -m roi_pipeline.reports.generate_phase2 --report chokyo
    python -m roi_pipeline.reports.generate_phase2 --report kyusha

出力:
    roi_pipeline/reports/phase2/umaban_x_course.md
    roi_pipeline/reports/phase2/chokyo_shisu_x_surface.md
    roi_pipeline/reports/phase2/kyusha_shisu_x_surface.md

前提条件:
    - PostgreSQL (127.0.0.1:5432, database: pckeiba) に接続可能であること
    - Phase 1 レポートが生成済みであること
    - pip install -r roi_pipeline/requirements.txt 済みであること
"""
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional, Set

import pandas as pd
import numpy as np

from roi_pipeline.engine.corrected_return import (
    calc_corrected_return_rate,
    BASELINE_RATE,
)
from roi_pipeline.engine.hierarchical_bayes import (
    hierarchical_bayes_estimate,
    three_level_estimate,
    BayesEstimate,
)
from roi_pipeline.engine.interaction_analysis import (
    InteractionResult,
    InteractionCell,
    assign_course_category_fast,
    assign_surface,
    run_interaction_analysis,
)
from roi_pipeline.engine.walk_forward import (
    run_walk_forward,
    WalkForwardConfig,
)
from roi_pipeline.factors.definitions import (
    FACTOR_DEFINITIONS,
    FactorDefinition,
    FactorType,
    get_factor_by_id,
)
from roi_pipeline.factors.binning import apply_binning
from roi_pipeline.config.course_categories import ALL_CATEGORIES


# レポート出力先
REPORT_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "phase2"
)


def ensure_report_dir() -> None:
    """レポート出力ディレクトリが存在することを保証する。"""
    os.makedirs(REPORT_DIR, exist_ok=True)


# ====================================================================
# レポート生成関数
# ====================================================================


def generate_umaban_x_course_report(
    df: pd.DataFrame,
    global_rate: float,
) -> str:
    """
    馬番(1-18) × コースカテゴリ(27) の交互作用分析レポートを生成する。

    Args:
        df: 前処理済みベースデータ
        global_rate: グローバル補正回収率

    Returns:
        Markdownレポート文字列
    """
    lines: List[str] = []
    lines.append("# Phase 2 タスク1: 馬番 × コース分類 交互作用分析")
    lines.append("")
    lines.append(f"生成日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")

    # ---- 1. 概要 ----
    lines.append("## 1. 分析概要")
    lines.append("")
    lines.append("Phase 1で馬番に8つのエッジビン（4,8,10,12,14,15,16,17番）が検出された。")
    lines.append("Phase 2ではこれを27カテゴリのコース分類で分解し、")
    lines.append("馬番のエッジが「全コース共通の現象」か「特定のコース形状でのみ発生する現象」かを判別する。")
    lines.append("")
    lines.append(f"- **ファクター軸**: 馬番（1〜18）")
    lines.append(f"- **セグメント軸**: 27カテゴリコース分類（芝15 + ダート12）")
    lines.append(f"- **階層ベイズ**: 3層（グローバル → ファクター値別 → 個別セル）")
    lines.append(f"- **グローバル補正回収率**: {global_rate:.2f}%")
    lines.append("")

    # ---- 2. コースカテゴリ付与 ----
    print("    コースカテゴリ付与中...")
    df_work = df.copy()
    df_work["course_category"] = assign_course_category_fast(df)

    # unknown除外
    known_mask = df_work["course_category"] != "unknown"
    n_unknown = (~known_mask).sum()
    df_work = df_work[known_mask].copy()

    lines.append("## 2. データ概要")
    lines.append("")
    lines.append(f"- 総レコード数: {len(df):,}")
    lines.append(f"- コースカテゴリ付与済み: {len(df_work):,}")
    lines.append(f"- unknown（分類不能）: {n_unknown:,}")
    lines.append("")

    # カテゴリ別サンプル数
    lines.append("### セグメント別サンプル数")
    lines.append("")
    lines.append("| コースカテゴリ | N |")
    lines.append("|-------------|---|")
    cat_counts = df_work["course_category"].value_counts().sort_index()
    for cat, cnt in cat_counts.items():
        lines.append(f"| {cat} | {cnt:,} |")
    lines.append("")

    # ---- 3. 交互作用分析 ----
    print("    交互作用分析実行中...")
    result = run_interaction_analysis(
        df=df_work,
        factor_col="umaban",
        segment_col="course_category",
        global_rate=global_rate,
        factor_name="馬番",
        segment_name="コースカテゴリ",
        min_samples=30,
    )

    # ---- 4. レベル1: 馬番別グローバル回収率 ----
    lines.append("## 3. レベル1: 馬番別グローバル補正回収率")
    lines.append("")
    lines.append("| 馬番 | N | 補正回収率(%) | Phase1エッジ |")
    lines.append("|------|---|------------|------------|")
    phase1_edge_bins = {"4", "8", "10", "12", "14", "15", "16", "17",
                        "4.0", "8.0", "10.0", "12.0", "14.0", "15.0", "16.0", "17.0"}
    for fval in sorted(result.factor_rates.keys(), key=lambda x: float(x) if x.replace(".", "").isdigit() else 999):
        rate = result.factor_rates[fval]
        n = result.factor_n[fval]
        # Phase 1 エッジ判定
        edge_mark = "**YES**" if fval in phase1_edge_bins else "No"
        lines.append(f"| {fval} | {n:,} | {rate:.2f} | {edge_mark} |")
    lines.append("")

    # ---- 5. クロス集計マトリックス（エッジセルのみ） ----
    lines.append("## 4. 交互作用分析結果（全セル）")
    lines.append("")
    lines.append(f"- 分析セル数: {len(result.cells):,}")
    lines.append(f"- エッジセル数: {result.n_edge_cells}")
    lines.append(f"- エッジ出現ファクター値: {sorted(result.edge_factor_values, key=lambda x: float(x) if x.replace('.', '').isdigit() else 999)}")
    lines.append(f"- エッジ出現セグメント: {sorted(result.edge_segment_values)}")
    lines.append("")

    lines.append("### エッジ判定=YES のセル一覧")
    lines.append("")
    if result.edge_cells:
        lines.append("| 馬番 | コースカテゴリ | N | 的中率(%) | 実測回収率(%) | ベイズ推定(%) | 95%CI下限 | 95%CI上限 | 得点 |")
        lines.append("|------|-------------|---|---------|------------|------------|---------|---------|------|")
        for cell in sorted(result.edge_cells,
                           key=lambda c: c.bayes_estimate.score, reverse=True):
            b = cell.bayes_estimate
            lines.append(
                f"| {cell.factor_value} "
                f"| {cell.segment_value} "
                f"| {cell.n_samples:,} "
                f"| {cell.hit_rate:.2f} "
                f"| {cell.observed_rate:.2f} "
                f"| {b.estimated_rate:.2f} "
                f"| {b.ci_lower:.2f} "
                f"| {b.ci_upper:.2f} "
                f"| {b.score:.2f} |"
            )
    else:
        lines.append("エッジ判定=YESのセルなし。")
    lines.append("")

    # ---- 6. 全セル詳細テーブル ----
    lines.append("### 全セル詳細（得点上位50件）")
    lines.append("")
    lines.append("| 馬番 | コースカテゴリ | N | 的中率(%) | 実測回収率(%) | ベイズ推定(%) | 95%CI下限 | 得点 | エッジ |")
    lines.append("|------|-------------|---|---------|------------|------------|---------|------|--------|")
    sorted_cells = sorted(result.cells,
                          key=lambda c: c.bayes_estimate.score, reverse=True)
    for cell in sorted_cells[:50]:
        b = cell.bayes_estimate
        edge_flag = "**YES**" if cell.is_edge else "No"
        lines.append(
            f"| {cell.factor_value} "
            f"| {cell.segment_value} "
            f"| {cell.n_samples:,} "
            f"| {cell.hit_rate:.2f} "
            f"| {cell.observed_rate:.2f} "
            f"| {b.estimated_rate:.2f} "
            f"| {b.ci_lower:.2f} "
            f"| {b.score:.2f} "
            f"| {edge_flag} |"
        )
    lines.append("")

    # ---- 7. 判別結果 ----
    lines.append("## 5. 判別結果")
    lines.append("")

    # 馬番別のエッジ出現カテゴリ数を集計
    factor_edge_categories: Dict[str, Set[str]] = {}
    for cell in result.edge_cells:
        if cell.factor_value not in factor_edge_categories:
            factor_edge_categories[cell.factor_value] = set()
        factor_edge_categories[cell.factor_value].add(cell.segment_value)

    total_categories = len(cat_counts)
    lines.append("### 馬番別エッジ出現パターン")
    lines.append("")
    lines.append("| 馬番 | エッジ出現カテゴリ数 | 全カテゴリ比 | 判定 |")
    lines.append("|------|-----------------|-----------|------|")
    for fval in sorted(factor_edge_categories.keys(),
                       key=lambda x: float(x) if x.replace(".", "").isdigit() else 999):
        cats = factor_edge_categories[fval]
        ratio = len(cats) / total_categories * 100
        if ratio >= 50:
            judgment = "**全コース共通型**"
        elif ratio >= 20:
            judgment = "特定コース偏在型"
        else:
            judgment = "限定的"
        lines.append(f"| {fval} | {len(cats)} / {total_categories} | {ratio:.0f}% | {judgment} |")
    lines.append("")

    # 全体判定
    if result.n_edge_cells > 0:
        avg_factor_coverage = np.mean([len(v) / total_categories for v in factor_edge_categories.values()]) * 100
        if avg_factor_coverage >= 40:
            conclusion = "馬番エッジは**多くのコース形状で共通して発生する現象**である。"
        elif avg_factor_coverage >= 15:
            conclusion = "馬番エッジは**特定のコース形状に偏在する傾向**がある。"
        else:
            conclusion = "馬番エッジは**特定のコース形状でのみ発生する限定的な現象**である。"
        lines.append(f"### 総合判定")
        lines.append("")
        lines.append(conclusion)
        lines.append(f"（平均カテゴリカバー率: {avg_factor_coverage:.1f}%）")
    else:
        lines.append("### 総合判定")
        lines.append("")
        lines.append("Phase 2レベルでエッジが検出されなかった。Phase 1のエッジはサンプルサイズの影響で消失した可能性がある。")

    lines.append("")
    lines.append("---")
    lines.append(f"*生成日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")

    return "\n".join(lines)


def generate_factor_x_surface_report(
    df: pd.DataFrame,
    factor_id: int,
    global_rate: float,
    output_filename: str,
) -> str:
    """
    ファクター × 芝/ダート の交互作用分析レポートを生成する。

    Args:
        df: 前処理済みベースデータ
        factor_id: ファクターID (18=調教師指数, 20=厩舎指数)
        global_rate: グローバル補正回収率
        output_filename: 出力ファイル名（拡張子なし）

    Returns:
        Markdownレポート文字列
    """
    factor = get_factor_by_id(factor_id)
    lines: List[str] = []
    lines.append(f"# Phase 2: {factor.name} × 芝/ダート 交互作用分析")
    lines.append("")
    lines.append(f"生成日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")

    # ---- 1. 概要 ----
    lines.append("## 1. 分析概要")
    lines.append("")
    lines.append(f"Phase 1で{factor.name}にエッジビンが検出された。")
    lines.append(f"Phase 2ではこれを芝/ダートの2セグメントで分解し、")
    lines.append(f"エッジが「芝/ダート共通」か「片方のみ」かを判別する。")
    lines.append("")
    lines.append(f"- **ファクター**: {factor.name}（#{factor.id}, `{factor.column}`）")
    lines.append(f"- **データ型**: {factor.factor_type.value}")
    lines.append(f"- **セグメント軸**: 芝 / ダート")
    lines.append(f"- **階層ベイズ**: 3層（グローバル → ビン別 → ビン×芝ダ）")
    lines.append(f"- **グローバル補正回収率**: {global_rate:.2f}%")
    lines.append("")

    # ---- 2. ビン分割 + 芝ダ付与 ----
    print(f"    {factor.name}: ビン分割＋芝ダート付与中...")
    df_work = df.copy()

    # 芝/ダート付与
    df_work["surface_type"] = assign_surface(df_work)
    # 不明除外
    valid_surface = df_work["surface_type"].isin(["芝", "ダート"])
    df_work = df_work[valid_surface].copy()

    # ビン分割
    binned_series, bin_col = apply_binning(df_work, factor)
    df_work[bin_col] = binned_series

    # 有効データのみ
    df_work = df_work.dropna(subset=[bin_col]).copy()

    lines.append("## 2. データ概要")
    lines.append("")
    lines.append(f"- 有効レコード数: {len(df_work):,}")
    surf_counts = df_work["surface_type"].value_counts()
    for surf, cnt in surf_counts.items():
        lines.append(f"- {surf}: {cnt:,}")
    lines.append("")

    # ビン別サンプル数
    lines.append("### ビン別サンプル数")
    lines.append("")
    lines.append("| ビン | 芝 N | ダート N | 合計 N |")
    lines.append("|------|------|---------|-------|")
    bin_surf_ct = df_work.groupby([bin_col, "surface_type"]).size().unstack(fill_value=0)
    for bval in sorted(bin_surf_ct.index, key=lambda x: str(x)):
        turf_n = int(bin_surf_ct.loc[bval].get("芝", 0))
        dirt_n = int(bin_surf_ct.loc[bval].get("ダート", 0))
        lines.append(f"| {bval} | {turf_n:,} | {dirt_n:,} | {turf_n + dirt_n:,} |")
    lines.append("")

    # ---- 3. 交互作用分析 ----
    print(f"    {factor.name}: 交互作用分析実行中...")
    result = run_interaction_analysis(
        df=df_work,
        factor_col=bin_col,
        segment_col="surface_type",
        global_rate=global_rate,
        factor_name=factor.name,
        segment_name="芝/ダート",
        min_samples=30,
    )

    # ---- 4. レベル1: ビン別グローバル回収率（Phase 1エッジ情報含む） ----
    lines.append("## 3. レベル1: ビン別グローバル補正回収率")
    lines.append("")

    # Phase 1のエッジビン情報を取得
    phase1_report_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "phase1",
        f"factor_{factor.id:02d}_{factor.column}.md"
    )
    phase1_edge_bins = _extract_phase1_edge_bins(phase1_report_path)

    lines.append("| ビン | N | 補正回収率(%) | Phase1エッジ |")
    lines.append("|------|---|------------|------------|")
    for fval in sorted(result.factor_rates.keys(), key=lambda x: str(x)):
        rate = result.factor_rates[fval]
        n = result.factor_n[fval]
        edge_mark = "**YES**" if fval in phase1_edge_bins else "No"
        lines.append(f"| {fval} | {n:,} | {rate:.2f} | {edge_mark} |")
    lines.append("")

    # ---- 5. 全セル結果 ----
    lines.append("## 4. 交互作用分析結果")
    lines.append("")
    lines.append(f"- 分析セル数: {len(result.cells):,}")
    lines.append(f"- エッジセル数: {result.n_edge_cells}")
    lines.append("")

    lines.append("### 全セル詳細")
    lines.append("")
    lines.append("| ビン | 芝/ダ | N | 的中率(%) | 実測回収率(%) | ベイズ推定(%) | 95%CI下限 | 95%CI上限 | 得点 | エッジ |")
    lines.append("|------|------|---|---------|------------|------------|---------|---------|------|--------|")
    sorted_cells = sorted(result.cells,
                          key=lambda c: c.bayes_estimate.score, reverse=True)
    for cell in sorted_cells:
        b = cell.bayes_estimate
        edge_flag = "**YES**" if cell.is_edge else "No"
        ph1_mark = " (Ph1)" if cell.factor_value in phase1_edge_bins else ""
        lines.append(
            f"| {cell.factor_value}{ph1_mark} "
            f"| {cell.segment_value} "
            f"| {cell.n_samples:,} "
            f"| {cell.hit_rate:.2f} "
            f"| {cell.observed_rate:.2f} "
            f"| {b.estimated_rate:.2f} "
            f"| {b.ci_lower:.2f} "
            f"| {b.ci_upper:.2f} "
            f"| {b.score:.2f} "
            f"| {edge_flag} |"
        )
    lines.append("")

    # ---- 6. 芝/ダート別エッジ集計 ----
    lines.append("## 5. 芝/ダート別エッジ集計")
    lines.append("")
    edge_by_surface: Dict[str, List[InteractionCell]] = {"芝": [], "ダート": []}
    for cell in result.edge_cells:
        if cell.segment_value in edge_by_surface:
            edge_by_surface[cell.segment_value].append(cell)

    for surf in ["芝", "ダート"]:
        edges = edge_by_surface[surf]
        lines.append(f"### {surf}のエッジビン ({len(edges)}個)")
        lines.append("")
        if edges:
            for cell in sorted(edges, key=lambda c: c.bayes_estimate.score, reverse=True):
                b = cell.bayes_estimate
                lines.append(f"- **{cell.factor_value}**: N={cell.n_samples:,}, "
                             f"推定={b.estimated_rate:.2f}%, CI[{b.ci_lower:.2f}, {b.ci_upper:.2f}], "
                             f"得点={b.score:.2f}")
        else:
            lines.append("エッジなし。")
        lines.append("")

    # ---- 7. 判別結果 ----
    lines.append("## 6. 判別結果")
    lines.append("")

    turf_edge_bins = {c.factor_value for c in edge_by_surface["芝"]}
    dirt_edge_bins = {c.factor_value for c in edge_by_surface["ダート"]}
    common_bins = turf_edge_bins & dirt_edge_bins
    turf_only = turf_edge_bins - dirt_edge_bins
    dirt_only = dirt_edge_bins - turf_edge_bins

    lines.append(f"| 区分 | エッジビン数 | ビン一覧 |")
    lines.append(f"|------|-----------|---------|")
    lines.append(f"| 芝/ダート共通 | {len(common_bins)} | {', '.join(sorted(common_bins, key=str))} |")
    lines.append(f"| 芝のみ | {len(turf_only)} | {', '.join(sorted(turf_only, key=str))} |")
    lines.append(f"| ダートのみ | {len(dirt_only)} | {', '.join(sorted(dirt_only, key=str))} |")
    lines.append("")

    if len(common_bins) > 0 and len(common_bins) >= max(len(turf_only), len(dirt_only)):
        conclusion = f"{factor.name}のエッジは**芝・ダート共通の現象**が主体である。"
    elif len(turf_edge_bins) > len(dirt_edge_bins):
        conclusion = f"{factor.name}のエッジは**芝に偏在**する傾向がある。"
    elif len(dirt_edge_bins) > len(turf_edge_bins):
        conclusion = f"{factor.name}のエッジは**ダートに偏在**する傾向がある。"
    else:
        conclusion = f"{factor.name}のエッジは芝とダートで異なるパターンを示す。"

    lines.append(f"### 総合判定")
    lines.append("")
    lines.append(conclusion)

    # Phase 1との整合性
    lines.append("")
    lines.append("### Phase 1エッジとの整合性")
    lines.append("")
    phase1_in_phase2 = phase1_edge_bins & (turf_edge_bins | dirt_edge_bins)
    phase1_lost = phase1_edge_bins - (turf_edge_bins | dirt_edge_bins)
    new_in_phase2 = (turf_edge_bins | dirt_edge_bins) - phase1_edge_bins

    lines.append(f"- Phase 1エッジビン: {len(phase1_edge_bins)}個")
    lines.append(f"- Phase 2で維持: {len(phase1_in_phase2)}個 {sorted(phase1_in_phase2, key=str)}")
    lines.append(f"- Phase 2で消失: {len(phase1_lost)}個 {sorted(phase1_lost, key=str)}")
    lines.append(f"- Phase 2で新規発見: {len(new_in_phase2)}個 {sorted(new_in_phase2, key=str)}")

    lines.append("")
    lines.append("---")
    lines.append(f"*生成日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")

    return "\n".join(lines)


def _extract_phase1_edge_bins(report_path: str) -> Set[str]:
    """
    Phase 1 レポートからエッジビン一覧を抽出する。

    Args:
        report_path: Phase 1 レポートファイルパス

    Returns:
        エッジビン値の集合
    """
    edge_bins: Set[str] = set()
    try:
        with open(report_path, "r", encoding="utf-8") as f:
            in_edge_section = False
            for line in f:
                line = line.strip()
                if "該当ビン/カテゴリ:" in line:
                    in_edge_section = True
                    continue
                if in_edge_section:
                    if line.startswith("- "):
                        bin_val = line[2:].strip()
                        edge_bins.add(bin_val)
                    elif line.startswith("---") or line == "":
                        if edge_bins:
                            break
    except FileNotFoundError:
        pass
    return edge_bins


# ====================================================================
# メイン実行
# ====================================================================


def main(target_reports: Optional[List[str]] = None) -> None:
    """
    メイン実行関数。

    Args:
        target_reports: 生成対象のレポートリスト。Noneなら全3レポート。
                        例: ["umaban", "chokyo", "kyusha"]
    """
    ensure_report_dir()

    # コマンドライン引数
    if target_reports is None and "--report" in sys.argv:
        idx = sys.argv.index("--report")
        if idx + 1 < len(sys.argv):
            target_reports = [sys.argv[idx + 1]]

    all_reports = ["umaban", "chokyo", "kyusha"]
    if target_reports is None:
        target_reports = all_reports

    print("=" * 60)
    print("Phase 2: 交互作用分析レポート生成")
    print(f"対象: {', '.join(target_reports)}")
    print("=" * 60)
    print()

    # 1. データ取得
    print("[1/3] PostgreSQLからベースデータを取得中...")
    from roi_pipeline.engine.data_loader import convert_numeric_columns
    from roi_pipeline.reports.generate_phase1 import add_hit_flag, add_race_year

    try:
        from roi_pipeline.engine.data_loader_v2 import load_base_race_data_v2
        df = load_base_race_data_v2(date_from="20161101", date_to="20251231")
        print(f"  ✅ v2 (jrd_*_fixed) テーブル使用: {len(df):,} 行")
    except Exception as e:
        print(f"  v2 ERROR: {e}")
        from roi_pipeline.engine.data_loader import load_base_race_data
        df = load_base_race_data(date_from="20161101", date_to="20251231")
        print(f"  v1 フォールバック: {len(df):,} 行")

    # 2. 前処理
    print("[2/3] データ前処理中...")
    df = convert_numeric_columns(df)
    df = add_hit_flag(df)
    df = add_race_year(df)

    # race_id
    if "race_id" not in df.columns:
        id_cols = ["keibajo_code", "race_date", "race_bango"]
        if all(c in df.columns for c in id_cols):
            df["race_id"] = (
                df["keibajo_code"].astype(str).str.strip()
                + "_" + df["race_date"].astype(str).str.strip()
                + "_" + df["race_bango"].astype(str).str.strip()
            )

    # オッズ変換（JRA-VAN 10倍単位格納→実オッズ）
    if "tansho_odds" in df.columns:
        median_val = df["tansho_odds"].dropna().median()
        if median_val >= 10.0:
            print(f"  tansho_odds JRA-VAN 10倍単位変換 (median={median_val:.1f})")
            df["tansho_odds"] = df["tansho_odds"] / 10.0

    print(f"  前処理完了: {len(df):,} rows")
    print()

    # 3. グローバル回収率
    print("[2.5] グローバル補正回収率算出...")
    global_result = calc_corrected_return_rate(df)
    global_rate = global_result["corrected_return_rate"]
    print(f"  グローバル補正回収率: {global_rate:.2f}%")
    print()

    # 4. レポート生成
    print(f"[3/3] レポート生成中...")

    for report_key in target_reports:
        if report_key == "umaban":
            print(f"  [umaban] 馬番 × コースカテゴリ(27)...")
            report_text = generate_umaban_x_course_report(df, global_rate)
            filename = "umaban_x_course.md"

        elif report_key == "chokyo":
            print(f"  [chokyo] 調教師指数 × 芝/ダート...")
            report_text = generate_factor_x_surface_report(
                df, factor_id=18, global_rate=global_rate,
                output_filename="chokyo_shisu_x_surface",
            )
            filename = "chokyo_shisu_x_surface.md"

        elif report_key == "kyusha":
            print(f"  [kyusha] 厩舎指数 × 芝/ダート...")
            report_text = generate_factor_x_surface_report(
                df, factor_id=20, global_rate=global_rate,
                output_filename="kyusha_shisu_x_surface",
            )
            filename = "kyusha_shisu_x_surface.md"

        else:
            print(f"  [SKIP] 不明なレポートキー: {report_key}")
            continue

        filepath = os.path.join(REPORT_DIR, filename)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(report_text)
        print(f"    ✅ 保存: {filepath}")

    print()
    print("=" * 60)
    print("Phase 2 完了!")
    print(f"  レポートディレクトリ: {REPORT_DIR}")
    print("=" * 60)


if __name__ == "__main__":
    main()
