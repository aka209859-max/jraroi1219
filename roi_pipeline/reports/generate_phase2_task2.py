"""
Phase 2 タスク2: 残り10ファクターの交互作用分析レポート生成

10ファクター × セグメント分析:
    SURFACE_2 (7): IDM, 総合指数, 上がり指数, ペース指数, 騎手指数, LS指数, 馬齢
    COURSE_27 (2): 距離適性, コース適性
    GLOBAL    (1): 馬場状態コード

単勝・複勝の両方のROI計算を実施する。

使用方法:
    cd /path/to/jraroi1219
    python -m roi_pipeline.reports.generate_phase2_task2

    特定ファクターのみ:
    python -m roi_pipeline.reports.generate_phase2_task2 --factor idm
    python -m roi_pipeline.reports.generate_phase2_task2 --factor sogo_shisu

出力:
    roi_pipeline/reports/phase2/{factor_column}_{segment_type}.md (10ファイル)

前提条件:
    - PostgreSQL (127.0.0.1:5432, database: pckeiba) に接続可能であること
    - Phase 1 レポートが生成済みであること
"""
import os
import sys
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd
import numpy as np

from roi_pipeline.engine.corrected_return import (
    calc_corrected_return_rate,
    calc_return_rate_by_bins,
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

# ====================================================================
# Phase 2 タスク2: 10ファクターの定義
# ====================================================================

# (factor_id, segment_type, Phase1エッジビン数)
TASK2_FACTORS: List[Tuple[int, str, int]] = [
    # SURFACE_2 (7)
    (1, "SURFACE_2", 5),     # IDM
    (2, "SURFACE_2", 4),     # 総合指数
    (3, "SURFACE_2", 7),     # 上がり指数
    (4, "SURFACE_2", 5),     # ペース指数
    (17, "SURFACE_2", 5),    # 騎手指数
    (19, "SURFACE_2", 5),    # LS指数
    (14, "SURFACE_2", 3),    # 馬齢
    # COURSE_27 (2)
    (5, "COURSE_27", 2),     # 距離適性
    (6, "COURSE_27", 2),     # コース適性
    # GLOBAL (1)
    (8, "GLOBAL", 3),        # 馬場状態コード
]


def ensure_report_dir() -> None:
    """レポート出力ディレクトリが存在することを保証する。"""
    os.makedirs(REPORT_DIR, exist_ok=True)


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


def _prepare_fukusho_df(df: pd.DataFrame) -> Optional[pd.DataFrame]:
    """
    複勝ROI計算用のDataFrameを準備する。

    jvd_hr（払戻テーブル）からの複勝オッズは的中馬（3着以内）のみ
    non-NULLで、非的中馬はNULLになる。

    計算方式:
        - fukusho_odds が non-NULL → fukusho_is_hit = 1
        - fukusho_odds が NULL → fukusho_is_hit = 0
        - 非的中馬のオッズは tansho_odds ベースで推定
          （均等払戻方式の分母計算に必要）
        - 推定式: fukusho_odds_est ≈ tansho_odds * 0.35
          （JRA複勝オッズは単勝の約30-40%が経験則）

    Args:
        df: fukusho_odds カラムを含むDataFrame

    Returns:
        fukusho_is_hit, fukusho_odds_filled が準備されたDataFrame
        データ不足の場合は None
    """
    if "fukusho_odds" not in df.columns:
        return None

    has_fukusho = df["fukusho_odds"].notna().sum()
    if has_fukusho == 0:
        return None

    fdf = df.copy()

    # 的中フラグ: fukusho_odds が non-NULL = 的中
    fdf["fukusho_is_hit"] = fdf["fukusho_odds"].notna().astype(int)

    # 非的中馬のオッズ推定（分母の bet_amount 計算に必要）
    # 方式: tansho_odds * 0.35 を代入（JRA経験則）
    # tansho_odds も無い場合はグローバル中央値を使用
    if "tansho_odds" in fdf.columns:
        tansho = pd.to_numeric(fdf["tansho_odds"], errors="coerce")
        estimated_fukusho = tansho * 0.35
        # 最低オッズ 1.0 を保証
        estimated_fukusho = estimated_fukusho.clip(lower=1.0)
    else:
        estimated_fukusho = pd.Series(3.0, index=fdf.index)

    # 的中馬は実オッズ、非的中馬は推定オッズ
    fdf["fukusho_odds"] = fdf["fukusho_odds"].fillna(estimated_fukusho)

    return fdf


def _compute_dual_roi(
    df: pd.DataFrame,
    odds_col: str = "tansho_odds",
    hit_flag_col: str = "is_hit",
    year_col: str = "race_year",
) -> Dict[str, dict]:
    """
    単勝・複勝の両方のROIを算出する。

    Args:
        df: 対象データ
        odds_col: 単勝オッズカラム名
        hit_flag_col: 的中フラグカラム名
        year_col: 年度カラム名

    Returns:
        {"tansho": {...}, "fukusho": {...}}
    """
    tansho_result = calc_corrected_return_rate(
        df, odds_col=odds_col, hit_flag_col=hit_flag_col,
        year_col=year_col, is_fukusho=False,
    )

    # 複勝: fukusho_odds カラムが存在し、non-NULLデータがある場合のみ
    fukusho_df = _prepare_fukusho_df(df)
    if fukusho_df is not None and len(fukusho_df) > 0:
        fukusho_result = calc_corrected_return_rate(
            fukusho_df, odds_col="fukusho_odds",
            hit_flag_col="fukusho_is_hit",
            year_col=year_col, is_fukusho=True,
        )
    else:
        fukusho_result = _empty_roi_result()

    return {"tansho": tansho_result, "fukusho": fukusho_result}


def _empty_roi_result() -> dict:
    """空のROI結果を返す。"""
    return {
        "corrected_return_rate": 0.0,
        "score": -BASELINE_RATE,
        "total_weighted_bet": 0.0,
        "total_weighted_payout": 0.0,
        "n_samples": 0,
        "n_hits": 0,
        "hit_rate": 0.0,
    }


# ====================================================================
# SURFACE_2 レポート生成
# ====================================================================

def generate_surface2_report(
    df: pd.DataFrame,
    factor_id: int,
    global_rate: float,
    phase1_edge_count: int,
) -> str:
    """
    ファクター × 芝/ダート(SURFACE_2) の交互作用レポートを生成する。
    単勝・複勝の両方のROIを含む。

    Args:
        df: 前処理済みベースデータ
        factor_id: ファクターID
        global_rate: グローバル補正回収率（単勝）
        phase1_edge_count: Phase 1のエッジビン数

    Returns:
        Markdownレポート文字列
    """
    factor = get_factor_by_id(factor_id)
    lines: List[str] = []

    lines.append(f"# Phase 2 タスク2: {factor.name} × 芝/ダート 交互作用分析")
    lines.append("")
    lines.append(f"生成日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")

    # ---- 1. 基本情報 ----
    lines.append("## 1. 基本情報")
    lines.append("")
    lines.append(f"| 項目 | 値 |")
    lines.append(f"|------|-----|")
    lines.append(f"| ファクター | {factor.name} (#{factor.id}) |")
    lines.append(f"| カラム | `{factor.column}` |")
    lines.append(f"| テーブル | `{factor.table}` |")
    lines.append(f"| データ型 | {factor.factor_type.value} |")
    lines.append(f"| セグメント | SURFACE_2 (芝/ダート) |")
    lines.append(f"| Phase 1 エッジビン数 | {phase1_edge_count} |")
    lines.append(f"| グローバル補正回収率（単勝） | {global_rate:.2f}% |")
    lines.append(f"| 階層ベイズ | 3層（グローバル → ビン別 → ビン×芝ダ） |")
    lines.append("")

    # ---- 2. データ準備 ----
    print(f"    {factor.name}: データ準備中...")
    df_work = df.copy()

    # 芝/ダート付与
    df_work["surface_type"] = assign_surface(df_work)
    valid_surface = df_work["surface_type"].isin(["芝", "ダート"])
    df_work = df_work[valid_surface].copy()

    # ビン分割
    binned_series, bin_col = apply_binning(df_work, factor)
    df_work[bin_col] = binned_series
    df_work = df_work.dropna(subset=[bin_col]).copy()

    lines.append("## 2. データ概要")
    lines.append("")
    lines.append(f"- 有効レコード数: {len(df_work):,}")
    surf_counts = df_work["surface_type"].value_counts()
    for surf, cnt in surf_counts.items():
        lines.append(f"- {surf}: {cnt:,}")
    lines.append("")

    # ---- 3. グローバルROI（単勝・複勝） ----
    dual_roi_global = _compute_dual_roi(df_work)
    lines.append("### グローバルROI")
    lines.append("")
    lines.append("| 券種 | N | 的中率(%) | 補正回収率(%) |")
    lines.append("|------|---|---------|------------|")
    lines.append(
        f"| 単勝 | {dual_roi_global['tansho']['n_samples']:,} "
        f"| {dual_roi_global['tansho']['hit_rate']:.2f} "
        f"| {dual_roi_global['tansho']['corrected_return_rate']:.2f} |"
    )
    if dual_roi_global['fukusho']['n_samples'] > 0:
        lines.append(
            f"| 複勝 | {dual_roi_global['fukusho']['n_samples']:,} "
            f"| {dual_roi_global['fukusho']['hit_rate']:.2f} "
            f"| {dual_roi_global['fukusho']['corrected_return_rate']:.2f} |"
        )
    else:
        lines.append("| 複勝 | - | - | （データなし） |")
    lines.append("")

    # ---- 4. 交互作用分析（単勝） ----
    print(f"    {factor.name}: 交互作用分析（単勝）実行中...")
    result = run_interaction_analysis(
        df=df_work,
        factor_col=bin_col,
        segment_col="surface_type",
        global_rate=global_rate,
        factor_name=factor.name,
        segment_name="芝/ダート",
        min_samples=30,
    )

    # Phase 1 エッジビン取得
    phase1_report_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "phase1",
        f"factor_{factor.id:02d}_{factor.column}.md"
    )
    phase1_edge_bins = _extract_phase1_edge_bins(phase1_report_path)

    # ---- 5. セグメント×ビン テーブル（単勝） ----
    lines.append("## 3. セグメント×ビン テーブル（単勝）")
    lines.append("")
    lines.append(f"- 分析セル数: {len(result.cells):,}")
    lines.append(f"- エッジセル数: {result.n_edge_cells}")
    lines.append("")

    lines.append("| ビン | 芝/ダ | N | 単勝的中率(%) | 単勝補正回収率(%) | ベイズ推定(%) | 95%CI下限 | 95%CI上限 | 得点 | エッジ |")
    lines.append("|------|------|---|------------|----------------|------------|---------|---------|------|--------|")
    sorted_cells = sorted(result.cells,
                          key=lambda c: c.bayes_estimate.score, reverse=True)
    for cell in sorted_cells:
        b = cell.bayes_estimate
        edge_flag = "**YES**" if cell.is_edge else "No"
        ph1_mark = " ★" if cell.factor_value in phase1_edge_bins else ""
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

    # ---- 6. 複勝 交互作用テーブル ----
    lines.append("## 4. セグメント×ビン テーブル（複勝）")
    lines.append("")
    df_fukusho = _prepare_fukusho_df(df_work)
    if df_fukusho is not None:
        fukusho_global = calc_corrected_return_rate(
            df_fukusho, odds_col="fukusho_odds",
            hit_flag_col="fukusho_is_hit", is_fukusho=True,
        )
        fukusho_result = run_interaction_analysis(
            df=df_fukusho,
            factor_col=bin_col,
            segment_col="surface_type",
            global_rate=fukusho_global["corrected_return_rate"],
            factor_name=factor.name,
            segment_name="芝/ダート（複勝）",
            min_samples=30,
        )
        lines.append(f"- グローバル複勝回収率: {fukusho_global['corrected_return_rate']:.2f}%")
        lines.append(f"- 複勝データソース: jvd_hr（払戻テーブル）からUNPIVOT")
        lines.append(f"- エッジセル数（複勝）: {fukusho_result.n_edge_cells}")
        lines.append("")
        lines.append("| ビン | 芝/ダ | N | 複勝的中率(%) | 複勝補正回収率(%) | ベイズ推定(%) | 95%CI下限 | 95%CI上限 | 得点 | エッジ |")
        lines.append("|------|------|---|------------|----------------|------------|---------|---------|------|--------|")
        sorted_fk = sorted(fukusho_result.cells,
                           key=lambda c: c.bayes_estimate.score, reverse=True)
        for cell in sorted_fk:
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
                f"| {b.ci_upper:.2f} "
                f"| {b.score:.2f} "
                f"| {edge_flag} |"
            )
        lines.append("")
    else:
        lines.append("複勝オッズデータなし。単勝のみで分析。")
        lines.append("")

    # ---- 7. Phase 1 エッジ比較 ----
    lines.append("## 5. Phase 1 エッジとの比較")
    lines.append("")

    edge_by_surface: Dict[str, List[InteractionCell]] = {"芝": [], "ダート": []}
    for cell in result.edge_cells:
        if cell.segment_value in edge_by_surface:
            edge_by_surface[cell.segment_value].append(cell)

    turf_edge_bins = {c.factor_value for c in edge_by_surface["芝"]}
    dirt_edge_bins = {c.factor_value for c in edge_by_surface["ダート"]}
    common_bins = turf_edge_bins & dirt_edge_bins
    turf_only = turf_edge_bins - dirt_edge_bins
    dirt_only = dirt_edge_bins - turf_edge_bins

    phase1_in_phase2 = phase1_edge_bins & (turf_edge_bins | dirt_edge_bins)
    phase1_lost = phase1_edge_bins - (turf_edge_bins | dirt_edge_bins)
    new_in_phase2 = (turf_edge_bins | dirt_edge_bins) - phase1_edge_bins

    lines.append(f"- Phase 1エッジビン: {len(phase1_edge_bins)}個")
    lines.append(f"- Phase 2で維持: {len(phase1_in_phase2)}個 {sorted(phase1_in_phase2, key=str)}")
    lines.append(f"- Phase 2で消失: {len(phase1_lost)}個 {sorted(phase1_lost, key=str)}")
    lines.append(f"- Phase 2で新規発見: {len(new_in_phase2)}個 {sorted(new_in_phase2, key=str)}")
    lines.append("")

    # ---- 8. セグメントサマリー ----
    lines.append("## 6. セグメントサマリー")
    lines.append("")
    lines.append(f"| 区分 | エッジビン数 | ビン一覧 |")
    lines.append(f"|------|-----------|---------|")
    lines.append(f"| 芝/ダート共通 | {len(common_bins)} | {', '.join(sorted(common_bins, key=str))} |")
    lines.append(f"| 芝のみ | {len(turf_only)} | {', '.join(sorted(turf_only, key=str))} |")
    lines.append(f"| ダートのみ | {len(dirt_only)} | {', '.join(sorted(dirt_only, key=str))} |")
    lines.append("")

    # 総合判定
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

    lines.append("")
    lines.append("---")
    lines.append(f"*生成日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")

    return "\n".join(lines)


# ====================================================================
# COURSE_27 レポート生成
# ====================================================================

def generate_course27_report(
    df: pd.DataFrame,
    factor_id: int,
    global_rate: float,
    phase1_edge_count: int,
) -> str:
    """
    ファクター × コースカテゴリ(27) の交互作用レポートを生成する。
    単勝・複勝の両方のROIを含む。100未満セルはベイズ収縮で処理。

    Args:
        df: 前処理済みベースデータ
        factor_id: ファクターID
        global_rate: グローバル補正回収率（単勝）
        phase1_edge_count: Phase 1のエッジビン数

    Returns:
        Markdownレポート文字列
    """
    factor = get_factor_by_id(factor_id)
    lines: List[str] = []

    lines.append(f"# Phase 2 タスク2: {factor.name} × コース分類(27) 交互作用分析")
    lines.append("")
    lines.append(f"生成日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")

    # ---- 1. 基本情報 ----
    lines.append("## 1. 基本情報")
    lines.append("")
    lines.append(f"| 項目 | 値 |")
    lines.append(f"|------|-----|")
    lines.append(f"| ファクター | {factor.name} (#{factor.id}) |")
    lines.append(f"| カラム | `{factor.column}` |")
    lines.append(f"| テーブル | `{factor.table}` |")
    lines.append(f"| データ型 | {factor.factor_type.value} |")
    lines.append(f"| セグメント | COURSE_27 (27カテゴリコース分類) |")
    lines.append(f"| Phase 1 エッジビン数 | {phase1_edge_count} |")
    lines.append(f"| グローバル補正回収率（単勝） | {global_rate:.2f}% |")
    lines.append(f"| ベイズ収縮 | サンプル<100のセルはグローバル平均に収縮 |")
    lines.append("")

    # ---- 2. データ準備 ----
    print(f"    {factor.name}: COURSE_27 データ準備中...")
    df_work = df.copy()

    # コースカテゴリ付与
    df_work["course_category"] = assign_course_category_fast(df_work)
    known_mask = df_work["course_category"] != "unknown"
    n_unknown = (~known_mask).sum()
    df_work = df_work[known_mask].copy()

    # ビン分割（カテゴリ型はそのまま）
    binned_series, bin_col = apply_binning(df_work, factor)
    df_work[bin_col] = binned_series
    df_work = df_work.dropna(subset=[bin_col]).copy()

    lines.append("## 2. データ概要")
    lines.append("")
    lines.append(f"- 有効レコード数: {len(df_work):,}")
    lines.append(f"- unknown（分類不能）: {n_unknown:,}")
    lines.append(f"- ファクター値のユニーク数: {df_work[bin_col].nunique()}")
    lines.append("")

    # カテゴリ別サンプル数
    lines.append("### コースカテゴリ別サンプル数")
    lines.append("")
    lines.append("| コースカテゴリ | N |")
    lines.append("|-------------|---|")
    cat_counts = df_work["course_category"].value_counts().sort_index()
    for cat, cnt in cat_counts.items():
        lines.append(f"| {cat} | {cnt:,} |")
    lines.append("")

    # ---- 3. グローバルROI（単勝・複勝） ----
    dual_roi_global = _compute_dual_roi(df_work)
    lines.append("### グローバルROI")
    lines.append("")
    lines.append("| 券種 | N | 的中率(%) | 補正回収率(%) |")
    lines.append("|------|---|---------|------------|")
    lines.append(
        f"| 単勝 | {dual_roi_global['tansho']['n_samples']:,} "
        f"| {dual_roi_global['tansho']['hit_rate']:.2f} "
        f"| {dual_roi_global['tansho']['corrected_return_rate']:.2f} |"
    )
    if dual_roi_global['fukusho']['n_samples'] > 0:
        lines.append(
            f"| 複勝 | {dual_roi_global['fukusho']['n_samples']:,} "
            f"| {dual_roi_global['fukusho']['hit_rate']:.2f} "
            f"| {dual_roi_global['fukusho']['corrected_return_rate']:.2f} |"
        )
    else:
        lines.append("| 複勝 | - | - | （データなし） |")
    lines.append("")

    # ---- 4. 交互作用分析（単勝）----
    # COURSE_27はセルが多いので min_samples=30 を使い、<100はベイズ収縮
    print(f"    {factor.name}: COURSE_27 交互作用分析（単勝）実行中...")
    result = run_interaction_analysis(
        df=df_work,
        factor_col=bin_col,
        segment_col="course_category",
        global_rate=global_rate,
        factor_name=factor.name,
        segment_name="コースカテゴリ(27)",
        min_samples=30,
    )

    # Phase 1 エッジビン取得
    phase1_report_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "phase1",
        f"factor_{factor.id:02d}_{factor.column}.md"
    )
    phase1_edge_bins = _extract_phase1_edge_bins(phase1_report_path)

    # ---- 5. セグメント×ビン テーブル（単勝・エッジのみ）----
    lines.append("## 3. エッジセル一覧（単勝）")
    lines.append("")
    lines.append(f"- 分析セル数: {len(result.cells):,}")
    lines.append(f"- エッジセル数: {result.n_edge_cells}")
    lines.append("")

    if result.edge_cells:
        lines.append("| ビン/カテゴリ | コースカテゴリ | N | 単勝的中率(%) | 単勝補正回収率(%) | ベイズ推定(%) | 95%CI下限 | 95%CI上限 | 得点 |")
        lines.append("|-------------|-------------|---|------------|----------------|------------|---------|---------|------|")
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

    # ---- 6. 全セル上位50件 ----
    lines.append("### 全セル詳細（得点上位50件）")
    lines.append("")
    lines.append("| ビン/カテゴリ | コースカテゴリ | N | 的中率(%) | 補正回収率(%) | ベイズ推定(%) | 95%CI下限 | 得点 | エッジ |")
    lines.append("|-------------|-------------|---|---------|------------|------------|---------|------|--------|")
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

    # ---- 7. Phase 1 エッジ比較 ----
    lines.append("## 4. Phase 1 エッジとの比較")
    lines.append("")

    phase2_edge_bins_all = {c.factor_value for c in result.edge_cells}
    phase1_in_phase2 = phase1_edge_bins & phase2_edge_bins_all
    phase1_lost = phase1_edge_bins - phase2_edge_bins_all
    new_in_phase2 = phase2_edge_bins_all - phase1_edge_bins

    lines.append(f"- Phase 1エッジビン: {len(phase1_edge_bins)}個")
    lines.append(f"- Phase 2で維持: {len(phase1_in_phase2)}個 {sorted(phase1_in_phase2, key=str)}")
    lines.append(f"- Phase 2で消失: {len(phase1_lost)}個 {sorted(phase1_lost, key=str)}")
    lines.append(f"- Phase 2で新規発見: {len(new_in_phase2)}個 {sorted(new_in_phase2, key=str)}")
    lines.append("")

    # ---- 8. セグメントサマリー ----
    lines.append("## 5. セグメントサマリー")
    lines.append("")

    # エッジが出現したカテゴリ数を集計
    edge_categories = {c.segment_value for c in result.edge_cells}
    total_categories = len(cat_counts)

    lines.append(f"- エッジが出現したカテゴリ数: {len(edge_categories)} / {total_categories}")
    lines.append(f"- カバー率: {len(edge_categories) / total_categories * 100:.1f}%")
    lines.append("")

    if edge_categories:
        lines.append("### エッジ出現カテゴリ")
        lines.append("")
        for cat in sorted(edge_categories):
            edge_cells_in_cat = [c for c in result.edge_cells if c.segment_value == cat]
            bins_in_cat = [c.factor_value for c in edge_cells_in_cat]
            lines.append(f"- **{cat}**: {', '.join(sorted(bins_in_cat, key=str))}")
        lines.append("")

    # 総合判定
    lines.append("### 総合判定")
    lines.append("")
    if len(edge_categories) >= total_categories * 0.5:
        lines.append(f"{factor.name}のエッジは**多くのコースで共通して出現する現象**である。")
    elif len(edge_categories) >= total_categories * 0.2:
        lines.append(f"{factor.name}のエッジは**中程度のコースカバー率を持つ偏在型**の現象である。")
    elif len(edge_categories) > 0:
        lines.append(f"{factor.name}のエッジは**特定のコース形状でのみ出現する限定的な現象**である。")
    else:
        lines.append(f"{factor.name}はPhase 2レベルでエッジが検出されなかった。")

    lines.append("")
    lines.append("---")
    lines.append(f"*生成日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")

    return "\n".join(lines)


# ====================================================================
# GLOBAL レポート生成
# ====================================================================

def generate_global_report(
    df: pd.DataFrame,
    factor_id: int,
    global_rate: float,
    phase1_edge_count: int,
) -> str:
    """
    ファクター × GLOBAL (セグメントなし) レポートを生成する。
    単勝・複勝の両方のROIを計算する。

    Args:
        df: 前処理済みベースデータ
        factor_id: ファクターID
        global_rate: グローバル補正回収率（単勝）
        phase1_edge_count: Phase 1のエッジビン数

    Returns:
        Markdownレポート文字列
    """
    factor = get_factor_by_id(factor_id)
    lines: List[str] = []

    lines.append(f"# Phase 2 タスク2: {factor.name} GLOBAL分析（単勝・複勝）")
    lines.append("")
    lines.append(f"生成日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")

    # ---- 1. 基本情報 ----
    lines.append("## 1. 基本情報")
    lines.append("")
    lines.append(f"| 項目 | 値 |")
    lines.append(f"|------|-----|")
    lines.append(f"| ファクター | {factor.name} (#{factor.id}) |")
    lines.append(f"| カラム | `{factor.column}` |")
    lines.append(f"| テーブル | `{factor.table}` |")
    lines.append(f"| データ型 | {factor.factor_type.value} |")
    lines.append(f"| セグメント | GLOBAL (セグメントなし) |")
    lines.append(f"| Phase 1 エッジビン数 | {phase1_edge_count} |")
    lines.append(f"| グローバル補正回収率（単勝） | {global_rate:.2f}% |")
    lines.append("")

    # ---- 2. データ準備 ----
    print(f"    {factor.name}: GLOBAL データ準備中...")
    df_work = df.copy()

    # 馬場状態コードの場合: 芝/ダート分岐
    # babajotai_code_shiba / babajotai_code_dirt を統合して1カラムにする
    if factor.column == "babajotai_code_shiba":
        surface = assign_surface(df_work)
        df_work["baba_code_unified"] = None
        turf_mask = surface == "芝"
        dirt_mask = surface == "ダート"
        if "babajotai_code_shiba" in df_work.columns:
            df_work.loc[turf_mask, "baba_code_unified"] = (
                df_work.loc[turf_mask, "babajotai_code_shiba"].astype(str).str.strip()
            )
        if "babajotai_code_dirt" in df_work.columns:
            df_work.loc[dirt_mask, "baba_code_unified"] = (
                df_work.loc[dirt_mask, "babajotai_code_dirt"].astype(str).str.strip()
            )
        # 有効データのみ
        df_work = df_work.dropna(subset=["baba_code_unified"]).copy()
        df_work = df_work[df_work["baba_code_unified"].str.strip() != ""].copy()
        bin_col = "baba_code_unified"
    else:
        binned_series, bin_col = apply_binning(df_work, factor)
        df_work[bin_col] = binned_series
        df_work = df_work.dropna(subset=[bin_col]).copy()

    lines.append("## 2. データ概要")
    lines.append("")
    lines.append(f"- 有効レコード数: {len(df_work):,}")
    lines.append(f"- ビン/カテゴリ数: {df_work[bin_col].nunique()}")
    lines.append("")

    # ---- 3. 単勝ビン別ROI ----
    lines.append("## 3. ビン別ROI（単勝）")
    lines.append("")

    tansho_by_bin = calc_return_rate_by_bins(
        df_work, bin_col=bin_col, is_fukusho=False,
    )

    # Phase 1 エッジビン取得
    phase1_report_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "phase1",
        f"factor_{factor.id:02d}_{factor.column}.md"
    )
    phase1_edge_bins = _extract_phase1_edge_bins(phase1_report_path)

    # ベイズ推定適用
    tansho_bayes: List[Tuple[str, dict, BayesEstimate]] = []
    if not tansho_by_bin.empty:
        lines.append("| ビン/カテゴリ | N | 単勝的中率(%) | 単勝補正回収率(%) | ベイズ推定(%) | 95%CI下限 | 95%CI上限 | 得点 | エッジ | Ph1 |")
        lines.append("|-------------|---|------------|----------------|------------|---------|---------|------|--------|-----|")

        for _, row in tansho_by_bin.iterrows():
            bin_val = str(row["bin_value"])
            bayes = hierarchical_bayes_estimate(
                observed_rate=row["corrected_return_rate"],
                n_samples=row["n_samples"],
                prior_rate=global_rate,
            )
            is_edge = bayes.ci_lower > BASELINE_RATE
            edge_flag = "**YES**" if is_edge else "No"
            ph1_mark = "★" if bin_val in phase1_edge_bins else ""
            tansho_bayes.append((bin_val, dict(row), bayes))

            lines.append(
                f"| {bin_val} "
                f"| {int(row['n_samples']):,} "
                f"| {row['hit_rate']:.2f} "
                f"| {row['corrected_return_rate']:.2f} "
                f"| {bayes.estimated_rate:.2f} "
                f"| {bayes.ci_lower:.2f} "
                f"| {bayes.ci_upper:.2f} "
                f"| {bayes.score:.2f} "
                f"| {edge_flag} "
                f"| {ph1_mark} |"
            )
    lines.append("")

    # ---- 4. 複勝ビン別ROI ----
    lines.append("## 4. ビン別ROI（複勝）")
    lines.append("")
    df_fukusho = _prepare_fukusho_df(df_work)
    if df_fukusho is not None:
        fukusho_global = calc_corrected_return_rate(
            df_fukusho, odds_col="fukusho_odds",
            hit_flag_col="fukusho_is_hit", is_fukusho=True,
        )
        fukusho_by_bin = calc_return_rate_by_bins(
            df_fukusho, bin_col=bin_col,
            odds_col="fukusho_odds",
            hit_flag_col="fukusho_is_hit",
            is_fukusho=True,
        )
        lines.append(f"- グローバル複勝回収率: {fukusho_global['corrected_return_rate']:.2f}%")
        lines.append(f"- 複勝データソース: jvd_hr（払戻テーブル）からUNPIVOT")
        lines.append("")

        if not fukusho_by_bin.empty:
            lines.append("| ビン/カテゴリ | N | 複勝的中率(%) | 複勝補正回収率(%) | ベイズ推定(%) | 95%CI下限 | 95%CI上限 | 得点 | エッジ |")
            lines.append("|-------------|---|------------|----------------|------------|---------|---------|------|--------|")

            for _, row in fukusho_by_bin.iterrows():
                bin_val = str(row["bin_value"])
                bayes = hierarchical_bayes_estimate(
                    observed_rate=row["corrected_return_rate"],
                    n_samples=row["n_samples"],
                    prior_rate=fukusho_global["corrected_return_rate"],
                )
                is_edge = bayes.ci_lower > BASELINE_RATE
                edge_flag = "**YES**" if is_edge else "No"

                lines.append(
                    f"| {bin_val} "
                    f"| {int(row['n_samples']):,} "
                    f"| {row['hit_rate']:.2f} "
                    f"| {row['corrected_return_rate']:.2f} "
                    f"| {bayes.estimated_rate:.2f} "
                    f"| {bayes.ci_lower:.2f} "
                    f"| {bayes.ci_upper:.2f} "
                    f"| {bayes.score:.2f} "
                    f"| {edge_flag} |"
                )
        lines.append("")
    else:
        lines.append("複勝オッズデータなし。単勝のみで分析。")
        lines.append("")

    # ---- 5. Phase 1 エッジ比較 ----
    lines.append("## 5. Phase 1 エッジとの比較")
    lines.append("")

    phase2_edge_bins = set()
    for bin_val, row_dict, bayes in tansho_bayes:
        if bayes.ci_lower > BASELINE_RATE:
            phase2_edge_bins.add(bin_val)

    phase1_in_phase2 = phase1_edge_bins & phase2_edge_bins
    phase1_lost = phase1_edge_bins - phase2_edge_bins
    new_in_phase2 = phase2_edge_bins - phase1_edge_bins

    lines.append(f"- Phase 1エッジビン: {len(phase1_edge_bins)}個")
    lines.append(f"- Phase 2で維持: {len(phase1_in_phase2)}個 {sorted(phase1_in_phase2, key=str)}")
    lines.append(f"- Phase 2で消失: {len(phase1_lost)}個 {sorted(phase1_lost, key=str)}")
    lines.append(f"- Phase 2で新規発見: {len(new_in_phase2)}個 {sorted(new_in_phase2, key=str)}")
    lines.append("")

    # ---- 6. セグメントサマリー ----
    lines.append("## 6. セグメントサマリー")
    lines.append("")
    lines.append("GLOBALセグメント（分割なし）のため、Phase 1と同一条件での再計算。")
    lines.append("単勝・複勝の両方のROIを提示し、Phase 1エッジの頑健性を確認する。")
    lines.append("")

    if len(phase2_edge_bins) > 0:
        lines.append(f"Phase 2エッジビン数（単勝）: {len(phase2_edge_bins)}個")
    else:
        lines.append("Phase 2レベルでエッジが検出されなかった。")

    lines.append("")
    lines.append("---")
    lines.append(f"*生成日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")

    return "\n".join(lines)


# ====================================================================
# メイン実行
# ====================================================================

def _get_factor_key(factor_id: int) -> str:
    """ファクターIDからレポートファイル名のキーを取得する。"""
    factor = get_factor_by_id(factor_id)
    return factor.column


def _get_segment_suffix(segment_type: str) -> str:
    """セグメントタイプからファイル名のサフィックスを取得する。"""
    if segment_type == "SURFACE_2":
        return "surface2"
    elif segment_type == "COURSE_27":
        return "course27"
    elif segment_type == "GLOBAL":
        return "global"
    return segment_type.lower()


def main(target_factor: Optional[str] = None) -> None:
    """
    メイン実行関数。

    Args:
        target_factor: 特定ファクターカラム名。Noneなら全10ファクター。
    """
    ensure_report_dir()

    # コマンドライン引数
    if target_factor is None and "--factor" in sys.argv:
        idx = sys.argv.index("--factor")
        if idx + 1 < len(sys.argv):
            target_factor = sys.argv[idx + 1]

    print("=" * 60)
    print("Phase 2 タスク2: 10ファクター交互作用分析レポート生成")
    print(f"対象: {'全10ファクター' if target_factor is None else target_factor}")
    print("=" * 60)
    print()

    # 1. データ取得
    print("[1/4] PostgreSQLからベースデータを取得中...")
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
    print("[2/4] データ前処理中...")
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

    # 複勝オッズ変換
    if "fukusho_odds" in df.columns:
        fk_median = df["fukusho_odds"].dropna().median()
        if fk_median >= 10.0:
            print(f"  fukusho_odds JRA-VAN 10倍単位変換 (median={fk_median:.1f})")
            df["fukusho_odds"] = df["fukusho_odds"] / 10.0

    print(f"  前処理完了: {len(df):,} rows")
    print()

    # 3. グローバル回収率
    print("[3/4] グローバル補正回収率算出...")
    global_result = calc_corrected_return_rate(df)
    global_rate = global_result["corrected_return_rate"]
    print(f"  グローバル補正回収率（単勝）: {global_rate:.2f}%")

    # 品質ゲート: グローバルROI 75-85%
    if not (75.0 <= global_rate <= 85.0):
        print(f"  ⚠️ 警告: グローバルROI {global_rate:.2f}% が品質ゲート範囲外 (75-85%)")
    else:
        print(f"  ✅ 品質ゲート通過: グローバルROI {global_rate:.2f}% ∈ [75, 85]")
    print()

    # 4. レポート生成
    print("[4/4] レポート生成中...")
    generated_count = 0

    for factor_id, segment_type, phase1_edges in TASK2_FACTORS:
        factor = get_factor_by_id(factor_id)
        factor_key = _get_factor_key(factor_id)
        segment_suffix = _get_segment_suffix(segment_type)

        # 対象フィルタ
        if target_factor is not None and factor_key != target_factor:
            continue

        filename = f"{factor_key}_{segment_suffix}.md"
        print(f"  [{factor.name}] {segment_type}...")

        try:
            if segment_type == "SURFACE_2":
                report_text = generate_surface2_report(
                    df, factor_id, global_rate, phase1_edges,
                )
            elif segment_type == "COURSE_27":
                report_text = generate_course27_report(
                    df, factor_id, global_rate, phase1_edges,
                )
            elif segment_type == "GLOBAL":
                report_text = generate_global_report(
                    df, factor_id, global_rate, phase1_edges,
                )
            else:
                print(f"    ⚠️ 不明なセグメントタイプ: {segment_type}")
                continue

            filepath = os.path.join(REPORT_DIR, filename)
            with open(filepath, "w", encoding="utf-8") as f:
                f.write(report_text)
            print(f"    ✅ 保存: {filepath}")
            generated_count += 1

        except Exception as e:
            print(f"    ❌ エラー: {e}")
            import traceback
            traceback.print_exc()

    print()
    print("=" * 60)
    print(f"Phase 2 タスク2 完了! ({generated_count}レポート生成)")
    print(f"  レポートディレクトリ: {REPORT_DIR}")
    print("=" * 60)


if __name__ == "__main__":
    main()
