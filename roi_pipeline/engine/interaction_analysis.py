"""
Phase 2 交互作用分析エンジン

Phase 1で検出されたエッジファクターを、セグメント軸で分解し、
「全セグメント共通の現象」か「特定セグメントのみの現象」かを判別する。

分析パターン:
    1. 馬番(1-18) × コースカテゴリ(27) のクロス集計
    2. 調教師指数(ビン) × 芝/ダート(2) のクロス集計
    3. 厩舎指数(ビン) × 芝/ダート(2) のクロス集計

階層ベイズ推定の3層構造:
    レベル1（グローバル）: ファクター値xの全セグメント補正回収率
    レベル2（カテゴリ）: ファクター値xのセグメントy内の補正回収率
    レベル3（個別）: 実データの補正回収率
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

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
from roi_pipeline.config.course_categories import get_category, ALL_CATEGORIES


@dataclass
class InteractionCell:
    """交互作用分析の1セル（ファクター値 × セグメント）の結果"""
    factor_value: str
    segment_value: str
    n_samples: int
    n_hits: int
    hit_rate: float
    observed_rate: float  # 補正回収率（実測）
    bayes_estimate: BayesEstimate  # 3層ベイズ推定結果
    is_edge: bool  # エッジ判定（CI下限 > BASELINE_RATE）


@dataclass
class InteractionResult:
    """交互作用分析の全体結果"""
    factor_name: str
    segment_name: str
    global_rate: float  # グローバル補正回収率
    factor_rates: Dict[str, float]  # ファクター値別のグローバル回収率
    segment_rates: Dict[str, float]  # セグメント別のグローバル回収率
    cells: List[InteractionCell]  # 全セルの結果
    factor_n: Dict[str, int]  # ファクター値別のサンプル数
    segment_n: Dict[str, int]  # セグメント別のサンプル数

    @property
    def edge_cells(self) -> List[InteractionCell]:
        """エッジ判定=Trueのセルのみ"""
        return [c for c in self.cells if c.is_edge]

    @property
    def n_edge_cells(self) -> int:
        """エッジセル数"""
        return len(self.edge_cells)

    @property
    def edge_factor_values(self) -> set:
        """エッジが検出されたファクター値の集合"""
        return {c.factor_value for c in self.edge_cells}

    @property
    def edge_segment_values(self) -> set:
        """エッジが検出されたセグメント値の集合"""
        return {c.segment_value for c in self.edge_cells}


def assign_course_category(
    df: pd.DataFrame,
    keibajo_code_col: str = "keibajo_code",
    track_code_col: str = "track_code",
    distance_col: str = "ra_kyori",
    bac_distance_col: str = "bac_kyori",
) -> pd.Series:
    """
    各レコードにコースカテゴリを付与する。

    Args:
        df: ベースデータ
        keibajo_code_col: 競馬場コードカラム
        track_code_col: トラックコードカラム（芝/ダート判定）
        distance_col: 距離カラム（ra_kyoriから取得）
        bac_distance_col: BAC距離カラム（フォールバック）

    Returns:
        コースカテゴリのSeries
    """
    categories = []
    for _, row in df.iterrows():
        keibajo = str(row.get(keibajo_code_col, "")).strip().zfill(2)

        # 芝/ダート判定: track_codeの先頭で判別
        track_code = str(row.get(track_code_col, "")).strip()
        if track_code.startswith("1"):
            surface = "芝"
        elif track_code.startswith("2"):
            surface = "ダ"
        else:
            # フォールバック: track_codeが空の場合
            surface = "芝"

        # 距離取得
        dist_raw = row.get(distance_col)
        if pd.isna(dist_raw):
            dist_raw = row.get(bac_distance_col)
        try:
            distance = int(float(str(dist_raw).strip()))
        except (ValueError, TypeError):
            distance = 0

        cat = get_category(keibajo, surface, distance)
        categories.append(cat)

    return pd.Series(categories, index=df.index, name="course_category")


def assign_course_category_fast(
    df: pd.DataFrame,
    keibajo_code_col: str = "keibajo_code",
    track_code_col: str = "track_code",
    distance_col: str = "ra_kyori",
    bac_distance_col: str = "bac_kyori",
) -> pd.Series:
    """
    各レコードにコースカテゴリを付与する（ベクトル化版）。

    行単位ループを避け、ルックアップテーブルによる高速マッピングを行う。

    Args:
        df: ベースデータ
        keibajo_code_col: 競馬場コードカラム
        track_code_col: トラックコードカラム（芝/ダート判定）
        distance_col: 距離カラム（ra_kyoriから取得）
        bac_distance_col: BAC距離カラム（フォールバック）

    Returns:
        コースカテゴリのSeries
    """
    # ルックアップテーブルを事前構築:
    # (keibajo_code, surface, distance_int) → category_name
    lookup: Dict[Tuple[str, str, int], str] = {}
    for cat_name, entries in ALL_CATEGORIES.items():
        for code, surf, dist_str in entries:
            # 数値部分のみ抽出
            dist_num = int("".join(c for c in dist_str if c.isdigit()))
            lookup[(code, surf, dist_num)] = cat_name

    # 競馬場コード: 文字列の右側2桁をゼロ埋め
    keibajo = df[keibajo_code_col].astype(str).str.strip().str.zfill(2)

    # 芝/ダート: track_codeの先頭文字
    track = df[track_code_col].astype(str).str.strip()
    surface = track.str[0].map({"1": "芝", "2": "ダ"}).fillna("芝")

    # 距離: ra_kyori 優先、bac_kyori フォールバック
    dist = pd.to_numeric(df[distance_col], errors="coerce")
    if bac_distance_col in df.columns:
        dist_fallback = pd.to_numeric(df[bac_distance_col], errors="coerce")
        dist = dist.fillna(dist_fallback)
    dist = dist.fillna(0).astype(int)

    # ルックアップ適用
    result = pd.Series("unknown", index=df.index, name="course_category")
    for idx in df.index:
        key = (keibajo.loc[idx], surface.loc[idx], dist.loc[idx])
        result.loc[idx] = lookup.get(key, "unknown")

    return result


def assign_surface(
    df: pd.DataFrame,
    track_code_col: str = "track_code",
) -> pd.Series:
    """
    各レコードに芝/ダート区分を付与する。

    Args:
        df: ベースデータ
        track_code_col: トラックコードカラム

    Returns:
        芝/ダート区分のSeries
    """
    track = df[track_code_col].astype(str).str.strip()
    surface = track.str[0].map({"1": "芝", "2": "ダート"}).fillna("不明")
    surface.name = "surface_type"
    return surface


def run_interaction_analysis(
    df: pd.DataFrame,
    factor_col: str,
    segment_col: str,
    global_rate: float,
    factor_name: str = "",
    segment_name: str = "",
    min_samples: int = 30,
    C: int = 50,
) -> InteractionResult:
    """
    交互作用分析を実行する。

    ファクター値 × セグメント値 のクロス集計テーブルを作成し、
    各セルの補正回収率を3層階層ベイズで推定する。

    Args:
        df: 分析対象データ（factor_col, segment_col, tansho_odds, is_hit, race_year 必須）
        factor_col: ファクターカラム名
        segment_col: セグメントカラム名
        global_rate: グローバル補正回収率
        factor_name: レポート用ファクター名
        segment_name: レポート用セグメント名
        min_samples: 最小サンプル数（これ未満のセルはスキップ）
        C: 信頼性重みの定数

    Returns:
        InteractionResult: 交互作用分析結果
    """
    # 有効データのみ
    valid_df = df.dropna(subset=[factor_col, segment_col]).copy()

    # ====== レベル1: ファクター値別のグローバル補正回収率 ======
    factor_rates: Dict[str, float] = {}
    factor_n: Dict[str, int] = {}
    for fval, grp in valid_df.groupby(factor_col, sort=True):
        fval_str = str(fval)
        result = calc_corrected_return_rate(grp)
        factor_rates[fval_str] = result["corrected_return_rate"]
        factor_n[fval_str] = result["n_samples"]

    # ====== レベル1.5: セグメント別のグローバル補正回収率 ======
    segment_rates: Dict[str, float] = {}
    segment_n: Dict[str, int] = {}
    for sval, grp in valid_df.groupby(segment_col, sort=True):
        sval_str = str(sval)
        result = calc_corrected_return_rate(grp)
        segment_rates[sval_str] = result["corrected_return_rate"]
        segment_n[sval_str] = result["n_samples"]

    # ====== レベル2+3: クロス集計 + 3層ベイズ推定 ======
    cells: List[InteractionCell] = []

    for (fval, sval), grp in valid_df.groupby([factor_col, segment_col], sort=True):
        fval_str = str(fval)
        sval_str = str(sval)

        n = len(grp)
        if n < min_samples:
            continue

        # 個別セルの補正回収率
        cell_result = calc_corrected_return_rate(grp)

        # 3層階層ベイズ推定
        # レベル1: グローバル回収率
        # レベル2: ファクター値のグローバル回収率（ファクター軸の事前分布）
        # レベル3: 個別セル（ファクター値 × セグメント）
        bayes = three_level_estimate(
            observed_rate=cell_result["corrected_return_rate"],
            n_samples=cell_result["n_samples"],
            category_rate=factor_rates.get(fval_str, global_rate),
            category_n=factor_n.get(fval_str, 0),
            global_rate=global_rate,
            C=C,
        )

        is_edge = bayes.ci_lower > BASELINE_RATE

        cells.append(InteractionCell(
            factor_value=fval_str,
            segment_value=sval_str,
            n_samples=cell_result["n_samples"],
            n_hits=cell_result["n_hits"],
            hit_rate=cell_result["hit_rate"],
            observed_rate=cell_result["corrected_return_rate"],
            bayes_estimate=bayes,
            is_edge=is_edge,
        ))

    return InteractionResult(
        factor_name=factor_name,
        segment_name=segment_name,
        global_rate=global_rate,
        factor_rates=factor_rates,
        segment_rates=segment_rates,
        cells=cells,
        factor_n=factor_n,
        segment_n=segment_n,
    )
