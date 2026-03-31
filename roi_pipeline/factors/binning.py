"""
ビン分割ロジック

数値系ファクター → 等頻度分割（quantile-based binning）
カテゴリ系ファクター → 各カテゴリ値をそのまま使用
順序付きカテゴリ → 各値をそのまま使用（馬番1〜18等）
"""
import pandas as pd
import numpy as np
from typing import Tuple

from roi_pipeline.factors.definitions import FactorDefinition, FactorType


def apply_binning(
    df: pd.DataFrame,
    factor: FactorDefinition,
) -> Tuple[pd.Series, str]:
    """
    ファクターの型に応じてビン分割を適用する。

    Args:
        df: データ
        factor: ファクター定義

    Returns:
        Tuple[pd.Series, str]:
            - ビン化後のSeries（カテゴリラベル or 元の値）
            - ビンカラム名

    Note:
        数値系: 等頻度分割でn_binsのビンに分割。各ビンのラベルは範囲を表す文字列。
        カテゴリ系/順序付き: そのまま返す（各値ごとに集計）。
    """
    col = factor.column
    bin_col_name = f"{col}_bin"

    if col not in df.columns:
        raise KeyError(f"Column '{col}' not found in DataFrame")

    series = df[col].copy()

    if factor.factor_type == FactorType.NUMERIC:
        # 数値変換（文字列→float）
        numeric_series = pd.to_numeric(series, errors="coerce")
        valid_mask = numeric_series.notna()

        if valid_mask.sum() == 0:
            return pd.Series(pd.NA, index=df.index, name=bin_col_name), bin_col_name

        # 等頻度分割
        try:
            binned = pd.qcut(
                numeric_series[valid_mask],
                q=factor.n_bins,
                labels=False,
                duplicates="drop",
            )
            # ビンのラベルを範囲文字列に変換
            bin_edges = pd.qcut(
                numeric_series[valid_mask],
                q=factor.n_bins,
                retbins=True,
                duplicates="drop",
            )[1]

            labels = []
            for i in range(len(bin_edges) - 1):
                labels.append(f"{bin_edges[i]:.1f}-{bin_edges[i+1]:.1f}")

            binned_labels = pd.qcut(
                numeric_series[valid_mask],
                q=factor.n_bins,
                labels=labels,
                duplicates="drop",
            )

            result = pd.Series(pd.NA, index=df.index, dtype="object", name=bin_col_name)
            result[valid_mask] = binned_labels.astype(str)

        except ValueError:
            # ユニーク値が少なすぎてqcutできない場合はそのまま返す
            result = numeric_series.astype(str)
            result[~valid_mask] = pd.NA
            result.name = bin_col_name

        return result, bin_col_name

    elif factor.factor_type in (FactorType.CATEGORY, FactorType.ORDINAL):
        # カテゴリ・順序付きはそのまま
        result = series.copy()
        result.name = bin_col_name
        # 空文字列をNAに変換
        result = result.replace(r"^\s*$", pd.NA, regex=True)
        return result, bin_col_name

    else:
        raise ValueError(f"Unknown FactorType: {factor.factor_type}")
