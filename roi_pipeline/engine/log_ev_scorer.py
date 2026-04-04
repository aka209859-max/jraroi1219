"""
Log-EV得点化エンジン + LCBゲート

Phase 3 Task 1: Phase 2で確認された各ファクター×セグメントの補正回収率を
対数期待値空間に変換し、統計的に信頼できるエッジのみを抽出する。

設計思想:
    - 補正回収率 80% がゼロライン（JRA実効控除率に基づく）
    - LCBゲート: 事後分布の下方10%分位点が 80% を超えるセルのみ投資適格
    - 均等重み合成: 初期実装。Task 4完了後にLightGBMによる重み最適化を検討。
"""
from typing import Dict, List, Optional, Tuple

import numpy as np


def log_ev_score(corrected_return_rate: float, baseline: float = 0.80) -> float:
    """
    補正回収率を対数期待値スコアに変換する。

    baseline=0.80 がゼロライン（JRA実効控除率 75-77% からの補正後基準）。

    Args:
        corrected_return_rate: 補正回収率（例: 0.85 = 85%）
        baseline: ゼロラインの補正回収率（デフォルト 0.80）

    Returns:
        対数期待値スコア。正 = エッジあり、0 = エッジなし、負 = 不利

    Examples:
        >>> log_ev_score(0.80)  # 80% → ln(80/80) = 0.0
        0.0
        >>> log_ev_score(1.00)  # 100% → ln(100/80) ≈ +0.223
        0.22314355131420976
        >>> log_ev_score(0.60)  # 60% → ln(60/80) ≈ -0.288
        -0.28768207245178085
        >>> log_ev_score(0.0)   # 下限クリップ
        -10.0
    """
    if corrected_return_rate <= 0:
        return -10.0  # 下限クリップ
    return np.log(corrected_return_rate / baseline)


def lcb_gate(
    posterior_samples: np.ndarray,
    baseline: float = 0.80,
    quantile: float = 0.10,
) -> bool:
    """
    階層ベイズの事後分布サンプルから、下方10%分位点が
    baseline(80%)を超えるかどうかを判定する。

    少標本セグメントは事後分布が広くなり（シュリンケージにより）、
    LCBが baseline を下回るため自動的に除外される。

    Args:
        posterior_samples: 事後分布からのサンプル（補正回収率のスケール）
        baseline: 投資適格判定の下限（デフォルト 0.80）
        quantile: 下方信頼限界の分位点（デフォルト 0.10 = 下方10%）

    Returns:
        True = 投資適格（LCB > baseline）
        False = 投資不適格（LCB <= baseline）
    """
    lcb = np.quantile(posterior_samples, quantile)
    return bool(lcb > baseline)


def compute_horse_score(
    horse_factors: Dict[str, Tuple],
    edge_table: Dict,
    bet_type: str = "tansho",
) -> float:
    """
    馬iの全該当ファクター×セグメントのLog-EVスコアを均等重みで合成する。

    LCBゲートを通過したセルのみをスコア合成に使用する。
    ゲートを通過したセルが0個の場合は 0.0 を返す。

    Args:
        horse_factors: {factor_name: (segment, bin_value)} の辞書
            例: {"idm": ("turf", 5), "kishu_shisu": ("dirt", 3)}
        edge_table: {(factor, segment, bin, bet_type): {
            "posterior_mean": float,          # ベイズ事後平均（補正回収率）
            "posterior_samples": np.ndarray,  # 事後分布サンプル
            "N": int                          # セルのサンプル数
        }}
        bet_type: "tansho"（単勝）または "fukusho"（複勝）

    Returns:
        LCBゲート通過セルのLog-EVスコアの均等重み平均。
        該当セルなし、またはゲート通過セルなし → 0.0
    """
    scores: List[float] = []

    for factor_name, (segment, bin_value) in horse_factors.items():
        key = (factor_name, segment, bin_value, bet_type)
        if key not in edge_table:
            continue

        cell = edge_table[key]

        # LCBゲート: 下方10%分位点が baseline を超えるか
        if not lcb_gate(cell["posterior_samples"]):
            continue

        score = log_ev_score(cell["posterior_mean"])
        scores.append(score)

    if len(scores) == 0:
        return 0.0

    return sum(scores) / len(scores)  # 均等重み


def filter_edge_table(
    edge_table: Dict,
    baseline: float = 0.80,
    quantile: float = 0.10,
) -> Dict:
    """
    edge_tableからLCBゲートを通過したセルのみを返す。

    Phase 3 Walk-Forward検証で、学習窓の edge_table を事前フィルタリングして
    予測時の計算コストを削減するために使用する。

    Args:
        edge_table: compute_horse_score と同じ形式
        baseline: LCBゲートの baseline
        quantile: LCBゲートの分位点

    Returns:
        LCBゲート通過セルのみを含む edge_table のサブセット
    """
    filtered = {}
    for key, cell in edge_table.items():
        if lcb_gate(cell["posterior_samples"], baseline=baseline, quantile=quantile):
            filtered[key] = cell
    return filtered


def summarize_edge_table(edge_table: Dict) -> Dict:
    """
    edge_tableの統計サマリーを返す（LCBゲート前後の比較用）。

    Args:
        edge_table: compute_horse_score と同じ形式

    Returns:
        {
            "total_cells": int,
            "lcb_pass_cells": int,
            "lcb_pass_ratio": float,
            "mean_log_ev_all": float,
            "mean_log_ev_pass": float,
        }
    """
    total = len(edge_table)
    if total == 0:
        return {
            "total_cells": 0,
            "lcb_pass_cells": 0,
            "lcb_pass_ratio": 0.0,
            "mean_log_ev_all": 0.0,
            "mean_log_ev_pass": 0.0,
        }

    all_scores = [log_ev_score(cell["posterior_mean"]) for cell in edge_table.values()]
    pass_keys = [
        k for k, cell in edge_table.items()
        if lcb_gate(cell["posterior_samples"])
    ]
    pass_scores = [log_ev_score(edge_table[k]["posterior_mean"]) for k in pass_keys]

    return {
        "total_cells": total,
        "lcb_pass_cells": len(pass_keys),
        "lcb_pass_ratio": len(pass_keys) / total if total > 0 else 0.0,
        "mean_log_ev_all": float(np.mean(all_scores)) if all_scores else 0.0,
        "mean_log_ev_pass": float(np.mean(pass_scores)) if pass_scores else 0.0,
    }
