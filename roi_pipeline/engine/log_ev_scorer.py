"""
Phase 3 Task 1: Log-EV得点化エンジン＋LCBゲート

補正回収率を対数期待値空間に変換し、統計的に信頼できるエッジのみを抽出する。

Log-EV変換:
    score = ln(corrected_return_rate / baseline)
    baseline = 0.80 (80%) がゼロライン

    80% → 0.0    (エッジなし)
    85% → +0.061 (弱いエッジ)
    100% → +0.223 (強いエッジ)
    60% → -0.288 (負のエッジ)

LCBゲート:
    階層ベイズ事後分布の下方10%分位点が baseline を超えるセルのみ投資適格。
    少標本の偽エッジを自動排除する。

学術的根拠:
    Kelly (1956) の対数成長理論。補正回収率を対数空間に変換することで、
    Kelly基準と数学的に整合する得点化を実現。
"""
import numpy as np
from typing import Dict, List, Optional, Tuple

from roi_pipeline.engine.hierarchical_bayes import BASELINE_RATE


# Log-EV変換のbaseline（0.80 = 80%）
# BASELINE_RATE は hierarchical_bayes から import (80.0)
# Log-EV では 0.80 スケール（100%ではなく比率）で扱う
LOG_EV_BASELINE: float = BASELINE_RATE / 100.0  # 0.80

# LCBゲートのデフォルト分位点
DEFAULT_LCB_QUANTILE: float = 0.10

# Log-EVスコアの下限クリップ
LOG_EV_FLOOR: float = -10.0


def log_ev_score(corrected_return_rate: float, baseline: float = 0.80) -> float:
    """
    補正回収率を対数期待値スコアに変換する。

    Args:
        corrected_return_rate: 補正回収率（比率スケール）。
            80% → 0.80, 100% → 1.00, 120% → 1.20
            ※ BASELINE_RATE(80.0) スケールの場合は呼び出し側で /100.0 すること
        baseline: ゼロラインとなる基準回収率（デフォルト 0.80）

    Returns:
        対数期待値スコア。
        baseline と一致 → 0.0
        baseline 超過 → 正（エッジあり）
        baseline 未満 → 負

    Examples:
        >>> log_ev_score(0.80)  # 80%
        0.0
        >>> log_ev_score(1.00)  # 100%
        0.2231...
        >>> log_ev_score(0.60)  # 60%
        -0.2876...
    """
    if corrected_return_rate <= 0:
        return LOG_EV_FLOOR
    return float(np.log(corrected_return_rate / baseline))


def lcb_gate(
    posterior_samples: np.ndarray,
    baseline: float = 0.80,
    quantile: float = DEFAULT_LCB_QUANTILE,
) -> bool:
    """
    階層ベイズの事後分布サンプルから、LCB（下方信頼限界）ゲートを判定する。

    事後分布の下方 quantile 分位点が baseline を超えるかどうかで
    投資適格性を判定する。

    Args:
        posterior_samples: 事後分布のサンプル配列（比率スケール: 0.80 = 80%）
        baseline: 投資適格の基準値（デフォルト 0.80）
        quantile: 下方分位点（デフォルト 0.10 = 10%分位点）

    Returns:
        True: 投資適格（LCB > baseline）
        False: 投資不適格

    Examples:
        >>> samples = np.random.normal(0.90, 0.02, 1000)  # 90%中心、低分散
        >>> lcb_gate(samples)  # → True（下方10%でも80%超）
        True
    """
    if len(posterior_samples) == 0:
        return False
    lcb = float(np.quantile(posterior_samples, quantile))
    return lcb > baseline


def generate_posterior_samples(
    bayes_estimated_rate: float,
    ci_lower: float,
    ci_upper: float,
    n_samples: int = 1000,
    rng: Optional[np.random.Generator] = None,
) -> np.ndarray:
    """
    階層ベイズ推定結果から事後分布サンプルを生成する。

    Phase 2の階層ベイズ推定結果（BayesEstimate）には点推定とCIのみが
    含まれるため、正規近似で事後分布サンプルを復元する。

    Args:
        bayes_estimated_rate: ベイズ推定値（%スケール: 80.0 = 80%）
        ci_lower: 95% CI 下限（%スケール）
        ci_upper: 95% CI 上限（%スケール）
        n_samples: 生成するサンプル数
        rng: 乱数ジェネレータ（再現性用）

    Returns:
        事後分布サンプル（比率スケール: 0.80 = 80%）
    """
    if rng is None:
        rng = np.random.default_rng()

    # %スケール → 比率スケール
    mean = bayes_estimated_rate / 100.0
    ci_lo = ci_lower / 100.0
    ci_hi = ci_upper / 100.0

    # 95% CI → σ 推定: CI幅 ≈ 2 * 1.96σ
    ci_width = ci_hi - ci_lo
    if ci_width <= 0:
        # CI幅ゼロ → 分散なし → 全サンプル同一値
        return np.full(n_samples, mean)

    std = ci_width / (2 * 1.96)

    return rng.normal(mean, std, size=n_samples)


def compute_horse_score(
    horse_factors: Dict[str, Tuple[str, str]],
    edge_table: Dict[Tuple[str, str, str, str], dict],
    bet_type: str = "tansho",
) -> float:
    """
    馬iの全該当ファクター×セグメントのLog-EVスコアを均等重み平均で合成する。

    Args:
        horse_factors: {factor_name: (segment_value, bin_value)} の辞書
            例: {'idm': ('芝', '21.0-25.0'), 'pace_shisu': ('ダート', '5.0-40.2')}
        edge_table: {(factor_name, segment_value, bin_value, bet_type): {
            'posterior_mean': float,    # ベイズ推定値（%スケール: 85.0 = 85%）
            'posterior_samples': np.ndarray,  # 事後分布サンプル（比率スケール）
            'N': int                    # サンプル数
        }}
        bet_type: 'tansho' or 'fukusho'

    Returns:
        合成Log-EVスコア（均等重み平均）。
        該当ファクターなし or 全てLCB不合格 → 0.0

    Notes:
        初期実装では均等重み（1/K）。
        Phase 3 Task 4完了後、LightGBMによる非線形重み最適化を検討。
    """
    scores: List[float] = []
    for factor_name, (segment_value, bin_value) in horse_factors.items():
        key = (factor_name, segment_value, bin_value, bet_type)
        if key not in edge_table:
            continue
        cell = edge_table[key]

        # LCBゲート
        if not lcb_gate(cell["posterior_samples"]):
            continue

        # Log-EV変換（posterior_mean は %スケール → 比率スケールに変換）
        score = log_ev_score(cell["posterior_mean"] / 100.0)
        scores.append(score)

    if len(scores) == 0:
        return 0.0

    return float(sum(scores) / len(scores))
