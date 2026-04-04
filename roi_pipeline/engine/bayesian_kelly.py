"""
ベイズKelly資金配分エンジン

Phase 3 Task 3: Task 2で得た P_final(i) を点推定ではなく Beta 事後分布として扱い、
モンテカルロ積分で Kelly 最適投資比率を算出する。
フラクショナル係数（クォーターKelly）で破産リスクを制御する。

学術的根拠:
    Baker & McHale (2013), Swartz et al. (SFU)
    パラメータ不確実性を Beta 事後分布として内蔵し、フルKellyの破産リスクを回避。
    クォーターKelly（fractional_c=0.25）で年間破産確率 ≈ 1/81。
"""
from typing import List, Optional, Tuple

import numpy as np
from scipy.stats import beta as beta_dist


def build_posterior(
    p_final: float,
    n_eff: float,
    prior_strength: float = 50.0,
) -> Tuple[float, float]:
    """
    P_final を中心とする Beta 事後分布のパラメータ (a, b) を返す。

    κ = n_eff + prior_strength
    a = p_final * κ    （>= 1 を保証）
    b = (1 - p_final) * κ  （>= 1 を保証）

    n_eff が大きい → κ が大きい → Beta分布が尖る（確信度高）
    n_eff が小さい → κ が小さい → Beta分布が広い（不確実性大）
    少標本セグメントは事後分布が広くなり、Kelly比率が自動的に縮小される。

    Args:
        p_final: Task 2 から得た真の勝率推定値 (0, 1)
        n_eff: 実効サンプル数（該当ファクター×セグメントの N の調和平均）
        prior_strength: 事前分布の強さ（初期値50、Walk-Forwardで最適化）

    Returns:
        Beta分布のパラメータ (a, b)。a >= 1, b >= 1 を保証。
    """
    kappa = n_eff + prior_strength
    a = max(p_final * kappa, 1.0)
    b = max((1.0 - p_final) * kappa, 1.0)
    return (a, b)


def compute_n_eff(factor_sample_sizes: List[float]) -> float:
    """
    馬iが該当する全ファクター×セグメントのサンプル数の調和平均。

    最も弱いリンク（最小N）が全体の不確実性を支配する設計。
    1つでも極小Nのセルがあれば全体の確信度が下がり、Kelly比率が縮小される。

    Args:
        factor_sample_sizes: [N_1, N_2, ..., N_k]（各ファクターのセルのN）

    Returns:
        調和平均。空リストの場合は 0.0。
    """
    if len(factor_sample_sizes) == 0:
        return 0.0
    reciprocals = [1.0 / max(n, 1) for n in factor_sample_sizes]
    return len(reciprocals) / sum(reciprocals)


def bayesian_kelly(
    p_final: float,
    odds: float,
    n_eff: float,
    n_samples: int = 5000,
    fractional_c: float = 0.25,
    f_grid: Optional[np.ndarray] = None,
    prior_strength: float = 50.0,
    rng_seed: Optional[int] = None,
) -> float:
    """
    ベイズ事後分布に基づくフラクショナルKelly。

    アルゴリズム:
        1. P_final を中心とする Beta 分布から p を n_samples 回サンプリング
        2. 各 p について、投資比率 f での対数成長率を計算
        3. 全サンプルの平均対数成長率を最大化する f* を求める
        4. f* にフラクショナル係数 c を掛ける

    fractional_c = 0.25（クォーターKelly）
    → 年間破産確率 ≈ 1/81（フルKellyの1/3に対して劇的改善）

    Args:
        p_final: 真の勝率推定値（Task 2 の出力）
        odds: 確定オッズ（倍率表示。1.0 以下なら 0.0 を返す）
        n_eff: 実効サンプル数（compute_n_eff の出力）
        n_samples: モンテカルロサンプル数（デフォルト 5000）
        fractional_c: フラクショナル係数（デフォルト 0.25 = クォーターKelly）
        f_grid: 探索する投資比率の格子点。None なら 0.1%〜30% の 300点。
        prior_strength: build_posterior に渡す事前分布強度
        rng_seed: 再現性のための乱数シード（テスト用）

    Returns:
        最適投資比率（バンクロールに対する割合）。
        0.0 = 賭けない（EV非正またはオッズ <= 1.0）。
        最大値は f_grid 上限 * fractional_c。
    """
    if odds <= 1.0:
        return 0.0

    a, b = build_posterior(p_final, n_eff, prior_strength=prior_strength)

    if rng_seed is not None:
        p_samples = beta_dist.rvs(a, b, size=n_samples,
                                  random_state=np.random.default_rng(rng_seed))
    else:
        p_samples = beta_dist.rvs(a, b, size=n_samples)

    if f_grid is None:
        f_grid = np.linspace(0.001, 0.30, 300)

    net_odds = odds - 1.0  # ネットオッズ（配当 - 元本）

    # 各 f について平均対数成長率を一括計算（ベクトル化）
    # shape: (len(f_grid), n_samples)
    f_col = f_grid[:, np.newaxis]           # (F, 1)
    p_row = p_samples[np.newaxis, :]        # (1, S)

    log_win = np.log(1.0 + f_col * net_odds)   # (F, S)
    log_lose = np.log(1.0 - f_col)              # (F, S) — f < 1 を仮定

    mean_log_growth = np.mean(
        p_row * log_win + (1.0 - p_row) * log_lose,
        axis=1,
    )  # (F,)

    best_idx = int(np.argmax(mean_log_growth))
    best_growth = mean_log_growth[best_idx]
    best_f = f_grid[best_idx]

    if best_growth <= 0:
        return 0.0

    return float(best_f * fractional_c)
