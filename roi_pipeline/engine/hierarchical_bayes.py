"""
階層ベイズ推定モジュール

3層構造:
    レベル1（グローバル）: 全条件の補正回収率
    レベル2（カテゴリ）: セグメント分類に基づくカテゴリ別
    レベル3（個別）: 個別条件の実データ

信頼性重み:
    reliability_weight = N / (N + C)
    C = 50（デフォルト、チューニング対象）

推定値:
    estimated_rate = reliability_weight * observed_rate
                   + (1 - reliability_weight) * prior_rate

Phase 1: 全ファクターにGLOBALを適用（レベル2なし）
"""
import numpy as np
from dataclasses import dataclass
from typing import Optional


BASELINE_RATE: float = 80.0


@dataclass
class BayesEstimate:
    """階層ベイズ推定結果"""
    estimated_rate: float
    reliability_weight: float
    ci_lower: float
    ci_upper: float
    score: float
    n_samples: int
    observed_rate: float
    prior_rate: float


def hierarchical_bayes_estimate(
    observed_rate: float,
    n_samples: int,
    prior_rate: float,
    C: int = 50,
    confidence_level: float = 0.95,
    observed_std: Optional[float] = None,
) -> BayesEstimate:
    """
    階層ベイズ推定を実行する。

    Args:
        observed_rate: 観測された補正回収率（%）
        n_samples: サンプル数
        prior_rate: 事前分布の平均値（上位レベルの補正回収率, %）
        C: 信頼性重みの定数（デフォルト50）
        confidence_level: 信頼区間の水準（デフォルト0.95）
        observed_std: 観測値の標準偏差。Noneの場合は正規近似で推定。

    Returns:
        BayesEstimate: 推定結果
    """
    if n_samples <= 0:
        return BayesEstimate(
            estimated_rate=prior_rate,
            reliability_weight=0.0,
            ci_lower=prior_rate,
            ci_upper=prior_rate,
            score=prior_rate - BASELINE_RATE,
            n_samples=0,
            observed_rate=observed_rate,
            prior_rate=prior_rate,
        )

    # 信頼性重み
    reliability_weight = n_samples / (n_samples + C)

    # ベイズ推定値
    estimated_rate = (
        reliability_weight * observed_rate
        + (1 - reliability_weight) * prior_rate
    )

    # 95%信頼区間（正規近似）
    if observed_std is not None and observed_std > 0:
        se = observed_std / np.sqrt(n_samples)
    else:
        # 回収率の標準偏差を正規近似で推定
        # 回収率の分散は大きいため、保守的な推定を使用
        se = max(observed_rate, 100.0) / np.sqrt(n_samples)

    from scipy.stats import norm
    z = norm.ppf(1 - (1 - confidence_level) / 2)

    # 推定値に対する信頼区間（重み付き）
    adjusted_se = se * reliability_weight
    ci_lower = estimated_rate - z * adjusted_se
    ci_upper = estimated_rate + z * adjusted_se

    return BayesEstimate(
        estimated_rate=round(estimated_rate, 4),
        reliability_weight=round(reliability_weight, 4),
        ci_lower=round(ci_lower, 4),
        ci_upper=round(ci_upper, 4),
        score=round(estimated_rate - BASELINE_RATE, 4),
        n_samples=n_samples,
        observed_rate=round(observed_rate, 4),
        prior_rate=round(prior_rate, 4),
    )


def three_level_estimate(
    observed_rate: float,
    n_samples: int,
    category_rate: Optional[float],
    category_n: Optional[int],
    global_rate: float,
    C: int = 50,
) -> BayesEstimate:
    """
    3層階層ベイズ推定を実行する。

    レベル1（グローバル）→ レベル2（カテゴリ）→ レベル3（個別）

    Args:
        observed_rate: レベル3の観測値（個別条件の補正回収率）
        n_samples: レベル3のサンプル数
        category_rate: レベル2の観測値（カテゴリ別補正回収率）。Noneの場合はGLOBAL。
        category_n: レベル2のサンプル数。Noneの場合はGLOBAL。
        global_rate: レベル1の値（全体の補正回収率）
        C: 信頼性重みの定数

    Returns:
        BayesEstimate: 推定結果
    """
    # GLOBAL（カテゴリなし）の場合: 2層で推定
    if category_rate is None or category_n is None:
        return hierarchical_bayes_estimate(
            observed_rate=observed_rate,
            n_samples=n_samples,
            prior_rate=global_rate,
            C=C,
        )

    # レベル2: カテゴリの推定値をグローバルで引き寄せ
    level2 = hierarchical_bayes_estimate(
        observed_rate=category_rate,
        n_samples=category_n,
        prior_rate=global_rate,
        C=C,
    )

    # レベル3: 個別の推定値をレベル2の推定値で引き寄せ
    return hierarchical_bayes_estimate(
        observed_rate=observed_rate,
        n_samples=n_samples,
        prior_rate=level2.estimated_rate,
        C=C,
    )
