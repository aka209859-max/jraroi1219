"""
階層ベイズ推定モジュールのユニットテスト

テスト対象:
    - hierarchical_bayes_estimate()
    - three_level_estimate()
    - BayesEstimate dataclass
"""
import pytest
import numpy as np

from roi_pipeline.engine.hierarchical_bayes import (
    hierarchical_bayes_estimate,
    three_level_estimate,
    BayesEstimate,
    BASELINE_RATE,
)


class TestHierarchicalBayesEstimate:
    """階層ベイズ推定の基本テスト"""

    def test_returns_bayes_estimate(self) -> None:
        """BayesEstimateが返ること"""
        result = hierarchical_bayes_estimate(
            observed_rate=90.0,
            n_samples=100,
            prior_rate=80.0,
        )
        assert isinstance(result, BayesEstimate)

    def test_reliability_weight_formula(self) -> None:
        """信頼性重み = N / (N + C)"""
        result = hierarchical_bayes_estimate(
            observed_rate=90.0,
            n_samples=100,
            prior_rate=80.0,
            C=50,
        )
        expected_weight = 100 / (100 + 50)
        assert abs(result.reliability_weight - expected_weight) < 0.001

    def test_estimated_rate_formula(self) -> None:
        """推定値 = weight * observed + (1 - weight) * prior"""
        result = hierarchical_bayes_estimate(
            observed_rate=90.0,
            n_samples=100,
            prior_rate=80.0,
            C=50,
        )
        weight = 100 / (100 + 50)
        expected_rate = weight * 90.0 + (1 - weight) * 80.0
        assert abs(result.estimated_rate - expected_rate) < 0.01

    def test_score_is_rate_minus_baseline(self) -> None:
        """score = estimated_rate - 80.0"""
        result = hierarchical_bayes_estimate(
            observed_rate=90.0,
            n_samples=100,
            prior_rate=80.0,
        )
        assert abs(result.score - (result.estimated_rate - BASELINE_RATE)) < 0.01

    def test_zero_samples_returns_prior(self) -> None:
        """N=0の場合、事前分布をそのまま返す"""
        result = hierarchical_bayes_estimate(
            observed_rate=120.0,
            n_samples=0,
            prior_rate=80.0,
        )
        assert result.estimated_rate == 80.0
        assert result.reliability_weight == 0.0

    def test_large_samples_converge_to_observed(self) -> None:
        """Nが十分大きい場合、観測値に収束する"""
        result = hierarchical_bayes_estimate(
            observed_rate=95.0,
            n_samples=100000,
            prior_rate=80.0,
            C=50,
        )
        assert abs(result.estimated_rate - 95.0) < 0.1

    def test_small_samples_pulled_to_prior(self) -> None:
        """Nが小さい場合、事前分布に引き寄せられる"""
        result = hierarchical_bayes_estimate(
            observed_rate=120.0,
            n_samples=5,
            prior_rate=80.0,
            C=50,
        )
        # 5/(5+50) = 0.0909 → 観測値よりpriorに近い
        assert result.estimated_rate < 90.0
        assert result.estimated_rate > 80.0

    def test_confidence_interval_contains_estimate(self) -> None:
        """95%信頼区間が推定値を含むこと"""
        result = hierarchical_bayes_estimate(
            observed_rate=90.0,
            n_samples=100,
            prior_rate=80.0,
        )
        assert result.ci_lower <= result.estimated_rate <= result.ci_upper

    def test_wider_ci_with_fewer_samples(self) -> None:
        """サンプル数が少ないほど信頼区間が広い"""
        result_small = hierarchical_bayes_estimate(
            observed_rate=90.0,
            n_samples=10,
            prior_rate=80.0,
        )
        result_large = hierarchical_bayes_estimate(
            observed_rate=90.0,
            n_samples=1000,
            prior_rate=80.0,
        )
        ci_width_small = result_small.ci_upper - result_small.ci_lower
        ci_width_large = result_large.ci_upper - result_large.ci_lower
        assert ci_width_small > ci_width_large

    def test_negative_samples_returns_prior(self) -> None:
        """負のサンプル数は0扱い → 事前分布を返す"""
        result = hierarchical_bayes_estimate(
            observed_rate=90.0,
            n_samples=-5,
            prior_rate=80.0,
        )
        assert result.estimated_rate == 80.0
        assert result.reliability_weight == 0.0


class TestThreeLevelEstimate:
    """3層階層ベイズ推定のテスト"""

    def test_global_only(self) -> None:
        """カテゴリなし（GLOBAL）の場合、2層で推定"""
        result = three_level_estimate(
            observed_rate=90.0,
            n_samples=100,
            category_rate=None,
            category_n=None,
            global_rate=80.0,
        )
        assert isinstance(result, BayesEstimate)
        # 2層推定と同じ結果になること
        direct = hierarchical_bayes_estimate(
            observed_rate=90.0,
            n_samples=100,
            prior_rate=80.0,
        )
        assert abs(result.estimated_rate - direct.estimated_rate) < 0.01

    def test_three_levels_with_category(self) -> None:
        """カテゴリあり（3層）の場合"""
        result = three_level_estimate(
            observed_rate=95.0,
            n_samples=50,
            category_rate=88.0,
            category_n=500,
            global_rate=80.0,
        )
        assert isinstance(result, BayesEstimate)
        # 3層のため、カテゴリの影響を受ける
        assert result.estimated_rate != 95.0

    def test_category_pulls_toward_global(self) -> None:
        """カテゴリのサンプルが少ない場合、グローバルに引き寄せられる"""
        result_small_cat = three_level_estimate(
            observed_rate=95.0,
            n_samples=50,
            category_rate=100.0,
            category_n=5,  # カテゴリのサンプルが少ない
            global_rate=80.0,
        )
        result_large_cat = three_level_estimate(
            observed_rate=95.0,
            n_samples=50,
            category_rate=100.0,
            category_n=5000,  # カテゴリのサンプルが多い
            global_rate=80.0,
        )
        # カテゴリNが少ない方がグローバルに近い → 推定値が低くなる
        assert result_small_cat.estimated_rate < result_large_cat.estimated_rate

    def test_all_fields_populated(self) -> None:
        """BayesEstimateの全フィールドが埋まっていること"""
        result = three_level_estimate(
            observed_rate=90.0,
            n_samples=100,
            category_rate=85.0,
            category_n=1000,
            global_rate=80.0,
        )
        assert result.estimated_rate is not None
        assert result.reliability_weight is not None
        assert result.ci_lower is not None
        assert result.ci_upper is not None
        assert result.score is not None
        assert result.n_samples == 100
        assert result.observed_rate is not None
        assert result.prior_rate is not None
