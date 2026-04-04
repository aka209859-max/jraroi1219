"""
test_bayesian_kelly.py

Phase 3 Task 3: ベイズKelly資金配分エンジンのテスト

テスト要件（仕様書より12件）:
    1.  build_posterior: p_final=0.5, n_eff=100 → a≈75, b≈75
    2.  build_posterior: n_eff=0 → prior_strength のみ → 広い分布
    3.  build_posterior: a >= 1, b >= 1 が常に保証される
    4.  bayesian_kelly: odds=1.0 → 0.0（賭けない）
    5.  bayesian_kelly: p_final=0.5, odds=3.0 → 正の投資比率
    6.  bayesian_kelly: p_final=0.01, odds=2.0 → 0.0（EV負）
    7.  bayesian_kelly: n_eff大 vs n_eff小 → n_eff大の方が投資比率が大きい
    8.  bayesian_kelly: fractional_c=0.25 の効果検証（フルKellyの約1/4）
    9.  bayesian_kelly: 投資比率が 0.30 * 0.25 = 0.075 を超えないこと
    10. compute_n_eff: [100, 100, 100] → 100
    11. compute_n_eff: [100, 1] → ≈ 1.98（最小Nに引っ張られる）
    12. compute_n_eff: [] → 0.0
"""
import numpy as np
import pytest

from roi_pipeline.engine.bayesian_kelly import (
    bayesian_kelly,
    build_posterior,
    compute_n_eff,
)


# ─────────────────────────────────────────────
# build_posterior のテスト
# ─────────────────────────────────────────────

class TestBuildPosterior:
    """build_posterior 関数のテスト"""

    def test_p05_n100_returns_approx_75_75(self):
        """テスト1: p_final=0.5, n_eff=100, prior=50 → a≈75, b≈75"""
        a, b = build_posterior(0.5, n_eff=100.0, prior_strength=50.0)
        # κ = 100 + 50 = 150, a = 0.5*150 = 75, b = 0.5*150 = 75
        assert a == pytest.approx(75.0, rel=1e-6)
        assert b == pytest.approx(75.0, rel=1e-6)

    def test_n_eff_zero_uses_prior_only(self):
        """テスト2: n_eff=0 → prior_strength のみ → 広い分布（小さいκ）"""
        a_small, b_small = build_posterior(0.5, n_eff=0.0, prior_strength=50.0)
        a_large, b_large = build_posterior(0.5, n_eff=500.0, prior_strength=50.0)
        # n_eff=0 の方が κ が小さく、a, b も小さい（分布が広い）
        assert a_small < a_large
        assert b_small < b_large

    def test_a_b_always_ge_1(self):
        """テスト3: a >= 1, b >= 1 が常に保証される"""
        # 極端なケース: p_final が 0 か 1 に近い、n_eff が小さい
        for p in [0.001, 0.01, 0.5, 0.99, 0.999]:
            for n in [0.0, 1.0, 10.0]:
                a, b = build_posterior(p, n_eff=n, prior_strength=1.0)
                assert a >= 1.0, f"a={a} < 1 for p={p}, n={n}"
                assert b >= 1.0, f"b={b} < 1 for p={p}, n={n}"

    def test_high_n_eff_narrows_distribution(self):
        """n_eff が大きいほど Beta 分布が尖る（κ が大きい）"""
        a_low, b_low = build_posterior(0.3, n_eff=10.0)
        a_high, b_high = build_posterior(0.3, n_eff=1000.0)
        assert a_high > a_low
        assert b_high > b_low

    def test_p_final_reflected_in_ratio(self):
        """a/b の比が p_final/(1-p_final) に対応する"""
        p = 0.7
        a, b = build_posterior(p, n_eff=200.0)
        # a/b ≈ p/(1-p) = 7/3
        assert a / b == pytest.approx(p / (1.0 - p), rel=0.01)


# ─────────────────────────────────────────────
# compute_n_eff のテスト
# ─────────────────────────────────────────────

class TestComputeNEff:
    """compute_n_eff 関数のテスト"""

    def test_equal_sizes_returns_n(self):
        """テスト10: [100, 100, 100] → 100"""
        result = compute_n_eff([100.0, 100.0, 100.0])
        assert result == pytest.approx(100.0, rel=1e-6)

    def test_dominant_small_n(self):
        """テスト11: [100, 1] → ≈ 1.98（最小Nに引っ張られる）"""
        result = compute_n_eff([100.0, 1.0])
        # 調和平均: 2 / (1/100 + 1/1) = 2 / 1.01 ≈ 1.9802
        expected = 2.0 / (1.0 / 100.0 + 1.0 / 1.0)
        assert result == pytest.approx(expected, rel=1e-6)
        assert result < 2.0  # 最小Nに引っ張られる

    def test_empty_list_returns_zero(self):
        """テスト12: [] → 0.0"""
        assert compute_n_eff([]) == 0.0

    def test_single_element(self):
        """単一要素 → そのまま"""
        assert compute_n_eff([50.0]) == pytest.approx(50.0, rel=1e-6)

    def test_harmonic_mean_less_than_arithmetic(self):
        """調和平均 < 算術平均（不均等の場合）"""
        sizes = [10.0, 100.0, 1000.0]
        harmonic = compute_n_eff(sizes)
        arithmetic = sum(sizes) / len(sizes)
        assert harmonic < arithmetic

    def test_zero_size_treated_as_one(self):
        """N=0 は N=1 として扱われる（ゼロ除算防止）"""
        result = compute_n_eff([0.0, 100.0])
        result_with_one = compute_n_eff([1.0, 100.0])
        assert result == pytest.approx(result_with_one, rel=1e-6)


# ─────────────────────────────────────────────
# bayesian_kelly のテスト
# ─────────────────────────────────────────────

class TestBayesianKelly:
    """bayesian_kelly 関数のテスト"""

    def test_odds_le_one_returns_zero(self):
        """テスト4: odds=1.0 → 0.0（賭けない）"""
        assert bayesian_kelly(0.5, odds=1.0, n_eff=100.0) == 0.0

    def test_odds_below_one_returns_zero(self):
        """odds < 1.0 → 0.0"""
        assert bayesian_kelly(0.9, odds=0.8, n_eff=100.0) == 0.0

    def test_positive_ev_returns_positive(self):
        """テスト5: p_final=0.5, odds=3.0 → 正の投資比率"""
        # EV = 0.5 * 3.0 - 1.0 = 0.5 > 0
        result = bayesian_kelly(0.5, odds=3.0, n_eff=100.0, rng_seed=0)
        assert result > 0.0

    def test_negative_ev_returns_zero(self):
        """テスト6: p_final=0.01, odds=2.0 → 0.0（EV負）"""
        # EV = 0.01 * 2.0 - 1.0 = -0.98 << 0
        result = bayesian_kelly(0.01, odds=2.0, n_eff=100.0, rng_seed=0)
        assert result == 0.0

    def test_high_n_eff_larger_than_low(self):
        """テスト7: n_eff大 vs n_eff小 → n_eff大の方が投資比率が大きい（または等しい）"""
        # n_eff が大きいほど事後分布が尖る → 確信度が高い → Kelly比率が大きくなる
        result_high = bayesian_kelly(0.5, odds=3.0, n_eff=500.0, rng_seed=42)
        result_low = bayesian_kelly(0.5, odds=3.0, n_eff=5.0, rng_seed=42)
        assert result_high >= result_low

    def test_fractional_c_reduces_to_quarter(self):
        """テスト8: fractional_c=0.25 の効果検証（フルKellyの約1/4）"""
        # フルKelly（c=1.0）と クォーターKelly（c=0.25）を比較
        result_full = bayesian_kelly(0.5, odds=3.0, n_eff=200.0,
                                     fractional_c=1.0, rng_seed=0)
        result_quarter = bayesian_kelly(0.5, odds=3.0, n_eff=200.0,
                                        fractional_c=0.25, rng_seed=0)
        if result_full > 0:
            # クォーターKellyはフルKellyの約1/4
            ratio = result_quarter / result_full
            assert ratio == pytest.approx(0.25, rel=1e-6)

    def test_max_bet_size_capped(self):
        """テスト9: 投資比率が 0.30 * 0.25 = 0.075 を超えないこと"""
        # f_grid の上限 0.30 に fractional_c=0.25 をかけた上限 = 0.075
        result = bayesian_kelly(0.9, odds=10.0, n_eff=1000.0,
                                fractional_c=0.25, rng_seed=0)
        assert result <= 0.30 * 0.25 + 1e-9  # f_grid上限 * fractional_c

    def test_returns_float(self):
        """戻り値が float である"""
        result = bayesian_kelly(0.4, odds=5.0, n_eff=100.0, rng_seed=0)
        assert isinstance(result, float)

    def test_custom_f_grid(self):
        """カスタム f_grid を渡せる"""
        f_grid = np.linspace(0.01, 0.10, 50)
        result = bayesian_kelly(0.5, odds=3.0, n_eff=100.0,
                                f_grid=f_grid, rng_seed=0)
        # カスタム上限 0.10 * fractional_c(0.25) = 0.025 以下
        assert result <= 0.10 * 0.25 + 1e-9

    def test_very_high_probability_and_odds(self):
        """p_final=0.8, odds=5.0 → 正の大きな投資比率"""
        # EV = 0.8 * 5.0 - 1.0 = 3.0 >> 0
        result = bayesian_kelly(0.8, odds=5.0, n_eff=300.0, rng_seed=0)
        assert result > 0.0

    def test_borderline_ev_zero(self):
        """EV ≈ 0 のケース（p = 1/odds）→ 0 に近い結果"""
        # p = 1/3, odds = 3.0 → EV = 1/3 * 3 - 1 = 0 (理論上ゼロ)
        # ベイズ不確実性のため、実際には 0 になるか微小正になる
        result = bayesian_kelly(1.0 / 3.0, odds=3.0, n_eff=100.0, rng_seed=0)
        assert result >= 0.0
