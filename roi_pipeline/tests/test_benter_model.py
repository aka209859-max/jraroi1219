"""
test_benter_model.py

Phase 3 Task 2: Benter二段階モデルのテスト

テスト要件（仕様書より10件）:
    1.  implied_probability: オッズ均等 → 確率均等
    2.  implied_probability: 確率合計 = 1.0
    3.  implied_probability: 低オッズ馬の確率が高い
    4.  combine_scores: alpha_wp=0.35 の重み検証
    5.  combine_scores: 単勝0、複勝のみ → 0.65 × 複勝スコア
    6.  benter_integrate: alpha=0, beta=1 → P_final ≈ P_market
    7.  benter_integrate: alpha=1, beta=0 → P_final ∝ exp(s_combined)
    8.  benter_integrate: P_final の合計 = 1.0
    9.  benter_integrate: 最尤推定で alpha < beta となること（合成データ）
    10. benter_integrate: 全馬のスコアが0 → P_final = P_market
"""
import numpy as np
import pytest

from roi_pipeline.engine.benter_model import (
    benter_integrate,
    combine_scores,
    fit_benter_params,
    implied_probability,
)


# ─────────────────────────────────────────────
# implied_probability のテスト
# ─────────────────────────────────────────────

class TestImpliedProbability:
    """implied_probability 関数のテスト"""

    def test_equal_odds_equal_probability(self):
        """テスト1: 均等オッズ → 均等確率"""
        odds = np.array([2.0, 2.0, 2.0, 2.0])
        probs = implied_probability(odds)
        expected = np.array([0.25, 0.25, 0.25, 0.25])
        np.testing.assert_allclose(probs, expected, atol=1e-10)

    def test_sum_to_one(self):
        """テスト2: 確率合計 = 1.0"""
        odds = np.array([2.5, 3.0, 5.0, 10.0, 20.0])
        probs = implied_probability(odds)
        assert probs.sum() == pytest.approx(1.0, abs=1e-10)

    def test_lower_odds_higher_probability(self):
        """テスト3: 低オッズ馬の確率が高い"""
        odds = np.array([1.5, 5.0, 10.0])
        probs = implied_probability(odds)
        assert probs[0] > probs[1] > probs[2]

    def test_single_horse(self):
        """1頭のみ → 確率 1.0"""
        probs = implied_probability(np.array([2.0]))
        assert probs[0] == pytest.approx(1.0, abs=1e-10)

    def test_zero_odds_raises(self):
        """オッズ0 → ValueError"""
        with pytest.raises(ValueError):
            implied_probability(np.array([2.0, 0.0, 3.0]))

    def test_overround_normalized(self):
        """控除率込みのオッズでも合計が1になる"""
        # 控除率20%想定: 1/1.5 + 1/2.0 + 1/3.0 = 0.667 + 0.5 + 0.333 = 1.5
        odds = np.array([1.5, 2.0, 3.0])
        probs = implied_probability(odds)
        assert probs.sum() == pytest.approx(1.0, abs=1e-10)
        # 低オッズが高確率
        assert probs[0] > probs[1] > probs[2]


# ─────────────────────────────────────────────
# combine_scores のテスト
# ─────────────────────────────────────────────

class TestCombineScores:
    """combine_scores 関数のテスト"""

    def test_alpha_wp_035_weights(self):
        """テスト4: alpha_wp=0.35 の重み検証"""
        s_win = 1.0
        s_place = 2.0
        result = combine_scores(s_win, s_place, alpha_wp=0.35)
        expected = 0.35 * 1.0 + 0.65 * 2.0
        assert result == pytest.approx(expected, rel=1e-10)

    def test_win_zero_only_place(self):
        """テスト5: 単勝0、複勝のみ → 0.65 × 複勝スコア"""
        s_place = 0.5
        result = combine_scores(0.0, s_place, alpha_wp=0.35)
        assert result == pytest.approx(0.65 * s_place, rel=1e-10)

    def test_place_zero_only_win(self):
        """複勝0、単勝のみ → 0.35 × 単勝スコア"""
        s_win = 0.4
        result = combine_scores(s_win, 0.0, alpha_wp=0.35)
        assert result == pytest.approx(0.35 * s_win, rel=1e-10)

    def test_both_zero(self):
        """両方ゼロ → 0.0"""
        result = combine_scores(0.0, 0.0)
        assert result == 0.0

    def test_negative_scores(self):
        """負スコアも正しく重み付けされる"""
        result = combine_scores(-0.1, -0.2, alpha_wp=0.35)
        expected = 0.35 * (-0.1) + 0.65 * (-0.2)
        assert result == pytest.approx(expected, rel=1e-10)

    def test_default_alpha_is_035(self):
        """デフォルト alpha_wp = 0.35"""
        s_win, s_place = 0.3, 0.7
        result_default = combine_scores(s_win, s_place)
        result_explicit = combine_scores(s_win, s_place, alpha_wp=0.35)
        assert result_default == pytest.approx(result_explicit, rel=1e-10)


# ─────────────────────────────────────────────
# benter_integrate のテスト
# ─────────────────────────────────────────────

class TestBenterIntegrate:
    """benter_integrate 関数のテスト"""

    def _make_race(self, n: int = 5, rng_seed: int = 0):
        """テスト用レースデータを生成するヘルパー"""
        rng = np.random.default_rng(rng_seed)
        s_combined = rng.normal(0.1, 0.2, n)
        odds = rng.uniform(2.0, 20.0, n)
        p_market = implied_probability(odds)
        return s_combined, p_market

    def test_alpha0_beta1_equals_market(self):
        """テスト6: alpha=0, beta=1 → P_final ≈ P_market"""
        s_combined, p_market = self._make_race()
        p_final = benter_integrate(s_combined, p_market, alpha=0.0, beta=1.0)
        np.testing.assert_allclose(p_final, p_market, atol=1e-6)

    def test_alpha1_beta0_proportional_to_exp_score(self):
        """テスト7: alpha=1, beta=0 → P_final ∝ exp(s_combined)"""
        s_combined = np.array([0.1, 0.3, -0.2, 0.0, 0.5])
        p_market = np.array([0.2, 0.2, 0.2, 0.2, 0.2])  # 均等
        p_final = benter_integrate(s_combined, p_market, alpha=1.0, beta=0.0)

        # P_final が exp(s_combined) に比例するはず
        exp_s = np.exp(s_combined)
        expected = exp_s / exp_s.sum()
        np.testing.assert_allclose(p_final, expected, atol=1e-6)

    def test_sum_to_one(self):
        """テスト8: P_final の合計 = 1.0"""
        s_combined, p_market = self._make_race(n=8)
        p_final = benter_integrate(s_combined, p_market, alpha=0.5, beta=1.0)
        assert p_final.sum() == pytest.approx(1.0, abs=1e-10)

    def test_mle_alpha_less_than_beta(self):
        """テスト9: 最尤推定で alpha < beta となること（合成データ）

        市場は情報効率的であるため、一般に beta（市場への依存度）は
        alpha（ファンダメンタルへの依存度）より大きくなる。
        合成データでは市場確率を「真の確率」とし、
        ファンダメンタルはノイズとする。
        """
        rng = np.random.default_rng(123)
        n_races = 200
        n_horses = 6

        all_s, all_pm, all_oc = [], [], []
        for _ in range(n_races):
            # 市場確率が真の確率に近い合成データ
            true_probs = rng.dirichlet(np.ones(n_horses))
            odds = 1.0 / true_probs
            pm = implied_probability(odds)

            # ファンダメンタルはノイズ（市場より情報量が低い）
            s = rng.normal(0.0, 0.05, n_horses)

            # 勝馬を真の確率から抽出
            winner = rng.choice(n_horses, p=true_probs)
            oc = np.zeros(n_horses)
            oc[winner] = 1.0

            all_s.append(s)
            all_pm.append(pm)
            all_oc.append(oc)

        alpha_hat, beta_hat = fit_benter_params(all_s, all_pm, all_oc)
        assert alpha_hat < beta_hat, (
            f"合成データでは alpha({alpha_hat:.4f}) < beta({beta_hat:.4f}) が期待される"
        )

    def test_all_scores_zero_equals_market(self):
        """テスト10: 全馬スコアが0 → P_final = P_market"""
        _, p_market = self._make_race(n=6)
        s_combined = np.zeros(6)
        p_final = benter_integrate(s_combined, p_market, alpha=1.0, beta=1.0)
        # s_combined = 0 → alpha * 0 = 0 → logits = beta * log(p_market)
        # → P_final = softmax(log(p_market)) = p_market
        np.testing.assert_allclose(p_final, p_market, atol=1e-6)

    def test_all_probabilities_positive(self):
        """P_final の全要素が正である"""
        s_combined, p_market = self._make_race(n=10)
        p_final = benter_integrate(s_combined, p_market, alpha=0.5, beta=1.0)
        assert np.all(p_final > 0)

    def test_missing_alpha_beta_without_outcomes_raises(self):
        """alpha/beta 未指定 かつ outcomes なし → ValueError"""
        s_combined, p_market = self._make_race(n=5)
        with pytest.raises(ValueError):
            benter_integrate(s_combined, p_market)

    def test_mle_single_race(self):
        """最尤推定: 1レース分の outcomes から P_final が返る"""
        s_combined = np.array([0.2, 0.0, -0.1, 0.3])
        p_market = np.array([0.4, 0.3, 0.2, 0.1])
        outcomes = np.array([1.0, 0.0, 0.0, 0.0])  # 馬0が勝利
        p_final = benter_integrate(s_combined, p_market, outcomes=outcomes)
        # 結果が確率配列であること
        assert p_final.sum() == pytest.approx(1.0, abs=1e-6)
        assert np.all(p_final > 0)


# ─────────────────────────────────────────────
# fit_benter_params のテスト
# ─────────────────────────────────────────────

class TestFitBenterParams:
    """fit_benter_params 関数のテスト"""

    def test_returns_two_floats(self):
        """(alpha, beta) の2つのfloatを返す"""
        rng = np.random.default_rng(0)
        n = 5
        s = [rng.normal(0, 0.1, n) for _ in range(10)]
        pm = [implied_probability(rng.uniform(2, 10, n)) for _ in range(10)]
        oc = [np.eye(n)[rng.integers(n)] for _ in range(10)]
        alpha, beta = fit_benter_params(s, pm, oc)
        assert isinstance(alpha, float)
        assert isinstance(beta, float)

    def test_ideal_market_data_beta_dominates(self):
        """市場が正確なデータでは beta が alpha より大きくなる"""
        rng = np.random.default_rng(42)
        n_races, n_horses = 100, 5

        all_s, all_pm, all_oc = [], [], []
        for _ in range(n_races):
            true_p = rng.dirichlet(np.ones(n_horses) * 2)
            pm = implied_probability(1.0 / true_p)
            s = rng.normal(0.0, 0.02, n_horses)  # ノイズのみ
            winner = rng.choice(n_horses, p=true_p)
            oc = np.zeros(n_horses)
            oc[winner] = 1.0
            all_s.append(s)
            all_pm.append(pm)
            all_oc.append(oc)

        alpha, beta = fit_benter_params(all_s, all_pm, all_oc)
        assert alpha < beta
