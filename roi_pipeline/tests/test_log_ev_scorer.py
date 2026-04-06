"""
Phase 3 Task 1: Log-EV得点化エンジン＋LCBゲート テスト

PHASE3_ARCHITECTURE_SPEC.md テスト要件:
1.  log_ev_score(0.80) == 0.0
2.  log_ev_score(1.00) ≈ 0.223
3.  log_ev_score(0.60) ≈ -0.288
4.  log_ev_score(0.0) == -10.0（下限クリップ）
5.  lcb_gate: 事後分布の90%が80%超 → True
6.  lcb_gate: 事後分布の90%が80%以下 → False
7.  compute_horse_score: ファクターなし → 0.0
8.  compute_horse_score: 単一ファクター、エッジあり → 正のスコア
9.  compute_horse_score: 単一ファクター、LCB不合格 → 0.0
10. compute_horse_score: 複数ファクター合成 → 均等重み平均
11. 単勝と複勝で独立にスコアが算出されること
12. 全体回収率79.92%に対するLog-EVスコアが負であること（79.92/80 < 1）
"""
import numpy as np
import pytest

from roi_pipeline.engine.log_ev_scorer import (
    log_ev_score,
    lcb_gate,
    compute_horse_score,
    generate_posterior_samples,
    filter_edge_table,
    summarize_edge_table,
    LOG_EV_FLOOR,
    LOG_EV_BASELINE,
)


# ============================================================
# Test 1: log_ev_score(0.80) == 0.0
# ============================================================
class TestLogEvScore:
    def test_baseline_returns_zero(self):
        """log_ev_score(0.80) == 0.0: baseline と一致でゼロ"""
        assert log_ev_score(0.80) == pytest.approx(0.0, abs=1e-10)

    def test_100_percent(self):
        """log_ev_score(1.00) ≈ 0.223: 100% → ln(1.0/0.8)"""
        expected = np.log(1.00 / 0.80)
        assert log_ev_score(1.00) == pytest.approx(expected, abs=1e-3)
        assert abs(log_ev_score(1.00) - 0.223) < 0.001

    def test_60_percent(self):
        """log_ev_score(0.60) ≈ -0.288: 60% → ln(0.6/0.8)"""
        expected = np.log(0.60 / 0.80)
        assert log_ev_score(0.60) == pytest.approx(expected, abs=1e-3)
        assert abs(log_ev_score(0.60) - (-0.288)) < 0.001

    def test_zero_clips_to_floor(self):
        """log_ev_score(0.0) == -10.0: ゼロ入力は下限クリップ"""
        assert log_ev_score(0.0) == LOG_EV_FLOOR

    def test_negative_clips_to_floor(self):
        """負の入力も下限クリップ"""
        assert log_ev_score(-0.5) == LOG_EV_FLOOR

    def test_120_percent(self):
        """log_ev_score(1.20) ≈ 0.405: 非常に強いエッジ"""
        expected = np.log(1.20 / 0.80)
        assert log_ev_score(1.20) == pytest.approx(expected, abs=1e-3)

    def test_monotonically_increasing(self):
        """入力が大きいほどスコアも大きい（単調増加）"""
        rates = [0.60, 0.70, 0.80, 0.90, 1.00, 1.10, 1.20]
        scores = [log_ev_score(r) for r in rates]
        for i in range(len(scores) - 1):
            assert scores[i] < scores[i + 1]


# ============================================================
# Test 5-6: lcb_gate
# ============================================================
class TestLcbGate:
    def test_high_posterior_passes(self):
        """事後分布の90%が80%超 → True"""
        rng = np.random.default_rng(42)
        # 平均 0.90, 標準偏差 0.02 → 下方10%分位点 ≈ 0.874 > 0.80
        samples = rng.normal(0.90, 0.02, 10000)
        assert lcb_gate(samples) is True

    def test_low_posterior_fails(self):
        """事後分布の90%が80%以下 → False"""
        rng = np.random.default_rng(42)
        # 平均 0.75, 標準偏差 0.05 → 下方10%分位点 ≈ 0.686 < 0.80
        samples = rng.normal(0.75, 0.05, 10000)
        assert lcb_gate(samples) is False

    def test_borderline_high_variance_fails(self):
        """平均は80%超だが分散が大きい → LCB < 80% → False"""
        rng = np.random.default_rng(42)
        # 平均 0.85, 標準偏差 0.10 → 下方10%分位点 ≈ 0.72 < 0.80
        samples = rng.normal(0.85, 0.10, 10000)
        assert lcb_gate(samples) is False

    def test_empty_samples_returns_false(self):
        """空のサンプル配列 → False"""
        assert lcb_gate(np.array([])) is False


# ============================================================
# Test 7-10: compute_horse_score
# ============================================================
class TestComputeHorseScore:
    def _make_edge_table_entry(
        self, posterior_mean_pct: float, lcb_passes: bool, n: int = 10000
    ) -> dict:
        """テスト用エッジテーブルエントリを作成"""
        rng = np.random.default_rng(42)
        mean_ratio = posterior_mean_pct / 100.0
        if lcb_passes:
            # 下方10%分位点が0.80超になるよう狭い分散
            std = 0.01
        else:
            # 下方10%分位点が0.80以下になるよう広い分散
            std = 0.15
        samples = rng.normal(mean_ratio, std, 5000)
        return {
            "posterior_mean": posterior_mean_pct,
            "posterior_samples": samples,
            "N": n,
        }

    def test_no_factors_returns_zero(self):
        """ファクターなし → 0.0"""
        result = compute_horse_score({}, {}, bet_type="tansho")
        assert result == 0.0

    def test_single_factor_edge_positive(self):
        """単一ファクター、エッジあり → 正のスコア"""
        horse_factors = {"idm": ("芝", "21.0-25.0")}
        edge_table = {
            ("idm", "芝", "21.0-25.0", "tansho"): self._make_edge_table_entry(
                posterior_mean_pct=90.0, lcb_passes=True
            )
        }
        score = compute_horse_score(horse_factors, edge_table, bet_type="tansho")
        assert score > 0.0

    def test_single_factor_lcb_fail_returns_zero(self):
        """単一ファクター、LCB不合格 → 0.0"""
        horse_factors = {"idm": ("芝", "21.0-25.0")}
        edge_table = {
            ("idm", "芝", "21.0-25.0", "tansho"): self._make_edge_table_entry(
                posterior_mean_pct=85.0, lcb_passes=False
            )
        }
        score = compute_horse_score(horse_factors, edge_table, bet_type="tansho")
        assert score == 0.0

    def test_multiple_factors_equal_weight_average(self):
        """複数ファクター合成 → 均等重み平均"""
        horse_factors = {
            "idm": ("芝", "21.0-25.0"),
            "pace_shisu": ("ダート", "5.0-40.2"),
        }
        # 2つのファクター、両方LCBパス
        entry1 = self._make_edge_table_entry(90.0, True)
        entry2 = self._make_edge_table_entry(100.0, True)
        edge_table = {
            ("idm", "芝", "21.0-25.0", "tansho"): entry1,
            ("pace_shisu", "ダート", "5.0-40.2", "tansho"): entry2,
        }
        combined = compute_horse_score(horse_factors, edge_table, bet_type="tansho")

        # 個別スコア
        s1 = log_ev_score(90.0 / 100.0)
        s2 = log_ev_score(100.0 / 100.0)
        expected_avg = (s1 + s2) / 2.0

        assert combined == pytest.approx(expected_avg, abs=1e-6)


# ============================================================
# Test 11: 単勝と複勝で独立にスコアが算出されること
# ============================================================
class TestBetTypeIndependence:
    def test_tansho_fukusho_independent(self):
        """単勝と複勝で独立にスコアが算出される"""
        rng = np.random.default_rng(42)

        horse_factors = {"idm": ("芝", "21.0-25.0")}

        # 単勝エッジ: 90%, 複勝エッジ: 105%
        tansho_samples = rng.normal(0.90, 0.01, 5000)
        fukusho_samples = rng.normal(1.05, 0.01, 5000)

        edge_table = {
            ("idm", "芝", "21.0-25.0", "tansho"): {
                "posterior_mean": 90.0,
                "posterior_samples": tansho_samples,
                "N": 10000,
            },
            ("idm", "芝", "21.0-25.0", "fukusho"): {
                "posterior_mean": 105.0,
                "posterior_samples": fukusho_samples,
                "N": 10000,
            },
        }

        score_tansho = compute_horse_score(
            horse_factors, edge_table, bet_type="tansho"
        )
        score_fukusho = compute_horse_score(
            horse_factors, edge_table, bet_type="fukusho"
        )

        # 両方正のスコア
        assert score_tansho > 0.0
        assert score_fukusho > 0.0

        # 複勝（105%）の方がスコアが高い
        assert score_fukusho > score_tansho

        # 独立であること（異なる値）
        assert score_tansho != score_fukusho


# ============================================================
# Test 12: 全体回収率79.92%に対するLog-EVスコアが負
# ============================================================
class TestGlobalRoiNegative:
    def test_global_roi_79_92_is_negative(self):
        """全体回収率79.92%に対するLog-EVスコアが負（79.92/80 < 1）"""
        global_rate_ratio = 79.92 / 100.0  # 0.7992
        score = log_ev_score(global_rate_ratio)
        assert score < 0.0
        # 具体的な値: ln(0.7992/0.80) = ln(0.999) ≈ -0.001
        expected = np.log(0.7992 / 0.80)
        assert score == pytest.approx(expected, abs=1e-6)


# ============================================================
# generate_posterior_samples のテスト
# ============================================================
class TestGeneratePosteriorSamples:
    def test_mean_close_to_estimate(self):
        """生成されたサンプルの平均がベイズ推定値に近い"""
        samples = generate_posterior_samples(
            bayes_estimated_rate=85.0,
            ci_lower=83.0,
            ci_upper=87.0,
            n_samples=10000,
            rng=np.random.default_rng(42),
        )
        assert np.mean(samples) == pytest.approx(0.85, abs=0.01)

    def test_ci_width_determines_spread(self):
        """CI幅が広いほどサンプルの分散が大きい"""
        narrow = generate_posterior_samples(
            85.0, 84.0, 86.0, 10000, np.random.default_rng(42)
        )
        wide = generate_posterior_samples(
            85.0, 75.0, 95.0, 10000, np.random.default_rng(42)
        )
        assert np.std(narrow) < np.std(wide)

    def test_zero_ci_width_returns_constant(self):
        """CI幅ゼロ → 全サンプル同一値"""
        samples = generate_posterior_samples(85.0, 85.0, 85.0, 100)
        assert np.all(samples == pytest.approx(0.85, abs=1e-10))


# ============================================================
# filter_edge_table / summarize_edge_table のテスト
# ============================================================
def _make_cell_pct(posterior_mean_pct: float, lcb_pass: bool, n: int = 500) -> dict:
    """テスト用 edge_table セルを生成するヘルパー（posterior_mean は %スケール）"""
    rng = np.random.default_rng(42)
    mean_ratio = posterior_mean_pct / 100.0
    if lcb_pass:
        samples = rng.normal(mean_ratio, 0.01, 2000)
    else:
        samples = rng.normal(mean_ratio, 0.20, 2000)
    return {
        "posterior_mean": posterior_mean_pct,
        "posterior_samples": samples,
        "N": n,
    }


class TestFilterAndSummarize:
    """filter_edge_table と summarize_edge_table のテスト"""

    def test_filter_removes_lcb_fail(self):
        """LCB不合格セルがフィルタリングで除外される"""
        edge_table = {
            ("f1", "s1", "b1", "tansho"): _make_cell_pct(90.0, lcb_pass=True),
            ("f2", "s1", "b2", "tansho"): _make_cell_pct(85.0, lcb_pass=False),
            ("f3", "s1", "b3", "tansho"): _make_cell_pct(100.0, lcb_pass=True),
        }
        filtered = filter_edge_table(edge_table)
        assert len(filtered) == 2
        assert ("f1", "s1", "b1", "tansho") in filtered
        assert ("f3", "s1", "b3", "tansho") in filtered
        assert ("f2", "s1", "b2", "tansho") not in filtered

    def test_summarize_lcb_pass_ratio(self):
        """summarize_edge_table が LCB通過比率を正しく返す"""
        edge_table = {
            ("f1", "s1", "b1", "tansho"): _make_cell_pct(90.0, lcb_pass=True),
            ("f2", "s1", "b2", "tansho"): _make_cell_pct(85.0, lcb_pass=False),
        }
        summary = summarize_edge_table(edge_table)
        assert summary["total_cells"] == 2
        assert summary["lcb_pass_cells"] == 1
        assert summary["lcb_pass_ratio"] == pytest.approx(0.5, abs=1e-6)

    def test_summarize_empty_table(self):
        """空テーブルでゼロ除算しない"""
        summary = summarize_edge_table({})
        assert summary["total_cells"] == 0
        assert summary["lcb_pass_ratio"] == 0.0
