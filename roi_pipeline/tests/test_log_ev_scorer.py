"""
test_log_ev_scorer.py

Phase 3 Task 1: Log-EV得点化エンジン + LCBゲートのテスト

テスト要件（仕様書より12件）:
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
    compute_horse_score,
    filter_edge_table,
    lcb_gate,
    log_ev_score,
    summarize_edge_table,
)


# ─────────────────────────────────────────────
# log_ev_score のテスト
# ─────────────────────────────────────────────

class TestLogEvScore:
    """log_ev_score 関数のテスト"""

    def test_baseline_returns_zero(self):
        """テスト1: 80% → 0.0（ゼロライン）"""
        result = log_ev_score(0.80)
        assert result == pytest.approx(0.0, abs=1e-10)

    def test_100pct_returns_approx_0223(self):
        """テスト2: 100% → ln(1.00/0.80) ≈ +0.223"""
        result = log_ev_score(1.00)
        expected = np.log(1.00 / 0.80)
        assert result == pytest.approx(expected, rel=1e-6)
        assert result == pytest.approx(0.22314355, rel=1e-5)

    def test_60pct_returns_approx_neg_0288(self):
        """テスト3: 60% → ln(0.60/0.80) ≈ -0.288"""
        result = log_ev_score(0.60)
        expected = np.log(0.60 / 0.80)
        assert result == pytest.approx(expected, rel=1e-6)
        assert result == pytest.approx(-0.28768207, rel=1e-5)

    def test_zero_returns_floor(self):
        """テスト4: 0% → -10.0（下限クリップ）"""
        result = log_ev_score(0.0)
        assert result == -10.0

    def test_negative_value_returns_floor(self):
        """負の値も -10.0 にクリップされる"""
        result = log_ev_score(-0.5)
        assert result == -10.0

    def test_120pct_positive(self):
        """120% → 強いエッジ（正のスコア）"""
        result = log_ev_score(1.20)
        expected = np.log(1.20 / 0.80)
        assert result == pytest.approx(expected, rel=1e-6)
        assert result > 0.0

    def test_global_roi_7992_is_negative(self):
        """テスト12: グローバル補正回収率 79.92% のスコアが負"""
        result = log_ev_score(0.7992)
        assert result < 0.0, "79.92% < 80% なので負のスコアになるはず"

    def test_custom_baseline(self):
        """カスタム baseline が機能する"""
        result = log_ev_score(0.90, baseline=0.90)
        assert result == pytest.approx(0.0, abs=1e-10)


# ─────────────────────────────────────────────
# lcb_gate のテスト
# ─────────────────────────────────────────────

class TestLcbGate:
    """lcb_gate 関数のテスト"""

    def test_strong_edge_passes(self):
        """テスト5: 事後分布が全て 85% → LCB(10%) >> 80% → True"""
        samples = np.full(1000, 0.85)
        assert lcb_gate(samples) is True

    def test_clear_positive_passes(self):
        """事後分布の 90%超が 80% 超 → True"""
        rng = np.random.default_rng(42)
        # 平均85%, 小分散 → 下方10%分位点も 80% を超える
        samples = rng.normal(0.85, 0.01, 2000)
        assert lcb_gate(samples) is True

    def test_weak_edge_fails(self):
        """テスト6: 事後分布が広い（少標本） → LCB(10%) < 80% → False"""
        rng = np.random.default_rng(42)
        # 平均81%だが分散が大きい → 下方10%分位点が 80% を下回る
        samples = rng.normal(0.81, 0.10, 2000)
        lcb = np.quantile(samples, 0.10)
        # lcbが 0.80 以下であることを確認してからテスト
        assert lcb <= 0.80
        assert lcb_gate(samples) is False

    def test_exactly_at_baseline_fails(self):
        """全サンプルがちょうど baseline → LCB = baseline → False（超えない）"""
        samples = np.full(1000, 0.80)
        assert lcb_gate(samples) is False

    def test_just_below_baseline_fails(self):
        """全サンプルが baseline より微小に低い → False"""
        samples = np.full(1000, 0.799)
        assert lcb_gate(samples) is False

    def test_custom_quantile(self):
        """カスタム quantile が機能する"""
        rng = np.random.default_rng(0)
        samples = rng.normal(0.85, 0.03, 2000)
        # quantile=0.05 でも通過するほど強いエッジ
        assert lcb_gate(samples, quantile=0.05) is True


# ─────────────────────────────────────────────
# compute_horse_score のテスト
# ─────────────────────────────────────────────

def _make_cell(posterior_mean: float, lcb_pass: bool, n: int = 500) -> dict:
    """テスト用 edge_table セルを生成するヘルパー"""
    rng = np.random.default_rng(42)
    if lcb_pass:
        # LCBゲートを通過するサンプル（下方10%分位点 > 0.80）
        samples = rng.normal(posterior_mean, 0.01, 2000)
    else:
        # LCBゲートを通過しないサンプル（広い分布、下方10%分位点 <= 0.80）
        samples = rng.normal(posterior_mean, 0.15, 2000)
        # 強制的にLCBが 0.80 以下になるよう調整
        q10 = np.quantile(samples, 0.10)
        if q10 > 0.80:
            # 分散を更に広げる
            samples = rng.normal(0.81, 0.20, 2000)
    return {
        "posterior_mean": posterior_mean,
        "posterior_samples": samples,
        "N": n,
    }


class TestComputeHorseScore:
    """compute_horse_score 関数のテスト"""

    def test_no_factors_returns_zero(self):
        """テスト7: ファクターなし → 0.0"""
        score = compute_horse_score({}, {})
        assert score == 0.0

    def test_factor_not_in_edge_table_returns_zero(self):
        """edge_tableに存在しないファクター → 0.0"""
        horse_factors = {"idm": ("turf", 5)}
        score = compute_horse_score(horse_factors, {})
        assert score == 0.0

    def test_single_factor_with_edge_returns_positive(self):
        """テスト8: 単一ファクター、エッジあり（LCB通過）→ 正のスコア"""
        horse_factors = {"idm": ("turf", 5)}
        edge_table = {
            ("idm", "turf", 5, "tansho"): _make_cell(0.90, lcb_pass=True),
        }
        score = compute_horse_score(horse_factors, edge_table, bet_type="tansho")
        assert score > 0.0

    def test_single_factor_lcb_fail_returns_zero(self):
        """テスト9: 単一ファクター、LCB不合格 → 0.0"""
        horse_factors = {"idm": ("turf", 5)}
        edge_table = {
            ("idm", "turf", 5, "tansho"): _make_cell(0.81, lcb_pass=False),
        }
        score = compute_horse_score(horse_factors, edge_table, bet_type="tansho")
        assert score == 0.0

    def test_multiple_factors_equal_weight(self):
        """テスト10: 複数ファクター → 均等重み平均"""
        horse_factors = {
            "idm": ("turf", 5),
            "kishu_shisu": ("turf", 4),
        }
        cell1 = _make_cell(0.90, lcb_pass=True)  # log_ev ≈ 0.118
        cell2 = _make_cell(1.00, lcb_pass=True)  # log_ev ≈ 0.223
        edge_table = {
            ("idm", "turf", 5, "tansho"): cell1,
            ("kishu_shisu", "turf", 4, "tansho"): cell2,
        }
        score = compute_horse_score(horse_factors, edge_table, bet_type="tansho")

        # 均等重み平均: (log_ev(0.90) + log_ev(1.00)) / 2
        expected = (log_ev_score(0.90) + log_ev_score(1.00)) / 2
        assert score == pytest.approx(expected, rel=1e-6)

    def test_tansho_and_fukusho_independent(self):
        """テスト11: 単勝と複勝で独立にスコアが算出される"""
        horse_factors = {"idm": ("turf", 5)}
        edge_table = {
            ("idm", "turf", 5, "tansho"): _make_cell(0.90, lcb_pass=True),
            ("idm", "turf", 5, "fukusho"): _make_cell(1.10, lcb_pass=True),
        }
        score_tansho = compute_horse_score(horse_factors, edge_table, bet_type="tansho")
        score_fukusho = compute_horse_score(horse_factors, edge_table, bet_type="fukusho")

        assert score_tansho != score_fukusho
        assert score_tansho == pytest.approx(log_ev_score(0.90), rel=1e-6)
        assert score_fukusho == pytest.approx(log_ev_score(1.10), rel=1e-6)

    def test_mixed_lcb_pass_fail(self):
        """LCB通過と不通過が混在 → 通過したもののみで均等重み平均"""
        horse_factors = {
            "idm": ("turf", 5),
            "kishu_shisu": ("turf", 4),
            "pace_shisu": ("turf", 3),
        }
        edge_table = {
            ("idm", "turf", 5, "tansho"): _make_cell(0.90, lcb_pass=True),
            ("kishu_shisu", "turf", 4, "tansho"): _make_cell(0.81, lcb_pass=False),
            ("pace_shisu", "turf", 3, "tansho"): _make_cell(1.00, lcb_pass=True),
        }
        score = compute_horse_score(horse_factors, edge_table, bet_type="tansho")

        # 通過した2セルの均等平均
        expected = (log_ev_score(0.90) + log_ev_score(1.00)) / 2
        assert score == pytest.approx(expected, rel=1e-6)


# ─────────────────────────────────────────────
# filter_edge_table / summarize_edge_table のテスト
# ─────────────────────────────────────────────

class TestFilterAndSummarize:
    """filter_edge_table と summarize_edge_table のテスト"""

    def test_filter_removes_lcb_fail(self):
        """LCB不合格セルがフィルタリングで除外される"""
        edge_table = {
            ("f1", "s1", 1, "tansho"): _make_cell(0.90, lcb_pass=True),
            ("f2", "s1", 2, "tansho"): _make_cell(0.81, lcb_pass=False),
            ("f3", "s1", 3, "tansho"): _make_cell(1.00, lcb_pass=True),
        }
        filtered = filter_edge_table(edge_table)
        assert len(filtered) == 2
        assert ("f1", "s1", 1, "tansho") in filtered
        assert ("f3", "s1", 3, "tansho") in filtered
        assert ("f2", "s1", 2, "tansho") not in filtered

    def test_summarize_lcb_pass_ratio(self):
        """summarize_edge_table が LCB通過比率を正しく返す"""
        edge_table = {
            ("f1", "s1", 1, "tansho"): _make_cell(0.90, lcb_pass=True),
            ("f2", "s1", 2, "tansho"): _make_cell(0.81, lcb_pass=False),
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
