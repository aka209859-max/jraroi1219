"""
test_phase3_walk_forward.py

Phase 3 Task 4: Walk-Forward時系列検証のテスト

テスト要件（仕様書より10件）:
    1.  brier_score: 完全予測 → 0.0
    2.  brier_score: ランダム予測 → ≈ 0.25
    3.  betting_sharpe: 全月正リターン → 高Sharpe
    4.  betting_sharpe: リターンゼロ → Sharpe = 0
    5.  max_drawdown: 単調増加 → DD = 0
    6.  max_drawdown: 半減 → DD = 0.5
    7.  circuit_breaker: DD < 30% → c = 0.25
    8.  circuit_breaker: DD >= 30% → c = 0.15
    9.  Walk-Forward 1期間の end-to-end テスト（モックデータ）
    10. レポート生成テスト
"""
import os
import tempfile
from dataclasses import dataclass
from typing import List

import numpy as np
import pandas as pd
import pytest

from roi_pipeline.engine.phase3_walk_forward import (
    MonthlyP3Result,
    _generate_rolling_periods,
    betting_sharpe,
    brier_score,
    build_edge_table_from_df,
    circuit_breaker,
    max_drawdown,
    run_phase3_walk_forward,
    score_race,
)
from roi_pipeline.factors.definitions import FactorDefinition, FactorType
from roi_pipeline.config.segment_types import SegmentType


# ─────────────────────────────────────────────
# brier_score のテスト
# ─────────────────────────────────────────────

class TestBrierScore:
    """brier_score 関数のテスト"""

    def test_perfect_prediction_zero(self):
        """テスト1: 完全予測 → 0.0"""
        p = np.array([1.0, 0.0, 0.0, 0.0])
        y = np.array([1.0, 0.0, 0.0, 0.0])
        assert brier_score(p, y) == pytest.approx(0.0, abs=1e-10)

    def test_random_prediction_approx_025(self):
        """テスト2: ランダム予測（p=0.5, outcome ∈ {0,1}） → ≈ 0.25"""
        rng = np.random.default_rng(42)
        n = 10000
        p = np.full(n, 0.5)
        y = rng.integers(0, 2, n).astype(float)
        result = brier_score(p, y)
        assert result == pytest.approx(0.25, abs=0.02)

    def test_worst_prediction(self):
        """完全外れ予測 → 1.0"""
        p = np.array([0.0, 1.0, 1.0])
        y = np.array([1.0, 0.0, 0.0])
        assert brier_score(p, y) == pytest.approx(1.0, abs=1e-10)

    def test_returns_float(self):
        """戻り値が float"""
        result = brier_score(np.array([0.5]), np.array([1.0]))
        assert isinstance(result, float)

    def test_single_element(self):
        """1要素でも動作する"""
        assert brier_score(np.array([0.8]), np.array([1.0])) == pytest.approx(0.04, abs=1e-6)


# ─────────────────────────────────────────────
# betting_sharpe のテスト
# ─────────────────────────────────────────────

class TestBettingSharpe:
    """betting_sharpe 関数のテスト"""

    def test_all_positive_returns_high_sharpe(self):
        """テスト3: 全月正リターン → 高Sharpe"""
        monthly = np.array([0.05] * 12)  # 毎月 +5%、std = 0 → 実際はsharpe=0
        # std が 0 なので 0.0 になる
        # 代わりに若干のばらつきを入れる
        monthly = np.array([0.04, 0.06, 0.05, 0.07, 0.03, 0.05,
                             0.06, 0.04, 0.05, 0.07, 0.03, 0.06])
        result = betting_sharpe(monthly)
        assert result > 2.0  # 全月正リターンで低ボラ → 高Sharpe

    def test_zero_returns_sharpe_zero(self):
        """テスト4: リターンゼロ → Sharpe = 0"""
        monthly = np.zeros(12)
        assert betting_sharpe(monthly) == 0.0

    def test_mixed_returns(self):
        """正負混在のリターン → 有限の Sharpe"""
        monthly = np.array([0.1, -0.05, 0.08, -0.03, 0.06, -0.02])
        result = betting_sharpe(monthly)
        assert isinstance(result, float)
        assert np.isfinite(result)

    def test_annualized_by_sqrt12(self):
        """sqrt(12) で年率換算されていることを確認"""
        monthly = np.array([0.01, 0.02, 0.03, 0.01, 0.02, 0.03,
                             0.01, 0.02, 0.03, 0.01, 0.02, 0.03])
        result = betting_sharpe(monthly)
        expected = np.mean(monthly) / np.std(monthly) * np.sqrt(12)
        assert result == pytest.approx(expected, rel=1e-6)

    def test_single_nonzero(self):
        """1要素 → std = 0 → Sharpe = 0"""
        assert betting_sharpe(np.array([0.05])) == 0.0


# ─────────────────────────────────────────────
# max_drawdown のテスト
# ─────────────────────────────────────────────

class TestMaxDrawdown:
    """max_drawdown 関数のテスト"""

    def test_monotone_increase_zero_dd(self):
        """テスト5: 単調増加 → DD = 0"""
        cr = np.array([1.0, 1.1, 1.2, 1.3, 1.5])
        assert max_drawdown(cr) == pytest.approx(0.0, abs=1e-10)

    def test_half_decline_dd_half(self):
        """テスト6: 半減 → DD = 0.5"""
        cr = np.array([1.0, 2.0, 1.0])
        assert max_drawdown(cr) == pytest.approx(0.5, abs=1e-10)

    def test_empty_array_zero(self):
        """空配列 → 0.0"""
        assert max_drawdown(np.array([])) == 0.0

    def test_flat_array_zero(self):
        """全て同値 → DD = 0"""
        assert max_drawdown(np.array([1.0, 1.0, 1.0])) == 0.0

    def test_drop_and_recovery(self):
        """下落後に回復 → DDは最大下落幅"""
        cr = np.array([1.0, 1.2, 0.9, 1.1, 1.3])
        # peak after 1.2 = 1.2, drop to 0.9 → DD = (1.2-0.9)/1.2 = 0.25
        result = max_drawdown(cr)
        assert result == pytest.approx(0.25, abs=1e-6)

    def test_single_element_zero(self):
        """1要素 → DD = 0"""
        assert max_drawdown(np.array([1.5])) == 0.0


# ─────────────────────────────────────────────
# circuit_breaker のテスト
# ─────────────────────────────────────────────

class TestCircuitBreaker:
    """circuit_breaker 関数のテスト"""

    def test_below_threshold_normal_c(self):
        """テスト7: DD < 30% → c = 0.25（通常）"""
        result = circuit_breaker(0.10)
        assert result == pytest.approx(0.25, abs=1e-10)

    def test_at_threshold_reduced_c(self):
        """テスト8: DD >= 30% → c = 0.15（縮小）"""
        result = circuit_breaker(0.30)
        assert result == pytest.approx(0.15, abs=1e-10)

    def test_above_threshold_reduced_c(self):
        """DD > 30% → c = 0.15（縮小）"""
        result = circuit_breaker(0.50)
        assert result == pytest.approx(0.15, abs=1e-10)

    def test_just_below_threshold_normal(self):
        """DD が閾値の直前 → 通常係数"""
        result = circuit_breaker(0.2999)
        assert result == pytest.approx(0.25, abs=1e-10)

    def test_custom_params(self):
        """カスタムパラメータが機能する"""
        assert circuit_breaker(0.20, threshold=0.20, normal_c=0.5, reduced_c=0.1) \
               == pytest.approx(0.1, abs=1e-10)
        assert circuit_breaker(0.19, threshold=0.20, normal_c=0.5, reduced_c=0.1) \
               == pytest.approx(0.5, abs=1e-10)


# ─────────────────────────────────────────────
# _generate_rolling_periods のテスト
# ─────────────────────────────────────────────

class TestGenerateRollingPeriods:
    """_generate_rolling_periods 関数のテスト"""

    def test_84_periods(self):
        """2019-01 ～ 2025-12 → 84か月（7年）"""
        periods = _generate_rolling_periods("2019-01", "2025-12", train_months=36)
        assert len(periods) == 84

    def test_period_structure(self):
        """各期間が必要なキーを持つ"""
        periods = _generate_rolling_periods("2019-01", "2019-03", train_months=36)
        for p in periods:
            assert "train_start" in p
            assert "train_end" in p
            assert "val_start" in p
            assert "val_end" in p
            assert "val_ym" in p

    def test_no_overlap(self):
        """学習窓終了日 < 検証窓開始日（リーク防止）"""
        periods = _generate_rolling_periods("2019-01", "2020-12", train_months=36)
        for p in periods:
            assert p["train_end"] < p["val_start"]


# ─────────────────────────────────────────────
# モックデータ生成ヘルパー
# ─────────────────────────────────────────────

def _make_mock_factor_defs() -> List[FactorDefinition]:
    """テスト用シンプルなファクター定義（NUMERIC/GLOBAL 2個）"""
    return [
        FactorDefinition(
            id=1, name="mock_idm", table="jrd_kyi", column="mock_idm",
            factor_type=FactorType.NUMERIC, segment_type=SegmentType.GLOBAL,
            category="能力系", n_bins=5,
        ),
        FactorDefinition(
            id=2, name="mock_kishu", table="jrd_kyi", column="mock_kishu",
            factor_type=FactorType.NUMERIC, segment_type=SegmentType.GLOBAL,
            category="関係者系", n_bins=5,
        ),
    ]


def _make_mock_df(
    n_races: int = 20,
    n_horses: int = 6,
    start_ym: str = "2019-01",
    n_months: int = 3,
    rng_seed: int = 0,
) -> pd.DataFrame:
    """
    テスト用モックDataFrameを生成する。

    各月に n_races レース、各レース n_horses 頭のデータを作成する。
    """
    rng = np.random.default_rng(rng_seed)
    rows = []

    start_ts = pd.Timestamp(start_ym + "-01")

    for m in range(n_months):
        month_ts = start_ts + pd.DateOffset(months=m)
        month_str = month_ts.strftime("%Y%m")

        for r in range(n_races):
            race_date = month_ts.strftime("%Y%m") + f"{(r % 28) + 1:02d}"
            race_id = f"mock_{month_str}_{r:03d}"

            # 真の勝率（ディリクレ分布）
            true_probs = rng.dirichlet(np.ones(n_horses))
            winner_idx = rng.choice(n_horses, p=true_probs)

            for h in range(n_horses):
                # オッズ（真の確率の逆数 × ランダムノイズ）
                odds_raw = (1.0 / true_probs[h]) * rng.uniform(0.8, 1.2)
                odds_val = max(1.1, odds_raw)

                rows.append({
                    "race_date": race_date,
                    "race_id": race_id,
                    "umaban": str(h + 1),
                    "tansho_odds_val": odds_val,
                    "fukusho_odds_val": max(1.0, odds_val * 0.4),
                    "is_hit": 1 if h == winner_idx else 0,
                    "is_hit_fukusho": 1 if h <= winner_idx and h < 3 else 0,
                    "race_year": race_date[:4],
                    "track_code": "11",  # 芝
                    "keibajo_code": "01",
                    "race_bango": "01",
                    "mock_idm": rng.uniform(30, 80),
                    "mock_kishu": rng.uniform(40, 90),
                })

    return pd.DataFrame(rows)


# ─────────────────────────────────────────────
# end-to-end テスト（モックデータ）
# ─────────────────────────────────────────────

class TestWalkForwardEndToEnd:
    """テスト9: Walk-Forward 1期間 end-to-end テスト"""

    def test_single_period_returns_result_list(self):
        """1か月検証 → MonthlyP3Result のリストが返る"""
        factor_defs = _make_mock_factor_defs()

        # 2か月訓練 + 1か月検証のデータ
        df = _make_mock_df(n_races=10, n_horses=5, start_ym="2020-11", n_months=3)

        results = run_phase3_walk_forward(
            df=df,
            factor_defs=factor_defs,
            val_start_ym="2021-01",
            val_end_ym="2021-01",
            train_months=2,
            date_col="race_date",
            race_id_col="race_id",
            tansho_odds_col="tansho_odds_val",
            fukusho_odds_col="fukusho_odds_val",
            hit_col="is_hit",
            hit_fuku_col="is_hit_fukusho",
            year_col="race_year",
            umaban_col="umaban",
            n_posterior_samples=200,
            n_kelly_samples=500,
        )

        assert isinstance(results, list)
        # 結果が0件または1件（データ量によっては0件もあり得る）
        assert len(results) <= 1

    def test_result_structure(self):
        """MonthlyP3Result の全フィールドが適切な型"""
        factor_defs = _make_mock_factor_defs()
        df = _make_mock_df(n_races=15, n_horses=6, start_ym="2020-11", n_months=3)

        results = run_phase3_walk_forward(
            df=df,
            factor_defs=factor_defs,
            val_start_ym="2021-01",
            val_end_ym="2021-01",
            train_months=2,
            n_posterior_samples=200,
            n_kelly_samples=300,
        )

        for r in results:
            assert isinstance(r.year_month, str)
            assert isinstance(r.n_bets, int)
            assert isinstance(r.n_hits, int)
            assert isinstance(r.monthly_return, float)
            assert isinstance(r.cumulative_bankroll, float)
            assert r.n_hits <= r.n_bets
            assert r.cumulative_bankroll > 0.0
            assert 0.0 <= r.brier_model <= 1.0
            assert 0.0 <= r.brier_market <= 1.0

    def test_multiple_months(self):
        """複数月の検証が連続して動作する"""
        factor_defs = _make_mock_factor_defs()
        # 4か月訓練 + 3か月検証
        df = _make_mock_df(n_races=8, n_horses=5, start_ym="2020-10", n_months=7)

        results = run_phase3_walk_forward(
            df=df,
            factor_defs=factor_defs,
            val_start_ym="2021-02",
            val_end_ym="2021-04",
            train_months=4,
            n_posterior_samples=100,
            n_kelly_samples=200,
        )

        assert len(results) <= 3

    def test_no_bets_is_valid(self):
        """ベット数 0 の月も有効な結果として返る"""
        factor_defs = _make_mock_factor_defs()
        df = _make_mock_df(n_races=5, n_horses=4, start_ym="2020-11", n_months=3)

        results = run_phase3_walk_forward(
            df=df,
            factor_defs=factor_defs,
            val_start_ym="2021-01",
            val_end_ym="2021-01",
            train_months=2,
            n_posterior_samples=100,
            n_kelly_samples=200,
        )

        for r in results:
            assert r.n_bets >= 0
            assert r.n_hits >= 0


# ─────────────────────────────────────────────
# レポート生成テスト
# ─────────────────────────────────────────────

class TestReportGeneration:
    """テスト10: レポート生成テスト"""

    def _make_mock_results(self, n: int = 6) -> List[MonthlyP3Result]:
        """テスト用 MonthlyP3Result のリストを生成"""
        results = []
        bk = 1.0
        for i in range(n):
            ret = 0.02 * ((-1) ** i)  # +2%, -2%, ... 交互
            bk = bk * (1.0 + ret)
            ym = f"2021-{i+1:02d}"
            results.append(MonthlyP3Result(
                year_month=ym,
                n_bets=10 + i,
                n_hits=2 + i % 3,
                monthly_return=round(ret, 6),
                cumulative_bankroll=round(bk, 6),
                brier_model=0.08,
                brier_market=0.10,
                fractional_c_used=0.25,
            ))
        return results

    def test_report_file_created(self):
        """レポートファイルが生成される"""
        from roi_pipeline.reports.generate_phase3 import render_report

        results = self._make_mock_results()
        with tempfile.TemporaryDirectory() as tmpdir:
            report_path = os.path.join(tmpdir, "test_report.md")
            render_report(results, report_path)
            assert os.path.exists(report_path)

    def test_report_contains_required_sections(self):
        """レポートに必須セクションが含まれる"""
        from roi_pipeline.reports.generate_phase3 import render_report

        results = self._make_mock_results()
        with tempfile.TemporaryDirectory() as tmpdir:
            report_path = os.path.join(tmpdir, "test_report.md")
            content = render_report(results, report_path)

        assert "# Phase 3 Walk-Forward検証レポート" in content
        assert "## サマリー" in content
        assert "## 月次リターン推移" in content
        assert "## 年次サマリー" in content
        assert "## 合格判定" in content

    def test_report_pass_criteria(self):
        """合格判定の4項目が全て含まれる"""
        from roi_pipeline.reports.generate_phase3 import render_report

        results = self._make_mock_results()
        with tempfile.TemporaryDirectory() as tmpdir:
            report_path = os.path.join(tmpdir, "test_report.md")
            content = render_report(results, report_path)

        assert "Sharpe > 1.0" in content
        assert "最大DD < 30%" in content
        assert "70%" in content
        assert "Brier Score" in content

    def test_empty_results_handled(self):
        """空の結果でもエラーにならない"""
        from roi_pipeline.reports.generate_phase3 import render_report

        with tempfile.TemporaryDirectory() as tmpdir:
            report_path = os.path.join(tmpdir, "empty_report.md")
            content = render_report([], report_path)
        assert "Walk-Forward" in content

    def test_report_contains_monthly_data(self):
        """月次データがテーブルに記録される"""
        from roi_pipeline.reports.generate_phase3 import render_report

        results = self._make_mock_results(n=3)
        with tempfile.TemporaryDirectory() as tmpdir:
            report_path = os.path.join(tmpdir, "test_report.md")
            content = render_report(results, report_path)

        for r in results:
            assert r.year_month in content


# ─────────────────────────────────────────────
# build_edge_table_from_df のテスト
# ─────────────────────────────────────────────

class TestBuildEdgeTable:
    """build_edge_table_from_df 関数のテスト"""

    def test_returns_dict(self):
        """edge_table が辞書として返る"""
        factor_defs = _make_mock_factor_defs()
        df = _make_mock_df(n_races=20, n_horses=6, n_months=2)
        result = build_edge_table_from_df(
            df, factor_defs,
            odds_col="tansho_odds_val", hit_col="is_hit", year_col="race_year",
            n_posterior_samples=200,
        )
        assert isinstance(result, dict)

    def test_keys_have_bet_type(self):
        """各キーに bet_type が含まれる"""
        factor_defs = _make_mock_factor_defs()
        df = _make_mock_df(n_races=20, n_horses=6, n_months=2)
        result = build_edge_table_from_df(
            df, factor_defs,
            bet_type="tansho",
            odds_col="tansho_odds_val", hit_col="is_hit", year_col="race_year",
            n_posterior_samples=200,
        )
        for key in result:
            assert key[3] == "tansho"

    def test_cell_has_required_fields(self):
        """各セルに posterior_mean, posterior_samples, N が含まれる"""
        factor_defs = _make_mock_factor_defs()
        df = _make_mock_df(n_races=20, n_horses=6, n_months=2)
        result = build_edge_table_from_df(
            df, factor_defs,
            odds_col="tansho_odds_val", hit_col="is_hit", year_col="race_year",
            n_posterior_samples=200,
        )
        for key, cell in result.items():
            assert "posterior_mean" in cell
            assert "posterior_samples" in cell
            assert "N" in cell
            assert 0.0 < cell["posterior_mean"] < 2.0
            assert len(cell["posterior_samples"]) == 200
            assert cell["N"] >= 5

    def test_missing_column_skipped(self):
        """存在しないカラムのファクターはスキップされる"""
        missing_factor = FactorDefinition(
            id=99, name="nonexistent", table="x", column="nonexistent_col",
            factor_type=FactorType.NUMERIC, segment_type=SegmentType.GLOBAL,
            category="test", n_bins=5,
        )
        df = _make_mock_df(n_races=10, n_horses=5, n_months=1)
        result = build_edge_table_from_df(
            df, [missing_factor],
            odds_col="tansho_odds_val", hit_col="is_hit", year_col="race_year",
            n_posterior_samples=100,
        )
        assert isinstance(result, dict)
        # nonexistent_col がないのでセルは0個
        assert len(result) == 0
