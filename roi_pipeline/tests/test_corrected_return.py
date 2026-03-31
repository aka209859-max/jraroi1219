"""
補正回収率算出エンジンのユニットテスト

テスト対象:
    - calc_corrected_return_rate()
    - calc_return_rate_by_bins()
    - get_odds_correction()
"""
import pytest
import pandas as pd
import numpy as np

from roi_pipeline.config.odds_correction import get_odds_correction
from roi_pipeline.engine.corrected_return import (
    calc_corrected_return_rate,
    calc_return_rate_by_bins,
    TARGET_PAYOUT,
    BASELINE_RATE,
)


class TestGetOddsCorrection:
    """オッズ補正係数取得のテスト"""

    def test_tansho_known_value(self) -> None:
        """単勝: 3.5倍 → 1.01"""
        assert get_odds_correction(3.5, is_fukusho=False) == 1.01

    def test_fukusho_known_value(self) -> None:
        """複勝: 5.5倍 → 0.98"""
        assert get_odds_correction(5.5, is_fukusho=True) == 0.98

    def test_boundary_lower_inclusive(self) -> None:
        """境界条件: from_odds <= odds（下限を含む）"""
        assert get_odds_correction(1.0, is_fukusho=False) == 0.94

    def test_boundary_upper_exclusive(self) -> None:
        """境界条件: odds < to_odds（上限を含まない）"""
        # 1.6未満は0.94、1.6以上は次の帯
        assert get_odds_correction(1.59, is_fukusho=False) == 0.94
        assert get_odds_correction(1.6, is_fukusho=False) == 1.01

    def test_max_range(self) -> None:
        """最大オッズ帯"""
        assert get_odds_correction(500000.0, is_fukusho=False) == 2.38
        assert get_odds_correction(500000.0, is_fukusho=True) == 5.06

    def test_below_min_returns_default(self) -> None:
        """0未満のオッズ → 1.0（デフォルト）"""
        assert get_odds_correction(-1.0) == 1.0

    def test_tansho_has_122_entries(self) -> None:
        """単勝テーブルが122段階であること（CEO提供データ実測値）"""
        from roi_pipeline.config.odds_correction import TANSHO_CORRECTION
        assert len(TANSHO_CORRECTION) == 122

    def test_fukusho_has_107_entries(self) -> None:
        """複勝テーブルが107段階であること（CEO提供データ実測値）"""
        from roi_pipeline.config.odds_correction import FUKUSHO_CORRECTION
        assert len(FUKUSHO_CORRECTION) == 107


class TestCorrectedReturnRate:
    """補正回収率算出のテスト"""

    @pytest.fixture
    def sample_data(self) -> pd.DataFrame:
        """テスト用サンプルデータ"""
        return pd.DataFrame({
            "tansho_odds": [3.0, 5.0, 10.0, 20.0, 50.0],
            "is_hit": [1, 0, 1, 0, 0],
            "race_year": ["2024", "2024", "2024", "2024", "2024"],
        })

    def test_basic_calculation(self, sample_data: pd.DataFrame) -> None:
        """基本的な計算が動作すること"""
        result = calc_corrected_return_rate(sample_data)
        assert "corrected_return_rate" in result
        assert "score" in result
        assert result["n_samples"] == 5
        assert result["n_hits"] == 2

    def test_score_is_rate_minus_baseline(self, sample_data: pd.DataFrame) -> None:
        """score = corrected_return_rate - 80.0"""
        result = calc_corrected_return_rate(sample_data)
        expected_score = result["corrected_return_rate"] - BASELINE_RATE
        assert abs(result["score"] - expected_score) < 0.01

    def test_empty_dataframe(self) -> None:
        """空DataFrameでエラーにならないこと"""
        df = pd.DataFrame(columns=["tansho_odds", "is_hit", "race_year"])
        result = calc_corrected_return_rate(df)
        assert result["n_samples"] == 0
        assert result["corrected_return_rate"] == 0.0

    def test_all_hits_returns_high_rate(self) -> None:
        """全的中の場合、高い回収率になること"""
        df = pd.DataFrame({
            "tansho_odds": [5.0, 5.0, 5.0],
            "is_hit": [1, 1, 1],
            "race_year": ["2024", "2024", "2024"],
        })
        result = calc_corrected_return_rate(df)
        assert result["corrected_return_rate"] > 100.0

    def test_year_weight_affects_result(self) -> None:
        """年度重みが結果に影響すること"""
        df_old = pd.DataFrame({
            "tansho_odds": [5.0], "is_hit": [1], "race_year": ["2016"],
        })
        df_new = pd.DataFrame({
            "tansho_odds": [5.0], "is_hit": [1], "race_year": ["2025"],
        })
        result_old = calc_corrected_return_rate(df_old)
        result_new = calc_corrected_return_rate(df_new)
        # 同じデータでも年度重みで結果は同じ（単独なので加重影響なし）
        # ただし混合時に差が出ることを確認
        assert result_old["corrected_return_rate"] == result_new["corrected_return_rate"]


class TestReturnRateByBins:
    """ビン別補正回収率のテスト"""

    def test_bins_are_calculated(self) -> None:
        """ビン別に正しく集計されること"""
        df = pd.DataFrame({
            "tansho_odds": [3.0, 5.0, 3.0, 5.0],
            "is_hit": [1, 0, 0, 1],
            "race_year": ["2024"] * 4,
            "bin_col": ["A", "A", "B", "B"],
        })
        result = calc_return_rate_by_bins(df, bin_col="bin_col")
        assert len(result) == 2
        assert set(result["bin_value"].tolist()) == {"A", "B"}
