"""
Phase 2 交互作用分析エンジンのユニットテスト

テスト対象:
    - assign_course_category_fast()
    - assign_surface()
    - run_interaction_analysis()
    - InteractionResult プロパティ
"""
import pytest
import pandas as pd
import numpy as np

from roi_pipeline.engine.interaction_analysis import (
    assign_course_category_fast,
    assign_surface,
    run_interaction_analysis,
    InteractionResult,
    InteractionCell,
)
from roi_pipeline.engine.hierarchical_bayes import BayesEstimate


class TestAssignCourseCategory:
    """コースカテゴリ付与のテスト"""

    @pytest.fixture
    def sample_race_data(self) -> pd.DataFrame:
        """テスト用レースデータ"""
        return pd.DataFrame({
            "keibajo_code": ["06", "05", "09", "04", "01"],
            "track_code": ["11", "11", "11", "11", "11"],  # 全て芝
            "ra_kyori": [1200, 1600, 1200, 1000, 1200],
            "bac_kyori": [1200, 1600, 1200, 1000, 1200],
        })

    def test_returns_series(self, sample_race_data: pd.DataFrame) -> None:
        """Seriesが返ること"""
        result = assign_course_category_fast(sample_race_data)
        assert isinstance(result, pd.Series)
        assert len(result) == len(sample_race_data)

    def test_nakayama_1200_turf(self, sample_race_data: pd.DataFrame) -> None:
        """中山芝1200 → 右回り急坂U字_芝"""
        result = assign_course_category_fast(sample_race_data)
        assert result.iloc[0] == "右回り急坂U字_芝"

    def test_tokyo_1600_turf(self, sample_race_data: pd.DataFrame) -> None:
        """東京芝1600 → 東京U字_芝"""
        result = assign_course_category_fast(sample_race_data)
        assert result.iloc[1] == "東京U字_芝"

    def test_hanshin_1200_turf(self, sample_race_data: pd.DataFrame) -> None:
        """阪神芝1200 → 右回り急坂U字_芝"""
        result = assign_course_category_fast(sample_race_data)
        assert result.iloc[2] == "右回り急坂U字_芝"

    def test_niigata_1000_turf(self, sample_race_data: pd.DataFrame) -> None:
        """新潟芝1000 → 直線_芝"""
        result = assign_course_category_fast(sample_race_data)
        assert result.iloc[3] == "直線_芝"

    def test_sapporo_1200_turf(self, sample_race_data: pd.DataFrame) -> None:
        """札幌芝1200 → 北海道U字_芝"""
        result = assign_course_category_fast(sample_race_data)
        assert result.iloc[4] == "北海道U字_芝"

    def test_dirt_course(self) -> None:
        """ダートコースの分類"""
        df = pd.DataFrame({
            "keibajo_code": ["05"],
            "track_code": ["22"],  # ダート
            "ra_kyori": [1600],
            "bac_kyori": [1600],
        })
        result = assign_course_category_fast(df)
        assert result.iloc[0] == "東京U字_ダ"

    def test_unknown_combo(self) -> None:
        """定義外の組み合わせ → unknown"""
        df = pd.DataFrame({
            "keibajo_code": ["99"],
            "track_code": ["11"],
            "ra_kyori": [9999],
            "bac_kyori": [9999],
        })
        result = assign_course_category_fast(df)
        assert result.iloc[0] == "unknown"


class TestAssignSurface:
    """芝/ダート付与のテスト"""

    def test_turf(self) -> None:
        """track_code=1x → 芝"""
        df = pd.DataFrame({"track_code": ["11", "12", "10"]})
        result = assign_surface(df)
        assert (result == "芝").all()

    def test_dirt(self) -> None:
        """track_code=2x → ダート"""
        df = pd.DataFrame({"track_code": ["21", "22", "23"]})
        result = assign_surface(df)
        assert (result == "ダート").all()

    def test_unknown(self) -> None:
        """track_code不明 → 不明"""
        df = pd.DataFrame({"track_code": ["", "99"]})
        result = assign_surface(df)
        assert (result == "不明").all()


class TestRunInteractionAnalysis:
    """交互作用分析のテスト"""

    @pytest.fixture
    def interaction_data(self) -> pd.DataFrame:
        """交互作用分析用のテストデータ"""
        np.random.seed(42)
        n = 3000
        return pd.DataFrame({
            "factor_bin": np.random.choice(["A", "B", "C"], size=n),
            "segment": np.random.choice(["seg1", "seg2"], size=n),
            "tansho_odds": np.random.uniform(2.0, 30.0, size=n).round(1),
            "is_hit": np.random.choice([0, 1], size=n, p=[0.92, 0.08]),
            "race_year": np.random.choice(["2020", "2021", "2022", "2023"], size=n),
        })

    def test_returns_interaction_result(self, interaction_data: pd.DataFrame) -> None:
        """InteractionResultが返ること"""
        result = run_interaction_analysis(
            df=interaction_data,
            factor_col="factor_bin",
            segment_col="segment",
            global_rate=79.0,
        )
        assert isinstance(result, InteractionResult)

    def test_cells_not_empty(self, interaction_data: pd.DataFrame) -> None:
        """セルが生成されること"""
        result = run_interaction_analysis(
            df=interaction_data,
            factor_col="factor_bin",
            segment_col="segment",
            global_rate=79.0,
        )
        # 3 factors x 2 segments = 6 cells (if all have enough samples)
        assert len(result.cells) > 0
        assert len(result.cells) <= 6

    def test_factor_rates_populated(self, interaction_data: pd.DataFrame) -> None:
        """ファクター別回収率が算出されること"""
        result = run_interaction_analysis(
            df=interaction_data,
            factor_col="factor_bin",
            segment_col="segment",
            global_rate=79.0,
        )
        assert "A" in result.factor_rates
        assert "B" in result.factor_rates
        assert "C" in result.factor_rates

    def test_segment_rates_populated(self, interaction_data: pd.DataFrame) -> None:
        """セグメント別回収率が算出されること"""
        result = run_interaction_analysis(
            df=interaction_data,
            factor_col="factor_bin",
            segment_col="segment",
            global_rate=79.0,
        )
        assert "seg1" in result.segment_rates
        assert "seg2" in result.segment_rates

    def test_cells_have_valid_bayes(self, interaction_data: pd.DataFrame) -> None:
        """各セルにベイズ推定結果が付与されること"""
        result = run_interaction_analysis(
            df=interaction_data,
            factor_col="factor_bin",
            segment_col="segment",
            global_rate=79.0,
        )
        for cell in result.cells:
            assert isinstance(cell.bayes_estimate, BayesEstimate)
            assert cell.bayes_estimate.n_samples > 0
            assert not np.isnan(cell.bayes_estimate.estimated_rate)

    def test_min_samples_filter(self, interaction_data: pd.DataFrame) -> None:
        """min_samples以下のセルが除外されること"""
        result = run_interaction_analysis(
            df=interaction_data,
            factor_col="factor_bin",
            segment_col="segment",
            global_rate=79.0,
            min_samples=10000,  # 全セルが除外されるはず
        )
        assert len(result.cells) == 0

    def test_edge_cells_subset(self, interaction_data: pd.DataFrame) -> None:
        """edge_cellsはcellsのサブセット"""
        result = run_interaction_analysis(
            df=interaction_data,
            factor_col="factor_bin",
            segment_col="segment",
            global_rate=79.0,
        )
        assert result.n_edge_cells <= len(result.cells)

    def test_three_level_bayes_applied(self, interaction_data: pd.DataFrame) -> None:
        """3層ベイズ推定が適用されること（推定値が観測値と異なる）"""
        result = run_interaction_analysis(
            df=interaction_data,
            factor_col="factor_bin",
            segment_col="segment",
            global_rate=79.0,
        )
        # 少なくとも1セルでobserved_rate != estimated_rate
        # （信頼性重み < 1.0 の場合、事前分布に引き寄せられる）
        differences = [
            abs(c.bayes_estimate.observed_rate - c.bayes_estimate.estimated_rate)
            for c in result.cells
        ]
        # 全てのセルで完全一致はありえない（C=50 かつ N < inf）
        assert any(d > 0.001 for d in differences)

    def test_interaction_result_properties(self) -> None:
        """InteractionResultのプロパティテスト"""
        # ダミーセルを作成
        dummy_bayes = BayesEstimate(
            estimated_rate=85.0, reliability_weight=0.9,
            ci_lower=81.0, ci_upper=89.0, score=5.0,
            n_samples=1000, observed_rate=86.0, prior_rate=80.0,
        )
        cell1 = InteractionCell(
            factor_value="A", segment_value="seg1",
            n_samples=1000, n_hits=80, hit_rate=8.0,
            observed_rate=86.0, bayes_estimate=dummy_bayes, is_edge=True,
        )
        cell2 = InteractionCell(
            factor_value="B", segment_value="seg2",
            n_samples=500, n_hits=35, hit_rate=7.0,
            observed_rate=75.0,
            bayes_estimate=BayesEstimate(
                estimated_rate=77.0, reliability_weight=0.9,
                ci_lower=72.0, ci_upper=82.0, score=-3.0,
                n_samples=500, observed_rate=75.0, prior_rate=80.0,
            ),
            is_edge=False,
        )

        result = InteractionResult(
            factor_name="test", segment_name="test",
            global_rate=79.0, factor_rates={"A": 86.0, "B": 75.0},
            segment_rates={"seg1": 82.0, "seg2": 76.0},
            cells=[cell1, cell2],
            factor_n={"A": 1000, "B": 500},
            segment_n={"seg1": 800, "seg2": 700},
        )

        assert result.n_edge_cells == 1
        assert result.edge_factor_values == {"A"}
        assert result.edge_segment_values == {"seg1"}
        assert len(result.edge_cells) == 1
        assert result.edge_cells[0].factor_value == "A"
