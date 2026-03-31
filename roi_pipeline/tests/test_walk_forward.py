"""
Walk-Forward検証フレームワークのユニットテスト

テスト対象:
    - generate_monthly_periods()
    - run_walk_forward()
    - validate_no_leak()
    - WalkForwardConfig
    - MonthlyResult
"""
import pytest
import pandas as pd
import numpy as np

from roi_pipeline.engine.walk_forward import (
    generate_monthly_periods,
    run_walk_forward,
    validate_no_leak,
    WalkForwardConfig,
    MonthlyResult,
)


class TestGenerateMonthlyPeriods:
    """月次ペリオド生成のテスト"""

    def test_first_period(self) -> None:
        """最初のテスト月は2019-01であること"""
        config = WalkForwardConfig()
        periods = generate_monthly_periods(config)
        assert periods[0]["test_year_month"] == "2019-01"

    def test_first_train_end_is_december_2018(self) -> None:
        """最初のtrain_endは2018年12月末であること"""
        config = WalkForwardConfig()
        periods = generate_monthly_periods(config)
        assert periods[0]["train_end"] == "20181231"

    def test_expanding_train_start_is_fixed(self) -> None:
        """拡張型の場合、train_startは常に固定"""
        config = WalkForwardConfig(expanding=True)
        periods = generate_monthly_periods(config)
        for p in periods:
            assert p["train_start"] == "20161101"

    def test_train_end_before_test_start(self) -> None:
        """全ペリオドでtrain_end < test_start（リーク防止の核心）"""
        config = WalkForwardConfig()
        periods = generate_monthly_periods(config)
        for p in periods:
            assert p["train_end"] < p["test_start"], (
                f"LEAK: train_end={p['train_end']} >= test_start={p['test_start']}"
            )

    def test_total_periods_count(self) -> None:
        """テスト月の総数: 2019-01 〜 2025-12 = 84ヶ月"""
        config = WalkForwardConfig()
        periods = generate_monthly_periods(config)
        assert len(periods) == 84

    def test_last_period(self) -> None:
        """最後のテスト月は2025-12であること"""
        config = WalkForwardConfig()
        periods = generate_monthly_periods(config)
        assert periods[-1]["test_year_month"] == "2025-12"

    def test_consecutive_months(self) -> None:
        """テスト月が連続していること"""
        config = WalkForwardConfig()
        periods = generate_monthly_periods(config)
        for i in range(1, len(periods)):
            prev_month = pd.Timestamp(periods[i-1]["test_start"])
            curr_month = pd.Timestamp(periods[i]["test_start"])
            expected_next = prev_month + pd.offsets.MonthBegin(1)
            assert curr_month == expected_next, (
                f"Gap at index {i}: {prev_month} -> {curr_month}"
            )


class TestRunWalkForward:
    """Walk-Forward検証実行のテスト"""

    @pytest.fixture
    def sample_wf_data(self) -> pd.DataFrame:
        """Walk-Forward用のサンプルデータ（2019-01〜2019-06の6ヶ月分）"""
        np.random.seed(42)
        dates = []
        odds_list = []
        hits = []
        years = []
        race_ids = []

        for month in range(1, 7):
            for day in [5, 10, 15, 20, 25]:
                for horse in range(1, 13):
                    date_str = f"2019{month:02d}{day:02d}"
                    dates.append(date_str)
                    odds_list.append(round(np.random.uniform(1.5, 50.0), 1))
                    hits.append(1 if np.random.random() < 0.08 else 0)
                    years.append("2019")
                    race_ids.append(f"R{month:02d}{day:02d}01")

        return pd.DataFrame({
            "race_date": dates,
            "tansho_odds": odds_list,
            "is_hit": hits,
            "race_year": years,
            "race_id": race_ids,
        })

    def test_returns_monthly_results(self, sample_wf_data: pd.DataFrame) -> None:
        """MonthlyResultのリストが返ること"""
        config = WalkForwardConfig(
            initial_train_end="20181231",
            test_end="20190630",
        )
        results = run_walk_forward(sample_wf_data, config=config)
        assert len(results) > 0
        assert all(isinstance(r, MonthlyResult) for r in results)

    def test_monthly_result_fields(self, sample_wf_data: pd.DataFrame) -> None:
        """MonthlyResultの全フィールドが埋まっていること"""
        config = WalkForwardConfig(
            initial_train_end="20181231",
            test_end="20190630",
        )
        results = run_walk_forward(sample_wf_data, config=config)
        for r in results:
            assert r.year_month is not None
            assert r.n_horses >= 0
            assert r.n_hits >= 0
            assert r.n_hits <= r.n_horses
            assert isinstance(r.monthly_return_rate, float)
            assert isinstance(r.cumulative_return_rate, float)

    def test_cumulative_rate_stability(self, sample_wf_data: pd.DataFrame) -> None:
        """累積回収率が計算されること（NaNやInfでない）"""
        config = WalkForwardConfig(
            initial_train_end="20181231",
            test_end="20190630",
        )
        results = run_walk_forward(sample_wf_data, config=config)
        for r in results:
            assert not np.isnan(r.cumulative_return_rate)
            assert not np.isinf(r.cumulative_return_rate)

    def test_score_is_cumulative_minus_baseline(self, sample_wf_data: pd.DataFrame) -> None:
        """score = cumulative_return_rate - 80.0"""
        config = WalkForwardConfig(
            initial_train_end="20181231",
            test_end="20190630",
        )
        results = run_walk_forward(sample_wf_data, config=config)
        for r in results:
            assert abs(r.score - (r.cumulative_return_rate - 80.0)) < 0.1

    def test_empty_data_returns_empty(self) -> None:
        """データが空の場合、空リストを返す"""
        df = pd.DataFrame(columns=[
            "race_date", "tansho_odds", "is_hit", "race_year", "race_id"
        ])
        results = run_walk_forward(df)
        assert results == []


class TestValidateNoLeak:
    """ターゲットリーク検証のテスト"""

    def test_no_leak_passes(self) -> None:
        """リークがない場合はTrueを返す"""
        df = pd.DataFrame({
            "race_date": ["20200101", "20200301", "20200531"],
        })
        assert validate_no_leak(df, train_end="20200531") is True

    def test_leak_detected_raises(self) -> None:
        """リークがある場合はAssertionErrorを送出する"""
        df = pd.DataFrame({
            "race_date": ["20200101", "20200601", "20200715"],
        })
        with pytest.raises(AssertionError, match="TARGET LEAK DETECTED"):
            validate_no_leak(df, train_end="20200531")

    def test_boundary_exact_date_passes(self) -> None:
        """train_endと同じ日付はリークではない"""
        df = pd.DataFrame({
            "race_date": ["20200531"],
        })
        assert validate_no_leak(df, train_end="20200531") is True

    def test_one_day_after_is_leak(self) -> None:
        """train_endの翌日はリーク"""
        df = pd.DataFrame({
            "race_date": ["20200601"],
        })
        with pytest.raises(AssertionError, match="TARGET LEAK DETECTED"):
            validate_no_leak(df, train_end="20200531")
