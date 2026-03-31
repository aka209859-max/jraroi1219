"""
ターゲットリーク検出テスト

指示書§7.2に基づく専用テスト:
    テスト対象月を2020年6月に固定し、
    学習データの最大日付が2020年5月31日以前であることを検証する。

    Walk-Forward全ペリオドにおいて、
    train_end < test_start が成立することを証明する。
"""
import pytest
import pandas as pd

from roi_pipeline.engine.walk_forward import (
    generate_monthly_periods,
    validate_no_leak,
    WalkForwardConfig,
)


class TestNoTargetLeak:
    """ターゲットリーク完全排除テスト"""

    def test_fixed_month_june_2020(self) -> None:
        """
        テスト対象月を2020年6月に固定。
        学習データの最大日付が2020年5月31日以前であることを確認。
        """
        config = WalkForwardConfig()
        periods = generate_monthly_periods(config)

        # 2020-06のペリオドを取得
        june_2020 = [p for p in periods if p["test_year_month"] == "2020-06"]
        assert len(june_2020) == 1, "2020-06のペリオドが存在しない"

        period = june_2020[0]

        # 学習終了日が2020年5月31日であること
        assert period["train_end"] == "20200531", (
            f"train_end should be 20200531, got {period['train_end']}"
        )

        # テスト開始日が2020年6月1日であること
        assert period["test_start"] == "20200601", (
            f"test_start should be 20200601, got {period['test_start']}"
        )

        # train_end < test_start
        assert period["train_end"] < period["test_start"]

    def test_no_leak_in_simulated_train_data(self) -> None:
        """
        模擬学習データを作成し、validate_no_leakが正しく検出することを確認。
        """
        # 正常ケース: 学習データは2020-05-31以前
        train_data_ok = pd.DataFrame({
            "race_date": ["20200101", "20200228", "20200430", "20200531"],
        })
        assert validate_no_leak(train_data_ok, train_end="20200531") is True

        # リークケース: 2020-06-01のデータが混入
        train_data_leak = pd.DataFrame({
            "race_date": ["20200101", "20200228", "20200601"],
        })
        with pytest.raises(AssertionError, match="TARGET LEAK DETECTED"):
            validate_no_leak(train_data_leak, train_end="20200531")

    def test_all_periods_no_overlap(self) -> None:
        """
        全84ペリオドにおいて、train_end < test_start が成立すること。
        これが破れると、未来データが学習に混入する。
        """
        config = WalkForwardConfig()
        periods = generate_monthly_periods(config)

        for i, period in enumerate(periods):
            assert period["train_end"] < period["test_start"], (
                f"CRITICAL LEAK at period {i} ({period['test_year_month']}): "
                f"train_end={period['train_end']} >= test_start={period['test_start']}"
            )

    def test_train_start_never_after_train_end(self) -> None:
        """全ペリオドでtrain_start <= train_endであること"""
        config = WalkForwardConfig()
        periods = generate_monthly_periods(config)

        for period in periods:
            assert period["train_start"] <= period["train_end"], (
                f"Invalid period: train_start={period['train_start']} > "
                f"train_end={period['train_end']}"
            )

    def test_test_start_equals_first_of_month(self) -> None:
        """全ペリオドのtest_startが月初日であること"""
        config = WalkForwardConfig()
        periods = generate_monthly_periods(config)

        for period in periods:
            day = period["test_start"][-2:]
            assert day == "01", (
                f"test_start should be first of month, got day={day} "
                f"for {period['test_year_month']}"
            )

    def test_train_end_equals_last_of_previous_month(self) -> None:
        """全ペリオドのtrain_endが前月末日であること"""
        config = WalkForwardConfig()
        periods = generate_monthly_periods(config)

        for period in periods:
            test_start = pd.Timestamp(period["test_start"])
            expected_train_end = test_start - pd.Timedelta(days=1)
            assert period["train_end"] == expected_train_end.strftime("%Y%m%d"), (
                f"train_end mismatch for {period['test_year_month']}: "
                f"expected {expected_train_end.strftime('%Y%m%d')}, "
                f"got {period['train_end']}"
            )

    def test_expanding_window_monotonically_grows(self) -> None:
        """拡張型の場合、学習期間が単調増加すること"""
        config = WalkForwardConfig(expanding=True)
        periods = generate_monthly_periods(config)

        for i in range(1, len(periods)):
            prev_end = periods[i-1]["train_end"]
            curr_end = periods[i]["train_end"]
            assert curr_end > prev_end, (
                f"Train window not expanding at period {i}: "
                f"prev_end={prev_end}, curr_end={curr_end}"
            )
