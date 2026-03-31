"""
Walk-Forward検証フレームワーク

基本設計:
    初期学習ウィンドウ: 2016年11月〜2018年12月
    テスト期間: 1ヶ月単位
    スライド方式: 拡張型（学習データは累積）
    最終テスト月: 2025年12月

    Step 1: 2016-11〜2018-12で算出 → 2019-01をテスト
    Step 2: 2016-11〜2019-01で算出 → 2019-02をテスト
    ...
    最終:   2016-11〜2025-11で算出 → 2025-12をテスト

ターゲットリーク厳禁:
    全ての集計系特徴量は、テスト月の前月末日以前のデータのみで算出すること。
"""
from dataclasses import dataclass, field
from typing import Callable, List, Optional

import pandas as pd
import numpy as np

from roi_pipeline.engine.corrected_return import calc_corrected_return_rate


@dataclass
class MonthlyResult:
    """月次検証結果"""
    year_month: str
    n_races: int
    n_horses: int
    n_hits: int
    monthly_return_rate: float
    cumulative_return_rate: float
    score: float


@dataclass
class WalkForwardConfig:
    """Walk-Forward検証の設定"""
    train_start: str = "20161101"      # 学習開始日（YYYYMMDD）
    initial_train_end: str = "20181231"  # 初期学習終了日
    test_end: str = "20251231"           # 最終テスト日
    expanding: bool = True               # True=拡張型（累積）, False=スライディング


def generate_monthly_periods(
    config: WalkForwardConfig,
) -> List[dict]:
    """
    月次のtrain/testペリオドを生成する。

    Args:
        config: Walk-Forward検証の設定

    Returns:
        List[dict]: 各期間の情報
            - train_start: 学習開始日
            - train_end: 学習終了日（テスト月の前月末日）
            - test_start: テスト開始日
            - test_end: テスト終了日
            - test_year_month: テスト年月（"YYYY-MM"形式）
    """
    periods = []

    # テスト月の開始: 2019年1月
    test_start = pd.Timestamp("2019-01-01")
    test_final = pd.Timestamp(config.test_end)

    while test_start <= test_final:
        # テスト月の終了日
        test_end = test_start + pd.offsets.MonthEnd(0)

        # 学習の終了日 = テスト月の前月末日（リーク防止の核心）
        train_end = test_start - pd.Timedelta(days=1)

        # 学習の開始日
        if config.expanding:
            train_start = pd.Timestamp(config.train_start)
        else:
            # スライディングの場合は初期ウィンドウサイズを維持
            initial_start = pd.Timestamp(config.train_start)
            initial_end = pd.Timestamp(config.initial_train_end)
            window_size = (initial_end - initial_start).days
            train_start = max(initial_start, train_end - pd.Timedelta(days=window_size))

        periods.append({
            "train_start": train_start.strftime("%Y%m%d"),
            "train_end": train_end.strftime("%Y%m%d"),
            "test_start": test_start.strftime("%Y%m%d"),
            "test_end": test_end.strftime("%Y%m%d"),
            "test_year_month": test_start.strftime("%Y-%m"),
        })

        # 次の月へ
        test_start = test_start + pd.offsets.MonthBegin(1)

    return periods


def run_walk_forward(
    df: pd.DataFrame,
    date_col: str = "race_date",
    odds_col: str = "tansho_odds",
    hit_flag_col: str = "is_hit",
    year_col: str = "race_year",
    race_id_col: str = "race_id",
    is_fukusho: bool = False,
    config: Optional[WalkForwardConfig] = None,
) -> List[MonthlyResult]:
    """
    Walk-Forward検証を実行する。

    Args:
        df: 全期間のベースデータ（日付・オッズ・的中フラグ・年度を含む）
        date_col: 日付カラム名（YYYYMMDD形式の文字列）
        odds_col: オッズカラム名
        hit_flag_col: 的中フラグカラム名
        year_col: 年度カラム名
        race_id_col: レースID（ユニークレース特定用）
        is_fukusho: True=複勝, False=単勝
        config: Walk-Forward設定

    Returns:
        List[MonthlyResult]: 月次検証結果のリスト
    """
    if config is None:
        config = WalkForwardConfig()

    periods = generate_monthly_periods(config)
    results: List[MonthlyResult] = []

    cumulative_weighted_bet = 0.0
    cumulative_weighted_payout = 0.0

    for period in periods:
        # テスト月のデータを抽出
        test_mask = (
            (df[date_col] >= period["test_start"])
            & (df[date_col] <= period["test_end"])
        )
        test_df = df[test_mask].copy()

        if len(test_df) == 0:
            continue

        # 月次補正回収率を算出
        monthly = calc_corrected_return_rate(
            test_df,
            odds_col=odds_col,
            hit_flag_col=hit_flag_col,
            year_col=year_col,
            is_fukusho=is_fukusho,
        )

        # 累積
        cumulative_weighted_bet += monthly["total_weighted_bet"]
        cumulative_weighted_payout += monthly["total_weighted_payout"]

        if cumulative_weighted_bet > 0:
            cumulative_rate = (cumulative_weighted_payout / cumulative_weighted_bet) * 100.0
        else:
            cumulative_rate = 0.0

        # レース数
        if race_id_col in test_df.columns:
            n_races = test_df[race_id_col].nunique()
        else:
            n_races = 0

        results.append(MonthlyResult(
            year_month=period["test_year_month"],
            n_races=n_races,
            n_horses=monthly["n_samples"],
            n_hits=monthly["n_hits"],
            monthly_return_rate=round(monthly["corrected_return_rate"], 2),
            cumulative_return_rate=round(cumulative_rate, 2),
            score=round(cumulative_rate - 80.0, 2),
        ))

    return results


def validate_no_leak(
    df: pd.DataFrame,
    train_end: str,
    date_col: str = "race_date",
) -> bool:
    """
    ターゲットリークがないことを検証する。

    Args:
        df: 学習データとして使用するDataFrame
        train_end: 学習期間の終了日（YYYYMMDD形式）
        date_col: 日付カラム名

    Returns:
        True = リークなし, False = リークあり

    Raises:
        AssertionError: リークが検出された場合
    """
    max_date = df[date_col].max()
    assert max_date <= train_end, (
        f"TARGET LEAK DETECTED: Training data contains date {max_date} "
        f"which is after train_end {train_end}"
    )
    return True
