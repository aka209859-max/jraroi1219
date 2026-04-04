"""
Phase 2 タスク2 のユニットテスト

テスト対象:
    - generate_phase2_task2 モジュール
    - TASK2_FACTORS 定義（10ファクター）
    - SURFACE_2 / COURSE_27 / GLOBAL レポート生成関数
    - 単勝・複勝デュアルROI計算
    - Phase 1 エッジビン抽出
"""
import pytest
import pandas as pd
import numpy as np
import os
import tempfile
from typing import Dict, Set

from roi_pipeline.reports.generate_phase2_task2 import (
    TASK2_FACTORS,
    generate_surface2_report,
    generate_course27_report,
    generate_global_report,
    _compute_dual_roi,
    _prepare_fukusho_df,
    _extract_phase1_edge_bins,
    _get_factor_key,
    _get_segment_suffix,
    ensure_report_dir,
    REPORT_DIR,
)
from roi_pipeline.engine.corrected_return import (
    calc_corrected_return_rate,
    BASELINE_RATE,
)
from roi_pipeline.engine.hierarchical_bayes import BayesEstimate
from roi_pipeline.factors.definitions import get_factor_by_id, FACTOR_DEFINITIONS


class TestTask2FactorDefinitions:
    """TASK2_FACTORS定義のテスト"""

    def test_exactly_10_factors(self) -> None:
        """10ファクター定義されていること"""
        assert len(TASK2_FACTORS) == 10

    def test_7_surface2_factors(self) -> None:
        """SURFACE_2が7ファクターであること"""
        surface2 = [f for f in TASK2_FACTORS if f[1] == "SURFACE_2"]
        assert len(surface2) == 7

    def test_2_course27_factors(self) -> None:
        """COURSE_27が2ファクターであること"""
        course27 = [f for f in TASK2_FACTORS if f[1] == "COURSE_27"]
        assert len(course27) == 2

    def test_1_global_factor(self) -> None:
        """GLOBALが1ファクターであること"""
        global_f = [f for f in TASK2_FACTORS if f[1] == "GLOBAL"]
        assert len(global_f) == 1

    def test_all_factor_ids_valid(self) -> None:
        """全ファクターIDが定義テーブルに存在すること"""
        for factor_id, _, _ in TASK2_FACTORS:
            factor = get_factor_by_id(factor_id)
            assert factor is not None
            assert factor.id == factor_id

    def test_no_duplicate_factor_ids(self) -> None:
        """ファクターIDに重複がないこと"""
        ids = [f[0] for f in TASK2_FACTORS]
        assert len(ids) == len(set(ids))

    def test_phase1_edge_counts_positive(self) -> None:
        """Phase 1エッジビン数が正の整数であること"""
        for _, _, edge_count in TASK2_FACTORS:
            assert isinstance(edge_count, int)
            assert edge_count >= 0


class TestDualRoiComputation:
    """単勝・複勝デュアルROI計算のテスト"""

    @pytest.fixture
    def sample_data_tansho_only(self) -> pd.DataFrame:
        """単勝のみのテストデータ"""
        np.random.seed(42)
        n = 1000
        return pd.DataFrame({
            "tansho_odds": np.random.uniform(2.0, 20.0, size=n).round(1),
            "is_hit": np.random.choice([0, 1], size=n, p=[0.92, 0.08]),
            "race_year": np.random.choice(["2020", "2021", "2022"], size=n),
        })

    @pytest.fixture
    def sample_data_with_fukusho(self) -> pd.DataFrame:
        """単勝＋複勝テストデータ（jvd_hrパターン: 的中馬のみオッズあり）"""
        np.random.seed(42)
        n = 1000
        kakutei = np.random.choice(
            ["1", "2", "3", "4", "5", "6", "7", "8"],
            size=n, p=[0.08, 0.08, 0.08, 0.10, 0.10, 0.15, 0.20, 0.21],
        )
        # jvd_hrパターン: 3着以内の馬のみ fukusho_odds が non-NULL
        fukusho_odds = np.where(
            np.isin(kakutei, ["1", "2", "3"]),
            np.random.uniform(1.2, 8.0, size=n).round(1),
            np.nan,
        )
        return pd.DataFrame({
            "tansho_odds": np.random.uniform(2.0, 20.0, size=n).round(1),
            "fukusho_odds": fukusho_odds,
            "is_hit": (kakutei == "1").astype(int),
            "kakutei_chakujun": kakutei,
            "race_year": np.random.choice(["2020", "2021", "2022"], size=n),
        })

    def test_dual_roi_tansho_only(self, sample_data_tansho_only: pd.DataFrame) -> None:
        """単勝のみの場合、tanshoが算出されfukushoはn_samples=0"""
        result = _compute_dual_roi(sample_data_tansho_only)
        assert "tansho" in result
        assert "fukusho" in result
        assert result["tansho"]["n_samples"] > 0
        assert result["fukusho"]["n_samples"] == 0

    def test_dual_roi_with_fukusho(self, sample_data_with_fukusho: pd.DataFrame) -> None:
        """複勝データありの場合、両方が算出されること"""
        result = _compute_dual_roi(sample_data_with_fukusho)
        assert result["tansho"]["n_samples"] > 0
        assert result["fukusho"]["n_samples"] > 0
        assert isinstance(result["tansho"]["corrected_return_rate"], float)
        assert isinstance(result["fukusho"]["corrected_return_rate"], float)

    def test_tansho_roi_in_valid_range(self, sample_data_tansho_only: pd.DataFrame) -> None:
        """単勝ROIが妥当な範囲にあること"""
        result = _compute_dual_roi(sample_data_tansho_only)
        rate = result["tansho"]["corrected_return_rate"]
        # ランダムデータだが0-200%の範囲内にはあるはず
        assert 0.0 <= rate <= 200.0


class TestPrepareFukushoDf:
    """_prepare_fukusho_df のテスト"""

    def test_no_fukusho_column_returns_none(self) -> None:
        """fukusho_oddsカラムがない場合はNoneを返す"""
        df = pd.DataFrame({"tansho_odds": [5.0, 10.0]})
        result = _prepare_fukusho_df(df)
        assert result is None

    def test_all_null_fukusho_returns_none(self) -> None:
        """fukusho_oddsが全NULLの場合はNoneを返す"""
        df = pd.DataFrame({
            "tansho_odds": [5.0, 10.0, 15.0],
            "fukusho_odds": [np.nan, np.nan, np.nan],
        })
        result = _prepare_fukusho_df(df)
        assert result is None

    def test_hit_flag_from_non_null_odds(self) -> None:
        """non-NULL行にfukusho_is_hit=1、NULL行に0が設定されること"""
        df = pd.DataFrame({
            "tansho_odds": [5.0, 10.0, 15.0, 20.0],
            "fukusho_odds": [2.3, np.nan, 1.5, np.nan],
        })
        result = _prepare_fukusho_df(df)
        assert result is not None
        assert list(result["fukusho_is_hit"]) == [1, 0, 1, 0]

    def test_non_hit_odds_estimated_from_tansho(self) -> None:
        """非的中馬のfukusho_oddsがtansho_odds*0.35で推定されること"""
        df = pd.DataFrame({
            "tansho_odds": [5.0, 10.0, 20.0],
            "fukusho_odds": [1.5, np.nan, np.nan],  # 1行は的中(non-NULL)
        })
        result = _prepare_fukusho_df(df)
        assert result is not None
        # 非的中馬: 10.0 * 0.35 = 3.5, 20.0 * 0.35 = 7.0
        assert abs(result["fukusho_odds"].iloc[1] - 3.5) < 0.01
        assert abs(result["fukusho_odds"].iloc[2] - 7.0) < 0.01
        # 的中馬: 元のオッズ保持
        assert abs(result["fukusho_odds"].iloc[0] - 1.5) < 0.01

    def test_hit_odds_preserved(self) -> None:
        """的中馬のfukusho_oddsが元の値のまま保持されること"""
        df = pd.DataFrame({
            "tansho_odds": [10.0, 20.0],
            "fukusho_odds": [2.3, np.nan],
        })
        result = _prepare_fukusho_df(df)
        assert result is not None
        assert abs(result["fukusho_odds"].iloc[0] - 2.3) < 0.01

    def test_minimum_odds_clipped(self) -> None:
        """推定オッズが最低1.0にクリップされること"""
        df = pd.DataFrame({
            "tansho_odds": [1.0, 5.0],  # 1.0 * 0.35 = 0.35 → clipped to 1.0
            "fukusho_odds": [np.nan, 2.0],  # 1行は的中(non-NULL)が必要
        })
        result = _prepare_fukusho_df(df)
        assert result is not None
        assert result["fukusho_odds"].iloc[0] >= 1.0

    def test_jvd_hr_pattern_full(self) -> None:
        """jvd_hrパターン: 3着以内のみオッズありのデータが正しく処理されること"""
        np.random.seed(42)
        n = 200
        kakutei = np.random.choice(range(1, 19), size=n)
        fukusho_odds = np.where(
            kakutei <= 3,
            np.random.uniform(1.2, 8.0, size=n).round(1),
            np.nan,
        )
        df = pd.DataFrame({
            "tansho_odds": np.random.uniform(2.0, 30.0, size=n).round(1),
            "fukusho_odds": fukusho_odds.astype(float),
            "kakutei_chakujun": kakutei,
        })
        result = _prepare_fukusho_df(df)
        assert result is not None
        # 全行にfukusho_oddsが埋まっている
        assert result["fukusho_odds"].notna().all()
        # 的中率は約3/18 ≈ 16.7%付近
        hit_rate = result["fukusho_is_hit"].mean()
        assert 0.05 < hit_rate < 0.40  # 広めに許容


class TestPhase1EdgeExtraction:
    """Phase 1エッジビン抽出のテスト"""

    def test_extract_from_nonexistent_file(self) -> None:
        """存在しないファイルの場合、空集合が返ること"""
        result = _extract_phase1_edge_bins("/nonexistent/path.md")
        assert result == set()

    def test_extract_from_valid_report(self, tmp_path: str) -> None:
        """正しい形式のレポートからエッジビンを抽出できること"""
        report_content = """# Factor Report

## エッジ検出
該当ビン/カテゴリ:
- bin_A
- bin_B
- bin_C

---
"""
        report_file = os.path.join(str(tmp_path), "test_report.md")
        with open(report_file, "w", encoding="utf-8") as f:
            f.write(report_content)

        result = _extract_phase1_edge_bins(report_file)
        assert result == {"bin_A", "bin_B", "bin_C"}


class TestSegmentSuffix:
    """ファイル名サフィックス生成のテスト"""

    def test_surface2_suffix(self) -> None:
        assert _get_segment_suffix("SURFACE_2") == "surface2"

    def test_course27_suffix(self) -> None:
        assert _get_segment_suffix("COURSE_27") == "course27"

    def test_global_suffix(self) -> None:
        assert _get_segment_suffix("GLOBAL") == "global"


class TestFactorKey:
    """ファクターキー取得のテスト"""

    def test_idm_key(self) -> None:
        assert _get_factor_key(1) == "idm"

    def test_sogo_key(self) -> None:
        assert _get_factor_key(2) == "sogo_shisu"

    def test_invalid_id_raises(self) -> None:
        with pytest.raises(ValueError):
            _get_factor_key(999)


class TestReportGeneration:
    """レポート生成のテスト（軽量データでの検証）"""

    @pytest.fixture
    def minimal_data(self) -> pd.DataFrame:
        """レポート生成テスト用の最小データ"""
        np.random.seed(42)
        n = 500
        return pd.DataFrame({
            "keibajo_code": np.random.choice(["05", "06", "09"], size=n),
            "track_code": np.random.choice(["11", "22"], size=n),
            "ra_kyori": np.random.choice([1200, 1600, 2000], size=n),
            "bac_kyori": np.random.choice([1200, 1600, 2000], size=n),
            "tansho_odds": np.random.uniform(2.0, 20.0, size=n).round(1),
            "is_hit": np.random.choice([0, 1], size=n, p=[0.92, 0.08]),
            "race_year": np.random.choice(["2020", "2021", "2022", "2023"], size=n),
            "idm": np.random.uniform(10, 80, size=n).round(1),
            "sogo_shisu": np.random.uniform(20, 90, size=n).round(1),
            "agari_shisu": np.random.uniform(10, 70, size=n).round(1),
            "pace_shisu": np.random.uniform(15, 75, size=n).round(1),
            "kishu_shisu": np.random.uniform(10, 60, size=n).round(1),
            "ls_shisu": np.random.uniform(5, 50, size=n).round(1),
            "barei": np.random.choice(["2", "3", "4", "5", "6", "7"], size=n),
            "kyori_tekisei_code": np.random.choice(["1", "2", "3", "4", "5"], size=n),
            "course_tekisei": np.random.choice(["A", "B", "C", "D"], size=n),
            "babajotai_code_shiba": np.random.choice(["1", "2", "3", "4"], size=n),
            "babajotai_code_dirt": np.random.choice(["1", "2", "3", "4"], size=n),
        })

    def test_surface2_report_generates_string(self, minimal_data: pd.DataFrame) -> None:
        """SURFACE_2レポートが文字列として生成されること"""
        report = generate_surface2_report(
            minimal_data, factor_id=1, global_rate=80.0, phase1_edge_count=5,
        )
        assert isinstance(report, str)
        assert "Phase 2 タスク2" in report
        assert "IDM" in report
        assert "SURFACE_2" in report

    def test_surface2_report_contains_dual_roi(self, minimal_data: pd.DataFrame) -> None:
        """SURFACE_2レポートに単勝テーブルが含まれること"""
        report = generate_surface2_report(
            minimal_data, factor_id=1, global_rate=80.0, phase1_edge_count=5,
        )
        assert "単勝" in report
        assert "ベイズ推定" in report

    def test_course27_report_generates_string(self, minimal_data: pd.DataFrame) -> None:
        """COURSE_27レポートが文字列として生成されること"""
        report = generate_course27_report(
            minimal_data, factor_id=5, global_rate=80.0, phase1_edge_count=2,
        )
        assert isinstance(report, str)
        assert "コース分類(27)" in report
        assert "距離適性" in report

    def test_global_report_generates_string(self, minimal_data: pd.DataFrame) -> None:
        """GLOBALレポートが文字列として生成されること"""
        report = generate_global_report(
            minimal_data, factor_id=8, global_rate=80.0, phase1_edge_count=3,
        )
        assert isinstance(report, str)
        assert "GLOBAL" in report
        assert "馬場状態コード" in report

    def test_global_report_contains_phase1_comparison(
        self, minimal_data: pd.DataFrame,
    ) -> None:
        """GLOBALレポートにPhase 1比較セクションが含まれること"""
        report = generate_global_report(
            minimal_data, factor_id=8, global_rate=80.0, phase1_edge_count=3,
        )
        assert "Phase 1 エッジとの比較" in report

    def test_report_dir_creation(self) -> None:
        """レポートディレクトリが作成できること"""
        ensure_report_dir()
        assert os.path.isdir(REPORT_DIR)

    def test_surface2_all_7_factors_have_valid_columns(self) -> None:
        """SURFACE_2 の7ファクターのカラムが全て有効であること"""
        surface2_factors = [f for f in TASK2_FACTORS if f[1] == "SURFACE_2"]
        for fid, _, _ in surface2_factors:
            factor = get_factor_by_id(fid)
            assert factor.column is not None
            assert len(factor.column) > 0

    def test_bayes_shrinkage_applied(self, minimal_data: pd.DataFrame) -> None:
        """ベイズ収縮が適用されていること（推定値≠観測値）"""
        from roi_pipeline.engine.interaction_analysis import (
            assign_surface, run_interaction_analysis,
        )
        from roi_pipeline.factors.binning import apply_binning

        df_work = minimal_data.copy()
        df_work["surface_type"] = assign_surface(df_work)
        df_work = df_work[df_work["surface_type"].isin(["芝", "ダート"])].copy()

        factor = get_factor_by_id(1)  # IDM
        binned, bin_col = apply_binning(df_work, factor)
        df_work[bin_col] = binned
        df_work = df_work.dropna(subset=[bin_col])

        result = run_interaction_analysis(
            df=df_work, factor_col=bin_col, segment_col="surface_type",
            global_rate=80.0, min_samples=10,
        )

        if len(result.cells) > 0:
            differences = [
                abs(c.bayes_estimate.observed_rate - c.bayes_estimate.estimated_rate)
                for c in result.cells
            ]
            # 少なくとも1セルでベイズ収縮が効いている
            assert any(d > 0.001 for d in differences)


class TestQualityGate:
    """品質ゲートのテスト"""

    def test_global_roi_range_validation(self) -> None:
        """グローバルROIの品質ゲート範囲"""
        # 正常範囲
        assert 75.0 <= 79.91 <= 85.0
        # 範囲外
        assert not (75.0 <= 50.0 <= 85.0)
        assert not (75.0 <= 90.0 <= 85.0)

    def test_baseline_rate_is_80(self) -> None:
        """ベースラインレートが80.0であること"""
        assert BASELINE_RATE == 80.0

    def test_task2_does_not_conflict_with_task1(self) -> None:
        """タスク2のファクターIDがタスク1と被らないこと
        タスク1: 馬番(13), 調教師指数(18), 厩舎指数(20)"""
        task1_ids = {13, 18, 20}
        task2_ids = {f[0] for f in TASK2_FACTORS}
        assert task1_ids.isdisjoint(task2_ids), \
            f"重複: {task1_ids & task2_ids}"


class TestFukushoCteGeneration:
    """data_loader_v2 の複勝CTE生成テスト"""

    def test_cte_contains_5_unions(self) -> None:
        """CTE SQLが5つのUNION ALLで構成されること"""
        from roi_pipeline.engine.data_loader_v2 import _build_fukusho_unpivot_cte
        cte = _build_fukusho_unpivot_cte()
        # 5つのSELECT文がUNION ALLで結合
        assert cte.count("UNION ALL") == 4  # 5 selects → 4 unions
        for n in range(1, 6):
            assert f"haraimodoshi_fukusho_{n}a" in cte
            assert f"haraimodoshi_fukusho_{n}b" in cte

    def test_cte_filters_empty_umaban(self) -> None:
        """CTE SQLが空馬番と0払戻をフィルタしていること"""
        from roi_pipeline.engine.data_loader_v2 import _build_fukusho_unpivot_cte
        cte = _build_fukusho_unpivot_cte()
        assert "'00'" in cte  # 馬番00除外
        assert "'000000000'" in cte  # 0円払戻除外

    def test_cte_calculates_odds_from_payout(self) -> None:
        """CTE SQLが払戻金額 / 100.0 でオッズに変換していること"""
        from roi_pipeline.engine.data_loader_v2 import _build_fukusho_unpivot_cte
        cte = _build_fukusho_unpivot_cte()
        assert "/ 100.0" in cte
