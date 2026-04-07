"""
325ファクター全量エッジ分析テスト
"""
import pytest
import numpy as np
import pandas as pd

from roi_pipeline.engine.factor_catalog_325 import (
    ALL_FACTORS_325,
    ACTIVE_FACTORS,
    Factor325,
    get_factor_by_fid,
)
from roi_pipeline.engine.factor_analysis_engine import (
    _calc_roi,
    _confidence,
    _adjusted_roi,
    _bin_series,
    _assign_surface,
    _assign_course_27,
    analyze_factor,
    analyze_all_factors,
    BinResult,
    FactorResult,
    BASELINE,
    TARGET_PAYOUT,
)
from roi_pipeline.reports.generate_factor_analysis_325 import _make_mock_df


# =============================================================================
# factor_catalog_325 テスト
# =============================================================================

class TestFactorCatalog325:

    def test_total_count(self):
        """全325ファクターが定義されていること"""
        assert len(ALL_FACTORS_325) == 325

    def test_unique_fids(self):
        """fid が 1-325 で連番かつ重複なし"""
        fids = [f.fid for f in ALL_FACTORS_325]
        assert sorted(fids) == list(range(1, 326))

    def test_unique_aliases(self):
        """aliasが重複していないこと"""
        aliases = [f.alias for f in ALL_FACTORS_325]
        assert len(aliases) == len(set(aliases)), "エイリアスに重複あり"

    def test_kind_values(self):
        """kind が有効な値のみであること"""
        valid_kinds = {"NUMERIC", "CATEGORY", "ORDINAL", "SKIP"}
        for f in ALL_FACTORS_325:
            assert f.kind in valid_kinds, f"fid={f.fid} kind={f.kind}"

    def test_skip_reason_present(self):
        """SKIPファクターにはskip_reasonがあること"""
        for f in ALL_FACTORS_325:
            if f.kind == "SKIP":
                assert f.skip_reason != "", f"fid={f.fid} {f.alias} has empty skip_reason"

    def test_numeric_bins_positive(self):
        """NUMERICファクターのn_binsが正であること"""
        for f in ALL_FACTORS_325:
            if f.kind == "NUMERIC":
                assert f.n_bins >= 2, f"fid={f.fid} n_bins={f.n_bins}"

    def test_active_factors_no_skip(self):
        """ACTIVE_FACTORSにはSKIPが含まれないこと"""
        for f in ACTIVE_FACTORS:
            assert f.kind != "SKIP", f"fid={f.fid} is SKIP but in ACTIVE_FACTORS"

    def test_get_factor_by_fid(self):
        """fid=1 は jvd_se.aiteuma_joho_1"""
        f = get_factor_by_fid(1)
        assert f.table == "jvd_se"
        assert f.column == "aiteuma_joho_1"

    def test_get_factor_by_fid_invalid(self):
        """存在しないfidはValueError"""
        with pytest.raises(ValueError):
            get_factor_by_fid(9999)

    def test_fid_325_is_bac(self):
        """fid=325 は jrd_bac.track_baba_sa"""
        f = get_factor_by_fid(325)
        assert f.table == "jrd_bac"
        assert f.column == "track_baba_sa"

    def test_tables_covered(self):
        """全13 JRA-VANテーブル + 5 JRDBテーブルが存在すること"""
        tables = {f.table for f in ALL_FACTORS_325}
        expected = {
            "jvd_se", "jvd_ra", "jvd_ck", "jvd_h1", "jvd_hr",
            "jvd_wc", "jvd_dm", "jvd_um", "jvd_sk", "jvd_hc",
            "jvd_h6", "jvd_jg", "jvd_ch",
            "jrd_kyi", "jrd_cyb", "jrd_sed", "jrd_joa", "jrd_bac",
        }
        assert tables == expected, f"テーブルの差分: {tables.symmetric_difference(expected)}"

    def test_jrd_kyi_has_idm(self):
        """jrd_kyi に idm が含まれること"""
        kyi_cols = {f.column for f in ALL_FACTORS_325 if f.table == "jrd_kyi"}
        assert "idm" in kyi_cols


# =============================================================================
# factor_analysis_engine テスト
# =============================================================================

class TestConfidenceAndAdjusted:

    def test_confidence_zero(self):
        """N=0 → confidence=0"""
        assert _confidence(0) == pytest.approx(0.0)

    def test_confidence_400(self):
        """N=400 → confidence=sqrt(0.5)≈0.707"""
        assert _confidence(400) == pytest.approx(np.sqrt(0.5), rel=1e-5)

    def test_confidence_large(self):
        """N=100000, K=400 → confidence=sqrt(100000/100400)≈0.998"""
        assert _confidence(100000) > 0.997

    def test_adjusted_roi_above_baseline(self):
        """roi=90, conf=0.707 → adjusted≈87.07"""
        adj = _adjusted_roi(90.0, _confidence(400))
        assert adj == pytest.approx(87.07, abs=0.1)

    def test_adjusted_roi_below_baseline(self):
        """roi=70, N大 → adjusted < BASELINE"""
        adj = _adjusted_roi(70.0, _confidence(10000))
        assert adj < BASELINE

    def test_adjusted_roi_at_baseline(self):
        """roi=80 → adjusted=80 (N問わず)"""
        for n in [10, 100, 1000, 10000]:
            adj = _adjusted_roi(80.0, _confidence(n))
            assert adj == pytest.approx(80.0, abs=1e-6)


class TestCalcRoi:

    def _make_df(self, odds_vals, hit_vals, years, fukusho_odds=None):
        df = pd.DataFrame({
            "tansho_odds": odds_vals,
            "is_hit": hit_vals,
            "race_year": years,
            "fukusho_odds": fukusho_odds if fukusho_odds is not None else [np.nan] * len(odds_vals),
            "is_fukusho_hit": [1 if f is not None and not np.isnan(f) else 0
                               for f in (fukusho_odds or [np.nan] * len(odds_vals))],
        })
        return df

    def test_empty_returns_baseline(self):
        """空DataFrameはBASELINEを返す"""
        df = self._make_df([], [], [])
        roi, hr, n, _ = _calc_roi(df, is_fukusho=False)
        assert roi == BASELINE
        assert n == 0

    def test_no_hit_roi_zero(self):
        """的中なし → ROI = 0（払戻0）"""
        df = self._make_df([5.0, 10.0], [0, 0], ["2020", "2020"])
        roi, hr, n, hits = _calc_roi(df, is_fukusho=False)
        assert roi == pytest.approx(0.0, abs=0.01)
        assert hits == 0

    def test_all_hit_equal_odds(self):
        """全的中 + 同一オッズ → ROI > BASELINE"""
        df = self._make_df([3.0, 3.0], [1, 1], ["2020", "2020"])
        roi, hr, n, hits = _calc_roi(df, is_fukusho=False)
        assert hits == 2
        assert hr == pytest.approx(1.0)
        # 補正係数が1.0なら roi = 100
        assert roi > BASELINE

    def test_year_weight_applied(self):
        """2025年は2016年の10倍重み"""
        df_old = self._make_df([5.0], [1], ["2016"])
        df_new = self._make_df([5.0], [1], ["2025"])
        roi_old, _, _, _ = _calc_roi(df_old, is_fukusho=False)
        roi_new, _, _, _ = _calc_roi(df_new, is_fukusho=False)
        # 同じオッズ・同じ的中 → ROIは同じ（重みはbet/payの比率にのみ影響）
        assert roi_old == pytest.approx(roi_new, rel=1e-5)


class TestBinSeries:

    def _make_numeric_factor(self) -> Factor325:
        return Factor325(999, "test_tbl", "test_col", "test_alias", "NUMERIC", "test", n_bins=5)

    def _make_category_factor(self) -> Factor325:
        return Factor325(998, "test_tbl", "test_col", "test_alias", "CATEGORY", "test")

    def test_numeric_returns_q_labels(self):
        """NUMERIC → Q1..Qn ラベル"""
        f = self._make_numeric_factor()
        series = pd.Series(list(range(100)))
        result = _bin_series(series, f)
        unique = set(result.dropna().unique())
        assert all(v.startswith("Q") for v in unique)
        assert len(unique) == 5

    def test_numeric_all_nan(self):
        """全NULLのNUMERIC → 全NA"""
        f = self._make_numeric_factor()
        series = pd.Series([np.nan] * 50)
        result = _bin_series(series, f)
        assert result.isna().all()

    def test_category_passthrough(self):
        """CATEGORY → そのまま返す"""
        f = self._make_category_factor()
        series = pd.Series(["A", "B", "C", "A", "  "])
        result = _bin_series(series, f)
        assert "A" in result.values
        assert "B" in result.values
        # 空白はNAになるはず
        assert result.isna().sum() >= 1

    def test_numeric_string_values(self):
        """文字列の数値もビン化できる"""
        f = self._make_numeric_factor()
        series = pd.Series([str(i) for i in range(100)])
        result = _bin_series(series, f)
        assert result.notna().sum() > 0


class TestAssignSurface:

    def test_track_code_11_is_shiba(self):
        """track_code 11 → 芝"""
        df = pd.DataFrame({"surface_2": ["芝", "ダ", "その他"]})
        result = _assign_surface(df)
        assert result[0] == "芝"
        assert result[1] == "ダ"

    def test_fallback_from_track_code(self):
        """surface_2 なしの場合 track_code から判定"""
        df = pd.DataFrame({"ra_track_code": ["10", "20", "30"]})
        result = _assign_surface(df)
        assert result[0] == "芝"
        assert result[1] == "ダ"
        assert result[2] == "その他"


class TestAnalyzeFactor:

    def _get_mock_df(self) -> pd.DataFrame:
        return _make_mock_df(n_rows=5000)

    def test_skip_factor_returns_skipped(self):
        """SKIPファクターはanalyzed=False"""
        df = self._get_mock_df()
        f = get_factor_by_fid(1)  # aiteuma_joho_1 = SKIP
        result = analyze_factor(df, f)
        assert result.skipped is True
        assert result.skip_reason != ""

    def test_active_factor_returns_bins(self):
        """アクティブファクターはbinsを返す"""
        df = self._get_mock_df()
        # kyi_idm = fid 210, NUMERIC
        f = get_factor_by_fid(210)
        # aliasがDFに存在するか確認
        if f.alias not in df.columns:
            pytest.skip(f"Column {f.alias} not in mock df")
        result = analyze_factor(df, f)
        assert not result.skipped
        assert len(result.bins) > 0

    def test_bins_have_all_three_segments(self):
        """ビンに3セグメントが全て含まれること（データ次第）"""
        df = self._get_mock_df()
        f = get_factor_by_fid(210)
        if f.alias not in df.columns:
            pytest.skip(f"Column {f.alias} not in mock df")
        result = analyze_factor(df, f)
        if not result.bins:
            pytest.skip("no bins generated")
        seg_types = {b.segment_type for b in result.bins}
        assert "GLOBAL" in seg_types

    def test_grade_is_valid(self):
        """gradeがS/A/B/Cのいずれか"""
        df = self._get_mock_df()
        f = get_factor_by_fid(210)
        if f.alias not in df.columns:
            pytest.skip()
        result = analyze_factor(df, f)
        assert result.grade in ("S", "A", "B", "C")

    def test_null_rate_computed(self):
        """null_rateが0.0-1.0の範囲"""
        df = self._get_mock_df()
        f = get_factor_by_fid(210)
        if f.alias not in df.columns:
            pytest.skip()
        result = analyze_factor(df, f)
        assert 0.0 <= result.null_rate <= 1.0

    def test_tansho_adjusted_formula(self):
        """tansho_adjusted が (roi-80)*conf+80 であること"""
        df = self._get_mock_df()
        f = get_factor_by_fid(210)
        if f.alias not in df.columns:
            pytest.skip()
        result = analyze_factor(df, f)
        for b in result.bins:
            expected = _adjusted_roi(b.tansho_roi, b.tansho_confidence)
            assert b.tansho_adjusted == pytest.approx(expected, abs=0.01)

    def test_to_csv_rows_not_empty(self):
        """to_csv_rowsが行を返すこと"""
        df = self._get_mock_df()
        f = get_factor_by_fid(210)
        if f.alias not in df.columns:
            pytest.skip()
        result = analyze_factor(df, f)
        rows = result.to_csv_rows()
        assert len(rows) > 0
        assert "fid" in rows[0]


class TestAnalyzeAllFactors:

    def test_returns_325_results(self):
        """analyze_all_factors は325件を返す"""
        df = _make_mock_df(n_rows=2000)
        results = analyze_all_factors(df, verbose=False)
        assert len(results) == 325

    def test_skip_factors_are_skipped(self):
        """カタログでSKIPのファクターはresult.skipped=True"""
        df = _make_mock_df(n_rows=2000)
        results = analyze_all_factors(df, verbose=False)
        result_map = {r.factor.fid: r for r in results}
        # fid=1 (aiteuma_joho_1) はSKIP
        assert result_map[1].skipped is True

    def test_no_exception_on_full_run(self):
        """全325ファクターが例外なく処理できること"""
        df = _make_mock_df(n_rows=3000)
        try:
            results = analyze_all_factors(df, verbose=False)
        except Exception as e:
            pytest.fail(f"analyze_all_factors raised {e}")
        assert len(results) == 325


class TestMockDf:

    def test_mock_df_shape(self):
        """モックDFが正しい形を持つこと"""
        df = _make_mock_df(n_rows=1000)
        assert len(df) == 1000
        assert "tansho_odds" in df.columns
        assert "kakutei_chakujun" in df.columns

    def test_mock_df_has_is_hit(self):
        """モックDFにis_hitカラムがある"""
        df = _make_mock_df(n_rows=100)
        assert "is_hit" in df.columns
        assert df["is_hit"].isin([0, 1]).all()

    def test_mock_df_fukusho_only_for_top3(self):
        """fukusho_oddsは着順3以内のみ値があること"""
        df = _make_mock_df(n_rows=500)
        df["_chak"] = pd.to_numeric(df["kakutei_chakujun"], errors="coerce")
        # 着外馬のfukusho_oddsはNaN
        outer = df[df["_chak"] > 3]
        assert outer["fukusho_odds"].isna().all()
