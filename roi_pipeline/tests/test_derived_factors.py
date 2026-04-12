"""
加工ファクター生成エンジンのユニットテスト

テスト対象:
    - _bin_bataiju_20kg()
    - _compute_kyori_hendo()
    - _compute_kyori_kubun()
    - _compute_corner4_group()
    - _compute_babajotai_flag()
    - _synth_jrdb_key8_series()
    - _percentile_rank()
    - _bin_kijun_odds_5()
    - _bin_ls_shisu_4()
    - _compute_idm_rank()
    - derive_all_factors()（DB モック版）
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from roi_pipeline.engine.derived_factors import (
    _bin_bataiju_20kg,
    _bin_kijun_odds_5,
    _bin_ls_shisu_4,
    _compute_babajotai_flag,
    _compute_corner4_group,
    _compute_idm_rank,
    _compute_kyori_hendo,
    _compute_kyori_kubun,
    _percentile_rank,
    _synth_jrdb_key8_series,
    derive_all_factors,
)


# =============================================================================
# テスト用ヘルパー
# =============================================================================

def _base_df(**overrides) -> pd.DataFrame:
    """最小限の基本 DataFrame を作成するファクトリ。"""
    base = {
        "ketto_toroku_bango": ["H001", "H002"],
        "keibajo_code": ["06", "05"],
        "kaisai_nen": ["2024", "2024"],
        "kaisai_tsukihi": ["0301", "0301"],
        "kaisai_kai": ["1", "1"],
        "kaisai_nichime": ["1", "1"],
        "race_bango": ["01", "01"],
        "umaban": ["01", "02"],
        "race_date": ["20240301", "20240301"],
        "kyori": [1200, 1600],
        "track_code": ["11", "11"],
        "bataiju": [460.0, 480.0],
        "kakutei_chakujun": [1.0, 3.0],
        "corner_4": [1.0, 4.0],
        "blinker_shiyo_kubun": ["0", "0"],
        "time_sa": [0.0, 0.3],
        "bataiju_prev1": [450.0, 500.0],
        "kyori_prev1": [1200.0, 1400.0],
        "corner_4_prev1": [2.0, 7.0],
        "keibajo_code_prev1": ["06", "05"],
        "track_code_prev1": ["11", "11"],
    }
    base.update(overrides)
    return pd.DataFrame(base)


# =============================================================================
# _bin_bataiju_20kg のテスト
# =============================================================================

class TestBinBataiju20kg:
    """前走馬体重 20kg 刻みビンのテスト"""

    def test_basic_binning(self) -> None:
        """基本的なビン化"""
        df = pd.DataFrame({"bataiju_prev1": [370.0, 390.0, 410.0, 450.0, 490.0, 525.0]})
        result = _bin_bataiju_20kg(df)
        assert result.iloc[0] == "380未満"
        assert result.iloc[1] == "380-399"
        assert result.iloc[2] == "400-419"
        assert result.iloc[3] == "440-459"
        assert result.iloc[4] == "480-499"
        assert result.iloc[5] == "520以上"

    def test_boundary_at_380(self) -> None:
        """380 はビン '380-399' に入ること"""
        df = pd.DataFrame({"bataiju_prev1": [379.9, 380.0]})
        result = _bin_bataiju_20kg(df)
        assert result.iloc[0] == "380未満"
        assert result.iloc[1] == "380-399"

    def test_missing_column(self) -> None:
        """bataiju_prev1 が存在しない場合は NaN"""
        df = pd.DataFrame({"dummy": [1, 2]})
        result = _bin_bataiju_20kg(df)
        assert result.isna().all()

    def test_nan_input(self) -> None:
        """NaN 入力は NaN のまま"""
        df = pd.DataFrame({"bataiju_prev1": [np.nan, 460.0]})
        result = _bin_bataiju_20kg(df)
        assert pd.isna(result.iloc[0])
        assert result.iloc[1] == "460-479"

    def test_all_bins_present(self) -> None:
        """9 種類のビンが全て生成できること"""
        vals = [370, 385, 405, 425, 445, 465, 485, 505, 525]
        df = pd.DataFrame({"bataiju_prev1": vals})
        result = _bin_bataiju_20kg(df)
        expected = [
            "380未満", "380-399", "400-419", "420-439", "440-459",
            "460-479", "480-499", "500-519", "520以上",
        ]
        assert result.tolist() == expected


# =============================================================================
# _compute_kyori_hendo のテスト
# =============================================================================

class TestComputeKyoriHendo:
    """今走距離変化（増/減/同）のテスト"""

    def test_increase(self) -> None:
        """距離が増えた場合"""
        df = pd.DataFrame({"kyori": [1600], "kyori_prev1": [1200.0]})
        result = _compute_kyori_hendo(df)
        assert result.iloc[0] == "増"

    def test_decrease(self) -> None:
        """距離が減った場合"""
        df = pd.DataFrame({"kyori": [1200], "kyori_prev1": [1600.0]})
        result = _compute_kyori_hendo(df)
        assert result.iloc[0] == "減"

    def test_same(self) -> None:
        """距離が同じ場合"""
        df = pd.DataFrame({"kyori": [1600], "kyori_prev1": [1600.0]})
        result = _compute_kyori_hendo(df)
        assert result.iloc[0] == "同"

    def test_missing_prev_col(self) -> None:
        """kyori_prev1 が存在しない場合は NaN"""
        df = pd.DataFrame({"kyori": [1600]})
        result = _compute_kyori_hendo(df)
        assert result.isna().all()

    def test_nan_prev(self) -> None:
        """前走なし（初出走）は NaN"""
        df = pd.DataFrame({"kyori": [1600], "kyori_prev1": [np.nan]})
        result = _compute_kyori_hendo(df)
        assert pd.isna(result.iloc[0])


# =============================================================================
# _compute_kyori_kubun のテスト
# =============================================================================

class TestComputeKyoriKubun:
    """距離区分（短/マイル/中/長）のテスト"""

    @pytest.mark.parametrize("kyori,expected", [
        (1000, "短"),
        (1200, "短"),
        (1399, "短"),
        (1400, "マイル"),
        (1600, "マイル"),
        (1799, "マイル"),
        (1800, "中"),
        (2000, "中"),
        (2199, "中"),
        (2200, "長"),
        (3200, "長"),
    ])
    def test_kyori_boundaries(self, kyori: int, expected: str) -> None:
        """境界値テスト"""
        df = pd.DataFrame({"kyori": [kyori]})
        result = _compute_kyori_kubun(df)
        assert result.iloc[0] == expected

    def test_missing_column(self) -> None:
        """kyori カラムなし → NaN"""
        df = pd.DataFrame({"dummy": [1]})
        result = _compute_kyori_kubun(df)
        assert result.isna().all()


# =============================================================================
# _compute_corner4_group のテスト
# =============================================================================

class TestComputeCorner4Group:
    """前走4角通過順位グループ（先行/中団/後方）のテスト"""

    @pytest.mark.parametrize("pos,expected", [
        (1, "先行"),
        (2, "先行"),
        (3, "先行"),
        (4, "中団"),
        (5, "中団"),
        (6, "中団"),
        (7, "後方"),
        (10, "後方"),
        (18, "後方"),
    ])
    def test_group_boundaries(self, pos: int, expected: str) -> None:
        """境界値テスト"""
        df = pd.DataFrame({"corner_4_prev1": [float(pos)]})
        result = _compute_corner4_group(df)
        assert result.iloc[0] == expected

    def test_missing_column(self) -> None:
        """corner_4_prev1 カラムなし → NaN"""
        df = pd.DataFrame({"dummy": [1]})
        result = _compute_corner4_group(df)
        assert result.isna().all()

    def test_nan_input(self) -> None:
        """NaN 入力（初出走等）は NaN"""
        df = pd.DataFrame({"corner_4_prev1": [np.nan]})
        result = _compute_corner4_group(df)
        assert pd.isna(result.iloc[0])


# =============================================================================
# _compute_babajotai_flag のテスト
# =============================================================================

class TestComputeBabajotaiFlag:
    """重・不良馬場フラグのテスト"""

    def test_heavy_turf(self) -> None:
        """芝・重（コード3）→ フラグ1"""
        df = pd.DataFrame({
            "babajotai_code_shiba": ["3"],
            "babajotai_code_dirt": ["1"],
            "track_code": ["11"],
        })
        result = _compute_babajotai_flag(df)
        assert result.iloc[0] == 1.0

    def test_bad_turf(self) -> None:
        """芝・不良（コード4）→ フラグ1"""
        df = pd.DataFrame({
            "babajotai_code_shiba": ["4"],
            "babajotai_code_dirt": ["1"],
            "track_code": ["11"],
        })
        result = _compute_babajotai_flag(df)
        assert result.iloc[0] == 1.0

    def test_firm_turf(self) -> None:
        """芝・良（コード1）→ フラグ0"""
        df = pd.DataFrame({
            "babajotai_code_shiba": ["1"],
            "babajotai_code_dirt": ["1"],
            "track_code": ["11"],
        })
        result = _compute_babajotai_flag(df)
        assert result.iloc[0] == 0.0

    def test_slightly_soft_turf(self) -> None:
        """芝・稍重（コード2）→ フラグ0"""
        df = pd.DataFrame({
            "babajotai_code_shiba": ["2"],
            "babajotai_code_dirt": ["1"],
            "track_code": ["11"],
        })
        result = _compute_babajotai_flag(df)
        assert result.iloc[0] == 0.0

    def test_heavy_dirt(self) -> None:
        """ダート・重（コード3）→ フラグ1"""
        df = pd.DataFrame({
            "babajotai_code_shiba": ["1"],
            "babajotai_code_dirt": ["3"],
            "track_code": ["21"],
        })
        result = _compute_babajotai_flag(df)
        assert result.iloc[0] == 1.0

    def test_missing_columns(self) -> None:
        """両 babajotai カラムが存在しない場合は NaN"""
        df = pd.DataFrame({"dummy": [1]})
        result = _compute_babajotai_flag(df)
        assert result.isna().all()


# =============================================================================
# _synth_jrdb_key8_series のテスト
# =============================================================================

class TestSynthJrdbKey8:
    """JRDB 8byte レースキー合成のテスト"""

    def test_basic_key(self) -> None:
        """通常ケース: 中山 2024年 1回 1日目 1レース → "0624110 1" ではなく正しい形式"""
        df = pd.DataFrame({
            "keibajo_code": ["06"],
            "kaisai_nen": ["2024"],
            "kaisai_kai": ["1"],
            "kaisai_nichime": ["1"],
            "race_bango": ["1"],
        })
        result = _synth_jrdb_key8_series(df)
        # keibajo=06, year=24, kai=1, nichime=1, race=01
        assert result.iloc[0] == "062411" + "01"

    def test_nichime_10_is_a(self) -> None:
        """kaisai_nichime=10 → 'a'"""
        df = pd.DataFrame({
            "keibajo_code": ["05"],
            "kaisai_nen": ["2024"],
            "kaisai_kai": ["2"],
            "kaisai_nichime": ["10"],
            "race_bango": ["05"],
        })
        result = _synth_jrdb_key8_series(df)
        # keibajo=05, year=24, kai=2, nichime=a, race=05
        assert result.iloc[0] == "0524" + "2a" + "05"

    def test_nichime_11_is_b(self) -> None:
        """kaisai_nichime=11 → 'b'"""
        df = pd.DataFrame({
            "keibajo_code": ["09"],
            "kaisai_nen": ["2023"],
            "kaisai_kai": ["3"],
            "kaisai_nichime": ["11"],
            "race_bango": ["12"],
        })
        result = _synth_jrdb_key8_series(df)
        assert result.iloc[0] == "0923" + "3b" + "12"

    def test_race_bango_zero_padded(self) -> None:
        """race_bango は 2 桁ゼロパディングされること"""
        df = pd.DataFrame({
            "keibajo_code": ["01"],
            "kaisai_nen": ["2024"],
            "kaisai_kai": ["1"],
            "kaisai_nichime": ["1"],
            "race_bango": ["5"],
        })
        result = _synth_jrdb_key8_series(df)
        assert result.iloc[0][-2:] == "05"

    def test_key_length_8(self) -> None:
        """生成キーは 8 文字であること"""
        df = pd.DataFrame({
            "keibajo_code": ["06"],
            "kaisai_nen": ["2024"],
            "kaisai_kai": ["1"],
            "kaisai_nichime": ["1"],
            "race_bango": ["01"],
        })
        result = _synth_jrdb_key8_series(df)
        assert len(result.iloc[0]) == 8


# =============================================================================
# _percentile_rank のテスト
# =============================================================================

class TestPercentileRank:
    """S/A/B/C/D パーセンタイルランクのテスト"""

    def test_top10_is_s(self) -> None:
        """上位 10% が S ランクになること"""
        vals = list(range(100))
        series = pd.Series(vals, dtype=float)
        result = _percentile_rank(series)
        # 90 以上が S
        assert result[series >= 90].eq("S").all()

    def test_bottom25_is_d(self) -> None:
        """下位 25% が D ランクになること"""
        vals = list(range(100))
        series = pd.Series(vals, dtype=float)
        result = _percentile_rank(series)
        assert result[series < 25].eq("D").all()

    def test_all_ranks_present(self) -> None:
        """S/A/B/C/D 全ランクが生成されること"""
        series = pd.Series(list(range(100)), dtype=float)
        result = _percentile_rank(series)
        assert set(result.dropna().unique()) == {"S", "A", "B", "C", "D"}

    def test_empty_series(self) -> None:
        """空の Series は NaN のみ"""
        result = _percentile_rank(pd.Series([], dtype=float))
        assert len(result) == 0

    def test_all_nan(self) -> None:
        """全 NaN 入力は全 NaN 出力"""
        series = pd.Series([np.nan, np.nan, np.nan])
        result = _percentile_rank(series)
        assert result.isna().all()


# =============================================================================
# _bin_kijun_odds_5 のテスト
# =============================================================================

class TestBinKijunOdds5:
    """前日基準単勝オッズ 5 倍刻みビンのテスト"""

    @pytest.mark.parametrize("odds,expected", [
        (2.5, "1-4.9"),
        (5.0, "5-9.9"),
        (9.9, "5-9.9"),
        (10.0, "10-14.9"),
        (15.0, "15-19.9"),
        (25.0, "20-29.9"),
        (40.0, "30-49.9"),
        (75.0, "50-99.9"),
        (150.0, "100以上"),
    ])
    def test_bins(self, odds: float, expected: str) -> None:
        """各ビン境界値のテスト"""
        df = pd.DataFrame({"kijun_odds_tansho_joa": [odds]})
        result = _bin_kijun_odds_5(df)
        assert result.iloc[0] == expected

    def test_missing_column(self) -> None:
        """カラムなし → NaN"""
        df = pd.DataFrame({"dummy": [1]})
        result = _bin_kijun_odds_5(df)
        assert result.isna().all()

    def test_nan_input(self) -> None:
        """NaN 入力は NaN"""
        df = pd.DataFrame({"kijun_odds_tansho_joa": [np.nan]})
        result = _bin_kijun_odds_5(df)
        assert pd.isna(result.iloc[0])


# =============================================================================
# _bin_ls_shisu_4 のテスト
# =============================================================================

class TestBinLsShisu4:
    """LS指数 4 刻みビンのテスト"""

    @pytest.mark.parametrize("ls,expected_prefix", [
        (0, "0"),
        (3, "0"),
        (4, "4"),
        (7, "4"),
        (8, "8"),
        (16, "16"),
        (20, "20"),
    ])
    def test_bin_start(self, ls: int, expected_prefix: str) -> None:
        """ビン開始値のテスト"""
        df = pd.DataFrame({"ls_shisu_joa": [float(ls)]})
        result = _bin_ls_shisu_4(df)
        assert result.iloc[0].startswith(expected_prefix)

    def test_format(self) -> None:
        """ビン形式が 'X-Y' 形式であること"""
        df = pd.DataFrame({"ls_shisu_joa": [16.0]})
        result = _bin_ls_shisu_4(df)
        assert "-" in result.iloc[0]

    def test_bin_width_4(self) -> None:
        """ビン幅が 4 であること"""
        df = pd.DataFrame({"ls_shisu_joa": [16.0]})
        result = _bin_ls_shisu_4(df)
        parts = result.iloc[0].split("-")
        assert int(parts[1]) - int(parts[0]) == 3  # 16-19: 19-16=3

    def test_missing_column(self) -> None:
        """カラムなし → NaN"""
        df = pd.DataFrame({"dummy": [1]})
        result = _bin_ls_shisu_4(df)
        assert result.isna().all()


# =============================================================================
# _compute_idm_rank のテスト
# =============================================================================

class TestComputeIdmRank:
    """IDM ランク（S/A/B/C/D）のテスト"""

    def test_uses_kyi_idm_raw(self) -> None:
        """kyi_idm_raw カラムが優先されること"""
        df = pd.DataFrame({"kyi_idm_raw": list(range(100))})
        result = _compute_idm_rank(df)
        assert set(result.dropna().unique()) == {"S", "A", "B", "C", "D"}

    def test_falls_back_to_kyi_idm(self) -> None:
        """kyi_idm_raw がなければ kyi_idm を使うこと"""
        df = pd.DataFrame({"kyi_idm": list(range(100))})
        result = _compute_idm_rank(df)
        assert not result.dropna().empty

    def test_falls_back_to_idm(self) -> None:
        """kyi_idm もなければ idm を使うこと"""
        df = pd.DataFrame({"idm": list(range(100))})
        result = _compute_idm_rank(df)
        assert not result.dropna().empty

    def test_no_idm_column(self) -> None:
        """IDM カラムが何もない場合は NaN"""
        df = pd.DataFrame({"dummy": [1, 2, 3]})
        result = _compute_idm_rank(df)
        assert result.isna().all()


# =============================================================================
# derive_all_factors のモックテスト
# =============================================================================

class TestDeriveAllFactors:
    """derive_all_factors() のテスト（DB モック版）"""

    @pytest.fixture
    def mock_conn(self, monkeypatch) -> object:
        """
        DB を使わないモック接続。
        各 _load_extra_from_* 関数を空の DataFrame を返すようにパッチ。
        """
        import roi_pipeline.engine.derived_factors as module

        monkeypatch.setattr(module, "_load_extra_from_kyi_fixed", lambda conn, df: pd.DataFrame())
        monkeypatch.setattr(module, "_load_extra_from_joa", lambda conn, df: pd.DataFrame())
        monkeypatch.setattr(module, "_load_extra_from_joa_fixed", lambda conn, df: pd.DataFrame())
        monkeypatch.setattr(module, "_load_extra_from_kyi_raw", lambda conn, df: pd.DataFrame())
        monkeypatch.setattr(module, "_load_extra_from_sk", lambda conn, df: pd.DataFrame())
        monkeypatch.setattr(module, "_load_babajotai_from_ra", lambda conn, df: pd.DataFrame())
        monkeypatch.setattr(module, "_compute_trainer_rank", lambda conn: {})
        monkeypatch.setattr(module, "_compute_jockey_rank", lambda conn: {})
        return object()

    @pytest.fixture
    def sample_df(self) -> pd.DataFrame:
        """前走付きサンプル DataFrame"""
        return _base_df(
            babajotai_code_shiba=["1", "3"],
            babajotai_code_dirt=["1", "1"],
        )

    def test_returns_dataframe(self, mock_conn, sample_df) -> None:
        """DataFrame が返ること"""
        result = derive_all_factors(sample_df, mock_conn)
        assert isinstance(result, pd.DataFrame)

    def test_row_count_unchanged(self, mock_conn, sample_df) -> None:
        """行数が変わらないこと"""
        result = derive_all_factors(sample_df, mock_conn)
        assert len(result) == len(sample_df)

    def test_bataiju_bin_added(self, mock_conn, sample_df) -> None:
        """bataiju_bin_20kg カラムが追加されること"""
        result = derive_all_factors(sample_df, mock_conn)
        assert "bataiju_bin_20kg" in result.columns

    def test_kyori_hendo_added(self, mock_conn, sample_df) -> None:
        """kyori_hendo カラムが追加されること"""
        result = derive_all_factors(sample_df, mock_conn)
        assert "kyori_hendo" in result.columns

    def test_kyori_kubun_added(self, mock_conn, sample_df) -> None:
        """kyori_kubun カラムが追加されること"""
        result = derive_all_factors(sample_df, mock_conn)
        assert "kyori_kubun" in result.columns

    def test_corner4_group_added(self, mock_conn, sample_df) -> None:
        """corner4_group_prev1 カラムが追加されること"""
        result = derive_all_factors(sample_df, mock_conn)
        assert "corner4_group_prev1" in result.columns

    def test_babajotai_heavy_flag_added(self, mock_conn, sample_df) -> None:
        """babajotai_heavy_flag カラムが追加されること"""
        result = derive_all_factors(sample_df, mock_conn)
        assert "babajotai_heavy_flag" in result.columns

    def test_null_cols_added_for_missing_data(self, mock_conn, sample_df) -> None:
        """DB から取得不可のカラムも NULL で追加されること"""
        result = derive_all_factors(sample_df, mock_conn)
        assert "prev_rpci" in result.columns
        assert result["prev_rpci"].isna().all()

    def test_bataiju_bin_values(self, mock_conn, sample_df) -> None:
        """bataiju_prev1=450 → '440-459', bataiju_prev1=500 → '500-519'"""
        result = derive_all_factors(sample_df, mock_conn)
        assert result["bataiju_bin_20kg"].iloc[0] == "440-459"
        assert result["bataiju_bin_20kg"].iloc[1] == "500-519"

    def test_kyori_hendo_values(self, mock_conn, sample_df) -> None:
        """kyori=1200, prev=1200 → '同'; kyori=1600, prev=1400 → '増'"""
        result = derive_all_factors(sample_df, mock_conn)
        assert result["kyori_hendo"].iloc[0] == "同"
        assert result["kyori_hendo"].iloc[1] == "増"

    def test_kyori_kubun_values(self, mock_conn, sample_df) -> None:
        """kyori=1200 → '短'; kyori=1600 → 'マイル'"""
        result = derive_all_factors(sample_df, mock_conn)
        assert result["kyori_kubun"].iloc[0] == "短"
        assert result["kyori_kubun"].iloc[1] == "マイル"

    def test_corner4_group_values(self, mock_conn, sample_df) -> None:
        """corner_4_prev1=2 → '先行'; corner_4_prev1=7 → '後方'"""
        result = derive_all_factors(sample_df, mock_conn)
        assert result["corner4_group_prev1"].iloc[0] == "先行"
        assert result["corner4_group_prev1"].iloc[1] == "後方"

    def test_babajotai_heavy_flag_values(self, mock_conn, sample_df) -> None:
        """babajotai_code_shiba=1(良)→0, babajotai_code_shiba=3(重)→1"""
        result = derive_all_factors(sample_df, mock_conn)
        assert result["babajotai_heavy_flag"].iloc[0] == 0.0
        assert result["babajotai_heavy_flag"].iloc[1] == 1.0

    def test_idm_rank_added(self, mock_conn, sample_df) -> None:
        """idm_rank カラムが追加されること（IDM なければ NaN）"""
        result = derive_all_factors(sample_df, mock_conn)
        assert "idm_rank" in result.columns

    def test_kijun_odds_bin_added(self, mock_conn, sample_df) -> None:
        """kijun_odds_bin5 カラムが追加されること（データなければ NaN）"""
        result = derive_all_factors(sample_df, mock_conn)
        assert "kijun_odds_bin5" in result.columns

    def test_ls_shisu_bin_added(self, mock_conn, sample_df) -> None:
        """ls_shisu_bin4 カラムが追加されること（データなければ NaN）"""
        result = derive_all_factors(sample_df, mock_conn)
        assert "ls_shisu_bin4" in result.columns

    def test_original_columns_preserved(self, mock_conn, sample_df) -> None:
        """元の DataFrame のカラムが保持されること"""
        result = derive_all_factors(sample_df, mock_conn)
        for col in sample_df.columns:
            assert col in result.columns

    def test_no_row_duplication(self, mock_conn, sample_df) -> None:
        """DB モックがデータを返しても行が重複しないこと"""
        # kyi_fixed が返すデータをセット
        import roi_pipeline.engine.derived_factors as module

        def fake_kyi_fixed(conn, df):
            return pd.DataFrame({
                "jrdb_race_key8": [_synth_key(df, 0), _synth_key(df, 1)],
                "umaban": ["01", "02"],
                "kyi_idm_raw": ["55", "60"],
                "kyakushitsu_kyi": ["A", "B"],
                "kishu_code_kyi": ["K001", "K002"],
                "chokyoshi_code_kyi": ["T001", "T002"],
                "ichi_shisu_kyi": ["45", "50"],
                "pace_shisu_kyi": ["40", "45"],
                "kyusha_rank_kyi": ["3", "2"],
            })

        from roi_pipeline.engine.derived_factors import _synth_jrdb_key8_series

        def _synth_key(df, idx):
            row = df.iloc[[idx]]
            return _synth_jrdb_key8_series(row).iloc[0]

        monkeypatch_local = pytest.MonkeyPatch()
        monkeypatch_local.setattr(module, "_load_extra_from_kyi_fixed", fake_kyi_fixed)
        try:
            result = derive_all_factors(sample_df, mock_conn)
            assert len(result) == len(sample_df)
        finally:
            monkeypatch_local.undo()

    def test_chokyoshi_rank_added(self, mock_conn, sample_df) -> None:
        """chokyoshi_rank カラムが追加されること"""
        result = derive_all_factors(sample_df, mock_conn)
        assert "chokyoshi_rank" in result.columns

    def test_kishu_rank_added(self, mock_conn, sample_df) -> None:
        """kishu_rank カラムが追加されること"""
        result = derive_all_factors(sample_df, mock_conn)
        assert "kishu_rank" in result.columns

    def test_rank_from_trainer_map(self, monkeypatch, sample_df) -> None:
        """調教師ランクマップが正しく適用されること"""
        import roi_pipeline.engine.derived_factors as module

        monkeypatch.setattr(module, "_load_extra_from_kyi_fixed", lambda conn, df: pd.DataFrame())
        monkeypatch.setattr(module, "_load_extra_from_joa", lambda conn, df: pd.DataFrame())
        monkeypatch.setattr(module, "_load_extra_from_joa_fixed", lambda conn, df: pd.DataFrame())
        monkeypatch.setattr(module, "_load_extra_from_kyi_raw", lambda conn, df: pd.DataFrame())
        monkeypatch.setattr(module, "_load_extra_from_sk", lambda conn, df: pd.DataFrame())
        monkeypatch.setattr(module, "_load_babajotai_from_ra", lambda conn, df: pd.DataFrame())
        monkeypatch.setattr(module, "_compute_jockey_rank", lambda conn: {})

        df = _base_df(
            babajotai_code_shiba=["1", "1"],
            babajotai_code_dirt=["1", "1"],
            chokyoshi_code_kyi=["T001", "T002"],
        )
        # _compute_trainer_rank は T001→S, T002→D を返す
        monkeypatch.setattr(
            module,
            "_compute_trainer_rank",
            lambda conn: {"T001": "S", "T002": "D"},
        )
        result = derive_all_factors(df, object())
        assert result["chokyoshi_rank"].iloc[0] == "S"
        assert result["chokyoshi_rank"].iloc[1] == "D"
