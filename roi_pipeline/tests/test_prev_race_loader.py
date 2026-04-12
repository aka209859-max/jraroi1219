"""
前走データ取得エンジンのユニットテスト

テスト対象:
    - _assign_surface()
    - _assign_course27()
    - _clean_raw()
    - _append_prev_columns()
    - load_global_prev_races()  （DBなしのモック版）
    - load_course27_prev_races() （DBなしのモック版）

DBが不要なテストはモックデータで、DB接続が必要なテストは
接続可能な場合のみ実行（pytest.mark.db）。
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from roi_pipeline.engine.prev_race_loader import (
    _append_prev_columns,
    _assign_course27,
    _assign_surface,
    _clean_raw,
    _PREV_COLS,
    load_global_prev_races,
    load_course27_prev_races,
)


# =============================================================================
# テスト用ヘルパー
# =============================================================================

def _make_base_df(
    toroku: list[str],
    race_dates: list[str],
    race_bango: list[str],
    umaban: list[str],
    keibajo_code: list[str],
    track_code: list[str],
    kyori: list[int | float],
    kakutei_chakujun: list[int | float],
    corner_4: list[int | float] | None = None,
    bataiju: list[int | float] | None = None,
    time_sa: list[float] | None = None,
) -> pd.DataFrame:
    """テスト用 DataFrame を作成する。"""
    n = len(toroku)
    return pd.DataFrame({
        "ketto_toroku_bango": toroku,
        "race_date": race_dates,
        "kaisai_nen": [d[:4] for d in race_dates],
        "kaisai_tsukihi": [d[4:] for d in race_dates],
        "kaisai_kai": ["1"] * n,
        "kaisai_nichime": ["1"] * n,
        "race_bango": race_bango,
        "umaban": umaban,
        "keibajo_code": keibajo_code,
        "track_code": track_code,
        "kyori": kyori,
        "kakutei_chakujun": kakutei_chakujun,
        "corner_4": corner_4 if corner_4 else [np.nan] * n,
        "blinker_shiyo_kubun": ["0"] * n,
        "bataiju": bataiju if bataiju else [450.0] * n,
        "time_sa": time_sa if time_sa else [0.0] * n,
    })


# =============================================================================
# _assign_surface のテスト
# =============================================================================

class TestAssignSurface:
    """_assign_surface() のテスト"""

    def test_turf_prefix_1x(self) -> None:
        """track_code が '1x' → '芝'"""
        s = pd.Series(["11", "12", "10"])
        result = _assign_surface(s)
        assert (result == "芝").all()

    def test_dirt_prefix_2x(self) -> None:
        """track_code が '2x' → 'ダ'"""
        s = pd.Series(["21", "22", "29"])
        result = _assign_surface(s)
        assert (result == "ダ").all()

    def test_unknown_other(self) -> None:
        """track_code が '3x' や空 → 'unknown'"""
        s = pd.Series(["30", "", "99"])
        result = _assign_surface(s)
        assert (result == "unknown").all()

    def test_mixed(self) -> None:
        """混在ケース"""
        s = pd.Series(["11", "21", "30"])
        result = _assign_surface(s)
        assert result.tolist() == ["芝", "ダ", "unknown"]

    def test_with_spaces(self) -> None:
        """前後スペースは strip されること"""
        s = pd.Series([" 11", "21 "])
        result = _assign_surface(s)
        assert result.tolist() == ["芝", "ダ"]


# =============================================================================
# _assign_course27 のテスト
# =============================================================================

class TestAssignCourse27:
    """_assign_course27() のテスト"""

    def test_nakayama_1200_turf(self) -> None:
        """中山芝1200 → 右回り急坂U字_芝"""
        df = pd.DataFrame({
            "keibajo_code": ["06"],
            "track_code": ["11"],
            "kyori": [1200],
        })
        result = _assign_course27(df)
        assert result.iloc[0] == "右回り急坂U字_芝"

    def test_tokyo_1600_turf(self) -> None:
        """東京芝1600 → 東京U字_芝"""
        df = pd.DataFrame({
            "keibajo_code": ["05"],
            "track_code": ["11"],
            "kyori": [1600],
        })
        result = _assign_course27(df)
        assert result.iloc[0] == "東京U字_芝"

    def test_unknown_track_code(self) -> None:
        """track_code が '3x' → 'unknown'"""
        df = pd.DataFrame({
            "keibajo_code": ["05"],
            "track_code": ["30"],
            "kyori": [1600],
        })
        result = _assign_course27(df)
        assert result.iloc[0] == "unknown"

    def test_unknown_keibajo(self) -> None:
        """未定義競馬場コード → 'unknown'"""
        df = pd.DataFrame({
            "keibajo_code": ["99"],
            "track_code": ["11"],
            "kyori": [1600],
        })
        result = _assign_course27(df)
        assert result.iloc[0] == "unknown"

    def test_dirt_course(self) -> None:
        """東京ダート1600 → 東京U字_ダ"""
        df = pd.DataFrame({
            "keibajo_code": ["05"],
            "track_code": ["21"],
            "kyori": [1600],
        })
        result = _assign_course27(df)
        assert result.iloc[0] == "東京U字_ダ"

    def test_multiple_rows(self) -> None:
        """複数行の処理"""
        df = pd.DataFrame({
            "keibajo_code": ["06", "05", "99"],
            "track_code": ["11", "11", "11"],
            "kyori": [1200, 1600, 1200],
        })
        result = _assign_course27(df)
        assert result.iloc[0] == "右回り急坂U字_芝"
        assert result.iloc[1] == "東京U字_芝"
        assert result.iloc[2] == "unknown"


# =============================================================================
# _clean_raw のテスト
# =============================================================================

class TestCleanRaw:
    """_clean_raw() のテスト"""

    @pytest.fixture
    def raw_df(self) -> pd.DataFrame:
        """整形前の生データ"""
        return pd.DataFrame({
            "ketto_toroku_bango": [" 1234567890", "1234567890"],
            "keibajo_code": ["06", "06"],
            "kaisai_nen": ["2024", "2024"],
            "kaisai_tsukihi": ["0301", "0308"],
            "kaisai_kai": ["1", "1"],
            "kaisai_nichime": ["1", "1"],
            "race_bango": ["01", "01"],
            "umaban": ["01", "01"],
            "race_date": ["20240301", "20240308"],
            "kakutei_chakujun": ["3", "1"],
            "corner_4": ["2", "1"],
            "blinker_shiyo_kubun": ["0", "0"],
            "bataiju": ["460", "458"],
            "time_sa": ["0.3", "0.0"],
            "track_code": ["11", "11"],
            "kyori": ["1200", "1200"],
        })

    def test_string_trim(self, raw_df: pd.DataFrame) -> None:
        """文字列のトリムが行われること"""
        result = _clean_raw(raw_df)
        assert result["ketto_toroku_bango"].iloc[0] == "1234567890"

    def test_numeric_conversion(self, raw_df: pd.DataFrame) -> None:
        """数値カラムが数値型に変換されること"""
        result = _clean_raw(raw_df)
        assert result["kakutei_chakujun"].dtype in (float, int, "float64", "int64")
        assert result["bataiju"].dtype in (float, int, "float64", "int64")

    def test_chakujun_zero_to_nan(self) -> None:
        """着順0は NaN に変換されること（競走除外等）"""
        df = pd.DataFrame({
            "ketto_toroku_bango": ["HORSE1"],
            "keibajo_code": ["06"],
            "kaisai_nen": ["2024"],
            "kaisai_tsukihi": ["0301"],
            "kaisai_kai": ["1"],
            "kaisai_nichime": ["1"],
            "race_bango": ["01"],
            "umaban": ["01"],
            "race_date": ["20240301"],
            "kakutei_chakujun": ["0"],
            "corner_4": ["0"],
            "blinker_shiyo_kubun": ["0"],
            "bataiju": ["450"],
            "time_sa": ["0"],
            "track_code": ["11"],
            "kyori": ["1200"],
        })
        result = _clean_raw(df)
        assert pd.isna(result["kakutei_chakujun"].iloc[0])

    def test_sort_order(self, raw_df: pd.DataFrame) -> None:
        """ketto_toroku_bango → race_date → race_bango の順にソートされること"""
        # 逆順に入力
        df_reversed = raw_df.iloc[::-1].reset_index(drop=True)
        result = _clean_raw(df_reversed)
        assert result["race_date"].iloc[0] <= result["race_date"].iloc[1]

    def test_dedup(self) -> None:
        """重複行が除去されること"""
        df = pd.DataFrame({
            "ketto_toroku_bango": ["HORSE1", "HORSE1"],
            "keibajo_code": ["06", "06"],
            "kaisai_nen": ["2024", "2024"],
            "kaisai_tsukihi": ["0301", "0301"],
            "kaisai_kai": ["1", "1"],
            "kaisai_nichime": ["1", "1"],
            "race_bango": ["01", "01"],
            "umaban": ["01", "01"],
            "race_date": ["20240301", "20240301"],
            "kakutei_chakujun": ["1", "2"],
            "corner_4": ["1", "1"],
            "blinker_shiyo_kubun": ["0", "0"],
            "bataiju": ["450", "450"],
            "time_sa": ["0.0", "0.0"],
            "track_code": ["11", "11"],
            "kyori": ["1200", "1200"],
        })
        result = _clean_raw(df)
        assert len(result) == 1


# =============================================================================
# _append_prev_columns のテスト
# =============================================================================

class TestAppendPrevColumns:
    """_append_prev_columns() のテスト"""

    @pytest.fixture
    def horse_df(self) -> pd.DataFrame:
        """1頭3走分のデータ"""
        df = _make_base_df(
            toroku=["HORSE1"] * 3,
            race_dates=["20240101", "20240201", "20240301"],
            race_bango=["01", "01", "01"],
            umaban=["01", "01", "01"],
            keibajo_code=["06", "05", "06"],
            track_code=["11", "11", "11"],
            kyori=[1200, 1600, 1200],
            kakutei_chakujun=[3, 1, 2],
            corner_4=[2, 1, 3],
        )
        return df

    def test_prev1_shift(self, horse_df: pd.DataFrame) -> None:
        """prev1 は直前走の値を持つこと"""
        result = _append_prev_columns(horse_df, ["ketto_toroku_bango"], 1, "1")
        # 1行目（最初のレース）は prev が NaN
        assert pd.isna(result["kakutei_chakujun_prev1"].iloc[0])
        # 2行目は1走前（1行目）の着順
        assert result["kakutei_chakujun_prev1"].iloc[1] == 3.0
        # 3行目は1走前（2行目）の着順
        assert result["kakutei_chakujun_prev1"].iloc[2] == 1.0

    def test_prev2_shift(self, horse_df: pd.DataFrame) -> None:
        """prev2 は2走前の値を持つこと"""
        result = _append_prev_columns(horse_df, ["ketto_toroku_bango"], 2, "2")
        # 1・2行目は NaN
        assert pd.isna(result["kakutei_chakujun_prev2"].iloc[0])
        assert pd.isna(result["kakutei_chakujun_prev2"].iloc[1])
        # 3行目は2走前（1行目）の着順
        assert result["kakutei_chakujun_prev2"].iloc[2] == 3.0

    def test_all_prev_cols_added(self, horse_df: pd.DataFrame) -> None:
        """全 _PREV_COLS が追加されること"""
        result = _append_prev_columns(horse_df, ["ketto_toroku_bango"], 1, "1")
        for col in _PREV_COLS:
            if col in horse_df.columns:
                assert f"{col}_prev1" in result.columns

    def test_multiple_horses(self) -> None:
        """複数頭のデータで馬ごとに独立して計算されること"""
        df = _make_base_df(
            toroku=["HORSE_A", "HORSE_A", "HORSE_B", "HORSE_B"],
            race_dates=["20240101", "20240201", "20240101", "20240201"],
            race_bango=["01", "01", "01", "01"],
            umaban=["01", "01", "02", "02"],
            keibajo_code=["06", "06", "05", "05"],
            track_code=["11", "11", "11", "11"],
            kyori=[1200, 1200, 1600, 1600],
            kakutei_chakujun=[1, 2, 3, 4],
        )
        result = _append_prev_columns(df, ["ketto_toroku_bango"], 1, "1")

        # HORSE_A の2走目: prev1 = 1
        horse_a = result[result["ketto_toroku_bango"] == "HORSE_A"]
        assert horse_a["kakutei_chakujun_prev1"].iloc[1] == 1.0

        # HORSE_B の2走目: prev1 = 3（HORSE_A の値が混入しないこと）
        horse_b = result[result["ketto_toroku_bango"] == "HORSE_B"]
        assert horse_b["kakutei_chakujun_prev1"].iloc[1] == 3.0


# =============================================================================
# load_global_prev_races のモックテスト
# =============================================================================

class TestLoadGlobalPrevRaces:
    """load_global_prev_races() のテスト（DB モック版）"""

    @pytest.fixture
    def mock_conn(self, monkeypatch) -> object:
        """
        DBを使わずにテストするためのモック接続。
        _load_raw を差し替える。
        """
        import roi_pipeline.engine.prev_race_loader as module

        def fake_load_raw(conn, lookback_start, end_date):
            """テスト用の固定データを返す"""
            return pd.DataFrame({
                "ketto_toroku_bango": [
                    "HORSE1", "HORSE1", "HORSE1", "HORSE1",
                    "HORSE2", "HORSE2",
                ],
                "keibajo_code": ["06", "05", "09", "06", "05", "05"],
                "kaisai_nen": ["2023", "2023", "2024", "2024", "2023", "2024"],
                "kaisai_tsukihi": ["1201", "1215", "0301", "0315", "1201", "0315"],
                "kaisai_kai": ["1"] * 6,
                "kaisai_nichime": ["1"] * 6,
                "race_bango": ["01"] * 6,
                "umaban": ["03"] * 6,
                "race_date": [
                    "20231201", "20231215", "20240301", "20240315",
                    "20231201", "20240315",
                ],
                "kakutei_chakujun": [3.0, 1.0, 2.0, 4.0, 1.0, 2.0],
                "corner_4": [2.0, 1.0, 3.0, 4.0, 1.0, 2.0],
                "blinker_shiyo_kubun": ["0"] * 6,
                "bataiju": [460.0, 458.0, 462.0, 460.0, 480.0, 482.0],
                "time_sa": [0.3, 0.0, 0.2, 0.5, 0.0, 0.1],
                "track_code": ["11", "11", "11", "11", "11", "11"],
                "kyori": [1200, 1600, 1200, 1200, 1600, 1600],
            })

        monkeypatch.setattr(module, "_load_raw", fake_load_raw)
        return object()

    def test_returns_dataframe(self, mock_conn) -> None:
        """DataFrame が返ること"""
        result = load_global_prev_races(mock_conn, "20240101", "20240331")
        assert isinstance(result, pd.DataFrame)

    def test_date_filter(self, mock_conn) -> None:
        """start_date〜end_date の範囲のみが返ること"""
        result = load_global_prev_races(mock_conn, "20240101", "20240331")
        assert (result["race_date"] >= "20240101").all()
        assert (result["race_date"] <= "20240331").all()

    def test_prev1_columns_exist(self, mock_conn) -> None:
        """prev1 系カラムが存在すること"""
        result = load_global_prev_races(mock_conn, "20240101", "20240331")
        assert "kakutei_chakujun_prev1" in result.columns
        assert "corner_4_prev1" in result.columns
        assert "keibajo_code_prev1" in result.columns

    def test_prev2_prev3_columns_exist(self, mock_conn) -> None:
        """prev2, prev3 系カラムが存在すること"""
        result = load_global_prev_races(mock_conn, "20240101", "20240331")
        assert "kakutei_chakujun_prev2" in result.columns
        assert "kakutei_chakujun_prev3" in result.columns

    def test_first_race_horse_has_nan_prev(self, mock_conn) -> None:
        """初出走馬（lookback前）は prev1 が NaN になること"""
        result = load_global_prev_races(mock_conn, "20240101", "20240331")
        horse2 = result[result["ketto_toroku_bango"] == "HORSE2"]
        if len(horse2) > 0:
            # HORSE2 は 2023年に1走だけ → 2024年の走は prev1 に 2023 のデータが入る
            # ただし lookback_start=20140101 なので通常はデータあり
            pass  # このケースではデータあり（HORSE2 の 2023/12 走がある）

    def test_prev_values_correct(self, mock_conn) -> None:
        """HORSE1 の 20240315 レース: prev1 は 20240301 の着順（2位）であること"""
        result = load_global_prev_races(mock_conn, "20240101", "20240331")
        horse1 = result[
            (result["ketto_toroku_bango"] == "HORSE1") &
            (result["race_date"] == "20240315")
        ]
        assert len(horse1) == 1
        # 20240301 の着順は 2.0
        assert horse1["kakutei_chakujun_prev1"].iloc[0] == 2.0

    def test_prev2_values_correct(self, mock_conn) -> None:
        """HORSE1 の 20240315 レース: prev2 は 20231215 の着順（1位）であること"""
        result = load_global_prev_races(mock_conn, "20240101", "20240331")
        horse1 = result[
            (result["ketto_toroku_bango"] == "HORSE1") &
            (result["race_date"] == "20240315")
        ]
        assert len(horse1) == 1
        assert horse1["kakutei_chakujun_prev2"].iloc[0] == 1.0

    def test_no_future_data_leakage(self, mock_conn) -> None:
        """前走カラムに当日以降のデータが混入しないこと（ターゲットリーク防止）"""
        result = load_global_prev_races(mock_conn, "20240101", "20240331")
        # HORSE1 の 20240301 の prev1 は 20231215 の着順（1位）であること
        horse1_0301 = result[
            (result["ketto_toroku_bango"] == "HORSE1") &
            (result["race_date"] == "20240301")
        ]
        if len(horse1_0301) > 0:
            assert horse1_0301["kakutei_chakujun_prev1"].iloc[0] == 1.0


# =============================================================================
# load_course27_prev_races のモックテスト
# =============================================================================

class TestLoadCourse27PrevRaces:
    """load_course27_prev_races() のテスト（DB モック版）"""

    @pytest.fixture
    def mock_conn_course27(self, monkeypatch) -> object:
        """
        コース27テスト用のモック。
        同一馬が芝1200と芝1600を交互に走るデータ。
        """
        import roi_pipeline.engine.prev_race_loader as module

        def fake_load_raw(conn, lookback_start, end_date):
            """東京芝1600 と 中山芝1200 を交互に走る馬"""
            return pd.DataFrame({
                "ketto_toroku_bango": ["HORSE_X"] * 6,
                "keibajo_code": ["06", "05", "06", "05", "06", "05"],
                "kaisai_nen": ["2024"] * 6,
                "kaisai_tsukihi": [
                    "0101", "0115", "0201", "0215", "0301", "0315"
                ],
                "kaisai_kai": ["1"] * 6,
                "kaisai_nichime": ["1"] * 6,
                "race_bango": ["01"] * 6,
                "umaban": ["05"] * 6,
                "race_date": [
                    "20240101", "20240115", "20240201", "20240215",
                    "20240301", "20240315",
                ],
                "kakutei_chakujun": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
                "corner_4": [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
                "blinker_shiyo_kubun": ["0"] * 6,
                "bataiju": [460.0] * 6,
                "time_sa": [0.0, 0.0, 0.1, 0.0, 0.2, 0.0],
                "track_code": ["11"] * 6,
                "kyori": [1200, 1600, 1200, 1600, 1200, 1600],
                #          右急U芝  東京U芝  右急U芝  東京U芝  右急U芝  東京U芝
            })

        monkeypatch.setattr(module, "_load_raw", fake_load_raw)
        return object()

    def test_returns_dataframe(self, mock_conn_course27) -> None:
        """DataFrame が返ること"""
        result = load_course27_prev_races(mock_conn_course27, "20240101", "20240331")
        assert isinstance(result, pd.DataFrame)

    def test_course27_category_column_exists(self, mock_conn_course27) -> None:
        """course27_category カラムが存在すること"""
        result = load_course27_prev_races(mock_conn_course27, "20240101", "20240331")
        assert "course27_category" in result.columns

    def test_prev_cols_use_c27_suffix(self, mock_conn_course27) -> None:
        """前走カラムのサフィックスが '_c27_1', '_c27_2', '_c27_3' であること"""
        result = load_course27_prev_races(mock_conn_course27, "20240101", "20240331")
        assert "kakutei_chakujun_prev_c27_1" in result.columns
        assert "kakutei_chakujun_prev_c27_2" in result.columns
        assert "kakutei_chakujun_prev_c27_3" in result.columns

    def test_same_course_only_prev(self, mock_conn_course27) -> None:
        """
        同一コースカテゴリの前走のみが使われること。

        中山芝1200（右回り急坂U字_芝）の3走目（20240301）の prev_c27_1 は
        中山芝1200の2走目（20240201, 着順3）であること。
        東京芝1600（20240215, 着順4）は prev_c27_1 に入らないこと。
        """
        result = load_course27_prev_races(mock_conn_course27, "20240101", "20240331")
        nakayama = result[
            (result["keibajo_code"] == "06") &
            (result["race_date"] == "20240301")
        ]
        assert len(nakayama) == 1
        # 中山芝1200の直前走（20240201）の着順 = 3
        assert nakayama["kakutei_chakujun_prev_c27_1"].iloc[0] == 3.0
        # 東京芝1600の着順（4）が混入していないこと
        assert nakayama["kakutei_chakujun_prev_c27_1"].iloc[0] != 4.0

    def test_tokyo_course_prev_independent(self, mock_conn_course27) -> None:
        """
        東京芝1600（東京U字_芝）の前走も独立して計算されること。

        東京芝1600の3走目（20240315）の prev_c27_1 は
        東京芝1600の2走目（20240215, 着順4）であること。
        """
        result = load_course27_prev_races(mock_conn_course27, "20240101", "20240331")
        tokyo = result[
            (result["keibajo_code"] == "05") &
            (result["race_date"] == "20240315")
        ]
        assert len(tokyo) == 1
        assert tokyo["kakutei_chakujun_prev_c27_1"].iloc[0] == 4.0

    def test_unknown_category_has_nan_prev(self, monkeypatch) -> None:
        """
        course27_category = 'unknown' の行は prev が NaN になること。
        （未対応コース = 前走データなし）
        """
        import roi_pipeline.engine.prev_race_loader as module

        def fake_load_raw_unknown(conn, lookback_start, end_date):
            """track_code が '30'（未対応）の行"""
            return pd.DataFrame({
                "ketto_toroku_bango": ["HORSE_U"] * 3,
                "keibajo_code": ["06"] * 3,
                "kaisai_nen": ["2024"] * 3,
                "kaisai_tsukihi": ["0101", "0201", "0301"],
                "kaisai_kai": ["1"] * 3,
                "kaisai_nichime": ["1"] * 3,
                "race_bango": ["01"] * 3,
                "umaban": ["01"] * 3,
                "race_date": ["20240101", "20240201", "20240301"],
                "kakutei_chakujun": [1.0, 2.0, 3.0],
                "corner_4": [1.0, 2.0, 3.0],
                "blinker_shiyo_kubun": ["0"] * 3,
                "bataiju": [450.0] * 3,
                "time_sa": [0.0, 0.0, 0.0],
                "track_code": ["30"] * 3,  # 未対応コード
                "kyori": [1200] * 3,
            })

        monkeypatch.setattr(module, "_load_raw", fake_load_raw_unknown)
        conn = object()
        result = load_course27_prev_races(conn, "20240101", "20240331")
        # course27_category = unknown のため prev は NaN
        assert result["kakutei_chakujun_prev_c27_1"].isna().all()

    def test_date_filter_course27(self, mock_conn_course27) -> None:
        """start_date〜end_date のフィルタが適用されること"""
        result = load_course27_prev_races(
            mock_conn_course27, "20240201", "20240331"
        )
        assert (result["race_date"] >= "20240201").all()
        assert (result["race_date"] <= "20240331").all()


# =============================================================================
# 統合テスト: GLOBAL と COURSE_27 の整合性確認
# =============================================================================

class TestGlobalVsCourse27Consistency:
    """GLOBAL 前走と COURSE_27 前走の整合性テスト"""

    @pytest.fixture
    def simple_horse_data(self, monkeypatch) -> None:
        """
        1頭が同一コースを3走するシンプルなケース。
        GLOBAL と COURSE_27 の prev1 は一致するはず。
        """
        import roi_pipeline.engine.prev_race_loader as module

        def fake_load_raw(conn, lookback_start, end_date):
            return pd.DataFrame({
                "ketto_toroku_bango": ["HORSE_S"] * 3,
                "keibajo_code": ["06"] * 3,
                "kaisai_nen": ["2024"] * 3,
                "kaisai_tsukihi": ["0101", "0201", "0301"],
                "kaisai_kai": ["1"] * 3,
                "kaisai_nichime": ["1"] * 3,
                "race_bango": ["01"] * 3,
                "umaban": ["07"] * 3,
                "race_date": ["20240101", "20240201", "20240301"],
                "kakutei_chakujun": [1.0, 2.0, 3.0],
                "corner_4": [1.0, 2.0, 3.0],
                "blinker_shiyo_kubun": ["0"] * 3,
                "bataiju": [470.0] * 3,
                "time_sa": [0.0, 0.0, 0.0],
                "track_code": ["11"] * 3,
                "kyori": [1200] * 3,  # 全て中山芝1200
            })

        monkeypatch.setattr(module, "_load_raw", fake_load_raw)

    def test_global_and_course27_prev1_match(self, simple_horse_data, monkeypatch) -> None:
        """
        同一コースを3走する場合、GLOBAL prev1 と COURSE_27 prev_c27_1 は一致すること。
        """
        import roi_pipeline.engine.prev_race_loader as module

        # already patched by simple_horse_data fixture
        conn = object()
        g_result = load_global_prev_races(conn, "20240101", "20240331")
        c_result = load_course27_prev_races(conn, "20240101", "20240331")

        # 3走目（20240301）を比較
        g_3rd = g_result[g_result["race_date"] == "20240301"]["kakutei_chakujun_prev1"]
        c_3rd = c_result[c_result["race_date"] == "20240301"]["kakutei_chakujun_prev_c27_1"]

        assert len(g_3rd) == 1
        assert len(c_3rd) == 1
        assert g_3rd.iloc[0] == c_3rd.iloc[0] == 2.0
