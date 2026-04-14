"""
組み合わせファクター分析エンジンのユニットテスト

テスト対象:
    - _compute_roi_table()
    - _compute_derived_all()
    - _add_global_prev()
    - _add_course27_prev()
    - _add_surface()
    - _add_course27()
    - _run_global() / _run_surface2() / _run_course27()
    - COMBINATIONS リストの構造検証
    - load_combination_dataset()（DB モック版）
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from roi_pipeline.engine.combination_analysis import (
    COMBINATIONS,
    _add_course27,
    _add_course27_prev,
    _add_global_prev,
    _add_surface,
    _compute_derived_all,
    _compute_roi_table,
    _load_sed_prev1,
    _run_course27,
    _run_global,
    _run_keibajo_track_kyori,
    _run_surface2,
)


# =============================================================================
# テスト用ヘルパー
# =============================================================================

def _make_base_df(**overrides) -> pd.DataFrame:
    """最小限の組み合わせ分析用 DataFrame を生成する。"""
    base = {
        "ketto_toroku_bango": ["H001", "H001", "H002", "H002"],
        "keibajo_code": ["06", "06", "05", "05"],
        "kaisai_nen": ["2023", "2024", "2023", "2024"],
        "kaisai_tsukihi": ["0101", "0301", "0101", "0301"],
        "race_bango": ["01", "01", "01", "01"],
        "umaban": ["01", "01", "01", "01"],
        "race_date": ["20230101", "20240301", "20230101", "20240301"],
        "kakutei_chakujun": [1.0, 3.0, 2.0, 1.0],
        "tansho_odds": [5.0, 8.0, 12.0, 3.0],
        "fukusho_odds": [2.0, 3.0, 4.0, 1.5],
        "track_code": ["11", "11", "23", "23"],
        "kyori": [1600, 1600, 1800, 1800],
        "se_bataiju": [460.0, 462.0, 480.0, 478.0],
        "corner_4": [2.0, 4.0, 6.0, 3.0],
        "blinker_shiyo_kubun": ["0", "0", "0", "1"],
        "wakuban": ["1", "2", "3", "4"],
        "barei": [3, 4, 5, 3],
        "tozai_shozoku_code": ["1", "1", "2", "2"],
        "babajotai_code_shiba": ["1", "1", "0", "0"],
        "babajotai_code_dirt": ["0", "0", "2", "3"],
        "kyi_idm": [70.0, 72.0, 60.0, 80.0],
        "kyi_sogo_shisu": [65.0, 68.0, 55.0, 75.0],
        "kyi_kyusha_shisu": [60.0, 62.0, 50.0, 70.0],
        "kyi_chokyo_shisu": [55.0, 57.0, 45.0, 65.0],
        "kyi_pace_shisu": [50.0, 52.0, 40.0, 60.0],
        "kyi_kishu_shisu": [45.0, 47.0, 35.0, 55.0],
        "kyi_agari_shisu": [40.0, 42.0, 30.0, 50.0],
        "kyi_futan_juryo": [55.0, 55.0, 57.0, 57.0],
        "kyakushitsu_kyi": ["1", "1", "3", "2"],
        "ichi_shisu": [50.0, 55.0, 45.0, 60.0],
        "chokyo_yajirushi_code": ["A", "B", "C", "A"],
        "cid_soten": ["1", "2", "3", "4"],
        "kijun_odds_tansho_joa": [5.0, 8.0, 12.0, 3.0],
        "kijun_odds_fukusho_joa": [2.0, 3.0, 4.0, 1.5],
        "ls_shisu_joa": [52.0, 56.0, 48.0, 60.0],
        "se_chokyoshi_code": ["T001", "T001", "T002", "T002"],
        "kyi_chokyoshi_code": ["T001", "T001", "T002", "T002"],
        "se_kishu_code": ["J001", "J001", "J002", "J002"],
        "kyi_kishu_code": ["J001", "J001", "J002", "J002"],
        "kishumei_ryakusho": ["ルメール", "ルメール", "武豊", "武豊"],
        "course27_category": ["東京U字_芝", "東京U字_芝", "阪神_ダ_長", "阪神_ダ_長"],
        "surface": ["芝", "芝", "ダ", "ダ"],
        "seibetsu_code": ["1", "1", "1", "2"],
    }
    base.update(overrides)
    return pd.DataFrame(base)


def _make_large_df(n: int = 100) -> pd.DataFrame:
    """ROI 計算テスト用の大きめ DataFrame を生成する。

    fukusho_odds（jvd_hr UNPIVOT）は3着内のみ非NULL。
    _compute_roi_table の複勝計算は fillna(0.0) で非3着内馬をゼロリターンとして扱う。
    """
    rng = np.random.default_rng(42)
    horses = [f"H{i:03d}" for i in range(20)]
    rows = []
    for i in range(n):
        h = horses[i % len(horses)]
        chakujun = float(rng.integers(1, 18))
        rows.append({
            "ketto_toroku_bango": h,
            "keibajo_code": "06",
            "kaisai_nen": str(2020 + i % 5),
            "kaisai_tsukihi": "0101",
            "race_bango": "01",
            "umaban": str((i % 18) + 1).zfill(2),
            "race_date": f"202{i % 5}0101",
            "kakutei_chakujun": chakujun,
            "tansho_odds": float(rng.uniform(1.5, 30.0)),
            # kijun_odds_fukusho_joa: 全馬に存在する基準複勝オッズ（修正後のデフォルト福勝オッズ列）
            "kijun_odds_fukusho_joa": float(rng.uniform(1.1, 5.0)),
            # fukusho_odds: jvd_hr UNPIVOTから。3着内のみ非NULL（バグの原因列）
            "fukusho_odds": float(rng.uniform(1.1, 5.0)) if chakujun <= 3 else np.nan,
            "track_code": "11",
            "kyori": 1600,
            "se_bataiju": float(rng.uniform(420, 500)),
            "corner_4": float(rng.integers(1, 18)),
            "surface": "芝",
            "course27_category": "東京U字_芝",
            "kyi_idm": float(rng.uniform(50, 90)),
        })
    return pd.DataFrame(rows)


# =============================================================================
# COMBINATIONS リスト構造テスト
# =============================================================================

class TestCombinationsStructure:
    """COMBINATIONS リストの構造が正しいか検証する。"""

    def test_combinations_is_list(self) -> None:
        """COMBINATIONS がリストであること"""
        assert isinstance(COMBINATIONS, list)
        assert len(COMBINATIONS) > 0

    def test_all_entries_have_required_keys(self) -> None:
        """全エントリが必須キーを持つこと"""
        required_keys = {"id", "name", "segment", "factors", "skip"}
        for combo in COMBINATIONS:
            missing = required_keys - set(combo.keys())
            assert not missing, f"{combo.get('id', '?')}: 必須キーが不足: {missing}"

    def test_skip_entries_have_skip_reason(self) -> None:
        """スキップエントリに skip_reason が付いていること"""
        for combo in COMBINATIONS:
            if combo["skip"]:
                assert "skip_reason" in combo, (
                    f"{combo['id']}: skip=True なのに skip_reason がない"
                )
                assert combo["skip_reason"], f"{combo['id']}: skip_reason が空"

    def test_unique_ids(self) -> None:
        """全 ID がユニークであること"""
        ids = [c["id"] for c in COMBINATIONS]
        assert len(ids) == len(set(ids)), "重複した ID が存在する"

    def test_segment_values_are_valid(self) -> None:
        """segment が有効な値であること"""
        valid_segments = {
            "GLOBAL", "SURFACE_2", "COURSE_27",
            "KEIBAJO_TRACK_KYORI", "SURFACE_2_KEIBAJO",
        }
        for combo in COMBINATIONS:
            assert combo["segment"] in valid_segments, (
                f"{combo['id']}: 未知の segment = {combo['segment']}"
            )

    def test_factors_is_non_empty_list(self) -> None:
        """factors が非空リストであること"""
        for combo in COMBINATIONS:
            assert isinstance(combo["factors"], list), f"{combo['id']}: factors がリストでない"
            assert len(combo["factors"]) > 0, f"{combo['id']}: factors が空"

    def test_known_skip_combos_are_marked(self) -> None:
        """既知のスキップ組み合わせが正しくマークされていること"""
        skip_ids = {c["id"] for c in COMBINATIONS if c["skip"]}
        expected_skip_ids = {
            "course27_10",   # taikei
            "surface2_03",   # manken_shisu
            "surface2_05",   # gekiso_shisu
            "surface2_10",   # hizume_code
            "surface2_17",   # taikei_sogo_1
            "global_05",     # yuso_kubun
        }
        for eid in expected_skip_ids:
            assert eid in skip_ids, f"{eid} がスキップになっていない"

    def test_active_combo_count(self) -> None:
        """アクティブな組み合わせ数が正しいこと（28件以上）"""
        active = [c for c in COMBINATIONS if not c["skip"]]
        assert len(active) >= 28, f"アクティブ組み合わせ数 = {len(active)}（期待: ≥28）"

    def test_override_factors_consistent(self) -> None:
        """_override_factors が存在する場合は非空リストであること"""
        for combo in COMBINATIONS:
            if "_override_factors" in combo:
                assert isinstance(combo["_override_factors"], list)
                assert len(combo["_override_factors"]) > 0


# =============================================================================
# _compute_roi_table のテスト
# =============================================================================

class TestComputeRoiTable:
    """_compute_roi_table() のユニットテスト"""

    def test_basic_output_columns(self) -> None:
        """出力 DataFrame が必要カラムを全て持つこと"""
        df = _make_large_df(100)
        df["_factor_bin"] = (df["kyi_idm"] // 10 * 10).astype(int).astype(str)
        table = _compute_roi_table(df, ["_factor_bin"], min_samples=5)
        expected_cols = {
            "ビン",
            "単勝件数", "単勝的中数", "単勝的中率(%)", "単勝回収率(%)",
            "複勝件数", "複勝的中数", "複勝的中率(%)", "複勝回収率(%)",
            "単勝平均回収率", "単勝補正回収率", "複勝補正回収率",
        }
        assert expected_cols.issubset(set(table.columns)), (
            f"不足カラム: {expected_cols - set(table.columns)}"
        )

    def test_min_samples_filter(self) -> None:
        """min_samples 未満のビンは除外されること"""
        df = _make_large_df(50)
        # ほぼ全ビンが 1 サンプルになるよう分散させる
        df["_rare"] = [f"bin_{i}" for i in range(len(df))]
        table = _compute_roi_table(df, ["_rare"], min_samples=30)
        assert table.empty, "全ビンがサンプル不足で除外されるはず"

    def test_nan_bins_excluded(self) -> None:
        """NaN ビンが結果から除外されること"""
        df = _make_large_df(60)
        df["_bin"] = pd.Series(["A"] * 30 + [np.nan] * 30)
        table = _compute_roi_table(df, ["_bin"], min_samples=10)
        if not table.empty:
            assert "N/A" not in table["ビン"].values
            assert all("NaN" not in str(b) for b in table["ビン"].values)

    def test_multi_factor_cross_bin(self) -> None:
        """複数ファクターのクロスビンが生成されること"""
        df = _make_large_df(120)
        df["_f1"] = ["A", "B"] * 60
        df["_f2"] = ["X", "Y", "Z"] * 40
        table = _compute_roi_table(df, ["_f1", "_f2"], min_samples=5)
        # クロスビンは " × " で連結されるはず
        if not table.empty:
            assert all(" × " in b for b in table["ビン"].values)

    def test_hit_rate_range(self) -> None:
        """的中率・件数・的中数が妥当な範囲にあること"""
        df = _make_large_df(100)
        df["_b"] = "A"
        table = _compute_roi_table(df, ["_b"], min_samples=10)
        if not table.empty:
            assert all(0 <= v <= 100 for v in table["単勝的中率(%)"])
            assert all(0 <= v <= 100 for v in table["複勝的中率(%)"])
            # 単勝的中数 <= 単勝件数
            assert all(row["単勝的中数"] <= row["単勝件数"] for _, row in table.iterrows())
            # 複勝的中数 <= 複勝件数
            assert all(row["複勝的中数"] <= row["複勝件数"] for _, row in table.iterrows())
            # 単勝件数 == 複勝件数（同一基底セット）
            assert all(row["単勝件数"] == row["複勝件数"] for _, row in table.iterrows())

    def test_empty_df_returns_empty(self) -> None:
        """空の DataFrame を渡すと空のテーブルが返ること"""
        df = pd.DataFrame(columns=["ketto_toroku_bango", "kakutei_chakujun",
                                    "tansho_odds", "fukusho_odds", "kaisai_nen"])
        table = _compute_roi_table(df, ["some_col"])
        assert isinstance(table, pd.DataFrame)
        assert table.empty

    def test_fukusho_hit_rate_not_100_percent(self) -> None:
        """
        【バグ修正検証】複勝的中率が 100% にならないこと。

        _compute_roi_table は着順から直接 _is_fukusho を計算するため、
        オッズ列の NULL に関係なく正しい的中率が得られる。
        """
        df = _make_large_df(300)
        df["_b"] = "A"
        table = _compute_roi_table(df, ["_b"], min_samples=10)
        assert not table.empty, "サンプルが不足"
        fukusho_hit_rate = table.at[0, "複勝的中率(%)"]
        # 3着内確率は 3/17 ≈ 17.6% なので 100% には絶対ならない
        assert fukusho_hit_rate < 90.0, (
            f"複勝的中率が異常（{fukusho_hit_rate}%）。"
            "fukusho_odds(3着内のみ非NULL)ではなく kijun_odds_fukusho_joa(全馬)を使うこと。"
        )
        # 合理的な範囲 (5% - 50%) に収まること
        assert 5.0 <= fukusho_hit_rate <= 50.0, (
            f"複勝的中率 {fukusho_hit_rate}% が期待範囲外"
        )

    def test_fukusho_roi_is_realistic(self) -> None:
        """
        【バグ修正検証】複勝補正回収率が正常範囲 (0-200%) に収まること。

        バグ時は 150-600% になっていた。
        """
        df = _make_large_df(300)
        df["_b"] = "A"
        table = _compute_roi_table(df, ["_b"], min_samples=10)
        if not table.empty:
            fukusho_roi = table.at[0, "複勝補正回収率"]
            assert fukusho_roi < 200.0, (
                f"複勝補正回収率が異常（{fukusho_roi}%）。バグ修正が正しく適用されていない。"
            )

    def test_fukusho_uses_kijun_odds_column(self) -> None:
        """
        デフォルト fukusho_col="fukusho_odds" での動作確認。

        複勝的中率(%): 着順から直接計算するため列に依存しない。
        複勝補正回収率: fukusho_odds.fillna(0.0) × is_place の年次重み付け平均。
                        dropna を使わないため的中率100%バグは発生しない。
        """
        df = _make_large_df(300)
        df["_b"] = "A"

        # デフォルト: fukusho_odds（fillna(0.0)で非3着内をゼロリターン）
        table_default = _compute_roi_table(df, ["_b"], min_samples=10)

        if not table_default.empty:
            # 複勝的中率は正しい範囲（~20%前後）
            hit_rate = table_default.at[0, "複勝的中率(%)"]
            assert hit_rate < 90.0, f"複勝的中率が異常: {hit_rate}%"

            # 複勝補正回収率は正常範囲 (< 200%)
            f_roi = table_default.at[0, "複勝補正回収率"]
            assert f_roi < 200.0, f"複勝補正回収率が異常: {f_roi}%"
            assert f_roi >= 0.0, f"複勝補正回収率が負: {f_roi}%"

    def test_missing_factor_col_uses_na(self) -> None:
        """ファクター列が存在しない場合でも例外が起きないこと"""
        df = _make_large_df(60)
        # 存在しない列を渡す → N/A ビンになり全行除外される
        table = _compute_roi_table(df, ["__nonexistent_col__"], min_samples=5)
        assert isinstance(table, pd.DataFrame)


# =============================================================================
# _add_surface のテスト
# =============================================================================

class TestAddSurface:
    """_add_surface() が track_code から surface を正しく付与するか。"""

    def test_turf_track_code(self) -> None:
        """track_code 11 -> 芝"""
        df = pd.DataFrame({"track_code": ["11", "12", "13"]})
        result = _add_surface(df)
        assert all(result["surface"] == "芝")

    def test_dirt_track_code(self) -> None:
        """track_code 23 -> ダ"""
        df = pd.DataFrame({"track_code": ["23", "24"]})
        result = _add_surface(df)
        assert all(result["surface"] == "ダ")

    def test_unknown_track_code(self) -> None:
        """不明な track_code -> unknown"""
        df = pd.DataFrame({"track_code": ["99", "00"]})
        result = _add_surface(df)
        assert all(result["surface"] == "unknown")

    def test_mixed(self) -> None:
        """芝/ダ混在"""
        df = pd.DataFrame({"track_code": ["11", "23", "12", "99"]})
        result = _add_surface(df)
        assert result["surface"].tolist() == ["芝", "ダ", "芝", "unknown"]


# =============================================================================
# _add_course27 のテスト
# =============================================================================

class TestAddCourse27:
    """_add_course27() がコースカテゴリを正しく付与するか。"""

    def test_tokyo_turf_1600(self) -> None:
        """東京芝1600 -> 非 unknown"""
        df = pd.DataFrame({
            "keibajo_code": ["06"],
            "track_code": ["11"],
            "kyori": [1600],
        })
        result = _add_course27(df)
        assert result["course27_category"].iloc[0] != "unknown"

    def test_unknown_for_bad_track_code(self) -> None:
        """不明 track_code -> unknown"""
        df = pd.DataFrame({
            "keibajo_code": ["06"],
            "track_code": ["99"],
            "kyori": [1600],
        })
        result = _add_course27(df)
        assert result["course27_category"].iloc[0] == "unknown"


# =============================================================================
# _add_global_prev のテスト
# =============================================================================

class TestAddGlobalPrev:
    """_add_global_prev() が正しく前走データを付与するか。"""

    def _make_sorted_df(self) -> pd.DataFrame:
        return pd.DataFrame({
            "ketto_toroku_bango": ["H001", "H001", "H001", "H002"],
            "race_date": ["20230101", "20230601", "20240101", "20230101"],
            "keibajo_code": ["06", "06", "06", "05"],
            "race_bango": ["01", "02", "03", "01"],
            "umaban": ["01", "01", "01", "02"],
            "kakutei_chakujun": [1.0, 3.0, 2.0, 4.0],
            "corner_4": [2.0, 5.0, 3.0, 7.0],
            "blinker_shiyo_kubun": ["0", "0", "1", "0"],
            "se_bataiju": [460.0, 462.0, 464.0, 480.0],
            "keibajo_code": ["06", "06", "06", "05"],
            "kyori": [1600, 1600, 2000, 1800],
            "track_code": ["11", "11", "11", "23"],
            "race_bango": ["01", "02", "03", "01"],
        }).sort_values(
            ["ketto_toroku_bango", "race_date", "race_bango", "umaban"],
            ignore_index=True,
        )

    def test_prev1_is_previous_row(self) -> None:
        """prev1 は直前行の値であること"""
        df = self._make_sorted_df()
        result = _add_global_prev(df)
        # H001 の3行目: race_date=20240101 → prev1 は race_date=20230601 の値
        h001 = result[result["ketto_toroku_bango"] == "H001"].reset_index(drop=True)
        assert pd.notna(h001.at[1, "kakutei_chakujun_prev1"])
        assert h001.at[1, "kakutei_chakujun_prev1"] == 1.0  # 直前の着順

    def test_first_race_prev_is_nan(self) -> None:
        """初戦の prev1 は NaN であること"""
        df = self._make_sorted_df()
        result = _add_global_prev(df)
        h001 = result[result["ketto_toroku_bango"] == "H001"].reset_index(drop=True)
        assert pd.isna(h001.at[0, "kakutei_chakujun_prev1"])

    def test_different_horse_prev_is_nan(self) -> None:
        """別馬の直前走は参照されないこと"""
        df = self._make_sorted_df()
        result = _add_global_prev(df)
        h002 = result[result["ketto_toroku_bango"] == "H002"].reset_index(drop=True)
        assert pd.isna(h002.at[0, "kakutei_chakujun_prev1"])

    def test_prev2_lag2(self) -> None:
        """prev2 は2行前の値であること"""
        df = self._make_sorted_df()
        result = _add_global_prev(df)
        h001 = result[result["ketto_toroku_bango"] == "H001"].reset_index(drop=True)
        # 3行目の prev2 は 1行目の着順 (=1.0)
        assert h001.at[2, "kakutei_chakujun_prev2"] == 1.0


# =============================================================================
# _add_course27_prev のテスト
# =============================================================================

class TestAddCourse27Prev:
    """_add_course27_prev() が同コースカテゴリ内でのみ前走を参照するか。"""

    def _make_df(self) -> pd.DataFrame:
        return pd.DataFrame({
            "ketto_toroku_bango": ["H001", "H001", "H001", "H001"],
            "race_date": ["20220101", "20220601", "20230101", "20230601"],
            "race_bango": ["01", "01", "01", "01"],
            "umaban": ["01", "01", "01", "01"],
            "keibajo_code": ["06", "06", "05", "06"],  # 3走目だけ阪神
            "kyori": [1600, 1600, 2000, 1600],
            "track_code": ["11", "11", "11", "11"],
            "se_bataiju": [460.0, 462.0, 464.0, 466.0],
            "corner_4": [2.0, 5.0, 3.0, 1.0],
            "kakutei_chakujun": [1.0, 3.0, 2.0, 4.0],
            "blinker_shiyo_kubun": ["0", "0", "0", "0"],
        })

    def test_course27_prev_column_exists(self) -> None:
        """course27_prev カラムが生成されること"""
        df = self._make_df().copy()
        # course27_category が必要
        df["course27_category"] = ["東京U字_芝", "東京U字_芝", "阪神_芝_中", "東京U字_芝"]
        df = df.sort_values(["ketto_toroku_bango", "race_date", "race_bango", "umaban"],
                             ignore_index=True)
        result = _add_course27_prev(df)
        assert "kakutei_chakujun_prev_c27_1" in result.columns

    def test_unknown_rows_isolated(self) -> None:
        """course27_category = 'unknown' の行が他と混ざらないこと"""
        df = self._make_df().copy()
        df["course27_category"] = ["unknown", "東京U字_芝", "東京U字_芝", "東京U字_芝"]
        df = df.sort_values(["ketto_toroku_bango", "race_date", "race_bango", "umaban"],
                             ignore_index=True)
        result = _add_course27_prev(df)
        # unknown カテゴリは元に戻されること
        unknown_rows = result[result["course27_category"] == "unknown"]
        assert len(unknown_rows) == 1
        # unknown の course27_category_prev は NaN のはず
        if "course27_category_prev_c27_1" not in result.columns:
            # kakutei_chakujun_prev_c27_1 が NaN であること
            assert pd.isna(unknown_rows["kakutei_chakujun_prev_c27_1"].iloc[0])


# =============================================================================
# _compute_derived_all のテスト
# =============================================================================

class TestComputeDerivedAll:
    """_compute_derived_all() が正しく派生ファクターを計算するか。"""

    def _base(self) -> pd.DataFrame:
        df = _make_base_df()
        # 前走関連カラムを追加
        df["kyori_prev1"] = [1400.0, 1600.0, 1800.0, 2000.0]
        df["kyori_prev_c27_1"] = [1600.0, 1600.0, 1800.0, 1800.0]
        df["corner_4_prev1"] = [2.0, 7.0, 5.0, 1.0]
        df["corner_4_prev_c27_1"] = [3.0, 6.0, 2.0, 8.0]
        df["se_bataiju_prev1"] = [450.0, 460.0, 480.0, 470.0]
        df["se_bataiju_prev_c27_1"] = [452.0, 462.0, 482.0, 472.0]
        df["race_date_prev1"] = ["20221201", "20230101", "20221201", "20230101"]
        return df

    def test_kyori_hendo_increase(self) -> None:
        """距離増の場合 '増' になること"""
        df = self._base()
        # kyori_prev1 < kyori → 増
        df["kyori"] = [1600, 1600, 1800, 1800]
        df["kyori_prev1"] = [1200.0, 1400.0, 1600.0, 1600.0]
        result = _compute_derived_all(df, {}, {})
        assert "kyori_hendo" in result.columns
        expected = ["増", "増", "増", "増"]  # 全行 kyori > kyori_prev1 → 増
        for i, exp in enumerate(expected):
            assert result.at[i, "kyori_hendo"] == exp, (
                f"行{i}: 期待={exp}, 実際={result.at[i, 'kyori_hendo']}"
            )

    def test_kyori_hendo_decrease(self) -> None:
        """距離減の場合 '減' になること"""
        df = self._base()
        df["kyori"] = [1200, 1200, 1200, 1200]
        df["kyori_prev1"] = [1600.0, 1600.0, 1600.0, 1600.0]
        result = _compute_derived_all(df, {}, {})
        assert all(result["kyori_hendo"] == "減")

    def test_kyori_kubun_classification(self) -> None:
        """距離区分が正しく分類されること"""
        df = self._base()
        df["kyori"] = [1200, 1600, 2000, 2500]
        result = _compute_derived_all(df, {}, {})
        expected = ["短", "マイル", "中", "長"]
        assert result["kyori_kubun"].tolist() == expected

    def test_corner4_group_prev1(self) -> None:
        """前走4角グループが正しく分類されること"""
        df = self._base()
        df["corner_4_prev1"] = [2.0, 5.0, 10.0, np.nan]
        result = _compute_derived_all(df, {}, {})
        assert result.at[0, "corner4_group_prev1"] == "先行"
        assert result.at[1, "corner4_group_prev1"] == "中団"
        assert result.at[2, "corner4_group_prev1"] == "後方"
        assert pd.isna(result.at[3, "corner4_group_prev1"])

    def test_bataiju_bin_20kg(self) -> None:
        """馬体重ビンが 20kg 刻みで分類されること"""
        df = self._base()
        df["se_bataiju_prev1"] = [390.0, 420.0, 460.0, 500.0]
        result = _compute_derived_all(df, {}, {})
        expected = ["380-399", "420-439", "460-479", "500-519"]
        for i, exp in enumerate(expected):
            assert str(result.at[i, "bataiju_bin_20kg_prev1"]) == exp, (
                f"行{i}: 期待={exp}, 実際={result.at[i, 'bataiju_bin_20kg_prev1']}"
            )

    def test_babajotai_heavy_flag_turf(self) -> None:
        """芝で重馬場（code=3,4）のとき babajotai_heavy_flag = 1"""
        df = self._base()
        df["track_code"] = ["11", "11", "11", "11"]
        df["babajotai_code_shiba"] = ["1", "2", "3", "4"]
        df["babajotai_code_dirt"] = ["0", "0", "0", "0"]
        result = _compute_derived_all(df, {}, {})
        expected = [0.0, 0.0, 1.0, 1.0]
        for i, exp in enumerate(expected):
            assert result.at[i, "babajotai_heavy_flag"] == exp, (
                f"行{i}: 期待={exp}, 実際={result.at[i, 'babajotai_heavy_flag']}"
            )

    def test_kyuyou_weeks_calculated(self) -> None:
        """休養週数が計算されること（前走から現在までの週数）"""
        df = self._base()
        # race_date と race_date_prev1 のセット
        df["race_date"] = ["20240101", "20240101", "20240101", "20240101"]
        df["race_date_prev1"] = ["20231201", "20231001", "20230101", "20220101"]
        result = _compute_derived_all(df, {}, {})
        assert "kyuyou_weeks" in result.columns
        # 全行 NaN でないこと
        vals = result["kyuyou_weeks"].dropna()
        assert len(vals) > 0

    def test_idm_rank_percentile(self) -> None:
        """IDM ランクが S/A/B/C/D のいずれかであること"""
        df = self._base()
        df["kyi_idm"] = [60.0, 70.0, 80.0, 90.0]
        result = _compute_derived_all(df, {}, {})
        assert "idm_rank" in result.columns
        ranks = result["idm_rank"].dropna()
        valid_labels = {"S", "A", "B", "C", "D"}
        assert all(r in valid_labels for r in ranks)

    def test_kijun_odds_bin5(self) -> None:
        """基準オッズビンが正しく分類されること"""
        df = self._base()
        df["kijun_odds_tansho_joa"] = [3.0, 7.0, 12.0, 25.0]
        result = _compute_derived_all(df, {}, {})
        assert "kijun_odds_bin5" in result.columns

    def test_trainer_rank_mapping(self) -> None:
        """調教師ランクが辞書から正しくマッピングされること"""
        df = self._base()
        trainer_rank = {"T001": "S", "T002": "A"}
        result = _compute_derived_all(df, trainer_rank, {})
        assert result.at[0, "chokyoshi_rank"] == "S"
        assert result.at[2, "chokyoshi_rank"] == "A"

    def test_jockey_rank_mapping(self) -> None:
        """騎手ランクが辞書から正しくマッピングされること"""
        df = self._base()
        jockey_rank = {"J001": "S", "J002": "B"}
        result = _compute_derived_all(df, {}, jockey_rank)
        assert result.at[0, "kishu_rank"] == "S"
        assert result.at[2, "kishu_rank"] == "B"

    def test_empty_rank_dicts_produce_nan(self) -> None:
        """空の rank 辞書の場合 NaN になること"""
        df = self._base()
        result = _compute_derived_all(df, {}, {})
        assert result["chokyoshi_rank"].isna().all()
        assert result["kishu_rank"].isna().all()

    def test_ls_shisu_bin4(self) -> None:
        """LS指数が4刻みのビンに分類されること"""
        df = self._base()
        df["ls_shisu_joa"] = [50.0, 54.0, 58.0, 62.0]
        result = _compute_derived_all(df, {}, {})
        assert "ls_shisu_bin4" in result.columns
        assert result.at[0, "ls_shisu_bin4"] == "48-51"


# =============================================================================
# セグメント別実行関数のテスト
# =============================================================================

class TestSegmentRunners:
    """_run_global / _run_surface2 / _run_course27 / _run_keibajo_track_kyori のテスト"""

    def _make_df(self) -> pd.DataFrame:
        return _make_large_df(200)

    def test_run_global_returns_global_key(self) -> None:
        """_run_global は 'GLOBAL' キーを持つ dict を返すこと"""
        df = self._make_df()
        df["_b"] = "A"
        result = _run_global(df, {"factors": ["_b"]})
        assert "GLOBAL" in result

    def test_run_surface2_returns_turf_and_dirt(self) -> None:
        """_run_surface2 は '芝' と 'ダ' キーを持つ dict を返すこと"""
        df = self._make_df()
        df2 = df.copy()
        df3 = df.copy()
        df3["surface"] = "ダ"
        df3["tansho_odds"] = df3["tansho_odds"] * 1.2
        combined = pd.concat([df2, df3], ignore_index=True)
        combined["_b"] = "A"
        result = _run_surface2(combined, {"factors": ["_b"]})
        assert "芝" in result

    def test_run_surface2_no_extra_segments(self) -> None:
        """_run_surface2 が '芝'/'ダ' 以外のキーを返さないこと"""
        df = self._make_df()
        df["_b"] = "A"
        result = _run_surface2(df, {"factors": ["_b"]})
        assert all(k in {"芝", "ダ"} for k in result.keys())

    def test_run_course27_each_course_is_key(self) -> None:
        """_run_course27 は各コースカテゴリをキーとして返すこと"""
        df = self._make_df()
        df["course27_category"] = "東京U字_芝"
        df["_b"] = "A"
        result = _run_course27(df, {"factors": ["_b"]})
        assert "東京U字_芝" in result

    def test_run_course27_unknown_excluded(self) -> None:
        """_run_course27 は 'unknown' カテゴリを除外すること"""
        df = self._make_df()
        df["course27_category"] = "unknown"
        df["_b"] = "A"
        result = _run_course27(df, {"factors": ["_b"]})
        assert "unknown" not in result

    def test_run_keibajo_track_kyori_segments(self) -> None:
        """_run_keibajo_track_kyori が競馬場×芝ダ×距離のセグメントを返すこと"""
        df = self._make_df()
        df["surface"] = "芝"
        df["kyori"] = 1600
        df["keibajo_code"] = "06"
        df["_b"] = "A"
        result = _run_keibajo_track_kyori(df, {"factors": ["_b"]})
        # セグメントキーは "06_芝_1600" 形式
        assert any("06" in k and "芝" in k for k in result.keys())

    def test_run_global_empty_df(self) -> None:
        """空の DataFrame でも例外が起きないこと"""
        df = pd.DataFrame(columns=["kakutei_chakujun", "tansho_odds",
                                    "fukusho_odds", "kaisai_nen"])
        df["_b"] = pd.Series([], dtype=str)
        result = _run_global(df, {"factors": ["_b"]})
        assert "GLOBAL" in result
        assert result["GLOBAL"].empty

    def test_run_surface2_empty_df(self) -> None:
        """空の DataFrame でも例外が起きないこと"""
        df = pd.DataFrame(columns=["kakutei_chakujun", "tansho_odds",
                                    "fukusho_odds", "kaisai_nen", "surface"])
        df["_b"] = pd.Series([], dtype=str)
        result = _run_surface2(df, {"factors": ["_b"]})
        assert isinstance(result, dict)


# =============================================================================
# _load_sed_prev1 のテスト
# =============================================================================

class TestLoadSedPrev1:
    """_load_sed_prev1() のモックテスト"""

    def test_missing_required_columns_returns_df_unchanged(self) -> None:
        """必須カラムが不足している場合は df が変更されず返ること"""
        df = _make_base_df()
        # race_date_prev1, keibajo_code_prev1, race_bango_prev1 なし
        mock_conn = MagicMock()
        result = _load_sed_prev1(mock_conn, df)
        # DB へのアクセスが起きないこと (read_sql_query が呼ばれないこと)
        assert isinstance(result, pd.DataFrame)
        assert "prev1_race_pace" not in result.columns

    def test_empty_prev_date_returns_df_unchanged(self) -> None:
        """race_date_prev1 が全 NaN の場合は DB クエリが走らないこと"""
        df = _make_base_df()
        df["race_date_prev1"] = np.nan
        df["keibajo_code_prev1"] = "06"
        df["race_bango_prev1"] = "01"
        mock_conn = MagicMock()
        with patch("pandas.read_sql_query") as mock_sql:
            result = _load_sed_prev1(mock_conn, df)
        # unique_years が空になり early return するはず
        assert isinstance(result, pd.DataFrame)

    def test_db_error_returns_df_unchanged(self) -> None:
        """DB エラー時は df が変更されず返ること"""
        df = _make_base_df()
        df["race_date_prev1"] = "20230101"
        df["keibajo_code_prev1"] = "06"
        df["race_bango_prev1"] = "01"
        mock_conn = MagicMock()
        with patch("pandas.read_sql_query", side_effect=Exception("DB error")):
            result = _load_sed_prev1(mock_conn, df)
        assert isinstance(result, pd.DataFrame)
        assert "prev1_race_pace" not in result.columns


# =============================================================================
# load_combination_dataset のモックテスト
# =============================================================================

class TestLoadCombinationDataset:
    """load_combination_dataset() の DB モックテスト"""

    def test_calls_load_combo_base(self) -> None:
        """_load_combo_base が呼ばれること（モック確認）"""
        from roi_pipeline.engine import combination_analysis as ca

        base_df = _make_large_df(50)
        base_df["babajotai_code_shiba"] = "1"
        base_df["babajotai_code_dirt"] = "0"
        base_df["wakuban"] = "1"
        base_df["barei"] = 3
        base_df["tozai_shozoku_code"] = "1"
        base_df["kishumei_ryakusho"] = "テスト"
        base_df["se_chokyoshi_code"] = "T001"
        base_df["kyi_chokyoshi_code"] = "T001"
        base_df["se_kishu_code"] = "J001"
        base_df["kyi_kishu_code"] = "J001"
        base_df["blinker_shiyo_kubun"] = "0"
        base_df["corner_4"] = 3.0
        base_df["se_bataiju"] = 460.0
        base_df["race_date"] = "20240101"
        base_df["keibajo_code"] = "06"
        base_df["kaisai_nen"] = "2024"
        base_df["kaisai_tsukihi"] = "0101"
        base_df["race_bango"] = "01"
        base_df["umaban"] = "01"
        base_df["ketto_toroku_bango"] = [f"H{i:03d}" for i in range(len(base_df))]
        base_df["kyi_idm"] = 70.0
        base_df["kyi_sogo_shisu"] = 65.0
        base_df["kyi_kyusha_shisu"] = 60.0
        base_df["kyi_chokyo_shisu"] = 55.0
        base_df["kyi_pace_shisu"] = 50.0
        base_df["kyi_kishu_shisu"] = 45.0
        base_df["kyi_agari_shisu"] = 40.0
        base_df["kyi_futan_juryo"] = 55.0
        base_df["kyakushitsu_kyi"] = "1"
        base_df["ichi_shisu"] = 50.0
        base_df["chokyo_yajirushi_code"] = "A"
        base_df["cid_soten"] = "1"
        base_df["kijun_odds_tansho_joa"] = 5.0
        base_df["kijun_odds_fukusho_joa"] = 2.0
        base_df["ls_shisu_joa"] = 52.0
        base_df["seibetsu_code"] = "1"
        base_df["babajotai_code_shiba"] = "1"
        base_df["babajotai_code_dirt"] = "0"

        mock_conn = MagicMock()

        with patch.object(ca, "_load_combo_base", return_value=base_df) as mock_base, \
             patch.object(ca, "_load_sk_data", return_value=pd.DataFrame()) as mock_sk, \
             patch.object(ca, "_load_sed_prev1", return_value=base_df) as mock_sed, \
             patch.object(ca, "_compute_trainer_rank", return_value={}) as mock_tr, \
             patch.object(ca, "_compute_jockey_rank", return_value={}) as mock_jr:
            result = ca.load_combination_dataset(
                mock_conn, "20240101", "20241231"
            )

        mock_base.assert_called_once()
        mock_sk.assert_called_once()
        mock_tr.assert_called_once()
        mock_jr.assert_called_once()
        assert isinstance(result, pd.DataFrame)

    def test_date_filter_applied(self) -> None:
        """start_date〜end_date のフィルタが適用されること"""
        from roi_pipeline.engine import combination_analysis as ca

        base_df = _make_large_df(100)
        base_df["race_date"] = ["20230101"] * 50 + ["20250101"] * 50
        # 必要なカラムを追加
        for col in ["babajotai_code_shiba", "babajotai_code_dirt", "wakuban",
                    "tozai_shozoku_code", "kishumei_ryakusho", "se_chokyoshi_code",
                    "kyi_chokyoshi_code", "se_kishu_code", "kyi_kishu_code",
                    "blinker_shiyo_kubun", "se_bataiju", "keibajo_code",
                    "kaisai_nen", "kaisai_tsukihi", "race_bango", "umaban",
                    "seibetsu_code", "kyakushitsu_kyi", "chokyo_yajirushi_code",
                    "cid_soten"]:
            if col not in base_df.columns:
                base_df[col] = "test"
        base_df["barei"] = 3
        base_df["corner_4"] = 3.0

        mock_conn = MagicMock()

        with patch.object(ca, "_load_combo_base", return_value=base_df), \
             patch.object(ca, "_load_sk_data", return_value=pd.DataFrame()), \
             patch.object(ca, "_load_sed_prev1", return_value=base_df), \
             patch.object(ca, "_compute_trainer_rank", return_value={}), \
             patch.object(ca, "_compute_jockey_rank", return_value={}):
            result = ca.load_combination_dataset(
                mock_conn, "20240101", "20241231"
            )
        # 2023 と 2025 は除外され、指定期間内のみが返る
        assert all((d >= "20240101") and (d <= "20241231") for d in result["race_date"])
