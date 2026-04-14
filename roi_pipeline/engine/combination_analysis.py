"""
組み合わせファクター分析エンジン

CEOが指定したファクターの組み合わせごとに、セグメント別の
補正回収率テーブルを生成する。

セグメント種別:
  COURSE_27: 27コース分類ごと（前走は同一コース分類）
  SURFACE_2: 芝/ダートごと（前走は条件問わず直近走）
  GLOBAL: 全体（前走は条件問わず直近走）
  KEIBAJO_TRACK_KYORI: 競馬場×track_code×距離ごと

出力フォーマット（各組み合わせのビンごと）:
  ビン | 単勝件数 | 単勝的中率(%) | 複勝件数 | 複勝的中率(%) | 単勝補正回収率 | 複勝補正回収率

Usage:
    from roi_pipeline.engine.combination_analysis import run_combination_analysis
    results = run_combination_analysis(conn, "20160101", "20251231")
    # results: dict[combo_id -> dict[segment_label -> DataFrame]]
"""
from __future__ import annotations

import logging
from typing import Optional

import numpy as np
import pandas as pd

from roi_pipeline.engine.corrected_return import calc_corrected_return_rate, TARGET_PAYOUT
from roi_pipeline.config.year_weights import get_year_weight
from roi_pipeline.engine.data_loader_v2 import (
    JRA_KEIBAJO_CODES,
    JVAN_TO_JRDB_RACE_KEY8,
    _build_fukusho_unpivot_cte,
)
from roi_pipeline.engine.derived_factors import (
    _add_null_col,
    _bin_bataiju_20kg,
    _bin_kijun_odds_5,
    _bin_ls_shisu_4,
    _compute_babajotai_flag,
    _compute_corner4_group,
    _compute_idm_rank,
    _compute_jockey_rank,
    _compute_kyori_hendo,
    _compute_kyori_kubun,
    _compute_kyuyou_weeks,
    _compute_trainer_rank,
    _percentile_rank,
    _synth_jrdb_key8_series,
)
from roi_pipeline.engine.prev_race_loader import (
    _assign_surface,
    _assign_course27,
)

logger = logging.getLogger(__name__)

# =============================================================================
# 定数
# =============================================================================
_JRA_CODES_SQL = JRA_KEIBAJO_CODES
_LOOKBACK_START = "20140101"
_MIN_SAMPLES = 30   # ROI 表示に必要な最低サンプル数

# SKIP 理由（jrd_kyi raw は2026-03のみで歴史データなし）
_SKIP_REASON_KYI_RAW = "jrd_kyi(raw)にデータなし（2026-03-14の517行のみ）"


# =============================================================================
# データロード
# =============================================================================

def _load_combo_base(conn, lookback_start: str, end_date: str) -> pd.DataFrame:
    """
    組み合わせ分析用のベースデータを取得する。

    取得元:
      - jvd_se: 識別キー + 着順 + オッズ + 騎手/調教師/馬体情報
      - jvd_ra: 馬場条件 + track_code + kyori
      - jrd_kyi_fixed: JRDB指数（idm, kyakushitsu, ichi_shisu, futan_juryo 等）
      - jrd_joa: cid, kijun_odds_fukusho
      - jrd_joa_fixed: ls_shisu
      - jvd_hr UNPIVOT: 複勝払戻 → fukusho_odds
    """
    fukusho_cte = _build_fukusho_unpivot_cte()

    query = f"""
    WITH fukusho_pay AS (
        {fukusho_cte}
    )
    SELECT
        -- 識別キー
        se.keibajo_code,
        se.kaisai_nen,
        se.kaisai_tsukihi,
        se.kaisai_kai,
        se.kaisai_nichime,
        se.race_bango,
        se.umaban,
        TRIM(se.ketto_toroku_bango) AS ketto_toroku_bango,
        (se.kaisai_nen || se.kaisai_tsukihi) AS race_date,
        -- 結果
        se.kakutei_chakujun,
        -- オッズ（tansho は /10 で実オッズ換算）
        CAST(NULLIF(TRIM(se.tansho_odds), '') AS NUMERIC) / 10.0 AS tansho_odds,
        fp.fukusho_odds,
        -- jvd_se 追加カラム
        se.wakuban,
        se.barei,
        se.tozai_shozoku_code,
        se.kishumei_ryakusho,
        se.chokyoshi_code       AS se_chokyoshi_code,
        se.kishu_code           AS se_kishu_code,
        se.blinker_shiyo_kubun,
        se.bataiju              AS se_bataiju,
        se.corner_4,
        -- jvd_ra
        ra.track_code,
        ra.kyori,
        ra.babajotai_code_shiba,
        ra.babajotai_code_dirt,
        -- jrd_kyi_fixed
        kyi.idm                     AS kyi_idm,
        kyi.kyakushitsu             AS kyakushitsu_kyi,
        kyi.ichi_shisu              AS ichi_shisu,
        kyi.futan_juryo             AS kyi_futan_juryo,
        kyi.sogo_shisu              AS kyi_sogo_shisu,
        kyi.kyusha_shisu            AS kyi_kyusha_shisu,
        kyi.chokyo_shisu            AS kyi_chokyo_shisu,
        kyi.pace_shisu              AS kyi_pace_shisu,
        kyi.kishu_shisu             AS kyi_kishu_shisu,
        kyi.agari_shisu             AS kyi_agari_shisu,
        kyi.chokyo_yajirushi_code,
        kyi.kishu_code              AS kyi_kishu_code,
        kyi.chokyoshi_code          AS kyi_chokyoshi_code,
        kyi.seibetsu_code,
        -- jrd_joa
        joa.cid                     AS cid_soten,
        joa.kijun_odds_tansho       AS kijun_odds_tansho_joa,
        joa.kijun_odds_fukusho      AS kijun_odds_fukusho_joa,
        -- jrd_joa_fixed
        joaf.ls_shisu               AS ls_shisu_joa
    FROM jvd_se AS se
    LEFT JOIN jvd_ra AS ra
        ON  se.keibajo_code   = ra.keibajo_code
        AND se.kaisai_nen     = ra.kaisai_nen
        AND se.kaisai_tsukihi = ra.kaisai_tsukihi
        AND se.kaisai_kai     = ra.kaisai_kai
        AND se.kaisai_nichime = ra.kaisai_nichime
        AND se.race_bango     = ra.race_bango
    LEFT JOIN jrd_kyi_fixed AS kyi
        ON ({JVAN_TO_JRDB_RACE_KEY8}) = kyi.jrdb_race_key8
        AND TRIM(se.umaban) = TRIM(kyi.umaban)
    LEFT JOIN jrd_joa AS joa
        ON  TRIM(se.ketto_toroku_bango) = TRIM(joa.ketto_toroku_bango)
        AND (SUBSTRING(se.kaisai_nen, 3, 2)
             || SUBSTRING(se.kaisai_tsukihi, 3, 2)
             || SUBSTRING(se.kaisai_tsukihi, 1, 2)) = joa.race_shikonen
    LEFT JOIN jrd_joa_fixed AS joaf
        ON ({JVAN_TO_JRDB_RACE_KEY8}) = joaf.jrdb_race_key8
        AND TRIM(se.umaban) = TRIM(joaf.umaban)
    LEFT JOIN fukusho_pay AS fp
        ON  se.keibajo_code   = fp.keibajo_code
        AND se.kaisai_nen     = fp.kaisai_nen
        AND se.kaisai_tsukihi = fp.kaisai_tsukihi
        AND se.kaisai_kai     = fp.kaisai_kai
        AND se.kaisai_nichime = fp.kaisai_nichime
        AND se.race_bango     = fp.race_bango
        AND TRIM(se.umaban)   = TRIM(fp.umaban)
    WHERE se.keibajo_code IN {_JRA_CODES_SQL}
      AND (se.kaisai_nen || se.kaisai_tsukihi) >= '{lookback_start}'
      AND (se.kaisai_nen || se.kaisai_tsukihi) <= '{end_date}'
    """
    df = pd.read_sql_query(query, conn)

    # 型変換
    str_cols = ["keibajo_code", "kaisai_nen", "kaisai_tsukihi", "kaisai_kai",
                "kaisai_nichime", "race_bango", "umaban", "ketto_toroku_bango",
                "race_date", "wakuban", "tozai_shozoku_code", "kishumei_ryakusho",
                "se_chokyoshi_code", "se_kishu_code", "kyi_kishu_code",
                "kyi_chokyoshi_code", "blinker_shiyo_kubun", "track_code",
                "kyakushitsu_kyi", "seibetsu_code", "chokyo_yajirushi_code"]
    for c in str_cols:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip()

    num_cols = ["kakutei_chakujun", "se_bataiju", "barei", "corner_4",
                "kyori", "kyi_idm", "ichi_shisu", "kyi_futan_juryo",
                "kyi_sogo_shisu", "kyi_kyusha_shisu", "kyi_chokyo_shisu",
                "kyi_pace_shisu", "kyi_kishu_shisu", "kyi_agari_shisu",
                "kijun_odds_tansho_joa", "ls_shisu_joa"]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    return df


def _load_sk_data(conn, df: pd.DataFrame) -> pd.DataFrame:
    """jvd_sk から血統・産地情報を取得する。"""
    try:
        unique_ids = df["ketto_toroku_bango"].dropna().unique().tolist()
        if not unique_ids:
            return pd.DataFrame()
        ids_sql = ", ".join(f"'{i}'" for i in unique_ids)
        query = f"""
        SELECT
            TRIM(ketto_toroku_bango) AS ketto_toroku_bango,
            TRIM(ketto_joho_01a) AS chichi_name,
            TRIM(ketto_joho_05a) AS hahachichi_name,
            TRIM(sanchimei) AS sanchimei
        FROM jvd_sk
        WHERE ketto_toroku_bango IN ({ids_sql})
        """
        extra = pd.read_sql_query(query, conn)
        extra["ketto_toroku_bango"] = extra["ketto_toroku_bango"].astype(str).str.strip()
        return extra.drop_duplicates(subset=["ketto_toroku_bango"], keep="first")
    except Exception as e:
        logger.warning("combination_analysis: jvd_sk エラー: %s", e)
        return pd.DataFrame()


def _load_sed_prev1(conn, df: pd.DataFrame) -> pd.DataFrame:
    """
    jrd_sed から前走 race_pace / kyakushitsu_code を取得し df にマージして返す。

    df に race_date_prev1, keibajo_code_prev1, race_bango_prev1 が必要。
    """
    req = ["race_date_prev1", "keibajo_code_prev1", "race_bango_prev1", "ketto_toroku_bango"]
    if any(c not in df.columns for c in req):
        logger.warning("combination_analysis: sed prev1 JOIN キー不足")
        return df

    try:
        unique_years = set(
            s[:4] for s in df["race_date_prev1"].dropna().astype(str) if len(s) >= 4
        )
        if not unique_years:
            return df
        years_sql = ", ".join(f"'{y}'" for y in unique_years)
        unique_keibajo = df["keibajo_code_prev1"].dropna().astype(str).str.strip().unique().tolist()
        keibajo_sql = ", ".join(f"'{k}'" for k in unique_keibajo) if unique_keibajo else "'00'"

        query = f"""
        SELECT
            TRIM(ketto_toroku_bango) AS ketto_toroku_bango,
            TRIM(keibajo_code)       AS _sed_keibajo,
            TRIM(kaisai_nen)         AS _sed_nen,
            TRIM(kaisai_tsukihi)     AS _sed_tsukihi,
            TRIM(race_bango)         AS _sed_race_bango,
            TRIM(race_pace)          AS prev1_race_pace,
            TRIM(kyakushitsu_code)   AS prev1_kyakushitsu_sed
        FROM jrd_sed
        WHERE kaisai_nen IN ({years_sql})
          AND keibajo_code IN ({keibajo_sql})
        """
        sed = pd.read_sql_query(query, conn)
        if sed.empty:
            return df

        # df 側に JOIN キーを生成
        df = df.copy()
        df["_prev1_nen"] = df["race_date_prev1"].astype(str).str[:4]
        df["_prev1_tsukihi"] = df["race_date_prev1"].astype(str).str[4:]
        df["_k_prev1"] = df["keibajo_code_prev1"].astype(str).str.strip()
        df["_r_prev1"] = df["race_bango_prev1"].astype(str).str.strip()

        merged = df.merge(
            sed,
            left_on=["ketto_toroku_bango", "_k_prev1", "_prev1_nen", "_prev1_tsukihi", "_r_prev1"],
            right_on=["ketto_toroku_bango", "_sed_keibajo", "_sed_nen", "_sed_tsukihi", "_sed_race_bango"],
            how="left",
        )
        drop_cols = ["_prev1_nen", "_prev1_tsukihi", "_k_prev1", "_r_prev1",
                     "_sed_keibajo", "_sed_nen", "_sed_tsukihi", "_sed_race_bango"]
        merged.drop(columns=drop_cols, inplace=True, errors="ignore")
        return merged
    except Exception as e:
        logger.warning("combination_analysis: jrd_sed prev1 エラー: %s", e)
        return df


# =============================================================================
# 派生ファクター計算
# =============================================================================

def _add_surface(df: pd.DataFrame) -> pd.DataFrame:
    """track_code から surface（芝/ダ/その他）を付与する。"""
    df = df.copy()
    df["surface"] = _assign_surface(df["track_code"])
    return df


def _add_course27(df: pd.DataFrame) -> pd.DataFrame:
    """keibajo_code + track_code + kyori から course27_category を付与する。"""
    df = df.copy()
    df["course27_category"] = _assign_course27(df)
    return df


def _add_global_prev(df: pd.DataFrame) -> pd.DataFrame:
    """GLOBAL 前走（コース不問直近走）を追加する。"""
    _PREV = ["kakutei_chakujun", "corner_4", "blinker_shiyo_kubun",
             "se_bataiju", "keibajo_code", "kyori", "track_code",
             "race_date", "race_bango"]
    for lag in range(1, 4):
        for col in _PREV:
            if col in df.columns:
                df[f"{col}_prev{lag}"] = (
                    df.groupby("ketto_toroku_bango")[col].shift(lag)
                )
    return df


def _add_course27_prev(df: pd.DataFrame) -> pd.DataFrame:
    """COURSE_27 前走（同コースカテゴリ内直近走）を追加する。"""
    _PREV = ["kakutei_chakujun", "corner_4", "blinker_shiyo_kubun",
             "se_bataiju", "keibajo_code", "kyori", "track_code",
             "race_date", "race_bango"]

    # unknown 行を孤立させる（prev が NaN になるよう）
    unknown_mask = df["course27_category"] == "unknown"
    df.loc[unknown_mask, "course27_category"] = (
        "unknown_" + df.loc[unknown_mask].index.astype(str)
    )

    for lag in range(1, 4):
        for col in _PREV:
            if col in df.columns:
                df[f"{col}_prev_c27_{lag}"] = (
                    df.groupby(["ketto_toroku_bango", "course27_category"])[col].shift(lag)
                )

    # unknown を元に戻す
    df.loc[df["course27_category"].str.startswith("unknown_"), "course27_category"] = "unknown"
    return df


def _compute_derived_all(df: pd.DataFrame, trainer_rank: dict, jockey_rank: dict) -> pd.DataFrame:
    """全派生ファクターを計算して追加する（DB 不要・純計算）。"""
    df = df.copy()

    # 距離変化（GLOBAL: kyori vs kyori_prev1）
    if "kyori_prev1" in df.columns:
        cur = pd.to_numeric(df["kyori"], errors="coerce")
        prev = pd.to_numeric(df["kyori_prev1"], errors="coerce")
        diff = cur - prev
        df["kyori_hendo"] = np.where(diff > 0, "増", np.where(diff < 0, "減", "同"))
        df["kyori_hendo"] = df["kyori_hendo"].where(diff.notna() & prev.notna(), other=np.nan)

    # 距離変化（COURSE_27: kyori vs kyori_prev_c27_1）
    if "kyori_prev_c27_1" in df.columns:
        cur = pd.to_numeric(df["kyori"], errors="coerce")
        prev_c27 = pd.to_numeric(df["kyori_prev_c27_1"], errors="coerce")
        diff_c27 = cur - prev_c27
        df["kyori_hendo_c27"] = np.where(diff_c27 > 0, "増",
                                 np.where(diff_c27 < 0, "減", "同"))
        df["kyori_hendo_c27"] = df["kyori_hendo_c27"].where(
            diff_c27.notna() & prev_c27.notna(), other=np.nan
        )

    # 距離区分
    kyori_n = pd.to_numeric(df["kyori"], errors="coerce")
    df["kyori_kubun"] = np.where(
        kyori_n < 1400, "短",
        np.where(kyori_n < 1800, "マイル",
                 np.where(kyori_n < 2200, "中", "長")))
    df["kyori_kubun"] = df["kyori_kubun"].where(kyori_n.notna(), other=np.nan)

    # 前走4角順位グループ（GLOBAL prev1）
    if "corner_4_prev1" in df.columns:
        pos = pd.to_numeric(df["corner_4_prev1"], errors="coerce")
        df["corner4_group_prev1"] = np.where(
            pos <= 3, "先行",
            np.where(pos <= 6, "中団", "後方"))
        df["corner4_group_prev1"] = df["corner4_group_prev1"].where(pos.notna(), other=np.nan)

    # 前走4角順位グループ（COURSE_27 prev1）
    if "corner_4_prev_c27_1" in df.columns:
        pos_c = pd.to_numeric(df["corner_4_prev_c27_1"], errors="coerce")
        df["corner4_group_prev_c27_1"] = np.where(
            pos_c <= 3, "先行",
            np.where(pos_c <= 6, "中団", "後方"))
        df["corner4_group_prev_c27_1"] = df["corner4_group_prev_c27_1"].where(
            pos_c.notna(), other=np.nan
        )

    # 馬体重20kg刻みビン（GLOBAL prev1）
    for col_in, col_out in [("se_bataiju_prev1", "bataiju_bin_20kg_prev1"),
                             ("se_bataiju_prev_c27_1", "bataiju_bin_20kg_prev_c27_1")]:
        if col_in in df.columns:
            bataiju = pd.to_numeric(df[col_in], errors="coerce")
            bins = [0, 380, 400, 420, 440, 460, 480, 500, 520, 9999]
            labels = ["380未満", "380-399", "400-419", "420-439", "440-459",
                      "460-479", "480-499", "500-519", "520以上"]
            df[col_out] = pd.cut(bataiju, bins=bins, labels=labels, right=False)

    # 重・不良馬場フラグ
    if "babajotai_code_shiba" in df.columns and "babajotai_code_dirt" in df.columns:
        shiba_n = pd.to_numeric(
            df["babajotai_code_shiba"].astype(str).str.strip(), errors="coerce"
        )
        dirt_n = pd.to_numeric(
            df["babajotai_code_dirt"].astype(str).str.strip(), errors="coerce"
        )
        track = df["track_code"].astype(str).str.strip()
        turf = track.str.startswith("1")
        dirt = track.str.startswith("2")
        heavy_turf = shiba_n.isin([3, 4])
        heavy_dirt = dirt_n.isin([3, 4])
        df["babajotai_heavy_flag"] = np.nan
        df.loc[turf & heavy_turf, "babajotai_heavy_flag"] = 1.0
        df.loc[turf & ~heavy_turf & shiba_n.notna(), "babajotai_heavy_flag"] = 0.0
        df.loc[dirt & heavy_dirt, "babajotai_heavy_flag"] = 1.0
        df.loc[dirt & ~heavy_dirt & dirt_n.notna(), "babajotai_heavy_flag"] = 0.0

    # 休養週数
    if "race_date" in df.columns and "race_date_prev1" in df.columns:
        cur_d = pd.to_datetime(df["race_date"], format="%Y%m%d", errors="coerce")
        prv_d = pd.to_datetime(df["race_date_prev1"], format="%Y%m%d", errors="coerce")
        weeks = (cur_d - prv_d).dt.days / 7.0
        bins_k = [0, 1, 3, 5, 7, 9, 10, 9999]
        labels_k = ["1未満", "1-3未満", "3-5未満", "5-7未満", "7-9未満", "9-10未満", "10以上"]
        df["kyuyou_weeks"] = pd.cut(weeks, bins=bins_k, labels=labels_k, right=False)

    # IDM ランク
    for idm_col in ["kyi_idm", "idm"]:
        if idm_col in df.columns:
            df["idm_rank"] = _percentile_rank(pd.to_numeric(df[idm_col], errors="coerce"))
            break

    # 基準オッズビン
    if "kijun_odds_tansho_joa" in df.columns:
        odds = df["kijun_odds_tansho_joa"]
        bins_o = [0, 5, 10, 15, 20, 30, 50, 100, 99999]
        labels_o = ["1-4.9", "5-9.9", "10-14.9", "15-19.9",
                    "20-29.9", "30-49.9", "50-99.9", "100以上"]
        df["kijun_odds_bin5"] = pd.cut(odds, bins=bins_o, labels=labels_o, right=False)

    # LS指数4刻み
    if "ls_shisu_joa" in df.columns:
        ls = df["ls_shisu_joa"]
        bin_start = (ls // 4) * 4
        result = pd.Series(np.nan, index=df.index, dtype=object)
        valid = bin_start.notna()
        result[valid] = (
            bin_start[valid].astype(int).astype(str)
            + "-"
            + (bin_start[valid].astype(int) + 3).astype(str)
        )
        df["ls_shisu_bin4"] = result

    # 調教師ランク・騎手ランク
    cho_col = "kyi_chokyoshi_code" if "kyi_chokyoshi_code" in df.columns else "se_chokyoshi_code"
    if cho_col in df.columns:
        df["chokyoshi_rank"] = df[cho_col].map(trainer_rank)
    else:
        df["chokyoshi_rank"] = np.nan

    kis_col = "kyi_kishu_code" if "kyi_kishu_code" in df.columns else "se_kishu_code"
    if kis_col in df.columns:
        df["kishu_rank"] = df[kis_col].map(jockey_rank)
    else:
        df["kishu_rank"] = np.nan

    return df


# =============================================================================
# データセット統合ロード
# =============================================================================

def load_combination_dataset(
    conn,
    start_date: str,
    end_date: str,
    lookback_start: str = _LOOKBACK_START,
) -> pd.DataFrame:
    """
    組み合わせ分析用の全カラムを持つ DataFrame を構築する。

    1. jvd_se + jvd_ra + jrd_kyi_fixed + jrd_joa + jrd_joa_fixed + fukusho UNPIVOT
    2. jvd_sk（血統・産地）マージ
    3. GLOBAL prev（shift by horse）
    4. COURSE_27 prev（shift by horse × course27）
    5. jrd_sed prev1（race_pace / kyakushitsu_sed）
    6. 派生ファクター全計算
    7. 分析対象期間フィルタ（start_date〜end_date）
    """
    logger.info("combination_analysis: ベースデータロード開始")
    df = _load_combo_base(conn, lookback_start, end_date)
    logger.info("combination_analysis: ベースデータ %d 行", len(df))

    # 血統・産地
    sk = _load_sk_data(conn, df)
    if not sk.empty:
        df = df.merge(sk, on="ketto_toroku_bango", how="left")

    # surface / course27
    df = _add_surface(df)
    df = _add_course27(df)

    # ソート（shift の前提）
    df = df.sort_values(
        ["ketto_toroku_bango", "race_date", "keibajo_code", "race_bango", "umaban"],
        ignore_index=True,
    )

    # GLOBAL prev（shift by horse）
    logger.info("combination_analysis: GLOBAL 前走 shift 計算")
    df = _add_global_prev(df)

    # COURSE_27 prev（shift by horse × course27）
    logger.info("combination_analysis: COURSE_27 前走 shift 計算")
    df = _add_course27_prev(df)

    # jrd_sed prev1
    logger.info("combination_analysis: jrd_sed 前走データ取得")
    df = _load_sed_prev1(conn, df)

    # 調教師・騎手ランク（2025年通算）
    logger.info("combination_analysis: 調教師・騎手ランク計算")
    trainer_rank = _compute_trainer_rank(conn)
    jockey_rank = _compute_jockey_rank(conn)

    # 派生ファクター全計算
    logger.info("combination_analysis: 派生ファクター計算")
    df = _compute_derived_all(df, trainer_rank, jockey_rank)

    # 分析期間フィルタ
    mask = (df["race_date"] >= start_date) & (df["race_date"] <= end_date)
    df = df[mask].reset_index(drop=True)

    logger.info("combination_analysis: データセット完成 %d 行 × %d 列", len(df), len(df.columns))
    return df


# =============================================================================
# ROI 計算
# =============================================================================

def _compute_roi_table(
    df: pd.DataFrame,
    factor_cols: list[str],
    tansho_col: str = "tansho_odds",
    fukusho_col: str = "fukusho_odds",
    year_col: str = "kaisai_nen",
    min_samples: int = _MIN_SAMPLES,
) -> pd.DataFrame:
    """
    factor_cols のクロスビンごとに回収率テーブルを生成する。

    複勝回収率(%)     : fukusho_col（デフォルト="fukusho_odds"）を使用。
                       jvd_hr UNPIVOT の fukusho_odds は 3 着内のみ非 NULL だが、
                       fillna(0.0) で非3着内馬を「ゼロリターン」として扱い正しく計算する。
    複勝補正回収率    : 年次重み付けを適用した複勝回収率。
                       dropna しないため的中率100%バグが発生しない。
                       オッズ帯別補正は行わない（事前オッズデータが馬ごとに存在しないため）。

    Returns DataFrame 列:
        ビン | 単勝件数 | 単勝的中数 | 単勝的中率(%) | 単勝回収率(%) |
        複勝件数 | 複勝的中数 | 複勝的中率(%) | 複勝回収率(%) |
        単勝平均回収率 | 単勝補正回収率 | 複勝補正回収率

    用語:
        単勝回収率(%)   : 100円均等買い方式の単勝回収率
        複勝回収率(%)   : 100円均等買い方式の複勝回収率（実際の払戻オッズ使用）
        単勝平均回収率  : 均等払い戻し方式（補正なし・年次重み付けなし）
        単勝補正回収率  : 均等払い戻し方式 + オッズ帯別補正 + 年次重み付け
        複勝補正回収率  : 実際の複勝払戻を使った年次重み付け平均回収率
    """
    df = df.copy()

    # 着順整数化（character varying → numeric。文字列比較バグを防ぐため必ず変換）
    # 例: '10' < '3' (文字列) → pd.to_numeric で 10.0 > 3.0 (数値) に正しく変換
    df["_kakujun"] = pd.to_numeric(df["kakutei_chakujun"], errors="coerce")
    df["_is_tansho"] = (df["_kakujun"] == 1).astype(float).where(df["_kakujun"].notna(), other=np.nan)
    df["_is_fukusho"] = (df["_kakujun"] <= 3).astype(float).where(df["_kakujun"].notna(), other=np.nan)

    # Categorical 型（pd.cut 結果）は fillna 前に str 変換が必要
    for col in factor_cols:
        if col not in df.columns:
            df[col] = "N/A"
        if hasattr(df[col], "cat"):
            df[col] = df[col].astype(str)
        df[col] = df[col].fillna("N/A").astype(str)

    # ビンラベル生成
    if len(factor_cols) == 1:
        df["_bin"] = df[factor_cols[0]]
    else:
        df["_bin"] = df[factor_cols].apply(
            lambda row: " × ".join(str(v) for v in row), axis=1
        )

    records = []
    for bin_val, grp in df.groupby("_bin", sort=True):
        if bin_val == "N/A" or "N/A" in str(bin_val):
            continue

        # ---- 基底セット: tansho_odds・着順・kaisai_nen が全て非 NULL ----
        valid = grp.dropna(subset=[tansho_col, "_is_tansho", year_col])
        n = len(valid)
        if n < min_samples:
            continue

        odds_t = pd.to_numeric(valid[tansho_col], errors="coerce")
        is_win  = valid["_is_tansho"].astype(float)
        is_place = valid["_is_fukusho"].astype(float)

        # ---- 単勝: 件数・的中数・的中率 ----
        n_hit_t    = int(is_win.sum())
        hit_rate_t = n_hit_t / n * 100

        # 単勝回収率(%): 100円均等買い = sum(odds × is_win) / n × 100
        tansho_roi_simple = float((odds_t * is_win).sum() / n * 100) if n > 0 else 0.0

        # 単勝平均回収率: 均等払い戻し方式（補正なし・年次重み付けなし）
        #   bet_i = TARGET_PAYOUT / odds_i, payout_i = TARGET_PAYOUT × is_win_i
        pos_mask  = odds_t > 0
        bet_sum_t = (TARGET_PAYOUT / odds_t[pos_mask]).sum()
        pay_sum_t = (TARGET_PAYOUT * is_win[pos_mask]).sum()
        tansho_avg_roi = float(pay_sum_t / bet_sum_t * 100) if bet_sum_t > 0 else 0.0

        # 単勝補正回収率（年次重み付け + オッズ帯別補正）
        t = calc_corrected_return_rate(
            valid, odds_col=tansho_col, hit_flag_col="_is_tansho",
            year_col=year_col, is_fukusho=False,
        )

        # ---- 複勝: 複勝件数 = 単勝件数（同一基底セット） ----
        n_hit_f    = int(is_place.sum())
        hit_rate_f = n_hit_f / n * 100

        # 複勝回収率(%): 100円均等買い（実際の払戻オッズ使用、非3着内馬=0）
        if fukusho_col in valid.columns:
            odds_f = pd.to_numeric(valid[fukusho_col], errors="coerce").fillna(0.0)
            fukusho_roi_simple = float((odds_f * is_place).sum() / n * 100) if n > 0 else 0.0
        else:
            fukusho_roi_simple = 0.0

        # 複勝補正回収率: 年次重み付け平均回収率
        # dropna を使わず fillna(0.0) で非3着内馬をゼロリターンとして扱う。
        # オッズ帯別補正は行わない（馬ごとの事前複勝オッズが利用不可のため）。
        f_actual = pd.to_numeric(
            valid[fukusho_col] if fukusho_col in valid.columns else pd.Series(dtype=float),
            errors="coerce",
        ).fillna(0.0)
        f_returns = (f_actual * is_place.values)
        year_vals = valid[year_col].astype(str).values
        year_w = np.array([get_year_weight(y) for y in year_vals], dtype=float)
        total_w = float(year_w.sum())
        f_corrected_roi = float((f_returns * year_w).sum() / total_w * 100) if total_w > 0 else 0.0

        records.append({
            "ビン":         bin_val,
            "単勝件数":     n,
            "単勝的中数":   n_hit_t,
            "単勝的中率(%)": round(hit_rate_t, 2),
            "単勝回収率(%)": round(tansho_roi_simple, 2),
            "複勝件数":     n,               # 同一基底セット
            "複勝的中数":   n_hit_f,
            "複勝的中率(%)": round(hit_rate_f, 2),
            "複勝回収率(%)": round(fukusho_roi_simple, 2),
            "単勝平均回収率": round(tansho_avg_roi, 2),
            "単勝補正回収率": round(t["corrected_return_rate"], 2),
            "複勝補正回収率": round(f_corrected_roi, 2),
        })

    return pd.DataFrame(records)


def _run_global(df: pd.DataFrame, combo: dict) -> dict[str, pd.DataFrame]:
    """GLOBAL セグメント: 全体で1テーブル。"""
    table = _compute_roi_table(df, combo["factors"])
    return {"GLOBAL": table}


def _run_surface2(df: pd.DataFrame, combo: dict) -> dict[str, pd.DataFrame]:
    """SURFACE_2 セグメント: 芝/ダート別。"""
    out = {}
    for surf in ["芝", "ダ"]:
        sub = df[df["surface"] == surf]
        if len(sub) == 0:
            continue
        table = _compute_roi_table(sub, combo["factors"])
        out[surf] = table
    return out


def _run_course27(df: pd.DataFrame, combo: dict) -> dict[str, pd.DataFrame]:
    """COURSE_27 セグメント: 27コースカテゴリ別。"""
    out = {}
    for course, sub in df.groupby("course27_category"):
        if course == "unknown" or len(sub) == 0:
            continue
        table = _compute_roi_table(sub, combo["factors"])
        if not table.empty:
            out[course] = table
    return out


def _run_keibajo_track_kyori(df: pd.DataFrame, combo: dict) -> dict[str, pd.DataFrame]:
    """競馬場×track_code×距離 セグメント。"""
    out = {}
    df2 = df.copy()
    df2["_kyori_str"] = df2["kyori"].fillna(0).astype(int).astype(str)
    df2["_seg"] = (
        df2["keibajo_code"].astype(str)
        + "_"
        + df2["surface"].astype(str)
        + "_"
        + df2["_kyori_str"]
    )
    for seg, sub in df2.groupby("_seg"):
        if len(sub) < _MIN_SAMPLES:
            continue
        table = _compute_roi_table(sub, combo["factors"])
        if not table.empty:
            out[seg] = table
    return out


# =============================================================================
# CEO 組み合わせ定義
# =============================================================================

#: 組み合わせ仕様リスト
#: skip=True の場合は理由付きで報告のみ（計算スキップ）
COMBINATIONS: list[dict] = [
    # -----------------------------------------------------------------------
    # COURSE_27 セグメント
    # -----------------------------------------------------------------------
    {
        "id": "course27_01",
        "name": "前走着順（同コース前走1）",
        "segment": "COURSE_27",
        "factors": ["kakutei_chakujun_prev_c27_1"],
        "skip": False,
    },
    {
        "id": "course27_02",
        "name": "2前走着順（同コース前走2）",
        "segment": "COURSE_27",
        "factors": ["kakutei_chakujun_prev_c27_2"],
        "skip": False,
    },
    {
        "id": "course27_03",
        "name": "3前走着順（同コース前走3）",
        "segment": "COURSE_27",
        "factors": ["kakutei_chakujun_prev_c27_3"],
        "skip": False,
    },
    {
        "id": "course27_04",
        "name": "前走4角順位（同コース前走1）",
        "segment": "COURSE_27",
        "factors": ["corner_4_prev_c27_1"],
        "skip": False,
    },
    {
        "id": "course27_05",
        "name": "前走ブリンカー×今走ブリンカー（同コース）",
        "segment": "COURSE_27",
        "factors": ["blinker_shiyo_kubun_prev_c27_1", "blinker_shiyo_kubun"],
        "skip": False,
    },
    {
        "id": "course27_06",
        "name": "前走馬体重20kg刻みビン（同コース前走1）",
        "segment": "COURSE_27",
        "factors": ["bataiju_bin_20kg_prev_c27_1"],
        "skip": False,
    },
    {
        "id": "course27_07",
        "name": "距離増減（同コース: 今走距離 vs 同コース前走距離）",
        "segment": "COURSE_27",
        "factors": ["kyori_hendo_c27"],
        "skip": False,
    },
    {
        "id": "course27_08",
        "name": "母父名（hahachichi_name）",
        "segment": "COURSE_27",
        "factors": ["hahachichi_name"],
        "skip": False,
    },
    {
        "id": "course27_09",
        "name": "種牡馬名（chichi_name）",
        "segment": "COURSE_27",
        "factors": ["chichi_name"],
        "skip": False,
    },
    {
        "id": "course27_10",
        "name": "前日_体型_胴（taikei）",
        "segment": "COURSE_27",
        "factors": ["taikei"],
        "skip": True,
        "skip_reason": _SKIP_REASON_KYI_RAW,
    },
    {
        "id": "course27_11",
        "name": "調教師ランク S/A/B/C/D",
        "segment": "COURSE_27",
        "factors": ["chokyoshi_rank"],
        "skip": False,
    },
    {
        "id": "course27_12",
        "name": "騎手ランク S/A/B/C/D",
        "segment": "COURSE_27",
        "factors": ["kishu_rank"],
        "skip": False,
    },
    {
        "id": "course27_13",
        "name": "東西区分コード（tozai_shozoku_code）",
        "segment": "COURSE_27",
        "factors": ["tozai_shozoku_code"],
        "skip": False,
    },
    # -----------------------------------------------------------------------
    # SURFACE_2 セグメント
    # -----------------------------------------------------------------------
    {
        "id": "surface2_01",
        "name": "IDMランク × 前走タイム差",
        "segment": "SURFACE_2",
        "factors": ["idm_rank", "se_bataiju_prev1"],   # time_sa_prev1 使用
        "skip": False,
        "_override_factors": ["idm_rank", "time_sa_prev1"],
    },
    {
        "id": "surface2_02",
        "name": "前走race_pace × 前走実脚質 × 今走予想脚質",
        "segment": "SURFACE_2",
        "factors": ["prev1_race_pace", "prev1_kyakushitsu_sed", "kyakushitsu_kyi"],
        "skip": False,
    },
    {
        "id": "surface2_03",
        "name": "万券指数（manken_shisu）",
        "segment": "SURFACE_2",
        "factors": ["manken_shisu"],
        "skip": True,
        "skip_reason": _SKIP_REASON_KYI_RAW,
    },
    {
        "id": "surface2_04",
        "name": "IDM指数（kyi_idm）",
        "segment": "SURFACE_2",
        "factors": ["kyi_idm"],
        "skip": False,
    },
    {
        "id": "surface2_05",
        "name": "激走指数（gekiso_shisu）",
        "segment": "SURFACE_2",
        "factors": ["gekiso_shisu"],
        "skip": True,
        "skip_reason": _SKIP_REASON_KYI_RAW,
    },
    {
        "id": "surface2_06",
        "name": "総合指数（kyi_sogo_shisu）",
        "segment": "SURFACE_2",
        "factors": ["kyi_sogo_shisu"],
        "skip": False,
    },
    {
        "id": "surface2_07",
        "name": "厩舎指数（kyi_kyusha_shisu）",
        "segment": "SURFACE_2",
        "factors": ["kyi_kyusha_shisu"],
        "skip": False,
    },
    {
        "id": "surface2_08",
        "name": "騎手指数（kyi_kishu_shisu）",
        "segment": "SURFACE_2",
        "factors": ["kyi_kishu_shisu"],
        "skip": False,
    },
    {
        "id": "surface2_09",
        "name": "調教指数（kyi_chokyo_shisu）",
        "segment": "SURFACE_2",
        "factors": ["kyi_chokyo_shisu"],
        "skip": False,
    },
    {
        "id": "surface2_10",
        "name": "蹄コード（hizume_code）",
        "segment": "SURFACE_2",
        "factors": ["hizume_code"],
        "skip": True,
        "skip_reason": _SKIP_REASON_KYI_RAW,
    },
    {
        "id": "surface2_11",
        "name": "予想脚質 × 前走4角順位グループ × 馬齢",
        "segment": "SURFACE_2",
        "factors": ["kyakushitsu_kyi", "corner4_group_prev1", "barei"],
        "skip": False,
    },
    {
        "id": "surface2_12",
        "name": "産地名（sanchimei）",
        "segment": "SURFACE_2",
        "factors": ["sanchimei"],
        "skip": False,
    },
    {
        "id": "surface2_13",
        "name": "重不良馬場フラグ × 騎手名",
        "segment": "SURFACE_2",
        "factors": ["babajotai_heavy_flag", "kishumei_ryakusho"],
        "skip": False,
    },
    {
        "id": "surface2_14",
        "name": "重不良馬場フラグ × 種牡馬名",
        "segment": "SURFACE_2",
        "factors": ["babajotai_heavy_flag", "chichi_name"],
        "skip": False,
    },
    {
        "id": "surface2_15",
        "name": "騎手名 × 枠番",
        "segment": "SURFACE_2",
        "factors": ["kishumei_ryakusho", "wakuban"],
        "skip": False,
    },
    {
        "id": "surface2_16",
        "name": "前走競馬場 × 今走競馬場",
        "segment": "SURFACE_2",
        "factors": ["keibajo_code_prev1", "keibajo_code"],
        "skip": False,
    },
    {
        "id": "surface2_17",
        "name": "距離区分 × 今走_体型_トモ（taikei_sogo_1）",
        "segment": "SURFACE_2",
        "factors": ["kyori_kubun", "taikei_sogo_1"],
        "skip": True,
        "skip_reason": _SKIP_REASON_KYI_RAW,
    },
    # -----------------------------------------------------------------------
    # GLOBAL セグメント
    # -----------------------------------------------------------------------
    {
        "id": "global_01",
        "name": "調教師ランク × CID順位",
        "segment": "GLOBAL",
        "factors": ["chokyoshi_rank", "cid_soten"],
        "skip": False,
    },
    {
        "id": "global_02",
        "name": "騎手ランク × CID順位",
        "segment": "GLOBAL",
        "factors": ["kishu_rank", "cid_soten"],
        "skip": False,
    },
    {
        "id": "global_03",
        "name": "IDMランク × 基準オッズ5倍刻み",
        "segment": "GLOBAL",
        "factors": ["idm_rank", "kijun_odds_bin5"],
        "skip": False,
    },
    {
        "id": "global_04",
        "name": "LS指数4刻み × 予想脚質 × 位置指数",
        "segment": "GLOBAL",
        "factors": ["ls_shisu_bin4", "kyakushitsu_kyi", "ichi_shisu"],
        "skip": False,
    },
    {
        "id": "global_05",
        "name": "輸送区分（yuso_kubun）",
        "segment": "GLOBAL",
        "factors": ["yuso_kubun"],
        "skip": True,
        "skip_reason": _SKIP_REASON_KYI_RAW,
    },
    {
        "id": "global_06",
        "name": "負担重量 × 予想脚質",
        "segment": "GLOBAL",
        "factors": ["kyi_futan_juryo", "kyakushitsu_kyi"],
        "skip": False,
    },
    {
        "id": "global_07",
        "name": "予想矢印コード（chokyo_yajirushi_code）",
        "segment": "GLOBAL",
        "factors": ["chokyo_yajirushi_code"],
        "skip": False,
    },
    # -----------------------------------------------------------------------
    # 競馬場×芝ダート×距離 セグメント
    # -----------------------------------------------------------------------
    {
        "id": "ktk_01",
        "name": "馬番（umaban）",
        "segment": "KEIBAJO_TRACK_KYORI",
        "factors": ["umaban"],
        "skip": False,
    },
    {
        "id": "ktk_02",
        "name": "枠番（wakuban）",
        "segment": "KEIBAJO_TRACK_KYORI",
        "factors": ["wakuban"],
        "skip": False,
    },
    # -----------------------------------------------------------------------
    # SURFACE_2 × 競馬場別 追加
    # -----------------------------------------------------------------------
    {
        "id": "s2_keibajo_01",
        "name": "休養週数ビン（SURFACE_2 × keibajo_code 別）",
        "segment": "SURFACE_2_KEIBAJO",
        "factors": ["kyuyou_weeks"],
        "skip": False,
    },
]


# =============================================================================
# メインエントリ
# =============================================================================

def run_combination_analysis(
    conn,
    start_date: str,
    end_date: str,
) -> dict[str, dict[str, pd.DataFrame]]:
    """
    CEOの全組み合わせを実行し結果を返す。

    Args:
        conn:       DB 接続
        start_date: 分析開始日（YYYYMMDD）
        end_date:   分析終了日（YYYYMMDD）

    Returns:
        dict[combo_id -> dict[segment_label -> DataFrame]]
        スキップした組み合わせは {"SKIP": DataFrame({"reason": [...]})} を格納
    """
    logger.info("combination_analysis: データセット構築開始 %s〜%s", start_date, end_date)
    df = load_combination_dataset(conn, start_date, end_date)

    # time_sa_prev1 は se_bataiju_prev1 ではなく time_sa_prev1 が正しい
    # surface2_01 の _override_factors を反映
    results: dict[str, dict[str, pd.DataFrame]] = {}

    for combo in COMBINATIONS:
        cid = combo["id"]
        logger.info("combination_analysis: [%s] %s", cid, combo["name"])

        if combo.get("skip"):
            results[cid] = {
                "SKIP": pd.DataFrame({"reason": [combo.get("skip_reason", "不明")]}),
            }
            continue

        # _override_factors があれば優先
        factors = combo.get("_override_factors", combo["factors"])

        seg = combo["segment"]

        if seg == "GLOBAL":
            results[cid] = _run_global(df, {"factors": factors})

        elif seg == "SURFACE_2":
            results[cid] = _run_surface2(df, {"factors": factors})

        elif seg == "COURSE_27":
            results[cid] = _run_course27(df, {"factors": factors})

        elif seg == "KEIBAJO_TRACK_KYORI":
            results[cid] = _run_keibajo_track_kyori(df, {"factors": factors})

        elif seg == "SURFACE_2_KEIBAJO":
            # SURFACE_2 × keibajo_code 別
            out = {}
            for surf in ["芝", "ダ"]:
                for keibajo, sub in df[df["surface"] == surf].groupby("keibajo_code"):
                    if len(sub) < _MIN_SAMPLES:
                        continue
                    seg_label = f"{surf}_{keibajo}"
                    table = _compute_roi_table(sub, factors)
                    if not table.empty:
                        out[seg_label] = table
            results[cid] = out
        else:
            logger.warning("combination_analysis: 未知のセグメント %s", seg)
            results[cid] = {}

    return results
