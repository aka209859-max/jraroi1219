"""
全325ファクター対応データローダー

JRA-VAN 13テーブル + JRDB 5テーブルを結合し、
分析に必要な全カラムを1つのDataFrameとして返す。

特記事項:
    - jvd_wc / jvd_hc は時系列JOINが未確定のため除外（17+7=24列はNO_JOIN）
    - jvd_hr は複勝オッズ取得専用CTE経由（別処理済み）
    - JRDBは jrd_kyi_fixed(490,149件), jrd_cyb, jrd_joa, jrd_bac, jrd_sed を使用
    - jrd_kyi_fixed: 38カラム（実テーブル jrd_kyi は 517件のみのため使用不可）
    - JRDB JOIN条件: keibajo_code + race_shikonen(YYMMDD) + kaisai_kai + kaisai_nichime
                    + race_bango (+ umaban for horse-level tables)
    - 全カラムが character varying 型 → 数値はアプリ側でキャスト
    - ACTUAL_DB_SCHEMA_2293_COLUMNS.csv に存在しないカラムは一切SELECTしない
    - jvd_ck 成績カラム(18文字=3字×6フィールド)はロード後に勝率に変換する

メモリ制限対応:
    年単位の分割クエリ（load_by_year）を提供する。
    全期間は load_all_years() で年ループして結合する。
"""
from typing import Optional, List

import pandas as pd

from roi_pipeline.config.db import DBConfig, get_connection
from roi_pipeline.engine.data_loader_v2 import (
    JRA_KEIBAJO_CODES,
    JVAN_TO_JRDB_RACE_KEY8,
    _build_fukusho_unpivot_cte,
)


# =============================================================================
# 芝/ダート判別: track_code の先頭1桁
#   1X = 芝, 2X = ダート, 3X = 障害（除外）
# =============================================================================
SURFACE_EXPR = """
CASE
    WHEN TRIM(ra.track_code) LIKE '1%' THEN '芝'
    WHEN TRIM(ra.track_code) LIKE '2%' THEN 'ダ'
    ELSE 'その他'
END
"""

# =============================================================================
# JRDB JOIN 条件テンプレート
#   JRA-VAN の kaisai_nen(YYYY) + kaisai_tsukihi(MMDD)
#   → JRDB の race_shikonen(YYMMDD) に変換して結合
# =============================================================================
def _jrdb_horse_join(alias: str) -> str:
    """馬単位JRDB表のJOIN条件（keibajo+日付+kai+nichime+race_bango+umaban）。"""
    return f"""    LEFT JOIN {alias.split('_as_')[0] if '_as_' not in alias else alias} AS {alias.split('_as_')[1] if '_as_' in alias else alias}
        ON se.keibajo_code = {alias}.keibajo_code
        AND (SUBSTRING(se.kaisai_nen, 3, 2) || se.kaisai_tsukihi) = {alias}.race_shikonen
        AND se.kaisai_kai = {alias}.kaisai_kai
        AND se.kaisai_nichime = {alias}.kaisai_nichime
        AND se.race_bango = {alias}.race_bango
        AND TRIM(se.umaban) = TRIM({alias}.umaban)"""


def _jrdb_race_join(alias: str) -> str:
    """レース単位JRDB表のJOIN条件（umaban なし）。"""
    return f"""    LEFT JOIN {alias.split('_as_')[0] if '_as_' not in alias else alias} AS {alias.split('_as_')[1] if '_as_' in alias else alias}
        ON se.keibajo_code = {alias}.keibajo_code
        AND (SUBSTRING(se.kaisai_nen, 3, 2) || se.kaisai_tsukihi) = {alias}.race_shikonen
        AND se.kaisai_kai = {alias}.kaisai_kai
        AND se.kaisai_nichime = {alias}.kaisai_nichime
        AND se.race_bango = {alias}.race_bango"""


def _build_full_query(date_from: str, date_to: str) -> str:
    """
    全カラムを取得するSQL文を生成する。

    Args:
        date_from: 開始日 YYYYMMDD
        date_to: 終了日 YYYYMMDD

    Returns:
        SQL文字列
    """
    fukusho_cte = _build_fukusho_unpivot_cte()

    query = f"""
    WITH fukusho_pay AS (
        {fukusho_cte}
    )
    SELECT
        -- =====================================================================
        -- JVD_SE: 識別キー + 分析カラム
        -- =====================================================================
        se.keibajo_code,
        se.kaisai_nen,
        se.kaisai_tsukihi,
        se.kaisai_kai,
        se.kaisai_nichime,
        se.race_bango,
        se.umaban,
        se.ketto_toroku_bango,
        -- 分析対象
        se.barei                  AS se_barei,
        se.bataiju                AS se_bataiju,
        se.blinker_shiyo_kubun    AS se_blinker_shiyo_kubun,
        se.banushi_code           AS se_banushi_code,
        se.chokyoshi_code         AS se_chokyoshi_code,
        se.kaisai_nichime         AS se_kaisai_nichime,
        se.seibetsu_code          AS se_seibetsu_code,
        se.wakuban                AS se_wakuban,
        se.zogen_sa               AS se_zogen_sa,
        se.futan_juryo_henkomae   AS se_futan_juryo_henkomae,
        se.ijo_kubun_code         AS se_ijo_kubun_code,
        se.yoso_soha_time         AS se_yoso_soha_time,
        se.zogen_fugo             AS se_zogen_fugo,
        se.umakigo_code           AS se_umakigo_code,

        -- =====================================================================
        -- JVD_RA: レース条件
        -- =====================================================================
        ra.babajotai_code_dirt    AS ra_babajotai_code_dirt,
        ra.babajotai_code_shiba   AS ra_babajotai_code_shiba,
        ra.course_kubun           AS ra_course_kubun,
        ra.fukashokin             AS ra_fukashokin,
        ra.grade_code             AS ra_grade_code,
        ra.hasso_jikoku           AS ra_hasso_jikoku,
        ra.kyori                  AS ra_kyori,
        ra.shusso_tosu            AS ra_shusso_tosu,
        ra.tenko_code             AS ra_tenko_code,
        ra.track_code             AS ra_track_code,
        ra.kyoso_joken_code_3sai  AS ra_kyoso_joken_code_3sai,
        ra.kyoso_joken_code_4sai  AS ra_kyoso_joken_code_4sai,
        ra.kyoso_joken_code_5sai_ijo AS ra_kyoso_joken_code_5sai_ijo,
        ra.kyoso_kigo_code        AS ra_kyoso_kigo_code,
        ra.kyoso_shubetsu_code    AS ra_kyoso_shubetsu_code,

        -- =====================================================================
        -- JVD_CK: 過去成績（距離・場別）
        -- =====================================================================
        ck.chuo_gokei             AS ck_chuo_gokei,
        ck.dirt_1200_ika          AS ck_dirt_1200_ika,
        ck.dirt_1201_1400         AS ck_dirt_1201_1400,
        ck.dirt_1401_1600         AS ck_dirt_1401_1600,
        ck.dirt_1601_1800         AS ck_dirt_1601_1800,
        ck.dirt_1801_2000         AS ck_dirt_1801_2000,
        ck.dirt_2001_2200         AS ck_dirt_2001_2200,
        ck.dirt_2201_2400         AS ck_dirt_2201_2400,
        ck.dirt_2401_2800         AS ck_dirt_2401_2800,
        ck.dirt_2801_ijo          AS ck_dirt_2801_ijo,
        ck.dirt_choku             AS ck_dirt_choku,
        ck.dirt_chukyo            AS ck_dirt_chukyo,
        ck.dirt_fukushima         AS ck_dirt_fukushima,
        ck.dirt_hakodate          AS ck_dirt_hakodate,
        ck.dirt_hanshin           AS ck_dirt_hanshin,
        ck.dirt_kokura            AS ck_dirt_kokura,
        ck.dirt_kyoto             AS ck_dirt_kyoto,
        ck.dirt_migi              AS ck_dirt_migi,
        ck.shiba_1201_1400        AS ck_shiba_1201_1400,
        ck.shiba_1401_1600        AS ck_shiba_1401_1600,
        ck.shiba_1601_1800        AS ck_shiba_1601_1800,

        -- =====================================================================
        -- JVD_DM: データマイニング予想
        -- =====================================================================
        dm.mining_yoso_01         AS dm_mining_yoso_01,
        dm.mining_yoso_02         AS dm_mining_yoso_02,
        dm.mining_yoso_03         AS dm_mining_yoso_03,
        dm.mining_yoso_04         AS dm_mining_yoso_04,
        dm.mining_yoso_05         AS dm_mining_yoso_05,
        dm.mining_yoso_06         AS dm_mining_yoso_06,
        dm.mining_yoso_07         AS dm_mining_yoso_07,
        dm.mining_yoso_08         AS dm_mining_yoso_08,
        dm.mining_yoso_09         AS dm_mining_yoso_09,
        dm.mining_yoso_10         AS dm_mining_yoso_10,
        dm.mining_yoso_11         AS dm_mining_yoso_11,
        dm.mining_yoso_15         AS dm_mining_yoso_15,
        dm.mining_yoso_16         AS dm_mining_yoso_16,
        dm.mining_yoso_17         AS dm_mining_yoso_17,
        dm.mining_yoso_18         AS dm_mining_yoso_18,

        -- =====================================================================
        -- JVD_UM: 馬基本情報（適性スコア）
        -- =====================================================================
        um.dirt_furyo             AS um_dirt_furyo,
        um.dirt_hidari            AS um_dirt_hidari,
        um.dirt_long              AS um_dirt_long,
        um.dirt_middle            AS um_dirt_middle,
        um.dirt_omo               AS um_dirt_omo,
        um.dirt_ryo               AS um_dirt_ryo,
        um.dirt_short             AS um_dirt_short,
        um.dirt_yayaomo           AS um_dirt_yayaomo,
        um.ketto_joho_12a         AS um_ketto_joho_12a,
        um.ketto_joho_12b         AS um_ketto_joho_12b,
        um.ketto_joho_13a         AS um_ketto_joho_13a,
        um.ketto_joho_13b         AS um_ketto_joho_13b,

        -- =====================================================================
        -- JVD_SK: 血統情報
        -- =====================================================================
        sk.hinshu_code            AS sk_hinshu_code,
        sk.ketto_joho_01a         AS sk_ketto_joho_01a,
        sk.ketto_joho_02a         AS sk_ketto_joho_02a,
        sk.ketto_joho_03a         AS sk_ketto_joho_03a,
        sk.ketto_joho_04a         AS sk_ketto_joho_04a,
        sk.ketto_joho_05a         AS sk_ketto_joho_05a,
        sk.ketto_joho_06a         AS sk_ketto_joho_06a,
        sk.ketto_joho_07a         AS sk_ketto_joho_07a,
        sk.ketto_joho_08a         AS sk_ketto_joho_08a,
        sk.ketto_joho_09a         AS sk_ketto_joho_09a,
        sk.ketto_joho_10a         AS sk_ketto_joho_10a,

        -- =====================================================================
        -- JVD_H1: 馬券発売フラグ（レース条件として有効）
        -- =====================================================================
        h1.hatsubai_flag_fukusho  AS h1_hatsubai_flag_fukusho,
        h1.hatsubai_flag_tansho   AS h1_hatsubai_flag_tansho,

        -- =====================================================================
        -- JVD_HR: 不成立フラグ
        -- =====================================================================
        hr.fuseiritsu_flag_fukusho AS hr_fuseiritsu_flag_fukusho,
        hr.fuseiritsu_flag_tansho  AS hr_fuseiritsu_flag_tansho,

        -- =====================================================================
        -- JVD_H6: 三連単発売フラグ
        -- =====================================================================
        h6.hatsubai_flag_sanrentan AS h6_hatsubai_flag_sanrentan,
        h6.toroku_tosu             AS h6_toroku_tosu,

        -- =====================================================================
        -- JVD_JG: 除外・出走区分
        -- =====================================================================
        jg.jogai_jotai_kubun      AS jg_jogai_jotai_kubun,
        jg.shusso_kubun           AS jg_shusso_kubun,

        -- =====================================================================
        -- JVD_CH: 調教師東西所属
        -- =====================================================================
        ch_tbl.tozai_shozoku_code AS ch_tozai_shozoku_code,

        -- =====================================================================
        -- JRD_KYI_FIXED: 指数系（主要）
        --   jrd_kyi_fixed: 490,149件 (2016-2026), 38カラム
        --   ※ jrd_kyi（実テーブル）は 517件のみ(2026-03-14 1日分)のため使用不可
        --   ※ jrd_kyi_fixedに存在する23データカラムのみSELECT
        -- =====================================================================
        kyi.idm                   AS kyi_idm,
        kyi.kishu_shisu           AS kyi_kishu_shisu,
        kyi.sogo_shisu            AS kyi_sogo_shisu,
        kyi.agari_shisu           AS kyi_agari_shisu,
        kyi.chokyo_shisu          AS kyi_chokyo_shisu,
        kyi.ichi_shisu            AS kyi_ichi_shisu,
        kyi.kyusha_shisu          AS kyi_kyusha_shisu,
        kyi.pace_shisu            AS kyi_pace_shisu,
        kyi.ten_shisu             AS kyi_ten_shisu,
        kyi.chokyo_yajirushi_code AS kyi_chokyo_yajirushi_code,
        kyi.kishu_code            AS kyi_kishu_code,
        kyi.kyakushitsu           AS kyi_kyakushitsu_code,
        kyi.kyori_tekisei         AS kyi_kyori_tekisei_code,
        kyi.kyusha_rank           AS kyi_kyusha_rank,
        kyi.omo_tekisei_code      AS kyi_tekisei_code_omo,
        kyi.futan_juryo           AS kyi_futan_juryo,

        -- =====================================================================
        -- JRD_CYB: 調教分析
        --   ACTUAL_DB_SCHEMA確認済み: chokyo_corse_shubetsu, chokyo_hyoka (単一)
        --   非存在カラムを除外: chokyoshi_code/mei, choshubetsu, hyoka_2/3, sakusha_hyoka_f_h
        -- =====================================================================
        cyb.chokyo_corse_dirt      AS cyb_chokyo_corse_dirt,
        cyb.chokyo_corse_hanro     AS cyb_chokyo_corse_hanro,
        cyb.chokyo_corse_polytrack AS cyb_chokyo_corse_polytrack,
        cyb.chokyo_corse_pool      AS cyb_chokyo_corse_pool,
        cyb.chokyo_corse_shiba     AS cyb_chokyo_corse_shiba,
        cyb.chokyo_corse_shubetsu  AS cyb_chokyo_corse_shubetsu,
        cyb.chokyo_hyoka           AS cyb_chokyo_hyoka,
        cyb.chokyo_juten           AS cyb_chokyo_juten,
        cyb.chokyo_type            AS cyb_chokyo_type,
        cyb.oikiri_shisu           AS cyb_oikiri_shisu,
        cyb.shiage_shisu           AS cyb_shiage_shisu,

        -- =====================================================================
        -- JRD_JOA: 騎手・厩舎評価
        --   ACTUAL_DB_SCHEMA確認済み
        --   非存在カラムを除外: jockey_banushi_..., ten_shisu, uma_gucchi
        -- =====================================================================
        joa.em                    AS joa_em,
        joa.kishu_bb_shirushi     AS joa_kishu_bb_shirushi,
        joa.kyusha_bb_shirushi    AS joa_kyusha_bb_shirushi,
        joa.kyusha_bb_nijumaru_tansho_kaishuritsu AS joa_kyusha_bb_nijumaru_tansho_kaishuritsu,
        joa.ls_hyoka              AS joa_ls_hyoka,
        joa.ls_shisu              AS joa_ls_shisu,

        -- =====================================================================
        -- JRD_BAC: レース基本情報（レース単位）
        --   ACTUAL_DB_SCHEMA確認済み
        --   非存在カラムを除外: baba_sa_saishujikoku, kaisai_nen_gappi,
        --                      kyoso_joken, race_code_zenhan, race_comment, track_baba_sa
        -- =====================================================================
        bac.baken_hatsubai_flag   AS bac_baken_hatsubai_flag,
        bac.fukashokin            AS bac_fukashokin,
        bac.honshokin             AS bac_honshokin,

        -- =====================================================================
        -- JRD_SED: レース詳細（馬単位）
        --   ACTUAL_DB_SCHEMA確認済み
        --   非存在カラムを除外: race_pen_type
        -- =====================================================================
        sed.babasa                AS sed_babasa,
        sed.bataiju_zogen         AS sed_bataiju_zogen,
        sed.furi                  AS sed_furi,
        sed.pace                  AS sed_pace,
        sed.pace_shisu            AS sed_pace_shisu,
        sed.race_pace             AS sed_race_pace,

        -- =====================================================================
        -- 結果データ（ROI計算用）
        -- =====================================================================
        se.kakutei_chakujun,
        se.tansho_odds,
        fp.fukusho_odds,
        (se.kaisai_nen || se.kaisai_tsukihi) AS race_date,
        se.kaisai_nen AS race_year,

        -- セグメント付与用
        ({SURFACE_EXPR}) AS surface_2,
        ra.track_code             AS track_code_for_course,
        ra.kyori                  AS kyori_for_course

    FROM jvd_se AS se

    -- JRA-VAN レース情報
    LEFT JOIN jvd_ra AS ra
        ON se.keibajo_code = ra.keibajo_code
        AND se.kaisai_nen = ra.kaisai_nen
        AND se.kaisai_tsukihi = ra.kaisai_tsukihi
        AND se.kaisai_kai = ra.kaisai_kai
        AND se.kaisai_nichime = ra.kaisai_nichime
        AND se.race_bango = ra.race_bango

    -- JRA-VAN 成績（馬単位）
    LEFT JOIN jvd_ck AS ck
        ON se.keibajo_code = ck.keibajo_code
        AND se.kaisai_nen = ck.kaisai_nen
        AND se.kaisai_tsukihi = ck.kaisai_tsukihi
        AND se.kaisai_kai = ck.kaisai_kai
        AND se.kaisai_nichime = ck.kaisai_nichime
        AND se.race_bango = ck.race_bango
        AND se.ketto_toroku_bango = ck.ketto_toroku_bango

    -- JRA-VAN データマイニング（レース単位）
    LEFT JOIN jvd_dm AS dm
        ON se.keibajo_code = dm.keibajo_code
        AND se.kaisai_nen = dm.kaisai_nen
        AND se.kaisai_tsukihi = dm.kaisai_tsukihi
        AND se.kaisai_kai = dm.kaisai_kai
        AND se.kaisai_nichime = dm.kaisai_nichime
        AND se.race_bango = dm.race_bango

    -- JRA-VAN 馬マスタ
    LEFT JOIN jvd_um AS um
        ON se.ketto_toroku_bango = um.ketto_toroku_bango

    -- JRA-VAN 血統
    LEFT JOIN jvd_sk AS sk
        ON se.ketto_toroku_bango = sk.ketto_toroku_bango

    -- JRA-VAN 発売フラグ（レース単位）
    LEFT JOIN jvd_h1 AS h1
        ON se.keibajo_code = h1.keibajo_code
        AND se.kaisai_nen = h1.kaisai_nen
        AND se.kaisai_tsukihi = h1.kaisai_tsukihi
        AND se.kaisai_kai = h1.kaisai_kai
        AND se.kaisai_nichime = h1.kaisai_nichime
        AND se.race_bango = h1.race_bango

    -- JRA-VAN 不成立フラグ（レース単位）
    LEFT JOIN jvd_hr AS hr
        ON se.keibajo_code = hr.keibajo_code
        AND se.kaisai_nen = hr.kaisai_nen
        AND se.kaisai_tsukihi = hr.kaisai_tsukihi
        AND se.kaisai_kai = hr.kaisai_kai
        AND se.kaisai_nichime = hr.kaisai_nichime
        AND se.race_bango = hr.race_bango

    -- JRA-VAN 三連単フラグ（レース単位）
    LEFT JOIN jvd_h6 AS h6
        ON se.keibajo_code = h6.keibajo_code
        AND se.kaisai_nen = h6.kaisai_nen
        AND se.kaisai_tsukihi = h6.kaisai_tsukihi
        AND se.kaisai_kai = h6.kaisai_kai
        AND se.kaisai_nichime = h6.kaisai_nichime
        AND se.race_bango = h6.race_bango

    -- JRA-VAN 出走区分（馬単位: ketto_toroku_bangoでJOIN）
    LEFT JOIN jvd_jg AS jg
        ON se.keibajo_code = jg.keibajo_code
        AND se.kaisai_nen = jg.kaisai_nen
        AND se.kaisai_tsukihi = jg.kaisai_tsukihi
        AND se.kaisai_kai = jg.kaisai_kai
        AND se.kaisai_nichime = jg.kaisai_nichime
        AND se.race_bango = jg.race_bango
        AND se.ketto_toroku_bango = jg.ketto_toroku_bango

    -- JRA-VAN 調教師マスタ
    LEFT JOIN jvd_ch AS ch_tbl
        ON TRIM(se.chokyoshi_code) = TRIM(ch_tbl.chokyoshi_code)

    -- 複勝オッズ UNPIVOT
    LEFT JOIN fukusho_pay AS fp
        ON se.keibajo_code = fp.keibajo_code
        AND se.kaisai_nen = fp.kaisai_nen
        AND se.kaisai_tsukihi = fp.kaisai_tsukihi
        AND se.kaisai_kai = fp.kaisai_kai
        AND se.kaisai_nichime = fp.kaisai_nichime
        AND se.race_bango = fp.race_bango
        AND TRIM(se.umaban) = fp.umaban

    -- JRDB: jrd_kyi_fixed（馬単位: 490,149件 2016-2026）
    --   jrd_kyi（実テーブル）は 517件のみのため jrd_kyi_fixed を使用する
    --   JOIN: jrdb_race_key8 (8byte key) + umaban
    --   key8 = keibajo(2) + year_last2(2) + kai(1) + hex(nichime)(1) + race(2)
    LEFT JOIN jrd_kyi_fixed AS kyi
        ON ({JVAN_TO_JRDB_RACE_KEY8}) = kyi.jrdb_race_key8
        AND TRIM(se.umaban) = TRIM(kyi.umaban)

    -- JRDB: jrd_cyb（馬単位）
    LEFT JOIN jrd_cyb AS cyb
        ON se.keibajo_code = cyb.keibajo_code
        AND (SUBSTRING(se.kaisai_nen, 3, 2) || se.kaisai_tsukihi) = cyb.race_shikonen
        AND se.kaisai_kai = cyb.kaisai_kai
        AND se.kaisai_nichime = cyb.kaisai_nichime
        AND se.race_bango = cyb.race_bango
        AND TRIM(se.umaban) = TRIM(cyb.umaban)

    -- JRDB: jrd_joa（馬単位）
    LEFT JOIN jrd_joa AS joa
        ON se.keibajo_code = joa.keibajo_code
        AND (SUBSTRING(se.kaisai_nen, 3, 2) || se.kaisai_tsukihi) = joa.race_shikonen
        AND se.kaisai_kai = joa.kaisai_kai
        AND se.kaisai_nichime = joa.kaisai_nichime
        AND se.race_bango = joa.race_bango
        AND TRIM(se.umaban) = TRIM(joa.umaban)

    -- JRDB: jrd_bac（レース単位）
    LEFT JOIN jrd_bac AS bac
        ON se.keibajo_code = bac.keibajo_code
        AND (SUBSTRING(se.kaisai_nen, 3, 2) || se.kaisai_tsukihi) = bac.race_shikonen
        AND se.kaisai_kai = bac.kaisai_kai
        AND se.kaisai_nichime = bac.kaisai_nichime
        AND se.race_bango = bac.race_bango

    -- JRDB: jrd_sed（馬単位）
    LEFT JOIN jrd_sed AS sed
        ON se.keibajo_code = sed.keibajo_code
        AND (SUBSTRING(se.kaisai_nen, 3, 2) || se.kaisai_tsukihi) = sed.race_shikonen
        AND se.kaisai_kai = sed.kaisai_kai
        AND se.kaisai_nichime = sed.kaisai_nichime
        AND se.race_bango = sed.race_bango
        AND TRIM(se.umaban) = TRIM(sed.umaban)

    WHERE
        (se.kaisai_nen || se.kaisai_tsukihi) >= '{date_from}'
        AND (se.kaisai_nen || se.kaisai_tsukihi) <= '{date_to}'
        AND TRIM(se.keibajo_code) IN {JRA_KEIBAJO_CODES}
    ORDER BY race_date, se.keibajo_code, se.race_bango, se.umaban
    """
    return query


def load_full_factor_data(
    date_from: str,
    date_to: str,
    config: Optional[DBConfig] = None,
) -> pd.DataFrame:
    """
    全ファクター対応の結合済みDataFrameを返す。

    JRDBは jrd_kyi_fixed(490,149件), jrd_cyb, jrd_joa, jrd_bac, jrd_sed を使用。
    ACTUAL_DB_SCHEMA_2293_COLUMNS.csv に存在しないカラムは一切SELECTしない。
    jvd_ck 成績カラム（18文字=3字×6フィールド）はロード後に勝率(0.0-1.0)に変換する。

    Args:
        date_from: 開始日 YYYYMMDD（例: "20160101"）
        date_to: 終了日 YYYYMMDD（例: "20251231"）
        config: DB接続設定

    Returns:
        結合済みDataFrame（jvd_ck成績カラムは勝率に変換済み）
    """
    conn = get_connection(config)
    try:
        query = _build_full_query(date_from, date_to)
        df = pd.read_sql_query(query, conn)
    finally:
        conn.close()

    # -------------------------------------------------------------------------
    # jvd_ck 成績カラムの後処理:
    #   18文字エンコード（3字×6フィールド）→ 勝率(f1/f6) に変換
    #   変換後は NUMERIC として factor_analysis_engine が正常に分析できる
    # -------------------------------------------------------------------------
    for col in _CK_SEISEKI_ALIASES:
        if col in df.columns:
            df[col] = df[col].map(_parse_ck_seiseki_win_rate)

    return df


def load_full_factor_data_by_year(
    year: int,
    config: Optional[DBConfig] = None,
) -> pd.DataFrame:
    """
    1年分のデータを取得する（メモリ効率化）。

    Args:
        year: 年 (2016-2025)
        config: DB接続設定

    Returns:
        結合済みDataFrame
    """
    date_from = f"{year}0101"
    date_to = f"{year}1231"
    return load_full_factor_data(date_from, date_to, config)


def load_all_years(
    years: Optional[List[int]] = None,
    config: Optional[DBConfig] = None,
    verbose: bool = True,
) -> pd.DataFrame:
    """
    複数年のデータを年単位で取得して結合する。

    Args:
        years: 取得する年のリスト。Noneの場合は2016-2025。
        config: DB接続設定
        verbose: 進捗表示するか

    Returns:
        全期間の結合済みDataFrame
    """
    if years is None:
        years = list(range(2016, 2026))

    frames = []
    for year in years:
        if verbose:
            print(f"  Loading year {year}...", flush=True)
        df_year = load_full_factor_data_by_year(year, config)
        frames.append(df_year)
        if verbose:
            print(f"    {len(df_year):,} rows", flush=True)

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)


# =============================================================================
# jvd_ck 成績カラム後処理
#   jvd_ck の距離別・場別成績カラムは 18文字エンコード（3字×6フィールド）:
#     f1=chars[0:3]   = 1着数
#     f2=chars[3:6]   = 2着数
#     f3=chars[6:9]   = 3着数
#     f4=chars[9:12]  = 4着以下(?)
#     f5=chars[12:15] = (不明)
#     f6=chars[15:18] = 総出走数
#   例: "001003001002000018" → f1=1,f2=3,f3=1,f4=2,f5=0,f6=18
#       win_rate = 1/18 ≈ 0.056
# =============================================================================
_CK_SEISEKI_ALIASES: List[str] = [
    "ck_chuo_gokei",
    "ck_dirt_1200_ika",
    "ck_dirt_1201_1400",
    "ck_dirt_1401_1600",
    "ck_dirt_1601_1800",
    "ck_dirt_1801_2000",
    "ck_dirt_2001_2200",
    "ck_dirt_2201_2400",
    "ck_dirt_2401_2800",
    "ck_dirt_2801_ijo",
    "ck_dirt_choku",
    "ck_dirt_chukyo",
    "ck_dirt_fukushima",
    "ck_dirt_hakodate",
    "ck_dirt_hanshin",
    "ck_dirt_kokura",
    "ck_dirt_kyoto",
    "ck_dirt_migi",
    "ck_shiba_1201_1400",
    "ck_shiba_1401_1600",
    "ck_shiba_1601_1800",
]


def _parse_ck_seiseki_win_rate(val: object) -> float:
    """
    jvd_ck 成績カラム（18文字=3字×6フィールド）から勝率を算出する。

    フォーマット:
        f1=chars[0:3]   = 1着数
        f2=chars[3:6]   = 2着数
        f3=chars[6:9]   = 3着数
        f4=chars[9:12]  = 4着以下(?)
        f5=chars[12:15] = (不明)
        f6=chars[15:18] = 総出走数

    Args:
        val: 18文字の成績エンコード文字列

    Returns:
        勝率 = f1 / f6 (0.0-1.0)。NaN if parse fails or f6==0.
    """
    if pd.isna(val) or str(val).strip() == "":
        return float("nan")
    s = str(val).strip()
    if len(s) != 18:
        return float("nan")
    try:
        f1 = int(s[0:3])
        f6 = int(s[15:18])
        if f6 == 0:
            return float("nan")
        return float(f1) / float(f6)
    except (ValueError, IndexError):
        return float("nan")


def extract_win_rate_from_record(series: pd.Series) -> pd.Series:
    """
    jvd_ck の成績カラムシリーズから勝率を算出する。

    jvd_ck の成績カラムは 18文字エンコード（3字×6フィールド）:
        f1=1着数, f2=2着数, f3=3着数, f4-f5=不明, f6=総出走数
    例: "001003001002000018" → 1勝/18走 = 勝率0.056

    Args:
        series: 成績カラムのSeriesオブジェクト

    Returns:
        勝率Series (0.0-1.0)。NaN if parse fails.
    """
    return series.map(_parse_ck_seiseki_win_rate)
