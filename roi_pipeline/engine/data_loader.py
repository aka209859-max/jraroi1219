"""
PostgreSQLデータ取得・型変換モジュール

全カラムがcharacter varying（文字列型）であるため、
数値演算前に必ず型変換を実施する。空文字・異常値のエラーハンドリング必須。

重要:
    - 当日データ（直前オッズ、当日馬体重、パドック評価、当日馬場状態速報）は使用禁止
    - ターゲットリーク防止のため、全集計は予測対象日の前日以前のデータのみ使用
"""
from typing import Optional

import pandas as pd

from roi_pipeline.config.db import DBConfig, get_connection


def safe_to_numeric(series: pd.Series, errors: str = "coerce") -> pd.Series:
    """
    文字列Seriesを安全に数値変換する。

    Args:
        series: 変換対象のSeries（character varying由来）
        errors: エラー時の処理（"coerce"=NaN, "raise"=例外, "ignore"=そのまま）

    Returns:
        数値変換後のSeries。変換失敗はNaN。
    """
    # 空文字列・空白のみをNaNに変換してからpd.to_numeric
    cleaned = series.replace(r"^\s*$", pd.NA, regex=True)
    return pd.to_numeric(cleaned, errors=errors)


def load_base_race_data(
    date_from: str,
    date_to: str,
    config: Optional[DBConfig] = None,
) -> pd.DataFrame:
    """
    jvd_se（成績）を基準に、jvd_ra・jrd_kyi・jrd_cyb・jrd_bac・jrd_joaを結合した
    ベースデータを取得する。

    Args:
        date_from: 開始日（YYYYMMDD形式、例: "20161101"）
        date_to: 終了日（YYYYMMDD形式、例: "20251231"）
        config: DB接続設定

    Returns:
        結合済みDataFrame

    Note:
        JRA-VANの日付: kaisai_nen='2024', kaisai_tsukihi='0307'
        JRDBの日付: race_shikonen='240307'
        結合キー: (SUBSTRING(se.kaisai_nen, 3, 2) || se.kaisai_tsukihi) = kyi.race_shikonen
    """
    query = """
    SELECT
        se.*,
        ra.babajotai_code_shiba,
        ra.babajotai_code_dirt,
        ra.tenko_code,
        ra.kyori,
        ra.track_code,
        kyi.idm,
        kyi.sogo_shisu,
        kyi.agari_shisu,
        kyi.pace_shisu,
        kyi.kyori_tekisei_code,
        kyi.tekisei_code_shiba AS course_tekisei,
        kyi.tekisei_code_omo AS baba_tekisei,
        kyi.chokyo_yajirushi_code,
        kyi.soho,
        kyi.kishu_shisu,
        kyi.chokyo_shisu,
        kyi.kyusha_shisu,
        cyb.chokyo_hyoka,
        joa.ls_shisu,
        bac.juryo_shubetsu_code,
        -- 日付を統一フォーマットで算出
        (se.kaisai_nen || se.kaisai_tsukihi) AS race_date
    FROM jvd_se AS se
    LEFT JOIN jvd_ra AS ra
        ON se.keibajo_code = ra.keibajo_code
        AND se.kaisai_nen = ra.kaisai_nen
        AND se.kaisai_tsukihi = ra.kaisai_tsukihi
        AND se.kaisai_kai = ra.kaisai_kai
        AND se.kaisai_nichime = ra.kaisai_nichime
        AND se.race_bango = ra.race_bango
    LEFT JOIN jrd_kyi AS kyi
        ON se.keibajo_code = kyi.keibajo_code
        AND (SUBSTRING(se.kaisai_nen, 3, 2) || se.kaisai_tsukihi) = kyi.race_shikonen
        AND COALESCE(se.kaisai_kai, '00') = kyi.kaisai_kai
        AND COALESCE(se.kaisai_nichime, '00') = kyi.kaisai_nichime
        AND se.race_bango = kyi.race_bango
        AND se.umaban = kyi.umaban
    LEFT JOIN jrd_cyb AS cyb
        ON se.keibajo_code = cyb.keibajo_code
        AND (SUBSTRING(se.kaisai_nen, 3, 2) || se.kaisai_tsukihi) = cyb.race_shikonen
        AND COALESCE(se.kaisai_kai, '00') = cyb.kaisai_kai
        AND COALESCE(se.kaisai_nichime, '00') = cyb.kaisai_nichime
        AND se.race_bango = cyb.race_bango
        AND se.umaban = cyb.umaban
    LEFT JOIN jrd_joa AS joa
        ON se.keibajo_code = joa.keibajo_code
        AND (SUBSTRING(se.kaisai_nen, 3, 2) || se.kaisai_tsukihi) = joa.race_shikonen
        AND COALESCE(se.kaisai_kai, '00') = joa.kaisai_kai
        AND COALESCE(se.kaisai_nichime, '00') = joa.kaisai_nichime
        AND se.race_bango = joa.race_bango
        AND se.umaban = joa.umaban
    LEFT JOIN jrd_bac AS bac
        ON se.keibajo_code = bac.keibajo_code
        AND (SUBSTRING(se.kaisai_nen, 3, 2) || se.kaisai_tsukihi) = bac.race_shikonen
        AND COALESCE(se.kaisai_kai, '00') = bac.kaisai_kai
        AND COALESCE(se.kaisai_nichime, '00') = bac.kaisai_nichime
        AND se.race_bango = bac.race_bango
    WHERE
        (se.kaisai_nen || se.kaisai_tsukihi) >= %(date_from)s
        AND (se.kaisai_nen || se.kaisai_tsukihi) <= %(date_to)s
    ORDER BY race_date, se.keibajo_code, se.race_bango, se.umaban
    """

    conn = get_connection(config)
    try:
        df = pd.read_sql_query(query, conn, params={"date_from": date_from, "date_to": date_to})
    finally:
        conn.close()

    return df


def convert_numeric_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    文字列型カラムのうち数値系を安全に変換する。

    Args:
        df: load_base_race_data()の出力DataFrame

    Returns:
        数値変換済みDataFrame（変換失敗はNaN）
    """
    numeric_cols = [
        "umaban", "kakutei_chakujun", "tansho_odds", "tansho_ninkijun",
        "fukusho_odds", "bataiju", "barei",
        "idm", "sogo_shisu", "agari_shisu", "pace_shisu",
        "kishu_shisu", "chokyo_shisu", "kyusha_shisu", "ls_shisu",
        "kyori",
    ]

    for col in numeric_cols:
        if col in df.columns:
            df[col] = safe_to_numeric(df[col])

    return df
