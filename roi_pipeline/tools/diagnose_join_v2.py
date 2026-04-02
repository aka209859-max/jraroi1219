"""
JOIN v2 診断スクリプト: JRA限定でマッチ率を確認する

jvd_se にはJRA(中央)とNAR(地方)が混在している。
JRDBはJRAのみなので、keibajo_code <= 10 でフィルタする必要がある。
"""
import sys
from pathlib import Path

_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import psycopg2

DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 5432,
    "database": "pckeiba",
    "user": "postgres",
    "password": "postgres123",
}

JRA_CODES = "('01','02','03','04','05','06','07','08','09','10')"

SYNTH_KEY8 = """
    TRIM(se.keibajo_code)
    || SUBSTRING(se.kaisai_nen, 3, 2)
    || CAST(CAST(NULLIF(TRIM(se.kaisai_kai), '') AS INTEGER) AS TEXT)
    || CASE
        WHEN CAST(NULLIF(TRIM(se.kaisai_nichime), '') AS INTEGER) <= 9
            THEN CAST(CAST(NULLIF(TRIM(se.kaisai_nichime), '') AS INTEGER) AS TEXT)
        WHEN CAST(NULLIF(TRIM(se.kaisai_nichime), '') AS INTEGER) = 10 THEN 'a'
        WHEN CAST(NULLIF(TRIM(se.kaisai_nichime), '') AS INTEGER) = 11 THEN 'b'
        WHEN CAST(NULLIF(TRIM(se.kaisai_nichime), '') AS INTEGER) = 12 THEN 'c'
        ELSE CAST(CAST(NULLIF(TRIM(se.kaisai_nichime), '') AS INTEGER) AS TEXT)
       END
    || LPAD(CAST(CAST(NULLIF(TRIM(se.race_bango), '') AS INTEGER) AS TEXT), 2, '0')
"""


def main():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # 1. JRAのみ JOIN マッチ率 (全期間)
    print("=== JRAのみ KYI JOIN マッチ率 (2016-11 ~ 2025-12) ===")
    cur.execute(f"""
        SELECT
            COUNT(*) AS total,
            COUNT(kyi.idm) AS matched,
            ROUND(COUNT(kyi.idm)::NUMERIC / NULLIF(COUNT(*), 0) * 100, 2) AS pct
        FROM jvd_se se
        LEFT JOIN jrd_kyi_fixed kyi
            ON ({SYNTH_KEY8}) = kyi.jrdb_race_key8
            AND TRIM(se.umaban) = TRIM(kyi.umaban)
        WHERE (se.kaisai_nen || se.kaisai_tsukihi) >= '20161101'
            AND (se.kaisai_nen || se.kaisai_tsukihi) <= '20251231'
            AND TRIM(se.keibajo_code) IN {JRA_CODES}
    """)
    row = cur.fetchone()
    print(f"  total={row[0]:,}, matched={row[1]:,}, pct={row[2]}%")

    # 2. 2024年
    print()
    print("=== JRAのみ KYI JOIN マッチ率 (2024年) ===")
    cur.execute(f"""
        SELECT
            COUNT(*) AS total,
            COUNT(kyi.idm) AS matched,
            ROUND(COUNT(kyi.idm)::NUMERIC / NULLIF(COUNT(*), 0) * 100, 2) AS pct
        FROM jvd_se se
        LEFT JOIN jrd_kyi_fixed kyi
            ON ({SYNTH_KEY8}) = kyi.jrdb_race_key8
            AND TRIM(se.umaban) = TRIM(kyi.umaban)
        WHERE se.kaisai_nen = '2024'
            AND TRIM(se.keibajo_code) IN {JRA_CODES}
    """)
    row = cur.fetchone()
    print(f"  total={row[0]:,}, matched={row[1]:,}, pct={row[2]}%")

    # 3. マッチしないJRAレコードのサンプル
    print()
    print("=== マッチしないJRAレコード サンプル (2024年) ===")
    cur.execute(f"""
        SELECT
            TRIM(se.keibajo_code) AS basho,
            se.kaisai_nen, se.kaisai_tsukihi,
            TRIM(se.kaisai_kai) AS kai,
            TRIM(se.kaisai_nichime) AS nichime,
            TRIM(se.race_bango) AS race,
            TRIM(se.umaban) AS uma,
            ({SYNTH_KEY8}) AS synth_key8
        FROM jvd_se se
        LEFT JOIN jrd_kyi_fixed kyi
            ON ({SYNTH_KEY8}) = kyi.jrdb_race_key8
            AND TRIM(se.umaban) = TRIM(kyi.umaban)
        WHERE kyi.idm IS NULL
            AND se.kaisai_nen = '2024'
            AND TRIM(se.keibajo_code) IN {JRA_CODES}
        LIMIT 15
    """)
    rows = cur.fetchall()
    if not rows:
        print("  なし！全レコードマッチ！")
    else:
        for row in rows:
            print(f"  basho={row[0]} nen={row[1]} tsukihi={row[2]} kai={row[3]} nichime={row[4]} race={row[5]} uma={row[6]} => key8={row[7]}")

        # 4. そのキーがJRDB側にあるか確認
        sample_key = rows[0][7]
        prefix = sample_key[:4]
        print(f"\n=== JRDB側に '{prefix}*' のキーは存在するか？ ===")
        cur.execute(f"""
            SELECT DISTINCT jrdb_race_key8
            FROM jrd_kyi_fixed
            WHERE jrdb_race_key8 LIKE '{prefix}%'
            ORDER BY jrdb_race_key8
            LIMIT 10
        """)
        for row in cur.fetchall():
            print(f"  {row[0]}")

    conn.close()


if __name__ == "__main__":
    main()
