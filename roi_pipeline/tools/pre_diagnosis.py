"""
事前診断SQL: CEOのPC上で実行し、3つの問題の根本原因を特定する

使用方法:
    cd E:\\jraroi1219
    py -3.12 roi_pipeline/tools/pre_diagnosis.py

出力結果を全てコピーして開発者AIに貼り付けてください。
"""
import sys

try:
    import psycopg2
except ImportError:
    print("ERROR: psycopg2がインストールされていません。")
    print("  pip install psycopg2-binary")
    sys.exit(1)


DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 5432,
    "database": "pckeiba",
    "user": "postgres",
    "password": "postgres123",
}


def run_query(conn, label: str, sql: str) -> list:
    """SQLを実行して結果を表示する。"""
    print(f"\n{'='*70}")
    print(f"  {label}")
    print(f"{'='*70}")
    try:
        with conn.cursor() as cur:
            cur.execute(sql)
            cols = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            # ヘッダー表示
            print("  " + " | ".join(cols))
            print("  " + "-" * (sum(max(len(str(c)), 12) for c in cols) + 3 * (len(cols) - 1)))
            for row in rows[:50]:  # 最大50行
                print("  " + " | ".join(str(v) for v in row))
            if len(rows) > 50:
                print(f"  ... ({len(rows)} rows, showing first 50)")
            print(f"  [{len(rows)} rows]")
            return rows
    except Exception as e:
        print(f"  ERROR: {e}")
        return []


def main():
    print("=" * 70)
    print("  JRDB データ事前診断 v1.0")
    print("  目的: PC-KEIBAパースずれ・JOIN問題・1529%バグの根本原因特定")
    print("=" * 70)

    try:
        conn = psycopg2.connect(**DB_CONFIG)
    except Exception as e:
        print(f"DB接続エラー: {e}")
        sys.exit(1)

    # ===================================================================
    # Q1: オッズスケール診断
    # ===================================================================
    run_query(conn, "Q1: tansho_odds 分布（スケール判定）", """
        SELECT
            MIN(CAST(NULLIF(TRIM(tansho_odds), '') AS NUMERIC)) AS min_val,
            MAX(CAST(NULLIF(TRIM(tansho_odds), '') AS NUMERIC)) AS max_val,
            AVG(CAST(NULLIF(TRIM(tansho_odds), '') AS NUMERIC)) AS avg_val,
            PERCENTILE_CONT(0.5) WITHIN GROUP (
                ORDER BY CAST(NULLIF(TRIM(tansho_odds), '') AS NUMERIC)
            ) AS median_val,
            PERCENTILE_CONT(0.25) WITHIN GROUP (
                ORDER BY CAST(NULLIF(TRIM(tansho_odds), '') AS NUMERIC)
            ) AS p25_val,
            PERCENTILE_CONT(0.75) WITHIN GROUP (
                ORDER BY CAST(NULLIF(TRIM(tansho_odds), '') AS NUMERIC)
            ) AS p75_val,
            COUNT(*) AS total
        FROM jvd_se
        WHERE TRIM(tansho_odds) <> '' AND tansho_odds IS NOT NULL
    """)

    # ===================================================================
    # Q2: 既存テーブルにレースキー8byteカラムがあるか
    # ===================================================================
    run_query(conn, "Q2: JRDBテーブル全カラム一覧（jrd_kyi先頭10カラム）", """
        SELECT column_name, data_type, character_maximum_length
        FROM information_schema.columns
        WHERE table_name = 'jrd_kyi'
        ORDER BY ordinal_position
    """)

    run_query(conn, "Q2b: JRDBテーブル全カラム一覧（jrd_cyb）", """
        SELECT column_name, data_type, character_maximum_length
        FROM information_schema.columns
        WHERE table_name = 'jrd_cyb'
        ORDER BY ordinal_position
    """)

    run_query(conn, "Q2c: JRDBテーブル全カラム一覧（jrd_bac）", """
        SELECT column_name, data_type, character_maximum_length
        FROM information_schema.columns
        WHERE table_name = 'jrd_bac'
        ORDER BY ordinal_position
    """)

    run_query(conn, "Q2d: JRDBテーブル全カラム一覧（jrd_joa）", """
        SELECT column_name, data_type, character_maximum_length
        FROM information_schema.columns
        WHERE table_name = 'jrd_joa'
        ORDER BY ordinal_position
    """)

    # ===================================================================
    # Q3: アラインメントチェック — 各テーブルのJOINキー列サンプル
    # ===================================================================
    run_query(conn, "Q3: jrd_kyi JOINキーサンプル（2024年分、先頭20行）", """
        SELECT
            race_shikonen,
            keibajo_code,
            kaisai_kai,
            kaisai_nichime,
            race_bango,
            umaban,
            LENGTH(race_shikonen) AS len_shikonen,
            LENGTH(keibajo_code) AS len_basho,
            LENGTH(kaisai_kai) AS len_kai,
            LENGTH(kaisai_nichime) AS len_nichi,
            LENGTH(umaban) AS len_uma,
            idm,
            sogo_shisu
        FROM jrd_kyi
        WHERE race_shikonen LIKE '24%'
        ORDER BY race_shikonen
        LIMIT 20
    """)

    run_query(conn, "Q3b: jrd_cyb JOINキーサンプル（2024年分、先頭20行）", """
        SELECT
            race_shikonen,
            keibajo_code,
            kaisai_kai,
            kaisai_nichime,
            race_bango,
            umaban,
            chokyo_hyoka,
            LENGTH(keibajo_code) AS len_basho,
            LENGTH(umaban) AS len_uma
        FROM jrd_cyb
        WHERE race_shikonen LIKE '24%'
        ORDER BY race_shikonen
        LIMIT 20
    """)

    run_query(conn, "Q3c: jrd_bac JOINキーサンプル（2024年分、先頭20行）", """
        SELECT
            race_shikonen,
            keibajo_code,
            kaisai_nen,
            kaisai_tsukihi,
            kaisai_kai,
            kaisai_nichime,
            race_bango,
            juryo_shubetsu_code,
            LENGTH(keibajo_code) AS len_basho,
            LENGTH(race_bango) AS len_race
        FROM jrd_bac
        WHERE race_shikonen LIKE '24%'
        ORDER BY race_shikonen
        LIMIT 20
    """)

    run_query(conn, "Q3d: jrd_joa JOINキーサンプル（2024年分、先頭20行）", """
        SELECT
            race_shikonen,
            keibajo_code,
            kaisai_kai,
            kaisai_nichime,
            race_bango,
            umaban,
            ls_shisu,
            LENGTH(keibajo_code) AS len_basho,
            LENGTH(umaban) AS len_uma
        FROM jrd_joa
        WHERE race_shikonen LIKE '24%'
        ORDER BY race_shikonen
        LIMIT 20
    """)

    # ===================================================================
    # Q4: jvd_se JOINキーサンプル（比較用）
    # ===================================================================
    run_query(conn, "Q4: jvd_se JOINキーサンプル（2024年1月、先頭20行）", """
        SELECT
            keibajo_code,
            kaisai_nen,
            kaisai_tsukihi,
            kaisai_kai,
            kaisai_nichime,
            race_bango,
            umaban,
            tansho_odds,
            kakutei_chakujun,
            LENGTH(keibajo_code) AS len_basho,
            LENGTH(kaisai_kai) AS len_kai,
            LENGTH(kaisai_nichime) AS len_nichi,
            LENGTH(umaban) AS len_uma
        FROM jvd_se
        WHERE kaisai_nen = '2024' AND kaisai_tsukihi >= '0101' AND kaisai_tsukihi <= '0131'
        ORDER BY kaisai_tsukihi, keibajo_code, race_bango, umaban
        LIMIT 20
    """)

    # ===================================================================
    # Q5: race_shikonen合成テスト
    # ===================================================================
    run_query(conn, "Q5: race_shikonen合成テスト（JRA-VAN側から合成して一致率確認）", """
        WITH se_with_shikonen AS (
            SELECT
                SUBSTRING(kaisai_nen, 3, 2)
                || LPAD(CAST(CAST(NULLIF(TRIM(kaisai_kai), '') AS INTEGER) AS TEXT), 2, '0')
                || LPAD(CAST(CAST(NULLIF(TRIM(kaisai_nichime), '') AS INTEGER) AS TEXT), 2, '0')
                AS computed_shikonen,
                keibajo_code, kaisai_nen, kaisai_tsukihi, kaisai_kai,
                kaisai_nichime, race_bango, umaban
            FROM jvd_se
            WHERE kaisai_nen = '2024'
        ),
        kyi_shikonen AS (
            SELECT DISTINCT race_shikonen FROM jrd_kyi WHERE race_shikonen LIKE '24%'
        )
        SELECT
            (SELECT COUNT(DISTINCT computed_shikonen) FROM se_with_shikonen) AS se_unique,
            (SELECT COUNT(*) FROM kyi_shikonen) AS kyi_unique,
            (SELECT COUNT(*)
             FROM (SELECT DISTINCT computed_shikonen FROM se_with_shikonen) a
             INNER JOIN kyi_shikonen b ON a.computed_shikonen = b.race_shikonen
            ) AS matched
    """)

    # ===================================================================
    # Q6: 現行JOINマッチ率（改めて確認）
    # ===================================================================
    run_query(conn, "Q6: 現行JOIN方式マッチ率（kyi、2024年1月）", """
        SELECT
            COUNT(*) AS total_se,
            COUNT(kyi.idm) AS kyi_matched,
            ROUND(COUNT(kyi.idm)::NUMERIC / COUNT(*) * 100, 2) AS match_pct
        FROM jvd_se AS se
        LEFT JOIN jrd_kyi AS kyi
            ON TRIM(se.keibajo_code) = TRIM(kyi.keibajo_code)
            AND SUBSTRING(se.kaisai_nen, 3, 2) = SUBSTRING(kyi.race_shikonen, 1, 2)
            AND CAST(NULLIF(TRIM(se.kaisai_kai), '') AS INTEGER)
                = CAST(NULLIF(TRIM(kyi.kaisai_kai), '') AS INTEGER)
            AND CAST(NULLIF(TRIM(se.kaisai_nichime), '') AS INTEGER)
                = CAST(NULLIF(TRIM(kyi.kaisai_nichime), '') AS INTEGER)
            AND CAST(NULLIF(TRIM(se.race_bango), '') AS INTEGER)
                = CAST(NULLIF(TRIM(kyi.race_bango), '') AS INTEGER)
            AND CAST(NULLIF(TRIM(se.umaban), '') AS INTEGER)
                = CAST(NULLIF(TRIM(kyi.umaban), '') AS INTEGER)
        WHERE se.kaisai_nen = '2024'
            AND se.kaisai_tsukihi >= '0101' AND se.kaisai_tsukihi <= '0131'
    """)

    # ===================================================================
    # Q7: race_shikonenベースJOINテスト（race_shikonen + race_bango + umaban）
    # ===================================================================
    run_query(conn, "Q7: race_shikonenベースJOIN（race_shikonen+basho+race+uma、2024年1月）", """
        WITH se_synth AS (
            SELECT
                se.*,
                SUBSTRING(se.kaisai_nen, 3, 2)
                || LPAD(CAST(CAST(NULLIF(TRIM(se.kaisai_kai), '') AS INTEGER) AS TEXT), 2, '0')
                || LPAD(CAST(CAST(NULLIF(TRIM(se.kaisai_nichime), '') AS INTEGER) AS TEXT), 2, '0')
                AS synth_shikonen
            FROM jvd_se se
            WHERE se.kaisai_nen = '2024'
                AND se.kaisai_tsukihi >= '0101' AND se.kaisai_tsukihi <= '0131'
        )
        SELECT
            COUNT(*) AS total_se,
            COUNT(kyi.idm) AS kyi_matched,
            ROUND(COUNT(kyi.idm)::NUMERIC / NULLIF(COUNT(*), 0) * 100, 2) AS match_pct
        FROM se_synth se
        LEFT JOIN jrd_kyi kyi
            ON se.synth_shikonen = kyi.race_shikonen
            AND TRIM(se.keibajo_code) = TRIM(kyi.keibajo_code)
            AND CAST(NULLIF(TRIM(se.race_bango), '') AS INTEGER)
                = CAST(NULLIF(TRIM(kyi.race_bango), '') AS INTEGER)
            AND CAST(NULLIF(TRIM(se.umaban), '') AS INTEGER)
                = CAST(NULLIF(TRIM(kyi.umaban), '') AS INTEGER)
    """)

    # ===================================================================
    # Q8: 8byte race_key合成テスト
    #   JRA-VAN: keibajo_code(2) + year_last2(2) + kai(1) + hex(nichime)(1) + race(2)
    #   JRDB:    直接race_shikonen(6)から basho(2)+YY(2)+回(1)+日hex(1)+R(2) を合成
    # ===================================================================
    run_query(conn, "Q8: 8byte race_key合成テスト（JRA-VAN側）", """
        SELECT
            TRIM(keibajo_code)
            || SUBSTRING(kaisai_nen, 3, 2)
            || CAST(CAST(NULLIF(TRIM(kaisai_kai), '') AS INTEGER) AS TEXT)
            || CASE
                WHEN CAST(NULLIF(TRIM(kaisai_nichime), '') AS INTEGER) <= 9
                    THEN CAST(CAST(NULLIF(TRIM(kaisai_nichime), '') AS INTEGER) AS TEXT)
                WHEN CAST(NULLIF(TRIM(kaisai_nichime), '') AS INTEGER) = 10 THEN 'a'
                WHEN CAST(NULLIF(TRIM(kaisai_nichime), '') AS INTEGER) = 11 THEN 'b'
                WHEN CAST(NULLIF(TRIM(kaisai_nichime), '') AS INTEGER) = 12 THEN 'c'
                ELSE CAST(CAST(NULLIF(TRIM(kaisai_nichime), '') AS INTEGER) AS TEXT)
               END
            || LPAD(CAST(CAST(NULLIF(TRIM(race_bango), '') AS INTEGER) AS TEXT), 2, '0')
            AS synth_race_key8,
            keibajo_code, kaisai_nen, kaisai_kai, kaisai_nichime, race_bango, umaban
        FROM jvd_se
        WHERE kaisai_nen = '2024' AND kaisai_tsukihi >= '0101' AND kaisai_tsukihi <= '0131'
        ORDER BY kaisai_tsukihi, race_bango, umaban
        LIMIT 15
    """)

    # ===================================================================
    # Q9: テーブル行数確認
    # ===================================================================
    run_query(conn, "Q9: テーブル行数", """
        SELECT
            (SELECT COUNT(*) FROM jvd_se) AS jvd_se_count,
            (SELECT COUNT(*) FROM jvd_ra) AS jvd_ra_count,
            (SELECT COUNT(*) FROM jrd_kyi) AS jrd_kyi_count,
            (SELECT COUNT(*) FROM jrd_cyb) AS jrd_cyb_count,
            (SELECT COUNT(*) FROM jrd_joa) AS jrd_joa_count,
            (SELECT COUNT(*) FROM jrd_bac) AS jrd_bac_count
    """)

    # ===================================================================
    # Q10: race_shikonen値のパターン分析
    # ===================================================================
    run_query(conn, "Q10: kyi race_shikonen 先頭2桁の年分布", """
        SELECT
            SUBSTRING(race_shikonen, 1, 2) AS yy,
            COUNT(*) AS cnt,
            MIN(race_shikonen) AS min_val,
            MAX(race_shikonen) AS max_val
        FROM jrd_kyi
        GROUP BY SUBSTRING(race_shikonen, 1, 2)
        ORDER BY yy
    """)

    conn.close()

    print()
    print("=" * 70)
    print("  診断完了！")
    print("  上記の出力結果を全てコピーして開発者AIに貼り付けてください。")
    print("=" * 70)


if __name__ == "__main__":
    main()
