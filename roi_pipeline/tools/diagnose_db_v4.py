"""
DB診断スクリプト v4 — 全テーブルの生カラム値ダンプ & パースずれ逆算

確定事実:
    1. race_shikonen = YY(2) + 回(2) + 日目(2) — 唯一信頼できるキー
    2. kyi/cyb/joa の kaisai_kai/nichime は壊れている
    3. bac の kaisai_nen に MMDD が入っている（nen='0127' = 1月27日）
    4. bac の race_bango に YY が入っている（race='24' = 2024年）
    5. se の keibajo_code に '45' という異常値
    6. cyb の umaban に 10/20/30 という異常値

目的:
    各テーブルの全カラムを生ダンプし、パースずれのマッピングを完全に逆算する。

使用方法:
    py -3.12 roi_pipeline/tools/diagnose_db_v4.py
"""
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from roi_pipeline.config.db import get_connection


def dump_columns(cur, table_name, limit=5):
    """テーブルの全カラム名と先頭数行を表示する"""
    # カラム名一覧
    cur.execute("""
        SELECT column_name, data_type, character_maximum_length
        FROM information_schema.columns
        WHERE table_name = %s
        ORDER BY ordinal_position
    """, (table_name,))
    columns = cur.fetchall()
    
    print(f"\n    カラム数: {len(columns)}")
    for c in columns:
        max_len = f"({c[2]})" if c[2] else ""
        print(f"      {c[0]:30s} {c[1]}{max_len}")
    
    # 先頭データ
    col_names = [c[0] for c in columns]
    cur.execute(f"SELECT * FROM {table_name} LIMIT {limit}")
    rows = cur.fetchall()
    
    print(f"\n    先頭{len(rows)}行の生データ:")
    for i, row in enumerate(rows):
        print(f"\n    --- Row {i+1} ---")
        for col_name, val in zip(col_names, row):
            print(f"      {col_name:30s} = {val!r}")


def main():
    conn = get_connection()
    cur = conn.cursor()

    print("=" * 70)
    print("DB診断 v4 — 全テーブル生カラムダンプ & パースずれ逆算")
    print("=" * 70)

    # =================================================================
    # A. 各テーブルの生データダンプ
    # =================================================================
    
    # --- A1. jvd_se (JRA-VAN成績) ---
    print("\n[A1] === jvd_se (JRA-VAN成績) ===")
    print("    ※ JRA-VAN由来。PC-KEIBAのJRDBパースとは別ルート")
    cur.execute("SELECT COUNT(*) FROM jvd_se")
    print(f"    行数: {cur.fetchone()[0]:,}")
    dump_columns(cur, "jvd_se", limit=3)
    
    # keibajo_code の分布
    print("\n    keibajo_code 分布:")
    cur.execute("""
        SELECT TRIM(keibajo_code) AS kb, LENGTH(keibajo_code) AS len, COUNT(*) AS cnt
        FROM jvd_se
        GROUP BY TRIM(keibajo_code), LENGTH(keibajo_code)
        ORDER BY kb
        LIMIT 20
    """)
    for r in cur.fetchall():
        print(f"      keibajo={r[0]!r} len={r[1]} count={r[2]:,}")

    # --- A2. jvd_ra (JRA-VANレース情報) ---
    print("\n[A2] === jvd_ra (JRA-VANレース情報) ===")
    cur.execute("SELECT COUNT(*) FROM jvd_ra")
    print(f"    行数: {cur.fetchone()[0]:,}")
    dump_columns(cur, "jvd_ra", limit=2)
    
    # --- A3. jrd_kyi (JRDB KYI) ---
    print("\n[A3] === jrd_kyi (JRDB KYI) ===")
    cur.execute("SELECT COUNT(*) FROM jrd_kyi")
    print(f"    行数: {cur.fetchone()[0]:,}")
    dump_columns(cur, "jrd_kyi", limit=3)

    # --- A4. jrd_cyb (JRDB CYB) ---
    print("\n[A4] === jrd_cyb (JRDB CYB) ===")
    cur.execute("SELECT COUNT(*) FROM jrd_cyb")
    print(f"    行数: {cur.fetchone()[0]:,}")
    dump_columns(cur, "jrd_cyb", limit=3)

    # --- A5. jrd_joa (JRDB JOA) ---
    print("\n[A5] === jrd_joa (JRDB JOA) ===")
    cur.execute("SELECT COUNT(*) FROM jrd_joa")
    print(f"    行数: {cur.fetchone()[0]:,}")
    dump_columns(cur, "jrd_joa", limit=3)

    # --- A6. jrd_bac (JRDB BAC) ---
    print("\n[A6] === jrd_bac (JRDB BAC) ===")
    cur.execute("SELECT COUNT(*) FROM jrd_bac")
    print(f"    行数: {cur.fetchone()[0]:,}")
    dump_columns(cur, "jrd_bac", limit=3)

    # =================================================================
    # B. jvd_se の keibajo_code 深堀り
    # =================================================================
    print("\n[B] === jvd_se keibajo_code 深堀り ===")
    
    # 2024年1月のデータ（ORDER BYを変えて複数レコード見る）
    print("\n[B1] se: 2024年1月 先頭30行のキーカラム")
    cur.execute("""
        SELECT keibajo_code, kaisai_nen, kaisai_tsukihi, kaisai_kai, 
               kaisai_nichime, race_bango, umaban, kakutei_chakujun,
               tansho_odds
        FROM jvd_se
        WHERE kaisai_nen = '2024'
        ORDER BY kaisai_tsukihi, keibajo_code, race_bango, umaban
        LIMIT 30
    """)
    print("    keibajo | nen  | tsukihi | kai | nichime | race | uma | chakujun | odds")
    print("    " + "-" * 85)
    for r in cur.fetchall():
        print(f"    {r[0]:>7} | {r[1]} | {r[2]:>7} | {r[3]:>3} | {r[4]:>7} | {r[5]:>4} | {r[6]:>3} | {r[7]:>8} | {r[8]}")

    # =================================================================
    # C. race_shikonen ベースでの突合テスト
    # =================================================================
    print("\n[C] === race_shikonen直接突合テスト ===")
    
    # C1. jrd_cyb の race_shikonen ユニーク値サンプル
    print("\n[C1] cyb: race_shikonen先頭値と末尾値")
    cur.execute("SELECT MIN(race_shikonen), MAX(race_shikonen) FROM jrd_cyb")
    r = cur.fetchone()
    print(f"    MIN={r[0]!r}, MAX={r[1]!r}")
    
    # C2. se側から合成したshikonenの範囲
    print("\n[C2] se: computed_shikonen 範囲（CAST(kai as INT)で合成）")
    cur.execute("""
        SELECT 
            MIN(SUBSTRING(kaisai_nen, 3, 2) 
                || LPAD(CAST(CAST(NULLIF(TRIM(kaisai_kai), '') AS INTEGER) AS TEXT), 2, '0')
                || LPAD(CAST(CAST(NULLIF(TRIM(kaisai_nichime), '') AS INTEGER) AS TEXT), 2, '0')) AS min_v,
            MAX(SUBSTRING(kaisai_nen, 3, 2) 
                || LPAD(CAST(CAST(NULLIF(TRIM(kaisai_kai), '') AS INTEGER) AS TEXT), 2, '0')
                || LPAD(CAST(CAST(NULLIF(TRIM(kaisai_nichime), '') AS INTEGER) AS TEXT), 2, '0')) AS max_v
        FROM jvd_se
    """)
    r = cur.fetchone()
    print(f"    MIN={r[0]!r}, MAX={r[1]!r}")
    
    # C3. se側のcomputed_shikonen分布（2024年限定）
    print("\n[C3] se: computed_shikonen ユニーク値 (2024年、先頭20)")
    cur.execute("""
        SELECT 
            SUBSTRING(kaisai_nen, 3, 2) 
                || LPAD(CAST(CAST(NULLIF(TRIM(kaisai_kai), '') AS INTEGER) AS TEXT), 2, '0')
                || LPAD(CAST(CAST(NULLIF(TRIM(kaisai_nichime), '') AS INTEGER) AS TEXT), 2, '0') AS cs,
            COUNT(*) AS cnt
        FROM jvd_se
        WHERE kaisai_nen = '2024'
        GROUP BY cs
        ORDER BY cs
        LIMIT 20
    """)
    for r in cur.fetchall():
        print(f"    {r[0]}: {r[1]:,}件")

    # C4. cyb側のrace_shikonen分布（24で始まるもの、先頭20）
    print("\n[C4] cyb: race_shikonen ユニーク値 (24xxxx、先頭20)")
    cur.execute("""
        SELECT race_shikonen, COUNT(*) AS cnt
        FROM jrd_cyb
        WHERE SUBSTRING(race_shikonen, 1, 2) = '24'
        GROUP BY race_shikonen
        ORDER BY race_shikonen
        LIMIT 20
    """)
    for r in cur.fetchall():
        print(f"    {r[0]}: {r[1]:,}件")

    # C5. se(computed) と cyb(shikonen) のINTERSECT
    print("\n[C5] se(computed) と cyb(shikonen) の共通値 (2024年)")
    cur.execute("""
        SELECT cs FROM (
            SELECT DISTINCT 
                SUBSTRING(kaisai_nen, 3, 2) 
                    || LPAD(CAST(CAST(NULLIF(TRIM(kaisai_kai), '') AS INTEGER) AS TEXT), 2, '0')
                    || LPAD(CAST(CAST(NULLIF(TRIM(kaisai_nichime), '') AS INTEGER) AS TEXT), 2, '0') AS cs
            FROM jvd_se
            WHERE kaisai_nen = '2024'
        ) se_vals
        INNER JOIN (
            SELECT DISTINCT race_shikonen AS cs
            FROM jrd_cyb
            WHERE SUBSTRING(race_shikonen, 1, 2) = '24'
        ) cyb_vals
        USING (cs)
        ORDER BY cs
        LIMIT 30
    """)
    intersect = cur.fetchall()
    print(f"    共通値の数: {len(intersect)}")
    for r in intersect[:20]:
        print(f"    {r[0]}")

    # =================================================================
    # D. keibajo_code 問題の深堀り
    # =================================================================
    print("\n[D] === keibajo_code マッチングテスト ===")
    
    # D1. se側とcyb側のkeibajo_codeを直接比較
    print("\n[D1] se keibajo_code ユニーク値:")
    cur.execute("""
        SELECT DISTINCT keibajo_code, LENGTH(keibajo_code) AS len
        FROM jvd_se
        ORDER BY keibajo_code
        LIMIT 20
    """)
    for r in cur.fetchall():
        print(f"    {r[0]!r} (len={r[1]})")
    
    print("\n[D2] cyb keibajo_code ユニーク値:")
    cur.execute("""
        SELECT DISTINCT keibajo_code, LENGTH(keibajo_code) AS len
        FROM jrd_cyb
        ORDER BY keibajo_code
        LIMIT 20
    """)
    for r in cur.fetchall():
        print(f"    {r[0]!r} (len={r[1]})")

    # D3. TRIM後のkeibajo_codeの共通値
    print("\n[D3] TRIM後のkeibajo_code共通値:")
    cur.execute("""
        SELECT DISTINCT TRIM(se.keibajo_code) AS kb
        FROM jvd_se AS se
        INNER JOIN jrd_cyb AS cyb ON TRIM(se.keibajo_code) = TRIM(cyb.keibajo_code)
        ORDER BY kb
    """)
    common = cur.fetchall()
    print(f"    共通値の数: {len(common)}")
    for r in common[:15]:
        print(f"    {r[0]!r}")

    print("\n" + "=" * 70)
    print("診断v4完了")
    print("=" * 70)

    conn.close()


if __name__ == "__main__":
    main()
