"""
DB診断スクリプト v3 — JOIN 42%マッチの根本原因を特定する

目的:
    se 5,919行 vs cyb YY年一致JOIN 2,495件 (42%) の差分58%の原因を特定する。
    kai/nichimeの値フォーマット差、race_shikonenの3-6桁目との関係を解明する。

使用方法:
    py -3.12 roi_pipeline/tools/diagnose_db_v3.py
"""
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from roi_pipeline.config.db import get_connection


def main():
    conn = get_connection()
    cur = conn.cursor()

    print("=" * 70)
    print("DB診断 v3 — JOIN 42%マッチの根本原因特定")
    print("=" * 70)

    # =================================================================
    # A. race_shikonenの構造解析
    # =================================================================
    print("\n[A] === race_shikonen 構造解析 ===")

    # A1. race_shikonenの3-4桁目と5-6桁目 vs kaisai_kai/nichime
    print("\n[A1] cyb: race_shikonen分解 vs kaisai_kai/nichime (2024年データ)")
    cur.execute("""
        SELECT 
            race_shikonen,
            SUBSTRING(race_shikonen, 1, 2) AS yy,
            SUBSTRING(race_shikonen, 3, 2) AS digits_34,
            SUBSTRING(race_shikonen, 5, 2) AS digits_56,
            kaisai_kai,
            kaisai_nichime,
            keibajo_code,
            race_bango
        FROM jrd_cyb
        WHERE SUBSTRING(race_shikonen, 1, 2) = '24'
        ORDER BY race_shikonen
        LIMIT 30
    """)
    print("    shikonen | YY | 3-4桁 | 5-6桁 | kai    | nichime | keibajo | race")
    print("    " + "-" * 75)
    for r in cur.fetchall():
        match_kai = "OK" if r[2].strip() == r[4].strip() or str(int(r[2])) == r[4].strip() else "NG"
        match_nich = "OK" if r[3].strip() == r[5].strip() or str(int(r[3])) == r[5].strip() else "NG"
        print(f"    {r[0]} | {r[1]} | {r[2]:>5} | {r[3]:>5} | {r[4]:>6} | {r[5]:>7} | {r[6]:>7} | {r[7]:>4}  kai={match_kai} nich={match_nich}")

    # A2. 3-4桁目 == kai か検証（全データ）
    print("\n[A2] cyb全体: race_shikonen 3-4桁目 vs kaisai_kai 一致率")
    cur.execute("""
        SELECT 
            COUNT(*) AS total,
            SUM(CASE WHEN CAST(SUBSTRING(race_shikonen, 3, 2) AS INTEGER) 
                        = CAST(NULLIF(TRIM(kaisai_kai), '') AS INTEGER) 
                     THEN 1 ELSE 0 END) AS kai_match,
            SUM(CASE WHEN CAST(SUBSTRING(race_shikonen, 5, 2) AS INTEGER) 
                        = CAST(NULLIF(TRIM(kaisai_nichime), '') AS INTEGER) 
                     THEN 1 ELSE 0 END) AS nichime_match
        FROM jrd_cyb
    """)
    r = cur.fetchone()
    print(f"    total: {r[0]:,}")
    print(f"    3-4桁 == kai: {r[1]:,} ({r[1]/r[0]*100:.1f}%)")
    print(f"    5-6桁 == nichime: {r[2]:,} ({r[2]/r[0]*100:.1f}%)")

    # =================================================================
    # B. マッチしない行の特定
    # =================================================================
    print("\n[B] === マッチしない行の分析 ===")

    # B1. se側で2024年1月のレコードのうち、cybとマッチしないもの
    print("\n[B1] se: 2024年1月でcybとYY年一致JOINがマッチしない行のサンプル")
    cur.execute("""
        SELECT se.keibajo_code, se.kaisai_nen, se.kaisai_tsukihi, 
               se.kaisai_kai, se.kaisai_nichime, se.race_bango, se.umaban
        FROM jvd_se AS se
        LEFT JOIN jrd_cyb AS cyb
            ON TRIM(se.keibajo_code) = TRIM(cyb.keibajo_code)
            AND SUBSTRING(se.kaisai_nen, 3, 2) = SUBSTRING(cyb.race_shikonen, 1, 2)
            AND CAST(NULLIF(TRIM(se.kaisai_kai), '') AS INTEGER)
                = CAST(NULLIF(TRIM(cyb.kaisai_kai), '') AS INTEGER)
            AND CAST(NULLIF(TRIM(se.kaisai_nichime), '') AS INTEGER)
                = CAST(NULLIF(TRIM(cyb.kaisai_nichime), '') AS INTEGER)
            AND CAST(NULLIF(TRIM(se.race_bango), '') AS INTEGER)
                = CAST(NULLIF(TRIM(cyb.race_bango), '') AS INTEGER)
            AND CAST(NULLIF(TRIM(se.umaban), '') AS INTEGER)
                = CAST(NULLIF(TRIM(cyb.umaban), '') AS INTEGER)
        WHERE (se.kaisai_nen || se.kaisai_tsukihi) >= '20240101'
            AND (se.kaisai_nen || se.kaisai_tsukihi) <= '20240131'
            AND cyb.race_shikonen IS NULL
        LIMIT 15
    """)
    unmatched = cur.fetchall()
    print(f"    未マッチ件数（先頭15件）: {len(unmatched)}")
    for r in unmatched:
        print(f"    keibajo={r[0]!r} nen={r[1]!r} tsukihi={r[2]!r} "
              f"kai={r[3]!r} nichime={r[4]!r} race={r[5]!r} uma={r[6]!r}")

    # B2. 未マッチ行のkeibajo_code分布
    print("\n[B2] 未マッチ行のkeibajo_code分布")
    cur.execute("""
        SELECT se.keibajo_code, COUNT(*) AS cnt
        FROM jvd_se AS se
        LEFT JOIN jrd_cyb AS cyb
            ON TRIM(se.keibajo_code) = TRIM(cyb.keibajo_code)
            AND SUBSTRING(se.kaisai_nen, 3, 2) = SUBSTRING(cyb.race_shikonen, 1, 2)
            AND CAST(NULLIF(TRIM(se.kaisai_kai), '') AS INTEGER)
                = CAST(NULLIF(TRIM(cyb.kaisai_kai), '') AS INTEGER)
            AND CAST(NULLIF(TRIM(se.kaisai_nichime), '') AS INTEGER)
                = CAST(NULLIF(TRIM(cyb.kaisai_nichime), '') AS INTEGER)
            AND CAST(NULLIF(TRIM(se.race_bango), '') AS INTEGER)
                = CAST(NULLIF(TRIM(cyb.race_bango), '') AS INTEGER)
            AND CAST(NULLIF(TRIM(se.umaban), '') AS INTEGER)
                = CAST(NULLIF(TRIM(cyb.umaban), '') AS INTEGER)
        WHERE (se.kaisai_nen || se.kaisai_tsukihi) >= '20240101'
            AND (se.kaisai_nen || se.kaisai_tsukihi) <= '20240131'
            AND cyb.race_shikonen IS NULL
        GROUP BY se.keibajo_code
        ORDER BY cnt DESC
    """)
    for r in cur.fetchall():
        print(f"    keibajo={r[0]!r}: {r[1]:,}件未マッチ")

    # B3. 具体的未マッチレースをcyb側で検索
    if unmatched:
        sample = unmatched[0]
        s_keibajo = sample[0].strip()
        s_kai = sample[3].strip()
        s_nichime = sample[4].strip()
        s_race = sample[5].strip()
        s_uma = sample[6].strip()
        print(f"\n[B3] 未マッチサンプル: keibajo={s_keibajo} kai={s_kai} nichime={s_nichime} race={s_race} uma={s_uma}")
        print("     cyb側でkeibajoとrace_shikonenの'24'一致で検索:")
        cur.execute("""
            SELECT race_shikonen, keibajo_code, kaisai_kai, kaisai_nichime, 
                   race_bango, umaban
            FROM jrd_cyb
            WHERE TRIM(keibajo_code) = %s
                AND SUBSTRING(race_shikonen, 1, 2) = '24'
                AND TRIM(race_bango) = %s
                AND TRIM(umaban) = %s
            ORDER BY race_shikonen
            LIMIT 20
        """, (s_keibajo, s_race, s_uma))
        cyb_candidates = cur.fetchall()
        print(f"     候補数: {len(cyb_candidates)}")
        for r in cyb_candidates:
            print(f"     shikonen={r[0]!r} keibajo={r[1]!r} kai={r[2]!r} "
                  f"nichime={r[3]!r} race={r[4]!r} uma={r[5]!r}"
                  f"  ← se kai={s_kai}, nichime={s_nichime} と比較")

    # =================================================================
    # C. cyb側keibajo_code分布（YY='24'）
    # =================================================================
    print("\n[C] === cyb keibajo_code分布 (YY=24) ===")
    cur.execute("""
        SELECT TRIM(keibajo_code) AS kb, COUNT(*) AS cnt
        FROM jrd_cyb
        WHERE SUBSTRING(race_shikonen, 1, 2) = '24'
        GROUP BY TRIM(keibajo_code)
        ORDER BY kb
    """)
    for r in cur.fetchall():
        print(f"    keibajo={r[0]!r}: {r[1]:,}件")

    # se側の分布
    print("\n    se keibajo_code分布 (2024年1月):")
    cur.execute("""
        SELECT TRIM(keibajo_code) AS kb, COUNT(*) AS cnt
        FROM jvd_se
        WHERE (kaisai_nen || kaisai_tsukihi) >= '20240101'
            AND (kaisai_nen || kaisai_tsukihi) <= '20240131'
        GROUP BY TRIM(keibajo_code)
        ORDER BY kb
    """)
    for r in cur.fetchall():
        print(f"    keibajo={r[0]!r}: {r[1]:,}件")

    # =================================================================
    # D. 特殊JOIN: race_shikonenの3-4桁をkaiとして使う
    # =================================================================
    print("\n[D] === 代替JOIN戦略テスト ===")

    # D1. race_shikonenの3-4桁=kai, 5-6桁=nichimeとして直接JOINする
    print("\n[D1] cyb JOIN: YY + shikonen(3-4)=se.kai + shikonen(5-6)=se.nichime + race + uma")
    cur.execute("""
        SELECT COUNT(*) FROM jvd_se AS se
        INNER JOIN jrd_cyb AS cyb
            ON TRIM(se.keibajo_code) = TRIM(cyb.keibajo_code)
            AND SUBSTRING(se.kaisai_nen, 3, 2) = SUBSTRING(cyb.race_shikonen, 1, 2)
            AND CAST(NULLIF(TRIM(se.kaisai_kai), '') AS INTEGER)
                = CAST(SUBSTRING(cyb.race_shikonen, 3, 2) AS INTEGER)
            AND CAST(NULLIF(TRIM(se.kaisai_nichime), '') AS INTEGER)
                = CAST(SUBSTRING(cyb.race_shikonen, 5, 2) AS INTEGER)
            AND CAST(NULLIF(TRIM(se.race_bango), '') AS INTEGER)
                = CAST(NULLIF(TRIM(cyb.race_bango), '') AS INTEGER)
            AND CAST(NULLIF(TRIM(se.umaban), '') AS INTEGER)
                = CAST(NULLIF(TRIM(cyb.umaban), '') AS INTEGER)
        WHERE (se.kaisai_nen || se.kaisai_tsukihi) >= '20240101'
            AND (se.kaisai_nen || se.kaisai_tsukihi) <= '20240131'
    """)
    print(f"    結果: {cur.fetchone()[0]:,} マッチ")

    # D2. 現行JOIN（比較用）
    print("\n[D2] 現行JOIN: YY + CAST(kai) + CAST(nichime) + race + uma")
    cur.execute("""
        SELECT COUNT(*) FROM jvd_se AS se
        INNER JOIN jrd_cyb AS cyb
            ON TRIM(se.keibajo_code) = TRIM(cyb.keibajo_code)
            AND SUBSTRING(se.kaisai_nen, 3, 2) = SUBSTRING(cyb.race_shikonen, 1, 2)
            AND CAST(NULLIF(TRIM(se.kaisai_kai), '') AS INTEGER)
                = CAST(NULLIF(TRIM(cyb.kaisai_kai), '') AS INTEGER)
            AND CAST(NULLIF(TRIM(se.kaisai_nichime), '') AS INTEGER)
                = CAST(NULLIF(TRIM(cyb.kaisai_nichime), '') AS INTEGER)
            AND CAST(NULLIF(TRIM(se.race_bango), '') AS INTEGER)
                = CAST(NULLIF(TRIM(cyb.race_bango), '') AS INTEGER)
            AND CAST(NULLIF(TRIM(se.umaban), '') AS INTEGER)
                = CAST(NULLIF(TRIM(cyb.umaban), '') AS INTEGER)
        WHERE (se.kaisai_nen || se.kaisai_tsukihi) >= '20240101'
            AND (se.kaisai_nen || se.kaisai_tsukihi) <= '20240131'
    """)
    print(f"    結果: {cur.fetchone()[0]:,} マッチ")

    # D3. ハイブリッド: race_shikonen(3-4) OR cyb.kaisai_kai
    print("\n[D3] ハイブリッドJOIN: YY + (shikonen(3-4)=kai OR CAST(cyb.kai)=kai) + nichime同様 + race + uma")
    cur.execute("""
        SELECT COUNT(*) FROM jvd_se AS se
        INNER JOIN jrd_cyb AS cyb
            ON TRIM(se.keibajo_code) = TRIM(cyb.keibajo_code)
            AND SUBSTRING(se.kaisai_nen, 3, 2) = SUBSTRING(cyb.race_shikonen, 1, 2)
            AND (
                CAST(NULLIF(TRIM(se.kaisai_kai), '') AS INTEGER)
                    = CAST(NULLIF(TRIM(cyb.kaisai_kai), '') AS INTEGER)
                OR
                CAST(NULLIF(TRIM(se.kaisai_kai), '') AS INTEGER)
                    = CAST(SUBSTRING(cyb.race_shikonen, 3, 2) AS INTEGER)
            )
            AND (
                CAST(NULLIF(TRIM(se.kaisai_nichime), '') AS INTEGER)
                    = CAST(NULLIF(TRIM(cyb.kaisai_nichime), '') AS INTEGER)
                OR
                CAST(NULLIF(TRIM(se.kaisai_nichime), '') AS INTEGER)
                    = CAST(SUBSTRING(cyb.race_shikonen, 5, 2) AS INTEGER)
            )
            AND CAST(NULLIF(TRIM(se.race_bango), '') AS INTEGER)
                = CAST(NULLIF(TRIM(cyb.race_bango), '') AS INTEGER)
            AND CAST(NULLIF(TRIM(se.umaban), '') AS INTEGER)
                = CAST(NULLIF(TRIM(cyb.umaban), '') AS INTEGER)
        WHERE (se.kaisai_nen || se.kaisai_tsukihi) >= '20240101'
            AND (se.kaisai_nen || se.kaisai_tsukihi) <= '20240131'
    """)
    print(f"    結果: {cur.fetchone()[0]:,} マッチ")

    # =================================================================
    # E. kyi/joa も同様に検証
    # =================================================================
    print("\n[E] === kyi/joa/bac 各テーブルの代替JOIN ===")

    for tbl, has_uma in [("jrd_kyi", True), ("jrd_joa", True), ("jrd_bac", False)]:
        uma_cond_cur = """
            AND CAST(NULLIF(TRIM(se.umaban), '') AS INTEGER)
                = CAST(NULLIF(TRIM(t.umaban), '') AS INTEGER)
        """ if has_uma else ""

        # 現行JOIN
        cur.execute(f"""
            SELECT COUNT(*) FROM jvd_se AS se
            INNER JOIN {tbl} AS t
                ON TRIM(se.keibajo_code) = TRIM(t.keibajo_code)
                AND SUBSTRING(se.kaisai_nen, 3, 2) = SUBSTRING(t.race_shikonen, 1, 2)
                AND CAST(NULLIF(TRIM(se.kaisai_kai), '') AS INTEGER)
                    = CAST(NULLIF(TRIM(t.kaisai_kai), '') AS INTEGER)
                AND CAST(NULLIF(TRIM(se.kaisai_nichime), '') AS INTEGER)
                    = CAST(NULLIF(TRIM(t.kaisai_nichime), '') AS INTEGER)
                AND CAST(NULLIF(TRIM(se.race_bango), '') AS INTEGER)
                    = CAST(NULLIF(TRIM(t.race_bango), '') AS INTEGER)
                {uma_cond_cur}
            WHERE (se.kaisai_nen || se.kaisai_tsukihi) >= '20240101'
                AND (se.kaisai_nen || se.kaisai_tsukihi) <= '20240131'
        """)
        cur_count = cur.fetchone()[0]

        # shikonen 3-4/5-6 JOIN
        uma_cond_alt = """
            AND CAST(NULLIF(TRIM(se.umaban), '') AS INTEGER)
                = CAST(NULLIF(TRIM(t.umaban), '') AS INTEGER)
        """ if has_uma else ""

        cur.execute(f"""
            SELECT COUNT(*) FROM jvd_se AS se
            INNER JOIN {tbl} AS t
                ON TRIM(se.keibajo_code) = TRIM(t.keibajo_code)
                AND SUBSTRING(se.kaisai_nen, 3, 2) = SUBSTRING(t.race_shikonen, 1, 2)
                AND CAST(NULLIF(TRIM(se.kaisai_kai), '') AS INTEGER)
                    = CAST(SUBSTRING(t.race_shikonen, 3, 2) AS INTEGER)
                AND CAST(NULLIF(TRIM(se.kaisai_nichime), '') AS INTEGER)
                    = CAST(SUBSTRING(t.race_shikonen, 5, 2) AS INTEGER)
                AND CAST(NULLIF(TRIM(se.race_bango), '') AS INTEGER)
                    = CAST(NULLIF(TRIM(t.race_bango), '') AS INTEGER)
                {uma_cond_alt}
            WHERE (se.kaisai_nen || se.kaisai_tsukihi) >= '20240101'
                AND (se.kaisai_nen || se.kaisai_tsukihi) <= '20240131'
        """)
        alt_count = cur.fetchone()[0]

        # bac はkaisai_nenも試す
        bac_extra = ""
        if tbl == "jrd_bac":
            cur.execute(f"""
                SELECT COUNT(*) FROM jvd_se AS se
                INNER JOIN jrd_bac AS t
                    ON TRIM(se.keibajo_code) = TRIM(t.keibajo_code)
                    AND se.kaisai_nen = t.kaisai_nen
                    AND CAST(NULLIF(TRIM(se.kaisai_kai), '') AS INTEGER)
                        = CAST(NULLIF(TRIM(t.kaisai_kai), '') AS INTEGER)
                    AND CAST(NULLIF(TRIM(se.kaisai_nichime), '') AS INTEGER)
                        = CAST(NULLIF(TRIM(t.kaisai_nichime), '') AS INTEGER)
                    AND CAST(NULLIF(TRIM(se.race_bango), '') AS INTEGER)
                        = CAST(NULLIF(TRIM(t.race_bango), '') AS INTEGER)
                WHERE (se.kaisai_nen || se.kaisai_tsukihi) >= '20240101'
                    AND (se.kaisai_nen || se.kaisai_tsukihi) <= '20240131'
            """)
            bac_nen = cur.fetchone()[0]
            bac_extra = f", kaisai_nen JOIN={bac_nen:,}"

        print(f"    {tbl}: 現行={cur_count:,}, shikonen(3-4/5-6)={alt_count:,}{bac_extra}")

    # =================================================================
    # F. bac特殊診断
    # =================================================================
    print("\n[F] === bac テーブル特殊診断 ===")
    
    print("\n[F1] bac: kaisai_nen の値サンプル")
    cur.execute("""
        SELECT DISTINCT TRIM(kaisai_nen) AS v, COUNT(*) AS cnt
        FROM jrd_bac
        GROUP BY TRIM(kaisai_nen)
        ORDER BY v
        LIMIT 20
    """)
    for r in cur.fetchall():
        print(f"    kaisai_nen={r[0]!r}: {r[1]:,}件")
    
    print("\n[F2] bac: 先頭20行の全キー")
    cur.execute("""
        SELECT race_shikonen, keibajo_code, kaisai_nen, kaisai_kai, 
               kaisai_nichime, race_bango
        FROM jrd_bac
        ORDER BY race_shikonen
        LIMIT 20
    """)
    for r in cur.fetchall():
        print(f"    shikonen={r[0]!r} keibajo={r[1]!r} nen={r[2]!r} "
              f"kai={r[3]!r} nichime={r[4]!r} race={r[5]!r}")

    print("\n" + "=" * 70)
    print("診断v3完了")
    print("=" * 70)

    conn.close()


if __name__ == "__main__":
    main()
