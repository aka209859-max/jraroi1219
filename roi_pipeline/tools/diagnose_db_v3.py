"""
DB診断スクリプト v3 — PC-KEIBAパースずれ確定 & 新JOIN戦略テスト

確定事実:
    PC-KEIBAがJRDBファイルをパースする際、kaisai_kai/kaisai_nichimeに
    umaban（馬番）の上位桁/下位桁が誤って格納されている。
    
    例: shikonen='241101' kai='0' nichime='7' → 実際はuma=07
        shikonen='241101' kai='1' nichime='4' → 実際はuma=14
    
    したがって jrd_kyi/cyb/joa の kaisai_kai/kaisai_nichime は使用禁止。

新JOIN戦略:
    race_shikonen = YY(2) + 回(2) + 日目(2) を信頼できる唯一の開催識別子として使い、
    keibajo_code + race_shikonen + race_bango + umaban で結合する。
    
    se側は kaisai_nen(3,2) + kaisai_kai(zero-padded to 2) + kaisai_nichime(zero-padded to 2)
    でrace_shikonenに相当する値を合成する。

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
    print("DB診断 v3 — PC-KEIBAパースずれ確定 & 新JOIN戦略テスト")
    print("=" * 70)

    # =================================================================
    # A. パースずれの証拠
    # =================================================================
    print("\n[A] === kaisai_kai/nichime = umaban の証拠 ===")

    print("\n[A1] cyb: shikonen='241101' のkai/nichime vs umaban")
    cur.execute("""
        SELECT race_shikonen, keibajo_code, kaisai_kai, kaisai_nichime,
               race_bango, umaban,
               kaisai_kai || kaisai_nichime AS kai_nich_concat
        FROM jrd_cyb
        WHERE race_shikonen = '241101'
        ORDER BY keibajo_code, race_bango, umaban
        LIMIT 30
    """)
    print("    shikonen | keibajo | kai | nichime | race | uma | kai||nichime → uma?")
    print("    " + "-" * 70)
    match_count = 0
    total_count = 0
    for r in cur.fetchall():
        total_count += 1
        # kai と nichime を連結して umaban と比較
        try:
            concat_val = int(str(r[2]).strip() + str(r[3]).strip())
            uma_val = int(str(r[5]).strip())
            is_match = concat_val == uma_val
            if is_match:
                match_count += 1
        except (ValueError, TypeError):
            is_match = False
        flag = "MATCH" if is_match else "NO"
        print(f"    {r[0]} | {r[1]:>7} | {r[2]:>3} | {r[3]:>7} | {r[4]:>4} | {r[5]:>3} | "
              f"{r[6]:>5} → uma={r[5]:>3} {flag}")
    if total_count > 0:
        print(f"\n    kai||nichime == umaban 一致率: {match_count}/{total_count} ({match_count/total_count*100:.0f}%)")

    # A2. 全データでの一致率（安全な文字列比較）
    print("\n[A2] cyb全体: kai||nichime == umaban 一致率")
    cur.execute("""
        SELECT 
            COUNT(*) AS total,
            SUM(CASE WHEN TRIM(kaisai_kai) || TRIM(kaisai_nichime) = TRIM(umaban) 
                     THEN 1 ELSE 0 END) AS exact_match,
            SUM(CASE WHEN LPAD(TRIM(kaisai_kai) || TRIM(kaisai_nichime), 2, '0') 
                        = LPAD(TRIM(umaban), 2, '0')
                     THEN 1 ELSE 0 END) AS padded_match
        FROM jrd_cyb
    """)
    r = cur.fetchone()
    print(f"    total: {r[0]:,}")
    print(f"    完全一致 (kai||nichime == umaban): {r[1]:,} ({r[1]/r[0]*100:.1f}%)")
    print(f"    パディング一致 (padded): {r[2]:,} ({r[2]/r[0]*100:.1f}%)")

    # A3. kyi/joaでも同様か
    for tbl in ["jrd_kyi", "jrd_joa"]:
        print(f"\n[A3] {tbl}: kai||nichime == umaban 一致率")
        cur.execute(f"""
            SELECT 
                COUNT(*) AS total,
                SUM(CASE WHEN TRIM(kaisai_kai) || TRIM(kaisai_nichime) = TRIM(umaban)
                         THEN 1 ELSE 0 END) AS exact_match
            FROM {tbl}
        """)
        r = cur.fetchone()
        print(f"    exact_match: {r[1]:,} / {r[0]:,} ({r[1]/r[0]*100:.1f}%)")

    # =================================================================
    # B. 新JOIN戦略: race_shikonen + keibajo + race_bango + umaban
    # =================================================================
    print("\n[B] === 新JOIN戦略テスト ===")
    print("    se側: SUBSTRING(kaisai_nen,3,2) || LPAD(TRIM(kaisai_kai),2,'0') || LPAD(TRIM(kaisai_nichime),2,'0')")
    print("    jrdb側: race_shikonen")
    print("    + keibajo_code + race_bango + umaban")

    # B1. se側のcomputed_shikonen確認
    print("\n[B1] se: computed_shikonen サンプル (2024年1月)")
    cur.execute("""
        SELECT 
            SUBSTRING(kaisai_nen, 3, 2) 
                || LPAD(CAST(CAST(NULLIF(TRIM(kaisai_kai), '') AS INTEGER) AS TEXT), 2, '0')
                || LPAD(CAST(CAST(NULLIF(TRIM(kaisai_nichime), '') AS INTEGER) AS TEXT), 2, '0')
            AS computed_shikonen,
            keibajo_code, kaisai_nen, kaisai_tsukihi, kaisai_kai, kaisai_nichime,
            race_bango, umaban
        FROM jvd_se
        WHERE (kaisai_nen || kaisai_tsukihi) >= '20240101'
            AND (kaisai_nen || kaisai_tsukihi) <= '20240115'
        ORDER BY kaisai_tsukihi, keibajo_code, race_bango, umaban
        LIMIT 20
    """)
    for r in cur.fetchall():
        print(f"    computed={r[0]} keibajo={r[1]!r} nen={r[2]} tsukihi={r[3]} "
              f"kai={r[4]!r} nichime={r[5]!r} race={r[6]!r} uma={r[7]!r}")

    # B2. 新JOIN: cyb (2024年1月)
    print("\n[B2] cyb 新JOIN 2024年1月:")
    cur.execute("""
        SELECT COUNT(*) FROM jvd_se AS se
        INNER JOIN jrd_cyb AS cyb
            ON TRIM(se.keibajo_code) = TRIM(cyb.keibajo_code)
            AND (
                SUBSTRING(se.kaisai_nen, 3, 2)
                || LPAD(CAST(CAST(NULLIF(TRIM(se.kaisai_kai), '') AS INTEGER) AS TEXT), 2, '0')
                || LPAD(CAST(CAST(NULLIF(TRIM(se.kaisai_nichime), '') AS INTEGER) AS TEXT), 2, '0')
            ) = cyb.race_shikonen
            AND CAST(NULLIF(TRIM(se.race_bango), '') AS INTEGER)
                = CAST(NULLIF(TRIM(cyb.race_bango), '') AS INTEGER)
            AND CAST(NULLIF(TRIM(se.umaban), '') AS INTEGER)
                = CAST(NULLIF(TRIM(cyb.umaban), '') AS INTEGER)
        WHERE (se.kaisai_nen || se.kaisai_tsukihi) >= '20240101'
            AND (se.kaisai_nen || se.kaisai_tsukihi) <= '20240131'
    """)
    print(f"    結果: {cur.fetchone()[0]:,} マッチ (期待値: ~5,919)")

    # B3. 新JOIN: kyi (2024年1月)
    print("\n[B3] kyi 新JOIN 2024年1月:")
    cur.execute("""
        SELECT COUNT(*) FROM jvd_se AS se
        INNER JOIN jrd_kyi AS kyi
            ON TRIM(se.keibajo_code) = TRIM(kyi.keibajo_code)
            AND (
                SUBSTRING(se.kaisai_nen, 3, 2)
                || LPAD(CAST(CAST(NULLIF(TRIM(se.kaisai_kai), '') AS INTEGER) AS TEXT), 2, '0')
                || LPAD(CAST(CAST(NULLIF(TRIM(se.kaisai_nichime), '') AS INTEGER) AS TEXT), 2, '0')
            ) = kyi.race_shikonen
            AND CAST(NULLIF(TRIM(se.race_bango), '') AS INTEGER)
                = CAST(NULLIF(TRIM(kyi.race_bango), '') AS INTEGER)
            AND CAST(NULLIF(TRIM(se.umaban), '') AS INTEGER)
                = CAST(NULLIF(TRIM(kyi.umaban), '') AS INTEGER)
        WHERE (se.kaisai_nen || se.kaisai_tsukihi) >= '20240101'
            AND (se.kaisai_nen || se.kaisai_tsukihi) <= '20240131'
    """)
    print(f"    結果: {cur.fetchone()[0]:,} マッチ (期待値: ~5,919)")

    # B4. 新JOIN: joa (2024年1月)
    print("\n[B4] joa 新JOIN 2024年1月:")
    cur.execute("""
        SELECT COUNT(*) FROM jvd_se AS se
        INNER JOIN jrd_joa AS joa
            ON TRIM(se.keibajo_code) = TRIM(joa.keibajo_code)
            AND (
                SUBSTRING(se.kaisai_nen, 3, 2)
                || LPAD(CAST(CAST(NULLIF(TRIM(se.kaisai_kai), '') AS INTEGER) AS TEXT), 2, '0')
                || LPAD(CAST(CAST(NULLIF(TRIM(se.kaisai_nichime), '') AS INTEGER) AS TEXT), 2, '0')
            ) = joa.race_shikonen
            AND CAST(NULLIF(TRIM(se.race_bango), '') AS INTEGER)
                = CAST(NULLIF(TRIM(joa.race_bango), '') AS INTEGER)
            AND CAST(NULLIF(TRIM(se.umaban), '') AS INTEGER)
                = CAST(NULLIF(TRIM(joa.umaban), '') AS INTEGER)
        WHERE (se.kaisai_nen || se.kaisai_tsukihi) >= '20240101'
            AND (se.kaisai_nen || se.kaisai_tsukihi) <= '20240131'
    """)
    print(f"    結果: {cur.fetchone()[0]:,} マッチ (期待値: ~5,919)")

    # B5. 新JOIN: bac (2024年1月) — umabanなし + kaisai_nen使用
    print("\n[B5] bac JOIN戦略テスト 2024年1月:")
    
    # 戦略1: race_shikonen + keibajo + race_bango (uma なし)
    cur.execute("""
        SELECT COUNT(*) FROM jvd_se AS se
        INNER JOIN jrd_bac AS bac
            ON TRIM(se.keibajo_code) = TRIM(bac.keibajo_code)
            AND (
                SUBSTRING(se.kaisai_nen, 3, 2)
                || LPAD(CAST(CAST(NULLIF(TRIM(se.kaisai_kai), '') AS INTEGER) AS TEXT), 2, '0')
                || LPAD(CAST(CAST(NULLIF(TRIM(se.kaisai_nichime), '') AS INTEGER) AS TEXT), 2, '0')
            ) = bac.race_shikonen
            AND CAST(NULLIF(TRIM(se.race_bango), '') AS INTEGER)
                = CAST(NULLIF(TRIM(bac.race_bango), '') AS INTEGER)
        WHERE (se.kaisai_nen || se.kaisai_tsukihi) >= '20240101'
            AND (se.kaisai_nen || se.kaisai_tsukihi) <= '20240131'
    """)
    print(f"    race_shikonen方式: {cur.fetchone()[0]:,} マッチ")

    # 戦略2: kaisai_nen + keibajo + kai + nichime + race_bango
    cur.execute("""
        SELECT COUNT(*) FROM jvd_se AS se
        INNER JOIN jrd_bac AS bac
            ON TRIM(se.keibajo_code) = TRIM(bac.keibajo_code)
            AND TRIM(se.kaisai_nen) = TRIM(bac.kaisai_nen)
            AND TRIM(se.kaisai_kai) = TRIM(bac.kaisai_kai)
            AND TRIM(se.kaisai_nichime) = TRIM(bac.kaisai_nichime)
            AND CAST(NULLIF(TRIM(se.race_bango), '') AS INTEGER)
                = CAST(NULLIF(TRIM(bac.race_bango), '') AS INTEGER)
        WHERE (se.kaisai_nen || se.kaisai_tsukihi) >= '20240101'
            AND (se.kaisai_nen || se.kaisai_tsukihi) <= '20240131'
    """)
    print(f"    kaisai_nen直接方式: {cur.fetchone()[0]:,} マッチ")

    # bac内容確認
    print("\n[B6] bac: 先頭20行の全キー")
    cur.execute("""
        SELECT race_shikonen, keibajo_code, kaisai_nen, kaisai_kai, 
               kaisai_nichime, race_bango
        FROM jrd_bac
        WHERE SUBSTRING(race_shikonen, 1, 2) = '24'
        ORDER BY race_shikonen
        LIMIT 20
    """)
    for r in cur.fetchall():
        print(f"    shikonen={r[0]!r} keibajo={r[1]!r} nen={r[2]!r} "
              f"kai={r[3]!r} nichime={r[4]!r} race={r[5]!r}")

    # =================================================================
    # C. 全期間マッチテスト
    # =================================================================
    print("\n[C] === 全期間マッチテスト (2016-2025) ===")

    # se全体行数
    cur.execute("SELECT COUNT(*) FROM jvd_se")
    se_total = cur.fetchone()[0]
    print(f"    se全体: {se_total:,} 行")

    # cyb全期間
    cur.execute("""
        SELECT COUNT(*) FROM jvd_se AS se
        INNER JOIN jrd_cyb AS cyb
            ON TRIM(se.keibajo_code) = TRIM(cyb.keibajo_code)
            AND (
                SUBSTRING(se.kaisai_nen, 3, 2)
                || LPAD(CAST(CAST(NULLIF(TRIM(se.kaisai_kai), '') AS INTEGER) AS TEXT), 2, '0')
                || LPAD(CAST(CAST(NULLIF(TRIM(se.kaisai_nichime), '') AS INTEGER) AS TEXT), 2, '0')
            ) = cyb.race_shikonen
            AND CAST(NULLIF(TRIM(se.race_bango), '') AS INTEGER)
                = CAST(NULLIF(TRIM(cyb.race_bango), '') AS INTEGER)
            AND CAST(NULLIF(TRIM(se.umaban), '') AS INTEGER)
                = CAST(NULLIF(TRIM(cyb.umaban), '') AS INTEGER)
    """)
    cyb_count = cur.fetchone()[0]
    print(f"    cyb新JOIN全期間: {cyb_count:,} ({cyb_count/se_total*100:.1f}%)")

    # kyi全期間
    cur.execute("""
        SELECT COUNT(*) FROM jvd_se AS se
        INNER JOIN jrd_kyi AS kyi
            ON TRIM(se.keibajo_code) = TRIM(kyi.keibajo_code)
            AND (
                SUBSTRING(se.kaisai_nen, 3, 2)
                || LPAD(CAST(CAST(NULLIF(TRIM(se.kaisai_kai), '') AS INTEGER) AS TEXT), 2, '0')
                || LPAD(CAST(CAST(NULLIF(TRIM(se.kaisai_nichime), '') AS INTEGER) AS TEXT), 2, '0')
            ) = kyi.race_shikonen
            AND CAST(NULLIF(TRIM(se.race_bango), '') AS INTEGER)
                = CAST(NULLIF(TRIM(kyi.race_bango), '') AS INTEGER)
            AND CAST(NULLIF(TRIM(se.umaban), '') AS INTEGER)
                = CAST(NULLIF(TRIM(kyi.umaban), '') AS INTEGER)
    """)
    kyi_count = cur.fetchone()[0]
    print(f"    kyi新JOIN全期間: {kyi_count:,} ({kyi_count/se_total*100:.1f}%)")

    # joa全期間
    cur.execute("""
        SELECT COUNT(*) FROM jvd_se AS se
        INNER JOIN jrd_joa AS joa
            ON TRIM(se.keibajo_code) = TRIM(joa.keibajo_code)
            AND (
                SUBSTRING(se.kaisai_nen, 3, 2)
                || LPAD(CAST(CAST(NULLIF(TRIM(se.kaisai_kai), '') AS INTEGER) AS TEXT), 2, '0')
                || LPAD(CAST(CAST(NULLIF(TRIM(se.kaisai_nichime), '') AS INTEGER) AS TEXT), 2, '0')
            ) = joa.race_shikonen
            AND CAST(NULLIF(TRIM(se.race_bango), '') AS INTEGER)
                = CAST(NULLIF(TRIM(joa.race_bango), '') AS INTEGER)
            AND CAST(NULLIF(TRIM(se.umaban), '') AS INTEGER)
                = CAST(NULLIF(TRIM(joa.umaban), '') AS INTEGER)
    """)
    joa_count = cur.fetchone()[0]
    print(f"    joa新JOIN全期間: {joa_count:,} ({joa_count/se_total*100:.1f}%)")

    # bac全期間
    cur.execute("""
        SELECT COUNT(*) FROM jvd_se AS se
        INNER JOIN jrd_bac AS bac
            ON TRIM(se.keibajo_code) = TRIM(bac.keibajo_code)
            AND (
                SUBSTRING(se.kaisai_nen, 3, 2)
                || LPAD(CAST(CAST(NULLIF(TRIM(se.kaisai_kai), '') AS INTEGER) AS TEXT), 2, '0')
                || LPAD(CAST(CAST(NULLIF(TRIM(se.kaisai_nichime), '') AS INTEGER) AS TEXT), 2, '0')
            ) = bac.race_shikonen
            AND CAST(NULLIF(TRIM(se.race_bango), '') AS INTEGER)
                = CAST(NULLIF(TRIM(bac.race_bango), '') AS INTEGER)
    """)
    bac_count = cur.fetchone()[0]
    print(f"    bac新JOIN全期間: {bac_count:,} ({bac_count/se_total*100:.1f}%)")

    # =================================================================
    # D. 重複チェック（新JOINで1:1か確認）
    # =================================================================
    print("\n[D] === 新JOIN 1:1重複チェック ===")
    cur.execute("""
        SELECT se.keibajo_code, se.kaisai_nen, se.kaisai_tsukihi,
               se.race_bango, se.umaban, COUNT(*) AS cyb_matches
        FROM jvd_se AS se
        INNER JOIN jrd_cyb AS cyb
            ON TRIM(se.keibajo_code) = TRIM(cyb.keibajo_code)
            AND (
                SUBSTRING(se.kaisai_nen, 3, 2)
                || LPAD(CAST(CAST(NULLIF(TRIM(se.kaisai_kai), '') AS INTEGER) AS TEXT), 2, '0')
                || LPAD(CAST(CAST(NULLIF(TRIM(se.kaisai_nichime), '') AS INTEGER) AS TEXT), 2, '0')
            ) = cyb.race_shikonen
            AND CAST(NULLIF(TRIM(se.race_bango), '') AS INTEGER)
                = CAST(NULLIF(TRIM(cyb.race_bango), '') AS INTEGER)
            AND CAST(NULLIF(TRIM(se.umaban), '') AS INTEGER)
                = CAST(NULLIF(TRIM(cyb.umaban), '') AS INTEGER)
        WHERE (se.kaisai_nen || se.kaisai_tsukihi) >= '20240101'
            AND (se.kaisai_nen || se.kaisai_tsukihi) <= '20240131'
        GROUP BY se.keibajo_code, se.kaisai_nen, se.kaisai_tsukihi,
                 se.race_bango, se.umaban
        HAVING COUNT(*) > 1
        LIMIT 10
    """)
    dups = cur.fetchall()
    if dups:
        print(f"    1:多の重複: {len(dups)} 件あり!")
        for d in dups[:5]:
            print(f"    keibajo={d[0]} nen={d[1]} tsukihi={d[2]} "
                  f"race={d[3]} uma={d[4]} matches={d[5]}")
    else:
        print("    重複なし! 新JOINは完全1:1結合。")

    print("\n" + "=" * 70)
    print("診断v3完了")
    print("=" * 70)

    conn.close()


if __name__ == "__main__":
    main()
