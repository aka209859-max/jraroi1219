"""
DB診断スクリプト v2 — race_shikonenの正体を最終確定する

使用方法:
    py -3.12 roi_pipeline/tools/diagnose_db_v2.py
"""
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from roi_pipeline.config.db import get_connection


def main():
    conn = get_connection()
    cur = conn.cursor()

    print("=" * 70)
    print("DB診断 v2 — race_shikonen正体確定 & JOINキー最終テスト")
    print("=" * 70)

    # =================================================================
    # A. race_shikonen の正体を確定する
    # =================================================================
    print("\n[A] === race_shikonen の正体を確定する ===")

    # A1. CYBのrace_shikonenとkaisai_kai/nichimeの関係
    print("\n[A1] cyb: race_shikonen vs kaisai_kai/kaisai_nichime (先頭20件)")
    cur.execute(
        "SELECT race_shikonen, keibajo_code, kaisai_kai, kaisai_nichime, "
        "race_bango, umaban "
        "FROM jrd_cyb ORDER BY race_shikonen LIMIT 20"
    )
    for r in cur.fetchall():
        print(f"    shikonen={r[0]!r} keibajo={r[1]!r} kai={r[2]!r} "
              f"nichime={r[3]!r} race={r[4]!r} uma={r[5]!r}")

    # A2. 同じshikonenで何種類のkai/nichimeがあるか
    print("\n[A2] cyb: 同じrace_shikonenに紐づくkaisai_kai/nichimeのバリエーション")
    cur.execute(
        "SELECT race_shikonen, "
        "COUNT(DISTINCT kaisai_kai) AS kai_variants, "
        "COUNT(DISTINCT kaisai_nichime) AS nichime_variants, "
        "COUNT(*) AS row_count "
        "FROM jrd_cyb "
        "GROUP BY race_shikonen "
        "ORDER BY race_shikonen LIMIT 15"
    )
    for r in cur.fetchall():
        print(f"    shikonen={r[0]!r}: kai種類={r[1]}, nichime種類={r[2]}, 行数={r[3]}")

    # A3. JRA-VAN側のse: 特定日のkaisai_kai/nichime
    print("\n[A3] se: 2024年の先頭データ (keibajo, nen, tsukihi, kai, nichime)")
    cur.execute(
        "SELECT keibajo_code, kaisai_nen, kaisai_tsukihi, kaisai_kai, "
        "kaisai_nichime, race_bango, umaban "
        "FROM jvd_se "
        "WHERE (kaisai_nen || kaisai_tsukihi) >= '20240101' "
        "AND (kaisai_nen || kaisai_tsukihi) <= '20240115' "
        "LIMIT 15"
    )
    for r in cur.fetchall():
        print(f"    keibajo={r[0]!r} nen={r[1]!r} tsukihi={r[2]!r} "
              f"kai={r[3]!r} nichime={r[4]!r} race={r[5]!r} uma={r[6]!r}")

    # A4. CYB側で同じkeibajo='06' kai='2' nichime='5' を持つレコード
    print("\n[A4] cyb: keibajo='06', kaisai_kai='2' のサンプル")
    cur.execute(
        "SELECT race_shikonen, keibajo_code, kaisai_kai, kaisai_nichime, "
        "race_bango, umaban "
        "FROM jrd_cyb "
        "WHERE keibajo_code = '06' AND TRIM(kaisai_kai) = '2' "
        "ORDER BY race_shikonen LIMIT 15"
    )
    for r in cur.fetchall():
        print(f"    shikonen={r[0]!r} keibajo={r[1]!r} kai={r[2]!r} "
              f"nichime={r[3]!r} race={r[4]!r} uma={r[5]!r}")

    # =================================================================
    # B. race_shikonen を完全にバイパスしたJOINテスト
    # =================================================================
    print("\n[B] === race_shikonenバイパス JOIN テスト ===")
    print("    条件: keibajo + CAST(kai) + CAST(nichime) + CAST(race) + CAST(uma)")
    print("    ※ race_shikonenも年も一切使わない")

    # B1. cyb (年なし、全条件)
    cur.execute(
        "SELECT COUNT(*) FROM jvd_se AS se "
        "INNER JOIN jrd_cyb AS cyb "
        "ON TRIM(se.keibajo_code) = TRIM(cyb.keibajo_code) "
        "AND CAST(NULLIF(TRIM(se.kaisai_kai), '') AS INTEGER) "
        "  = CAST(NULLIF(TRIM(cyb.kaisai_kai), '') AS INTEGER) "
        "AND CAST(NULLIF(TRIM(se.kaisai_nichime), '') AS INTEGER) "
        "  = CAST(NULLIF(TRIM(cyb.kaisai_nichime), '') AS INTEGER) "
        "AND CAST(NULLIF(TRIM(se.race_bango), '') AS INTEGER) "
        "  = CAST(NULLIF(TRIM(cyb.race_bango), '') AS INTEGER) "
        "AND CAST(NULLIF(TRIM(se.umaban), '') AS INTEGER) "
        "  = CAST(NULLIF(TRIM(cyb.umaban), '') AS INTEGER) "
        "WHERE (se.kaisai_nen || se.kaisai_tsukihi) >= '20240101' "
        "AND (se.kaisai_nen || se.kaisai_tsukihi) <= '20240131'"
    )
    print(f"    cyb 年なしJOIN 2024年1月: {cur.fetchone()[0]:,} マッチ")

    # B2. cyb (YY年一致あり、全条件) — 比較用
    cur.execute(
        "SELECT COUNT(*) FROM jvd_se AS se "
        "INNER JOIN jrd_cyb AS cyb "
        "ON TRIM(se.keibajo_code) = TRIM(cyb.keibajo_code) "
        "AND SUBSTRING(se.kaisai_nen, 3, 2) = SUBSTRING(cyb.race_shikonen, 1, 2) "
        "AND CAST(NULLIF(TRIM(se.kaisai_kai), '') AS INTEGER) "
        "  = CAST(NULLIF(TRIM(cyb.kaisai_kai), '') AS INTEGER) "
        "AND CAST(NULLIF(TRIM(se.kaisai_nichime), '') AS INTEGER) "
        "  = CAST(NULLIF(TRIM(cyb.kaisai_nichime), '') AS INTEGER) "
        "AND CAST(NULLIF(TRIM(se.race_bango), '') AS INTEGER) "
        "  = CAST(NULLIF(TRIM(cyb.race_bango), '') AS INTEGER) "
        "AND CAST(NULLIF(TRIM(se.umaban), '') AS INTEGER) "
        "  = CAST(NULLIF(TRIM(cyb.umaban), '') AS INTEGER) "
        "WHERE (se.kaisai_nen || se.kaisai_tsukihi) >= '20240101' "
        "AND (se.kaisai_nen || se.kaisai_tsukihi) <= '20240131'"
    )
    print(f"    cyb YY年一致JOIN 2024年1月: {cur.fetchone()[0]:,} マッチ (比較)")

    # B3. bac (年なし、umabanなし)
    cur.execute(
        "SELECT COUNT(*) FROM jvd_se AS se "
        "INNER JOIN jrd_bac AS bac "
        "ON TRIM(se.keibajo_code) = TRIM(bac.keibajo_code) "
        "AND CAST(NULLIF(TRIM(se.kaisai_kai), '') AS INTEGER) "
        "  = CAST(NULLIF(TRIM(bac.kaisai_kai), '') AS INTEGER) "
        "AND CAST(NULLIF(TRIM(se.kaisai_nichime), '') AS INTEGER) "
        "  = CAST(NULLIF(TRIM(bac.kaisai_nichime), '') AS INTEGER) "
        "AND CAST(NULLIF(TRIM(se.race_bango), '') AS INTEGER) "
        "  = CAST(NULLIF(TRIM(bac.race_bango), '') AS INTEGER) "
        "WHERE (se.kaisai_nen || se.kaisai_tsukihi) >= '20240101' "
        "AND (se.kaisai_nen || se.kaisai_tsukihi) <= '20240131'"
    )
    print(f"    bac 年なしJOIN 2024年1月(uma除外): {cur.fetchone()[0]:,} マッチ")

    # =================================================================
    # C. kaisai_nichimeの値分布（ずれ確認）
    # =================================================================
    print("\n[C] === kaisai_nichime 値分布 ===")

    print("\n[C1] se側 kaisai_nichime ユニーク値:")
    cur.execute(
        "SELECT DISTINCT TRIM(kaisai_nichime) AS v, COUNT(*) AS cnt "
        "FROM jvd_se GROUP BY TRIM(kaisai_nichime) ORDER BY v LIMIT 20"
    )
    for r in cur.fetchall():
        print(f"    nichime={r[0]!r}: {r[1]:,}件")

    print("\n[C2] cyb側 kaisai_nichime ユニーク値:")
    cur.execute(
        "SELECT DISTINCT TRIM(kaisai_nichime) AS v, COUNT(*) AS cnt "
        "FROM jrd_cyb GROUP BY TRIM(kaisai_nichime) ORDER BY v LIMIT 20"
    )
    for r in cur.fetchall():
        print(f"    nichime={r[0]!r}: {r[1]:,}件")

    print("\n[C3] bac側 kaisai_nichime ユニーク値:")
    cur.execute(
        "SELECT DISTINCT TRIM(kaisai_nichime) AS v, COUNT(*) AS cnt "
        "FROM jrd_bac GROUP BY TRIM(kaisai_nichime) ORDER BY v LIMIT 20"
    )
    for r in cur.fetchall():
        print(f"    nichime={r[0]!r}: {r[1]:,}件")

    # =================================================================
    # D. kaisai_kaiの値分布
    # =================================================================
    print("\n[D] === kaisai_kai 値分布 ===")

    print("\n[D1] se側 kaisai_kai ユニーク値:")
    cur.execute(
        "SELECT DISTINCT TRIM(kaisai_kai) AS v, COUNT(*) AS cnt "
        "FROM jvd_se GROUP BY TRIM(kaisai_kai) ORDER BY v LIMIT 20"
    )
    for r in cur.fetchall():
        print(f"    kai={r[0]!r}: {r[1]:,}件")

    print("\n[D2] cyb側 kaisai_kai ユニーク値:")
    cur.execute(
        "SELECT DISTINCT TRIM(kaisai_kai) AS v, COUNT(*) AS cnt "
        "FROM jrd_cyb GROUP BY TRIM(kaisai_kai) ORDER BY v LIMIT 20"
    )
    for r in cur.fetchall():
        print(f"    kai={r[0]!r}: {r[1]:,}件")

    # =================================================================
    # E. 具体的な1レースで突き合わせ
    # =================================================================
    print("\n[E] === 具体的レース突き合わせ ===")

    # se側: 2024/01/06 中山1R
    print("\n[E1] se: 2024/01/06 (keibajo=06, kai=01, nichime=01, race=01)")
    cur.execute(
        "SELECT keibajo_code, kaisai_nen, kaisai_tsukihi, kaisai_kai, "
        "kaisai_nichime, race_bango, umaban "
        "FROM jvd_se "
        "WHERE kaisai_nen = '2024' AND kaisai_tsukihi = '0106' "
        "AND TRIM(keibajo_code) = '06' AND TRIM(race_bango) = '01' "
        "ORDER BY umaban LIMIT 10"
    )
    se_rows = cur.fetchall()
    for r in se_rows:
        print(f"    keibajo={r[0]!r} nen={r[1]!r} tsukihi={r[2]!r} "
              f"kai={r[3]!r} nichime={r[4]!r} race={r[5]!r} uma={r[6]!r}")

    if se_rows:
        se_kai = se_rows[0][3].strip()
        se_nichime = se_rows[0][4].strip()
        print(f"\n[E2] cyb: keibajo='06', kai一致(se: {se_kai!r}), nichime一致(se: {se_nichime!r}), race='01'")
        cur.execute(
            "SELECT race_shikonen, keibajo_code, kaisai_kai, kaisai_nichime, "
            "race_bango, umaban "
            "FROM jrd_cyb "
            "WHERE TRIM(keibajo_code) = '06' "
            "AND TRIM(kaisai_kai) = %s AND TRIM(kaisai_nichime) = %s "
            "AND TRIM(race_bango) = '01' "
            "ORDER BY umaban LIMIT 10",
            (se_kai, se_nichime)
        )
        for r in cur.fetchall():
            print(f"    shikonen={r[0]!r} keibajo={r[1]!r} kai={r[2]!r} "
                  f"nichime={r[3]!r} race={r[4]!r} uma={r[5]!r}")

        # 年を含めない場合にどうなるか
        print(f"\n[E3] cyb: keibajo='06', race='01', uma='01' で年なし検索")
        cur.execute(
            "SELECT race_shikonen, keibajo_code, kaisai_kai, kaisai_nichime, "
            "race_bango, umaban "
            "FROM jrd_cyb "
            "WHERE TRIM(keibajo_code) = '06' "
            "AND TRIM(race_bango) = '01' AND TRIM(umaban) = '01' "
            "ORDER BY race_shikonen LIMIT 20"
        )
        for r in cur.fetchall():
            print(f"    shikonen={r[0]!r} keibajo={r[1]!r} kai={r[2]!r} "
                  f"nichime={r[3]!r} race={r[4]!r} uma={r[5]!r}")

    print("\n" + "=" * 70)
    print("診断v2完了")
    print("=" * 70)

    conn.close()


if __name__ == "__main__":
    main()
