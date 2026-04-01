"""
DB診断スクリプト — PowerShellから安全に実行可能

使用方法:
    py -3.12 -m roi_pipeline.tools.diagnose_db
"""
import sys
import os

# モジュールパスを自動解決（直接実行時も対応）
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from roi_pipeline.config.db import get_connection


def main():
    conn = get_connection()
    cur = conn.cursor()

    print("=" * 60)
    print("DB診断レポート")
    print("=" * 60)

    # 1. jrd_kyi の行数
    cur.execute("SELECT COUNT(*) FROM jrd_kyi")
    print(f"\n[1] jrd_kyi total: {cur.fetchone()[0]}")

    # 2. cyb の race_shikonen サンプル（2024年1月付近）
    cur.execute(
        "SELECT race_shikonen, COUNT(*) "
        "FROM jrd_cyb "
        "WHERE race_shikonen >= '240101' AND race_shikonen <= '240131' "
        "GROUP BY race_shikonen ORDER BY race_shikonen LIMIT 10"
    )
    rows = cur.fetchall()
    print(f"\n[2] cyb 2024年1月 race_shikonen サンプル: {rows}")

    # 3. cyb の race_shikonen が '24' で始まるデータ数
    cur.execute(
        "SELECT COUNT(*) FROM jrd_cyb WHERE race_shikonen LIKE '24%'"
    )
    print(f"\n[3] cyb race_shikonen '24xxxx' 件数: {cur.fetchone()[0]}")

    # 4. se の computed_shikonen サンプル
    cur.execute(
        "SELECT SUBSTRING(kaisai_nen, 3, 2) || kaisai_tsukihi AS shikonen "
        "FROM jvd_se "
        "WHERE (kaisai_nen || kaisai_tsukihi) >= '20240101' "
        "LIMIT 5"
    )
    print(f"\n[4] se computed_shikonen: {[r[0] for r in cur.fetchall()]}")

    # 5. kyi関連テーブル一覧
    cur.execute(
        "SELECT tablename FROM pg_tables "
        "WHERE schemaname='public' AND tablename LIKE '%kyi%'"
    )
    print(f"\n[5] kyi関連テーブル: {[r[0] for r in cur.fetchall()]}")

    # 6. 全JRDBテーブルのrace_shikonen範囲
    print("\n[6] 各JRDBテーブルの race_shikonen 範囲:")
    for tbl in ["jrd_kyi", "jrd_cyb", "jrd_joa", "jrd_bac"]:
        try:
            cur.execute(f"SELECT MIN(race_shikonen), MAX(race_shikonen), COUNT(*) FROM {tbl}")
            row = cur.fetchone()
            print(f"    {tbl}: {row[0]} 〜 {row[1]} ({row[2]:,} rows)")
        except Exception as e:
            print(f"    {tbl}: ERROR - {e}")
            conn.rollback()

    # 7. cyb のサンプルレコード（JOINキー確認用）
    cur.execute(
        "SELECT keibajo_code, race_shikonen, kaisai_kai, kaisai_nichime, "
        "race_bango, umaban FROM jrd_cyb LIMIT 5"
    )
    print("\n[7] cyb サンプルレコード:")
    for row in cur.fetchall():
        print(f"    keibajo={row[0]!r}, shikonen={row[1]!r}, "
              f"kai={row[2]!r}, nichime={row[3]!r}, "
              f"race={row[4]!r}, uma={row[5]!r}")

    # 8. cyb + se のJOIN診断（全期間）
    cur.execute(
        "SELECT COUNT(*) FROM jvd_se AS se "
        "INNER JOIN jrd_cyb AS cyb "
        "ON TRIM(se.keibajo_code) = TRIM(cyb.keibajo_code) "
        "AND TRIM(SUBSTRING(se.kaisai_nen, 3, 2) || se.kaisai_tsukihi) = TRIM(cyb.race_shikonen) "
        "AND CAST(NULLIF(TRIM(se.kaisai_kai), '') AS INTEGER) "
        "  = CAST(NULLIF(TRIM(cyb.kaisai_kai), '') AS INTEGER) "
        "AND CAST(NULLIF(TRIM(se.kaisai_nichime), '') AS INTEGER) "
        "  = CAST(NULLIF(TRIM(cyb.kaisai_nichime), '') AS INTEGER) "
        "AND CAST(NULLIF(TRIM(se.race_bango), '') AS INTEGER) "
        "  = CAST(NULLIF(TRIM(cyb.race_bango), '') AS INTEGER) "
        "AND CAST(NULLIF(TRIM(se.umaban), '') AS INTEGER) "
        "  = CAST(NULLIF(TRIM(cyb.umaban), '') AS INTEGER)"
    )
    print(f"\n[8] cyb CAST JOIN (全期間): {cur.fetchone()[0]:,} マッチ")

    # 9. shikonen形式の不一致チェック
    # se側: '240101' vs cyb側のフォーマットが違う可能性を検査
    cur.execute(
        "SELECT TRIM(SUBSTRING(se.kaisai_nen, 3, 2) || se.kaisai_tsukihi) AS se_shikonen, "
        "LENGTH(TRIM(SUBSTRING(se.kaisai_nen, 3, 2) || se.kaisai_tsukihi)) AS se_len "
        "FROM jvd_se AS se "
        "WHERE (se.kaisai_nen || se.kaisai_tsukihi) >= '20240101' "
        "LIMIT 3"
    )
    se_rows = cur.fetchall()
    cur.execute(
        "SELECT TRIM(race_shikonen) AS cyb_shikonen, "
        "LENGTH(TRIM(race_shikonen)) AS cyb_len "
        "FROM jrd_cyb "
        "WHERE race_shikonen >= '240101' AND race_shikonen <= '240131' "
        "LIMIT 3"
    )
    cyb_rows = cur.fetchall()
    print(f"\n[9] shikonen フォーマット比較:")
    print(f"    se  側: {se_rows}")
    print(f"    cyb 側: {cyb_rows}")

    # もしcyb側が空ならrace_shikonen全体から先頭を表示
    if not cyb_rows:
        cur.execute(
            "SELECT TRIM(race_shikonen), LENGTH(TRIM(race_shikonen)) "
            "FROM jrd_cyb ORDER BY race_shikonen LIMIT 5"
        )
        print(f"    cyb 先頭5件: {cur.fetchall()}")
        cur.execute(
            "SELECT TRIM(race_shikonen), LENGTH(TRIM(race_shikonen)) "
            "FROM jrd_cyb ORDER BY race_shikonen DESC LIMIT 5"
        )
        print(f"    cyb 末尾5件: {cur.fetchall()}")

    # =================================================================
    # 10. race_shikonen フォーマット詳細解析
    # =================================================================
    print("\n[10] race_shikonen フォーマット詳細解析:")

    # cyb の race_shikonen ユニーク値をソートして分布を見る
    cur.execute(
        "SELECT DISTINCT race_shikonen FROM jrd_cyb ORDER BY race_shikonen LIMIT 20"
    )
    print(f"    cyb ユニーク値(先頭20): {[r[0] for r in cur.fetchall()]}")

    cur.execute(
        "SELECT DISTINCT race_shikonen FROM jrd_cyb ORDER BY race_shikonen DESC LIMIT 20"
    )
    print(f"    cyb ユニーク値(末尾20): {[r[0] for r in cur.fetchall()]}")

    # '24' で始まるユニーク値
    cur.execute(
        "SELECT DISTINCT race_shikonen FROM jrd_cyb "
        "WHERE race_shikonen LIKE '24%' ORDER BY race_shikonen LIMIT 30"
    )
    vals_24 = [r[0] for r in cur.fetchall()]
    print(f"    cyb '24xxxx' ユニーク値(先頭30): {vals_24}")

    # JRDBの全カラム名を確認 — race_shikonen以外に日付キーがあるか
    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'jrd_cyb' ORDER BY ordinal_position"
    )
    cyb_cols = [r[0] for r in cur.fetchall()]
    print(f"\n[11] jrd_cyb 全カラム名: {cyb_cols}")

    cur.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'jrd_kyi' ORDER BY ordinal_position"
    )
    kyi_cols = [r[0] for r in cur.fetchall()]
    print(f"\n[12] jrd_kyi 全カラム名: {kyi_cols}")

    # cyb の先頭レコード全カラム（生データ確認）
    cur.execute("SELECT * FROM jrd_cyb LIMIT 1")
    colnames = [desc[0] for desc in cur.description]
    row = cur.fetchone()
    print(f"\n[13] jrd_cyb 先頭レコード（全カラム）:")
    for c, v in zip(colnames, row):
        print(f"    {c} = {v!r}")

    # JRA-VAN se の 2024/3/14 データ（kyiの260314と照合）
    cur.execute(
        "SELECT keibajo_code, kaisai_nen, kaisai_tsukihi, kaisai_kai, "
        "kaisai_nichime, race_bango, umaban, "
        "SUBSTRING(kaisai_nen, 3, 2) || kaisai_tsukihi AS computed_shikonen "
        "FROM jvd_se "
        "WHERE kaisai_nen = '2026' AND kaisai_tsukihi = '0314' "
        "LIMIT 5"
    )
    se_0314 = cur.fetchall()
    print(f"\n[14] se 2026/03/14 データ: {se_0314}")

    # JRDB bac のサンプル（レース単位テーブル）
    cur.execute(
        "SELECT * FROM jrd_bac LIMIT 1"
    )
    bac_colnames = [desc[0] for desc in cur.description]
    bac_row = cur.fetchone()
    print(f"\n[15] jrd_bac 先頭レコード（全カラム）:")
    for c, v in zip(bac_colnames, bac_row):
        print(f"    {c} = {v!r}")

    print("\n" + "=" * 60)
    print("診断完了")
    print("=" * 60)

    conn.close()


if __name__ == "__main__":
    main()
