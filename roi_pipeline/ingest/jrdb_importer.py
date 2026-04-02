"""
JRDBパース結果をPostgreSQLの新テーブル（jrd_*_fixed）にインポートする

使用方法:
    py -3.12 -m roi_pipeline.ingest.jrdb_importer --scan E:\\JRDB
    py -3.12 -m roi_pipeline.ingest.jrdb_importer --import E:\\JRDB
    py -3.12 -m roi_pipeline.ingest.jrdb_importer --import E:\\JRDB --type KYI

ステップ:
  1. --scan: ディレクトリ内のJRDBファイルを一覧表示（ドライランラン）
  2. --import: パース→テーブル作成→UPSERT

新テーブル:
  - jrd_kyi_fixed: KYI（競走馬データ）正しくパースされたデータ
  - jrd_cyb_fixed: CYB（調教分析データ）
  - jrd_bac_fixed: BAC（番組データ）
  - jrd_joa_fixed: JOA（成績速報データ）

各テーブルに追加される派生カラム:
  - jrdb_race_key8: 公式8byteレースキー
  - race_shikonen: PC-KEIBA互換6桁コード
  - keibajo_code, kaisai_kai, kaisai_nichime, race_bango: JRA-VAN JOIN用
"""
import argparse
import os
import sys
import time
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Optional

try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    print("ERROR: psycopg2がインストールされていません。")
    print("  pip install psycopg2-binary")
    sys.exit(1)

from roi_pipeline.ingest.jrdb_parser import (
    parse_kyi_file,
    parse_cyb_file,
    parse_bac_file,
    parse_joa_file,
)
from roi_pipeline.ingest.jrdb_spec import (
    KYI_IMPORT_FIELDS, CYB_IMPORT_FIELDS,
    BAC_IMPORT_FIELDS, JOA_IMPORT_FIELDS,
)

DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 5432,
    "database": "pckeiba",
    "user": "postgres",
    "password": "postgres123",
}

# 派生フィールド（全テーブル共通）
DERIVED_FIELDS = [
    "jrdb_race_key8",
    "race_shikonen",
    "keibajo_code",
    "kaisai_kai",
    "kaisai_nichime",
    "kaisai_nen_2",
    "race_bango",
]


# =============================================================================
# テーブル定義
# =============================================================================

TABLE_CONFIGS = {
    "KYI": {
        "table_name": "jrd_kyi_fixed",
        "parser": parse_kyi_file,
        "import_fields": KYI_IMPORT_FIELDS,
        "has_umaban": True,
        "pk_fields": ["jrdb_race_key8", "umaban"],  # レースキー + 馬番
    },
    "CYB": {
        "table_name": "jrd_cyb_fixed",
        "parser": parse_cyb_file,
        "import_fields": CYB_IMPORT_FIELDS,
        "has_umaban": True,
        "pk_fields": ["jrdb_race_key8", "umaban"],
    },
    "BAC": {
        "table_name": "jrd_bac_fixed",
        "parser": parse_bac_file,
        "import_fields": BAC_IMPORT_FIELDS,
        "has_umaban": False,
        "pk_fields": ["jrdb_race_key8"],  # レースキーのみ（馬番なし）
    },
    "JOA": {
        "table_name": "jrd_joa_fixed",
        "parser": parse_joa_file,
        "import_fields": JOA_IMPORT_FIELDS,
        "has_umaban": True,
        "pk_fields": ["jrdb_race_key8", "umaban"],
    },
}


def get_all_columns(import_fields: List[str], has_umaban: bool) -> List[str]:
    """テーブルの全カラムリストを返す。"""
    # 元のフィールド + 派生フィールド（重複除去、順序保持）
    cols = []
    seen = set()
    
    # 派生フィールドを先頭に
    for f in DERIVED_FIELDS:
        if f not in seen:
            cols.append(f)
            seen.add(f)
    
    # umabanは派生フィールドに含まれない場合
    if has_umaban and "umaban" not in seen:
        cols.append("umaban")
        seen.add("umaban")
    
    # 元のインポートフィールド
    for f in import_fields:
        if f not in seen:
            cols.append(f)
            seen.add(f)
    
    return cols


def create_table(conn, table_name: str, columns: List[str], pk_fields: List[str]) -> None:
    """テーブルを作成する（既存の場合はDROPして再作成）。"""
    drop_sql = f"DROP TABLE IF EXISTS {table_name} CASCADE"
    
    col_defs = []
    for col in columns:
        col_defs.append(f"    {col} VARCHAR(255)")
    
    pk_clause = f",\n    PRIMARY KEY ({', '.join(pk_fields)})" if pk_fields else ""
    
    create_sql = f"""
    CREATE TABLE {table_name} (
{(','+chr(10)).join(col_defs)}{pk_clause}
    )
    """
    
    with conn.cursor() as cur:
        cur.execute(drop_sql)
        cur.execute(create_sql)
    conn.commit()
    print(f"  テーブル {table_name} を作成しました（{len(columns)} カラム）")


def create_indexes(conn, table_name: str, has_umaban: bool) -> None:
    """インデックスを作成する。"""
    indexes = [
        (f"idx_{table_name}_race_key8", "jrdb_race_key8"),
        (f"idx_{table_name}_shikonen", "race_shikonen"),
        (f"idx_{table_name}_basho_year", "keibajo_code, kaisai_nen_2"),
    ]
    if has_umaban:
        indexes.append(
            (f"idx_{table_name}_race_uma", "jrdb_race_key8, umaban")
        )
    
    with conn.cursor() as cur:
        for idx_name, idx_cols in indexes:
            try:
                cur.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table_name} ({idx_cols})")
            except Exception as e:
                print(f"  INDEX WARNING: {idx_name}: {e}")
    conn.commit()


def upsert_records(
    conn,
    table_name: str,
    columns: List[str],
    records: List[Dict[str, str]],
    pk_fields: List[str],
    batch_size: int = 5000,
) -> int:
    """
    レコードをUPSERTする。
    ON CONFLICT で重複キーは上書き。
    """
    if not records:
        return 0
    
    # VALUES用のタプルリストを作成
    values_list = []
    for rec in records:
        row = tuple(rec.get(col, "") for col in columns)
        values_list.append(row)
    
    # UPSERT SQL
    col_list = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns))
    update_cols = [c for c in columns if c not in pk_fields]
    update_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in update_cols)
    
    pk_clause = ", ".join(pk_fields)
    
    sql = f"""
        INSERT INTO {table_name} ({col_list})
        VALUES ({placeholders})
        ON CONFLICT ({pk_clause}) DO UPDATE SET
        {update_clause}
    """
    
    inserted = 0
    with conn.cursor() as cur:
        for i in range(0, len(values_list), batch_size):
            batch = values_list[i:i + batch_size]
            cur.executemany(sql, batch)
            inserted += len(batch)
    
    conn.commit()
    return inserted


def scan_jrdb_files(root_dir: str) -> Dict[str, List[Path]]:
    """
    ディレクトリ内のJRDBファイルを検索する。
    
    以下の構造に対応:
      1. root_dir/KYI160105.txt         （ルート直下にtxtがある場合）
      2. root_dir/KYI/KYI160105.txt     （タイプ名サブフォルダ）
      3. root_dir/data/jrdb/raw/KYI/... （深い階層）
    """
    targets = ["KYI", "CYB", "BAC", "JOA"]
    extensions = [".txt"]
    
    found: Dict[str, List[Path]] = defaultdict(list)
    
    root = Path(root_dir)
    if not root.exists():
        print(f"ERROR: ディレクトリが存在しません: {root_dir}")
        return found
    
    # 戦略1: タイプ名のサブフォルダを直接探す（最速）
    for target in targets:
        # root/KYI/ , root/data/jrdb/raw/KYI/ 等を再帰的に探す
        for subdir in root.rglob(target):
            if not subdir.is_dir():
                continue
            try:
                for f in subdir.iterdir():
                    if f.is_file():
                        name_upper = f.name.upper()
                        if name_upper.startswith(target) and name_upper.endswith(".TXT"):
                            found[target].append(f)
            except PermissionError:
                continue
    
    # 戦略2: ルート直下のファイルも検索（フラット配置対応）
    try:
        for f in root.iterdir():
            if not f.is_file():
                continue
            name_upper = f.name.upper()
            for target in targets:
                if name_upper.startswith(target) and name_upper.endswith(".TXT"):
                    if f not in found[target]:  # 重複回避
                        found[target].append(f)
    except PermissionError:
        pass
    
    return found


def run_scan(root_dir: str) -> None:
    """スキャンモード: ファイル一覧を表示する。"""
    print(f"\nJRDBファイルスキャン: {root_dir}")
    print("=" * 60)
    
    found = scan_jrdb_files(root_dir)
    
    for ftype in ["KYI", "CYB", "BAC", "JOA"]:
        files = sorted(found.get(ftype, []), key=lambda p: p.name)
        if not files:
            print(f"  [{ftype}] ファイルなし")
            continue
        
        total_size = sum(f.stat().st_size for f in files)
        print(f"  [{ftype}] {len(files)} ファイル, 合計 {total_size / 1024 / 1024:.1f} MB")
        
        # 先頭3件+末尾3件
        if len(files) <= 6:
            for f in files:
                print(f"    {f.name}  ({f.stat().st_size:,} bytes)")
        else:
            for f in files[:3]:
                print(f"    {f.name}  ({f.stat().st_size:,} bytes)")
            print(f"    ... ({len(files) - 6} files omitted) ...")
            for f in files[-3:]:
                print(f"    {f.name}  ({f.stat().st_size:,} bytes)")
    
    print()


def run_import(
    root_dir: str,
    file_types: Optional[List[str]] = None,
    dry_run: bool = False,
) -> None:
    """
    インポートモード: パース→テーブル作成→UPSERT
    """
    if file_types is None:
        file_types = ["KYI", "CYB", "BAC", "JOA"]
    
    found = scan_jrdb_files(root_dir)
    
    print(f"\nJRDBインポート: {root_dir}")
    print(f"対象: {', '.join(file_types)}")
    print("=" * 60)
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
    except Exception as e:
        print(f"DB接続エラー: {e}")
        sys.exit(1)
    
    for ftype in file_types:
        ftype = ftype.upper()
        if ftype not in TABLE_CONFIGS:
            print(f"  [{ftype}] 未知のファイルタイプ、スキップ")
            continue
        
        config = TABLE_CONFIGS[ftype]
        files = sorted(found.get(ftype, []), key=lambda p: p.name)
        
        if not files:
            print(f"  [{ftype}] ファイルなし、スキップ")
            continue
        
        print(f"\n--- {ftype}: {len(files)} ファイルをインポート ---")
        
        # カラムリスト
        all_columns = get_all_columns(config["import_fields"], config["has_umaban"])
        
        if not dry_run:
            # テーブル作成
            create_table(conn, config["table_name"], all_columns, config["pk_fields"])
        
        total_records = 0
        total_time = 0.0
        errors = 0
        
        for i, fpath in enumerate(files, 1):
            t0 = time.time()
            try:
                records = config["parser"](str(fpath))
                elapsed = time.time() - t0
                total_time += elapsed
                
                if not dry_run and records:
                    inserted = upsert_records(
                        conn, config["table_name"],
                        all_columns, records, config["pk_fields"],
                    )
                    total_records += inserted
                else:
                    total_records += len(records)
                
                # 進捗表示（10ファイルごと or 最後）
                if i % 10 == 0 or i == len(files):
                    print(f"  [{i}/{len(files)}] {fpath.name}: "
                          f"{len(records)} records ({elapsed:.1f}s) "
                          f"→ 累計: {total_records:,}")
                    
            except Exception as e:
                errors += 1
                print(f"  ERROR: {fpath.name}: {e}")
                if errors > 10:
                    print("  エラーが10件を超えたため中断します。")
                    break
        
        if not dry_run:
            # インデックス作成
            create_indexes(conn, config["table_name"], config["has_umaban"])
        
        print(f"  {ftype} 完了: {total_records:,} records, {total_time:.1f}s, errors: {errors}")
    
    # 結果検証
    if not dry_run:
        print(f"\n--- インポート結果検証 ---")
        with conn.cursor() as cur:
            for ftype in file_types:
                ftype = ftype.upper()
                if ftype not in TABLE_CONFIGS:
                    continue
                table = TABLE_CONFIGS[ftype]["table_name"]
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cur.fetchone()[0]
                    cur.execute(f"""
                        SELECT MIN(race_shikonen), MAX(race_shikonen)
                        FROM {table}
                    """)
                    min_val, max_val = cur.fetchone()
                    print(f"  {table}: {count:,} rows ({min_val} 〜 {max_val})")
                except Exception as e:
                    print(f"  {table}: ERROR - {e}")
    
    conn.close()
    print("\nインポート完了!")


def run_verify(conn=None) -> None:
    """
    インポート済みデータの検証: JRA-VANとのJOIN成功率を確認する。
    """
    close_conn = False
    if conn is None:
        conn = psycopg2.connect(**DB_CONFIG)
        close_conn = True
    
    print("\n--- JOIN検証: jrd_*_fixed テーブル vs jvd_se ---")
    
    queries = {
        "kyi_fixed JOIN (race_key8)": """
            SELECT
                COUNT(*) AS total_se,
                COUNT(kyi.idm) AS matched,
                ROUND(COUNT(kyi.idm)::NUMERIC / NULLIF(COUNT(*), 0) * 100, 2) AS pct
            FROM jvd_se se
            LEFT JOIN jrd_kyi_fixed kyi
                ON TRIM(se.keibajo_code)
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
                    = kyi.jrdb_race_key8
                AND TRIM(se.umaban) = TRIM(kyi.umaban)
            WHERE se.kaisai_nen >= '2024'
        """,
        "cyb_fixed JOIN (race_key8)": """
            SELECT
                COUNT(*) AS total_se,
                COUNT(cyb.chokyo_hyoka) AS matched,
                ROUND(COUNT(cyb.chokyo_hyoka)::NUMERIC / NULLIF(COUNT(*), 0) * 100, 2) AS pct
            FROM jvd_se se
            LEFT JOIN jrd_cyb_fixed cyb
                ON TRIM(se.keibajo_code)
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
                    = cyb.jrdb_race_key8
                AND TRIM(se.umaban) = TRIM(cyb.umaban)
            WHERE se.kaisai_nen >= '2024'
        """,
    }
    
    with conn.cursor() as cur:
        for label, sql in queries.items():
            try:
                cur.execute(sql)
                row = cur.fetchone()
                print(f"  {label}: total={row[0]:,}, matched={row[1]:,}, pct={row[2]}%")
            except Exception as e:
                print(f"  {label}: ERROR - {e}")
    
    if close_conn:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="JRDB固定長ファイル→PostgreSQLインポーター")
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--scan", metavar="DIR", help="ファイルスキャンモード")
    group.add_argument("--import", metavar="DIR", dest="import_dir", help="インポートモード")
    group.add_argument("--verify", action="store_true", help="JOIN検証モード")
    parser.add_argument("--type", nargs="+", choices=["KYI", "CYB", "BAC", "JOA"],
                        help="対象ファイルタイプ（省略時は全タイプ）")
    parser.add_argument("--dry-run", action="store_true", help="パースのみ（DB書き込みなし）")
    
    args = parser.parse_args()
    
    if args.scan:
        run_scan(args.scan)
    elif args.import_dir:
        run_import(args.import_dir, args.type, args.dry_run)
    elif args.verify:
        run_verify()


if __name__ == "__main__":
    main()
