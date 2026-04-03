"""
全自動実行スクリプト: JRDB修正パイプライン

CEOのPC上で実行する統合スクリプト。
以下を順番に実行する:

  STEP 0: 事前チェック（DB接続、依存パッケージ）
  STEP 1: Eドライブのスキャン（JRDBファイル検索）
  STEP 2: JRDBファイルのパース＆インポート（jrd_*_fixed テーブル作成）
  STEP 3: JOIN検証（v2 JOINのマッチ率確認）
  STEP 4: Phase 1レポート再生成（v2データ使用）
  STEP 5: Phase 2 交互作用分析レポート生成

使用方法:
    cd E:\\jraroi1219
    py -3.12 roi_pipeline/tools/run_all.py

    # ステップを指定して実行する場合
    py -3.12 roi_pipeline/tools/run_all.py --step 0  # 事前チェックのみ
    py -3.12 roi_pipeline/tools/run_all.py --step 2  # インポートのみ
    py -3.12 roi_pipeline/tools/run_all.py --step 3  # JOIN検証のみ
    py -3.12 roi_pipeline/tools/run_all.py --step 4  # Phase1のみ
    py -3.12 roi_pipeline/tools/run_all.py --step 5  # Phase2のみ
"""
import argparse
import os
import sys
import time
from pathlib import Path

# プロジェクトルートをsys.pathに追加（直接実行対応）
_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

# デフォルトJRDBディレクトリ（CEOのPC上のパス）
DEFAULT_JRDB_DIR = r"E:\jraroi1219\data\jrdb\raw"


def step0_check():
    """STEP 0: 事前チェック"""
    print("\n" + "=" * 60)
    print("  STEP 0: 事前チェック")
    print("=" * 60)
    
    # Python version
    print(f"  Python: {sys.version}")
    
    # 依存パッケージ
    missing = []
    for pkg in ["psycopg2", "pandas", "numpy"]:
        try:
            __import__(pkg)
            print(f"  ✅ {pkg}")
        except ImportError:
            print(f"  ❌ {pkg} がインストールされていません")
            missing.append(pkg)
    
    if missing:
        print(f"\n  pip install {' '.join(missing)}")
        return False
    
    # DB接続
    try:
        import psycopg2
        conn = psycopg2.connect(
            host="127.0.0.1", port=5432,
            database="pckeiba", user="postgres", password="postgres123",
        )
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM jvd_se")
            count = cur.fetchone()[0]
            print(f"  ✅ DB接続OK (jvd_se: {count:,} rows)")
        conn.close()
    except Exception as e:
        print(f"  ❌ DB接続エラー: {e}")
        return False
    
    return True


def step1_scan(jrdb_dir: str):
    """STEP 1: ファイルスキャン"""
    print("\n" + "=" * 60)
    print(f"  STEP 1: ファイルスキャン ({jrdb_dir})")
    print("=" * 60)
    
    from roi_pipeline.ingest.jrdb_importer import run_scan
    run_scan(jrdb_dir)


def step2_import(jrdb_dir: str):
    """STEP 2: パース＆インポート"""
    print("\n" + "=" * 60)
    print(f"  STEP 2: パース＆インポート ({jrdb_dir})")
    print("=" * 60)
    
    from roi_pipeline.ingest.jrdb_importer import run_import
    
    t0 = time.time()
    run_import(jrdb_dir)
    elapsed = time.time() - t0
    
    print(f"\n  STEP 2 完了: {elapsed:.1f}秒")


def step3_verify():
    """STEP 3: JOIN検証"""
    print("\n" + "=" * 60)
    print("  STEP 3: v2 JOIN検証")
    print("=" * 60)
    
    from roi_pipeline.engine.data_loader_v2 import diagnose_v2_join
    
    # 全期間テスト
    result = diagnose_v2_join(date_from="20161101", date_to="20251231")
    print(result)
    
    # 2024年テスト
    print()
    result_2024 = diagnose_v2_join(date_from="20240101", date_to="20241231")
    print(result_2024)


def step4_phase1():
    """STEP 4: Phase 1 再生成"""
    print("\n" + "=" * 60)
    print("  STEP 4: Phase 1 レポート再生成")
    print("=" * 60)
    
    from roi_pipeline.reports.generate_phase1 import main as gen_main
    gen_main()


def step5_phase2():
    """STEP 5: Phase 2 交互作用分析レポート生成"""
    print("\n" + "=" * 60)
    print("  STEP 5: Phase 2 交互作用分析レポート生成")
    print("=" * 60)
    
    from roi_pipeline.reports.generate_phase2 import main as gen_phase2
    gen_phase2()


def main():
    parser = argparse.ArgumentParser(description="JRDB修正パイプライン全自動実行")
    parser.add_argument("jrdb_dir", nargs="?", default=None,
                        help=f"JRDBファイルのディレクトリ (デフォルト: {DEFAULT_JRDB_DIR})")
    parser.add_argument("--step", type=int, choices=[0, 1, 2, 3, 4, 5],
                        help="特定のステップのみ実行")
    
    args = parser.parse_args()
    
    # JRDBディレクトリの解決（指定なければデフォルト使用）
    jrdb_dir = args.jrdb_dir or DEFAULT_JRDB_DIR
    
    # ステップ指定実行
    if args.step is not None:
        if args.step == 0:
            step0_check()
        elif args.step == 1:
            step1_scan(jrdb_dir)
        elif args.step == 2:
            step2_import(jrdb_dir)
        elif args.step == 3:
            step3_verify()
        elif args.step == 4:
            step4_phase1()
        elif args.step == 5:
            step5_phase2()
        return
    
    # 全ステップ実行
    print("=" * 60)
    print("  JRDB修正パイプライン 全自動実行")
    print(f"  JRDBデータ: {jrdb_dir}")
    print("=" * 60)
    
    total_t0 = time.time()
    
    # STEP 0
    if not step0_check():
        print("\n事前チェックに失敗しました。")
        sys.exit(1)
    
    # STEP 1
    step1_scan(jrdb_dir)
    
    # STEP 2
    step2_import(jrdb_dir)
    
    # STEP 3
    step3_verify()
    
    # STEP 4
    step4_phase1()
    
    # STEP 5
    step5_phase2()
    
    total_elapsed = time.time() - total_t0
    print()
    print("=" * 60)
    print(f"  全ステップ完了: {total_elapsed:.1f}秒")
    print("=" * 60)


if __name__ == "__main__":
    main()
