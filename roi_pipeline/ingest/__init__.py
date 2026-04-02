# JRDB direct-parse ingest pipeline
#
# PC-KEIBAのパースずれを完全に回避し、JRDB公式仕様書通りにパースする。
#
# モジュール構成:
#   jrdb_spec.py     - JRDB公式仕様のフィールド定義
#   jrdb_parser.py   - 固定長テキストファイルパーサー
#   jrdb_importer.py - PostgreSQL新テーブル(jrd_*_fixed)へのインポーター
