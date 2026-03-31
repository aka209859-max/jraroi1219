# roi_pipeline - Phase 1

JRA回収率特化パイプライン。感情を排除し、期待値（回収率）最大化のみを追求する。

## 概要

Phase 1では以下の4つの成果物を構築する:

1. **補正回収率算出エンジン** (`engine/corrected_return.py`)
   - 均等払戻方式（TARGET_PAYOUT=10,000円）
   - オッズ帯別配当補正係数（単勝123段階/複勝108段階）
   - 年度別重み係数（2016=1 〜 2025=10）
   - 基準点: 補正回収率80% = 0点

2. **階層ベイズ推定モジュール** (`engine/hierarchical_bayes.py`)
   - 3層構造: グローバル → カテゴリ → 個別
   - 信頼性重み: N / (N + C), C=50
   - 95%信頼区間付き

3. **Walk-Forward検証フレームワーク** (`engine/walk_forward.py`)
   - 初期学習: 2016-11 〜 2018-12
   - テスト: 月次スライド（拡張型）
   - 最終テスト月: 2025-12
   - ターゲットリーク検出テスト必須

4. **代表ファクター20個の検証レポート** (`reports/phase1/`)
   - 能力系4 + 適性系4 + 状態系4 + 環境系4 + 関係者系4 = 20ファクター
   - ビン別/カテゴリ別の補正回収率テーブル
   - Walk-Forward月次推移グラフ
   - エッジ判定（80%超の統計的有意性）

## ディレクトリ構成

```
roi_pipeline/
├── README.md
├── requirements.txt
├── __init__.py
│
├── config/
│   ├── __init__.py
│   ├── db.py                  # PostgreSQL接続設定
│   ├── odds_correction.py     # CEO提供: オッズ帯別補正係数
│   ├── year_weights.py        # 年度別重み係数
│   ├── course_categories.py   # CEO提供: 27カテゴリコース分類
│   └── segment_types.py       # セグメント分類タイプ定義
│
├── engine/
│   ├── __init__.py
│   ├── data_loader.py         # PostgreSQLデータ取得・型変換
│   ├── corrected_return.py    # 補正回収率算出エンジン
│   ├── hierarchical_bayes.py  # 階層ベイズ推定
│   └── walk_forward.py        # Walk-Forward検証
│
├── factors/
│   ├── __init__.py
│   ├── definitions.py         # 代表ファクター20個の定義
│   └── binning.py             # ビン分割ロジック
│
├── reports/
│   └── phase1/                # 検証レポート出力先
│
└── tests/
    ├── __init__.py
    ├── test_corrected_return.py
    ├── test_hierarchical_bayes.py
    ├── test_walk_forward.py
    └── test_no_leak.py
```

## セットアップ

```bash
pip install -r roi_pipeline/requirements.txt
```

## テスト実行

```bash
cd /path/to/jraroi1219
python -m pytest roi_pipeline/tests/ -v
```

## レポート生成

```bash
cd /path/to/jraroi1219
python -m roi_pipeline.reports.generate_phase1
```

※ PostgreSQL (127.0.0.1:5432, database: pckeiba) への接続が必要。

## 技術スタック

| 項目 | 仕様 |
|------|------|
| 言語 | Python 3.9 / 3.10 |
| DB | PostgreSQL (pckeiba) |
| Host | 127.0.0.1:5432 |
| 主要ライブラリ | pandas, numpy, scipy.stats, matplotlib, plotly |
| テスト | pytest |

## 重要な制約

- **全カラムがcharacter varying（文字列型）** → 数値演算前に必ず型変換
- **当日データ使用禁止** → 直前オッズ・当日馬体重・パドック評価・馬場状態速報
- **ターゲットリーク厳禁** → 全集計は予測対象日の前日以前のデータのみ
- **k分割交差検証禁止** → Walk-Forward法のみ使用可
- **config/内のファイルはCEO承認なく変更禁止**

## Phase 1 完了条件

- [ ] PostgreSQLからデータ取得できる
- [ ] 補正回収率算出エンジンが正しく動作する（単体テスト済み）
- [ ] 階層ベイズ推定が3層構造で動作する（単体テスト済み）
- [ ] Walk-Forwardでターゲットリークが一切ないことが自動テストで証明される
- [ ] 代表ファクター20個の検証レポートが生成される
- [ ] 補正回収率が80%を統計的に有意に超えるファクターの存在有無がレポートに明記される
