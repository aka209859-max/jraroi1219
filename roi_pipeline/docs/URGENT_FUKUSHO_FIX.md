# 緊急タスク: 複勝データ対応 — 完全実行指示書

## 発行日: 2026-04-03
## ステータス: URGENT / BLOCKING Phase 3

---

## 1. 根本原因分析（確定）

### 症状
Phase 2 タスク2の全10レポートで `複勝オッズデータなし。単勝のみで分析。` と表示。

### 原因
`data_loader_v2.py` (line 92) で `se.*` を使い jvd_se テーブルの全カラムを取得している。  
**しかし、`generate_phase2_task2.py` の `_compute_dual_roi()` (line 148) が `fukusho_odds` カラムの存在をチェックしたところ、DataFrameに存在しない。**

これは以下のいずれかが原因：

| 仮説 | 可能性 | 確認方法 |
|------|--------|---------|
| **A: jvd_se テーブルに `fukusho_odds` カラムが存在しない** | 高 | SQL実行で確認 |
| **B: カラムは存在するが全行NULL** | 中 | SQL実行で確認 |
| **C: `convert_numeric_columns()` で変換に失敗しドロップされた** | 低 | コード確認済み（drop しない設計） |

### 関連するカラム名の整理

| 用途 | Phase 1 (generate_phase1.py) | Phase 2 (generate_phase2_task2.py) | data_loader v1 |
|------|-----|-----|-----|
| 単勝オッズ | `tansho_odds` | `tansho_odds` | `tansho_odds` ✅ |
| 複勝オッズ | `fukusho_odds` | `fukusho_odds` | `fukusho_odds` ✅ |
| 確定着順（単勝判定: ==1） | `kakutei_chakujun` | `kakutei_chakujun` | `kakutei_chakujun` ✅ |
| 複勝判定（<=3） | なし（Phase 1は単勝のみ） | `kakutei_jyuni` をチェック（line 160-163） | — |

**重要発見**: `_compute_dual_roi()` の line 160 で `kakutei_jyuni` を参照しているが、jvd_se テーブルのカラム名は `kakutei_chakujun`。  
→ **仮に `fukusho_odds` が存在しても、`kakutei_jyuni` が無いためフォールバックで `_empty_roi_result()` が返される。**

---

## 2. 実行手順

### Step 1: DBスキーマ確認SQL（CEOのPC環境で実行）

以下のSQLをPostgreSQL（pckeiba データベース）で実行し、結果をコピペで報告してください。

```sql
-- Q1: jvd_se テーブルに fukusho 関連カラムがあるか？
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'jvd_se'
  AND column_name ILIKE '%fuku%'
ORDER BY ordinal_position;

-- Q2: jvd_se テーブルの確定着順カラム名は？
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'jvd_se'
  AND (column_name ILIKE '%kakutei%' OR column_name ILIKE '%chaku%' OR column_name ILIKE '%jyuni%')
ORDER BY ordinal_position;

-- Q3: jvd_se に tansho_odds / fukusho_odds の実データ確認
SELECT
  COUNT(*) AS total_rows,
  COUNT(tansho_odds) AS tansho_odds_count,
  COUNT(fukusho_odds) AS fukusho_odds_count
FROM jvd_se
WHERE (kaisai_nen || kaisai_tsukihi) >= '20240101';

-- Q4: fukusho_odds の値サンプル（NULLでなければ）
SELECT tansho_odds, fukusho_odds, kakutei_chakujun, umaban, keibajo_code
FROM jvd_se
WHERE fukusho_odds IS NOT NULL
  AND TRIM(fukusho_odds) != ''
  AND (kaisai_nen || kaisai_tsukihi) >= '20240101'
LIMIT 10;

-- Q5: jvd_hr テーブルの存在確認と構造
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'jvd_hr'
ORDER BY ordinal_position;

-- Q6: jvd_hr の行数
SELECT COUNT(*) FROM jvd_hr;

-- Q7: jvd_o1/jvd_o2 テーブルの存在確認
SELECT table_name
FROM information_schema.tables
WHERE table_name IN ('jvd_o1', 'jvd_o2', 'jvd_hr', 'jvd_h1')
ORDER BY table_name;
```

### Step 2: 結果に基づく対応方針

#### パターンA: `fukusho_odds` が jvd_se に存在する場合

**修正箇所は `generate_phase2_task2.py` の1箇所のみ。**

`_compute_dual_roi()` (line 160) の `kakutei_jyuni` を `kakutei_chakujun` に修正する：

```python
# 修正前 (line 160-163):
if "kakutei_jyuni" in fukusho_df.columns:
    fukusho_df = fukusho_df.copy()
    fukusho_df["fukusho_is_hit"] = (
        pd.to_numeric(fukusho_df["kakutei_jyuni"], errors="coerce") <= 3
    ).astype(int)

# 修正後:
if "kakutei_chakujun" in fukusho_df.columns:
    fukusho_df = fukusho_df.copy()
    fukusho_df["fukusho_is_hit"] = (
        pd.to_numeric(fukusho_df["kakutei_chakujun"], errors="coerce") <= 3
    ).astype(int)
```

**同様に line 337-339 も修正:**

```python
# 修正前 (line 337-339):
if "kakutei_jyuni" in df_fukusho.columns:
    df_fukusho["fukusho_is_hit"] = (
        pd.to_numeric(df_fukusho["kakutei_jyuni"], errors="coerce") <= 3
    ).astype(int)

# 修正後:
if "kakutei_chakujun" in df_fukusho.columns:
    df_fukusho["fukusho_is_hit"] = (
        pd.to_numeric(df_fukusho["kakutei_chakujun"], errors="coerce") <= 3
    ).astype(int)
```

**同様に line 799 も修正:**

```python
# 修正前:
if "kakutei_jyuni" in df_fukusho.columns:

# 修正後:
if "kakutei_chakujun" in df_fukusho.columns:
```

→ ファイル全体で `kakutei_jyuni` を `kakutei_chakujun` に一括置換する。

#### パターンB: `fukusho_odds` が jvd_se に存在しない場合

jvd_hr（払戻テーブル）から複勝オッズを取得する必要がある。  
新規ファイル `roi_pipeline/engine/data_loader_fukusho.py` を作成し、  
jvd_hr をJOINして `fukusho_odds` を補完する。

**この場合は追加のSQLを実行して jvd_hr のスキーマを確認する必要がある。**

#### パターンC: `fukusho_odds` が存在するが全行NULL

jvd_hr からデータを取得する方式に切り替え（パターンBと同じ対応）。

### Step 3: 修正適用後の確認

```bash
# 修正後、レポートを1つだけ再生成してテスト
cd E:\jraroi1219
py -3.12 -m roi_pipeline.reports.generate_phase2_task2 --factor idm

# 出力レポートで「複勝」セクションにデータが入っていることを確認
type roi_pipeline\reports\phase2\idm_surface2.md | findstr "複勝"
```

### Step 4: 全レポート再生成

```bash
# 全10レポートを再生成
py -3.12 -m roi_pipeline.reports.generate_phase2_task2
```

### Step 5: テスト実行

```bash
# 全テスト（104+）をパス
py -3.12 -m pytest roi_pipeline/tests/ -v
```

### Step 6: Git コミット

```bash
git add -A
git commit -m "fix: kakutei_jyuni → kakutei_chakujun for fukusho hit detection in Phase2 Task2"
git push origin main
```

---

## 3. 技術根拠の詳細

### 現行コードフロー（なぜ複勝データが出ないか）

```
generate_phase2_task2.py main()
  ↓
load_base_race_data_v2()   ← se.* で jvd_se 全カラム取得
  ↓                           （fukusho_odds が存在すれば取得される）
convert_numeric_columns()  ← fukusho_odds を numeric に変換
  ↓
add_hit_flag()             ← kakutei_chakujun == 1 で is_hit 作成（単勝用）
  ↓
_compute_dual_roi()
  ↓
  if "fukusho_odds" in df.columns:  ← チェック①: OK（存在すれば通過）
    ↓
    if "fukusho_is_hit" in df.columns:  ← チェック②: NG（まだ作成されていない）
      ↓ [else分岐]
      if "kakutei_jyuni" in df.columns:  ← チェック③: NG!!!
        ↓                                   カラム名は kakutei_chakujun であり
        ↓                                   kakutei_jyuni は存在しない
        [else → _empty_roi_result()]  ← 「データなし」として処理される
```

**結論: `kakutei_jyuni` → `kakutei_chakujun` の1箇所の修正で、fukusho_odds がDBに存在する限り、複勝ROIが計算される。**

### 補正回収率エンジンの複勝対応状況

| コンポーネント | 複勝対応 | 備考 |
|-------------|---------|------|
| `corrected_return.py` | ✅ 完了 | `is_fukusho=True` パラメータあり |
| `odds_correction.py` | ✅ 完了 | 複勝108段階マスタ定義済み |
| `generate_phase2_task2.py` | ⚠️ バグ | `kakutei_jyuni` が未定義カラム |
| `interaction_analysis.py` | ✅ 完了 | odds_col/hit_flag_col 設計 |

---

## 4. 品質ゲート

修正後に確認すべき品質ゲート:

1. **全テスト (104+) がパス** → `py -3.12 -m pytest roi_pipeline/tests/ -v`
2. **グローバル単勝ROI: 75-85%** → 既存の ≈79.91% が維持されること
3. **グローバル複勝ROI: 75-85%** → 新たに算出される値がこの範囲内であること
4. **10レポート全てに複勝テーブルが出力** → `findstr "複勝" roi_pipeline\reports\phase2\*.md`
5. **Phase 1 ファイル無変更** → `git diff roi_pipeline/reports/phase1/`

---

## 5. リスク評価

| リスク | 影響 | 対策 |
|--------|------|------|
| fukusho_odds が jvd_se に無い | 高 | Step 1 SQL で確認 → パターンBへ分岐 |
| fukusho_odds が全行NULL | 中 | jvd_hr からの取得ルートを準備 |
| 修正後の複勝ROIが範囲外 | 低 | 品質ゲートで検出→オッズ補正テーブル確認 |
| 既存テスト破壊 | 低 | kakutei_jyuni はテストコードに不使用（確認済み） |

---

*作成: CSO AI / 2026-04-03*
