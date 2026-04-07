# 🔍 データベーススキーマ完全調査レポート

**調査日**: 2026-04-07  
**調査者**: GenSpark AI Developer  
**プロジェクト**: jraroi1219  
**データソース**: pckeiba データベース（PostgreSQL 16.11）

---

## 📊 調査結果サマリー

### 🎯 最重要発見

**実際のDBには合計773カラムが存在**（以前の記載325カラムの**2.4倍**）

| データソース | 以前の記載 | 実際 | 差分 |
|-------------|-----------|------|------|
| **JRDB _fixed** | 116カラム | **97カラム** | **-19** ❌ |
| **JRA-VAN 主要13テーブル** | 209カラム | **676カラム** | **+467** ⚠️ |
| **合計** | **325カラム** | **773カラム** | **+448** 🚨 |

---

## 🧐 問題の原因

### 1️⃣ JRDB_116_COLUMNS.md の誤り

**問題点**:
- 別プロジェクト（anonymous-keiba-ai-jra）のCSVファイル `PHASE7A_COMBINED_497_UNIQUE_COLNAME.csv` を使用
- **元データ**（`jrd_kyi`, `jrd_cyb`など）の仕様を記載
- **実際に使用可能な`_fixed`テーブル**（パース済みデータ）のカラム数が少ない

**不一致の例**:
| ドキュメント記載 | 実際のDB | カラム例 |
|----------------|---------|---------|
| `jrd_kyi` 65カラム | `jrd_kyi_fixed` 38カラム | `joho_shisu` → 存在せず（`sogo_shisu`が存在） |
| `jrd_cyb` 18カラム | `jrd_cyb_fixed` 19カラム | 1カラム多い |
| `jrd_joa` 10カラム | `jrd_joa_fixed` 16カラム | 6カラム多い |
| `jrd_bac` 9カラム | `jrd_bac_fixed` 24カラム | 15カラム多い |

**Claude Code が遭遇したエラー**:
```
psycopg2.errors.UndefinedColumn: column kyi.joho_shisu does not exist
```
→ `joho_shisu` はドキュメントに記載されていたが、実際のDBには存在しない。

---

### 2️⃣ JRA_VAN_209_COLUMNS.md の不足

**問題点**:
- Phase 7-Aで**使用された統合データセット**（2016-2025、約460,424行）のカラムのみを記載
- **実際のDBには、その他多数のカラムが存在**

**不一致の例**:
| テーブル | ドキュメント記載 | 実際のDB | 差分 |
|---------|----------------|---------|------|
| `jvd_se` | 40カラム | **70カラム** | **+30** |
| `jvd_ra` | 31カラム | **62カラム** | **+31** |
| `jvd_ck` | 23カラム | **106カラム** | **+83** |
| `jvd_h1` | 21カラム | **43カラム** | **+22** |
| `jvd_hr` | 18カラム | **158カラム** | **+140** |
| `jvd_um` | 14カラム | **89カラム** | **+75** |

---

## ✅ 正確なスキーマ

### 実DBエクスポートデータ
- **ファイル**: `ACTUAL_DB_SCHEMA_2294_COLUMNS.csv`（238KB）
- **行数**: 2,294行（ヘッダー含む）
- **データソース**: pgAdmin4から直接エクスポート
- **取得クエリ**:
```sql
SELECT 
    t.table_name, 
    c.column_name, 
    c.ordinal_position, 
    c.data_type, 
    c.character_maximum_length,
    c.is_nullable,
    pg_catalog.col_description(t.oid, c.ordinal_position) AS comment
FROM information_schema.tables t
JOIN information_schema.columns c ON t.table_name = c.table_name
WHERE t.table_schema = 'public'
  AND (t.table_name LIKE 'jrd_%' OR t.table_name LIKE 'jvd_%')
ORDER BY t.table_name, c.ordinal_position;
```

---

## 📋 JRDB _fixedテーブル（97カラム）

| テーブル名 | カラム数 | 主な内容 |
|-----------|---------|---------|
| **jrd_kyi_fixed** | **38** | IDM、騎手指数、総合指数、適性コード、負担重量、調教矢印 |
| **jrd_cyb_fixed** | **19** | 調教タイプ、追切指数、仕上指数、調教量評価、仕上指数変化 |
| **jrd_joa_fixed** | **16** | LS指数、LS評価、オッズ指数 |
| **jrd_bac_fixed** | **24** | 年月日、距離、芝ダ障害、右左回り、内外、種別、条件、グレード |

### 🔥 最重要ファクター（jrd_kyi_fixed）
| カラム名 | 説明 | 重要度 |
|---------|------|--------|
| `idm` | IDMスピード指数 | ★★★★★ |
| `kishu_shisu` | 騎手指数 | ★★★★★ |
| `sogo_shisu` | 総合指数 | ★★★★★ |
| `kyakushitsu` | 脚質 | ★★★★ |
| `kyori_tekisei` | 距離適性 | ★★★★ |

**詳細ドキュメント**: `ACTUAL_JRDB_97_COLUMNS.md`

---

## 📋 JRA-VAN 主要テーブル（676カラム）

| テーブル名 | カラム数 | 主な内容 |
|-----------|---------|---------|
| **jvd_se** | **70** | 着順、タイム、オッズ、獲得賞金、後半ラップ、脚質判定 |
| **jvd_ra** | **62** | レース名、グレード、距離、コース、天候、馬場状態、ラップタイム |
| **jvd_ck** | **106** | 本賞金累計、コース別成績、馬場別成績、距離別成績、競馬場別成績 |
| **jvd_h1** | **43** | 単勝～三連複の票数（最大16320文字） |
| **jvd_hr** | **158** | 単勝～三連単の払戻金額詳細（全組み合わせ） |
| **jvd_um** | **89** | 血統、生産者、馬主、所属、馬記号 |
| **jvd_sk** | **26** | 父、母、母父の成績情報 |
| **jvd_dm** | **28** | 調教師成績、所属厩舎 |
| **jvd_wc** | **29** | 馬体重、増減 |
| **jvd_h6** | **16** | 6ハロン指数、速度指数 |
| **jvd_hc** | **14** | 詳細な馬場状態 |
| **jvd_jg** | **14** | 騎手成績、所属 |
| **jvd_ch** | **21** | GI・重賞成績 |

### 🌐 全JRA-VANテーブル（38テーブル、1,649カラム）
最大規模テーブル:
- `jvd_tk`: 336カラム（拓殖データ）
- `jvd_wf`: 266カラム（払戻し詳細フォーマット）
- `jvd_hr`: 158カラム（払戻し金額詳細）
- `jvd_ck`: 106カラム（過去成績）

**詳細ドキュメント**: `ACTUAL_JRA_VAN_676_COLUMNS.md`

---

## 🔗 テーブル結合条件

### JRDB _fixed テーブル結合
```sql
-- 主キー（馬単位）
keibajo_code, race_shikonen, kaisai_kai, kaisai_nichime, race_bango, umaban

-- 主キー（レース単位 - jrd_bac_fixed）
keibajo_code, race_shikonen, kaisai_kai, kaisai_nichime, race_bango
```

### JRA-VAN テーブル結合
```sql
-- 主キー（馬単位）
kaisai_nen, kaisai_tsukihi, keibajo_code, kaisai_kai, kaisai_nichime, race_bango, umaban

-- 主キー（レース単位）
kaisai_nen, kaisai_tsukihi, keibajo_code, kaisai_kai, kaisai_nichime, race_bango
```

### JRDB ⇔ JRA-VAN 結合
```sql
-- 日付変換が必要
jrd.race_shikonen = SUBSTRING(jvd.kaisai_nen, 3, 2) || jvd.kaisai_tsukihi
-- 例: jrd='260222' ⇔ jvd.kaisai_nen='2026' + jvd.kaisai_tsukihi='0222'
```

---

## ⚠️ データ型の注意事項

### 全カラムが `character varying(255)` または可変長
- **数値計算時は必ずCAST**:
  ```sql
  CAST(idm AS INTEGER)
  CAST(kishu_shisu AS NUMERIC)
  CAST(kyori AS NUMERIC)
  ```

- **NULL値処理**:
  ```sql
  COALESCE(CAST(oikiri_shisu AS INTEGER), 0)
  ```

- **大規模文字列カラム**:
  - `jvd_h1.hyosu_sanrenpuku`: 16,320文字
  - `jvd_ck.seiseki_joho_kishu_1/2`: 1,220文字 × 2

---

## 🚨 full_factor_loader.py 修正ポイント

### 問題のあるクエリ例（修正前）
```python
# ❌ 存在しないカラム名
kyi.joho_shisu,        # 実際には存在しない
kyi.gekiso_shisu,      # 実際には存在しない
kyi.info_index         # 実際には存在しない
```

### 修正版クエリ例（修正後）
```python
# ✅ 実際に存在するカラム名
kyi.sogo_shisu,        # 総合指数（存在する）
kyi.idm,               # IDM（存在する）
kyi.kishu_shisu,       # 騎手指数（存在する）
```

### LEFT JOIN修正
```python
# ✅ 正しい主キー結合
LEFT JOIN jrd_cyb_fixed cyb 
    ON kyi.keibajo_code = cyb.keibajo_code
    AND kyi.race_shikonen = cyb.race_shikonen
    AND kyi.kaisai_kai = cyb.kaisai_kai
    AND kyi.kaisai_nichime = cyb.kaisai_nichime
    AND kyi.race_bango = cyb.race_bango
    AND kyi.umaban = cyb.umaban
```

---

## 📂 作成ドキュメント一覧

| ファイル名 | サイズ | 説明 |
|-----------|-------|------|
| `ACTUAL_DB_SCHEMA_2294_COLUMNS.csv` | 238KB | pgAdmin4からエクスポートした全スキーマ（59テーブル、2,293カラム） |
| `ACTUAL_JRDB_97_COLUMNS.md` | 8KB | JRDB _fixedテーブル詳細（4テーブル、97カラム） |
| `ACTUAL_JRA_VAN_676_COLUMNS.md` | 8KB | JRA-VAN主要テーブル詳細（13テーブル、676カラム） |
| `SCHEMA_INVESTIGATION_REPORT.md` | 本ファイル | 調査レポート総まとめ |

---

## 🎯 次のアクション

### 1. Claude Code への情報提供 ✅
- [x] `ACTUAL_JRDB_97_COLUMNS.md` を共有
- [x] 実カラム名リストを提供
- [ ] `full_factor_loader.py` の修正を依頼

### 2. jraroi1219 リポジトリ更新
- [ ] 新ドキュメント3点をコミット
- [ ] README更新（773カラム存在を明記）
- [ ] PRを作成してmainブランチにマージ

### 3. 325ファクター分析の再設計
- [ ] 773カラムから有効ファクターを選定
- [ ] 特徴量エンジニアリング計画
- [ ] ROI最大化のための特徴量選択

---

## 📝 結論

### ✅ 解決したこと
1. **全DBスキーマを正確に把握**（2,293カラム、59テーブル）
2. **使用可能なカラムを明確化**（773カラム: JRDB 97 + JRA-VAN 676）
3. **存在しないカラム名を特定**（`joho_shisu`, `gekiso_shisu`等）

### 🚨 判明した問題
1. 以前のドキュメント（JRDB_116_COLUMNS.md, JRA_VAN_209_COLUMNS.md）は**不正確**
2. `full_factor_loader.py` に存在しないカラム名が含まれている
3. 実際には**325カラムではなく773カラム**が使用可能

### 🔧 今後の対応
1. **full_factor_loader.py の緊急修正**（存在しないカラム名を置換）
2. **ドキュメントの更新**（jraroi1219 リポジトリ）
3. **ファクター分析の再実行**（773カラム対応）

---

**報告者**: GenSpark AI Developer  
**調査完了日時**: 2026-04-07  
**次回更新**: full_factor_loader.py 修正完了後
