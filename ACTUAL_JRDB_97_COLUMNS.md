# 📊 JRDB実データベーススキーマ（97カラム）

**作成日**: 2026-04-07  
**データソース**: pckeiba データベース（PostgreSQL 16.11）  
**スキーマファイル**: ACTUAL_DB_SCHEMA_2294_COLUMNS.csv（pgAdmin4エクスポート）

---

## ⚠️ 重要な発見

### ❌ 以前のドキュメント（JRDB_116_COLUMNS.md）は**不正確**でした

**問題点**:
- 存在しないカラム名を含む（例: `joho_shisu`, `info_index` など）
- カラム数が不一致（記載: 116カラム → 実際: 97カラム）
- 元データ（`jrd_kyi`, `jrd_cyb`など）と`_fixed`テーブルを混同

**原因**:
- 別プロジェクト（anonymous-keiba-ai-jra）のCSVファイルを使用
- `_fixed`テーブル（パース済みデータ）のみが実際に利用可能

### ✅ このドキュメントは実際のDBから直接エクスポート

---

## 📋 JRDB _fixedテーブル一覧

| テーブル名 | カラム数 | 説明 | 主な用途 |
|-----------|---------|------|---------|
| `jrd_kyi_fixed` | **38** | 競馬指数・評価データ | IDM、騎手指数、総合指数 |
| `jrd_cyb_fixed` | **19** | 調教評価データ | 追切指数、仕上指数 |
| `jrd_joa_fixed` | **16** | 場・オッズ分析データ | LS指数、オッズ指数 |
| `jrd_bac_fixed` | **24** | レース基本情報 | 距離、コース、馬場状態 |

**合計**: **97カラム** (JRDB _fixedテーブル群)

---

## 1️⃣ jrd_kyi_fixed（38カラム）- 競馬指数・評価データ

### キー列（レース・馬特定）
| # | カラム名 | 型 | 説明 |
|---|---------|-----|------|
| 1 | `jrdb_race_key8` | varchar(255) | JRDB独自レースキー（8桁） |
| 2 | `race_shikonen` | varchar(255) | レース識別年（例: 260222） |
| 3 | `keibajo_code` | varchar(255) | 競馬場コード（01=札幌...） |
| 4 | `kaisai_kai` | varchar(255) | 開催回数 |
| 5 | `kaisai_nichime` | varchar(255) | 開催日目 |
| 6 | `kaisai_nen_2` | varchar(255) | 開催年（2桁） |
| 7 | `race_bango` | varchar(255) | レース番号 |
| 8 | `umaban` | varchar(255) | 馬番 |
| 9 | `basho_code` | varchar(255) | 場所コード |
| 10 | `year` | varchar(255) | 年 |
| 11 | `kai` | varchar(255) | 回 |
| 12 | `nichi` | varchar(255) | 日 |
| 13 | `race_num` | varchar(255) | レース番号 |
| 14 | `kettou_toroku_bango` | varchar(255) | 血統登録番号 |

### 馬名・基本情報
| # | カラム名 | 型 | 説明 |
|---|---------|-----|------|
| 15 | `bamei` | varchar(255) | 馬名 |

### 🔥 JRDB指数群（最重要）
| # | カラム名 | 型 | 説明 | 重要度 |
|---|---------|-----|------|--------|
| **16** | **`idm`** | varchar(255) | **IDMスピード指数** | ★★★★★ |
| **17** | **`kishu_shisu`** | varchar(255) | **騎手指数** | ★★★★★ |
| **18** | **`sogo_shisu`** | varchar(255) | **総合指数** | ★★★★★ |
| 19 | `kyakushitsu` | varchar(255) | 脚質 | ★★★★ |
| 20 | `kyori_tekisei` | varchar(255) | 距離適性 | ★★★★ |
| 21 | `chokyo_shisu` | varchar(255) | 調教指数 | ★★★ |
| 22 | `kyusha_shisu` | varchar(255) | 厩舎指数 | ★★★ |

### 適性・評価コード
| # | カラム名 | 型 | 説明 |
|---|---------|-----|------|
| 23 | `chokyo_yajirushi_code` | varchar(255) | 調教矢印コード |
| 24 | `omo_tekisei_code` | varchar(255) | 重適性コード |
| 25 | `shiba_tekisei_code` | varchar(255) | 芝適性コード |
| 26 | `da_tekisei_code` | varchar(255) | ダート適性コード |

### 装備・負担
| # | カラム名 | 型 | 説明 |
|---|---------|-----|------|
| 27 | `soho` | varchar(255) | 装鞍 |
| 28 | `blinker` | varchar(255) | ブリンカー |
| 29 | `futan_juryo` | varchar(255) | 負担重量 |

### レース展開指数
| # | カラム名 | 型 | 説明 |
|---|---------|-----|------|
| 30 | `ten_shisu` | varchar(255) | テン指数 |
| 31 | `pace_shisu` | varchar(255) | ペース指数 |
| 32 | `agari_shisu` | varchar(255) | 上がり指数 |
| 33 | `ichi_shisu` | varchar(255) | 位置指数 |
| 34 | `kyori_tekisei_2` | varchar(255) | 距離適性2 |

### 馬・人物コード
| # | カラム名 | 型 | 説明 |
|---|---------|-----|------|
| 35 | `seibetsu_code` | varchar(255) | 性別コード |
| 36 | `kishu_code` | varchar(255) | 騎手コード |
| 37 | `chokyoshi_code` | varchar(255) | 調教師コード |
| 38 | `kyusha_rank` | varchar(255) | 厩舎ランク |

---

## 2️⃣ jrd_cyb_fixed（19カラム）- 調教評価データ

### キー列
| # | カラム名 | 型 | 説明 |
|---|---------|-----|------|
| 1-8 | (jrd_kyi_fixedと同じ) | varchar(255) | レース・馬特定キー |
| 9-13 | (jrd_kyi_fixedと同じ) | varchar(255) | 追加識別子 |

### 調教評価指数
| # | カラム名 | 型 | 説明 | 重要度 |
|---|---------|-----|------|--------|
| **14** | **`chokyo_type`** | varchar(255) | **調教タイプ** | ★★★★ |
| **15** | **`oikiri_shisu`** | varchar(255) | **追切指数** | ★★★★★ |
| **16** | **`shiage_shisu`** | varchar(255) | **仕上指数** | ★★★★★ |
| 17 | `chokyo_ryo_hyoka` | varchar(255) | 調教量評価 | ★★★ |
| 18 | `shiage_shisu_henka` | varchar(255) | 仕上指数変化 | ★★★ |
| 19 | `chokyo_hyoka` | varchar(255) | 調教評価 | ★★★ |

---

## 3️⃣ jrd_joa_fixed（16カラム）- 場・オッズ分析データ

### キー列
| # | カラム名 | 型 | 説明 |
|---|---------|-----|------|
| 1-8 | (jrd_kyi_fixedと同じ) | varchar(255) | レース・馬特定キー |
| 9-13 | (jrd_kyi_fixedと同じ) | varchar(255) | 追加識別子 |

### 場・オッズ指数
| # | カラム名 | 型 | 説明 | 重要度 |
|---|---------|-----|------|--------|
| **14** | **`ls_shisu`** | varchar(255) | **LS指数（芝・ダート適性）** | ★★★★ |
| **15** | **`ls_hyoka`** | varchar(255) | **LS評価** | ★★★ |
| **16** | **`odds_shisu`** | varchar(255) | **オッズ指数** | ★★★★ |

---

## 4️⃣ jrd_bac_fixed（24カラム）- レース基本情報

### キー列（レース特定）
| # | カラム名 | 型 | 説明 |
|---|---------|-----|------|
| 1 | `jrdb_race_key8` | varchar(255) | JRDB独自レースキー |
| 2 | `race_shikonen` | varchar(255) | レース識別年 |
| 3 | `keibajo_code` | varchar(255) | 競馬場コード |
| 4 | `kaisai_kai` | varchar(255) | 開催回数 |
| 5 | `kaisai_nichime` | varchar(255) | 開催日目 |
| 6 | `kaisai_nen_2` | varchar(255) | 開催年（2桁） |
| 7 | `race_bango` | varchar(255) | レース番号 |
| 8 | `basho_code` | varchar(255) | 場所コード |
| 9 | `year` | varchar(255) | 年 |
| 10 | `kai` | varchar(255) | 回 |
| 11 | `nichi` | varchar(255) | 日 |
| 12 | `race_num` | varchar(255) | レース番号 |

### レース条件
| # | カラム名 | 型 | 説明 |
|---|---------|-----|------|
| 13 | `nengappi` | varchar(255) | 年月日 |
| 14 | `kyori` | varchar(255) | 距離 |
| 15 | `shiba_da_shogai_code` | varchar(255) | 芝・ダート・障害コード |
| 16 | `migi_hidari` | varchar(255) | 右・左回り |
| 17 | `uchi_soto` | varchar(255) | 内・外回り |
| 18 | `shubetsu` | varchar(255) | 種別 |
| 19 | `jouken` | varchar(255) | 条件 |
| 20 | `juryo_shubetsu_code` | varchar(255) | 重量種別コード |
| 21 | `grade` | varchar(255) | グレード |
| 22 | `tosu` | varchar(255) | 頭数 |
| 23 | `course` | varchar(255) | コース |
| 24 | `kaisai_kubun` | varchar(255) | 開催区分 |

---

## 🔗 主キー・結合条件

### 馬単位データ（jrd_kyi_fixed, jrd_cyb_fixed, jrd_joa_fixed）
```sql
PRIMARY KEY (keibajo_code, race_shikonen, kaisai_kai, kaisai_nichime, race_bango, umaban)
```

### レース単位データ（jrd_bac_fixed）
```sql
PRIMARY KEY (keibajo_code, race_shikonen, kaisai_kai, kaisai_nichime, race_bango)
```

### 結合例（馬データ + レース情報）
```sql
SELECT 
    kyi.*,
    cyb.oikiri_shisu,
    cyb.shiage_shisu,
    joa.ls_shisu,
    joa.odds_shisu,
    bac.kyori,
    bac.shiba_da_shogai_code
FROM jrd_kyi_fixed kyi
LEFT JOIN jrd_cyb_fixed cyb 
    ON kyi.keibajo_code = cyb.keibajo_code
    AND kyi.race_shikonen = cyb.race_shikonen
    AND kyi.kaisai_kai = cyb.kaisai_kai
    AND kyi.kaisai_nichime = cyb.kaisai_nichime
    AND kyi.race_bango = cyb.race_bango
    AND kyi.umaban = cyb.umaban
LEFT JOIN jrd_joa_fixed joa
    ON kyi.keibajo_code = joa.keibajo_code
    AND kyi.race_shikonen = joa.race_shikonen
    AND kyi.kaisai_kai = joa.kaisai_kai
    AND kyi.kaisai_nichime = joa.kaisai_nichime
    AND kyi.race_bango = joa.race_bango
    AND kyi.umaban = joa.umaban
LEFT JOIN jrd_bac_fixed bac
    ON kyi.keibajo_code = bac.keibajo_code
    AND kyi.race_shikonen = bac.race_shikonen
    AND kyi.kaisai_kai = bac.kaisai_kai
    AND kyi.kaisai_nichime = bac.kaisai_nichime
    AND kyi.race_bango = bac.race_bango;
```

---

## ⚠️ 注意事項

### 1. データ型は全て`character varying(255)`
- **数値計算時は必ずCASTが必要**:
  ```sql
  CAST(idm AS INTEGER)
  CAST(kishu_shisu AS NUMERIC)
  ```

### 2. NULL値の処理
- LEFT JOINで結合するため、必ずNULL処理:
  ```sql
  COALESCE(CAST(cyb.oikiri_shisu AS INTEGER), 0)
  ```

### 3. _fixedテーブルのみ使用可能
- `jrd_kyi`（元データ、132カラム）は使用不可
- `jrd_kyi_fixed`（パース済み、38カラム）のみ利用可能

### 4. 日付フォーマット
- `race_shikonen`: `YYMMDD`形式（例: 260222 = 2026年2月22日）
- `nengappi`: `YYYYMMDD`形式（例: 20260222）

---

## 📊 データ統計（参考）

| テーブル | 行数（概算） | 期間 |
|---------|-------------|------|
| jrd_kyi_fixed | 491,176行 | 2016-2025 |
| jrd_cyb_fixed | 491,194行 | 2016-2025 |
| jrd_joa_fixed | 491,194行 | 2016-2025 |
| jrd_bac_fixed | 35,173行 | 2016-11-01～2026-12-12 |

---

## 🚀 次のステップ

1. **full_factor_loader.py修正**
   - 存在しないカラム名（`joho_shisu`など）を実際のカラム名（`sogo_shisu`など）に置換
   - LEFT JOINを正しい主キーで実装
   - NULL値処理追加

2. **ファクター選定**
   - 97カラムの中から予測に有効なファクターを選定
   - JRA-VANデータ（209カラム）との統合

3. **データ検証**
   - 各テーブルの行数確認
   - 欠損値チェック
   - データ型変換テスト

---

**作成者**: GenSpark AI Developer  
**プロジェクト**: jraroi1219  
**最終更新**: 2026-04-07
