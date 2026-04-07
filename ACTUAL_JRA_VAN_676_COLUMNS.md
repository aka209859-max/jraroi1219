# 📊 JRA-VAN実データベーススキーマ（676カラム）

**作成日**: 2026-04-07  
**データソース**: pckeiba データベース（PostgreSQL 16.11）  
**スキーマファイル**: ACTUAL_DB_SCHEMA_2294_COLUMNS.csv（pgAdmin4エクスポート）

---

## ⚠️ 重要な発見

### ❌ 以前のドキュメント（JRA_VAN_209_COLUMNS.md）は**大幅に不足**していました

**問題点**:
- 記載カラム数: 209カラム
- **実際のカラム数: 676カラム（主要13テーブル）**
- **全38テーブル: 1,649カラム**
- **差分: +467カラム不足**（主要テーブルのみ）

**原因**:
- 別プロジェクト（anonymous-keiba-ai-jra）のCSVファイルを使用
- Phase 7-Aで使用された統合データセット（2016-2025）のカラム抽出が不完全

### ✅ このドキュメントは実際のDBから直接エクスポート

---

## 📋 JRA-VAN 主要テーブル一覧

| テーブル名 | カラム数 | 説明 | 主な用途 |
|-----------|---------|------|---------|
| `jvd_se` | **70** | レース成績データ | 着順、タイム、オッズ |
| `jvd_ra` | **62** | レース情報 | レース条件、賞金、馬場状態 |
| `jvd_ck` | **106** | 過去成績（競走馬） | 競走成績サマリー |
| `jvd_h1` | **43** | 払戻し・票数 | 単勝・複勝・馬連等の票数 |
| `jvd_hr` | **158** | 払戻し金額詳細 | 全券種の払戻し金額 |
| `jvd_um` | **89** | 競走馬基本情報 | 血統、生産者、馬主 |
| `jvd_sk` | **26** | 血統情報 | 父、母、母父の成績 |
| `jvd_dm` | **28** | 調教師情報 | 調教師成績、所属 |
| `jvd_wc` | **29** | 馬体重データ | 馬体重、増減 |
| `jvd_h6` | **16** | 6ハロン指数 | 速度指数関連 |
| `jvd_hc` | **14** | 馬場・天候 | 詳細な馬場状態 |
| `jvd_jg` | **14** | 騎手情報 | 騎手成績、所属 |
| `jvd_ch` | **21** | チャンピオンデータ | GI・重賞成績 |

**主要13テーブル合計**: **676カラム**

---

## 🌐 全テーブル一覧（38テーブル、1,649カラム）

| テーブル名 | カラム数 | 説明 |
|-----------|---------|------|
| jvd_tk | 336 | 拓殖データ（最大） |
| jvd_wf | 266 | 払戻し詳細フォーマット |
| jvd_hr | 158 | 払戻し金額詳細 |
| jvd_ck | 106 | 過去成績（競走馬） |
| jvd_um | 89 | 競走馬基本情報 |
| jvd_se | 70 | レース成績データ |
| jvd_ra | 62 | レース情報 |
| jvd_h1 | 43 | 払戻し・票数 |
| jvd_ks | 30 | 騎手成績 |
| jvd_wc | 29 | 馬体重データ |
| jvd_dm | 28 | 調教師情報 |
| jvd_tm | 28 | 調教師マスタ |
| jvd_wh | 28 | 馬体重履歴 |
| jvd_sk | 26 | 血統情報 |
| jvd_rc | 24 | レースカード |
| jvd_o1 | 22 | オッズ1 |
| jvd_ch | 21 | チャンピオンデータ |
| jvd_jc | 20 | 騎手カレンダー |
| jvd_hn | 19 | 馬名変更履歴 |
| jvd_h6 | 16 | 6ハロン指数 |
| jvd_we | 16 | 天候・馬場詳細 |
| jvd_cc | 15 | コース変更 |
| jvd_o2 | 15 | オッズ2 |
| jvd_o3 | 15 | オッズ3 |
| jvd_o4 | 15 | オッズ4 |
| jvd_o5 | 15 | オッズ5 |
| jvd_o6 | 15 | オッズ6 |
| jvd_hc | 14 | 馬場・天候 |
| jvd_hs | 14 | 速度指数 |
| jvd_jg | 14 | 騎手情報 |
| jvd_av | 13 | 平均値データ |
| jvd_tc | 12 | 調教コース |
| jvd_ys | 12 | 予想指数 |
| jvd_bn | 11 | 馬番変更 |
| jvd_br | 11 | 繁殖馬 |
| jvd_cs | 8 | コース情報 |
| jvd_bt | 7 | バッテリー |
| jvd_hy | 6 | 履歴 |

**全テーブル合計**: **1,649カラム**

---

## 1️⃣ jvd_se（70カラム）- レース成績データ

### キー列（レース・馬特定）
| # | カラム名 | 型 | 説明 |
|---|---------|-----|------|
| 1 | `record_id` | varchar(2) | レコードID |
| 2 | `data_kubun` | varchar(1) | データ区分 |
| 3 | `data_sakusei_nengappi` | varchar(8) | データ作成年月日 |
| 4 | `kaisai_nen` | varchar(4) | 開催年 |
| 5 | `kaisai_tsukihi` | varchar(4) | 開催月日 |
| 6 | `keibajo_code` | varchar(2) | 競馬場コード |
| 7 | `kaisai_kai` | varchar(2) | 開催回 |
| 8 | `kaisai_nichime` | varchar(2) | 開催日目 |
| 9 | `race_bango` | varchar(2) | レース番号 |
| 10 | `wakuban` | varchar(1) | 枠番 |
| 11 | `umaban` | varchar(2) | 馬番 |
| 12 | `ketto_toroku_bango` | varchar(10) | 血統登録番号 |

### 馬・騎手・調教師情報
| # | カラム名 | 説明 |
|---|---------|------|
| 13 | `bamei` | 馬名 |
| 14 | `umakigo_code` | 馬記号コード |
| 15 | `seibetsu_code` | 性別コード |
| 16 | `hinshu_code` | 品種コード |
| 17 | `moshoku_code` | 毛色コード |
| 18 | `barei` | 馬齢 |
| 19 | `tozai_shozoku_code` | 東西所属コード |
| 20-23 | `chokyoshi_code`, `chokyoshimei_ryakusho`, `banushi_code`, `banushimei` | 調教師・馬主情報 |
| 30-35 | `kishu_code`, `kishu_code_henkomae`, `kishumei_ryakusho` 等 | 騎手情報 |

### レース結果
| # | カラム名 | 説明 | 重要度 |
|---|---------|------|--------|
| **40** | **`nyusen_juni`** | **入線順位** | ★★★★★ |
| **41** | **`kakutei_chakujun`** | **確定着順** | ★★★★★ |
| 42 | `dochaku_kubun` | 同着区分 | ★★★ |
| 43 | `dochaku_tosu` | 同着頭数 | ★★★ |
| 44 | `soha_time` | 走破タイム | ★★★★★ |
| 45-47 | `chakusa_code_1/2/3` | 着差コード | ★★★★ |
| 48-51 | `corner_1/2/3/4` | コーナー通過順 | ★★★★ |
| 52-53 | `tansho_odds`, `tansho_ninkijun` | 単勝オッズ・人気順 | ★★★★★ |
| 54-55 | `kakutoku_honshokin`, `kakutoku_fukashokin` | 獲得賞金 | ★★★ |

### 速度・ペース指標
| # | カラム名 | 説明 | 重要度 |
|---|---------|------|--------|
| 58-59 | `kohan_4f`, `kohan_3f` | 後半4F・3F | ★★★★ |
| 60-62 | `aiteuma_joho_1/2/3` | 相手馬情報 | ★★★ |
| 63 | `time_sa` | タイム差 | ★★★ |
| 66-69 | `yoso_soha_time`, `yoso_gosa_plus/minus`, `yoso_juni` | 予想走破タイム・誤差・順位 | ★★★ |
| 70 | `kyakushitsu_hantei` | 脚質判定 | ★★★★ |

（残りのテーブル詳細は省略 - カラム名リストは `ACTUAL_DB_SCHEMA_2294_COLUMNS.csv` を参照）

---

## 2️⃣ jvd_ra（62カラム）- レース情報

### レース基本情報
| # | カラム名 | 説明 | 重要度 |
|---|---------|------|--------|
| 11-12 | `tokubetsu_kyoso_bango`, `kyosomei_hondai` | 特別競走番号、競走名 | ★★★★ |
| 21-27 | `kyosomei_kubun`, `grade_code`, `kyoso_shubetsu_code`, `juryo_shubetsu_code` 等 | レース種別情報 | ★★★★★ |
| 34-39 | `kyori`, `track_code`, `course_kubun` 等 | 距離・コース情報 | ★★★★★ |

### 馬場・天候
| # | カラム名 | 説明 | 重要度 |
|---|---------|------|--------|
| 49-51 | `tenko_code`, `babajotai_code_shiba`, `babajotai_code_dirt` | 天候・馬場状態 | ★★★★★ |

### ラップタイム
| # | カラム名 | 説明 | 重要度 |
|---|---------|------|--------|
| 52 | `lap_time` | ラップタイム（75文字） | ★★★★ |
| 54-57 | `zenhan_3f/4f`, `kohan_3f/4f` | 前半・後半ラップ | ★★★★ |
| 58-61 | `corner_tsuka_juni_1/2/3/4` | コーナー通過順位（72文字×4） | ★★★ |

---

## 3️⃣ jvd_ck（106カラム）- 過去成績（競走馬）

### 賞金情報
| # | カラム名 | 説明 |
|---|---------|------|
| 12-17 | `heichi_honshokin_ruikei`, `shogai_honshokin_ruikei` 等 | 平地・障害本賞金累計 |

### 成績サマリー（18文字形式）
**形式**: `1着回数:2着回数:3着回数` の固定長文字列

| # | カラム名 | 説明 |
|---|---------|------|
| 18 | `sogo` | 総合成績 |
| 19 | `chuo_gokei` | 中央合計 |
| 20-26 | `shiba_choku`, `shiba_migi`, `shiba_hidari`, `dirt_choku` 等 | コース別成績 |
| 27-34 | `shiba_ryo`, `shiba_yayaomo`, `shiba_omo`, `shiba_furyo` 等 | 馬場状態別成績 |
| 39-56 | `shiba_1200_ika`, `shiba_1201_1400` 等 | 距離別成績（芝・ダート） |
| 57-86 | `shiba_sapporo`, `shiba_hakodate` 等 | 競馬場別成績（芝・ダート・障害） |

### 関係者成績情報
| # | カラム名 | 説明 |
|---|---------|------|
| 87 | `kyakushitsu_keiko` | 脚質傾向 |
| 89-96 | `kishu_code`, `kishumei`, `seiseki_joho_kishu_1/2` | 騎手成績情報（1220文字×2） |
| 93-96 | `chokyoshi_code`, `chokyoshimei`, `seiseki_joho_chokyoshi_1/2` | 調教師成績情報（1220文字×2） |
| 97-101 | `banushi_code`, `banushimei_hojinkaku`, `seiseki_joho_banushi_1/2` | 馬主成績情報（60文字×2） |
| 102-106 | `seisansha_code`, `seisanshamei_hojinkaku`, `seiseki_joho_seisansha_1/2` | 生産者成績情報（60文字×2） |

---

## 4️⃣ jvd_h1（43カラム）- 払戻し・票数

### 票数情報（大規模データ）
| # | カラム名 | データ長 | 説明 |
|---|---------|---------|------|
| 23 | `hyosu_tansho` | 420文字 | 単勝票数 |
| 24 | `hyosu_fukusho` | 420文字 | 複勝票数 |
| 25 | `hyosu_wakuren` | 540文字 | 枠連票数 |
| 26 | `hyosu_umaren` | 2754文字 | 馬連票数 |
| 27 | `hyosu_wide` | 2754文字 | ワイド票数 |
| 28 | `hyosu_umatan` | 5508文字 | 馬単票数 |
| 29 | `hyosu_sanrenpuku` | 16320文字 | 三連複票数 |

---

## 5️⃣ jvd_hr（158カラム）- 払戻し金額詳細

全券種の払戻し金額を格納する超大規模テーブル。
- 単勝: 3組（馬番・払戻金・人気）
- 複勝: 5組
- 枠連: 3組
- 馬連: 3組
- ワイド: 7組
- 馬単: 6組
- 三連複: 3組
- 三連単: 6組

（詳細カラムリストは省略 - 158カラムすべてを記載すると膨大）

---

## 🔗 主キー・結合条件

### レース単位結合
```sql
-- jvd_se + jvd_ra + jvd_ck
SELECT se.*, ra.*, ck.*
FROM jvd_se se
LEFT JOIN jvd_ra ra
    ON se.kaisai_nen = ra.kaisai_nen
    AND se.kaisai_tsukihi = ra.kaisai_tsukihi
    AND se.keibajo_code = ra.keibajo_code
    AND se.kaisai_kai = ra.kaisai_kai
    AND se.kaisai_nichime = ra.kaisai_nichime
    AND se.race_bango = ra.race_bango
LEFT JOIN jvd_ck ck
    ON se.kaisai_nen = ck.kaisai_nen
    AND se.kaisai_tsukihi = ck.kaisai_tsukihi
    AND se.keibajo_code = ck.keibajo_code
    AND se.kaisai_kai = ck.kaisai_kai
    AND se.kaisai_nichime = ck.kaisai_nichime
    AND se.race_bango = ck.race_bango
    AND se.ketto_toroku_bango = ck.ketto_toroku_bango;
```

---

## ⚠️ 注意事項

### 1. データ型は全て`character varying`
- 数値計算時は必ずCASTが必要
- 大規模データカラム（1000文字超）が多数存在

### 2. 固定長文字列フォーマット
- 成績データ: `1着:2着:3着` 形式（18文字）
- 票数データ: 馬番・票数の組み合わせ（最大16320文字）

### 3. NULL値の処理
- LEFT JOINで結合するため、必ずNULL処理が必要

---

## 📊 データ統計（参考）

| テーブル | 行数（概算） | 期間 |
|---------|-------------|------|
| jvd_se | ~460,000行 | 2016-2025 |
| jvd_ra | ~35,000行 | 2016-2025 |
| jvd_ck | ~460,000行 | 2016-2025 |

---

## 🚀 次のステップ

1. **full_factor_loader.py修正**
   - 676カラムに対応したクエリ作成
   - 正しい結合条件の実装

2. **ファクター選定**
   - 773カラム（JRDB 97 + JRA-VAN 676）から有効ファクターを選定

3. **データ検証**
   - 各テーブルの行数確認
   - 欠損値チェック

---

**作成者**: GenSpark AI Developer  
**プロジェクト**: jraroi1219  
**最終更新**: 2026-04-07
