# JRDB データ形式: 116列完全版

**証拠ファイル**: `docs/PHASE7A_COMBINED_497_UNIQUE_COLNAME.csv`  
**調査日**: 2026-04-01（修正版）  
**総列数**: 116列（全て使用）

---

## 1. テーブル別カラム一覧

### 1.1 jrd_kyi (競馬指数) - 65列【完全版】

**説明**: JRDBの独自指数、評価、適性、体型など（メインデータ）

#### 主な指数系カラム（20列）

| # | カラム名 | 日本語名 | 説明 |
|---|---------|---------|------|
| 1 | `idm` | IDM（スピード指数） | JRDB独自の総合スピード指数 |
| 2 | `joho_shisu` | 情報指数 | JRDB独自の情報指数（パドック・調教情報等を総合） |
| 4 | `kishu_shisu` | 騎手指数 | JRDBの騎手能力指数（騎手の近走成績を数値化） |
| 6 | `agari_shisu` | 上がり指数 | 終盤（上がり3F）の速さ指数 |
| 8 | `chokyo_shisu` | 調教指数 | 調教内容の評価指数 |
| **16** | **`gekiso_shisu`** | **激走指数** | **JRDB 激走指数（好走期待度）** |
| 24 | `ichi_shisu` | 位置取り指数 | JRDB 位置取り指数 |
| 43 | `kyusha_shisu` | 厩舎指数 | 厩舎の状態評価指数 |
| 47 | `manken_shisu` | 万券指数 | JRDB 万券指数 |
| 48 | `pace_shisu` | ペース指数 | ペース評価指数 |
| 52 | `sogo_shisu` | 総合指数 | 総合的な能力評価指数 |
| 58 | `ten_shisu` | テン指数 | 序盤（最初の3F）の速さ指数 |
| 61 | `uma_start_shisu` | 馬スタート指数 | JRDB 馬スタート指数 |

#### 順位・差・内外系カラム（15列）

| # | カラム名 | 日本語名 | 説明 |
|---|---------|---------|------|
| 7 | `agari_shisu_juni` | 上がり指数順位 | 上がり指数の順位 |
| 11 | `dochu_juni` | 道中順位 | 道中の順位 |
| 12 | `dochu_sa` | 道中差 | 道中の着差 |
| 13 | `dochu_uchisoto` | 道中内外 | 道中の内外位置 |
| 15 | `gekiso_juni` | 激走順位 | 激走指数の順位 |
| 18 | `goal_juni` | ゴール順位 | ゴール時の順位 |
| 19 | `goal_sa` | ゴール差 | ゴール時の着差 |
| 20 | `goal_uchisoto` | ゴール内外 | ゴール時の内外位置 |
| 25 | `ichi_shisu_juni` | 位置取り指数順位 | 位置取り指数の順位 |
| 36 | `kohan_3f_juni` | 後半3F順位 | 後半3Fの順位 |
| 37 | `kohan_3f_sa` | 後半3F差 | 後半3Fの着差 |
| 38 | `kohan_3f_uchisoto` | 後半3F内外 | 後半3Fの内外位置 |
| 45 | `ls_shisu_juni` | LS指数順位 | LS指数の順位 |
| 49 | `pace_shisu_juni` | ペース指数順位 | ペース指数の順位 |
| 59 | `ten_shisu_juni` | テン指数順位 | テン指数の順位 |

#### コード・評価系カラム（18列）

| # | カラム名 | 日本語名 | 説明 |
|---|---------|---------|------|
| 9 | `chokyo_yajirushi_code` | 調教矢印コード | 調教矢印の識別コード |
| 10 | `class_code` | クラスコード | クラスの識別コード |
| 17 | `gekiso_type` | 激走タイプ | 激走タイプ分類 |
| 21 | `hizume_code` | 蹄コード | 蹄の識別コード |
| 22 | `hobokusaki` | 放牧先 | 放牧先情報 |
| 23 | `hobokusaki_rank` | 放牧先ランク | 放牧先のランク |
| 26 | `joshodo_code` | 上昇度コード | 上昇度の識別コード |
| 32 | `kishu_code` | 騎手コード | 騎手の固有コード（JRA公式） |
| 39 | `kyakushitsu_code` | 脚質コード | 脚質の識別コード |
| 40 | `kyori_tekisei_code` | 距離適性コード | 距離適性の識別コード |
| 41 | `kyusha_hyoka_code` | 厩舎評価コード | 厩舎評価の識別コード |
| 42 | `kyusha_rank` | 厩舎ランク | 厩舎のランク |
| 44 | `kyuyo_riyu_bunrui_code` | 休養理由分類コード | 休養理由分類の識別コード |
| 46 | `manken_shirushi` | 万券印 | 万券印（予想印） |
| 50 | `pace_yoso` | ペース予想 | ペース予想 |
| 57 | `tekisei_code_omo` | 適性コード（重） | 重馬場適性コード |
| 65 | `yuso_kubun` | 輸送区分 | 輸送の有無・方法 |

#### 賞金・オッズ・人気系カラム（7列）

| # | カラム名 | 日本語名 | 説明 |
|---|---------|---------|------|
| 27 | `kakutoku_shokin_ruikei` | 獲得賞金累計 | 獲得賞金累計 |
| 28 | `kijun_ninkijun_fukusho` | 基準人気順（複勝） | 複勝の基準人気順 |
| 29 | `kijun_ninkijun_tansho` | 基準人気順（単勝） | 単勝の基準人気順 |
| 30 | `kijun_odds_fukusho` | 基準オッズ（複勝） | 複勝の基準オッズ |
| 31 | `kijun_odds_tansho` | 基準オッズ（単勝） | 単勝の基準オッズ |
| 51 | `shutoku_shokin_ruikei` | 取得賞金累計 | 取得賞金累計 |

#### 騎手・厩舎期待値系カラム（3列）

| # | カラム名 | 日本語名 | 説明 |
|---|---------|---------|------|
| 33 | `kishu_kitai_rentai_ritsu` | 騎手期待連対率 | 騎手の期待連対率 |
| 34 | `kishu_kitai_sanchakunai_ritsu` | 騎手期待3着内率 | 騎手の期待3着内率 |
| 35 | `kishu_kitai_tansho_ritsu` | 騎手期待単勝率 | 騎手の期待単勝率 |

#### 馬体型・特記事項（7列）

| # | カラム名 | 日本語名 | 説明 |
|---|---------|---------|------|
| 53 | `taikei` | 体型 | 馬の体型評価 |
| 54 | `taikei_sogo_1` | 体型総合1 | 体型総合評価1 |
| 55 | `taikei_sogo_2` | 体型総合2 | 体型総合評価2 |
| 56 | `taikei_sogo_3` | 体型総合3 | 体型総合評価3 |
| 62 | `uma_tokki_1` | 馬特記1 | 馬の特記事項1 |
| 63 | `uma_tokki_2` | 馬特記2 | 馬の特記事項2 |
| 64 | `uma_tokki_3` | 馬特記3 | 馬の特記事項3 |

#### その他（5列）

| # | カラム名 | 日本語名 | 説明 |
|---|---------|---------|------|
| 3 | `ketto_toroku_bango` | 血統登録番号 | 馬の一意識別番号（JRA公式） |
| 5 | `race_shikonen` | レース施行年 | 競走が施行された年コード（YYYY形式） |
| 14 | `futan_juryo` | 負担重量 | 斤量（騎手+装具の合計） |
| 60 | `uma_deokure_ritsu` | 馬出遅れ率 | 馬の出遅れ率 |

---

### 1.2 jrd_cyb (調教分析) - 18列

**説明**: 調教コメント、評価、コース情報

| # | カラム名 | 日本語名 | 説明 |
|---|---------|---------|------|
| 1 | `chokyo_comment` | 調教コメント | 調教師によるコメント |
| 2 | `chokyo_corse_dirt` | 調教コースダートタイム | ダートコースでの調教タイム |
| 3 | `chokyo_corse_hanro` | 調教コース坂路タイム | 坂路コースでの調教タイム |
| 4 | `chokyo_corse_polytrack` | 調教コースポリトラックタイム | ポリトラックコースの調教タイム |
| 5 | `chokyo_corse_pool` | 調教コースプール | プールでの調教記録 |
| 6 | `chokyo_corse_shiba` | 調教コース芝タイム | 芝コースでの調教タイム |
| 7 | `chokyo_course_shubetsu` | 調教コース種別 | 調教コースの種別 |
| 8 | `chokyo_hyoka_1` | 調教評価1 | 調教評価1 |
| 9 | `chokyo_hyoka_2` | 調教評価2 | 調教評価2 |
| 10 | `chokyo_hyoka_3` | 調教評価3 | 調教評価3 |
| 11 | `chokyo_juten` | 調教重点 | 調教の重点項目 |
| 12 | `chokyo_type` | 調教タイプ | 調教のタイプ |
| 13 | `chokyoshi_code` | 調教師コード | 調教師の固有コード |
| 14 | `chokyoshi_mei` | 調教師名 | 調教師の氏名 |
| 15 | `choshubetsu` | 調種別 | 調教の種別 |
| 16 | `oikiri_shisu` | 追切指数 | 追切の評価指数 |
| 17 | `sakusha_hyoka_f_h` | 作者評価F/H | 作者による評価（F/H） |
| 18 | `shiage_shisu` | 仕上指数 | 仕上がり状態の評価指数 |

---

### 1.3 jrd_sed (レース詳細) - 14列

**説明**: 馬場差、ペース、コース取り、振り

| # | カラム名 | 日本語名 | 説明 |
|---|---------|---------|------|
| 1 | `babasa` | 馬場差 | 馬場の有利不利 |
| 2 | `bataiju_zogen` | 馬体重増減 | 馬体重の増減 |
| 3 | `corner_tsuka_juni_1` | 1コーナー通過順位 | 1コーナー通過順位 |
| 4 | `corner_tsuka_juni_2` | 2コーナー通過順位 | 2コーナー通過順位 |
| 5 | `corner_tsuka_juni_3` | 3コーナー通過順位 | 3コーナー通過順位 |
| 6 | `corner_tsuka_juni_4` | 4コーナー通過順位 | 4コーナー通過順位 |
| 7 | `furi` | 振り | スタート時の振り |
| 8 | `id` | ID | レコードID |
| 9 | `pace` | ペース | レースのペース |
| 10 | `pace_shisu` | ペース指数 | ペース評価指数 |
| 11 | `race_comments` | レースコメント | レースに関するコメント |
| 12 | `race_pace` | レースペース | レースのペース評価 |
| 13 | `race_pace_runner` | レースペース走者 | ペースを作った馬 |
| 14 | `race_pen_type` | レースペンタイプ | ペースのタイプ分類 |

---

### 1.4 jrd_joa (騎手・厩舎評価) - 10列

**説明**: LS指数、騎手・厩舎の評価指標

| # | カラム名 | 日本語名 | 説明 |
|---|---------|---------|------|
| 1 | `cid` | CID | 馬の識別ID |
| 2 | `em` | EM | EM評価値 |
| 3 | `jockey_banushi_nijumaru_tansho_kaishuritsu` | 騎手×馬主◎◎単勝回収率 | 騎手と馬主の◎◎単勝回収率 |
| 4 | `kishu_bb_shirushi` | 騎手◎印 | 騎手の◎印 |
| 5 | `kyusha_bb_shirushi` | 厩舎◎印 | 厩舎の◎印 |
| 6 | `kyusha_bb_nijumaru_tansho_kaishuritsu` | 厩舎◎◎単勝回収率 | 厩舎の◎◎単勝回収率 |
| 7 | `ls_hyoka` | LS評価 | LS評価値 |
| 8 | `ls_shisu` | LS指数 | 騎手・厩舎の総合評価指数 |
| 9 | `ten_shisu` | テン指数 | 序盤の速さ指数 |
| 10 | `uma_gucchi` | 馬ぐっち | 馬の評価（ぐっち） |

---

### 1.5 jrd_bac (レース基本情報) - 9列

**説明**: 賞金、競走条件、馬券発売フラグ（レース単位、umaban なし）

| # | カラム名 | 日本語名 | 説明 |
|---|---------|---------|------|
| 1 | `baken_hatsubai_flag` | 馬券発売フラグ | 各券種の発売有無 |
| 2 | `baba_sa_saishujikoku` | 馬場差最終時刻 | 馬場差の最終時刻 |
| 3 | `fukashokin` | 付加賞金 | 付加賞金額 |
| 4 | `honshokin` | 本賞金 | 1着賞金 |
| 5 | `kaisai_nen_gappi` | 開催年月日 | レース開催日（YYYYMMDD） |
| 6 | `kyoso_joken` | 競走条件 | レースの条件 |
| 7 | `race_code_zenhan` | レースコード前半 | レースコードの前半部分 |
| 8 | `race_comment` | レースコメント | レースに関するコメント |
| 9 | `track_baba_sa` | トラック馬場差 | トラック別の馬場差 |

---

## 2. 主キー構造

### 馬単位テーブル (jrd_kyi, jrd_cyb, jrd_sed, jrd_joa)
```python
PRIMARY KEY (
    keibajo_code,         # 競馬場コード (2桁)
    race_shikonen,        # レース施行日 (YYMMDD形式)
    kaisai_kai,           # 開催回 (2桁)
    kaisai_nichime,       # 開催日目 (2桁)
    race_bango,           # レース番号 (2桁)
    umaban                # 馬番 (2桁)
)
```

### レース単位テーブル (jrd_bac) - umaban なし
```python
PRIMARY KEY (
    keibajo_code,
    race_shikonen,
    kaisai_kai,
    kaisai_nichime,
    race_bango
)
```

---

## 3. データ型

**全カラム共通**: `character varying` (文字列型)

---

## 4. JOIN 条件（JRA-VANとの結合）

### 日付変換ロジック

**JRA-VAN側**:
- `kaisai_nen`: '2024' (YYYY形式)
- `kaisai_tsukihi`: '0307' (MMDD形式)

**JRDB側**:
- `race_shikonen`: '240307' (YYMMDD形式)

**変換SQL**:
```sql
(SUBSTRING(se.kaisai_nen, 3, 2) || se.kaisai_tsukihi) = kyi.race_shikonen
-- '2024' → '24' + '0307' = '240307'
```

### 馬単位テーブル JOIN
```sql
LEFT JOIN jrd_kyi AS kyi 
    ON se.keibajo_code = kyi.keibajo_code 
    AND (SUBSTRING(se.kaisai_nen, 3, 2) || se.kaisai_tsukihi) = kyi.race_shikonen 
    AND COALESCE(se.kaisai_kai, '00') = kyi.kaisai_kai 
    AND COALESCE(se.kaisai_nichime, '00') = kyi.kaisai_nichime 
    AND se.race_bango = kyi.race_bango 
    AND se.umaban = kyi.umaban
```

### レース単位テーブル JOIN (jrd_bac のみ)
```sql
LEFT JOIN jrd_bac AS bac 
    ON se.keibajo_code = bac.keibajo_code 
    AND (SUBSTRING(se.kaisai_nen, 3, 2) || se.kaisai_tsukihi) = bac.race_shikonen 
    AND COALESCE(se.kaisai_kai, '00') = bac.kaisai_kai 
    AND COALESCE(se.kaisai_nichime, '00') = bac.kaisai_nichime 
    AND se.race_bango = bac.race_bango
```

**注**: `umaban` は含まない（レース単位のため）

---

## 5. データ件数

**証拠**: PostgreSQL 検証結果

| テーブル | 行数 | 期間 |
|---------|------|------|
| `jrd_kyi` | 491,176 | 161101～262612 |
| `jrd_cyb` | 491,194 | 161101～262612 |
| `jrd_sed` | 491,017 | 161101～262612 |
| `jrd_joa` | 491,194 | 161101～262612 |
| `jrd_bac` | 35,173 | 161101～262612 |

**期間**: 2016年11月1日～2026年12月12日

---

## 6. 実装コード

**証拠ファイル**: `phase7/scripts/phase7b_factor_roi/create_merged_dataset_334cols.py`

```python
jrdb_tables = {
    'jrd_kyi': 'kyi',  # 65列
    'jrd_cyb': 'cyb',  # 18列
    'jrd_sed': 'sed',  # 14列
    'jrd_joa': 'joa',  # 10列
    'jrd_bac': 'bac'   # 9列 (レース単位)
}
```

---

## 7. JRDB データの特徴

### 7.1 独自指数（重要）
- **IDM**: JRDB独自の総合スピード指数（最重要）
- **激走指数（gekiso_shisu）**: 好走期待度を示す重要指数
- **各種指数**: 騎手、調教師、厩舎、展開など13種類以上

### 7.2 詳細な適性評価
- 距離適性、馬場適性、コース適性を数値化
- 脚質コード、上昇度コード

### 7.3 調教分析
- 調教コメント、評価ランク
- 追切指数、仕上指数

### 7.4 LS指数
- 騎手・厩舎の総合評価指数
- 各種回収率データ（騎手×馬主、厩舎◎◎単勝等）

### 7.5 レース詳細分析
- ペース、馬場差、振り
- コーナー通過順位、ペースタイプ

---

## 8. 使用上の注意

### 8.1 NULL値の扱い
- LEFT JOIN のため、JRDB データが存在しないレースでは全カラムが NULL
- `COALESCE` で NULL を適切に処理

### 8.2 日付変換の必須性
- JRA-VAN と JRDB で日付形式が異なる
- 必ず `SUBSTRING` + `||` で変換すること

### 8.3 文字列型の扱い
- 全カラムが `character varying` 型
- 数値として使用する場合は CAST が必要

---

**作成日**: 2026-04-01（修正版）  
**証拠元**: サンドボックス実ファイル検証  
**総カラム数**: 116列（全て使用）  
**修正内容**: jrd_kyi 65列を完全記載、激走指数を明記
