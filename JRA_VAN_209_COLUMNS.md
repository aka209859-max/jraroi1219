# JRA-VAN データ形式: 209列完全版

**証拠ファイル**: `docs/PHASE7A_COMBINED_497_UNIQUE_COLNAME.csv`  
**調査日**: 2026-04-01（修正版）  
**実際の使用列数**: 218列（元データ）→ **209列**（最終使用、race_id等9列を動的生成・除外）

---

## 1. テーブル別カラム一覧

### 1.1 jvd_se (成績テーブル) - 40列【完全版】

**説明**: レース結果の基礎データ（最重要ベーステーブル）

| # | カラム名 | 日本語名 | 説明 |
|---|---------|---------|------|
| 1 | `aiteuma_joho_1` | 相手馬情報1 | 相手馬情報1 |
| 2 | `aiteuma_joho_2` | 相手馬情報2 | 相手馬情報2 |
| 3 | `aiteuma_joho_3` | 相手馬情報3 | 相手馬情報3 |
| 4 | `bamei` | 馬名 | 競走馬の正式名称 |
| 5 | `banushi_code` | 馬主コード | 馬主の固有コード |
| 6 | `banushimei` | 馬主名 | 馬主の氏名または法人名 |
| 7 | `barei` | 馬齢 | 競走時の馬の年齢 |
| 8 | `bataiju` | 馬体重 | 計量時の馬体重 |
| 9 | `blinker_shiyo_kubun` | ブリンカー使用区分 | ブリンカー装着の有無 |
| 10 | `chakusa_code_1` | 着差コード1 | 着差コード1 |
| 11 | `chakusa_code_2` | 着差コード2 | 着差コード2 |
| 12 | `chakusa_code_3` | 着差コード3 | 着差コード3 |
| 13 | `chokyoshi_code` | 調教師コード | 調教師の固有コード（JRA公式） |
| 14 | `chokyoshimei_ryakusho` | 調教師名略称 | 調教師名略称 |
| 15 | `corner_1` | コーナー1 | コーナー通過順1 |
| 16 | `corner_2` | コーナー2 | コーナー通過順2 |
| 17 | `corner_3` | コーナー3 | コーナー通過順3 |
| 18 | `corner_4` | コーナー4 | コーナー通過順4 |
| 19 | `data_kubun` | データ区分 | 速報・確定などのデータ種別 |
| 20 | `kaisai_kai` | 開催回 | 同一競馬場での年内開催回数 |
| 21 | `kaisai_nen` | 開催年 | 競走が行われた年（西暦） |
| 22 | `kaisai_nichime` | 開催日目 | 開催何日目か |
| 23 | `kaisai_tsukihi` | 開催月日 | 競走が行われた月日 |
| 24 | `keibajo_code` | 競馬場コード | 競馬場の識別コード |
| 25 | `kishumei_ryakusho` | 騎手名略称 | 騎手名略称 |
| 26 | `race_bango` | レース番号 | その日のレース番号 |
| 27 | `seibetsu_code` | 性別コード | 馬の性別 |
| 28 | `umaban` | 馬番 | 出走馬の番号 |
| 29 | `wakuban` | 枠番 | 枠組番号 |
| 30 | `zogen_sa` | 増減差 | 前走比の増減量 |
| 31 | `dochaku_kubun` | 土着区分 | 土着区分 |
| 32 | `dochaku_tosu` | 土着頭数 | 土着頭数 |
| 33 | `fukushoku_hyoji` | 服色表示 | 服色表示 |
| 34 | `futan_juryo_henkomae` | 負担重量変更前 | 負担重量変更前 |
| 35 | `ijo_kubun_code` | 異常区分コード | 異常区分コード |
| 36 | `yoso_soha_time` | 予想走破タイム | 予想走破タイム |
| 37 | `zogen_fugo` | 増減符号 | 増減符号 |
| 38 | `umakigo_code` | 馬記号コード | 馬記号コード |
| 39 | `yobi_1` | 予備1 | 予備1 |
| 40 | `yobi_2` | 予備2 | 予備2 |

---

### 1.2 jvd_ra (レース基本情報) - 31列【完全版】

**説明**: 馬場状態、天候、距離などのレース条件

| # | カラム名 | 日本語名 | 説明 |
|---|---------|---------|------|
| 1 | `babajotai_code_dirt` | 馬場状態コードダート | 馬場状態コードダート |
| 2 | `babajotai_code_shiba` | 馬場状態コード芝 | 馬場状態コード芝 |
| 3 | `corner_tsuka_juni_1` | コーナー通過順位1 | コーナー通過順位1 |
| 4 | `corner_tsuka_juni_2` | コーナー通過順位2 | コーナー通過順位2 |
| 5 | `corner_tsuka_juni_3` | コーナー通過順位3 | コーナー通過順位3 |
| 6 | `corner_tsuka_juni_4` | コーナー通過順位4 | コーナー通過順位4 |
| 7 | `course_kubun` | コース区分 | 使用コースの区分 |
| 8 | `course_kubun_henkomae` | コース区分変更前 | コース区分変更前 |
| 9 | `data_sakusei_nengappi` | データ作成年月日 | JRA-VANがデータを作成した日付 |
| 10 | `fukashokin` | 付加賞金 | レースの付加賞金額 |
| 11 | `fukashokin_henkomae` | 付加賞金変更前 | 付加賞金変更前 |
| 12 | `grade_code` | グレードコード | レースのグレード・格付け |
| 13 | `grade_code_henkomae` | グレードコード変更前 | グレードコード変更前 |
| 14 | `hasso_jikoku` | 発走時刻 | レースの発走時刻 |
| 15 | `kyori` | 距離 | コースの競走距離 |
| 16 | `shusso_tosu` | 出走頭数 | 実際に出走した頭数 |
| 17 | `tenko_code` | 天候コード | レース当日の天候 |
| 18 | `track_code` | トラックコード | 芝・ダート・内外・向きの組み合わせ |
| 19 | `kohan_4f` | 後半4F | 後半4F |
| 20 | `zenhan_3f` | 前半3F | 前半3F |
| 21 | `zenhan_4f` | 前半4F | 前半4F |
| 22 | `kyoso_joken_code_3sai` | 競走条件コード3歳 | 競走条件コード3歳 |
| 23 | `kyoso_joken_code_4sai` | 競走条件コード4歳 | 競走条件コード4歳 |
| 24 | `kyoso_joken_code_5sai_ijo` | 競走条件コード5歳以上 | 競走条件コード5歳以上 |
| 25 | `kyoso_joken_meisho` | 競走条件名称 | 競走条件名称 |
| 26 | `kyoso_kigo_code` | 競走記号コード | 競走記号コード |
| 27 | `kyoso_shubetsu_code` | 競走種別コード | 競走種別コード |
| 28 | `kyosomei_fukudai` | 競走名副題 | 競走名副題 |
| 29 | `kyosomei_fukudai_eur` | 競走名副題欧字 | 競走名副題欧字 |
| 30 | `hasso_jikoku_henkomae` | 発走時刻変更前 | 発走時刻変更前 |
| 31 | `honshokin_henkomae` | 本賞金変更前 | 本賞金変更前 |

---

### 1.3 jvd_ck (レース成績・距離別成績) - 23列【完全版】

**説明**: 過去成績、距離別パフォーマンス

| # | カラム名 | 日本語名 | 説明 |
|---|---------|---------|------|
| 1 | `banushimei_hojinkaku` | 馬主名法人格 | 馬主名法人格 |
| 2 | `chokyoshimei` | 調教師名 | 調教師名 |
| 3 | `chuo_gokei` | 中央合計 | 中央合計 |
| 4 | `dirt_1200_ika` | ダート1200m以下 | ダート1200m以下 |
| 5 | `dirt_1201_1400` | ダート1201-1400m | ダート1201-1400m |
| 6 | `dirt_1401_1600` | ダート1401-1600m | ダート1401-1600m |
| 7 | `dirt_1601_1800` | ダート1601-1800m | ダート1601-1800m |
| 8 | `dirt_1801_2000` | ダート1801-2000m | ダート1801-2000m |
| 9 | `dirt_2001_2200` | ダート2001-2200m | ダート2001-2200m |
| 10 | `dirt_2201_2400` | ダート2201-2400m | ダート2201-2400m |
| 11 | `dirt_2401_2800` | ダート2401-2800m | ダート2401-2800m |
| 12 | `dirt_2801_ijo` | ダート2801m以上 | ダート2801m以上 |
| 13 | `dirt_choku` | ダート直線 | ダート直線 |
| 14 | `dirt_chukyo` | ダート中京 | ダート中京 |
| 15 | `dirt_fukushima` | ダート福島 | ダート福島 |
| 16 | `dirt_hakodate` | ダート函館 | ダート函館 |
| 17 | `dirt_hanshin` | ダート阪神 | ダート阪神 |
| 18 | `dirt_kokura` | ダート小倉 | ダート小倉 |
| 19 | `dirt_kyoto` | ダート京都 | ダート京都 |
| 20 | `dirt_migi` | ダート右回り | ダート右回り |
| 21 | `shiba_1201_1400` | 芝1201-1400m | 芝1201-1400m |
| 22 | `shiba_1401_1600` | 芝1401-1600m | 芝1401-1600m |
| 23 | `shiba_1601_1800` | 芝1601-1800m | 芝1601-1800m |

---

### 1.4 jvd_h1 (払戻金情報) - 21列【完全版】

**説明**: 各馬券の配当金額

| # | カラム名 | 日本語名 | 説明 |
|---|---------|---------|------|
| 1 | `fukusho_chakubarai_key` | 複勝着払いキー | 複勝着払いキー |
| 2 | `hatsubai_flag_fukusho` | 発売フラグ複勝 | 発売フラグ複勝 |
| 3 | `hatsubai_flag_sanrenpuku` | 発売フラグ三連複 | 発売フラグ三連複 |
| 4 | `hatsubai_flag_tansho` | 発売フラグ単勝 | 発売フラグ単勝 |
| 5 | `hatsubai_flag_umaren` | 発売フラグ馬連 | 発売フラグ馬連 |
| 6 | `hatsubai_flag_umatan` | 発売フラグ馬単 | 発売フラグ馬単 |
| 7 | `hatsubai_flag_wakuren` | 発売フラグ枠連 | 発売フラグ枠連 |
| 8 | `hatsubai_flag_wide` | 発売フラグワイド | 発売フラグワイド |
| 9 | `henkan_dowaku_joho` | 返還同枠情報 | 返還同枠情報 |
| 10 | `henkan_hyosu_gokei_fukusho` | 返還票数合計複勝 | 返還票数合計複勝 |
| 11 | `henkan_hyosu_gokei_sanrenpuku` | 返還票数合計三連複 | 返還票数合計三連複 |
| 12 | `henkan_hyosu_gokei_wakuren` | 返還票数合計枠連 | 返還票数合計枠連 |
| 13 | `henkan_hyosu_gokei_wide` | 返還票数合計ワイド | 返還票数合計ワイド |
| 14 | `henkan_umaban_joho` | 返還馬番情報 | 返還馬番情報 |
| 15 | `henkan_wakuban_joho` | 返還枠番情報 | 返還枠番情報 |
| 16 | `hyosu_fukusho` | 票数複勝 | 票数複勝 |
| 17 | `hyosu_gokei_fukusho` | 票数合計複勝 | 票数合計複勝 |
| 18 | `hyosu_gokei_sanrenpuku` | 票数合計三連複 | 票数合計三連複 |
| 19 | `hyosu_gokei_tansho` | 票数合計単勝 | 票数合計単勝 |
| 20 | `henkan_hyosu_gokei_tansho` | 返還票数合計単勝 | 返還票数合計単勝 |
| 21 | `henkan_hyosu_gokei_umaren` | 返還票数合計馬連 | 返還票数合計馬連 |

---

### 1.5 jvd_hr (払戻金詳細) - 18列【完全版】

**説明**: 払戻金の詳細内訳

| # | カラム名 | 日本語名 | 説明 |
|---|---------|---------|------|
| 1 | `haraimodoshi_fukusho_1a` | 払戻複勝1着A | 払戻複勝1着A |
| 2 | `haraimodoshi_fukusho_1b` | 払戻複勝1着B | 払戻複勝1着B |
| 3 | `haraimodoshi_fukusho_1c` | 払戻複勝1着C | 払戻複勝1着C |
| 4 | `haraimodoshi_fukusho_2a` | 払戻複勝2着A | 払戻複勝2着A |
| 5 | `haraimodoshi_fukusho_2b` | 払戻複勝2着B | 払戻複勝2着B |
| 6 | `haraimodoshi_fukusho_2c` | 払戻複勝2着C | 払戻複勝2着C |
| 7 | `haraimodoshi_fukusho_3a` | 払戻複勝3着A | 払戻複勝3着A |
| 8 | `haraimodoshi_fukusho_3b` | 払戻複勝3着B | 払戻複勝3着B |
| 9 | `fuseiritsu_flag_fukusho` | 不成立フラグ複勝 | 不成立フラグ複勝 |
| 10 | `fuseiritsu_flag_sanrenpuku` | 不成立フラグ三連複 | 不成立フラグ三連複 |
| 11 | `fuseiritsu_flag_sanrentan` | 不成立フラグ三連単 | 不成立フラグ三連単 |
| 12 | `fuseiritsu_flag_tansho` | 不成立フラグ単勝 | 不成立フラグ単勝 |
| 13 | `haraimodoshi_sanrentan_2b` | 払戻三連単2B | 払戻三連単2B |
| 14 | `haraimodoshi_sanrentan_2c` | 払戻三連単2C | 払戻三連単2C |
| 15 | `haraimodoshi_sanrentan_3a` | 払戻三連単3A | 払戻三連単3A |
| 16 | `haraimodoshi_sanrentan_3b` | 払戻三連単3B | 払戻三連単3B |
| 17 | `haraimodoshi_sanrentan_3c` | 払戻三連単3C | 払戻三連単3C |
| 18 | `haraimodoshi_sanrentan_4a` | 払戻三連単4A | 払戻三連単4A |

---

### 1.6 jvd_wc (ウッドチップ調教) - 17列【完全版】

**説明**: ウッドチップコースでの調教データ

| # | カラム名 | 日本語名 | 説明 |
|---|---------|---------|------|
| 1 | `lap_time_10f` | ラップタイム10F | 10ハロン地点のラップタイム |
| 2 | `lap_time_5f` | ラップタイム5F | 5ハロン地点のラップタイム |
| 3 | `lap_time_6f` | ラップタイム6F | 6ハロン地点のラップタイム |
| 4 | `lap_time_7f` | ラップタイム7F | 7ハロン地点のラップタイム |
| 5 | `lap_time_8f` | ラップタイム8F | 8ハロン地点のラップタイム |
| 6 | `lap_time_9f` | ラップタイム9F | 9ハロン地点のラップタイム |
| 7 | `time_gokei_10f` | タイム合計10F | タイム合計10F |
| 8 | `time_gokei_5f` | タイム合計5F | タイム合計5F |
| 9 | `time_gokei_6f` | タイム合計6F | タイム合計6F |
| 10 | `time_gokei_7f` | タイム合計7F | タイム合計7F |
| 11 | `time_gokei_8f` | タイム合計8F | タイム合計8F |
| 12 | `time_gokei_9f` | タイム合計9F | タイム合計9F |
| 13 | `babamawari` | 馬場回り | 馬場回り |
| 14 | `chokyo_jikoku` | 調教時刻 | 調教時刻 |
| 15 | `chokyo_nengappi` | 調教年月日 | 調教年月日 |
| 16 | `course` | コース | コース |
| 17 | `tracen_kubun` | トラセン区分 | トラセン区分 |

---

### 1.7 jvd_dm (データマイニング予想) - 16列【完全版】

**説明**: JRA公式のデータマイニング予想値

| # | カラム名 | 日本語名 | 説明 |
|---|---------|---------|------|
| 1 | `data_sakusei_jifun` | データ作成時分 | データ作成時分 |
| 2 | `mining_yoso_01` | マイニング予想01 | マイニング予想01 |
| 3 | `mining_yoso_02` | マイニング予想02 | マイニング予想02 |
| 4 | `mining_yoso_03` | マイニング予想03 | マイニング予想03 |
| 5 | `mining_yoso_04` | マイニング予想04 | マイニング予想04 |
| 6 | `mining_yoso_05` | マイニング予想05 | マイニング予想05 |
| 7 | `mining_yoso_06` | マイニング予想06 | マイニング予想06 |
| 8 | `mining_yoso_15` | マイニング予想15 | マイニング予想15 |
| 9 | `mining_yoso_16` | マイニング予想16 | マイニング予想16 |
| 10 | `mining_yoso_17` | マイニング予想17 | マイニング予想17 |
| 11 | `mining_yoso_18` | マイニング予想18 | マイニング予想18 |
| 12 | `mining_yoso_07` | マイニング予想07 | マイニング予想07 |
| 13 | `mining_yoso_08` | マイニング予想08 | マイニング予想08 |
| 14 | `mining_yoso_09` | マイニング予想09 | マイニング予想09 |
| 15 | `mining_yoso_10` | マイニング予想10 | マイニング予想10 |
| 16 | `mining_yoso_11` | マイニング予想11 | マイニング予想11 |

---

### 1.8 jvd_um (馬基本情報) - 14列【完全版】

**説明**: 馬の血統・基本プロフィール

| # | カラム名 | 日本語名 | 説明 |
|---|---------|---------|------|
| 1 | `bamei_eur` | 馬名欧字 | 馬名欧字 |
| 2 | `bamei_hankaku_kana` | 馬名半角カナ | 馬名半角カナ |
| 3 | `dirt_furyo` | ダート不良 | ダート不良 |
| 4 | `dirt_hidari` | ダート左回り | ダート左回り |
| 5 | `dirt_long` | ダート長距離 | ダート長距離 |
| 6 | `dirt_middle` | ダート中距離 | ダート中距離 |
| 7 | `dirt_omo` | ダート重 | ダート重 |
| 8 | `dirt_ryo` | ダート良 | ダート良 |
| 9 | `dirt_short` | ダート短距離 | ダート短距離 |
| 10 | `dirt_yayaomo` | ダートやや重 | ダートやや重 |
| 11 | `ketto_joho_12a` | 血統情報12A | 血統情報12A |
| 12 | `ketto_joho_12b` | 血統情報12B | 血統情報12B |
| 13 | `ketto_joho_13a` | 血統情報13A | 血統情報13A |
| 14 | `ketto_joho_13b` | 血統情報13B | 血統情報13B |

---

### 1.9 jvd_sk (血統情報) - 12列【完全版】

**説明**: 10代分の先祖血統データ

| # | カラム名 | 日本語名 | 説明 |
|---|---------|---------|------|
| 1 | `hinshu_code` | 品種コード | 馬の品種 |
| 2 | `ketto_joho_01a` | 血統情報01A | 血統情報01A |
| 3 | `ketto_joho_02a` | 血統情報02A | 血統情報02A |
| 4 | `ketto_joho_03a` | 血統情報03A | 血統情報03A |
| 5 | `ketto_joho_04a` | 血統情報04A | 血統情報04A |
| 6 | `ketto_joho_05a` | 血統情報05A | 血統情報05A |
| 7 | `ketto_joho_06a` | 血統情報06A | 血統情報06A |
| 8 | `ketto_joho_07a` | 血統情報07A | 血統情報07A |
| 9 | `seinengappi` | 生年月日 | 生年月日 |
| 10 | `ketto_joho_08a` | 血統情報08A | 血統情報08A |
| 11 | `ketto_joho_09a` | 血統情報09A | 血統情報09A |
| 12 | `ketto_joho_10a` | 血統情報10A | 血統情報10A |

---

### 1.10 jvd_hc (芝・ダート調教) - 7列【完全版】

**説明**: 芝・ダートコースでの調教データ

| # | カラム名 | 日本語名 | 説明 |
|---|---------|---------|------|
| 1 | `lap_time_1f` | ラップタイム1F | 1ハロン地点のラップタイム |
| 2 | `lap_time_2f` | ラップタイム2F | 2ハロン地点のラップタイム |
| 3 | `lap_time_3f` | ラップタイム3F | 3ハロン地点のラップタイム |
| 4 | `lap_time_4f` | ラップタイム4F | 4ハロン地点のラップタイム |
| 5 | `time_gokei_2f` | タイム合計2F | タイム合計2F |
| 6 | `time_gokei_3f` | タイム合計3F | タイム合計3F |
| 7 | `time_gokei_4f` | タイム合計4F | タイム合計4F |

---

### 1.11 jvd_h6 (三連単払戻) - 6列【完全版】

**説明**: 三連単の配当情報

| # | カラム名 | 日本語名 | 説明 |
|---|---------|---------|------|
| 1 | `hatsubai_flag_sanrentan` | 発売フラグ三連単 | 発売フラグ三連単 |
| 2 | `record_id` | レコードID | レコードID |
| 3 | `toroku_tosu` | 登録頭数 | 登録頭数 |
| 4 | `henkan_hyosu_gokei_sanrentan` | 返還票数合計三連単 | 返還票数合計三連単 |
| 5 | `hyosu_gokei_sanrentan` | 票数合計三連単 | 票数合計三連単 |
| 6 | `hyosu_sanrentan` | 票数三連単 | 票数三連単 |

---

### 1.12 jvd_jg (除外・取消情報) - 3列【完全版】

**説明**: 出走取消・除外の理由

| # | カラム名 | 日本語名 | 説明 |
|---|---------|---------|------|
| 1 | `jogai_jotai_kubun` | 除外状態区分 | 除外状態区分 |
| 2 | `shusso_kubun` | 出走区分 | 出走区分 |
| 3 | `shutsuba_tohyo_uketsuke` | 出馬投票受付 | 出馬投票受付 |

---

### 1.13 jvd_ch (調教師マスタ) - 1列【完全版】

**説明**: 調教師の東西所属コード

| # | カラム名 | 日本語名 | 説明 |
|---|---------|---------|------|
| 1 | `tozai_shozoku_code` | 東西所属コード | 馬の東西所属 |

---

## 2. 除外されたテーブル（結合不可）

### ❌ jvd_bt (繁殖馬) - 2列
**除外理由**: `hanshoku_toroku_bango`（繁殖登録番号）が `jvd_se` に存在しない

### ❌ jvd_hn (繁殖牝馬) - 6列
**除外理由**: `hanshoku_toroku_bango` が `jvd_se` に存在しない

### ❌ jvd_br (生産者) - 1列
**除外理由**: `seisansha_code`（生産者コード）が `jvd_se` に存在しない

**代替情報**: 血統データは `jvd_sk` (12列) + `jvd_um` (14列) で十分カバー

---

## 3. データ型

**全カラム共通**: `character varying` (文字列型)

**注意点**:
- 数値比較ではなく文字列比較を使用
- 日付変換には `SUBSTRING()` と文字列結合 `||` を使用
- `TO_CHAR()` は使用不可（既に文字列型のため）

---

## 4. JOIN 条件

### レース単位テーブル
```sql
ON se.kaisai_nen = ra.kaisai_nen 
   AND se.kaisai_tsukihi = ra.kaisai_tsukihi 
   AND se.keibajo_code = ra.keibajo_code 
   AND se.race_bango = ra.race_bango
```

### レース+馬単位テーブル
```sql
ON se.kaisai_nen = ck.kaisai_nen 
   AND se.kaisai_tsukihi = ck.kaisai_tsukihi 
   AND se.keibajo_code = ck.keibajo_code 
   AND se.race_bango = ck.race_bango 
   AND se.ketto_toroku_bango = ck.ketto_toroku_bango
```

### 馬マスタテーブル
```sql
ON se.ketto_toroku_bango = um.ketto_toroku_bango
```

### 調教師マスタテーブル
```sql
ON se.chokyoshi_code = ch.chokyoshi_code
```

---

## 5. 実装コード

**証拠ファイル**: `phase7/scripts/phase7b_factor_roi/create_merged_dataset_334cols.py`

```python
jvd_tables = {
    'jvd_ra': 'ra',
    'jvd_ck': 'ck',
    'jvd_um': 'um',
    'jvd_hr': 'hr',
    'jvd_h1': 'h1',
    'jvd_h6': 'h6',
    'jvd_dm': 'dm',
    # 'jvd_bt': 'bt',  # ❌ 除外
    'jvd_wc': 'wc',
    'jvd_hc': 'hc',
    'jvd_ch': 'ch',
    # 'jvd_hn': 'hn',  # ❌ 除外
    # 'jvd_br': 'br',  # ❌ 除外
    'jvd_jg': 'jg',
    'jvd_sk': 'sk'
}
```

---

## 6. カラム数サマリー

| テーブル | カラム数 | 説明 |
|---------|---------|------|
| jvd_se | 40 | 成績テーブル（ベース） |
| jvd_ra | 31 | レース基本情報 |
| jvd_ck | 23 | レース成績・距離別成績 |
| jvd_h1 | 21 | 払戻金情報 |
| jvd_hr | 18 | 払戻金詳細 |
| jvd_wc | 17 | ウッドチップ調教 |
| jvd_dm | 16 | データマイニング予想 |
| jvd_um | 14 | 馬基本情報 |
| jvd_sk | 12 | 血統情報 |
| jvd_hc | 7 | 芝・ダート調教 |
| jvd_h6 | 6 | 三連単払戻 |
| jvd_jg | 3 | 除外・取消情報 |
| jvd_ch | 1 | 調教師マスタ |
| **合計** | **209** | **全13テーブル** |

---

**作成日**: 2026-04-01（修正版）  
**証拠元**: サンドボックス実ファイル検証  
**総カラム数**: 209列（全て記載）  
**修正内容**: 全13テーブル・209列を完全記載、省略なし
