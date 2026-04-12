# 325ファクター 全量エッジ分析 最終サマリー

**生成日時**: 2026-04-12 11:42:05
**データ期間**: 2016-2025年（10年間）
**基準値**: 補正回収率 80.0%

## 採用基準
- **S**: 単勝・複勝両方で信頼度調整済み回収率 > 80% のビンが2個以上
- **A**: 単勝または複勝で合格ビンが2個以上
- **B**: 単勝または複勝で合格ビンが1個
- **C**: 合格ビンなし（不採用）

---

## 採用推奨ファクター（S/A/B）

| ファクター名 | テーブル | カラム | 最適セグメント | 単勝合格ビン数 | 複勝合格ビン数 | 最高単勝調整済 | 最高複勝調整済 | 評価 |
|---|---|---|---|---|---|---|---|---|
| 馬齢 | jvd_se | barei | COURSE_27 | 106 | 220 | **410.8** | **280.0** | **S** |
| 血統情報02A（母） | jvd_sk | ketto_joho_02a | SURFACE_2 | 5251 | 10775 | **322.3** | **2542.2** | **S** |
| 血統情報06A（母母） | jvd_sk | ketto_joho_06a | SURFACE_2 | 4830 | 10105 | **309.2** | **606.4** | **S** |
| 馬主コード | jvd_se | banushi_code | GLOBAL | 1627 | 3479 | **251.1** | **557.2** | **S** |
| 騎手コード | jrd_kyi | kishu_code | COURSE_27 | 1526 | 3223 | **231.0** | **2878.3** | **S** |
| 総合指数 | jrd_kyi | sogo_shisu | COURSE_27 | 137 | 300 | **208.7** | **697.1** | **S** |
| 調教矢印コード | jrd_kyi | chokyo_yajirushi_code | COURSE_27 | 56 | 114 | **186.4** | **471.6** | **S** |
| IDM（スピード指数） | jrd_kyi | idm | COURSE_27 | 137 | 300 | **185.0** | **511.3** | **S** |
| 騎手指数 | jrd_kyi | kishu_shisu | COURSE_27 | 140 | 298 | **179.5** | **1644.2** | **S** |
| 血統情報13A | jvd_um | ketto_joho_13a | SURFACE_2 | 1280 | 2850 | **178.1** | **308.8** | **S** |
| 血統情報13B | jvd_um | ketto_joho_13b | SURFACE_2 | 1277 | 2847 | **178.1** | **308.8** | **S** |
| 血統情報01A（父） | jvd_sk | ketto_joho_01a | COURSE_27 | 1960 | 4232 | **176.8** | **1408.9** | **S** |
| 血統情報04A（父母） | jvd_sk | ketto_joho_04a | COURSE_27 | 1875 | 4037 | **176.8** | **1408.9** | **S** |
| 血統情報10A | jvd_sk | ketto_joho_10a | COURSE_27 | 1869 | 4005 | **176.8** | **1408.9** | **S** |
| 調教師コード | jvd_se | chokyoshi_code | COURSE_27 | 2129 | 4649 | **176.2** | **398.3** | **S** |
| 血統情報03A（父父） | jvd_sk | ketto_joho_03a | COURSE_27 | 1133 | 2389 | **167.1** | **1408.9** | **S** |
| 血統情報08A | jvd_sk | ketto_joho_08a | COURSE_27 | 1107 | 2321 | **167.1** | **1408.9** | **S** |
| 血統情報12A | jvd_um | ketto_joho_12a | GLOBAL | 867 | 1994 | **166.6** | **506.2** | **S** |
| 血統情報12B | jvd_um | ketto_joho_12b | GLOBAL | 865 | 1992 | **166.6** | **506.2** | **S** |
| 血統情報05A（母父） | jvd_sk | ketto_joho_05a | GLOBAL | 970 | 2208 | **166.6** | **506.2** | **S** |
| 血統情報09A | jvd_sk | ketto_joho_09a | COURSE_27 | 1415 | 2970 | **165.7** | **386.9** | **S** |
| 血統情報07A（3代父） | jvd_sk | ketto_joho_07a | COURSE_27 | 724 | 1539 | **154.3** | **1408.9** | **S** |
| 出走区分 | jvd_jg | shusso_kubun | COURSE_27 | 27 | 53 | **151.6** | **211.7** | **S** |
| 調教指数 | jrd_kyi | chokyo_shisu | COURSE_27 | 142 | 300 | **150.4** | **913.3** | **S** |
| 厩舎ランク | jrd_kyi | kyusha_rank | COURSE_27 | 106 | 220 | **146.4** | **256.5** | **S** |
| ダート左回り適性 | jvd_um | dirt_hidari | COURSE_27 | 42 | 108 | **140.0** | **231.4** | **S** |
| 上がり指数 | jrd_kyi | agari_shisu | COURSE_27 | 147 | 300 | **137.2** | **424.2** | **S** |
| 厩舎指数 | jrd_kyi | kyusha_shisu | COURSE_27 | 155 | 300 | **124.7** | **615.6** | **S** |
| ペース指数 | jrd_kyi | pace_shisu | COURSE_27 | 158 | 300 | **124.2** | **278.0** | **S** |
| ダート短距離適性 | jvd_um | dirt_short | COURSE_27 | 39 | 108 | **123.2** | **235.4** | **S** |
| 距離適性コード | jrd_kyi | kyori_tekisei | COURSE_27 | 86 | 152 | **113.7** | **240.7** | **S** |
| ダート良適性 | jvd_um | dirt_ryo | COURSE_27 | 33 | 120 | **113.3** | **253.3** | **S** |
| ダート中距離適性 | jvd_um | dirt_middle | COURSE_27 | 36 | 88 | **112.9** | **241.1** | **S** |
| テン指数 | jrd_kyi | ten_shisu | COURSE_27 | 145 | 300 | **110.7** | **257.0** | **S** |
| 位置取り指数 | jrd_kyi | ichi_shisu | COURSE_27 | 141 | 300 | **110.0** | **293.4** | **S** |
| 脚質コード | jrd_kyi | kyakushitsu | COURSE_27 | 73 | 147 | **109.0** | **255.6** | **S** |
| 馬記号コード | jvd_se | umakigo_code | COURSE_27 | 55 | 109 | **107.3** | **295.7** | **S** |
| 適性コード（重） | jrd_kyi | omo_tekisei_code | COURSE_27 | 57 | 115 | **106.0** | **238.3** | **S** |
| 性別コード | jvd_se | seibetsu_code | COURSE_27 | 49 | 90 | **104.3** | **243.8** | **S** |
| 予想走破タイム | jvd_se | yoso_soha_time | COURSE_27 | 36 | 81 | **103.1** | **222.5** | **S** |
| 中央合計成績 | jvd_ck | chuo_gokei | COURSE_27 | 48 | 90 | **99.4** | **269.6** | **S** |
| 馬体重増減差 | jvd_se | zogen_sa | COURSE_27 | 56 | 120 | **98.5** | **229.0** | **S** |
| ダートやや重適性 | jvd_um | dirt_yayaomo | COURSE_27 | 32 | 90 | **98.4** | **216.5** | **S** |
| 枠番 | jvd_se | wakuban | COURSE_27 | 121 | 240 | **98.0** | **210.0** | **S** |
| 品種コード | jvd_sk | hinshu_code | GLOBAL | 16 | 31 | **96.3** | **211.9** | **S** |
| 負担重量（斤量） | jrd_kyi | futan_juryo | COURSE_27 | 69 | 150 | **95.5** | **216.5** | **S** |
| 東西所属コード | jvd_ch | tozai_shozoku_code | COURSE_27 | 36 | 72 | **95.4** | **219.9** | **S** |
| ダート重適性 | jvd_um | dirt_omo | COURSE_27 | 36 | 90 | **94.2** | **223.3** | **S** |
| 発走時刻 | jvd_ra | hasso_jikoku | COURSE_27 | 745 | 1843 | **94.1** | **224.5** | **S** |
| マイニング予想16 | jvd_dm | mining_yoso_16 | COURSE_27 | 37 | 72 | **93.8** | **245.6** | **S** |
| ブリンカー使用区分 | jvd_se | blinker_shiyo_kubun | COURSE_27 | 30 | 60 | **92.5** | **235.7** | **S** |
| マイニング予想01 | jvd_dm | mining_yoso_01 | COURSE_27 | 34 | 81 | **92.0** | **222.4** | **S** |
| マイニング予想02 | jvd_dm | mining_yoso_02 | COURSE_27 | 35 | 83 | **92.0** | **222.5** | **S** |
| マイニング予想03 | jvd_dm | mining_yoso_03 | COURSE_27 | 36 | 83 | **92.0** | **222.6** | **S** |
| マイニング予想04 | jvd_dm | mining_yoso_04 | COURSE_27 | 36 | 83 | **92.0** | **222.7** | **S** |
| マイニング予想05 | jvd_dm | mining_yoso_05 | COURSE_27 | 37 | 83 | **92.0** | **222.6** | **S** |
| マイニング予想06 | jvd_dm | mining_yoso_06 | COURSE_27 | 36 | 82 | **92.0** | **222.5** | **S** |
| マイニング予想15 | jvd_dm | mining_yoso_15 | COURSE_27 | 40 | 73 | **92.0** | **245.0** | **S** |
| マイニング予想07 | jvd_dm | mining_yoso_07 | COURSE_27 | 35 | 83 | **92.0** | **222.8** | **S** |
| マイニング予想08 | jvd_dm | mining_yoso_08 | COURSE_27 | 31 | 83 | **92.0** | **223.6** | **S** |
| マイニング予想09 | jvd_dm | mining_yoso_09 | COURSE_27 | 37 | 83 | **92.0** | **226.7** | **S** |
| マイニング予想10 | jvd_dm | mining_yoso_10 | COURSE_27 | 38 | 82 | **92.0** | **229.5** | **S** |
| マイニング予想11 | jvd_dm | mining_yoso_11 | COURSE_27 | 41 | 82 | **92.0** | **233.3** | **S** |
| 増減符号 | jvd_se | zogen_fugo | COURSE_27 | 31 | 60 | **89.7** | **212.0** | **S** |
| 馬体重（計量時） | jvd_se | bataiju | COURSE_27 | 81 | 150 | **89.4** | **228.7** | **S** |
| 競走記号コード | jvd_ra | kyoso_kigo_code | COURSE_27 | 160 | 407 | **88.2** | **223.3** | **S** |
| ダート不良適性 | jvd_um | dirt_furyo | COURSE_27 | 29 | 60 | **88.1** | **217.4** | **S** |
| 天候コード | jvd_ra | tenko_code | COURSE_27 | 53 | 138 | **88.0** | **209.6** | **S** |
| マイニング予想18 | jvd_dm | mining_yoso_18 | COURSE_27 | 21 | 39 | **87.2** | **244.7** | **S** |
| 開催日目 | jvd_se | kaisai_nichime | COURSE_27 | 141 | 351 | **86.0** | **204.8** | **S** |
| 登録頭数 | jvd_h6 | toroku_tosu | COURSE_27 | 45 | 98 | **84.4** | **240.1** | **S** |
| 出走頭数 | jvd_ra | shusso_tosu | COURSE_27 | 43 | 98 | **83.8** | **240.2** | **S** |
| 馬場状態コード芝 | jvd_ra | babajotai_code_shiba | COURSE_27 | 36 | 81 | **83.8** | **209.4** | **S** |
| グレードコード | jvd_ra | grade_code | GLOBAL | 34 | 113 | **83.4** | **229.3** | **S** |
| 競走種別コード | jvd_ra | kyoso_shubetsu_code | COURSE_27 | 49 | 115 | **83.4** | **224.5** | **S** |
| 競走条件コード3歳 | jvd_ra | kyoso_joken_code_3sai | COURSE_27 | 66 | 194 | **82.8** | **221.8** | **S** |
| マイニング予想17 | jvd_dm | mining_yoso_17 | COURSE_27 | 21 | 40 | **82.8** | **244.0** | **S** |
| 馬場状態コードダート | jvd_ra | babajotai_code_dirt | COURSE_27 | 23 | 74 | **82.4** | **211.9** | **S** |
| コース区分 | jvd_ra | course_kubun | COURSE_27 | 26 | 54 | **82.3** | **211.3** | **S** |
| 競走条件コード4歳 | jvd_ra | kyoso_joken_code_4sai | COURSE_27 | 60 | 148 | **82.0** | **232.2** | **S** |
| 競走条件コード5歳以上 | jvd_ra | kyoso_joken_code_5sai_ijo | COURSE_27 | 60 | 148 | **82.0** | **232.2** | **S** |
| 付加賞金 | jvd_ra | fukashokin | COURSE_27 | 23 | 59 | **81.6** | **238.3** | **S** |
| 距離 | jvd_ra | kyori | COURSE_27 | 23 | 62 | **81.1** | **222.6** | **S** |
| 異常区分コード | jvd_se | ijo_kubun_code | COURSE_27 | 18 | 30 | **80.8** | **211.9** | **S** |
| 負担重量変更前 | jvd_se | futan_juryo_henkomae | COURSE_27 | 14 | 30 | **80.8** | **211.9** | **S** |
| トラックコード（芝/ダ/向き） | jvd_ra | track_code | GLOBAL | 22 | 56 | **80.8** | **218.8** | **S** |
| 発売フラグ複勝 | jvd_h1 | hatsubai_flag_fukusho | COURSE_27 | 14 | 30 | **80.8** | **211.5** | **S** |
| 発売フラグ単勝 | jvd_h1 | hatsubai_flag_tansho | COURSE_27 | 14 | 30 | **80.8** | **211.5** | **S** |
| 不成立フラグ複勝 | jvd_hr | fuseiritsu_flag_fukusho | COURSE_27 | 14 | 30 | **80.8** | **211.9** | **S** |
| 不成立フラグ単勝 | jvd_hr | fuseiritsu_flag_tansho | COURSE_27 | 14 | 30 | **80.8** | **211.9** | **S** |
| ダート長距離適性 | jvd_um | dirt_long | COURSE_27 | 14 | 30 | **80.8** | **211.9** | **S** |
| 発売フラグ三連単 | jvd_h6 | hatsubai_flag_sanrentan | COURSE_27 | 14 | 30 | **80.8** | **211.5** | **S** |
| 除外状態区分 | jvd_jg | jogai_jotai_kubun | COURSE_27 | 14 | 30 | **80.8** | **211.9** | **S** |

---

## 全325ファクター一覧（スキップ含む）

| # | ファクター名 | テーブル | カラム | 種別 | NULL率 | 単勝合格数 | 複勝合格数 | 最高単勝 | 最高複勝 | 評価 | 備考 |
|---|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 相手馬情報1 | jvd_se | aiteuma_joho_1 | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 2 | 相手馬情報2 | jvd_se | aiteuma_joho_2 | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 3 | 相手馬情報3 | jvd_se | aiteuma_joho_3 | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 4 | 馬名 | jvd_se | bamei | SKIP | 0.0% | - | - | - | - | **SKIP** | IDENTIFIER |
| 5 | 馬主コード | jvd_se | banushi_code | CATEGORY | 0.0% | 1627 | 3479 | 251.1 | 557.2 | **S** |  |
| 6 | 馬主名 | jvd_se | banushimei | SKIP | 0.0% | - | - | - | - | **SKIP** | IDENTIFIER |
| 7 | 馬齢 | jvd_se | barei | ORDINAL | 0.0% | 106 | 220 | 410.8 | 280.0 | **S** |  |
| 8 | 馬体重（計量時） | jvd_se | bataiju | NUMERIC | 0.0% | 81 | 150 | 89.4 | 228.7 | **S** |  |
| 9 | ブリンカー使用区分 | jvd_se | blinker_shiyo_kubun | CATEGORY | 0.0% | 30 | 60 | 92.5 | 235.7 | **S** |  |
| 10 | 着差コード1 | jvd_se | chakusa_code_1 | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 11 | 着差コード2 | jvd_se | chakusa_code_2 | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 12 | 着差コード3 | jvd_se | chakusa_code_3 | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 13 | 調教師コード | jvd_se | chokyoshi_code | CATEGORY | 0.0% | 2129 | 4649 | 176.2 | 398.3 | **S** |  |
| 14 | 調教師名略称 | jvd_se | chokyoshimei_ryakusho | SKIP | 0.0% | - | - | - | - | **SKIP** | IDENTIFIER |
| 15 | コーナー通過1 | jvd_se | corner_1 | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 16 | コーナー通過2 | jvd_se | corner_2 | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 17 | コーナー通過3 | jvd_se | corner_3 | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 18 | コーナー通過4 | jvd_se | corner_4 | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 19 | データ区分 | jvd_se | data_kubun | SKIP | 0.0% | - | - | - | - | **SKIP** | ADMINISTRATIVE |
| 20 | 開催回 | jvd_se | kaisai_kai | SKIP | 0.0% | - | - | - | - | **SKIP** | IDENTIFIER |
| 21 | 開催年 | jvd_se | kaisai_nen | SKIP | 0.0% | - | - | - | - | **SKIP** | IDENTIFIER |
| 22 | 開催日目 | jvd_se | kaisai_nichime | ORDINAL | 0.0% | 141 | 351 | 86.0 | 204.8 | **S** |  |
| 23 | 開催月日 | jvd_se | kaisai_tsukihi | SKIP | 0.0% | - | - | - | - | **SKIP** | IDENTIFIER |
| 24 | 競馬場コード | jvd_se | keibajo_code | CATEGORY | 0.0% | - | - | - | - | **SKIP** | COLUMN_NOT_IN_DATA |
| 25 | 騎手名略称 | jvd_se | kishumei_ryakusho | SKIP | 0.0% | - | - | - | - | **SKIP** | IDENTIFIER |
| 26 | レース番号 | jvd_se | race_bango | SKIP | 0.0% | - | - | - | - | **SKIP** | IDENTIFIER |
| 27 | 性別コード | jvd_se | seibetsu_code | CATEGORY | 0.0% | 49 | 90 | 104.3 | 243.8 | **S** |  |
| 28 | 馬番 | jvd_se | umaban | ORDINAL | 0.0% | - | - | - | - | **SKIP** | COLUMN_NOT_IN_DATA |
| 29 | 枠番 | jvd_se | wakuban | ORDINAL | 0.0% | 121 | 240 | 98.0 | 210.0 | **S** |  |
| 30 | 馬体重増減差 | jvd_se | zogen_sa | NUMERIC | 0.0% | 56 | 120 | 98.5 | 229.0 | **S** |  |
| 31 | 同着区分 | jvd_se | dochaku_kubun | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 32 | 同着頭数 | jvd_se | dochaku_tosu | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 33 | 服色表示 | jvd_se | fukushoku_hyoji | SKIP | 0.0% | - | - | - | - | **SKIP** | IDENTIFIER |
| 34 | 負担重量変更前 | jvd_se | futan_juryo_henkomae | NUMERIC | 0.0% | 14 | 30 | 80.8 | 211.9 | **S** |  |
| 35 | 異常区分コード | jvd_se | ijo_kubun_code | CATEGORY | 0.0% | 18 | 30 | 80.8 | 211.9 | **S** |  |
| 36 | 予想走破タイム | jvd_se | yoso_soha_time | NUMERIC | 0.0% | 36 | 81 | 103.1 | 222.5 | **S** |  |
| 37 | 増減符号 | jvd_se | zogen_fugo | CATEGORY | 0.0% | 31 | 60 | 89.7 | 212.0 | **S** |  |
| 38 | 馬記号コード | jvd_se | umakigo_code | CATEGORY | 0.0% | 55 | 109 | 107.3 | 295.7 | **S** |  |
| 39 | 予備1 | jvd_se | yobi_1 | SKIP | 0.0% | - | - | - | - | **SKIP** | ADMINISTRATIVE |
| 40 | 予備2 | jvd_se | yobi_2 | SKIP | 0.0% | - | - | - | - | **SKIP** | ADMINISTRATIVE |
| 41 | 馬場状態コードダート | jvd_ra | babajotai_code_dirt | CATEGORY | 0.0% | 23 | 74 | 82.4 | 211.9 | **S** |  |
| 42 | 馬場状態コード芝 | jvd_ra | babajotai_code_shiba | CATEGORY | 0.0% | 36 | 81 | 83.8 | 209.4 | **S** |  |
| 43 | コーナー通過順位1（レース集計） | jvd_ra | corner_tsuka_juni_1 | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 44 | コーナー通過順位2 | jvd_ra | corner_tsuka_juni_2 | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 45 | コーナー通過順位3 | jvd_ra | corner_tsuka_juni_3 | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 46 | コーナー通過順位4 | jvd_ra | corner_tsuka_juni_4 | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 47 | コース区分 | jvd_ra | course_kubun | CATEGORY | 0.0% | 26 | 54 | 82.3 | 211.3 | **S** |  |
| 48 | コース区分変更前 | jvd_ra | course_kubun_henkomae | SKIP | 0.0% | - | - | - | - | **SKIP** | REDUNDANT |
| 49 | データ作成年月日 | jvd_ra | data_sakusei_nengappi | SKIP | 0.0% | - | - | - | - | **SKIP** | ADMINISTRATIVE |
| 50 | 付加賞金 | jvd_ra | fukashokin | NUMERIC | 0.0% | 23 | 59 | 81.6 | 238.3 | **S** |  |
| 51 | 付加賞金変更前 | jvd_ra | fukashokin_henkomae | SKIP | 0.0% | - | - | - | - | **SKIP** | REDUNDANT |
| 52 | グレードコード | jvd_ra | grade_code | CATEGORY | 0.0% | 34 | 113 | 83.4 | 229.3 | **S** |  |
| 53 | グレードコード変更前 | jvd_ra | grade_code_henkomae | SKIP | 0.0% | - | - | - | - | **SKIP** | REDUNDANT |
| 54 | 発走時刻 | jvd_ra | hasso_jikoku | CATEGORY | 0.0% | 745 | 1843 | 94.1 | 224.5 | **S** |  |
| 55 | 距離 | jvd_ra | kyori | NUMERIC | 0.0% | 23 | 62 | 81.1 | 222.6 | **S** |  |
| 56 | 出走頭数 | jvd_ra | shusso_tosu | NUMERIC | 0.0% | 43 | 98 | 83.8 | 240.2 | **S** |  |
| 57 | 天候コード | jvd_ra | tenko_code | CATEGORY | 0.0% | 53 | 138 | 88.0 | 209.6 | **S** |  |
| 58 | トラックコード（芝/ダ/向き） | jvd_ra | track_code | CATEGORY | 0.0% | 22 | 56 | 80.8 | 218.8 | **S** |  |
| 59 | 後半4F（レース結果） | jvd_ra | kohan_4f | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 60 | 前半3F（レース結果） | jvd_ra | zenhan_3f | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 61 | 前半4F（レース結果） | jvd_ra | zenhan_4f | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 62 | 競走条件コード3歳 | jvd_ra | kyoso_joken_code_3sai | CATEGORY | 0.0% | 66 | 194 | 82.8 | 221.8 | **S** |  |
| 63 | 競走条件コード4歳 | jvd_ra | kyoso_joken_code_4sai | CATEGORY | 0.0% | 60 | 148 | 82.0 | 232.2 | **S** |  |
| 64 | 競走条件コード5歳以上 | jvd_ra | kyoso_joken_code_5sai_ijo | CATEGORY | 0.0% | 60 | 148 | 82.0 | 232.2 | **S** |  |
| 65 | 競走条件名称 | jvd_ra | kyoso_joken_meisho | SKIP | 0.0% | - | - | - | - | **SKIP** | IDENTIFIER |
| 66 | 競走記号コード | jvd_ra | kyoso_kigo_code | CATEGORY | 0.0% | 160 | 407 | 88.2 | 223.3 | **S** |  |
| 67 | 競走種別コード | jvd_ra | kyoso_shubetsu_code | CATEGORY | 0.0% | 49 | 115 | 83.4 | 224.5 | **S** |  |
| 68 | 競走名副題 | jvd_ra | kyosomei_fukudai | SKIP | 0.0% | - | - | - | - | **SKIP** | IDENTIFIER |
| 69 | 競走名副題欧字 | jvd_ra | kyosomei_fukudai_eur | SKIP | 0.0% | - | - | - | - | **SKIP** | IDENTIFIER |
| 70 | 発走時刻変更前 | jvd_ra | hasso_jikoku_henkomae | SKIP | 0.0% | - | - | - | - | **SKIP** | REDUNDANT |
| 71 | 本賞金変更前 | jvd_ra | honshokin_henkomae | SKIP | 0.0% | - | - | - | - | **SKIP** | REDUNDANT |
| 72 | 馬主名法人格 | jvd_ck | banushimei_hojinkaku | SKIP | 0.0% | - | - | - | - | **SKIP** | IDENTIFIER |
| 73 | 調教師名 | jvd_ck | chokyoshimei | SKIP | 0.0% | - | - | - | - | **SKIP** | IDENTIFIER |
| 74 | 中央合計成績 | jvd_ck | chuo_gokei | NUMERIC | 18.1% | 48 | 90 | 99.4 | 269.6 | **S** |  |
| 75 | ダート1200m以下成績 | jvd_ck | dirt_1200_ika | NUMERIC | 78.9% | - | - | - | - | **SKIP** | HIGH_NULL(78.9%) |
| 76 | ダート1201-1400m成績 | jvd_ck | dirt_1201_1400 | NUMERIC | 76.2% | - | - | - | - | **SKIP** | HIGH_NULL(76.2%) |
| 77 | ダート1401-1600m成績 | jvd_ck | dirt_1401_1600 | NUMERIC | 88.9% | - | - | - | - | **SKIP** | HIGH_NULL(88.9%) |
| 78 | ダート1601-1800m成績 | jvd_ck | dirt_1601_1800 | NUMERIC | 68.0% | - | - | - | - | **SKIP** | HIGH_NULL(68.0%) |
| 79 | ダート1801-2000m成績 | jvd_ck | dirt_1801_2000 | NUMERIC | 95.0% | - | - | - | - | **SKIP** | HIGH_NULL(95.0%) |
| 80 | ダート2001-2200m成績 | jvd_ck | dirt_2001_2200 | NUMERIC | 96.7% | - | - | - | - | **SKIP** | HIGH_NULL(96.7%) |
| 81 | ダート2201-2400m成績 | jvd_ck | dirt_2201_2400 | NUMERIC | 98.5% | - | - | - | - | **SKIP** | HIGH_NULL(98.5%) |
| 82 | ダート2401-2800m成績 | jvd_ck | dirt_2401_2800 | NUMERIC | 99.7% | - | - | - | - | **SKIP** | HIGH_NULL(99.7%) |
| 83 | ダート2801m以上成績 | jvd_ck | dirt_2801_ijo | NUMERIC | 100.0% | - | - | - | - | **SKIP** | HIGH_NULL(100.0%) |
| 84 | ダート直線成績 | jvd_ck | dirt_choku | NUMERIC | 100.0% | - | - | - | - | **SKIP** | HIGH_NULL(100.0%) |
| 85 | ダート中京成績 | jvd_ck | dirt_chukyo | NUMERIC | 82.8% | - | - | - | - | **SKIP** | HIGH_NULL(82.8%) |
| 86 | ダート福島成績 | jvd_ck | dirt_fukushima | NUMERIC | 91.2% | - | - | - | - | **SKIP** | HIGH_NULL(91.2%) |
| 87 | ダート函館成績 | jvd_ck | dirt_hakodate | NUMERIC | 96.1% | - | - | - | - | **SKIP** | HIGH_NULL(96.1%) |
| 88 | ダート阪神成績 | jvd_ck | dirt_hanshin | NUMERIC | 80.5% | - | - | - | - | **SKIP** | HIGH_NULL(80.5%) |
| 89 | ダート小倉成績 | jvd_ck | dirt_kokura | NUMERIC | 91.6% | - | - | - | - | **SKIP** | HIGH_NULL(91.6%) |
| 90 | ダート京都成績 | jvd_ck | dirt_kyoto | NUMERIC | 83.1% | - | - | - | - | **SKIP** | HIGH_NULL(83.1%) |
| 91 | ダート右回り成績 | jvd_ck | dirt_migi | NUMERIC | 54.3% | - | - | - | - | **SKIP** | HIGH_NULL(54.3%) |
| 92 | 芝1201-1400m成績 | jvd_ck | shiba_1201_1400 | NUMERIC | 81.4% | - | - | - | - | **SKIP** | HIGH_NULL(81.4%) |
| 93 | 芝1401-1600m成績 | jvd_ck | shiba_1401_1600 | NUMERIC | 70.7% | - | - | - | - | **SKIP** | HIGH_NULL(70.7%) |
| 94 | 芝1601-1800m成績 | jvd_ck | shiba_1601_1800 | NUMERIC | 74.8% | - | - | - | - | **SKIP** | HIGH_NULL(74.8%) |
| 95 | 複勝着払いキー | jvd_h1 | fukusho_chakubarai_key | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 96 | 発売フラグ複勝 | jvd_h1 | hatsubai_flag_fukusho | CATEGORY | 0.5% | 14 | 30 | 80.8 | 211.5 | **S** |  |
| 97 | 発売フラグ三連複 | jvd_h1 | hatsubai_flag_sanrenpuku | CATEGORY | 0.0% | - | - | - | - | **SKIP** | COLUMN_NOT_IN_DATA |
| 98 | 発売フラグ単勝 | jvd_h1 | hatsubai_flag_tansho | CATEGORY | 0.5% | 14 | 30 | 80.8 | 211.5 | **S** |  |
| 99 | 発売フラグ馬連 | jvd_h1 | hatsubai_flag_umaren | CATEGORY | 0.0% | - | - | - | - | **SKIP** | COLUMN_NOT_IN_DATA |
| 100 | 発売フラグ馬単 | jvd_h1 | hatsubai_flag_umatan | CATEGORY | 0.0% | - | - | - | - | **SKIP** | COLUMN_NOT_IN_DATA |
| 101 | 発売フラグ枠連 | jvd_h1 | hatsubai_flag_wakuren | CATEGORY | 0.0% | - | - | - | - | **SKIP** | COLUMN_NOT_IN_DATA |
| 102 | 発売フラグワイド | jvd_h1 | hatsubai_flag_wide | CATEGORY | 0.0% | - | - | - | - | **SKIP** | COLUMN_NOT_IN_DATA |
| 103 | 返還同枠情報 | jvd_h1 | henkan_dowaku_joho | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 104 | 返還票数合計複勝 | jvd_h1 | henkan_hyosu_gokei_fukusho | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 105 | 返還票数合計三連複 | jvd_h1 | henkan_hyosu_gokei_sanrenpuku | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 106 | 返還票数合計枠連 | jvd_h1 | henkan_hyosu_gokei_wakuren | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 107 | 返還票数合計ワイド | jvd_h1 | henkan_hyosu_gokei_wide | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 108 | 返還馬番情報 | jvd_h1 | henkan_umaban_joho | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 109 | 返還枠番情報 | jvd_h1 | henkan_wakuban_joho | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 110 | 票数複勝 | jvd_h1 | hyosu_fukusho | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 111 | 票数合計複勝 | jvd_h1 | hyosu_gokei_fukusho | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 112 | 票数合計三連複 | jvd_h1 | hyosu_gokei_sanrenpuku | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 113 | 票数合計単勝 | jvd_h1 | hyosu_gokei_tansho | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 114 | 返還票数合計単勝 | jvd_h1 | henkan_hyosu_gokei_tansho | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 115 | 返還票数合計馬連 | jvd_h1 | henkan_hyosu_gokei_umaren | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 116 | 払戻複勝1着A | jvd_hr | haraimodoshi_fukusho_1a | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 117 | 払戻複勝1着B | jvd_hr | haraimodoshi_fukusho_1b | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 118 | 払戻複勝1着C | jvd_hr | haraimodoshi_fukusho_1c | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 119 | 払戻複勝2着A | jvd_hr | haraimodoshi_fukusho_2a | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 120 | 払戻複勝2着B | jvd_hr | haraimodoshi_fukusho_2b | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 121 | 払戻複勝2着C | jvd_hr | haraimodoshi_fukusho_2c | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 122 | 払戻複勝3着A | jvd_hr | haraimodoshi_fukusho_3a | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 123 | 払戻複勝3着B | jvd_hr | haraimodoshi_fukusho_3b | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 124 | 不成立フラグ複勝 | jvd_hr | fuseiritsu_flag_fukusho | CATEGORY | 0.4% | 14 | 30 | 80.8 | 211.9 | **S** |  |
| 125 | 不成立フラグ三連複 | jvd_hr | fuseiritsu_flag_sanrenpuku | CATEGORY | 0.0% | - | - | - | - | **SKIP** | COLUMN_NOT_IN_DATA |
| 126 | 不成立フラグ三連単 | jvd_hr | fuseiritsu_flag_sanrentan | CATEGORY | 0.0% | - | - | - | - | **SKIP** | COLUMN_NOT_IN_DATA |
| 127 | 不成立フラグ単勝 | jvd_hr | fuseiritsu_flag_tansho | CATEGORY | 0.4% | 14 | 30 | 80.8 | 211.9 | **S** |  |
| 128 | 払戻三連単2B | jvd_hr | haraimodoshi_sanrentan_2b | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 129 | 払戻三連単2C | jvd_hr | haraimodoshi_sanrentan_2c | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 130 | 払戻三連単3A | jvd_hr | haraimodoshi_sanrentan_3a | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 131 | 払戻三連単3B | jvd_hr | haraimodoshi_sanrentan_3b | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 132 | 払戻三連単3C | jvd_hr | haraimodoshi_sanrentan_3c | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 133 | 払戻三連単4A | jvd_hr | haraimodoshi_sanrentan_4a | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 134 | ラップタイム10F | jvd_wc | lap_time_10f | SKIP | 0.0% | - | - | - | - | **SKIP** | NO_JOIN |
| 135 | ラップタイム5F | jvd_wc | lap_time_5f | SKIP | 0.0% | - | - | - | - | **SKIP** | NO_JOIN |
| 136 | ラップタイム6F | jvd_wc | lap_time_6f | SKIP | 0.0% | - | - | - | - | **SKIP** | NO_JOIN |
| 137 | ラップタイム7F | jvd_wc | lap_time_7f | SKIP | 0.0% | - | - | - | - | **SKIP** | NO_JOIN |
| 138 | ラップタイム8F | jvd_wc | lap_time_8f | SKIP | 0.0% | - | - | - | - | **SKIP** | NO_JOIN |
| 139 | ラップタイム9F | jvd_wc | lap_time_9f | SKIP | 0.0% | - | - | - | - | **SKIP** | NO_JOIN |
| 140 | タイム合計10F | jvd_wc | time_gokei_10f | SKIP | 0.0% | - | - | - | - | **SKIP** | NO_JOIN |
| 141 | タイム合計5F | jvd_wc | time_gokei_5f | SKIP | 0.0% | - | - | - | - | **SKIP** | NO_JOIN |
| 142 | タイム合計6F | jvd_wc | time_gokei_6f | SKIP | 0.0% | - | - | - | - | **SKIP** | NO_JOIN |
| 143 | タイム合計7F | jvd_wc | time_gokei_7f | SKIP | 0.0% | - | - | - | - | **SKIP** | NO_JOIN |
| 144 | タイム合計8F | jvd_wc | time_gokei_8f | SKIP | 0.0% | - | - | - | - | **SKIP** | NO_JOIN |
| 145 | タイム合計9F | jvd_wc | time_gokei_9f | SKIP | 0.0% | - | - | - | - | **SKIP** | NO_JOIN |
| 146 | 馬場回り | jvd_wc | babamawari | SKIP | 0.0% | - | - | - | - | **SKIP** | NO_JOIN |
| 147 | 調教時刻 | jvd_wc | chokyo_jikoku | SKIP | 0.0% | - | - | - | - | **SKIP** | NO_JOIN |
| 148 | 調教年月日 | jvd_wc | chokyo_nengappi | SKIP | 0.0% | - | - | - | - | **SKIP** | NO_JOIN |
| 149 | コース | jvd_wc | course | SKIP | 0.0% | - | - | - | - | **SKIP** | NO_JOIN |
| 150 | トラセン区分 | jvd_wc | tracen_kubun | SKIP | 0.0% | - | - | - | - | **SKIP** | NO_JOIN |
| 151 | データ作成時分 | jvd_dm | data_sakusei_jifun | SKIP | 0.0% | - | - | - | - | **SKIP** | ADMINISTRATIVE |
| 152 | マイニング予想01 | jvd_dm | mining_yoso_01 | NUMERIC | 0.4% | 34 | 81 | 92.0 | 222.4 | **S** |  |
| 153 | マイニング予想02 | jvd_dm | mining_yoso_02 | NUMERIC | 0.4% | 35 | 83 | 92.0 | 222.5 | **S** |  |
| 154 | マイニング予想03 | jvd_dm | mining_yoso_03 | NUMERIC | 0.4% | 36 | 83 | 92.0 | 222.6 | **S** |  |
| 155 | マイニング予想04 | jvd_dm | mining_yoso_04 | NUMERIC | 0.4% | 36 | 83 | 92.0 | 222.7 | **S** |  |
| 156 | マイニング予想05 | jvd_dm | mining_yoso_05 | NUMERIC | 0.4% | 37 | 83 | 92.0 | 222.6 | **S** |  |
| 157 | マイニング予想06 | jvd_dm | mining_yoso_06 | NUMERIC | 0.4% | 36 | 82 | 92.0 | 222.5 | **S** |  |
| 158 | マイニング予想15 | jvd_dm | mining_yoso_15 | NUMERIC | 0.4% | 40 | 73 | 92.0 | 245.0 | **S** |  |
| 159 | マイニング予想16 | jvd_dm | mining_yoso_16 | NUMERIC | 0.4% | 37 | 72 | 93.8 | 245.6 | **S** |  |
| 160 | マイニング予想17 | jvd_dm | mining_yoso_17 | NUMERIC | 0.4% | 21 | 40 | 82.8 | 244.0 | **S** |  |
| 161 | マイニング予想18 | jvd_dm | mining_yoso_18 | NUMERIC | 0.4% | 21 | 39 | 87.2 | 244.7 | **S** |  |
| 162 | マイニング予想07 | jvd_dm | mining_yoso_07 | NUMERIC | 0.4% | 35 | 83 | 92.0 | 222.8 | **S** |  |
| 163 | マイニング予想08 | jvd_dm | mining_yoso_08 | NUMERIC | 0.4% | 31 | 83 | 92.0 | 223.6 | **S** |  |
| 164 | マイニング予想09 | jvd_dm | mining_yoso_09 | NUMERIC | 0.4% | 37 | 83 | 92.0 | 226.7 | **S** |  |
| 165 | マイニング予想10 | jvd_dm | mining_yoso_10 | NUMERIC | 0.4% | 38 | 82 | 92.0 | 229.5 | **S** |  |
| 166 | マイニング予想11 | jvd_dm | mining_yoso_11 | NUMERIC | 0.4% | 41 | 82 | 92.0 | 233.3 | **S** |  |
| 167 | 馬名欧字 | jvd_um | bamei_eur | SKIP | 0.0% | - | - | - | - | **SKIP** | IDENTIFIER |
| 168 | 馬名半角カナ | jvd_um | bamei_hankaku_kana | SKIP | 0.0% | - | - | - | - | **SKIP** | IDENTIFIER |
| 169 | ダート不良適性 | jvd_um | dirt_furyo | NUMERIC | 0.0% | 29 | 60 | 88.1 | 217.4 | **S** |  |
| 170 | ダート左回り適性 | jvd_um | dirt_hidari | NUMERIC | 0.0% | 42 | 108 | 140.0 | 231.4 | **S** |  |
| 171 | ダート長距離適性 | jvd_um | dirt_long | NUMERIC | 0.0% | 14 | 30 | 80.8 | 211.9 | **S** |  |
| 172 | ダート中距離適性 | jvd_um | dirt_middle | NUMERIC | 0.0% | 36 | 88 | 112.9 | 241.1 | **S** |  |
| 173 | ダート重適性 | jvd_um | dirt_omo | NUMERIC | 0.0% | 36 | 90 | 94.2 | 223.3 | **S** |  |
| 174 | ダート良適性 | jvd_um | dirt_ryo | NUMERIC | 0.0% | 33 | 120 | 113.3 | 253.3 | **S** |  |
| 175 | ダート短距離適性 | jvd_um | dirt_short | NUMERIC | 0.0% | 39 | 108 | 123.2 | 235.4 | **S** |  |
| 176 | ダートやや重適性 | jvd_um | dirt_yayaomo | NUMERIC | 0.0% | 32 | 90 | 98.4 | 216.5 | **S** |  |
| 177 | 血統情報12A | jvd_um | ketto_joho_12a | CATEGORY | 0.0% | 867 | 1994 | 166.6 | 506.2 | **S** |  |
| 178 | 血統情報12B | jvd_um | ketto_joho_12b | CATEGORY | 0.0% | 865 | 1992 | 166.6 | 506.2 | **S** |  |
| 179 | 血統情報13A | jvd_um | ketto_joho_13a | CATEGORY | 0.0% | 1280 | 2850 | 178.1 | 308.8 | **S** |  |
| 180 | 血統情報13B | jvd_um | ketto_joho_13b | CATEGORY | 0.0% | 1277 | 2847 | 178.1 | 308.8 | **S** |  |
| 181 | 品種コード | jvd_sk | hinshu_code | CATEGORY | 0.1% | 16 | 31 | 96.3 | 211.9 | **S** |  |
| 182 | 血統情報01A（父） | jvd_sk | ketto_joho_01a | CATEGORY | 0.1% | 1960 | 4232 | 176.8 | 1408.9 | **S** |  |
| 183 | 血統情報02A（母） | jvd_sk | ketto_joho_02a | CATEGORY | 0.1% | 5251 | 10775 | 322.3 | 2542.2 | **S** |  |
| 184 | 血統情報03A（父父） | jvd_sk | ketto_joho_03a | CATEGORY | 0.1% | 1133 | 2389 | 167.1 | 1408.9 | **S** |  |
| 185 | 血統情報04A（父母） | jvd_sk | ketto_joho_04a | CATEGORY | 0.1% | 1875 | 4037 | 176.8 | 1408.9 | **S** |  |
| 186 | 血統情報05A（母父） | jvd_sk | ketto_joho_05a | CATEGORY | 0.1% | 970 | 2208 | 166.6 | 506.2 | **S** |  |
| 187 | 血統情報06A（母母） | jvd_sk | ketto_joho_06a | CATEGORY | 0.1% | 4830 | 10105 | 309.2 | 606.4 | **S** |  |
| 188 | 血統情報07A（3代父） | jvd_sk | ketto_joho_07a | CATEGORY | 0.1% | 724 | 1539 | 154.3 | 1408.9 | **S** |  |
| 189 | 生年月日 | jvd_sk | seinengappi | SKIP | 0.0% | - | - | - | - | **SKIP** | IDENTIFIER |
| 190 | 血統情報08A | jvd_sk | ketto_joho_08a | CATEGORY | 0.1% | 1107 | 2321 | 167.1 | 1408.9 | **S** |  |
| 191 | 血統情報09A | jvd_sk | ketto_joho_09a | CATEGORY | 0.1% | 1415 | 2970 | 165.7 | 386.9 | **S** |  |
| 192 | 血統情報10A | jvd_sk | ketto_joho_10a | CATEGORY | 0.1% | 1869 | 4005 | 176.8 | 1408.9 | **S** |  |
| 193 | ラップタイム1F | jvd_hc | lap_time_1f | SKIP | 0.0% | - | - | - | - | **SKIP** | NO_JOIN |
| 194 | ラップタイム2F | jvd_hc | lap_time_2f | SKIP | 0.0% | - | - | - | - | **SKIP** | NO_JOIN |
| 195 | ラップタイム3F | jvd_hc | lap_time_3f | SKIP | 0.0% | - | - | - | - | **SKIP** | NO_JOIN |
| 196 | ラップタイム4F | jvd_hc | lap_time_4f | SKIP | 0.0% | - | - | - | - | **SKIP** | NO_JOIN |
| 197 | タイム合計2F | jvd_hc | time_gokei_2f | SKIP | 0.0% | - | - | - | - | **SKIP** | NO_JOIN |
| 198 | タイム合計3F | jvd_hc | time_gokei_3f | SKIP | 0.0% | - | - | - | - | **SKIP** | NO_JOIN |
| 199 | タイム合計4F | jvd_hc | time_gokei_4f | SKIP | 0.0% | - | - | - | - | **SKIP** | NO_JOIN |
| 200 | 発売フラグ三連単 | jvd_h6 | hatsubai_flag_sanrentan | CATEGORY | 0.5% | 14 | 30 | 80.8 | 211.5 | **S** |  |
| 201 | レコードID | jvd_h6 | record_id | SKIP | 0.0% | - | - | - | - | **SKIP** | IDENTIFIER |
| 202 | 登録頭数 | jvd_h6 | toroku_tosu | NUMERIC | 0.5% | 45 | 98 | 84.4 | 240.1 | **S** |  |
| 203 | 返還票数合計三連単 | jvd_h6 | henkan_hyosu_gokei_sanrentan | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 204 | 票数合計三連単 | jvd_h6 | hyosu_gokei_sanrentan | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 205 | 票数三連単 | jvd_h6 | hyosu_sanrentan | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 206 | 除外状態区分 | jvd_jg | jogai_jotai_kubun | CATEGORY | 0.0% | 14 | 30 | 80.8 | 211.9 | **S** |  |
| 207 | 出走区分 | jvd_jg | shusso_kubun | CATEGORY | 0.0% | 27 | 53 | 151.6 | 211.7 | **S** |  |
| 208 | 出馬投票受付 | jvd_jg | shutsuba_tohyo_uketsuke | SKIP | 0.0% | - | - | - | - | **SKIP** | ADMINISTRATIVE |
| 209 | 東西所属コード | jvd_ch | tozai_shozoku_code | CATEGORY | 0.0% | 36 | 72 | 95.4 | 219.9 | **S** |  |
| 210 | IDM（スピード指数） | jrd_kyi | idm | NUMERIC | 0.1% | 137 | 300 | 185.0 | 511.3 | **S** |  |
| 211 | 情報指数 | jrd_kyi | joho_shisu | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 212 | 騎手指数 | jrd_kyi | kishu_shisu | NUMERIC | 0.1% | 140 | 298 | 179.5 | 1644.2 | **S** |  |
| 213 | 上がり指数 | jrd_kyi | agari_shisu | NUMERIC | 0.1% | 147 | 300 | 137.2 | 424.2 | **S** |  |
| 214 | 調教指数 | jrd_kyi | chokyo_shisu | NUMERIC | 0.1% | 142 | 300 | 150.4 | 913.3 | **S** |  |
| 215 | 激走指数 | jrd_kyi | gekiso_shisu | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 216 | 位置取り指数 | jrd_kyi | ichi_shisu | NUMERIC | 0.1% | 141 | 300 | 110.0 | 293.4 | **S** |  |
| 217 | 厩舎指数 | jrd_kyi | kyusha_shisu | NUMERIC | 0.1% | 155 | 300 | 124.7 | 615.6 | **S** |  |
| 218 | 万券指数 | jrd_kyi | manken_shisu | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 219 | ペース指数 | jrd_kyi | pace_shisu | NUMERIC | 0.1% | 158 | 300 | 124.2 | 278.0 | **S** |  |
| 220 | 総合指数 | jrd_kyi | sogo_shisu | NUMERIC | 0.1% | 137 | 300 | 208.7 | 697.1 | **S** |  |
| 221 | テン指数 | jrd_kyi | ten_shisu | NUMERIC | 0.1% | 145 | 300 | 110.7 | 257.0 | **S** |  |
| 222 | 馬スタート指数 | jrd_kyi | uma_start_shisu | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 223 | 上がり指数順位 | jrd_kyi | agari_shisu_juni | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 224 | 道中順位 | jrd_kyi | dochu_juni | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 225 | 道中差 | jrd_kyi | dochu_sa | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 226 | 道中内外 | jrd_kyi | dochu_uchisoto | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 227 | 激走順位 | jrd_kyi | gekiso_juni | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 228 | ゴール順位 | jrd_kyi | goal_juni | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 229 | ゴール差 | jrd_kyi | goal_sa | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 230 | ゴール内外 | jrd_kyi | goal_uchisoto | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 231 | 位置取り指数順位 | jrd_kyi | ichi_shisu_juni | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 232 | 後半3F順位 | jrd_kyi | kohan_3f_juni | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 233 | 後半3F差 | jrd_kyi | kohan_3f_sa | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 234 | 後半3F内外 | jrd_kyi | kohan_3f_uchisoto | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 235 | LS指数順位 | jrd_kyi | ls_shisu_juni | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 236 | ペース指数順位 | jrd_kyi | pace_shisu_juni | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 237 | テン指数順位 | jrd_kyi | ten_shisu_juni | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 238 | 調教矢印コード | jrd_kyi | chokyo_yajirushi_code | CATEGORY | 0.1% | 56 | 114 | 186.4 | 471.6 | **S** |  |
| 239 | クラスコード | jrd_kyi | class_code | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 240 | 激走タイプ | jrd_kyi | gekiso_type | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 241 | 蹄コード | jrd_kyi | hizume_code | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 242 | 放牧先 | jrd_kyi | hobokusaki | SKIP | 0.0% | - | - | - | - | **SKIP** | IDENTIFIER |
| 243 | 放牧先ランク | jrd_kyi | hobokusaki_rank | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 244 | 上昇度コード | jrd_kyi | joshodo_code | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 245 | 騎手コード | jrd_kyi | kishu_code | CATEGORY | 0.1% | 1526 | 3223 | 231.0 | 2878.3 | **S** |  |
| 246 | 脚質コード | jrd_kyi | kyakushitsu | CATEGORY | 0.1% | 73 | 147 | 109.0 | 255.6 | **S** |  |
| 247 | 距離適性コード | jrd_kyi | kyori_tekisei | CATEGORY | 0.1% | 86 | 152 | 113.7 | 240.7 | **S** |  |
| 248 | 厩舎評価コード | jrd_kyi | kyusha_hyoka_code | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 249 | 厩舎ランク | jrd_kyi | kyusha_rank | CATEGORY | 0.1% | 106 | 220 | 146.4 | 256.5 | **S** |  |
| 250 | 休養理由分類コード | jrd_kyi | kyuyo_riyu_bunrui_code | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 251 | 万券印 | jrd_kyi | manken_shirushi | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 252 | ペース予想 | jrd_kyi | pace_yoso | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 253 | 適性コード（重） | jrd_kyi | omo_tekisei_code | CATEGORY | 0.1% | 57 | 115 | 106.0 | 238.3 | **S** |  |
| 254 | 輸送区分 | jrd_kyi | yuso_kubun | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 255 | 獲得賞金累計 | jrd_kyi | kakutoku_shokin_ruikei | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 256 | 基準人気順（複勝） | jrd_kyi | kijun_ninkijun_fukusho | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 257 | 基準人気順（単勝） | jrd_kyi | kijun_ninkijun_tansho | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 258 | 基準オッズ（複勝） | jrd_kyi | kijun_odds_fukusho | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 259 | 基準オッズ（単勝） | jrd_kyi | kijun_odds_tansho | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 260 | 取得賞金累計 | jrd_kyi | shutoku_shokin_ruikei | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 261 | 騎手期待連対率 | jrd_kyi | kishu_kitai_rentai_ritsu | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 262 | 騎手期待3着内率 | jrd_kyi | kishu_kitai_sanchakunai_ritsu | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 263 | 騎手期待単勝率 | jrd_kyi | kishu_kitai_tansho_ritsu | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 264 | 体型（テキスト） | jrd_kyi | taikei | SKIP | 0.0% | - | - | - | - | **SKIP** | IDENTIFIER |
| 265 | 体型総合1 | jrd_kyi | taikei_sogo_1 | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 266 | 体型総合2 | jrd_kyi | taikei_sogo_2 | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 267 | 体型総合3 | jrd_kyi | taikei_sogo_3 | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 268 | 馬特記1 | jrd_kyi | uma_tokki_1 | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 269 | 馬特記2 | jrd_kyi | uma_tokki_2 | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 270 | 馬特記3 | jrd_kyi | uma_tokki_3 | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 271 | 血統登録番号 | jrd_kyi | ketto_toroku_bango | SKIP | 0.0% | - | - | - | - | **SKIP** | IDENTIFIER |
| 272 | レース施行年 | jrd_kyi | race_shikonen | SKIP | 0.0% | - | - | - | - | **SKIP** | IDENTIFIER |
| 273 | 負担重量（斤量） | jrd_kyi | futan_juryo | NUMERIC | 0.1% | 69 | 150 | 95.5 | 216.5 | **S** |  |
| 274 | 馬出遅れ率 | jrd_kyi | uma_deokure_ritsu | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 275 | 調教コメント | jrd_cyb | chokyo_comment | SKIP | 0.0% | - | - | - | - | **SKIP** | IDENTIFIER |
| 276 | 調教コースダートタイム | jrd_cyb | chokyo_corse_dirt | NUMERIC | 100.0% | - | - | - | - | **SKIP** | HIGH_NULL(100.0%) |
| 277 | 調教コース坂路タイム | jrd_cyb | chokyo_corse_hanro | NUMERIC | 100.0% | - | - | - | - | **SKIP** | HIGH_NULL(100.0%) |
| 278 | 調教ポリトラックタイム | jrd_cyb | chokyo_corse_polytrack | NUMERIC | 100.0% | - | - | - | - | **SKIP** | HIGH_NULL(100.0%) |
| 279 | 調教プール | jrd_cyb | chokyo_corse_pool | NUMERIC | 100.0% | - | - | - | - | **SKIP** | HIGH_NULL(100.0%) |
| 280 | 調教コース芝タイム | jrd_cyb | chokyo_corse_shiba | NUMERIC | 100.0% | - | - | - | - | **SKIP** | HIGH_NULL(100.0%) |
| 281 | 調教コース種別 | jrd_cyb | chokyo_corse_shubetsu | CATEGORY | 100.0% | - | - | - | - | **SKIP** | HIGH_NULL(100.0%) |
| 282 | 調教評価 | jrd_cyb | chokyo_hyoka | CATEGORY | 100.0% | - | - | - | - | **SKIP** | HIGH_NULL(100.0%) |
| 283 | 調教評価2 | jrd_cyb | chokyo_hyoka_2 | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 284 | 調教評価3 | jrd_cyb | chokyo_hyoka_3 | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 285 | 調教重点 | jrd_cyb | chokyo_juten | CATEGORY | 100.0% | - | - | - | - | **SKIP** | HIGH_NULL(100.0%) |
| 286 | 調教タイプ | jrd_cyb | chokyo_type | CATEGORY | 100.0% | - | - | - | - | **SKIP** | HIGH_NULL(100.0%) |
| 287 | 調教師コード（CYB） | jrd_cyb | chokyoshi_code | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 288 | 調教師名（CYB） | jrd_cyb | chokyoshi_mei | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 289 | 調種別 | jrd_cyb | choshubetsu | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 290 | 追切指数 | jrd_cyb | oikiri_shisu | NUMERIC | 100.0% | - | - | - | - | **SKIP** | HIGH_NULL(100.0%) |
| 291 | 作者評価F/H | jrd_cyb | sakusha_hyoka_f_h | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 292 | 仕上指数 | jrd_cyb | shiage_shisu | NUMERIC | 100.0% | - | - | - | - | **SKIP** | HIGH_NULL(100.0%) |
| 293 | 馬場差 | jrd_sed | babasa | NUMERIC | 100.0% | - | - | - | - | **SKIP** | HIGH_NULL(100.0%) |
| 294 | 馬体重増減（JRDB） | jrd_sed | bataiju_zogen | NUMERIC | 100.0% | - | - | - | - | **SKIP** | HIGH_NULL(100.0%) |
| 295 | 1コーナー通過順位 | jrd_sed | corner_tsuka_juni_1 | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 296 | 2コーナー通過順位 | jrd_sed | corner_tsuka_juni_2 | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 297 | 3コーナー通過順位 | jrd_sed | corner_tsuka_juni_3 | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 298 | 4コーナー通過順位 | jrd_sed | corner_tsuka_juni_4 | SKIP | 0.0% | - | - | - | - | **SKIP** | TARGET_LEAK |
| 299 | 振り（スタート） | jrd_sed | furi | CATEGORY | 100.0% | - | - | - | - | **SKIP** | HIGH_NULL(100.0%) |
| 300 | レコードID（SED） | jrd_sed | id | SKIP | 0.0% | - | - | - | - | **SKIP** | IDENTIFIER |
| 301 | ペース | jrd_sed | pace | CATEGORY | 100.0% | - | - | - | - | **SKIP** | HIGH_NULL(100.0%) |
| 302 | ペース指数（SED） | jrd_sed | pace_shisu | NUMERIC | 100.0% | - | - | - | - | **SKIP** | HIGH_NULL(100.0%) |
| 303 | レースコメント | jrd_sed | race_comments | SKIP | 0.0% | - | - | - | - | **SKIP** | IDENTIFIER |
| 304 | レースペース | jrd_sed | race_pace | CATEGORY | 100.0% | - | - | - | - | **SKIP** | HIGH_NULL(100.0%) |
| 305 | レースペース走者 | jrd_sed | race_pace_runner | SKIP | 0.0% | - | - | - | - | **SKIP** | IDENTIFIER |
| 306 | レースペンタイプ | jrd_sed | race_pen_type | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 307 | CID | jrd_joa | cid | SKIP | 0.0% | - | - | - | - | **SKIP** | IDENTIFIER |
| 308 | EM評価 | jrd_joa | em | CATEGORY | 100.0% | - | - | - | - | **SKIP** | HIGH_NULL(100.0%) |
| 309 | 騎手×馬主◎◎単勝回収率 | jrd_joa | jockey_banushi_nijumaru_tansho_kaishuritsu | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 310 | 騎手◎印 | jrd_joa | kishu_bb_shirushi | CATEGORY | 100.0% | - | - | - | - | **SKIP** | HIGH_NULL(100.0%) |
| 311 | 厩舎◎印 | jrd_joa | kyusha_bb_shirushi | CATEGORY | 100.0% | - | - | - | - | **SKIP** | HIGH_NULL(100.0%) |
| 312 | 厩舎◎◎単勝回収率 | jrd_joa | kyusha_bb_nijumaru_tansho_kaishuritsu | NUMERIC | 100.0% | - | - | - | - | **SKIP** | HIGH_NULL(100.0%) |
| 313 | LS評価 | jrd_joa | ls_hyoka | CATEGORY | 100.0% | - | - | - | - | **SKIP** | HIGH_NULL(100.0%) |
| 314 | LS指数 | jrd_joa | ls_shisu | NUMERIC | 100.0% | - | - | - | - | **SKIP** | HIGH_NULL(100.0%) |
| 315 | テン指数（JOA） | jrd_joa | ten_shisu | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 316 | 馬ぐっち | jrd_joa | uma_gucchi | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 317 | 馬券発売フラグ | jrd_bac | baken_hatsubai_flag | CATEGORY | 100.0% | - | - | - | - | **SKIP** | HIGH_NULL(100.0%) |
| 318 | 馬場差最終時刻 | jrd_bac | baba_sa_saishujikoku | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 319 | 付加賞金（BAC） | jrd_bac | fukashokin | NUMERIC | 100.0% | - | - | - | - | **SKIP** | HIGH_NULL(100.0%) |
| 320 | 本賞金 | jrd_bac | honshokin | NUMERIC | 100.0% | - | - | - | - | **SKIP** | HIGH_NULL(100.0%) |
| 321 | 開催年月日 | jrd_bac | kaisai_nen_gappi | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 322 | 競走条件 | jrd_bac | kyoso_joken | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 323 | レースコード前半 | jrd_bac | race_code_zenhan | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 324 | レースコメント（BAC） | jrd_bac | race_comment | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |
| 325 | トラック馬場差 | jrd_bac | track_baba_sa | SKIP | 0.0% | - | - | - | - | **SKIP** | NOT_IN_DB |

---

## 統計サマリー

- **総ファクター数**: 325
- **分析実施**: 93
- **スキップ**: 232
- **S評価**: 93
- **A評価**: 0
- **B評価**: 0
- **C評価（不採用）**: 0

### スキップ理由内訳

- NOT_IN_DB: 61件
- TARGET_LEAK: 54件
- HIGH_NULL(100.0%): 28件
- IDENTIFIER: 27件
- NO_JOIN: 24件
- COLUMN_NOT_IN_DATA: 9件
- ADMINISTRATIVE: 6件
- REDUNDANT: 5件
- HIGH_NULL(78.9%): 1件
- HIGH_NULL(76.2%): 1件
- HIGH_NULL(88.9%): 1件
- HIGH_NULL(68.0%): 1件
- HIGH_NULL(95.0%): 1件
- HIGH_NULL(96.7%): 1件
- HIGH_NULL(98.5%): 1件
- HIGH_NULL(99.7%): 1件
- HIGH_NULL(82.8%): 1件
- HIGH_NULL(91.2%): 1件
- HIGH_NULL(96.1%): 1件
- HIGH_NULL(80.5%): 1件
- HIGH_NULL(91.6%): 1件
- HIGH_NULL(83.1%): 1件
- HIGH_NULL(54.3%): 1件
- HIGH_NULL(81.4%): 1件
- HIGH_NULL(70.7%): 1件
- HIGH_NULL(74.8%): 1件