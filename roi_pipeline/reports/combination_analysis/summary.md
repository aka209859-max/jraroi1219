# 組み合わせファクター分析 サマリー

- **分析期間**: 20160101 〜 20251231
- **生成日時**: 2026-04-14 15:24:05
- **組み合わせ総数**: 40
  - 実行: 34  |  スキップ: 6

## 実行組み合わせ（エッジビン降順）

| ID | 名称 | セグメント | 単勝エッジビン | 複勝エッジビン | 有効ビン数 |
|---|---|---|---|---|---|
| [course27_08](course27_08.md) | 母父名（hahachichi_name） | COURSE_27 | **1340** | **920** | 2822 |
| [course27_09](course27_09.md) | 種牡馬名（chichi_name） | COURSE_27 | **1322** | **934** | 2901 |
| [surface2_15](surface2_15.md) | 騎手名 × 枠番 | SURFACE_2 | **1065** | **704** | 2275 |
| [global_04](global_04.md) | LS指数4刻み × 予想脚質 × 位置指数 | GLOBAL | **855** | **740** | 1877 |
| [ktk_01](ktk_01.md) | 馬番（umaban） | KEIBAJO_TRACK_KYORI | **722** | **433** | 1508 |
| [surface2_06](surface2_06.md) | 総合指数（kyi_sogo_shisu） | SURFACE_2 | **611** | **446** | 1269 |
| [surface2_04](surface2_04.md) | IDM指数（kyi_idm） | SURFACE_2 | **503** | **422** | 1084 |
| [surface2_14](surface2_14.md) | 重不良馬場フラグ × 種牡馬名 | SURFACE_2 | **498** | **304** | 1052 |
| [surface2_07](surface2_07.md) | 厩舎指数（kyi_kyusha_shisu） | SURFACE_2 | **486** | **377** | 1010 |
| [ktk_02](ktk_02.md) | 枠番（wakuban） | KEIBAJO_TRACK_KYORI | **404** | **235** | 845 |
| [surface2_09](surface2_09.md) | 調教指数（kyi_chokyo_shisu） | SURFACE_2 | **390** | **342** | 777 |
| [surface2_13](surface2_13.md) | 重不良馬場フラグ × 騎手名 | SURFACE_2 | **320** | **191** | 693 |
| [course27_01](course27_01.md) | 前走着順（同コース前走1） | COURSE_27 | **205** | **118** | 439 |
| [course27_04](course27_04.md) | 前走4角順位（同コース前走1） | COURSE_27 | **193** | **123** | 432 |
| [course27_02](course27_02.md) | 2前走着順（同コース前走2） | COURSE_27 | **173** | **125** | 392 |
| [course27_03](course27_03.md) | 3前走着順（同コース前走3） | COURSE_27 | **144** | **120** | 339 |
| [surface2_16](surface2_16.md) | 前走競馬場 × 今走競馬場 | SURFACE_2 | **105** | **49** | 200 |
| [course27_06](course27_06.md) | 前走馬体重20kg刻みビン（同コース前走1） | COURSE_27 | **93** | **51** | 209 |
| [s2_keibajo_01](s2_keibajo_01.md) | 休養週数ビン（SURFACE_2 × keibajo_code 別） | SURFACE_2_KEIBAJO | **81** | **29** | 140 |
| [surface2_11](surface2_11.md) | 予想脚質 × 前走4角順位グループ × 馬齢 | SURFACE_2 | **75** | **35** | 170 |
| [course27_05](course27_05.md) | 前走ブリンカー×今走ブリンカー（同コース） | COURSE_27 | **46** | **36** | 103 |
| [global_06](global_06.md) | 負担重量 × 予想脚質 | GLOBAL | **39** | **10** | 75 |
| [surface2_08](surface2_08.md) | 騎手指数（kyi_kishu_shisu） | SURFACE_2 | **32** | **35** | 76 |
| [course27_07](course27_07.md) | 距離増減（同コース: 今走距離 vs 同コース前走距離） | COURSE_27 | **31** | **10** | 68 |
| [course27_13](course27_13.md) | 東西区分コード（tozai_shozoku_code） | COURSE_27 | **31** | **5** | 62 |
| [surface2_12](surface2_12.md) | 産地名（sanchimei） | SURFACE_2 | **31** | **8** | 64 |
| [global_07](global_07.md) | 予想矢印コード（chokyo_yajirushi_code） | GLOBAL | **2** | **2** | 5 |
| [course27_11](course27_11.md) | 調教師ランク S/A/B/C/D | COURSE_27 | **0** | **0** | 0 |
| [course27_12](course27_12.md) | 騎手ランク S/A/B/C/D | COURSE_27 | **0** | **0** | 0 |
| [surface2_01](surface2_01.md) | IDMランク × 前走タイム差 | SURFACE_2 | **0** | **0** | 0 |
| [surface2_02](surface2_02.md) | 前走race_pace × 前走実脚質 × 今走予想脚質 | SURFACE_2 | **0** | **0** | 0 |
| [global_01](global_01.md) | 調教師ランク × CID順位 | GLOBAL | **0** | **0** | 0 |
| [global_02](global_02.md) | 騎手ランク × CID順位 | GLOBAL | **0** | **0** | 0 |
| [global_03](global_03.md) | IDMランク × 基準オッズ5倍刻み | GLOBAL | **0** | **0** | 0 |

## スキップ組み合わせ

| ID | 名称 | セグメント | スキップ理由 |
|---|---|---|---|
| course27_10 | 前日_体型_胴（taikei） | COURSE_27 | jrd_kyi(raw)にデータなし（2026-03-14の517行のみ） |
| surface2_03 | 万券指数（manken_shisu） | SURFACE_2 | jrd_kyi(raw)にデータなし（2026-03-14の517行のみ） |
| surface2_05 | 激走指数（gekiso_shisu） | SURFACE_2 | jrd_kyi(raw)にデータなし（2026-03-14の517行のみ） |
| surface2_10 | 蹄コード（hizume_code） | SURFACE_2 | jrd_kyi(raw)にデータなし（2026-03-14の517行のみ） |
| surface2_17 | 距離区分 × 今走_体型_トモ（taikei_sogo_1） | SURFACE_2 | jrd_kyi(raw)にデータなし（2026-03-14の517行のみ） |
| global_05 | 輸送区分（yuso_kubun） | GLOBAL | jrd_kyi(raw)にデータなし（2026-03-14の517行のみ） |
