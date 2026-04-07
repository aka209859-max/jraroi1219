"""
325ファクター完全カタログ（JRA-VAN 209列 + JRDB 116列）

各ファクターについて:
    - table: DBテーブル名
    - column: DBカラム名
    - alias: SQLでのエイリアス（重複回避）
    - kind: NUMERIC / CATEGORY / ORDINAL / SKIP
    - skip_reason: SKIPの場合の理由
    - description: 日本語説明
    - n_bins: NUMERIC型の分位数ビン数（デフォルト5）

SKIPカテゴリ:
    IDENTIFIER   → ID・名前・コード（予測に使わない識別子）
    TARGET_LEAK  → ターゲット情報（着順・着差・払戻金など）
    ADMINISTRATIVE → 管理用メタデータ
    REDUNDANT    → 他カラムと重複または変更前後のペア
    NO_JOIN      → 結合方法が未確定なテーブル
"""
from dataclasses import dataclass, field
from typing import Optional


@dataclass(frozen=True)
class Factor325:
    """325ファクターの1エントリ"""
    fid: int                          # 通し番号 1-325
    table: str                        # DBテーブル名
    column: str                       # DBカラム名
    alias: str                        # SQL alias（SELECT時のAS名）
    kind: str                         # NUMERIC / CATEGORY / ORDINAL / SKIP
    description: str = ""
    skip_reason: str = ""             # SKIPの場合の理由
    n_bins: int = 5                   # NUMERIC型の分位ビン数


# =============================================================================
# JVD_SE (40列)
# =============================================================================
_SE = [
    Factor325(1,  "jvd_se", "aiteuma_joho_1",       "se_aiteuma_joho_1",       "SKIP",     "相手馬情報1",           "TARGET_LEAK"),
    Factor325(2,  "jvd_se", "aiteuma_joho_2",       "se_aiteuma_joho_2",       "SKIP",     "相手馬情報2",           "TARGET_LEAK"),
    Factor325(3,  "jvd_se", "aiteuma_joho_3",       "se_aiteuma_joho_3",       "SKIP",     "相手馬情報3",           "TARGET_LEAK"),
    Factor325(4,  "jvd_se", "bamei",                "se_bamei",                "SKIP",     "馬名",                  "IDENTIFIER"),
    Factor325(5,  "jvd_se", "banushi_code",         "se_banushi_code",         "CATEGORY", "馬主コード"),
    Factor325(6,  "jvd_se", "banushimei",           "se_banushimei",           "SKIP",     "馬主名",                "IDENTIFIER"),
    Factor325(7,  "jvd_se", "barei",                "se_barei",                "ORDINAL",  "馬齢"),
    Factor325(8,  "jvd_se", "bataiju",              "se_bataiju",              "NUMERIC",  "馬体重（計量時）",       skip_reason=""),
    Factor325(9,  "jvd_se", "blinker_shiyo_kubun",  "se_blinker_shiyo_kubun",  "CATEGORY", "ブリンカー使用区分"),
    Factor325(10, "jvd_se", "chakusa_code_1",       "se_chakusa_code_1",       "SKIP",     "着差コード1",           "TARGET_LEAK"),
    Factor325(11, "jvd_se", "chakusa_code_2",       "se_chakusa_code_2",       "SKIP",     "着差コード2",           "TARGET_LEAK"),
    Factor325(12, "jvd_se", "chakusa_code_3",       "se_chakusa_code_3",       "SKIP",     "着差コード3",           "TARGET_LEAK"),
    Factor325(13, "jvd_se", "chokyoshi_code",       "se_chokyoshi_code",       "CATEGORY", "調教師コード"),
    Factor325(14, "jvd_se", "chokyoshimei_ryakusho","se_chokyoshimei_ryakusho","SKIP",     "調教師名略称",          "IDENTIFIER"),
    Factor325(15, "jvd_se", "corner_1",             "se_corner_1",             "SKIP",     "コーナー通過1",         "TARGET_LEAK"),
    Factor325(16, "jvd_se", "corner_2",             "se_corner_2",             "SKIP",     "コーナー通過2",         "TARGET_LEAK"),
    Factor325(17, "jvd_se", "corner_3",             "se_corner_3",             "SKIP",     "コーナー通過3",         "TARGET_LEAK"),
    Factor325(18, "jvd_se", "corner_4",             "se_corner_4",             "SKIP",     "コーナー通過4",         "TARGET_LEAK"),
    Factor325(19, "jvd_se", "data_kubun",           "se_data_kubun",           "SKIP",     "データ区分",            "ADMINISTRATIVE"),
    Factor325(20, "jvd_se", "kaisai_kai",           "se_kaisai_kai",           "SKIP",     "開催回",                "IDENTIFIER"),
    Factor325(21, "jvd_se", "kaisai_nen",           "se_kaisai_nen",           "SKIP",     "開催年",                "IDENTIFIER"),
    Factor325(22, "jvd_se", "kaisai_nichime",       "se_kaisai_nichime",       "ORDINAL",  "開催日目"),
    Factor325(23, "jvd_se", "kaisai_tsukihi",       "se_kaisai_tsukihi",       "SKIP",     "開催月日",              "IDENTIFIER"),
    Factor325(24, "jvd_se", "keibajo_code",         "se_keibajo_code",         "CATEGORY", "競馬場コード"),
    Factor325(25, "jvd_se", "kishumei_ryakusho",    "se_kishumei_ryakusho",    "SKIP",     "騎手名略称",            "IDENTIFIER"),
    Factor325(26, "jvd_se", "race_bango",           "se_race_bango",           "SKIP",     "レース番号",            "IDENTIFIER"),
    Factor325(27, "jvd_se", "seibetsu_code",        "se_seibetsu_code",        "CATEGORY", "性別コード"),
    Factor325(28, "jvd_se", "umaban",               "se_umaban",               "ORDINAL",  "馬番"),
    Factor325(29, "jvd_se", "wakuban",              "se_wakuban",              "ORDINAL",  "枠番"),
    Factor325(30, "jvd_se", "zogen_sa",             "se_zogen_sa",             "NUMERIC",  "馬体重増減差"),
    Factor325(31, "jvd_se", "dochaku_kubun",        "se_dochaku_kubun",        "SKIP",     "同着区分",              "TARGET_LEAK"),
    Factor325(32, "jvd_se", "dochaku_tosu",         "se_dochaku_tosu",         "SKIP",     "同着頭数",              "TARGET_LEAK"),
    Factor325(33, "jvd_se", "fukushoku_hyoji",      "se_fukushoku_hyoji",      "SKIP",     "服色表示",              "IDENTIFIER"),
    Factor325(34, "jvd_se", "futan_juryo_henkomae", "se_futan_juryo_henkomae", "NUMERIC",  "負担重量変更前"),
    Factor325(35, "jvd_se", "ijo_kubun_code",       "se_ijo_kubun_code",       "CATEGORY", "異常区分コード"),
    Factor325(36, "jvd_se", "yoso_soha_time",       "se_yoso_soha_time",       "NUMERIC",  "予想走破タイム"),
    Factor325(37, "jvd_se", "zogen_fugo",           "se_zogen_fugo",           "CATEGORY", "増減符号"),
    Factor325(38, "jvd_se", "umakigo_code",         "se_umakigo_code",         "CATEGORY", "馬記号コード"),
    Factor325(39, "jvd_se", "yobi_1",               "se_yobi_1",               "SKIP",     "予備1",                 "ADMINISTRATIVE"),
    Factor325(40, "jvd_se", "yobi_2",               "se_yobi_2",               "SKIP",     "予備2",                 "ADMINISTRATIVE"),
]

# =============================================================================
# JVD_RA (31列)
# =============================================================================
_RA = [
    Factor325(41, "jvd_ra", "babajotai_code_dirt",      "ra_babajotai_code_dirt",      "CATEGORY", "馬場状態コードダート"),
    Factor325(42, "jvd_ra", "babajotai_code_shiba",     "ra_babajotai_code_shiba",     "CATEGORY", "馬場状態コード芝"),
    Factor325(43, "jvd_ra", "corner_tsuka_juni_1",      "ra_corner_tsuka_juni_1",      "SKIP",     "コーナー通過順位1（レース集計）", "TARGET_LEAK"),
    Factor325(44, "jvd_ra", "corner_tsuka_juni_2",      "ra_corner_tsuka_juni_2",      "SKIP",     "コーナー通過順位2",              "TARGET_LEAK"),
    Factor325(45, "jvd_ra", "corner_tsuka_juni_3",      "ra_corner_tsuka_juni_3",      "SKIP",     "コーナー通過順位3",              "TARGET_LEAK"),
    Factor325(46, "jvd_ra", "corner_tsuka_juni_4",      "ra_corner_tsuka_juni_4",      "SKIP",     "コーナー通過順位4",              "TARGET_LEAK"),
    Factor325(47, "jvd_ra", "course_kubun",             "ra_course_kubun",             "CATEGORY", "コース区分"),
    Factor325(48, "jvd_ra", "course_kubun_henkomae",    "ra_course_kubun_henkomae",    "SKIP",     "コース区分変更前",               "REDUNDANT"),
    Factor325(49, "jvd_ra", "data_sakusei_nengappi",    "ra_data_sakusei_nengappi",    "SKIP",     "データ作成年月日",               "ADMINISTRATIVE"),
    Factor325(50, "jvd_ra", "fukashokin",               "ra_fukashokin",               "NUMERIC",  "付加賞金"),
    Factor325(51, "jvd_ra", "fukashokin_henkomae",      "ra_fukashokin_henkomae",      "SKIP",     "付加賞金変更前",                 "REDUNDANT"),
    Factor325(52, "jvd_ra", "grade_code",               "ra_grade_code",               "CATEGORY", "グレードコード"),
    Factor325(53, "jvd_ra", "grade_code_henkomae",      "ra_grade_code_henkomae",      "SKIP",     "グレードコード変更前",           "REDUNDANT"),
    Factor325(54, "jvd_ra", "hasso_jikoku",             "ra_hasso_jikoku",             "CATEGORY", "発走時刻"),
    Factor325(55, "jvd_ra", "kyori",                    "ra_kyori",                    "NUMERIC",  "距離"),
    Factor325(56, "jvd_ra", "shusso_tosu",              "ra_shusso_tosu",              "NUMERIC",  "出走頭数"),
    Factor325(57, "jvd_ra", "tenko_code",               "ra_tenko_code",               "CATEGORY", "天候コード"),
    Factor325(58, "jvd_ra", "track_code",               "ra_track_code",               "CATEGORY", "トラックコード（芝/ダ/向き）"),
    Factor325(59, "jvd_ra", "kohan_4f",                 "ra_kohan_4f",                 "SKIP",     "後半4F（レース結果）",           "TARGET_LEAK"),
    Factor325(60, "jvd_ra", "zenhan_3f",                "ra_zenhan_3f",                "SKIP",     "前半3F（レース結果）",           "TARGET_LEAK"),
    Factor325(61, "jvd_ra", "zenhan_4f",                "ra_zenhan_4f",                "SKIP",     "前半4F（レース結果）",           "TARGET_LEAK"),
    Factor325(62, "jvd_ra", "kyoso_joken_code_3sai",    "ra_kyoso_joken_code_3sai",    "CATEGORY", "競走条件コード3歳"),
    Factor325(63, "jvd_ra", "kyoso_joken_code_4sai",    "ra_kyoso_joken_code_4sai",    "CATEGORY", "競走条件コード4歳"),
    Factor325(64, "jvd_ra", "kyoso_joken_code_5sai_ijo","ra_kyoso_joken_code_5sai_ijo","CATEGORY", "競走条件コード5歳以上"),
    Factor325(65, "jvd_ra", "kyoso_joken_meisho",       "ra_kyoso_joken_meisho",       "SKIP",     "競走条件名称",                   "IDENTIFIER"),
    Factor325(66, "jvd_ra", "kyoso_kigo_code",          "ra_kyoso_kigo_code",          "CATEGORY", "競走記号コード"),
    Factor325(67, "jvd_ra", "kyoso_shubetsu_code",      "ra_kyoso_shubetsu_code",      "CATEGORY", "競走種別コード"),
    Factor325(68, "jvd_ra", "kyosomei_fukudai",         "ra_kyosomei_fukudai",         "SKIP",     "競走名副題",                     "IDENTIFIER"),
    Factor325(69, "jvd_ra", "kyosomei_fukudai_eur",     "ra_kyosomei_fukudai_eur",     "SKIP",     "競走名副題欧字",                 "IDENTIFIER"),
    Factor325(70, "jvd_ra", "hasso_jikoku_henkomae",    "ra_hasso_jikoku_henkomae",    "SKIP",     "発走時刻変更前",                 "REDUNDANT"),
    Factor325(71, "jvd_ra", "honshokin_henkomae",       "ra_honshokin_henkomae",       "SKIP",     "本賞金変更前",                   "REDUNDANT"),
]

# =============================================================================
# JVD_CK (23列)
# =============================================================================
_CK = [
    Factor325(72, "jvd_ck", "banushimei_hojinkaku",  "ck_banushimei_hojinkaku",  "SKIP",    "馬主名法人格",           "IDENTIFIER"),
    Factor325(73, "jvd_ck", "chokyoshimei",          "ck_chokyoshimei",          "SKIP",    "調教師名",               "IDENTIFIER"),
    Factor325(74, "jvd_ck", "chuo_gokei",            "ck_chuo_gokei",            "NUMERIC", "中央合計成績"),
    Factor325(75, "jvd_ck", "dirt_1200_ika",         "ck_dirt_1200_ika",         "NUMERIC", "ダート1200m以下成績"),
    Factor325(76, "jvd_ck", "dirt_1201_1400",        "ck_dirt_1201_1400",        "NUMERIC", "ダート1201-1400m成績"),
    Factor325(77, "jvd_ck", "dirt_1401_1600",        "ck_dirt_1401_1600",        "NUMERIC", "ダート1401-1600m成績"),
    Factor325(78, "jvd_ck", "dirt_1601_1800",        "ck_dirt_1601_1800",        "NUMERIC", "ダート1601-1800m成績"),
    Factor325(79, "jvd_ck", "dirt_1801_2000",        "ck_dirt_1801_2000",        "NUMERIC", "ダート1801-2000m成績"),
    Factor325(80, "jvd_ck", "dirt_2001_2200",        "ck_dirt_2001_2200",        "NUMERIC", "ダート2001-2200m成績"),
    Factor325(81, "jvd_ck", "dirt_2201_2400",        "ck_dirt_2201_2400",        "NUMERIC", "ダート2201-2400m成績"),
    Factor325(82, "jvd_ck", "dirt_2401_2800",        "ck_dirt_2401_2800",        "NUMERIC", "ダート2401-2800m成績"),
    Factor325(83, "jvd_ck", "dirt_2801_ijo",         "ck_dirt_2801_ijo",         "NUMERIC", "ダート2801m以上成績"),
    Factor325(84, "jvd_ck", "dirt_choku",            "ck_dirt_choku",            "NUMERIC", "ダート直線成績"),
    Factor325(85, "jvd_ck", "dirt_chukyo",           "ck_dirt_chukyo",           "NUMERIC", "ダート中京成績"),
    Factor325(86, "jvd_ck", "dirt_fukushima",        "ck_dirt_fukushima",        "NUMERIC", "ダート福島成績"),
    Factor325(87, "jvd_ck", "dirt_hakodate",         "ck_dirt_hakodate",         "NUMERIC", "ダート函館成績"),
    Factor325(88, "jvd_ck", "dirt_hanshin",          "ck_dirt_hanshin",          "NUMERIC", "ダート阪神成績"),
    Factor325(89, "jvd_ck", "dirt_kokura",           "ck_dirt_kokura",           "NUMERIC", "ダート小倉成績"),
    Factor325(90, "jvd_ck", "dirt_kyoto",            "ck_dirt_kyoto",            "NUMERIC", "ダート京都成績"),
    Factor325(91, "jvd_ck", "dirt_migi",             "ck_dirt_migi",             "NUMERIC", "ダート右回り成績"),
    Factor325(92, "jvd_ck", "shiba_1201_1400",       "ck_shiba_1201_1400",       "NUMERIC", "芝1201-1400m成績"),
    Factor325(93, "jvd_ck", "shiba_1401_1600",       "ck_shiba_1401_1600",       "NUMERIC", "芝1401-1600m成績"),
    Factor325(94, "jvd_ck", "shiba_1601_1800",       "ck_shiba_1601_1800",       "NUMERIC", "芝1601-1800m成績"),
]

# =============================================================================
# JVD_H1 (21列) - 馬券発売・払戻集計 → TARGET_LEAK
# =============================================================================
_H1 = [
    Factor325(95,  "jvd_h1", "fukusho_chakubarai_key",       "h1_fukusho_chakubarai_key",       "SKIP", "複勝着払いキー",      "TARGET_LEAK"),
    Factor325(96,  "jvd_h1", "hatsubai_flag_fukusho",        "h1_hatsubai_flag_fukusho",        "CATEGORY", "発売フラグ複勝"),
    Factor325(97,  "jvd_h1", "hatsubai_flag_sanrenpuku",     "h1_hatsubai_flag_sanrenpuku",     "CATEGORY", "発売フラグ三連複"),
    Factor325(98,  "jvd_h1", "hatsubai_flag_tansho",         "h1_hatsubai_flag_tansho",         "CATEGORY", "発売フラグ単勝"),
    Factor325(99,  "jvd_h1", "hatsubai_flag_umaren",         "h1_hatsubai_flag_umaren",         "CATEGORY", "発売フラグ馬連"),
    Factor325(100, "jvd_h1", "hatsubai_flag_umatan",         "h1_hatsubai_flag_umatan",         "CATEGORY", "発売フラグ馬単"),
    Factor325(101, "jvd_h1", "hatsubai_flag_wakuren",        "h1_hatsubai_flag_wakuren",        "CATEGORY", "発売フラグ枠連"),
    Factor325(102, "jvd_h1", "hatsubai_flag_wide",           "h1_hatsubai_flag_wide",           "CATEGORY", "発売フラグワイド"),
    Factor325(103, "jvd_h1", "henkan_dowaku_joho",           "h1_henkan_dowaku_joho",           "SKIP", "返還同枠情報",        "TARGET_LEAK"),
    Factor325(104, "jvd_h1", "henkan_hyosu_gokei_fukusho",   "h1_henkan_hyosu_gokei_fukusho",   "SKIP", "返還票数合計複勝",    "TARGET_LEAK"),
    Factor325(105, "jvd_h1", "henkan_hyosu_gokei_sanrenpuku","h1_henkan_hyosu_gokei_sanrenpuku","SKIP", "返還票数合計三連複",  "TARGET_LEAK"),
    Factor325(106, "jvd_h1", "henkan_hyosu_gokei_wakuren",   "h1_henkan_hyosu_gokei_wakuren",   "SKIP", "返還票数合計枠連",    "TARGET_LEAK"),
    Factor325(107, "jvd_h1", "henkan_hyosu_gokei_wide",      "h1_henkan_hyosu_gokei_wide",      "SKIP", "返還票数合計ワイド",  "TARGET_LEAK"),
    Factor325(108, "jvd_h1", "henkan_umaban_joho",           "h1_henkan_umaban_joho",           "SKIP", "返還馬番情報",        "TARGET_LEAK"),
    Factor325(109, "jvd_h1", "henkan_wakuban_joho",          "h1_henkan_wakuban_joho",          "SKIP", "返還枠番情報",        "TARGET_LEAK"),
    Factor325(110, "jvd_h1", "hyosu_fukusho",                "h1_hyosu_fukusho",                "SKIP", "票数複勝",            "TARGET_LEAK"),
    Factor325(111, "jvd_h1", "hyosu_gokei_fukusho",          "h1_hyosu_gokei_fukusho",          "SKIP", "票数合計複勝",        "TARGET_LEAK"),
    Factor325(112, "jvd_h1", "hyosu_gokei_sanrenpuku",       "h1_hyosu_gokei_sanrenpuku",       "SKIP", "票数合計三連複",      "TARGET_LEAK"),
    Factor325(113, "jvd_h1", "hyosu_gokei_tansho",           "h1_hyosu_gokei_tansho",           "SKIP", "票数合計単勝",        "TARGET_LEAK"),
    Factor325(114, "jvd_h1", "henkan_hyosu_gokei_tansho",    "h1_henkan_hyosu_gokei_tansho",    "SKIP", "返還票数合計単勝",    "TARGET_LEAK"),
    Factor325(115, "jvd_h1", "henkan_hyosu_gokei_umaren",    "h1_henkan_hyosu_gokei_umaren",    "SKIP", "返還票数合計馬連",    "TARGET_LEAK"),
]

# =============================================================================
# JVD_HR (18列) - 払戻金詳細 → TARGET_LEAK
# =============================================================================
_HR = [
    Factor325(116, "jvd_hr", "haraimodoshi_fukusho_1a", "hr_haraimodoshi_fukusho_1a", "SKIP", "払戻複勝1着A", "TARGET_LEAK"),
    Factor325(117, "jvd_hr", "haraimodoshi_fukusho_1b", "hr_haraimodoshi_fukusho_1b", "SKIP", "払戻複勝1着B", "TARGET_LEAK"),
    Factor325(118, "jvd_hr", "haraimodoshi_fukusho_1c", "hr_haraimodoshi_fukusho_1c", "SKIP", "払戻複勝1着C", "TARGET_LEAK"),
    Factor325(119, "jvd_hr", "haraimodoshi_fukusho_2a", "hr_haraimodoshi_fukusho_2a", "SKIP", "払戻複勝2着A", "TARGET_LEAK"),
    Factor325(120, "jvd_hr", "haraimodoshi_fukusho_2b", "hr_haraimodoshi_fukusho_2b", "SKIP", "払戻複勝2着B", "TARGET_LEAK"),
    Factor325(121, "jvd_hr", "haraimodoshi_fukusho_2c", "hr_haraimodoshi_fukusho_2c", "SKIP", "払戻複勝2着C", "TARGET_LEAK"),
    Factor325(122, "jvd_hr", "haraimodoshi_fukusho_3a", "hr_haraimodoshi_fukusho_3a", "SKIP", "払戻複勝3着A", "TARGET_LEAK"),
    Factor325(123, "jvd_hr", "haraimodoshi_fukusho_3b", "hr_haraimodoshi_fukusho_3b", "SKIP", "払戻複勝3着B", "TARGET_LEAK"),
    Factor325(124, "jvd_hr", "fuseiritsu_flag_fukusho",  "hr_fuseiritsu_flag_fukusho",  "CATEGORY", "不成立フラグ複勝"),
    Factor325(125, "jvd_hr", "fuseiritsu_flag_sanrenpuku","hr_fuseiritsu_flag_sanrenpuku","CATEGORY","不成立フラグ三連複"),
    Factor325(126, "jvd_hr", "fuseiritsu_flag_sanrentan","hr_fuseiritsu_flag_sanrentan","CATEGORY", "不成立フラグ三連単"),
    Factor325(127, "jvd_hr", "fuseiritsu_flag_tansho",   "hr_fuseiritsu_flag_tansho",   "CATEGORY", "不成立フラグ単勝"),
    Factor325(128, "jvd_hr", "haraimodoshi_sanrentan_2b","hr_haraimodoshi_sanrentan_2b","SKIP", "払戻三連単2B", "TARGET_LEAK"),
    Factor325(129, "jvd_hr", "haraimodoshi_sanrentan_2c","hr_haraimodoshi_sanrentan_2c","SKIP", "払戻三連単2C", "TARGET_LEAK"),
    Factor325(130, "jvd_hr", "haraimodoshi_sanrentan_3a","hr_haraimodoshi_sanrentan_3a","SKIP", "払戻三連単3A", "TARGET_LEAK"),
    Factor325(131, "jvd_hr", "haraimodoshi_sanrentan_3b","hr_haraimodoshi_sanrentan_3b","SKIP", "払戻三連単3B", "TARGET_LEAK"),
    Factor325(132, "jvd_hr", "haraimodoshi_sanrentan_3c","hr_haraimodoshi_sanrentan_3c","SKIP", "払戻三連単3C", "TARGET_LEAK"),
    Factor325(133, "jvd_hr", "haraimodoshi_sanrentan_4a","hr_haraimodoshi_sanrentan_4a","SKIP", "払戻三連単4A", "TARGET_LEAK"),
]

# =============================================================================
# JVD_WC (17列) - ウッドチップ調教 → NO_JOIN（時系列結合未確定）
# =============================================================================
_WC = [
    Factor325(134, "jvd_wc", "lap_time_10f",   "wc_lap_time_10f",   "SKIP", "ラップタイム10F", "NO_JOIN"),
    Factor325(135, "jvd_wc", "lap_time_5f",    "wc_lap_time_5f",    "SKIP", "ラップタイム5F",  "NO_JOIN"),
    Factor325(136, "jvd_wc", "lap_time_6f",    "wc_lap_time_6f",    "SKIP", "ラップタイム6F",  "NO_JOIN"),
    Factor325(137, "jvd_wc", "lap_time_7f",    "wc_lap_time_7f",    "SKIP", "ラップタイム7F",  "NO_JOIN"),
    Factor325(138, "jvd_wc", "lap_time_8f",    "wc_lap_time_8f",    "SKIP", "ラップタイム8F",  "NO_JOIN"),
    Factor325(139, "jvd_wc", "lap_time_9f",    "wc_lap_time_9f",    "SKIP", "ラップタイム9F",  "NO_JOIN"),
    Factor325(140, "jvd_wc", "time_gokei_10f", "wc_time_gokei_10f", "SKIP", "タイム合計10F",   "NO_JOIN"),
    Factor325(141, "jvd_wc", "time_gokei_5f",  "wc_time_gokei_5f",  "SKIP", "タイム合計5F",    "NO_JOIN"),
    Factor325(142, "jvd_wc", "time_gokei_6f",  "wc_time_gokei_6f",  "SKIP", "タイム合計6F",    "NO_JOIN"),
    Factor325(143, "jvd_wc", "time_gokei_7f",  "wc_time_gokei_7f",  "SKIP", "タイム合計7F",    "NO_JOIN"),
    Factor325(144, "jvd_wc", "time_gokei_8f",  "wc_time_gokei_8f",  "SKIP", "タイム合計8F",    "NO_JOIN"),
    Factor325(145, "jvd_wc", "time_gokei_9f",  "wc_time_gokei_9f",  "SKIP", "タイム合計9F",    "NO_JOIN"),
    Factor325(146, "jvd_wc", "babamawari",      "wc_babamawari",      "SKIP", "馬場回り",        "NO_JOIN"),
    Factor325(147, "jvd_wc", "chokyo_jikoku",   "wc_chokyo_jikoku",   "SKIP", "調教時刻",        "NO_JOIN"),
    Factor325(148, "jvd_wc", "chokyo_nengappi", "wc_chokyo_nengappi", "SKIP", "調教年月日",      "NO_JOIN"),
    Factor325(149, "jvd_wc", "course",          "wc_course",          "SKIP", "コース",          "NO_JOIN"),
    Factor325(150, "jvd_wc", "tracen_kubun",    "wc_tracen_kubun",    "SKIP", "トラセン区分",    "NO_JOIN"),
]

# =============================================================================
# JVD_DM (16列) - データマイニング予想
# =============================================================================
_DM = [
    Factor325(151, "jvd_dm", "data_sakusei_jifun", "dm_data_sakusei_jifun", "SKIP",    "データ作成時分",  "ADMINISTRATIVE"),
    Factor325(152, "jvd_dm", "mining_yoso_01",     "dm_mining_yoso_01",     "NUMERIC", "マイニング予想01"),
    Factor325(153, "jvd_dm", "mining_yoso_02",     "dm_mining_yoso_02",     "NUMERIC", "マイニング予想02"),
    Factor325(154, "jvd_dm", "mining_yoso_03",     "dm_mining_yoso_03",     "NUMERIC", "マイニング予想03"),
    Factor325(155, "jvd_dm", "mining_yoso_04",     "dm_mining_yoso_04",     "NUMERIC", "マイニング予想04"),
    Factor325(156, "jvd_dm", "mining_yoso_05",     "dm_mining_yoso_05",     "NUMERIC", "マイニング予想05"),
    Factor325(157, "jvd_dm", "mining_yoso_06",     "dm_mining_yoso_06",     "NUMERIC", "マイニング予想06"),
    Factor325(158, "jvd_dm", "mining_yoso_15",     "dm_mining_yoso_15",     "NUMERIC", "マイニング予想15"),
    Factor325(159, "jvd_dm", "mining_yoso_16",     "dm_mining_yoso_16",     "NUMERIC", "マイニング予想16"),
    Factor325(160, "jvd_dm", "mining_yoso_17",     "dm_mining_yoso_17",     "NUMERIC", "マイニング予想17"),
    Factor325(161, "jvd_dm", "mining_yoso_18",     "dm_mining_yoso_18",     "NUMERIC", "マイニング予想18"),
    Factor325(162, "jvd_dm", "mining_yoso_07",     "dm_mining_yoso_07",     "NUMERIC", "マイニング予想07"),
    Factor325(163, "jvd_dm", "mining_yoso_08",     "dm_mining_yoso_08",     "NUMERIC", "マイニング予想08"),
    Factor325(164, "jvd_dm", "mining_yoso_09",     "dm_mining_yoso_09",     "NUMERIC", "マイニング予想09"),
    Factor325(165, "jvd_dm", "mining_yoso_10",     "dm_mining_yoso_10",     "NUMERIC", "マイニング予想10"),
    Factor325(166, "jvd_dm", "mining_yoso_11",     "dm_mining_yoso_11",     "NUMERIC", "マイニング予想11"),
]

# =============================================================================
# JVD_UM (14列) - 馬基本情報（適性評価スコア）
# =============================================================================
_UM = [
    Factor325(167, "jvd_um", "bamei_eur",         "um_bamei_eur",         "SKIP",    "馬名欧字",       "IDENTIFIER"),
    Factor325(168, "jvd_um", "bamei_hankaku_kana", "um_bamei_hankaku_kana","SKIP",   "馬名半角カナ",   "IDENTIFIER"),
    Factor325(169, "jvd_um", "dirt_furyo",         "um_dirt_furyo",         "NUMERIC", "ダート不良適性"),
    Factor325(170, "jvd_um", "dirt_hidari",        "um_dirt_hidari",        "NUMERIC", "ダート左回り適性"),
    Factor325(171, "jvd_um", "dirt_long",          "um_dirt_long",          "NUMERIC", "ダート長距離適性"),
    Factor325(172, "jvd_um", "dirt_middle",        "um_dirt_middle",        "NUMERIC", "ダート中距離適性"),
    Factor325(173, "jvd_um", "dirt_omo",           "um_dirt_omo",           "NUMERIC", "ダート重適性"),
    Factor325(174, "jvd_um", "dirt_ryo",           "um_dirt_ryo",           "NUMERIC", "ダート良適性"),
    Factor325(175, "jvd_um", "dirt_short",         "um_dirt_short",         "NUMERIC", "ダート短距離適性"),
    Factor325(176, "jvd_um", "dirt_yayaomo",       "um_dirt_yayaomo",       "NUMERIC", "ダートやや重適性"),
    Factor325(177, "jvd_um", "ketto_joho_12a",     "um_ketto_joho_12a",     "CATEGORY","血統情報12A"),
    Factor325(178, "jvd_um", "ketto_joho_12b",     "um_ketto_joho_12b",     "CATEGORY","血統情報12B"),
    Factor325(179, "jvd_um", "ketto_joho_13a",     "um_ketto_joho_13a",     "CATEGORY","血統情報13A"),
    Factor325(180, "jvd_um", "ketto_joho_13b",     "um_ketto_joho_13b",     "CATEGORY","血統情報13B"),
]

# =============================================================================
# JVD_SK (12列) - 血統情報
# =============================================================================
_SK = [
    Factor325(181, "jvd_sk", "hinshu_code",    "sk_hinshu_code",    "CATEGORY", "品種コード"),
    Factor325(182, "jvd_sk", "ketto_joho_01a", "sk_ketto_joho_01a", "CATEGORY", "血統情報01A（父）"),
    Factor325(183, "jvd_sk", "ketto_joho_02a", "sk_ketto_joho_02a", "CATEGORY", "血統情報02A（母）"),
    Factor325(184, "jvd_sk", "ketto_joho_03a", "sk_ketto_joho_03a", "CATEGORY", "血統情報03A（父父）"),
    Factor325(185, "jvd_sk", "ketto_joho_04a", "sk_ketto_joho_04a", "CATEGORY", "血統情報04A（父母）"),
    Factor325(186, "jvd_sk", "ketto_joho_05a", "sk_ketto_joho_05a", "CATEGORY", "血統情報05A（母父）"),
    Factor325(187, "jvd_sk", "ketto_joho_06a", "sk_ketto_joho_06a", "CATEGORY", "血統情報06A（母母）"),
    Factor325(188, "jvd_sk", "ketto_joho_07a", "sk_ketto_joho_07a", "CATEGORY", "血統情報07A（3代父）"),
    Factor325(189, "jvd_sk", "seinengappi",    "sk_seinengappi",    "SKIP",     "生年月日",       "IDENTIFIER"),
    Factor325(190, "jvd_sk", "ketto_joho_08a", "sk_ketto_joho_08a", "CATEGORY", "血統情報08A"),
    Factor325(191, "jvd_sk", "ketto_joho_09a", "sk_ketto_joho_09a", "CATEGORY", "血統情報09A"),
    Factor325(192, "jvd_sk", "ketto_joho_10a", "sk_ketto_joho_10a", "CATEGORY", "血統情報10A"),
]

# =============================================================================
# JVD_HC (7列) - 芝・ダート調教 → NO_JOIN（時系列結合未確定）
# =============================================================================
_HC = [
    Factor325(193, "jvd_hc", "lap_time_1f",  "hc_lap_time_1f",  "SKIP", "ラップタイム1F", "NO_JOIN"),
    Factor325(194, "jvd_hc", "lap_time_2f",  "hc_lap_time_2f",  "SKIP", "ラップタイム2F", "NO_JOIN"),
    Factor325(195, "jvd_hc", "lap_time_3f",  "hc_lap_time_3f",  "SKIP", "ラップタイム3F", "NO_JOIN"),
    Factor325(196, "jvd_hc", "lap_time_4f",  "hc_lap_time_4f",  "SKIP", "ラップタイム4F", "NO_JOIN"),
    Factor325(197, "jvd_hc", "time_gokei_2f","hc_time_gokei_2f","SKIP", "タイム合計2F",   "NO_JOIN"),
    Factor325(198, "jvd_hc", "time_gokei_3f","hc_time_gokei_3f","SKIP", "タイム合計3F",   "NO_JOIN"),
    Factor325(199, "jvd_hc", "time_gokei_4f","hc_time_gokei_4f","SKIP", "タイム合計4F",   "NO_JOIN"),
]

# =============================================================================
# JVD_H6 (6列) - 三連単払戻 → TARGET_LEAK
# =============================================================================
_H6 = [
    Factor325(200, "jvd_h6", "hatsubai_flag_sanrentan",      "h6_hatsubai_flag_sanrentan",      "CATEGORY", "発売フラグ三連単"),
    Factor325(201, "jvd_h6", "record_id",                   "h6_record_id",                   "SKIP",     "レコードID",      "IDENTIFIER"),
    Factor325(202, "jvd_h6", "toroku_tosu",                 "h6_toroku_tosu",                 "NUMERIC",  "登録頭数"),
    Factor325(203, "jvd_h6", "henkan_hyosu_gokei_sanrentan","h6_henkan_hyosu_gokei_sanrentan","SKIP",     "返還票数合計三連単","TARGET_LEAK"),
    Factor325(204, "jvd_h6", "hyosu_gokei_sanrentan",       "h6_hyosu_gokei_sanrentan",       "SKIP",     "票数合計三連単",   "TARGET_LEAK"),
    Factor325(205, "jvd_h6", "hyosu_sanrentan",             "h6_hyosu_sanrentan",             "SKIP",     "票数三連単",       "TARGET_LEAK"),
]

# =============================================================================
# JVD_JG (3列) - 除外・取消情報
# =============================================================================
_JG = [
    Factor325(206, "jvd_jg", "jogai_jotai_kubun",       "jg_jogai_jotai_kubun",       "CATEGORY", "除外状態区分"),
    Factor325(207, "jvd_jg", "shusso_kubun",            "jg_shusso_kubun",            "CATEGORY", "出走区分"),
    Factor325(208, "jvd_jg", "shutsuba_tohyo_uketsuke", "jg_shutsuba_tohyo_uketsuke", "SKIP",     "出馬投票受付",    "ADMINISTRATIVE"),
]

# =============================================================================
# JVD_CH (1列) - 調教師マスタ
# =============================================================================
_CH = [
    Factor325(209, "jvd_ch", "tozai_shozoku_code", "ch_tozai_shozoku_code", "CATEGORY", "東西所属コード"),
]

# =============================================================================
# JRD_KYI (65列) - 競馬指数
# =============================================================================
_KYI = [
    Factor325(210, "jrd_kyi", "idm",                       "kyi_idm",                       "NUMERIC",  "IDM（スピード指数）",       n_bins=10),
    Factor325(211, "jrd_kyi", "joho_shisu",                "kyi_joho_shisu",                "NUMERIC",  "情報指数",                  n_bins=10),
    Factor325(212, "jrd_kyi", "kishu_shisu",               "kyi_kishu_shisu",               "NUMERIC",  "騎手指数",                  n_bins=10),
    Factor325(213, "jrd_kyi", "agari_shisu",               "kyi_agari_shisu",               "NUMERIC",  "上がり指数",                n_bins=10),
    Factor325(214, "jrd_kyi", "chokyo_shisu",              "kyi_chokyo_shisu",              "NUMERIC",  "調教指数",                  n_bins=10),
    Factor325(215, "jrd_kyi", "gekiso_shisu",              "kyi_gekiso_shisu",              "NUMERIC",  "激走指数",                  n_bins=5),
    Factor325(216, "jrd_kyi", "ichi_shisu",                "kyi_ichi_shisu",                "NUMERIC",  "位置取り指数",              n_bins=10),
    Factor325(217, "jrd_kyi", "kyusha_shisu",              "kyi_kyusha_shisu",              "NUMERIC",  "厩舎指数",                  n_bins=10),
    Factor325(218, "jrd_kyi", "manken_shisu",              "kyi_manken_shisu",              "NUMERIC",  "万券指数",                  n_bins=5),
    Factor325(219, "jrd_kyi", "pace_shisu",                "kyi_pace_shisu",                "NUMERIC",  "ペース指数",                n_bins=10),
    Factor325(220, "jrd_kyi", "sogo_shisu",                "kyi_sogo_shisu",                "NUMERIC",  "総合指数",                  n_bins=10),
    Factor325(221, "jrd_kyi", "ten_shisu",                 "kyi_ten_shisu",                 "NUMERIC",  "テン指数",                  n_bins=10),
    Factor325(222, "jrd_kyi", "uma_start_shisu",           "kyi_uma_start_shisu",           "NUMERIC",  "馬スタート指数",            n_bins=5),
    Factor325(223, "jrd_kyi", "agari_shisu_juni",          "kyi_agari_shisu_juni",          "ORDINAL",  "上がり指数順位"),
    Factor325(224, "jrd_kyi", "dochu_juni",                "kyi_dochu_juni",                "ORDINAL",  "道中順位"),
    Factor325(225, "jrd_kyi", "dochu_sa",                  "kyi_dochu_sa",                  "NUMERIC",  "道中差",                    n_bins=5),
    Factor325(226, "jrd_kyi", "dochu_uchisoto",            "kyi_dochu_uchisoto",            "CATEGORY", "道中内外"),
    Factor325(227, "jrd_kyi", "gekiso_juni",               "kyi_gekiso_juni",               "ORDINAL",  "激走順位"),
    Factor325(228, "jrd_kyi", "goal_juni",                 "kyi_goal_juni",                 "ORDINAL",  "ゴール順位"),
    Factor325(229, "jrd_kyi", "goal_sa",                   "kyi_goal_sa",                   "NUMERIC",  "ゴール差",                  n_bins=5),
    Factor325(230, "jrd_kyi", "goal_uchisoto",             "kyi_goal_uchisoto",             "CATEGORY", "ゴール内外"),
    Factor325(231, "jrd_kyi", "ichi_shisu_juni",           "kyi_ichi_shisu_juni",           "ORDINAL",  "位置取り指数順位"),
    Factor325(232, "jrd_kyi", "kohan_3f_juni",             "kyi_kohan_3f_juni",             "ORDINAL",  "後半3F順位"),
    Factor325(233, "jrd_kyi", "kohan_3f_sa",               "kyi_kohan_3f_sa",               "NUMERIC",  "後半3F差",                  n_bins=5),
    Factor325(234, "jrd_kyi", "kohan_3f_uchisoto",         "kyi_kohan_3f_uchisoto",         "CATEGORY", "後半3F内外"),
    Factor325(235, "jrd_kyi", "ls_shisu_juni",             "kyi_ls_shisu_juni",             "ORDINAL",  "LS指数順位"),
    Factor325(236, "jrd_kyi", "pace_shisu_juni",           "kyi_pace_shisu_juni",           "ORDINAL",  "ペース指数順位"),
    Factor325(237, "jrd_kyi", "ten_shisu_juni",            "kyi_ten_shisu_juni",            "ORDINAL",  "テン指数順位"),
    Factor325(238, "jrd_kyi", "chokyo_yajirushi_code",     "kyi_chokyo_yajirushi_code",     "CATEGORY", "調教矢印コード"),
    Factor325(239, "jrd_kyi", "class_code",                "kyi_class_code",                "CATEGORY", "クラスコード"),
    Factor325(240, "jrd_kyi", "gekiso_type",               "kyi_gekiso_type",               "CATEGORY", "激走タイプ"),
    Factor325(241, "jrd_kyi", "hizume_code",               "kyi_hizume_code",               "CATEGORY", "蹄コード"),
    Factor325(242, "jrd_kyi", "hobokusaki",                "kyi_hobokusaki",                "SKIP",     "放牧先",                    "IDENTIFIER"),
    Factor325(243, "jrd_kyi", "hobokusaki_rank",           "kyi_hobokusaki_rank",           "CATEGORY", "放牧先ランク"),
    Factor325(244, "jrd_kyi", "joshodo_code",              "kyi_joshodo_code",              "CATEGORY", "上昇度コード"),
    Factor325(245, "jrd_kyi", "kishu_code",                "kyi_kishu_code",                "CATEGORY", "騎手コード"),
    Factor325(246, "jrd_kyi", "kyakushitsu_code",          "kyi_kyakushitsu_code",          "CATEGORY", "脚質コード"),
    Factor325(247, "jrd_kyi", "kyori_tekisei_code",        "kyi_kyori_tekisei_code",        "CATEGORY", "距離適性コード"),
    Factor325(248, "jrd_kyi", "kyusha_hyoka_code",         "kyi_kyusha_hyoka_code",         "CATEGORY", "厩舎評価コード"),
    Factor325(249, "jrd_kyi", "kyusha_rank",               "kyi_kyusha_rank",               "CATEGORY", "厩舎ランク"),
    Factor325(250, "jrd_kyi", "kyuyo_riyu_bunrui_code",    "kyi_kyuyo_riyu_bunrui_code",    "CATEGORY", "休養理由分類コード"),
    Factor325(251, "jrd_kyi", "manken_shirushi",           "kyi_manken_shirushi",           "CATEGORY", "万券印"),
    Factor325(252, "jrd_kyi", "pace_yoso",                 "kyi_pace_yoso",                 "CATEGORY", "ペース予想"),
    Factor325(253, "jrd_kyi", "tekisei_code_omo",          "kyi_tekisei_code_omo",          "CATEGORY", "適性コード（重）"),
    Factor325(254, "jrd_kyi", "yuso_kubun",                "kyi_yuso_kubun",                "CATEGORY", "輸送区分"),
    Factor325(255, "jrd_kyi", "kakutoku_shokin_ruikei",    "kyi_kakutoku_shokin_ruikei",    "NUMERIC",  "獲得賞金累計",              n_bins=5),
    Factor325(256, "jrd_kyi", "kijun_ninkijun_fukusho",    "kyi_kijun_ninkijun_fukusho",    "ORDINAL",  "基準人気順（複勝）"),
    Factor325(257, "jrd_kyi", "kijun_ninkijun_tansho",     "kyi_kijun_ninkijun_tansho",     "ORDINAL",  "基準人気順（単勝）"),
    Factor325(258, "jrd_kyi", "kijun_odds_fukusho",        "kyi_kijun_odds_fukusho",        "NUMERIC",  "基準オッズ（複勝）",        n_bins=5),
    Factor325(259, "jrd_kyi", "kijun_odds_tansho",         "kyi_kijun_odds_tansho",         "NUMERIC",  "基準オッズ（単勝）",        n_bins=5),
    Factor325(260, "jrd_kyi", "shutoku_shokin_ruikei",     "kyi_shutoku_shokin_ruikei",     "NUMERIC",  "取得賞金累計",              n_bins=5),
    Factor325(261, "jrd_kyi", "kishu_kitai_rentai_ritsu",  "kyi_kishu_kitai_rentai_ritsu",  "NUMERIC",  "騎手期待連対率",            n_bins=5),
    Factor325(262, "jrd_kyi", "kishu_kitai_sanchakunai_ritsu","kyi_kishu_kitai_sanchakunai_ritsu","NUMERIC","騎手期待3着内率",       n_bins=5),
    Factor325(263, "jrd_kyi", "kishu_kitai_tansho_ritsu",  "kyi_kishu_kitai_tansho_ritsu",  "NUMERIC",  "騎手期待単勝率",            n_bins=5),
    Factor325(264, "jrd_kyi", "taikei",                    "kyi_taikei",                    "SKIP",     "体型（テキスト）",          "IDENTIFIER"),
    Factor325(265, "jrd_kyi", "taikei_sogo_1",             "kyi_taikei_sogo_1",             "CATEGORY", "体型総合1"),
    Factor325(266, "jrd_kyi", "taikei_sogo_2",             "kyi_taikei_sogo_2",             "CATEGORY", "体型総合2"),
    Factor325(267, "jrd_kyi", "taikei_sogo_3",             "kyi_taikei_sogo_3",             "CATEGORY", "体型総合3"),
    Factor325(268, "jrd_kyi", "uma_tokki_1",               "kyi_uma_tokki_1",               "CATEGORY", "馬特記1"),
    Factor325(269, "jrd_kyi", "uma_tokki_2",               "kyi_uma_tokki_2",               "CATEGORY", "馬特記2"),
    Factor325(270, "jrd_kyi", "uma_tokki_3",               "kyi_uma_tokki_3",               "CATEGORY", "馬特記3"),
    Factor325(271, "jrd_kyi", "ketto_toroku_bango",        "kyi_ketto_toroku_bango",        "SKIP",     "血統登録番号",              "IDENTIFIER"),
    Factor325(272, "jrd_kyi", "race_shikonen",             "kyi_race_shikonen",             "SKIP",     "レース施行年",              "IDENTIFIER"),
    Factor325(273, "jrd_kyi", "futan_juryo",               "kyi_futan_juryo",               "NUMERIC",  "負担重量（斤量）",          n_bins=5),
    Factor325(274, "jrd_kyi", "uma_deokure_ritsu",         "kyi_uma_deokure_ritsu",         "NUMERIC",  "馬出遅れ率",                n_bins=5),
]

# =============================================================================
# JRD_CYB (18列) - 調教分析
# =============================================================================
_CYB = [
    Factor325(275, "jrd_cyb", "chokyo_comment",        "cyb_chokyo_comment",        "SKIP",     "調教コメント",        "IDENTIFIER"),
    Factor325(276, "jrd_cyb", "chokyo_corse_dirt",     "cyb_chokyo_corse_dirt",     "NUMERIC",  "調教コースダートタイム",  n_bins=5),
    Factor325(277, "jrd_cyb", "chokyo_corse_hanro",    "cyb_chokyo_corse_hanro",    "NUMERIC",  "調教コース坂路タイム",    n_bins=5),
    Factor325(278, "jrd_cyb", "chokyo_corse_polytrack","cyb_chokyo_corse_polytrack","NUMERIC",  "調教ポリトラックタイム",  n_bins=5),
    Factor325(279, "jrd_cyb", "chokyo_corse_pool",     "cyb_chokyo_corse_pool",     "NUMERIC",  "調教プール",             n_bins=5),
    Factor325(280, "jrd_cyb", "chokyo_corse_shiba",    "cyb_chokyo_corse_shiba",    "NUMERIC",  "調教コース芝タイム",      n_bins=5),
    Factor325(281, "jrd_cyb", "chokyo_corse_shubetsu", "cyb_chokyo_corse_shubetsu", "CATEGORY", "調教コース種別"),          # 正: corse (not course)
    Factor325(282, "jrd_cyb", "chokyo_hyoka",          "cyb_chokyo_hyoka",          "CATEGORY", "調教評価"),               # 正: 1カラムのみ
    Factor325(283, "jrd_cyb", "chokyo_hyoka_2",        "cyb_chokyo_hyoka_2",        "SKIP",     "調教評価2",           "NOT_IN_DB"),
    Factor325(284, "jrd_cyb", "chokyo_hyoka_3",        "cyb_chokyo_hyoka_3",        "SKIP",     "調教評価3",           "NOT_IN_DB"),
    Factor325(285, "jrd_cyb", "chokyo_juten",          "cyb_chokyo_juten",          "CATEGORY", "調教重点"),
    Factor325(286, "jrd_cyb", "chokyo_type",           "cyb_chokyo_type",           "CATEGORY", "調教タイプ"),
    Factor325(287, "jrd_cyb", "chokyoshi_code",        "cyb_chokyoshi_code",        "SKIP",     "調教師コード（CYB）",  "NOT_IN_DB"),
    Factor325(288, "jrd_cyb", "chokyoshi_mei",         "cyb_chokyoshi_mei",         "SKIP",     "調教師名（CYB）",      "NOT_IN_DB"),
    Factor325(289, "jrd_cyb", "choshubetsu",           "cyb_choshubetsu",           "SKIP",     "調種別",              "NOT_IN_DB"),
    Factor325(290, "jrd_cyb", "oikiri_shisu",          "cyb_oikiri_shisu",          "NUMERIC",  "追切指数",              n_bins=5),
    Factor325(291, "jrd_cyb", "sakusha_hyoka_f_h",     "cyb_sakusha_hyoka_f_h",     "SKIP",     "作者評価F/H",          "NOT_IN_DB"),
    Factor325(292, "jrd_cyb", "shiage_shisu",          "cyb_shiage_shisu",          "NUMERIC",  "仕上指数",              n_bins=5),
]

# =============================================================================
# JRD_SED (14列) - レース詳細
# =============================================================================
_SED = [
    Factor325(293, "jrd_sed", "babasa",               "sed_babasa",               "NUMERIC",  "馬場差",                n_bins=5),
    Factor325(294, "jrd_sed", "bataiju_zogen",        "sed_bataiju_zogen",        "NUMERIC",  "馬体重増減（JRDB）",    n_bins=5),
    Factor325(295, "jrd_sed", "corner_tsuka_juni_1",  "sed_corner_tsuka_juni_1",  "SKIP",     "1コーナー通過順位",     "TARGET_LEAK"),
    Factor325(296, "jrd_sed", "corner_tsuka_juni_2",  "sed_corner_tsuka_juni_2",  "SKIP",     "2コーナー通過順位",     "TARGET_LEAK"),
    Factor325(297, "jrd_sed", "corner_tsuka_juni_3",  "sed_corner_tsuka_juni_3",  "SKIP",     "3コーナー通過順位",     "TARGET_LEAK"),
    Factor325(298, "jrd_sed", "corner_tsuka_juni_4",  "sed_corner_tsuka_juni_4",  "SKIP",     "4コーナー通過順位",     "TARGET_LEAK"),
    Factor325(299, "jrd_sed", "furi",                 "sed_furi",                 "CATEGORY", "振り（スタート）"),
    Factor325(300, "jrd_sed", "id",                   "sed_id",                   "SKIP",     "レコードID（SED）",     "IDENTIFIER"),
    Factor325(301, "jrd_sed", "pace",                 "sed_pace",                 "CATEGORY", "ペース"),
    Factor325(302, "jrd_sed", "pace_shisu",           "sed_pace_shisu",           "NUMERIC",  "ペース指数（SED）",     n_bins=5),
    Factor325(303, "jrd_sed", "race_comments",        "sed_race_comments",        "SKIP",     "レースコメント",         "IDENTIFIER"),
    Factor325(304, "jrd_sed", "race_pace",            "sed_race_pace",            "CATEGORY", "レースペース"),
    Factor325(305, "jrd_sed", "race_pace_runner",     "sed_race_pace_runner",     "SKIP",     "レースペース走者",       "IDENTIFIER"),
    Factor325(306, "jrd_sed", "race_pen_type",        "sed_race_pen_type",        "SKIP",     "レースペンタイプ",   "NOT_IN_DB"),
]

# =============================================================================
# JRD_JOA (10列) - 騎手・厩舎評価
# =============================================================================
_JOA = [
    Factor325(307, "jrd_joa", "cid",                                       "joa_cid",                                       "SKIP",     "CID",             "IDENTIFIER"),
    Factor325(308, "jrd_joa", "em",                                         "joa_em",                                         "CATEGORY", "EM評価"),
    Factor325(309, "jrd_joa", "jockey_banushi_nijumaru_tansho_kaishuritsu", "joa_jockey_banushi_nijumaru_tansho_kaishuritsu", "SKIP",     "騎手×馬主◎◎単勝回収率", "NOT_IN_DB"),
    Factor325(310, "jrd_joa", "kishu_bb_shirushi",                          "joa_kishu_bb_shirushi",                          "CATEGORY", "騎手◎印"),
    Factor325(311, "jrd_joa", "kyusha_bb_shirushi",                         "joa_kyusha_bb_shirushi",                         "CATEGORY", "厩舎◎印"),
    Factor325(312, "jrd_joa", "kyusha_bb_nijumaru_tansho_kaishuritsu",      "joa_kyusha_bb_nijumaru_tansho_kaishuritsu",      "NUMERIC",  "厩舎◎◎単勝回収率",     n_bins=5),
    Factor325(313, "jrd_joa", "ls_hyoka",                                   "joa_ls_hyoka",                                   "CATEGORY", "LS評価"),
    Factor325(314, "jrd_joa", "ls_shisu",                                   "joa_ls_shisu",                                   "NUMERIC",  "LS指数",                n_bins=10),
    Factor325(315, "jrd_joa", "ten_shisu",                                  "joa_ten_shisu",                                  "SKIP",     "テン指数（JOA）",       "NOT_IN_DB"),
    Factor325(316, "jrd_joa", "uma_gucchi",                                 "joa_uma_gucchi",                                 "SKIP",     "馬ぐっち",              "NOT_IN_DB"),
]

# =============================================================================
# JRD_BAC (9列) - レース基本情報
# =============================================================================
_BAC = [
    Factor325(317, "jrd_bac", "baken_hatsubai_flag",  "bac_baken_hatsubai_flag",  "CATEGORY", "馬券発売フラグ"),
    Factor325(318, "jrd_bac", "baba_sa_saishujikoku", "bac_baba_sa_saishujikoku", "SKIP",     "馬場差最終時刻",         "NOT_IN_DB"),
    Factor325(319, "jrd_bac", "fukashokin",           "bac_fukashokin",           "NUMERIC",  "付加賞金（BAC）",        n_bins=5),
    Factor325(320, "jrd_bac", "honshokin",            "bac_honshokin",            "NUMERIC",  "本賞金",                 n_bins=5),
    Factor325(321, "jrd_bac", "kaisai_nen_gappi",     "bac_kaisai_nen_gappi",     "SKIP",     "開催年月日",             "NOT_IN_DB"),
    Factor325(322, "jrd_bac", "kyoso_joken",          "bac_kyoso_joken",          "SKIP",     "競走条件",               "NOT_IN_DB"),
    Factor325(323, "jrd_bac", "race_code_zenhan",     "bac_race_code_zenhan",     "SKIP",     "レースコード前半",        "NOT_IN_DB"),
    Factor325(324, "jrd_bac", "race_comment",         "bac_race_comment",         "SKIP",     "レースコメント（BAC）",  "NOT_IN_DB"),
    Factor325(325, "jrd_bac", "track_baba_sa",        "bac_track_baba_sa",        "SKIP",     "トラック馬場差",          "NOT_IN_DB"),
]

# =============================================================================
# 全325ファクターのリスト
# =============================================================================
ALL_FACTORS_325: list[Factor325] = (
    _SE + _RA + _CK + _H1 + _HR + _WC + _DM + _UM + _SK + _HC + _H6 + _JG + _CH +
    _KYI + _CYB + _SED + _JOA + _BAC
)

# 分析対象ファクター（SKIPでないもの）
ACTIVE_FACTORS: list[Factor325] = [f for f in ALL_FACTORS_325 if f.kind != "SKIP"]

# テーブル別グループ
def get_factors_by_table(table: str) -> list[Factor325]:
    """テーブル名でファクターをフィルタする。"""
    return [f for f in ALL_FACTORS_325 if f.table == table]


def get_factor_by_fid(fid: int) -> Factor325:
    """通し番号でファクターを取得する。"""
    for f in ALL_FACTORS_325:
        if f.fid == fid:
            return f
    raise ValueError(f"Factor fid={fid} not found")


assert len(ALL_FACTORS_325) == 325, f"Expected 325 factors, got {len(ALL_FACTORS_325)}"
