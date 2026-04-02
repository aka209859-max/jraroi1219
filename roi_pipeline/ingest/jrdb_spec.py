"""
JRDB公式仕様準拠のフィールド定義

各テーブルのレコード構造を公式仕様書から正確に転記。
バイト位置は「相対位置」（1始まり）を使用。
Pythonスライスへの変換: slice(relative - 1, relative - 1 + byte)

重要:
  - 「日」フィールドはTYPE=F（16進数）: 1-9 → '1'-'9', 10→'a', 11→'b', 12→'c'
  - レースキー = 場コード(2) + 年(2) + 回(1) + 日(1,hex) + R(2) = 8byte
  - KYI/CYB/JOA: レースキー(8) + 馬番(2) = 10byte が先頭
  - BAC: レースキー(8) + 年月日(8) + ... (馬番なし、レース単位)
"""
from dataclasses import dataclass
from typing import List


@dataclass(frozen=True)
class FieldDef:
    """フィールド定義"""
    name: str           # Python側カラム名
    relative: int       # JRDB仕様の相対位置（1始まり）
    byte: int           # バイト長
    type_code: str      # '9','Z','X','F' のいずれか
    description: str = ""

    @property
    def start(self) -> int:
        """Python slice開始位置（0始まり）"""
        return self.relative - 1

    @property
    def end(self) -> int:
        """Python slice終了位置"""
        return self.relative - 1 + self.byte


# =============================================================================
# 共通: レースキー 8byte
# =============================================================================
RACE_KEY_FIELDS: List[FieldDef] = [
    FieldDef("basho_code",   1, 2, "9", "場コード"),
    FieldDef("year",         3, 2, "9", "年（下2桁）"),
    FieldDef("kai",          5, 1, "9", "回"),
    FieldDef("nichi",        6, 1, "F", "日（16進数: 1-9,a,b,c）"),
    FieldDef("race_num",     7, 2, "9", "レース番号R"),
]


# =============================================================================
# KYI: 競走馬データ（1024byte/record）
# =============================================================================
KYI_RECORD_LENGTH = 1024

KYI_FIELDS: List[FieldDef] = [
    # レースキー
    FieldDef("basho_code",       1,  2, "9", "場コード"),
    FieldDef("year",             3,  2, "9", "年（下2桁）"),
    FieldDef("kai",              5,  1, "9", "回"),
    FieldDef("nichi",            6,  1, "F", "日（16進数）"),
    FieldDef("race_num",         7,  2, "9", "R"),
    FieldDef("umaban",           9,  2, "9", "馬番"),
    FieldDef("kettou_toroku_bango", 11, 8, "X", "血統登録番号"),
    FieldDef("bamei",           19, 36, "X", "馬名（全角18文字）"),
    # 能力指数
    FieldDef("idm",             55,  5, "Z", "IDM"),
    FieldDef("kishu_shisu",     60,  5, "Z", "騎手指数"),
    FieldDef("joho_shisu",      65,  5, "Z", "情報指数"),
    FieldDef("yobi1",           70,  5, "Z", "予備1"),
    FieldDef("yobi2",           75,  5, "Z", "予備2"),
    FieldDef("yobi3",           80,  5, "Z", "予備3"),
    FieldDef("sogo_shisu",      85,  5, "Z", "総合指数"),
    # 基本データ
    FieldDef("kyakushitsu",     90,  1, "9", "脚質"),
    FieldDef("kyori_tekisei",   91,  1, "9", "距離適性"),
    FieldDef("josho_do",        92,  1, "9", "上昇度"),
    FieldDef("rotation",        93,  3, "Z", "ローテーション"),
    FieldDef("kijun_odds",      96,  5, "Z", "基準オッズ"),
    FieldDef("kijun_ninki_junni", 101, 2, "Z", "基準人気順位"),
    FieldDef("kijun_fukusho_odds", 103, 5, "Z", "基準複勝オッズ"),
    FieldDef("kijun_fukusho_ninki", 108, 2, "Z", "基準複勝人気順位"),
    # 印数
    FieldDef("tokutei_honmei",  110, 3, "Z", "特定情報◎"),
    FieldDef("tokutei_taikou",  113, 3, "Z", "特定情報○"),
    FieldDef("tokutei_tanana",  116, 3, "Z", "特定情報▲"),
    FieldDef("tokutei_renka",   119, 3, "Z", "特定情報△"),
    FieldDef("tokutei_himo",    122, 3, "Z", "特定情報×"),
    FieldDef("sogo_honmei",     125, 3, "Z", "総合情報◎"),
    FieldDef("sogo_taikou",     128, 3, "Z", "総合情報○"),
    FieldDef("sogo_tanana",     131, 3, "Z", "総合情報▲"),
    FieldDef("sogo_renka",      134, 3, "Z", "総合情報△"),
    FieldDef("sogo_himo",       137, 3, "Z", "総合情報×"),
    # 指数
    FieldDef("ninki_shisu",     140, 5, "Z", "人気指数"),
    FieldDef("chokyo_shisu",    145, 5, "Z", "調教指数"),
    FieldDef("kyusha_shisu",    150, 5, "Z", "厩舎指数"),
    # 第3版追加
    FieldDef("chokyo_yajirushi_code", 155, 1, "9", "調教矢印コード"),
    FieldDef("kyusha_hyoka_code", 156, 1, "9", "厩舎評価コード"),
    FieldDef("kishu_rentai_ritsu", 157, 4, "Z", "騎手期待連対率"),
    FieldDef("gekiso_shisu",    161, 3, "Z", "激走指数"),
    FieldDef("hizume_code",     164, 2, "9", "蹄コード"),
    FieldDef("omo_tekisei_code", 166, 1, "9", "重適性コード"),
    FieldDef("class_code",      167, 2, "9", "クラスコード"),
    # 第4版追加
    FieldDef("blinker",         171, 1, "X", "ブリンカー"),
    FieldDef("kishu_mei",       172, 12, "X", "騎手名"),
    FieldDef("futan_juryo",     184, 3, "9", "負担重量(0.1kg単位)"),
    FieldDef("minarai_kubun",   187, 1, "9", "見習い区分"),
    FieldDef("chokyoshi_mei",   188, 12, "X", "調教師名"),
    FieldDef("chokyoshi_shozoku", 200, 4, "X", "調教師所属"),
    # 前走リンクキー
    FieldDef("zenso1_key",      204, 16, "9", "前走1競走成績キー"),
    FieldDef("zenso2_key",      220, 16, "9", "前走2競走成績キー"),
    FieldDef("zenso3_key",      236, 16, "9", "前走3競走成績キー"),
    FieldDef("zenso4_key",      252, 16, "9", "前走4競走成績キー"),
    FieldDef("zenso5_key",      268, 16, "9", "前走5競走成績キー"),
    FieldDef("zenso1_race_key", 284, 8, "9", "前走1レースキー"),
    FieldDef("zenso2_race_key", 292, 8, "9", "前走2レースキー"),
    FieldDef("zenso3_race_key", 300, 8, "9", "前走3レースキー"),
    FieldDef("zenso4_race_key", 308, 8, "9", "前走4レースキー"),
    FieldDef("zenso5_race_key", 316, 8, "9", "前走5レースキー"),
    FieldDef("wakuban",         324, 1, "9", "枠番"),
    # 第5版追加
    FieldDef("sogo_shirushi",   327, 1, "9", "総合印"),
    FieldDef("idm_shirushi",    328, 1, "9", "IDM印"),
    FieldDef("joho_shirushi",   329, 1, "9", "情報印"),
    FieldDef("kishu_shirushi",  330, 1, "9", "騎手印"),
    FieldDef("kyusha_shirushi", 331, 1, "9", "厩舎印"),
    FieldDef("chokyo_shirushi", 332, 1, "9", "調教印"),
    FieldDef("gekiso_shirushi", 333, 1, "9", "激走印"),
    FieldDef("shiba_tekisei_code", 334, 1, "X", "芝適性コード"),
    FieldDef("da_tekisei_code",  335, 1, "X", "ダ適性コード"),
    FieldDef("kishu_code",       336, 5, "9", "騎手コード"),
    FieldDef("chokyoshi_code",   341, 5, "9", "調教師コード"),
    # 第6版追加
    FieldDef("kakutoku_shokin",  347, 6, "Z", "獲得賞金（万円）"),
    FieldDef("shutoku_shokin",   353, 5, "Z", "収得賞金（万円）"),
    FieldDef("jouken_class",     358, 1, "9", "条件クラス"),
    FieldDef("ten_shisu",        359, 5, "Z", "テン指数"),
    FieldDef("pace_shisu",       364, 5, "Z", "ペース指数"),
    FieldDef("agari_shisu",      369, 5, "Z", "上がり指数"),
    FieldDef("ichi_shisu",       374, 5, "Z", "位置指数"),
    FieldDef("pace_yosou",       379, 1, "X", "ペース予想(H/M/S)"),
    FieldDef("dochu_junni",      380, 2, "Z", "道中順位"),
    FieldDef("dochu_sa",         382, 2, "Z", "道中差"),
    FieldDef("dochu_uchisoto",   384, 1, "9", "道中内外"),
    FieldDef("ato3f_junni",      385, 2, "Z", "後3F順位"),
    FieldDef("ato3f_sa",         387, 2, "Z", "後3F差"),
    FieldDef("ato3f_uchisoto",   389, 1, "9", "後3F内外"),
    FieldDef("goal_junni",       390, 2, "Z", "ゴール順位"),
    FieldDef("goal_sa",          392, 2, "Z", "ゴール差"),
    FieldDef("goal_uchisoto",    394, 1, "9", "ゴール内外"),
    FieldDef("tenkai_kigou",     395, 1, "X", "展開記号"),
    # 第6a版追加
    FieldDef("kyori_tekisei_2",  396, 1, "9", "距離適性2"),
    FieldDef("waku_bataiju",     397, 3, "9", "枠確定馬体重"),
    FieldDef("waku_bataiju_zougen", 400, 3, "X", "枠確定馬体重増減"),
    # 第7版追加
    FieldDef("torikeshi_flag",   403, 1, "9", "取消フラグ"),
    FieldDef("seibetsu_code",    404, 1, "9", "性別コード"),
    FieldDef("banushi_mei",      405, 40, "X", "馬主名"),
    FieldDef("banushikai_code",  445, 2, "9", "馬主会コード"),
    FieldDef("uma_kigou_code",   447, 2, "9", "馬記号コード"),
    FieldDef("gekiso_junni",     449, 2, "Z", "激走順位"),
    FieldDef("ls_shisu_junni",   451, 2, "Z", "LS指数順位"),
    FieldDef("ten_shisu_junni",  453, 2, "Z", "テン指数順位"),
    FieldDef("pace_shisu_junni", 455, 2, "Z", "ペース指数順位"),
    FieldDef("agari_shisu_junni", 457, 2, "Z", "上がり指数順位"),
    FieldDef("ichi_shisu_junni", 459, 2, "Z", "位置指数順位"),
    # 第8版追加
    FieldDef("kishu_tansho_ritsu", 461, 4, "Z", "騎手期待単勝率"),
    FieldDef("kishu_3chaku_ritsu", 465, 4, "Z", "騎手期待3着内率"),
    FieldDef("yuso_kubun",       469, 1, "X", "輸送区分"),
    # 第9版追加
    FieldDef("soho",             470, 8, "9", "走法"),
    FieldDef("taikei",           478, 24, "X", "体型"),
    FieldDef("taikei_sogo1",     502, 3, "9", "体型総合1"),
    FieldDef("taikei_sogo2",     505, 3, "9", "体型総合2"),
    FieldDef("taikei_sogo3",     508, 3, "9", "体型総合3"),
    FieldDef("uma_tokki1",       511, 3, "9", "馬特記1"),
    FieldDef("uma_tokki2",       514, 3, "9", "馬特記2"),
    FieldDef("uma_tokki3",       517, 3, "9", "馬特記3"),
    FieldDef("uma_start_shisu",  520, 4, "Z", "馬スタート指数"),
    FieldDef("uma_deokure_ritsu", 524, 4, "Z", "馬出遅率"),
    FieldDef("sankou_zenso",     528, 2, "9", "参考前走"),
    FieldDef("sankou_zenso_kishu_code", 530, 5, "X", "参考前走騎手コード"),
    FieldDef("manken_shisu",     535, 3, "Z", "万券指数"),
    FieldDef("manken_shirushi",  538, 1, "9", "万券印"),
    # 第10版追加
    FieldDef("koukyu_flag",      539, 1, "9", "降級フラグ"),
    FieldDef("gekiso_type",      540, 2, "X", "激走タイプ"),
    FieldDef("kyuuyou_bunrui_code", 542, 2, "9", "休養理由分類コード"),
    # 第11版追加
    FieldDef("flag",             544, 16, "X", "フラグ"),
    FieldDef("nyuukyu_nansou_me", 560, 2, "Z", "入厩何走目"),
    FieldDef("nyuukyu_nengappi", 562, 8, "9", "入厩年月日"),
    FieldDef("nyuukyu_nannnichi_mae", 570, 3, "Z", "入厩何日前"),
    FieldDef("houboku_saki",     573, 50, "X", "放牧先"),
    FieldDef("houboku_rank",     623, 1, "X", "放牧先ランク"),
    FieldDef("kyusha_rank",      624, 1, "9", "厩舎ランク"),
]


# =============================================================================
# CYB: 調教分析データ（96byte/record）
# =============================================================================
CYB_RECORD_LENGTH = 96

CYB_FIELDS: List[FieldDef] = [
    # レースキー
    FieldDef("basho_code",       1,  2, "9", "場コード"),
    FieldDef("year",             3,  2, "9", "年（下2桁）"),
    FieldDef("kai",              5,  1, "9", "回"),
    FieldDef("nichi",            6,  1, "F", "日（16進数）"),
    FieldDef("race_num",         7,  2, "9", "R"),
    FieldDef("umaban",           9,  2, "9", "馬番"),
    # 調教データ
    FieldDef("chokyo_type",      11, 2, "X", "調教タイプ"),
    FieldDef("chokyo_course_shubetsu", 13, 1, "X", "調教コース種別"),
    FieldDef("chokyo_saka",      14, 2, "9", "坂路"),
    FieldDef("chokyo_w",         16, 2, "9", "ウッドコース"),
    FieldDef("chokyo_da",        18, 2, "9", "ダートコース"),
    FieldDef("chokyo_shiba",     20, 2, "9", "芝コース"),
    FieldDef("chokyo_pu",        22, 2, "9", "プール"),
    FieldDef("chokyo_sho",       24, 2, "9", "障害"),
    FieldDef("chokyo_po",        26, 2, "9", "ポリトラック"),
    FieldDef("chokyo_kyori",     28, 1, "X", "調教距離"),
    FieldDef("chokyo_juten",     29, 1, "X", "調教重点"),
    FieldDef("oikiri_shisu",     30, 3, "Z", "追切指数"),
    FieldDef("shiage_shisu",     33, 3, "Z", "仕上指数"),
    FieldDef("chokyo_ryo_hyoka", 36, 1, "X", "調教量評価"),
    FieldDef("shiage_shisu_henka", 37, 1, "X", "仕上指数変化"),
    FieldDef("chokyo_comment",   38, 40, "X", "調教コメント"),
    FieldDef("comment_nengappi", 78, 8, "X", "コメント年月日"),
    FieldDef("chokyo_hyoka",     86, 1, "X", "調教評価"),
    FieldDef("isshuumae_oikiri_shisu", 87, 3, "Z", "一週前追切指数"),
    FieldDef("isshuumae_oikiri_course", 90, 2, "Z", "一週前追切コース"),
]


# =============================================================================
# BAC: 番組データ（176byte/record → 実際は184byte: 176+CR+LF=178 or 仕様書レコード長176byte）
# 注意: 仕様書では「改行 2 X 183」→ 実質レコード長は184byte (176+8=184? or 176+2=178)
# → 仕様書の最終「改行 2 X 183」は相対183=record末尾。レコード長は176+改行=184?
# → BAC仕様書: レコード長:176BYTE, 改行は183相対=182offset, 2byte → 182+2=184
# → wait: WIN5フラグ 相対177 1byte, 予備 相対178 5byte = 178+5-1 = 182, 改行183 2byte = 184
#   しかし仕様書は「レコード長：176BYTE」— 改行含まないのが「レコード長」
# =============================================================================
BAC_RECORD_LENGTH = 176  # 改行含まず。ファイル読み込み時にrstrip()で対応

BAC_FIELDS: List[FieldDef] = [
    # レースキー
    FieldDef("basho_code",       1,  2, "9", "場コード"),
    FieldDef("year",             3,  2, "9", "年（下2桁）"),
    FieldDef("kai",              5,  1, "9", "回"),
    FieldDef("nichi",            6,  1, "F", "日（16進数）"),
    FieldDef("race_num",         7,  2, "9", "R"),
    # 番組データ（馬番なし！）
    FieldDef("nengappi",         9,  8, "9", "年月日YYYYMMDD"),
    FieldDef("hasso_jikan",     17,  4, "X", "発走時間HHMM"),
    FieldDef("kyori",           21,  4, "9", "距離"),
    FieldDef("shiba_da_shogai_code", 25, 1, "9", "芝ダ障害コード"),
    FieldDef("migi_hidari",     26,  1, "9", "右左"),
    FieldDef("uchi_soto",       27,  1, "9", "内外"),
    FieldDef("shubetsu",        28,  2, "9", "種別"),
    FieldDef("jouken",          30,  2, "X", "条件"),
    FieldDef("kigou",           32,  3, "9", "記号"),
    FieldDef("juryo_shubetsu_code", 35, 1, "9", "重量(ハンデ等)"),
    FieldDef("grade",           36,  1, "9", "グレード"),
    FieldDef("race_mei",        37, 50, "X", "レース名"),
    FieldDef("kaisu",           87,  8, "X", "回数"),
    FieldDef("tosu",            95,  2, "9", "頭数"),
    FieldDef("course",          97,  1, "X", "コース"),
    # 第2版追加
    FieldDef("kaisai_kubun",    98,  1, "X", "開催区分"),
    FieldDef("race_mei_tanshuku", 99, 8, "X", "レース名短縮"),
    # 第3版追加
    FieldDef("race_mei_9",     107, 18, "X", "レース名9文字"),
    FieldDef("data_kubun",     125,  1, "X", "データ区分"),
    FieldDef("shokin_1",       126,  5, "Z", "1着賞金(万円)"),
    FieldDef("shokin_2",       131,  5, "Z", "2着賞金(万円)"),
    FieldDef("shokin_3",       136,  5, "Z", "3着賞金(万円)"),
    FieldDef("shokin_4",       141,  5, "Z", "4着賞金(万円)"),
    FieldDef("shokin_5",       146,  5, "Z", "5着賞金(万円)"),
    FieldDef("sannyu_shokin_1", 151, 5, "Z", "1着算入賞金(万円)"),
    FieldDef("sannyu_shokin_2", 156, 5, "Z", "2着算入賞金(万円)"),
    # 第4版追加
    FieldDef("baken_hatsubai_flag", 161, 16, "9", "馬券発売フラグ"),
    FieldDef("win5_flag",      177,  1, "Z", "WIN5フラグ"),
]


# =============================================================================
# JOA: 成績速報データ（116byte/record）
# =============================================================================
JOA_RECORD_LENGTH = 116

JOA_FIELDS: List[FieldDef] = [
    # レースキー
    FieldDef("basho_code",       1,  2, "9", "場コード"),
    FieldDef("year",             3,  2, "9", "年（下2桁）"),
    FieldDef("kai",              5,  1, "9", "回"),
    FieldDef("nichi",            6,  1, "F", "日（16進数）"),
    FieldDef("race_num",         7,  2, "9", "R"),
    FieldDef("umaban",           9,  2, "9", "馬番"),
    FieldDef("kettou_toroku_bango", 11, 8, "X", "血統登録番号"),
    FieldDef("bamei",           19, 36, "X", "馬名"),
    # 指数
    FieldDef("odds_shisu",      55,  5, "Z", "オッズ指数"),
    FieldDef("odds_shisu_junni", 60, 5, "Z", "オッズ指数順位"),
    FieldDef("maid_odds_yosoku", 65, 5, "Z", "MAID・オッズ予測"),
    FieldDef("maid_odds_yosoku_naka", 70, 5, "Z", "MAID・オッズ予測中"),
    FieldDef("maid_shisu",       75, 5, "Z", "MAID指数"),
    FieldDef("maid_junni",       80, 3, "Z", "MAID順位"),
    FieldDef("ls_shisu",         83, 5, "Z", "LS指数"),
    FieldDef("ls_hyoka",         88, 1, "X", "LS評価(A,B,C)"),
    FieldDef("em_flag",          89, 1, "X", "EM(該当時1)"),
    # 第2版追加
    FieldDef("kyusha_bb",        90, 1, "9", "厩舎におけるBB"),
    FieldDef("kyusha_bb_tanp",   91, 5, "Z", "厩舎BB単P"),
    FieldDef("kyusha_bb_rentai", 96, 5, "Z", "厩舎BB連対率"),
    FieldDef("kishu_bb",        101, 1, "9", "騎手BB"),
    FieldDef("kishu_bb_tanp",   102, 5, "Z", "騎手BB単P"),
    FieldDef("kishu_bb_rentai", 107, 5, "Z", "騎手BB連対率"),
]


# =============================================================================
# 必要カラムの選択（PostgreSQLに格納するカラム）
# =============================================================================

# Phase1で使用するカラム（20ファクター + JOINキー + 診断用）
KYI_IMPORT_FIELDS = [
    "basho_code", "year", "kai", "nichi", "race_num", "umaban",
    "kettou_toroku_bango", "bamei",
    "idm", "kishu_shisu", "sogo_shisu",
    "kyakushitsu", "kyori_tekisei",
    "chokyo_shisu", "kyusha_shisu",
    "chokyo_yajirushi_code",
    "omo_tekisei_code",
    "shiba_tekisei_code", "da_tekisei_code",
    "soho",
    "blinker",
    "futan_juryo",
    "ten_shisu", "pace_shisu", "agari_shisu", "ichi_shisu",
    "kyori_tekisei_2",
    "seibetsu_code",
    "kishu_code", "chokyoshi_code",
    "kyusha_rank",
]

CYB_IMPORT_FIELDS = [
    "basho_code", "year", "kai", "nichi", "race_num", "umaban",
    "chokyo_type",
    "oikiri_shisu", "shiage_shisu",
    "chokyo_ryo_hyoka", "shiage_shisu_henka",
    "chokyo_hyoka",
]

BAC_IMPORT_FIELDS = [
    "basho_code", "year", "kai", "nichi", "race_num",
    "nengappi",
    "kyori", "shiba_da_shogai_code", "migi_hidari", "uchi_soto",
    "shubetsu", "jouken",
    "juryo_shubetsu_code", "grade",
    "tosu", "course",
    "kaisai_kubun",
]

JOA_IMPORT_FIELDS = [
    "basho_code", "year", "kai", "nichi", "race_num", "umaban",
    "ls_shisu", "ls_hyoka",
    "odds_shisu",
]
