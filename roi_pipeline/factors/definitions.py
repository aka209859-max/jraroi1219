"""
代表ファクター20個の定義

Phase 1: 全ファクターのセグメント分類は GLOBAL。
SURFACE_2 / COURSE_27 は後続フェーズで適用。

分類:
    能力系 (4): IDM, 総合指数, 上がり指数, ペース指数
    適性系 (4): 距離適性, コース適性, 馬場適性, 馬場状態コード
    状態系 (4): 変わり身, 成長曲線, 調教評価, 調教評価ランク
    環境系 (4): 馬番, 馬齢, ブリンカー使用, 負担重量種別
    関係者系 (4): 騎手指数, 調教師指数, LS指数, 厩舎指数
"""
from dataclasses import dataclass
from enum import Enum
from typing import List

from roi_pipeline.config.segment_types import SegmentType


class FactorType(str, Enum):
    """ファクターのデータ型"""
    NUMERIC = "numeric"      # 数値系 → 等頻度分割でビン化
    CATEGORY = "category"    # カテゴリ系 → 各値ごとに集計
    ORDINAL = "ordinal"      # 順序付きカテゴリ → 各値ごとに集計


@dataclass(frozen=True)
class FactorDefinition:
    """ファクター定義"""
    id: int
    name: str
    table: str
    column: str
    factor_type: FactorType
    segment_type: SegmentType
    category: str
    description: str = ""
    n_bins: int = 10          # NUMERIC型の場合のデフォルトビン数


# =============================================================================
# 20個の代表ファクター定義
# =============================================================================

FACTOR_DEFINITIONS: List[FactorDefinition] = [
    # ---- 能力系 ----
    FactorDefinition(
        id=1, name="IDM", table="jrd_kyi", column="idm",
        factor_type=FactorType.NUMERIC, segment_type=SegmentType.GLOBAL,
        category="能力系", description="総合能力指数（JRDB）",
        n_bins=20,
    ),
    FactorDefinition(
        id=2, name="総合指数", table="jrd_kyi", column="sogo_index",
        factor_type=FactorType.NUMERIC, segment_type=SegmentType.GLOBAL,
        category="能力系", description="総合指数（JRDB）",
        n_bins=20,
    ),
    FactorDefinition(
        id=3, name="上がり指数", table="jrd_kyi", column="joh_index",
        factor_type=FactorType.NUMERIC, segment_type=SegmentType.GLOBAL,
        category="能力系", description="上がり3Fの能力指数",
        n_bins=20,
    ),
    FactorDefinition(
        id=4, name="ペース指数", table="jrd_kyi", column="pace_index",
        factor_type=FactorType.NUMERIC, segment_type=SegmentType.GLOBAL,
        category="能力系", description="ペース対応能力指数",
        n_bins=20,
    ),

    # ---- 適性系 ----
    FactorDefinition(
        id=5, name="距離適性", table="jrd_kyi", column="kyori_tekisei",
        factor_type=FactorType.CATEGORY, segment_type=SegmentType.GLOBAL,
        category="適性系", description="距離適性評価",
    ),
    FactorDefinition(
        id=6, name="コース適性", table="jrd_kyi", column="course_tekisei",
        factor_type=FactorType.CATEGORY, segment_type=SegmentType.GLOBAL,
        category="適性系", description="コース適性評価",
    ),
    FactorDefinition(
        id=7, name="馬場適性", table="jrd_kyi", column="baba_tekisei",
        factor_type=FactorType.CATEGORY, segment_type=SegmentType.GLOBAL,
        category="適性系", description="馬場適性評価",
    ),
    FactorDefinition(
        id=8, name="馬場状態コード", table="jvd_ra",
        column="babajotai_code_shiba",
        factor_type=FactorType.CATEGORY, segment_type=SegmentType.GLOBAL,
        category="適性系",
        description="馬場状態コード（芝: babajotai_code_shiba / ダート: babajotai_code_dirt）",
    ),

    # ---- 状態系 ----
    FactorDefinition(
        id=9, name="変わり身", table="jrd_kyi", column="kawarimi",
        factor_type=FactorType.CATEGORY, segment_type=SegmentType.GLOBAL,
        category="状態系", description="前走からの変わり身評価",
    ),
    FactorDefinition(
        id=10, name="成長曲線", table="jrd_kyi", column="seichoku",
        factor_type=FactorType.CATEGORY, segment_type=SegmentType.GLOBAL,
        category="状態系", description="成長曲線の段階評価",
    ),
    FactorDefinition(
        id=11, name="調教評価", table="jrd_kyi", column="chokyo_hyoka",
        factor_type=FactorType.CATEGORY, segment_type=SegmentType.GLOBAL,
        category="状態系", description="調教評価コード",
    ),
    FactorDefinition(
        id=12, name="調教評価ランク", table="jrd_cyb", column="chokyo_hyoka_rank",
        factor_type=FactorType.CATEGORY, segment_type=SegmentType.GLOBAL,
        category="状態系", description="調教分析によるランク評価",
    ),

    # ---- 環境系 ----
    FactorDefinition(
        id=13, name="馬番", table="jvd_se", column="umaban",
        factor_type=FactorType.ORDINAL, segment_type=SegmentType.GLOBAL,
        category="環境系", description="馬番（1〜18の各値ごと）",
    ),
    FactorDefinition(
        id=14, name="馬齢", table="jvd_se", column="barei",
        factor_type=FactorType.ORDINAL, segment_type=SegmentType.GLOBAL,
        category="環境系", description="馬齢",
    ),
    FactorDefinition(
        id=15, name="ブリンカー使用", table="jvd_se", column="blinker_shiyo_kubun",
        factor_type=FactorType.CATEGORY, segment_type=SegmentType.GLOBAL,
        category="環境系", description="ブリンカー使用区分",
    ),
    FactorDefinition(
        id=16, name="負担重量種別", table="jrd_bac", column="race_weight_type",
        factor_type=FactorType.CATEGORY, segment_type=SegmentType.GLOBAL,
        category="環境系", description="負担重量の種別コード",
    ),

    # ---- 関係者系 ----
    FactorDefinition(
        id=17, name="騎手指数", table="jrd_kyi", column="jockey_index",
        factor_type=FactorType.NUMERIC, segment_type=SegmentType.GLOBAL,
        category="関係者系", description="騎手の能力指数",
        n_bins=20,
    ),
    FactorDefinition(
        id=18, name="調教師指数", table="jrd_kyi", column="trainer_index",
        factor_type=FactorType.NUMERIC, segment_type=SegmentType.GLOBAL,
        category="関係者系", description="調教師の能力指数",
        n_bins=20,
    ),
    FactorDefinition(
        id=19, name="LS指数", table="jrd_joa", column="ls_index",
        factor_type=FactorType.NUMERIC, segment_type=SegmentType.GLOBAL,
        category="関係者系", description="騎手・厩舎連携指数",
        n_bins=20,
    ),
    FactorDefinition(
        id=20, name="厩舎指数", table="jrd_kyi", column="stable_index",
        factor_type=FactorType.NUMERIC, segment_type=SegmentType.GLOBAL,
        category="関係者系", description="厩舎の能力指数",
        n_bins=20,
    ),
]


def get_factor_by_id(factor_id: int) -> FactorDefinition:
    """IDでファクター定義を取得する。"""
    for f in FACTOR_DEFINITIONS:
        if f.id == factor_id:
            return f
    raise ValueError(f"Factor ID {factor_id} not found")


def get_factors_by_category(category: str) -> List[FactorDefinition]:
    """カテゴリでファクター定義をフィルタする。"""
    return [f for f in FACTOR_DEFINITIONS if f.category == category]
