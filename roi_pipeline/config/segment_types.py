"""
セグメント分類タイプ定義

Phase 1では全ファクターに GLOBAL を適用。
SURFACE_2 と COURSE_27 は実装するが適用は後続フェーズ。
"""
from enum import Enum


class SegmentType(str, Enum):
    """セグメント分類のタイプ"""

    GLOBAL = "global"
    """レベル2なし。全条件を1つのグループとして扱う。"""

    SURFACE_2 = "surface"
    """芝/ダートの2分類。"""

    COURSE_27 = "course"
    """27カテゴリ分類（芝15 + ダート12）。"""
