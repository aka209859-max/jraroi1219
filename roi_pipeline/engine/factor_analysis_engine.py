"""
325ファクター × 3セグメント エッジ分析エンジン

各ファクター × 各セグメント（GLOBAL / SURFACE_2 / COURSE_27）で
補正回収率を算出し、採用可否を判定する。

補正回収率の計算方法:
    1. 均等払戻方式: bet = TARGET_PAYOUT / odds, hit払戻 = TARGET_PAYOUT * correction
    2. オッズ帯別補正係数: odds_correction.py の108段階テーブル
    3. 期間重み付け: 2016=1 ... 2025=10

信頼度調整:
    confidence = sqrt(N / (N + 400))
    adjusted = (corrected_roi - 80) * confidence + 80

採用基準:
    S: 単勝・複勝両方で adjusted > 80 のビンが2個以上
    A: 単勝または複勝で adjusted > 80 のビンが2個以上
    B: 単勝または複勝で adjusted > 80 のビンが1個
    C: 合格ビンなし（不採用）
"""
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from roi_pipeline.config.odds_correction import get_odds_correction
from roi_pipeline.config.year_weights import get_year_weight
from roi_pipeline.config.course_categories import get_category, ALL_CATEGORIES
from roi_pipeline.engine.factor_catalog_325 import Factor325, ALL_FACTORS_325, ACTIVE_FACTORS

TARGET_PAYOUT = 10000.0
BASELINE = 80.0
NULL_SKIP_THRESHOLD = 0.50   # NULLが50%超でスキップ
MIN_BIN_SAMPLES = 30          # ビンのサンプル数下限
CONFIDENCE_K = 400.0          # 信頼度計算のハーフ点


# =============================================================================
# 結果データクラス
# =============================================================================

@dataclass
class BinResult:
    """1ビン × 1セグメントの分析結果"""
    factor_alias: str
    segment_type: str       # GLOBAL / SURFACE_2 / COURSE_27
    segment_value: str      # "全体" / "芝" / "ダ" / コースカテゴリ名
    bin_value: str
    n: int
    tansho_hit_rate: float
    tansho_roi: float       # 補正回収率（%）
    tansho_confidence: float
    tansho_adjusted: float  # 信頼度調整済み回収率
    fukusho_hit_rate: float
    fukusho_roi: float
    fukusho_confidence: float
    fukusho_adjusted: float
    tansho_pass: bool       # adjusted > BASELINE
    fukusho_pass: bool


@dataclass
class FactorResult:
    """1ファクターの全セグメント分析結果"""
    factor: Factor325
    skipped: bool = False
    skip_reason: str = ""
    null_rate: float = 0.0
    bins: List[BinResult] = field(default_factory=list)

    @property
    def tansho_pass_bins(self) -> List[BinResult]:
        return [b for b in self.bins if b.tansho_pass]

    @property
    def fukusho_pass_bins(self) -> List[BinResult]:
        return [b for b in self.bins if b.fukusho_pass]

    @property
    def best_tansho_adjusted(self) -> float:
        vals = [b.tansho_adjusted for b in self.bins]
        return max(vals) if vals else BASELINE

    @property
    def best_fukusho_adjusted(self) -> float:
        vals = [b.fukusho_adjusted for b in self.bins]
        return max(vals) if vals else BASELINE

    @property
    def grade(self) -> str:
        t = len(self.tansho_pass_bins)
        f = len(self.fukusho_pass_bins)
        if t >= 2 and f >= 2:
            return "S"
        if t >= 2 or f >= 2:
            return "A"
        if t >= 1 or f >= 1:
            return "B"
        return "C"

    @property
    def best_segment(self) -> str:
        """最高単勝調整済みROIを持つセグメント。"""
        if not self.bins:
            return "N/A"
        best = max(self.bins, key=lambda b: b.tansho_adjusted)
        return best.segment_type

    def to_csv_rows(self) -> List[dict]:
        """CSV出力用の行リストを返す。"""
        if self.skipped:
            return [{
                "fid": self.factor.fid,
                "factor_alias": self.factor.alias,
                "table": self.factor.table,
                "column": self.factor.column,
                "description": self.factor.description,
                "kind": self.factor.kind,
                "skipped": True,
                "skip_reason": self.skip_reason or self.factor.skip_reason,
                "null_rate": round(self.null_rate, 4),
                "segment_type": "",
                "segment_value": "",
                "bin_value": "",
                "n": 0,
                "tansho_hit_rate": None,
                "tansho_roi": None,
                "tansho_confidence": None,
                "tansho_adjusted": None,
                "fukusho_hit_rate": None,
                "fukusho_roi": None,
                "fukusho_confidence": None,
                "fukusho_adjusted": None,
                "tansho_pass": False,
                "fukusho_pass": False,
                "grade": "C",
            }]
        rows = []
        for b in self.bins:
            rows.append({
                "fid": self.factor.fid,
                "factor_alias": self.factor.alias,
                "table": self.factor.table,
                "column": self.factor.column,
                "description": self.factor.description,
                "kind": self.factor.kind,
                "skipped": False,
                "skip_reason": "",
                "null_rate": round(self.null_rate, 4),
                "segment_type": b.segment_type,
                "segment_value": b.segment_value,
                "bin_value": b.bin_value,
                "n": b.n,
                "tansho_hit_rate": round(b.tansho_hit_rate, 4),
                "tansho_roi": round(b.tansho_roi, 2),
                "tansho_confidence": round(b.tansho_confidence, 4),
                "tansho_adjusted": round(b.tansho_adjusted, 2),
                "fukusho_hit_rate": round(b.fukusho_hit_rate, 4),
                "fukusho_roi": round(b.fukusho_roi, 2),
                "fukusho_confidence": round(b.fukusho_confidence, 4),
                "fukusho_adjusted": round(b.fukusho_adjusted, 2),
                "tansho_pass": b.tansho_pass,
                "fukusho_pass": b.fukusho_pass,
                "grade": self.grade,
            })
        return rows


# =============================================================================
# コアロジック
# =============================================================================

def _calc_roi(group: pd.DataFrame, is_fukusho: bool) -> Tuple[float, float, int, int]:
    """
    グループのROIを計算する（均等払戻方式）。

    Returns:
        (roi, hit_rate, n_samples, n_hits)
    """
    if is_fukusho:
        odds_col = "fukusho_odds"
        hit_col = "is_fukusho_hit"
    else:
        odds_col = "tansho_odds"
        hit_col = "is_hit"

    valid = group.dropna(subset=[odds_col, hit_col, "race_year"])
    valid = valid[pd.to_numeric(valid[odds_col], errors="coerce") > 0]
    n = len(valid)
    if n == 0:
        return BASELINE, 0.0, 0, 0

    odds = pd.to_numeric(valid[odds_col], errors="coerce")
    hits = valid[hit_col].astype(int)
    years = valid["race_year"].astype(str)

    year_weights = years.map(lambda y: get_year_weight(y)).astype(float)
    corrections = odds.map(lambda o: get_odds_correction(o, is_fukusho)).astype(float)

    bet_amounts = TARGET_PAYOUT / odds.replace(0, np.nan)
    corrected_payouts = TARGET_PAYOUT * corrections * hits

    total_bet = (bet_amounts * year_weights).sum()
    total_pay = (corrected_payouts * year_weights).sum()

    if total_bet <= 0:
        roi = BASELINE
    else:
        roi = (total_pay / total_bet) * 100.0

    n_hits = int(hits.sum())
    hit_rate = n_hits / n if n > 0 else 0.0
    return roi, hit_rate, n, n_hits


def _confidence(n: int) -> float:
    """信頼度 = sqrt(N / (N + 400))"""
    return float(np.sqrt(n / (n + CONFIDENCE_K)))


def _adjusted_roi(roi: float, conf: float) -> float:
    """信頼度調整済み回収率 = (roi - 80) * conf + 80"""
    return (roi - BASELINE) * conf + BASELINE


def _bin_series(series: pd.Series, factor: Factor325) -> pd.Series:
    """
    ファクターの種類に応じてビン化した Series を返す。
    NUMERIC: n_bins 分位、CATEGORY/ORDINAL: そのまま文字列
    """
    alias = factor.alias
    if factor.kind == "NUMERIC":
        numeric = pd.to_numeric(series, errors="coerce")
        valid_mask = numeric.notna()
        if valid_mask.sum() == 0:
            return pd.Series(pd.NA, index=series.index, dtype="object")
        try:
            n_bins = min(factor.n_bins, numeric[valid_mask].nunique())
            if n_bins < 2:
                return pd.Series(pd.NA, index=series.index, dtype="object")
            binned = pd.qcut(numeric, q=n_bins, labels=False, duplicates="drop")
            # ラベルを "Q1", "Q2", ... に
            bin_map = {i: f"Q{i+1}" for i in range(n_bins)}
            result = binned.map(lambda x: bin_map.get(int(x), pd.NA) if pd.notna(x) else pd.NA)
            return result.astype("object")
        except Exception:
            return pd.Series(pd.NA, index=series.index, dtype="object")
    else:
        # CATEGORY / ORDINAL: 空白→NA
        result = series.astype(str).replace(r"^\s*$", pd.NA, regex=True)
        result = result.replace("nan", pd.NA)
        return result


def _assign_surface(df: pd.DataFrame) -> pd.Series:
    """surface_2 カラムを確認して返す（既に付与済みの場合はそのまま）。"""
    if "surface_2" in df.columns:
        return df["surface_2"].copy()
    # フォールバック: track_code から判定
    tc = df.get("ra_track_code", df.get("track_code", pd.Series(dtype="str")))
    return tc.astype(str).map(
        lambda x: "芝" if x.startswith("1") else ("ダ" if x.startswith("2") else "その他")
    )


def _assign_course_27(df: pd.DataFrame) -> pd.Series:
    """COURSE_27 カテゴリを付与する。"""
    keibajo = df.get("keibajo_code", pd.Series(dtype="str"))
    track = df.get("track_code_for_course", df.get("ra_track_code", pd.Series(dtype="str")))
    kyori = pd.to_numeric(
        df.get("kyori_for_course", df.get("ra_kyori", pd.Series(dtype="str"))),
        errors="coerce"
    )
    surface = _assign_surface(df)

    def _get_cat(row):
        kb = str(row[0]).strip()
        surf = str(row[1])
        dist = row[2]
        if pd.isna(dist):
            return "unknown"
        return get_category(kb, surf, int(dist))

    return pd.Series(
        list(map(_get_cat, zip(keibajo, surface, kyori))),
        index=df.index,
    )


def analyze_factor(
    df: pd.DataFrame,
    factor: Factor325,
) -> FactorResult:
    """
    1ファクターを GLOBAL / SURFACE_2 / COURSE_27 の3セグメントで分析する。

    Args:
        df: load_full_factor_data で取得したDataFrame（全期間）
        factor: 分析するファクター

    Returns:
        FactorResult
    """
    result = FactorResult(factor=factor)

    # SKIP チェック（カタログで既にSKIPマーク）
    if factor.kind == "SKIP":
        result.skipped = True
        result.skip_reason = factor.skip_reason
        return result

    # カラム存在確認
    col = factor.alias
    if col not in df.columns:
        result.skipped = True
        result.skip_reason = "COLUMN_NOT_IN_DATA"
        return result

    # NULL率チェック
    n_total = len(df)
    n_valid = df[col].notna().sum()
    null_rate = 1.0 - (n_valid / n_total) if n_total > 0 else 1.0
    result.null_rate = null_rate

    if null_rate > NULL_SKIP_THRESHOLD:
        result.skipped = True
        result.skip_reason = f"HIGH_NULL({null_rate:.1%})"
        return result

    # ビン化
    binned = _bin_series(df[col], factor)
    if binned.isna().all():
        result.skipped = True
        result.skip_reason = "BIN_FAILED"
        return result

    # セグメント付与
    surface = _assign_surface(df)
    course27 = _assign_course_27(df)

    # 単勝的中フラグ
    if "is_hit" not in df.columns:
        df = df.copy()
        df["is_hit"] = (
            pd.to_numeric(df.get("kakutei_chakujun", pd.Series(dtype="str")), errors="coerce") == 1
        ).astype(int)
    # 複勝的中フラグ（fukusho_oddsがある=3着以内）
    if "is_fukusho_hit" not in df.columns:
        df = df.copy()
        df["is_fukusho_hit"] = df["fukusho_odds"].notna().astype(int)

    work = df.copy()
    work["_bin"] = binned
    work["_surface"] = surface
    work["_course27"] = course27
    work = work.dropna(subset=["_bin"])

    all_bins: List[BinResult] = []

    # ----- GLOBAL -----
    for bin_val, grp in work.groupby("_bin", sort=True):
        if len(grp) < MIN_BIN_SAMPLES:
            continue
        t_roi, t_hr, t_n, _ = _calc_roi(grp, is_fukusho=False)
        f_roi, f_hr, f_n, _ = _calc_roi(grp, is_fukusho=True)
        t_conf = _confidence(t_n)
        f_conf = _confidence(f_n)
        t_adj = _adjusted_roi(t_roi, t_conf)
        f_adj = _adjusted_roi(f_roi, f_conf)
        all_bins.append(BinResult(
            factor_alias=col,
            segment_type="GLOBAL",
            segment_value="全体",
            bin_value=str(bin_val),
            n=t_n,
            tansho_hit_rate=t_hr,
            tansho_roi=t_roi,
            tansho_confidence=t_conf,
            tansho_adjusted=t_adj,
            fukusho_hit_rate=f_hr,
            fukusho_roi=f_roi,
            fukusho_confidence=f_conf,
            fukusho_adjusted=f_adj,
            tansho_pass=(t_adj > BASELINE),
            fukusho_pass=(f_adj > BASELINE),
        ))

    # ----- SURFACE_2 -----
    for surf_val in ["芝", "ダ"]:
        surf_grp = work[work["_surface"] == surf_val]
        if len(surf_grp) < MIN_BIN_SAMPLES:
            continue
        for bin_val, grp in surf_grp.groupby("_bin", sort=True):
            if len(grp) < MIN_BIN_SAMPLES:
                continue
            t_roi, t_hr, t_n, _ = _calc_roi(grp, is_fukusho=False)
            f_roi, f_hr, f_n, _ = _calc_roi(grp, is_fukusho=True)
            t_conf = _confidence(t_n)
            f_conf = _confidence(f_n)
            t_adj = _adjusted_roi(t_roi, t_conf)
            f_adj = _adjusted_roi(f_roi, f_conf)
            all_bins.append(BinResult(
                factor_alias=col,
                segment_type="SURFACE_2",
                segment_value=surf_val,
                bin_value=str(bin_val),
                n=t_n,
                tansho_hit_rate=t_hr,
                tansho_roi=t_roi,
                tansho_confidence=t_conf,
                tansho_adjusted=t_adj,
                fukusho_hit_rate=f_hr,
                fukusho_roi=f_roi,
                fukusho_confidence=f_conf,
                fukusho_adjusted=f_adj,
                tansho_pass=(t_adj > BASELINE),
                fukusho_pass=(f_adj > BASELINE),
            ))

    # ----- COURSE_27 -----
    course_names = list(ALL_CATEGORIES.keys())
    for course_val in course_names:
        course_grp = work[work["_course27"] == course_val]
        if len(course_grp) < MIN_BIN_SAMPLES:
            continue
        for bin_val, grp in course_grp.groupby("_bin", sort=True):
            if len(grp) < MIN_BIN_SAMPLES:
                continue
            t_roi, t_hr, t_n, _ = _calc_roi(grp, is_fukusho=False)
            f_roi, f_hr, f_n, _ = _calc_roi(grp, is_fukusho=True)
            t_conf = _confidence(t_n)
            f_conf = _confidence(f_n)
            t_adj = _adjusted_roi(t_roi, t_conf)
            f_adj = _adjusted_roi(f_roi, f_conf)
            all_bins.append(BinResult(
                factor_alias=col,
                segment_type="COURSE_27",
                segment_value=course_val,
                bin_value=str(bin_val),
                n=t_n,
                tansho_hit_rate=t_hr,
                tansho_roi=t_roi,
                tansho_confidence=t_conf,
                tansho_adjusted=t_adj,
                fukusho_hit_rate=f_hr,
                fukusho_roi=f_roi,
                fukusho_confidence=f_conf,
                fukusho_adjusted=f_adj,
                tansho_pass=(t_adj > BASELINE),
                fukusho_pass=(f_adj > BASELINE),
            ))

    result.bins = all_bins
    return result


def analyze_all_factors(
    df: pd.DataFrame,
    factors: Optional[List[Factor325]] = None,
    verbose: bool = True,
) -> List[FactorResult]:
    """
    全ファクター（または指定ファクター）を一括分析する。

    Args:
        df: load_full_factor_data で取得したDataFrame
        factors: 分析するファクターリスト。Noneなら全325。
        verbose: 進捗表示

    Returns:
        FactorResult のリスト（325件）
    """
    # 事前に共通カラムを付与
    if "is_hit" not in df.columns:
        df = df.copy()
        df["is_hit"] = (
            pd.to_numeric(df.get("kakutei_chakujun", pd.Series(dtype="str")), errors="coerce") == 1
        ).astype(int)
    if "is_fukusho_hit" not in df.columns:
        df["is_fukusho_hit"] = df["fukusho_odds"].notna().astype(int)
    if "tansho_odds" in df.columns:
        df["tansho_odds"] = pd.to_numeric(df["tansho_odds"], errors="coerce")
        # JRA-VAN 単勝オッズは /10 単位で格納 → 実際のオッズに変換
        # （median > 10 → /10 変換が必要か確認）
        median_odds = df["tansho_odds"].median()
        if pd.notna(median_odds) and median_odds >= 10:
            df["tansho_odds"] = df["tansho_odds"] / 10.0
    if "race_year" not in df.columns:
        df["race_year"] = df["race_date"].str[:4] if "race_date" in df.columns else df.get("kaisai_nen", "")

    target_factors = factors if factors is not None else ALL_FACTORS_325
    results = []

    for i, factor in enumerate(target_factors):
        if verbose and (i % 20 == 0 or i == len(target_factors) - 1):
            print(f"  [{i+1}/{len(target_factors)}] {factor.alias} ({factor.kind})", flush=True)
        r = analyze_factor(df, factor)
        results.append(r)

    return results
