"""
Phase 3 Walk-Forward時系列検証

Task 4: Task 1〜3の全パイプラインを月次ローリングWalk-Forwardで検証する。

Walk-Forward設計:
    学習窓: 24か月（スライディング）
    検証窓: 1か月
    ステップ: 1か月ずつスライド
    検証期間: 2021-01 ～ 2024-12（約48か月）

各月の処理フロー:
    1. 学習窓のデータで階層ベイズ推定 → edge_table 生成
    2. 学習窓のデータで Benter の alpha, beta を最尤推定
    3. 検証窓の各レースについて:
       a. 各馬の Log-EV スコア算出（Task 1）
       b. Benter 統合確率 P_final 算出（Task 2）
       c. ベイズ Kelly 投資比率 f* 算出（Task 3）
       d. f* > 0 の馬について実際のオッズと着順で損益を計算
    4. 月次指標を記録
"""
from __future__ import annotations

import warnings
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from scipy.stats import beta as beta_dist

from roi_pipeline.config.segment_types import SegmentType
from roi_pipeline.engine.bayesian_kelly import (
    bayesian_kelly,
    build_posterior,
    compute_n_eff,
)
from roi_pipeline.engine.benter_model import (
    benter_integrate,
    combine_scores,
    fit_benter_params,
    implied_probability,
)
from roi_pipeline.engine.corrected_return import calc_corrected_return_rate
from roi_pipeline.engine.hierarchical_bayes import hierarchical_bayes_estimate
from roi_pipeline.engine.log_ev_scorer import compute_horse_score
from roi_pipeline.factors.binning import apply_binning
from roi_pipeline.factors.definitions import FactorDefinition, FactorType


# ─────────────────────────────────────────────
# 評価指標関数
# ─────────────────────────────────────────────

def brier_score(p_predicted: np.ndarray, outcomes: np.ndarray) -> float:
    """
    Brier Score: BS = mean((p - outcome)^2)

    0 に近いほど確率予測が正確。完全予測 = 0.0。
    ランダム予測（p = 0.5, outcome ∈ {0,1}） ≈ 0.25。

    Args:
        p_predicted: 予測確率配列（各馬が勝つ確率）
        outcomes: 実績配列（勝馬=1, 他=0）

    Returns:
        Brier Score（float）
    """
    p = np.asarray(p_predicted, dtype=float)
    y = np.asarray(outcomes, dtype=float)
    return float(np.mean((p - y) ** 2))


def betting_sharpe(monthly_returns: np.ndarray) -> float:
    """
    Betting Sharpe Ratio（年率換算）

    Sharpe = mean(月次リターン) / std(月次リターン) * sqrt(12)
    目標: > 1.0

    Args:
        monthly_returns: 月次リターン配列（バンクロールに対する小数表示。0.05 = +5%）

    Returns:
        年率換算 Sharpe Ratio。std = 0 の場合は 0.0。
    """
    r = np.asarray(monthly_returns, dtype=float)
    std = float(np.std(r, ddof=0))
    if std == 0.0:
        return 0.0
    return float(np.mean(r) / std * np.sqrt(12))


def max_drawdown(cumulative_returns: np.ndarray) -> float:
    """
    最大ドローダウン（バンクロール比）

    目標: < 30%

    Args:
        cumulative_returns: 累積リターン配列（バンクロール乗数。1.0 = 開始時）
                           例: [1.0, 1.05, 1.02, 1.08, ...]

    Returns:
        最大ドローダウン（0.0〜1.0）。単調増加なら 0.0。半減なら 0.5。
    """
    cr = np.asarray(cumulative_returns, dtype=float)
    if len(cr) == 0:
        return 0.0
    peak = np.maximum.accumulate(cr)
    # ゼロ除算防止
    denom = np.where(peak == 0.0, 1.0, peak)
    dd = (peak - cr) / denom
    return float(np.max(dd))


def circuit_breaker(
    current_dd: float,
    threshold: float = 0.30,
    normal_c: float = 0.25,
    reduced_c: float = 0.15,
) -> float:
    """
    サーキットブレーカー: 最大DDが閾値を超えた場合にフラクショナル係数を縮小する。

    Args:
        current_dd: 現時点の最大ドローダウン（0.0〜1.0）
        threshold: 発動閾値（デフォルト 0.30 = 30%）
        normal_c: 通常時のフラクショナル係数（デフォルト 0.25）
        reduced_c: 発動後のフラクショナル係数（デフォルト 0.15）

    Returns:
        使用するフラクショナル係数
    """
    return reduced_c if current_dd >= threshold else normal_c


# ─────────────────────────────────────────────
# 月次検証結果
# ─────────────────────────────────────────────

@dataclass
class MonthlyP3Result:
    """Phase 3 Walk-Forward の月次検証結果"""
    year_month: str          # "YYYY-MM"
    n_bets: int              # ベット数（f* > 0 の馬数）
    n_hits: int              # 的中数
    monthly_return: float    # 月次リターン（バンクロール比。0.05 = +5%）
    cumulative_bankroll: float  # 累積バンクロール乗数（開始時 1.0）
    brier_model: float       # モデルの Brier Score
    brier_market: float      # 市場（オッズ逆数）の Brier Score
    fractional_c_used: float  # 使用したフラクショナル係数


# ─────────────────────────────────────────────
# セグメントラベル付与
# ─────────────────────────────────────────────

def _apply_train_bins_to_val(
    train_df: pd.DataFrame,
    val_df: pd.DataFrame,
    factor_def: FactorDefinition,
) -> pd.Series:
    """
    学習窓のビン境界を使用して検証データのビンラベルを算出する。

    edge_tableは train_df のみで apply_binning したラベルを使用しているため、
    bin_lookup も同じビン境界（train_df の qcut 境界）を val_df に適用する
    ことでキーの一致を保証する。

    数値系: train_df の qcut 境界を pd.cut で val_df に適用。
    カテゴリ/順序系: val_df の値をそのまま使用。

    Args:
        train_df: 学習窓 DataFrame
        val_df: 検証月 DataFrame
        factor_def: ファクター定義

    Returns:
        val_df のインデックスに対応するビンラベルの Series
    """
    from roi_pipeline.factors.definitions import FactorType as _FT

    col = factor_def.column
    bin_col_name = f"{col}_bin"

    if factor_def.factor_type == _FT.NUMERIC:
        train_numeric = pd.to_numeric(train_df[col], errors="coerce")
        val_numeric   = pd.to_numeric(val_df[col],   errors="coerce")
        train_valid   = train_numeric.dropna()

        if len(train_valid) == 0:
            return pd.Series(pd.NA, index=val_df.index, name=bin_col_name)

        try:
            # 学習窓でビン境界を計算（edge_table と同じ手順）
            _, bin_edges = pd.qcut(
                train_valid,
                q=factor_def.n_bins,
                retbins=True,
                duplicates="drop",
            )
            # edge_table と同じラベル形式
            n_actual = len(bin_edges) - 1
            labels = [
                f"{bin_edges[i]:.1f}-{bin_edges[i+1]:.1f}"
                for i in range(n_actual)
            ]
            # 端点を ±∞ に拡張して val_df の範囲外値も最端ビンに割り当て
            bin_edges_cut = bin_edges.copy()
            bin_edges_cut[0]  = -np.inf
            bin_edges_cut[-1] =  np.inf

            val_valid_mask = val_numeric.notna()
            result = pd.Series(pd.NA, index=val_df.index, dtype="object",
                               name=bin_col_name)
            if val_valid_mask.any():
                cut_result = pd.cut(
                    val_numeric[val_valid_mask],
                    bins=bin_edges_cut,
                    labels=labels,
                    include_lowest=True,
                )
                result[val_valid_mask] = cut_result.astype(str)
            return result

        except Exception:
            # フォールバック: 文字列変換
            result = val_numeric.astype(str)
            result[result == "nan"] = pd.NA
            result.name = bin_col_name
            return result

    else:
        # CATEGORY / ORDINAL: そのまま
        result = val_df[col].copy().astype(str)
        result = result.replace(r"^\s*$", pd.NA, regex=True)
        result.name = bin_col_name
        return result


def _assign_surface_label(df: pd.DataFrame) -> pd.Series:
    """
    track_code カラムから芝/ダートのセグメントラベルを付与する。

    track_code の先頭文字:
        "1" → "turf"（芝）
        "2" → "dirt"（ダート）
        その他 → "turf"（フォールバック）

    Args:
        df: track_code カラムを含む DataFrame

    Returns:
        "turf" または "dirt" の Series
    """
    track = df.get("track_code", pd.Series("", index=df.index))
    track = track.astype(str).str.strip().str[:1]
    return track.map({"1": "turf", "2": "dirt"}).fillna("turf").rename("_surface")


def _assign_segment_labels(
    df: pd.DataFrame,
    factor_def: FactorDefinition,
) -> pd.Series:
    """
    FactorDefinition の segment_type に応じてセグメントラベルを付与する。

    Args:
        df: 対象 DataFrame
        factor_def: ファクター定義

    Returns:
        セグメントラベルの Series（GLOBAL → "all", SURFACE_2 → "turf"/"dirt"）
    """
    if factor_def.segment_type == SegmentType.GLOBAL:
        return pd.Series("all", index=df.index, name="_segment")
    elif factor_def.segment_type == SegmentType.SURFACE_2:
        return _assign_surface_label(df)
    else:
        # COURSE_27 は "all" にフォールバック（Phase 3ではGLOBAL/SURFACE_2を優先）
        return pd.Series("all", index=df.index, name="_segment")


# ─────────────────────────────────────────────
# エッジテーブル構築
# ─────────────────────────────────────────────

def build_edge_table_from_df(
    df: pd.DataFrame,
    factor_defs: List[FactorDefinition],
    bet_type: str = "tansho",
    odds_col: str = "tansho_odds_val",
    hit_col: str = "is_hit",
    year_col: str = "race_year",
    is_fukusho: bool = False,
    global_rate: Optional[float] = None,
    C: int = 50,
    n_posterior_samples: int = 2000,
    rng_seed: int = 42,
) -> Dict:
    """
    学習窓の DataFrame から edge_table を構築する。

    各ファクター×セグメント×ビンについて:
        1. 観測補正回収率を算出
        2. 階層ベイズ推定（グローバル率を事前分布として使用）
        3. Beta 事後分布からサンプル生成

    edge_table の形式:
        {(factor_name, segment, bin, bet_type): {
            "posterior_mean": float,          # 補正回収率（小数。0.85 = 85%）
            "posterior_samples": np.ndarray,  # Beta サンプル（小数スケール）
            "N": int,                         # セルのサンプル数
        }}

    Args:
        df: 学習窓のデータ
        factor_defs: 使用するファクター定義リスト
        bet_type: "tansho" または "fukusho"
        odds_col: オッズカラム名
        hit_col: 的中フラグカラム名
        year_col: 年度カラム名（year_weight 計算用）
        is_fukusho: True=複勝モード
        global_rate: グローバル補正回収率（%）。None なら全データから計算。
        C: 階層ベイズの信頼性定数
        n_posterior_samples: 事後分布のサンプル数
        rng_seed: 再現性のための乱数シード

    Returns:
        edge_table（辞書）
    """
    rng = np.random.default_rng(rng_seed)
    edge_table: Dict = {}

    # グローバル補正回収率（事前分布の中心）
    if global_rate is None:
        global_stats = calc_corrected_return_rate(
            df, odds_col=odds_col, hit_flag_col=hit_col,
            year_col=year_col, is_fukusho=is_fukusho,
        )
        global_rate = global_stats["corrected_return_rate"]

    for factor_def in factor_defs:
        # ファクターカラムが存在しない場合はスキップ
        if factor_def.column not in df.columns:
            continue

        # ビン分割
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            try:
                bin_series, bin_col = apply_binning(df, factor_def)
            except Exception:
                continue

        # セグメントラベル付与
        seg_series = _assign_segment_labels(df, factor_def)

        # 一時 DataFrame に追加
        tmp = df[[odds_col, hit_col, year_col]].copy()
        tmp["_bin"] = bin_series.values
        tmp["_seg"] = seg_series.values

        # NaN を持つ行を除外
        tmp = tmp.dropna(subset=["_bin", "_seg", odds_col, hit_col, year_col])
        if len(tmp) == 0:
            continue

        # (segment, bin) でグループ化して補正回収率を算出
        for (seg_val, bin_val), group in tmp.groupby(["_seg", "_bin"], observed=True):
            if len(group) < 5:
                continue  # サンプルが少なすぎるセルはスキップ

            stats = calc_corrected_return_rate(
                group,
                odds_col=odds_col,
                hit_flag_col=hit_col,
                year_col=year_col,
                is_fukusho=is_fukusho,
            )

            observed_rate = stats["corrected_return_rate"]
            N = stats["n_samples"]

            if N == 0 or observed_rate <= 0:
                continue

            # 階層ベイズ推定（グローバルを事前分布として使用）
            bayes = hierarchical_bayes_estimate(
                observed_rate=observed_rate,
                n_samples=N,
                prior_rate=global_rate,
                C=C,
            )
            posterior_mean_pct = bayes.estimated_rate  # % スケール（例: 85.0）
            posterior_mean = posterior_mean_pct / 100.0  # 小数スケール（例: 0.85）

            # Beta 事後分布からサンプル生成
            a, b = build_posterior(
                p_final=posterior_mean,
                n_eff=float(N),
                prior_strength=float(C),
            )
            samples = beta_dist.rvs(a, b, size=n_posterior_samples,
                                    random_state=rng.integers(1 << 31))

            key = (factor_def.name, str(seg_val), str(bin_val), bet_type)
            edge_table[key] = {
                "posterior_mean": posterior_mean,
                "posterior_samples": samples,
                "N": N,
            }

    return edge_table


# ─────────────────────────────────────────────
# 1馬の horse_factors 構築
# ─────────────────────────────────────────────

def _get_horse_factors(
    horse_row: pd.Series,
    factor_defs: List[FactorDefinition],
    bin_lookup: Dict[str, pd.Series],
    idx,
) -> Dict[str, Tuple[str, str]]:
    """
    馬1頭の horse_factors 辞書を構築する。

    Args:
        horse_row: 馬1頭分の Series（元 DataFrame の行）
        factor_defs: ファクター定義リスト
        bin_lookup: {factor_name: bin_series} の辞書（検証月全体のビン値）
        idx: horse_row のインデックス

    Returns:
        {factor_name: (segment_label, bin_label)} の辞書
    """
    horse_factors: Dict[str, Tuple[str, str]] = {}

    for factor_def in factor_defs:
        if factor_def.column not in horse_row.index:
            continue
        if factor_def.name not in bin_lookup:
            continue

        bin_val = bin_lookup[factor_def.name].get(idx)
        if pd.isna(bin_val):
            continue

        # セグメントラベル
        if factor_def.segment_type == SegmentType.GLOBAL:
            seg = "all"
        elif factor_def.segment_type == SegmentType.SURFACE_2:
            track = str(horse_row.get("track_code", "")).strip()[:1]
            seg = {"1": "turf", "2": "dirt"}.get(track, "turf")
        else:
            seg = "all"

        horse_factors[factor_def.name] = (seg, str(bin_val))

    return horse_factors


# ─────────────────────────────────────────────
# Benter パラメータ推定（学習窓）
# ─────────────────────────────────────────────

def fit_benter_from_df(
    train_df: pd.DataFrame,
    edge_table_win: Dict,
    edge_table_place: Dict,
    factor_defs: List[FactorDefinition],
    race_id_col: str = "race_id",
    odds_col: str = "tansho_odds_val",
    hit_col: str = "is_hit",
) -> Tuple[float, float]:
    """
    学習窓のデータから Benter モデルの alpha, beta を最尤推定する。

    Args:
        train_df: 学習窓のデータ
        edge_table_win: 単勝 edge_table
        edge_table_place: 複勝 edge_table
        factor_defs: ファクター定義リスト
        race_id_col: レース識別子カラム
        odds_col: 単勝オッズカラム
        hit_col: 的中フラグカラム

    Returns:
        (alpha_hat, beta_hat) のタプル。
        推定に失敗した場合は (0.5, 1.0) を返す。
    """
    # 事前に全ファクターのビン値を計算（レース横断）
    bin_lookup_win: Dict[str, pd.Series] = {}
    bin_lookup_place: Dict[str, pd.Series] = {}

    for factor_def in factor_defs:
        if factor_def.column not in train_df.columns:
            continue
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            try:
                bs, _ = apply_binning(train_df, factor_def)
                bin_lookup_win[factor_def.name] = bs
                bin_lookup_place[factor_def.name] = bs
            except Exception:
                continue

    all_s: List[np.ndarray] = []
    all_pm: List[np.ndarray] = []
    all_oc: List[np.ndarray] = []

    for race_id, race_df in train_df.groupby(race_id_col, sort=False):
        if len(race_df) < 2:
            continue

        # オッズを数値変換
        odds_raw = pd.to_numeric(race_df[odds_col], errors="coerce")
        valid_odds = odds_raw > 1.0
        race_valid = race_df[valid_odds].copy()
        odds_arr = odds_raw[valid_odds].values.astype(float)

        if len(race_valid) < 2:
            continue

        try:
            pm = implied_probability(odds_arr)
        except Exception:
            continue

        # 各馬のスコア計算
        s_list = []
        for idx, row in race_valid.iterrows():
            hf_win = _get_horse_factors(row, factor_defs, bin_lookup_win, idx)
            hf_place = _get_horse_factors(row, factor_defs, bin_lookup_place, idx)
            s_w = compute_horse_score(hf_win, edge_table_win, bet_type="tansho")
            s_p = compute_horse_score(hf_place, edge_table_place, bet_type="fukusho")
            s_list.append(combine_scores(s_w, s_p))

        s_arr = np.array(s_list, dtype=float)

        # 的中フラグから outcomes を構築
        hit_vals = pd.to_numeric(race_valid[hit_col], errors="coerce").fillna(0).values
        if hit_vals.sum() != 1:
            continue  # 単勝的中が1頭でない場合はスキップ

        oc = hit_vals.astype(float)

        all_s.append(s_arr)
        all_pm.append(pm)
        all_oc.append(oc)

    if len(all_s) < 5:
        return 0.5, 1.0  # データ不足時はデフォルト値

    try:
        alpha_hat, beta_hat = fit_benter_params(all_s, all_pm, all_oc)
        return alpha_hat, beta_hat
    except Exception:
        return 0.5, 1.0


# ─────────────────────────────────────────────
# 1レースのスコアリング + Kelly 資金配分
# ─────────────────────────────────────────────

@dataclass
class HorseBet:
    """1頭分のベット情報"""
    umaban: str
    f_star: float       # Kelly 投資比率（バンクロール比）
    p_final: float      # Benter 統合確率
    p_market: float     # 市場暗示確率
    odds: float         # 確定オッズ
    is_hit: int         # 的中フラグ（1=的中, 0=不的中）
    n_eff: float        # 実効サンプル数


def score_race(
    race_df: pd.DataFrame,
    edge_table_win: Dict,
    edge_table_place: Dict,
    factor_defs: List[FactorDefinition],
    bin_lookup: Dict[str, pd.Series],
    benter_alpha: float,
    benter_beta: float,
    fractional_c: float = 0.25,
    odds_col: str = "tansho_odds_val",
    hit_col: str = "is_hit",
    umaban_col: str = "umaban",
    n_kelly_samples: int = 2000,
    kelly_rng_seed: Optional[int] = None,
) -> List[HorseBet]:
    """
    1レースを Task 1 → 2 → 3 の全パイプラインで処理する。

    Args:
        race_df: 1レース分のデータ
        edge_table_win: 単勝 edge_table
        edge_table_place: 複勝 edge_table
        factor_defs: ファクター定義リスト
        bin_lookup: {factor_name: bin_series}（検証月全体のビン値）
        benter_alpha: Benter モデルの alpha
        benter_beta: Benter モデルの beta
        fractional_c: フラクショナルKelly係数
        odds_col: 単勝オッズカラム
        hit_col: 的中フラグカラム
        umaban_col: 馬番カラム
        n_kelly_samples: Kelly モンテカルロサンプル数
        kelly_rng_seed: 再現性シード

    Returns:
        List[HorseBet]。f_star > 0 の馬のみ（f_star=0 の馬は含まれない）。
    """
    if len(race_df) < 2:
        return []

    odds_raw = pd.to_numeric(race_df[odds_col], errors="coerce")
    valid_mask = odds_raw > 1.0
    race_valid = race_df[valid_mask].copy()
    odds_arr = odds_raw[valid_mask].values.astype(float)

    if len(race_valid) < 2:
        return []

    try:
        pm = implied_probability(odds_arr)
    except Exception:
        return []

    # 各馬のスコアと n_eff を計算
    s_combined_list = []
    n_eff_list = []

    for idx, row in race_valid.iterrows():
        hf_win = _get_horse_factors(row, factor_defs, bin_lookup, idx)
        hf_place = _get_horse_factors(row, factor_defs, bin_lookup, idx)

        s_w = compute_horse_score(hf_win, edge_table_win, bet_type="tansho")
        s_p = compute_horse_score(hf_place, edge_table_place, bet_type="fukusho")
        s_combined_list.append(combine_scores(s_w, s_p))

        # n_eff: 通過したセルの N の調和平均
        ns = []
        for factor_name, (seg, bv) in hf_win.items():
            key = (factor_name, seg, bv, "tansho")
            if key in edge_table_win:
                ns.append(edge_table_win[key]["N"])
        n_eff_list.append(compute_n_eff(ns) if ns else 0.0)

    s_arr = np.array(s_combined_list, dtype=float)

    # Benter 統合 → P_final
    try:
        p_final_arr = benter_integrate(s_arr, pm, alpha=benter_alpha, beta=benter_beta)
    except Exception:
        p_final_arr = pm.copy()

    # Kelly 資金配分
    bets: List[HorseBet] = []
    hit_vals = pd.to_numeric(race_valid[hit_col], errors="coerce").fillna(0).values

    for i, (idx, row) in enumerate(race_valid.iterrows()):
        f_star = bayesian_kelly(
            p_final=float(p_final_arr[i]),
            odds=float(odds_arr[i]),
            n_eff=float(n_eff_list[i]),
            n_samples=n_kelly_samples,
            fractional_c=fractional_c,
            rng_seed=kelly_rng_seed,
        )

        if f_star > 0.0:
            bets.append(HorseBet(
                umaban=str(row.get(umaban_col, f"h{i}")),
                f_star=f_star,
                p_final=float(p_final_arr[i]),
                p_market=float(pm[i]),
                odds=float(odds_arr[i]),
                is_hit=int(hit_vals[i]),
                n_eff=float(n_eff_list[i]),
            ))

    return bets


# ─────────────────────────────────────────────
# 月次期間生成
# ─────────────────────────────────────────────

def _generate_rolling_periods(
    val_start_ym: str,
    val_end_ym: str,
    train_months: int = 24,
) -> List[Dict]:
    """
    ローリングWalk-Forwardの月次期間リストを生成する。

    Args:
        val_start_ym: 検証開始年月（"YYYY-MM"）
        val_end_ym: 検証終了年月（"YYYY-MM"）
        train_months: 学習窓の月数

    Returns:
        [{"train_start": "YYYYMMDD", "train_end": "YYYYMMDD",
          "val_start": "YYYYMMDD", "val_end": "YYYYMMDD",
          "val_ym": "YYYY-MM"}, ...]
    """
    periods = []
    val_ts = pd.Timestamp(val_start_ym + "-01")
    val_end_ts = pd.Timestamp(val_end_ym + "-01") + pd.offsets.MonthEnd(0)

    while val_ts <= val_end_ts:
        train_end_ts = val_ts - pd.Timedelta(days=1)
        train_start_ts = val_ts - pd.DateOffset(months=train_months)

        periods.append({
            "train_start": train_start_ts.strftime("%Y%m%d"),
            "train_end": train_end_ts.strftime("%Y%m%d"),
            "val_start": val_ts.strftime("%Y%m%d"),
            "val_end": (val_ts + pd.offsets.MonthEnd(0)).strftime("%Y%m%d"),
            "val_ym": val_ts.strftime("%Y-%m"),
        })
        val_ts += pd.offsets.MonthBegin(1)

    return periods


# ─────────────────────────────────────────────
# Phase 3 Walk-Forward メイン
# ─────────────────────────────────────────────

def run_phase3_walk_forward(
    df: pd.DataFrame,
    factor_defs: List[FactorDefinition],
    val_start_ym: str = "2021-01",
    val_end_ym: str = "2024-12",
    train_months: int = 24,
    date_col: str = "race_date",
    race_id_col: str = "race_id",
    tansho_odds_col: str = "tansho_odds_val",
    fukusho_odds_col: str = "fukusho_odds_val",
    hit_col: str = "is_hit",
    hit_fuku_col: str = "is_hit_fukusho",
    year_col: str = "race_year",
    umaban_col: str = "umaban",
    fractional_c_base: float = 0.25,
    dd_threshold: float = 0.30,
    n_posterior_samples: int = 500,
    n_kelly_samples: int = 500,
    benter_alpha: float = 50.0,
    benter_beta: float = 1.0,
    fit_benter: bool = False,
    verbose: bool = False,
) -> List[MonthlyP3Result]:
    """
    Phase 3 Walk-Forward時系列検証を実行する。

    速度最適化モード（デフォルト）:
        - Benter パラメータは固定値（alpha=0.3, beta=1.0）を使用
          fit_benter=True にすると月次学習するが 140s/月 × 84 = 196分かかる
        - 複勝 edge_table は fukusho_odds_val が有効な場合のみ構築
        - n_posterior_samples=500, n_kelly_samples=500 で十分な精度を確保

    Args:
        df: 全期間のデータ（検証期間 + 学習窓を含む）
        factor_defs: 使用するファクター定義リスト
        val_start_ym: 検証開始年月（"YYYY-MM"）
        val_end_ym: 検証終了年月（"YYYY-MM"）
        train_months: 学習窓の月数（デフォルト 36）
        date_col: 日付カラム名（YYYYMMDD 文字列）
        race_id_col: レース識別子カラム名
        tansho_odds_col: 単勝オッズカラム名
        fukusho_odds_col: 複勝オッズカラム名
        hit_col: 単勝的中フラグカラム名
        hit_fuku_col: 複勝的中フラグカラム名
        year_col: 年度カラム名
        umaban_col: 馬番カラム名
        fractional_c_base: 基本フラクショナル係数（サーキットブレーカー適用前）
        dd_threshold: サーキットブレーカー発動閾値
        n_posterior_samples: 事後分布サンプル数（デフォルト500）
        n_kelly_samples: Kelly モンテカルロサンプル数（デフォルト500）
        benter_alpha: Benter モデルの alpha 固定値（fit_benter=False 時に使用）。
                     log_ev_score の典型値 0.02-0.10、combine_scores(alpha_wp=0.35)後は
                     0.007-0.035 程度。alpha=50 で logit 調整 0.35-1.75 となり、
                     高オッズ馬の p_final がブレークイーブンを超えるのに十分なスケール。
        benter_beta: Benter モデルの beta 固定値（fit_benter=False 時に使用）
        fit_benter: True の場合のみ月次 Benter 学習を実行（速度に注意）
        verbose: True で進捗を表示

    Returns:
        List[MonthlyP3Result]: 月次検証結果のリスト
    """
    periods = _generate_rolling_periods(val_start_ym, val_end_ym, train_months)
    results: List[MonthlyP3Result] = []

    # 累積バンクロール管理
    bankroll = 1.0
    cumulative_bankroll_series: List[float] = []

    for period in periods:
        val_ym = period["val_ym"]
        if verbose:
            print(f"  [{val_ym}] 学習: {period['train_start']} ～ {period['train_end']}", flush=True)

        # ── 学習窓データ抽出 ──
        train_mask = (
            (df[date_col] >= period["train_start"])
            & (df[date_col] <= period["train_end"])
        )
        train_df = df[train_mask].copy()

        if len(train_df) < 100:
            if verbose:
                print(f"  [{val_ym}] SKIP: 学習データ不足 ({len(train_df)} 行)")
            continue

        # ── 検証月データ抽出 ──
        val_mask = (
            (df[date_col] >= period["val_start"])
            & (df[date_col] <= period["val_end"])
        )
        val_df = df[val_mask].copy()

        if len(val_df) == 0:
            continue

        # ── サーキットブレーカー ──
        current_dd = max_drawdown(np.array(cumulative_bankroll_series)) \
            if cumulative_bankroll_series else 0.0
        fractional_c = circuit_breaker(
            current_dd, threshold=dd_threshold,
            normal_c=fractional_c_base, reduced_c=fractional_c_base * 0.6,
        )

        # ── edge_table 構築（単勝） ──
        edge_table_win = build_edge_table_from_df(
            train_df, factor_defs,
            bet_type="tansho", odds_col=tansho_odds_col,
            hit_col=hit_col, year_col=year_col, is_fukusho=False,
            n_posterior_samples=n_posterior_samples,
        )

        # 複勝 edge_table: fukusho_odds_val に有効値がある場合のみ構築
        # （generate_phase3 では fukusho_odds_val = NaN のためスキップ）
        has_valid_fukusho = (
            hit_fuku_col in train_df.columns
            and fukusho_odds_col in train_df.columns
            and pd.to_numeric(train_df[fukusho_odds_col], errors="coerce").notna().any()
        )
        if has_valid_fukusho:
            edge_table_place = build_edge_table_from_df(
                train_df, factor_defs,
                bet_type="fukusho", odds_col=fukusho_odds_col,
                hit_col=hit_fuku_col, year_col=year_col, is_fukusho=True,
                n_posterior_samples=n_posterior_samples,
            )
        else:
            edge_table_place = {}

        # ── Benter パラメータ ──
        # fit_benter=True の場合は月次学習（140s/月），False なら固定値を使用
        if fit_benter:
            _alpha, _beta = fit_benter_from_df(
                train_df, edge_table_win, edge_table_place,
                factor_defs, race_id_col=race_id_col,
                odds_col=tansho_odds_col, hit_col=hit_col,
            )
        else:
            _alpha, _beta = benter_alpha, benter_beta

        # ── 検証月: ビン値を事前計算 ──
        # 学習窓のビン境界を val_df に適用してラベルを生成する。
        # edge_table も train_df のみで apply_binning しているため、
        # 同じ境界を使うことでキーの文字列が一致する（ターゲットリーク防止も兼ねる）。
        bin_lookup: Dict[str, pd.Series] = {}
        for factor_def in factor_defs:
            if factor_def.column not in val_df.columns:
                continue
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                try:
                    bs = _apply_train_bins_to_val(train_df, val_df, factor_def)
                    bin_lookup[factor_def.name] = bs
                except Exception:
                    continue

        # ── 検証月: レースごとに処理 ──
        all_bets: List[HorseBet] = []
        all_p_model: List[float] = []
        all_p_market: List[float] = []
        all_outcomes: List[float] = []

        for race_id, race_df in val_df.groupby(race_id_col, sort=False):
            if len(race_df) < 2:
                continue

            bets = score_race(
                race_df=race_df,
                edge_table_win=edge_table_win,
                edge_table_place=edge_table_place,
                factor_defs=factor_defs,
                bin_lookup=bin_lookup,
                benter_alpha=_alpha,
                benter_beta=_beta,
                fractional_c=fractional_c,
                odds_col=tansho_odds_col,
                hit_col=hit_col,
                umaban_col=umaban_col,
                n_kelly_samples=n_kelly_samples,
            )

            # Brier Score 用: 全馬の P_final と outcomes を記録
            odds_raw = pd.to_numeric(race_df[tansho_odds_col], errors="coerce")
            valid_mask = odds_raw > 1.0
            race_valid = race_df[valid_mask]
            odds_arr = odds_raw[valid_mask].values.astype(float)

            if len(race_valid) >= 2:
                pm = implied_probability(odds_arr)
                hit_arr = pd.to_numeric(
                    race_valid[hit_col], errors="coerce"
                ).fillna(0).values.astype(float)

                s_list = []
                for idx, row in race_valid.iterrows():
                    hf_w = _get_horse_factors(row, factor_defs, bin_lookup, idx)
                    hf_p = _get_horse_factors(row, factor_defs, bin_lookup, idx)
                    s_w = compute_horse_score(hf_w, edge_table_win, "tansho")
                    s_p2 = compute_horse_score(hf_p, edge_table_place, "fukusho")
                    s_list.append(combine_scores(s_w, s_p2))

                s_arr_v = np.array(s_list, dtype=float)
                try:
                    p_final_v = benter_integrate(
                        s_arr_v, pm, alpha=_alpha, beta=_beta
                    )
                except Exception:
                    p_final_v = pm.copy()

                all_p_model.extend(p_final_v.tolist())
                all_p_market.extend(pm.tolist())
                all_outcomes.extend(hit_arr.tolist())

            all_bets.extend(bets)

        # ── 月次損益計算 ──
        n_bets = len(all_bets)
        n_hits = sum(b.is_hit for b in all_bets)

        # バンクロール変動: SUM(f_star * (odds - 1) * is_hit - f_star * (1 - is_hit))
        monthly_pnl = 0.0
        for bet in all_bets:
            if bet.is_hit:
                monthly_pnl += bet.f_star * (bet.odds - 1.0)
            else:
                monthly_pnl -= bet.f_star

        bankroll = bankroll * (1.0 + monthly_pnl)
        cumulative_bankroll_series.append(bankroll)

        # ── Brier Score 算出 ──
        if all_outcomes:
            bs_model = brier_score(
                np.array(all_p_model), np.array(all_outcomes)
            )
            bs_market = brier_score(
                np.array(all_p_market), np.array(all_outcomes)
            )
        else:
            bs_model = bs_market = 0.25  # データなし時のデフォルト

        results.append(MonthlyP3Result(
            year_month=val_ym,
            n_bets=n_bets,
            n_hits=n_hits,
            monthly_return=round(monthly_pnl, 6),
            cumulative_bankroll=round(bankroll, 6),
            brier_model=round(bs_model, 6),
            brier_market=round(bs_market, 6),
            fractional_c_used=round(fractional_c, 4),
        ))

        if verbose:
            sign = "+" if monthly_pnl >= 0 else ""
            print(f"  [{val_ym}] ベット={n_bets}, 的中={n_hits}, "
                  f"月次={sign}{monthly_pnl:.3f}, "
                  f"累積BK={bankroll:.4f}")

    return results
