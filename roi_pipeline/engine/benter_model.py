"""
Benter二段階モデル（市場確率統合）

Phase 3 Task 2: Task 1で算出した馬固有スコアを「ファンダメンタル確率」に変換し、
確定オッズから算出した市場暗示確率と多項ロジットで統合して
「真の勝率 P_final(i)」を算出する。

学術的根拠:
    Benter (1994): ドイツ競馬への適用で 914ベット +54.9ユニット (p=0.0218) を達成。
    市場オッズを事前情報として活用し、ファンダメンタルモデルとの乖離（オーバーレイ）を抽出。
    典型的には beta > alpha（市場は賢い、モデルは差分を捉える）。
"""
from typing import Optional, Tuple

import numpy as np
from scipy.optimize import minimize


def implied_probability(odds: np.ndarray) -> np.ndarray:
    """
    確定オッズから市場暗示確率を算出する。

    オーバーラウンド（控除率）を正規化して確率合計を1にする。

    Args:
        odds: レース内全馬のオッズ配列 [O_1, O_2, ..., O_n]
              各要素は 1.0 以上の実数（倍率表示）

    Returns:
        正規化済み市場暗示確率配列。合計 = 1.0。

    Raises:
        ValueError: odds に 0 以下の値が含まれる場合
    """
    odds = np.asarray(odds, dtype=float)
    if np.any(odds <= 0):
        raise ValueError("オッズは全て正の値である必要があります。")
    raw_prob = 1.0 / odds
    return raw_prob / raw_prob.sum()


def combine_scores(
    s_win: float,
    s_place: float,
    alpha_wp: float = 0.35,
) -> float:
    """
    単勝スコアと複勝スコアを加重統合する。

    複勝重み = 1 - alpha_wp = 0.65（Phase 2データに基づく）

    根拠: Phase 2で複勝エッジが単勝の3-5倍検出されたため、複勝に重みを置く。

    Args:
        s_win: 単勝Log-EVスコア（compute_horse_score の出力）
        s_place: 複勝Log-EVスコア
        alpha_wp: 単勝の重み（デフォルト 0.35）

    Returns:
        統合スコア = alpha_wp * s_win + (1 - alpha_wp) * s_place
    """
    return alpha_wp * s_win + (1.0 - alpha_wp) * s_place


def benter_integrate(
    s_combined: np.ndarray,
    p_market: np.ndarray,
    outcomes: Optional[np.ndarray] = None,
    alpha: Optional[float] = None,
    beta: Optional[float] = None,
) -> np.ndarray:
    """
    Benter (1994) の二段階モデル。

    第1段階: s_combined（ファンダメンタルスコア）は既に算出済み
    第2段階: ファンダメンタルと市場確率を多項ロジットで統合

    ln(P_final(i)) = alpha * s_combined(i) + beta * ln(p_market(i))
    P_final(i) = softmax(alpha * s_combined + beta * ln(p_market))

    典型的には beta > alpha（市場は賢い、モデルは差分を捉える）。

    Args:
        s_combined: 各馬の統合ファンダメンタルスコア配列（N匹分）
        p_market: 市場暗示確率配列（合計 = 1.0）
        outcomes: 実績の one-hot ベクトル（勝馬=1, 他=0）。
                  None かつ alpha/beta も None の場合はエラー。
        alpha: ファンダメンタルスコアの係数。None なら最尤推定。
        beta: 市場対数確率の係数。None なら最尤推定。

    Returns:
        真の勝率推定値 P_final（合計 = 1.0 の確率配列）

    Raises:
        ValueError: alpha/beta が None かつ outcomes も None の場合
    """
    s_combined = np.asarray(s_combined, dtype=float)
    p_market = np.asarray(p_market, dtype=float)

    def _softmax_integrate(a: float, b: float) -> np.ndarray:
        logits = a * s_combined + b * np.log(p_market + 1e-10)
        logits -= logits.max()  # オーバーフロー防止
        exp_logits = np.exp(logits)
        return exp_logits / exp_logits.sum()

    # alpha, beta が指定されている場合はそのまま使用
    if alpha is not None and beta is not None:
        return _softmax_integrate(alpha, beta)

    # 最尤推定（Walk-Forward学習窓で使用）
    if outcomes is None:
        raise ValueError(
            "alpha/beta が未指定の場合、最尤推定のために outcomes が必要です。"
        )

    outcomes = np.asarray(outcomes, dtype=float)

    def neg_log_likelihood(params: np.ndarray) -> float:
        a, b = params
        probs = _softmax_integrate(a, b)
        ll = np.sum(outcomes * np.log(probs + 1e-10))
        return -ll

    result = minimize(
        neg_log_likelihood,
        x0=np.array([0.5, 1.0]),
        method="Nelder-Mead",
        options={"xatol": 1e-6, "fatol": 1e-8, "maxiter": 10000},
    )
    alpha_hat, beta_hat = result.x
    return _softmax_integrate(alpha_hat, beta_hat)


def fit_benter_params(
    races_s_combined: list,
    races_p_market: list,
    races_outcomes: list,
) -> Tuple[float, float]:
    """
    複数レースのデータを使って Benter の alpha, beta を最尤推定する。

    Walk-Forward の学習窓で呼び出し、推定した alpha/beta を
    検証窓で benter_integrate(alpha=alpha_hat, beta=beta_hat) として使う。

    Args:
        races_s_combined: レースごとの s_combined リスト（各要素: np.ndarray）
        races_p_market: レースごとの p_market リスト（各要素: np.ndarray）
        races_outcomes: レースごとの outcomes リスト（各要素: np.ndarray, one-hot）

    Returns:
        (alpha_hat, beta_hat) のタプル
    """
    def total_neg_log_likelihood(params: np.ndarray) -> float:
        a, b = params
        total_ll = 0.0
        for s, pm, oc in zip(races_s_combined, races_p_market, races_outcomes):
            s = np.asarray(s, dtype=float)
            pm = np.asarray(pm, dtype=float)
            oc = np.asarray(oc, dtype=float)
            logits = a * s + b * np.log(pm + 1e-10)
            logits -= logits.max()
            exp_l = np.exp(logits)
            probs = exp_l / exp_l.sum()
            total_ll += np.sum(oc * np.log(probs + 1e-10))
        return -total_ll

    result = minimize(
        total_neg_log_likelihood,
        x0=np.array([0.5, 1.0]),
        method="Nelder-Mead",
        options={"xatol": 1e-6, "fatol": 1e-8, "maxiter": 10000},
    )
    return float(result.x[0]), float(result.x[1])
