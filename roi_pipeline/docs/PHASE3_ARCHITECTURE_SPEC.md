# Phase 3: 統合アーキテクチャ実装仕様書
# The Unified Pipeline — 投資競馬における最高峰の得点化・期待値算出・資金配分

> **バージョン**: 1.0  
> **作成日**: 2026-04-04  
> **前提**: Phase 1 完了（20ファクター検証）、Phase 2 完了（13ファクター×セグメント交互作用、114テスト全パス、グローバル補正回収率 79.92%）

---

## 目次

1. [設計思想](#設計思想)
2. [Task 1: Log-EV得点化エンジン＋LCBゲート](#task-1-log-ev得点化エンジンlcbゲート)
3. [Task 2: Benter二段階モデル（市場確率統合）](#task-2-benter二段階モデル市場確率統合)
4. [Task 3: ベイズKelly資金配分エンジン](#task-3-ベイズkelly資金配分エンジン)
5. [Task 4: Walk-Forward時系列検証](#task-4-walk-forward時系列検証)
6. [実行コマンド](#実行コマンド)
7. [データベース前提](#データベース前提)
8. [アーキテクチャ根拠（学術的背景）](#アーキテクチャ根拠学術的背景)

---

## 設計思想

全ての処理は1本のパイプラインとして貫通する。
最終出力は「馬iに対してバンクロールのX%を賭けろ（またはX=0で賭けるな）」という単一の数値。

```
入力: レースr の出走馬リスト + 各馬のファクター値 + 確定オッズ
  ↓ Task 1: Log-EV得点化 + LCBゲート
  ↓ Task 2: Benter二段階統合 → 真の勝率 P_final(i)
  ↓ Task 3: Beta事後分布 → ベイズKelly → 最適投資比率 f*(i)
出力: f*(i) > 0 の馬のみ投資対象、f*(i) = 投資比率
```

---

## Task 1: Log-EV得点化エンジン＋LCBゲート

### 目的
Phase 2で確認された各ファクター×セグメントの補正回収率を、対数期待値空間に変換し、統計的に信頼できるエッジのみを抽出する。

### 新規ファイル
- `roi_pipeline/engine/log_ev_scorer.py`
- `roi_pipeline/tests/test_log_ev_scorer.py`

### 核心ロジック

#### 1. Log-EV変換
```python
import numpy as np

def log_ev_score(corrected_return_rate: float, baseline: float = 0.80) -> float:
    """
    補正回収率を対数期待値スコアに変換する。
    baseline=0.80 がゼロライン。
    
    例:
      80% → ln(80/80) = 0.0   (±0、エッジなし)
      85% → ln(85/80) = +0.061 (弱いエッジ)
      100% → ln(100/80) = +0.223 (強いエッジ)
      120% → ln(120/80) = +0.405 (非常に強いエッジ)
      60% → ln(60/80) = -0.288 (負のエッジ)
    """
    if corrected_return_rate <= 0:
        return -10.0  # 下限クリップ
    return np.log(corrected_return_rate / baseline)
```

#### 2. LCBゲート（下方信頼限界）
```python
def lcb_gate(posterior_samples: np.ndarray, baseline: float = 0.80, 
             quantile: float = 0.10) -> bool:
    """
    階層ベイズの事後分布サンプルから、下方10%分位点が
    baseline(80%)を超えるかどうかを判定する。
    
    超えなければ False（投資不適格）。
    超えれば True（投資適格）。
    """
    lcb = np.quantile(posterior_samples, quantile)
    return lcb > baseline
```

#### 3. ファクタースコアの合成
```python
def compute_horse_score(horse_factors: dict, 
                        edge_table: dict,
                        bet_type: str = 'tansho') -> float:
    """
    馬iの全該当ファクター×セグメントのLogEVスコアを合計する。
    
    horse_factors: {factor_name: (segment, bin_value)} の辞書
    edge_table: {(factor, segment, bin): {
        'posterior_mean': float,
        'posterior_samples': np.ndarray,
        'N': int
    }} 
    bet_type: 'tansho' or 'fukusho'
    
    初期実装では均等重み（1/K）。Phase 3 Task 4完了後、
    LightGBMによる非線形重み最適化を検討。
    """
    scores = []
    for factor_name, (segment, bin_value) in horse_factors.items():
        key = (factor_name, segment, bin_value, bet_type)
        if key not in edge_table:
            continue
        cell = edge_table[key]
        # LCBゲート
        if not lcb_gate(cell['posterior_samples']):
            continue
        score = log_ev_score(cell['posterior_mean'])
        scores.append(score)
    
    if len(scores) == 0:
        return 0.0
    return sum(scores) / len(scores)  # 均等重み
```

### テスト要件（test_log_ev_scorer.py）
1. log_ev_score(0.80) == 0.0
2. log_ev_score(1.00) ≈ 0.223
3. log_ev_score(0.60) ≈ -0.288
4. log_ev_score(0.0) == -10.0（下限クリップ）
5. lcb_gate: 事後分布の90%が80%超 → True
6. lcb_gate: 事後分布の90%が80%以下 → False
7. compute_horse_score: ファクターなし → 0.0
8. compute_horse_score: 単一ファクター、エッジあり → 正のスコア
9. compute_horse_score: 単一ファクター、LCB不合格 → 0.0
10. compute_horse_score: 複数ファクター合成 → 均等重み平均
11. 単勝と複勝で独立にスコアが算出されること
12. 全体回収率79.92%に対するLog-EVスコアが負であること（79.92/80 < 1）

### 品質ゲート
- 全テスト通過（既存114テスト + 新規12テスト = 126テスト以上）
- LCBフィルター後のエッジセル数: Phase 2の総エッジセル数の30-60%に絞り込まれること

---

## Task 2: Benter二段階モデル（市場確率統合）

### 目的
Task 1で算出した馬固有スコアを「ファンダメンタル確率」に変換し、確定オッズから算出した市場暗示確率と多項ロジットで統合して「真の勝率 P_final(i)」を算出する。

### 新規ファイル
- `roi_pipeline/engine/benter_model.py`
- `roi_pipeline/tests/test_benter_model.py`

### 核心ロジック

#### 1. 市場暗示確率の算出
```python
def implied_probability(odds: np.ndarray) -> np.ndarray:
    """
    確定オッズから市場暗示確率を算出する。
    オーバーラウンド（控除率）を正規化して確率合計を1にする。
    
    odds: レース内全馬のオッズ配列 [O_1, O_2, ..., O_n]
    """
    raw_prob = 1.0 / odds
    return raw_prob / raw_prob.sum()
```

#### 2. 単勝・複勝スコアの統合
```python
def combine_scores(s_win: float, s_place: float, 
                   alpha_wp: float = 0.35) -> float:
    """
    単勝スコアと複勝スコアを加重統合する。
    複勝重み = 1 - alpha_wp = 0.65（Phase 2データに基づく）
    
    根拠: Phase 2で複勝エッジが単勝の3-5倍検出されたため、
    複勝に重みを置く。
    """
    return alpha_wp * s_win + (1.0 - alpha_wp) * s_place
```

#### 3. Benter二段階統合
```python
from scipy.optimize import minimize

def benter_integrate(s_combined: np.ndarray, 
                     p_market: np.ndarray,
                     outcomes: np.ndarray = None,
                     alpha: float = None,
                     beta: float = None) -> np.ndarray:
    """
    Benter (1994) の二段階モデル。
    
    第1段階: s_combined（ファンダメンタルスコア）は既に算出済み
    第2段階: ファンダメンタルと市場確率を多項ロジットで統合
    
    ln(P_final(i)) = alpha * s_combined(i) + beta * ln(p_market(i))
    P_final(i) = softmax(alpha * s_combined + beta * ln(p_market))
    
    alpha, beta が None の場合は outcomes から最尤推定する。
    alpha, beta が指定されている場合はそのまま使用する。
    
    典型的にはbeta > alpha（市場は賢い、モデルは差分を捉える）
    """
    if alpha is not None and beta is not None:
        logits = alpha * s_combined + beta * np.log(p_market + 1e-10)
        logits -= logits.max()  # オーバーフロー防止
        p_final = np.exp(logits) / np.exp(logits).sum()
        return p_final
    
    # 最尤推定（Walk-Forward学習窓で使用）
    def neg_log_likelihood(params):
        a, b = params
        logits = a * s_combined + b * np.log(p_market + 1e-10)
        logits -= logits.max()
        probs = np.exp(logits) / np.exp(logits).sum()
        # outcomes: one-hot (勝馬=1, 他=0)
        ll = np.sum(outcomes * np.log(probs + 1e-10))
        return -ll
    
    result = minimize(neg_log_likelihood, x0=[0.5, 1.0], 
                      method='Nelder-Mead')
    alpha_hat, beta_hat = result.x
    return benter_integrate(s_combined, p_market, 
                           alpha=alpha_hat, beta=beta_hat)
```

### テスト要件（test_benter_model.py）
1. implied_probability: オッズ均等 → 確率均等
2. implied_probability: 確率合計 = 1.0
3. implied_probability: 低オッズ馬の確率が高い
4. combine_scores: alpha_wp=0.35 の重み検証
5. combine_scores: 単勝0、複勝のみ → 0.65 × 複勝スコア
6. benter_integrate: alpha=0, beta=1 → P_final ≈ P_market
7. benter_integrate: alpha=1, beta=0 → P_final ∝ exp(s_combined)
8. benter_integrate: P_final の合計 = 1.0
9. benter_integrate: 最尤推定で alpha < beta となること（合成データ）
10. benter_integrate: 全馬のスコアが0 → P_final = P_market

### 品質ゲート
- 全テスト通過（既存126 + 新規10 = 136テスト以上）
- バックテストデータで P_final の Brier Score < P_market の Brier Score（市場より正確）

---

## Task 3: ベイズKelly資金配分エンジン

### 目的
Task 2で得た P_final(i) を点推定ではなくBeta事後分布として扱い、モンテカルロ積分でKelly最適投資比率を算出する。フラクショナル係数で破産リスクを制御する。

### 新規ファイル
- `roi_pipeline/engine/bayesian_kelly.py`
- `roi_pipeline/tests/test_bayesian_kelly.py`

### 核心ロジック

#### 1. Beta事後分布の構築
```python
from scipy.stats import beta as beta_dist

def build_posterior(p_final: float, n_eff: float, 
                    prior_strength: float = 50.0) -> tuple:
    """
    P_final を中心とする Beta 事後分布のパラメータ (a, b) を返す。
    
    n_eff: 実効サンプル数（該当ファクター×セグメントのNの調和平均）
    prior_strength: 事前分布の強さ（初期値50、Walk-Forwardで最適化）
    
    κ = n_eff + prior_strength
    a = p_final * κ
    b = (1 - p_final) * κ
    
    n_eff が大きい → κ が大きい → Beta分布が尖る（確信度高）
    n_eff が小さい → κ が小さい → Beta分布が広い（不確実性大）
    """
    kappa = n_eff + prior_strength
    a = max(p_final * kappa, 1.0)  # a >= 1 を保証
    b = max((1.0 - p_final) * kappa, 1.0)  # b >= 1 を保証
    return (a, b)
```

#### 2. モンテカルロKelly
```python
def bayesian_kelly(p_final: float, odds: float, n_eff: float,
                   n_samples: int = 5000,
                   fractional_c: float = 0.25,
                   f_grid: np.ndarray = None) -> float:
    """
    ベイズ事後分布に基づくフラクショナルKelly。
    
    1. P_final を中心とする Beta 分布から p を n_samples 回サンプリング
    2. 各 p について、投資比率 f での対数成長率を計算
    3. 全サンプルの平均対数成長率を最大化する f* を求める
    4. f* にフラクショナル係数 c を掛ける
    
    fractional_c = 0.25（クォーターKelly）
    → 年間破産確率 ≈ 1/81（フルKellyの1/3に対して劇的改善）
    
    戻り値: 最適投資比率（バンクロールに対する%）。0以下なら賭けない。
    """
    if odds <= 1.0:
        return 0.0
    
    a, b = build_posterior(p_final, n_eff)
    p_samples = beta_dist.rvs(a, b, size=n_samples)
    
    if f_grid is None:
        f_grid = np.linspace(0.001, 0.30, 300)  # 0.1% ～ 30%
    
    best_f = 0.0
    best_growth = -np.inf
    
    net_odds = odds - 1.0  # ネットオッズ
    
    for f in f_grid:
        # 勝った場合: 1 + f * net_odds
        # 負けた場合: 1 - f
        log_growth = np.mean(
            p_samples * np.log(1.0 + f * net_odds) +
            (1.0 - p_samples) * np.log(1.0 - f)
        )
        if log_growth > best_growth:
            best_growth = log_growth
            best_f = f
    
    if best_growth <= 0:
        return 0.0
    
    return best_f * fractional_c
```

#### 3. 実効サンプル数の算出
```python
def compute_n_eff(factor_sample_sizes: list) -> float:
    """
    馬iが該当する全ファクター×セグメントのサンプル数の調和平均。
    最も弱いリンク（最小N）が全体の不確実性を支配する設計。
    
    factor_sample_sizes: [N_1, N_2, ..., N_k]（各ファクターのセルのN）
    """
    if len(factor_sample_sizes) == 0:
        return 0.0
    reciprocals = [1.0 / max(n, 1) for n in factor_sample_sizes]
    return len(reciprocals) / sum(reciprocals)
```

### テスト要件（test_bayesian_kelly.py）
1. build_posterior: p_final=0.5, n_eff=100 → a≈75, b≈75
2. build_posterior: n_eff=0 → prior_strength のみ → 広い分布
3. build_posterior: a >= 1, b >= 1 が常に保証される
4. bayesian_kelly: odds=1.0 → 0.0（賭けない）
5. bayesian_kelly: p_final=0.5, odds=3.0 → 正の投資比率
6. bayesian_kelly: p_final=0.01, odds=2.0 → 0.0（EV負）
7. bayesian_kelly: n_eff大 vs n_eff小 → n_eff大の方が投資比率が大きい
8. bayesian_kelly: fractional_c=0.25 の効果検証（フルKellyの約1/4）
9. bayesian_kelly: 投資比率が0.30 * 0.25 = 0.075 を超えないこと
10. compute_n_eff: [100, 100, 100] → 100
11. compute_n_eff: [100, 1] → ≈ 1.98（最小Nに引っ張られる）
12. compute_n_eff: [] → 0.0

### 品質ゲート
- 全テスト通過（既存136 + 新規12 = 148テスト以上）
- f* > 0 の馬の割合: 全出走馬の5〜15%
- バックテスト上、f* > 0 の馬の平均補正回収率 > 85%

---

## Task 4: Walk-Forward時系列検証

### 目的
Task 1〜3の全パイプラインを、2019年1月〜2025年12月の期間で月次ローリングWalk-Forwardにより検証する。

### 新規ファイル
- `roi_pipeline/engine/walk_forward.py`
- `roi_pipeline/reports/generate_phase3.py`
- `roi_pipeline/tests/test_walk_forward.py`

### Walk-Forward設計

#### ローリング窓
```
学習窓: 24か月（例: 2019-01 ～ 2020-12）
検証窓: 1か月（例: 2021-01）
ステップ: 1か月ずつスライド

期間: 2019-01 開始 → 2025-12 終了
検証期間数: 約48か月分（2021-01 ～ 2024-12）
```

#### 各月の処理フロー
```
1. 学習窓のデータで階層ベイズ推定 → edge_table 生成
2. 学習窓のデータで Benter の alpha, beta を最尤推定
3. 検証窓の各レースについて:
   a. 各馬の Log-EV スコア算出（Task 1）
   b. Benter 統合確率 P_final 算出（Task 2）
   c. ベイズ Kelly 投資比率 f* 算出（Task 3）
   d. f* > 0 の馬について、実際のオッズと着順で損益を計算
4. 月次の以下指標を記録:
   - 月次リターン（%）
   - Brier Score
   - ベット数
   - 的中数
   - 最大ドローダウン（累積）
```

### 評価指標

#### 1. Brier Score（確率キャリブレーション）
```python
def brier_score(p_predicted: np.ndarray, outcomes: np.ndarray) -> float:
    """BS = mean((p - outcome)^2)。0に近いほど良い。"""
    return np.mean((p_predicted - outcomes) ** 2)
```

目標: P_final の Brier Score < P_market の Brier Score

#### 2. Betting Sharpe Ratio
```python
def betting_sharpe(monthly_returns: np.ndarray) -> float:
    """
    Sharpe = mean(月次リターン) / std(月次リターン) * sqrt(12)
    年率換算。目標: > 1.0
    """
    if np.std(monthly_returns) == 0:
        return 0.0
    return np.mean(monthly_returns) / np.std(monthly_returns) * np.sqrt(12)
```

#### 3. 最大ドローダウン
```python
def max_drawdown(cumulative_returns: np.ndarray) -> float:
    """最大ドローダウン。目標: < 30%"""
    peak = np.maximum.accumulate(cumulative_returns)
    dd = (peak - cumulative_returns) / peak
    return np.max(dd)
```

#### 4. サーキットブレーカー
```python
def circuit_breaker(current_dd: float, threshold: float = 0.30,
                    normal_c: float = 0.25, reduced_c: float = 0.15) -> float:
    """
    最大DDが閾値を超えた場合、フラクショナル係数を自動縮小。
    """
    if current_dd >= threshold:
        return reduced_c
    return normal_c
```

### レポート出力
`roi_pipeline/reports/phase3/walk_forward_report.md` に以下を出力:

```markdown
# Phase 3 Walk-Forward検証レポート

## サマリー
- 検証期間: 2021-01 ～ 2024-12（48か月）
- 年間Betting Sharpe Ratio: [値]
- 最大ドローダウン: [値]%
- 累積リターン: [値]%
- 平均月次ベット数: [値]
- Brier Score（モデル）: [値]
- Brier Score（市場）: [値]

## 月次リターン推移
| 年月 | ベット数 | 的中数 | 月次リターン | 累積リターン | DD |
|------|---------|--------|------------|------------|-----|

## 年次サマリー
| 年 | 年間リターン | Sharpe | 最大DD | ベット数 |
|----|------------|--------|--------|---------|

## 合格判定
- [ ] Sharpe > 1.0
- [ ] 最大DD < 30%
- [ ] 年間リターン > 0% が検証期間の70%以上
- [ ] Brier Score（モデル） < Brier Score（市場）
```

### テスト要件（test_walk_forward.py）
1. brier_score: 完全予測 → 0.0
2. brier_score: ランダム予測 → ≈ 0.25
3. betting_sharpe: 全月正リターン → 高Sharpe
4. betting_sharpe: リターンゼロ → Sharpe = 0
5. max_drawdown: 単調増加 → DD = 0
6. max_drawdown: 半減 → DD = 0.5
7. circuit_breaker: DD < 30% → c = 0.25
8. circuit_breaker: DD >= 30% → c = 0.15
9. Walk-Forward 1期間の end-to-end テスト（モックデータ）
10. レポート生成テスト

### 品質ゲート
- 全テスト通過（既存148 + 新規10 = 158テスト以上）
- Walk-Forward レポートが正常に生成されること
- 合格判定の4項目中3項目以上をクリア

---

## 実行コマンド

```bash
# Task 1 テスト
cd E:\jraroi1219 && py -3.12 -m pytest roi_pipeline/tests/test_log_ev_scorer.py -v

# Task 2 テスト
py -3.12 -m pytest roi_pipeline/tests/test_benter_model.py -v

# Task 3 テスト
py -3.12 -m pytest roi_pipeline/tests/test_bayesian_kelly.py -v

# Task 4 テスト + レポート生成
py -3.12 -m pytest roi_pipeline/tests/test_walk_forward.py -v
py -3.12 -m roi_pipeline.reports.generate_phase3

# 全テスト一括
py -3.12 -m pytest roi_pipeline/tests/ -v
```

---

## データベース前提

- テーブル: jvd_se（成績）、jvd_ra（レース）、jrd_kyi（JRDB競走馬情報）
- 確定着順カラム: `kakutei_chakujun`（※ `kakutei_jyuni` ではない。Phase 2で修正済み）
- オッズカラム: `tansho_odds`（単勝）、`fukusho_odds`（複勝）
- 既存エンジン: `roi_pipeline/engine/corrected_return.py`、`roi_pipeline/engine/odds_correction.py`（108段階補正テーブル）
- 既存エンジン: `roi_pipeline/engine/interaction_analysis.py`（階層ベイズ推定）

---

## アーキテクチャ根拠（学術的背景）

本設計は以下の学術論文・実証研究に基づく:

1. **Log-EV得点化**: Kelly (1956) の対数成長理論。補正回収率を対数空間に変換することで、Kelly基準と数学的に整合する得点化を実現。
2. **LCBゲート**: ベイズ下方信頼限界。事後分布の下方10%分位点が基準80%を超えるセグメントのみを投資適格とし、少標本の偽エッジを自動排除。
3. **Benter二段階モデル**: Benter (1994)。ドイツ競馬への適用で914ベット+54.9ユニット（p=0.0218）を達成。市場オッズを事前情報として活用し、ファンダメンタルモデルとの乖離（オーバーレイ）を抽出。
4. **ベイズKelly**: Baker & McHale (2013)、Swartz et al. (SFU)。パラメータ不確実性をBeta事後分布として内蔵し、フルKellyの破産リスクを回避。クォーターKellyで年間破産確率≈1/81。
5. **Brier Score**: Log-lossの代替。穴馬的中時のペナルティ発散を防ぎ、低評価帯の非効率性を正しく評価。
6. **Betting Sharpe Ratio**: リスク調整後リターン。期待値と的中率（ボラティリティ）を単一指標で評価。
