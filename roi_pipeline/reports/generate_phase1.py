"""
Phase 1 代表ファクター20個の検証レポート生成スクリプト

使用方法:
    cd /path/to/jraroi1219
    python -m roi_pipeline.reports.generate_phase1

出力:
    roi_pipeline/reports/phase1/factor_XX_ファクター名.md  (×20)
    roi_pipeline/reports/phase1/summary.md

前提条件:
    - PostgreSQL (127.0.0.1:5432, database: pckeiba) に接続可能であること
    - pip install -r roi_pipeline/requirements.txt 済みであること
"""
import os
import sys
from datetime import datetime
from typing import List, Optional

import pandas as pd
import numpy as np

from roi_pipeline.config.odds_correction import get_odds_correction
from roi_pipeline.config.year_weights import get_year_weight
from roi_pipeline.engine.data_loader import load_base_race_data, convert_numeric_columns, diagnose_join_keys
from roi_pipeline.engine.data_loader_v2 import load_base_race_data_v2, diagnose_v2_join
from roi_pipeline.engine.corrected_return import (
    calc_corrected_return_rate,
    calc_return_rate_by_bins,
    BASELINE_RATE,
)
from roi_pipeline.engine.hierarchical_bayes import (
    hierarchical_bayes_estimate,
    BayesEstimate,
)
from roi_pipeline.engine.walk_forward import (
    run_walk_forward,
    WalkForwardConfig,
    MonthlyResult,
)
from roi_pipeline.factors.definitions import FACTOR_DEFINITIONS, FactorDefinition, FactorType
from roi_pipeline.factors.binning import apply_binning


# レポート出力先
REPORT_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "phase1"
)


def ensure_report_dir() -> None:
    """レポート出力ディレクトリが存在することを保証する。"""
    os.makedirs(REPORT_DIR, exist_ok=True)


def add_hit_flag(df: pd.DataFrame) -> pd.DataFrame:
    """
    単勝的中フラグを追加する。

    Args:
        df: ベースデータ（kakutei_chakujun カラム必須）

    Returns:
        is_hit カラムが追加されたDataFrame
    """
    df = df.copy()
    if "kakutei_chakujun" in df.columns:
        df["is_hit"] = (pd.to_numeric(df["kakutei_chakujun"], errors="coerce") == 1).astype(int)
    else:
        df["is_hit"] = 0
    return df


def add_race_year(df: pd.DataFrame) -> pd.DataFrame:
    """
    race_yearカラムを追加する。

    Args:
        df: ベースデータ（race_date or kaisai_nen カラム必須）

    Returns:
        race_year カラムが追加されたDataFrame
    """
    df = df.copy()
    if "race_date" in df.columns:
        df["race_year"] = df["race_date"].str[:4]
    elif "kaisai_nen" in df.columns:
        df["race_year"] = df["kaisai_nen"]
    return df


def generate_factor_report(
    df: pd.DataFrame,
    factor: FactorDefinition,
    global_rate: float,
) -> str:
    """
    1ファクターの検証レポートをMarkdown形式で生成する。

    Args:
        df: ベースデータ（全期間）
        factor: ファクター定義
        global_rate: グローバル補正回収率（全条件の平均）

    Returns:
        レポート文字列（Markdown）
    """
    lines: List[str] = []

    # ====== 1. ファクター基本情報 ======
    lines.append(f"# ファクター{factor.id}: {factor.name}")
    lines.append("")
    lines.append(f"- **テーブル**: {factor.table}")
    lines.append(f"- **カラム**: {factor.column}")
    lines.append(f"- **カテゴリ**: {factor.category}")
    lines.append(f"- **データ型**: {factor.factor_type.value}")
    lines.append(f"- **セグメント**: GLOBAL（Phase 1）")
    if factor.factor_type == FactorType.NUMERIC:
        lines.append(f"- **ビン数**: {factor.n_bins}")
    lines.append(f"- **説明**: {factor.description}")
    lines.append("")

    # カラムの存在確認
    if factor.column not in df.columns:
        lines.append(f"**WARNING**: カラム `{factor.column}` がデータに存在しません。")
        lines.append("レポート生成をスキップします。")
        return "\n".join(lines)

    # 総サンプル数
    total_n = len(df)
    valid_n = df[factor.column].notna().sum()
    lines.append(f"- **総サンプル数**: {total_n:,}")
    lines.append(f"- **有効サンプル数**: {valid_n:,} ({valid_n/total_n*100:.1f}%)")
    lines.append("")

    # 年度別内訳
    lines.append("### 年度別サンプル数")
    lines.append("")
    lines.append("| 年度 | サンプル数 | 有効数 |")
    lines.append("|------|----------|--------|")
    if "race_year" in df.columns:
        for year in sorted(df["race_year"].unique()):
            year_df = df[df["race_year"] == year]
            year_valid = year_df[factor.column].notna().sum()
            lines.append(f"| {year} | {len(year_df):,} | {year_valid:,} |")
    lines.append("")

    # ====== 2. ビン別補正回収率テーブル ======
    lines.append("## ビン別（カテゴリ別）補正回収率")
    lines.append("")

    edge_bins: List[str] = []

    try:
        binned_series, bin_col_name = apply_binning(df, factor)
        df_with_bins = df.copy()
        df_with_bins[bin_col_name] = binned_series

        # ビン別補正回収率を算出
        bin_results = calc_return_rate_by_bins(
            df_with_bins.dropna(subset=[bin_col_name]),
            bin_col=bin_col_name,
        )

        if len(bin_results) == 0:
            lines.append("データ不足のため算出不可。")
        else:
            # 階層ベイズ推定を適用
            lines.append("| ビン/カテゴリ | N | 的中率(%) | 補正回収率(%) | ベイズ推定後(%) | 95%CI下限 | 95%CI上限 | 得点 | エッジ |")
            lines.append("|-------------|---|---------|------------|--------------|---------|---------|------|--------|")

            for _, row in bin_results.iterrows():
                bayes = hierarchical_bayes_estimate(
                    observed_rate=row["corrected_return_rate"],
                    n_samples=int(row["n_samples"]),
                    prior_rate=global_rate,
                )

                edge_flag = "**YES**" if bayes.ci_lower > BASELINE_RATE else "No"
                if bayes.ci_lower > BASELINE_RATE:
                    edge_bins.append(str(row["bin_value"]))

                lines.append(
                    f"| {row['bin_value']} "
                    f"| {int(row['n_samples']):,} "
                    f"| {row['hit_rate']:.2f} "
                    f"| {row['corrected_return_rate']:.2f} "
                    f"| {bayes.estimated_rate:.2f} "
                    f"| {bayes.ci_lower:.2f} "
                    f"| {bayes.ci_upper:.2f} "
                    f"| {bayes.score:.2f} "
                    f"| {edge_flag} |"
                )

            lines.append("")

    except Exception as e:
        lines.append(f"ビン分割エラー: {e}")
        lines.append("")

    # ====== 3. Walk-Forward検証結果 ======
    lines.append("## Walk-Forward検証結果")
    lines.append("")

    try:
        wf_results = run_walk_forward(df)

        if wf_results:
            lines.append("| 年月 | レース数 | 頭数 | 的中数 | 月次回収率(%) | 累積回収率(%) | 得点 |")
            lines.append("|------|---------|------|--------|-------------|-------------|------|")
            for r in wf_results:
                lines.append(
                    f"| {r.year_month} "
                    f"| {r.n_races} "
                    f"| {r.n_horses} "
                    f"| {r.n_hits} "
                    f"| {r.monthly_return_rate:.2f} "
                    f"| {r.cumulative_return_rate:.2f} "
                    f"| {r.score:.2f} |"
                )

            lines.append("")
            final = wf_results[-1]
            lines.append(f"**最終累積補正回収率**: {final.cumulative_return_rate:.2f}%")
            lines.append(f"**最終得点**: {final.score:.2f}")
        else:
            lines.append("Walk-Forward期間にデータが存在しません。")
    except Exception as e:
        lines.append(f"Walk-Forward検証エラー: {e}")

    lines.append("")

    # ====== 4. エッジ判定 ======
    lines.append("## エッジ判定")
    lines.append("")

    if edge_bins:
        lines.append(f"**Yes** - 補正回収率が80%を統計的に有意に超えるビン/カテゴリが**{len(edge_bins)}個**存在する。")
        lines.append("")
        lines.append("**該当ビン/カテゴリ:**")
        for eb in edge_bins:
            lines.append(f"- {eb}")
    else:
        lines.append("**No** - 補正回収率が80%を統計的に有意に超えるビン/カテゴリは存在しない。")

    lines.append("")
    lines.append(f"---\n*生成日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")

    return "\n".join(lines)


def generate_summary_report(
    factor_results: List[dict],
) -> str:
    """
    全20ファクターのサマリーレポートを生成する。

    Args:
        factor_results: 各ファクターの結果辞書リスト

    Returns:
        サマリーレポート文字列（Markdown）
    """
    lines: List[str] = []
    lines.append("# Phase 1 検証サマリー")
    lines.append("")
    lines.append(f"生成日時: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("")
    lines.append("## 代表ファクター20個の検証結果一覧")
    lines.append("")
    lines.append("| # | ファクター名 | カテゴリ | エッジビン数 | 結論 |")
    lines.append("|---|------------|---------|-----------|------|")

    total_edge_factors = 0
    for fr in factor_results:
        edge_count = fr.get("edge_count", 0)
        conclusion = "**エッジあり**" if edge_count > 0 else "エッジなし"
        if edge_count > 0:
            total_edge_factors += 1
        lines.append(
            f"| {fr['id']} | {fr['name']} | {fr['category']} "
            f"| {edge_count} | {conclusion} |"
        )

    lines.append("")
    lines.append("## 総合判定")
    lines.append("")
    lines.append(f"- 検証ファクター数: 20")
    lines.append(f"- エッジが検出されたファクター数: {total_edge_factors}")
    lines.append(f"- エッジ検出率: {total_edge_factors/20*100:.1f}%")
    lines.append("")

    if total_edge_factors > 0:
        lines.append(
            f"**結論: {total_edge_factors}個のファクターにおいて、"
            f"補正回収率80%を統計的に有意に超えるビン/カテゴリが存在する。**"
        )
    else:
        lines.append(
            "**結論: 補正回収率80%を統計的に有意に超えるビン/カテゴリは"
            "いずれのファクターにおいても検出されなかった。**"
        )

    lines.append("")
    lines.append("---")
    lines.append("*このレポートはroi_pipeline Phase 1の自動検証で生成されました*")

    return "\n".join(lines)


def main() -> None:
    """
    メイン実行関数。

    PostgreSQLから全期間データを取得し、
    20ファクターそれぞれの検証レポートを生成する。
    """
    ensure_report_dir()

    print("=" * 60)
    print("Phase 1: 代表ファクター20個の検証レポート生成")
    print("=" * 60)
    print()

    # 1. データ取得（v2優先、フォールバックでv1）
    print("[1/4] PostgreSQLからベースデータを取得中...")
    use_v2 = False
    try:
        df = load_base_race_data_v2(date_from="20161101", date_to="20251231")
        use_v2 = True
        print(f"  ✅ v2 (jrd_*_fixed) テーブル使用: {len(df):,} 行")
    except RuntimeError:
        print("  jrd_*_fixed テーブルなし → v1（旧テーブル）にフォールバック")
        try:
            df = load_base_race_data(date_from="20161101", date_to="20251231")
            print(f"  取得行数: {len(df):,}")
        except Exception as e:
            print(f"  ERROR: データ取得失敗 - {e}")
            print("  PostgreSQL (127.0.0.1:5432, database: pckeiba) への接続を確認してください。")
            sys.exit(1)
    except Exception as e:
        print(f"  v2 ERROR: {e}")
        print("  v1（旧テーブル）にフォールバック...")
        try:
            df = load_base_race_data(date_from="20161101", date_to="20251231")
            print(f"  取得行数: {len(df):,}")
        except Exception as e2:
            print(f"  ERROR: データ取得失敗 - {e2}")
            sys.exit(1)

    # 2. 数値変換 + 的中フラグ + 年度 + レースID
    print("[2/4] データ前処理中...")
    df = convert_numeric_columns(df)
    df = add_hit_flag(df)
    df = add_race_year(df)

    # race_id生成（keibajo_code + race_date + race_bango でユニークレースを特定）
    if "race_id" not in df.columns:
        id_cols = ["keibajo_code", "race_date", "race_bango"]
        if all(c in df.columns for c in id_cols):
            df["race_id"] = (
                df["keibajo_code"].astype(str).str.strip()
                + "_" + df["race_date"].astype(str).str.strip()
                + "_" + df["race_bango"].astype(str).str.strip()
            )
            print(f"  race_id 生成完了: {df['race_id'].nunique():,} ユニークレース")

    print(f"  前処理完了: {len(df):,} rows, {len(df.columns)} columns")

    # 2.5. データ診断（包括的チェック）
    print()
    print("[2.5] データ診断...")
    print()

    # --- A. 全カラム一覧とNULL率 ---
    print("  --- A. カラム別NULL率（JRDB系ファクター） ---")
    jrdb_factor_cols = [
        "idm", "sogo_shisu", "agari_shisu", "pace_shisu",
        "kyori_tekisei_code", "course_tekisei", "baba_tekisei",
        "chokyo_yajirushi_code", "soho",
        "kishu_shisu", "chokyo_shisu", "kyusha_shisu",
        "chokyo_hyoka", "ls_shisu", "juryo_shubetsu_code",
        "babajotai_code_shiba", "babajotai_code_dirt",
    ]
    jra_factor_cols = [
        "umaban", "barei", "blinker_shiyo_kubun",
        "tansho_odds", "fukusho_odds", "kakutei_chakujun",
    ]

    for col in jrdb_factor_cols + jra_factor_cols:
        if col in df.columns:
            valid = df[col].notna().sum()
            # 文字列カラムの場合、空文字もチェック
            if df[col].dtype == object:
                non_empty = (df[col].notna() & (df[col].str.strip() != "")).sum()
                print(f"    {col:30s}: {valid:>8,} notna / {non_empty:>8,} 非空文字 / {len(df):,}")
            else:
                print(f"    {col:30s}: {valid:>8,} notna / {len(df):,} ({valid/len(df)*100:.1f}%)")
        else:
            print(f"    {col:30s}: *** カラム不在 ***")
    print()

    # --- B. JOIN品質チェック ---
    print("  --- B. JOIN品質（JRDBテーブル結合率） ---")
    # jrd_kyiからのカラムが1つでもnon-nullなら結合成功
    kyi_cols = ["idm", "sogo_shisu", "agari_shisu", "pace_shisu", "kishu_shisu"]
    kyi_joined = 0
    for col in kyi_cols:
        if col in df.columns:
            kyi_joined = max(kyi_joined, df[col].notna().sum())
    print(f"    jrd_kyi 結合率: {kyi_joined:,} / {len(df):,} ({kyi_joined/len(df)*100:.1f}%)")

    cyb_cols = ["chokyo_hyoka"]
    cyb_joined = 0
    for col in cyb_cols:
        if col in df.columns:
            cyb_joined = max(cyb_joined, df[col].notna().sum())
    print(f"    jrd_cyb 結合率: {cyb_joined:,} / {len(df):,} ({cyb_joined/len(df)*100:.1f}%)")

    joa_cols = ["ls_shisu"]
    joa_joined = 0
    for col in joa_cols:
        if col in df.columns:
            joa_joined = max(joa_joined, df[col].notna().sum())
    print(f"    jrd_joa 結合率: {joa_joined:,} / {len(df):,} ({joa_joined/len(df)*100:.1f}%)")

    bac_cols = ["juryo_shubetsu_code"]
    bac_joined = 0
    for col in bac_cols:
        if col in df.columns:
            bac_joined = max(bac_joined, df[col].notna().sum())
    print(f"    jrd_bac 結合率: {bac_joined:,} / {len(df):,} ({bac_joined/len(df)*100:.1f}%)")

    # JOIN結合率が0%の場合、JOINキー診断を実行
    if kyi_joined == 0:
        print()
        print("  ⚠️  JRDB全テーブルJOIN結合率0% → JOINキー診断を実行...")
        try:
            diag_result = diagnose_join_keys()
            print(diag_result)
        except Exception as e:
            print(f"    診断エラー: {e}")
    print()

    # --- C. オッズ診断 ---
    print("  --- C. オッズ値の妥当性 ---")
    if "tansho_odds" in df.columns:
        odds_series = df["tansho_odds"].dropna()
        print(f"    tansho_odds: count={len(odds_series):,}, "
              f"min={odds_series.min():.1f}, max={odds_series.max():.1f}, "
              f"mean={odds_series.mean():.1f}, median={odds_series.median():.1f}")
        print(f"    tansho_odds サンプル (先頭10件): {odds_series.head(10).tolist()}")

        # JRA-VANオッズフォーマット変換
        # JRA-VAN (jvd_se) の tansho_odds は常に10倍単位格納
        # 例: 249 = 24.9倍, 42 = 4.2倍, 4215 = 421.5倍
        # diagnose_odds.py で検証済み:
        #   /10変換 → 回収率79.74%（市場理論値≒78-80%に一致）
        #   /100変換 → 回収率7.89%（的中率≒回収率 = オッズ未反映）
        median_val = odds_series.median()
        if median_val >= 10.0:
            print(f"    JRA-VAN 10倍単位格納 (median={median_val:.1f})")
            print(f"    → 実オッズ = tansho_odds / 10 に変換。")
            df["tansho_odds"] = df["tansho_odds"] / 10.0
        else:
            print(f"    ✅ tansho_odds は実オッズ値 (median={median_val:.1f})")
        
        # 変換後の妥当性検証
        converted_median = df["tansho_odds"].dropna().median()
        if converted_median < 1.0 or converted_median > 100.0:
            print(f"    ⚠️  WARNING: 変換後median={converted_median:.2f} - 想定範囲外（1.0-100.0）")

        odds_after = df["tansho_odds"].dropna()
        print(f"    変換後: min={odds_after.min():.2f}, max={odds_after.max():.1f}, "
              f"mean={odds_after.mean():.2f}, median={odds_after.median():.2f}")

    if "fukusho_odds" in df.columns:
        fuku_series = df["fukusho_odds"].dropna()
        if len(fuku_series) > 0:
            print(f"    fukusho_odds: count={len(fuku_series):,}, "
                  f"min={fuku_series.min():.1f}, max={fuku_series.max():.1f}, "
                  f"mean={fuku_series.mean():.1f}, median={fuku_series.median():.1f}")
            fuku_median = fuku_series.median()
            if fuku_median >= 10.0:
                print(f"    fukusho_odds: JRA-VAN 10倍単位格納 (median={fuku_median:.1f})")
                print(f"    → 実オッズ = fukusho_odds / 10 に変換。")
                df["fukusho_odds"] = df["fukusho_odds"] / 10.0
            else:
                print(f"    ✅ fukusho_odds は実オッズ値 (median={fuku_median:.1f})")
    print()

    # --- D. 的中フラグ診断 ---
    print("  --- D. 的中フラグ ---")
    if "kakutei_chakujun" in df.columns:
        hit_count = int(df["is_hit"].sum())
        print(f"    is_hit (1着): {hit_count:,} / {len(df):,} "
              f"({hit_count/len(df)*100:.2f}%)")
        # 的中率が5-10%前後でないと異常
        hit_pct = hit_count / len(df) * 100
        if hit_pct < 3.0 or hit_pct > 15.0:
            print(f"    ⚠️  的中率 {hit_pct:.2f}% は異常値（想定: 5〜10%）")
        else:
            print(f"    ✅ 的中率は妥当。")
    print()

    # 3. グローバル補正回収率を算出
    print("[3/4] グローバル補正回収率を算出中...")
    global_result = calc_corrected_return_rate(df)
    global_rate = global_result["corrected_return_rate"]
    print(f"  グローバル補正回収率: {global_rate:.2f}%")
    print(f"  グローバルスコア: {global_result['score']:.2f}")

    # 妥当性チェック（JRA単勝の市場全体回収率は理論上 ~78-80%）
    if global_rate > 120.0 or global_rate < 60.0:
        print(f"  ⚠️  WARNING: グローバル回収率 {global_rate:.2f}% は異常値です。")
        print(f"       想定範囲: 60〜120%（JRA市場理論値≒78-80%）")
        print(f"       オッズ値のフォーマットを確認してください。")
        print(f"       JRA-VAN tansho_odds は10倍単位格納（/10で実オッズ）。")
    print()

    # 4. 各ファクターのレポート生成
    print("[4/4] ファクター別レポートを生成中...")
    factor_results: List[dict] = []

    for factor in FACTOR_DEFINITIONS:
        print(f"  [{factor.id:02d}/20] {factor.name} ({factor.column})...", end=" ")

        report_text = generate_factor_report(df, factor, global_rate)

        # エッジ数をカウント（**YES**の出現回数）
        edge_count = report_text.count("**YES**")

        # レポートファイルを保存
        filename = f"factor_{factor.id:02d}_{factor.column}.md"
        filepath = os.path.join(REPORT_DIR, filename)
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(report_text)

        factor_results.append({
            "id": factor.id,
            "name": factor.name,
            "category": factor.category,
            "edge_count": edge_count,
        })

        print(f"Done (edges: {edge_count})")

    # 5. サマリーレポート生成
    print()
    print("サマリーレポートを生成中...")
    summary = generate_summary_report(factor_results)
    summary_path = os.path.join(REPORT_DIR, "summary.md")
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write(summary)

    print(f"  保存先: {summary_path}")
    print()
    print("=" * 60)
    print("完了!")
    print(f"  レポートディレクトリ: {REPORT_DIR}")
    print(f"  ファクターレポート: {len(factor_results)}件")

    total_edges = sum(fr["edge_count"] for fr in factor_results)
    edge_factors = sum(1 for fr in factor_results if fr["edge_count"] > 0)
    print(f"  エッジ検出ファクター: {edge_factors}/20")
    print(f"  総エッジビン数: {total_edges}")
    print("=" * 60)


if __name__ == "__main__":
    main()
