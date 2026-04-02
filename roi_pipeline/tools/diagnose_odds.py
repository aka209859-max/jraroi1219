"""
オッズ値診断スクリプト

v2データローダーで取得したtansho_oddsの実態を調べる。
補正回収率が7-8%に収束している問題の原因特定用。

使用方法:
    cd E:\jraroi1219
    py -3.12 roi_pipeline/tools/diagnose_odds.py
"""
import sys
from pathlib import Path

# プロジェクトルートをsys.pathに追加
_PROJECT_ROOT = str(Path(__file__).resolve().parent.parent.parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import pandas as pd
import numpy as np


def main():
    print("=" * 60)
    print("  オッズ値診断: v2データローダー")
    print("=" * 60)

    # --- 1. v2でデータ取得 ---
    print("\n[1] v2データ取得（2024年）...")
    from roi_pipeline.engine.data_loader_v2 import load_base_race_data_v2
    from roi_pipeline.engine.data_loader import convert_numeric_columns

    df = load_base_race_data_v2("20240101", "20241231")
    print(f"  取得行数: {len(df):,}")

    # --- 2. 変換前のtansho_odds ---
    print("\n[2] convert_numeric_columns 前:")
    col = "tansho_odds"
    print(f"  dtype: {df[col].dtype}")
    print(f"  sample (先頭20件): {df[col].head(20).tolist()}")
    # 型が文字列の場合のサンプル
    if df[col].dtype == object:
        non_null = df[col].dropna()
        non_empty = non_null[non_null.str.strip() != ""]
        print(f"  非NULL: {len(non_null):,}, 非空文字: {len(non_empty):,}")
        print(f"  文字列サンプル: {non_empty.head(10).tolist()}")

    # --- 3. 変換後のtansho_odds ---
    print("\n[3] convert_numeric_columns 後:")
    df = convert_numeric_columns(df)
    odds = df[col].dropna()
    print(f"  dtype: {odds.dtype}")
    print(f"  count: {len(odds):,}")
    print(f"  min={odds.min()}, max={odds.max()}")
    print(f"  mean={odds.mean():.2f}, median={odds.median():.2f}")
    print(f"  zero count: {(odds == 0).sum():,}")
    print(f"  NaN count: {df[col].isna().sum():,}")
    print(f"  sample nonzero (先頭10): {odds[odds > 0].head(10).tolist()}")

    # --- 4. 分布 ---
    print("\n[4] 値の分布:")
    bins = [0, 1, 5, 10, 50, 100, 500, 1000, 10000]
    for i in range(len(bins) - 1):
        cnt = ((odds >= bins[i]) & (odds < bins[i+1])).sum()
        print(f"  [{bins[i]:>6} - {bins[i+1]:>6}): {cnt:>8,}")
    cnt = (odds >= 10000).sum()
    print(f"  [10000+          ): {cnt:>8,}")

    # --- 5. オッズ変換シミュレーション ---
    print("\n[5] オッズ変換シミュレーション:")
    median_val = odds.median()
    print(f"  median = {median_val:.2f}")
    if median_val >= 100.0:
        print(f"  → 100倍単位: /100 → median={median_val/100:.2f}")
    elif median_val >= 10.0:
        print(f"  → 10倍単位: /10 → median={median_val/10:.2f}")
    else:
        print(f"  → 変換不要（実オッズ値）")

    # --- 6. 補正回収率テスト ---
    print("\n[6] 補正回収率テスト:")
    from roi_pipeline.engine.corrected_return import calc_corrected_return_rate

    # is_hit, race_year を準備
    df["is_hit"] = (pd.to_numeric(df["kakutei_chakujun"], errors="coerce") == 1).astype(int)
    if "race_date" in df.columns:
        df["race_year"] = df["race_date"].str[:4]

    hit_count = int(df["is_hit"].sum())
    print(f"  的中数: {hit_count:,} / {len(df):,} ({hit_count/len(df)*100:.2f}%)")

    # 生の状態で計算
    result_raw = calc_corrected_return_rate(df)
    print(f"  [変換なし]  回収率={result_raw['corrected_return_rate']:.2f}%, "
          f"score={result_raw['score']:.2f}")

    # /10 変換後
    df_10 = df.copy()
    df_10["tansho_odds"] = df_10["tansho_odds"] / 10.0
    result_10 = calc_corrected_return_rate(df_10)
    print(f"  [/10変換後] 回収率={result_10['corrected_return_rate']:.2f}%, "
          f"score={result_10['score']:.2f}")

    # /100 変換後
    df_100 = df.copy()
    df_100["tansho_odds"] = df_100["tansho_odds"] / 100.0
    result_100 = calc_corrected_return_rate(df_100)
    print(f"  [/100変換後] 回収率={result_100['corrected_return_rate']:.2f}%, "
          f"score={result_100['score']:.2f}")

    # --- 7. bet_amount / payout の詳細 ---
    print("\n[7] bet/payout 詳細（サンプル10件、的中馬のみ）:")
    from roi_pipeline.config.odds_correction import get_odds_correction
    from roi_pipeline.config.year_weights import get_year_weight

    hits = df[df["is_hit"] == 1].head(10)
    for _, row in hits.iterrows():
        o = float(row["tansho_odds"])
        yr = str(row["race_year"])
        corr = get_odds_correction(o)
        yw = get_year_weight(yr)
        bet = 10000 / o if o > 0 else float("nan")
        payout = 10000 * corr
        print(f"  odds={o:>8.1f}, correction={corr:.2f}, "
              f"year_w={yw}, bet={bet:>10.2f}, payout={payout:>10.2f}, "
              f"return={payout/bet*100:.1f}%" if bet > 0 else
              f"  odds={o:>8.1f}, correction={corr:.2f}, year_w={yw}, bet=NaN")

    print("\n" + "=" * 60)
    print("  診断完了")
    print("=" * 60)


if __name__ == "__main__":
    main()
