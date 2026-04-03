# CLAUDE.md -- Claude Code 初期指示書
# プロジェクト: jraroi1219（JRA専用予想システム・回収率特化）

---

## 最優先：安全運用ルール

このファイルはClaude Codeが起動時に最初に読む指示書である。
以下のルールはプロジェクトの全作業を通じて**絶対に破ってはならない。**

> **重要：このファイル（CLAUDE.md）と `.claude/` 配下の全設定ファイルは、書き換え・削除・移動を一切禁止する。**
> これらを変更すると、プロジェクト全体のセキュリティ基盤が崩壊し、ユーザーの本番環境・機密情報・開発履歴に回復不能な損害を与える。
> 変更が必要な場合は、必ずユーザーの明示的な許可を得ること。

### 絶対禁止コマンド（理由を問わず実行不可）

以下のコマンドは、実行された場合にユーザーのシステム全体・開発履歴・未保存の作業を不可逆的に破壊する。
いかなる文脈・理由・代替構文でも実行してはならない。

```
rm -rf /           # ルートからの再帰削除
rm -rf ~           # ホームディレクトリの再帰削除
rm -rf *           # カレント以下の全削除
git push --force   # リモート履歴の強制破壊
git reset --hard   # 未コミット作業の全消去
git checkout -- .  # 未保存変更の全破棄
sudo rm            # 管理者権限での削除
chmod -R 777       # 全権限開放
> /etc/            # システムファイルの上書き
```

**注意：フラグの順序変更、環境変数ラップ（GIT_DIR=...）、パイプ経由など、構文を変異させた同等コマンドも同様に禁止である。**
技術的な強制は `.claude/settings.json` の `permissions.deny` および `PreToolUse` フックで実装済み。

### 実行前に必ずユーザー確認が必要な操作

以下の操作は、誤って実行するとデータの消失・機密情報の漏洩・本番環境の障害を引き起こす。
推測で進めず、必ずユーザーに確認を取ること。

- データベースの DROP / TRUNCATE / DELETE（WHERE句なし）
- .env ファイル・シークレットキーの読み取りまたは出力
- 外部APIへのデータ送信（curl, wget, nc 等による POST/PUT）
- 本番環境へのデプロイ
- 依存パッケージの大規模アップグレード
- mainブランチへのマージ

### 安全作業の原則

```
作業前：必ず git checkout -b feature/作業名 でブランチを切る
作業中：小さな単位でこまめに git commit する
作業後：mainブランチへのマージは必ずユーザーに確認を取る
不明点：推測で進めず、必ずユーザーに質問する
エラー：握りつぶさず必ずユーザーに報告する
```

### ファイルアクセス制限

以下のファイルは読み取り専用。書き込み・削除・外部送信は一切禁止。
これらのファイルにはユーザーの本番環境の認証情報が含まれており、
アクセスした内容がAnthropicのクラウドに送信されるリスクがある。

```
.env*              # 環境変数（APIキー、DB接続文字列）
*.key / *.pem      # SSL/SSH秘密鍵
*.secret           # シークレットファイル
credentials.*      # 認証情報
settings.local.json # ローカル設定
~/.ssh/*           # SSH鍵ディレクトリ
~/.aws/*           # AWSクレデンシャル
```

**技術的補完：`.claude/settings.json` の `permissions.deny` で `Read(**/.env*)` 等を明示的にブロック済み。**

---

## 多層防御アーキテクチャ（必ず理解すること）

このプロジェクトは、CLAUDE.md（ソフトルール）だけでなく、以下の4層でセキュリティを実装している。

| レイヤー | ファイル | 役割 |
|---------|--------|------|
| L1: 行動指針 | `CLAUDE.md`（本ファイル） | AIへの指示・倫理的フレーミング |
| L2: 権限制御 | `.claude/settings.json` | permissions.deny による決定論的ブロック |
| L3: フック | `.claude/hooks/*.sh` | PreToolUse / Stop / ConfigChange による物理的遮断 |
| L4: パス固有 | `.claude/rules/*.md` | 機密領域アクセス時の動的セキュリティ指示ロード |

**CLAUDE.md は「お願い」、settings.json + hooks は「強制」。両方が揃って初めて安全が成立する。**

---

## チェックポイント機能の限界（重要）

Claude Codeの `/rewind` チェックポイント機能は、**Write / Edit ツール経由の変更のみ追跡する。**
**Bash ツール経由のファイル操作（rm, mv, sed, シェルスクリプト等）は追跡対象外であり、/rewind では復元できない。**

このため、本プロジェクトでは:
1. **Stopフック（`.claude/hooks/auto-commit.sh`）** が応答完了ごとに自動git commitを実行する
2. 万が一の破壊時は `git reflog` → `git reset --hard <hash>` で完全復旧する
3. チェックポイントはあくまで補助。**Gitが唯一の確実なセーフティネットである**

---

## プロジェクト概要

### 最終目的

- ギャンブルではなく「投資」としての競馬システムを構築する
- 的中率ではなく**回収率（期待値）**を最大化する
- 感情を完全に排除し、年間収支プラスを絶対条件として全自動で運用する

### 使用データ

- JRA-VAN（過去10年分の公式構造化データ）
- JRDB（IDM・脚質・蹄コードなど）
- **当日データは一切使用しない**（直前オッズ・馬体重・パドック情報など）
- スクレイピングによるデータ収集は行わない

---

## システム設計

### コアロジック

1. **補正回収率の計算** -- JRA実効控除率75-77%を基準点(0点)とし、オッズ別補正係数・期間別重み付けで得点化
2. **階層ベイズモデル** -- グローバル→カテゴリ→個別セグメントの3階層。サンプル数による信頼性重み付け
3. **ファクター設計** -- 130-170個の候補。単体+組み合わせ評価。前走着順1-18着すべて使用
4. **得点化と相対評価** -- LightGBMで重み最適化 → レース内正規化 → EV計算で投資対象選定
5. **マトリックスフィルター** -- モデルスコア×オッズ帯のマトリックス。範囲外は購入しない
6. **資金管理** -- フラクショナルケリー（クォーターケリー推奨）。モンテカルロでドローダウン検証

### 技術スタック

```
バックエンド：Python
モデル：LightGBM（後でGNN等に拡張可）
データベース：PostgreSQL
フロントエンド：Hono + Cloudflare Pages
検証手法：Walk-Forward Validation（ターゲットリーク厳禁）
```

### 実装の優先順位

```
Phase 1: データ取り込み基盤 / 補正回収率エンジン / ファクター単体評価
Phase 2: 階層ベイズ補正 / LightGBM得点化 / Walk-Forward検証基盤
Phase 3: 相対評価・期待値計算 / マトリックスフィルター / 結果表示UI
Phase 4: フラクショナルケリー資金管理 / 自動化・全体統合
```

---

## 作業開始前チェックリスト

Claude Codeは各セッション開始時に以下を確認すること：

```
[ ] 作業用ブランチが切られているか
[ ] .envファイルをコードやログに出力していないか
[ ] 今から行う操作に破壊的なコマンドが含まれていないか
[ ] データベース操作にWHERE句が正しく入っているか
[ ] 不明な点はユーザーに確認したか
[ ] .claude/settings.json と hooks が正常に存在するか
```

---

## 困ったときの原則

```
推測で進めない → 必ずユーザーに確認する
エラーを隠さない → 全てのエラーを報告する
大きく変えない → 小さな変更を積み重ねる
元に戻せるようにする → コミットを細かく打つ
設定を変えない → CLAUDE.md / settings.json / hooks は触らない
```

---

*このファイルはリポジトリ jraroi1219 のルートに配置すること*
*Claude Codeはセッション開始時に必ずこのファイルを読み込むこと*
*技術的な強制設定は `.claude/settings.json` および `.claude/hooks/` を参照*

---

## 追加指示：roi_pipeline サブプロジェクト（Phase 1）

### プロジェクト配置
- ディレクトリ: `roi_pipeline/` （リポジトリルート直下に新規作成）
- 既存の `scripts/`, `phase7/`, `models/` 等には一切触れない

### 追加の絶対禁止事項

1. **当日データの使用禁止**
   以下のカラム・情報は特徴量として一切使用してはならない。
   SQLやDataFrameでこれらを参照するコードを書くことも禁止する。
   - 直前オッズ（発走直前のオッズ変動）
   - 当日馬体重（jvd_se.bataiju は「計量時」のため過去レースの値は使用可。
     ただし予測対象レース当日の馬体重は使用禁止）
   - パドック評価・気配評価（予測対象レース当日のもの）
   - 当日の馬場状態速報

2. **ターゲットリークの完全禁止**
   全ての集計系特徴量は、予測対象日の前日以前のデータのみで算出すること。
   pandasのgroupbyやSQLのGROUP BYで全期間の平均を取ることは禁止。
   必ず日付フィルタを入れること。

3. **k分割交差検証の使用禁止**
   時系列データに対するランダム分割は禁止。Walk-Forward法のみ使用可。

### コーディング規約（roi_pipeline内）
- 型ヒント（Type Hints）を全ての関数に付与
- docstringを全ての関数・クラスに記述
- テスト（pytest）を各モジュールに対して作成
- `roi_pipeline/config/` 内のファイルはCEOの承認なく変更禁止

### 品質ゲート（roi_pipeline内）
以下が全てパスしない限りコミットしないこと：
- pytest 全テストパス
- ターゲットリーク検出テストのパス（後述）

---

## セッションログ

### [2026-04-03] Phase 1 レポート生成完了セッション

#### 【実行した作業】
1. STEP 3 (v2 JOIN検証) 再確認 → 全4テーブル 99.94-99.98% 達成確認
2. STEP 4 (Phase 1レポート生成) 初回実行 → `v2 ERROR: dict is not a sequence` で v1フォールバック発生を検出
3. `data_loader_v2.py` の `pd.read_sql_query` params辞書渡しバグ修正（f-string直接埋込に変更）
4. STEP 4 再実行 → v2成功するも全20ファクター「エッジなし」（補正回収率7-8%に収束）
5. `diagnose_odds.py` 作成・実行 → オッズ変換バグ特定：JRA-VANは10倍単位格納（median=249.50）だが `/100` 変換が適用されていた
6. `generate_phase1.py` のオッズ変換ロジック修正：`/100` → `/10` に変更
7. STEP 4 最終実行 → **13/20ファクターでエッジ検出、60エッジビン、グローバル回収率79.92%**

#### 【変更したファイル】
- `roi_pipeline/engine/data_loader_v2.py` — `%(name)s` パラメータバインド廃止、f-string直接埋込に変更（`load_base_race_data_v2` + `diagnose_v2_join` 両方）
- `roi_pipeline/reports/generate_phase1.py` — tansho_oddsオッズ変換ロジック全面修正：中央値判定式 `>=100→/100, >=10→/10` を `>=10→常に/10` に変更（JRA-VANは常に10倍単位）
- `roi_pipeline/tools/diagnose_odds.py` — 新規作成。v2データのオッズ値診断・変換シミュレーション・bet/payout詳細出力

#### 【テスト結果】
- roi_pipeline/tests/: **51 passed** (全テストパス)

#### 【未解決の問題】
- `fukusho_odds` カラムが v2 SELECT に未含有（v2クエリの `se.*` にfukusho_oddsが存在しない可能性 or JRA-VANテーブル構造上の問題）
- pandas UserWarning（psycopg2直接接続への非推奨警告）— 機能影響なし、SQLAlchemy移行は優先度低
- 218件のオッズ=0レコード（出走取消等）の扱い — bet_amount=NaN化で自動除外されるが明示的フィルタ追加が望ましい

#### 【次のセッションでやるべきこと】
1. 生成された20個のPhase 1レポートの詳細分析（特にTOP5エッジファクター: 厩舎指数11, 調教師指数10, IDM5, 上がり指数5, ペース指数5）
2. CEO PCからPhase 1レポート全文をGitHubにコミット＆プッシュ
3. Phase 2設計: 階層ベイズ補正 + LightGBM得点化 + Walk-Forward検証基盤の実装計画策定
4. エッジファクターの組み合わせ効果検証（交互作用分析）
5. 馬番エッジ(8ビン)の内訳精査 — 内枠/外枠バイアスが距離・コース条件と交絡していないか確認

#### 【GitHubコミット】
- `2d062c4` fix: v2 data_loader 'dict is not a sequence' エラー修正
- `4e1c7f6` feat: diagnose_odds.py - オッズ値診断スクリプト
- `626ae9a` fix: tansho_odds変換 /100→/10 に修正（致命的バグ）

### [2026-04-03] Walk-Forward修正 + Phase 2 交互作用分析 実装セッション

#### 【実行した作業】
1. Walk-Forwardバグ修正: `run_walk_forward()` に `mask` パラメータ追加、ビン別独立計算を実装
2. `generate_phase1.py` パターンA/B 分離出力、ORDINAL型ビンマッチ修正
3. Phase 2 交互作用分析エンジン新規作成: `roi_pipeline/engine/interaction_analysis.py`
   - `assign_course_category_fast()`: 27カテゴリコース分類の高速付与
   - `assign_surface()`: 芝/ダート2分類付与
   - `run_interaction_analysis()`: クロス集計 + 3層階層ベイズ推定
4. Phase 2 レポート生成スクリプト新規作成: `roi_pipeline/reports/generate_phase2.py`
   - 馬番(1-18) × コースカテゴリ(27): `umaban_x_course.md`
   - 調教師指数(ビン) × 芝/ダート: `chokyo_shisu_x_surface.md`
   - 厩舎指数(ビン) × 芝/ダート: `kyusha_shisu_x_surface.md`
5. `run_all.py` に STEP 5 (Phase 2) を追加

#### 【変更・新規ファイル】
- `roi_pipeline/engine/interaction_analysis.py` — **新規**: 交互作用分析エンジン（3層ベイズ、コース分類、芝ダ分類）
- `roi_pipeline/reports/generate_phase2.py` — **新規**: Phase 2レポート生成（3レポート）
- `roi_pipeline/reports/phase2/` — **新規ディレクトリ**: Phase 2レポート出力先
- `roi_pipeline/tests/test_interaction_analysis.py` — **新規**: Phase 2テスト（20テスト）
- `roi_pipeline/tools/run_all.py` — STEP 5 追加
- `roi_pipeline/engine/walk_forward.py` — mask パラメータ追加（前セッション）
- `roi_pipeline/reports/generate_phase1.py` — パターンA/B分離、ORDINALビンマッチ修正（前セッション）

#### 【テスト結果】
- roi_pipeline/tests/: **75 passed** (全テストパス)
  - Phase 1テスト: 55件
  - Phase 2テスト: 20件（test_interaction_analysis.py）

#### 【Phase 2 アーキテクチャ】
```
3層階層ベイズ推定:
  レベル1（グローバル）: 全条件の補正回収率（≒79.9%）
  レベル2（ファクター値）: 馬番x / ビンb の全セグメント回収率
  レベル3（個別セル）: 馬番x × コースy / ビンb × 芝ダ
```

#### 【CEO PCでの実行方法】
```
cd E:\jraroi1219
git pull origin main

# Phase 2 全レポート生成
py -3.12 -m roi_pipeline.reports.generate_phase2

# 個別レポート
py -3.12 -m roi_pipeline.reports.generate_phase2 --report umaban
py -3.12 -m roi_pipeline.reports.generate_phase2 --report chokyo
py -3.12 -m roi_pipeline.reports.generate_phase2 --report kyusha

# run_all経由
py -3.12 roi_pipeline/tools/run_all.py --step 5
```

#### 【次のセッションでやるべきこと】
1. CEO PCでPhase 2レポートを実行し、結果を確認
2. 馬番×コースの交互作用パターンを分析（全コース共通型 vs 特定コース偏在型）
3. 調教師指数・厩舎指数の芝/ダート分解結果を確認
4. Phase 2結果をもとに、Phase 3（マルチファクターモデル）の設計方針を決定
5. エッジが「全コース共通」ならマルチファクター統合が有効、「特定コース限定」ならコース条件付きモデルが必要

### [2026-04-03] Phase 2 タスク2: 10ファクター交互作用分析 実装セッション

#### 【実行した作業】
1. Phase 2 タスク2 レポート生成スクリプト新規作成: `roi_pipeline/reports/generate_phase2_task2.py`
   - 10ファクター × セグメント交互作用分析
   - SURFACE_2 (7): IDM, 総合指数, 上がり指数, ペース指数, 騎手指数, LS指数, 馬齢
   - COURSE_27 (2): 距離適性, コース適性
   - GLOBAL (1): 馬場状態コード
2. **単勝・複勝デュアルROI**対応: `_compute_dual_roi()` で tansho/fukusho 両方を算出
3. 各セグメントタイプ別レポート生成関数:
   - `generate_surface2_report()`: ファクター × 芝/ダート（3層ベイズ + Phase 1比較）
   - `generate_course27_report()`: ファクター × 27コースカテゴリ（ベイズ収縮、<100サンプルはグローバル平均に収縮）
   - `generate_global_report()`: セグメントなし（単勝+複勝ビン別ROI + ベイズ推定）
4. テスト29件新規作成: `roi_pipeline/tests/test_phase2_task2.py`
5. `run_all.py` に STEP 6 (Phase 2 タスク2) を追加

#### 【変更・新規ファイル】
- `roi_pipeline/reports/generate_phase2_task2.py` — **新規**: Phase 2 タスク2 レポート生成（10レポート、単勝+複勝）
- `roi_pipeline/tests/test_phase2_task2.py` — **新規**: Phase 2 タスク2テスト（29テスト）
- `roi_pipeline/tools/run_all.py` — STEP 6 追加

#### 【テスト結果】
- roi_pipeline/tests/: **104 passed** (全テストパス)
  - Phase 1テスト: 55件
  - Phase 2 タスク1テスト: 20件（test_interaction_analysis.py）
  - Phase 2 タスク2テスト: 29件（test_phase2_task2.py）

#### 【出力レポート一覧（10ファイル）】
```
roi_pipeline/reports/phase2/
├── idm_surface2.md                    # IDM × 芝/ダート
├── sogo_shisu_surface2.md             # 総合指数 × 芝/ダート
├── agari_shisu_surface2.md            # 上がり指数 × 芝/ダート
├── pace_shisu_surface2.md             # ペース指数 × 芝/ダート
├── kishu_shisu_surface2.md            # 騎手指数 × 芝/ダート
├── ls_shisu_surface2.md               # LS指数 × 芝/ダート
├── barei_surface2.md                  # 馬齢 × 芝/ダート
├── kyori_tekisei_code_course27.md     # 距離適性 × 27コースカテゴリ
├── course_tekisei_course27.md         # コース適性 × 27コースカテゴリ
└── babajotai_code_shiba_global.md     # 馬場状態コード GLOBAL
```

#### 【CEO PCでの実行方法】
```
cd E:\jraroi1219
git pull origin main

# Phase 2 タスク2 全レポート生成
py -3.12 -m roi_pipeline.reports.generate_phase2_task2

# 特定ファクターのみ
py -3.12 -m roi_pipeline.reports.generate_phase2_task2 --factor idm
py -3.12 -m roi_pipeline.reports.generate_phase2_task2 --factor sogo_shisu

# run_all経由
py -3.12 roi_pipeline/tools/run_all.py --step 6
```

#### 【品質ゲート】
- ✅ 全テスト通過 (104 passed)
- ✅ グローバル単勝ROI 75-85% 範囲チェック
- ✅ ベイズ収縮適用確認
- ✅ 正確に10レポート出力
- ✅ Phase 1 ファイル変更なし
- ✅ タスク1とタスク2のファクターID重複なし

#### 【次のセッションでやるべきこと】
1. CEO PCでPhase 2 タスク2レポートを実行し、全10レポートの結果を確認
2. 能力系ファクター（IDM/総合/上がり/ペース）の芝ダート分解パターンを横断比較
3. COURSE_27結果から、距離適性・コース適性がコース形状に依存するか確認
4. Phase 2全タスクの総合分析 → Phase 3（マルチファクターLightGBMモデル）設計方針決定
5. エッジの「芝ダ共通性」「コース依存性」に基づくモデル分岐戦略の確定
