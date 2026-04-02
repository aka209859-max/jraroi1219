# ディープサーチ指示文: PC-KEIBAのJRDBテーブルJOIN問題の完全解決

## 背景と目的

JRA競馬の回収率最大化AIシステムを構築中。PostgreSQL上にPC-KEIBAソフトが取り込んだJRA-VANデータ（jvd_se, jvd_ra）とJRDBデータ（jrd_kyi, jrd_cyb, jrd_joa, jrd_bac）が格納されている。これらを結合（JOIN）して20ファクターの分析を行いたいが、**JRDBテーブルのカラムが固定長パースずれにより壊れていて、JOINが正しく成立しない**（42%マッチ→実質的に使えない）。

このディープサーチで、以下3つの問題の**根本原因の特定と、具体的な解決策（SQLコード付き）**を求める。

---

## 問題1: JRDBテーブルのカラムがPC-KEIBAの固定長パースずれで壊れている

### 状況

PC-KEIBAというソフトウェアがJRDBの固定長テキストファイル（KYI, CYB, BAC, JOA等）をパースしてPostgreSQLに格納している。しかし、パースのバイトオフセットがずれており、DBに格納された値が仕様と異なる。

### JRDB公式仕様書から判明したレースキー構造

JRDB全ファイル共通の「レースキー」は以下の構造:

```
場コード(2byte) + 年(2byte) + 回(1byte) + 日(1byte,16進数!) + R(2byte) = 計8byte
```

**重要: 「日」は16進数（F型）の1バイト。** 値は 1,2,3,...,9,a,b,c（10日目=a, 11日目=b, 12日目=c）。これが2006年11月7日の競馬法施行規則改正に伴い変更された。

各ファイルの先頭構造:
- **KYI** (1024byte/record): レースキー(8byte, 相対1) + 馬番(2byte, 相対9) + 血統登録番号(8byte, 相対11) + 馬名(36byte, 相対19) + ... + IDM(5byte, 相対55) + 騎手指数(5byte, 相対60) + ...
- **CYB** (96byte/record): レースキー(8byte, 相対1) + 馬番(2byte, 相対9) + 調教タイプ(2byte, 相対11) + ... + 調教評価(1byte, 相対86) + ...
- **BAC** (176byte/record): レースキー(8byte, 相対1) + 年月日(8byte, 相対9) + 発走時間(4byte, 相対17) + 距離(4byte, 相対21) + ...
- **JOA** (116byte/record): レースキー(8byte, 相対1) + 馬番(2byte, 相対9) + 血統登録番号(8byte, 相対11) + 馬名(36byte, 相対19) + ... + LS指数(5byte, 相対83) + ...

### 診断で判明した異常データ

PC-KEIBAが格納した実データには以下の異常がある:

1. **jrd_kyiのkaisai_kai/kaisai_nichime**: 全行がNG（壊れた値）。一致率0%
2. **jrd_cybのkeibajo_code**: '45'などJRA競馬場コード範囲外の値が出現（JRAの場コードは01〜10）
3. **jrd_cybのumaban**: 10/20/30という異常値（通常は1〜18）
4. **jrd_bacのkaisai_nen**: '0127'（MMDD＝1月27日が入っている、本来は年が入るべき）
5. **jrd_bacのrace_bango**: '24'（年の下2桁が入っている、本来はレース番号）
6. **唯一race_shikonenだけは正しい値を保持** — 先頭2桁がYY（年下2桁）と完全一致

### 仮説

PC-KEIBAの固定長パース設定（DataSettings.xml等）が、JRDBの仕様変更（特に「日」フィールドの16進数1バイト化）に追従できておらず、1〜数バイトずれて全カラムが間違った位置のデータを読んでいる。

### 調査してほしいこと

1. **PC-KEIBAがJRDBファイルをどのようにパースしているか**: DataSettings.xmlのバイトオフセット定義と、JRDB公式仕様のバイト位置の差異
2. **race_shikonenだけが正しい理由**: PC-KEIBAはrace_shikonenをどこから読んでいるのか
3. **パースずれの修正方法**: PC-KEIBAの設定を直すか、SQL側でバイトオフセットを補正するか
4. **PC-KEIBAを使わずにJRDBファイルを直接Pythonでパースしてインポートする方法**: 仕様書通りのバイト位置でパースすればずれは起きない。これが最も確実な解決策か？

---

## 問題2: JRA-VAN（jvd_se）とJRDB（jrd_kyi等）のJOINキーをどう設計するか

### 現状

JRA-VAN側（jvd_se）は以下のカラムでレースを識別:
- keibajo_code（場コード）
- kaisai_nen（年, '2024'形式の4桁）
- kaisai_tsukihi（月日, '0307'形式の4桁）
- kaisai_kai（回, '01'形式）
- kaisai_nichime（日目, '05'形式）
- race_bango（レース番号）
- umaban（馬番）

JRDB側はレースキーが:
- 場コード(2) + 年(2) + 回(1) + 日(1, 16進数) + R(2) = 8byte

さらにPC-KEIBAが独自に分解してkaisai_kai, kaisai_nichime, keibajo_code, race_bango, umaban等のカラムを生成しているが、これらが壊れている。

### race_shikonenの正体

診断の結果、**race_shikonen = YY(2桁) + 回(2桁) + 日目(2桁)** の6桁と判明。ただし、公式仕様のレースキーの「回」は1バイト、「日」は16進数1バイト。PC-KEIBAがこれを2桁ずつに変換している可能性がある。

例: 場コード=05, 年=24, 回=1, 日=4(16進数で4), R=01 → レースキー = "05241401"
→ race_shikonen = "240104"（年24 + 回01 + 日04）???

### 調査してほしいこと

1. **race_shikonenの正確な生成ロジック**: JRDB公式レースキーのどのバイトから組み立てられているか
2. **JRA-VANのkaisai_kai/kaisai_nichimeとJRDBの回/日の対応関係**: JRA-VANの「第1回東京5日目」のkaisai_kai='01', kaisai_nichime='05'と、JRDBの回='1', 日='5'はどう対応するか
3. **race_shikonenだけを使ってJOINする方法**: race_shikonenが唯一正しいなら、JRA-VAN側から同じ値を合成できるか
4. **具体的なJOIN SQL**: jvd_seの(kaisai_nen, kaisai_kai, kaisai_nichime)からrace_shikonenと同じ6桁値を合成するSQL式

### JOINテスト結果

| 方法 | マッチ率 | 問題 |
|------|---------|------|
| 年なしJOIN（keibajo+kai+nichime+race+uma） | 多重結合（530倍） | 10年分が全部マッチして使えない |
| 年ありJOIN（YY + kai + nichime + race + uma） | 42%（2495/5919） | 58%ミスマッチ |
| 新戦略JOIN（race_shikonen分解） | 0% | 分解ロジックが間違っていた |

---

## 問題3: 補正回収率1529%バグ

### 状況

Phase 1レポート生成時に「グローバル補正回収率 = 1529.97%」と出力された。正常値は75〜85%。

### 計算式（正しい仕様）

```
bet_amount_i = 10000 / odds_i
corrected_payout_i = 10000 * get_odds_correction(odds_i) （的中馬のみ）
補正回収率 = Σ(corrected_payout_i * year_weight_i) / Σ(bet_amount_i * year_weight_i) × 100%
```

### 推定原因

1. **JOINが42%しか成功しないため、JRDB系カラムが大量にNULLになり、データ品質が崩壊**
2. **オッズ値のフォーマット問題**: JRA-VANのtansho_oddsが「100円あたりの払戻金額」形式（例: 300 = 3.0倍）で格納されている可能性。自動検出ロジック（median >= 100 → /100）は実装済みだが、JOINが壊れた状態では正しく動作しない可能性
3. **オッズ補正係数テーブルは「実オッズ」（1.0〜999999.9）を前提にしている**ため、変換前の値を渡すと係数が大きくずれる

### 調査してほしいこと

1. **JRA-VANのtansho_oddsの正しいフォーマット**: PC-KEIBAが格納する値は実オッズか、100倍値か、10倍値か
2. **JOIN問題が解決すれば1529%バグも自然消滅するか**: JOINの失敗が計算への影響経路
3. **オッズ自動検出の安全な実装方法**: median以外に、min/max/分布形状で判定する方法

---

## 技術環境

- **DB**: PostgreSQL（127.0.0.1:5432, database: pckeiba）
- **データ取込ソフト**: PC-KEIBA Database（JRA-VAN + JRDB両方を取り込む）
- **JRA-VANテーブル**: jvd_se（約49万行）, jvd_ra
- **JRDBテーブル**: jrd_kyi（約49万行）, jrd_cyb（約49万行）, jrd_joa（約49万行）, jrd_bac（約3.5万行）
- **全カラムがcharacter varying型**（文字列）
- **データ期間**: 2016年11月〜2025年12月
- **言語**: Python 3.12, psycopg2, pandas

## 求める回答

1. **問題1の根本原因**: PC-KEIBAのパースずれが起きている具体的なメカニズム
2. **問題2の解決策**: JRA-VANとJRDBを90%以上のマッチ率でJOINできる具体的なSQL
3. **問題3の解決策**: 1529%の原因特定と修正方法
4. **最も確実な解決策の推奨**: PC-KEIBAの設定修正 vs SQL側補正 vs Pythonで直接パース、どれが最善か
5. **PC-KEIBAを使わずにJRDBファイルを自前パースする場合の実装例**（Python）

## JRDB公式仕様書URL（参照推奨）

- KYI: http://www.jrdb.com/program/Kyi/kyi_doc.txt
- CYB: http://www.jrdb.com/program/Cyb/cyb_doc.txt
- BAC: http://www.jrdb.com/program/Bac/bac_doc.txt
- JOA: http://www.jrdb.com/program/Jo/Jodata_doc2.txt
- KYI内容説明: http://www.jrdb.com/program/Kyi/ky_siyo_doc.txt
- CYB内容説明: http://www.jrdb.com/program/Cyb/cybsiyo_doc.txt
- JOA内容説明: http://www.jrdb.com/program/Jo/Josiyo_doc.txt
- JRDBデータコード表: http://www.jrdb.com/program/jrdb_code.txt
- JRDBデータ一覧: http://www.jrdb.com/program/data.html
- KYIサンプル: http://www.jrdb.com/program/Kyi/KYI150712.txt
- CYBサンプル: http://www.jrdb.com/program/Cyb/CYB081018.txt
- BACサンプル: http://www.jrdb.com/program/Bac/BAC080913.txt

## 追加コンテキスト

### JRDBレースキーの「日」フィールドが16進数である理由

2006年11月7日の競馬法施行規則改正により、開催日数が12日を超えるケースが出た。従来の1桁数字（1-9）では表現できないため、16進数（1-9, a, b, c, ...）に変更された。これにより:
- 1日目〜9日目: '1'〜'9'（通常の数字）
- 10日目: 'a'
- 11日目: 'b'
- 12日目: 'c'

PC-KEIBAがこの16進数フィールドを通常の数字として2バイトでパースしようとした場合、オフセットが1バイトずれる可能性がある。

### PC-KEIBA側の情報

PC-KEIBAのJRDB取り込み設定はおそらく `DataSettings.xml` に定義されている。ユーザーのローカル環境（Windows, E:\jraroi1219）にPC-KEIBAがインストールされている。設定ファイルの場所やフォーマットの情報があれば問題解決が大幅に加速する。

### 我々が試した解決策と結果

1. **race_shikonenをYYMMDDと仮定してJOIN** → 0%マッチ（フォーマット不一致）
2. **CAST(INTEGER)でパディング差を吸収** → 42%マッチ（値自体が壊れているため限界）
3. **race_shikonenの3-4桁をkai、5-6桁をnichimeとしてJOIN** → 0%マッチ（ロジックが違った）
4. **keibajo + race + uma のみでJOIN（年なし）** → 530倍多重結合（使えない）

すべて失敗。根本的に異なるアプローチが必要。
