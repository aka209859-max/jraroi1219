"""
JRDB固定長テキストファイルの公式仕様準拠パーサー

PC-KEIBAのパースずれを完全に回避し、JRDB公式仕様書のバイト位置通りに
正確にフィールドを抽出する。

使用方法:
    from roi_pipeline.ingest.jrdb_parser import parse_kyi_file, parse_cyb_file
    records = parse_kyi_file("E:/JRDB/KYI240307.txt")

重要:
  - エンコーディングはShift_JIS (cp932)
  - 「日」フィールドはF型（16進数）: '1'-'9','a','b','c'
  - レコード末尾のCR+LFは自動的にstrip
  - race_key8 = basho_code(2) + year(2) + kai(1) + nichi(1) + race_num(2)
  - race_shikonen = year(2) + LPAD(kai,2) + hex_to_dec(nichi) の2桁ゼロ埋め
"""
import os
import sys
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from roi_pipeline.ingest.jrdb_spec import (
    FieldDef,
    KYI_FIELDS, KYI_RECORD_LENGTH, KYI_IMPORT_FIELDS,
    CYB_FIELDS, CYB_RECORD_LENGTH, CYB_IMPORT_FIELDS,
    BAC_FIELDS, BAC_RECORD_LENGTH, BAC_IMPORT_FIELDS,
    JOA_FIELDS, JOA_RECORD_LENGTH, JOA_IMPORT_FIELDS,
)


def hex_nichi_to_int(nichi_char: str) -> int:
    """
    16進数の「日」フィールドを整数に変換する。
    '1'-'9' → 1-9, 'a'/'A' → 10, 'b'/'B' → 11, 'c'/'C' → 12
    """
    if nichi_char.isdigit():
        return int(nichi_char)
    return int(nichi_char, 16)


def build_race_key8(basho: str, year: str, kai: str, nichi: str, race_num: str) -> str:
    """
    8byte レースキーを構築する。
    basho(2) + year(2) + kai(1) + nichi(1,hex) + race_num(2)
    
    これがJRDBの公式レースキーフォーマット。
    """
    return f"{basho}{year}{kai}{nichi}{race_num}"


def build_race_shikonen(year: str, kai: str, nichi: str) -> str:
    """
    race_shikonenを構築する。
    PC-KEIBAが格納するrace_shikonen = YY(2) + 回(2桁ゼロ埋め) + 日目(2桁ゼロ埋め)
    
    ここでの「日目」は16進数→10進数変換後の値。
    例: year='24', kai='1', nichi='4' → '240104'
    例: year='24', kai='5', nichi='a'(=10) → '240510'
    """
    nichi_int = hex_nichi_to_int(nichi)
    return f"{year}{int(kai):02d}{nichi_int:02d}"


def build_jvan_race_key8(
    keibajo_code: str,
    kaisai_nen: str,
    kaisai_kai: str,
    kaisai_nichime: str,
    race_bango: str,
) -> str:
    """
    JRA-VAN側のカラムから8byte JRDBレースキーを合成する。
    
    JRA-VAN → JRDB変換:
      keibajo_code(2) → basho_code(2)  ※同じ
      kaisai_nen[2:4] → year(2)         ※'2024' → '24'
      kaisai_kai → kai(1)               ※'01' → '1', '2' → '2'
      kaisai_nichime → nichi(hex 1)     ※'05' → '5', '10' → 'a', '11' → 'b'
      race_bango → race_num(2)          ※'01' → '01', '12' → '12'
    """
    basho = keibajo_code.strip().zfill(2)
    yy = kaisai_nen.strip()[-2:]
    kai = str(int(kaisai_kai.strip()))  # '01' → '1'
    
    nichime_int = int(kaisai_nichime.strip())
    if nichime_int >= 10:
        nichi_hex = chr(ord('a') + nichime_int - 10)  # 10→'a', 11→'b', 12→'c'
    else:
        nichi_hex = str(nichime_int)
    
    race = race_bango.strip().zfill(2)
    
    return f"{basho}{yy}{kai}{nichi_hex}{race}"


def _parse_line(
    raw_line: bytes,
    fields: List[FieldDef],
    import_fields: Optional[List[str]],
    encoding: str = "cp932",
) -> Optional[Dict[str, str]]:
    """
    1行（1レコード）をパースする。
    
    Args:
        raw_line: バイト列（CR/LF含む場合あり）
        fields: フィールド定義リスト
        import_fields: インポートするフィールド名リスト（Noneなら全フィールド）
        encoding: エンコーディング
    
    Returns:
        フィールド名→値の辞書。パース失敗時はNone。
    """
    # CR/LF除去
    line = raw_line.rstrip(b"\r\n")
    
    if len(line) == 0:
        return None
    
    record: Dict[str, str] = {}
    
    for field in fields:
        if import_fields and field.name not in import_fields:
            continue
        
        start = field.start
        end = field.end
        
        if start >= len(line):
            # レコードが短い場合は空文字
            record[field.name] = ""
            continue
        
        # 実際の終了位置を調整
        actual_end = min(end, len(line))
        raw_bytes = line[start:actual_end]
        
        try:
            value = raw_bytes.decode(encoding).strip()
        except UnicodeDecodeError:
            try:
                value = raw_bytes.decode("ascii", errors="replace").strip()
            except Exception:
                value = ""
        
        record[field.name] = value
    
    return record


def _add_derived_fields(record: Dict[str, str], has_umaban: bool = True) -> Dict[str, str]:
    """
    派生フィールドを追加する。
    
    - jrdb_race_key8: 8byte レースキー
    - race_shikonen: PC-KEIBA互換の6桁識別子（YY + 回2桁 + 日2桁）
    - kaisai_kai: 回（10進整数文字列）
    - kaisai_nichime: 日目（10進整数文字列）
    - keibajo_code: 場コード（basho_codeのエイリアス）
    - race_bango: レース番号（race_numのエイリアス）
    """
    basho = record.get("basho_code", "")
    year = record.get("year", "")
    kai = record.get("kai", "")
    nichi = record.get("nichi", "")
    race_num = record.get("race_num", "")
    
    # 8byte レースキー
    record["jrdb_race_key8"] = build_race_key8(basho, year, kai, nichi, race_num)
    
    # race_shikonen（PC-KEIBA互換）
    if year and kai and nichi:
        record["race_shikonen"] = build_race_shikonen(year, kai, nichi)
    else:
        record["race_shikonen"] = ""
    
    # JRA-VAN互換フィールド
    if nichi:
        record["kaisai_nichime"] = str(hex_nichi_to_int(nichi))
    else:
        record["kaisai_nichime"] = ""
    
    record["kaisai_kai"] = kai
    record["keibajo_code"] = basho
    record["race_bango"] = race_num
    record["kaisai_nen_2"] = year  # 年の下2桁
    
    return record


def parse_file(
    filepath: str,
    fields: List[FieldDef],
    import_fields: Optional[List[str]],
    record_length: int,
    has_umaban: bool = True,
    encoding: str = "cp932",
) -> List[Dict[str, str]]:
    """
    JRDBファイルをパースする汎用関数。
    
    Args:
        filepath: ファイルパス
        fields: フィールド定義リスト
        import_fields: インポートするフィールド名リスト
        record_length: レコード長（改行含まず）
        has_umaban: 馬番フィールドがあるか（BACはFalse）
        encoding: エンコーディング
    
    Returns:
        レコード辞書のリスト
    """
    records: List[Dict[str, str]] = []
    errors = 0
    
    with open(filepath, "rb") as f:
        for line_num, raw_line in enumerate(f, 1):
            try:
                record = _parse_line(raw_line, fields, import_fields, encoding)
                if record is None:
                    continue
                
                record = _add_derived_fields(record, has_umaban)
                records.append(record)
                
            except Exception as e:
                errors += 1
                if errors <= 5:
                    print(f"  WARNING: {filepath} line {line_num}: {e}", file=sys.stderr)
    
    if errors > 5:
        print(f"  WARNING: {filepath} {errors} total parse errors", file=sys.stderr)
    
    return records


def parse_kyi_file(filepath: str, encoding: str = "cp932") -> List[Dict[str, str]]:
    """KYI（競走馬データ）ファイルをパースする。"""
    return parse_file(filepath, KYI_FIELDS, KYI_IMPORT_FIELDS, KYI_RECORD_LENGTH,
                      has_umaban=True, encoding=encoding)


def parse_cyb_file(filepath: str, encoding: str = "cp932") -> List[Dict[str, str]]:
    """CYB（調教分析データ）ファイルをパースする。"""
    return parse_file(filepath, CYB_FIELDS, CYB_IMPORT_FIELDS, CYB_RECORD_LENGTH,
                      has_umaban=True, encoding=encoding)


def parse_bac_file(filepath: str, encoding: str = "cp932") -> List[Dict[str, str]]:
    """BAC（番組データ）ファイルをパースする。"""
    return parse_file(filepath, BAC_FIELDS, BAC_IMPORT_FIELDS, BAC_RECORD_LENGTH,
                      has_umaban=False, encoding=encoding)


def parse_joa_file(filepath: str, encoding: str = "cp932") -> List[Dict[str, str]]:
    """JOA（成績速報データ）ファイルをパースする。"""
    return parse_file(filepath, JOA_FIELDS, JOA_IMPORT_FIELDS, JOA_RECORD_LENGTH,
                      has_umaban=True, encoding=encoding)


# =============================================================================
# CLIテスト用
# =============================================================================
def test_parse_sample(filepath: str, file_type: str) -> None:
    """
    サンプルファイルをパースして先頭3レコードを表示する。
    CEOのPC上でテスト実行用。
    """
    parsers = {
        "KYI": parse_kyi_file,
        "CYB": parse_cyb_file,
        "BAC": parse_bac_file,
        "JOA": parse_joa_file,
    }
    
    parser = parsers.get(file_type.upper())
    if not parser:
        print(f"ERROR: Unknown file type: {file_type}")
        return
    
    print(f"\nParsing: {filepath}")
    print(f"Type: {file_type.upper()}")
    
    records = parser(filepath)
    print(f"Total records: {len(records)}")
    
    if records:
        print(f"\n--- First 3 records ---")
        for i, rec in enumerate(records[:3]):
            print(f"\n[Record {i+1}]")
            print(f"  race_key8:     {rec.get('jrdb_race_key8', 'N/A')}")
            print(f"  race_shikonen: {rec.get('race_shikonen', 'N/A')}")
            print(f"  basho_code:    {rec.get('basho_code', 'N/A')}")
            print(f"  year:          {rec.get('year', 'N/A')}")
            print(f"  kai:           {rec.get('kai', 'N/A')}")
            print(f"  nichi(raw):    {rec.get('nichi', 'N/A')}")
            print(f"  kaisai_nichime:{rec.get('kaisai_nichime', 'N/A')}")
            print(f"  race_num:      {rec.get('race_num', 'N/A')}")
            if "umaban" in rec:
                print(f"  umaban:        {rec.get('umaban', 'N/A')}")
            if "idm" in rec:
                print(f"  IDM:           {rec.get('idm', 'N/A')}")
            if "sogo_shisu" in rec:
                print(f"  sogo_shisu:    {rec.get('sogo_shisu', 'N/A')}")
            if "chokyo_hyoka" in rec:
                print(f"  chokyo_hyoka:  {rec.get('chokyo_hyoka', 'N/A')}")
            if "ls_shisu" in rec:
                print(f"  ls_shisu:      {rec.get('ls_shisu', 'N/A')}")
            if "juryo_shubetsu_code" in rec:
                print(f"  juryo_shubetsu:{rec.get('juryo_shubetsu_code', 'N/A')}")
            if "nengappi" in rec:
                print(f"  nengappi:      {rec.get('nengappi', 'N/A')}")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("使用方法:")
        print("  py -3.12 -m roi_pipeline.ingest.jrdb_parser <filepath> <KYI|CYB|BAC|JOA>")
        print()
        print("例:")
        print("  py -3.12 -m roi_pipeline.ingest.jrdb_parser E:/JRDB/KYI240307.txt KYI")
        sys.exit(1)
    
    test_parse_sample(sys.argv[1], sys.argv[2])
