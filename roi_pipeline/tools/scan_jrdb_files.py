"""
Eドライブ上のJRDBファイル構成を調査するスクリプト

使用方法:
    py -3.12 roi_pipeline/tools/scan_jrdb_files.py

CEOのPC上で実行し、出力結果を貼ってください。
"""
import os
import sys
from pathlib import Path
from collections import defaultdict


def scan_drive(root: str = "E:\\") -> None:
    """
    指定ドライブからJRDB関連ファイル（KYI/CYB/BAC/JOA）を探索する。
    .txt と .lzh の両方を検索。
    """
    print("=" * 70)
    print(f"JRDB ファイル探索: {root}")
    print("=" * 70)

    # 検索対象パターン
    targets = ["KYI", "CYB", "BAC", "JOA"]
    extensions = [".txt", ".lzh", ".zip"]

    found: dict[str, list[Path]] = defaultdict(list)
    dirs_checked = 0
    errors = 0

    for dirpath, dirnames, filenames in os.walk(root):
        dirs_checked += 1

        # 進捗表示（1000ディレクトリごと）
        if dirs_checked % 1000 == 0:
            print(f"  ... {dirs_checked} ディレクトリ検索済み", file=sys.stderr)

        # 深すぎるディレクトリはスキップ（パフォーマンス）
        depth = dirpath.replace(root, "").count(os.sep)
        if depth > 8:
            dirnames.clear()
            continue

        # Windows系システムフォルダはスキップ
        skip_dirs = {"$RECYCLE.BIN", "System Volume Information", "Windows", "ProgramData"}
        dirnames[:] = [d for d in dirnames if d not in skip_dirs]

        for fname in filenames:
            fname_upper = fname.upper()
            for target in targets:
                for ext in extensions:
                    if fname_upper.startswith(target) and fname_upper.endswith(ext.upper()):
                        fpath = Path(dirpath) / fname
                        try:
                            size = fpath.stat().st_size
                            found[target].append(fpath)
                        except (PermissionError, OSError):
                            errors += 1

    print(f"\n検索完了: {dirs_checked} ディレクトリ, エラー: {errors}")
    print()

    if not any(found.values()):
        print("  *** JRDBファイルが見つかりませんでした ***")
        print()
        print("  以下を確認してください:")
        print("  1. PC-KEIBAのJRDBデータ保存先設定")
        print("  2. JRDBからダウンロードした .lzh ファイルの保存先")
        print("  3. E: 以外のドライブにデータがないか")
        return

    # 結果表示
    for target in targets:
        files = found.get(target, [])
        if not files:
            print(f"[{target}] 見つかりませんでした")
            continue

        # ディレクトリごとにグルーピング
        dirs: dict[str, list[Path]] = defaultdict(list)
        for f in files:
            dirs[str(f.parent)].append(f)

        print(f"[{target}] {len(files)} ファイル")
        for d, file_list in sorted(dirs.items()):
            sizes = []
            for f in file_list:
                try:
                    sizes.append(f.stat().st_size)
                except (PermissionError, OSError):
                    pass

            total_mb = sum(sizes) / (1024 * 1024)
            print(f"  ディレクトリ: {d}")
            print(f"    ファイル数: {len(file_list)}")
            if sizes:
                print(f"    合計サイズ: {total_mb:.1f} MB")
                print(f"    最小ファイル: {min(sizes):,} bytes")
                print(f"    最大ファイル: {max(sizes):,} bytes")

            # サンプルファイル名（先頭5件 + 末尾5件）
            sorted_files = sorted(file_list, key=lambda f: f.name)
            print(f"    先頭5件:")
            for f in sorted_files[:5]:
                try:
                    s = f.stat().st_size
                    print(f"      {f.name}  ({s:,} bytes)")
                except (PermissionError, OSError):
                    print(f"      {f.name}  (サイズ不明)")
            if len(sorted_files) > 10:
                print(f"    ... ({len(sorted_files) - 10} 件省略) ...")
            if len(sorted_files) > 5:
                print(f"    末尾5件:")
                for f in sorted_files[-5:]:
                    try:
                        s = f.stat().st_size
                        print(f"      {f.name}  ({s:,} bytes)")
                    except (PermissionError, OSError):
                        print(f"      {f.name}  (サイズ不明)")
        print()

    # 先頭レコード検証（.txtファイルがある場合）
    print("=" * 70)
    print("先頭レコード検証（バイトレベル）")
    print("=" * 70)

    for target in targets:
        files = found.get(target, [])
        txt_files = [f for f in files if f.suffix.lower() == ".txt"]
        if not txt_files:
            continue

        # 最新のファイルを1つ選んで先頭レコードを表示
        sample = sorted(txt_files, key=lambda f: f.name)[-1]
        print(f"\n[{target}] サンプル: {sample.name}")
        try:
            with open(sample, "rb") as fh:
                line = fh.readline()
                raw = line[:80]  # 先頭80byte

                print(f"  先頭80byte (hex): {raw.hex()}")
                print(f"  先頭80byte (ascii): {raw.decode('ascii', errors='replace')}")

                # レースキー8byte分解
                rk = raw[:8].decode("ascii", errors="replace")
                print(f"  レースキー8byte: '{rk}'")
                print(f"    場コード (byte 1-2): '{rk[0:2]}'")
                print(f"    年       (byte 3-4): '{rk[2:4]}'")
                print(f"    回       (byte 5):   '{rk[4:5]}'")
                print(f"    日(hex)  (byte 6):   '{rk[5:6]}'")
                print(f"    R        (byte 7-8): '{rk[6:8]}'")

                if target != "BAC":
                    uma = raw[8:10].decode("ascii", errors="replace")
                    print(f"    馬番     (byte 9-10): '{uma}'")

        except Exception as e:
            print(f"  読み込みエラー: {e}")

    print()
    print("=" * 70)
    print("スキャン完了")
    print("=" * 70)
    print()
    print("【次のステップ】")
    print("上記の出力結果を全てコピーして、開発者AIに貼り付けてください。")
    print(".txt ファイルのディレクトリパスが最も重要な情報です。")


if __name__ == "__main__":
    # Eドライブを検索（引数で変更可能）
    drive = sys.argv[1] if len(sys.argv) > 1 else "E:\\"
    scan_drive(drive)
