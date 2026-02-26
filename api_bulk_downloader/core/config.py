"""
設定ファイルの読み書きユーティリティ。

設定ファイルパス: ~/.config/api_bulk_downloader/config.json
"""
import json
from pathlib import Path

CONFIG_PATH = Path.home() / ".config" / "api_bulk_downloader" / "config.json"


def load_dest() -> Path | None:
    """設定ファイルから dest を読み込む。ファイルなし・dest キーなし → None。"""
    if not CONFIG_PATH.exists():
        return None

    data = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    dest = data.get("dest")
    if dest is None:
        return None
    return Path(dest)


def save_dest(path: Path) -> None:
    """dest を設定ファイルに書き込む。既存ファイルは上書き。"""
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    CONFIG_PATH.write_text(
        json.dumps({"dest": path.as_posix()}, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
