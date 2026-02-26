"""
設定ファイルの読み書きユーティリティ。

設定ファイルパス: ~/.config/api_bulk_downloader/config.toml
"""
import sys
from pathlib import Path

CONFIG_PATH = Path.home() / ".config" / "api_bulk_downloader" / "config.toml"


def load_dest() -> Path | None:
    """設定ファイルから dest を読み込む。ファイルなし・dest キーなし → None。"""
    if not CONFIG_PATH.exists():
        return None

    if sys.version_info >= (3, 11):
        import tomllib
    else:
        try:
            import tomllib  # type: ignore[no-redef]
        except ImportError:
            try:
                import tomli as tomllib  # type: ignore[no-redef]
            except ImportError:
                return None

    with CONFIG_PATH.open("rb") as f:
        data = tomllib.load(f)

    dest = data.get("dest")
    if dest is None:
        return None
    return Path(dest)


def save_dest(path: Path) -> None:
    """dest を設定ファイルに書き込む。既存ファイルは上書き。"""
    CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    CONFIG_PATH.write_text(f'dest = "{path.as_posix()}"\n', encoding="utf-8")
