from __future__ import annotations

import logging
from pathlib import Path

import duckdb

from wdi_pipeline.exceptions import ExportError

logger = logging.getLogger(__name__)


def export(
    conn: duckdb.DuckDBPyConnection,
    sql: str,
    dest: Path,
    fmt: str,
) -> int:
    """Execute `sql`, export result to `dest`, return row count.

    Args:
        conn: An open DuckDB connection that already has the 'dataset' TABLE.
        sql: A rendered SQL string ({{}} placeholders already replaced with literal values).
        dest: Destination file path (created / overwritten).
        fmt: Export format — 'csv' or 'parquet' (case-insensitive).

    Returns:
        Number of rows exported.

    Raises:
        ExportError: On unsupported format or DuckDB failure.
    """
    # 出力先フォルダが無ければ作る。親フォルダもまとめて作る/exist_ok=True で「既にあってもOK」
    dest.parent.mkdir(parents=True, exist_ok=True)

    # フォーマット判定を case-insensitive にする。
    fmt_upper = fmt.upper()

    try:
        # sql is pre-rendered by render() in runner.py (literals inlined); no bind params needed.
        # SQL の結果を TEMP VIEW _export として作る。
        # TEMP VIEW はセッション（その DuckDB 接続）の間だけ存在。
        conn.execute(f"CREATE OR REPLACE TEMP VIEW _export AS {sql}")

        # 行数カウント/fetchone() は 1 行返す, [0] でその値を取り出す
        row_count: int = conn.execute(
            "SELECT COUNT(*) FROM _export"
        ).fetchone()[0]

        # CSV: HEADER TRUE でヘッダ付き/dest は f-string で埋め込まれてる
        if fmt_upper == "CSV":
            conn.execute(
                f"COPY _export TO '{dest}' (FORMAT CSV, HEADER TRUE)"
            )
        # Parquet: 列指向で圧縮効率が良くて、分析用途に強い。
        elif fmt_upper == "PARQUET":
            conn.execute(
                f"COPY _export TO '{dest}' (FORMAT PARQUET)"
            )
        else:
            raise ExportError(f"Unsupported export format: '{fmt}'")

    except ExportError:
        raise
    except Exception as exc:
        raise ExportError(f"Export failed (dest={dest}, fmt={fmt}): {exc}") from exc

    logger.info("Exported %d rows → %s [%s]", row_count, dest, fmt_upper)
    return row_count
