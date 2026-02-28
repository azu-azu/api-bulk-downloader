from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from src.exceptions import ExportError

logger = logging.getLogger(__name__)


def export(
    conn: Any,  # duckdb.DuckDBPyConnection
    sql: str,
    values: list,
    dest: Path,
    fmt: str,
) -> int:
    """Execute `sql`, export result to `dest`, return row count.

    Args:
        conn: An open DuckDB connection that already has the 'dataset' TABLE.
        sql: A rendered SQL string ({{}} placeholders already replaced with literal values).
        values: Bind parameter list — always empty after render(); kept for API compatibility.
        dest: Destination file path (created / overwritten).
        fmt: Export format — 'csv' or 'parquet' (case-insensitive).

    Returns:
        Number of rows exported.

    Raises:
        ExportError: On unsupported format or DuckDB failure.
    """
    dest.parent.mkdir(parents=True, exist_ok=True)
    fmt_upper = fmt.upper()

    try:
        # render() embeds literal values, so no bind params are needed here.
        conn.execute(f"CREATE OR REPLACE TEMP VIEW _export AS {sql}")
        row_count: int = conn.execute(
            "SELECT COUNT(*) FROM _export"
        ).fetchone()[0]

        if fmt_upper == "CSV":
            conn.execute(
                f"COPY _export TO '{dest}' (FORMAT CSV, HEADER TRUE)"
            )
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
