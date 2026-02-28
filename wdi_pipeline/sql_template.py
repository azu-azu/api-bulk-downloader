from __future__ import annotations

import re

from wdi_pipeline.exceptions import SQLTemplateError

_PLACEHOLDER = re.compile(r"\{\{(\w+)\}\}")


def render(sql: str, params: dict[str, str]) -> tuple[str, list]:
    """Replace {{key}} placeholders with SQL literal values.

    Integer/float strings are embedded as bare numeric literals.
    Other strings are embedded as single-quoted SQL strings (apostrophes escaped).

    Returns:
        (rendered_sql, []) — the list is always empty; values are inlined as literals.

    Raises:
        SQLTemplateError: if any {{key}} in `sql` is not present in `params`.

    Note:
        Params come from trusted manifest YAML (operator-controlled config), not from
        external user input, so direct literal embedding is safe here.
    """
    def _replace(match: re.Match) -> str:
        key = match.group(1)
        if key not in params:
            raise SQLTemplateError(
                f"SQL template references undefined parameter: '{{{{{key}}}}}'"
            )
        return _to_sql_literal(params[key])

    rendered = _PLACEHOLDER.sub(_replace, sql)
    return rendered, []


def _to_sql_literal(val: str) -> str:
    """Format a Python string as a SQL literal (integer, float, or quoted string)."""
    try:
        int(val)
        return val
    except ValueError:
        pass
    try:
        float(val)
        return val
    except ValueError:
        pass
    # Quoted string — escape single quotes per SQL standard
    return "'" + val.replace("'", "''") + "'"
