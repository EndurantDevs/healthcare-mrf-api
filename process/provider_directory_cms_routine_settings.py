# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure settings successors for existing CMS routines."""

import re


def _require(condition, code):
    if not condition:
        raise ValueError("cms_routine_settings_" + code)


def approved_cms_routine_settings(expected_legacy):
    """Return detached exact old/new arrays; preserve the two source NULL exceptions."""
    if expected_legacy is None:
        return None, None
    _require(type(expected_legacy) is list and bool(expected_legacy), "source_settings_required")
    if expected_legacy in (["search_path=pg_catalog"], ["search_path=pg_catalog", "TimeZone=UTC"]):
        return list(expected_legacy), ["search_path=pg_catalog, pg_temp", *expected_legacy[1:]]
    _require(
        len(expected_legacy) == 1
        and type(expected_legacy[0]) is str
        and expected_legacy[0].startswith("search_path=pg_catalog, "),
        "source_settings_required",
    )
    rendered = expected_legacy[0].removeprefix("search_path=pg_catalog, ")
    schema = rendered[1:-1] if rendered.startswith('"') and rendered.endswith('"') else rendered
    _require(
        re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,62}", schema) is not None
        and schema not in {"pg_catalog", "pg_temp"}
        and (rendered == '"' + schema + '"' or rendered == schema == schema.lower()),
        "source_settings_required",
    )
    return list(expected_legacy), [expected_legacy[0] + ", pg_temp"]
