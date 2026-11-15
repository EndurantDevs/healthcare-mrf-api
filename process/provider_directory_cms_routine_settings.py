# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure settings successors and declaration rendering for existing CMS routines."""

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


def _source_targets(routines, catalog_schema, catalog_schema_oid, preparation_schema):
    from process import provider_directory_cms_callable_custody as callables
    from process import provider_directory_cms_transition_routines as transition

    transition.require_cms_transition_routine_definitions(
        routines,
        catalog_schema=catalog_schema,
        catalog_schema_oid=catalog_schema_oid,
        preparation_schema=preparation_schema,
    )
    targets_by_oid = {}
    for routine in routines:
        native = routine["native"]
        if native["namespace"][:2] == [11, "pg_catalog"]:
            continue
        name = native["definition"]["proname"]
        if name in {"guard_cms_npd_relationship_truncate", "provider_directory_profile_delta_receipt_immutable_v1"}:
            legacy_settings = None
        elif name in callables._CALLEE_SETTINGS:
            legacy_settings = callables._CALLEE_SETTINGS[name]
        elif name.startswith("ptg_wave_"):
            legacy_settings = ["search_path=pg_catalog, " + native["namespace_identifier"]]
        else:
            legacy_settings = ["search_path=pg_catalog"]
        targets_by_oid[routine["oid"]] = approved_cms_routine_settings(legacy_settings)
    return targets_by_oid


def require_cms_terminal_routine_settings(routines, *, catalog_schema, catalog_schema_oid, preparation_schema):
    """Require source target settings; compatibility-old arrays never establish terminal readiness."""
    targets = _source_targets(routines, catalog_schema, catalog_schema_oid, preparation_schema)
    for routine in routines:
        if routine["oid"] in targets:
            _require(
                routine["native"]["definition"]["proconfig"] == targets[routine["oid"]][1], "terminal_settings_required"
            )


def _search_path_line(settings):
    parts = settings[0].removeprefix("search_path=").split(", ")
    names = [part[1:-1] if part.startswith('"') and part.endswith('"') else part for part in parts]
    return " SET search_path TO " + ", ".join("'" + name + "'" for name in names) + "\n"


def _declaration_after(declaration, old, new):
    """Change only PG18's source-fixed SET line before its AS body delimiter."""
    _require(type(declaration) is str and declaration.startswith("CREATE OR REPLACE FUNCTION "), "declaration_required")
    lines = declaration.splitlines(keepends=True)
    boundary = next((index for index, line in enumerate(lines) if line.startswith("AS ")), None)
    _require(boundary is not None, "declaration_body_required")
    header = lines[:boundary]
    original = _search_path_line(old)
    _require(header.count(original) == 1, "declaration_settings_changed")
    _require(sum(line.startswith(" SET search_path TO ") for line in header) == 1, "declaration_settings_changed")
    header[header.index(original)] = _search_path_line(new)
    return "".join([*header, *lines[boundary:]])
