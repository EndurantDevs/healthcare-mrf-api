# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact settings successors for existing source-verified CMS routines.

Pure-helper inputs are source-defined expectations, never observed settings.
The installer requires complete caller-authenticated native routine pins and
retains its caller's original transaction/deadline. It grants no authority.
"""

import asyncio
import math
import re
from copy import deepcopy


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


def _expected_postimage(routines, targets):
    expected = deepcopy(routines)
    for routine in expected:
        settings = targets.get(routine["oid"])
        if settings is None:
            continue
        old, new = settings
        native = routine["native"]
        current = native["definition"]["proconfig"]
        _require(current in (old, new), "settings_changed")
        if current != new:
            native["body"] = _declaration_after(native["body"], old, new)
            native["definition"]["proconfig"] = deepcopy(new)
    return expected


def _deadline(deadline):
    _require(
        type(deadline) in {int, float}
        and math.isfinite(deadline)
        and 0 < deadline - asyncio.get_running_loop().time() <= 3,
        "deadline_required",
    )


def _pinned_routines(expected_routines):
    _require(type(expected_routines) is list and bool(expected_routines), "protected_pins_required")
    routines = deepcopy(expected_routines)
    identities = []
    for routine in routines:
        _require(type(routine) is dict and set(routine) == {"oid", "native"}, "protected_pins_required")
        _require(type(routine["oid"]) is int and 0 < routine["oid"] < 2**32, "protected_pins_required")
        _require(type(routine["native"]) is dict, "protected_pins_required")
        identities.append(routine["oid"])
    _require(identities == sorted(set(identities)), "protected_pins_required")
    return routines


async def _current_routines(connection, identities):
    from process import provider_directory_cms_dependency_policy as dependencies
    from process import provider_directory_cms_preparation_custody as custody

    return [
        dict(oid=row["oid"], native=custody._decoded(row["native"]))
        for row in await connection.fetch(dependencies._ROUTINES_SQL, [], identities)
    ]


async def _original(connection, task, actor):
    from process import provider_directory_cms_preparation_custody as custody

    _require(
        asyncio.current_task() is task and connection.is_in_transaction() is True and connection.is_closed() is False,
        "original_transaction_required",
    )
    _require(dict(await connection.fetchrow(custody._ALLOCATION_ACTOR_SQL)) == actor, "original_admin_changed")


async def _admin(connection):
    from process import provider_directory_cms_preparation_custody as custody

    _require(connection.is_in_transaction() is True and connection.is_closed() is False, "transaction_required")
    await custody._require_allocation_resolution(connection)
    actor_by_field = dict(await connection.fetchrow(custody._ALLOCATION_ACTOR_SQL))
    _require(
        actor_by_field["administrator"] is True
        and actor_by_field["session"] == actor_by_field["current"]
        and actor_by_field["backend"] == connection.get_server_pid()
        and bool(actor_by_field["transaction"])
        and actor_by_field["replication"] == "origin"
        and actor_by_field["readonly"] == "off"
        and actor_by_field["search_path"] == "pg_catalog, pg_temp",
        "original_admin_required",
    )
    await custody._lock_allocation_catalogs(connection)
    return actor_by_field


_STABILITY_SQL = """WITH hooks AS MATERIALIZED (
 SELECT t.tgfoid,pg_catalog.jsonb_agg(pg_catalog.to_jsonb(t) ORDER BY t.oid) AS bindings
 FROM pg_catalog.pg_trigger t WHERE t.tgfoid=ANY($1::pg_catalog.oid[]) GROUP BY t.tgfoid
)
SELECT p.oid::bigint,pg_catalog.obj_description(p.oid,'pg_proc') AS description,
 COALESCE(h.bindings,'[]'::pg_catalog.jsonb) AS hooks
 FROM pg_catalog.pg_proc p LEFT JOIN hooks h ON h.tgfoid=p.oid
 WHERE p.oid=ANY($1::pg_catalog.oid[]) ORDER BY p.oid"""


def _statements(routines, targets, catalog_schema, preparation_schema):
    from db.plan_release_storage import storage_relation
    from process import provider_directory_cms_transition_routines as transition

    alias_by_type = {"smallint": "int2", "integer": "int4", "bigint": "int8"}
    types = {"smallint", "integer", "bigint", "jsonb", "regclass", "text", "text[]", "varchar[]"}
    specs = transition._contracts(catalog_schema, preparation_schema)
    statements, changed_oids = [], []
    for routine in routines:
        if routine["oid"] not in targets:
            continue
        _old, new = targets[routine["oid"]]
        definition = routine["native"]["definition"]
        if definition["proconfig"] == new:
            continue
        name = definition["proname"]
        arguments = [kind for _name, kind in specs[name][0]]
        _require(all(kind in types for kind in arguments), "source_signature_required")
        signature = (
            storage_relation(catalog_schema, name)
            + "("
            + ",".join("pg_catalog." + alias_by_type.get(kind, kind) for kind in arguments)
            + ")"
        )
        statements.append("ALTER FUNCTION " + signature + " SET search_path TO " + new[0].removeprefix("search_path="))
        changed_oids.append(routine["oid"])
    return statements, changed_oids


async def install_cms_routine_settings(
    connection, *, expected_routines, catalog_schema, catalog_schema_oid, preparation_schema, deadline
):
    """Install only approved settings on pinned functions; caller rolls back every failed cut.

    Pins must come from genuine protected inventory on this exact database. The
    caller holds complete owner/writer/reverse-hook custody and the same absolute
    deadline through outer COMMIT/ROLLBACK. This opens no transaction/savepoint,
    grants no privileges and performs no commit or automatic retry. Comment/hook
    observations below are stability checks only, never new expected authority.
    """
    from process import provider_directory_cms_owner_execution as execution
    from process import provider_directory_cms_owner_postimage as postimages

    _deadline(deadline)
    routines = _pinned_routines(expected_routines)
    targets_by_oid = _source_targets(routines, catalog_schema, catalog_schema_oid, preparation_schema)
    expected = _expected_postimage(routines, targets_by_oid)
    commands, changed_oids = _statements(routines, targets_by_oid, catalog_schema, preparation_schema)
    identities = [routine_row["oid"] for routine_row in routines]
    task = asyncio.current_task()
    async with asyncio.timeout_at(deadline):
        actor = await _admin(connection)
        _require(await connection.fetchval(execution._DDL_HOOKS_SQL) is True, "ddl_hooks_enabled")
        _require(
            postimages._same(await _current_routines(connection, identities), routines), "protected_preimage_changed"
        )
        stability_rows = [dict(routine_row) for routine_row in await connection.fetch(_STABILITY_SQL, changed_oids)]
        _require([routine_row["oid"] for routine_row in stability_rows] == changed_oids, "stability_scope_changed")
        await _original(connection, task, actor)
        _deadline(deadline)
        if commands:
            await connection.execute(";\n".join(commands) + ";")
        observed = await _current_routines(connection, identities)
        _require(postimages._same(observed, expected), "settings_postimage_changed")
        require_cms_terminal_routine_settings(
            observed,
            catalog_schema=catalog_schema,
            catalog_schema_oid=catalog_schema_oid,
            preparation_schema=preparation_schema,
        )
        _require(
            postimages._same(
                [dict(routine_row) for routine_row in await connection.fetch(_STABILITY_SQL, changed_oids)],
                stability_rows,
            ),
            "stability_changed",
        )
        await _original(connection, task, actor)
        return deepcopy(observed)
