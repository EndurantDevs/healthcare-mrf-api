# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Render exact typed retirement successors; install nothing and grant no authority."""

import hashlib

from db import migration_provider_directory_first_acquisition_retirement as first
from db import migration_provider_directory_first_retirement_owner_lineage as lineage
from db import migration_provider_directory_terminal_root_retirement_evidence as evidence
from db import migration_provider_directory_terminal_root_retirement_guards as guards
from db import migration_provider_directory_terminal_root_retirement_v2 as v2
from db.plan_release_storage import storage_identifier, storage_relation

ORIGINAL_SHA256 = {
    evidence.EVIDENCE_FUNCTION: "ba101807efd88ec7618eb5b68b11cd6129c33e30f3d7a5d8d2c2ec5ce2c4c7ea",
    v2.EVIDENCE_FUNCTION: "87c8b3756173597edd37b86bfdc02efaf57440c854867e176f3bf4e76cb5042c",
    guards.VALID_FUNCTION: "da88c9fe2f778e12d5ea319fa5d4380c96dd949d7378895d998ff091b7a4048c",
    v2.VALID_FUNCTION: "66d6abf589c8c7c9e18b08e5926e481add2fbd9d21d61509c60b14c3010ce9de",
    guards.PARENT_GUARD: "bc73e0e7e9a1dd6c1e7900d27fef68f68d73ecb8c347616f37208d47158d3968",
    guards.IMPORT_RUN_GUARD: "e1b5e94a87add4618a764e3b6039e6d6eafe3cf6f4e3d0a3f6b0fcbe1dc1278a",
}


def _require(condition, code):
    if not condition:
        raise RuntimeError("cms_retirement_resolution_" + code)


def _original_statements(schema):
    return (
        lineage.replacements(schema)[0][1],
        v2.evidence_function_sql(schema),
        guards.valid_function_sql(schema),
        v2.valid_function_sql(schema),
        first.parent_sql(schema),
        first.run_sql(schema),
    )


def _digest(body, schema):
    canonical = body.replace('"' + schema + '"', '"__schema__"').replace("'" + schema + "'", "'__schema__'")
    return hashlib.sha256(canonical.encode()).hexdigest()


def _evidence_calls(schema):
    function = storage_relation(schema, evidence.RELATION_EVIDENCE_FUNCTION)
    return [
        (
            function + "(parent.dataset_id, " + evidence._ql(root) + ")",
            function + "(parent.dataset_id::pg_catalog.text, " + evidence._ql(root) + "::pg_catalog.text)",
        )
        for root in evidence._relation_specs(schema)
    ]


def _valid_calls(schema, version):
    return [
        (
            storage_relation(schema, name) + "(\n                row.dataset_id,",
            storage_relation(schema, name) + "(\n                row.dataset_id::pg_catalog.text,",
        )
        for name in (version.MARKER_FUNCTION, version.ELIGIBLE_FUNCTION)
    ]


def _parent_calls(schema):
    return [
        (storage_relation(schema, name) + "(" + argument, storage_relation(schema, name) + "(" + typed)
        for version in (guards, v2)
        for name, argument, typed in (
            (
                version.ELIGIBLE_FUNCTION,
                "OLD.dataset_id, minimum_age)",
                "OLD.dataset_id::pg_catalog.text, minimum_age)",
            ),
            (version.MARKER_FUNCTION, "NULL, marker)", "NULL::pg_catalog.text, marker)"),
            (version.EVIDENCE_FUNCTION, "OLD.dataset_id)", "OLD.dataset_id::pg_catalog.text)"),
        )
    ]


def _run_calls(schema):
    function = storage_relation(schema, guards.RUN_RETIRED_FUNCTION)
    return [
        (function + "(" + argument + ")", function + "(" + argument + "::pg_catalog.text)")
        for argument in ("OLD.run_id", "NEW.run_id", "NEW.retry_of_run_id")
    ]


def retirement_resolution_bodies(schema):
    """Return six current-source (old,new) body pairs, never observation-selected variants.

    The caller owns exact identity/ACL/hook pins, every body consumer, original
    Admin custody, deadline and rollback. Older pre-first-acquisition/owner-lineage
    variants intentionally refuse; they need their own separately reviewed pair.
    """
    storage_identifier(schema)
    repairs = (
        _evidence_calls(schema),
        _evidence_calls(schema),
        _valid_calls(schema, guards),
        _valid_calls(schema, v2),
        _parent_calls(schema),
        _run_calls(schema),
    )
    pairs_by_name = {}
    for (name, digest), statement, replacements in zip(
        ORIGINAL_SHA256.items(), _original_statements(schema), repairs, strict=True
    ):
        _require(statement.count("AS $function$") == statement.count("$function$;") == 1, "predecessor_changed")
        original = statement.split("AS $function$", 1)[1].split("$function$;", 1)[0]
        _require(_digest(original, schema) == digest, "predecessor_changed")
        hardened = original
        for previous, current in replacements:
            _require(hardened.count(previous) == 1, "callsite_changed")
            hardened = hardened.replace(previous, current, 1)
        pairs_by_name[name] = original, hardened
    return pairs_by_name


def _snapshot(connection, schema, name):
    """Read one exact existing signature; full row custody stays with the caller."""
    import sqlalchemy as sa

    arguments = "" if name.startswith("guard_") else "pg_catalog.text"
    return (
        connection.execute(
            sa.text(
                "SELECT p.prosrc AS body,pg_catalog.pg_get_functiondef(p.oid) AS definition,"
                "pg_catalog.to_jsonb(p)-'prosrc' AS identity,pg_catalog.obj_description(p.oid,'pg_proc') AS description "
                "FROM pg_catalog.pg_proc p WHERE p.oid=pg_catalog.to_regprocedure(:signature) AND p.prokind='f'"
            ),
            {"signature": storage_relation(schema, name) + "(" + arguments + ")"},
        )
        .mappings()
        .one_or_none()
    )


def upgrade_cms_retirement_resolution(connection, schema, *, expected):
    """Replace six approved bodies atomically; never change attributes or commit.

    The original Admin must authenticate the six OID/owner pins from protected
    custody, qualify all consumers and retain its deadline through outer commit.
    This helper supplies no owner, writer, creator or profile authority.
    """
    from db.migration_cms_snapshot_allocator import _INSTALLER_ADMIN_SQL, _identity

    _require(connection.in_transaction(), "transaction_required")
    pairs_by_name = retirement_resolution_bodies(schema)
    _require(
        type(expected) is dict
        and expected.keys() == pairs_by_name.keys()
        and all(type(pin) is dict for pin in expected.values()),
        "identities_required",
    )
    pins_by_name = {name: dict(pin) for name, pin in expected.items()}
    _require(
        connection.exec_driver_sql("SHOW search_path").scalar_one() in {"pg_catalog", "pg_catalog, pg_temp"},
        "search_path",
    )
    _require(connection.execute(_INSTALLER_ADMIN_SQL).scalar_one_or_none() is True, "original_admin_required")
    before_by_name = {name: _snapshot(connection, schema, name) for name in pairs_by_name}
    for name, bodies in pairs_by_name.items():
        _identity(before_by_name[name], pins_by_name[name], bodies)
    with connection.begin_nested():
        for name, (original, hardened) in pairs_by_name.items():
            before = before_by_name[name]
            if before["body"] == original:
                _require(before["definition"].count(original) == 1, "definition_changed")
                connection.exec_driver_sql(before["definition"].replace(original, hardened, 1))
            after = _snapshot(connection, schema, name)
            _identity(after, pins_by_name[name], (hardened,))
            _require(
                after["identity"] == before["identity"] and after["description"] == before["description"],
                "identity_changed",
            )
