# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare one existing allocator variant for an explicit CMS snapshot cut.

This changes no routine until the shared owner executes the checked upgrade.
The selector grants no rights: only an original superuser may use the branch.
CMS profile/claim, snapshot custody and terminal accounting belong to its caller.
"""

import hashlib

import sqlalchemy as sa

from db.migration_generic_source_candidates import _COPY_GUARD_SNAPSHOT, _canonical_copy_guard_snapshot
from db.migration_provider_directory_dataset_candidates import candidate_functions
from db.plan_release_storage import storage_identifier, storage_relation

SELECTOR = "app.cms_snapshot_dataset"
ORIGINAL_ALLOCATOR_SHA256 = "4078d67c51ad7277287e143282ab858e2335ba422d9fc24685d4c8abeb5590d2"
_ADMIN_SQL = sa.text("""SELECT session_user=current_user AND r.rolsuper
 AND pg_catalog.current_setting('transaction_read_only')='off'
 AND pg_catalog.current_setting('transaction_isolation')='read committed'
 AND pg_catalog.current_setting('session_replication_role')='origin' AS accepted
 FROM pg_catalog.pg_roles r WHERE r.rolname=session_user""")
# Body installation shares the existing SERIALIZABLE Source/profile transaction.
_INSTALLER_ADMIN_SQL = sa.text(
    str(_ADMIN_SQL).replace(
        "pg_catalog.current_setting('transaction_isolation')='read committed'",
        "pg_catalog.current_setting('transaction_isolation') IN ('read committed','serializable')",
        1,
    )
)


def _require(condition, code):
    if not condition:
        raise RuntimeError("cms_snapshot_allocator_" + code)


def allocator_bodies(schema):
    """Preserve the complete prior body and add only the original-admin branch."""
    declaration = (
        "CREATE FUNCTION " + storage_identifier(schema) + ".prepare_pd_generic_dataset_storage() RETURNS trigger\n"
        "LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog AS $fn$"
    )
    source = candidate_functions(schema)
    _require(source.count(declaration) == 1, "source_template_changed")
    original, separator, _remainder = source.split(declaration, 1)[1].partition("$fn$;")
    _require(separator and original.count("BEGIN\n") == 1, "source_template_changed")
    branch = f"""    IF pg_catalog.current_setting('{SELECTOR}',true)=NEW.dataset_id
       AND NEW.status='acquiring' AND NOT NEW.is_current AND NEW.published_at IS NULL
       AND NEW.publication_metadata_json->'source_ids'='["cms-npd"]'::jsonb
       AND NEW.publication_metadata_json->'source_release'->>'source_id'='cms-npd'
       AND NEW.publication_metadata_json->'source_release'->>'vector_sha256' ~ '^[0-9a-f]{{64}}$'
       AND EXISTS(SELECT FROM pg_catalog.pg_roles actor WHERE actor.rolname=session_user AND actor.rolsuper) THEN
        RETURN NEW;
    END IF;
"""
    return original, original.replace("BEGIN\n", "BEGIN\n" + branch, 1)


def generic_finalization_bodies(schema):
    """Bind the existing bulk validator to its exact text,text argument types."""
    declaration = (
        "CREATE FUNCTION " + storage_identifier(schema) + ".guard_pd_generic_dataset_finalization() RETURNS trigger\n"
    )
    source = candidate_functions(schema)
    _require(source.count(declaration) == 1, "finalization_template_changed")
    original = source.split(declaration, 1)[1].split("AS $fn$", 1)[1].split("$fn$;", 1)[0]
    call = ".validate_pd_dataset_bulk_relationships(NEW.dataset_id,'generic');"
    _require(original.count(call) == 1, "finalization_template_changed")
    return original, original.replace(
        call, ".validate_pd_dataset_bulk_relationships(NEW.dataset_id::pg_catalog.text,'generic'::pg_catalog.text);"
    )


def bound_cms_allocator_body(schema):
    """Bind the existing AFTER INSERT row hook before either allocator branch."""
    original, snapshot = allocator_bodies(schema)
    canonical = original.replace('"' + schema + '"', '"__schema__"').replace("'" + schema + "'", "'__schema__'")
    _require(
        hashlib.sha256(canonical.encode()).hexdigest() == ORIGINAL_ALLOCATOR_SHA256, "allocator_predecessor_changed"
    )
    header = storage_relation(schema, "provider_directory_endpoint_dataset")
    header_literal = "'" + header.replace("'", "''") + "'"
    invocation = f"""    IF TG_WHEN IS DISTINCT FROM 'AFTER' OR TG_OP IS DISTINCT FROM 'INSERT'
       OR TG_LEVEL IS DISTINCT FROM 'ROW'
       OR TG_TABLE_SCHEMA IS DISTINCT FROM '{schema}'
       OR TG_TABLE_NAME IS DISTINCT FROM 'provider_directory_endpoint_dataset'
       OR TG_RELID IS DISTINCT FROM pg_catalog.to_regclass({header_literal})::pg_catalog.oid THEN
        RAISE EXCEPTION 'provider_directory_dataset_allocator_invocation_invalid' USING ERRCODE='55000';
    END IF;
"""
    _require(snapshot.count("BEGIN\n") == 1, "allocator_template_changed")
    return snapshot.replace("BEGIN\n", "BEGIN\n" + invocation, 1)


def _identity(observed, expected, bodies):
    _require(
        type(expected) is dict
        and set(expected) == {"oid", "owner_oid"}
        and all(type(oid) is int and 0 < oid < 2**32 for oid in expected.values()),
        "identity_required",
    )
    _require(
        observed is not None
        and observed["body"] in bodies
        and int(observed["identity"]["oid"]) == expected["oid"]
        and int(observed["identity"]["proowner"]) == expected["owner_oid"],
        "function_changed",
    )


def upgrade_cms_snapshot_allocator(connection, schema, *, expected):
    """Change only the exact existing body; preserve its OID, ACLs and properties.

    Execute only after the shared owner accepts all exact-body consumers. This
    neither changes hooks nor issues a profile, role, grant or publication claim.
    """
    _require(connection.in_transaction(), "transaction_required")
    _require(
        connection.exec_driver_sql("SHOW search_path").scalar_one() in {"pg_catalog", "pg_catalog, pg_temp"},
        "search_path",
    )
    _require(connection.execute(_INSTALLER_ADMIN_SQL).scalar_one_or_none() is True, "original_admin_required")
    signature = storage_relation(schema, "prepare_pd_generic_dataset_storage") + "()"
    original, previous = allocator_bodies(schema)
    snapshot = bound_cms_allocator_body(schema)
    before = _canonical_copy_guard_snapshot(connection, signature)
    _identity(before, expected, (original, previous, snapshot))
    if before["body"] == snapshot:
        return
    _require(before["definition"].count(before["body"]) == 1, "definition_changed")
    with connection.begin_nested():
        connection.exec_driver_sql(before["definition"].replace(before["body"], snapshot, 1))
        after = _canonical_copy_guard_snapshot(connection, signature)
        _identity(after, expected, (snapshot,))
        _require(
            after["identity"] == before["identity"] and after["description"] == before["description"],
            "identity_changed",
        )


def upgrade_cms_generic_finalization_guard(connection, schema, *, expected):
    """Replace only the approved call expression after the paired consumer transition.

    The existing original Admin owns cohort disposition, protected expected
    identity and complete cut custody. This grants nothing and commits nothing.
    Exact casts prevent varchar-overload execution; schema CREATE remains a
    separate guard because default-argument overloads can still cause refusal.
    """
    _require(connection.in_transaction(), "transaction_required")
    _require(
        connection.exec_driver_sql("SHOW search_path").scalar_one() in {"pg_catalog", "pg_catalog, pg_temp"},
        "search_path",
    )
    _require(connection.execute(_INSTALLER_ADMIN_SQL).scalar_one_or_none() is True, "original_admin_required")
    signature = storage_relation(schema, "guard_pd_generic_dataset_finalization") + "()"
    original, hardened = generic_finalization_bodies(schema)
    before = _canonical_copy_guard_snapshot(connection, signature)
    _identity(before, expected, (original, hardened))
    if before["body"] == hardened:
        return
    _require(before["definition"].count(original) == 1, "definition_changed")
    with connection.begin_nested():
        connection.exec_driver_sql(before["definition"].replace(original, hardened, 1))
        after = _canonical_copy_guard_snapshot(connection, signature)
        _identity(after, expected, (hardened,))
        _require(
            after["identity"] == before["identity"] and after["description"] == before["description"],
            "identity_changed",
        )


async def require_cms_snapshot_allocator(connection, schema, *, expected):
    """Compare the approved body on the existing transaction, without repairing it."""
    from process.network_cms_registry_address_capture import NativeReceiptSession

    _require(connection.is_in_transaction() and not connection.is_closed(), "transaction_required")
    _require(await connection.fetchval("SHOW search_path") in {"pg_catalog", "pg_catalog, pg_temp"}, "search_path")
    signature = storage_relation(schema, "prepare_pd_generic_dataset_storage") + "()"
    observed = (
        await NativeReceiptSession(connection).execute(_COPY_GUARD_SNAPSHOT, {"signature": signature})
    ).one_or_none()
    _identity(observed, expected, (bound_cms_allocator_body(schema),))
    return expected.copy()
