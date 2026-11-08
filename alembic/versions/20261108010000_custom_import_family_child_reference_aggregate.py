# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Refresh only the membership-reference witness and its scoped planner choice."""

from __future__ import annotations

import hashlib
import importlib.util
from pathlib import Path

from sqlalchemy import text

from alembic import op

revision = "20261108010000_custom_import_family_child_reference_aggregate"
down_revision = "20261107010000_custom_import_child_reference_aggregate"
branch_labels = None
depends_on = None

_PREVIOUS_QUERY = """-- Licensed under the HealthPorta Non-Commercial License (see LICENSE).
SELECT violation.* FROM (
        -- Execute only inside the protected frozen-candidate validator. All three
        -- namespace tokens are registry/OID bindings, never caller-supplied identifiers.
        -- Failure rows are internal diagnostics, not publication or validation receipts.
        WITH expected_build AS (
            SELECT b.* FROM __CONTROL__.custom_import_build_attempt b
            WHERE b.execution_id=$5 AND b.producing_fence=$7
        ), revision_counts AS (
            SELECT pack_id,count(*) AS record_count FROM (
                SELECT r.pack_id FROM __CANDIDATE__.custom_import_root_revision r
                UNION ALL SELECT c.pack_id FROM __CANDIDATE__.custom_import_child_revision c
            ) revisions GROUP BY pack_id
        ), child_counts AS (
            SELECT e.family_revision_id,count(*) AS child_count
            FROM __CANDIDATE__.custom_import_family_child e GROUP BY e.family_revision_id
        )
        SELECT 'family_child_reference',e.child_revision_id FROM __CANDIDATE__.custom_import_family_child e
        WHERE ROW(e.dataset_id,e.schema_revision_id) IS DISTINCT FROM ROW($2,$4)
          OR NOT EXISTS (
            SELECT 1 FROM __CANDIDATE__.custom_import_family_revision f WHERE f.family_revision_id=e.family_revision_id
              AND ROW(f.dataset_id,f.schema_revision_id,f.root_record_id)
                IS NOT DISTINCT FROM ROW(e.dataset_id,e.schema_revision_id,e.root_record_id)
          )
          OR NOT EXISTS (
            SELECT 1 FROM __CANDIDATE__.custom_import_child_revision c WHERE c.child_revision_id=e.child_revision_id
              AND ROW(c.dataset_id,c.definition_revision_id,c.schema_revision_id,c.root_record_id,c.collection_slot)
                IS NOT DISTINCT FROM ROW(e.dataset_id,$3,e.schema_revision_id,e.root_record_id,e.collection_slot)
          )) violation(failure_code,row_identity) LIMIT 1;
"""
_UPDATED_QUERY = """-- Licensed under the HealthPorta Non-Commercial License (see LICENSE).
SELECT violation.* FROM (
        -- Execute only inside the protected frozen-candidate validator. All three
        -- namespace tokens are registry/OID bindings, never caller-supplied identifiers.
        -- Failure rows are internal diagnostics, not publication or validation receipts.
        WITH expected_build AS (
            SELECT b.* FROM __CONTROL__.custom_import_build_attempt b
            WHERE b.execution_id=$5 AND b.producing_fence=$7
        ), revision_counts AS (
            SELECT pack_id,count(*) AS record_count FROM (
                SELECT r.pack_id FROM __CANDIDATE__.custom_import_root_revision r
                UNION ALL SELECT c.pack_id FROM __CANDIDATE__.custom_import_child_revision c
            ) revisions GROUP BY pack_id
        ), child_counts AS (
            SELECT e.family_revision_id,count(*) AS child_count
            FROM __CANDIDATE__.custom_import_family_child e GROUP BY e.family_revision_id
        )
        SELECT 'family_child_reference',min(e.child_revision_id)
        FROM __CANDIDATE__.custom_import_family_child e
        LEFT JOIN __CANDIDATE__.custom_import_family_revision f
          ON f.family_revision_id=e.family_revision_id
          AND ROW(f.dataset_id,f.schema_revision_id,f.root_record_id)
            IS NOT DISTINCT FROM ROW(e.dataset_id,e.schema_revision_id,e.root_record_id)
        LEFT JOIN __CANDIDATE__.custom_import_child_revision c
          ON c.child_revision_id=e.child_revision_id
          AND ROW(c.dataset_id,c.definition_revision_id,c.schema_revision_id,c.root_record_id,c.collection_slot)
            IS NOT DISTINCT FROM ROW(e.dataset_id,$3,e.schema_revision_id,e.root_record_id,e.collection_slot)
        WHERE ROW(e.dataset_id,e.schema_revision_id) IS DISTINCT FROM ROW($2,$4)
          OR f.family_revision_id IS NULL
          OR c.child_revision_id IS NULL
        HAVING count(*)>0) violation(failure_code,row_identity) LIMIT 1;
"""
_QUERY_SHA256 = "a4b77c5ce19624b2eb372de9d614adafa9f1da22850a47981016bb575d2c22f8"
_EXECUTE = """            EXECUTE query INTO failure USING g.generation_id,g.dataset_id,g.definition_revision_id,g.schema_revision_id,
                g.execution_id,g.capture_bundle_id,g.producing_fence,g.producing_token_sha256,g.base_generation_id,root_ids,b.build_id;"""


def _parent():
    path = Path(__file__).with_name("20261107010000_custom_import_child_reference_aggregate.py")
    spec = importlib.util.spec_from_file_location("family_child_reference_parent", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _schema():
    return _parent()._ranges()._schema()


def _bodies(schema):
    parent = _parent()
    ranges = parent._ranges()
    arms = ranges._arms()
    staged = arms._staged()
    bulk = staged._finality()._bulk()
    storage = bulk._storage()
    continuation = staged._previous("20261019010000_custom_import_build_continuation")
    if (
        arms._queries(staged)[20] != _PREVIOUS_QUERY
        or hashlib.sha256(_UPDATED_QUERY.encode()).hexdigest() != _QUERY_SHA256
    ):
        raise RuntimeError("custom_import_family_child_reference_query_identity_mismatch")
    previous = parent._bodies(schema)[1]
    updated = continuation._replace(
        previous,
        storage._literal(bulk._control_sql(storage, schema, _PREVIOUS_QUERY)),
        storage._literal(bulk._control_sql(storage, schema, _UPDATED_QUERY)),
    )
    updated = continuation._replace(
        updated,
        "failure record; root_ids bigint[]; stage integer;",
        "failure record; root_ids bigint[]; previous_enable_nestloop text; stage integer;",
    )
    # The exception block rolls back its transaction-local setting even for a
    # canceled statement; the caller still receives the original failure.
    guarded = (
        """            IF stage=21 THEN
                previous_enable_nestloop:=pg_catalog.current_setting('enable_nestloop');
                BEGIN
                    PERFORM pg_catalog.set_config('enable_nestloop','off',true);
"""
        + _EXECUTE
        + """
                    PERFORM pg_catalog.set_config('enable_nestloop',previous_enable_nestloop,true);
                EXCEPTION WHEN OTHERS THEN
                    PERFORM pg_catalog.set_config('enable_nestloop',previous_enable_nestloop,true);
                    RAISE;
                END;
            ELSE
"""
        + _EXECUTE
        + """
            END IF;"""
    )
    updated = continuation._replace(updated, _EXECUTE, guarded)
    return previous, updated


def _refresh(*, rollback):
    parent = _parent()
    ranges = parent._ranges()
    schema = _schema()
    staged = ranges._arms()._staged()
    storage = staged._finality()._bulk()._storage()
    identity = f"{storage._quote(schema)}.verify_custom_import_build_structure(bigint)"
    previous, updated = _bodies(schema)
    installed = (
        op.get_bind()
        .execute(text("SELECT prosrc FROM pg_proc WHERE oid=to_regprocedure(:identity)"), {"identity": identity})
        .scalar_one_or_none()
    )
    if installed not in ({updated} if rollback else {previous, updated}):
        raise RuntimeError("custom_import_family_child_reference_function_identity_mismatch")
    op.execute(
        text(f"LOCK TABLE {storage._quote(schema)}.custom_import_build_verification IN SHARE ROW EXCLUSIVE MODE")
    )
    continuation = staged._previous("20261019010000_custom_import_build_continuation")
    continuation.op = op
    continuation._schema = lambda: schema
    continuation._refresh(identity, installed, previous if rollback else updated)


def upgrade():
    """Preserve the existing verifier's checked OID, metadata and privileges."""
    _refresh(rollback=False)


def downgrade():
    """Restore the exact previous verifier body and planner behavior."""
    _refresh(rollback=True)
