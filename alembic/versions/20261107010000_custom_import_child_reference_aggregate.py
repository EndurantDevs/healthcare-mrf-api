# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Refresh the existing child-reference check with an aggregate witness."""

from __future__ import annotations

import hashlib
import importlib.util
from pathlib import Path

from sqlalchemy import text

from alembic import op

revision = "20261107010000_custom_import_child_reference_aggregate"
down_revision = "20261104010000_custom_import_unlogged_landing"
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
        SELECT 'child_revision_reference',c.child_revision_id FROM __CANDIDATE__.custom_import_child_revision c
        WHERE ROW(c.dataset_id,c.definition_revision_id,c.schema_revision_id) IS DISTINCT FROM
            ROW($2,$3,$4)
          OR NOT EXISTS (
            SELECT 1 FROM __CANDIDATE__.custom_import_root_record k
            WHERE k.root_record_id=c.root_record_id AND k.dataset_id=c.dataset_id
              AND k.canonical_logical_key COLLATE "C" IS NOT DISTINCT FROM c.canonical_parent_key COLLATE "C"
              AND k.logical_key_sha256 IS NOT DISTINCT FROM c.parent_key_sha256
          )
          OR NOT EXISTS (
            SELECT 1 FROM __CONTROL__.custom_import_child_collection collection
            WHERE collection.collection_slot=c.collection_slot
              AND collection.dataset_id=c.dataset_id AND collection.schema_revision_id=c.schema_revision_id
          )
          OR NOT EXISTS (
            SELECT 1 FROM __CANDIDATE__.custom_import_pack p
            JOIN __CONTROL__.custom_import_source_stream stream ON stream.definition_revision_id=p.definition_revision_id
              AND stream.dataset_id=p.dataset_id AND stream.schema_revision_id=p.schema_revision_id
              AND stream.stream_slot=p.stream_slot AND stream.record_kind='child' AND stream.collection_slot=c.collection_slot
            WHERE p.pack_id=c.pack_id AND ROW(p.execution_id,p.dataset_id,p.definition_revision_id,p.schema_revision_id,
                p.capture_bundle_id,p.producing_fence,p.producing_token_sha256)
              IS NOT DISTINCT FROM ROW($5,c.dataset_id,c.definition_revision_id,c.schema_revision_id,
                $6,$7,$8)
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
        SELECT 'child_revision_reference',min(c.child_revision_id) FROM __CANDIDATE__.custom_import_child_revision c
        LEFT JOIN __CANDIDATE__.custom_import_root_record k
          ON k.root_record_id=c.root_record_id AND k.dataset_id=c.dataset_id
          AND k.canonical_logical_key COLLATE "C" IS NOT DISTINCT FROM c.canonical_parent_key COLLATE "C"
          AND k.logical_key_sha256 IS NOT DISTINCT FROM c.parent_key_sha256
        LEFT JOIN __CONTROL__.custom_import_child_collection collection
          ON collection.collection_slot=c.collection_slot
          AND collection.dataset_id=c.dataset_id AND collection.schema_revision_id=c.schema_revision_id
        LEFT JOIN (
            SELECT p.pack_id,p.execution_id,p.dataset_id,p.definition_revision_id,p.schema_revision_id,
              p.capture_bundle_id,p.producing_fence,p.producing_token_sha256,stream.collection_slot
            FROM __CANDIDATE__.custom_import_pack p
            JOIN __CONTROL__.custom_import_source_stream stream ON stream.definition_revision_id=p.definition_revision_id
              AND stream.dataset_id=p.dataset_id AND stream.schema_revision_id=p.schema_revision_id
              AND stream.stream_slot=p.stream_slot AND stream.record_kind='child'
            WHERE ROW(p.execution_id,p.dataset_id,p.definition_revision_id,p.schema_revision_id,
                p.capture_bundle_id,p.producing_fence,p.producing_token_sha256)
              IS NOT DISTINCT FROM ROW($5,$2,$3,$4,$6,$7,$8)
        ) pack_stream ON pack_stream.pack_id=c.pack_id AND pack_stream.collection_slot=c.collection_slot
        WHERE ROW(c.dataset_id,c.definition_revision_id,c.schema_revision_id) IS DISTINCT FROM
            ROW($2,$3,$4)
          OR k.root_record_id IS NULL
          OR collection.collection_slot IS NULL
          OR pack_stream.pack_id IS NULL
        HAVING count(*)>0) violation(failure_code,row_identity) LIMIT 1;
"""
_QUERY_SHA256 = "1eae0e78f09899f138d71fa22d457efa371b4cdba63ee50bf5d1e412f472ec59"


def _ranges():
    path = Path(__file__).with_name("20261020190000_custom_import_verification_root_ranges.py")
    spec = importlib.util.spec_from_file_location("child_reference_aggregate_ranges", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _bodies(schema):
    ranges = _ranges()
    arms = ranges._arms()
    staged = arms._staged()
    bulk = staged._finality()._bulk()
    storage = bulk._storage()
    continuation = staged._previous("20261019010000_custom_import_build_continuation")
    query = arms._queries(staged)[18]
    if query != _PREVIOUS_QUERY or hashlib.sha256(_UPDATED_QUERY.encode()).hexdigest() != _QUERY_SHA256:
        raise RuntimeError("custom_import_child_reference_query_identity_mismatch")
    previous = " " + ranges._body(schema) + " "
    updated = continuation._replace(
        previous,
        storage._literal(bulk._control_sql(storage, schema, _PREVIOUS_QUERY)),
        storage._literal(bulk._control_sql(storage, schema, _UPDATED_QUERY)),
    )
    return previous, updated


def _refresh(*, rollback):
    ranges = _ranges()
    schema = ranges._schema()
    staged = ranges._arms()._staged()
    storage = staged._finality()._bulk()._storage()
    quoted = storage._quote(schema)
    identity = f"{quoted}.verify_custom_import_build_structure(bigint)"
    previous, updated = _bodies(schema)
    installed = (
        op.get_bind()
        .execute(
            text("SELECT prosrc FROM pg_proc WHERE oid=to_regprocedure(:identity)"),
            {"identity": identity},
        )
        .scalar_one_or_none()
    )
    # An already applied successor still has to pass the exact native metadata checks.
    permitted = {updated} if rollback else {previous, updated}
    if installed not in permitted:
        raise RuntimeError("custom_import_child_reference_function_identity_mismatch")
    op.execute(text(f"LOCK TABLE {quoted}.custom_import_build_verification IN SHARE ROW EXCLUSIVE MODE"))
    continuation = staged._previous("20261019010000_custom_import_build_continuation")
    continuation.op = op
    continuation._schema = lambda: schema
    continuation._refresh(identity, installed, previous if rollback else updated)


def upgrade():
    """Refresh the existing verifier while preserving its checked native identity."""
    _refresh(rollback=False)


def downgrade():
    """Restore the exact prior verifier without reversing the landing migration."""
    _refresh(rollback=True)
