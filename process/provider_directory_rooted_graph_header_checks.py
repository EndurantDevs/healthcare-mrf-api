# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Preserve the exact callable header CHECK as protected application validation."""

import hashlib
import json

from db.plan_release_storage import storage_identifier, storage_relation
from process.provider_directory_generic_source_storage import _require

HEADER = "provider_directory_rooted_graph_dataset"
CHECK = "pd_rooted_graph_dataset_check"
COVERAGE = "provider_directory_fhir_request_failure_coverage_valid"
CHECK_SHA256 = "bb6af15aeadf6b2a3f0db0126e684d6f5e0659c1a5e54bf00bfa9c3527b31248"


def header_check_snapshot_sql(schema):
    """Bind the complete original constraint and callable to the protected handoff snapshot."""
    header = "'" + storage_relation(schema, HEADER).replace("'", "''") + "'"
    function = "'" + (storage_relation(schema, COVERAGE) + "(jsonb)").replace("'", "''") + "'"
    return f"""SELECT jsonb_build_object('oid',k.oid::bigint,'relation_oid',k.conrelid::bigint,
        'definition',pg_get_constraintdef(k.oid),'expression',pg_get_expr(k.conbin,k.conrelid),
        'function',to_jsonb(p),'language',l.lanname,'owner',c.relowner::bigint,'namespace',c.relnamespace::bigint)
        FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid
        JOIN pg_proc p ON p.oid=to_regprocedure({function}) JOIN pg_language l ON l.oid=p.prolang
        WHERE k.conrelid=to_regclass({header}) AND k.conname='{CHECK}' AND k.contype='c' AND k.convalidated"""


def _constraint_digest(definition, schema):
    variants = (
        storage_relation(schema, COVERAGE),
        storage_identifier(schema) + "." + COVERAGE,
        schema + "." + COVERAGE,
        COVERAGE,
    )
    for qualified in variants:
        if qualified + "(" in definition:
            definition = definition.replace(qualified + "(", "__coverage_valid(")
            break
    return hashlib.sha256(definition.encode()).hexdigest()


async def capture_rooted_header_check(connection, schema):
    """Accept only the known Core CHECK and the existing exact coverage callable contract."""
    from process.provider_directory_source_callable_custody import _CONTRACTS, _TYPE_OIDS, _require_definition

    encoded = await connection.fetchval(header_check_snapshot_sql(schema))
    snapshot_by_field = json.loads(encoded) if isinstance(encoded, str) else encoded
    _require(type(snapshot_by_field) is dict, "rooted_header_check_changed")
    _require(_constraint_digest(snapshot_by_field["definition"], schema) == CHECK_SHA256, "rooted_header_check_changed")
    definition = snapshot_by_field["function"]
    _require_definition(
        {"definition": definition, "language": snapshot_by_field["language"]},
        _CONTRACTS[COVERAGE + "(jsonb)"],
        schema,
        snapshot_by_field["namespace"],
        _TYPE_OIDS,
        storage_identifier(schema),
    )
    _require(int(definition["proowner"]) == snapshot_by_field["owner"], "rooted_header_check_owner_changed")
    return snapshot_by_field


async def defer_rooted_header_check(connection, catalog_schema, candidate_schema, candidate_name):
    """Exclude only the exact copied callable CHECK from a new empty private heap."""
    expected_by_field = await capture_rooted_header_check(connection, catalog_schema)
    relation = storage_relation(candidate_schema, candidate_name)
    copied = await connection.fetchval(
        "SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid=to_regclass($1) AND conname=$2",
        relation,
        CHECK,
    )
    _require(copied == expected_by_field["definition"], "rooted_header_check_changed")
    await connection.execute(f"ALTER TABLE {relation} DROP CONSTRAINT {storage_identifier(CHECK)}")


async def validate_rooted_header_check(connection, document):
    """Replay the complete original CHECK after writer closure, preserving PostgreSQL NULL semantics."""
    schema = document["storage"]["catalog_schema"]
    snapshot_by_field = await capture_rooted_header_check(connection, schema)
    _require(document["snapshot"]["header_check"] == snapshot_by_field, "rooted_header_check_changed")
    table = document["tables"][HEADER]
    relation = storage_relation(document["storage"]["storage_schema"], table["name"])
    accepted = await connection.fetchval(
        f"SELECT count(*)=1 AND bool_and(({snapshot_by_field['expression']}) IS NOT FALSE) "
        f"FROM {relation} WHERE dataset_id=$1",
        document["identity"]["dataset_id"],
    )
    _require(accepted is True, "rooted_header_check_invalid")
