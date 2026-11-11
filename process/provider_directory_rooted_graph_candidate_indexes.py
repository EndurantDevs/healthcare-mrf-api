# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Refuse attachment unless every existing parent index is already built on the candidate."""

import json
from collections import Counter

from db.plan_release_storage import storage_identifier, storage_relation
from process.provider_directory_generic_source_storage import _require

_INDEXES_SQL = """SELECT jsonb_build_object(
    'primary',i.indisprimary,'unique',i.indisunique,'nulls_not_distinct',i.indnullsnotdistinct,
    'keys',i.indkey::text,'operator_classes',i.indclass::text,'collations',i.indcollation::text,
    'options',i.indoption::text,'key_count',i.indnkeyatts,'attribute_count',i.indnatts,
    'immediate',i.indimmediate,'method',am.amname,
    'predicate',pg_get_expr(i.indpred,i.indrelid),'expressions',pg_get_expr(i.indexprs,i.indrelid)) AS signature,
    i.indisvalid AND i.indisready AND i.indislive AS valid
    FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_am am ON am.oid=c.relam
    WHERE i.indrelid=to_regclass($1)"""
_CURRENT_NPI_PREDICATE = (
    "((resource_type)::text = ANY ((ARRAY['Practitioner'::character varying, "
    "'PractitionerRole'::character varying])::text[]))"
)
_LEGACY_NPI_PREDICATE = (
    "((resource_type)::text = ANY (ARRAY[('Practitioner'::character varying)::text, "
    "('PractitionerRole'::character varying)::text]))"
)


async def prepare_rooted_parent_indexes(connection, catalog_schema, candidate_schema, relations):
    """Prebuild only the known rewritten NPI predicate before private candidate freeze."""
    root = "provider_directory_dataset_resource"
    parent_indexes = await connection.fetch(_INDEXES_SQL, storage_relation(catalog_schema, root))
    candidate_indexes = await connection.fetch(_INDEXES_SQL, storage_relation(candidate_schema, relations[root]))
    prepared_signatures = {_signature(index) for index in candidate_indexes}
    for index in parent_indexes:
        if _signature(index) in prepared_signatures:
            continue
        fields_by_name = _signature_fields(index)
        _require(fields_by_name["predicate"] == _LEGACY_NPI_PREDICATE, "rooted_parent_index_unsupported")
        fields_by_name["predicate"] = _CURRENT_NPI_PREDICATE
        _require(
            json.dumps(fields_by_name, sort_keys=True, separators=(",", ":")) in prepared_signatures,
            "rooted_parent_index_unsupported",
        )
        target = storage_relation(candidate_schema, relations[root])
        name = storage_identifier(relations[root] + "_legacy_npi")
        await connection.execute(
            f"CREATE INDEX {name} ON {target} "
            "((payload_json::jsonb->>'npi'),dataset_id,resource_type) WHERE " + _LEGACY_NPI_PREDICATE
        )
    await require_prepared_parent_indexes(connection, catalog_schema, candidate_schema, relations)


async def require_prepared_parent_indexes(connection, catalog_schema, candidate_schema, relations):
    """Keep every index build outside the final parent-lock transaction."""
    for root, name in relations.items():
        parent = await connection.fetch(_INDEXES_SQL, storage_relation(catalog_schema, root))
        candidate = await connection.fetch(_INDEXES_SQL, storage_relation(candidate_schema, name))
        _require(parent and all(row["valid"] for row in (*parent, *candidate)), "rooted_parent_index_changed")
        required_counts = Counter(_signature(row) for row in parent)
        prepared_counts = Counter(_signature(row) for row in candidate)
        _require(not (required_counts - prepared_counts), "rooted_parent_index_unprepared")


def _signature(row):
    return json.dumps(_signature_fields(row), sort_keys=True, separators=(",", ":"))


def _signature_fields(row):
    value = row["signature"]
    if isinstance(value, str):
        value = json.loads(value)
    return dict(value)
