# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Resolve source custody without caller-selected schemas or replacement roots."""

from sqlalchemy import text

from api.ptg2_code_scope import source_read_relation
from api.ptg2_tables import snapshot_serving_tables
from api.ptg2_v4_graph import load_v4_graph_root
from api.ptg_membership_contract import identity, physical_identity
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from process.ptg_parts.ptg2_v4_finalizer_maps import has_complete_v4_finalizer_map


def _require_source_custody(tables, expected, source_key, snapshot_id):
    from api import ptg2_serving as serving

    serving._require_strict_shared_v3(tables)
    if tables.source_key != source_key or tables.snapshot_id != snapshot_id or not tables.uses_v4_graph:
        raise ValueError("membership index source snapshot is not an audited V4 serving source")
    key, schema = serving._required_shared_snapshot_key(tables), serving._payload_schema(tables)
    if expected is not None and (
        physical_identity(expected) != physical_identity(tables)
        or serving._required_shared_snapshot_key(expected) != key
        or serving._payload_schema(expected) != schema
    ):
        raise PTG2ManifestArtifactError("membership source physical custody changed")
    return key, schema


async def _finalizer_identity(session, schema, key):
    packed = await has_complete_v4_finalizer_map(session, schema_name=schema, snapshot_key=key)
    finalizer_by_field = {
        "state": "legacy_relational",
        "map_digest": None,
        "canonical_mapping_digest": None,
        "target_identity_digest": None,
    }
    if packed:
        root_relation = source_read_relation(
            session, schema, "ptg2_v4_finalizer_map_root", default=f'"{schema}".ptg2_v4_finalizer_map_root'
        )
        finalizer_row = (
            (
                await session.execute(
                    text(
                        f"SELECT map_digest,canonical_mapping_digest,target_identity_digest FROM {root_relation} WHERE snapshot_key=:key"
                    ),
                    {"key": key},
                )
            )
            .mappings()
            .one()
        )
        finalizer_by_field = {"state": "packed", **{name: bytes(value).hex() for name, value in finalizer_row.items()}}
    return finalizer_by_field


async def _source_storage(session, schema, snapshot_id):
    from api import ptg2_serving as serving

    required_relations = {
        "ptg2_v4_snapshot_map_root",
        "ptg2_v4_relation_manifest",
        "ptg2_v4_snapshot_map_pack",
        "ptg2_v3_block",
        "ptg2_v3_snapshot_block",
        "ptg2_v4_npi_scope",
        "ptg2_v3_code",
        "ptg2_v3_provider_set",
        "ptg2_v3_provider_group",
    }
    optional_relations = {"ptg2_v4_finalizer_map_root", "ptg2_v4_finalizer_map_pack", "ptg2_v4_finalizer_map_target"}
    storage_rows = (
        (
            await session.execute(
                text(
                    "SELECT d.oid AS database_oid,n.oid AS schema_oid,c.relname,c.oid AS relation_oid "
                    "FROM pg_namespace n JOIN pg_class c ON c.relnamespace=n.oid "
                    "CROSS JOIN pg_database d WHERE n.nspname=:schema AND d.datname=current_database() "
                    "AND c.relkind IN ('r','p') AND c.relname=ANY(CAST(:names AS text[])) ORDER BY c.relname"
                ),
                {"schema": schema, "names": sorted(required_relations | optional_relations)},
            )
        )
        .mappings()
        .all()
    )
    if not storage_rows or not required_relations.issubset({storage_row["relname"] for storage_row in storage_rows}):
        raise PTG2ManifestArtifactError("membership source relation inventory is incomplete")
    attestation_relation = source_read_relation(
        session,
        serving.PTG2_SCHEMA,
        "ptg2_v3_candidate_audit_attestation",
        default=f'"{serving.PTG2_SCHEMA}".ptg2_v3_candidate_audit_attestation',
    )
    attestation_digest = await session.scalar(
        text(
            f"SELECT encode(attestation_digest,'hex') FROM {attestation_relation} "
            "WHERE snapshot_id=:snapshot_id AND activated_at IS NOT NULL"
        ),
        {"snapshot_id": snapshot_id},
    )
    if not isinstance(attestation_digest, str) or len(attestation_digest) != 64:
        raise PTG2ManifestArtifactError("membership source audit attestation is unavailable")
    return {
        "database_oid": int(storage_rows[0]["database_oid"]),
        "schema_oid": int(storage_rows[0]["schema_oid"]),
        "relation_oids": [[storage_row["relname"], int(storage_row["relation_oid"])] for storage_row in storage_rows],
        "attestation_digest": attestation_digest,
    }


async def source_identity(session, source_key, snapshot_id, code_system, code, *, tables=None, api_read=False):
    """Resolve fresh audited source custody and its immutable physical/code identity."""
    expected = tables
    if type(api_read) is not bool:
        raise ValueError("membership Source API mode is invalid")
    tables = await snapshot_serving_tables(session, snapshot_id, **({"api_read": True} if api_read else {}))
    key, schema = _require_source_custody(tables, expected, source_key, snapshot_id)
    root = await load_v4_graph_root(session, key, schema_name=schema, refresh=True)
    finalizer_by_field = await _finalizer_identity(session, schema, key)
    document = identity(tables, root, finalizer_by_field, code_system, code)
    document["source_storage"] = await _source_storage(session, schema, snapshot_id)
    return tables, root, document
