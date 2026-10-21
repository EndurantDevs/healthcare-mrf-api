# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Resolve source custody without caller-selected schemas or replacement roots."""

from sqlalchemy import text

from api.ptg2_tables import snapshot_serving_tables
from api.ptg2_v4_graph import load_v4_graph_root
from api.ptg_membership_contract import identity, physical_identity
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from process.ptg_parts.ptg2_v4_finalizer_maps import has_complete_v4_finalizer_map


async def source_identity(session, source_key, snapshot_id, code_system, code, *, tables=None):
    from api import ptg2_serving as serving

    expected = tables
    tables = await snapshot_serving_tables(session, snapshot_id)
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
    root = await load_v4_graph_root(session, key, schema_name=schema, refresh=True)
    packed = await has_complete_v4_finalizer_map(session, schema_name=schema, snapshot_key=key)
    finalizer = {
        "state": "legacy_relational",
        "map_digest": None,
        "canonical_mapping_digest": None,
        "target_identity_digest": None,
    }
    if packed:
        row = (
            (
                await session.execute(
                    text(
                        f'SELECT map_digest,canonical_mapping_digest,target_identity_digest FROM "{schema}".ptg2_v4_finalizer_map_root WHERE snapshot_key=:key'
                    ),
                    {"key": key},
                )
            )
            .mappings()
            .one()
        )
        finalizer = {"state": "packed", **{name: bytes(value).hex() for name, value in row.items()}}
    document = identity(tables, root, finalizer, code_system, code)
    required = {
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
    optional = {"ptg2_v4_finalizer_map_root", "ptg2_v4_finalizer_map_pack", "ptg2_v4_finalizer_map_target"}
    rows = (
        (
            await session.execute(
                text(
                    "SELECT d.oid AS database_oid,n.oid AS schema_oid,c.relname,c.oid AS relation_oid "
                    "FROM pg_namespace n JOIN pg_class c ON c.relnamespace=n.oid "
                    "CROSS JOIN pg_database d WHERE n.nspname=:schema AND d.datname=current_database() "
                    "AND c.relkind IN ('r','p') AND c.relname=ANY(CAST(:names AS text[])) ORDER BY c.relname"
                ),
                {"schema": schema, "names": sorted(required | optional)},
            )
        )
        .mappings()
        .all()
    )
    if not rows or not required.issubset({row["relname"] for row in rows}):
        raise PTG2ManifestArtifactError("membership source relation inventory is incomplete")
    attestation_digest = await session.scalar(
        text(
            f"SELECT encode(attestation_digest,'hex') FROM \"{serving.PTG2_SCHEMA}\".ptg2_v3_candidate_audit_attestation "
            "WHERE snapshot_id=:snapshot_id AND activated_at IS NOT NULL"
        ),
        {"snapshot_id": snapshot_id},
    )
    if not isinstance(attestation_digest, str) or len(attestation_digest) != 64:
        raise PTG2ManifestArtifactError("membership source audit attestation is unavailable")
    document["source_storage"] = {
        "database_oid": int(rows[0]["database_oid"]),
        "schema_oid": int(rows[0]["schema_oid"]),
        "relation_oids": [[row["relname"], int(row["relation_oid"])] for row in rows],
        "attestation_digest": attestation_digest,
    }
    return tables, root, document
