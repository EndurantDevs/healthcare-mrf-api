# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named NPI publication provenance for direct custom provider references."""

from dataclasses import asdict
from uuid import UUID

from process import network_address_artifact as catalog
from process import npi_result_generation as npi
from process.network_address_projection import PinnedAddressSource, _identifier


class NetworkNPIArtifactError(ValueError):
    """The logical NPI edition, source scope or required key index differs."""


def _require(condition, reason):
    if not condition:
        raise NetworkNPIArtifactError("network_npi_artifact_" + reason)


async def _indexed_npi(connection, relation):
    valid = await connection.fetchval(
        "SELECT EXISTS(SELECT FROM pg_attribute a JOIN pg_index i ON i.indrelid=a.attrelid "
        "JOIN pg_class idx ON idx.oid=i.indexrelid JOIN pg_am am ON am.oid=idx.relam "
        "WHERE a.attrelid=to_regclass($1) AND a.attname='npi' AND format_type(a.atttypid,a.atttypmod)='bigint' AND a.attnotnull "
        "AND i.indisunique AND i.indisvalid AND i.indisready AND i.indislive AND i.indnkeyatts=1 "
        "AND i.indkey[0]=a.attnum AND i.indpred IS NULL AND i.indexprs IS NULL AND am.amname='btree')",
        relation,
    )
    _require(valid is True, "indexed_npi_required")


async def _publication(connection, schema):
    record_by_field = await connection.fetchrow(
        "SELECT singleton,local_lineage_id,local_generation,origin_lineage_id,origin_generation,published_at,relation_oids,"
        "canonical_publication_ref,canonical_publication_generation,canonical_chain_ref,canonical_import_date "
        f"FROM {_identifier(schema)}.{npi.TABLE_NAME} WHERE singleton IS TRUE"
    )
    _require(record_by_field is not None, "generation_unavailable")
    lineage, local = str(record_by_field["local_lineage_id"]), record_by_field["local_generation"]
    _require(
        str(UUID(lineage)) == lineage and UUID(lineage).int and type(local) is int and 0 < local < 2**63,
        "generation_unavailable",
    )
    serving = npi.validate_npi_serving_generation(
        {name: record_by_field[name] for name in ("origin_lineage_id", "origin_generation", "published_at")}
    ).as_dict()
    provenance_fields = (
        "canonical_publication_ref",
        "canonical_publication_generation",
        "canonical_chain_ref",
        "canonical_import_date",
    )
    provenance_values = tuple(record_by_field[name] for name in provenance_fields)
    provenance = None
    if any(provenance_value is not None for provenance_value in provenance_values):
        _require(all(provenance_value is not None for provenance_value in provenance_values), "generation_unavailable")
        provenance = npi.validate_npi_canonical_provenance(
            dict(
                zip(
                    ("publication_ref", "publication_generation", "chain_ref", "import_date"),
                    provenance_values,
                    strict=True,
                )
            )
        ).as_dict()
    publication_by_field = {
        "local_lineage_id": lineage,
        "local_generation": local,
        "serving_generation": serving,
        "canonical_provenance": provenance,
    }
    legacy_by_field = {**publication_by_field, "relation_oids": record_by_field["relation_oids"]}
    return catalog._digest(publication_by_field), catalog._digest(legacy_by_field)


async def capture_network_npi_artifact(connection, schema_name, *, runtime_roles=(), generation_id=None):
    """Read one logical publication and require its supported named NPI key paths."""
    _identifier(schema_name)
    await catalog._require_capture_snapshot(connection)
    logical, legacy = await _publication(connection, schema_name)
    selected = logical if generation_id is None else generation_id
    _require(selected in {logical, legacy}, "source_changed")
    for name in (*npi.RELATION_NAMES, npi.TABLE_NAME):
        await connection.fetch(f"SELECT * FROM {_identifier(schema_name)}.{_identifier(name)} LIMIT 0")
    await _indexed_npi(connection, _identifier(schema_name) + ".npi")
    source = PinnedAddressSource(schema_name, "npi", selected)
    return source, asdict(source)


async def verify_network_npi_artifact(connection, pinned_source, identity, *, runtime_roles=()):
    """Revalidate the same logical source before independent office-row comparison."""
    _require(type(pinned_source) is PinnedAddressSource and pinned_source.table_name == "npi", "source_invalid")
    if type(identity) is tuple and len(identity) == 4:
        identity = asdict(pinned_source)
    _require(type(identity) is dict and identity == asdict(pinned_source), "identity_invalid")
    current, current_identity = await capture_network_npi_artifact(
        connection, pinned_source.schema_name, runtime_roles=runtime_roles, generation_id=pinned_source.generation_id
    )
    _require((current, current_identity) == (pinned_source, identity), "source_changed")
    return current_identity
