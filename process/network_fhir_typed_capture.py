# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named immutable graph editions and independent Source data validation."""

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime
from types import SimpleNamespace
from uuid import UUID

from process.network_address_projection import _identifier
from process.network_fhir_source_witness_contract import canonical, canonical_timestamp, digest, source_origin
from process.network_fhir_typed_contract import (
    ADDRESS_FIELDS,
    CELL_COLUMNS,
    PAIR_COLUMNS,
    require,
    validate_graph_document,
)
from process.network_fhir_typed_source import (
    _DDL,
    bind_typed_graph_stage,
    produce_typed_source_graph,
    verify_typed_source_graph,
)
from process.provider_directory_generic_source_provision import freeze_fhir_source_snapshot
from process.uhc_flex_practitioner_async_safety import drain_operation

PREFIX = "registry_fhir_typed_"
COMPONENT = "registry_fhir_typed_capture"
COMMENT = COMPONENT + ":5:"
LEGACY_COMMENT = COMPONENT + ":4:"
TABLES = {"cells": "source_cells", "addresses": "source_addresses", "pairs": "source_pairs"}
COLUMNS = {
    "cells": CELL_COLUMNS,
    "addresses": ("raw_key", *ADDRESS_FIELDS, "address_key", "canonical_identity"),
    "pairs": PAIR_COLUMNS,
}
KEYS = {
    "cells": ("resource_type", "resource_id"),
    "addresses": ("raw_key",),
    "pairs": ("resource_type", "resource_id", "network_source_key", "location_resource_id"),
}


@dataclass(frozen=True)
class TypedGraphAnchor:
    """Logical operation and observation coordinates for one graph edition."""

    capture_id: str
    observed_at: str

    def __post_init__(self):
        require(
            type(self.capture_id) is str and str(UUID(self.capture_id)) == self.capture_id and UUID(self.capture_id).int
        )
        require(canonical_timestamp(self.observed_at) == self.observed_at)


def _typed_capture_document(capture_by_field):
    """Decode the supported closed wire shapes and validate their named graph tables."""
    fields = {
        "component",
        "descriptor_version",
        "capture_id",
        "observed_at",
        "schema_name",
        "tables",
        "content",
        "graph",
        "catalog_descriptor",
        "capture_sha256",
    }
    legacy_fields = (fields - {"tables"}) | {"owner_role", "runtime_roles", "catalog"}
    require(type(capture_by_field) is dict, "capture_document")
    if set(capture_by_field) == legacy_fields and capture_by_field["descriptor_version"] == 4:
        capture_by_field = {key: capture_by_field[key] for key in fields - {"tables"}}
        capture_by_field["tables"] = {
            kind: {"schema": capture_by_field["schema_name"], "name": name} for kind, name in TABLES.items()
        }
        capture_by_field["graph"] = validate_graph_document(capture_by_field["graph"])
    allowed_fields = (
        (fields, fields | {"catalog_generation"}) if capture_by_field.get("descriptor_version") == 4 else (fields,)
    )
    require(set(capture_by_field) in allowed_fields, "capture_document")
    require(
        capture_by_field["component"] == COMPONENT
        and type(capture_by_field["descriptor_version"]) is int
        and capture_by_field["descriptor_version"] in {4, 5},
        "capture_document",
    )
    TypedGraphAnchor(capture_by_field["capture_id"], capture_by_field["observed_at"])
    require(capture_by_field["schema_name"] == PREFIX + UUID(capture_by_field["capture_id"]).hex, "capture_schema")
    tables_by_kind = {kind: {"schema": capture_by_field["schema_name"], "name": name} for kind, name in TABLES.items()}
    require(capture_by_field["tables"] == tables_by_kind, "capture_tables")
    return capture_by_field


def validate_typed_capture(document):
    """Validate named relations, source scope and complete graph accounting."""
    capture_by_field = _typed_capture_document(document)
    graph = validate_graph_document(capture_by_field["graph"])
    from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
    from process.network_fhir_typed_mapping import require_catalog_graph_scope
    from process.registry_fhir_catalog_sources import (
        fhir_catalog_source_digest,
        validate_fhir_catalog_source_descriptor,
    )

    stored_catalog = capture_by_field["catalog_descriptor"]
    descriptor = validate_fhir_catalog_source_descriptor(capture_by_field["catalog_descriptor"])
    capture_by_field = {**capture_by_field, "catalog_descriptor": descriptor}
    if capture_by_field["descriptor_version"] == 4:
        generation = (
            capture_by_field["catalog_generation"]
            if "catalog_generation" in capture_by_field
            else fhir_catalog_source_digest(stored_catalog)
        )
        require(type(generation) is str and re.fullmatch(r"[0-9a-f]{64}", generation), "capture_catalog")
        capture_by_field.update(graph=graph, catalog_generation=generation)
    require_catalog_graph_scope(
        SimpleNamespace(document=capture_by_field["graph"]),
        RegistryNetworkSourceCoordinates(**descriptor["coordinates"]),
        descriptor,
    )
    require(
        type(capture_by_field["content"]) is dict and set(capture_by_field["content"]) == set(TABLES), "capture_content"
    )
    for content in capture_by_field["content"].values():
        require(type(content) is dict and set(content) == {"rows", "sha256"}, "capture_content")
        require(type(content["rows"]) is int and 0 <= content["rows"] < 2**63, "capture_content")
        require(type(content["sha256"]) is str and re.fullmatch(r"[0-9a-f]{64}", content["sha256"]), "capture_content")
    require(
        capture_by_field["content"]["cells"]["rows"] == capture_by_field["graph"]["counts"]["projected_rows"]
        and capture_by_field["content"]["pairs"]["rows"] == capture_by_field["graph"]["counts"]["pair_rows"],
        "capture_counts",
    )
    origin = source_origin(capture_by_field["graph"]["origin"])
    require(
        capture_by_field["graph"]["membership_as_of"]
        == (
            origin.semantic_projection_as_of
            or datetime.fromisoformat(capture_by_field["observed_at"]).date().isoformat()
        ),
        "membership_date",
    )
    require(
        type(capture_by_field["capture_sha256"]) is str
        and re.fullmatch(r"[0-9a-f]{64}", capture_by_field["capture_sha256"]),
        "capture_content",
    )
    if capture_by_field["descriptor_version"] == 5:
        require(
            capture_by_field["capture_sha256"]
            == digest({key: field_value for key, field_value in capture_by_field.items() if key != "capture_sha256"}),
            "capture_content",
        )
    return capture_by_field


def typed_capture_catalog_generation(document):
    """Keep a retained catalog content key through interpretation of historical editions."""
    from process.registry_fhir_catalog_sources import fhir_catalog_source_generation

    if document["descriptor_version"] == 4:
        generation = document["catalog_generation"]
        require(type(generation) is str and re.fullmatch(r"[0-9a-f]{64}", generation), "capture_catalog")
        return generation
    return fhir_catalog_source_generation(document["catalog_descriptor"])


async def _content(connection, stage):
    """Hash ordered native binary packets without per-edge Python objects or JSON witnesses."""
    content_by_kind = {}
    for kind, keys in KEYS.items():
        folded = hashlib.sha256((COMPONENT + ":binary-v4:" + kind + "\0").encode())
        query = "SELECT " + ",".join(COLUMNS[kind]) + " FROM " + stage.relation(kind) + " ORDER BY " + ",".join(keys)

        async def consume(packet):
            """Fold each ordered binary COPY packet into the table digest."""
            folded.update(packet)

        copied = await drain_operation(
            connection.copy_from_query(query, output=consume, format="binary"), preserve_cancellation=True
        )
        content_by_kind[kind] = {"rows": int(copied.removeprefix("COPY ")), "sha256": folded.hexdigest()}
    return content_by_kind


@dataclass(frozen=True)
class TypedGraphCaptureOptions:
    """Existing administrator configuration used to freeze one completed generation."""

    expected_anchor: TypedGraphAnchor
    owner_role: str
    runtime_roles: tuple


async def _create_typed_capture_stage(writer, schema):
    """Create the graph tables and address index inside the caller's transaction."""
    await writer.execute("CREATE SCHEMA " + _identifier(schema))
    for kind, name in TABLES.items():
        await writer.execute("CREATE TABLE " + _identifier(schema) + "." + _identifier(name) + "(" + _DDL[kind] + ")")
    await writer.execute(
        "CREATE INDEX ON " + _identifier(schema) + "." + _identifier(TABLES["addresses"]) + "(address_key)"
    )
    return await bind_typed_graph_stage(writer, schema_name=schema, table_names=TABLES)


async def capture_typed_source_graph(
    source_export, writer, *, source_profile, reviewed_origin, membership_as_of, capture_options, catalog_descriptor
):
    """Build and freeze a graph using the existing administrator preparation connection."""
    anchor = capture_options.expected_anchor
    require(type(anchor) is TypedGraphAnchor, "anchor")
    anchor.__post_init__()
    owner, readers = capture_options.owner_role, capture_options.runtime_roles
    _identifier(owner)
    require(type(readers) is tuple and bool(readers) and tuple(sorted(set(readers))) == readers, "capture_roles")
    for role in readers:
        _identifier(role)
    schema = PREFIX + UUID(anchor.capture_id).hex
    async with writer.transaction():
        stage = await _create_typed_capture_stage(writer, schema)
        graph = await produce_typed_source_graph(
            source_export,
            writer,
            stage,
            source_profile=source_profile,
            reviewed_origin=reviewed_origin,
            membership_as_of=membership_as_of,
        )
        await freeze_fhir_source_snapshot(
            writer, schema, namespace_owner=owner, reader_roles=readers, application_roles=readers
        )
        await verify_typed_source_graph(
            source_export,
            writer,
            stage,
            source_profile=source_profile,
            reviewed_origin=reviewed_origin,
            membership_as_of=membership_as_of,
        )
        capture_by_field = {
            "component": COMPONENT,
            "descriptor_version": 5,
            "capture_id": anchor.capture_id,
            "observed_at": anchor.observed_at,
            "schema_name": schema,
            "tables": {kind: {"schema": schema, "name": name} for kind, name in TABLES.items()},
            "content": await _content(writer, stage),
            "graph": graph,
            "catalog_descriptor": catalog_descriptor,
        }
        capture_by_field["capture_sha256"] = digest(capture_by_field)
        validate_typed_capture(capture_by_field)
        await writer.execute(
            "COMMENT ON SCHEMA "
            + _identifier(schema)
            + " IS '"
            + (COMMENT + canonical(capture_by_field)).replace("'", "''")
            + "'"
        )
        stage.active = False
        return capture_by_field


async def require_typed_graph_capture(connection, document, *, expected_anchor):
    """Resolve the selected logical edition through its qualified relation names."""
    require(connection.is_in_transaction(), "transaction_required")
    document = validate_typed_capture(document)
    require(
        type(expected_anchor) is TypedGraphAnchor
        and (document["capture_id"], document["observed_at"])
        == (expected_anchor.capture_id, expected_anchor.observed_at),
        "anchor",
    )
    return await bind_typed_graph_stage(
        connection, schema_name=document["schema_name"], table_names=TABLES, document=document["graph"]
    )


async def verify_typed_graph_capture(
    source_export, connection, document, *, source_profile, reviewed_origin, expected_anchor, replay_stage=None
):
    """Independently rederive the graph and compare complete ordered data hashes."""
    document = validate_typed_capture(document)
    stage = await require_typed_graph_capture(connection, document, expected_anchor=expected_anchor)
    await verify_typed_source_graph(
        source_export,
        connection,
        stage,
        source_profile=source_profile,
        reviewed_origin=reviewed_origin,
        membership_as_of=document["graph"]["membership_as_of"],
        replay_stage=replay_stage,
    )
    require(await _content(connection, stage) == document["content"], "content_changed")
    stage.require_ready()
    return stage


async def resolve_typed_graph_capture_reference(connection, *, expected_anchor, capture_sha256, catalog_generation):
    """Load bounded edition metadata stored with the schema-qualified snapshot."""
    require(connection.is_in_transaction(), "transaction_required")
    require(type(expected_anchor) is TypedGraphAnchor, "anchor")
    expected_anchor.__post_init__()
    require(
        all(
            type(value) is str and re.fullmatch(r"[0-9a-f]{64}", value)
            for value in (capture_sha256, catalog_generation)
        ),
        "reference_hash",
    )
    schema = PREFIX + UUID(expected_anchor.capture_id).hex
    metadata = await connection.fetchval(
        "SELECT CASE WHEN octet_length(obj_description(n.oid,'pg_namespace')) <= 262144 "
        "THEN obj_description(n.oid,'pg_namespace') END FROM pg_namespace n WHERE n.nspname=$1",
        schema,
    )
    require(type(metadata) is str and metadata.startswith((COMMENT, LEGACY_COMMENT)), "reference_metadata")
    prefix = COMMENT if metadata.startswith(COMMENT) else LEGACY_COMMENT
    try:
        document = validate_typed_capture(json.loads(metadata[len(prefix) :]))
    except (TypeError, ValueError, KeyError) as error:
        raise ValueError("registry_fhir_typed_reference_invalid") from error
    require(document["capture_sha256"] == capture_sha256, "reference_context")
    require(typed_capture_catalog_generation(document) == catalog_generation, "reference_catalog")
    await require_typed_graph_capture(connection, document, expected_anchor=expected_anchor)
    return document


@dataclass(frozen=True)
class TypedGraphRead:
    """Named graph data and the logical edition selected by the caller."""

    stage: object
    capture: dict
    anchor: TypedGraphAnchor

    @property
    def document(self):
        """Return the validated graph document selected by this read."""
        return self.stage.document

    def relation(self, kind):
        """Return the selected graph table's schema-qualified name."""
        return self.stage.relation(kind)

    def require_ready(self, *, complete=True):
        """Require an active graph stage whose data matches the captured edition."""
        self.stage.require_ready(complete=False)
        validate_typed_capture(self.capture)
        require(self.document == self.capture["graph"], "capture_graph")


async def load_typed_graph_capture(connection, document, *, expected_anchor):
    """Read a completed edition and validate its content before network preparation."""
    document = validate_typed_capture(document)
    stage = await require_typed_graph_capture(connection, document, expected_anchor=expected_anchor)
    require(await _content(connection, stage) == document["content"], "content_changed")
    return TypedGraphRead(stage, document, expected_anchor)
