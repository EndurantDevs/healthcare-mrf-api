# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded immutable COPY rows from source facts and already allocated network IDs."""

from __future__ import annotations

import hashlib
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from uuid import NAMESPACE_URL, UUID, uuid5

from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.registry_fhir_catalog_enrichment import FHIRCatalogEnrichedRelationship
from process.registry_fhir_catalog_extraction import (
    DEFAULT_BYTES,
    MAX_BYTES,
    FHIRCatalogOrganization,
    fhir_catalog_json_bytes,
    fhir_catalog_text,
)

MAX_RECORDS = 1000
RETAINED_PRODUCER = "registry-retained-normalized-fhir-v1"
PUBLISHED_PRODUCER = "registry-fhir-catalog-v1"
NETWORK_COLUMNS = (
    "metadata_generation",
    "network_id",
    "display_name",
    "aliases",
    "archived",
    "revision",
    "benefit_codes",
    "search_name",
)
RELATIONSHIP_COLUMNS = (
    "metadata_generation",
    "relationship_id",
    "network_id",
    "source_id",
    "source_generation",
    "binding_id",
    "company_id",
    "group_id",
    "company_search",
    "plan_search",
    "document_json",
)
BINDING_COLUMNS = ("metadata_generation", "network_id", "binding_id", "source_id", "document_json")


class FHIRCatalogProjectionError(ValueError):
    """Reject incomplete identity evidence or malformed complete COPY batches."""


@dataclass(frozen=True)
class SourceCatalogContext:
    """The caller admits source custody; these fields only format its pinned evidence."""

    coordinates: RegistryNetworkSourceCoordinates
    source_generation: str
    metadata_generation: str
    accepted_at: datetime
    profile: str
    status: str = "current"
    capture_sha256: str | None = None
    legacy_aliases: tuple[tuple[str, str, str], ...] = ()


@dataclass(frozen=True)
class FHIRCatalogProjectionBatch:
    network_rows: tuple[tuple, ...]
    relationship_rows: tuple[tuple, ...]
    binding_rows: tuple[tuple, ...]
    unresolved_relationships: int


def _fail():
    raise FHIRCatalogProjectionError("registry_fhir_catalog_projection_invalid")


def _digest(value):
    if type(value) is not str or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        _fail()
    return value


def _network_id(value):
    if type(value) is not int or not 1 <= value <= 2147483647:
        _fail()
    return value


def _resource_id(value):
    if type(value) is not str or re.fullmatch(r"[A-Za-z0-9.-]{1,64}", value) is None:
        _fail()
    return value


def _uuid(value):
    try:
        parsed = UUID(value) if type(value) is str else None
    except ValueError:
        _fail()
    if parsed is None or not parsed.int or str(parsed) != value:
        _fail()
    return parsed


def _json(value):
    return fhir_catalog_json_bytes(value).decode()


def _validate_context(context):
    if type(context) is not SourceCatalogContext or type(context.coordinates) is not RegistryNetworkSourceCoordinates:
        _fail()
    if context.coordinates.source_system != "fhir" or context.status not in {"current", "last_good"}:
        _fail()
    _digest(context.source_generation)
    _digest(context.metadata_generation)
    if type(context.accepted_at) is not datetime or context.accepted_at.utcoffset() is None:
        _fail()
    if context.profile == "retained_normalized":
        if context.coordinates.producer_id != RETAINED_PRODUCER or context.legacy_aliases:
            _fail()
        _digest(context.capture_sha256)
    elif context.profile == "published_normalized":
        if context.coordinates.producer_id != PUBLISHED_PRODUCER or context.capture_sha256 is not None:
            _fail()
    elif (
        context.profile not in {"raw_fhir", "published_normalized"}
        or context.capture_sha256 is not None
        or context.coordinates.producer_id == RETAINED_PRODUCER
    ):
        _fail()


def _aliases(values):
    if type(values) not in {list, tuple} or len(values) > 100:
        _fail()
    aliases = tuple(fhir_catalog_text(value, 512) for value in values)
    if any(value is None for value in aliases) or len(set(aliases)) != len(aliases):
        _fail()
    return aliases


def _canonical_networks(networks):
    if type(networks) is not tuple or len(networks) > MAX_RECORDS:
        _fail()
    networks_by_id = {}
    for network in networks:
        if type(network) is not dict:
            _fail()
        network_id = _network_id(network.get("network_id"))
        if network_id in networks_by_id:
            _fail()
        if (
            not network.get("display_name")
            or fhir_catalog_text(network["display_name"], 512) != network["display_name"]
        ):
            _fail()
        _aliases(network.get("aliases", []))
        if type(network.get("archived")) is not bool:
            _fail()
        revision = network.get("revision")
        if type(revision) is not int or not 1 <= revision < 2**63:
            _fail()
        networks_by_id[network_id] = network
    return networks_by_id


def _network_map(network_ids, organizations_by_id):
    if type(network_ids) is not tuple or len(network_ids) > MAX_RECORDS:
        _fail()
    ids_by_organization = {}
    for binding in network_ids:
        if type(binding) is not tuple or len(binding) != 2:
            _fail()
        organization_id, network_id = binding
        _resource_id(organization_id)
        _network_id(network_id)
        if organization_id not in organizations_by_id or organization_id in ids_by_organization:
            _fail()
        ids_by_organization[organization_id] = network_id
    return ids_by_organization


def _legacy_alias_map(context):
    if type(context.legacy_aliases) is not tuple or len(context.legacy_aliases) > MAX_RECORDS:
        _fail()
    aliases_by_organization = {}
    for alias in context.legacy_aliases:
        if type(alias) is not tuple or len(alias) != 3:
            _fail()
        resource_id, legacy_uuid, scope = alias
        _resource_id(resource_id)
        _uuid(legacy_uuid)
        if not scope or fhir_catalog_text(scope, 512) != scope or resource_id in aliases_by_organization:
            _fail()
        aliases_by_organization[resource_id] = (legacy_uuid, scope)
    return aliases_by_organization


def _binding_id(context, resource_id):
    key_by_field = {**asdict(context.coordinates), "organization_id": resource_id}
    return uuid5(NAMESPACE_URL, "registry-fhir-catalog/source-binding/v1:" + _json(key_by_field))


def _binding_document(context, organization, aliases_by_organization, metadata_sha256):
    resource_id = organization.resource_id
    scope_by_field = {"organization_id": resource_id}
    if context.profile == "retained_normalized":
        scope_by_field["capture_sha256"] = context.capture_sha256
    elif context.profile == "published_normalized":
        scope_by_field["source_generation"] = context.source_generation
    else:
        alias = aliases_by_organization.get(resource_id)
        if alias is None:
            _fail()
        scope_by_field.update(legacy_uuid=alias[0], alias_scope=alias[1])
    accepted_at = context.accepted_at.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    return {
        "namespace": "source_binding",
        "binding_id": str(_binding_id(context, resource_id)),
        "revision": 1,
        **asdict(context.coordinates),
        "source_key": resource_id,
        "source_scope": scope_by_field,
        "evidence_id": f"{context.source_generation}:Organization/{resource_id}",
        "evidence_sha256": _digest(organization.source_hash),
        "metadata_sha256": metadata_sha256,
        "accepted_at": accepted_at,
        "status": context.status,
    }


def _network_row(context, network_id, observations, canonical_by_field):
    if canonical_by_field is None:
        observation = next((item for item in observations if item.name is not None), observations[0])
        canonical_by_field = {
            "display_name": observation.name or "Organization/" + observation.resource_id,
            "aliases": observation.aliases,
            "archived": False,
            "revision": 1,
        }
    name = canonical_by_field["display_name"]
    aliases = _aliases(canonical_by_field.get("aliases", []))
    codes = canonical_by_field.get("benefit_codes")
    if codes is not None and (
        type(codes) not in {list, tuple}
        or not codes
        or list(codes) != [code for code in ("medical", "dental", "vision") if code in codes]
    ):
        _fail()
    return (
        context.metadata_generation,
        network_id,
        name,
        _json(aliases),
        canonical_by_field["archived"],
        canonical_by_field["revision"],
        codes,
        "\n".join((name, *aliases)).lower(),
    )


def _relationship_rows(context, relationships, ids_by_organization):
    relationship_rows, seen_by_id, unresolved = [], {}, 0
    for relationship in relationships:
        if type(relationship) is not FHIRCatalogEnrichedRelationship:
            _fail()
        document = relationship.document
        if type(document) is not dict or document.get("source_id") != context.coordinates.source_id:
            _fail()
        relationship_id = _digest(document.get("relationship_id"))
        if relationship_id in seen_by_id:
            if seen_by_id[relationship_id] != relationship:
                _fail()
            continue
        seen_by_id[relationship_id] = relationship
        network_id = ids_by_organization.get(relationship.network_resource_id)
        if document.get("resolution_status") != "resolved" or network_id is None:
            unresolved += 1
        if network_id is None:
            continue
        company, group = document.get("company"), document.get("group")
        company_id = _uuid(company["company_id"]) if company is not None else None
        group_id = _uuid(group["group_id"]) if group is not None and group.get("group_id") is not None else None
        encoded = _json(document)
        if (
            len(encoded.encode()) > 32768
            or type(relationship.company_search) is not str
            or type(relationship.plan_search) is not str
        ):
            _fail()
        relationship_rows.append(
            (
                context.metadata_generation,
                relationship_id,
                network_id,
                context.coordinates.source_id,
                context.source_generation,
                _binding_id(context, relationship.network_resource_id),
                company_id,
                group_id,
                relationship.company_search,
                relationship.plan_search,
                encoded,
            )
        )
    return tuple(sorted(relationship_rows, key=lambda row: row[1])), unresolved


def _metadata_hash(context, organization, network_id):
    observation = asdict(organization)
    observation.pop("source_hash")
    # The admitted generation binds every plan edge, independently of COPY page boundaries.
    return hashlib.sha256(
        fhir_catalog_json_bytes((context.source_generation, context.profile, network_id, observation))
    ).hexdigest()


def _output_size(row_groups):
    total = 2
    for rows in row_groups:
        for row in rows:
            total += (
                len(fhir_catalog_json_bytes(tuple(str(value) if type(value) is UUID else value for value in row))) + 1
            )
    return total


def format_fhir_catalog_projection(
    organizations, relationships, network_ids, context, *, canonical_networks=(), max_output_bytes=DEFAULT_BYTES
):
    """Format COPY data only; never allocate IDs, publish, archive or rewrite manual heads."""
    _validate_context(context)
    if (
        type(organizations) is not tuple
        or type(relationships) is not tuple
        or max(len(organizations), len(relationships)) > MAX_RECORDS
    ):
        _fail()
    if type(max_output_bytes) is not int or not 1 <= max_output_bytes <= MAX_BYTES:
        _fail()
    if any(type(organization) is not FHIRCatalogOrganization for organization in organizations):
        _fail()
    if any(type(relationship) is not FHIRCatalogEnrichedRelationship for relationship in relationships):
        _fail()
    input_documents = (
        [asdict(organization) for organization in organizations],
        [asdict(relationship) for relationship in relationships],
        canonical_networks,
    )
    if len(fhir_catalog_json_bytes(input_documents)) > MAX_BYTES:
        _fail()
    organizations_by_id = {organization.resource_id: organization for organization in organizations}
    if len(organizations_by_id) != len(organizations):
        _fail()
    ids_by_organization = _network_map(network_ids, organizations_by_id)
    canonical_by_id = _canonical_networks(canonical_networks)
    aliases_by_organization = _legacy_alias_map(context)
    relationship_rows, unresolved = _relationship_rows(context, relationships, ids_by_organization)
    observations_by_network, binding_rows = {}, []
    for resource_id, network_id in sorted(ids_by_organization.items()):
        organization = organizations_by_id[resource_id]
        observations_by_network.setdefault(network_id, []).append(organization)
        document = _binding_document(
            context, organization, aliases_by_organization, _metadata_hash(context, organization, network_id)
        )
        binding_rows.append(
            (
                context.metadata_generation,
                network_id,
                _binding_id(context, resource_id),
                context.coordinates.source_id,
                _json(document),
            )
        )
    network_rows = tuple(
        _network_row(context, network_id, observations, canonical_by_id.get(network_id))
        for network_id, observations in sorted(observations_by_network.items())
    )
    if _output_size((network_rows, relationship_rows, binding_rows)) > max_output_bytes:
        _fail()
    return FHIRCatalogProjectionBatch(network_rows, relationship_rows, tuple(binding_rows), unresolved)
