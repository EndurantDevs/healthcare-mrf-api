# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Explicit source-derived ACA tiers and negotiated provider-set catalogue definitions."""

import json
import re
from dataclasses import asdict
from datetime import datetime
from uuid import UUID, uuid5

from db.plan_release_storage import storage_identifier
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_fhir_source_witness_contract import digest

CONTRACT = "registry-native-network-source-v1"
_NAMESPACE = UUID("ac939438-ad63-570b-ad2e-b16279627587")
_COORDINATES = tuple(RegistryNetworkSourceCoordinates.__dataclass_fields__)


def is_native_catalog_source(descriptor):
    """Recognize the explicit native source descriptor family."""
    return type(descriptor) is dict and descriptor.get("contract") == CONTRACT


def native_catalog_descriptor(source):
    """Keep actual native family coordinates and provenance, without a FHIR or approval surrogate."""
    coordinates = RegistryNetworkSourceCoordinates(
        source["source_system"],
        source["source_id"],
        source["schema_name"],
        source["edition_id"],
        CONTRACT,
        source["edition_id"],
    )
    descriptor_by_field = {
        "contract": CONTRACT,
        "profile": "published_normalized",
        "coordinates": asdict(coordinates),
        "complete": True,
        "input": source,
    }
    return validate_native_catalog_descriptor(descriptor_by_field)


def validate_native_catalog_descriptor(descriptor):
    """Require the native family, exact coordinates and definition policy."""
    if (
        not is_native_catalog_source(descriptor)
        or set(descriptor) != {"contract", "profile", "coordinates", "complete", "input"}
        or descriptor["profile"] != "published_normalized"
        or descriptor["complete"] is not True
    ):
        raise ValueError("registry_native_catalog_source_invalid")
    coordinates = RegistryNetworkSourceCoordinates(**descriptor["coordinates"])
    source = descriptor["input"]
    if (
        coordinates.source_system not in {"aca", "ptg"}
        or coordinates.producer_id != CONTRACT
        or type(source) is not dict
        or source.get("contract") != CONTRACT
        or (source.get("source_system"), source.get("source_id"), source.get("schema_name"), source.get("edition_id"))
        != (coordinates.source_system, coordinates.source_id, coordinates.dataset_schema, coordinates.dataset_id)
        or coordinates.edition_id != coordinates.dataset_id
        or source.get("policy") != "native-provider-only-networks-v1"
        or type(source.get("definitions")) is not list
        or not source["definitions"]
    ):
        raise ValueError("registry_native_catalog_source_invalid")
    definitions = native_network_definitions(descriptor)
    if not definitions:
        raise ValueError("registry_native_catalog_source_invalid")
    return descriptor


def native_network_definitions(descriptor):
    """Group ACA plans only by their proved issuer/year/tier; a PTG source graph is one provider set."""
    source_document = descriptor["input"]
    if source_document["source_system"] == "ptg":
        if (
            len(source_document["definitions"]) != 1
            or source_document["definitions"][0].get("source_key") != source_document["source_id"]
        ):
            raise ValueError("registry_native_catalog_ptg_invalid")
        definition = source_document["definitions"][0]
        names = definition.get("network_names")
        if type(names) is not list or any(type(name) is not str for name in names):
            raise ValueError("registry_native_catalog_ptg_invalid")
        display = "Provider set " + source_document["source_id"]
        return {
            "PTGProviderSet/" + source_document["source_id"]: {
                "display_name": display[:512],
                "plans": definition["plans"],
            }
        }
    definitions_by_key, tiers_by_checksum = {}, {}
    for definition in source_document["definitions"]:
        issuer = definition["issuer_id"]
        year = definition["plan_year"]
        tier = definition["network_tier"]
        checksum = definition["checksum_network"]
        if (
            type(issuer) is not str
            or re.fullmatch(r"[1-9][0-9]{4}", issuer) is None
            or type(year) is not int
            or not 2010 <= year <= 2100
            or type(tier) is not str
            or not tier.strip()
            or type(checksum) is not int
            or not -(2**31) <= checksum < 2**31
        ):
            raise ValueError("registry_native_catalog_aca_invalid")
        identity = issuer, year, tier
        previous = tiers_by_checksum.setdefault((issuer, year, checksum), tier)
        if previous != tier:
            raise ValueError("registry_native_catalog_aca_ambiguous")
        key = "ACATier/" + digest(identity)
        label = definition.get("issuer_name") or "Issuer " + issuer
        definition_by_field = definitions_by_key.setdefault(
            key, {"display_name": f"{label} {year} {tier}"[:512], "plans": [], "checksum": checksum}
        )
        if definition_by_field["checksum"] != checksum:
            raise ValueError("registry_native_catalog_aca_ambiguous")
        definition_by_field["plans"].append(definition)
    return definitions_by_key


def _allocation_key(descriptor, source_key):
    coordinates = descriptor["coordinates"]
    return uuid5(_NAMESPACE, json.dumps([coordinates["source_system"], coordinates["source_id"], source_key]))


async def native_network_ids(connection, descriptor, *, control_schema, allocate=False):
    """Resolve enduring logical identities in one set; only the producer allocates missing IDs."""
    definitions = native_network_definitions(descriptor)
    identities_by_key = {key: _allocation_key(descriptor, key) for key in definitions}
    namespace = storage_identifier(control_schema)
    if allocate:
        await connection.execute(
            f"INSERT INTO {namespace}.network_registry_identity(allocation_key) "
            "SELECT key FROM unnest($1::uuid[]) key ORDER BY key ON CONFLICT(allocation_key) DO NOTHING",
            list(identities_by_key.values()),
        )
    rows = await connection.fetch(
        f"SELECT allocation_key,network_id FROM {namespace}.network_registry_identity WHERE allocation_key=ANY($1::uuid[])",
        list(identities_by_key.values()),
    )
    by_key = {row["allocation_key"]: row["network_id"] for row in rows}
    if set(by_key) != set(identities_by_key.values()):
        raise ValueError("registry_native_catalog_identity_unavailable")
    return {key: by_key[value] for key, value in identities_by_key.items()}


def native_catalog_rows(
    descriptor, *, generation, accepted_at, metadata_generation, networks, status="current", manual_network_ids=()
):
    """Format native definitions without asserting PTG plan partitions or office identities."""
    from process.registry_fhir_catalog_projection import FHIRCatalogProjectionBatch

    source_document = descriptor["input"]
    definitions_by_key = native_network_definitions(descriptor)
    network_rows, binding_rows, relationship_rows = [], [], []
    accepted = accepted_at.isoformat().replace("+00:00", "Z") if isinstance(accepted_at, datetime) else accepted_at
    for source_key, definition_by_field in definitions_by_key.items():
        network_id = networks[source_key]
        display = definition_by_field["display_name"]
        if network_id not in manual_network_ids:
            network_rows.append((metadata_generation, network_id, display, "[]", False, 1, None, display.casefold()))
        plans = definition_by_field["plans"] if source_document["source_system"] == "aca" else [None]
        for ordinal, plan in enumerate(plans):
            binding_by_field = _native_binding(descriptor, source_key, plan, generation, accepted, status)
            binding_id = UUID(binding_by_field["binding_id"])
            binding_rows.append(
                (
                    metadata_generation,
                    network_id,
                    binding_id,
                    descriptor["coordinates"]["source_id"],
                    json.dumps(binding_by_field),
                )
            )
            if plan is not None:
                relationship_rows.append(
                    _native_plan_relationship(
                        binding_by_field, plan, ordinal, metadata_generation, network_id, display, source_document
                    )
                )
    return FHIRCatalogProjectionBatch(
        tuple(network_rows), tuple(relationship_rows), tuple(binding_rows), len(relationship_rows)
    )


def _native_binding(descriptor, source_key, plan, generation, accepted, status):
    """Bind one actual native source selector to its published data generation."""
    from process.registry_fhir_catalog_read import fhir_catalog_binding_document

    coordinates = descriptor["coordinates"]
    native_input = descriptor["input"]
    if native_input["source_system"] == "ptg":
        scope_by_field = {
            "kind": "imported_provider_set_v1",
            "source_generation": generation,
            "source_key": native_input["source_id"],
            "snapshot_id": native_input["edition_id"],
            "snapshot_key": native_input["graph"]["snapshot_key"],
        }
    else:
        source_key = "ACAPlan/" + plan["plan_id"] + "/" + str(plan["checksum_network"])
        scope_by_field = {
            "issuer_id": plan["issuer_id"],
            "plan_id": plan["plan_id"],
            "plan_year": plan["plan_year"],
            "state": plan["state"],
            "checksum_network": plan["checksum_network"],
            "source_generation": generation,
        }
    binding_id = uuid5(_NAMESPACE, json.dumps([*(coordinates[name] for name in _COORDINATES), source_key]))
    evidence_by_field = {"source_key": source_key, "source_scope": scope_by_field}
    binding_by_field = {
        "namespace": "source_binding",
        "binding_id": str(binding_id),
        "revision": 1,
        **coordinates,
        **evidence_by_field,
        "evidence_id": generation + ":" + source_key,
        "evidence_sha256": digest(evidence_by_field),
        "metadata_sha256": digest(descriptor),
        "accepted_at": accepted,
        "status": status,
    }
    fhir_catalog_binding_document(binding_by_field)
    return binding_by_field


def _native_plan_relationship(binding, plan, ordinal, metadata_generation, network_id, display, source_document):
    """Expose an ACA plan join only after the source tier was matched to that exact retained plan."""
    from process.registry_aca_company_catalog import aca_company_relationship
    from process.registry_fhir_catalog_read import fhir_catalog_relationship_document

    plan_id, market_type = plan["plan_id"], plan.get("plan_market_type")
    plan_label = plan_id if market_type is None else plan_id + " (" + market_type + ")"
    source_id = binding["source_id"]
    generation = binding["source_scope"]["source_generation"]
    relation_id = digest([generation, binding["source_key"], plan_id, market_type, ordinal])
    company, group, status, reason = aca_company_relationship(source_document, plan)
    relationship_by_field = {
        "relationship_id": relation_id,
        "source_id": source_id,
        "plan": {
            "resource_id": digest([source_id, plan_id, market_type]),
            "scope_path": "/native_plans/" + str(ordinal),
            "display_name": plan_label,
            "valid_from": None,
            "valid_to": None,
        },
        "organization_reference": None,
        "company_role": "insurer" if company is not None else None,
        "company": company,
        "group": group,
        "resolution_status": status,
        "resolution_reason": reason,
    }
    fhir_catalog_relationship_document(relationship_by_field)
    return (
        metadata_generation,
        relation_id,
        network_id,
        source_id,
        generation,
        UUID(binding["binding_id"]),
        UUID(company["company_id"]) if company is not None else None,
        UUID(group["group_id"]) if group is not None else None,
        (display + " " + (company["display_name"] if company else "")).casefold(),
        plan_label.casefold(),
        json.dumps(relationship_by_field),
    )
