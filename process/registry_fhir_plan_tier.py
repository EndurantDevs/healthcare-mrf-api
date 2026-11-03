# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact plan-tier identities from published provider-file evidence."""

import json
import re
from contextlib import contextmanager
from contextvars import ContextVar

from process.uhc_provider_file_identity import UHCSourceFileDescriptor, logical_scope_for_file
from process.uhc_provider_file_semantic_identity import (
    UHCNetworkKey,
    UHCPlanKey,
    validate_network_key_identity,
    validate_plan_key_identity,
)
from process.uhc_provider_file_source_identity import UHC_PROVIDER_FILE_SOURCE_ID

KIND = "uhc_plan_tier_v1"
TRANSFORM_VERSION = "fhir-catalog/plan-tier-v2"
SUPPORTED_TRANSFORMS = ("fhir-catalog/v1", TRANSFORM_VERSION)
_TRANSFORM = ContextVar("fhir_catalog_extraction_transform", default=TRANSFORM_VERSION)
_PLAN_FIELDS = {
    "logical_scope_id",
    "family",
    "market",
    "product",
    "jurisdiction",
    "plan_id_type",
    "plan_id",
    "plan_year",
    "plan_key_id",
}
_TIER_FIELDS = {"kind", "plan_scope", "network_tier", "network_key_id", "plan_resource_id"}
_SHA = re.compile(r"[0-9a-f]{64}\Z")


def _require(condition):
    if not condition:
        raise ValueError("registry_fhir_plan_tier_invalid")


def validate_plan_scope(document):
    """Reconstruct the reviewed census scope and reuse the existing key validators."""
    _require(type(document) is dict and set(document) == _PLAN_FIELDS)
    _require(all(type(document[field]) is str for field in _PLAN_FIELDS - {"jurisdiction", "plan_year"}))
    _require(type(document["plan_year"]) is int)
    product, jurisdiction = document["product"], document["jurisdiction"]
    suffix = jurisdiction + "IEX" if product == "IEX" and type(jurisdiction) is str else product
    scope = logical_scope_for_file(
        UHCSourceFileDescriptor(document["family"], "provider_membership", "JSON_Providers_" + suffix + ".json")
    )
    _require(
        all(
            document[field] == getattr(scope, field)
            for field in ("logical_scope_id", "family", "market", "product", "jurisdiction")
        )
    )
    return validate_plan_key_identity(
        UHCPlanKey(scope, document["plan_id_type"], document["plan_id"], document["plan_year"], document["plan_key_id"])
    )


def validate_plan_tier(document):
    """Validate the complete key and exact normalized InsurancePlan reference."""
    _require(type(document) is dict and set(document) == _TIER_FIELDS and document["kind"] == KIND)
    key = validate_plan_scope(document["plan_scope"])
    _require(type(document["network_tier"]) is str and type(document["network_key_id"]) is str)
    validate_network_key_identity(UHCNetworkKey(key, document["network_tier"], document["network_key_id"]))
    _require(document["plan_resource_id"] == "uhcplan-" + key.plan_key_id[:48])
    return document


def source_key(document):
    """Return the namespaced semantic key without manufacturing an Organization."""
    return "UHCPlanTier/" + validate_plan_tier(document)["network_key_id"]


def validate_binding_scope(scope, producer_id):
    """Keep published observations and genuine witness captures distinct."""
    field = {"registry-fhir-catalog-v1": "source_generation", "registry-source-fhir-witness-v1": "capture_sha256"}.get(
        producer_id
    )
    _require(field is not None and type(scope) is dict and set(scope) == _TIER_FIELDS | {field})
    _require(type(scope[field]) is str and _SHA.fullmatch(scope[field]) is not None)
    return validate_plan_tier({name: scope[name] for name in _TIER_FIELDS})


def supports_plan_tiers(descriptor):
    """This extension reads only the published source, never a legacy adapter."""
    return (
        _TRANSFORM.get() == TRANSFORM_VERSION
        and descriptor["profile"] == "published_normalized"
        and descriptor["coordinates"]["source_id"] == UHC_PROVIDER_FILE_SOURCE_ID
    )


@contextmanager
def extraction_transform(version):
    """Pin the original extractor while independently verifying an older sealed snapshot."""
    _require(version in SUPPORTED_TRANSFORMS)
    token = _TRANSFORM.set(version)
    try:
        yield
    finally:
        _TRANSFORM.reset(token)


def needs_plan_tier_refresh(previous, snapshot, descriptor):
    """Refresh only a current typed source whose prior observations used the old extractor."""
    if previous is None or snapshot["projector_version"] != TRANSFORM_VERSION or not supports_plan_tiers(descriptor):
        return False
    from process.registry_fhir_catalog_sources import fhir_catalog_source_generation

    generation = fhir_catalog_source_generation(descriptor)
    previous_vector = previous["source_vector_json"]
    if type(previous_vector) is str:
        previous_vector = json.loads(previous_vector)
    current_vector = snapshot["source_vector_json"]
    if type(current_vector) is str:
        current_vector = json.loads(current_vector)
    current = next((entry for entry in current_vector if entry["source_generation"] == generation), None)
    prior = next((entry for entry in previous_vector if entry["source_generation"] == generation), None)
    return (
        current is not None
        and current["status"] == "current"
        and prior is not None
        and current.get("extraction_transform") == TRANSFORM_VERSION
        and prior.get("extraction_transform") != TRANSFORM_VERSION
    )


def role_plan_tier(payload):
    """A missing tier stays unresolved; partial or forged supplied identity is refused."""
    if payload.get("network_key_id") is None and payload.get("network_tier") is None:
        return None
    references = payload.get("insurance_plan_refs")
    _require(type(references) is list and len(references) == 1 and type(references[0]) is str)
    _require(references[0].startswith("InsurancePlan/"))
    return validate_plan_tier(
        dict(
            kind=KIND,
            plan_scope=payload.get("plan_scope"),
            network_tier=payload.get("network_tier"),
            network_key_id=payload.get("network_key_id"),
            plan_resource_id=references[0].removeprefix("InsurancePlan/"),
        )
    )
