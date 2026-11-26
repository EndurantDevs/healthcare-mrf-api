# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reuse logical Source editions without changing their retained recipe identity."""

import json
from dataclasses import dataclass
from uuid import UUID, uuid5

from process.network_fhir_source_witness_contract import digest
from process.network_fhir_typed_capture import COMPONENT, PREFIX, TypedGraphAnchor, validate_typed_capture
from process.network_fhir_typed_contract import (
    CONTRACT,
    DERIVATION_VERSION,
    PLAN_TIER_CONTRACT,
    PLAN_TIER_NETWORK_POLICY,
    POLICY,
    policy_date,
)
from process.network_fhir_typed_recipe import PinnedTypedFHIRMembershipSource
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_source_recipe_store import resolve_retained_registry_source_recipes
from process.uhc_provider_file_source_identity import UHC_PROVIDER_FILE_SOURCE_ID

_CAPTURE_NAMESPACE = UUID("cfb2cdb4-02d5-550d-90f1-17898e427f87")
_MAX_METADATA_BYTES = 262144


@dataclass(frozen=True)
class ImportedSourcePreparation:
    """Distinguish created and reused completed caches without granting deletion authority."""

    recipes: tuple
    created_schemas: tuple[str, ...]
    reused_schemas: tuple[str, ...]


def source_edition(descriptor):
    """Retain logical admission and named data scope, excluding physical catalog observations."""
    logical_by_field = {
        name: descriptor[name]
        for name in (
            "coordinates",
            "profile",
            "endpoint_id",
            "dataset_sha256",
            "source_uri",
            "source_aliases",
            "accepted_receipt_id",
            "accepted_receipt_sha256",
            "complete",
            "previous_dataset_id",
            "admission",
        )
    }
    logical_by_field["tables"] = descriptor["tables"]
    native = descriptor["native_read_profile"]
    logical_by_field["catalog_schema"] = (
        descriptor.get("catalog_schema") or native.get("catalog_schema_name") or native["schema_name"]
    )
    return logical_by_field


def graph_policy(source_id):
    """A new semantic derivation must choose a new named preparation."""
    is_plan_tier_graph = source_id == UHC_PROVIDER_FILE_SOURCE_ID
    return {
        "contract": PLAN_TIER_CONTRACT if is_plan_tier_graph else CONTRACT,
        "derivation_version": 4 if is_plan_tier_graph else DERIVATION_VERSION,
        "office_policy": POLICY,
        "canonicalizer_version": 2,
        **({"network_policy": PLAN_TIER_NETWORK_POLICY} if is_plan_tier_graph else {}),
    }


def imported_source_capture_id(descriptor, membership_as_of):
    """Name one immutable input edition independently of a request or catalog refresh."""
    policy_date(membership_as_of)
    return _imported_capture_identity(
        descriptor, membership_as_of, graph_policy(descriptor["coordinates"]["source_id"])
    )


def _imported_capture_identity(descriptor, membership_as_of, policy_by_field):
    identity_by_field = {
        "source": source_edition(descriptor),
        "policy": policy_by_field,
        "membership_as_of": membership_as_of,
    }
    return uuid5(_CAPTURE_NAMESPACE, digest(identity_by_field))


def has_imported_capture_policy_date(capture):
    """Recognize the existing logical import name using its declared historical graph policy."""
    graph = capture["graph"]
    policy_by_field = {
        name: graph[name] for name in ("contract", "derivation_version", "office_policy", "canonicalizer_version")
    }
    if "network_policy" in graph:
        policy_by_field["network_policy"] = graph["network_policy"]
    expected = _imported_capture_identity(capture["catalog_descriptor"], graph["membership_as_of"], policy_by_field)
    return capture["capture_id"] == str(expected)


def compatible_source_edition(descriptor):
    """Normalize logical edition identity across legacy per-family digests and count-only observations."""
    logical_by_field = source_edition(descriptor)
    admission = logical_by_field.get("admission")
    if type(admission) is dict and admission.get("kind") == "catalog_observation":
        logical_by_field = {
            **logical_by_field,
            "admission": {
                **admission,
                "families": {family: {"rows": counts["rows"]} for family, counts in admission["families"].items()},
            },
        }
        logical_by_field.pop("accepted_receipt_id", None)
        logical_by_field.pop("accepted_receipt_sha256", None)
    return logical_by_field


def is_compatible_typed_recipe(recipe, descriptor, observed_date):
    """Reuse only exact data semantics; without a date, a retained capture keeps its own membership date."""
    if type(recipe.source_pin) is not PinnedTypedFHIRMembershipSource:
        return False
    pin = recipe.source_pin
    graph = pin.capture["graph"]
    membership_as_of = graph["origin"]["semantic_projection_as_of"] or observed_date
    return (
        compatible_source_edition(pin.catalog_descriptor) == compatible_source_edition(descriptor)
        and (observed_date is None or graph["membership_as_of"] == membership_as_of)
        and {name: graph.get(name) for name in graph_policy(descriptor["coordinates"]["source_id"])}
        == graph_policy(descriptor["coordinates"]["source_id"])
    )


async def predecessor_recipes(connection, command, control_schema):
    """Resolve the pinned serving inventory before any Source export or capture."""
    generation = command["expected_serving_generation"]
    if generation == 0:
        return ()
    manifest = await resolve_network_serving_manifest(
        connection, generation_id=generation, control_schema=control_schema
    )
    if manifest.manifest_sha256 != command["expected_serving_manifest_sha256"]:
        raise ValueError("registry_imported_preparation_predecessor_changed")
    return await resolve_retained_registry_source_recipes(connection, manifest, control_schema=control_schema)


async def completed_capture(connection, descriptor, membership_as_of):
    """Recover completed named metadata; unknown or partial schemas are never overwritten."""
    identity = imported_source_capture_id(descriptor, membership_as_of)
    schema = PREFIX + identity.hex
    metadata_record = await connection.fetchrow(
        "SELECT CASE WHEN octet_length(obj_description(n.oid,'pg_namespace'))<=$2 "
        "THEN obj_description(n.oid,'pg_namespace') END AS metadata "
        "FROM pg_namespace n WHERE n.nspname=$1",
        schema,
        _MAX_METADATA_BYTES,
    )
    if metadata_record is None:
        return None
    metadata = metadata_record["metadata"]
    prefixes = (COMPONENT + ":4:", COMPONENT + ":5:")
    if type(metadata) is not str or not metadata.startswith(prefixes):
        raise ValueError("registry_imported_preparation_existing_artifact_invalid")
    try:
        document = validate_typed_capture(json.loads(metadata[len(prefixes[0]) :]))
        anchor = TypedGraphAnchor(document["capture_id"], document["observed_at"])
        pin = PinnedTypedFHIRMembershipSource(document, anchor, document["catalog_descriptor"])
        from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
        from process.registry_source_recipe_store import RegistrySourceMembershipRecipe

        recipe = RegistrySourceMembershipRecipe(
            pin, RegistryNetworkSourceCoordinates(**pin.catalog_descriptor["coordinates"])
        )
        if (
            document["schema_name"] != schema
            or document["capture_id"] != str(identity)
            or document["graph"]["membership_as_of"] != membership_as_of
            or not is_compatible_typed_recipe(recipe, descriptor, membership_as_of)
        ):
            raise ValueError("incompatible")
    except (TypeError, ValueError, KeyError, UnicodeError, RecursionError) as error:
        raise ValueError("registry_imported_preparation_existing_artifact_invalid") from error
    return recipe
