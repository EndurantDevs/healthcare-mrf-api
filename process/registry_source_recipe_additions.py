# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Append recipes to one exact retained inventory without changing its read authority."""

import re
from dataclasses import dataclass
from uuid import UUID

from db.registry_source_inventory_schema import MAX_REFERENCE_BYTES
from process.network_serving_read import PinnedNetworkServingManifest
from process.registry_fhir_serving_preservation import fhir_vector_succession
from process.registry_initial_source_composition import validate_initial_source_recipes
from process.registry_source_inventory_preparation import SourceInventoryPreparation
from process.registry_source_inventory_reference import (
    SourceInventoryReference,
    decode_source_inventory_reference,
)
from process.registry_source_observation_store import _namespace
from process.registry_source_recipe_store import (
    decode_registry_source_recipes,
    resolve_retained_registry_source_recipes,
)


@dataclass(frozen=True)
class RegistrySourceRecipeAdditions:
    expected_serving_generation: int
    manifest_sha256: str
    inventory: SourceInventoryReference
    recipes: tuple

    def __post_init__(self):
        if (
            type(self.expected_serving_generation) is not int
            or not 0 < self.expected_serving_generation < 2**63
            or type(self.manifest_sha256) is not str
            or re.fullmatch(r"[0-9a-f]{64}", self.manifest_sha256) is None
            or type(self.inventory) is not SourceInventoryReference
            or type(self.recipes) is not tuple
            or not self.recipes
        ):
            raise ValueError("registry_source_recipe_additions_invalid")
        object.__setattr__(self, "recipes", validate_initial_source_recipes(self.recipes, inventory=True))

    def require_predecessor(self, manifest, expected_head):
        """Bind the input to the command's exact retained generation and manifest."""
        if (
            type(manifest) is not PinnedNetworkServingManifest
            or type(expected_head) is not int
            or expected_head != self.expected_serving_generation
            or manifest.generation_id != expected_head
            or manifest.manifest_sha256 != self.manifest_sha256
        ):
            raise ValueError("registry_source_recipe_additions_predecessor_changed")


def decode_source_recipe_additions(document):
    """Keep the worker input closed and reuse the existing recipe/reference codecs."""
    if (
        type(document) is not dict
        or set(document) != {"expected_serving_generation", "manifest_sha256", "inventory", "recipes"}
        or type(document["recipes"]) is not list
    ):
        raise ValueError("registry_source_recipe_additions_invalid")
    return RegistrySourceRecipeAdditions(
        document["expected_serving_generation"],
        document["manifest_sha256"],
        decode_source_inventory_reference(document["inventory"]),
        decode_registry_source_recipes(document["recipes"]),
    )


async def resolve_source_recipe_additions(connection, additions, manifest, expected_head, *, control_schema):
    """Verify native retained custody before forming a complete, append-only source vector."""
    additions.require_predecessor(manifest, expected_head)
    retained = await resolve_retained_registry_source_recipes(connection, manifest, control_schema=control_schema)
    encoded = await connection.fetchval(
        f"SELECT CASE WHEN octet_length(source_recipes_json::text)<=$2 THEN source_recipes_json::text END "
        f"FROM {_namespace(control_schema)}.network_membership_candidate WHERE candidate_id=$1 AND state='published'",
        UUID(manifest.candidate_id),
        MAX_REFERENCE_BYTES,
    )
    if decode_source_inventory_reference(encoded) != additions.inventory:
        raise ValueError("registry_source_recipe_additions_inventory_changed")
    recipes = SourceInventoryPreparation((*retained, *additions.recipes)).recipes
    fhir_vector_succession(retained, recipes)
    return recipes
