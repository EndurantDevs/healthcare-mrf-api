# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Append recipes to one exact retained inventory without changing its read authority."""

import re
from dataclasses import dataclass
from uuid import UUID

from db.registry_source_inventory_schema import MAX_REFERENCE_BYTES
from process.network_fhir_membership_source import PinnedFHIRMembershipSource
from process.network_fhir_typed_recipe import PinnedTypedFHIRMembershipSource, TypedFHIRRecipeReference
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
        ordinary = tuple(recipe for recipe in self.recipes if type(recipe) is not TypedFHIRRecipeReference)
        references = tuple(recipe for recipe in self.recipes if type(recipe) is TypedFHIRRecipeReference)
        for reference in references:
            reference.__post_init__()
        if len(self.recipes) > 100 or len({reference.capture_id for reference in references}) != len(references):
            raise ValueError("registry_source_recipe_additions_invalid")
        ordinary = validate_initial_source_recipes(ordinary, inventory=True)
        object.__setattr__(
            self, "recipes", ordinary + tuple(sorted(references, key=lambda reference: reference.capture_id))
        )

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


@dataclass(frozen=True)
class RegistryTypedSourceRecipeReplacement(RegistrySourceRecipeAdditions):
    """Replace the complete FHIR vector through native typed capture references."""

    def __post_init__(self):
        super().__post_init__()
        if any(type(recipe) is not TypedFHIRRecipeReference for recipe in self.recipes):
            raise ValueError("registry_source_recipe_replacement_invalid")


def decode_source_recipe_additions(document):
    """Keep the worker input closed and reuse the existing recipe/reference codecs."""
    replacement = type(document) is dict and document.get("operation") == "replace_typed_source_vector"
    fields = {"expected_serving_generation", "manifest_sha256", "inventory", "recipes"}
    if (
        type(document) is not dict
        or set(document) != fields | ({"operation"} if replacement else set())
        or type(document["recipes"]) is not list
    ):
        raise ValueError("registry_source_recipe_additions_invalid")
    input_type = RegistryTypedSourceRecipeReplacement if replacement else RegistrySourceRecipeAdditions
    return input_type(
        document["expected_serving_generation"],
        document["manifest_sha256"],
        decode_source_inventory_reference(document["inventory"]),
        _decode_added_recipes(document["recipes"]),
    )


def _decode_added_recipes(documents):
    ordinary, references = [], []
    if len(documents) > 100:
        raise ValueError("registry_source_recipe_additions_invalid")
    for document in documents:
        if type(document) is dict and document.get("source_kind") == "fhir_typed_reference":
            if set(document) != {"source_kind", "capture_id", "observed_at", "capture_sha256", "catalog_generation"}:
                raise ValueError("registry_source_recipe_additions_invalid")
            references.append(
                TypedFHIRRecipeReference(**{key: value for key, value in document.items() if key != "source_kind"})
            )
        else:
            ordinary.append(document)
    return (*decode_registry_source_recipes(ordinary), *references)


async def resolve_source_recipe_additions(
    connection, additions, manifest, expected_head, *, control_schema, source_witness_runtime=None
):
    """Verify native retained custody before appending or replacing the complete FHIR vector."""
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
    added = tuple(
        [
            await recipe.resolve(connection, source_witness_runtime)
            if type(recipe) is TypedFHIRRecipeReference
            else recipe
            for recipe in additions.recipes
        ]
    )
    preserved = retained
    if type(additions) is RegistryTypedSourceRecipeReplacement:
        if any(type(recipe.source_pin) is not PinnedTypedFHIRMembershipSource for recipe in added):
            raise ValueError("registry_source_recipe_replacement_invalid")
        preserved = tuple(
            recipe
            for recipe in retained
            if type(recipe.source_pin) not in (PinnedFHIRMembershipSource, PinnedTypedFHIRMembershipSource)
        )
    recipes = SourceInventoryPreparation((*preserved, *added)).recipes
    fhir_vector_succession(retained, recipes)
    return recipes
