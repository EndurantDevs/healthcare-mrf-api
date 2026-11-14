# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reuse typed content checked by a published logical generation."""

import hashlib
import json
from dataclasses import dataclass

from process.network_fhir_source_witness_contract import digest
from process.network_fhir_typed_contract import require
from process.network_fhir_typed_recipe import PinnedTypedFHIRMembershipSource
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_source_contribution_artifacts import (
    _artifact_copy_target,
    _published_artifact_candidates,
    load_published_artifact_validation,
)
from process.registry_source_inventory_store import load_source_inventory
from process.registry_source_recipe_store import (
    canonical_registry_source_recipes,
    decode_registry_source_recipes,
    resolve_retained_registry_source_recipes,
)
from process.registry_typed_source_metadata import validate_typed_contribution, validate_typed_derivation


@dataclass(frozen=True)
class RetainedTypedSourceProof:
    recipe_sha256: str
    candidate_id: str
    generation_id: str
    capture_sha256: str
    semantic_receipt: dict | None = None


@dataclass(frozen=True)
class RetainedTypedGraphRead:
    """Named graph data with its retained logical validation record."""

    loaded: object
    proof: RetainedTypedSourceProof

    @property
    def document(self):
        """Expose the selected retained graph descriptor."""
        return self.loaded.document

    def relation(self, kind):
        """Resolve a relation in the retained named Source edition."""
        return self.loaded.relation(kind)

    def require_ready(self, *, complete=True):
        """Keep the retained edition bound to its recipe and content capture."""
        require(type(self.proof) is RetainedTypedSourceProof, "retained_source_record")
        require(self.loaded.capture["capture_sha256"] == self.proof.capture_sha256, "retained_source_changed")
        self.loaded.require_ready(complete=False)


async def retained_typed_source_proofs(connection, manifest, recipes, control_schema, *, allow_sampled=False):
    """Resolve published data evidence and complete inventory before reusing a Source edition."""
    require(type(allow_sampled) is bool, "retained_sampling_mode")
    recipes_by_json = {
        canonical_registry_source_recipes((recipe,))[1:-1]: recipe
        for recipe in recipes
        if type(recipe.source_pin) is PinnedTypedFHIRMembershipSource
    }
    if manifest is None or not recipes_by_json:
        return {}
    current = await resolve_network_serving_manifest(
        connection, generation_id=manifest.generation_id, control_schema=control_schema
    )
    require(current == manifest, "retained_source_manifest")
    previous_recipes = await resolve_retained_registry_source_recipes(
        connection, current, control_schema=control_schema
    )
    if not any(type(recipe.source_pin) is PinnedTypedFHIRMembershipSource for recipe in previous_recipes):
        return {}
    candidates = await _published_artifact_candidates(connection, control_schema, {manifest.candidate_id})
    candidate = candidates[manifest.candidate_id]
    evidence = await load_published_artifact_validation(connection, candidate, control_schema)
    require(
        evidence["candidate_id"] == manifest.candidate_id and evidence["generation_id"] == manifest.manifest_sha256,
        "retained_source_manifest",
    )
    if evidence["validation"] is None:
        return {}
    semantic_sources_by_capture = _retained_semantic_sources(evidence, manifest.candidate_id)
    _, entries = await load_source_inventory(
        connection, _artifact_copy_target(candidate), candidate, control_schema=control_schema
    )
    proofs_by_recipe = {}
    for entry in entries:
        decoded = decode_registry_source_recipes("[" + entry.recipe_json + "]")
        normalized_json = canonical_registry_source_recipes(decoded)[1:-1]
        recipe = recipes_by_json.get(normalized_json)
        if recipe is None:
            continue
        semantic_receipt = None
        if semantic_sources_by_capture is not None:
            source_evidence = semantic_sources_by_capture.get(recipe.source_pin.generation_id)
            if source_evidence is None or (source_evidence["method"] != "exhaustive-retained" and not allow_sampled):
                continue
            semantic_receipt = source_evidence["receipt"]
        identity = _verified_inventory_recipe(entry, recipe)
        recipe_key = hashlib.sha256(("[" + normalized_json + "]").encode()).hexdigest()
        proofs_by_recipe[recipe_key] = RetainedTypedSourceProof(
            identity,
            manifest.candidate_id,
            evidence["generation_id"],
            recipe.source_pin.generation_id,
            semantic_receipt,
        )
    return proofs_by_recipe


def _retained_semantic_sources(evidence, candidate_id):
    """Preserve sampled classification from logical published validation metadata."""
    if evidence["semantic_validation"] is None:
        return None
    from process.network_sampled_custody import validate_sampled_custody

    semantic = validate_sampled_custody(evidence["semantic_validation"], candidate_id=candidate_id)
    return {source["capture_sha256"]: source for source in semantic["sources"]}


def _verified_inventory_recipe(entry, recipe):
    """Require matching contribution, office derivation and logical capture coordinates."""
    require(
        entry.contribution_json is not None and entry.office_derivation_json is not None,
        "retained_source_inventory",
    )
    descriptor = validate_typed_contribution(json.loads(entry.contribution_json))
    derivation = validate_typed_derivation(json.loads(entry.office_derivation_json))
    identity = hashlib.sha256(("[" + entry.recipe_json + "]").encode()).hexdigest()
    require(
        descriptor["recipe_sha256"] == derivation["recipe_sha256"] == identity
        and descriptor["capture_sha256"] == derivation["capture_sha256"] == recipe.source_pin.generation_id
        and descriptor["catalog_generation"] == recipe.source_pin.catalog_generation
        and derivation["contribution_sha256"] == digest(descriptor)
        and derivation["mapping_sha256"] == descriptor["mapping_sha256"],
        "retained_source_inventory",
    )
    return identity
