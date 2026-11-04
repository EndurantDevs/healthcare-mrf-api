# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reuse exact typed inputs already proved by a published, natively closed candidate."""

import asyncio
import hashlib
import json
from dataclasses import dataclass

from db.registry_source_inventory_schema import INVENTORY_TABLE
from process.network_fhir_typed_contract import require
from process.network_fhir_typed_recipe import PinnedTypedFHIRMembershipSource
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_source_contribution_artifacts import (
    _artifact_copy_target,
    _published_artifact_candidates,
    _verify_artifact_owner,
)
from process.registry_source_inventory_store import _load_locked_source_inventory
from process.registry_source_recipe_store import canonical_registry_source_recipes
from process.registry_typed_source_metadata import validate_typed_contribution, validate_typed_derivation

_BRAND = object()


@dataclass(frozen=True)
class RetainedTypedSourceProof:
    connection: object
    task: object
    recipe_sha256: str
    candidate_id: str
    seal_sha256: str
    capture_sha256: str
    brand: object


@dataclass(frozen=True)
class RetainedTypedGraphRead:
    """Distinct K capability: prior independent candidate custody, not a fresh Source observation."""

    loaded: object
    proof: RetainedTypedSourceProof

    @property
    def document(self):
        """Expose the graph descriptor retained by the independently verified predecessor."""
        return self.loaded.document

    def relation(self, kind):
        """Resolve a relation through the retained producer capability."""
        return self.loaded.relation(kind)

    async def verify(self, connection):
        """Require predecessor custody, task identity and the unchanged capture hash."""
        require(type(self.proof) is RetainedTypedSourceProof and self.proof.brand is _BRAND, "retained_source_custody")
        require(
            self.proof.connection is connection and self.proof.task is asyncio.current_task(), "retained_source_scope"
        )
        require(self.loaded.capture["capture_sha256"] == self.proof.capture_sha256, "retained_source_changed")
        await self.loaded.verify(connection)


async def retained_typed_source_proofs(connection, manifest, recipes, control_schema):
    """Authenticate the exact predecessor and native inventory before reusing any immutable Source input."""
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
    candidates = await _published_artifact_candidates(connection, control_schema, {manifest.candidate_id})
    candidate = candidates[manifest.candidate_id]
    custody = await _verify_artifact_owner(connection, candidate, control_schema)
    inventory_oid = custody["receipt"]["closure"]["relation_oids"].get(INVENTORY_TABLE)
    if inventory_oid is None:
        return {}
    _, entries = await _load_locked_source_inventory(
        connection,
        _artifact_copy_target(candidate),
        candidate,
        expected_table_oid=inventory_oid,
        control_schema=control_schema,
    )
    proofs_by_recipe = {}
    for entry in entries:
        recipe = recipes_by_json.get(entry.recipe_json)
        if recipe is None:
            continue
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
            and descriptor["catalog_generation"] == recipe.source_pin.catalog_generation,
            "retained_source_inventory",
        )
        proofs_by_recipe[identity] = RetainedTypedSourceProof(
            connection,
            asyncio.current_task(),
            identity,
            manifest.candidate_id,
            custody["seal_sha256"],
            recipe.source_pin.generation_id,
            _BRAND,
        )
    return proofs_by_recipe
