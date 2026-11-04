# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed typed recipe pins; serialization never establishes Source or office custody."""

import json
from dataclasses import dataclass


@dataclass(frozen=True)
class PinnedTypedFHIRMembershipSource:
    capture: dict
    anchor: object
    catalog_descriptor: dict

    def __post_init__(self):
        from process.network_fhir_source_witness_contract import canonical, source_origin
        from process.network_fhir_typed_capture import TypedGraphAnchor, validate_typed_capture
        from process.network_fhir_typed_contract import require
        from process.registry_fhir_catalog_sources import validate_fhir_catalog_source_descriptor

        capture = validate_typed_capture(self.capture)
        require(type(self.anchor) is TypedGraphAnchor, "recipe_anchor")
        self.anchor.__post_init__()
        require(
            (capture["capture_id"], capture["observed_at"]) == (self.anchor.capture_id, self.anchor.observed_at),
            "recipe_anchor",
        )
        catalog = validate_fhir_catalog_source_descriptor(self.catalog_descriptor)
        require(catalog == capture["catalog_descriptor"], "recipe_catalog")
        origin = source_origin(capture["graph"]["origin"])
        coordinates = catalog["coordinates"]
        require(catalog["profile"] == "published_normalized" and catalog["complete"] is True, "recipe_catalog")
        require(
            (
                coordinates["dataset_schema"],
                coordinates["dataset_id"],
                catalog["endpoint_id"],
                catalog["dataset_sha256"],
                catalog["resource_table_oid"],
                catalog["header_table_oid"],
                catalog["database_oid"],
                catalog["source_uri"],
                tuple(catalog["source_aliases"]),
            )
            == (
                origin.schema_name,
                origin.dataset_id,
                origin.endpoint_id,
                origin.dataset_sha256,
                origin.resource_parent_oid,
                origin.header_oid,
                origin.database_oid,
                origin.source_uri,
                origin.source_aliases,
            )
            and origin.resource_leaf_oid in catalog["resource_leaf_oids"]
            and coordinates["source_id"] == origin.source_id,
            "recipe_catalog",
        )
        object.__setattr__(self, "capture", json.loads(canonical(capture)))
        object.__setattr__(self, "catalog_descriptor", catalog)

    @property
    def generation_id(self):
        return self.capture["capture_sha256"]

    @property
    def catalog_generation(self):
        from process.registry_fhir_catalog_sources import fhir_catalog_source_generation

        return fhir_catalog_source_generation(self.catalog_descriptor)


@dataclass(frozen=True)
class TypedFHIRRecipeReference:
    """Small transport coordinates; only native hydration can produce a membership recipe."""

    capture_id: str
    observed_at: str
    capture_sha256: str
    catalog_generation: str

    def __post_init__(self):
        from process.network_fhir_typed_capture import TypedGraphAnchor
        from process.registry_source_office_summary_v3 import _sha

        TypedGraphAnchor(self.capture_id, self.observed_at)
        _sha(self.capture_sha256)
        _sha(self.catalog_generation)

    async def resolve(self, connection, runtime):
        from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
        from process.network_fhir_typed_capture import TypedGraphAnchor, resolve_typed_graph_capture_reference
        from process.registry_source_recipe_composition import SourceWitnessRuntime
        from process.registry_source_recipe_store import RegistrySourceMembershipRecipe

        if type(runtime) is not SourceWitnessRuntime:
            raise ValueError("registry_source_typed_runtime_unavailable")
        self.__post_init__()
        runtime.__post_init__()
        anchor = TypedGraphAnchor(self.capture_id, self.observed_at)
        capture = await resolve_typed_graph_capture_reference(
            connection,
            expected_anchor=anchor,
            capture_sha256=self.capture_sha256,
            catalog_generation=self.catalog_generation,
            owner_role=runtime.capture_owner_role,
            runtime_roles=runtime.runtime_roles,
        )
        pin = PinnedTypedFHIRMembershipSource(capture, anchor, capture["catalog_descriptor"])
        return RegistrySourceMembershipRecipe(
            pin, RegistryNetworkSourceCoordinates(**pin.catalog_descriptor["coordinates"])
        )
