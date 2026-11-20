# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare imported Source graphs atomically through the installed Source factory."""

from uuid import UUID

from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_fhir_source_witness_contract import canonical_timestamp
from process.network_fhir_typed_capture import TypedGraphAnchor, TypedGraphCaptureOptions, capture_typed_source_graph
from process.network_fhir_typed_recipe import PinnedTypedFHIRMembershipSource
from process.registry_imported_catalog_bindings import PublishedFHIRCatalogBindingScope
from process.registry_imported_membership_refresh import require_imported_catalog_inputs
from process.registry_imported_source_reuse import (
    ImportedSourcePreparation,
    completed_capture,
    imported_source_capture_id,
    is_compatible_typed_recipe,
    predecessor_recipes,
)
from process.registry_source_recipe_composition import SourceWitnessRuntime
from process.registry_source_recipe_store import RegistrySourceMembershipRecipe


def _source_profile(runtime, descriptor):
    native = descriptor["native_read_profile"]
    matches = [
        profile
        for profile in runtime.source_profiles
        if (profile.schema_name, profile.catalog_schema)
        == (native["schema_name"], native.get("catalog_schema_name") or native["schema_name"])
    ]
    if len(matches) != 1:
        raise ValueError("registry_imported_preparation_profile_unavailable")
    return matches[0]


async def _reviewed_origin(runtime, profile, descriptor):
    coordinates = descriptor["coordinates"]
    async with runtime.source_export(
        source_profile=profile,
        source_id=coordinates["source_id"],
        endpoint_id=descriptor["endpoint_id"],
        dataset_id=coordinates["dataset_id"],
    ) as export:
        if type(export) is not dict or "origin" not in export:
            raise ValueError("registry_imported_preparation_origin_unavailable")
        origin = export["origin"]
        expected_by_field = {
            "schema_name": coordinates["dataset_schema"],
            "source_id": coordinates["source_id"],
            "dataset_id": coordinates["dataset_id"],
            "endpoint_id": descriptor["endpoint_id"],
            "dataset_sha256": descriptor["dataset_sha256"],
            "source_uri": descriptor["source_uri"],
            "source_aliases": tuple(descriptor["source_aliases"]),
        }
        if any(getattr(origin, name, None) != value for name, value in expected_by_field.items()):
            raise ValueError("registry_imported_preparation_source_changed")
    return origin


async def _prepare_recipe(connection, runtime, descriptor, observed_at):
    profile = _source_profile(runtime, descriptor)
    # Source owns a one-slot pool: release metadata before the capture reopens its resource stream.
    origin = await _reviewed_origin(runtime, profile, descriptor)
    from process.registry_imported_source_inventory import current_imported_effective_date

    effective_date = current_imported_effective_date(observed_at[:10])
    membership_as_of = origin.semantic_projection_as_of or effective_date
    recovered = await completed_capture(connection, descriptor, membership_as_of)
    if recovered is not None:
        return recovered, False
    identity = imported_source_capture_id(descriptor, membership_as_of)
    anchor = TypedGraphAnchor(str(identity), observed_at)
    captured = await capture_typed_source_graph(
        runtime.source_export,
        connection,
        source_profile=profile,
        reviewed_origin=origin,
        membership_as_of=membership_as_of,
        capture_options=TypedGraphCaptureOptions(anchor, runtime.capture_owner_role, runtime.runtime_roles),
        catalog_descriptor=descriptor,
    )
    pin = PinnedTypedFHIRMembershipSource(captured, anchor, descriptor)
    recipe = RegistrySourceMembershipRecipe(pin, RegistryNetworkSourceCoordinates(**descriptor["coordinates"]))
    if not is_compatible_typed_recipe(recipe, descriptor, effective_date):
        raise ValueError("registry_imported_preparation_graph_policy_changed")
    return recipe, True


async def _prepare_native_recipes(connection, runtime, generation, previous):
    from process.network_fhir_source_witness_contract import digest
    from process.registry_imported_source_inventory import current_imported_source_inventory
    from process.registry_native_network_catalog import native_catalog_descriptor
    from process.registry_native_provider_source import (
        PinnedNativeProviderMembershipSource,
        prepare_native_provider_recipe,
    )

    if runtime.catalog_bindings.prepared_inventory_token is None:
        return ()
    inventory = await current_imported_source_inventory()
    if inventory.token != generation:
        raise ValueError("registry_imported_preparation_catalog_changed")
    prepared_recipes = []
    for source_document in inventory.native_sources:
        descriptor = native_catalog_descriptor(source_document)
        matching_recipes = [
            recipe
            for recipe in previous
            if type(recipe.source_pin) is PinnedNativeProviderMembershipSource
            and recipe.source_pin.catalog_generation == digest(descriptor)
        ]
        if len(matching_recipes) > 1:
            raise ValueError("registry_imported_preparation_predecessor_ambiguous")
        prepared_recipes.append(
            (matching_recipes[0], False)
            if matching_recipes
            else await prepare_native_provider_recipe(
                connection, descriptor, owner_role=runtime.capture_owner_role, runtime_roles=runtime.runtime_roles
            )
        )
    return tuple(prepared_recipes)


async def prepare_imported_source_recipes(connection, *, request_id, envelope, source_runtime, control_schema):
    """Commit one batch; retain unchanged recipes and distinguish new artifacts from reused inputs."""
    if (
        type(request_id) is not UUID
        or not request_id.int
        or type(source_runtime) is not SourceWitnessRuntime
        or type(source_runtime.catalog_bindings) is not PublishedFHIRCatalogBindingScope
        or source_runtime.semantic_policy is not None
        or source_runtime.semantic_validation is not None
        or not connection.is_in_transaction()
    ):
        raise ValueError("registry_imported_preparation_scope_invalid")
    source_runtime.__post_init__()
    async with connection.transaction():
        generation, descriptors = await require_imported_catalog_inputs(connection, envelope["command"], control_schema)
        expected_token = (
            source_runtime.catalog_bindings.prepared_inventory_token
            or source_runtime.catalog_bindings.metadata_generation
        )
        if generation != expected_token:
            raise ValueError("registry_imported_preparation_catalog_changed")
        previous = await predecessor_recipes(connection, envelope["command"], control_schema)
        observed_at = canonical_timestamp(await connection.fetchval("SELECT transaction_timestamp()"))
        from process.registry_imported_source_inventory import current_imported_effective_date

        effective_date = current_imported_effective_date(observed_at[:10])
        recipes, created, reused = [], [], []
        for descriptor in descriptors:
            # A published edition is immutable: keep its capture on later days instead of capturing it again.
            matching_recipes = [recipe for recipe in previous if is_compatible_typed_recipe(recipe, descriptor, None)]
            if len(matching_recipes) > 1:
                raise ValueError("registry_imported_preparation_predecessor_ambiguous")
            recipe, is_new = (
                (matching_recipes[0], False)
                if matching_recipes
                else await _prepare_recipe(connection, source_runtime, descriptor, observed_at)
            )
            recipes.append(recipe)
            (created if is_new else reused).append(recipe.source_pin.capture["schema_name"])
        for recipe, is_new in await _prepare_native_recipes(connection, source_runtime, generation, previous):
            recipes.append(recipe)
            (created if is_new else reused).append(recipe.source_pin.capture["schema_name"])
        return ImportedSourcePreparation(tuple(recipes), tuple(created), tuple(reused))
