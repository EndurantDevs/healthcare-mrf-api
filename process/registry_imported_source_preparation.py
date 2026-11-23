# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare and checkpoint imported Source graphs through the installed Source factories."""

import asyncio
from collections import deque
from dataclasses import replace
from uuid import UUID

from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_fhir_source_witness_contract import canonical_timestamp
from process.network_fhir_typed_capture import (
    TypedGraphAnchor,
    TypedGraphCaptureOptions,
    capture_typed_source_graph,
    require_typed_graph_capture,
)
from process.network_fhir_typed_recipe import PinnedTypedFHIRMembershipSource
from process.registry_fhir_catalog_sources import is_delta_positive_catalog_descriptor
from process.registry_imported_catalog_bindings import PublishedFHIRCatalogBindingScope
from process.registry_imported_membership_refresh import require_imported_catalog_inputs
from process.registry_imported_source_inventory import is_practitioner_enrichment
from process.registry_imported_source_reuse import (
    ImportedSourcePreparation,
    completed_capture,
    imported_source_capture_id,
    is_compatible_typed_recipe,
    predecessor_recipes,
)
from process.registry_network_import_progress import NETWORK_PROGRESS, network_import_stage
from process.registry_source_recipe_composition import SourceWitnessRuntime
from process.registry_source_recipe_store import RegistrySourceMembershipRecipe
from process.uhc_flex_practitioner_async_safety import drain_operation


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
    from process.registry_imported_source_inventory import (
        current_imported_checkpoint_date,
        current_imported_effective_date,
    )

    effective_date = current_imported_effective_date(observed_at[:10])
    membership_as_of = origin.semantic_projection_as_of or effective_date
    recovered = await completed_capture(connection, descriptor, membership_as_of)
    checkpoint_date = current_imported_checkpoint_date()
    if (
        recovered is None
        and origin.semantic_projection_as_of is None
        and checkpoint_date
        and checkpoint_date < effective_date
    ):
        recovered = await completed_capture(connection, descriptor, checkpoint_date)
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


async def _prepare_native_recipes(capture_transaction, runtime, generation, previous):
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
        if matching_recipes:
            prepared_recipes.append((matching_recipes[0], False))
            continue
        async with capture_transaction() as connection:
            prepared = await prepare_native_provider_recipe(
                connection,
                descriptor,
                owner_role=runtime.capture_owner_role,
                runtime_roles=runtime.runtime_roles,
            )
        prepared_recipes.append(prepared)
    return tuple(prepared_recipes)


async def _require_cumulative_delta_pairs(connection, recipe, previous):
    """Reject dropped or reassigned positive offices on the verified graph replacement."""
    successor = recipe.source_pin
    descriptor = successor.catalog_descriptor
    if not is_delta_positive_catalog_descriptor(descriptor):
        return
    prior_pins = [
        old.source_pin
        for old in previous
        if type(old.source_pin) is PinnedTypedFHIRMembershipSource
        and is_delta_positive_catalog_descriptor(old.source_pin.catalog_descriptor)
    ]
    if not prior_pins:
        return
    if len(prior_pins) != 1:
        raise ValueError("registry_delta_predecessor_ambiguous")
    predecessor = prior_pins[0]
    if (
        predecessor.generation_id == successor.generation_id
        or predecessor.catalog_descriptor["coordinates"]["dataset_id"] == descriptor["previous_dataset_id"]
    ):
        return
    old_stage = await require_typed_graph_capture(connection, predecessor.capture, expected_anchor=predecessor.anchor)
    new_stage = await require_typed_graph_capture(connection, successor.capture, expected_anchor=successor.anchor)
    before, after = old_stage.relation("pairs"), new_stage.relation("pairs")
    lost = await connection.fetchval(
        f"""SELECT EXISTS (SELECT 1 FROM {before} AS old
            WHERE NOT EXISTS (SELECT 1 FROM {after} AS new
              WHERE (new.resource_type,new.resource_id,new.network_source_key,new.location_resource_id)
                  = (old.resource_type,old.resource_id,old.network_source_key,old.location_resource_id)
                AND (new.provider_system,new.provider_id,new.address_key,new.canonical_identity)
                  = (old.provider_system,old.provider_id,old.address_key,old.canonical_identity)))"""
    )
    if lost:
        raise ValueError("registry_delta_predecessor_positives_lost")


async def _capture_or_reuse_fhir(capture_transaction, runtime, descriptor, observed_at, previous):
    """Reuse an unchanged edition or commit its validated replacement."""
    matching_recipes = [recipe for recipe in previous if is_compatible_typed_recipe(recipe, descriptor, None)]
    if len(matching_recipes) > 1:
        raise ValueError("registry_imported_preparation_predecessor_ambiguous")
    if matching_recipes:
        recipe = matching_recipes[0]
        if is_delta_positive_catalog_descriptor(descriptor):
            async with capture_transaction() as connection:
                await _require_cumulative_delta_pairs(connection, recipe, previous)
        return recipe, False
    async with capture_transaction() as connection:
        recipe, is_new = await _prepare_recipe(connection, runtime, descriptor, observed_at)
        await _require_cumulative_delta_pairs(connection, recipe, previous)
    return recipe, is_new


def _capture_size(descriptor):
    """Start larger published editions first to reduce the serial tail."""
    return descriptor["admission"].get("publication", {}).get("claimed_resource_count", 0)


async def _cancel_capture_workers(workers):
    """Let active capture transactions and Source exports unwind before their pools close."""
    for capture_worker in workers:
        if not capture_worker.done() and not capture_worker.cancelling():
            capture_worker.cancel()
    await asyncio.gather(*workers, return_exceptions=True)


async def _prepare_fhir_recipes(capture_transaction, runtime, descriptors, observed_at, previous):
    """Use one genuine export per worker and return recipes in the pinned inventory order."""
    eligible_descriptors = [descriptor for descriptor in descriptors if not is_practitioner_enrichment(descriptor)]
    progress = NETWORK_PROGRESS.get()
    if progress is not None:
        progress.total = len(eligible_descriptors)
        await network_import_stage("FHIR graph capture")
    indexed_descriptors = list(enumerate(eligible_descriptors))
    exports = runtime.capture_source_exports or (runtime.source_export,)
    if len(exports) > 1:
        indexed_descriptors.sort(key=lambda entry: _capture_size(entry[1]), reverse=True)
    pending_descriptors = deque(indexed_descriptors)
    recipes_by_index = {}

    async def capture_worker(source_export):
        """Take the next edition without yielding until it has been assigned to this worker."""
        capture_runtime = replace(runtime, source_export=source_export)
        while pending_descriptors:
            index, descriptor = pending_descriptors.popleft()
            try:
                recipes_by_index[index] = await _capture_or_reuse_fhir(
                    capture_transaction, capture_runtime, descriptor, observed_at, previous
                )
            except BaseException as error:
                pending_descriptors.clear()
                if progress is not None and isinstance(error, Exception):
                    progress.source_failed(index, descriptor, error)
                raise
            if progress is not None:
                if recipes_by_index[index][1]:
                    progress.committed += 1
                else:
                    progress.reused += 1
                await progress.emit()

    if len(exports) == 1:
        await capture_worker(exports[0])
    else:
        workers = [
            asyncio.create_task(capture_worker(source_export)) for source_export in exports[: len(indexed_descriptors)]
        ]
        try:
            await asyncio.gather(*(asyncio.shield(worker) for worker in workers))
        except BaseException:
            pending_descriptors.clear()
            await drain_operation(_cancel_capture_workers(workers), preserve_cancellation=True)
            raise
    return tuple(recipes_by_index[index] for index in range(len(indexed_descriptors)))


async def prepare_imported_source_recipes(capture_transaction, *, request_id, envelope, source_runtime, control_schema):
    """Checkpoint each changed source and retain compatible recipes for the selected inventory."""
    if (
        not isinstance(request_id, UUID)
        or not request_id.int
        or type(source_runtime) is not SourceWitnessRuntime
        or type(source_runtime.catalog_bindings) is not PublishedFHIRCatalogBindingScope
        or source_runtime.semantic_policy is not None
        or source_runtime.semantic_validation is not None
    ):
        raise ValueError("registry_imported_preparation_scope_invalid")
    source_runtime.__post_init__()
    async with capture_transaction() as connection:
        generation, descriptors = await require_imported_catalog_inputs(connection, envelope["command"], control_schema)
        expected_token = (
            source_runtime.catalog_bindings.prepared_inventory_token
            or source_runtime.catalog_bindings.metadata_generation
        )
        if generation != expected_token:
            raise ValueError("registry_imported_preparation_catalog_changed")
        previous = await predecessor_recipes(connection, envelope["command"], control_schema)
        observed_at = canonical_timestamp(await connection.fetchval("SELECT transaction_timestamp()"))

    fhir_recipes = await _prepare_fhir_recipes(capture_transaction, source_runtime, descriptors, observed_at, previous)
    await network_import_stage("Native source preparation")
    native_recipes = await _prepare_native_recipes(capture_transaction, source_runtime, generation, previous)
    recipes, created, reused = [], [], []
    for recipe, is_new in (*fhir_recipes, *native_recipes):
        recipes.append(recipe)
        (created if is_new else reused).append(recipe.source_pin.capture["schema_name"])
    await network_import_stage("Membership publication")
    return ImportedSourcePreparation(tuple(recipes), tuple(created), tuple(reused))
