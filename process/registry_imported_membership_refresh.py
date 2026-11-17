# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native imported catalog inputs for the authenticated controlled publisher."""

from process.network_fhir_source_witness_contract import digest
from process.registry_approval_store import RegistryApprovalConflict
from process.registry_imported_source_inventory import ImportedSourceInventory, current_imported_source_inventory
from process.registry_imported_source_reuse import graph_policy, source_edition
from process.registry_membership_refresh_contract import (
    RegistryMembershipRefreshUnavailable,
    validated_membership_refresh_envelope,
)
from process.registry_refresh_anchor_contract import _hash

TRANSFORM_VERSION = "fhir-imported-catalog-v1"


def imported_catalog_generation(command):
    """Recognize one complete catalog vector without inventing binding enrollments."""
    sources = command["accepted_sources"]
    if not any(source["transform_version"] == TRANSFORM_VERSION for source in sources):
        return None
    if command["enrolled_scopes"] or any(source["transform_version"] != TRANSFORM_VERSION for source in sources):
        raise ValueError("registry_imported_membership_scope_invalid")
    generations = {source["capture_generation"] for source in sources}
    if len(generations) != 1:
        raise ValueError("registry_imported_membership_catalog_invalid")
    generation = generations.pop()
    _hash(generation)
    return generation


class RegistryImportedMembershipAdmission:
    """An operator-authenticated queue and its owned worker need no second App grant."""

    is_enabled = True

    async def __call__(self, *, actor, command):
        validated_membership_refresh_envelope({"actor": actor, "command": command})
        if imported_catalog_generation(command) is None:
            raise PermissionError("registry_imported_membership_scope_required")

    async def authorize_commit(self, *, actor, command):
        """Recheck the ordinary imported publication command at commit."""
        await self(actor=actor, command=command)


async def imported_catalog_inputs(connection, control_schema, *, expected_generation=None, inventory=None):
    """Use observed Source data, never a previously derived serving catalogue pointer."""
    if inventory is None:
        inventory = await current_imported_source_inventory()
    if type(inventory) is not ImportedSourceInventory:
        raise RegistryMembershipRefreshUnavailable("registry_imported_native_inventory_required")
    generation = inventory.token
    if expected_generation is not None and generation != expected_generation:
        raise RegistryApprovalConflict("registry_imported_catalog_changed")
    accepted_sources = []
    for descriptor in inventory.fhir_descriptors:
        source_id = descriptor["coordinates"]["source_id"]
        accepted_sources.append(
            {
                "source_id": source_id,
                "capture_generation": generation,
                "bindings_sha256": digest([source_edition(descriptor), graph_policy(source_id)]),
                "transform_version": TRANSFORM_VERSION,
            }
        )
    for descriptor in inventory.native_sources:
        accepted_sources.append(
            {
                "source_id": descriptor["source_id"],
                "capture_generation": generation,
                "bindings_sha256": digest(descriptor),
                "transform_version": TRANSFORM_VERSION,
            }
        )
    accepted_sources.sort(key=lambda source_document: source_document["source_id"])
    if (
        not accepted_sources
        or len(accepted_sources) > 1000
        or len({source_document["source_id"] for source_document in accepted_sources}) != len(accepted_sources)
    ):
        raise RegistryMembershipRefreshUnavailable("registry_imported_catalog_sources_invalid")
    return generation, accepted_sources, list(inventory.fhir_descriptors)


async def require_imported_catalog_inputs(connection, command, control_schema, *, inventory=None):
    """Require the complete freshly observed source vector selected by this command."""
    generation = imported_catalog_generation(command)
    if generation is None:
        raise ValueError("registry_imported_membership_scope_required")
    _, sources, descriptors = await imported_catalog_inputs(
        connection, control_schema, expected_generation=generation, inventory=inventory
    )
    if sources != command["accepted_sources"]:
        raise RegistryApprovalConflict("registry_imported_catalog_sources_changed")
    return generation, descriptors
