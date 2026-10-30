# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare an external address origin without cloning its address rows."""

from dataclasses import dataclass, replace

from process.network_address_projection import PinnedAddressSource
from process.network_compact_address_origin import resolve_compact_address_origin
from process.registry_initial_source_composition import prepare_source_offices

COMPACT_FORMAT_KEY = "network_serving_format"
COMPACT_FORMAT = "compact_office_slices_v1"


@dataclass(frozen=True)
class CompactCompositionAddressSource:
    address_source: PinnedAddressSource
    runtime_roles: tuple[str, ...]
    address_artifact_sha256: str
    origin_manifest_sha256: str | None = None

    @property
    def generation_sha256(self):
        """Membership preparation retains the original native address generation."""
        return self.address_source.generation_id


async def prepare_compact_composition_source(
    connection, address_sources, approved, writer_roles, recipes, request, *, verified_stage=None
):
    """Native capture or a previous seal authenticates the external address origin."""
    roles = tuple(sorted(set(writer_roles["loader_roles"] + writer_roles["reader_roles"])))
    source_manifest = request.source_manifest
    generations_by_source = {"unified_address": address_sources.base_source.generation_id}
    if source_manifest is not None:
        generations_by_source["retained_network_source"] = source_manifest.manifest_sha256
    descriptor, physical = await resolve_compact_address_origin(
        connection,
        {"source_generations": generations_by_source},
        address_sources.base_source,
        runtime_roles=roles,
        control_schema=request.control_schema,
    )
    summary = None
    origin = (
        source_manifest.manifest_sha256
        if source_manifest is not None
        and source_manifest.compact_layout is not None
        and source_manifest.compact_layout.base.table_oid == physical.table_oid
        and source_manifest.source_generations.get("unified_address") == address_sources.base_source.generation_id
        else None
    )
    if verified_stage is not None:
        await _verify_witness_stage(connection, address_sources.base_source, roles, approved, request, verified_stage)
        origin = None
    elif recipes:
        summary = await prepare_source_offices(
            connection,
            recipes,
            approved,
            address_sources.base_source,
            replace(request, runtime_roles=roles, origin_manifest_sha256=origin),
        )
    return CompactCompositionAddressSource(
        address_sources.base_source, roles, descriptor["descriptor_sha256"], origin
    ), summary


async def _verify_witness_stage(connection, address_source, roles, approved, request, stage):
    """The direct EAU matcher never claims a retained logical-office origin."""
    from process.network_initial_cms_office_bindings import verified_retained_offices
    from process.registry_verified_witness_offices import _policy_context

    if request.revision != 5 or not stage.inventory or stage.office_policy_context is None:
        raise ValueError("registry_verified_office_context_changed")
    context, _ = await _policy_context(connection, address_source, roles, approved, request.control_schema)
    if context != stage.office_policy_context or context["origin_manifest_sha256"] is not None:
        raise ValueError("registry_verified_office_context_changed")
    await verified_retained_offices(connection, stage.office_matches, address_source, roles, request.control_schema)
