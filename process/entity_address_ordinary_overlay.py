# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prove that the issued affected Overlay was applied to its original live heap."""

import importlib
from dataclasses import dataclass

from process import entity_address_preparation_admission as admitted
from process import provider_directory_serving_recovery_receipts as receipts
from process.provider_directory_output_snapshot_catalog import encoded

_ROOT = "provider_directory_address_overlay"


def _require(condition):
    if not condition:
        raise RuntimeError("entity_address_ordinary_overlay_changed")


@dataclass(frozen=True)
class OverlayDependency:
    """Original captured native Overlay and the producer's exact bounded stage."""

    live_native_json: str
    stage_table: str
    stage_oid: int
    live_oid: int
    layout_sha256: str


async def capture(native, schema, build):
    """Bind the signed original OID/layout before the long Address preparation."""
    from process import provider_directory_ordinary_retention_capacity as retention
    from process.provider_directory_cms_preparation import OwnedRelation

    fhir = importlib.import_module("process.provider_directory_fhir")
    _require(native.db is fhir.db)
    roots = [root for root in build.capture.predecessor["roots"] if root["family"] == "overlay"]
    _require(len(roots) == 1)
    root = roots[0]
    _require(root["schema"] == schema and root["relation"] == _ROOT)
    async with admitted.native_transaction():
        await native.db.status(f"LOCK TABLE {receipts.qualified(schema, _ROOT)} IN SHARE MODE NOWAIT")
        observed = (await receipts.catalog(native.db, schema, (_ROOT,)))[_ROOT]
        _require(observed["oid"] == root["oid"])
        layout = await retention.retention_layout_sha256(fhir, OwnedRelation(schema, _ROOT, root["oid"], 0, "p"))
        _require(layout == root["layout_sha256"])
    return encoded(observed)


def bind(native, build):
    """Keep both genuine identities; a staged OID never becomes a live OID by assertion."""
    inputs = native.candidate_preparation.current()
    _require(build.overlay_native_json is not None and inputs is not None)
    root = next(root for root in build.capture.predecessor["roots"] if root["family"] == "overlay")
    _require(inputs.overlay_table != _ROOT and inputs.overlay_relation_oid != root["oid"])
    return OverlayDependency(
        build.overlay_native_json, inputs.overlay_table, inputs.overlay_relation_oid, root["oid"], root["layout_sha256"]
    )


async def assert_applied(native, prepared, descriptor):
    """Compare exact native custody and the complete changed-Source row multiset in the cut."""
    binding = descriptor.overlay_dependency
    _require(type(binding) is OverlayDependency)
    inputs, schema = prepared.preparation_input, descriptor.schema
    _require((inputs.overlay_table, inputs.overlay_relation_oid) == (binding.stage_table, binding.stage_oid))
    await native.db.status(f"LOCK TABLE {receipts.qualified(schema, _ROOT)} IN SHARE ROW EXCLUSIVE MODE NOWAIT")
    await native.db.status(f"LOCK TABLE {receipts.qualified(schema, binding.stage_table)} IN SHARE MODE NOWAIT")
    current = (await receipts.catalog(native.db, schema, (_ROOT,)))[_ROOT]
    _require(encoded(current) == binding.live_native_json and current["oid"] == binding.live_oid)
    _require(
        await native.db.scalar("SELECT to_regclass(:relation)::oid::bigint", relation=f"{schema}.{binding.stage_table}")
        == binding.stage_oid
    )
    _require(await native._address_alias_generation(schema) == inputs.address_alias_generation)
    generation, live_oid = await native._provider_directory_overlay_alias_fence(schema)
    _require((generation, live_oid) == (inputs.address_alias_generation, binding.live_oid))
    _require(await native.db.scalar(applied_rows_sql(native, descriptor)) is True)


def applied_rows_sql(native, descriptor):
    """Removed Sources are empty; surviving unaffected Sources are outside this DML scope."""
    binding = descriptor.overlay_dependency
    schema, stage = descriptor.schema, descriptor.overlay_dependency.stage_table
    changed = native._string_array_literal(list(descriptor.source_ids))
    refreshed = native._string_array_literal(descriptor.capture.affected_scope["refresh_source_ids"])
    live = f"SELECT to_jsonb(live) FROM {receipts.qualified(schema, _ROOT)} live WHERE live.source_id=ANY({changed})"
    candidate = f"SELECT to_jsonb(candidate) FROM {receipts.qualified(schema, stage)} candidate"
    _require(binding.live_oid != binding.stage_oid)
    return (
        f"SELECT NOT EXISTS(SELECT 1 FROM {receipts.qualified(schema, stage)} candidate WHERE candidate.source_id IS NULL OR NOT(candidate.source_id=ANY({refreshed}))) "
        f"AND NOT EXISTS (({live} EXCEPT ALL {candidate}) UNION ALL ({candidate} EXCEPT ALL {live}))"
    )
