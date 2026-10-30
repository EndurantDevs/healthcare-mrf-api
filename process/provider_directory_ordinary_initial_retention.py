# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep authentic initial targets through the existing atomic stage installation."""

import asyncio
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import replace

from process import provider_directory_serving_recovery_receipts as receipts

_ACTIVE = ContextVar("ordinary_initial_retention", default=None)
_FAMILIES = {"provider_directory_profile": "profile", "provider_directory_profile_evidence": "evidence"}


@contextmanager
def initial_retention_scope(prepared, native_roots):
    """Carry prepared data only; the existing cut actor authenticates each rename."""
    receipts.require(_ACTIVE.get() is None, "initial_retention_scope_active")
    token = _ACTIVE.set((prepared, native_roots, asyncio.current_task()))
    try:
        yield
    finally:
        _ACTIVE.reset(token)


async def retain_initial_stage(directory, stage):
    """Retain the exact original heap and indexes after initial checks and before install."""
    active = _ACTIVE.get()
    if active is None or stage.target_relation not in _FAMILIES:
        return
    from process import provider_directory_ordinary_row_retention as rows
    from process.provider_directory_ordinary_retention_capacity import acknowledge_initial_retained_relation

    prepared, native_roots, task = active
    receipts.require(directory is prepared.fhir and task is asyncio.current_task(), "initial_retention_owner_changed")
    receipts.require(any(stage is original for original in prepared.profile_bundle.stages), "initial_stage_changed")
    retained = rows._require_retained(prepared)
    receipts.require(retained.capture.predecessor["kind"] == "initial", "initial_retention_required")
    receipts.require(stage.profile_initial_build is not None, "initial_stage_required")
    family = _FAMILIES[stage.target_relation]
    originals = [relation for relation in retained.families if relation.family == family]
    receipts.require(len(originals) == 1 and originals[0].reused_initial, "initial_target_changed")
    original = originals[0]
    native = native_roots[stage.target_relation]
    receipts.require(
        stage.schema == original.schema == directory._schema()
        and native["oid"] == original.oid == original.source_oid
        and native["relname"] == original.name == original.source == stage.target_relation,
        "initial_target_changed",
    )
    _session, _driver, actor = await receipts.ordinary(directory.db)
    receipts.require(
        retained.cutover_actor is not None and actor == retained.cutover_actor, "initial_cut_actor_changed"
    )
    await receipts.retain(directory.db, stage.schema, {"native": native})
    await acknowledge_initial_retained_relation(prepared.nonprofile_admission, directory, family=family)
    saved_name = receipts.retained_name(original.oid)
    retained.families = tuple(
        replace(relation, name=saved_name) if relation is original else relation for relation in retained.families
    )
