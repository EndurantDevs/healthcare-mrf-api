# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Declared native-observation doubles test guards, not PostgreSQL acceptance."""

from contextlib import asynccontextmanager
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from process.custom_import import family_shard_output_checkpoint as output
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from process.custom_import.family_block_layout import FamilyBlockLayout
from process.custom_import.family_block_store import ShardOutputProof
from process.custom_import.materialization import GenerationIdentity
from tests.test_custom_import_build_source import _request


def _observed(phase="output", storage="loading"):
    request = _request()
    layout = FamilyBlockLayout(request.definition, {"details": 7})
    stamp = datetime(2026, 1, 1, tzinfo=UTC)
    build = SimpleNamespace(
        phase=phase,
        generation_id=29,
        capture_bundle_id=8,
        source_frozen_at=stamp,
        plan_complete_at=stamp,
        graph_frozen_at=stamp,
        candidate_error_count=0,
        source_occurrence_count=3,
        producing_token_sha256=b"p" * 32,
        plan_stage="complete",
        output_frozen_at=stamp if phase == "verifying" else None,
    )
    generation = SimpleNamespace(
        generation_id=29,
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
        execution_id=request.execution_id,
        capture_bundle_id=8,
        base_generation_id=request.expected_base_generation_id,
        producing_fence=request.fence,
        producing_token_sha256=b"p" * 32,
        source_bundle_sha256=b"s" * 32,
        candidate_sha256=b"c" * 32,
        root_count=2,
        family_count=2,
        base_dataset_id=None if request.expected_base_generation_id is None else request.dataset_id,
        created_at=stamp,
    )
    owner = SimpleNamespace(
        source=SimpleNamespace(
            owner_profile=("declared-host-profile",),
            context=SimpleNamespace(request=request, family_id=17, build_id=11, layout=layout),
            allocated=SimpleNamespace(source=SimpleNamespace(counts=(3,))),
        ),
        generation=GenerationIdentity(
            29, request.dataset_id, request.definition_revision_id, request.schema_revision_id
        ),
    )
    descriptor = SimpleNamespace(phase=storage)
    if phase == "graph":
        build.generation_id = build.graph_frozen_at = owner.generation = None
    binding = SimpleNamespace(tables_by_role=layout.tables(17))
    return owner, build, generation, descriptor, binding


def _install(monkeypatch, observations):
    owner, build, generation, descriptor, binding = observations

    class Session:
        async def get(self, model, _identity, **_options):
            return build if model.__name__ == "CustomImportBuildAttempt" else generation

    session = Session()

    async def candidate(*_args, **_options):
        return descriptor, binding

    @asynccontextmanager
    async def page(*_args):
        yield session, build

    monkeypatch.setattr(output.store, "_candidate", candidate)
    monkeypatch.setattr(output, "_page_session", page)
    return owner, session


@pytest.mark.parametrize(
    "phase,storage",
    [
        ("graph", "loading"),
        ("output", "loading"),
        ("output", "closed"),
        ("verifying", "closed"),
        ("verifying", "verified"),
        ("verifying", "logged"),
    ],
)
async def test_existing_late_phase_pairs_require_original_native_source(monkeypatch, phase, storage):
    observations = _observed(phase, storage)
    owner, session = _install(monkeypatch, observations)
    assert (await output._native(session, owner))[0] is observations[1]
    observations[1].source_occurrence_count += 1
    with pytest.raises(FamilyBlockUnavailable, match="completed SOURCE"):
        await output._native(session, owner)


@pytest.mark.parametrize(
    "phase,storage",
    [
        ("source", "loading"),
        ("admission", "loading"),
        ("graph", "closed"),
        ("output", "verified"),
        ("output", "logged"),
        ("verifying", "loading"),
        ("verified", "logged"),
    ],
)
async def test_unknown_late_native_phase_never_grants_write_permission(monkeypatch, phase, storage):
    observations = _observed(phase, storage)
    owner, session = _install(monkeypatch, observations)
    with pytest.raises(FamilyBlockUnavailable, match="native phase"):
        await output._native(session, owner)


@pytest.mark.parametrize(
    "field", ["candidate_sha256", "source_bundle_sha256", "root_count", "family_count", "generation_id"]
)
async def test_original_generation_header_cannot_change_after_observation(monkeypatch, field):
    observations = _observed()
    owner, session = _install(monkeypatch, observations)
    await output._native(session, owner)
    generation = observations[2]
    setattr(generation, field, b"x" * 32 if field.endswith("sha256") else getattr(generation, field) + 1)
    with pytest.raises(FamilyBlockUnavailable, match="header or timestamp"):
        await output._native(session, owner)


@pytest.mark.parametrize("field", ["source_frozen_at", "plan_complete_at", "graph_frozen_at"])
async def test_original_native_completion_timestamps_cannot_change(monkeypatch, field):
    observations = _observed()
    owner, session = _install(monkeypatch, observations)
    await output._native(session, owner)
    setattr(observations[1], field, datetime(2026, 1, 2, tzinfo=UTC))
    with pytest.raises(FamilyBlockUnavailable, match="header or timestamp"):
        await output._native(session, owner)


@pytest.mark.parametrize(
    "storage,present", [("loading", False), ("loading", True), ("closed", True), ("verified", True), ("logged", True)]
)
async def test_receipt_observation_is_exact_and_closed_cannot_be_missing(monkeypatch, storage, present):
    observations = _observed("output" if storage in {"loading", "closed"} else "verifying", storage)
    owner, _session = _install(monkeypatch, observations)
    receipt = ShardOutputProof(owner.source.context.layout, 9, b"r" * 32, 0).receipt()
    row = {**receipt, "completed_at": datetime(2026, 1, 1, tzinfo=UTC)} if present else None

    async def execute(*_args):
        return SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: row))

    monkeypatch.setattr(output.store, "_execute", execute)
    assert await output.has_copy_receipt(None, owner, receipt) is present
    if present:
        row["result_sha256"] = b"z" * 32
        with pytest.raises(FamilyBlockUnavailable, match="receipt differs"):
            await output.has_copy_receipt(None, owner, receipt)


@pytest.mark.parametrize("storage", ["closed", "verified", "logged"])
async def test_closed_missing_receipt_refuses_recopy(monkeypatch, storage):
    observations = _observed("output" if storage == "closed" else "verifying", storage)
    owner, _session = _install(monkeypatch, observations)
    receipt = ShardOutputProof(owner.source.context.layout, 9, b"r" * 32, 0).receipt()

    async def execute(*_args):
        return SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: None))

    monkeypatch.setattr(output.store, "_execute", execute)
    with pytest.raises(FamilyBlockUnavailable, match="missing an original COPY"):
        await output.has_copy_receipt(None, owner, receipt)
