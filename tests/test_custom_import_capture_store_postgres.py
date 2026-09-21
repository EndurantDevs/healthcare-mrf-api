# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Opt-in native PostgreSQL proof for generic sealed capture registration."""

from __future__ import annotations

import asyncio
import hashlib
import json
import uuid
from dataclasses import dataclass
from pathlib import Path

import pytest
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from db.models.custom_import import (
    CustomImportCapture,
    CustomImportCaptureBundle,
    CustomImportDataset,
    CustomImportDefinitionRevision,
    CustomImportExecution,
    CustomImportLease,
    CustomImportSchemaRevision,
    CustomImportSourceStream,
)
from process.custom_import import execution as lifecycle
from process.custom_import.capture_store import (
    CaptureBundleConflict,
    CaptureReceipt,
    CaptureStoreError,
    register_capture_bundle,
)
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.definition_store import register_definition
from tests.custom_import_postgres_support import isolated_publication_case

_FIXTURE = Path(__file__).with_name("fixtures") / "custom_import" / "v1_valid.json"


@dataclass(frozen=True)
class _Seed:
    dataset_id: int
    definition_revision_id: int
    schema_revision_id: int


def _digest(label: str) -> bytes:
    return hashlib.sha256(label.encode("utf-8")).digest()


def _build_receipt(stream_id: str, *, snapshot: str = "synthetic-snapshot-1", suffix: str = "") -> CaptureReceipt:
    canonical_manifest = json.dumps(
        {"connector": "synthetic", "stream": stream_id, "version": 1},
        separators=(",", ":"),
        sort_keys=True,
    )
    return CaptureReceipt(
        stream_id=stream_id,
        source_snapshot_token=snapshot,
        byte_count=17 + len(stream_id),
        content_sha256=_digest(f"content:{stream_id}:{suffix}").hex(),
        canonical_manifest=canonical_manifest,
        manifest_sha256=_digest(f"manifest:{stream_id}:{suffix}").hex(),
    )


def _build_receipt_set(*, snapshot: str = "synthetic-snapshot-1", suffix: str = "") -> tuple[CaptureReceipt, ...]:
    return (
        _build_receipt("providers", snapshot=snapshot, suffix=suffix),
        _build_receipt("rates", snapshot=snapshot, suffix=suffix),
    )


async def _register_synthetic_definition(session: AsyncSession) -> _Seed:
    """Register the exact parsed v1 fixture through the production definition store."""

    suffix = uuid.uuid4().hex[:16]
    registration = await register_definition(
        session,
        f"synthetic_capture_store_{suffix}",
        CustomImportDefinition.from_json(_FIXTURE.read_text()),
    )
    return _Seed(
        dataset_id=registration.dataset_id,
        definition_revision_id=registration.definition_revision_id,
        schema_revision_id=registration.schema_revision_id,
    )


async def _register_bundle(case, seed: _Seed, receipts: tuple[CaptureReceipt, ...]):
    async with case.sessions() as session:
        async with session.begin():
            return await register_capture_bundle(
                session,
                dataset_id=seed.dataset_id,
                definition_revision_id=seed.definition_revision_id,
                schema_revision_id=seed.schema_revision_id,
                receipts=receipts,
            )


async def _bundle_count(case, seed: _Seed) -> int:
    async with case.sessions() as session:
        return len(
            (
                await session.execute(
                    select(CustomImportCaptureBundle).where(
                        CustomImportCaptureBundle.dataset_id == seed.dataset_id,
                        CustomImportCaptureBundle.definition_revision_id == seed.definition_revision_id,
                        CustomImportCaptureBundle.schema_revision_id == seed.schema_revision_id,
                    )
                )
            )
            .scalars()
            .all()
        )


async def _seed_case(case) -> _Seed:
    async with case.sessions() as session:
        async with session.begin():
            return await _register_synthetic_definition(session)


async def _seed_incomplete_definition_graph(session: AsyncSession) -> _Seed:
    """Insert a parseable definition whose durable descendants are incomplete."""

    suffix = uuid.uuid4().hex[:16]
    definition = CustomImportDefinition.from_json(_FIXTURE.read_text())
    dataset = CustomImportDataset(dataset_key=f"synthetic_incomplete_capture_{suffix}")
    session.add(dataset)
    await session.flush()
    schema = CustomImportSchemaRevision(
        dataset_id=dataset.dataset_id,
        revision_number=definition.schema_revision,
        canonical_schema=definition.schema_canonical,
        schema_sha256=bytes.fromhex(definition.schema_digest),
    )
    session.add(schema)
    await session.flush()
    definition_model = CustomImportDefinitionRevision(
        dataset_id=dataset.dataset_id,
        schema_revision_id=schema.schema_revision_id,
        revision_number=definition.definition_revision,
        contract_version="custom-import/v1",
        refresh_mode=definition.refresh_mode,
        canonical_definition=definition.canonical,
        definition_sha256=bytes.fromhex(definition.digest),
    )
    session.add(definition_model)
    await session.flush()
    session.add(
        CustomImportSourceStream(
            definition_revision_id=definition_model.definition_revision_id,
            dataset_id=dataset.dataset_id,
            schema_revision_id=schema.schema_revision_id,
            stream_slot=1,
            stream_id="providers",
            record_kind="root",
            decoder="csv",
            compression="none",
            snapshot_token_selector="snapshot_id",
        )
    )
    await session.flush()
    return _Seed(dataset.dataset_id, definition_model.definition_revision_id, schema.schema_revision_id)


async def test_register_capture_bundle_persists_complete_scope_and_replays_exact_ids():
    async with isolated_publication_case() as case:
        seed = await _seed_case(case)
        first = await _register_bundle(case, seed, _build_receipt_set())
        replay = await _register_bundle(case, seed, _build_receipt_set())

        assert first.created is True
        assert replay.created is False
        assert replay.capture_bundle_id == first.capture_bundle_id
        assert first.stream_slots == replay.stream_slots == (1, 2)
        async with case.sessions() as session:
            bundle = await session.get(CustomImportCaptureBundle, first.capture_bundle_id)
            captures = (
                (
                    await session.execute(
                        select(CustomImportCapture)
                        .where(CustomImportCapture.capture_bundle_id == first.capture_bundle_id)
                        .order_by(CustomImportCapture.stream_slot)
                    )
                )
                .scalars()
                .all()
            )

        assert bundle is not None
        assert bundle.snapshot_token == "synthetic-snapshot-1"
        assert bundle.stream_count == 2
        assert tuple(capture.stream_slot for capture in captures) == (1, 2)
        assert tuple(capture.canonical_manifest for capture in captures) == tuple(
            receipt.canonical_manifest for receipt in _build_receipt_set()
        )


async def test_register_capture_bundle_rejects_partial_scope_snapshot_drift_and_receipt_drift():
    async with isolated_publication_case() as case:
        seed = await _seed_case(case)
        with pytest.raises(CaptureStoreError, match="exactly cover"):
            await _register_bundle(case, seed, _build_receipt_set()[:1])
        with pytest.raises(CaptureStoreError, match="share one exact snapshot"):
            await _register_bundle(
                case,
                seed,
                (
                    _build_receipt("providers", snapshot="synthetic-a"),
                    _build_receipt("rates", snapshot="synthetic-b"),
                ),
            )
        assert await _bundle_count(case, seed) == 0

        registered = await _register_bundle(case, seed, _build_receipt_set())
        with pytest.raises(CaptureBundleConflict, match="different or partial"):
            await _register_bundle(case, seed, _build_receipt_set(suffix="drift"))

        assert await _bundle_count(case, seed) == 1
        assert registered.created is True


async def _register_in_transaction(session: AsyncSession, seed: _Seed, receipts: tuple[CaptureReceipt, ...]):
    async with session.begin():
        return await register_capture_bundle(
            session,
            dataset_id=seed.dataset_id,
            definition_revision_id=seed.definition_revision_id,
            schema_revision_id=seed.schema_revision_id,
            receipts=receipts,
        )


async def _exercise_concurrent_replay(case, seed: _Seed, receipts: tuple[CaptureReceipt, ...]):
    async with case.sessions() as first_session, case.sessions() as second_session:
        transaction = await first_session.begin()
        replay_task = None
        try:
            first = await register_capture_bundle(
                first_session,
                dataset_id=seed.dataset_id,
                definition_revision_id=seed.definition_revision_id,
                schema_revision_id=seed.schema_revision_id,
                receipts=receipts,
            )
            replay_task = asyncio.create_task(_register_in_transaction(second_session, seed, receipts))
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(asyncio.shield(replay_task), timeout=0.1)
            await transaction.commit()
            replay = await asyncio.wait_for(replay_task, timeout=5)
            return first, replay
        finally:
            if transaction.is_active:
                await transaction.rollback()
            if replay_task is not None and not replay_task.done():
                replay_task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await replay_task


async def _attempt_incomplete_capture_registration(case):
    async with case.sessions() as session:
        async with session.begin():
            seed = await _seed_incomplete_definition_graph(session)
            return await register_capture_bundle(
                session,
                dataset_id=seed.dataset_id,
                definition_revision_id=seed.definition_revision_id,
                schema_revision_id=seed.schema_revision_id,
                receipts=(_build_receipt("providers"),),
            )


async def test_concurrent_exact_replays_serialize_on_the_dataset_lock():
    async with isolated_publication_case() as case:
        seed = await _seed_case(case)
        first, replay = await _exercise_concurrent_replay(case, seed, _build_receipt_set())

        assert first.created is True
        assert replay.created is False
        assert replay.capture_bundle_id == first.capture_bundle_id


async def test_capture_registration_rejects_an_incomplete_definition_graph():
    async with isolated_publication_case() as case:
        with pytest.raises(CaptureStoreError, match="definition graph is invalid"):
            await _attempt_incomplete_capture_registration(case)


@pytest.mark.asyncio
async def test_stale_execution_bind_rolls_back_the_enclosing_capture_registration():
    async with isolated_publication_case() as case:
        seed = await _seed_case(case)
        stale_owner = "synthetic-stale-owner"
        recovery_identity = "synthetic-recovery-owner"
        async with case.sessions() as session:
            async with session.begin():
                reserved = await lifecycle.reserve_execution(
                    session,
                    dataset_id=seed.dataset_id,
                    definition_revision_id=seed.definition_revision_id,
                    schema_revision_id=seed.schema_revision_id,
                    idempotency_key="synthetic-stale-bind",
                    mechanism="local",
                )
                first_owner = await lifecycle.claim_execution(
                    session,
                    execution_id=reserved.execution_id,
                    token=stale_owner,
                    lease_seconds=60,
                )
        assert first_owner is not None

        async with case.sessions() as session:
            async with session.begin():
                await session.execute(
                    update(CustomImportLease)
                    .where(CustomImportLease.execution_id == reserved.execution_id)
                    .values(expires_at=func.clock_timestamp())
                )
        async with case.sessions() as session:
            async with session.begin():
                recovery_owner = await lifecycle.claim_execution(
                    session,
                    execution_id=reserved.execution_id,
                    token=recovery_identity,
                    lease_seconds=60,
                )
        assert recovery_owner is not None and recovery_owner.fence == first_owner.fence + 1

        with pytest.raises(RuntimeError, match="rollback stale bind"):
            async with case.sessions() as session:
                async with session.begin():
                    bundle = await register_capture_bundle(
                        session,
                        dataset_id=seed.dataset_id,
                        definition_revision_id=seed.definition_revision_id,
                        schema_revision_id=seed.schema_revision_id,
                        receipts=_build_receipt_set(),
                    )
                    binding = await lifecycle.bind_execution_capture_bundle(
                        session,
                        execution_id=reserved.execution_id,
                        dataset_id=seed.dataset_id,
                        definition_revision_id=seed.definition_revision_id,
                        schema_revision_id=seed.schema_revision_id,
                        capture_bundle_id=bundle.capture_bundle_id,
                        fence=first_owner.fence,
                        token=stale_owner,
                    )
                    assert binding is None
                    raise RuntimeError("rollback stale bind")

        async with case.sessions() as session:
            bundles = await session.scalar(
                select(func.count()).select_from(CustomImportCaptureBundle).where(
                    CustomImportCaptureBundle.dataset_id == seed.dataset_id
                )
            )
            captures = await session.scalar(
                select(func.count()).select_from(CustomImportCapture).where(
                    CustomImportCapture.dataset_id == seed.dataset_id
                )
            )
            execution = await session.get(CustomImportExecution, reserved.execution_id)

        assert bundles == captures == 0
        assert execution is not None and execution.capture_bundle_id is None

        async with case.sessions() as session:
            async with session.begin():
                recovery_bundle = await register_capture_bundle(
                    session,
                    dataset_id=seed.dataset_id,
                    definition_revision_id=seed.definition_revision_id,
                    schema_revision_id=seed.schema_revision_id,
                    receipts=_build_receipt_set(),
                )
                binding = await lifecycle.bind_execution_capture_bundle(
                    session,
                    execution_id=reserved.execution_id,
                    dataset_id=seed.dataset_id,
                    definition_revision_id=seed.definition_revision_id,
                    schema_revision_id=seed.schema_revision_id,
                    capture_bundle_id=recovery_bundle.capture_bundle_id,
                    fence=recovery_owner.fence,
                    token=recovery_identity,
                )
                assert binding is not None
                assert binding.execution_id == reserved.execution_id
                assert binding.capture_bundle_id == recovery_bundle.capture_bundle_id

        async with case.sessions() as session:
            async with session.begin():
                replayed = await lifecycle.reserve_execution(
                    session,
                    dataset_id=seed.dataset_id,
                    definition_revision_id=seed.definition_revision_id,
                    schema_revision_id=seed.schema_revision_id,
                    idempotency_key="synthetic-stale-bind",
                    mechanism="local",
                )

        assert replayed.created is False
        assert replayed.execution_id == reserved.execution_id
        assert replayed.capture_bundle_id == recovery_bundle.capture_bundle_id
