# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Opt-in native PostgreSQL proof for generic sealed capture registration."""

from __future__ import annotations

import asyncio
import hashlib
import importlib.util
import json
import uuid
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import func, inspect, select, update
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncSession

from db.models.custom_import import (
    CustomImportCapture,
    CustomImportCaptureBundle,
    CustomImportCaptureParquetPart,
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
    CapturePayloadUnavailable,
    CaptureReceipt,
    CaptureStoreError,
    ReplayableParquetCapture,
    load_replayable_parquet_bundle,
    register_capture_bundle,
    register_replayable_parquet_bundle,
)
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.definition_store import register_definition
from tests.custom_import_postgres_support import isolated_publication_case

_FIXTURE = Path(__file__).with_name("fixtures") / "custom_import" / "v1_valid.json"
_DURABLE_CAPTURE_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic"
    / "versions"
    / "20260922000000_custom_import_durable_parquet_capture.py"
)


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


def _synthetic_parquet_part(stream_id: str, ordinal: int) -> bytes:
    output = BytesIO()
    pq.write_table(pa.table({"ordinal": [ordinal], "stream_id": [stream_id]}), output)
    return output.getvalue()


def _schema_bearing_zero_row_parquet_part() -> bytes:
    schema = pa.schema((pa.field("ordinal", pa.int64()), pa.field("stream_id", pa.string())))
    table = pa.Table.from_arrays(
        (pa.array((), type=pa.int64()), pa.array((), type=pa.string())),
        schema=schema,
    )
    output = BytesIO()
    pq.write_table(table, output)
    return output.getvalue()


def _build_replayable_capture(
    stream_id: str,
    parts: tuple[bytes, ...],
    *,
    snapshot: str = "synthetic-parquet-snapshot-1",
) -> ReplayableParquetCapture:
    canonical_manifest = json.dumps(
        {"connector": "synthetic-parquet", "part_count": len(parts), "stream": stream_id, "version": 1},
        separators=(",", ":"),
        sort_keys=True,
    )
    receipt = CaptureReceipt(
        stream_id=stream_id,
        source_snapshot_token=snapshot,
        byte_count=sum(len(part) for part in parts),
        content_sha256=hashlib.sha256(b"".join(parts)).hexdigest(),
        canonical_manifest=canonical_manifest,
        manifest_sha256=hashlib.sha256(canonical_manifest.encode("utf-8")).hexdigest(),
    )
    return ReplayableParquetCapture(receipt=receipt, parts=parts)


def _build_replayable_capture_set() -> tuple[ReplayableParquetCapture, ...]:
    return (
        _build_replayable_capture(
            "providers",
            (_synthetic_parquet_part("providers", 1), _synthetic_parquet_part("providers", 2)),
        ),
        _build_replayable_capture("rates", (_synthetic_parquet_part("rates", 1),)),
    )


def _parquet_definition() -> CustomImportDefinition:
    definition_document = json.loads(_FIXTURE.read_text())
    for stream in definition_document["streams"]:
        stream["format"] = "parquet"
        stream["compression"] = "none"
    return CustomImportDefinition.from_mapping(definition_document)


def _downgrade_durable_capture(sync_connection, schema_name: str) -> None:
    spec = importlib.util.spec_from_file_location(
        "custom_import_durable_capture_downgrade",
        _DURABLE_CAPTURE_MIGRATION_PATH,
    )
    assert spec and spec.loader
    migration = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration)
    migration._schema = lambda: schema_name
    migration.op = Operations(MigrationContext.configure(sync_connection))
    migration.downgrade()


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


async def _register_parquet_synthetic_definition(session: AsyncSession) -> _Seed:
    suffix = uuid.uuid4().hex[:16]
    registration = await register_definition(
        session,
        f"synthetic_durable_capture_{suffix}",
        _parquet_definition(),
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


async def _register_replayable_bundle(case, seed: _Seed, captures: tuple[ReplayableParquetCapture, ...]):
    async with case.sessions() as session:
        async with session.begin():
            return await register_replayable_parquet_bundle(
                session,
                dataset_id=seed.dataset_id,
                definition_revision_id=seed.definition_revision_id,
                schema_revision_id=seed.schema_revision_id,
                captures=captures,
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


async def _seed_parquet_case(case) -> _Seed:
    async with case.sessions() as session:
        async with session.begin():
            return await _register_parquet_synthetic_definition(session)


async def _stage_durable_bundle(
    session: AsyncSession,
    seed: _Seed,
    captures: tuple[ReplayableParquetCapture, ...],
    *,
    payload_set_sha256: bytes,
    omitted_part: tuple[str, int] | None = None,
    durable_stream_ids: frozenset[str] | None = None,
    staged_parts: tuple[tuple[str, int, bytes], ...] | None = None,
) -> None:
    """Stage a database-level durable bundle for one deferred-guard assertion."""

    durable_stream_ids = (
        frozenset(capture.receipt.stream_id for capture in captures)
        if durable_stream_ids is None
        else durable_stream_ids
    )
    source_slots = dict(
        (
            await session.execute(
                select(CustomImportSourceStream.stream_id, CustomImportSourceStream.stream_slot).where(
                    CustomImportSourceStream.dataset_id == seed.dataset_id,
                    CustomImportSourceStream.definition_revision_id == seed.definition_revision_id,
                    CustomImportSourceStream.schema_revision_id == seed.schema_revision_id,
                )
            )
        ).all()
    )
    snapshot_token = captures[0].receipt.source_snapshot_token
    bundle_manifest = json.dumps(
        {"contract": "synthetic-durable", "snapshot": snapshot_token},
        separators=(",", ":"),
        sort_keys=True,
    )
    bundle = CustomImportCaptureBundle(
        dataset_id=seed.dataset_id,
        definition_revision_id=seed.definition_revision_id,
        schema_revision_id=seed.schema_revision_id,
        snapshot_token=snapshot_token,
        snapshot_token_sha256=hashlib.sha256(snapshot_token.encode("utf-8")).digest(),
        canonical_manifest=bundle_manifest,
        manifest_sha256=hashlib.sha256(bundle_manifest.encode("utf-8")).digest(),
        stream_count=len(captures),
    )
    session.add(bundle)
    await session.flush()
    assert isinstance(bundle.capture_bundle_id, int)

    session.add_all(
        CustomImportCapture(
            capture_bundle_id=bundle.capture_bundle_id,
            dataset_id=seed.dataset_id,
            definition_revision_id=seed.definition_revision_id,
            schema_revision_id=seed.schema_revision_id,
            stream_slot=source_slots[capture.receipt.stream_id],
            content_sha256=bytes.fromhex(capture.receipt.content_sha256),
            byte_count=capture.receipt.byte_count,
            canonical_manifest=capture.receipt.canonical_manifest,
            manifest_sha256=bytes.fromhex(capture.receipt.manifest_sha256),
            payload_contract=(
                "custom-import/parquet-parts/v1" if capture.receipt.stream_id in durable_stream_ids else None
            ),
            payload_part_count=(len(capture.parts) if capture.receipt.stream_id in durable_stream_ids else None),
            payload_set_sha256=(payload_set_sha256 if capture.receipt.stream_id in durable_stream_ids else None),
        )
        for capture in captures
    )
    await session.flush()
    if staged_parts is None:
        staged_parts = tuple(
            (capture.receipt.stream_id, ordinal, payload)
            for capture in captures
            if capture.receipt.stream_id in durable_stream_ids
            for ordinal, payload in enumerate(capture.parts, start=1)
            if (capture.receipt.stream_id, ordinal) != omitted_part
        )
    session.add_all(
        CustomImportCaptureParquetPart(
            capture_bundle_id=bundle.capture_bundle_id,
            stream_slot=source_slots[stream_id],
            part_ordinal=ordinal,
            byte_count=len(payload),
            payload=payload,
            payload_sha256=hashlib.sha256(payload).digest(),
        )
        for stream_id, ordinal, payload in staged_parts
    )
    await session.flush()


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


async def test_register_and_load_replayable_parquet_bundle_in_a_fresh_session():
    async with isolated_publication_case() as case:
        seed = await _seed_parquet_case(case)
        captures = _build_replayable_capture_set()
        first = await _register_replayable_bundle(case, seed, captures)
        replay = await _register_replayable_bundle(case, seed, captures)

        assert first.created is True
        assert replay.created is False
        assert replay.capture_bundle_id == first.capture_bundle_id
        async with case.sessions() as session:
            loaded = await load_replayable_parquet_bundle(
                session,
                capture_bundle_id=first.capture_bundle_id,
                dataset_id=seed.dataset_id,
                definition_revision_id=seed.definition_revision_id,
                schema_revision_id=seed.schema_revision_id,
            )
            part_count = await session.scalar(
                select(func.count())
                .select_from(CustomImportCaptureParquetPart)
                .where(CustomImportCaptureParquetPart.capture_bundle_id == first.capture_bundle_id)
            )

        assert tuple(capture.receipt for capture in loaded) == tuple(capture.receipt for capture in captures)
        assert tuple(capture.parts for capture in loaded) == tuple(capture.parts for capture in captures)
        assert part_count == 3


async def test_replayable_parquet_round_trip_preserves_schema_bearing_zero_row_parts():
    async with isolated_publication_case() as case:
        seed = await _seed_parquet_case(case)
        zero_row_part = _schema_bearing_zero_row_parquet_part()
        captures = tuple(_build_replayable_capture(stream_id, (zero_row_part,)) for stream_id in ("providers", "rates"))
        registered = await _register_replayable_bundle(case, seed, captures)
        async with case.sessions() as session:
            loaded = await load_replayable_parquet_bundle(
                session,
                capture_bundle_id=registered.capture_bundle_id,
                dataset_id=seed.dataset_id,
                definition_revision_id=seed.definition_revision_id,
                schema_revision_id=seed.schema_revision_id,
            )

    expected_schema = pa.schema((pa.field("ordinal", pa.int64()), pa.field("stream_id", pa.string())))
    assert all(pq.read_table(BytesIO(capture.parts[0])).num_rows == 0 for capture in loaded)
    assert all(pq.read_table(BytesIO(capture.parts[0])).schema == expected_schema for capture in loaded)


async def test_durable_registration_refuses_metadata_only_and_payload_drift():
    async with isolated_publication_case() as case:
        seed = await _seed_parquet_case(case)
        captures = _build_replayable_capture_set()
        await _register_bundle(case, seed, tuple(capture.receipt for capture in captures))

        with pytest.raises(CapturePayloadUnavailable, match="no durable payload"):
            await _register_replayable_bundle(case, seed, captures)

    async with isolated_publication_case() as case:
        seed = await _seed_parquet_case(case)
        captures = _build_replayable_capture_set()
        await _register_replayable_bundle(case, seed, captures)
        first = captures[0]
        changed_payload = first.parts[0][:-1] + bytes((first.parts[0][-1] ^ 1,))
        drifted = (
            ReplayableParquetCapture(receipt=first.receipt, parts=(changed_payload, *first.parts[1:])),
            captures[1],
        )

        with pytest.raises(CaptureBundleConflict, match="durable capture bundle"):
            await _register_replayable_bundle(case, seed, drifted)


async def test_durable_registration_requires_persisted_parquet_without_compression():
    async with isolated_publication_case() as case:
        seed = await _seed_case(case)
        with pytest.raises(CaptureStoreError, match="Parquet streams without compression"):
            await _register_replayable_bundle(case, seed, _build_replayable_capture_set())


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


async def _register_replayable_in_transaction(
    session: AsyncSession,
    seed: _Seed,
    captures: tuple[ReplayableParquetCapture, ...],
):
    async with session.begin():
        return await register_replayable_parquet_bundle(
            session,
            dataset_id=seed.dataset_id,
            definition_revision_id=seed.definition_revision_id,
            schema_revision_id=seed.schema_revision_id,
            captures=captures,
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


async def _exercise_concurrent_replayable_parquet_registration(
    case,
    seed: _Seed,
    captures: tuple[ReplayableParquetCapture, ...],
):
    async with case.sessions() as first_session, case.sessions() as second_session:
        transaction = await first_session.begin()
        replay_task = None
        try:
            first = await register_replayable_parquet_bundle(
                first_session,
                dataset_id=seed.dataset_id,
                definition_revision_id=seed.definition_revision_id,
                schema_revision_id=seed.schema_revision_id,
                captures=captures,
            )
            replay_task = asyncio.create_task(_register_replayable_in_transaction(second_session, seed, captures))
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


async def test_concurrent_exact_durable_replays_serialize_on_the_dataset_lock():
    async with isolated_publication_case() as case:
        seed = await _seed_parquet_case(case)
        first, replay = await _exercise_concurrent_replayable_parquet_registration(
            case,
            seed,
            _build_replayable_capture_set(),
        )

        assert first.created is True
        assert replay.created is False
        assert replay.capture_bundle_id == first.capture_bundle_id


async def test_durable_registration_rolls_back_all_parts_with_the_caller_transaction():
    async with isolated_publication_case() as case:
        seed = await _seed_parquet_case(case)
        with pytest.raises(RuntimeError, match="rollback durable capture"):
            async with case.sessions() as session:
                async with session.begin():
                    registered = await register_replayable_parquet_bundle(
                        session,
                        dataset_id=seed.dataset_id,
                        definition_revision_id=seed.definition_revision_id,
                        schema_revision_id=seed.schema_revision_id,
                        captures=_build_replayable_capture_set(),
                    )
                    assert registered.created is True
                    raise RuntimeError("rollback durable capture")

        assert await _durable_capture_registration_counts(case, seed) == (0, 0, 0)


async def test_deferred_durable_guard_rejects_a_wrong_ordered_payload_set_digest_at_commit():
    async with isolated_publication_case() as case:
        seed = await _seed_parquet_case(case)
        with pytest.raises(DBAPIError, match="custom_import_capture_parquet_payload_set_digest_mismatch"):
            async with case.sessions() as session:
                async with session.begin():
                    await _stage_durable_bundle(
                        session,
                        seed,
                        _build_replayable_capture_set(),
                        payload_set_sha256=b"\x00" * 32,
                    )


async def test_deferred_durable_guard_rejects_a_missing_part_at_commit():
    async with isolated_publication_case() as case:
        seed = await _seed_parquet_case(case)
        with pytest.raises(DBAPIError, match="custom_import_capture_parquet_parts_incomplete"):
            async with case.sessions() as session:
                async with session.begin():
                    await _stage_durable_bundle(
                        session,
                        seed,
                        _build_replayable_capture_set(),
                        payload_set_sha256=b"\x00" * 32,
                        omitted_part=("providers", 2),
                    )


async def test_deferred_durable_guard_rejects_a_missing_one_based_first_part_at_commit():
    async with isolated_publication_case() as case:
        seed = await _seed_parquet_case(case)
        with pytest.raises(DBAPIError, match="custom_import_capture_parquet_parts_incomplete"):
            async with case.sessions() as session:
                async with session.begin():
                    await _stage_durable_bundle(
                        session,
                        seed,
                        _build_replayable_capture_set(),
                        payload_set_sha256=b"\x00" * 32,
                        omitted_part=("providers", 1),
                    )


async def test_deferred_durable_guard_rejects_terminal_first_parts_at_commit():
    async with isolated_publication_case() as case:
        seed = await _seed_parquet_case(case)
        captures = _build_replayable_capture_set()
        with pytest.raises(DBAPIError, match="custom_import_capture_parquet_parts_incomplete"):
            async with case.sessions() as session:
                async with session.begin():
                    await _stage_durable_bundle(
                        session,
                        seed,
                        captures,
                        payload_set_sha256=b"\x00" * 32,
                        staged_parts=(
                            ("providers", 2, captures[0].parts[1]),
                            ("rates", 1, captures[1].parts[0]),
                        ),
                    )


async def test_part_shape_guard_rejects_explicit_ordinal_zero():
    async with isolated_publication_case() as case:
        seed = await _seed_parquet_case(case)
        captures = _build_replayable_capture_set()
        with pytest.raises(DBAPIError, match="custom_import_capture_parquet_part_shape_check"):
            async with case.sessions() as session:
                async with session.begin():
                    await _stage_durable_bundle(
                        session,
                        seed,
                        captures,
                        payload_set_sha256=b"\x00" * 32,
                        staged_parts=(("providers", 0, captures[0].parts[0]),),
                    )


async def test_deferred_durable_guard_rejects_mixed_legacy_and_durable_captures():
    async with isolated_publication_case() as case:
        seed = await _seed_parquet_case(case)
        with pytest.raises(DBAPIError, match="custom_import_capture_parquet_bundle_incomplete"):
            async with case.sessions() as session:
                async with session.begin():
                    await _stage_durable_bundle(
                        session,
                        seed,
                        _build_replayable_capture_set(),
                        payload_set_sha256=b"\x00" * 32,
                        durable_stream_ids=frozenset(("rates",)),
                    )


async def test_durable_part_guards_enforce_parent_shape_completeness_and_immutability():
    async with isolated_publication_case() as case:
        parquet_seed = await _seed_parquet_case(case)
        captures = _build_replayable_capture_set()
        durable = await _register_replayable_bundle(case, parquet_seed, captures)

        async with case.sessions() as session:
            with pytest.raises(DBAPIError, match="custom_import_capture_payload_shape_check"):
                async with session.begin():
                    bundle = CustomImportCaptureBundle(
                        dataset_id=parquet_seed.dataset_id,
                        definition_revision_id=parquet_seed.definition_revision_id,
                        schema_revision_id=parquet_seed.schema_revision_id,
                        snapshot_token="synthetic-partial-payload",
                        snapshot_token_sha256=_digest("partial-payload-snapshot"),
                        canonical_manifest='{"contract":"synthetic","partial":true}',
                        manifest_sha256=_digest("partial-payload-manifest"),
                        stream_count=2,
                    )
                    session.add(bundle)
                    await session.flush()
                    session.add(
                        CustomImportCapture(
                            capture_bundle_id=bundle.capture_bundle_id,
                            dataset_id=parquet_seed.dataset_id,
                            definition_revision_id=parquet_seed.definition_revision_id,
                            schema_revision_id=parquet_seed.schema_revision_id,
                            stream_slot=1,
                            content_sha256=_digest("partial-payload-content"),
                            byte_count=1,
                            canonical_manifest='{"connector":"synthetic","partial":true}',
                            manifest_sha256=_digest("partial-payload-capture-manifest"),
                            payload_part_count=1,
                        )
                    )
                    await session.flush()

        async with case.sessions() as session:
            with pytest.raises(DBAPIError, match="custom_import_capture_parquet_part_shape_check"):
                async with session.begin():
                    bundle = CustomImportCaptureBundle(
                        dataset_id=parquet_seed.dataset_id,
                        definition_revision_id=parquet_seed.definition_revision_id,
                        schema_revision_id=parquet_seed.schema_revision_id,
                        snapshot_token="synthetic-invalid-part",
                        snapshot_token_sha256=_digest("invalid-part-snapshot"),
                        canonical_manifest='{"contract":"synthetic","invalid_part":true}',
                        manifest_sha256=_digest("invalid-part-bundle-manifest"),
                        stream_count=2,
                    )
                    session.add(bundle)
                    await session.flush()
                    session.add(
                        CustomImportCapture(
                            capture_bundle_id=bundle.capture_bundle_id,
                            dataset_id=parquet_seed.dataset_id,
                            definition_revision_id=parquet_seed.definition_revision_id,
                            schema_revision_id=parquet_seed.schema_revision_id,
                            stream_slot=1,
                            content_sha256=_digest("invalid-part-content"),
                            byte_count=1,
                            canonical_manifest='{"connector":"synthetic","invalid_part":true}',
                            manifest_sha256=_digest("invalid-part-capture-manifest"),
                            payload_contract="custom-import/parquet-parts/v1",
                            payload_part_count=1,
                            payload_set_sha256=_digest("invalid-part-payload-set"),
                        )
                    )
                    await session.flush()
                    session.add(
                        CustomImportCaptureParquetPart(
                            capture_bundle_id=bundle.capture_bundle_id,
                            stream_slot=1,
                            part_ordinal=1,
                            byte_count=1,
                            payload=b"x",
                            payload_sha256=b"\x00" * 32,
                        )
                    )
                    await session.flush()

        async with case.sessions() as session:
            with pytest.raises(DBAPIError, match="custom_import_capture_parquet_part_already_complete"):
                async with session.begin():
                    payload = b"extra-valid-part"
                    session.add(
                        CustomImportCaptureParquetPart(
                            capture_bundle_id=durable.capture_bundle_id,
                            stream_slot=1,
                            part_ordinal=3,
                            byte_count=len(payload),
                            payload=payload,
                            payload_sha256=hashlib.sha256(payload).digest(),
                        )
                    )

        async with case.sessions() as session:
            with pytest.raises(DBAPIError, match="custom_import_immutable_row"):
                async with session.begin():
                    await session.execute(
                        update(CustomImportCaptureParquetPart)
                        .where(
                            CustomImportCaptureParquetPart.capture_bundle_id == durable.capture_bundle_id,
                            CustomImportCaptureParquetPart.stream_slot == 1,
                            CustomImportCaptureParquetPart.part_ordinal == 1,
                        )
                        .values(payload=b"attempted-part-mutation")
                    )

        metadata_seed = await _seed_parquet_case(case)
        metadata_captures = _build_replayable_capture_set()
        metadata = await _register_bundle(case, metadata_seed, tuple(capture.receipt for capture in metadata_captures))
        async with case.sessions() as session:
            with pytest.raises(DBAPIError, match="custom_import_capture_parquet_part_parent_invalid"):
                async with session.begin():
                    payload = b"part-under-metadata-only-capture"
                    session.add(
                        CustomImportCaptureParquetPart(
                            capture_bundle_id=metadata.capture_bundle_id,
                            stream_slot=1,
                            part_ordinal=1,
                            byte_count=len(payload),
                            payload=payload,
                            payload_sha256=hashlib.sha256(payload).digest(),
                        )
                    )
                    await session.flush()


async def test_durable_capture_migration_refuses_downgrade_when_payload_exists():
    async with isolated_publication_case() as case:
        seed = await _seed_parquet_case(case)
        await _register_replayable_bundle(case, seed, _build_replayable_capture_set())
        async with case.engine.connect() as connection:
            transaction = await connection.begin()
            try:
                with pytest.raises(DBAPIError, match="custom_import_capture_parquet_downgrade_blocked"):
                    await connection.run_sync(_downgrade_durable_capture, case.schema_name)
            finally:
                if transaction.is_active:
                    await transaction.rollback()


async def test_durable_capture_migration_downgrades_legacy_all_null_captures():
    async with isolated_publication_case() as case:
        seed = await _seed_parquet_case(case)
        registered = await _register_bundle(case, seed, _build_receipt_set())
        async with case.sessions() as session:
            captures = (
                (
                    await session.execute(
                        select(CustomImportCapture).where(
                            CustomImportCapture.capture_bundle_id == registered.capture_bundle_id
                        )
                    )
                )
                .scalars()
                .all()
            )
        assert all(
            capture.payload_contract is None
            and capture.payload_part_count is None
            and capture.payload_set_sha256 is None
            for capture in captures
        )
        async with case.engine.connect() as connection:
            async with connection.begin():
                await connection.run_sync(_downgrade_durable_capture, case.schema_name)
            table_names = await connection.run_sync(
                lambda sync_connection: inspect(sync_connection).get_table_names(schema=case.schema_name)
            )

        assert "custom_import_capture_parquet_part" not in table_names


async def test_capture_registration_rejects_an_incomplete_definition_graph():
    async with isolated_publication_case() as case:
        with pytest.raises(CaptureStoreError, match="definition graph is invalid"):
            await _attempt_incomplete_capture_registration(case)


async def _reserve_execution_with_lease_takeover(
    case,
    seed: _Seed,
    *,
    stale_owner: str,
    recovery_identity: str,
) -> tuple[lifecycle.ExecutionSubmission, lifecycle.LeaseGrant, lifecycle.LeaseGrant]:
    """Reserve an execution, expire its first lease, and return both owners."""

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
    assert recovery_owner is not None
    assert recovery_owner.fence == first_owner.fence + 1
    return reserved, first_owner, recovery_owner


async def _roll_back_stale_capture_registration(
    case,
    seed: _Seed,
    *,
    execution_id: int,
    stale_fence: int,
    stale_owner: str,
) -> None:
    """Require a stale bind to roll back its caller-owned capture transaction."""

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
                    execution_id=execution_id,
                    dataset_id=seed.dataset_id,
                    definition_revision_id=seed.definition_revision_id,
                    schema_revision_id=seed.schema_revision_id,
                    capture_bundle_id=bundle.capture_bundle_id,
                    fence=stale_fence,
                    token=stale_owner,
                )
                assert binding is None
                raise RuntimeError("rollback stale bind")


async def _capture_registration_counts(case, seed: _Seed) -> tuple[int, int]:
    """Return the retained bundle and capture row counts for one dataset."""

    async with case.sessions() as session:
        bundle_count = await session.scalar(
            select(func.count())
            .select_from(CustomImportCaptureBundle)
            .where(CustomImportCaptureBundle.dataset_id == seed.dataset_id)
        )
        capture_count = await session.scalar(
            select(func.count())
            .select_from(CustomImportCapture)
            .where(CustomImportCapture.dataset_id == seed.dataset_id)
        )
    return bundle_count, capture_count


async def _durable_capture_registration_counts(case, seed: _Seed) -> tuple[int, int, int]:
    """Return bundle, capture, and durable part rows retained for one dataset."""

    async with case.sessions() as session:
        bundle_count = await session.scalar(
            select(func.count())
            .select_from(CustomImportCaptureBundle)
            .where(CustomImportCaptureBundle.dataset_id == seed.dataset_id)
        )
        capture_count = await session.scalar(
            select(func.count())
            .select_from(CustomImportCapture)
            .where(CustomImportCapture.dataset_id == seed.dataset_id)
        )
        part_count = await session.scalar(
            select(func.count())
            .select_from(CustomImportCaptureParquetPart)
            .join(
                CustomImportCapture,
                (CustomImportCaptureParquetPart.capture_bundle_id == CustomImportCapture.capture_bundle_id)
                & (CustomImportCaptureParquetPart.stream_slot == CustomImportCapture.stream_slot),
            )
            .where(CustomImportCapture.dataset_id == seed.dataset_id)
        )
    return int(bundle_count or 0), int(capture_count or 0), int(part_count or 0)


async def _register_recovery_capture_bundle(
    case,
    seed: _Seed,
    *,
    execution_id: int,
    recovery_fence: int,
    recovery_identity: str,
) -> int:
    """Register and bind a retained bundle while the recovery owner is current."""

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
                execution_id=execution_id,
                dataset_id=seed.dataset_id,
                definition_revision_id=seed.definition_revision_id,
                schema_revision_id=seed.schema_revision_id,
                capture_bundle_id=recovery_bundle.capture_bundle_id,
                fence=recovery_fence,
                token=recovery_identity,
            )
            assert binding is not None
            assert binding.execution_id == execution_id
            assert binding.capture_bundle_id == recovery_bundle.capture_bundle_id
            return recovery_bundle.capture_bundle_id


@pytest.mark.asyncio
async def test_stale_execution_bind_rolls_back_the_enclosing_capture_registration():
    """A stale bind rolls back capture rows, while recovery binds and replays."""

    async with isolated_publication_case() as case:
        seed = await _seed_case(case)
        stale_owner = "synthetic-stale-owner"
        recovery_identity = "synthetic-recovery-owner"
        reserved, first_owner, recovery_owner = await _reserve_execution_with_lease_takeover(
            case,
            seed,
            stale_owner=stale_owner,
            recovery_identity=recovery_identity,
        )
        await _roll_back_stale_capture_registration(
            case,
            seed,
            execution_id=reserved.execution_id,
            stale_fence=first_owner.fence,
            stale_owner=stale_owner,
        )
        bundle_count, capture_count = await _capture_registration_counts(case, seed)
        async with case.sessions() as session:
            execution = await session.get(CustomImportExecution, reserved.execution_id)
        assert bundle_count == capture_count == 0
        assert execution is not None and execution.capture_bundle_id is None

        recovery_bundle_id = await _register_recovery_capture_bundle(
            case,
            seed,
            execution_id=reserved.execution_id,
            recovery_fence=recovery_owner.fence,
            recovery_identity=recovery_identity,
        )
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
        assert replayed.capture_bundle_id == recovery_bundle_id
