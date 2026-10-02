# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native direct-SQL proofs for incremental capture authority and accounting."""

from __future__ import annotations

import asyncio
import datetime as dt
import hashlib
import json
import struct
import uuid
from copy import deepcopy
from dataclasses import asdict
from io import BytesIO
from types import SimpleNamespace

import pytest
from sqlalchemy import insert, select, text, update
from sqlalchemy.exc import DBAPIError

from db.models.custom_import import (
    CustomImportCapture,
    CustomImportCaptureBundle,
    CustomImportCaptureParquetPart,
    CustomImportCaptureUsage,
    CustomImportExecution,
    CustomImportLease,
    CustomImportSourceStream,
)
from process.custom_import.capture import capture_stream
from tests.custom_import_postgres_support import isolated_publication_case
from tests.test_custom_import_capture_store_postgres import (
    _build_replayable_capture_set,
    _parquet_definition,
    _register_replayable_bundle,
    _schema_bearing_zero_row_parquet_part,
    _seed_parquet_case,
    _synthetic_parquet_part,
)

_V2 = "custom-import/parquet-parts/v2"
_RECEIPT = _V2
_POLICY = {
    "contract": "custom-import/segmented-capture-policy/v1",
    "part_limits": {
        "maximum_compressed_bytes": 1_048_576,
        "maximum_decoded_bytes": 2_097_152,
        "maximum_record_bytes": 1024,
        "maximum_records": 100,
        "maximum_fields_per_record": 16,
        "read_chunk_bytes": 4096,
    },
    "stream_budget": {
        "maximum_parts": 4,
        "maximum_compressed_bytes": 4_194_304,
        "maximum_decoded_bytes": 8_388_608,
        "maximum_arrow_bytes": 4_194_304,
        "maximum_records": 400,
        "maximum_manifest_bytes": 8192,
    },
    "bundle_budget": {
        "maximum_parts": 8,
        "maximum_compressed_bytes": 8_388_608,
        "maximum_decoded_bytes": 16_777_216,
        "maximum_arrow_bytes": 8_388_608,
        "maximum_records": 800,
        "maximum_manifest_bytes": 16384,
    },
    "maximum_part_arrow_bytes": 1_048_576,
    "maximum_part_manifest_bytes": 2048,
    "maximum_dataset_retained_bytes": 16_777_216,
    "acquisition_deadline_seconds": 600,
}


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _sha(value):
    return hashlib.sha256(value.encode() if isinstance(value, str) else value).digest()


async def _pending(case, *, seed=None, policy=None, started_at=...):
    """Create one fenced pending capture and its registered stream headers."""

    seed = seed or await _seed_parquet_case(case)
    policy_text = _canonical(policy or _POLICY)
    lease_token = "synthetic-token-" + uuid.uuid4().hex
    now = dt.datetime.now(dt.UTC)
    async with case.sessions() as session, session.begin():
        execution = await _pending_execution(session, seed, lease_token, now, started_at)
        bundle = CustomImportCaptureBundle(
            dataset_id=seed.dataset_id,
            definition_revision_id=seed.definition_revision_id,
            schema_revision_id=seed.schema_revision_id,
            payload_contract=_V2,
            capture_state="pending",
            snapshot_token="synthetic-segmented-snapshot",
            snapshot_token_sha256=_sha("synthetic-segmented-snapshot"),
            stream_count=2,
            producing_execution_id=execution.execution_id,
            producing_fence=1,
            producing_token_sha256=_sha(lease_token),
            request_identity_sha256=execution.request_identity_sha256,
            source_request_sha256=_sha("synthetic-source-request"),
            statement_sha256=_sha("synthetic-statement"),
            canonical_policy=policy_text,
            policy_sha256=_sha(_POLICY["contract"] + ":" + policy_text),
        )
        session.add(bundle)
        await session.flush()
        streams = await _pending_streams(session, seed, bundle)
        return SimpleNamespace(
            seed=seed,
            bundle_id=bundle.capture_bundle_id,
            execution_id=execution.execution_id,
            streams=streams,
            token=lease_token,
            policy_sha256=bytes(bundle.policy_sha256),
        )


async def _pending_execution(session, seed, lease_token, now, started_at):
    """Persist the producing execution and its current lease in order."""

    execution = CustomImportExecution(
        dataset_id=seed.dataset_id,
        definition_revision_id=seed.definition_revision_id,
        schema_revision_id=seed.schema_revision_id,
        idempotency_key=uuid.uuid4().hex,
        mechanism="local",
        state="running",
        started_at=now if started_at is ... else started_at,
        request_identity_sha256=_sha("synthetic-request"),
    )
    session.add(execution)
    await session.flush()
    session.add(
        CustomImportLease(
            execution_id=execution.execution_id,
            fence=1,
            token_sha256=_sha(lease_token),
            heartbeat_at=now,
            expires_at=now + dt.timedelta(minutes=5),
        )
    )
    await session.flush()
    return execution


async def _pending_streams(session, seed, bundle):
    """Persist a pending capture header for each declared source stream."""

    streams = (
        await session.scalars(
            select(CustomImportSourceStream)
            .where(CustomImportSourceStream.definition_revision_id == seed.definition_revision_id)
            .order_by(CustomImportSourceStream.stream_slot)
        )
    ).all()
    for stream in streams:
        session.add(
            CustomImportCapture(
                capture_bundle_id=bundle.capture_bundle_id,
                dataset_id=seed.dataset_id,
                definition_revision_id=seed.definition_revision_id,
                schema_revision_id=seed.schema_revision_id,
                stream_slot=stream.stream_slot,
                payload_contract=_V2,
                capture_state="pending",
            )
        )
    await session.flush()
    return tuple((stream.stream_slot, stream.stream_id) for stream in streams)


def _part(attempt, slot=None, ordinal=1, *, empty=False):
    slot = slot or attempt.streams[0][0]
    stream_id = dict(attempt.streams)[slot]
    source_stream = next(stream for stream in _parquet_definition().source_streams if stream.stream_id == stream_id)
    payload = _schema_bearing_zero_row_parquet_part() if empty else _synthetic_parquet_part(stream_id, ordinal)
    capture = capture_stream(BytesIO(payload), source_stream, source_snapshot_token="synthetic-segmented-snapshot")
    manifest = _canonical(asdict(capture.manifest))
    return dict(
        capture_bundle_id=attempt.bundle_id,
        stream_slot=slot,
        part_ordinal=ordinal,
        payload=payload,
        byte_count=len(payload),
        payload_sha256=_sha(payload),
        canonical_capture_manifest=manifest,
        capture_manifest_sha256=_sha(manifest),
        decoded_byte_count=capture.manifest.decoded_bytes,
        arrow_byte_count=0 if empty else 8 + len(stream_id),
        record_count=0 if empty else 1,
    )


async def _append(case, values):
    async with case.sessions() as session, session.begin():
        await session.execute(insert(CustomImportCaptureParquetPart).values(**values))


async def _counts(case, attempt):
    async with case.sessions() as session:
        bundle = await session.get(CustomImportCaptureBundle, attempt.bundle_id)
        usage = await session.get(CustomImportCaptureUsage, attempt.seed.dataset_id)
        return bundle.committed_part_count, bundle.committed_byte_count, usage.retained_bytes


async def _eof(session, attempt, slot):
    await session.execute(
        update(CustomImportCapture)
        .where(CustomImportCapture.capture_bundle_id == attempt.bundle_id, CustomImportCapture.stream_slot == slot)
        .values(eof_at=dt.datetime.now(dt.UTC))
    )


async def _seal_stream(session, attempt, slot):
    parquet_parts = (
        await session.scalars(
            select(CustomImportCaptureParquetPart)
            .where(
                CustomImportCaptureParquetPart.capture_bundle_id == attempt.bundle_id,
                CustomImportCaptureParquetPart.stream_slot == slot,
            )
            .order_by(CustomImportCaptureParquetPart.part_ordinal)
        )
    ).all()
    payload_hash = hashlib.sha256(b"custom-import/parquet-parts/v1\0")
    manifest_hash = hashlib.sha256(b"custom-import/parquet-manifest-accounting/v1\0")
    for part in parquet_parts:
        payload_hash.update(struct.pack(">iq", part.part_ordinal, part.byte_count) + part.payload_sha256)
        manifest_hash.update(
            struct.pack(">iq", part.part_ordinal, len(part.canonical_capture_manifest.encode()))
            + part.capture_manifest_sha256
            + struct.pack(">qqq", part.decoded_byte_count, part.arrow_byte_count, part.record_count)
        )
    receipt_by_field = dict(
        contract_version=_RECEIPT,
        part_count=len(parquet_parts),
        byte_count=sum(part.byte_count for part in parquet_parts),
        decoded_byte_count=sum(part.decoded_byte_count for part in parquet_parts),
        arrow_byte_count=sum(part.arrow_byte_count for part in parquet_parts),
        record_count=sum(part.record_count for part in parquet_parts),
        manifest_byte_count=sum(len(part.canonical_capture_manifest.encode()) for part in parquet_parts),
        payload_set_sha256=payload_hash.hexdigest(),
        manifest_set_sha256=manifest_hash.hexdigest(),
        policy_sha256=attempt.policy_sha256.hex(),
    )
    manifest = _canonical(receipt_by_field)
    await session.execute(
        update(CustomImportCapture)
        .where(CustomImportCapture.capture_bundle_id == attempt.bundle_id, CustomImportCapture.stream_slot == slot)
        .values(
            capture_state="sealed",
            canonical_manifest=manifest,
            manifest_sha256=_sha(manifest),
            byte_count=receipt_by_field["byte_count"],
            payload_part_count=len(parquet_parts),
            content_sha256=payload_hash.digest(),
            payload_set_sha256=payload_hash.digest(),
            manifest_set_sha256=manifest_hash.digest(),
        )
    )


async def _seal(session, attempt, *, bind=True):
    await session.execute(text("SET LOCAL statement_timeout = '2s'"))
    for slot, _ in attempt.streams:
        await _eof(session, attempt, slot)
        await _seal_stream(session, attempt, slot)
    streams = (
        await session.scalars(
            select(CustomImportCapture)
            .where(CustomImportCapture.capture_bundle_id == attempt.bundle_id)
            .order_by(CustomImportCapture.stream_slot)
        )
    ).all()
    manifest = _canonical(
        dict(
            contract_version=_RECEIPT,
            policy_sha256=attempt.policy_sha256.hex(),
            stream_count=len(streams),
            streams=[
                dict(stream_slot=stream.stream_slot, manifest_sha256=stream.manifest_sha256.hex()) for stream in streams
            ],
            part_count=sum(stream.committed_part_count for stream in streams),
            byte_count=sum(stream.committed_byte_count for stream in streams),
            decoded_byte_count=sum(stream.committed_decoded_byte_count for stream in streams),
            arrow_byte_count=sum(stream.committed_arrow_byte_count for stream in streams),
            record_count=sum(stream.committed_record_count for stream in streams),
            manifest_byte_count=sum(stream.committed_manifest_byte_count for stream in streams),
        )
    )
    await session.execute(
        update(CustomImportCaptureBundle)
        .where(CustomImportCaptureBundle.capture_bundle_id == attempt.bundle_id)
        .values(capture_state="sealed", canonical_manifest=manifest, manifest_sha256=_sha(manifest))
    )
    if bind:
        await session.execute(
            update(CustomImportExecution)
            .where(CustomImportExecution.execution_id == attempt.execution_id)
            .values(capture_bundle_id=attempt.bundle_id)
        )


async def test_pending_parts_commit_retry_once_and_seal_bind_atomically():
    async with isolated_publication_case() as case:
        attempt = await _pending(case)
        first = _part(attempt)
        await _append(case, first)
        await _append(case, first)
        assert await _counts(case, attempt) == (
            1,
            first["byte_count"],
            first["byte_count"] + len(first["canonical_capture_manifest"].encode()),
        )
        for slot, _ in attempt.streams[1:]:
            await _append(case, _part(attempt, slot))
        async with case.sessions() as session, session.begin():
            await _seal(session, attempt)
        async with case.sessions() as session:
            bundle = await session.get(CustomImportCaptureBundle, attempt.bundle_id)
            assert bundle.capture_state == "sealed" and bundle.sealed_at is not None
            assert (
                await session.get(CustomImportExecution, attempt.execution_id)
            ).capture_bundle_id == attempt.bundle_id
        with pytest.raises(DBAPIError, match="append_closed"):
            await _append(case, first)


@pytest.mark.parametrize("mutation", ["gap", "bytes", "manifest", "accounting"])
async def test_part_collisions_and_gaps_are_rejected(mutation):
    async with isolated_publication_case() as case:
        attempt = await _pending(case)
        first = _part(attempt)
        await _append(case, first)
        changed_part_by_field = dict(first)
        changed_part_by_field.update(
            {
                "gap": {"part_ordinal": 3},
                "bytes": {"payload": b"changed"},
                "manifest": {"canonical_capture_manifest": "{}"},
                "accounting": {"record_count": 2},
            }[mutation]
        )
        with pytest.raises(DBAPIError, match="part_collision|part_shape_invalid"):
            await _append(case, changed_part_by_field)
        assert (await _counts(case, attempt))[0] == 1


@pytest.mark.parametrize("mutation", ["expired", "fence", "token", "canceling", "request"])
async def test_lost_producer_authority_rejects_append_and_eof(mutation):
    async with isolated_publication_case() as case:
        attempt = await _pending(case)
        async with case.sessions() as session, session.begin():
            if mutation in {"expired", "fence", "token"}:
                lease_updates_by_field = {
                    "expired": {"expires_at": dt.datetime.now(dt.UTC) - dt.timedelta(seconds=1)},
                    "fence": {"fence": 2},
                    "token": {"token_sha256": _sha("different")},
                }[mutation]
                await session.execute(
                    update(CustomImportLease)
                    .where(CustomImportLease.execution_id == attempt.execution_id)
                    .values(**lease_updates_by_field)
                )
            elif mutation == "canceling":
                await session.execute(
                    update(CustomImportExecution)
                    .where(CustomImportExecution.execution_id == attempt.execution_id)
                    .values(state="canceling")
                )
            else:
                await session.execute(
                    update(CustomImportExecution)
                    .where(CustomImportExecution.execution_id == attempt.execution_id)
                    .values(request_identity_sha256=_sha("different"))
                )
        with pytest.raises(DBAPIError, match="authority_lost"):
            await _append(case, _part(attempt))
        with pytest.raises(DBAPIError, match="authority_lost"):
            async with case.sessions() as session, session.begin():
                await _eof(session, attempt, attempt.streams[0][0])


async def test_pending_cannot_bind_and_seal_without_binding_rolls_back():
    async with isolated_publication_case() as case:
        attempt = await _pending(case)
        with pytest.raises(DBAPIError, match="capture_not_sealed"):
            async with case.sessions() as session, session.begin():
                await session.execute(
                    update(CustomImportExecution)
                    .where(CustomImportExecution.execution_id == attempt.execution_id)
                    .values(capture_bundle_id=attempt.bundle_id)
                )
        with pytest.raises(DBAPIError, match="capture_not_sealed"):
            async with case.sessions() as session, session.begin():
                await session.execute(
                    insert(CustomImportExecution).values(
                        dataset_id=attempt.seed.dataset_id,
                        definition_revision_id=attempt.seed.definition_revision_id,
                        schema_revision_id=attempt.seed.schema_revision_id,
                        idempotency_key="synthetic-explicit-capture",
                        mechanism="local",
                        state="queued",
                        capture_bundle_id=attempt.bundle_id,
                    )
                )
        for slot, _ in attempt.streams:
            await _append(case, _part(attempt, slot))
        with pytest.raises(DBAPIError, match="atomic_binding"):
            async with case.sessions() as session, session.begin():
                await _seal(session, attempt, bind=False)
        async with case.sessions() as session:
            assert (await session.get(CustomImportCaptureBundle, attempt.bundle_id)).capture_state == "pending"


async def test_eof_is_monotone_and_metadata_scan_requires_statement_timeout():
    async with isolated_publication_case() as case:
        attempt = await _pending(case)
        slot = attempt.streams[0][0]
        await _append(case, _part(attempt))
        async with case.sessions() as session, session.begin():
            await _eof(session, attempt, slot)
        with pytest.raises(DBAPIError, match="append_closed"):
            await _append(case, _part(attempt, ordinal=2))
        with pytest.raises(DBAPIError, match="eof_invalid"):
            async with case.sessions() as session, session.begin():
                await session.execute(
                    update(CustomImportCapture)
                    .where(
                        CustomImportCapture.capture_bundle_id == attempt.bundle_id,
                        CustomImportCapture.stream_slot == slot,
                    )
                    .values(eof_at=None)
                )
        with pytest.raises(DBAPIError, match="timeout_required"):
            async with case.sessions() as session, session.begin():
                await _seal_stream(session, attempt, slot)


async def test_same_part_concurrent_retry_has_single_charge():
    async with isolated_publication_case() as case:
        attempt = await _pending(case)
        part = _part(attempt)
        await asyncio.gather(_append(case, part), _append(case, part))
        assert await _counts(case, attempt) == (
            1,
            part["byte_count"],
            part["byte_count"] + len(part["canonical_capture_manifest"].encode()),
        )


async def test_legacy_multirow_insert_is_accounted_once_and_abandoned_pending_is_retained():
    async with isolated_publication_case() as case:
        seed = await _seed_parquet_case(case)
        captures = _build_replayable_capture_set()
        await _register_replayable_bundle(case, seed, captures)
        legacy_bytes = sum(len(part) for capture in captures for part in capture.parts)
        attempt = await _pending(case, seed=seed)
        first = _part(attempt)
        await _append(case, first)
        assert (await _counts(case, attempt))[2] == legacy_bytes + first["byte_count"] + len(
            first["canonical_capture_manifest"].encode()
        )
        abandoned = await _pending(case, seed=seed)
        await _append(case, _part(abandoned))
        assert (await _counts(case, abandoned))[2] == legacy_bytes + 2 * (
            first["byte_count"] + len(first["canonical_capture_manifest"].encode())
        )


async def test_first_part_seeds_existing_legacy_bytes_before_charging_new_row():
    async with isolated_publication_case() as case:
        seed = await _seed_parquet_case(case)
        # Simulate payload rows retained before this schema-only migration installed accounting.
        async with case.engine.begin() as connection:
            for trigger in ("a_custom_import_capture_part_account_before", "custom_import_capture_part_account_after"):
                await connection.execute(
                    text(
                        f'ALTER TABLE "{case.schema_name}".custom_import_capture_parquet_part DISABLE TRIGGER {trigger}'
                    )
                )
        captures = _build_replayable_capture_set()
        await _register_replayable_bundle(case, seed, captures)
        async with case.engine.begin() as connection:
            for trigger in ("a_custom_import_capture_part_account_before", "custom_import_capture_part_account_after"):
                await connection.execute(
                    text(
                        f'ALTER TABLE "{case.schema_name}".custom_import_capture_parquet_part ENABLE ALWAYS TRIGGER {trigger}'
                    )
                )
        attempt = await _pending(case, seed=seed)
        first = _part(attempt)
        await _append(case, first)
        legacy_bytes = sum(len(part) for capture in captures for part in capture.parts)
        assert (await _counts(case, attempt))[2] == legacy_bytes + first["byte_count"] + len(
            first["canonical_capture_manifest"].encode()
        )


async def test_unrelated_trigger_depth_does_not_grant_accounting_authority():
    async with isolated_publication_case() as case:
        attempt = await _pending(case)
        first = _part(attempt)
        role = "segmented_writer_" + uuid.uuid4().hex[:16]
        is_role_created = False
        try:
            async with case.engine.begin() as connection:
                await connection.execute(text(f'CREATE ROLE "{role}" NOLOGIN NOSUPERUSER NOINHERIT'))
            is_role_created = True
            async with case.engine.begin() as connection:
                await connection.execute(text(f'GRANT USAGE ON SCHEMA "{case.schema_name}" TO "{role}"'))
                await connection.execute(
                    text(
                        f'GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA "{case.schema_name}" TO "{role}"'
                    )
                )
            async with case.sessions() as session, session.begin():
                await session.execute(text(f'SET LOCAL ROLE "{role}"'))
                await session.execute(insert(CustomImportCaptureParquetPart).values(**first))
            with pytest.raises(DBAPIError, match="usage_protected"):
                async with case.sessions() as session, session.begin():
                    await session.execute(text(f'SET LOCAL ROLE "{role}"'))
                    await session.execute(text("CREATE TEMP TABLE counterfeit_accounting (value integer)"))
                    await session.execute(
                        text(f'''CREATE FUNCTION pg_temp.counterfeit_accounting() RETURNS trigger LANGUAGE plpgsql AS $$
                        BEGIN UPDATE "{case.schema_name}".custom_import_capture_usage SET retained_bytes = retained_bytes + 1;
                        RETURN NEW; END; $$''')
                    )
                    await session.execute(
                        text(
                            "CREATE TRIGGER counterfeit AFTER INSERT ON counterfeit_accounting FOR EACH ROW EXECUTE FUNCTION pg_temp.counterfeit_accounting()"
                        )
                    )
                    await session.execute(text("INSERT INTO counterfeit_accounting VALUES (1)"))
            assert (await _counts(case, attempt))[0] == 1
        finally:
            if is_role_created:
                async with case.engine.begin() as connection:
                    await connection.execute(text(f'DROP OWNED BY "{role}"'))
                    await connection.execute(text(f'DROP ROLE "{role}"'))


async def test_usage_and_progress_reject_direct_updates_even_for_installing_owner():
    async with isolated_publication_case() as case:
        attempt = await _pending(case)
        await _append(case, _part(attempt))
        for statement in (
            update(CustomImportCaptureUsage).values(retained_bytes=0),
            update(CustomImportCaptureBundle)
            .where(CustomImportCaptureBundle.capture_bundle_id == attempt.bundle_id)
            .values(committed_part_count=2),
            text(f'TRUNCATE "{case.schema_name}".custom_import_capture_usage'),
            text(f'DELETE FROM "{case.schema_name}".custom_import_capture_usage'),
        ):
            with pytest.raises(DBAPIError, match="protected"):
                async with case.sessions() as session, session.begin():
                    await session.execute(statement)


@pytest.mark.parametrize("field", ["producing_fence", "source_request_sha256", "eof_at", "manifest_set_sha256"])
async def test_legacy_null_contract_cannot_hide_segmented_fields(field):
    async with isolated_publication_case() as case:
        seed = await _seed_parquet_case(case)
        bundle_values_by_field = dict(
            dataset_id=seed.dataset_id,
            definition_revision_id=seed.definition_revision_id,
            schema_revision_id=seed.schema_revision_id,
            snapshot_token="synthetic-legacy",
            snapshot_token_sha256=_sha("synthetic-legacy"),
            canonical_manifest="{}",
            manifest_sha256=_sha("{}"),
            stream_count=2,
        )
        with pytest.raises(DBAPIError, match="lifecycle_check|payload_shape_check"):
            async with case.sessions() as session, session.begin():
                if field in {"producing_fence", "source_request_sha256"}:
                    bundle_values_by_field[field] = 1 if field == "producing_fence" else _sha("stray")
                    await session.execute(insert(CustomImportCaptureBundle).values(**bundle_values_by_field))
                else:
                    bundle_id = (
                        await session.execute(
                            insert(CustomImportCaptureBundle)
                            .values(**bundle_values_by_field)
                            .returning(CustomImportCaptureBundle.capture_bundle_id)
                        )
                    ).scalar_one()
                    stream_values_by_field = dict(
                        capture_bundle_id=bundle_id,
                        dataset_id=seed.dataset_id,
                        definition_revision_id=seed.definition_revision_id,
                        schema_revision_id=seed.schema_revision_id,
                        stream_slot=1,
                        canonical_manifest="{}",
                        manifest_sha256=_sha("{}"),
                        byte_count=1,
                        content_sha256=_sha("one"),
                    )
                    stream_values_by_field[field] = dt.datetime.now(dt.UTC) if field == "eof_at" else _sha("stray")
                    await session.execute(insert(CustomImportCapture).values(**stream_values_by_field))


@pytest.mark.parametrize(
    "field,value",
    [
        ("payload_contract", None),
        ("producing_fence", 2),
        ("policy_sha256", _sha("different")),
        ("source_request_sha256", _sha("different")),
    ],
)
async def test_pending_header_identity_is_immutable(field, value):
    async with isolated_publication_case() as case:
        attempt = await _pending(case)
        with pytest.raises(DBAPIError, match="immutable|authority_lost"):
            async with case.sessions() as session, session.begin():
                await session.execute(
                    update(CustomImportCaptureBundle)
                    .where(CustomImportCaptureBundle.capture_bundle_id == attempt.bundle_id)
                    .values(**{field: value})
                )


async def test_v2_direct_sealed_insert_and_unrelated_trigger_invocation_fail():
    async with isolated_publication_case() as case:
        attempt = await _pending(case)
        async with case.sessions() as session:
            bundle = await session.get(CustomImportCaptureBundle, attempt.bundle_id)
            bundle_values_by_field = {
                column.name: getattr(bundle, column.name)
                for column in bundle.__table__.columns
                if column.name != "capture_bundle_id"
            }
        bundle_values_by_field.update(
            capture_state="sealed",
            canonical_manifest="{}",
            manifest_sha256=_sha("{}"),
            sealed_at=dt.datetime.now(dt.UTC),
        )
        with pytest.raises(DBAPIError, match="must_begin_pending"):
            async with case.sessions() as session, session.begin():
                await session.execute(insert(CustomImportCaptureBundle).values(**bundle_values_by_field))
        with pytest.raises(DBAPIError, match="accounting_context_invalid"):
            async with case.sessions() as session, session.begin():
                await session.execute(text("CREATE TEMP TABLE counterfeit_part (value integer)"))
                await session.execute(
                    text(
                        f'CREATE TRIGGER counterfeit_part BEFORE INSERT ON counterfeit_part FOR EACH ROW EXECUTE FUNCTION "{case.schema_name}".account_custom_import_capture_parts()'
                    )
                )
                await session.execute(text("INSERT INTO counterfeit_part VALUES (1)"))


@pytest.mark.parametrize(
    "table", ["custom_import_generation", "custom_import_generation_seal", "custom_import_no_change_seal"]
)
async def test_direct_finality_consumers_reject_pending_before_other_validation(table):
    async with isolated_publication_case() as case:
        attempt = await _pending(case)
        with pytest.raises(DBAPIError, match="capture_not_sealed"):
            async with case.sessions() as session, session.begin():
                await session.execute(
                    text(
                        f'INSERT INTO "{case.schema_name}".{table} '
                        "(capture_bundle_id,dataset_id,definition_revision_id,schema_revision_id) VALUES (:bundle,:dataset,:definition,:schema)"
                    ),
                    dict(
                        bundle=attempt.bundle_id,
                        dataset=attempt.seed.dataset_id,
                        definition=attempt.seed.definition_revision_id,
                        schema=attempt.seed.schema_revision_id,
                    ),
                )


async def test_stream_quota_rejection_and_conflict_do_nothing_do_not_charge():
    async with isolated_publication_case() as case:
        policy = deepcopy(_POLICY)
        policy["stream_budget"]["maximum_parts"] = 1
        attempt = await _pending(case, policy=policy)
        first = _part(attempt)
        await _append(case, first)
        from sqlalchemy.dialects.postgresql import insert as pg_insert

        async with case.sessions() as session, session.begin():
            await session.execute(pg_insert(CustomImportCaptureParquetPart).values(**first).on_conflict_do_nothing())
        with pytest.raises(DBAPIError, match="quota_exceeded"):
            await _append(case, _part(attempt, ordinal=2))
        assert (await _counts(case, attempt))[0] == 1


async def test_multirow_cross_stream_insert_rechecks_bundle_quota_and_rolls_back():
    async with isolated_publication_case() as case:
        policy = deepcopy(_POLICY)
        policy["stream_budget"]["maximum_parts"] = 1
        policy["bundle_budget"]["maximum_parts"] = 1
        attempt = await _pending(case, policy=policy)
        parts = [_part(attempt, slot) for slot, _ in attempt.streams]
        with pytest.raises(DBAPIError, match="quota_exceeded"):
            async with case.sessions() as session, session.begin():
                await session.execute(insert(CustomImportCaptureParquetPart).values(parts))
        async with case.sessions() as session:
            assert (await session.get(CustomImportCaptureBundle, attempt.bundle_id)).committed_part_count == 0
            assert await session.get(CustomImportCaptureUsage, attempt.seed.dataset_id) is None
            assert not (await session.scalars(select(CustomImportCaptureParquetPart))).all()
            assert all(
                stream.committed_part_count == 0
                for stream in (await session.scalars(select(CustomImportCapture))).all()
            )
        await _append(case, parts[0])
        assert (await _counts(case, attempt))[0] == 1


async def test_multirow_cross_bundle_insert_rechecks_dataset_quota_and_rolls_back():
    async with isolated_publication_case() as case:
        policy = deepcopy(_POLICY)
        policy["part_limits"].update(maximum_compressed_bytes=1024, maximum_decoded_bytes=2048)
        policy["stream_budget"].update(
            maximum_compressed_bytes=1024, maximum_decoded_bytes=2048, maximum_manifest_bytes=2048
        )
        policy["bundle_budget"].update(
            maximum_compressed_bytes=2048, maximum_decoded_bytes=4096, maximum_manifest_bytes=4096
        )
        policy["maximum_dataset_retained_bytes"] = 6144
        first = await _pending(case, policy=policy)
        second = await _pending(case, seed=first.seed, policy=policy)
        parts = [_part(first), _part(second)]
        one_retained = parts[0]["byte_count"] + len(parts[0]["canonical_capture_manifest"].encode())
        prior_parts = policy["maximum_dataset_retained_bytes"] // one_retained - 1
        for _ in range(prior_parts):
            abandoned = await _pending(case, seed=first.seed, policy=policy)
            await _append(case, _part(abandoned))
        with pytest.raises(DBAPIError, match="retained_quota_exceeded"):
            async with case.sessions() as session, session.begin():
                await session.execute(insert(CustomImportCaptureParquetPart).values(parts))
        assert await _counts(case, first) == (0, 0, prior_parts * one_retained)
        assert await _counts(case, second) == (0, 0, prior_parts * one_retained)
        async with case.sessions() as session:
            assert not (
                await session.scalars(
                    select(CustomImportCaptureParquetPart).where(
                        CustomImportCaptureParquetPart.capture_bundle_id.in_([first.bundle_id, second.bundle_id])
                    )
                )
            ).all()
        await _append(case, parts[0])
        assert (await _counts(case, first))[2] == (prior_parts + 1) * one_retained


@pytest.mark.parametrize("document_kind", ["policy", "part", "receipt"])
async def test_declared_integer_fields_reject_exponent_tokens(document_kind, monkeypatch):
    async with isolated_publication_case() as case:
        if document_kind == "policy":
            document = _canonical(_POLICY).replace('"maximum_records":100,', '"maximum_records":1e2,')
            with pytest.raises(DBAPIError, match="policy_invalid"):
                async with case.sessions() as session:
                    await session.execute(
                        text(f'SELECT "{case.schema_name}".validate_custom_import_segmented_policy(:document,:digest)'),
                        dict(document=document, digest=_sha(_POLICY["contract"] + ":" + document)),
                    )
            return
        attempt = await _pending(case)
        if document_kind == "part":
            part = _part(attempt)
            part["canonical_capture_manifest"] = part["canonical_capture_manifest"].replace(
                f'"compressed_bytes":{part["byte_count"]},', f'"compressed_bytes":{part["byte_count"]}e0,'
            )
            part["capture_manifest_sha256"] = _sha(part["canonical_capture_manifest"])
            with pytest.raises(DBAPIError, match="manifest_invalid"):
                await _append(case, part)
            return
        for slot, _ in attempt.streams:
            await _append(case, _part(attempt, slot))
        canonical = _canonical
        monkeypatch.setattr(
            f"{__name__}._canonical", lambda value: canonical(value).replace('"part_count":1,', '"part_count":1e0,')
        )
        with pytest.raises(DBAPIError, match="receipt_integer_required"):
            async with case.sessions() as session, session.begin():
                await _seal(session, attempt)
        async with case.sessions() as session:
            assert (await session.get(CustomImportCaptureBundle, attempt.bundle_id)).capture_state == "pending"


async def test_dataset_quota_includes_abandoned_bytes_and_manifest_lengths():
    async with isolated_publication_case() as case:
        policy = deepcopy(_POLICY)
        policy["part_limits"].update(maximum_compressed_bytes=1024, maximum_decoded_bytes=2048)
        policy["stream_budget"].update(
            maximum_compressed_bytes=1024, maximum_decoded_bytes=2048, maximum_manifest_bytes=2048
        )
        policy["bundle_budget"].update(
            maximum_compressed_bytes=2048, maximum_decoded_bytes=4096, maximum_manifest_bytes=4096
        )
        policy["maximum_dataset_retained_bytes"] = 6144
        attempt = await _pending(case, policy=policy)
        part = _part(attempt)
        one_retained = part["byte_count"] + len(part["canonical_capture_manifest"].encode())
        for _ in range(policy["maximum_dataset_retained_bytes"] // one_retained):
            abandoned = await _pending(case, seed=attempt.seed, policy=policy)
            await _append(case, _part(abandoned))
        with pytest.raises(DBAPIError, match="retained_quota_exceeded"):
            await _append(case, part)


async def test_seal_recomputes_metadata_and_receipt_composition():
    async with isolated_publication_case() as case:
        attempt = await _pending(case)
        for slot, _ in attempt.streams:
            await _append(case, _part(attempt, slot))
        async with case.sessions() as session, session.begin():
            await _eof(session, attempt, attempt.streams[0][0])
        with pytest.raises(DBAPIError, match="part_totals_mismatch"):
            async with case.sessions() as session, session.begin():
                await session.execute(text("SET LOCAL statement_timeout='2s'"))
                manifest = _canonical(dict(contract_version=_V2, policy_sha256=attempt.policy_sha256.hex()))
                await session.execute(
                    update(CustomImportCapture)
                    .where(
                        CustomImportCapture.capture_bundle_id == attempt.bundle_id,
                        CustomImportCapture.stream_slot == attempt.streams[0][0],
                    )
                    .values(
                        capture_state="sealed",
                        canonical_manifest=manifest,
                        manifest_sha256=_sha(manifest),
                        byte_count=999,
                        payload_part_count=1,
                        payload_set_sha256=_sha("bad"),
                        content_sha256=_sha("bad"),
                        manifest_set_sha256=_sha("bad"),
                    )
                )
        with pytest.raises(DBAPIError, match="bundle_receipt_mismatch"):
            async with case.sessions() as session, session.begin():
                await session.execute(text("SET LOCAL statement_timeout='2s'"))
                await _seal_stream(session, attempt, attempt.streams[0][0])
                await _eof(session, attempt, attempt.streams[1][0])
                await _seal_stream(session, attempt, attempt.streams[1][0])
                manifest = _canonical(dict(contract_version=_V2, policy_sha256=attempt.policy_sha256.hex(), streams=[]))
                await session.execute(
                    update(CustomImportCaptureBundle)
                    .where(CustomImportCaptureBundle.capture_bundle_id == attempt.bundle_id)
                    .values(capture_state="sealed", canonical_manifest=manifest, manifest_sha256=_sha(manifest))
                )


async def test_unordered_header_update_retries_without_waiting_for_dataset_lock():
    async with isolated_publication_case() as case:
        attempt = await _pending(case)
        await _append(case, _part(attempt))

        async def unordered_eof():
            async with case.sessions() as session, session.begin():
                await _eof(session, attempt, attempt.streams[0][0])

        async with case.sessions() as blocker, blocker.begin():
            await blocker.execute(
                text(
                    f'SELECT dataset_id FROM "{case.schema_name}".custom_import_dataset '
                    "WHERE dataset_id=:dataset FOR UPDATE"
                ),
                dict(dataset=attempt.seed.dataset_id),
            )
            with pytest.raises(DBAPIError, match="could not obtain lock"):
                await asyncio.wait_for(unordered_eof(), timeout=2)
        await unordered_eof()


async def test_database_deadline_expires_and_zero_record_streams_can_seal():
    async with isolated_publication_case() as case:
        policy = deepcopy(_POLICY)
        policy["acquisition_deadline_seconds"] = 1
        expired = await _pending(case, policy=policy)
        async with case.sessions() as session:
            await session.execute(text("SELECT pg_sleep(1.05)"))
        with pytest.raises(DBAPIError, match="authority_lost"):
            await _append(case, _part(expired))
        empty = await _pending(case)
        for slot, _ in empty.streams:
            await _append(case, _part(empty, slot, empty=True))
        async with case.sessions() as session, session.begin():
            await _seal(session, empty)
        async with case.sessions() as session:
            bundle = await session.get(CustomImportCaptureBundle, empty.bundle_id)
            assert bundle.capture_state == "sealed" and bundle.committed_record_count == 0
            assert bundle.committed_part_count == 2 and bundle.committed_byte_count > 0


@pytest.mark.parametrize("start", ["missing", "future", "elapsed"])
async def test_pending_rejects_invalid_or_elapsed_execution_start(start):
    async with isolated_publication_case() as case:
        now = dt.datetime.now(dt.UTC)
        started_at = {
            "missing": None,
            "future": now + dt.timedelta(minutes=1),
            "elapsed": now - dt.timedelta(seconds=_POLICY["acquisition_deadline_seconds"] + 1),
        }[start]
        with pytest.raises(DBAPIError, match="acquisition_start_invalid|authority_lost"):
            await _pending(case, started_at=started_at)


async def test_deadline_includes_open_time_and_cannot_be_extended():
    async with isolated_publication_case() as case:
        started_at = dt.datetime.now(dt.UTC) - dt.timedelta(minutes=2)
        attempt = await _pending(case, started_at=started_at)
        deadline = started_at + dt.timedelta(seconds=_POLICY["acquisition_deadline_seconds"])
        async with case.sessions() as session:
            bundle = await session.get(CustomImportCaptureBundle, attempt.bundle_id)
            assert bundle.acquisition_started_at == started_at
            assert bundle.acquisition_deadline_at == deadline
        first = _part(attempt)
        await _append(case, first)
        async with case.sessions() as session, session.begin():
            now = dt.datetime.now(dt.UTC)
            await session.execute(
                update(CustomImportLease)
                .where(CustomImportLease.execution_id == attempt.execution_id)
                .values(heartbeat_at=now, expires_at=now + dt.timedelta(minutes=10))
            )
        await _append(case, first)
        async with case.sessions() as session:
            bundle = await session.get(CustomImportCaptureBundle, attempt.bundle_id)
            assert bundle.acquisition_started_at == started_at
            assert bundle.acquisition_deadline_at == deadline
            assert bundle.committed_part_count == 1
        with pytest.raises(DBAPIError, match="identity_immutable"):
            async with case.sessions() as session, session.begin():
                await session.execute(
                    update(CustomImportCaptureBundle)
                    .where(CustomImportCaptureBundle.capture_bundle_id == attempt.bundle_id)
                    .values(acquisition_deadline_at=deadline + dt.timedelta(seconds=1))
                )
