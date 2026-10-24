# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Prepared bytes remain charged through close, seal and execution abandonment."""

from __future__ import annotations

import datetime as dt
import hashlib
import uuid

import pytest
from sqlalchemy import insert, select

from db.models.custom_import import CustomImportCaptureBundle, CustomImportDataset, CustomImportExecution
from db.models.custom_import_prepared_source import PREPARED_SOURCE_CONTRACT, CustomImportPreparedSource
from process.custom_import import execution
from process.custom_import.capture_retention import validate_dataset_retained_bytes
from process.custom_import.capture_store import CaptureBundleConflict
from process.custom_import.source_binding_store import register_source_binding
from tests.custom_import_postgres_support import isolated_publication_case
from tests.custom_import_prepared_source_migration_postgres_cases import _header_values
from tests.test_custom_import_capture_authority_postgres import _mixed_history, _originals
from tests.test_custom_import_capture_store_postgres import _parquet_definition
from tests.test_custom_import_raw_capture_postgres import _register_raw
from tests.test_custom_import_source_binding_store import _object_binding


def _prepared_header_fields(binding, registered, bundle, grant, token, state):
    summary = '{"label":"é"}' if state == "sealed" else None
    return _header_values(
        execution_id=grant.execution_id,
        capture_bundle_id=bundle.capture_bundle_id,
        dataset_id=registered.dataset_id,
        definition_revision_id=registered.definition_revision_id,
        schema_revision_id=registered.schema_revision_id,
        source_binding_revision_id=registered.source_binding_revision_id,
        source_binding_sha256=registered.source_binding_sha256,
        raw_manifest_sha256=bundle.manifest_sha256,
        producer_fence=grant.fence,
        producer_token_sha256=execution.lease_token_sha256(token),
        canonical_processing_policy=binding.processing_policy.canonical,
        processing_policy_sha256=bytes.fromhex(binding.processing_policy.digest),
        canonical_progress='{"label":"é"}',
        byte_count=31,
        manifest_byte_count=17,
        state="pending" if state in {"canceled", "failed"} else state,
        canonical_summary=summary,
        prepared_sha256=hashlib.sha256((PREPARED_SOURCE_CONTRACT + ":" + summary).encode()).digest()
        if summary
        else None,
        sealed_at=dt.datetime.now(dt.UTC) if summary else None,
    )


async def _retained_preparation(case, state):
    seed, legacy, original_bytes = await _mixed_history(case, "raw")
    definition = _parquet_definition()
    binding = _object_binding(definition)
    token = b"synthetic-prepared-retention-owner"
    async with case.sessions() as session, session.begin():
        dataset = await session.get(CustomImportDataset, seed.dataset_id)
        registered = await register_source_binding(
            session, dataset_key=dataset.dataset_key, definition=definition, binding=binding
        )
        raw = await _register_raw(session, seed, _originals(legacy, "synthetic-mixed-original"), 0)
        submission = await execution.reserve_execution(
            session,
            dataset_id=seed.dataset_id,
            definition_revision_id=seed.definition_revision_id,
            schema_revision_id=seed.schema_revision_id,
            source_binding_revision_id=registered.source_binding_revision_id,
            idempotency_key=uuid.uuid4().hex,
            mechanism="local",
            request_identity_sha256=b"r" * 32,
        )
        grant = await execution.claim_execution(session, execution_id=submission.execution_id, token=token)
        assert grant is not None
        bound = await execution.bind_execution_capture_bundle(
            session,
            execution_id=submission.execution_id,
            dataset_id=seed.dataset_id,
            definition_revision_id=seed.definition_revision_id,
            schema_revision_id=seed.schema_revision_id,
            capture_bundle_id=raw.capture_bundle_id,
            fence=grant.fence,
            token=token,
        )
        assert bound is not None
        bundle = await session.get(CustomImportCaptureBundle, raw.capture_bundle_id)
        header_by_field = _prepared_header_fields(binding, registered, bundle, grant, token, state)
        await session.execute(insert(CustomImportPreparedSource), header_by_field)
        if state in {"canceled", "failed"}:
            if state == "canceled":
                await execution.request_cancellation(session, execution_id=submission.execution_id)
            transition = await execution.finish_execution(
                session, execution_id=submission.execution_id, fence=grant.fence, token=token, terminal_state=state
            )
            assert transition.changed and transition.state == state
    expected_prepared = (
        31
        + 17
        + sum(
            len((header_by_field[field] or "").encode("utf-8"))
            for field in ("canonical_processing_policy", "canonical_progress", "canonical_summary")
        )
    )
    return seed, legacy, original_bytes, submission.execution_id, expected_prepared


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ("pending", "closed", "sealed", "canceled", "failed"))
async def test_prepared_header_bytes_remain_charged_for_every_retained_state(state):
    async with isolated_publication_case(is_snapshot_storage_enabled=False) as case:
        seed, legacy, original_bytes, execution_id, prepared_bytes = await _retained_preparation(case, state)
        async with case.sessions() as session, session.begin():
            await session.execute(
                select(CustomImportDataset).where(CustomImportDataset.dataset_id == seed.dataset_id).with_for_update()
            )
            header = await session.get(CustomImportPreparedSource, execution_id)
            retained_execution = await session.get(CustomImportExecution, execution_id)
            assert header.state == ("pending" if state in {"canceled", "failed"} else state)
            assert retained_execution.state == (state if state in {"canceled", "failed"} else "running")
            assert header.retained_byte_count == prepared_bytes
            total = original_bytes + prepared_bytes
            await validate_dataset_retained_bytes(
                session, dataset_id=seed.dataset_id, new_byte_count=0, maximum_retained_bytes=total
            )
            with pytest.raises(CaptureBundleConflict, match="quota"):
                await validate_dataset_retained_bytes(
                    session, dataset_id=seed.dataset_id, new_byte_count=0, maximum_retained_bytes=total - 1
                )
        next_originals = _originals(legacy, "synthetic-next-with-prepared")
        new_bytes = sum(len(capture.payload) for capture in next_originals)
        with pytest.raises(CaptureBundleConflict, match="quota"):
            async with case.sessions() as session, session.begin():
                await _register_raw(session, seed, next_originals, original_bytes + new_bytes)
