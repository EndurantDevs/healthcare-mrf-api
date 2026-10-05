# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native one-claim capture, bounded build, activation, and source-free resume."""

from __future__ import annotations

import datetime as dt
from dataclasses import asdict, replace
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy import func, select, update

from db.models.custom_import import (
    CustomImportBuildAttempt,
    CustomImportCaptureBundle,
    CustomImportCurrentGeneration,
    CustomImportExecution,
    CustomImportGeneration,
    CustomImportGenerationSeal,
    CustomImportLease,
    CustomImportPublicationEvent,
)
from process.custom_import import snowflake_candidate
from process.custom_import import snowflake_segmented_runner as runner
from process.custom_import.processing_policy import BuildPolicy, ProcessingPolicy
from process.custom_import.snowflake_binding import SOURCE_BINDING_V2_CONTRACT, SnowflakeSourceBinding
from process.custom_import.snowflake_candidate import SnowflakeBundleCandidateRequest
from process.custom_import.snowflake_source_binding import register_snowflake_source_binding
from tests.test_custom_import_build_source_postgres import _source_case
from tests.test_custom_import_snowflake_capture import _policy as _capture_policy
from tests.test_custom_import_snowflake_shared_capture import _runtime, _shared_row

_ROWS = (_shared_row("1003000126", key="a"), _shared_row("1234567893", key="b"))


def _binding(statement, policy):
    definition = statement.request.definition
    streams = [
        {
            "stream_id": binding.stream_id,
            "relation": list(binding.relation.parts),
            "source_snapshot_token_relation": None
            if binding.source_snapshot_token_relation is None
            else list(binding.source_snapshot_token_relation.parts),
            "semantic_token_metadata_key": binding.semantic_token_metadata_key,
            "source_snapshot_token_column_identifier": None if snapshot is None else snapshot.column_identifier,
            "columns": [asdict(column) for column in columns],
        }
        for binding, columns, snapshot in zip(
            statement.request.bindings,
            statement.selected_columns_by_stream,
            statement.source_snapshot_token_columns_by_stream,
            strict=True,
        )
    ]
    return SnowflakeSourceBinding.from_mapping(
        {
            "contract": SOURCE_BINDING_V2_CONTRACT,
            "connector": "snowflake_bundle",
            "definition_sha256": definition.digest,
            "schema_sha256": definition.schema_digest,
            "source_object": {"fingerprint_sha256": "2" * 64, "version": "synthetic-snapshot"},
            "role": "reader_role",
            "warehouse": "import_wh",
            "streams": streams,
            "processing_policy": policy.to_mapping(),
            **(
                {"snapshot_token_mode": statement.request.snapshot_token_mode}
                if statement.request.snapshot_token_mode is not None
                else {}
            ),
        }
    )


async def _registered_runtime(case, monkeypatch):
    policy = ProcessingPolicy(_capture_policy(), 17, BuildPolicy(8, 65_536, 2000, 120, 300))
    connector, bundle, adapter, cursor, connection = _runtime(
        monkeypatch, _ROWS, partition_rows=1, processing_policy=policy
    )
    monkeypatch.setattr(adapter, "_connect", lambda _credentials, **_options: (connection, cursor))
    binding = _binding(connector.build_statement(bundle), policy)
    async with case.sessions() as session, session.begin():
        registered = await register_snowflake_source_binding(
            session, dataset_key="synthetic_segmented_runner", definition=bundle.definition, binding=binding
        )
    request = SnowflakeBundleCandidateRequest(
        dataset_id=registered.dataset_id,
        definition_revision_id=registered.definition_revision_id,
        schema_revision_id=registered.schema_revision_id,
        definition=bundle.definition,
        bundle_request=bundle,
        idempotency_key="synthetic-segmented-run",
        lease_token=b"synthetic-segmented-owner",
        source_binding_revision_id=registered.source_binding_revision_id,
        source_binding_sha256=registered.source_binding_sha256,
    )
    return connector, request, policy, cursor, connection


def _assert_seal_counts(seal, outcome):
    for name, expected in {
        "root_count": 2,
        "family_count": 2,
        "generation_family_count": 2,
        "family_child_count": 2,
        "root_scalar_count": 0,
        "child_scalar_count": 0,
        "profile_count": 0,
        "winner_count": 0,
    }.items():
        assert getattr(seal, name) == getattr(outcome.seal, name) == expected
    assert seal.materialization_sha256.hex() == outcome.seal.materialization_sha256
    assert seal.effective_output_sha256.hex() == outcome.seal.effective_output_sha256


async def _assert_activated(case, request, policy, outcome, *, fence):
    assert outcome.status == "activated"
    assert (outcome.accepted_family_count, outcome.rejection_count) == (2, 0)
    assert outcome.publication.event_kind == "activated"
    assert outcome.publication.from_generation_id is None
    assert outcome.publication.to_generation_id == outcome.generation_id
    assert (outcome.publication.expected_pointer_version, outcome.publication.committed_pointer_version) == (0, 1)
    async with case.sessions() as session:
        execution = (await session.scalars(select(CustomImportExecution))).one()
        capture = (await session.scalars(select(CustomImportCaptureBundle))).one()
        generation = (await session.scalars(select(CustomImportGeneration))).one()
        seal = (await session.scalars(select(CustomImportGenerationSeal))).one()
        build = (
            await session.scalars(
                select(CustomImportBuildAttempt).where(CustomImportBuildAttempt.producing_fence == fence)
            )
        ).one()
        lease = await session.get(CustomImportLease, outcome.execution_id)
        pointer = await session.get(CustomImportCurrentGeneration, request.dataset_id)
        assert execution.execution_id == capture.producing_execution_id == outcome.execution_id
        assert execution.capture_bundle_id == capture.capture_bundle_id
        assert (
            execution.source_binding_revision_id
            == capture.source_binding_revision_id
            == request.source_binding_revision_id
        )
        assert capture.source_binding_sha256 == request.source_binding_sha256
        assert capture.capture_state == "sealed" and capture.sealed_at is not None
        assert capture.producing_fence == 1 and lease.fence == fence
        assert capture.committed_part_count == capture.committed_record_count == 4
        assert execution.state == "completed" and execution.finished_at is not None
        for artifact in (build, generation, seal):
            assert artifact.execution_id == execution.execution_id
            assert artifact.capture_bundle_id == capture.capture_bundle_id
            assert (artifact.dataset_id, artifact.definition_revision_id, artifact.schema_revision_id) == (
                request.dataset_id,
                request.definition_revision_id,
                request.schema_revision_id,
            )
        assert generation.producing_fence == seal.sealing_fence == fence
        assert generation.generation_id == build.generation_id == outcome.generation_id == pointer.generation_id
        assert pointer.pointer_version == 1
        assert await session.scalar(select(func.count()).select_from(CustomImportPublicationEvent)) == 1
        assert build.phase == "verified" and build.verified_at is not None
        assert build.source_occurrence_count == 4 and build.candidate_error_count == 0
        assert build.selected_family_count == build.completed_family_count == 2
        assert build.base_generation_id is None and build.base_pointer_version == 0
        assert build.build_deadline_at == capture.sealed_at + dt.timedelta(seconds=policy.build.build_deadline_seconds)
        assert build.request_identity_sha256 == execution.request_identity_sha256
        _assert_seal_counts(seal, outcome)
        return build


def _assert_source_once(connector, request, cursor, connection):
    assert cursor.executed == [connector.build_statement(request.bundle_request).sql]
    assert cursor.fetchone.call_count == len(request.bundle_request.bindings) + len(_ROWS) + 1
    assert cursor.closed and connection.closed


async def test_native_cursor_runs_one_claim_through_ordinary_activation(monkeypatch):
    claim = AsyncMock(wraps=snowflake_candidate.claim_execution)
    monkeypatch.setattr(snowflake_candidate, "claim_execution", claim)
    async with _source_case() as case:
        connector, request, policy, cursor, connection = await _registered_runtime(case, monkeypatch)
        result = await runner.run_segmented_snowflake_candidate(
            case.sessions, connector, request, processing_policy=policy
        )
        await _assert_activated(case, request, policy, result, fence=1)
        claim.assert_awaited_once()
        assert claim.await_args.kwargs["execution_id"] == result.execution_id
        _assert_source_once(connector, request, cursor, connection)


async def test_sealed_capture_resumes_next_fence_without_source_or_deadline_extension(monkeypatch):
    stage_source = runner.stage_segmented_source

    async def interrupt_after_source(*args, **kwargs):
        await stage_source(*args, **kwargs)
        raise ConnectionError("synthetic interruption after durable source staging")

    claim = AsyncMock(wraps=snowflake_candidate.claim_execution)
    monkeypatch.setattr(snowflake_candidate, "claim_execution", claim)
    async with _source_case() as case:
        connector, request, policy, cursor, connection = await _registered_runtime(case, monkeypatch)
        with monkeypatch.context() as interrupted:
            interrupted.setattr(runner, "stage_segmented_source", interrupt_after_source)
            with pytest.raises(ConnectionError, match="after durable source staging"):
                await runner.run_segmented_snowflake_candidate(
                    case.sessions, connector, request, processing_policy=policy
                )
        claim.assert_awaited_once()
        async with case.sessions() as session, session.begin():
            original = (await session.scalars(select(CustomImportBuildAttempt))).one()
            capture = (await session.scalars(select(CustomImportCaptureBundle))).one()
            assert original.phase == "graph" and original.producing_fence == 1
            assert original.build_deadline_at == capture.sealed_at + dt.timedelta(
                seconds=policy.build.build_deadline_seconds
            )
            assert await session.scalar(select(CustomImportGeneration.generation_id)) is None
            await session.execute(
                update(CustomImportLease)
                .where(CustomImportLease.execution_id == original.execution_id)
                .values(expires_at=func.clock_timestamp() - dt.timedelta(seconds=1))
            )
        forbidden = Mock(side_effect=AssertionError("sealed capture resume must not access the source"))
        for source_component, name in (
            (connector._credential_provider, "load_key_pair"),
            (connector._adapter, "_connect"),
            (connector._adapter, "fetch_bundle"),
            (connector._adapter, "open_bundle_landing"),
        ):
            monkeypatch.setattr(source_component, name, forbidden)
        resumed = replace(request, lease_token=b"synthetic-next-owner")
        outcome = await runner.run_segmented_snowflake_candidate(
            case.sessions, connector, resumed, processing_policy=policy
        )
        build = await _assert_activated(case, resumed, policy, outcome, fence=2)
        assert build.build_id != original.build_id
        assert build.execution_id == original.execution_id
        assert build.capture_bundle_id == original.capture_bundle_id
        assert build.build_deadline_at == original.build_deadline_at
        assert claim.await_count == 2
        assert [call.kwargs["execution_id"] for call in claim.await_args_list] == [outcome.execution_id] * 2
        forbidden.assert_not_called()
        _assert_source_once(connector, request, cursor, connection)
