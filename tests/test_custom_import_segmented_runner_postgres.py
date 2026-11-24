# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native one-claim capture, bounded build, activation, and source-free resume."""

from __future__ import annotations

import datetime as dt
import hashlib
import json
from dataclasses import asdict, replace
from decimal import Decimal
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy import func, select, text, update
from sqlalchemy.exc import DBAPIError

from db.models.custom_import import (
    CustomImportBuildAttempt,
    CustomImportCapture,
    CustomImportCaptureBundle,
    CustomImportCurrentGeneration,
    CustomImportExecution,
    CustomImportGeneration,
    CustomImportGenerationSeal,
    CustomImportLease,
    CustomImportPublicationEvent,
    CustomImportRegistrationAuthority,
)
from db.models.custom_import_family_block import CustomImportFamilyBlockLayout
from db.models.custom_import_storage import CustomImportSnapshotFamily
from process.custom_import import compact_materialization, family_block_sample, runner_codec, snowflake_candidate
from process.custom_import import snowflake_segmented_runner as runner
from process.custom_import.definition import (
    ROOT_BOOLEAN_TRUE_STORAGE_PROFILE,
    ROOTED_CHILD_TEXT_STORAGE_PROFILE,
    CustomImportDefinition,
    canonical_json,
)
from process.custom_import.family_block_codec import FamilyBlockLimits
from process.custom_import.processing_policy import BuildPolicy, ProcessingPolicy
from process.custom_import.publication import activate_generation, rollback_generation
from process.custom_import.read_core import (
    CustomImportReadAuthorizationError,
    CustomImportReadService,
    ExtensionReadAuthorization,
    ExtensionReadScope,
    PinnedReadTarget,
    ReadFilter,
    SearchRequest,
)
from process.custom_import.registration_authority import mint_registration_authority, register_with_authority
from process.custom_import.retained_profile_replay import run_retained_profile_replay
from process.custom_import.runner_registry import load_current_pointer
from process.custom_import.runner_types import CandidateRunnerError
from process.custom_import.snowflake_binding import (
    SOURCE_BINDING_V2_CONTRACT,
    RetainedProfileBase,
    SnowflakeSourceBinding,
)
from process.custom_import.snowflake_candidate import SnowflakeBundleCandidateRequest
from process.custom_import.snowflake_source_binding import (
    load_snowflake_source_binding,
    register_snowflake_source_binding,
)
from process.custom_import.storage_layout import snapshot_schema
from tests.test_custom_import_build_source_postgres import _source_case
from tests.test_custom_import_bulk_snapshot_writers_postgres import _install_snapshot_revision
from tests.test_custom_import_snowflake_capture import _policy as _capture_policy
from tests.test_custom_import_snowflake_shared_capture import _runtime, _shared_row

_ROWS = (_shared_row("1003000126", key="a"), _shared_row("1234567893", key="b"))
_ROOTED_TEXT_INDEX = "custom_import_child_scalar_rooted_text_idx"


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


async def _registered_runtime(case, monkeypatch, *, child_text=False, storage_profile=None, retained_base=None):
    policy = ProcessingPolicy(_capture_policy(), 17, BuildPolicy(16 if child_text else 8, 65_536, 2000, 120, 300))
    connector, bundle, adapter, cursor, connection = _runtime(
        monkeypatch, _ROWS, partition_rows=1, processing_policy=policy
    )
    monkeypatch.setattr(adapter, "_connect", lambda _credentials, **_options: (connection, cursor))
    definition = _read_definition(bundle.definition) if child_text else bundle.definition
    if storage_profile is not None:
        document = json.loads(definition.canonical)
        document["revision"]["definition"] += 1
        document["storage_profile"] = storage_profile
        definition = CustomImportDefinition.from_mapping(document, previous=definition)
    if child_text or storage_profile is not None:
        bundle = connector.prepare_request(
            definition,
            bindings=bundle.bindings,
            encoding=bundle.encoding,
            processing_policy=policy,
        )
    binding = _binding(connector.build_statement(bundle), policy)
    if retained_base is not None:
        binding = replace(binding, retained_profile_base=retained_base)
    dataset_key = "synthetic_segmented_runner"
    if storage_profile is not None:
        registered = await _register_profile(case, monkeypatch, dataset_key, bundle.definition, binding)
    else:
        async with case.sessions() as session, session.begin():
            registered = await register_snowflake_source_binding(
                session, dataset_key=dataset_key, definition=bundle.definition, binding=binding
            )
    request = SnowflakeBundleCandidateRequest(
        dataset_id=registered.dataset_id,
        definition_revision_id=registered.definition_revision_id,
        schema_revision_id=registered.schema_revision_id,
        definition=bundle.definition,
        bundle_request=bundle,
        idempotency_key=f"synthetic-{storage_profile}-run" if storage_profile else "synthetic-segmented-run",
        lease_token=b"synthetic-segmented-owner",
        source_binding_revision_id=registered.source_binding_revision_id,
        source_binding_sha256=registered.source_binding_sha256,
    )
    return connector, request, policy, cursor, connection


async def _register_profile(case, monkeypatch, dataset_key, definition, binding):
    registration_by_name = {
        "dataset_key": dataset_key,
        "definition": json.loads(definition.canonical),
        "source_binding": json.loads(binding.canonical),
    }
    digest = hashlib.sha256(canonical_json(registration_by_name).encode("utf-8")).hexdigest()
    monkeypatch.setenv("HLTHPRT_CUSTOM_IMPORT_STORAGE_PROFILE_REGISTRATION_SHA256", digest)
    token = b"a" * 32
    async with case.sessions() as session, session.begin():
        await mint_registration_authority(
            session,
            authority_id="synthetic-rooted-text",
            registration=registration_by_name,
            token_sha256=hashlib.sha256(token).digest(),
            expires_at=dt.datetime.now(dt.UTC) + dt.timedelta(seconds=120),
        )
    async with case.sessions() as session, session.begin():
        state = await register_with_authority(
            session, authority_id="synthetic-rooted-text", registration=registration_by_name, token=token
        )
        return await load_snowflake_source_binding(
            session,
            definition_revision_id=state.result["definition_revision_id"],
            source_binding_revision_id=state.result["source_binding_revision_id"],
        )


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


async def _install_storage_profile_fixture(case):
    async with case.engine.begin() as connection:
        for revision in (
            "20261114010000_custom_import_snapshot_names",
            "20261114020000_custom_import_snapshot_metadata",
            "20261121050000_custom_import_rooted_text",
            "20261123182000_custom_import_root_boolean_index",
        ):
            await connection.run_sync(_install_snapshot_revision, case.schema_name, revision)
        await connection.run_sync(CustomImportRegistrationAuthority.__table__.create)


async def _assert_current_child_text_lookups(case, request, generation_id):
    target = PinnedReadTarget(
        request.dataset_id, generation_id, request.definition_revision_id, request.schema_revision_id, "default"
    )
    reader = CustomImportReadService(authorizer=_ExactReadAuthorizer(target), cursor_secret=b"r" * 32)
    async with case.sessions() as session:
        pointer = await session.get(CustomImportCurrentGeneration, request.dataset_id)
        assert pointer.generation_id == generation_id
        for key, expected_npis in (("a", ("1003000126",)), ("b", ("1234567893",)), ("absent", ())):
            page = await reader.search(
                session,
                authorization=ExtensionReadAuthorization("synthetic-reader"),
                request=SearchRequest(target, filters=(ReadFilter("detail_id", "eq", key),)),
            )
            assert (page.total, page.next_cursor) == (len(expected_npis), None)
            assert tuple(item.root_fields[0].value for item in page.items) == expected_npis
            for item in page.items:
                detail_id = next(field for field in item.context_fields if field.field_id == "detail_id")
                assert (detail_id.state, detail_id.value) == ("value", key)


async def _assert_candidate_index_and_payload_parity(
    case, baseline_generation_id, candidate_generation_id, index_name, scalar_table, index_shape
):
    """Check the index on the new family and compare both logical snapshots."""

    async with case.sessions() as session, session.begin():
        family_id_by_generation = dict(
            (
                await session.execute(
                    select(CustomImportSnapshotFamily.generation_id, CustomImportSnapshotFamily.family_id)
                )
            )
            .tuples()
            .all()
        )
        old = snapshot_schema(family_id_by_generation[baseline_generation_id])
        candidate_family_id = family_id_by_generation[candidate_generation_id]
        new = snapshot_schema(candidate_family_id)
        assert await session.scalar(text("SELECT to_regclass(:index)"), {"index": f"{old}.{index_name}"}) is None
        shape = (
            await session.execute(
                text("""
                SELECT i.indnkeyatts, i.indnatts,
                       ARRAY(SELECT pg_get_indexdef(i.indexrelid,n,false) FROM generate_series(1,i.indnatts) n),
                       regexp_replace(pg_get_expr(i.indpred,i.indrelid),'[()[:space:]]|::text','','g'),
                       i.indisvalid AND i.indisready AND i.indislive AND am.amname='btree' AND NOT i.indisunique
                FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_am am ON am.oid=c.relam
                WHERE i.indexrelid=to_regclass(:index) AND i.indrelid=to_regclass(:heap)
                """),
                {"index": f"{new}.{index_name}", "heap": f"{new}.{scalar_table}"},
            )
        ).one()
        assert shape == index_shape
        for table in ("custom_import_root_revision", "custom_import_child_revision"):
            query = 'SELECT array_agg(canonical_payload ORDER BY canonical_payload) FROM "{}".{}'
            expected = await session.scalar(text(query.format(old, table)))
            actual = await session.scalar(text(query.format(new, table)))
            assert expected and actual == expected
    return old, candidate_family_id


async def _profile_source_base(case, baseline):
    """Capture the exact current source scope for approval on the successor binding."""

    async with case.sessions() as session:
        generation = await session.get(CustomImportGeneration, baseline.generation_id)
        capture = await session.get(CustomImportCaptureBundle, generation.capture_bundle_id)
        return RetainedProfileBase(
            baseline.generation_id, baseline.publication.committed_pointer_version, capture.manifest_sha256.hex()
        )


async def _replay_profile(case, original_statement, connector, request, baseline, *, base=None, compact_limits=None):
    """Build from the parent capture with the successor's registered binding."""

    async with case.sessions() as session:
        loaded = await load_snowflake_source_binding(
            session,
            definition_revision_id=request.definition_revision_id,
            source_binding_revision_id=request.source_binding_revision_id,
        )
        generation = await session.get(CustomImportGeneration, baseline.generation_id)
        parent_capture_id = generation.capture_bundle_id
        base = base or loaded.binding.retained_profile_base
    args = (case.sessions, loaded, request, connector.build_statement(request.bundle_request))

    def parent_statement(_binding):
        return original_statement

    with pytest.raises(CandidateRunnerError, match="retained replay (base differs|source is unavailable)"):
        wrong = RetainedProfileBase(base.generation_id, base.pointer_version, "0" * 64)
        await run_retained_profile_replay(
            *args, base=wrong, parent_statement=parent_statement, compact_limits=compact_limits
        )
    if compact_limits is not None:
        with pytest.raises(CandidateRunnerError, match="compact retained replay requires the local owner"):
            await run_retained_profile_replay(
                *args,
                base=base,
                parent_statement=parent_statement,
                writer_transports=object(),
                compact_limits=compact_limits,
            )
    unpublished = await run_retained_profile_replay(
        *args, base=base, parent_statement=parent_statement, compact_limits=compact_limits
    )
    assert unpublished.status == "sealed_unpublished"
    assert unpublished.publication is None
    recovered = await run_retained_profile_replay(
        *args, base=base, parent_statement=parent_statement, compact_limits=compact_limits
    )
    assert (recovered.status, recovered.execution_id, recovered.generation_id) == (
        unpublished.status,
        unpublished.execution_id,
        unpublished.generation_id,
    )
    async with case.sessions() as session:
        pointer = await load_current_pointer(session, request.dataset_id)
        assert (pointer.generation_id, pointer.version) == (base.generation_id, base.pointer_version)
        generation = await session.get(CustomImportGeneration, unpublished.generation_id)
        assert generation.capture_bundle_id != parent_capture_id
        manifest = await session.scalar(
            select(CustomImportCapture.canonical_manifest)
            .where(CustomImportCapture.capture_bundle_id == generation.capture_bundle_id)
            .limit(1)
        )
        origin = json.loads(manifest)["retained_replay"]
        assert origin["parent_capture_bundle_id"] == parent_capture_id
        assert origin["parent_capture_manifest_sha256"] == base.capture_manifest_sha256
    return unpublished


async def _approve_profile_publication(case, request, baseline, candidate):
    """Apply the ordinary pointer switch after the unpublished proof."""

    if candidate.publication is not None:
        return candidate.publication
    async with case.sessions() as session, session.begin():
        return await activate_generation(
            session,
            dataset_id=request.dataset_id,
            target_generation_id=candidate.generation_id,
            expected_generation_id=baseline.generation_id,
            expected_pointer_version=baseline.publication.committed_pointer_version,
        )


async def test_owner_retained_replay_seals_compact_candidate_before_publication(monkeypatch):
    async with _source_case() as case:
        await _install_storage_profile_fixture(case)
        async with case.engine.begin() as connection:
            await connection.run_sync(
                _install_snapshot_revision, case.schema_name, "20261104020000_custom_import_family_blocks"
            )
        connector, original, policy, _, _ = await _registered_runtime(case, monkeypatch, child_text=True)
        baseline = await runner.run_segmented_snowflake_candidate(
            case.sessions, connector, original, processing_policy=policy
        )
        assert baseline.status == "activated"
        original_statement = connector.build_statement(original.bundle_request)
        base = await _profile_source_base(case, baseline)
        request = replace(original, idempotency_key="synthetic-compact-rebuild", lease_token=b"synthetic-compact-owner")
        limits = FamilyBlockLimits(max_spool_bytes=16 * 1024**2, max_record_bytes=64 * 1024)
        candidate = await _replay_profile(
            case, original_statement, connector, request, baseline, base=base, compact_limits=limits
        )
        assert candidate.seal.effective_output_sha256 == baseline.seal.effective_output_sha256
        async with case.sessions() as session:
            layout = await session.scalar(
                select(CustomImportFamilyBlockLayout)
                .join(
                    CustomImportSnapshotFamily,
                    CustomImportSnapshotFamily.family_id == CustomImportFamilyBlockLayout.family_id,
                )
                .where(CustomImportSnapshotFamily.generation_id == candidate.generation_id)
            )
            assert layout is not None and layout.phase == "logged"
        publication = await _approve_profile_publication(case, original, baseline, candidate)
        await _assert_current_child_text_lookups(case, request, candidate.generation_id)
        candidate = replace(candidate, publication=publication)
        await _assert_unchanged_compact_replay(case, request, original_statement, candidate, limits)
        rejected, carried = await _build_rejected_compact_refresh(case, monkeypatch, request, policy, candidate, limits)
        await _assert_compact_refresh_publication(case, rejected, candidate, carried)


async def _assert_unchanged_compact_replay(case, request, original_statement, candidate, limits):
    compact_base = await _profile_source_base(case, candidate)
    retry = replace(request, idempotency_key="synthetic-compact-refresh", lease_token=b"synthetic-refresh-owner")
    async with case.sessions() as session:
        loaded = await load_snowflake_source_binding(
            session,
            definition_revision_id=retry.definition_revision_id,
            source_binding_revision_id=retry.source_binding_revision_id,
        )
    unchanged = await run_retained_profile_replay(
        case.sessions,
        loaded,
        retry,
        original_statement,
        base=compact_base,
        parent_statement=lambda _binding: original_statement,
        compact_limits=limits,
    )
    assert unchanged.status == "no_change"
    assert unchanged.publication.to_generation_id == candidate.generation_id
    await _assert_current_child_text_lookups(case, retry, candidate.generation_id)


async def _build_rejected_compact_refresh(case, monkeypatch, request, policy, candidate, limits):
    compact_base = await _profile_source_base(case, candidate)
    changed_b = _shared_row(_ROWS[1][4], key="b", amount=Decimal("7.25"))
    connector, _, adapter, cursor, connection = _runtime(
        monkeypatch, (_ROWS[0], changed_b, _ROWS[0]), partition_rows=1, processing_policy=policy
    )
    monkeypatch.setattr(adapter, "_connect", lambda _credentials, **_options: (connection, cursor))
    rejected = replace(request, idempotency_key="synthetic-rejected-refresh", lease_token=b"synthetic-reject-owner")
    captured = await runner._capture_for_build(case.sessions, connector, rejected, policy)
    retained_key = runner_codec.root_key_hash_from_tuple(request.definition, (_ROWS[0][4],))
    seed = next(
        word.to_bytes(32, "big")
        for word in range(256)
        if int.from_bytes(hashlib.sha256(word.to_bytes(32, "big") + retained_key).digest()[:2], "big") % 5 == 0
    )
    monkeypatch.setattr(compact_materialization, "_seed", lambda *_args: seed)
    sampled = AsyncMock(wraps=family_block_sample._sample_retained)
    monkeypatch.setattr(family_block_sample, "_sample_retained", sampled)
    carried = await runner.build_segmented_capture(
        case.sessions,
        rejected,
        captured,
        policy,
        retained_base=(compact_base.generation_id, compact_base.pointer_version),
        compact_limits=limits,
    )
    assert carried.status == "sealed_unpublished" and carried.rejection_count > 0
    assert carried.seal.effective_output_sha256 != candidate.seal.effective_output_sha256
    sampled.assert_awaited_once()
    async with case.sessions() as session:
        family_id = await session.scalar(
            select(CustomImportSnapshotFamily.family_id).where(
                CustomImportSnapshotFamily.generation_id == carried.generation_id
            )
        )
        population = await session.execute(
            text(
                f'SELECT retained, count(*) FROM "{snapshot_schema(family_id)}".custom_import_block_family '
                "GROUP BY retained ORDER BY retained"
            )
        )
        assert population.all() == [(False, 1), (True, 1)]
    return rejected, carried


async def _assert_compact_refresh_publication(case, rejected, candidate, carried):
    await _assert_current_child_text_lookups(case, rejected, candidate.generation_id)
    cut = await _approve_profile_publication(case, rejected, candidate, carried)
    assert cut.from_generation_id == candidate.generation_id
    await _assert_current_child_text_lookups(case, rejected, carried.generation_id)
    async with case.sessions() as session, session.begin():
        rollback = await rollback_generation(
            session,
            dataset_id=rejected.dataset_id,
            target_generation_id=candidate.generation_id,
            expected_generation_id=carried.generation_id,
            expected_pointer_version=cut.committed_pointer_version,
        )
        assert rollback.to_generation_id == candidate.generation_id
    await _assert_current_child_text_lookups(case, rejected, candidate.generation_id)


@pytest.mark.parametrize(
    ("storage_profile", "index_name", "scalar_table", "index_shape"),
    (
        (
            ROOT_BOOLEAN_TRUE_STORAGE_PROFILE,
            "custom_import_root_scalar_true_idx",
            "custom_import_root_scalar",
            (2, 2, ["root_revision_id", "field_slot"], "boolean_value", True),
        ),
        (
            ROOTED_CHILD_TEXT_STORAGE_PROFILE,
            _ROOTED_TEXT_INDEX,
            "custom_import_child_scalar",
            (
                5,
                7,
                [
                    "schema_revision_id",
                    "collection_slot",
                    "field_slot",
                    "root_record_id",
                    "string_value",
                    "dataset_id",
                    "child_revision_id",
                ],
                "value_state='value'ANDstring_valueISNOTNULL",
                True,
            ),
        ),
    ),
)
async def test_native_storage_profile_builds_on_candidate_and_preserves_rollback(
    monkeypatch, storage_profile, index_name, scalar_table, index_shape
):
    """Verify the candidate index and query results across activation and rollback."""

    async with _source_case() as case:
        await _install_storage_profile_fixture(case)
        connector, original, policy, _, _ = await _registered_runtime(case, monkeypatch, child_text=True)
        baseline = await runner.run_segmented_snowflake_candidate(
            case.sessions, connector, original, processing_policy=policy
        )
        assert baseline.status == "activated"
        original_statement = connector.build_statement(original.bundle_request)
        await _assert_current_child_text_lookups(case, original, baseline.generation_id)
        retained_base = (
            await _profile_source_base(case, baseline) if storage_profile == ROOT_BOOLEAN_TRUE_STORAGE_PROFILE else None
        )
        connector, opted_in, policy, _, _ = await _registered_runtime(
            case, monkeypatch, child_text=True, storage_profile=storage_profile, retained_base=retained_base
        )
        assert (opted_in.dataset_id, opted_in.schema_revision_id) == (original.dataset_id, original.schema_revision_id)
        assert opted_in.definition_revision_id != original.definition_revision_id
        if storage_profile == ROOT_BOOLEAN_TRUE_STORAGE_PROFILE:
            candidate = await _replay_profile(case, original_statement, connector, opted_in, baseline)
        else:
            candidate = await runner.run_segmented_snowflake_candidate(
                case.sessions, connector, opted_in, processing_policy=policy
            )
            assert candidate.status == "activated"
        assert candidate.seal.effective_output_sha256 != baseline.seal.effective_output_sha256
        assert candidate.seal.child_scalar_count == baseline.seal.child_scalar_count == 6
        old, candidate_family_id = await _assert_candidate_index_and_payload_parity(
            case, baseline.generation_id, candidate.generation_id, index_name, scalar_table, index_shape
        )
        publication = await _approve_profile_publication(case, original, baseline, candidate)
        assert publication.from_generation_id == baseline.generation_id
        await _assert_current_child_text_lookups(case, opted_in, candidate.generation_id)

        with pytest.raises(DBAPIError, match="custom_import_snapshot_attempt_lost"):
            async with case.sessions() as session, session.begin():
                await session.execute(text("SET LOCAL statement_timeout='2000ms'"))
                await session.scalar(
                    text(f"SELECT \"{case.schema_name}\".prepare_custom_import_snapshot_indexes(:family,'serving')"),
                    {"family": candidate_family_id},
                )

        async with case.sessions() as session, session.begin():
            rollback = await rollback_generation(
                session,
                dataset_id=original.dataset_id,
                target_generation_id=baseline.generation_id,
                expected_generation_id=candidate.generation_id,
                expected_pointer_version=publication.committed_pointer_version,
            )
            assert rollback.to_generation_id == baseline.generation_id
            assert rollback.event_kind == "rolled_back"
            assert await session.scalar(text("SELECT to_regclass(:index)"), {"index": f"{old}.{index_name}"}) is None
        await _assert_current_child_text_lookups(case, original, baseline.generation_id)


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


def _read_definition(definition, *, nullable_other=False):
    document = json.loads(definition.canonical)
    if nullable_other:
        document["schema"]["children"][1]["fields"][2]["nullable"] = True
    projection_slot = 0
    for scope in (document["schema"]["root"], *document["schema"]["children"]):
        for field in scope["fields"]:
            projection_slot += 1
            field["projection_slot"] = projection_slot
    document["query"] = {
        "root_fields": ["npi", "score", "enabled"],
        "child": {"collection": "details", "fields": ["detail_npi", "detail_id", "amount"]},
        "order": [{"field": "amount", "direction": "asc", "nulls": "last"}],
    }
    document["selection_profiles"] = [
        {
            "id": "default",
            "selection": [{"field": "amount", "direction": "asc", "nulls": "last"}],
            "context_dimensions": ["detail_id"],
        }
    ]
    return CustomImportDefinition.from_mapping(document)


async def _registered_capture(case, monkeypatch):
    key = 'same "key" \\ café'
    physical_rows = [
        _shared_row("1003000126", key=key, amount=Decimal("0")) + (None, None, None),
        _shared_row("1234567893", key=key, amount=Decimal("123456789012345678.123456789012")) + (None, None, None),
        (1, 2, "other_source", None, None, None, None, None, None, None, "1234567893", key, None),
        (1, 2, "other_source", None, None, None, None, None, None, None, "1003000126", key, Decimal("1.25")),
    ]
    physical_rows[1] = (*physical_rows[1][:6], False, *physical_rows[1][7:])
    policy = ProcessingPolicy(_capture_policy(), 17, BuildPolicy(16, 65_536, 2000, 120, 300))
    connector, bundle, adapter, cursor, connection = _runtime(
        monkeypatch, physical_rows, interleaved=True, partition_rows=1, processing_policy=policy
    )
    bundle = connector.prepare_request(
        _read_definition(bundle.definition, nullable_other=True),
        bindings=bundle.bindings,
        encoding=bundle.encoding,
        processing_policy=policy,
    )
    monkeypatch.setattr(adapter, "_connect", lambda _credentials, **_options: (connection, cursor))
    async with case.sessions() as session, session.begin():
        registered = await register_snowflake_source_binding(
            session,
            dataset_key="synthetic_two_collections",
            definition=bundle.definition,
            binding=_binding(connector.build_statement(bundle), policy),
        )
    request = SnowflakeBundleCandidateRequest(
        dataset_id=registered.dataset_id,
        definition_revision_id=registered.definition_revision_id,
        schema_revision_id=registered.schema_revision_id,
        definition=bundle.definition,
        bundle_request=bundle,
        idempotency_key="synthetic-two-collections",
        lease_token=b"synthetic-two-collection-owner",
        source_binding_revision_id=registered.source_binding_revision_id,
        source_binding_sha256=registered.source_binding_sha256,
    )
    return connector, request, policy, cursor, connection, key


class _ExactReadAuthorizer:
    def __init__(self, target):
        self.target = target

    def authorize(self, authorization, *, target):
        if target == self.target and authorization.credential == "synthetic-reader":
            return ExtensionReadScope("synthetic:two-collections")
        return None


def _assert_fields(fields, expected):
    assert {field.field_id for field in fields} == set(expected)
    for field in fields:
        field_type, value = expected[field.field_id]
        assert (field.field_type, field.state, field.value) == (field_type, "null" if value is None else "value", value)
        assert type(field.value) is type(value)


async def _assert_published(session, request, outcome):
    capture = (await session.scalars(select(CustomImportCaptureBundle))).one()
    execution = await session.get(CustomImportExecution, outcome.execution_id)
    build = (await session.scalars(select(CustomImportBuildAttempt))).one()
    seal = (await session.scalars(select(CustomImportGenerationSeal))).one()
    pointer = await session.get(CustomImportCurrentGeneration, request.dataset_id)
    assert capture.capture_state == "sealed" and capture.committed_record_count == capture.committed_part_count == 6
    assert capture.producing_execution_id == execution.execution_id == build.execution_id == outcome.execution_id
    assert execution.state == "completed" and build.phase == "verified"
    assert build.capture_bundle_id == capture.capture_bundle_id and build.source_occurrence_count == 6
    assert pointer.generation_id == seal.generation_id == build.generation_id == outcome.generation_id
    assert (seal.root_count, seal.family_count, seal.family_child_count, seal.winner_count) == (2, 2, 4, 2)


def _assert_family(detail, key):
    npi = next(field.value for field in detail.root_fields if field.field_id == "npi")
    is_first_family = npi == "1003000126"
    assert npi in {"1003000126", "1234567893"}
    score = Decimal("0") if is_first_family else Decimal("123456789012345678.123456789012")
    _assert_fields(
        detail.root_fields,
        {"npi": ("string", npi), "score": ("decimal", score), "enabled": ("boolean", is_first_family)},
    )
    children_by_collection = {child.collection: child for child in detail.children}
    assert len(detail.children) == len(children_by_collection) == 2 and set(children_by_collection) == {
        "details",
        "other",
    }
    _assert_fields(
        children_by_collection["details"].fields,
        {"detail_npi": ("string", npi), "detail_id": ("string", key), "amount": ("decimal", score)},
    )
    _assert_fields(
        children_by_collection["other"].fields,
        {
            "other_npi": ("string", npi),
            "other_id": ("string", key),
            "other_amount": ("decimal", Decimal("1.25") if is_first_family else None),
        },
    )
    child_ids = {child.child_revision_id for child in detail.children}
    assert len(child_ids) == 2
    return npi, child_ids


async def _assert_reads(session, read_target, key):
    service = CustomImportReadService(authorizer=_ExactReadAuthorizer(read_target), cursor_secret=b"r" * 32)
    authorization = ExtensionReadAuthorization("synthetic-reader")
    with pytest.raises(CustomImportReadAuthorizationError):
        await service.search(
            session, authorization=ExtensionReadAuthorization("synthetic-denied"), request=SearchRequest(read_target)
        )
    with pytest.raises(CustomImportReadAuthorizationError):
        await service.search(
            session,
            authorization=authorization,
            request=SearchRequest(replace(read_target, generation_id=read_target.generation_id + 1)),
        )
    page = await service.search(session, authorization=authorization, request=SearchRequest(read_target))
    assert page.total == len(page.items) == 2 and page.next_cursor is None
    with pytest.raises(CustomImportReadAuthorizationError):
        await service.root_detail(
            session,
            authorization=authorization,
            target=replace(read_target, generation_id=read_target.generation_id + 1),
            winner=page.items[0].winner,
        )
    seen_child_ids = set()
    seen_npis = set()
    for search_item in page.items:
        detail = await service.root_detail(
            session, authorization=authorization, target=read_target, winner=search_item.winner
        )
        npi, child_ids = _assert_family(detail, key)
        assert child_ids.isdisjoint(seen_child_ids)
        seen_child_ids.update(child_ids)
        seen_npis.add(npi)
    assert len(seen_child_ids) == 4 and seen_npis == {"1003000126", "1234567893"}


async def test_two_collections_capture_publish_and_read_exact_family_values(monkeypatch):
    """One source statement retains, publishes and authorizes both typed collections."""
    async with _source_case() as case:
        connector, request, policy, cursor, connection, key = await _registered_capture(case, monkeypatch)
        outcome = await runner.run_segmented_snowflake_candidate(
            case.sessions, connector, request, processing_policy=policy
        )
        assert outcome.status == "activated"
        assert (outcome.accepted_family_count, outcome.rejection_count) == (2, 0)
        assert outcome.publication.to_generation_id == outcome.generation_id
        assert cursor.executed == [connector.build_statement(request.bundle_request).sql]
        assert cursor.fetchone.call_count == 8 and cursor.closed and connection.closed
        read_target = PinnedReadTarget(
            request.dataset_id,
            outcome.generation_id,
            request.definition_revision_id,
            request.schema_revision_id,
            "default",
        )
        async with case.sessions() as session:
            await _assert_published(session, request, outcome)
            await _assert_reads(session, read_target, key)
