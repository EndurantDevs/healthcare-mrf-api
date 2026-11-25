# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL integration tests for direct compact v2 fast import and storage invariants."""

from __future__ import annotations

import datetime as dt
import json
from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock

import httpx
import pytest
from sanic.compat import Header
from sqlalchemy import select, text

from api import control_admission_batch as admission_api
from api import control_source_batch as source_api
from db.models.custom_import import (
    CustomImportBuildAttempt,
    CustomImportExecution,
    CustomImportGenerationSeal,
)
from db.models.custom_import_family_block import CustomImportFamilyBlockLayout
from db.models.custom_import_query_projection import CustomImportQueryProjection
from db.models.custom_import_storage import CustomImportSnapshotFamily
from process.custom_import import admission_authorization as admission_auth
from process.custom_import import admission_launch, admission_worker, runner_codec, snowflake_python, source_worker
from process.custom_import import snowflake_segmented_runner as runner
from process.custom_import import source_authorization as source_auth
from process.custom_import.capture_limits import CaptureLimits
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.processing_policy import BuildPolicy, ProcessingPolicy
from process.custom_import.publication import rollback_generation
from process.custom_import.read_core import (
    CustomImportReadService,
    ExtensionReadAuthorization,
    PinnedReadTarget,
    ReadFilter,
    ReadOrderTerm,
    SearchRequest,
)
from process.custom_import.snowflake import SnowflakeApprovedRelation, SnowflakeDeclaredColumn, SnowflakeRelation
from process.custom_import.snowflake_bundle import (
    SnowflakeBundleAcquisitionConnector,
    SnowflakeBundleBinding,
    SnowflakeBundleEncoding,
)
from process.custom_import.snowflake_candidate import SnowflakeBundleCandidateRequest
from process.custom_import.snowflake_source_binding import register_snowflake_source_binding
from process.custom_import.storage_layout import snapshot_schema
from tests import custom_import_grouped_child_support as grouped_child_fixture
from tests.admission_transport_test_support import writer_tls_context
from tests.test_custom_import_build_source_postgres import _source_case
from tests.test_custom_import_bulk_snapshot_writers_postgres import _install_snapshot_revision
from tests.test_custom_import_derived_query import derived_document
from tests.test_custom_import_segmented_runner_postgres import (
    _assert_reads,
    _binding,
    _ExactReadAuthorizer,
    _install_storage_profile_fixture,
    _read_definition,
    _registered_capture,
)
from tests.test_custom_import_snowflake_bundle import (
    _SNAPSHOT,
    _Credentials,
    _PythonConnection,
    _PythonCursor,
    _relations,
)
from tests.test_custom_import_snowflake_capture import _policy as _capture_policy
from tests.test_custom_import_snowflake_shared_capture import _runtime, _shared_definition, _shared_row


async def _install_compact_schema(case) -> None:
    await _install_storage_profile_fixture(case)
    async with case.engine.begin() as connection:
        await connection.run_sync(
            _install_snapshot_revision, case.schema_name, "20261104020000_custom_import_family_blocks"
        )


async def _snapshot_row_count(session, family_id: int, table_name: str) -> int:
    schema_name = snapshot_schema(family_id)
    return int(await session.scalar(text(f'SELECT count(*) FROM "{schema_name}"."{table_name}"')) or 0)


async def _snapshot_indexes(session, family_id: int) -> set[str]:
    schema_name = snapshot_schema(family_id)
    index_names = (
        await session.scalars(
            text("SELECT indexname FROM pg_indexes WHERE schemaname = :schema_name"),
            {"schema_name": schema_name},
        )
    ).all()
    return set(index_names)


async def _direct_stage_tables(session, family_id: int) -> list[str]:
    schema_name = snapshot_schema(family_id)
    return list(
        (
            await session.scalars(
                text(
                    "SELECT tablename FROM pg_tables "
                    "WHERE schemaname = :schema_name AND tablename LIKE 'direct_stage_%'"
                ),
                {"schema_name": schema_name},
            )
        ).all()
    )


async def _snapshot_family_id_for_generation(session, generation_id: int) -> int:
    return int(
        await session.scalar(
            select(CustomImportSnapshotFamily.family_id).where(
                CustomImportSnapshotFamily.generation_id == generation_id
            )
        )
    )


def _inline_root_definition(base_definition: CustomImportDefinition) -> CustomImportDefinition:
    doc_by_key = json.loads(base_definition.canonical)
    doc_by_key["streams"] = [stream for stream in doc_by_key["streams"] if stream["kind"] == "root"]
    doc_by_key["schema"]["children"] = []
    doc_by_key["aliases"] = {stream["id"]: doc_by_key["aliases"][stream["id"]] for stream in doc_by_key["streams"]}
    for slot, field in enumerate(doc_by_key["schema"]["root"]["fields"], 1):
        field["projection_slot"] = slot
    doc_by_key["query"] = {
        "root_fields": [field["id"] for field in doc_by_key["schema"]["root"]["fields"] if field["id"] != "npi"],
        "order": [{"field": "score", "direction": "asc", "nulls": "last"}],
    }
    doc_by_key["selection_profiles"] = [
        {
            "id": "default",
            "selection": [{"field": "score", "direction": "asc", "nulls": "last"}],
            "context_dimensions": [],
        }
    ]
    return CustomImportDefinition.from_mapping(doc_by_key)


def _mock_root_only_cursor(stream_id: str, root_tuples: tuple[tuple[object, ...], ...]) -> _PythonCursor:
    metadata_row = (0, 1, stream_id, _SNAPSHOT, None, None, None)
    data_rows = tuple((1, 1, stream_id, None, npi, score, enabled) for npi, score, enabled in root_tuples)
    cursor = _PythonCursor((metadata_row, *data_rows))
    cursor.description = (
        SimpleNamespace(name="__ci_bundle_row_kind", type_name="FIXED", is_nullable=False, precision=1, scale=0),
        SimpleNamespace(name="__ci_stream_ordinal", type_name="FIXED", is_nullable=False, precision=2, scale=0),
        SimpleNamespace(name="__ci_stream_id", type_name="TEXT", is_nullable=False),
        SimpleNamespace(name="__ci_source_snapshot_token", type_name="TEXT", is_nullable=True),
        SimpleNamespace(name="npi", type_name="TEXT", is_nullable=True),
        SimpleNamespace(name="score", type_name="FIXED", is_nullable=True, precision=30, scale=12),
        SimpleNamespace(name="enabled", type_name="BOOLEAN", is_nullable=True),
    )
    cursor.fetchone = Mock(wraps=cursor.fetchone)
    return cursor


async def _registered_root_only_capture(
    case, monkeypatch, root_tuples, *, dataset_key="synthetic_inline_v2", idempotency_key="inline-v2-1"
):
    """Register a root-only Snowflake bundle capture fixture in PostgreSQL."""
    policy = ProcessingPolicy(_capture_policy(), 17, BuildPolicy(16, 65_536, 10_000, 120, 300))
    snapshot, root_rel, _detail = _relations()
    root_columns = (
        root_rel.columns[0],
        replace(root_rel.columns[1], column_identifier="detail_amount"),
        root_rel.columns[2],
    )
    approved = (snapshot, SnowflakeApprovedRelation(root_rel.relation, root_columns))
    root_def = _inline_root_definition(_shared_definition())
    root_binding = SnowflakeBundleBinding(
        root_def.source_streams[0].stream_id,
        root_rel.relation,
        snapshot.relation,
        tuple(field.field_id for field in root_def.root_fields),
        "semantic_snapshot",
    )
    adapter = snowflake_python.SnowflakePythonConnectorAdapter(role="reader_role", warehouse="import_wh")
    connector = SnowflakeBundleAcquisitionConnector(
        approved_relations=approved,
        credential_provider=_Credentials(),
        adapter=adapter,
        capture_limits=CaptureLimits(),
    )
    bundle = connector.prepare_request(
        root_def,
        bindings=(root_binding,),
        encoding=SnowflakeBundleEncoding(partition_rows=2),
        processing_policy=policy,
    )
    cursor = _mock_root_only_cursor(root_binding.stream_id, root_tuples)
    connection = _PythonConnection(cursor)
    monkeypatch.setattr(adapter, "_connect", lambda _credentials, **_options: (connection, cursor))
    async with case.sessions() as session, session.begin():
        registered = await register_snowflake_source_binding(
            session,
            dataset_key=dataset_key,
            definition=bundle.definition,
            binding=_binding(connector.build_statement(bundle), policy),
        )
    request = SnowflakeBundleCandidateRequest(
        dataset_id=registered.dataset_id,
        definition_revision_id=registered.definition_revision_id,
        schema_revision_id=registered.schema_revision_id,
        definition=bundle.definition,
        bundle_request=bundle,
        idempotency_key=idempotency_key,
        lease_token=f"lease-{idempotency_key}".encode(),
        source_binding_revision_id=registered.source_binding_revision_id,
        source_binding_sha256=registered.source_binding_sha256,
    )
    return connector, request, policy


def _seal_counts(seal) -> tuple[int, ...]:
    return (
        seal.root_count,
        seal.family_count,
        seal.generation_family_count,
        seal.family_child_count,
        seal.winner_count,
        seal.profile_count,
        seal.root_scalar_count,
        seal.child_scalar_count,
    )


async def _assert_v2_storage_invariants(session, family_id: int, expected_families: int) -> None:
    layout = await session.get(CustomImportFamilyBlockLayout, family_id)
    assert layout is not None and layout.phase == "logged"
    assert layout.role_counts["family"] == expected_families
    assert layout.role_counts["generation_family"] == 0
    assert layout.role_counts["root_record"] == 0
    assert layout.role_counts["revision_run"] == 0
    assert layout.role_counts["fragment"] >= expected_families
    assert await _snapshot_row_count(session, family_id, "custom_import_generation_family") == 0
    assert await _snapshot_row_count(session, family_id, "custom_import_root_record") == 0
    assert await _snapshot_row_count(session, family_id, "custom_import_block_revision_run") == 0
    assert await _snapshot_row_count(session, family_id, "custom_import_build_occurrence") == 0
    assert await _direct_stage_tables(session, family_id) == []
    index_names = await _snapshot_indexes(session, family_id)
    assert "custom_import_block_fragment_record_lookup_idx" not in index_names
    assert "custom_import_entity_binding_canonical_order_idx" in index_names
    assert "custom_import_entity_binding_value_key" not in index_names
    assert "custom_import_entity_binding_dataset_value_key" not in index_names
    assert "custom_import_entity_binding_owner_key" not in index_names
    entity_indexes = set(
        (
            await session.scalars(
                text(
                    "SELECT indexname FROM pg_catalog.pg_indexes "
                    "WHERE schemaname = :schema_name AND tablename = 'custom_import_entity_binding'"
                ),
                {"schema_name": snapshot_schema(family_id)},
            )
        ).all()
    )
    assert entity_indexes == {
        "custom_import_entity_binding_pkey",
        "custom_import_entity_binding_canonical_order_idx",
    }


@pytest.mark.asyncio
async def test_direct_v2_matches_legacy(monkeypatch):
    """Direct compact v2 produces identical effective_output_sha256, counts, and API reads."""
    async with _source_case() as legacy_case:
        connector, request, policy, _cursor, _connection, _key = await _registered_capture(legacy_case, monkeypatch)
        legacy_outcome = await runner.run_segmented_snowflake_candidate(
            legacy_case.sessions, connector, request, processing_policy=policy
        )
        assert legacy_outcome.status == "activated"

    async with _source_case() as direct_case:
        await _install_compact_schema(direct_case)
        connector, request, policy, _cursor, _connection, _key = await _registered_capture(direct_case, monkeypatch)
        direct_outcome = await runner.run_segmented_snowflake_candidate(
            direct_case.sessions, connector, request, processing_policy=policy, direct_compact=True
        )
        assert direct_outcome.status == "activated"
        assert (direct_outcome.accepted_family_count, direct_outcome.rejection_count) == (
            legacy_outcome.accepted_family_count,
            legacy_outcome.rejection_count,
        )
        assert direct_outcome.seal.effective_output_sha256 == legacy_outcome.seal.effective_output_sha256
        assert _seal_counts(direct_outcome.seal) == _seal_counts(legacy_outcome.seal)

        async with direct_case.sessions() as session:
            family_id = await _snapshot_family_id_for_generation(session, direct_outcome.generation_id)
            await _assert_v2_storage_invariants(session, family_id, expected_families=2)
            read_target = PinnedReadTarget(
                request.dataset_id,
                direct_outcome.generation_id,
                request.definition_revision_id,
                request.schema_revision_id,
                "default",
            )
            await _assert_reads(session, read_target, _key)


@pytest.mark.asyncio
async def test_direct_v2_inline_and_no_change(monkeypatch):
    """Inlineable zero-child root families store 0 fragments, hydrate inline, and hit no_change."""
    root_tuples = (("1003000126", Decimal("1.5"), True), ("1234567893", Decimal("2.25"), False))
    async with _source_case() as case:
        await _install_compact_schema(case)
        connector, request, policy = await _registered_root_only_capture(case, monkeypatch, root_tuples)
        first = await runner.run_segmented_snowflake_candidate(
            case.sessions, connector, request, processing_policy=policy, direct_compact=True
        )
        assert first.status == "activated" and (first.accepted_family_count, first.rejection_count) == (2, 0)

        async with case.sessions() as session:
            family_id = await _snapshot_family_id_for_generation(session, first.generation_id)
            assert await _snapshot_row_count(session, family_id, "custom_import_block_family") == 2
            assert await _snapshot_row_count(session, family_id, "custom_import_block_fragment") == 0
            assert await _snapshot_row_count(session, family_id, "custom_import_generation_family") == 0
            assert await _snapshot_row_count(session, family_id, "custom_import_block_revision_run") == 0
            schema_name = snapshot_schema(family_id)
            extents = (
                await session.execute(
                    text(
                        f"SELECT fragment_count, raw_bytes, stored_bytes, membership_count "
                        f'FROM "{schema_name}".custom_import_block_family'
                    )
                )
            ).all()
            assert extents == [(0, 0, 0, 0), (0, 0, 0, 0)]
            inline_target = PinnedReadTarget(
                request.dataset_id,
                first.generation_id,
                request.definition_revision_id,
                request.schema_revision_id,
                "default",
            )
            service = CustomImportReadService(authorizer=_ExactReadAuthorizer(inline_target), cursor_secret=b"v" * 32)
            auth = ExtensionReadAuthorization("synthetic-reader")
            page = await service.search(session, authorization=auth, request=SearchRequest(inline_target))
            assert page.total == len(page.items) == 2
            hydrated_by_npi = {}
            for search_item in page.items:
                detail = await service.root_detail(
                    session, authorization=auth, target=inline_target, winner=search_item.winner
                )
                assert detail.children == ()
                vals_by_field = {field.field_id: field.value for field in detail.root_fields}
                hydrated_by_npi[vals_by_field["npi"]] = vals_by_field
            assert hydrated_by_npi == {
                "1003000126": {"npi": "1003000126", "score": Decimal("1.500000000000"), "enabled": True},
                "1234567893": {"npi": "1234567893", "score": Decimal("2.250000000000"), "enabled": False},
            }

        connector2, request2, policy2 = await _registered_root_only_capture(
            case, monkeypatch, root_tuples, idempotency_key="inline-v2-2"
        )
        second = await runner.run_segmented_snowflake_candidate(
            case.sessions, connector2, request2, processing_policy=policy2, direct_compact=True
        )
        assert second.status == "no_change"
        assert (second.accepted_family_count, second.rejection_count) == (2, 0)
        assert second.seal.effective_output_sha256 == first.seal.effective_output_sha256


@pytest.mark.asyncio
async def test_direct_v2_retention_and_rollback(monkeypatch):
    """Rejected roots carry forward cleanly from an inline compact v2 base and rollback works."""
    base_tuples = (("1003000126", Decimal("1.5"), True), ("1234567893", Decimal("2.25"), False))
    updated_tuples = (
        ("1003000126", Decimal("9.5"), True),
        ("1234567893", Decimal("3.0"), True),
        ("1234567893", Decimal("4.0"), False),
    )
    async with _source_case() as case:
        await _install_compact_schema(case)
        c1, req1, pol1 = await _registered_root_only_capture(case, monkeypatch, base_tuples, idempotency_key="ret-1")
        base_out = await runner.run_segmented_snowflake_candidate(
            case.sessions, c1, req1, processing_policy=pol1, direct_compact=True
        )
        assert base_out.status == "activated"

        c2, req2, pol2 = await _registered_root_only_capture(case, monkeypatch, updated_tuples, idempotency_key="ret-2")
        next_out = await runner.run_segmented_snowflake_candidate(
            case.sessions, c2, req2, processing_policy=pol2, direct_compact=True
        )
        assert next_out.status == "activated"
        assert (next_out.accepted_family_count, next_out.rejection_count) == (1, 1)
        assert next_out.seal.family_count == 2
        async with case.sessions() as session:
            next_fid = await _snapshot_family_id_for_generation(session, next_out.generation_id)
            schema_name = snapshot_schema(next_fid)
            retained_counts = (
                await session.execute(
                    text(
                        f'SELECT retained, count(*) FROM "{schema_name}".custom_import_block_family '
                        "GROUP BY retained ORDER BY retained"
                    )
                )
            ).all()
            assert retained_counts == [(False, 1), (True, 1)]
        async with case.sessions() as session, session.begin():
            receipt = await rollback_generation(
                session,
                dataset_id=req2.dataset_id,
                target_generation_id=base_out.generation_id,
                expected_generation_id=next_out.generation_id,
                expected_pointer_version=next_out.publication.committed_pointer_version,
            )
            assert receipt.to_generation_id == base_out.generation_id


@pytest.mark.asyncio
async def test_direct_v2_orphan_child_rejects(monkeypatch):
    """Orphan child rows transition direct compact v2 builds cleanly to candidate_rejected."""
    orphan_rows = [
        _shared_row("1003000126", key="k1", amount=Decimal("1.0")) + (None, None, None),
        (1, 2, "other_source", None, None, None, None, None, None, None, "1234567893", "orphan-k", Decimal("2.0")),
    ]
    async with _source_case() as case:
        await _install_compact_schema(case)
        policy = ProcessingPolicy(_capture_policy(), 17, BuildPolicy(16, 65_536, 2000, 120, 300))
        connector, bundle, adapter, _cursor, connection = _runtime(
            monkeypatch, orphan_rows, interleaved=True, partition_rows=2, processing_policy=policy
        )
        bundle = connector.prepare_request(
            _read_definition(bundle.definition, nullable_other=True),
            bindings=bundle.bindings,
            encoding=bundle.encoding,
            processing_policy=policy,
        )
        monkeypatch.setattr(adapter, "_connect", lambda _credentials, **_options: (connection, _cursor))
        async with case.sessions() as session, session.begin():
            registered = await register_snowflake_source_binding(
                session,
                dataset_key="synthetic_orphan_v2",
                definition=bundle.definition,
                binding=_binding(connector.build_statement(bundle), policy),
            )
        request = SnowflakeBundleCandidateRequest(
            dataset_id=registered.dataset_id,
            definition_revision_id=registered.definition_revision_id,
            schema_revision_id=registered.schema_revision_id,
            definition=bundle.definition,
            bundle_request=bundle,
            idempotency_key="orphan-v2-1",
            lease_token=b"orphan-v2-lease",
            source_binding_revision_id=registered.source_binding_revision_id,
            source_binding_sha256=registered.source_binding_sha256,
        )
        outcome = await runner.run_segmented_snowflake_candidate(
            case.sessions, connector, request, processing_policy=policy, direct_compact=True
        )
        assert outcome.status == "candidate_rejected" and (outcome.accepted_family_count, outcome.rejection_count) == (
            1,
            0,
        )
        async with case.sessions() as session:
            build = (await session.scalars(select(CustomImportBuildAttempt))).one()
            execution = await session.get(CustomImportExecution, outcome.execution_id)
            assert build.phase == "rejected" and build.candidate_error_count == 1
            assert execution.state == "failed"
            assert (await session.scalars(select(CustomImportGenerationSeal))).all() == []


def _sign_hosted_permit(
    request: SnowflakeBundleCandidateRequest,
    keyring: admission_auth.AdmissionKeyring,
    origin: str,
    window: tuple[dt.datetime, dt.datetime, dt.datetime, dt.datetime],
    purpose: tuple[str, str, bool],
):
    now_utc, issued_at, expires_at, launch_expires_at = window
    contract, path, is_source = purpose
    permit_by_field = {
        "audience": "custom-import-engine",
        "contract": contract,
        "dataset_id": request.dataset_id,
        "definition_revision_id": request.definition_revision_id,
        "schema_revision_id": request.schema_revision_id,
        "source_binding_revision_id": request.source_binding_revision_id,
        "source_binding_sha256": request.source_binding_sha256.hex(),
        "idempotency_key": request.idempotency_key,
        "issued_at": issued_at.isoformat().replace("+00:00", "Z"),
        "expires_at": expires_at.isoformat().replace("+00:00", "Z"),
        "issuer": "custom-import-execution-controller",
        "method": "POST",
        "origin": origin,
        "path": path,
    }
    canonical_bytes = json.dumps(permit_by_field, sort_keys=True, separators=(",", ":")).encode("ascii")
    return admission_auth.sign_permit(
        canonical_bytes,
        expected_origin=origin,
        trusted_now=now_utc,
        launch_expires_at=launch_expires_at,
        keyring=keyring,
        is_source=is_source,
    )


def _build_hosted_writer_transports(sessions, request: SnowflakeBundleCandidateRequest, *, on_call=None):
    """Wire real source-batch and admission-batch handlers behind an in-memory httpx transport."""
    origin = "https://engine.example.invalid"
    keyring = admission_auth.AdmissionKeyring("synthetic", (("synthetic", b"s" * 32),))
    now_utc = dt.datetime.now(dt.UTC).replace(microsecond=0)
    window = (
        now_utc,
        now_utc - dt.timedelta(seconds=5),
        now_utc + dt.timedelta(minutes=10),
        now_utc + dt.timedelta(minutes=15),
    )
    recorded_calls: list[tuple[str, int, dict[str, object]]] = []

    async def _dispatch_http(outbound: httpx.Request) -> httpx.Response:
        sanic_req = SimpleNamespace(
            headers=Header(list(outbound.headers.multi_items())),
            body=outbound.content,
            method=outbound.method,
            path=outbound.url.path,
            query_string="",
        )
        serve_now = dt.datetime.now(dt.UTC)
        handler = (
            source_api.serve_source_batch
            if outbound.url.path == source_auth.SOURCE_PATH
            else admission_api.serve_admission_batch
        )
        reply = await handler(sanic_req, sessions, keyring=keyring, expected_origin=origin, trusted_now=serve_now)
        body_bytes = bytes(reply.body)
        recorded_calls.append((outbound.url.path, reply.status, json.loads(body_bytes.decode("utf-8"))))
        if on_call is not None:
            on_call(outbound.url.path, reply.status)
        return httpx.Response(reply.status, headers=list(reply.headers.items()), content=body_bytes)

    mock_http = httpx.MockTransport(_dispatch_http)
    tls_context = writer_tls_context()
    source_transport = source_worker.SourceBatchTransport(
        expected_origin=origin,
        signed_permit=_sign_hosted_permit(
            request, keyring, origin, window, (source_auth.PERMIT_CONTRACT, source_auth.SOURCE_PATH, True)
        ),
        launch_expires_at=window[3],
        tls_context=tls_context,
        transport=mock_http,
    )
    admission_transport = admission_worker.AdmissionBatchTransport(
        expected_origin=origin,
        signed_permit=_sign_hosted_permit(
            request, keyring, origin, window, (admission_auth.PERMIT_CONTRACT, admission_auth.ADMISSION_PATH, False)
        ),
        launch_expires_at=window[3],
        tls_context=tls_context,
        transport=mock_http,
    )
    return admission_launch.WriterBatchTransports(admission_transport, source_transport), recorded_calls


async def _sealed_capture_handle(sessions, execution_id: int):
    async with sessions() as replay_session:
        exec_row = await replay_session.get(CustomImportExecution, execution_id)
        build_row = (
            await replay_session.scalars(
                select(CustomImportBuildAttempt).where(CustomImportBuildAttempt.execution_id == execution_id)
            )
        ).one()
    return SimpleNamespace(
        status="capture_sealed",
        execution_id=execution_id,
        fence=build_row.producing_fence,
        capture_bundle_id=exec_row.capture_bundle_id,
    )


@pytest.mark.asyncio
async def test_direct_v2_hosted_http_handoff(monkeypatch):
    """Hosted source-batch and admission-batch handoff follows persisted direct stage across flag flips and replays."""
    monkeypatch.setenv("HLTHPRT_CUSTOM_IMPORT_DIRECT_COMPACT", "true")
    async with _source_case() as case:
        await _install_compact_schema(case)
        connector, request, policy, _cursor, _connection, _key = await _registered_capture(case, monkeypatch)

        def _disable_flag_after_first_source(path: str, status: int) -> None:
            if path == source_auth.SOURCE_PATH and status == 200:
                monkeypatch.delenv("HLTHPRT_CUSTOM_IMPORT_DIRECT_COMPACT", raising=False)

        transports, recorded_calls = _build_hosted_writer_transports(
            case.sessions, request, on_call=_disable_flag_after_first_source
        )
        async with transports as active_transports:
            outcome = await runner.run_segmented_snowflake_candidate(
                case.sessions, connector, request, processing_policy=policy, writer_transports=active_transports
            )
            captured_handle = await _sealed_capture_handle(case.sessions, outcome.execution_id)
            replayed_outcome = await runner.build_segmented_capture(
                case.sessions, request, captured_handle, policy, writer_transports=active_transports
            )
        assert outcome.status == "activated" and replayed_outcome.status == "activated"
        assert replayed_outcome.generation_id == outcome.generation_id
        assert (outcome.accepted_family_count, outcome.rejection_count) == (2, 0)
        source_calls = [entry for entry in recorded_calls if entry[0] == source_auth.SOURCE_PATH]
        admission_calls = [entry for entry in recorded_calls if entry[0] == admission_auth.ADMISSION_PATH]
        assert source_calls and all(status == 200 for _path, status, _body in source_calls)
        assert admission_calls and all(status == 200 for _path, status, _body in admission_calls)
        assert admission_calls[-1][2]["phase"] == "verified"

        async with case.sessions() as session:
            family_id = await _snapshot_family_id_for_generation(session, outcome.generation_id)
            await _assert_v2_storage_invariants(session, family_id, expected_families=2)
            read_target = PinnedReadTarget(
                request.dataset_id,
                outcome.generation_id,
                request.definition_revision_id,
                request.schema_revision_id,
                "default",
            )
            await _assert_reads(session, read_target, _key)


def _derived_parquet_definition() -> CustomImportDefinition:
    doc_by_key = derived_document()
    doc_by_key["refresh_mode"] = "snapshot"
    for stream in doc_by_key["streams"]:
        stream["format"] = "parquet"
        stream["compression"] = "none"
        if stream["kind"] == "child":
            stream["duplicate_policy"] = "collapse_identical"
    doc_by_key["aliases"] = {
        "providers": {field["id"].upper(): field["id"] for field in doc_by_key["schema"]["root"]["fields"]},
        "rates": {field["id"].upper(): field["id"] for field in doc_by_key["schema"]["children"][0]["fields"]},
    }
    return CustomImportDefinition.from_mapping(doc_by_key)


_DERIVED_SAMPLE_ENTRIES = (
    (
        1,
        "providers",
        {
            "npi": "1234567893",
            "display_name": "Clinic A",
            "period": 2025,
            "segment": "segment_a",
            "score": Decimal("4.0"),
            "weight": 10,
        },
    ),
    (
        1,
        "providers",
        {
            "npi": "1234567893",
            "display_name": "Clinic A",
            "period": 2025,
            "segment": "segment_b",
            "score": Decimal("2.0"),
            "weight": 30,
        },
    ),
    (
        1,
        "providers",
        {
            "npi": "1003000126",
            "display_name": "Clinic B",
            "period": 2025,
            "segment": "segment_a",
            "score": Decimal("5.0"),
            "weight": 4,
        },
    ),
    (
        2,
        "rates",
        {
            "rate_npi": "1234567893",
            "rate_period": 2025,
            "rate_segment": "segment_a",
            "service_code": "svc-1",
            "amount": Decimal("100.0"),
            "quality": Decimal("4.0"),
            "quantity": 5,
        },
    ),
    # Identical duplicate child row -> must collapse identically under collapse_identical.
    (
        2,
        "rates",
        {
            "rate_npi": "1234567893",
            "rate_period": 2025,
            "rate_segment": "segment_a",
            "service_code": "svc-1",
            "amount": Decimal("100.0"),
            "quality": Decimal("4.0"),
            "quantity": 5,
        },
    ),
    (
        2,
        "rates",
        {
            "rate_npi": "1234567893",
            "rate_period": 2025,
            "rate_segment": "segment_b",
            "service_code": "svc-1",
            "amount": Decimal("200.0"),
            "quality": Decimal("2.0"),
            "quantity": 15,
        },
    ),
    (
        2,
        "rates",
        {
            "rate_npi": "1003000126",
            "rate_period": 2025,
            "rate_segment": "segment_a",
            "service_code": "svc-1",
            "amount": None,
            "quality": Decimal("5.0"),
            "quantity": 2,
        },
    ),
)


def _derived_snowflake_rows(derived_def: CustomImportDefinition) -> tuple[tuple[object, ...], ...]:
    """Build union Snowflake rows ordered by field_slot for derived projection parity."""
    ordered_ids = tuple(field.field_id for field in sorted(derived_def.fields, key=lambda field: field.field_slot))
    null_tail = (None,) * len(ordered_ids)
    return (
        (0, 1, "providers", _SNAPSHOT, *null_tail),
        (0, 2, "rates", _SNAPSHOT, *null_tail),
        *(
            (1, ordinal, stream_id, None, *(values_by_field.get(field_id) for field_id in ordered_ids))
            for ordinal, stream_id, values_by_field in _DERIVED_SAMPLE_ENTRIES
        ),
    )


def _derived_approved_relations(derived_def: CustomImportDefinition) -> tuple[SnowflakeApprovedRelation, ...]:
    snapshot_rel = SnowflakeRelation("synthetic", "public", "derived_snapshots")
    root_rel = SnowflakeRelation("synthetic", "public", "derived_roots")
    child_rel = SnowflakeRelation("synthetic", "public", "derived_rates")
    rate_fields = runner_codec.fields_by_collection(derived_def)["rates"]
    return (
        SnowflakeApprovedRelation(snapshot_rel, (SnowflakeDeclaredColumn("snapshot_id", "snapshot_id"),)),
        SnowflakeApprovedRelation(
            root_rel,
            tuple(SnowflakeDeclaredColumn(field.field_id, field.field_id) for field in derived_def.root_fields),
        ),
        SnowflakeApprovedRelation(
            child_rel, tuple(SnowflakeDeclaredColumn(field.field_id, field.field_id) for field in rate_fields)
        ),
    )


def _derived_column_meta(field) -> SimpleNamespace:
    if field.value_type == "integer":
        return SimpleNamespace(name=field.field_id, type_name="FIXED", is_nullable=True, precision=10, scale=0)
    if field.value_type == "decimal":
        return SimpleNamespace(name=field.field_id, type_name="FIXED", is_nullable=True, precision=30, scale=12)
    return SimpleNamespace(name=field.field_id, type_name="TEXT", is_nullable=True)


def _derived_mock_cursor(derived_def: CustomImportDefinition) -> _PythonCursor:
    ordered_fields = tuple(sorted(derived_def.fields, key=lambda field: field.field_slot))
    cursor = _PythonCursor(_derived_snowflake_rows(derived_def))
    cursor.description = (
        SimpleNamespace(name="__ci_bundle_row_kind", type_name="FIXED", is_nullable=False, precision=1, scale=0),
        SimpleNamespace(name="__ci_stream_ordinal", type_name="FIXED", is_nullable=False, precision=2, scale=0),
        SimpleNamespace(name="__ci_stream_id", type_name="TEXT", is_nullable=False),
        SimpleNamespace(name="__ci_source_snapshot_token", type_name="TEXT", is_nullable=True),
        *(_derived_column_meta(field) for field in ordered_fields),
    )
    cursor.fetchone = Mock(wraps=cursor.fetchone)
    return cursor


async def _registered_derived_capture(case, monkeypatch, *, idempotency_key: str):
    """Register a derived-projection Snowflake bundle capture fixture in PostgreSQL."""
    policy = ProcessingPolicy(_capture_policy(), 17, BuildPolicy(16, 65_536, 10_000, 120, 300))
    derived_def = _derived_parquet_definition()
    approved = _derived_approved_relations(derived_def)
    snapshot_rel = approved[0].relation
    root_rel = approved[1].relation
    child_rel = approved[2].relation
    rate_fields = runner_codec.fields_by_collection(derived_def)["rates"]
    adapter = snowflake_python.SnowflakePythonConnectorAdapter(role="reader_role", warehouse="import_wh")
    connector = SnowflakeBundleAcquisitionConnector(
        approved_relations=approved,
        credential_provider=_Credentials(),
        adapter=adapter,
        capture_limits=CaptureLimits(),
    )
    bindings = (
        SnowflakeBundleBinding(
            "providers",
            root_rel,
            snapshot_rel,
            tuple(field.field_id for field in derived_def.root_fields),
            "snapshot_id",
        ),
        SnowflakeBundleBinding(
            "rates", child_rel, snapshot_rel, tuple(field.field_id for field in rate_fields), "snapshot_id"
        ),
    )
    bundle = connector.prepare_request(
        derived_def, bindings=bindings, encoding=SnowflakeBundleEncoding(partition_rows=4), processing_policy=policy
    )
    cursor = _derived_mock_cursor(derived_def)
    connection = _PythonConnection(cursor)
    monkeypatch.setattr(adapter, "_connect", lambda _credentials, **_options: (connection, cursor))
    async with case.sessions() as session, session.begin():
        registered = await register_snowflake_source_binding(
            session,
            dataset_key="synthetic_derived_v2",
            definition=bundle.definition,
            binding=_binding(connector.build_statement(bundle), policy),
        )
    request = SnowflakeBundleCandidateRequest(
        dataset_id=registered.dataset_id,
        definition_revision_id=registered.definition_revision_id,
        schema_revision_id=registered.schema_revision_id,
        definition=bundle.definition,
        bundle_request=bundle,
        idempotency_key=idempotency_key,
        lease_token=f"lease-{idempotency_key}".encode(),
        source_binding_revision_id=registered.source_binding_revision_id,
        source_binding_sha256=registered.source_binding_sha256,
    )
    return connector, request, policy


async def _grouped_hydration_summary(session, pinned_target: PinnedReadTarget) -> dict[str, tuple]:
    service = CustomImportReadService(authorizer=_ExactReadAuthorizer(pinned_target), cursor_secret=b"d" * 32)
    auth = ExtensionReadAuthorization("synthetic-reader")
    grouped_query = grouped_child_fixture.query(
        filters=(ReadFilter("combined_score", "gte", Decimal("2.0")),),
        order_terms=(ReadOrderTerm("combined_score", "desc", "last"),),
        family_entitlement="full_family",
    )
    prepared = await service.prepare_npi_entity_relation(
        session, authorization=auth, target=pinned_target, query=grouped_query
    )
    hydrated_by_npi = await service.hydrate_npi_page(
        session,
        authorization=auth,
        pinned_target=pinned_target,
        query=grouped_query,
        prepared=prepared,
        entity_values=("1234567893", "1003000126"),
    )
    return {
        npi: (
            entry.selection_value,
            entry.missing_group_values,
            tuple(
                (
                    group_value,
                    tuple((field.field_id, field.value) for field in detail.root_fields),
                    tuple(
                        tuple((field.field_id, field.value) for field in child.fields)
                        for child in getattr(detail, "children", ())
                    ),
                )
                for group_value, detail in entry.families
            ),
        )
        for npi, entry in hydrated_by_npi.items()
    }


@pytest.mark.asyncio
async def test_direct_v2_derived_projections_parity(monkeypatch):
    """Direct compact v2 matches legacy on derived query_root/query_child projections and reads."""
    async with _source_case() as legacy_case:
        await _install_compact_schema(legacy_case)
        c_leg, req_leg, pol_leg = await _registered_derived_capture(legacy_case, monkeypatch, idempotency_key="der-leg")
        leg_out = await runner.run_segmented_snowflake_candidate(
            legacy_case.sessions, c_leg, req_leg, processing_policy=pol_leg
        )
        assert leg_out.status == "activated"
        async with legacy_case.sessions() as session:
            leg_family_id = await _snapshot_family_id_for_generation(session, leg_out.generation_id)
            leg_proj = await session.get(CustomImportQueryProjection, leg_family_id)
            assert leg_proj is not None and leg_proj.phase == "complete"
            leg_target = PinnedReadTarget(
                req_leg.dataset_id,
                leg_out.generation_id,
                req_leg.definition_revision_id,
                req_leg.schema_revision_id,
                "families_by_period",
            )
            leg_reads_by_npi = await _grouped_hydration_summary(session, leg_target)

    async with _source_case() as direct_case:
        await _install_compact_schema(direct_case)
        c_dir, req_dir, pol_dir = await _registered_derived_capture(direct_case, monkeypatch, idempotency_key="der-dir")
        dir_out = await runner.run_segmented_snowflake_candidate(
            direct_case.sessions, c_dir, req_dir, processing_policy=pol_dir, direct_compact=True
        )
        assert dir_out.status == "activated"
        assert dir_out.seal.effective_output_sha256 == leg_out.seal.effective_output_sha256
        assert _seal_counts(dir_out.seal) == _seal_counts(leg_out.seal)
        assert dir_out.seal.family_child_count == 3  # 4 staged child rows with 1 identical duplicate collapsed
        async with direct_case.sessions() as session:
            family_id = await _snapshot_family_id_for_generation(session, dir_out.generation_id)
            dir_proj = await session.get(CustomImportQueryProjection, family_id)
            assert dir_proj is not None and dir_proj.phase == "complete"
            assert (
                dir_proj.projection_sha256,
                dir_proj.capability_sha256,
                dir_proj.root_count,
                dir_proj.child_count,
                dir_proj.digest_chain_sha256,
            ) == (
                leg_proj.projection_sha256,
                leg_proj.capability_sha256,
                leg_proj.root_count,
                leg_proj.child_count,
                leg_proj.digest_chain_sha256,
            )
            await _assert_v2_storage_invariants(session, family_id, expected_families=3)
            dir_target = PinnedReadTarget(
                req_dir.dataset_id,
                dir_out.generation_id,
                req_dir.definition_revision_id,
                req_dir.schema_revision_id,
                "families_by_period",
            )
            dir_reads_by_npi = await _grouped_hydration_summary(session, dir_target)
            assert dir_reads_by_npi == leg_reads_by_npi
