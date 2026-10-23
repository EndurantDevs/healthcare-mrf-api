# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Mocked preparation scope and verifier wiring; no native fencing proof."""

from __future__ import annotations

import datetime as dt
from contextlib import asynccontextmanager
from dataclasses import FrozenInstanceError, asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy.dialects.postgresql import dialect

from process.custom_import import admission_authorization, runner_registry, source_batch
from process.custom_import import prepared_source_context as prepared
from process.custom_import import snowflake_source_binding as legacy
from process.custom_import.execution import lease_token_sha256
from process.custom_import.runner_types import CandidateRunnerError, LeaseAuthorityLost
from process.custom_import.source_binding_store import LoadedObjectSourceBinding
from tests.test_custom_import_object_source_binding import _definition
from tests.test_custom_import_snowflake_source_binding import _binding as _legacy_binding
from tests.test_custom_import_snowflake_source_binding import _definition as _legacy_definition
from tests.test_custom_import_snowflake_source_binding import _loaded_rows
from tests.test_custom_import_source_batch import _EXPIRY, _permit
from tests.test_custom_import_source_binding_store import _object_binding


def _retained_rows():
    definition = _definition()
    binding = _object_binding(definition)
    loaded = LoadedObjectSourceBinding(1, 2, 3, 5, bytes.fromhex(binding.digest), definition, binding)
    execution = SimpleNamespace(
        execution_id=4,
        dataset_id=1,
        definition_revision_id=2,
        schema_revision_id=3,
        source_binding_revision_id=5,
        capture_bundle_id=9,
        idempotency_key="synthetic-request",
        request_identity_sha256=b"r" * 32,
        started_at=_EXPIRY - dt.timedelta(minutes=1),
        state="running",
    )
    bundle = SimpleNamespace(
        capture_bundle_id=9,
        dataset_id=1,
        definition_revision_id=2,
        schema_revision_id=3,
        capture_state="sealed",
        payload_contract=None,
        manifest_sha256=b"m" * 32,
    )
    return SimpleNamespace(
        execution=execution,
        bundle=bundle,
        loaded=loaded,
        permit=_permit(source_binding_sha256=binding.digest),
        token=b"synthetic-preparation-owner",
    )


def _mock_sessions(retained):
    events = []
    rows_by_model = {
        prepared.CustomImportExecution: retained.execution,
        prepared.CustomImportCaptureBundle: retained.bundle,
    }

    async def get(model, identifier):
        events.append("execution" if model is prepared.CustomImportExecution else "bundle")
        return rows_by_model[model] if identifier == (4 if model is prepared.CustomImportExecution else 9) else None

    async def execute(*_args, **_kwargs):
        events.append("permit")
        return SimpleNamespace(scalar_one_or_none=lambda: 4)

    async def is_owner(*_args, **_kwargs):
        events.append("prepared-owner")
        return True

    @asynccontextmanager
    async def transaction():
        events.append("begin")
        try:
            yield
        except BaseException:
            events.append("rollback")
            raise
        else:
            events.append("commit")

    connection = SimpleNamespace(dialect=dialect(), sync_connection=SimpleNamespace(get_execution_options=lambda: {}))
    session = SimpleNamespace(
        get=AsyncMock(side_effect=get),
        execute=AsyncMock(side_effect=execute),
        scalar=AsyncMock(side_effect=is_owner),
        connection=AsyncMock(return_value=connection),
        begin=transaction,
        in_transaction=lambda: True,
        new=(),
        dirty=(),
        deleted=(),
        info={},
    )

    @asynccontextmanager
    async def session_scope():
        events.append("open")
        try:
            yield session
        finally:
            events.append("close")

    return SimpleNamespace(
        session=session,
        sessions=Mock(side_effect=session_scope),
        connection=connection,
        events=events,
        rows_by_model=rows_by_model,
    )


@pytest.fixture
def case(monkeypatch):
    retained = _retained_rows()
    mocked = _mock_sessions(retained)
    load_binding = AsyncMock(return_value=retained.loaded)
    stream_slots = tuple(
        (stream.stream_id, index) for index, stream in enumerate(retained.loaded.definition.source_streams, 1)
    )
    streams = AsyncMock(return_value=stream_slots)
    monkeypatch.setattr(prepared, "load_source_binding", load_binding)
    monkeypatch.setattr(prepared, "_validated_streams", streams)
    monkeypatch.setattr(prepared.staging, "_set_timeout", AsyncMock())
    monkeypatch.setattr(prepared.staging, "_prepare_statement", AsyncMock())
    monkeypatch.setattr(source_batch, "_require_owner", AsyncMock())
    return SimpleNamespace(
        **vars(retained),
        **vars(mocked),
        load_binding=load_binding,
        stream_slots=stream_slots,
        streams=streams,
    )


async def _load(case, **changes):
    arguments = dict(execution_id=4, fence=7, token=case.token, source_permit=case.permit) | changes
    return await prepared.load_prepared_source_context(case.sessions, **arguments)


async def test_loader_uses_only_retained_pins_stream_slots_and_original_start_deadline(case):
    context = await _load(case)
    assert (
        context.dataset_id,
        context.definition_revision_id,
        context.schema_revision_id,
        context.execution_id,
        context.capture_bundle_id,
        context.source_binding_revision_id,
    ) == (1, 2, 3, 4, 9, 5)
    assert context.source_binding_sha256 == bytes.fromhex(case.loaded.binding.digest)
    assert context.request_identity_sha256 == b"r" * 32 and context.raw_manifest_sha256 == b"m" * 32
    assert context.definition is case.loaded.definition and context.policy is case.loaded.binding.processing_policy
    assert context.stream_slots == case.stream_slots
    assert context.build_deadline_at == case.execution.started_at + dt.timedelta(
        seconds=context.policy.build.build_deadline_seconds
    )
    assert context.authorization_expires_at == case.permit.expires_at
    assert context.lease_seconds == context.policy.build.lease_seconds
    assert context.statement_timeout_ms == context.policy.build.statement_timeout_ms
    assert context.continuation is None and context.operation_deadline is None
    assert not hasattr(context, "expected_base_generation_id")
    case.load_binding.assert_awaited_once_with(case.session, definition_revision_id=2, source_binding_revision_id=5)
    identity = case.streams.await_args.args[1]
    assert (identity.dataset_id, identity.definition_revision_id, identity.schema_revision_id) == (1, 2, 3)
    assert case.events[-2:] == ["commit", "close"]
    with pytest.raises(FrozenInstanceError):
        context.fence = 8
    assert "synthetic-preparation-owner" not in repr(context)


async def test_loading_a_new_fence_never_resets_original_deadline(case):
    first = await _load(case)
    resumed = await _load(case, fence=8, token=b"synthetic-next-owner")
    assert first.build_deadline_at == resumed.build_deadline_at
    assert (resumed.fence, resumed.lease_token) == (8, b"synthetic-next-owner")


@pytest.mark.parametrize(
    "field,value",
    [
        ("contract", "other/v1"),
        ("path", "/control/v1/custom-import/admission-batch"),
        ("method", "GET"),
        ("issuer", "other"),
        ("audience", "other"),
    ],
)
async def test_loader_rejects_other_purposes_before_opening_a_session(case, field, value):
    with pytest.raises(CandidateRunnerError, match="^prepared_source_scope_invalid$"):
        await _load(case, source_permit=replace(case.permit, **{field: value}))
    case.sessions.assert_not_called()


async def test_structurally_equal_admission_permit_cannot_authorize_preparation(case):
    permit = admission_authorization.AdmissionPermit(**asdict(case.permit))
    with pytest.raises(CandidateRunnerError):
        await _load(case, source_permit=permit)
    case.sessions.assert_not_called()


@pytest.mark.parametrize(
    "field",
    ["dataset_id", "definition_revision_id", "schema_revision_id", "source_binding_revision_id", "idempotency_key"],
)
async def test_loader_rejects_cross_scope_before_binding_or_stream_reads(case, field):
    value = "other-request" if field == "idempotency_key" else 99
    with pytest.raises(CandidateRunnerError):
        await _load(case, source_permit=replace(case.permit, **{field: value}))
    case.load_binding.assert_not_awaited()
    case.streams.assert_not_awaited()
    assert case.events[-2:] == ["rollback", "close"]


@pytest.mark.parametrize(
    "field,value", [("execution_id", True), ("execution_id", 0), ("fence", True), ("fence", 0), ("token", b"")]
)
async def test_loader_rejects_invalid_native_scope_inputs_before_session(case, field, value):
    with pytest.raises(ValueError):
        await _load(case, **{field: value})
    case.sessions.assert_not_called()


@pytest.mark.parametrize(
    "expires_at", [None, _EXPIRY.replace(tzinfo=None), _EXPIRY.astimezone(dt.timezone(dt.timedelta(hours=1)))]
)
async def test_loader_requires_exact_utc_authorization_expiry(case, expires_at):
    with pytest.raises(ValueError):
        await _load(case, source_permit=replace(case.permit, expires_at=expires_at))
    case.sessions.assert_not_called()


@pytest.mark.parametrize(
    "field,value",
    [
        ("source_binding_revision_id", None),
        ("capture_bundle_id", None),
        ("request_identity_sha256", None),
        ("request_identity_sha256", b"short"),
        ("started_at", None),
        ("started_at", _EXPIRY.replace(tzinfo=None)),
    ],
)
async def test_loader_refuses_missing_or_unfrozen_execution_pins(case, field, value):
    setattr(case.execution, field, value)
    with pytest.raises(CandidateRunnerError):
        await _load(case)
    case.streams.assert_not_awaited()


@pytest.mark.parametrize(
    "field,value",
    [
        ("capture_bundle_id", 99),
        ("dataset_id", 99),
        ("definition_revision_id", 99),
        ("schema_revision_id", 99),
        ("capture_state", "pending"),
        ("payload_contract", "custom-import/parquet-parts/v2"),
        ("manifest_sha256", None),
        ("manifest_sha256", b"short"),
    ],
)
async def test_loader_refuses_cross_owner_unsealed_mixed_or_invalid_original_headers(case, field, value):
    setattr(case.bundle, field, value)
    with pytest.raises(CandidateRunnerError):
        await _load(case)
    case.streams.assert_not_awaited()


@pytest.mark.parametrize("model", [prepared.CustomImportExecution, prepared.CustomImportCaptureBundle])
async def test_loader_refuses_absent_retained_rows(case, model):
    case.rows_by_model[model] = None
    with pytest.raises(CandidateRunnerError):
        await _load(case)


@pytest.mark.parametrize(
    "field",
    [
        "dataset_id",
        "definition_revision_id",
        "schema_revision_id",
        "source_binding_revision_id",
        "source_binding_sha256",
    ],
)
async def test_loader_refuses_changed_loaded_owner_or_approved_binding_digest(case, field):
    value = b"b" * 32 if field == "source_binding_sha256" else 99
    case.load_binding.return_value = replace(case.loaded, **{field: value})
    with pytest.raises(CandidateRunnerError):
        await _load(case)
    case.streams.assert_not_awaited()


async def test_legacy_binding_type_is_refused_even_with_matching_owner_fields(case):
    definition = _legacy_definition()
    binding = _legacy_binding(definition)
    loaded = legacy._loaded_snowflake_source_binding(*_loaded_rows(definition, binding))
    case.load_binding.return_value = replace(
        loaded, dataset_id=1, definition_revision_id=2, schema_revision_id=3, source_binding_revision_id=5
    )
    case.permit = replace(case.permit, source_binding_sha256=binding.digest)
    with pytest.raises(CandidateRunnerError):
        await _load(case)
    case.streams.assert_not_awaited()


async def test_retained_digest_buffers_are_frozen_into_context(case):
    request_buffer, manifest_buffer = bytearray(b"r" * 32), bytearray(b"m" * 32)
    case.execution.request_identity_sha256 = request_buffer
    case.bundle.manifest_sha256 = memoryview(manifest_buffer)
    context = await _load(case)
    request_buffer[0] = manifest_buffer[0] = 0
    assert type(context.request_identity_sha256) is type(context.raw_manifest_sha256) is bytes
    assert context.request_identity_sha256 == b"r" * 32 and context.raw_manifest_sha256 == b"m" * 32


@pytest.mark.parametrize("view", [False, True])
async def test_loader_never_retains_a_mutable_lease_token_buffer(case, view):
    token_buffer = bytearray(case.token)
    token = memoryview(token_buffer) if view else token_buffer
    with pytest.raises(CandidateRunnerError):
        await _load(case, token=token)
    case.sessions.assert_not_called()


async def test_locked_recheck_uses_existing_permit_query_then_prepared_owner_before_metadata(case):
    context = await _load(case)
    case.events.clear()
    await prepared.require_prepared_source_context(case.session, context)
    assert case.events == ["permit", "prepared-owner", "execution", "bundle"]
    source_batch._require_owner.assert_awaited_once_with(case.session)
    statement = case.session.execute.await_args.args[0].compile(dialect=dialect())
    assert case.permit.idempotency_key in statement.params.values()
    assert bytes.fromhex(case.permit.source_binding_sha256) in statement.params.values()
    assert "custom_import_source_binding_revision" in str(statement)


@pytest.mark.parametrize(
    "field,value",
    [
        ("dataset_id", 99),
        ("definition_revision_id", 99),
        ("schema_revision_id", 99),
        ("contract", "other/v1"),
        ("path", "/other"),
    ],
)
async def test_locked_permit_mismatch_prevents_prepared_authority_and_metadata(case, field, value):
    context = await _load(case)
    context = replace(context, source_permit=replace(case.permit, **{field: value}))
    case.session.get.reset_mock()
    with pytest.raises(RuntimeError, match="authority_mismatch"):
        await prepared.require_prepared_source_context(case.session, context)
    case.session.scalar.assert_not_awaited()
    case.session.get.assert_not_awaited()


@pytest.mark.parametrize(
    "field,value",
    [
        ("capture_bundle_id", 10),
        ("request_identity_sha256", b"x" * 32),
        ("started_at", _EXPIRY),
        ("source_binding_revision_id", 99),
        ("dataset_id", 99),
    ],
)
async def test_locked_recheck_rejects_changed_execution_header_pins(case, field, value):
    context = await _load(case)
    setattr(case.execution, field, value)
    with pytest.raises(CandidateRunnerError):
        await prepared.require_prepared_source_context(case.session, context)


@pytest.mark.parametrize(
    "field,value", [("manifest_sha256", b"x" * 32), ("payload_contract", "mixed/v1"), ("capture_state", "pending")]
)
async def test_locked_recheck_rejects_changed_original_header(case, field, value):
    context = await _load(case)
    setattr(case.bundle, field, value)
    with pytest.raises(CandidateRunnerError):
        await prepared.require_prepared_source_context(case.session, context)


@pytest.mark.parametrize(
    "field",
    [
        "source_binding_sha256",
        "definition",
        "policy",
        "request_identity_sha256",
        "raw_manifest_sha256",
        "build_deadline_at",
    ],
)
async def test_locked_recheck_rejects_changed_frozen_context(case, field):
    context = await _load(case)
    changed_by_field = {
        "source_binding_sha256": b"x" * 32,
        "definition": None,
        "policy": replace(context.policy, acquisition_timeout_seconds=1),
        "request_identity_sha256": b"x" * 32,
        "raw_manifest_sha256": b"x" * 32,
        "build_deadline_at": context.build_deadline_at + dt.timedelta(seconds=1),
    }
    with pytest.raises(CandidateRunnerError):
        await prepared.require_prepared_source_context(
            case.session, replace(context, **{field: changed_by_field[field]})
        )


async def test_locked_recheck_rejects_new_retained_policy(case):
    context = await _load(case)
    binding = replace(case.loaded.binding, processing_policy=replace(context.policy, acquisition_timeout_seconds=1))
    case.load_binding.return_value = replace(case.loaded, binding=binding)
    with pytest.raises(CandidateRunnerError):
        await prepared.require_prepared_source_context(case.session, context)


async def test_prepared_authority_uses_both_mapped_tables_and_complete_write_acl_roots(case):
    case.connection.sync_connection.get_execution_options = lambda: {
        "schema_translate_map": {"mrf": 'synthetic"prepared'}
    }
    await prepared.require_prepared_source_authority(case.session)
    statement, parameters = case.session.scalar.await_args.args
    assert parameters == {
        "relation_count": 2,
        "relations": [
            '"synthetic""prepared".custom_import_prepared_source',
            '"synthetic""prepared".custom_import_prepared_source_part',
        ],
    }
    sql = str(statement)
    assert "pg_catalog.pg_has_role(relowner, 'USAGE')" in sql
    assert "pg_catalog.aclexplode(attribute.attacl)" in sql and "grant_entry.grantee <> grant_entry.relowner" in sql
    assert all(
        privilege in sql
        for privilege in ("INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER", "MAINTAIN")
    )
    assert all(forbidden not in sql for forbidden in ("GRANT ", "REVOKE ", "SET ROLE", ".payload"))


@pytest.mark.parametrize("allowed", [False, None, 0, 1, "true"])
async def test_prepared_owner_requires_exact_catalog_true(case, allowed):
    case.session.scalar.side_effect = None
    case.session.scalar.return_value = allowed
    with pytest.raises(CandidateRunnerError):
        await prepared.require_prepared_source_authority(case.session)


@pytest.mark.parametrize("schema", [None, ""])
async def test_unresolved_prepared_schema_fails_before_catalog_query(case, schema):
    case.connection.sync_connection.get_execution_options = lambda: {"schema_translate_map": {"mrf": schema}}
    with pytest.raises(CandidateRunnerError):
        await prepared.require_prepared_source_authority(case.session)
    case.session.scalar.assert_not_awaited()


def _page_guards(case, monkeypatch, context, *, lost_fence=False, expires_at_commit=False):
    now = case.permit.expires_at - dt.timedelta(seconds=1)
    lease = SimpleNamespace(
        fence=8 if lost_fence else context.fence,
        token_sha256=lease_token_sha256(case.token),
        expires_at=now + dt.timedelta(minutes=1),
    )
    monkeypatch.setattr(prepared.staging, "_initial_page_window", AsyncMock(return_value=lease.expires_at))
    monkeypatch.setattr(prepared.staging, "lock_dataset", AsyncMock())
    monkeypatch.setattr(prepared.staging, "lock_execution", AsyncMock(return_value=case.execution))
    monkeypatch.setattr(prepared.staging, "lock_lease", AsyncMock(return_value=lease))
    monkeypatch.setattr(prepared.staging, "_renew_page_window", AsyncMock())
    monkeypatch.setattr(prepared.staging, "_flush_page", AsyncMock())
    monkeypatch.setattr(
        runner_registry,
        "database_now",
        AsyncMock(side_effect=[now, case.permit.expires_at if expires_at_commit else now]),
    )
    original = prepared.staging.verify_live_attempt

    async def verify(*args, **kwargs):
        case.events.append("live-check")
        return await original(*args, **kwargs)

    monkeypatch.setattr(prepared.staging, "verify_live_attempt", AsyncMock(side_effect=verify))


async def test_shared_page_verifier_precedes_metadata_and_rechecks_before_commit(case, monkeypatch):
    context = await _load(case)
    _page_guards(case, monkeypatch, context)
    case.events.clear()
    async with prepared.staging._page_session(case.sessions, context) as (session, build):
        assert build is None
        await prepared.require_prepared_source_context(session, context)
    assert case.events == [
        "open",
        "begin",
        "live-check",
        "permit",
        "prepared-owner",
        "execution",
        "bundle",
        "live-check",
        "commit",
        "close",
    ]
    assert prepared.staging.verify_live_attempt.await_count == 2


async def test_stale_fence_in_shared_verifier_prevents_metadata_loading(case, monkeypatch):
    context = await _load(case)
    _page_guards(case, monkeypatch, context, lost_fence=True)
    case.events.clear()
    case.session.get.reset_mock()
    with pytest.raises(LeaseAuthorityLost):
        async with prepared.staging._page_session(case.sessions, context) as (session, _build):
            await prepared.require_prepared_source_context(session, context)
    assert case.events == ["open", "begin", "live-check", "rollback", "close"]
    case.session.get.assert_not_awaited()


async def test_shared_page_rolls_back_if_source_authorization_expires_before_commit(case, monkeypatch):
    context = await _load(case)
    _page_guards(case, monkeypatch, context, expires_at_commit=True)
    case.events.clear()
    with pytest.raises(LeaseAuthorityLost, match="authorization expired"):
        async with prepared.staging._page_session(case.sessions, context) as (session, _build):
            await prepared.require_prepared_source_context(session, context)
    assert case.events[-2:] == ["rollback", "close"]
