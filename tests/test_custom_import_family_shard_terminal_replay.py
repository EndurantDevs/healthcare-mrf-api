# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Actual terminal/continuation/receipt logic over finite synthetic SQL transport.

No PostgreSQL registration, native row locks, block seal or API acceptance is
claimed. Synthetic rows use actual authorization and immutable receipt producers.
"""

import datetime as dt
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.dialects import postgresql
from sqlalchemy.sql.elements import TextClause
from sqlalchemy.sql.util import find_tables

from db.models import custom_import as models
from db.models.custom_import_continuation import CustomImportBuildContinuation
from process.custom_import import build_output as output
from process.custom_import import continuation_runtime as continuation
from process.custom_import import publication
from process.custom_import.build_continuation import _authorization_row
from process.custom_import.execution import lease_token_sha256
from process.custom_import.runner_types import CandidateRunnerError, LeaseAuthorityLost
from tests.test_custom_import_build_continuation import _test_authorization
from tests.test_custom_import_build_graph import _request
from tests.test_custom_import_build_output import _materialization_case

_NOW = dt.datetime(2030, 1, 2, tzinfo=dt.UTC)
_FINISHED = _NOW - dt.timedelta(seconds=10)


@pytest.fixture(autouse=True)
def _native_storage_boundary(monkeypatch):
    """Native catalog custody is outside this finite continuation SQL transport."""
    from process.custom_import import read_identity

    monkeypatch.setattr(read_identity, "verify_generation_storage", AsyncMock())
    monkeypatch.setattr(publication, "_finality_materialization_models", AsyncMock(return_value={}))


class _Result:
    def __init__(self, rows):
        self.rows = rows

    def scalar_one_or_none(self):
        assert len(self.rows) <= 1
        return self.rows[0] if self.rows else None

    def scalar_one(self):
        assert len(self.rows) == 1
        return self.rows[0]

    def scalars(self):
        return self

    def one_or_none(self):
        return self.scalar_one_or_none()


class _Transport:
    """Read-only SQL values; actual caller guards and receipt validators run."""

    def __init__(self, case):
        self.case, self.statements = case, []
        self.info, self.new, self.dirty, self.deleted = {}, set(), set(), set()
        self.transaction = SimpleNamespace(is_active=False)
        self.connection_info = {}

    @asynccontextmanager
    async def factory(self):
        yield self

    @asynccontextmanager
    async def begin(self):
        self.transaction.is_active = True
        try:
            yield self
        finally:
            self.transaction.is_active = False

    def in_transaction(self):
        return self.transaction.is_active

    def get_transaction(self):
        return self.transaction

    async def connection(self):
        return SimpleNamespace(
            get_transaction=lambda: self.transaction,
            info=self.connection_info,
            sync_connection=SimpleNamespace(
                get_execution_options=lambda: {"schema_translate_map": {"mrf": "synthetic"}}
            ),
            dialect=postgresql.dialect(),
        )

    async def get(self, model, identity, **options):
        if model is models.CustomImportBuildAttempt:
            return self.case.build if identity == self.case.build.build_id else None
        if model is models.CustomImportGenerationSeal:
            return next((row for row in self.case.seals if row.generation_id == identity), None)
        raise AssertionError(model)

    async def scalar(self, statement):
        return (await self.execute(statement)).scalar_one()

    async def execute(self, statement, parameters=None):
        sql = str(statement.compile(dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
        self.statements.append(sql)
        if isinstance(statement, TextClause):
            assert "resolve_custom_import_generation_snapshot(" in sql
            return _Result([None])  # Explicit synthetic canonical storage response.
        assert statement.is_select
        if "transaction_isolation" in sql:
            return _Result(["read committed"])
        if "clock_timestamp" in sql:
            return _Result([_NOW])
        if "set_config" in sql:
            return _Result([None])
        names = {table.name for table in find_tables(statement, check_columns=True)}
        assert len(names) == 1, sql
        rows = self.case.rows[next(iter(names))]
        return _Result([row for row in rows if self._is_match(statement, row)])

    @staticmethod
    def _is_match(statement, row):
        for criterion in statement._where_criteria:
            assert hasattr(criterion.left, "name") and hasattr(criterion.right, "value")
            if getattr(row, criterion.left.name) != criterion.right.value:
                return False
        return True


def _continued_build():
    request = _request(expected_base_generation_id=9, expected_pointer_version=2)
    original_digest = lease_token_sha256(request.lease_token)
    request = replace(
        request,
        lease_token=b"n" * 32,
        continuation=continuation.BuildContinuationContext(3, 7, original_digest),
    )
    build, generation, _proof, materialization = _materialization_case()
    for name in ("complete_scope", "page_row_limit", "page_byte_limit", "statement_timeout_ms", "build_deadline_at"):
        setattr(build, name, getattr(request, name))
    build.refresh_mode = request.definition.refresh_mode
    build.base_generation_id, build.base_pointer_version = 9, 2
    build.request_identity_sha256 = b"r" * 32
    generation.base_generation_id, generation.base_dataset_id = 9, request.dataset_id
    return request, build, generation, materialization


def _control_rows(state):
    request, build, generation, materialization = _continued_build()
    execution = models.CustomImportExecution(
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
        execution_id=request.execution_id,
        capture_bundle_id=20,
        request_identity_sha256=build.request_identity_sha256,
        state=state,
        finished_at=_FINISHED if state in {"completed", "no_change"} else None,
    )
    lease = models.CustomImportLease(
        execution_id=request.execution_id,
        fence=request.fence,
        token_sha256=lease_token_sha256(request.lease_token),
        continuation_id=3,
        continuation_started_at=_FINISHED - dt.timedelta(minutes=3),
        heartbeat_at=_FINISHED,
        expires_at=_FINISHED,
    )
    document = _test_authorization(
        continuation.producer_token_sha256(request), request.lease_token, _FINISHED - dt.timedelta(minutes=12)
    )
    document.update(
        engine_execution_id=str(request.execution_id),
        build_id=7,
        snapshot_family_id=17,
        capture_bundle_id=20,
        producer_fence=request.fence,
        base_generation_id=9,
        base_pointer_version=2,
    )
    authorization = _authorization_row(document, execution, build)
    authorization.continuation_id = 3
    return SimpleNamespace(
        request=request,
        build=build,
        generation=generation,
        execution=execution,
        lease=lease,
        authorization=authorization,
        materialization=materialization,
    )


def _receipt_rows(case):
    request, generation, materialization = case.request, case.generation, case.materialization
    original_digest = continuation.producer_token_sha256(request)
    seal_request = publication._GenerationSealRequest(
        request.dataset_id, generation.generation_id, request.fence, original_digest
    )
    seal = publication._new_generation_seal(generation, materialization, seal_request)
    seal.sealed_at = _FINISHED
    base = models.CustomImportGeneration(
        generation_id=9,
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
        execution_id=90,
        capture_bundle_id=91,
        producing_fence=1,
        producing_token_sha256=b"b" * 32,
        source_bundle_sha256=b"b" * 32,
        root_count=1,
        family_count=1,
    )
    base_seal = publication._new_generation_seal(
        base,
        replace(materialization, source_bundle_sha256=base.source_bundle_sha256),
        publication._GenerationSealRequest(request.dataset_id, 9, 1, b"b" * 32),
    )
    base_seal.sealed_at = _FINISHED - dt.timedelta(hours=1)
    no_change = publication._NoChangeRequest(
        request.dataset_id, request.execution_id, 9, 2, generation.generation_id, request.fence, original_digest
    )
    no_change_seal = publication._new_no_change_seal(no_change, case.execution, base, generation, base_seal, seal)
    event = publication._no_change_publication_event(no_change, base)
    event.publication_event_id = 15
    case.seal, case.seals, case.event, case.no_change_seal = seal, (base_seal, seal), event, no_change_seal
    return base


def _case(state="completed"):
    """Compose real control and receipt producers with a finite SQL-only transport."""
    case = _control_rows(state)
    base = _receipt_rows(case)
    case.rows = {
        "custom_import_dataset": [models.CustomImportDataset(dataset_id=case.request.dataset_id)],
        "custom_import_execution": [case.execution],
        "custom_import_lease": [case.lease],
        "custom_import_build_continuation": [case.authorization],
        "custom_import_generation": [base, case.generation],
        "custom_import_generation_seal": list(case.seals),
        "custom_import_no_change_seal": [case.no_change_seal],
        "custom_import_publication_event": [case.event],
    }
    case.session = _Transport(case)
    return case


@pytest.mark.parametrize("state", ["completed", "no_change"])
async def test_actual_terminal_replay_uses_closed_epoch_and_original_receipts(state):
    case = _case(state)
    assert type(case.authorization) is CustomImportBuildContinuation
    assert case.lease.expires_at == case.lease.heartbeat_at == case.execution.finished_at < _NOW
    assert lease_token_sha256(case.request.lease_token) != continuation.producer_token_sha256(case.request)
    with pytest.raises(LeaseAuthorityLost):
        continuation.verify_continuation_scope(
            case.request, case.execution, case.lease, case.authorization, _NOW, build=case.build
        )
    result = await output._replay(case.session.factory, case.request, case.build.build_id)
    assert result.generation_id == case.generation.generation_id and result.seal.replayed
    assert result.seal == publication._seal_receipt(case.seal, replayed=True)
    assert case.seal.sealing_token_sha256 == continuation.producer_token_sha256(case.request)
    if state == "no_change":
        assert result.no_change.replayed and result.no_change.publication_event_id == case.event.publication_event_id
    else:
        assert result.no_change is None
    assert not any("custom_import.continuation_" in sql for sql in case.session.statements)


async def test_terminal_replay_preserves_shared_storage_and_projection_refusal(monkeypatch):
    case = _case()
    validate = AsyncMock(side_effect=publication.PublicationConflict("synthetic projection custody changed"))
    monkeypatch.setattr(publication, "_validate_sealed_projection", validate)
    with pytest.raises(publication.PublicationConflict, match="projection custody changed"):
        await output._replay(case.session.factory, case.request, case.build.build_id)
    validate.assert_awaited_once_with(case.session, case.generation, case.seal)


@pytest.mark.parametrize("state", ["completed", "no_change"])
@pytest.mark.parametrize("fault", ["nonce", "epoch", "producer", "fence", "build", "generation", "authorization"])
async def test_terminal_replay_refuses_changed_immutable_custody(state, fault):
    case = _case(state)
    match fault:
        case "nonce":
            case.request = replace(case.request, lease_token=b"synthetic-wrong-nonce")
        case "epoch":
            case.lease.continuation_id += 1
        case "producer":
            case.request = replace(
                case.request, continuation=replace(case.request.continuation, producer_token_sha256=b"x" * 32)
            )
        case "fence":
            case.request = replace(case.request, fence=case.request.fence + 1)
        case "build":
            case.request = replace(case.request, continuation=replace(case.request.continuation, build_id=8))
        case "generation":
            case.generation.producing_token_sha256 = b"x" * 32
        case "authorization":
            case.authorization.request_identity_sha256 = b"x" * 32
    with pytest.raises((LeaseAuthorityLost, CandidateRunnerError, publication.PublicationConflict)):
        await output._replay(case.session.factory, case.request, case.build.build_id)
    assert not any("custom_import.continuation_" in sql for sql in case.session.statements)


@pytest.mark.parametrize("fault", ["state", "finished", "heartbeat", "expires", "future", "not_started"])
async def test_terminal_helper_refuses_incompatible_completion_stamps(fault):
    case = _case()
    match fault:
        case "state":
            case.execution.state = "failed"
        case "finished":
            case.execution.finished_at = None
        case "heartbeat":
            case.lease.heartbeat_at += dt.timedelta(microseconds=1)
        case "expires":
            case.lease.expires_at += dt.timedelta(microseconds=1)
        case "future":
            case.execution.finished_at = case.lease.heartbeat_at = case.lease.expires_at = _NOW + dt.timedelta(
                seconds=1
            )
        case "not_started":
            case.lease.continuation_started_at = None
    with pytest.raises(LeaseAuthorityLost):
        await continuation.verify_terminal_continuation(
            case.session, case.request, case.execution, case.lease, _NOW, build=case.build
        )
    assert not any("custom_import.continuation_" in sql for sql in case.session.statements)


async def test_unfinished_continuation_retains_actual_live_expiry_and_guc_binding():
    case = _case("running")
    case.rows["custom_import_generation_seal"] = []
    with pytest.raises(LeaseAuthorityLost):
        await output._replay(case.session.factory, case.request, case.build.build_id)
    assert not any("custom_import.continuation_" in sql for sql in case.session.statements)
    case.lease.expires_at = _NOW + dt.timedelta(seconds=30)
    assert await output._replay(case.session.factory, case.request, case.build.build_id) is None
    assert sum("custom_import.continuation_" in sql for sql in case.session.statements) == 1


@pytest.mark.parametrize("fault", ["missing_seal", "missing_receipt", "receipt_bytes", "missing_event", "event_bytes"])
async def test_no_change_replay_still_requires_exact_immutable_seals_and_event(fault):
    case = _case("no_change")
    if fault == "missing_seal":
        case.rows["custom_import_generation_seal"] = []
    elif fault == "missing_receipt":
        case.rows["custom_import_no_change_seal"] = []
    elif fault == "receipt_bytes":
        case.no_change_seal.canonical_receipt += "changed"
    elif fault == "missing_event":
        case.rows["custom_import_publication_event"] = []
    else:
        case.event.canonical_event += "changed"
    with pytest.raises((LeaseAuthorityLost, CandidateRunnerError, publication.PublicationConflict)):
        await output._replay(case.session.factory, case.request, case.build.build_id)
    assert not any("custom_import.continuation_" in sql for sql in case.session.statements)
