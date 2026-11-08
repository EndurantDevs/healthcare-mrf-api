# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Host transport checks; native lineage, constraints and grants need PostgreSQL."""

from __future__ import annotations

import time
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from sqlalchemy.exc import IntegrityError

from db.models.custom_import import CustomImportGeneration
from process.custom_import import legacy_generation_store as store
from process.custom_import import runner_graph as graph
from process.custom_import import runner_registry as registry
from process.custom_import.execution import lease_token_sha256
from process.custom_import.runner_types import CandidateRunnerError, LeaseAuthorityLost, PublishedCandidateFamily
from tests.test_custom_import_legacy_graph_store import _bind, _request
from tests.test_custom_import_legacy_graph_store import _Session as _GraphSession


class _Session(_GraphSession):
    def __init__(self, request):
        super().__init__(request)
        self.result_count = None
        self.set_error = None
        self.flush_error = None
        self.final_error = None
        self.final_checks = 0
        self.add_all = Mock(side_effect=AssertionError("memberships require protected sets"))
        self.begin = self.commit = self.rollback = Mock(side_effect=AssertionError("transaction belongs to caller"))

    async def flush(self):
        if self.flush_error is not None:
            raise self.flush_error
        await super().flush()

    async def execute(self, statement, parameters=None):
        sql = str(statement)
        if ".persist_custom_import_legacy_generation_family_set" in sql:
            self.calls.append((sql, parameters))
            self.set_count += 1
            if self.set_error is not None and self.set_count == (self.fail_set or 1):
                raise self.set_error
            value = len(parameters["p8"]) if self.result_count is None else self.result_count
            return SimpleNamespace(scalar_one=lambda: value)
        if ".check_custom_import_generation_materialization_authority" in sql:
            self.final_checks += 1
            if self.final_error is not None:
                raise self.final_error
        return await super().execute(statement, parameters)


def _generation(request):
    return CustomImportGeneration(
        generation_id=61,
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
        execution_id=request.execution_id,
        capture_bundle_id=51,
        producing_fence=2,
        producing_token_sha256=lease_token_sha256(request.lease_token),
    )


def _families(count=1):
    return tuple(
        PublishedCandidateFamily(
            root_record_id=1000 + index,
            root_revision_id=2000 + index,
            family_revision_id=3000 + index,
            entity_binding_id=4000 + index,
            family_sha256=b"f" * 32,
            root_values_by_field={},
            children=(),
        )
        for index in range(count)
    )


def _pages(session):
    return [(sql, args) for sql, args in session.calls if ".persist_custom_import_legacy_generation_family_set" in sql]


@pytest.mark.asyncio
async def test_standalone_memberships_use_bounded_native_pages_and_one_final_check():
    request = _request()
    session = _Session(request)
    families = _families(16_385)
    assert await graph.attach_generation_families(session, request, _generation(request), families) is None
    pages = _pages(session)
    assert [len(args["p8"]) for _, args in pages] == [16_384, 1]
    assert all('"synthetic schema".persist_custom_import_legacy_generation_family_set' in sql for sql, _ in pages)
    assert pages[0][1]["p8"] == tuple(family.root_record_id for family in families[:16_384])
    assert pages[0][1]["p9"] == tuple(family.family_revision_id for family in families[:16_384])
    assert all(args["p10"] is args["p11"] is args["p12"] is None for _, args in pages)
    assert all(store._PAGE_OVERHEAD_BYTES + 40 + 24 * len(args["p8"]) <= store._PAGE_BYTES for _, args in pages)
    final_sql, final_args = session.calls[-1]
    assert "check_custom_import_generation_materialization_authority" in final_sql
    assert tuple(final_args[f"p{index}"] for index in range(8)) == (
        61,
        11,
        21,
        31,
        41,
        51,
        2,
        lease_token_sha256(request.lease_token),
    )
    assert final_args["p8"] is final_args["p9"] is final_args["p10"] is None
    assert session.final_checks == session.flushes == 1 and session.info == {}
    session.add_all.assert_not_called()
    session.commit.assert_not_called()


@pytest.mark.asyncio
async def test_encoded_byte_ceiling_can_bound_pages_before_row_limit(monkeypatch):
    request = _request()
    session = _Session(request)
    monkeypatch.setattr(store, "_PAGE_BYTES", store._PAGE_OVERHEAD_BYTES + 40 + 48)
    await graph.attach_generation_families(session, request, _generation(request), _families(5))
    assert [len(args["p8"]) for _, args in _pages(session)] == [2, 2, 1]


@pytest.mark.asyncio
@pytest.mark.parametrize("runner", [False, True])
async def test_empty_memberships_flush_and_check_exact_generation(runner):
    request = _request()
    session = _Session(request)
    if runner:
        await _bind(session, request)
        session.calls.clear()
    await graph.attach_generation_families(session, request, _generation(request), ())
    assert _pages(session) == [] and session.flushes == session.final_checks == 1
    session.add_all.assert_not_called()
    session.commit.assert_not_called()


@pytest.mark.asyncio
async def test_real_runner_expectation_is_bound_to_current_transaction():
    request = _request()
    session = _Session(request)
    await _bind(session, request)
    window = session.info[registry._MATERIALIZATION_WINDOW_KEY]
    session.calls.clear()
    await graph.attach_generation_families(session, request, _generation(request), _families())
    args = _pages(session)[0][1]
    assert args["p10"] == (11, 21, 31, 41, 51, 2)
    assert args["p11"] == lease_token_sha256(request.lease_token) and args["p12"] == window.expires_at
    session.transaction = object()
    session.calls.clear()
    with pytest.raises(CandidateRunnerError, match="not bound"):
        await graph.attach_generation_families(session, request, _generation(request), _families())
    assert session.calls == [] and session.flushes == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("attribute", ["capture_bundle_id", "producing_fence", "producing_token_sha256"])
async def test_real_runner_rejects_generation_producer_drift_before_flush(attribute):
    request = _request()
    session = _Session(request)
    await _bind(session, request)
    generation = _generation(request)
    setattr(generation, attribute, b"x" * 32 if attribute == "producing_token_sha256" else 99)
    session.calls.clear()
    with pytest.raises(CandidateRunnerError, match="authority identity differs"):
        await graph.attach_generation_families(session, request, generation, _families())
    assert session.calls == [] and session.flushes == 0


@pytest.mark.asyncio
async def test_standalone_request_owner_and_token_must_match_exact_generation():
    request = _request()
    session = _Session(request)
    for changed in (replace(request, execution_id=99), replace(request, lease_token="synthetic-other-token")):
        with pytest.raises(CandidateRunnerError, match="authority identity differs"):
            await graph.attach_generation_families(session, changed, _generation(request), _families())
    assert session.calls == [] and session.flushes == 0


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("attribute", "value"),
    [
        ("generation_id", True),
        ("dataset_id", 0),
        ("schema_revision_id", 2**63),
        ("capture_bundle_id", None),
        ("producing_fence", -1),
    ],
)
async def test_malformed_generation_identity_precedes_flush(attribute, value):
    request = _request()
    session = _Session(request)
    generation = _generation(request)
    setattr(generation, attribute, value)
    with pytest.raises(CandidateRunnerError, match="generation identity is malformed"):
        await graph.attach_generation_families(session, request, generation, _families())
    assert session.calls == [] and session.flushes == session.final_checks == 0
    session.commit.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("token", [None, "x" * 32, b"x" * 31, b"x" * 33])
async def test_malformed_generation_token_precedes_flush(token):
    request = _request()
    session = _Session(request)
    generation = _generation(request)
    generation.producing_token_sha256 = token
    with pytest.raises(CandidateRunnerError, match="generation producer token is malformed"):
        await graph.attach_generation_families(session, request, generation, _families())
    assert session.calls == [] and session.flushes == session.final_checks == 0
    session.commit.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [True, 0, -1, 2**63, None])
async def test_malformed_late_member_fails_before_flush_or_first_page(value):
    request = _request()
    session = _Session(request)
    families = _families(16_385)
    families = (*families[:-1], replace(families[-1], family_revision_id=value))
    with pytest.raises(CandidateRunnerError, match="membership identity is malformed"):
        await graph.attach_generation_families(session, request, _generation(request), families)
    assert session.calls == [] and session.flushes == 0


@pytest.mark.asyncio
async def test_duplicates_are_sent_unchanged_for_native_constraint_rejection(monkeypatch):
    request = _request()
    session = _Session(request)
    monkeypatch.setattr(store, "_PAGE_ROWS", 2)
    first, second = _families(2)
    await graph.attach_generation_families(session, request, _generation(request), (first, second, first))
    assert [args["p8"] for _, args in _pages(session)] == [(1000, 1001), (1000,)]
    assert [args["p9"] for _, args in _pages(session)] == [(3000, 3001), (3000,)]
    session = _Session(request)
    session.set_error = IntegrityError("synthetic protected set", {}, ValueError("duplicate membership"))
    with pytest.raises(IntegrityError):
        await graph.attach_generation_families(session, request, _generation(request), (first, first))
    assert session.final_checks == 0
    session.commit.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [True, False, 0, 2, "1"])
async def test_invalid_native_count_escapes_without_final_check(count):
    request = _request()
    session = _Session(request)
    session.result_count = count
    with pytest.raises(CandidateRunnerError, match="persisted count differs"):
        await graph.attach_generation_families(session, request, _generation(request), _families())
    assert session.final_checks == 0
    session.commit.assert_not_called()


@pytest.mark.asyncio
async def test_active_caller_transaction_is_required_even_when_empty():
    request = _request()
    session = _Session(request)
    session.active = False
    with pytest.raises(ValueError, match="active caller transaction"):
        await graph.attach_generation_families(session, request, _generation(request), ())
    assert session.calls == [] and session.flushes == 0


@pytest.mark.asyncio
async def test_flush_and_late_page_failures_escape_to_transaction_owner(monkeypatch):
    request = _request()
    session = _Session(request)
    session.flush_error = RuntimeError("synthetic pending flush failure")
    with pytest.raises(RuntimeError, match="pending flush"):
        await graph.attach_generation_families(session, request, _generation(request), ())
    assert session.calls == [] and session.final_checks == 0
    session = _Session(request)
    monkeypatch.setattr(store, "_PAGE_ROWS", 2)
    session.fail_set = 2
    session.set_error = RuntimeError("synthetic late page failure")
    with pytest.raises(RuntimeError, match="late page"):
        await graph.attach_generation_families(session, request, _generation(request), _families(3))
    assert session.set_count == 2 and session.final_checks == 0
    session.commit.assert_not_called()
    session.rollback.assert_not_called()


@pytest.mark.asyncio
async def test_expired_runner_and_final_authority_failure_cannot_return_success():
    request = _request()
    session = _Session(request)
    await _bind(session, request)
    window = session.info[registry._MATERIALIZATION_WINDOW_KEY]
    session.info[registry._MATERIALIZATION_WINDOW_KEY] = replace(window, monotonic_deadline=time.monotonic() - 1)
    session.calls.clear()
    with pytest.raises(LeaseAuthorityLost, match="window expired"):
        await graph.attach_generation_families(session, request, _generation(request), ())
    assert session.calls == [] and session.flushes == 0
    session = _Session(request)
    session.final_error = CandidateRunnerError("synthetic final authority changed")
    with pytest.raises(CandidateRunnerError, match="final authority changed"):
        await graph.attach_generation_families(session, request, _generation(request), _families())
    assert session.set_count == session.final_checks == 1
    session.commit.assert_not_called()
