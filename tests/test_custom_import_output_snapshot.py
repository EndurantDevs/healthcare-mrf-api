# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""OUTPUT reads bind immutable hot aliases inside their native read pages."""

from contextlib import asynccontextmanager, contextmanager
from itertools import count
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy import inspect, select
from sqlalchemy.dialects import postgresql

from db.models import custom_import as models
from process.custom_import import build_graph as graph
from process.custom_import import build_output as output
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.execution import lease_token_sha256
from process.custom_import.runner_types import CandidateRunnerError, LeaseAuthorityLost
from process.custom_import.storage_layout import SNAPSHOT_MODELS, snapshot_models
from tests.test_custom_import_build_graph import _request
from tests.test_custom_import_build_output import _generation


def _sql(expression):
    return str(expression.compile(dialect=postgresql.dialect()))


def _same_query(factory):
    canonical = factory({model: model for model in SNAPSHOT_MODELS})
    candidate = factory(snapshot_models(41))
    snapshot_models(42)
    assert _sql(candidate[0]).replace("ci_snapshot_41.", "mrf.") == _sql(canonical[0])
    assert candidate[0].compile().params == canonical[0].compile().params
    assert tuple(_sql(key).replace("ci_snapshot_41.", "mrf.") for key in candidate[1]) == tuple(
        _sql(key) for key in canonical[1]
    )
    assert tuple(inspect(model).mapper.class_ for model in candidate[2]) == canonical[2]
    assert "ci_snapshot_42." not in _sql(candidate[0])
    return candidate


@pytest.mark.parametrize(
    "name",
    ["_family_material", "_child_material", "_scalar_material", "_winner_material"],
)
def test_five_publication_queries_keep_exact_shape_order_and_models(monkeypatch, name):
    query_factories = []
    monkeypatch.setattr(
        output,
        "_output_rows",
        lambda _session, _request, _build, query, **_kwargs: query_factories.append(query) or (row for row in ()),
    )
    request = _request()
    arguments = (
        (None, request, None, 7, _generation(request), ())
        if name in {"_family_material", "_scalar_material"}
        else (None, request, 7, _generation(request), ())
    )
    assert getattr(output, name)(*arguments, **({"child": False} if name == "_scalar_material" else {})) == 0
    assert len(query_factories) == 1
    statement, _keys, row_models = _same_query(query_factories[0])
    assert "ci_snapshot_41." in _sql(statement)
    if name == "_winner_material":
        assert row_models[-1] is models.CustomImportSelectionProfile
        assert "mrf.custom_import_selection_profile" in _sql(statement)


@pytest.mark.parametrize("verify", [False, True])
def test_complete_context_query_and_stored_winner_join_are_candidate_bound(verify):
    request = _request()
    query = lambda aliases: output._context_query(
        aliases,
        7,
        group=(1, 2, b"c" * 32),
        after=(1, 1, b"a" * 32),
        generation=_generation(request) if verify else None,
    )
    statement, keys, row_models = _same_query(query)
    assert len(keys) == len(row_models) == 4
    assert ("ci_snapshot_41.custom_import_winner" in _sql(statement)) is verify


@pytest.mark.parametrize("child", [False, True])
def test_projection_query_retains_native_field_order(child):
    statement, keys, _models = _same_query(
        lambda aliases: output._projection_query(aliases, _generation(_request()), child=child)
    )
    assert "LEFT OUTER JOIN ci_snapshot_41.custom_import_" + ("child" if child else "root") + "_scalar" in _sql(
        statement
    )
    assert "coalesce" in _sql(keys[-1])


@pytest.mark.parametrize("model", [models.CustomImportPack, models.CustomImportRejection])
def test_attempt_query_keeps_exact_captured_producer(model):
    request = _request()
    statement, _keys, _models = _same_query(lambda aliases: output._attempt_query(aliases, request, model))
    parameters = statement.compile().params
    assert request.execution_id in parameters.values()
    assert request.fence in parameters.values()
    assert lease_token_sha256(request.lease_token) in parameters.values()


@pytest.mark.parametrize("kind", ["source", "retained"])
@pytest.mark.parametrize("collapse", [False, True])
def test_canonical_child_query_binds_occurrences_and_correlated_duplicate_alias(kind, collapse):
    request = _request()
    definition = request.definition
    if collapse:
        import json

        document = json.loads(definition.canonical)
        document["streams"][1]["duplicate_policy"] = "collapse_identical"
        definition = CustomImportDefinition.from_mapping(document)
    plan = SimpleNamespace(build_id=7, root_record_id=8, selection_kind=kind, base_family_revision_id=9)

    def query(aliases):
        statement, keys = graph._child_statement(plan, definition, canonical=True, collection_slot=1, models=aliases)
        return statement, keys, (aliases[models.CustomImportChildRevision],)

    statement, _keys, _models = _same_query(query)
    assert "ci_snapshot_41.custom_import_build_occurrence" in _sql(statement)
    assert "mrf.custom_import_build_occurrence" not in _sql(statement)
    if collapse and kind == "source":
        assert "EXISTS" in _sql(statement) and "source_ordinal >" in _sql(statement)


def test_candidate_plan_is_joined_to_the_bound_family_page():
    statement, _keys, _models = _same_query(
        lambda aliases: output._family_query(aliases, 7, _generation(_request()), None)
    )
    assert "LEFT OUTER JOIN ci_snapshot_41.custom_import_build_family" in _sql(statement)


def test_family_child_reducer_binds_all_page_roots_and_canonical_collection_order():
    statement, keys, _models = _same_query(
        lambda aliases: output._family_children_query(aliases, _generation(_request()), (8, 9))
    )
    assert "ci_snapshot_41.custom_import_family_child" in _sql(statement)
    assert "COLLATE" in _sql(keys[1])
    assert (8, 9) in map(
        tuple, (value for value in statement.compile().params.values() if isinstance(value, (tuple, list)))
    )


def test_family_root_array_has_constant_bind_count():
    aliases = snapshot_models(41)
    generation = _generation(_request())
    small = output._family_children_query(aliases, generation, (1,))[0].compile(dialect=postgresql.dialect())
    large = output._family_children_query(aliases, generation, tuple(range(1, 50_001)))[0].compile(
        dialect=postgresql.dialect()
    )
    assert len(small.params) == len(large.params)
    assert len(large.params["root_ids"]) == 50_000
    assert "= ANY" in str(large) and "POSTCOMPILE" not in str(large)


def _read_session(monkeypatch, *, fail_binding=False):
    state = SimpleNamespace(active=False, bindings=0, transactions=0)
    scalar_row = models.CustomImportRootScalar(root_revision_id=9, field_slot=1, string_value="native")
    page_responses = iter([[(1, 6)], [(scalar_row, 1, 1)], []])
    session = SimpleNamespace(info={})

    @contextmanager
    def transaction(_session, _request, _build):
        state.active = True
        state.transactions += 1
        session.info["custom_import_build_read_deadline"] = 20
        try:
            yield None, 20
        finally:
            state.active = False

    def bind(current, build_id):
        assert current is session and build_id == 7 and state.active
        state.bindings += 1
        if fail_binding:
            raise CandidateRunnerError("binding refused")
        return snapshot_models(41), None

    def execute(statement):
        assert state.active
        assert "ci_snapshot_41.custom_import_root_scalar" in _sql(statement)
        return SimpleNamespace(all=lambda: next(page_responses))

    session.execute = Mock(side_effect=execute)
    monkeypatch.setattr(graph, "_read_transaction", transaction)
    monkeypatch.setattr(graph, "_prepare_read", lambda *_args: None)
    monkeypatch.setattr(graph.time, "monotonic", lambda: 10)
    monkeypatch.setattr(output, "_build_storage_models", bind)
    return session, state, scalar_row


def test_query_factory_resolves_each_real_page_and_closes_transaction_before_yield(monkeypatch):
    session, state, row = _read_session(monkeypatch)
    stream = output._output_rows(session, _request(), 7, _root_scalar_query)
    assert state.bindings == 0
    assert next(stream) == (row,)
    assert not state.active and state.bindings == 1
    snapshot_models(42)
    assert list(stream) == []
    assert not state.active and state.bindings == state.transactions == 2
    assert session.execute.call_count == 3


def test_binding_failure_precedes_metadata_and_payload_reads(monkeypatch):
    session, state, _row = _read_session(monkeypatch, fail_binding=True)
    factory = Mock(side_effect=AssertionError("no query without authority"))
    with pytest.raises(CandidateRunnerError, match="binding refused"):
        next(output._output_rows(session, _request(), 7, factory))
    assert not state.active and state.bindings == 1
    session.execute.assert_not_called()
    factory.assert_not_called()


def _root_scalar_query(aliases):
    scalar = aliases[models.CustomImportRootScalar]
    return select(scalar), (scalar.field_slot,), (scalar,)


def test_single_page_factory_closes_without_a_second_page(monkeypatch):
    session, state, row = _read_session(monkeypatch)
    page_records = list(
        output._output_rows(session, _request(), 7, _root_scalar_query, bounds=graph._ReadPage(single_page=True))
    )
    assert page_records == [(row,)] and not state.active
    assert state.bindings == state.transactions == 1


@pytest.mark.parametrize("expired", [False, True])
def test_structural_verification_rechecks_budget_after_protected_binding(monkeypatch, expired):
    events = []
    request = _request()
    connection = SimpleNamespace(get_execution_options=lambda: {})
    session = SimpleNamespace(connection=lambda: connection)

    @contextmanager
    def transaction(current, same_request, build_id):
        assert (current, same_request, build_id) == (session, request, 7)
        events.append("transaction")
        yield object(), 20

    def bind(current, build_id):
        assert current is session and build_id == 7
        events.append("binding")
        return snapshot_models(41), None

    def prepare(current, same_request, deadline):
        assert (current, same_request, deadline) == (session, request, 20)
        events.append("budget")
        if expired:
            raise LeaseAuthorityLost("build read deadline elapsed")

    def execute(statement):
        events.append("verification")
        assert "mrf.verify_custom_import_build_structure" in _sql(statement)
        assert statement.compile().params == {"param_1": 7}
        return SimpleNamespace(one=lambda: SimpleNamespace(verification_state="complete"))

    session.execute = Mock(side_effect=execute)
    monkeypatch.setattr(output, "_read_transaction", transaction)
    monkeypatch.setattr(output, "_build_storage_models", bind)
    monkeypatch.setattr(output, "_prepare_read", prepare)
    monkeypatch.setattr(output, "_require_budget", lambda _deadline: None)
    if expired:
        with pytest.raises(LeaseAuthorityLost, match="deadline elapsed"):
            output._verify_page(session, request, 7)
        session.execute.assert_not_called()
    else:
        assert output._verify_page(session, request, 7) == "complete"
    assert events == ["transaction", "binding", "budget"] + ([] if expired else ["verification"])


@pytest.mark.parametrize("family_id", [41, None, 0, True])
async def test_snapshot_freeze_uses_real_captured_attempt_tuple(monkeypatch, family_id):
    request = _request()
    session = object()

    @asynccontextmanager
    async def page(_factory, same_request, build_id):
        assert same_request is request and build_id == 7
        yield session, object()

    call = AsyncMock(return_value=SimpleNamespace(scalar_one=lambda: family_id))
    monkeypatch.setattr(output, "_page_session", page)
    monkeypatch.setattr(output, "_typed_call", call)
    if family_id == 41:
        await output._freeze_output_snapshot(None, request, 7)
    else:
        with pytest.raises(CandidateRunnerError, match="invalid family"):
            await output._freeze_output_snapshot(None, request, 7)
    call.assert_awaited_once_with(
        session,
        "freeze_custom_import_snapshot_family",
        (
            ("bigint", request.execution_id),
            ("bigint", request.fence),
            ("bytea", lease_token_sha256(request.lease_token)),
        ),
    )


@pytest.mark.parametrize("phase", ["output", "verifying", "verified"])
async def test_output_resumes_only_after_real_snapshot_freeze(monkeypatch, phase):
    events = []
    request = _request()
    build = SimpleNamespace(phase=phase, generation_id=10)
    generation = _generation(request)
    session = SimpleNamespace(get=AsyncMock(return_value=generation), expunge=lambda _row: None)
    steps = count()

    async def run_sync(operation):
        return operation(session)

    session.run_sync = run_sync

    @asynccontextmanager
    async def context(*_args, **_kwargs):
        yield session

    @asynccontextmanager
    async def page(*_args):
        yield session, build

    async def snapshot(*_args):
        return build if next(steps) == 0 else SimpleNamespace(phase="verified")

    async def freeze(*_args):
        events.append("snapshot freeze")

    async def control_freeze(*_args):
        events.append("control freeze")

    async def prepare_indexes(_factory, _request, _build_id, index_phase):
        events.append(f"{index_phase} indexes")

    def materialization(*_args):
        events.append("materialization")
        return object()

    monkeypatch.setattr(output, "_replay", AsyncMock(return_value=None))
    monkeypatch.setattr(output, "_renew_while_reading", context)
    monkeypatch.setattr(output, "_snapshot", snapshot)
    monkeypatch.setattr(output, "_page_session", page)
    monkeypatch.setattr(output, "_session", context)
    monkeypatch.setattr(output, "load_registry", AsyncMock(return_value=object()))
    monkeypatch.setattr(output, "_prepare_statement", AsyncMock())
    monkeypatch.setattr(output, "_prepare_snapshot_indexes", prepare_indexes)
    monkeypatch.setattr(output, "_attach_families", AsyncMock())
    monkeypatch.setattr(output, "_write_winners", AsyncMock())
    monkeypatch.setattr(output, "_call", control_freeze)
    monkeypatch.setattr(output, "_freeze_output_snapshot", freeze)
    monkeypatch.setattr(output, "_one_row", lambda *_args: (object(),))
    monkeypatch.setattr(output, "_frozen_materialization", materialization)
    monkeypatch.setattr(output, "_terminal_seal", AsyncMock(return_value="sealed"))
    assert await output.build_output(None, request, 7) == "sealed"
    assert events == (["output indexes", "control freeze"] if phase == "output" else []) + [
        "snapshot freeze",
        "serving indexes",
        "materialization",
    ]


async def test_immutable_replay_does_not_reopen_or_freeze_a_candidate(monkeypatch):
    monkeypatch.setattr(output, "_replay", AsyncMock(return_value="replayed"))
    freeze = AsyncMock(side_effect=AssertionError("terminal replay has no mutable binding"))
    monkeypatch.setattr(output, "_freeze_output_snapshot", freeze)
    assert await output.build_output(None, _request(), 7) == "replayed"
    freeze.assert_not_called()
