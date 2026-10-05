# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Legacy graph assembly pins its base and closes its isolated candidate."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock, call

import pytest
from sqlalchemy.dialects.postgresql import dialect

from process.custom_import import materialization_store, read_identity
from process.custom_import import runner_graph as graph
from process.custom_import.execution import lease_token_sha256
from process.custom_import.runner_types import CandidateRunnerError, CurrentGenerationPointer
from process.custom_import.storage_layout import snapshot_models
from tests.test_custom_import_legacy_graph_store import _request


def _compiled(statement):
    return str(statement.compile(dialect=dialect(), compile_kwargs={"literal_binds": True}))


@pytest.mark.parametrize("query", [graph.selected_family_statement, graph.previous_children_statement])
def test_base_query_has_identical_semantics_and_no_mixed_hot_namespaces(query):
    """All hot aliases change together while definition/control tables stay fixed."""

    request = _request()
    pointer = CurrentGenerationPointer(61, 21, 31, 1)
    canonical = _compiled(query(request, pointer))
    namespace = graph.CustomImportGenerationFamily.__table__.schema
    models = snapshot_models(23)
    rendered = _compiled(query(request, pointer, models=models))
    assert rendered.replace("ci_snapshot_23.", namespace + ".") == canonical
    for model in models:
        assert f"{namespace}.{model.__tablename__}" not in rendered
    with pytest.raises(KeyError):
        query(request, pointer, models={})


@pytest.mark.parametrize("family_id", [None, 23, 24])
async def test_base_storage_is_resolved_again_for_each_transaction(monkeypatch, family_id):
    """Only the protected resolver can select canonical legacy storage."""

    request = _request()
    pointer = CurrentGenerationPointer(61, 22, 31, 1)
    session = object()
    prepare = AsyncMock()
    resolve = AsyncMock(return_value=family_id)
    monkeypatch.setattr(graph, "prepare_materialization_statement", prepare)
    monkeypatch.setattr(read_identity, "resolve_generation_snapshot", resolve)
    for _ in range(2):
        models = await graph.previous_snapshot_models(session, request, pointer)
        assert models is (None if family_id is None else snapshot_models(family_id))
    assert prepare.await_count == resolve.await_count == 2
    target = resolve.await_args.args[1]
    assert (target.dataset_id, target.generation_id, target.definition_revision_id, target.schema_revision_id) == (
        request.dataset_id,
        61,
        22,
        31,
    )


async def test_base_resolver_failure_is_not_a_canonical_fallback(monkeypatch):
    request = _request()
    monkeypatch.setattr(graph, "prepare_materialization_statement", AsyncMock())
    failure = RuntimeError("synthetic resolver failure")
    monkeypatch.setattr(read_identity, "resolve_generation_snapshot", AsyncMock(side_effect=failure))
    with pytest.raises(RuntimeError) as captured:
        await graph.previous_snapshot_models(object(), request, CurrentGenerationPointer(61, 21, 31, 1))
    assert captured.value is failure


@pytest.mark.parametrize("binding", [None, True, 0, -1, 2**63, "23", 23.0, 23])
async def test_generation_is_flushed_and_bound_before_hot_writes(monkeypatch, binding):
    """A failed binding cannot proceed to pack or family persistence."""

    request = _request()
    session = SimpleNamespace(add=Mock())
    grant = SimpleNamespace(fence=2)
    flush_receipts = []

    async def flush(current_session):
        assert current_session is session
        generation = session.add.call_args.args[0]
        generation.generation_id = 61
        flush_receipts.append(True)

    async def call(current_session, name, arguments):
        assert current_session is session and flush_receipts == [True]
        assert name == "resolve_custom_import_legacy_generation_snapshot" and arguments == (("bigint", 61),)
        return binding

    monkeypatch.setattr(graph, "flush_materialization", flush)
    monkeypatch.setattr(materialization_store, "_call", call)
    if binding != 23 or type(binding) is not int:
        with pytest.raises(CandidateRunnerError, match="snapshot binding is malformed"):
            await graph.create_generation(session, request, grant, None, b"s" * 32, (), 51)
    else:
        generation = await graph.create_generation(session, request, grant, None, b"s" * 32, (), 51)
        assert generation is session.add.call_args.args[0] and generation.generation_id == 61


@pytest.mark.parametrize("binding", [None, True, 0, -1, 2**63, "23", 23.0, 23])
async def test_snapshot_freeze_uses_exact_producer_and_checks_its_result(monkeypatch, binding):
    request = _request()
    grant = SimpleNamespace(fence=2)
    session = object()
    call = AsyncMock(return_value=binding)
    monkeypatch.setattr(materialization_store, "_call", call)
    if binding != 23 or type(binding) is not int:
        with pytest.raises(CandidateRunnerError, match="freeze binding is malformed"):
            await graph.freeze_legacy_snapshot(session, request, grant)
    else:
        await graph.freeze_legacy_snapshot(session, request, grant)
    call.assert_awaited_once_with(
        session,
        "freeze_custom_import_snapshot_family",
        (
            ("bigint", request.execution_id),
            ("bigint", grant.fence),
            ("bytea", lease_token_sha256(request.lease_token)),
        ),
    )


async def test_legacy_assembly_binds_before_packs_and_freezes_last(monkeypatch):
    """No hot append precedes storage creation; no append follows freeze."""

    completed_stages = []
    request = _request()
    session = object()
    generation = SimpleNamespace(generation_id=61)
    admitted = SimpleNamespace(families=(), rejections=())
    stage_results_by_name = {
        "locked_candidate_context": (None, SimpleNamespace(capture_bundle_id=51), None),
        "select_candidate_families": (),
        "source_bundle_digest": b"s" * 32,
        "create_generation": generation,
        "create_packs": {},
        "publish_families": (),
    }
    for name in (
        "locked_candidate_context",
        "prepare_materialization_statement",
        "ensure_selection_profiles",
        "select_candidate_families",
        "source_bundle_digest",
        "create_generation",
        "create_packs",
        "publish_families",
        "attach_generation_families",
        "persist_projections_and_winners",
        "persist_rejections",
        "freeze_legacy_snapshot",
    ):

        async def stage(*_args, stage_name=name):
            completed_stages.append(stage_name)
            return stage_results_by_name.get(stage_name)

        monkeypatch.setattr(graph, name, stage)
    monkeypatch.setattr(materialization_store, "verify_materialization_authority", AsyncMock())
    clear = Mock()
    monkeypatch.setattr(graph, "clear_materialization_authority", clear)
    candidate_result = await graph.build_candidate_graph(session, request, SimpleNamespace(fence=2), admitted)
    assert candidate_result.generation_id == 61
    assert completed_stages.index("create_generation") < completed_stages.index("create_packs")
    assert completed_stages[-1] == "freeze_legacy_snapshot"
    clear.assert_called_once_with(session)


async def test_legacy_serving_indexes_follow_graph_commit_with_one_index_per_transaction(
    monkeypatch,
):
    request = _request()
    grant = SimpleNamespace(fence=2)
    admitted = object()
    materialized = SimpleNamespace(generation_id=61)
    sessions = [MagicMock() for _ in range(3)]
    for session in sessions:
        session.__aenter__.return_value = session
        session.begin.return_value = AsyncMock()
    factory = Mock(side_effect=sessions)
    build = AsyncMock(return_value=materialized)
    completed_sessions = []

    async def lock(session, current_request, current_grant):
        assert current_request is request and current_grant is grant
        previous = sessions[len(completed_sessions)]
        previous.begin.return_value.__aexit__.assert_awaited_once_with(None, None, None)
        completed_sessions.append(session)
        return None, SimpleNamespace(capture_bundle_id=51), None

    protected_call = AsyncMock(side_effect=[23, False, 23, True])
    clear = Mock()
    monkeypatch.setattr(graph, "build_candidate_graph", build)
    monkeypatch.setattr(graph, "locked_candidate_context", lock)
    monkeypatch.setattr(graph, "clear_materialization_authority", clear)
    monkeypatch.setattr(materialization_store, "_call", protected_call)
    assert await graph.materialize_candidate(factory, request, grant, admitted) is materialized
    build.assert_awaited_once_with(sessions[0], request, grant, admitted)
    assert completed_sessions == sessions[1:]
    arguments = (
        ("bigint", 61),
        ("bigint", request.dataset_id),
        ("bigint", request.definition_revision_id),
        ("bigint", request.schema_revision_id),
        ("bigint", request.execution_id),
        ("bigint", 51),
        ("bigint", grant.fence),
        ("bytea", lease_token_sha256(request.lease_token)),
    )
    assert protected_call.await_args_list == [
        expected_call
        for session in sessions[1:]
        for expected_call in (
            call(session, "resolve_custom_import_generation_finality_snapshot", arguments),
            call(
                session,
                "prepare_custom_import_snapshot_indexes",
                (("bigint", 23), ("text", "serving")),
            ),
        )
    ]
    assert clear.call_args_list == [call(session) for session in sessions[1:]]
    for session in sessions:
        session.begin.assert_called_once_with()
        session.begin.return_value.__aexit__.assert_awaited_once_with(None, None, None)


@pytest.mark.parametrize(
    ("results", "message"),
    [
        ([None], "snapshot binding"),
        ([True], "snapshot binding"),
        ([0], "snapshot binding"),
        ([2**63], "snapshot binding"),
        ([23, None], "preparation result"),
        ([23, 1], "preparation result"),
        ([23, "true"], "preparation result"),
    ],
)
async def test_legacy_index_preparation_rejects_malformed_results_and_clears_authority(monkeypatch, results, message):
    session = object()
    protected_call = AsyncMock(side_effect=results)
    monkeypatch.setattr(
        graph,
        "locked_candidate_context",
        AsyncMock(return_value=(None, SimpleNamespace(capture_bundle_id=51), None)),
    )
    monkeypatch.setattr(materialization_store, "_call", protected_call)
    clear = Mock()
    monkeypatch.setattr(graph, "clear_materialization_authority", clear)
    with pytest.raises(CandidateRunnerError, match=message):
        await graph.prepare_legacy_serving_step(session, _request(), SimpleNamespace(fence=2), 61)
    assert protected_call.await_count == len(results)
    clear.assert_called_once_with(session)


@pytest.mark.parametrize("failure_stage", ["authority", "resolve", "index"])
async def test_legacy_index_preparation_propagates_failure_and_rolls_back_its_transaction(monkeypatch, failure_stage):
    session = MagicMock()
    session.__aenter__.return_value = session
    session.begin.return_value = AsyncMock()
    failure = RuntimeError("synthetic index preparation failure")
    lock = AsyncMock(return_value=(None, SimpleNamespace(capture_bundle_id=51), None))
    protected_call = AsyncMock(side_effect=[23, failure] if failure_stage == "index" else failure)
    if failure_stage == "authority":
        lock.side_effect = failure
    clear = Mock()
    monkeypatch.setattr(graph, "locked_candidate_context", lock)
    monkeypatch.setattr(graph, "clear_materialization_authority", clear)
    monkeypatch.setattr(materialization_store, "_call", protected_call)
    with pytest.raises(RuntimeError) as captured:
        await graph.prepare_legacy_snapshot_indexes(lambda: session, _request(), SimpleNamespace(fence=2), 61)
    assert captured.value is failure
    assert session.begin.return_value.__aexit__.await_args.args[:2] == (
        RuntimeError,
        failure,
    )
    clear.assert_called_once_with(session)
    assert protected_call.await_count == {"authority": 0, "resolve": 1, "index": 2}[failure_stage]
