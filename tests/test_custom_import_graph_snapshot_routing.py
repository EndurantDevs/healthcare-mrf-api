# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Graph query routing and authority freshness; no native execution is claimed."""

from contextlib import asynccontextmanager, contextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy import select
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import DBAPIError
from sqlalchemy.sql.elements import TextClause

from db.models import custom_import as models
from process.custom_import import build_graph as graph
from process.custom_import import build_graph_prepare_page as prepare
from process.custom_import import build_graph_sets as sets
from process.custom_import.runner_codec import candidate_hash_ordered
from process.custom_import.runner_types import CandidateRunnerError, LeaseAuthorityLost
from process.custom_import.storage_layout import snapshot_models
from tests.test_custom_import_build_graph import _registry, _request
from tests.test_custom_import_build_graph_prepare import root_row


def _resolver_session(candidate_id=17, base_id=18, *, schema="mrf"):
    connection = SimpleNamespace(
        dialect=postgresql.dialect(),
        get_execution_options=lambda: {"schema_translate_map": {"mrf": schema}},
    )
    return SimpleNamespace(
        connection=lambda: connection,
        execute=Mock(return_value=SimpleNamespace(one=lambda: (candidate_id, base_id))),
    )


@pytest.mark.parametrize("base_id", [None, 18, 2**63 - 1])
def test_build_resolvers_use_exact_build_and_quoted_control_namespace(base_id):
    session = _resolver_session(base_id=base_id, schema='control"namespace')
    candidate_models, base_models = graph._build_storage_models(session, 7)
    assert candidate_models is snapshot_models(17)
    assert base_models is (None if base_id is None else snapshot_models(base_id))
    statement, parameters = session.execute.call_args.args
    assert str(statement) == (
        'SELECT "control""namespace".resolve_custom_import_build_snapshot(CAST(:build_id AS bigint)), '
        '"control""namespace".resolve_custom_import_build_base_snapshot(CAST(:build_id AS bigint))'
    )
    assert parameters == {"build_id": 7}


@pytest.mark.parametrize("invalid_id", [None, True, False, 0, -1, 2**63, "17", 17.0])
def test_candidate_resolution_never_falls_back_to_canonical(invalid_id):
    with pytest.raises(CandidateRunnerError, match="candidate snapshot identity"):
        graph._build_storage_models(_resolver_session(candidate_id=invalid_id), 7)


@pytest.mark.parametrize("invalid_id", [True, False, 0, -1, 2**63, "18", 18.0])
def test_only_explicit_null_allows_canonical_base(invalid_id):
    with pytest.raises(CandidateRunnerError, match="base snapshot identity"):
        graph._build_storage_models(_resolver_session(base_id=invalid_id), 7)


def test_missing_control_schema_or_resolver_failure_does_not_read_hot_tables():
    session = _resolver_session(schema=None)
    with pytest.raises(CandidateRunnerError, match="explicit control schema"):
        graph._build_storage_models(session, 7)
    session.execute.assert_not_called()
    session = _resolver_session()
    failure = DBAPIError("resolver", {}, RuntimeError("unavailable"))
    session.execute.side_effect = failure
    with pytest.raises(DBAPIError) as caught:
        graph._build_storage_models(session, 7)
    assert caught.value is failure
    assert session.execute.call_count == 1


@pytest.mark.parametrize("base_id", [None, 18])
def test_pending_root_query_separates_selected_roots_and_preserves_failed_joins(base_id):
    candidate_models = snapshot_models(17)
    base_models = None if base_id is None else snapshot_models(base_id)
    statement, keys, selected_models = prepare._pending_statement(_request(), 7, candidate_models, base_models)
    sql = str(statement.compile(dialect=postgresql.dialect()))
    base_schema = "mrf" if base_id is None else "ci_snapshot_18"
    assert len(statement.get_final_froms()) == 1
    assert len(selected_models) == 7 and len(statement.column_descriptions) == 8
    assert keys[0].class_ is candidate_models[models.CustomImportBuildFamily]
    for name in ("build_family", "build_occurrence", "root_revision", "pack"):
        assert f"ci_snapshot_17.custom_import_{name}" in sql
        assert f"mrf.custom_import_{name}." not in sql
    assert "ci_snapshot_17.custom_import_family_revision AS current_family" in sql
    assert f"{base_schema}.custom_import_family_revision AS base_family" in sql
    assert f"{base_schema}.custom_import_root_revision AS retained_root" in sql
    assert (
        f"{base_schema}.custom_import_generation_family.generation_id = mrf.custom_import_build_attempt.base_generation_id"
        in sql
    )
    assert "LEFT OUTER JOIN ci_snapshot_17.custom_import_root_revision ON" in sql
    assert f"LEFT OUTER JOIN {base_schema}.custom_import_root_revision AS retained_root ON" in sql
    for name in ("build_attempt", "build_stream", "source_stream", "generation_seal", "root_record", "entity_binding"):
        assert f"mrf.custom_import_{name}" in sql
        assert f"ci_snapshot_17.custom_import_{name} " not in sql
    where_sql = sql.rsplit("\nWHERE ", 1)[1]
    assert "root_revision_id" not in where_sql
    assert "scope_valid" in sql
    assert "retained_root.root_revision_id IS NOT NULL" in sql


@pytest.mark.parametrize("retained", [False, True])
def test_selected_root_adaptation_keeps_the_original_reducer_shape_and_budget(retained):
    request = _request()
    original_row = root_row(request, 1, retained=retained, started=True)
    plan, root, *identity_fields = original_row
    sql_row = (plan, None, root, *identity_fields) if retained else (plan, root, None, *identity_fields)
    adapted_row = prepare._selected_root_row(sql_row)
    assert adapted_row == original_row
    assert graph._model_bytes(model for model in sql_row[:-1] if model is not None) == graph._model_bytes(
        model for model in adapted_row[:-1] if model is not None
    )
    missing_row = (plan, None, None, *identity_fields[:-1], False)
    with pytest.raises(CandidateRunnerError, match="identity or provenance"):
        prepare._root_input(request, prepare._selected_root_row(missing_row))


@pytest.mark.parametrize("base_id", [None, 18])
@pytest.mark.parametrize("retained", [False, True])
def test_child_consumption_keeps_candidate_plan_and_exact_child_namespace(base_id, retained):
    request = _request()
    selected = root_row(request, 1, retained=retained, started=True)
    family_input = prepare._root_input(request, selected)
    state = sets._Started(family_input, selected[0], selected[5], False)
    statement, keys, selected_models = sets._children_statement(
        request,
        _registry(request.definition),
        (state,),
        snapshot_models(17),
        None if base_id is None else snapshot_models(base_id),
    )
    sql = str(statement.order_by(*keys).compile(dialect=postgresql.dialect()))
    child_schema = ("mrf" if base_id is None else "ci_snapshot_18") if retained else "ci_snapshot_17"
    assert len(statement.get_final_froms()) == 1
    assert len(selected_models) == 1
    assert "FROM ci_snapshot_17.custom_import_build_family JOIN mrf.custom_import_build_attempt" in sql
    assert f"LEFT OUTER JOIN {child_schema}.custom_import_child_revision" in sql
    assert (f"JOIN {child_schema}.custom_import_family_child" in sql) is retained
    assert "ci_snapshot_17.custom_import_build_family.family_revision_id" in sql
    assert "scope_valid" in sql
    assert "mrf.custom_import_build_family" not in sql
    assert "mrf.custom_import_build_occurrence" not in sql
    if not retained:
        assert "ci_snapshot_17.custom_import_pack" in sql
        assert "mrf.custom_import_source_stream" in sql
        assert "mrf.custom_import_build_stream" in sql
    assert f"{child_schema}.custom_import_child_revision.dataset_id = mrf.custom_import_build_attempt.dataset_id" in sql


def _paged_session(monkeypatch, pages, *, fail_resolution=None):
    """Exercise real query factories inside a strict transaction-aware SQL double."""

    session = _resolver_session()
    session.info = {}
    session.events = []
    session.transaction_count = 0
    session.has_transaction = False
    responses = iter(pages)

    @contextmanager
    def transaction(_session, _request, _build_id):
        assert not session.has_transaction
        session.has_transaction = True
        session.transaction_count += 1
        session.info["custom_import_build_read_deadline"] = 20
        try:
            yield None, 20
        finally:
            session.has_transaction = False

    def execute(statement, parameters=None):
        assert session.has_transaction
        if isinstance(statement, TextClause):
            assert parameters == {"build_id": 7}
            session.events.append("resolve")
            if session.transaction_count == fail_resolution:
                raise CandidateRunnerError("binding unavailable")
            return SimpleNamespace(one=lambda: (17, 18))
        description_by_key = statement.column_descriptions[0]
        session.events.append("payload" if description_by_key["expr"] is description_by_key["entity"] else "metadata")
        rows = next(responses)
        return SimpleNamespace(all=lambda: rows)

    session.execute = Mock(side_effect=execute)
    monkeypatch.setattr(graph, "_read_transaction", transaction)
    monkeypatch.setattr(graph, "_prepare_read", Mock())
    monkeypatch.setattr(graph.time, "monotonic", lambda: 10)
    return session


def _plan_query(candidate_models, _base_models):
    plan = candidate_models[models.CustomImportBuildFamily]
    return select(plan).where(plan.build_id == 7), (plan.root_record_id,), (plan,)


def test_each_page_resolves_before_metadata_and_closes_before_yield(monkeypatch):
    session = _paged_session(monkeypatch, [[(1, 1)], [("first",)], [(2, 1)], [("second",)], []])
    stream = graph._read_snapshot_rows(session, _request(), 7, _plan_query)
    assert next(stream) == ("first",) and not session.has_transaction
    assert list(stream) == [("second",)] and not session.has_transaction
    assert session.events == ["resolve", "metadata", "payload", "resolve", "metadata", "payload", "resolve", "metadata"]
    assert session.transaction_count == 3
    statements = [call.args[0] for call in session.execute.call_args_list if not isinstance(call.args[0], TextClause)]
    assert all("ci_snapshot_17.custom_import_build_family" in str(statement) for statement in statements)
    assert any("root_record_id) >" in str(statement) for statement in statements[2:])


def test_next_page_resolution_failure_cannot_reuse_prior_alias_authority(monkeypatch):
    session = _paged_session(monkeypatch, [[(1, 1)], [("first",)]], fail_resolution=2)
    stream = graph._read_snapshot_rows(session, _request(), 7, _plan_query)
    assert next(stream) == ("first",)
    with pytest.raises(CandidateRunnerError, match="binding unavailable"):
        next(stream)
    assert session.events == ["resolve", "metadata", "payload", "resolve"]
    assert not session.has_transaction and stream.gi_frame is None


def test_resolver_elapsed_deadline_prevents_metadata_fetch(monkeypatch):
    prepare_read = graph._prepare_read
    session = _paged_session(monkeypatch, [])
    execute = session.execute.side_effect

    def expire_during_resolution(statement, parameters=None):
        resolved = execute(statement, parameters)
        monkeypatch.setattr(graph.time, "monotonic", lambda: 21)
        return resolved

    session.execute.side_effect = expire_during_resolution
    monkeypatch.setattr(graph, "_prepare_read", prepare_read)
    with pytest.raises(LeaseAuthorityLost, match="deadline"):
        list(graph._read_snapshot_rows(session, _request(), 7, _plan_query))
    assert session.events == ["resolve"]
    assert not session.has_transaction


@pytest.mark.parametrize("physical", [False, True])
def test_candidate_digest_reads_only_resolved_plan_and_preserves_hash(monkeypatch, physical):
    root_hashes = (b"a" * 32, b"b" * 32)
    families = [SimpleNamespace(complete_at=object(), root_key_sha256=digest) for digest in root_hashes]
    records = [
        (family, family.root_key_sha256, index, len(families)) if physical else (family,)
        for index, family in enumerate(families, 1)
    ]
    session = _paged_session(
        monkeypatch,
        [[(root_hashes[0], 1, 1), (root_hashes[1], 2, 1)], records, []],
    )
    request = _request()
    digest, count = graph._candidate_digest(session, request, 7, bounds=graph._ReadPage(physical=physical))
    assert count == 2
    assert digest == candidate_hash_ordered(
        execution_id=request.execution_id,
        fence=request.fence,
        base_generation_id=request.expected_base_generation_id,
        root_key_hashes=iter(root_hashes),
    )
    assert session.events == ["resolve", "metadata", "payload", "resolve", "metadata"]
    for call in session.execute.call_args_list:
        if not isinstance(call.args[0], TextClause):
            assert "ci_snapshot_17.custom_import_build_family" in str(call.args[0])
            assert "mrf.custom_import_build_family" not in str(call.args[0])


@pytest.mark.asyncio
async def test_output_opening_coalesces_candidate_fingerprint_before_generation_write(monkeypatch):
    request = _request(page_row_limit=1)
    root_hashes = (b"a" * 32, b"b" * 32)
    families = [SimpleNamespace(complete_at=object(), root_key_sha256=digest) for digest in root_hashes]
    sync_session = _paged_session(
        monkeypatch,
        [
            [(digest, index, 1) for index, digest in enumerate(root_hashes, 1)],
            [(family, family.root_key_sha256, index, 2) for index, family in enumerate(families, 1)],
            [],
        ],
    )
    read_session = SimpleNamespace(run_sync=AsyncMock(side_effect=lambda read: read(sync_session)))
    write_session = SimpleNamespace(add=Mock())
    build = SimpleNamespace(generation_id=None, capture_bundle_id=9, base_generation_id=None)

    @asynccontextmanager
    async def reading(_factory):
        yield read_session

    @asynccontextmanager
    async def writing(_factory, supplied_request, build_id):
        assert supplied_request is request and build_id == 7
        assert sync_session.transaction_count == 2 and not sync_session.has_transaction
        yield write_session, build

    async def flush(session):
        assert session is write_session
        session.add.call_args.args[0].generation_id = 11

    monkeypatch.setattr(graph, "_session", reading)
    monkeypatch.setattr(graph, "_page_session", writing)
    monkeypatch.setattr(graph, "_source_digest", Mock(return_value=b"s" * 32))
    monkeypatch.setattr(graph, "_flush_page", flush)
    protected_call = AsyncMock(return_value="output")
    monkeypatch.setattr(graph, "_call", protected_call)
    assert await graph._open_output(object(), request, 7, 9) == 11
    generation = write_session.add.call_args.args[0]
    assert generation.root_count == generation.family_count == 2
    assert generation.candidate_sha256 == candidate_hash_ordered(
        execution_id=request.execution_id,
        fence=request.fence,
        base_generation_id=request.expected_base_generation_id,
        root_key_hashes=iter(root_hashes),
    )
    assert generation.source_bundle_sha256 == b"s" * 32
    graph._source_digest.assert_called_once_with(sync_session, request, 7, 9)
    protected_call.assert_awaited_once_with(write_session, "open_custom_import_build_output", (7, 11))
    assert sync_session.events == ["resolve", "metadata", "payload", "resolve", "metadata"]


@pytest.mark.parametrize("failure", ["incomplete", "byte_limit", "binding"])
def test_physical_candidate_fingerprint_preserves_fail_closed_reads(monkeypatch, failure):
    family = SimpleNamespace(complete_at=None if failure == "incomplete" else object(), root_key_sha256=b"a" * 32)
    session = _paged_session(
        monkeypatch,
        [[(family.root_key_sha256, 1, 2)], [(family, family.root_key_sha256, 1, 1)]],
        fail_resolution=2 if failure == "binding" else None,
    )
    request = _request(page_byte_limit=1 if failure == "byte_limit" else 2)
    message = {
        "incomplete": "requires completed families",
        "byte_limit": "one build record exceeds",
        "binding": "binding unavailable",
    }[failure]
    with pytest.raises(CandidateRunnerError, match=message):
        graph._candidate_digest(session, request, 7, bounds=graph._ReadPage(physical=True))
    assert not session.has_transaction
    assert session.transaction_count == (2 if failure == "binding" else 1)
    if failure == "byte_limit":
        assert session.events == ["resolve", "metadata"]


def test_one_query_row_closes_without_resolving_another_page(monkeypatch):
    session = _paged_session(monkeypatch, [[(1, 1)], [("first",)]])

    def query_factory(sync_session):
        assert sync_session.has_transaction
        return _plan_query(*graph._build_storage_models(sync_session, 7))

    assert graph._one_query_row(session, _request(), 7, query_factory) == ("first",)
    assert session.events == ["resolve", "metadata", "payload"]
    assert not session.has_transaction
