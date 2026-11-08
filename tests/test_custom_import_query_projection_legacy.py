# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Legacy projection work retains the caller's real transaction and authority."""

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy.dialects import postgresql

from process.custom_import import query_projection_legacy as legacy
from process.custom_import import runner_graph
from process.custom_import.execution import lease_token_sha256
from process.custom_import.query_projection_evidence import ProjectionDigest, QueryProjectionEvidence
from process.custom_import.runner_types import CandidateRunnerError
from tests.test_custom_import_query_projection_storage import _definition
from tests.test_custom_import_query_projection_values import _source


def _case():
    request = SimpleNamespace(
        definition=_definition(),
        dataset_id=1,
        definition_revision_id=2,
        schema_revision_id=3,
        execution_id=4,
        lease_token="synthetic-token",
    )
    generation = SimpleNamespace(
        generation_id=6,
        dataset_id=1,
        definition_revision_id=2,
        schema_revision_id=3,
        execution_id=4,
        capture_bundle_id=7,
        producing_fence=5,
        producing_token_sha256=lease_token_sha256(request.lease_token),
    )
    manifest = SimpleNamespace(
        family_id=17,
        generation_id=6,
        phase="root",
        after_entity_binding_id=0,
        root_count=0,
        child_count=0,
        index_count=0,
    )
    context = legacy._source_context(request, generation, manifest.family_id)
    return request, generation, manifest, context


def _comparison(expected=2, actual=2, *, differs=False):
    return SimpleNamespace(one=lambda: SimpleNamespace(expected_count=expected, actual_count=actual, differs=differs))


def _scalar(value):
    return SimpleNamespace(scalar_one=lambda: value)


@pytest.mark.asyncio
async def test_unconfigured_legacy_request_leaves_authority_and_storage_untouched(monkeypatch):
    request, generation, _, _ = _case()
    request.definition = _definition(enabled=())
    initialize = AsyncMock()
    monkeypatch.setattr(legacy, "_initialize", initialize)
    assert await legacy.persist_legacy_query_projection(object(), request, object(), generation) is None
    initialize.assert_not_awaited()


@pytest.mark.asyncio
async def test_real_window_and_generation_are_checked_before_resolution_or_projection_dml(monkeypatch):
    request, generation, _, _ = _case()
    authority_ids = (1, 2, 3, 4, 7, 5, generation.producing_token_sha256)
    monkeypatch.setattr(legacy, "_authority", AsyncMock(return_value=SimpleNamespace(authority=authority_ids)))
    call = AsyncMock(return_value=17)
    monkeypatch.setattr(legacy, "_call", call)
    generation.producing_fence += 1
    with pytest.raises(CandidateRunnerError, match="authority identity"):
        await legacy._initialize(object(), request, generation)
    call.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [None, "resolver", "parent", "phase", "root_count", "child_count", "after_entity_binding_id"],
)
async def test_legacy_initialization_uses_real_parent_and_rejects_partial_or_foreign_state(monkeypatch, failure):
    request, generation, manifest, _ = _case()
    authority_ids = (1, 2, 3, 4, 7, 5, generation.producing_token_sha256)
    monkeypatch.setattr(legacy, "_authority", AsyncMock(return_value=SimpleNamespace(authority=authority_ids)))
    monkeypatch.setattr(legacy, "verify_materialization_authority", AsyncMock())
    resolver = AsyncMock(return_value=True if failure == "resolver" else 17)
    parent, relation = SimpleNamespace(family_id=18 if failure == "parent" else 17), object()
    if failure in ("phase", "root_count", "child_count", "after_entity_binding_id"):
        setattr(manifest, failure, "child" if failure == "phase" else 1)
    bind_parent = AsyncMock(return_value=(parent, relation))
    initialize = AsyncMock(return_value=manifest)
    monkeypatch.setattr(legacy, "_call", resolver)
    monkeypatch.setattr(legacy, "projection_parent", bind_parent)
    monkeypatch.setattr(legacy, "initialize_projection_parent", initialize)
    session = object()
    if failure:
        with pytest.raises(CandidateRunnerError, match="binding is malformed|resolver differs|partial contexts"):
            await legacy._initialize(session, request, generation)
        if failure == "resolver":
            bind_parent.assert_not_awaited()
        if failure in ("resolver", "parent"):
            initialize.assert_not_awaited()
        return
    assert await legacy._initialize(session, request, generation) is manifest
    assert resolver.await_args.args[1:] == ("resolve_custom_import_legacy_generation_snapshot", (("bigint", 6),))
    assert bind_parent.await_args.kwargs == {"prepare": legacy._prepare}
    assert bind_parent.await_args.args[2]["producing_fence"] == generation.producing_fence
    initialize.assert_awaited_once_with(
        session, request.definition, parent, relation, prepare=legacy._prepare, flush=legacy._flush
    )


@pytest.mark.asyncio
async def test_native_owner_failure_precedes_context_writes(monkeypatch):
    request, generation, _, _ = _case()
    monkeypatch.setattr(legacy, "_initialize", AsyncMock(side_effect=CandidateRunnerError("existing snapshot owner")))
    execute = AsyncMock()
    monkeypatch.setattr(legacy, "_execute", execute)
    with pytest.raises(CandidateRunnerError, match="owner"):
        await legacy.persist_legacy_query_projection(object(), request, object(), generation)
    execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_legacy_scope_order_closes_writes_before_verification_and_completion(monkeypatch):
    request, generation, manifest, _ = _case()
    phases = []

    async def scope(session, context, retained, registry, *, child):
        phases.append((retained.phase, child))

    monkeypatch.setattr(legacy, "_initialize", AsyncMock(return_value=manifest))
    monkeypatch.setattr(legacy, "_write_scope", scope)
    monkeypatch.setattr(legacy, "_verify_scope", scope)
    execute, indexes, complete = AsyncMock(), AsyncMock(), AsyncMock(return_value="evidence")
    monkeypatch.setattr(legacy, "_execute", execute)
    monkeypatch.setattr(legacy, "validate_projection_indexes", indexes)
    monkeypatch.setattr(legacy, "_complete", complete)
    assert await legacy.persist_legacy_query_projection(object(), request, object(), generation) == "evidence"
    assert phases == [("root", False), ("child", True), ("verify_root", False), ("verify_child", True)]
    assert (
        execute.await_count
        == manifest.index_count
        == len(legacy.query_projection_index_statements(manifest.family_id, request.definition))
    )
    assert indexes.await_args.kwargs == {"prepare": legacy._prepare}
    complete.assert_awaited_once()


@pytest.mark.asyncio
async def test_each_legacy_statement_rechecks_the_same_real_authority(monkeypatch):
    authority, prepare = AsyncMock(), AsyncMock()
    monkeypatch.setattr(legacy, "_authority", authority)
    monkeypatch.setattr(legacy, "prepare_materialization_statement", prepare)
    session = SimpleNamespace(execute=AsyncMock(return_value="native"))
    statement = object()
    assert await legacy._execute(session, statement) == "native"
    authority.assert_awaited_once_with(session)
    prepare.assert_awaited_once_with(session)
    session.execute.assert_awaited_once_with(statement)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["root", "child", "collision"])
async def test_complete_source_ambiguity_is_rejected_before_any_reduction(monkeypatch, failure):
    _, _, _, context = _case()
    roots, children = _source(), _source(child=True)
    values_by_field = {"score": roots.c.metric, "weight": roots.c.weight}
    monkeypatch.setattr(legacy, "root_source", Mock(return_value=(roots, values_by_field)))
    child_source = Mock(return_value=(children, values_by_field))
    monkeypatch.setattr(legacy, "child_source", child_source)
    failure_position = {"root": 0, "child": 1, "collision": 2}[failure]
    execute = AsyncMock(
        side_effect=[SimpleNamespace(first=lambda: None)] * failure_position
        + [SimpleNamespace(first=lambda: ("ambiguous",))]
    )
    monkeypatch.setattr(legacy, "_execute", execute)
    registry = SimpleNamespace(child_collection_slots={context.definition.query.child_collection: 1})
    with pytest.raises(CandidateRunnerError, match="context is ambiguous|complete child key is ambiguous"):
        await legacy._validated_source(object(), context, registry, [1], child=True)
    assert execute.await_count == failure_position + 1
    if failure == "root":
        child_source.assert_not_called()


def test_legacy_key_pages_obey_the_existing_byte_budget(monkeypatch):
    monkeypatch.setattr(legacy, "_PAGE_BYTES", 100)
    assert legacy._accepted_keys([(1, 2024, 60), (2, 2024, 60)]) == [(1, 2024)]
    with pytest.raises(CandidateRunnerError, match="byte page"):
        legacy._accepted_keys([(1, 2024, 101)])


@pytest.mark.asyncio
async def test_one_entity_window_can_write_several_complete_context_pages(monkeypatch):
    _, _, manifest, context = _case()
    source_relation = _source()
    monkeypatch.setattr(
        legacy,
        "_validated_source",
        AsyncMock(
            return_value=(source_relation, {"score": source_relation.c.metric, "weight": source_relation.c.weight})
        ),
    )
    keys = AsyncMock(side_effect=[[(1, 2024), (1, 2025)], [(2, 2024)], []])
    monkeypatch.setattr(legacy, "_context_keys", keys)
    execute = AsyncMock(side_effect=[SimpleNamespace(rowcount=2), SimpleNamespace(rowcount=1)])
    monkeypatch.setattr(legacy, "_execute", execute)
    await legacy._write_window(
        object(), context, manifest, [1, 2], object(), legacy._scope_fields(context.definition, False), child=False
    )
    assert manifest.root_count == 3
    assert [call.kwargs["after"] for call in keys.await_args_list] == [None, (1, 2025), (2, 2024)]
    assert all(
        "INSERT INTO ci_snapshot_17.custom_import_derived_root"
        in str(call.args[1].compile(dialect=postgresql.dialect()))
        for call in execute.await_args_list
    )


@pytest.mark.asyncio
async def test_partial_context_insert_does_not_advance_accounting_or_page(monkeypatch):
    _, _, manifest, context = _case()
    source = _source()
    monkeypatch.setattr(
        legacy,
        "_validated_source",
        AsyncMock(return_value=(source, {"score": source.c.metric, "weight": source.c.weight})),
    )
    keys = AsyncMock(return_value=[(1, 2024), (1, 2025)])
    monkeypatch.setattr(legacy, "_context_keys", keys)
    monkeypatch.setattr(legacy, "_execute", AsyncMock(return_value=SimpleNamespace(rowcount=1)))
    with pytest.raises(CandidateRunnerError, match="context coverage differs"):
        await legacy._write_window(
            object(), context, manifest, [1], object(), legacy._scope_fields(context.definition, False), child=False
        )
    assert manifest.root_count == 0
    keys.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("configured", [False, True])
async def test_write_scope_skips_disabled_fields_and_visits_each_entity_window_once(monkeypatch, configured):
    request, generation, manifest, context = _case()
    if not configured:
        request.definition = _definition(enabled=("combined_score",))
        context = legacy._source_context(request, generation, manifest.family_id)
    entities = AsyncMock(side_effect=[[1, 2], [4], []])
    write = AsyncMock()
    monkeypatch.setattr(legacy, "_entity_window", entities)
    monkeypatch.setattr(legacy, "_write_window", write)
    await legacy._write_scope(object(), context, manifest, object(), child=True)
    assert [call.args[2] for call in entities.await_args_list] == ([0, 2, 4] if configured else [])
    assert [call.args[3] for call in write.await_args_list] == ([[1, 2], [4]] if configured else [])


@pytest.mark.asyncio
async def test_source_ambiguity_cannot_be_hidden_by_later_page_filtering(monkeypatch):
    _, _, manifest, context = _case()
    monkeypatch.setattr(legacy, "_validated_source", AsyncMock(side_effect=CandidateRunnerError("ambiguous")))
    keys, execute = AsyncMock(), AsyncMock()
    monkeypatch.setattr(legacy, "_context_keys", keys)
    monkeypatch.setattr(legacy, "_execute", execute)
    with pytest.raises(CandidateRunnerError, match="ambiguous"):
        await legacy._write_window(
            object(), context, manifest, [1], object(), legacy._scope_fields(context.definition, False), child=False
        )
    keys.assert_not_awaited()
    execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", (None, "value", "count", "foreign"))
async def test_legacy_verification_compares_values_and_all_actual_counts(monkeypatch, failure):
    _, _, manifest, context = _case()
    manifest.root_count = 2
    source_relation = _source()
    monkeypatch.setattr(legacy, "_entity_window", AsyncMock(side_effect=[[1, 2], []]))
    monkeypatch.setattr(
        legacy,
        "_validated_source",
        AsyncMock(
            return_value=(source_relation, {"score": source_relation.c.metric, "weight": source_relation.c.weight})
        ),
    )
    execute = AsyncMock(
        side_effect=[
            _comparison(2, 1 if failure == "count" else 2, differs=failure == "value"),
            _scalar(3 if failure == "foreign" else 2),
        ]
    )
    monkeypatch.setattr(legacy, "_execute", execute)
    if failure is not None:
        with pytest.raises(CandidateRunnerError, match="values differ|accounting differs"):
            await legacy._verify_scope(object(), context, manifest, object(), child=False)
    else:
        await legacy._verify_scope(object(), context, manifest, object(), child=False)
        assert manifest.verified_root_count == 2


@pytest.mark.asyncio
async def test_legacy_disabled_scope_still_rejects_actual_rows(monkeypatch):
    request, generation, manifest, _ = _case()
    request.definition = _definition(enabled=("combined_score",))
    context = legacy._source_context(request, generation, manifest.family_id)
    execute, entities = AsyncMock(return_value=_scalar(1)), AsyncMock()
    monkeypatch.setattr(legacy, "_execute", execute)
    monkeypatch.setattr(legacy, "_entity_window", entities)
    with pytest.raises(CandidateRunnerError, match="accounting differs"):
        await legacy._verify_scope(object(), context, manifest, object(), child=True)
    entities.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("child", (False, True))
async def test_legacy_digest_streams_configured_values_and_canonical_cursor(monkeypatch, child):
    _, _, manifest, context = _case()
    context_by_column = {"canonical_entity": "1000000010", "selected_value": -2024, "field_1": Decimal("1.5")}
    if child:
        context_by_column.update(canonical_child_key='["chosen"]', field_2=Decimal("2.5"), field_3=0)
    pages = AsyncMock(side_effect=[[context_by_column], []])
    monkeypatch.setattr(legacy, "_digest_rows", pages)
    digest = ProjectionDigest(context.definition)
    await legacy._digest_scope(object(), context, manifest, digest, child=child)
    evidence = digest.finish()
    assert (evidence.root_count, evidence.child_count) == ((0, 1) if child else (1, 0))
    assert pages.await_args.kwargs["after"] == (("1000000010", -2024, '["chosen"]') if child else ("1000000010", -2024))


@pytest.mark.asyncio
async def test_disabled_digest_scope_does_not_read_storage(monkeypatch):
    request, generation, manifest, _ = _case()
    request.definition = _definition(enabled=("combined_score",))
    context = legacy._source_context(request, generation, manifest.family_id)
    pages = AsyncMock()
    monkeypatch.setattr(legacy, "_digest_rows", pages)
    digest = ProjectionDigest(context.definition)
    await legacy._digest_scope(object(), context, manifest, digest, child=True)
    pages.assert_not_awaited()
    assert digest.finish().child_count == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "coverage", "evidence"])
async def test_legacy_completion_requires_exact_digest_before_final_authority_check(monkeypatch, failure):
    request, generation, manifest, context = _case()
    manifest.root_count = manifest.verified_root_count = 2 if failure == "coverage" else 1
    manifest.child_count = manifest.verified_child_count = 0
    evidence = QueryProjectionEvidence("custom-import/query-projection/v1", b"c" * 32, b"p" * 32, 1, 0)
    checkpoint = SimpleNamespace(
        chain_sha256=b"s" * 32, canonical_entity="1000000010", selected_value=2024, canonical_child_key=None
    )
    monkeypatch.setattr(
        legacy,
        "ProjectionDigest",
        Mock(return_value=SimpleNamespace(finish=lambda: evidence, checkpoint=lambda: checkpoint)),
    )
    monkeypatch.setattr(legacy, "_digest_scope", AsyncMock())
    flush = AsyncMock()
    monkeypatch.setattr(legacy, "_flush", flush)
    monkeypatch.setattr(legacy, "_prepare", AsyncMock())
    completed = Mock(return_value=None if failure == "evidence" else evidence)
    monkeypatch.setattr(legacy, "completed_projection_evidence", completed)
    authority = AsyncMock()
    monkeypatch.setattr(legacy, "verify_materialization_authority", authority)
    session = SimpleNamespace(refresh=AsyncMock())
    if failure:
        with pytest.raises(CandidateRunnerError, match="context coverage differs|completed evidence differs"):
            await legacy._complete(session, request, generation, context, manifest)
        authority.assert_not_awaited()
        if failure == "coverage":
            flush.assert_not_awaited()
            completed.assert_not_called()
        return
    assert await legacy._complete(session, request, generation, context, manifest) == evidence
    assert manifest.phase == "complete" and manifest.digest_chain_sha256 == checkpoint.chain_sha256
    assert (manifest.digested_root_count, manifest.digested_child_count) == (1, 0)
    assert manifest.digest_after_entity == "1000000010" and manifest.digest_after_selected_value == 2024
    authority.assert_awaited_once_with(session)


@pytest.mark.asyncio
async def test_runner_materializes_derived_queries_only_after_its_final_winners(monkeypatch):
    phases = []

    async def scalar(*args):
        phases.append("scalars")

    async def winners(*args):
        phases.append("winners")

    async def derived(*args):
        phases.append("derived")

    monkeypatch.setattr(runner_graph, "prepare_materialization_statement", AsyncMock())
    monkeypatch.setattr(runner_graph, "build_root_projections", Mock(return_value=[]))
    monkeypatch.setattr(runner_graph, "build_child_projections", Mock(return_value=[]))
    monkeypatch.setattr(runner_graph, "persist_scalar_projection_batches", scalar)
    monkeypatch.setattr(runner_graph, "persist_generation_winners", winners)
    monkeypatch.setattr(legacy, "persist_legacy_query_projection", derived)
    await runner_graph.persist_projections_and_winners(object(), object(), object(), object(), [])
    assert phases == ["scalars", "winners", "derived"]
