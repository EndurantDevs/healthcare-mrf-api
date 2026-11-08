# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""SOURCE bootstrap and exact replay use transaction-resolved snapshot aliases."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy.dialects.postgresql import dialect
from sqlalchemy.exc import ProgrammingError

import process.custom_import.build_source as staging
from process.custom_import.runner_types import CandidateRunnerError, LeaseAuthorityLost
from process.custom_import.storage_layout import snapshot_models
from tests.test_custom_import_build_source import _child, _root
from tests.test_custom_import_build_source_bulk import _bulk_context, _prepared_page, _transaction_attempts

BAD_BINDINGS = [None, True, False, 0, -1, 2**63, "23", 23.0]
HOT_MODELS = (
    staging.CustomImportBuildOccurrence,
    staging.CustomImportRootRecord,
    staging.CustomImportRootRevision,
    staging.CustomImportChildRevision,
    staging.CustomImportRejection,
)


def _scalar(value):
    return SimpleNamespace(scalar_one=lambda: value)


def _page_stubs(monkeypatch, context, stored_records=()):
    """Keep the real page context so failure and precommit ownership are tested."""
    session = SimpleNamespace(
        scalars=AsyncMock(return_value=SimpleNamespace(all=lambda: [])),
        execute=AsyncMock(return_value=SimpleNamespace(all=lambda: stored_records)),
        add_all=Mock(),
    )
    outcomes = []
    sessions = _transaction_attempts(session, outcomes)
    monkeypatch.setattr(staging, "_lock_page", AsyncMock())
    monkeypatch.setattr(staging, "_prepare_statement", AsyncMock())
    monkeypatch.setattr(staging, "_flush_page", AsyncMock())
    monkeypatch.setattr(staging, "lock_execution", AsyncMock())
    monkeypatch.setattr(staging, "lock_lease", AsyncMock())
    verified = AsyncMock(return_value=context.request.build_deadline_at.replace(year=2029))
    monkeypatch.setattr(staging, "verify_live_attempt", verified)
    monkeypatch.setattr(staging, "load_registry", AsyncMock(return_value=context.registry))
    return session, sessions, outcomes, verified


def _stored_rows(page, *, is_child=False):
    """Use ordinary mapped occurrence instances in the selected ORM row shape."""
    stored_rows = []
    for offset, prepared in enumerate(page.records):
        occurrence = staging.CustomImportBuildOccurrence(
            part_row_ordinal=page.first_row + offset,
            source_ordinal=page.first_source + offset,
            raw_parent_key_canonical=None if prepared.raw_key is None else prepared.raw_key[0],
            raw_parent_key_sha256=None if prepared.raw_key is None else prepared.raw_key[1],
        )
        rejection = prepared.rejection
        stored_rows.append(
            (
                occurrence,
                None if prepared.typed_key is None else prepared.typed_key[0],
                None if is_child else prepared.payload,
                prepared.payload if is_child else None,
                prepared.child_key,
                None if rejection is None else rejection.code,
                None if rejection is None else rejection.canonical_root_key,
                None if rejection is None else rejection.canonical_evidence,
            )
        )
    return stored_rows


def _compiled(statement):
    return str(statement.compile(dialect=dialect(), compile_kwargs={"literal_binds": True}))


async def test_bootstrap_binds_before_commit(monkeypatch):
    """Create profiles, begin the build and bind its snapshot in one transaction."""
    context = _bulk_context()
    session, sessions, outcomes, verified = _page_stubs(monkeypatch, context)

    async def bind(_session, name, _arguments):
        assert outcomes == []
        return _scalar(11 if name == "begin_custom_import_build" else 23)

    called = AsyncMock(side_effect=bind)
    monkeypatch.setattr(staging, "_call", called)
    assert await staging._begin_build(sessions, context.request) == (11, context.registry)
    assert [call.args[1] for call in called.await_args_list] == [
        "begin_custom_import_build",
        "resolve_custom_import_build_snapshot",
    ]
    assert all(call.args[0] is session for call in called.await_args_list)
    assert called.await_args.args[2] == (("bigint", 11),)
    session.add_all.assert_called_once()
    verified.assert_awaited_once()
    assert outcomes == ["commit"]


@pytest.mark.parametrize("family_id", BAD_BINDINGS)
async def test_bootstrap_rejects_bad_binding(monkeypatch, family_id):
    """A malformed result rolls back creation; canonical storage is no fallback."""
    context = _bulk_context()
    _, sessions, outcomes, verified = _page_stubs(monkeypatch, context)
    monkeypatch.setattr(staging, "_call", AsyncMock(side_effect=[_scalar(11), _scalar(family_id)]))
    with pytest.raises(CandidateRunnerError, match="snapshot binding is malformed"):
        await staging._begin_build(sessions, context.request)
    verified.assert_not_awaited()
    assert outcomes == ["rollback"]


def test_replay_compiles_exact_hot_aliases():
    """All five tables, joins, projection, range, order and limit route together."""
    context = _bulk_context()
    page = _prepared_page(context, part=2, row=3, ordinal=8)
    canonical = _compiled(staging._committed_prefix_statement(context, page))
    for family_id in (23, 24):
        rendered = _compiled(staging._committed_prefix_statement(context, page, models=snapshot_models(family_id)))
        for model in HOT_MODELS:
            assert f"ci_snapshot_{family_id}.{model.__tablename__}" in rendered
            assert f"{model.__table__.schema}.{model.__tablename__}" not in rendered
        assert rendered.replace(f"ci_snapshot_{family_id}.", HOT_MODELS[0].__table__.schema + ".") == canonical
    with pytest.raises(KeyError):
        staging._committed_prefix_statement(context, page, models={})


@pytest.mark.parametrize("stream", [0, 1])
async def test_replay_uses_resolved_snapshot(monkeypatch, stream):
    """Normal root/child and rejection rows retain the exact fingerprint contract."""
    context = _bulk_context(stream)
    source_values = [_root(), _root(score=None)] if stream == 0 else [_child(), _child(amount=None)]
    page = _prepared_page(context, source_values, part=2, row=3, ordinal=8)
    session, sessions, outcomes, verified = _page_stubs(monkeypatch, context, _stored_rows(page, is_child=stream == 1))

    async def bind(_session, name, _arguments):
        assert outcomes == []
        if name == "resolve_custom_import_build_snapshot":
            session.execute.assert_not_awaited()
            return _scalar(23)
        assert name == "check_custom_import_source_replay_homes"
        session.execute.assert_awaited_once()
        return _scalar(len(page.records))

    called = AsyncMock(side_effect=bind)
    statement = Mock(wraps=staging._committed_prefix_statement)
    monkeypatch.setattr(staging, "_call", called)
    monkeypatch.setattr(staging, "_committed_prefix_statement", statement)
    await staging._compare_committed_page(sessions, context, page)
    assert [call.args[1] for call in called.await_args_list] == [
        "resolve_custom_import_build_snapshot",
        "check_custom_import_source_replay_homes",
    ]
    assert called.await_args_list[0].args == (session, "resolve_custom_import_build_snapshot", (("bigint", 11),))
    assert called.await_args_list[1].args[2] == (
        ("bigint", 11),
        ("smallint", context.stream_slot),
        ("integer", 2),
        ("bigint", 3),
        ("integer", len(page.records)),
    )
    assert statement.call_args.kwargs["models"] is snapshot_models(23)
    assert "ci_snapshot_23.custom_import_build_occurrence" in _compiled(session.execute.await_args.args[0])
    verified.assert_awaited_once()
    assert outcomes == ["commit"]


async def test_replay_resolves_each_transaction(monkeypatch):
    """Cached ORM metadata never substitutes for a fresh native binding."""
    context = _bulk_context()
    page = _prepared_page(context)
    session, sessions, outcomes, _ = _page_stubs(monkeypatch, context, _stored_rows(page))
    called = AsyncMock(side_effect=[_scalar(23), _scalar(len(page.records)), _scalar(24), _scalar(len(page.records))])
    monkeypatch.setattr(staging, "_call", called)
    await staging._compare_committed_page(sessions, context, page)
    await staging._compare_committed_page(sessions, context, page)
    assert called.await_count == 4 and outcomes == ["commit", "commit"]
    for family_id, call in zip((23, 24), session.execute.await_args_list, strict=True):
        assert f"ci_snapshot_{family_id}.custom_import_build_occurrence" in _compiled(call.args[0])


@pytest.mark.parametrize("family_id", BAD_BINDINGS)
async def test_replay_rejects_bad_binding(monkeypatch, family_id):
    """Reject invalid authority output before compiling or querying hot data."""
    context = _bulk_context()
    session, sessions, outcomes, verified = _page_stubs(monkeypatch, context)
    compiled_models = Mock(wraps=staging.snapshot_models)
    monkeypatch.setattr(staging, "snapshot_models", compiled_models)
    monkeypatch.setattr(staging, "_call", AsyncMock(return_value=_scalar(family_id)))
    with pytest.raises(CandidateRunnerError, match="snapshot binding is malformed"):
        await staging._compare_committed_page(sessions, context, _prepared_page(context))
    compiled_models.assert_not_called()
    session.execute.assert_not_awaited()
    verified.assert_not_awaited()
    assert outcomes == ["rollback"]


@pytest.mark.parametrize("is_bootstrap", [False, True])
@pytest.mark.parametrize("failure_kind", ["sql", "cancel", "lease"])
async def test_resolver_errors_fail_closed(monkeypatch, is_bootstrap, failure_kind):
    """SQL errors, cancellation and lease loss retain the error and roll back."""
    context = _bulk_context()
    session, sessions, outcomes, verified = _page_stubs(monkeypatch, context)
    failure = {
        "sql": ProgrammingError("SELECT resolver", {}, RuntimeError("synthetic resolver failure")),
        "cancel": asyncio.CancelledError("synthetic resolver cancellation"),
        "lease": LeaseAuthorityLost("synthetic resolver lease loss"),
    }[failure_kind]
    called = AsyncMock(side_effect=[_scalar(11), failure] if is_bootstrap else failure)
    monkeypatch.setattr(staging, "_call", called)
    with pytest.raises(type(failure)) as captured:
        if is_bootstrap:
            await staging._begin_build(sessions, context.request)
        else:
            await staging._compare_committed_page(sessions, context, _prepared_page(context))
    assert captured.value is failure and outcomes == ["rollback"]
    session.execute.assert_not_awaited()
    verified.assert_not_awaited()


@pytest.mark.parametrize("defect", ["missing", "payload", "raw_key", "evidence", "ordinal"])
async def test_replay_difference_still_rolls_back(monkeypatch, defect):
    """Snapshot selection preserves coverage and complete ordered evidence checks."""
    context = _bulk_context()
    page = _prepared_page(context, [_root(), _root(score=None)])
    stored_rows = _stored_rows(page)
    if defect == "missing":
        stored_rows.pop()
    elif defect == "payload":
        stored_rows[0] = (*stored_rows[0][:2], "changed", *stored_rows[0][3:])
    elif defect == "evidence":
        stored_rows[1] = (*stored_rows[1][:7], "changed")
    elif defect == "raw_key":
        stored_rows[0][0].raw_parent_key_canonical = "changed"
    else:
        stored_rows[0][0].source_ordinal += 1
    _, sessions, outcomes, verified = _page_stubs(monkeypatch, context, stored_rows)
    monkeypatch.setattr(staging, "_call", AsyncMock(return_value=_scalar(23)))
    with pytest.raises(CandidateRunnerError, match="committed source prefix"):
        await staging._compare_committed_page(sessions, context, page)
    verified.assert_not_awaited()
    assert outcomes == ["rollback"]


async def test_fresh_precommit_loss_rolls_back(monkeypatch):
    """An earlier valid resolver cannot replace the page's final authority check."""
    context = _bulk_context()
    page = _prepared_page(context)
    session, sessions, outcomes, verified = _page_stubs(monkeypatch, context, _stored_rows(page))
    monkeypatch.setattr(staging, "_call", AsyncMock(side_effect=[_scalar(23), _scalar(len(page.records))]))
    verified.side_effect = LeaseAuthorityLost("synthetic precommit loss")
    with pytest.raises(LeaseAuthorityLost, match="synthetic precommit loss"):
        await staging._compare_committed_page(sessions, context, page)
    session.execute.assert_awaited_once()
    assert outcomes == ["rollback"]
