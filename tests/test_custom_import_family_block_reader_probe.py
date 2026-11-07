# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Independent Reader ownership, actual native identities and cold/replay callers."""

import time
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.dialects.postgresql import dialect

from process.custom_import import family_block_reader as readers
from process.custom_import import family_block_store as store
from process.custom_import import family_shard_completion as completion
from process.custom_import import family_shard_owner_job as owners
from process.custom_import.build_source import _WINDOW, _PageWindow
from process.custom_import.family_block_binding import FamilyBlockUnavailable
from tests.test_custom_import_family_block_reader import _binding, _document, _result
from tests.test_custom_import_family_shard_completion import _case
from tests.test_custom_import_family_shard_owner_job import _attempt
from tests.test_custom_import_family_shard_owner_job import pipeline as pipeline


@pytest.mark.parametrize("problem", [None, "not_owned", "wrong_session", "unverified", "wrong_oid"])
async def test_probe_requires_real_reader_ownership_and_restores_local_window(monkeypatch, problem):
    sentinel = object()
    session = SimpleNamespace(
        info={
            "api_reader_verified": problem != "unverified",
            "api_reader_identity": {"role_oid": 89 if problem == "wrong_oid" else 88},
            _WINDOW: sentinel,
        }
    )
    arguments = []

    @asynccontextmanager
    async def factory(**options):
        arguments.append(options)
        yield session

    database = SimpleNamespace(reader_session=factory, has_reader_session=lambda: problem != "not_owned")
    activation = readers.ReaderActivation(readers.activation_profile(_document()), database)
    monkeypatch.setattr(readers, "current_session", lambda: object() if problem == "wrong_session" else session)
    checked = AsyncMock()
    monkeypatch.setattr(readers.ReaderActivation, "_probe_snapshot", checked)
    owner = SimpleNamespace(info={_WINDOW: _PageWindow(time.monotonic() + 10, 1000)})
    if problem:
        with pytest.raises(FamilyBlockUnavailable):
            await activation._probe(owner, _binding(), object(), time.monotonic() + 10)
        checked.assert_not_awaited()
    else:
        await activation._probe(owner, _binding(), object(), time.monotonic() + 10)
        checked.assert_awaited_once()
    assert arguments == [{"independent": True}] and session.info[_WINDOW] is sentinel


@pytest.mark.parametrize("problem", [None, "wrong_login", "wrong_database", "writable", "wrong_candidate", "excluded"])
async def test_native_reader_scope_is_independent_of_session_flags(monkeypatch, problem):
    activation = readers.ReaderActivation(
        readers.activation_profile(_document()), SimpleNamespace(reader_session=lambda: None)
    )
    binding = _binding()
    binding.layout.definition, binding.layout.collection_slots = object(), {"details": 1}
    actual = _binding()
    if problem == "wrong_candidate":
        actual.oids_by_role = dict(actual.oids_by_role, query_root=999)
    statements = []

    async def execute(_session, statement, parameters=None):
        statements.append(statement)
        if "current_user,session_user" in statement:
            login = (
                "writer" if problem == "wrong_login" else "synthetic_reader",
                "synthetic_reader",
                88,
                "repeatable read",
                "off" if problem == "writable" else "on",
                22 if problem == "wrong_database" else 21,
            )
            return SimpleNamespace(one=lambda: login)
        if statement.startswith("SELECT oid::bigint"):
            return _result(scalar=21)
        if "bool_and" in statement:
            assert len(parameters["relations"]) == 5
            return _result(scalar=problem != "excluded")
        return _result()

    monkeypatch.setattr(store, "_control_schema", AsyncMock(return_value=(dialect().identifier_preparer, "control")))
    monkeypatch.setattr(store, "_execute", execute)
    monkeypatch.setattr(readers, "resolve_generation_snapshot_binding", AsyncMock(return_value=object()))
    monkeypatch.setattr(readers, "resolve_pinned_family_block_binding", AsyncMock(return_value=actual))
    if problem:
        with pytest.raises(FamilyBlockUnavailable):
            await activation._probe_snapshot(object(), object(), binding, object())
    else:
        await activation._probe_snapshot(object(), object(), binding, object())
        assert len([sql for sql in statements if sql.startswith("SELECT 1 FROM")]) == 11


@pytest.mark.parametrize("permitted", [True, False])
async def test_effective_inherited_public_or_extra_object_rights_refuse_before_usage(monkeypatch, permitted):
    async def execute(_session, sql, parameters):
        assert len(parameters["serving"]) == 11 and len(parameters["control"]) == 2
        assert "has_table_privilege(CAST(:role AS oid)" in sql
        assert "has_sequence_privilege" in sql and "has_function_privilege" in sql
        assert "has_any_column_privilege" in sql and "aclexplode(a.attacl)" in sql
        assert "c.oid<>ALL" in sql and "INSERT,UPDATE,DELETE" in sql
        return _result(scalar=permitted)

    monkeypatch.setattr(store, "_control_schema", AsyncMock(return_value=(dialect().identifier_preparer, "control")))
    monkeypatch.setattr(store, "_execute", execute)
    if permitted:
        await readers._effective_rights(object(), _binding(), readers.activation_profile(_document()))
    else:
        with pytest.raises(FamilyBlockUnavailable, match="effective"):
            await readers._effective_rights(object(), _binding(), readers.activation_profile(_document()))


async def test_warm_completion_installs_only_after_full_validation_and_logging(monkeypatch, tmp_path):
    factory, context, proof, work, calls = _case(monkeypatch, tmp_path)

    async def install(actual_factory, actual_context, **arguments):
        assert actual_factory is factory and actual_context is context and arguments == {"identity": proof.identity}
        assert calls[-1] == "logged"
        calls.append("reader-install")

    activation = SimpleNamespace(install=AsyncMock(side_effect=install), verify_candidate=AsyncMock())
    assert (
        await completion.complete_closed_shard_build(
            factory, context, object(), work=work, limits=object(), reader_activation=activation
        )
        == "sealed"
    )
    assert calls.index("logged") < calls.index("reader-install") < calls.index("record") < calls.index("freeze")
    activation.verify_candidate.assert_not_awaited()


async def test_verified_replay_checks_existing_acls_without_installing_or_revalidating(monkeypatch, tmp_path):
    factory, context, _proof, work, calls = _case(monkeypatch, tmp_path)
    monkeypatch.setattr(completion, "_snapshot", AsyncMock(return_value=SimpleNamespace(phase="verified")))
    activation = SimpleNamespace(install=AsyncMock(), verify_candidate=AsyncMock())
    assert (
        await completion.complete_closed_shard_build(
            factory, context, None, work=work, limits=None, reader_activation=activation
        )
        == "sealed"
    )
    activation.verify_candidate.assert_awaited_once_with(factory, context)
    activation.install.assert_not_awaited()
    assert calls == ["freeze", "seal"]


async def test_sealed_replay_performs_no_acl_installation_in_completion(monkeypatch, tmp_path):
    factory, context, _proof, work, calls = _case(monkeypatch, tmp_path)
    monkeypatch.setattr(completion.build_output, "_replay", AsyncMock(return_value="sealed-output"))
    activation = SimpleNamespace(install=AsyncMock(), verify_candidate=AsyncMock())
    assert (
        await completion.complete_closed_shard_build(
            factory, context, None, work=work, limits=None, reader_activation=activation
        )
        == "sealed-output"
    )
    activation.install.assert_not_awaited()
    activation.verify_candidate.assert_not_awaited()
    assert not calls


@pytest.mark.parametrize("sealed_replay", [False, True])
async def test_owner_warm_and_sealed_replay_keep_enabled_reader_binding(pipeline, monkeypatch, sealed_replay):
    job, boundary = pipeline
    activation = readers.ReaderActivation(
        readers.activation_profile(_document()), SimpleNamespace(reader_session=lambda: None)
    )
    job.reader_activation = activation
    pointer = AsyncMock(return_value=boundary.publication)
    monkeypatch.setattr(owners, "_activate", pointer)
    if sealed_replay:
        boundary.failure = "output"
        with pytest.raises(ConnectionError, match="output"):
            await owners.advance_owner_shard_job(lambda: None, job, attempt_root=_attempt(job))
        pointer.assert_not_awaited()
        boundary.replayed = boundary.result
    await owners.advance_owner_shard_job(lambda: None, job, attempt_root=_attempt(job))
    pointer.assert_awaited_once()
    assert pointer.await_args.args[1:] == (job.request, job.output)
    assert pointer.await_args.kwargs == {"reader_activation": activation, "reader_layout": job.layout}
