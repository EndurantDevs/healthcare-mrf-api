# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""The rooted and practitioner publication group stays private until all checks."""

from contextlib import asynccontextmanager, contextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from db.migration_provider_directory_dataset_candidates import TABLES
from process import provider_directory_generic_source_kind_candidate as candidate
from process.provider_directory_generic_candidate import GenericCandidateError


def _setup(monkeypatch):
    events = []
    connection = SimpleNamespace(
        fetchrow=AsyncMock(return_value={"status": "building", "is_current": False}),
        execute=AsyncMock(side_effect=lambda sql, *args: events.append(sql)),
    )
    publisher = SimpleNamespace(connection=connection, policy=SimpleNamespace(schema="source_schema"))

    @asynccontextmanager
    async def owner(database):
        yield publisher

    @contextmanager
    def scope(relations):
        assert len(relations) == 5
        events.append("private_scope")
        yield

    monkeypatch.setattr(candidate, "source_owner_transaction", owner)
    monkeypatch.setattr(candidate, "_close_grants", AsyncMock())
    monkeypatch.setattr(candidate, "_capture", AsyncMock(return_value={"native": "same"}))
    monkeypatch.setattr(candidate, "_indexes", AsyncMock(side_effect=lambda *args: events.append("index")))
    monkeypatch.setattr(candidate, "_provenance_indexes", AsyncMock(side_effect=lambda *args: events.append("index")))
    monkeypatch.setattr(candidate, "_validate_content", AsyncMock(side_effect=lambda *args: events.append("content")))
    monkeypatch.setattr(
        candidate, "_validate_original_checks", AsyncMock(side_effect=lambda *args: events.append("checks"))
    )
    monkeypatch.setattr(
        candidate, "_validate_relationships", AsyncMock(side_effect=lambda *args: events.append("check"))
    )
    directory = SimpleNamespace(_schema=lambda: "source_schema", _provider_directory_artifact_relation_scope=scope)
    return directory, connection, events


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["rooted", "practitioner"])
async def test_whole_group_copy_then_indexes_then_set_checks_then_attach(monkeypatch, kind):
    directory, connection, events = _setup(monkeypatch)
    connection.fetchrow.side_effect = [{"status": "building", "is_current": False}, ("0", 0)]
    async with candidate.native_kind_candidate(object(), directory, "source-dataset", kind=kind):
        events.append("copy")
    first_attach = next(index for index, event in enumerate(events) if "ATTACH PARTITION" in event)
    assert (
        events.index("copy")
        < events.index("index")
        < events.index("content")
        < events.index("checks")
        < events.index("check")
        < first_attach
    )
    assert events.count("index") == 5
    assert sum("ATTACH PARTITION" in event for event in events) == 5
    candidate._capture.assert_awaited()


@pytest.mark.asyncio
async def test_failed_relationship_validation_never_attaches_any_group_member(monkeypatch):
    directory, _, events = _setup(monkeypatch)
    monkeypatch.setattr(candidate, "_validate_relationships", AsyncMock(side_effect=ValueError("orphan")))
    with pytest.raises(ValueError, match="orphan"):
        async with candidate.native_kind_candidate(object(), directory, "source-dataset", kind="rooted"):
            events.append("copy")
    assert not any("ATTACH PARTITION" in event for event in events)


@pytest.mark.asyncio
async def test_failed_content_validation_never_checks_relationships_or_attaches(monkeypatch):
    directory, _, events = _setup(monkeypatch)
    monkeypatch.setattr(candidate, "_validate_content", AsyncMock(side_effect=GenericCandidateError("invalid content")))
    with pytest.raises(GenericCandidateError, match="invalid content"):
        async with candidate.native_kind_candidate(object(), directory, "source-dataset", kind="practitioner"):
            events.append("copy")
    candidate._validate_relationships.assert_not_awaited()
    candidate._validate_original_checks.assert_not_awaited()
    assert not any("ATTACH PARTITION" in event for event in events)


@pytest.mark.asyncio
async def test_failed_original_checks_never_check_relationships_or_attach(monkeypatch):
    directory, _, events = _setup(monkeypatch)
    monkeypatch.setattr(
        candidate, "_validate_original_checks", AsyncMock(side_effect=GenericCandidateError("invalid check"))
    )
    with pytest.raises(GenericCandidateError, match="invalid check"):
        async with candidate.native_kind_candidate(object(), directory, "source-dataset", kind="practitioner"):
            events.append("copy")
    candidate._validate_relationships.assert_not_awaited()
    assert not any("ATTACH PARTITION" in event for event in events)


def _original_check_setup(checks=(), *, keys=()):
    identity_by_root = {
        root: {"original_oid": index, "parent_oid": index + 100, "keys": list(keys)}
        for index, root in enumerate(TABLES, 1)
    }
    provenance = candidate._PROVENANCE["practitioner"]
    identity = identity_by_root[provenance]
    history_by_field = {
        "oid": identity["original_oid"],
        "relname": "pd_dataset_history_" + str(identity["original_oid"]),
        "nspname": "source_schema",
        "relkind": "r",
        "is_owned": True,
        "current_parent": identity["parent_oid"],
        "parents": [identity["parent_oid"]] if keys else [],
        "checks": list(checks),
    }
    plan_by_field = {"complete": True, "tables": identity_by_root}
    connection = SimpleNamespace(
        fetchval=AsyncMock(side_effect=[plan_by_field, False]), fetchrow=AsyncMock(return_value=history_by_field)
    )
    return (
        connection,
        SimpleNamespace(schema="source_schema"),
        {provenance: "private_candidate"},
        plan_by_field,
        history_by_field,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("keys", [(), ("older-dataset",)])
async def test_original_checks_use_exact_plan_heap_for_attached_or_empty_history(keys):
    connection, policy, relations, plan, history = _original_check_setup(
        [("content_check", "requested_npi > 0")], keys=keys
    )
    await candidate._validate_original_checks(connection, policy, relations, "dataset")
    assert connection.fetchval.await_args_list[0].args == (
        'SELECT plan FROM "source_schema"."pd_dataset_migration_plan" WHERE singleton',
    )
    connection.fetchrow.assert_awaited_once_with(
        candidate._ORIGINAL_CHECKS_SQL, history["oid"], '"source_schema"."' + next(iter(relations)) + '"'
    )
    assert history["oid"] == plan["tables"][next(iter(relations))]["original_oid"]
    assert connection.fetchval.await_args.args == (
        'SELECT EXISTS(SELECT FROM "source_schema"."private_candidate" WHERE dataset_id=$1 AND (NOT (requested_npi > 0)))',
        "dataset",
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("checks", [[], [("pd_dataset_history_bound", "false")]])
async def test_original_checks_allow_empty_predicates_and_skip_only_named_bound(checks):
    connection, policy, relations, _, _ = _original_check_setup(checks)
    await candidate._validate_original_checks(connection, policy, relations, "dataset")
    assert connection.fetchval.await_count == 1
    connection.fetchrow.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("check_name", ["content_check", "pd_dataset_history_bound_extra"])
async def test_original_checks_keep_false_data_predicates(check_name):
    connection, policy, relations, plan, _ = _original_check_setup(
        [("pd_dataset_history_bound", "false"), (check_name, "false")]
    )
    connection.fetchval.side_effect = [plan, True]
    with pytest.raises(GenericCandidateError, match="candidate_content_changed"):
        await candidate._validate_original_checks(connection, policy, relations, "dataset")
    assert connection.fetchval.await_args.args[0].endswith("AND (NOT (false)))")


@pytest.mark.asyncio
async def test_original_checks_preserve_nullable_check_semantics():
    connection, policy, relations, _, _ = _original_check_setup([("nullable_check", "NULL::boolean")])
    await candidate._validate_original_checks(connection, policy, relations, "dataset")
    statement = connection.fetchval.await_args.args[0]
    assert statement.endswith("AND (NOT (NULL::boolean)))")
    assert "IS NOT TRUE" not in statement


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", [True, None, 0])
async def test_original_checks_refuse_any_non_false_query_result(invalid):
    connection, policy, relations, plan, _ = _original_check_setup([("content_check", "true")])
    connection.fetchval.side_effect = [plan, invalid]
    with pytest.raises(GenericCandidateError, match="candidate_content_changed"):
        await candidate._validate_original_checks(connection, policy, relations, "dataset")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field_name,changed_identity",
    [
        ("oid", 999),
        ("relname", "replaced_history"),
        ("nspname", "other_schema"),
        ("relkind", "p"),
        ("is_owned", False),
        ("current_parent", 999),
        ("parents", [999]),
    ],
)
async def test_original_checks_refuse_changed_history_identity(field_name, changed_identity):
    connection, policy, relations, _, history = _original_check_setup()
    history[field_name] = changed_identity
    with pytest.raises(GenericCandidateError, match="legacy_history_identity_changed"):
        await candidate._validate_original_checks(connection, policy, relations, "dataset")
    assert connection.fetchval.await_count == 1


@pytest.mark.asyncio
async def test_original_checks_refuse_absent_heap_even_without_predicates():
    connection, policy, relations, _, _ = _original_check_setup()
    connection.fetchrow.return_value = None
    with pytest.raises(GenericCandidateError, match="legacy_history_identity_changed"):
        await candidate._validate_original_checks(connection, policy, relations, "dataset")
    assert connection.fetchval.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("plan_fault", ["missing_plan", "incomplete", "missing_identity", "missing_oid", "bool_oid"])
async def test_original_checks_refuse_absent_or_invalid_plan_identity(plan_fault):
    connection, policy, relations, plan, _ = _original_check_setup()
    provenance = next(iter(relations))
    if plan_fault == "missing_plan":
        plan = None
    elif plan_fault == "incomplete":
        plan["complete"] = False
    elif plan_fault == "missing_identity":
        del plan["tables"][provenance]
    elif plan_fault == "missing_oid":
        del plan["tables"][provenance]["original_oid"]
    else:
        plan["tables"][provenance]["original_oid"] = True
    connection.fetchval.side_effect = [plan]
    with pytest.raises(GenericCandidateError, match="legacy_(?:plan|history_identity)_changed"):
        await candidate._validate_original_checks(connection, policy, relations, "dataset")
    connection.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
async def test_sealed_parent_refuses_before_heap_creation(monkeypatch):
    directory, connection, events = _setup(monkeypatch)
    connection.fetchrow.return_value = {"status": "published", "is_current": True}
    with pytest.raises(GenericCandidateError, match="header_sealed"):
        async with candidate.native_kind_candidate(object(), directory, "source-dataset", kind="rooted"):
            pytest.fail("sealed parent admitted")
    assert events == []


@pytest.mark.asyncio
@pytest.mark.parametrize("timeout,timeout_ms", [("0", 0), ("5s", 5000), ("100ms", 100)])
async def test_attach_bounds_parent_locks_and_restores_caller_setting(monkeypatch, timeout, timeout_ms):
    """A caller's tighter deadline survives while unbounded or long waits are capped."""
    _, connection, events = _setup(monkeypatch)
    connection.fetchrow.return_value = (timeout, timeout_ms)
    policy = SimpleNamespace(schema="source_schema")
    await candidate._attach_partitions(connection, policy, {"parent": "candidate"}, "dataset")
    lock_index = next(index for index, event in enumerate(events) if event.startswith("LOCK TABLE"))
    assert ("SET LOCAL lock_timeout='1s'" in events[:lock_index]) is (timeout_ms == 0 or timeout_ms > 1000)
    assert events[-1] == "SELECT set_config('lock_timeout',$1,true)"
    assert connection.execute.await_args.args == (events[-1], timeout)


@pytest.mark.asyncio
async def test_attach_lock_failure_preserves_original_error_for_savepoint_rollback(monkeypatch):
    """Do not mask the rejected lock by issuing restoration SQL in its aborted savepoint."""
    _, connection, events = _setup(monkeypatch)
    connection.fetchrow.return_value = ("0", 0)

    async def fail_lock(statement, *args):
        events.append(statement)
        if statement.startswith("LOCK TABLE"):
            raise RuntimeError("lock unavailable")

    connection.execute.side_effect = fail_lock
    with pytest.raises(RuntimeError, match="lock unavailable"):
        await candidate._attach_partitions(
            connection, SimpleNamespace(schema="source_schema"), {"parent": "candidate"}, "dataset"
        )
    assert events == [
        "SET LOCAL lock_timeout='1s'",
        'LOCK TABLE "source_schema"."parent" IN SHARE UPDATE EXCLUSIVE MODE',
    ]
