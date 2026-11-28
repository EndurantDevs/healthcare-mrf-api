# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Only an actually published Florida generation crosses the native boundary."""

import importlib
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from process import florida_projection_archive as archive
from tests.source_profile_archive_support import florida_run as _native_run

florida = importlib.import_module("process.florida_mqa_profile")


def _run(publication="atomic_table_swap"):
    return {
        "run_id": "a" * 32,
        "source_key": "florida-mqa",
        "jurisdiction": "FL",
        "schema_version": "provider-profile/v1",
        "status": "completed",
        "started_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "finished_at": datetime(2026, 1, 2, tzinfo=timezone.utc),
        "error": None,
        "source_manifest": {"sources": ["profile_master"]},
        "metrics": {
            "published_providers": 1,
            "publication": {"publication": publication, "published_rows": 1},
        },
    }


def test_published_and_partial_flows_are_distinct():
    archive._validate_run(_run())
    for changed in (
        {"metrics": {"published_providers": 0, "publication": {"publication": "skipped_partial", "published_rows": 0}}},
        {"status": "validating"},
        {"source_key": "other"},
    ):
        with pytest.raises(archive.FloridaProjectionArchiveError):
            archive._validate_run({**_run(), **changed})


@pytest.mark.asyncio
async def test_ordinary_candidate_closes_default_grants_before_sealing(monkeypatch):
    events = []
    session = SimpleNamespace(scalar=AsyncMock(return_value=11))

    async def clear(*_args):
        events.append("clear")

    async def seal(*_args):
        events.append("seal")
        return {"relation_oid": 10, "owner_oid": 11, "table_name": "candidate"}

    monkeypatch.setattr(archive, "_clear_stage_grants", clear)
    monkeypatch.setattr(archive, "_projection_candidate_seal", seal)
    assert await archive.isolate_ordinary_projection(session, "mrf", "candidate") == {
        "relation_oid": 10,
        "owner_oid": 11,
        "table_name": "candidate",
    }
    assert events == ["clear", "seal"]


@pytest.mark.asyncio
async def test_ordinary_publication_copies_locked_access_without_rechecking_candidate(monkeypatch):
    seal_by_field = {"relation_oid": 10, "owner_oid": 11, "table_name": "candidate"}
    session = object()
    lock, restore, seal = AsyncMock(), AsyncMock(), AsyncMock()
    monkeypatch.setattr(archive, "_projection_candidate_seal", seal)
    monkeypatch.setattr(archive.native, "_lock_family", lock)
    monkeypatch.setattr(archive.native, "_relation_oid", AsyncMock(return_value=20))
    monkeypatch.setattr(archive, "_restore_projection_access", restore)
    await archive.preserve_ordinary_projection_access(session, "mrf", seal_by_field)
    seal.assert_not_awaited()
    assert [call.args[2:] for call in lock.await_args_list] == [
        (("candidate",), "SHARE"),
        ((archive.PROJECTION,), "ACCESS EXCLUSIVE"),
    ]
    restore.assert_awaited_once_with(session, "mrf", "candidate", 20)


@pytest.mark.asyncio
@pytest.mark.parametrize(("relkind", "accepted"), [("r", True), ("v", False)])
async def test_candidate_seal_checks_shape_but_not_owner_or_grants(monkeypatch, relkind, accepted):
    result = SimpleNamespace(mappings=lambda: SimpleNamespace(one=lambda: {"relkind": relkind, "relpersistence": "p"}))
    session = SimpleNamespace(execute=AsyncMock(return_value=result), scalar=AsyncMock())
    monkeypatch.setattr(archive.native, "_relation_oid", AsyncMock(return_value=10))
    if accepted:
        assert await archive._projection_candidate_seal(session, "mrf", "candidate", 11) == {
            "relation_oid": 10,
            "owner_oid": 11,
            "table_name": "candidate",
        }
    else:
        with pytest.raises(archive.FloridaProjectionArchiveError, match="shape differs"):
            await archive._projection_candidate_seal(session, "mrf", "candidate", 11)
    assert "relowner" not in str(session.execute.await_args.args[0])
    session.scalar.assert_not_awaited()


@pytest.mark.parametrize("fault", [None, "partial", "sources", "header", "schema", "quarantine", "policy", "failed"])
def test_native_fl_policy_remains_complete_and_fail_closed(fault):
    run = deepcopy(_native_run())
    source = run["source_manifest"]["sources"][0]
    match fault:
        case "partial":
            run["source_manifest"]["partial_publish_reasons"] = ["max_providers:1"]
        case "sources":
            run["source_manifest"]["sources"].pop()
        case "header":
            run["metrics"]["source_metrics"][source]["header_sha256"] = None
        case "schema":
            run["metrics"]["source_metrics"][source]["schema_complete"] = False
        case "quarantine":
            run["metrics"]["source_metrics"][source]["quarantined_rows"] = 1
        case "policy":
            run["source_manifest"]["publication_guard"]["min_publish_ratio"] = float("nan")
        case "failed":
            run["status"] = "failed"
    if fault:
        with pytest.raises(archive.FloridaProjectionArchiveError):
            archive.validate_native_run(run)
    else:
        archive.validate_native_run(run)


@pytest.mark.asyncio
async def test_native_five_model_copy_reuses_one_bounded_copier(monkeypatch):
    native = archive.native
    copy = AsyncMock(side_effect=[90, 70, 50, 20, 0])
    monkeypatch.setattr(native, "_copy_source_projection", copy)
    spec = archive.profiles.source_spec(archive.IMPORTER_ID)
    assert (
        await native._copy_model_run_scope(
            object(),
            spec,
            source_schema="source",
            target_schema="target",
            target_names=archive.TABLES,
            run_scope=(("run_id", "run_id", "run_id", "run_id", "generation_id"), ("a" * 32,)),
            source_copy=native.ReferenceFamilySourceCopy(AsyncMock(), 100, 30),
            deadline=1000,
        )
        == 0
    )
    assert [call.args[-2] for call in copy.await_args_list] == [100, 90, 70, 50, 20]
    for index, call in enumerate(copy.await_args_list):
        assert call.args[4] == archive.TABLES[index]
        assert 'WHERE "generation_id"' in call.args[2] if index == 4 else 'WHERE "run_id"' in call.args[2]
        assert "npi" not in call.args[2].split(" WHERE ")[1]


def _native_deadline_case(monkeypatch, precheck_seconds):
    """Keep the real evidence and projection copy chain on one deterministic clock."""
    native, profiles = archive.native, archive.profiles
    from process import florida_projection_named_source as named

    clock = SimpleNamespace(now=0.0)
    clock_api = SimpleNamespace(get_running_loop=lambda: SimpleNamespace(time=lambda: clock.now))
    monkeypatch.setattr(native, "asyncio", clock_api)
    monkeypatch.setattr(profiles, "asyncio", clock_api)

    async def copy_rows(*_args, **_kwargs):
        clock.now += 1
        return 10

    async def precheck(*_args):
        clock.now += precheck_seconds

    copier = AsyncMock(side_effect=copy_rows)
    monkeypatch.setattr(profiles, "_require_stage_topology", AsyncMock())
    monkeypatch.setattr(profiles, "_validate_publication_sets", AsyncMock())
    monkeypatch.setattr(profiles, "_run", AsyncMock(return_value=None))
    monkeypatch.setattr(named, "require_native_publication_order", precheck)
    monkeypatch.setattr(named, "_publication_lock", AsyncMock())
    monkeypatch.setattr(native, "seal_model_family_storage", AsyncMock())
    monkeypatch.setattr(
        named, "named_publication_pointer", AsyncMock(return_value={"current_run_id": None, "previous_run_id": None})
    )
    monkeypatch.setattr(native, "_relation_oid", AsyncMock(return_value=None))
    monkeypatch.setattr(native, "_create_model_indexes", AsyncMock())
    monkeypatch.setattr(native, "_is_model_table_equal", AsyncMock(return_value=True))
    monkeypatch.setattr(archive, "_clear_stage_grants", AsyncMock())
    pin_id = uuid4()
    monkeypatch.setattr(
        named,
        "freeze_named_projection",
        AsyncMock(return_value={"schema": "destination", "table_name": archive._cutover_name(pin_id)}),
    )
    prepared = SimpleNamespace(
        manifest={"importer_id": archive.IMPORTER_ID, "run_id": "a" * 32, "run_ids": ["a" * 32]},
        ownership=SimpleNamespace(
            schema_name="stage",
            relation_names=profiles.source_spec(archive.IMPORTER_ID, publication=True).table_names,
        ),
    )
    session = SimpleNamespace(
        execute=AsyncMock(),
        scalar=AsyncMock(
            side_effect=lambda query, *_args: "protected_owner" if "rolname" in str(query) else True,
        ),
    )
    return session, prepared, pin_id, copier


@pytest.mark.asyncio
@pytest.mark.parametrize("precheck_seconds", [4, 7])
async def test_projection_copy_keeps_original_deadline_after_evidence_and_prechecks(monkeypatch, precheck_seconds):
    session, prepared, pin_id, copier = _native_deadline_case(monkeypatch, precheck_seconds)
    projection_by_field = {
        "expected": {
            "current_run_id": None,
            "previous_run_id": None,
            "current_relation_oid": 1,
            "previous_relation_oid": None,
        },
        "source_copy": archive.native.ReferenceFamilySourceCopy(copier, 100, 10),
    }
    preparation = archive.profiles._prepare_adoption_storage(
        session,
        prepared,
        "destination",
        pin_id,
        projection_by_field,
    )
    if precheck_seconds == 7:
        with pytest.raises(TimeoutError, match="COPY deadline expired"):
            await preparation
    else:
        await preparation
    expected_timeouts = [10, 9, 8, 7] + ([2] if precheck_seconds == 4 else [])
    assert [call.kwargs["timeout"] for call in copier.await_args_list] == expected_timeouts
    assert [call.kwargs["max_bytes"] for call in copier.await_args_list] == [100, 90, 80, 70, 60][
        : len(expected_timeouts)
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", [None, "newer", "header", "volume"])
async def test_native_transfer_reuses_ordinary_destination_guards(monkeypatch, fault):
    candidate, current = _native_run("b" * 32), _native_run("a" * 32)
    current["started_at"] -= timedelta(days=1)
    if fault == "newer":
        current["started_at"] += timedelta(days=2)
    elif fault == "header":
        current["metrics"]["source_metrics"][florida.DEFAULT_SOURCE_KEYS[0]]["header_sha256"] = "c" * 64
    elif fault == "volume":
        current["metrics"]["published_providers"] = 100
    monkeypatch.setattr(archive, "_run", AsyncMock(side_effect=[candidate, current]))
    prepared = SimpleNamespace(manifest={"run_id": candidate["run_id"]}, ownership=SimpleNamespace(schema_name="stage"))
    if fault:
        with pytest.raises(archive.FloridaProjectionArchiveError):
            await archive.require_native_publication_order(
                object(), prepared, "destination", {"current_run_id": current["run_id"]}
            )
    else:
        await archive.require_native_publication_order(
            object(), prepared, "destination", {"current_run_id": current["run_id"]}
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", [None, "active", "oid"])
async def test_projection_admission_preserves_cancel_and_exact_oid_fences(monkeypatch, fault):
    profiles = archive.profiles
    pointer_by_field = {
        "current_run_id": "a" * 32,
        "previous_run_id": None,
        "current_relation_oid": 10,
        "previous_relation_oid": None,
    }
    expected = {**pointer_by_field, "current_relation_oid": 11} if fault == "oid" else pointer_by_field
    monkeypatch.setattr(profiles, "_source_lock", AsyncMock())
    monkeypatch.setattr(profiles.native, "_lock_family", AsyncMock())
    monkeypatch.setattr(profiles, "_pointer", AsyncMock(return_value=pointer_by_field))
    session = SimpleNamespace(scalar=AsyncMock(return_value=fault == "active"))
    args = (session, {"importer_id": archive.IMPORTER_ID, "source_key": "florida-mqa"}, "destination", "a" * 32)
    if fault:
        with pytest.raises(profiles.SourceProfileArchiveError):
            await profiles._admission(*args, projection={"expected": expected})
    else:
        await profiles._admission(*args, projection={"expected": expected})


@pytest.mark.asyncio
async def test_cutover_stage_drops_role_grants_and_replays_only_non_owner_grants(monkeypatch):
    statements = []
    rows = iter(
        [
            [{"grantee": 0, "rolname": None}, {"grantee": 7, "rolname": "owner"}, {"grantee": 9, "rolname": "reader"}],
            [
                {"grantee": 7, "privilege_type": "SELECT", "is_grantable": False, "rolname": "owner"},
                {"grantee": 9, "privilege_type": "SELECT", "is_grantable": True, "rolname": "reader"},
            ],
        ]
    )

    async def execute(statement, parameters=None):
        statements.append(str(statement))
        return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: next(rows)))

    monkeypatch.setattr(archive.native, "_relation_oid", AsyncMock(return_value=11))
    session = SimpleNamespace(execute=execute)
    await archive._clear_stage_grants(session, "mrf", "projection_stage", 7)
    await archive._replay_live_grants(session, '"mrf"."projection_stage"', 11, 7)
    assert 'REVOKE ALL ON TABLE "mrf"."projection_stage" FROM PUBLIC' in statements
    assert 'REVOKE ALL ON TABLE "mrf"."projection_stage" FROM "reader"' in statements
    assert 'GRANT SELECT ON TABLE "mrf"."projection_stage" TO "reader" WITH GRANT OPTION' in statements
    assert not any('"owner"' in statement for statement in statements)
