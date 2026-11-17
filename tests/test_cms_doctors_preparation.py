# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Small preparation preconditions and shared source-validation contracts."""

import asyncio
import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import cms_doctors_preparation as preparation
from tests.test_cms_doctors_organizations import _mock_publication, _publication_context

native = importlib.import_module("process.cms_doctors")


@pytest.mark.asyncio
@pytest.mark.parametrize("entrypoint", ["_apply_cms_doctors_stage", "_apply_locked_cms_doctors_stage"])
async def test_publication_refuses_missing_owner_transaction(monkeypatch, entrypoint):
    database = SimpleNamespace(_transaction_binding=lambda: None, status=AsyncMock())
    monkeypatch.setattr(native, "db", database)
    with pytest.raises(RuntimeError, match="^cms_doctors_publication_requires_transaction$"):
        await getattr(native, entrypoint)(object(), "mrf", "synthetic")
    database.status.assert_not_awaited()


def test_uuid_length_stage_indexes_are_distinct_and_bounded():
    """PostgreSQL must not silently truncate several index names into one."""
    stage = "doctor_clinician_address_" + "a" * 32
    index_names = [
        native._stage_index_name(stage, suffix)
        for suffix in ("primary", "zip_provider_type", "provider_type", "address_key")
    ]
    assert len(set(index_names)) == len(index_names)
    assert all(len(name) <= 63 for name in index_names)


@pytest.mark.asyncio
@pytest.mark.parametrize("import_date", [None, 4, "", "bad-name", "a" * 33])
async def test_preparation_rejects_invalid_stage_identity(import_date):
    with pytest.raises(RuntimeError, match="requires_completed_import"):
        async with preparation.prepare_cms_doctors_generation({"import_date": import_date, "context": {"run": True}}):
            pytest.fail("invalid stage identity must not reach preparation")


@pytest.mark.asyncio
async def test_source_preparation_reuses_validation_without_publication(monkeypatch):
    calls = _mock_publication(monkeypatch, None)
    ctx = _publication_context()
    metrics = await native._prepare_cms_doctors_sources(ctx, object(), "mrf", 20000)
    assert calls == ["education", "group", "artifact", "binding", "sites", "cancel"]
    assert metrics["organization_groups"] == 101 and metrics["sites"] == 71
    native._publish_cms_doctors_stage.assert_not_awaited()
    native.mark_control_run.assert_not_awaited()


def test_prepared_stage_names_project_saved_tokens_without_mutating_them():
    from copy import deepcopy

    stages = tuple(
        (model.__tablename__, native.make_class(model, "synthetic").__tablename__) for model in preparation._models()
    )
    saved = SimpleNamespace(
        import_date="synthetic", stage_oids=tuple((*entry, {"opaque": index}) for index, entry in enumerate(stages))
    )
    before = deepcopy(vars(saved))
    assert preparation.prepared_stage_relations(saved) == stages
    assert vars(saved) == before
    assert (
        preparation.prepared_stage_relations(SimpleNamespace(import_date="synthetic", stage_relations=stages)) == stages
    )


@pytest.mark.parametrize("fault", ["missing", "duplicate", "target", "stage", "extra", "physical"])
def test_prepared_stage_names_require_complete_declared_model_family(fault):
    stages = tuple(
        (model.__tablename__, native.make_class(model, "synthetic").__tablename__) for model in preparation._models()
    )
    match fault:
        case "missing":
            stages = stages[:1]
        case "duplicate":
            stages = (stages[0],) * 3
        case "target":
            stages = (("other", stages[0][1]), *stages[1:])
        case "stage":
            stages = ((stages[0][0], "other"), *stages[1:])
        case "extra":
            stages = (*stages, ("other", "other"))
        case "physical":
            stages = tuple((*entry, 41) for entry in stages)
    with pytest.raises(RuntimeError, match="family_incomplete"):
        preparation.prepared_stage_relations(SimpleNamespace(import_date="synthetic", stage_relations=stages))


@pytest.mark.asyncio
@pytest.mark.parametrize("read_only", [False, True])
async def test_publication_locks_stages_with_candidate_permissions(monkeypatch, read_only):
    """Frozen candidates can be held with their read permissions."""
    session = object()
    stage = native.make_class(native.DoctorClinicianAddress, "synthetic")
    locks = AsyncMock()
    monkeypatch.setattr(native, "_lock_family", locks)
    await native._lock_cms_doctors_publication(session, stage, "mrf", "synthetic", stage_read_only=read_only)
    stages = tuple(sorted(native.make_class(model, "synthetic").__tablename__ for model in preparation._models()))
    locks.assert_awaited_once_with(
        session,
        "mrf",
        stages,
        "ACCESS SHARE" if read_only else "ACCESS EXCLUSIVE",
        nowait=True,
    )


@pytest.mark.asyncio
async def test_publication_rejects_nonboolean_frozen_stage_flag(monkeypatch):
    session = object()
    locks = AsyncMock()
    monkeypatch.setattr(native, "_lock_family", locks)
    with pytest.raises(RuntimeError, match="lock_scope_invalid"):
        await native._lock_cms_doctors_publication(
            session,
            native.make_class(native.DoctorClinicianAddress, "synthetic"),
            "mrf",
            "synthetic",
            stage_read_only="true",
        )
    locks.assert_not_awaited()


@pytest.mark.asyncio
async def test_cutover_wait_spends_one_shared_budget_across_live_tables(monkeypatch):
    session = SimpleNamespace(execute=AsyncMock())
    monkeypatch.setattr(native.db, "_transaction_binding", lambda: SimpleNamespace(session=session))
    await native._bound_cutover_wait(None)
    session.execute.assert_not_awaited()
    loop_time = asyncio.get_running_loop().time()
    await native._bound_cutover_wait(loop_time + 0.06)
    await native._bound_cutover_wait(loop_time - 1)
    values = [call.args[1]["value"] for call in session.execute.await_args_list]
    # Later live tables wait only for what remains of the 100 ms budget, never less than 1 ms.
    assert int(values[0].removesuffix("ms")) <= 60 and values[1] == "1ms"
