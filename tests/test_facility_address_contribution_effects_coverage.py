# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic guard coverage for facility contribution effects."""

from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from process import facility_address_contribution_effects as effects
from process import facility_address_contribution_merge as merge


class _Result:
    def __init__(self, *, mapping=None, rows=()):
        self.mapping = mapping
        self.rows = rows

    def mappings(self):
        return self

    def one(self):
        return self.mapping

    def all(self):
        return self.rows

    def scalars(self):
        return self


def _session(*, scalar_values=None, execute_values=None):
    return SimpleNamespace(
        in_transaction=lambda: True,
        scalar=AsyncMock() if scalar_values is None else AsyncMock(side_effect=scalar_values),
        execute=AsyncMock() if execute_values is None else AsyncMock(side_effect=execute_values),
    )


def _relation_record(**overrides):
    return {
        "oid": 7,
        "relkind": "r",
        "relpersistence": "p",
        "relrowsecurity": False,
        "relforcerowsecurity": False,
        "hooks": False,
        "rules": False,
        "inheritance": False,
        "generated": False,
    } | overrides


def _receipt(**overrides):
    return {
        "contract": effects.CONTRACT,
        "operation_id": str(uuid4()),
        "archive_oid": 7,
        "schema": "mrf",
        "stage_schema": "facility_stage",
        "stage_schema_oid": 8,
        "contribution_oid": 9,
        "alias": None,
        "row_count": 0,
    } | overrides


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["SHARE", "SHARE ROW EXCLUSIVE"])
async def test_lock_archive_binds_expected_relation_and_timeout_mode(monkeypatch, mode):
    monkeypatch.setattr(effects, "archive_table_name", lambda: "address_archive_v2")
    monkeypatch.setattr(effects, "_archive_lock_key", lambda *_args: "synthetic-lock")
    monkeypatch.setattr(effects, "alias_advisory_xact_lock_sql", lambda: "SELECT 1")
    monkeypatch.setattr(effects, "_qtable", lambda schema, table: f'"{schema}"."{table}"')
    session = _session(execute_values=[None] * 6 + [_Result(mapping=_relation_record())])

    archive, oid = await effects._lock_archive(session, "mrf", mode)

    assert (archive, oid) == ('"mrf"."address_archive_v2"', 7)
    calls = session.execute.await_args_list
    assert calls[1].args[1] == {"setting": "lock_timeout", "ceiling": "500ms"}
    assert calls[2].args[1] == {
        "setting": "statement_timeout",
        "ceiling": "1800s" if mode == "SHARE" else "2500ms",
    }
    assert calls[5].args[0].text == f'LOCK TABLE ONLY "mrf"."address_archive_v2" IN {mode} MODE'
    assert calls[6].args[1] == {"relation": archive}


@pytest.mark.asyncio
async def test_lock_archive_rejects_security_hooks(monkeypatch):
    monkeypatch.setattr(effects, "archive_table_name", lambda: "address_archive_v2")
    monkeypatch.setattr(effects, "_archive_lock_key", lambda *_args: "synthetic-lock")
    monkeypatch.setattr(effects, "alias_advisory_xact_lock_sql", lambda: "SELECT 1")
    monkeypatch.setattr(effects, "_qtable", lambda schema, table: f'"{schema}"."{table}"')
    session = _session(execute_values=[None] * 6 + [_Result(mapping=_relation_record(hooks=True))])

    with pytest.raises(RuntimeError, match="security hooks"):
        await effects._lock_archive(session, "mrf", "SHARE")


@pytest.mark.asyncio
async def test_prepare_effects_captures_enabled_rows_and_skips_disabled_or_duplicate_plans(monkeypatch):
    lock_archive = AsyncMock(return_value=("archive", 7))
    project = AsyncMock()
    monkeypatch.setattr(effects, "_lock_archive", lock_archive)
    monkeypatch.setattr(effects, "project_observations", project)
    monkeypatch.setattr(
        effects, "validate_observations", AsyncMock(return_value=({"enabled": True}, {"bound": "alias"}))
    )
    session = _session(
        scalar_values=[False, 2],
        execute_values=[None, _Result(mapping=(8, 9)), None],
    )

    receipt = await effects.prepare_facility_address_effects(
        session,
        operation_id=uuid4(),
        stage_schema="facility_stage",
    )

    assert receipt["row_count"] == 2
    assert receipt["alias"] == {"bound": "alias"}
    project.assert_awaited_once()

    disabled_project = AsyncMock()
    monkeypatch.setattr(effects, "project_observations", disabled_project)
    monkeypatch.setattr(effects, "validate_observations", AsyncMock(return_value=({"enabled": False}, None)))
    disabled = await effects.prepare_facility_address_effects(
        _session(scalar_values=[False], execute_values=[None, _Result(mapping=(8, 9))]),
        operation_id=uuid4(),
        stage_schema="facility_stage",
    )
    assert disabled["row_count"] == 0
    disabled_project.assert_not_awaited()

    monkeypatch.setattr(effects, "validate_observations", AsyncMock(return_value=({"enabled": False}, None)))
    with pytest.raises(RuntimeError, match="plan already exists"):
        await effects.prepare_facility_address_effects(
            _session(scalar_values=[True], execute_values=[None, _Result(mapping=(8, 9))]),
            operation_id=uuid4(),
            stage_schema="facility_stage",
        )


def test_effect_receipt_requires_fixed_scope_and_bounded_resources():
    receipt = _receipt()
    assert effects.validate_effect_receipt(receipt) is receipt
    with pytest.raises(RuntimeError, match="effect receipt is invalid"):
        effects.validate_effect_receipt({})
    with pytest.raises(RuntimeError, match="effect receipt is invalid"):
        effects.validate_effect_receipt({**receipt, "schema": "other"})


@pytest.mark.asyncio
async def test_fenced_effects_binds_alias_and_rejects_alias_or_destination_drift(monkeypatch):
    monkeypatch.setattr(effects, "_lock_archive", AsyncMock(return_value=("archive", 7)))
    session = _session(scalar_values=[0, False])
    archive, parameters = await effects._fenced_effects(session, _receipt(), rollback=False)
    assert archive == "archive" and parameters["operation"].version == 4

    alias_by_field = {"bound": "alias"}
    semantic_alias = SimpleNamespace(as_dict=lambda: alias_by_field)
    monkeypatch.setattr(
        effects,
        "capture_entity_address_alias_semantic_receipt",
        AsyncMock(return_value=semantic_alias),
    )
    receipt = _receipt(alias=alias_by_field, row_count=1)
    archive, parameters = await effects._fenced_effects(_session(scalar_values=[1, False]), receipt, rollback=True)
    assert archive == "archive"
    assert str(parameters["operation"]) == receipt["operation_id"]

    monkeypatch.setattr(
        effects,
        "capture_entity_address_alias_semantic_receipt",
        AsyncMock(return_value=SimpleNamespace(as_dict=lambda: {"changed": True})),
    )
    with pytest.raises(RuntimeError, match="alias identity changed"):
        await effects._fenced_effects(_session(), receipt, rollback=False)

    monkeypatch.setattr(
        effects,
        "capture_entity_address_alias_semantic_receipt",
        AsyncMock(return_value=semantic_alias),
    )
    with pytest.raises(RuntimeError, match="destination changed"):
        await effects._fenced_effects(_session(scalar_values=[1, True]), receipt, rollback=False)


@pytest.mark.asyncio
async def test_apply_and_rollback_pass_exact_fence_and_writer_mode(monkeypatch):
    parameters_by_name = {"operation": uuid4()}
    receipt = _receipt()
    fence = AsyncMock(return_value=("archive", parameters_by_name))
    monkeypatch.setattr(effects, "_fenced_effects", fence)
    writer = AsyncMock()
    monkeypatch.setattr(effects, "_write_images", writer)
    apply_session = _session()
    await effects.apply_facility_address_effects(apply_session, receipt)
    fence.assert_awaited_with(apply_session, receipt, rollback=False)
    writer.assert_awaited_with(apply_session, "archive", parameters_by_name, rollback=False)
    assert apply_session.execute.await_count == 2
    rollback_session = _session()
    await effects.rollback_facility_address_effects(rollback_session, receipt)
    fence.assert_awaited_with(rollback_session, receipt, rollback=True)
    writer.assert_awaited_with(rollback_session, "archive", parameters_by_name, rollback=True)
    assert rollback_session.execute.await_count == 1


def test_merge_require_rejects_invalid_state():
    with pytest.raises(RuntimeError, match="synthetic failure"):
        merge.require(False, "synthetic failure")


@pytest.mark.asyncio
async def test_validate_observations_handles_enabled_disabled_and_unbound_aliases(monkeypatch):
    metadata = {
        "contract": merge.CONTRACT,
        "enabled": True,
        "canon_version": merge.current_canon_version(),
        "alias_semantics": {"portable": "alias"},
        "source_bit": 8,
        "priority": 4,
    }
    monkeypatch.setattr(merge, "require_capture_bounds", AsyncMock())
    monkeypatch.setattr(merge, "_validate_payloads", AsyncMock())
    alias = SimpleNamespace(portable_identity=lambda: metadata["alias_semantics"], as_dict=lambda: {"bound": "alias"})
    capture_alias = AsyncMock(return_value=alias)
    monkeypatch.setattr(merge, "capture_entity_address_alias_semantic_receipt", capture_alias)
    metadata_result = _Result(rows=[SimpleNamespace(address_key=merge.METADATA_KEY, payload=metadata)])
    observed, bound = await merge.validate_observations(
        _session(execute_values=[metadata_result]), stage_schema="stage", schema="mrf"
    )
    assert observed is metadata and bound == {"bound": "alias"}

    disabled_metadata_by_field = {**metadata, "enabled": False, "alias_semantics": None}
    observed, bound = await merge.validate_observations(
        _session(
            execute_values=[
                _Result(rows=[SimpleNamespace(address_key=merge.METADATA_KEY, payload=disabled_metadata_by_field)])
            ],
            scalar_values=[0],
        ),
        stage_schema="stage",
        schema="mrf",
    )
    assert observed is disabled_metadata_by_field and bound is None

    observed, bound = await merge.validate_observations(
        _session(execute_values=[metadata_result]),
        stage_schema="stage",
        schema="mrf",
        bind_alias=False,
    )
    assert observed is metadata and bound is None


@pytest.mark.asyncio
async def test_merge_payload_and_identity_guards():
    await merge._validate_payloads(_session(scalar_values=[False]), "stage.contribution")
    with pytest.raises(RuntimeError, match="payload is invalid"):
        await merge._validate_payloads(_session(scalar_values=[True]), "stage.contribution")

    require_identity_collision_free = merge._require_identity_collision_free
    await require_identity_collision_free(_session(scalar_values=[False]))
    with pytest.raises(RuntimeError, match="key collision"):
        await require_identity_collision_free(_session(scalar_values=[True]))
