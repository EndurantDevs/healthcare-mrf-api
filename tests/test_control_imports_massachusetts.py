# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bind managed Massachusetts requests to the requested acquisition mode."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.exc import IntegrityError

from api import control_imports
from tests.test_massachusetts_profile import worker


IMPORTER = "massachusetts-borim-profile"
PARAMS = {"max_providers": 100, "reprocess_from": "a" * 64}


@pytest.mark.parametrize("params", [[], {"reprocessing_from": "a" * 64}, {"test": True},
                                       {**PARAMS, "resume_from": "b" * 64}])
async def test_invalid_modes_never_create_or_enqueue_a_run(monkeypatch, params):
    database = SimpleNamespace(execute=AsyncMock(side_effect=AssertionError("No control mutation")))
    monkeypatch.setattr(control_imports, "db", database)
    with pytest.raises(ValueError, match="massachusetts_profile_"):
        await control_imports.create_import_run({"importer": IMPORTER, "params": params})
    database.execute.assert_not_called()


@pytest.mark.parametrize("lookup", ["idempotency", "active"])
@pytest.mark.parametrize("same_request", [False, True])
async def test_existing_request_must_match_mode_parent_and_bound(monkeypatch, lookup, same_request):
    existing_by_field = {"run_id": "synthetic-existing", "importer": IMPORTER, "status": "running",
                "params": dict(PARAMS) if same_request else {"resume_from": "a" * 64, "max_providers": 100}}
    monkeypatch.setattr(control_imports, "find_importer_run_by_idempotency_key", AsyncMock(return_value=existing_by_field if lookup == "idempotency" else None))
    monkeypatch.setattr(control_imports, "find_earliest_active_run_by_importer", AsyncMock(return_value=existing_by_field))
    enqueue = AsyncMock(side_effect=AssertionError("Existing request must not enqueue again"))
    monkeypatch.setattr(control_imports, "_enqueue_import_start", enqueue)
    request_by_field = {"importer": IMPORTER, "params": PARAMS, "idempotency_key": "synthetic-key"}
    if same_request:
        result, created = await control_imports.create_import_run(request_by_field)
        assert created is False and result["run_id"] == existing_by_field["run_id"]
    else:
        with pytest.raises(ValueError, match="existing_request_mismatch"):
            await control_imports.create_import_run(request_by_field)
    enqueue.assert_not_called()


async def test_retained_reprocessing_idempotency_includes_terminal_runs_only_for_that_mode(monkeypatch):
    terminal = AsyncMock(return_value={"run_id": "completed"})
    active = AsyncMock(return_value={"run_id": "active"})
    monkeypatch.setattr(control_imports, "find_importer_run_by_idempotency_key", terminal)
    monkeypatch.setattr(control_imports, "find_active_run_by_idempotency_key", active)
    assert await control_imports._idempotent_import_run(IMPORTER, "key", params=PARAMS) == {"run_id": "completed"}
    assert await control_imports._idempotent_import_run(IMPORTER, "key", params={"resume_from": "a" * 64}) == {"run_id": "active"}
    assert terminal.await_count == 2
    terminal.assert_awaited_with(IMPORTER, "key")
    active.assert_awaited_once_with(IMPORTER, "key")


async def test_terminal_retained_key_cannot_be_reused_for_a_network_request(monkeypatch):
    existing_by_field = {"run_id": "synthetic-existing", "importer": IMPORTER, "status": "succeeded", "params": PARAMS}
    terminal = AsyncMock(return_value=existing_by_field)
    monkeypatch.setattr(control_imports, "find_importer_run_by_idempotency_key", terminal)
    active = AsyncMock(side_effect=AssertionError("A terminal retained key must not become a fresh request"))
    monkeypatch.setattr(control_imports, "find_active_run_by_idempotency_key", active)
    with pytest.raises(ValueError, match="existing_request_mismatch"):
        await control_imports.create_import_run({"importer": IMPORTER, "params": {}, "idempotency_key": "synthetic-key"})
    active.assert_not_called()


async def test_idempotency_race_rechecks_the_exact_retained_request(monkeypatch):
    existing_by_field = {"run_id": "synthetic-existing", "importer": IMPORTER, "status": "succeeded",
                "params": {**PARAMS, "max_providers": None}}
    monkeypatch.setattr(control_imports, "find_importer_run_by_idempotency_key", AsyncMock(side_effect=[None, existing_by_field]))
    monkeypatch.setattr(control_imports, "find_earliest_active_run_by_importer", AsyncMock(return_value=None))
    monkeypatch.setattr(control_imports, "_admit_import_row", AsyncMock(side_effect=IntegrityError("insert", {}, Exception("synthetic collision"))))
    with pytest.raises(ValueError, match="existing_request_mismatch"):
        await control_imports.create_import_run({"importer": IMPORTER, "params": PARAMS, "idempotency_key": "synthetic-key"})


@pytest.mark.parametrize("state", ["legacy_terminal", "retained_terminal", "same_active", "different_active", "no_key"])
async def test_locked_admission_preserves_legacy_terminal_keys_and_checks_active_scope(monkeypatch, state):
    existing_by_field = {"run_id": "synthetic-existing", "importer": IMPORTER,
                         "status": "succeeded" if state in {"legacy_terminal", "retained_terminal"} else "running",
                         "params": PARAMS if state in {"same_active", "retained_terminal"} else {}}
    connection = SimpleNamespace(scalar=AsyncMock(), status=AsyncMock(),
                                 all=AsyncMock(return_value=[SimpleNamespace(_mapping=existing_by_field)] if state in {"legacy_terminal", "retained_terminal"} else []))

    @asynccontextmanager
    async def acquire():
        yield connection

    monkeypatch.setattr(control_imports, "db", SimpleNamespace(acquire=acquire))
    monkeypatch.setattr(control_imports, "_active_importer_runs", AsyncMock(return_value=[existing_by_field] if state in {"same_active", "different_active"} else []))
    request_by_field = {"run_id": "synthetic-new", "importer": IMPORTER, "status": "queued",
                        "params": {} if state == "legacy_terminal" else PARAMS,
                        "idempotency_key": None if state == "no_key" else "synthetic-key"}
    if state == "different_active":
        with pytest.raises(ValueError, match="existing_request_mismatch"):
            await control_imports._admit_massachusetts_import_run(request_by_field)
    else:
        result = await control_imports._admit_massachusetts_import_run(request_by_field)
        assert (result is not None) == (state in {"same_active", "retained_terminal"})
    assert connection.status.await_count == (1 if state in {"legacy_terminal", "no_key"} else 0)
    assert "pg_advisory_xact_lock" in str(connection.scalar.call_args.args[0])
    if state == "no_key":
        connection.all.assert_not_called()


def test_registry_cli_and_worker_payload_expose_the_same_reprocessing_parameter():
    from click.testing import CliRunner

    registration = next(entry for entry in control_imports.importer_registry() if entry["name"] == IMPORTER)
    parameter = next(option for option in registration["params_schema"] if option["name"] == "reprocess_from")
    assert parameter["opts"] == ["--reprocess-from"] and parameter["default"] is None
    payload = control_imports._adapter_payload(control_imports._SINGLE_JOB_ADAPTERS[IMPORTER],
                                               {"run_id": "synthetic-control", "importer": IMPORTER}, PARAMS)
    assert worker._parameters(payload["task"]) == (100, None, "a" * 64)
    assert payload["target_function"] == "import_profiles" and payload["call_style"] == "ctx_task"
    result = CliRunner().invoke(worker.massachusetts_borim_profile, ["--reprocess-from", "a" * 64])
    assert result.exit_code == 2 and "managed import API" in result.output
