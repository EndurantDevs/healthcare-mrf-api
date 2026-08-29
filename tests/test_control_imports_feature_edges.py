from __future__ import annotations

import importlib
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

control_imports = importlib.import_module("api.control_imports")


@pytest.mark.asyncio
async def test_hospital_registry_validation_runs_off_event_loop(monkeypatch):
    event_loop_thread = threading.get_ident()
    validation_threads: list[int] = []
    artifact_store = object()

    def validate(_params):
        validation_threads.append(threading.get_ident())
        return (
            {
                "hospital_id": "hospital-000001",
                "cms_hpt_url": "https://hospital.example/cms-hpt.txt",
            },
        )

    def capacity(store, locator_count):
        validation_threads.append(threading.get_ident())
        assert store is artifact_store
        assert locator_count == 1

    monkeypatch.setattr(control_imports, "selected_hospital_hpt_registry", validate)
    monkeypatch.setattr(
        control_imports, "hospital_price_artifact_store", lambda: artifact_store
    )
    monkeypatch.setattr(control_imports, "configured_resource_limits", capacity)

    normalized = await control_imports._validate_hospital_price_params(
        "hospital-prices", {}
    )

    assert len(validation_threads) == 2
    assert all(thread != event_loop_thread for thread in validation_threads)
    assert normalized == {"hospital_ids": ["hospital-000001"]}


@pytest.mark.asyncio
async def test_hospital_capacity_rejection_precedes_admission_and_enqueue(monkeypatch):
    admission = AsyncMock()
    enqueue = AsyncMock()
    monkeypatch.setattr(control_imports, "importer_names", lambda: {"hospital-prices"})
    monkeypatch.setattr(
        control_imports,
        "_validate_hospital_price_params",
        AsyncMock(side_effect=RuntimeError("hospital capacity rejected")),
    )
    monkeypatch.setattr(control_imports, "_admit_import_row", admission)
    monkeypatch.setattr(control_imports, "_enqueue_import_start", enqueue)

    with pytest.raises(RuntimeError, match="capacity rejected"):
        await control_imports.create_import_run({
            "importer": "hospital-prices",
            "params": {"all_hospitals": True},
        })

    admission.assert_not_awaited()
    enqueue.assert_not_awaited()


def _acquisition_params() -> dict[str, object]:
    return {
        "import_resources": True,
        "stale_cleanup": False,
        "publish_artifacts": False,
        "publish_after_acquisition": False,
        "publish_corroboration": False,
        "source_concurrency": 1,
        "source_ids": ["source-one"],
    }


@pytest.mark.asyncio
async def test_finalize_and_active_run_boundaries(monkeypatch):
    get_run = AsyncMock(return_value=None)
    monkeypatch.setattr(control_imports, "get_import_run", get_run)
    assert await control_imports.finalize_import_run("missing", {}) is None

    get_run.return_value = {"importer": "unsupported", "status": "running"}
    with pytest.raises(ValueError, match="does not support finalize"):
        await control_imports.finalize_import_run("run", {})

    get_run.return_value = {
        "importer": next(iter(control_imports._FINISH_IMPORTERS)),
        "status": "succeeded",
    }
    assert (await control_imports.finalize_import_run("run", {}))["status"] == (
        "succeeded"
    )

    class Scalars:
        def all(self):
            return [{"run_id": "active"}]

    monkeypatch.setattr(
        control_imports.db,
        "execute",
        AsyncMock(return_value=SimpleNamespace(scalars=lambda: Scalars())),
    )
    monkeypatch.setattr(control_imports, "normalize_run", lambda row: row)
    assert await control_imports.find_active_runs_by_importer("profile") == [
        {"run_id": "active"}
    ]


@pytest.mark.asyncio
async def test_active_idempotency_lookups_bind_importer(monkeypatch):
    """Keep active idempotency ownership within one importer."""

    active_lookup = AsyncMock(
        return_value=SimpleNamespace(scalar_one_or_none=lambda: None)
    )
    monkeypatch.setattr(control_imports.db, "execute", active_lookup)
    assert (
        await control_imports.find_active_run_by_idempotency_key(
            "profile",
            "key",
        )
        is None
    )
    assert active_lookup.await_args.args[0].compile().params["importer_1"] == (
        "profile"
    )

    connection = SimpleNamespace(all=AsyncMock(return_value=[]))
    assert (
        await control_imports._active_idempotency_run(connection, "profile", "key")
        is None
    )
    assert connection.all.await_args.args[0].compile().params["importer_1"] == (
        "profile"
    )


def test_provider_profile_admission_and_adapter_edge_contracts(monkeypatch):
    assert control_imports._canonical_provider_directory_endpoint_scope(" ") is None
    monkeypatch.setattr(
        control_imports,
        "validated_profile_execution",
        Mock(
            side_effect=control_imports.ProviderDirectoryProfileSelectionError(
                "invalid proof"
            )
        ),
    )
    assert control_imports._provider_directory_operation(
        {"provider_directory_profile_contract_id": "bad"}
    )[0] == control_imports._PROVIDER_DIRECTORY_EXCLUSIVE
    assert control_imports._provider_directory_blocking_run(
        {
            **_acquisition_params(),
            "provider_directory_endpoint_scope": "https://example.test/fhir",
        },
        [{"params": "invalid"}],
    ) == {"params": "invalid"}

    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_MAX_ACTIVE", "invalid")
    assert control_imports._provider_directory_max_active() == 2
    assert control_imports._safe_enqueued_job_id(
        "ordinary",
        {},
        object(),
        requested_job_id=None,
    ).startswith("<object object")
    assert control_imports._adapter_payload(
        {"payload": "passthrough"},
        {"run_id": "run"},
        {"source": "value"},
    ) == {"source": "value"}
    assert control_imports._run_import_adapter_payload(
        {"run_id": "run", "import_id": "fallback"},
        {"source_urls": ["https://example.invalid"]},
        test_mode=False,
    )["source_urls"] == ["https://example.invalid"]


@pytest.mark.asyncio
async def test_create_enqueue_cancel_and_retry_error_boundaries(monkeypatch):
    monkeypatch.setattr(control_imports, "importer_names", lambda: {"profile"})
    with pytest.raises(ValueError, match="unknown importer"):
        await control_imports.create_import_run({"importer": "unknown"})

    active_by_key = {"run_id": "active", "status": "running"}
    monkeypatch.setattr(
        control_imports,
        "find_active_run_by_idempotency_key",
        AsyncMock(return_value=active_by_key),
    )
    assert await control_imports.create_import_run(
        {
            "importer": "profile",
            "idempotency_key": "same",
            "params": {},
        }
    ) == (active_by_key, False)

    monkeypatch.setattr(control_imports, "_adapter_for_import_row", lambda _run: None)
    enqueue = await control_imports._enqueue_import_start(
        {"importer": "profile", "params": {}}
    )
    assert enqueue["metrics"]["enqueue_adapter"] == "pending"

    monkeypatch.setattr(
        control_imports.asyncio,
        "to_thread",
        AsyncMock(side_effect=RuntimeError("worker unavailable")),
    )
    deleted = await control_imports._delete_active_worker_jobs({"run_id": "run"})
    assert deleted["enabled"] is False

    assert (await control_imports._remove_queued_job({}))["reason"] == (
        "missing queue or job_id"
    )
    monkeypatch.setattr(
        control_imports,
        "create_pool",
        AsyncMock(side_effect=RuntimeError("redis unavailable")),
    )
    removed = await control_imports._remove_queued_job(
        {"metrics": {"queue": "profile", "job_id": "job"}}
    )
    assert removed["redis"] is False

    monkeypatch.setattr(control_imports, "get_import_run", AsyncMock(return_value=None))
    assert await control_imports.retry_import_run("missing", {}) is None


@pytest.mark.asyncio
@pytest.mark.parametrize("idempotency_key", [None, "retry-key"])
async def test_integrity_conflicts_without_active_owner_are_not_hidden(
    monkeypatch,
    idempotency_key,
):
    monkeypatch.setattr(control_imports, "importer_names", lambda: {"profile"})
    monkeypatch.setattr(
        control_imports,
        "find_active_run_by_idempotency_key",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(
        control_imports,
        "find_earliest_active_run_by_importer",
        AsyncMock(return_value=None),
    )
    conflict = control_imports.IntegrityError(
        "insert import run",
        {},
        RuntimeError("duplicate"),
    )
    monkeypatch.setattr(
        control_imports.db,
        "execute",
        AsyncMock(side_effect=conflict),
    )

    with pytest.raises(control_imports.IntegrityError):
        await control_imports.create_import_run(
            {
                "importer": "profile",
                "idempotency_key": idempotency_key,
                "params": {},
            }
        )
