# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native enqueue acknowledgements cannot replace worker or terminal state."""

import json
import os
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy import select, text, update
from sqlalchemy.engine import URL

from api import control_imports, control_workers, plan_pricing_worker_policy
from db.models import ImportRun, db
from process import control_lifecycle
from process import plan_pricing_control_policy as pricing_policy
from process.plan_pricing_admission import pricing_control_layout
from process.plan_pricing_control import (
    DELIVERY_FIELD,
    preserve_delivery,
    pricing_control_digest,
    projection_admission_key,
)
from process.plan_pricing_control_transaction import pricing_driver, pricing_worker_claim_transaction
from tests.test_entity_address_snapshot_destination import _install_destination_extensions
from tests.test_plan_pricing_idempotency import _durable_import_request
from tests.test_plan_pricing_idempotency_postgres import (
    _drop_import_run_table,
    _fake_enqueue,
    _require_disposable_database,
    _reset_import_run_schema,
)
from tests.test_provider_directory_profile_capacity_adoption_postgres import _upgrade_disposable_schema_to_head

pytestmark = pytest.mark.asyncio(loop_scope="module")


@pytest_asyncio.fixture(loop_scope="module")
async def pricing_control_db(monkeypatch):
    await _reset_import_run_schema()
    monkeypatch.setattr(control_imports, "read_live_progress", lambda _run_id: None)
    monkeypatch.setattr(control_imports, "_write_run_live_progress", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(control_imports, "enqueue_status_event", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(control_lifecycle, "write_live_progress", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(control_lifecycle, "_should_update_control_run_db", AsyncMock(return_value=True))
    try:
        yield
    finally:
        await _drop_import_run_table()


async def _raw(run_id):
    result = await db.execute(select(ImportRun.__table__).where(ImportRun.run_id == run_id))
    return dict(result.mappings().one())


async def _worker_update(run_id, status):
    pair_by_field = {"attempt_id": run_id + ":" + "a" * 32, "attempt_started_at": "2026-10-15T13:00:00.000000+00:00"}
    assert await control_lifecycle.mark_control_run(
        run_id, status="running", phase_detail="worker running", progress_message="running", **pair_by_field
    )
    if status != "running":
        assert await control_lifecycle.mark_control_run(
            run_id,
            status=status,
            phase_detail="worker " + status,
            progress_message=status,
            metrics={"projection_receipt": {"exact": True}},
            error={"code": "synthetic-worker-failure"} if status == "failed" else None,
            **pair_by_field,
        )


@pytest.mark.parametrize("worker_status", ("running", "succeeded", "failed"))
async def test_native_late_ack_preserves_full_newer_worker_state(pricing_control_db, monkeypatch, worker_status):
    request = _durable_import_request("plan-pricing-projection", "late_ack_run")
    captured_by_field = {}

    async def enqueue(run):
        pending = await _raw(run["run_id"])
        assert pending["metrics"][DELIVERY_FIELD] == {"run_id": run["run_id"], "state": "pending"}
        await _worker_update(run["run_id"], worker_status)
        captured_by_field.update(await _raw(run["run_id"]))
        return await _fake_enqueue(run)

    monkeypatch.setattr(control_imports, "_enqueue_import_start", enqueue)
    response, created = await control_imports.create_import_run(request)
    stored = await _raw(request["run_id"])
    assert created and response["status"] == worker_status
    for field in ("status", "phase_detail", "progress", "error", "started_at", "finished_at", "heartbeat_at"):
        assert stored[field] == captured_by_field[field]
    assert stored["metrics"][DELIVERY_FIELD] == {"run_id": request["run_id"], "state": "settled", "outcome": "queued"}
    if worker_status != "running":
        assert stored["metrics"]["projection_receipt"] == {"exact": True}


async def test_native_lost_enqueue_reply_preserves_terminal_and_uncertainty(pricing_control_db, monkeypatch):
    request = _durable_import_request("plan-pricing-projection", "lost_reply_run")

    async def enqueue(run):
        await _worker_update(run["run_id"], "succeeded")
        return control_imports._failed_enqueue_result(
            importer=run["importer"],
            params=run["params"],
            adapter=control_imports._SINGLE_JOB_ADAPTERS[run["importer"]],
            error=TimeoutError("lost reply"),
        )

    monkeypatch.setattr(control_imports, "_enqueue_import_start", enqueue)
    response, _created = await control_imports.create_import_run(request)
    stored = await _raw(request["run_id"])
    assert response["status"] == stored["status"] == "succeeded"
    assert stored["metrics"][DELIVERY_FIELD]["state"] == "unknown"
    assert stored["metrics"]["projection_receipt"] == {"exact": True}
    assert stored["error"] is None


async def test_native_stale_worker_metrics_cannot_erase_settled_delivery(pricing_control_db, monkeypatch):
    request = _durable_import_request("plan-pricing-projection", "settled_run")
    monkeypatch.setattr(control_imports, "_enqueue_import_start", _fake_enqueue)
    await control_imports.create_import_run(request)
    settled = (await _raw(request["run_id"]))["metrics"][DELIVERY_FIELD]
    await _worker_update(request["run_id"], "succeeded")
    await db.execute(
        update(ImportRun)
        .where(ImportRun.run_id == request["run_id"])
        .values(metrics=preserve_delivery(ImportRun, {DELIVERY_FIELD: {"state": "pending"}, "new": True}))
    )
    metrics = (await _raw(request["run_id"]))["metrics"]
    assert metrics[DELIVERY_FIELD] == settled
    assert metrics["new"] is True


async def test_native_conflicting_key_never_changes_original_request(pricing_control_db, monkeypatch):
    request = _durable_import_request("plan-pricing-projection", "original_run")
    enqueue = AsyncMock(side_effect=_fake_enqueue)
    monkeypatch.setattr(control_imports, "_enqueue_import_start", enqueue)
    await control_imports.create_import_run(request)
    with pytest.raises(ValueError, match="different request"):
        await control_imports.create_import_run({**request, "params": {"different": True}, "run_id": "other_run"})
    assert enqueue.await_count == 1
    assert (await _raw(request["run_id"]))["params"] == request["params"]


async def test_native_cancel_keeps_current_producer_settlement(pricing_control_db, monkeypatch):
    request = _durable_import_request("plan-pricing-projection", "cancel_race_run")
    monkeypatch.setattr(control_imports, "_enqueue_import_start", _fake_enqueue)
    await control_imports.create_import_run(request)
    current_by_field = await _raw(request["run_id"])
    settled = current_by_field["metrics"][DELIVERY_FIELD]
    stale_metrics_by_field = {DELIVERY_FIELD: {"run_id": request["run_id"], "state": "pending"}, "cancel_signal": {}}
    await control_imports._persist_cancel_request(
        request["run_id"],
        current_run=current_by_field,
        requested_at=control_imports.utc_now(),
        cancel_state_by_name=control_imports._cancel_state_by_name(
            canceled_before_start=True,
            has_terminalized_active_worker=False,
            current_progress=current_by_field["progress"],
        ),
        run_metrics_by_name=stale_metrics_by_field,
    )
    stored = await _raw(request["run_id"])
    assert stored["status"] == "canceled"
    assert stored["metrics"][DELIVERY_FIELD] == settled


_HISTORICAL_REVISION = "20261028010000_registry_fhir_catalog"


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def historical_pricing_database(tmp_path_factory):
    """Use the supported baseline and real upgrades in one exactly cleaned test DB."""
    _require_disposable_database()
    await db.disconnect()
    database_name = "historical_pricing_test_" + uuid4().hex
    admin_url = URL.create(
        "postgresql+asyncpg",
        username=os.getenv("HLTHPRT_DB_USER", "postgres"),
        password=os.getenv("HLTHPRT_DB_PASSWORD") or None,
        host=os.getenv("HLTHPRT_DB_HOST", "127.0.0.1"),
        port=int(os.getenv("HLTHPRT_DB_PORT", "5432")),
        database=os.environ["HLTHPRT_DB_DATABASE"],
    )
    admin = await asyncpg.connect(str(admin_url.set(drivername="postgresql").render_as_string(hide_password=False)))
    is_database_created = False
    try:
        await admin.execute(f'CREATE DATABASE "{database_name}"')
        is_database_created = True
        with pytest.MonkeyPatch.context() as environment:
            for name in ("DB_DATABASE", "HLTHPRT_DB_DATABASE"):
                environment.setenv(name, database_name)
            for name in ("DB_SCHEMA", "HLTHPRT_DB_SCHEMA", "HLTHPRT_PLAN_PRICING_CONTROL_SCHEMA"):
                environment.setenv(name, "mrf")
            environment.setenv("HLTHPRT_IMPORT_NODE_ID", "synthetic-pricing-node")
            environment.setenv("HLTHPRT_CONTROL_RUN_DB_UPDATE_THROTTLE_SECONDS", "0")
            child_url = admin_url.set(database=database_name)
            await db.connect()
            await _install_destination_extensions(db)
            await db.disconnect()
            await _upgrade_disposable_schema_to_head(
                child_url.render_as_string(hide_password=False), "mrf", target_revision=_HISTORICAL_REVISION
            )
            await db.connect()
            assert await db.scalar("SELECT version_num FROM mrf.alembic_version") == _HISTORICAL_REVISION
            assert await db.scalar(
                "SELECT to_regclass('mrf.catalog_release_preparation') IS NULL "
                "AND to_regclass('mrf.catalog_release_admission_attempt') IS NULL"
            )
            yield SimpleNamespace(policy_path=tmp_path_factory.mktemp("historical-pricing") / "historical.json")
    finally:
        await db.disconnect()
        try:
            if is_database_created:
                await admin.execute(f'DROP DATABASE "{database_name}"')
                assert not await admin.fetchval("SELECT 1 FROM pg_database WHERE datname=$1", database_name)
        finally:
            await admin.close(timeout=5)


@pytest_asyncio.fixture(loop_scope="module")
async def historical_pricing_db(historical_pricing_database, monkeypatch):
    """A local candidate policy exercises the parser; it grants no deployment authority."""
    policy_path = historical_pricing_database.policy_path
    if policy_path.exists():
        policy_path.chmod(0o600)
    policy_path.write_text(
        json.dumps(
            {"contract": pricing_policy.HISTORICAL_POLICY_CONTRACT, "schema": "mrf", "revision": _HISTORICAL_REVISION}
        )
    )
    policy_path.chmod(0o400)
    monkeypatch.setattr(pricing_policy, "HISTORICAL_POLICY_PATH", policy_path)
    monkeypatch.setenv("HLTHPRT_PLAN_PRICING_HISTORICAL_CONFIGMAP", "synthetic-historical-pricing-policy")
    await _reset_import_run_schema()
    enqueued_run_ids = []

    async def enqueue(run):
        enqueued_run_ids.append(run["run_id"])
        result = await _fake_enqueue(run)
        adapter = control_imports._SINGLE_JOB_ADAPTERS[run["importer"]]
        result["metrics"].update(
            queue=adapter["queue"], function=adapter["function"], job_id="plan_pricing_projection_" + run["run_id"]
        )
        return result

    monkeypatch.setattr(control_imports, "_enqueue_import_start", enqueue)
    monkeypatch.setattr(control_imports, "read_live_progress", lambda _run_id: None)
    monkeypatch.setattr(control_imports, "_write_run_live_progress", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(control_imports, "enqueue_status_event", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(control_lifecycle, "write_live_progress", lambda *_args, **_kwargs: None)
    try:
        yield SimpleNamespace(policy_path=policy_path, enqueued_run_ids=enqueued_run_ids)
    finally:
        await _drop_import_run_table()


def _historical_request(run_id):
    bindings = [
        {
            "ordinal": 0,
            "snapshot_id": "synthetic-pricing-snapshot",
            "source_key": "synthetic-pricing-source",
            "plan_id": "100000001",
            "market_type": "group",
            "role": "in_network",
            "required": True,
        }
    ]
    request_by_field = {
        "binding_manifest_digest": pricing_control_digest(
            {"contract": "hp-plan-serving-revision-v1", "bindings": bindings}, domain="hp_plan_serving_revision_v1"
        ),
        "bindings": bindings,
        "projection_contract": "plan_pricing_factorized_v4",
        "publication_identity": pricing_control_digest({"publication": "synthetic-historical"}),
        "publication_attempt_identity": pricing_control_digest({"attempt": run_id}),
    }
    return {
        "run_id": run_id,
        "importer": "plan-pricing-projection",
        "triggered_by": "release",
        "idempotency_key": projection_admission_key(request_by_field),
        "params": {
            name: request_by_field[name] for name in ("binding_manifest_digest", "bindings", "projection_contract")
        },
    }


def _historical_dispatch(run):
    return control_imports._adapter_payload(
        control_imports._SINGLE_JOB_ADAPTERS[run["importer"]], run, deepcopy(run["params"])
    )


async def test_native_historical_reserved_replay_launch_and_claim(historical_pricing_db):
    request = _historical_request("historical_reserved_run")
    original = deepcopy(request)
    first, created = await control_imports.create_import_run(request)
    replay, created_again = await control_imports.create_import_run({**request, "run_id": "historical_replayed_run"})
    assert created and not created_again
    assert first["run_id"] == replay["run_id"] == request["run_id"]
    assert historical_pricing_db.enqueued_run_ids == [request["run_id"]]
    assert request == original
    stored = await _raw(request["run_id"])
    spec = control_workers.WorkerSpec(
        "arq:PTGCandidateAudit", "process.PTGCandidateAudit", ("plan-pricing-projection",)
    )
    worker_payload = control_workers._AdmittedWorkerRequest(run_id=stored["run_id"], importer=stored["importer"])
    control_workers._bind_admitted_job(worker_payload, stored)
    async with db.transaction() as session:
        driver = await pricing_driver(session)
        assert await pricing_control_layout(driver) == ("mrf", _HISTORICAL_REVISION, False)
        assert await plan_pricing_worker_policy.admit_pricing_worker(driver, stored["run_id"], worker_payload) is None
    assert worker_payload.pricing_control_admission is None
    assert worker_payload.historical_pricing_policy == plan_pricing_worker_policy.HistoricalPricingWorkerPolicy(
        "mrf", _HISTORICAL_REVISION, stored["run_id"]
    )
    assert plan_pricing_worker_policy.pricing_worker_policy_environment(worker_payload) == [
        {"name": "HLTHPRT_PLAN_PRICING_CONTROL_SCHEMA", "value": "mrf"}
    ]
    volumes = plan_pricing_worker_policy.pricing_worker_policy_volumes(spec, worker_payload)
    assert volumes[0]["volumeMount"] == {
        "name": "pricing-control-policy",
        "mountPath": "/etc/pricing-control",
        "readOnly": True,
    }
    assert volumes[0]["volume"]["projected"]["sources"] == [
        {
            "configMap": {
                "name": "synthetic-historical-pricing-policy",
                "optional": False,
                "items": [{"key": "historical.json", "path": "historical.json"}],
            }
        }
    ]
    async with pricing_worker_claim_transaction(_historical_dispatch(stored)):
        assert await db.scalar(text("SELECT current_setting('transaction_read_only')")) == "off"
    assert await _raw(stored["run_id"]) == stored


@pytest.mark.parametrize("damage", ("missing", "revision"))
async def test_native_historical_policy_refuses_before_reserved_creation(historical_pricing_db, damage):
    path = historical_pricing_db.policy_path
    if damage == "missing":
        path.unlink()
    else:
        document = json.loads(path.read_text())
        document["revision"] = "different_revision"
        path.chmod(0o600)
        path.write_text(json.dumps(document))
    with pytest.raises(ValueError, match="schema_revision_invalid"):
        await control_imports.create_import_run(_historical_request("historical_refused_run"))
    assert historical_pricing_db.enqueued_run_ids == []
    assert await db.scalar(text("SELECT count(*) FROM mrf.import_run")) == 0


async def test_native_historical_claim_rollback_and_revoked_policy(historical_pricing_db):
    request = _historical_request("historical_claim_run")
    await control_imports.create_import_run(request)
    stored = await _raw(request["run_id"])
    dispatch = _historical_dispatch(stored)
    with pytest.raises(RuntimeError, match="synthetic claim rollback"):
        async with pricing_worker_claim_transaction(dispatch):
            await db.execute(update(ImportRun).where(ImportRun.run_id == stored["run_id"]).values(status="running"))
            raise RuntimeError("synthetic claim rollback")
    assert await _raw(stored["run_id"]) == stored
    historical_pricing_db.policy_path.unlink()
    has_entered_claim = False
    with pytest.raises(ValueError, match="schema_revision_invalid"):
        async with pricing_worker_claim_transaction(dispatch):
            has_entered_claim = True
    assert has_entered_claim is False
    assert await _raw(stored["run_id"]) == stored
    assert historical_pricing_db.enqueued_run_ids == [stored["run_id"]]
