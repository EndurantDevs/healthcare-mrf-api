# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Candidate sealing lifecycle and retained validation receipts."""

import asyncio
import json
from contextlib import asynccontextmanager
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import registry_fhir_catalog_validation as catalog_validation
from tests.test_registry_fhir_catalog_grant import snapshot, standing_grant


def _sealing_actor(name, events):
    @asynccontextmanager
    async def transaction(**options):
        events.append((name, "begin", options))
        try:
            yield
        except BaseException:
            events.append((name, "rollback"))
            raise
        else:
            events.append((name, "commit"))

    return SimpleNamespace(transaction=transaction, execute=AsyncMock())


def _sealing_case(monkeypatch, events):
    """Supply real configured roles and the current logical preparation lifecycle."""
    from uuid import uuid4

    from process import registry_fhir_catalog_sync as sync
    from process import registry_fhir_catalog_worker as worker
    from process.registry_fhir_catalog_custody import FHIRCatalogTarget
    from tests.test_registry_fhir_catalog_worker_activation import _configuration

    config = worker.parse_fhir_catalog_sync_config(json.dumps(_configuration()))
    candidate = uuid4()
    copy_target = FHIRCatalogTarget(*(str(uuid4()) for _ in range(3)), str(candidate), "fhir_catalog_" + candidate.hex)
    selected = snapshot(config.grant, schema_name=copy_target.schema_name)
    selected["schema_name"] = copy_target.schema_name
    selected["custody_json"] = {
        "preparation": {"target": asdict(copy_target), "previous_metadata_generation": None, "accounting": {}}
    }
    actors_by_name = {name: _sealing_actor(name, events) for name in ("preparation", "sealer", "completion")}
    acquisitions = iter(actors_by_name.values())

    @asynccontextmanager
    async def acquire():
        yield next(acquisitions)

    store = SimpleNamespace(yield_named_claim=AsyncMock(), validate_named=AsyncMock())
    claim = object()
    admission = AsyncMock(return_value=(store, {"state": "frozen"}, claim))
    monkeypatch.setattr(sync, "admit_catalog_preparation", admission)
    return SimpleNamespace(
        config=config,
        target=copy_target,
        snapshot=selected,
        actors=actors_by_name,
        pool=SimpleNamespace(acquire=acquire),
        store=store,
        claim=claim,
        admission=admission,
    )


def _assert_sealing_outcome(case, events, failure, receipt):
    """Require the sealer to settle before completion, with no completion on failure."""
    outcome = "rollback" if failure else "commit"
    expected_events = [
        ("preparation", "begin", {}),
        ("preparation", "commit"),
        ("sealer", "begin", {}),
        ("sealer", outcome),
    ]
    if not failure:
        expected_events.extend([("completion", "begin", {}), ("completion", "commit")])
    assert events == expected_events
    assert case.admission.await_count == case.store.yield_named_claim.await_count == (1 if failure else 2)
    roles_by_name = {
        "namespace_owner_role": "registry_snapshot_owner",
        "publisher_role": "catalog_sealer",
        "application_roles": [
            "catalog_loader",
            "catalog_sealer",
            "registry_reader",
            "worker_execution",
            "worker_lifecycle",
        ],
    }
    for request in case.admission.await_args_list:
        assert request.kwargs == {"control_schema": "registry", "roles": roles_by_name}
    if failure:
        case.store.validate_named.assert_not_awaited()
        case.actors["completion"].execute.assert_not_awaited()
    else:
        case.store.validate_named.assert_awaited_once_with(
            case.claim,
            {
                "kind": "catalog",
                "candidate_id": case.target.candidate_id,
                "data": {key: receipt[key] for key in ("metadata_generation", "tables", "content_sha256")},
            },
        )
        case.actors["completion"].execute.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, RuntimeError, asyncio.CancelledError], ids=["success", "failure", "cancel"])
async def test_candidate_sealing_records_completion_only_after_success(monkeypatch, failure):
    """Validate on one sealer transaction and record completion only after it commits."""
    from process import registry_fhir_catalog_sync as sync

    events = []
    case = _sealing_case(monkeypatch, events)
    receipt_by_field = {
        "metadata_generation": case.snapshot["metadata_generation"],
        "inputs": [],
        "tables": {},
        "content_sha256": "a" * 64,
    }

    async def freeze(connection, target, **options):
        assert events[-1:] == [("sealer", "begin", {})]
        assert connection is case.actors["sealer"] and target is case.target
        assert options == {
            "expected_snapshot": case.snapshot,
            "control_schema": case.config.grant.control_schema,
            "standing_grant": case.config.grant,
        }
        if failure:
            raise failure("synthetic sealing failure")
        return receipt_by_field

    monkeypatch.setattr(sync, "validate_fhir_catalog_snapshot_data", freeze)
    operation = sync._seal_catalog_snapshot(case.config, case.target, case.snapshot, case.pool)
    if failure:
        with pytest.raises(failure, match="synthetic sealing failure"):
            await operation
    else:
        assert await operation == "validated"
    _assert_sealing_outcome(case, events, failure, receipt_by_field)


def empty_validation_receipt():
    import hashlib

    selected = snapshot(standing_grant(), network_generation=None, manifest_sha256=None)
    reports_by_table = {
        "registry_fhir_catalog_" + table: {
            "rows": 0,
            "sha256": hashlib.sha256(b"").hexdigest(),
            "batches": 0,
            "bytes": 0,
        }
        for table in ("source", "observation", "network", "relationship", "binding")
    }
    selected["accounting_json"] = json.dumps(reports_by_table)
    return selected, {
        "metadata_generation": selected["metadata_generation"],
        "inputs": catalog_validation.snapshot_inputs(selected),
        "tables": reports_by_table,
        "content_sha256": catalog_validation._content_digest(reports_by_table),
    }


def test_empty_catalog_validation_receipt_preserves_unattached_directory_and_serialized_accounting():
    selected, receipt = empty_validation_receipt()
    catalog_validation.verify_fhir_catalog_validation(selected, receipt)
    assert receipt["inputs"]["network_generation"] is receipt["inputs"]["manifest_sha256"] is None
    assert all(report["rows"] == report["batches"] == report["bytes"] == 0 for report in receipt["tables"].values())


@pytest.mark.parametrize("tamper", ["generation", "inputs", "tables", "counts", "batches", "content", "accounting"])
def test_stored_validation_receipt_refuses_changed_inputs_accounting_or_content(tamper):
    selected, receipt = empty_validation_receipt()
    report_by_field = receipt["tables"]["registry_fhir_catalog_network"]
    match tamper:
        case "generation":
            receipt["metadata_generation"] = "f" * 64
        case "inputs":
            receipt["inputs"]["metadata_approved_revision"] += 1
        case "tables":
            receipt["tables"].pop("registry_fhir_catalog_source")
        case "counts":
            report_by_field["rows"] = True
        case "batches":
            report_by_field.update(rows=1, batches=0)
        case "content":
            receipt["content_sha256"] = "f" * 64
        case "accounting":
            selected["accounting_json"] = {}
    with pytest.raises(ValueError, match="registry_fhir_catalog_"):
        catalog_validation.verify_fhir_catalog_validation(selected, receipt)
