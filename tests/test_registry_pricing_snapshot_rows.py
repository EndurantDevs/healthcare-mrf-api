# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual shared validators and consumed native-set cold row phase; no PG."""

import asyncio
import json
from copy import deepcopy
from types import SimpleNamespace

import asyncpg
import pytest

from api import ptg2_tables
from process import registry_pricing_release_binding_lookup as lookup
from process import registry_pricing_snapshot_rows as rows
from process.network_approved_catalog_evidence import ApprovedNetworkCatalogEvidenceRecord
from tests.ptg2_manifest_tables_support import (
    FakeSession,
    strict_serving_index,
    strict_snapshot_row,
    strict_v4_serving_index,
)
from tests.test_plan_release_serving import PLAN_RELEASE_ID, _binding_row
from tests.test_registry_pricing_release_binding_lookup import REFERENCE, REPORT, Driver


class SnapshotDriver(Driver):
    def __init__(self, snapshots=(), binding_rows=None):
        super().__init__(binding_rows)
        self.snapshots = list(snapshots)
        self.isolation = "repeatable read"
        self.read_only = "on"

    async def fetch(self, sql, *parameters):
        if "requested.table_name" in sql:
            self.calls.append((sql, parameters))
            self.tasks.append(asyncio.current_task())
            return []
        if any("ptg2_v3_snapshot_plan_scope" in name for name in parameters[0]):
            self.calls.append((sql, parameters))
            self.tasks.append(asyncio.current_task())
            return [{"relation_name": name, "available": True} for name in parameters[0]]
        if any("ptg2_v4_" in name for name in parameters[0]):
            self.calls.append((sql, parameters))
            self.tasks.append(asyncio.current_task())
            return [{"relation_name": name, "available": False} for name in parameters[0]]
        return await super().fetch(sql, *parameters)

    async def fetchrow(self, sql, *parameters):
        if not parameters and "transaction_isolation" in sql:
            self.calls.append((sql, parameters))
            self.tasks.append(asyncio.current_task())
            return {"isolation": self.isolation, "read_only": self.read_only, "search_path": "pg_catalog, public"}
        if "request.request_no" in sql:
            self.calls.append((sql, parameters))
            self.tasks.append(asyncio.current_task())
            encoded = json.dumps(
                [
                    {"request_no": index, "snapshot_id": identity, "matches": True}
                    for index, identity in enumerate(parameters[0], 1)
                ]
            )
            return {
                "isolation": self.isolation,
                "read_only": self.read_only,
                "bounded": len(encoded.encode()) <= parameters[-1],
                "rows_json": encoded if len(encoded.encode()) <= parameters[-1] else None,
            }
        if "WITH snapshots" not in sql:
            return await super().fetchrow(sql, *parameters)
        self.calls.append((sql, parameters))
        self.tasks.append(asyncio.current_task())
        if self.failure:
            raise self.failure
        encoded = json.dumps(self.snapshots)
        is_bounded = len(encoded.encode()) <= parameters[3]
        return {
            "isolation": self.isolation,
            "read_only": self.read_only,
            "bounded": is_bounded,
            "rows_json": encoded if is_bounded else None,
        }

    async def execute(self, sql, *parameters):
        self.calls.append((sql, parameters))
        self.tasks.append(asyncio.current_task())
        assert sql.startswith("SELECT pg_catalog.set_config('search_path',")


def metadata(snapshot_ids, role="in_network"):
    return {
        PLAN_RELEASE_ID: {
            "bindings": [
                {
                    "snapshot_id": identity,
                    "role": role,
                    "binding_ordinal": index,
                    "source_key": "source-a",
                    "plan_id": "TEST-PLAN-001",
                    "plan_market_type": "group",
                    "required": True,
                    "logical_scope_present": False,
                }
                for index, identity in enumerate(snapshot_ids)
            ]
        }
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("size", [1, 8, 64])
async def test_snapshot_phase_is_one_bound_set_on_the_same_reader(size):
    identities = [f"snapshot-{index}" for index in range(size)]
    driver = SnapshotDriver([{**strict_snapshot_row(), "snapshot_id": identity} for identity in identities])
    checks = await rows.read_pricing_snapshot_row_checks(
        driver, metadata([*identities, *identities]), max_report_bytes=16 * 1024 * 1024
    )
    assert set(checks) == set(identities) and len(driver.calls) == 4
    assert driver.calls[1][1][0] == sorted(identities)
    assert all(task is asyncio.current_task() for task in driver.tasks)
    assert driver.lifecycle == ["start", "release", "start", "release"] and driver.is_in_transaction()
    assert all(
        check["status"] == "published_row_validated" and check["full_readiness"] == "not_assessed"
        for check in checks.values()
    )
    assert "$1::text[]" in driver.calls[1][0] and ":snapshot_id" not in driver.calls[1][0]
    assert "LIMIT 1" not in driver.calls[1][0]
    assert "__PTG2_SCHEMA__" not in driver.calls[1][0]
    assert all(f"{rows.PTG2_SCHEMA}.{table}" in driver.calls[1][0] for table in rows._REQUIRED_TABLES)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["bound", "scope", "audit", "sources", "attestation", "database", "codes"])
async def test_existing_singleton_and_set_reject_the_same_published_row(change):
    snapshot = strict_snapshot_row()
    updates_by_case = {
        "bound": {"bound_snapshot_key": 42},
        "scope": {"attested_coverage_scope_id": "f" * 64},
        "audit": {"attested_audit_sample_digest": "f" * 64},
        "sources": {"source_row_count": 1},
        "attestation": {"attested_source_set_digest": "f" * 64},
        "database": {"backend_session_active": False},
        "codes": {"layout_serving_index": {**strict_serving_index(), "code_count": -1}},
    }
    snapshot.update(updates_by_case[change])
    with pytest.raises(ptg2_tables.PTG2ManifestArtifactError):
        await ptg2_tables.snapshot_serving_tables(FakeSession([deepcopy(snapshot)]), "snapshot-a")
    driver = SnapshotDriver([{**snapshot, "snapshot_id": "snapshot-a"}])
    checks = await rows.read_pricing_snapshot_row_checks(
        driver, metadata(["snapshot-a"]), max_report_bytes=16 * 1024 * 1024
    )
    assert checks["snapshot-a"] == {"status": "unavailable", "full_readiness": "not_assessed"}


@pytest.mark.asyncio
async def test_missing_duplicate_local_and_allowed_amounts_remain_unproven():
    driver = SnapshotDriver(
        [
            {**strict_snapshot_row(), "snapshot_id": "duplicate"},
            {**strict_snapshot_row(), "snapshot_id": "duplicate"},
            {**strict_snapshot_row(), "snapshot_id": "local", "has_local_physical_binding": True},
        ]
    )
    checks = await rows.read_pricing_snapshot_row_checks(
        driver, metadata(["missing", "duplicate", "local"]), max_report_bytes=16 * 1024 * 1024
    )
    assert checks["missing"]["status"] == checks["duplicate"]["status"] == "unavailable"
    assert checks["local"]["status"] == "local_custody_not_assessed"
    calls_before = len(driver.calls)
    assert await rows.read_pricing_snapshot_row_checks(
        driver, metadata(["allowed"], role="allowed_amounts"), max_report_bytes=1
    ) == {
        "allowed": {"status": "unavailable", "full_readiness": "not_assessed", "allowed_amounts_status": "unavailable"}
    }
    assert len(driver.calls) == calls_before


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [
        asyncpg.UndefinedTableError("synthetic"),
        asyncio.CancelledError(),
        asyncpg.InsufficientPrivilegeError("synthetic"),
    ],
)
async def test_only_recovered_missing_storage_is_optional(failure):
    driver = SnapshotDriver()
    driver.failure = failure
    if isinstance(failure, rows._OPTIONAL_STORAGE_ERRORS):
        checks = await rows.read_pricing_snapshot_row_checks(driver, metadata(["one"]), max_report_bytes=100)
        assert checks["one"]["status"] == "unavailable"
    else:
        with pytest.raises(type(failure)) as caught:
            await rows.read_pricing_snapshot_row_checks(driver, metadata(["one"]), max_report_bytes=100)
        assert caught.value is failure
    expected_lifecycle = (
        ["start", "rollback", "start", "release"]
        if isinstance(failure, rows._OPTIONAL_STORAGE_ERRORS)
        else ["start", "rollback"]
    )
    assert driver.lifecycle == expected_lifecycle and driver.is_in_transaction()


@pytest.mark.asyncio
async def test_bound_set_identity_and_rollback_uncertainty_fail_closed():
    driver = SnapshotDriver([{**strict_snapshot_row(), "snapshot_id": "other"}])
    with pytest.raises(ValueError, match="identity_invalid"):
        await rows.read_pricing_snapshot_row_checks(driver, metadata(["one"]), max_report_bytes=16 * 1024 * 1024)
    driver = SnapshotDriver()
    driver.failure = asyncpg.UndefinedTableError("synthetic")
    driver.rollback_error = asyncio.CancelledError()
    with pytest.raises(asyncpg.UndefinedTableError) as caught:
        await rows.read_pricing_snapshot_row_checks(driver, metadata(["one"]), max_report_bytes=1)
    assert caught.value is driver.failure


@pytest.mark.asyncio
async def test_actual_metadata_consumer_checks_all_sibling_snapshots(monkeypatch):
    sibling = _binding_row(snapshot_id="sibling", binding_ordinal=1, expected_binding_count=2)
    bindings = [_binding_row(expected_binding_count=2), sibling]
    driver = SnapshotDriver([{**strict_snapshot_row(), "snapshot_id": REFERENCE["snapshot_id"]}], bindings)

    async def retained_export(*args, **kwargs):
        return SimpleNamespace(
            records=(ApprovedNetworkCatalogEvidenceRecord(1, 3, 2, "retained"),),
            approved_revision=4,
            approved_map_sha256="a" * 64,
            request_sha256="b" * 64,
            request_bytes=json.dumps(
                {"networks": [{"network_id": 1, "pricing_refs": [REFERENCE], "benefit_refs": None}]}
            ).encode(),
        )

    monkeypatch.setattr(lookup, "read_approved_network_catalog_evidence", retained_export)
    extended_report = await lookup.append_pricing_binding_metadata(
        driver,
        deepcopy(REPORT),
        object(),
        control_schema="sample_control",
        max_report_bytes=16 * 1024 * 1024,
    )
    assert len(driver.calls) == 6 and driver.lifecycle == ["start", "release"] * 3
    provenance = extended_report["provenance"]["pricing_binding_metadata"]
    assert provenance["snapshot_row_checks"]["sibling"]["status"] == "unavailable"
    assert provenance["snapshot_row_checks"][REFERENCE["snapshot_id"]]["status"] == "published_row_validated"
    assert provenance["networks"][0]["pricing_refs"][0]["resolved"] is False
    assert extended_report["targets"] == REPORT["targets"] and extended_report["totals"] == REPORT["totals"]


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["read_committed", "writeable", "ended"])
async def test_actual_native_transaction_contract_is_required(mode):
    driver = SnapshotDriver([{**strict_snapshot_row(), "snapshot_id": "one"}])
    if mode == "read_committed":
        driver.isolation = "read committed"
    if mode == "writeable":
        driver.read_only = "off"
    if mode == "ended":
        driver.in_transaction = False
    with pytest.raises(ValueError, match="transaction_invalid"):
        await rows.read_pricing_snapshot_row_checks(driver, metadata(["one"]), max_report_bytes=16 * 1024 * 1024)
    assert driver.lifecycle == ([] if mode == "ended" else ["start", "rollback"])


@pytest.mark.asyncio
async def test_snapshot_payload_byte_refusal_and_no_cache_are_explicit():
    driver = SnapshotDriver([{**strict_snapshot_row(), "snapshot_id": "one"}])
    checks = await rows.read_pricing_snapshot_row_checks(driver, metadata(["one"]), max_report_bytes=1)
    assert checks["one"]["status"] == "unavailable"
    driver.snapshots[0]["attested_coverage_scope_id"] = "f" * 64
    checks = await rows.read_pricing_snapshot_row_checks(driver, metadata(["one"]), max_report_bytes=16 * 1024 * 1024)
    assert checks["one"]["status"] == "unavailable" and len(driver.calls) == 6


@pytest.mark.asyncio
async def test_v4_manifest_row_does_not_claim_persisted_graph_or_finalizer():
    driver = SnapshotDriver([{**strict_snapshot_row(strict_v4_serving_index()), "snapshot_id": "v4"}])
    checks = await rows.read_pricing_snapshot_row_checks(driver, metadata(["v4"]), max_report_bytes=16 * 1024 * 1024)
    proof = checks["v4"]
    assert proof["status"] == "published_row_validated" and proof["storage_generation"] == "shared_blocks_v4"
    assert proof["unresolved_reason"] == "full_readiness_not_proven" and proof["full_readiness"] == "not_assessed"
    assert proof["source_count"] == 2 and len(proof["source_set_sha256"]) == 64
    assert proof["shared_descriptor_status"] == "unavailable"
    assert len(driver.calls) == 6 and "ptg2_v4_snapshot_map_root" not in driver.calls[1][0]
