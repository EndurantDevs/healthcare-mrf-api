# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import struct
from types import SimpleNamespace
from typing import AsyncIterator
from unittest.mock import AsyncMock
import zlib

import asyncpg
import pytest

import process.uhc_semantic_build_store as store
import process.uhc_semantic_stage_verifier as stage_verifier
import process.uhc_semantic_verifier_identity as verifier_identity
from process.uhc_provider_file_source_identity import UHC_PROVIDER_FILE_SOURCE_ID
from process.uhc_provider_quarantine_contract import (
    UHC_PROVIDER_QUARANTINE_CONTRACT_ID,
    UHC_PROVIDER_QUARANTINE_REASON_INVALID_NPI_CHECKSUM,
    UhcProviderQuarantine,
    quarantine_identity_set_sha256,
    validate_provider_quarantine_fact,
)
from process.uhc_provider_quarantine_raw_verifier import (
    UhcProviderQuarantineRawSource,
)
from process.uhc_provider_quarantine_record import (
    UhcProviderQuarantineRecordCensus,
)
from process.uhc_semantic_evidence import UhcNpiEvidenceSummary
from process.uhc_semantic_stage_verifier import (
    _evidence_identity,
    _fact_identity,
    verify_sealed_uhc_semantic_build,
    verify_uhc_semantic_stage,
)


def _digest(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


def _identity(**overrides: object) -> store.UhcSemanticBuildIdentity:
    identity_by_field = {
        "catalog_set_sha256": _digest("catalog"),
        "source_file_id": _digest("source"),
        "artifact_sha256": _digest("artifact"),
        "raw_contract_version": 2,
        "raw_range_count": 4,
        "manifest_sha256": _digest("manifest"),
        "range_set_sha256": _digest("ranges"),
        "raw_record_count": 4,
        "raw_producer_build_id": "fixture-producer-v1",
        "collection_kind": "provider_membership",
        "encoder_sha256": _digest("encoder"),
    }
    identity_by_field.update(overrides)
    return store.UhcSemanticBuildIdentity(**identity_by_field)


def _native_report(identity: store.UhcSemanticBuildIdentity) -> dict[str, object]:
    """Build one exact native-report fixture for the supplied identity."""

    blocks = [
        {
            "range_ordinal": ordinal,
            "record_start": ordinal,
            "record_count": 1,
            "fact_count": 1,
            "compressed_bytes": 10,
            "compressed_payload_sha256": _digest(f"payload-{ordinal}"),
            "semantic_block_sha256": _digest(f"semantic-{ordinal}"),
        }
        for ordinal in range(identity.raw_range_count)
    ]
    evidence_ranges = [
        {
            "range_ordinal": ordinal,
            "evidence_count": 1,
            "run_count": 1,
            "layout_sha256": _digest(f"layout-{ordinal}"),
        }
        for ordinal in range(identity.raw_range_count)
    ]
    fact_count = identity.raw_range_count
    return {
        "contract_id": store.UHC_SEMANTIC_CONTRACT_ID,
        "contract_version": store.UHC_SEMANTIC_CONTRACT_VERSION,
        "copy_format_id": store.UHC_SEMANTIC_COPY_FORMAT_ID,
        "source_id": store.UHC_SEMANTIC_SOURCE_ID,
        "encoder_sha256": identity.encoder_sha256,
        "lineage": {
            "artifact_sha256": identity.artifact_sha256,
            "manifest_sha256": identity.manifest_sha256,
            "range_set_sha256": identity.range_set_sha256,
            "source_file_id": identity.source_file_id,
            "source_binding_id": (
                f"{identity.catalog_set_sha256}/{identity.source_file_id}"
            ),
            "collection_kind": identity.collection_kind,
        },
        "fact_count": fact_count,
        "evidence_count": fact_count,
        "quarantine_count": 0,
        "quarantine_identity_set_sha256": hashlib.sha256(b"").hexdigest(),
        "fact_set_sha256": _digest("facts"),
        "record_identity_set_sha256": _digest("records"),
        "evidence_identity_set_sha256": _digest("evidence"),
        "evidence_layout_set_sha256": _digest("evidence-layout"),
        "output_bytes": 1024,
        "output_sha256": _digest("copy-output"),
        "copy_row_count": fact_count + identity.raw_range_count,
        "counters": _native_counter_by_field(fact_count),
        "fact_blocks": blocks,
        "evidence_ranges": evidence_ranges,
    }


def _native_counter_by_field(fact_count: int) -> dict[str, int]:
    return {
        "raw_provider_records": fact_count,
        "raw_plan_records": 0,
        "raw_individual_records": fact_count,
        "raw_facility_records": 0,
        "raw_address_rows": fact_count,
        "raw_provider_plan_rows": fact_count,
        "invalid_npi_count": 0,
        "invalid_npi_individual_records": 0,
        "invalid_npi_facility_records": 0,
        "invalid_npi_address_rows": 0,
        "invalid_npi_provider_plan_rows": 0,
    }


def _native_report_with_quarantine(
    identity: store.UhcSemanticBuildIdentity,
) -> dict[str, object]:
    report = deepcopy(_native_report(identity))
    report["evidence_count"] = 3
    report["quarantine_count"] = 1
    report["quarantine_identity_set_sha256"] = _digest("quarantine")
    report["copy_row_count"] = 7
    report["evidence_ranges"][1]["evidence_count"] = 0
    report["evidence_ranges"][1]["run_count"] = 0
    report["counters"].update(
        invalid_npi_count=1,
        invalid_npi_individual_records=1,
        invalid_npi_facility_records=0,
        invalid_npi_address_rows=1,
        invalid_npi_provider_plan_rows=1,
    )
    return report


def test_build_identity_is_exact_and_stage_is_private(monkeypatch) -> None:
    identity = _identity()
    original_build_id = identity.semantic_build_id

    assert len(original_build_id) == 64
    assert original_build_id == _identity().semantic_build_id
    identity_mutations = (
        {"catalog_set_sha256": _digest("other catalog")},
        {"source_file_id": _digest("other source")},
        {"artifact_sha256": _digest("other artifact")},
        {"raw_contract_version": 3},
        {"raw_range_count": 5},
        {"manifest_sha256": _digest("other manifest")},
        {"range_set_sha256": _digest("other ranges")},
        {"raw_record_count": 5},
        {"raw_producer_build_id": "fixture-producer-v2"},
        {"collection_kind": "plan_reference"},
        {"encoder_sha256": _digest("other encoder")},
        {
            "semantic_verifier_sha256": _digest(
                "other verifier dependency set"
            )
        },
    )
    assert all(
        original_build_id != _identity(**mutation).semantic_build_id
        for mutation in identity_mutations
    )
    assert identity.stage_relation.startswith("provider_directory_uhc_sem_")
    assert len(identity.stage_relation) <= 63
    assert len(store.UHC_SEMANTIC_COPY_COLUMNS) == 11
    create_sql = store._stage_create_sql('"mrf"."stage"')
    assert "CREATE TABLE" in create_sql
    assert "payload_bytes bytea" in create_sql
    assert "conflict_signature_pack bytea" in create_sql
    assert "PRIMARY KEY" not in create_sql
    assert "UNIQUE" not in create_sql


def test_verifier_identity_binds_actual_dependency_bytes(
    monkeypatch,
    tmp_path,
) -> None:
    dependency_names = verifier_identity._DEPENDENCY_NAMES
    assert {
        "uhc_provider_quarantine_contract.py",
        "uhc_provider_quarantine_record.py",
        "uhc_provider_quarantine_raw_verifier.py",
        "uhc_retained_range_manifest.py",
        "uhc_retained_types.py",
        "uhc_semantic_build_store.py",
        "uhc_semantic_evidence.py",
        "uhc_semantic_stage_verifier.py",
    } <= set(dependency_names)
    for dependency_name in dependency_names:
        (tmp_path / dependency_name).write_text(
            f"dependency={dependency_name}\n",
            encoding="utf-8",
        )
    monkeypatch.setattr(
        verifier_identity,
        "__file__",
        str(tmp_path / "uhc_semantic_verifier_identity.py"),
    )
    verifier_identity.semantic_verifier_identity_sha256.cache_clear()
    first_identity = verifier_identity.semantic_verifier_identity_sha256()
    changed_path = tmp_path / "uhc_retained_range_manifest.py"
    changed_path.write_text("dependency=changed\n", encoding="utf-8")
    verifier_identity.semantic_verifier_identity_sha256.cache_clear()
    second_identity = verifier_identity.semantic_verifier_identity_sha256()

    assert first_identity != second_identity
    verifier_identity.semantic_verifier_identity_sha256.cache_clear()


def test_native_and_independent_reports_must_match_exactly() -> None:
    identity = _identity()
    native = _native_report(identity)
    fact_count, evidence_count, _, blocks, ranges = store._validate_native_report(
        identity,
        native,
    )

    assert fact_count == 4
    assert evidence_count == 4
    assert len(blocks) == len(ranges) == 4
    verifier_by_field = {
        field: native[field]
        for field in (
            "fact_count",
            "evidence_count",
            "quarantine_count",
            "quarantine_identity_set_sha256",
            "fact_set_sha256",
            "record_identity_set_sha256",
            "evidence_identity_set_sha256",
            "evidence_layout_set_sha256",
            "output_bytes",
            "output_sha256",
            "copy_row_count",
        )
    }
    verifier_by_field["verifier_sha256"] = identity.semantic_verifier_sha256
    assert store._assert_verifier_report(
        identity,
        native,
        verifier_by_field,
    ) == identity.semantic_verifier_sha256
    verifier_by_field["fact_set_sha256"] = _digest("wrong")
    with pytest.raises(store.UhcSemanticBuildError, match="fact_set_sha256"):
        store._assert_verifier_report(identity, native, verifier_by_field)

    verifier_by_field["fact_set_sha256"] = native["fact_set_sha256"]
    verifier_by_field["verifier_sha256"] = _digest("wrong verifier")
    with pytest.raises(store.UhcSemanticBuildError, match="identity changed"):
        store._assert_verifier_report(identity, native, verifier_by_field)


def test_nonzero_quarantine_report_is_exactly_balanced_and_publicly_aggregated():
    identity = _identity()
    native = _native_report_with_quarantine(identity)

    fact_count, evidence_count, counters, _blocks, _ranges = (
        store._validate_native_report(identity, native)
    )
    combined = store._combined_counters(
        counters,
        UhcNpiEvidenceSummary(
            evidence_count=3,
            distinct_npis=3,
            duplicate_npi_groups=0,
            conflicting_npi_groups=0,
            conflict_counts={},
        ),
    )

    assert (fact_count, evidence_count) == (4, 3)
    assert combined["rejected_counts"] == {
        "invalid_npi_checksum": 1,
        "invalid_npi_checksum_individual_records": 1,
        "invalid_npi_checksum_facility_records": 0,
        "invalid_npi_checksum_address_rows": 1,
        "invalid_npi_checksum_provider_plan_rows": 1,
        "invalid_npi_structure": 0,
        "invalid_npi_structure_individual_records": 0,
        "invalid_npi_structure_facility_records": 0,
        "invalid_npi_structure_address_rows": 0,
        "invalid_npi_structure_provider_plan_rows": 0,
    }


def test_native_report_accepts_started_bucket_quarantine_boundary() -> None:
    identity = _identity(raw_range_count=1, raw_record_count=10_001)
    native = _native_report(identity)
    native.update(
        fact_count=10_001,
        evidence_count=9_999,
        quarantine_count=2,
        quarantine_identity_set_sha256=_digest("two quarantines"),
        copy_row_count=10_000,
    )
    native["fact_blocks"][0].update(record_count=10_001, fact_count=10_001)
    native["evidence_ranges"][0].update(evidence_count=9_999, run_count=1)
    native["counters"].update(
        raw_provider_records=10_001,
        raw_individual_records=10_001,
        raw_address_rows=10_001,
        raw_provider_plan_rows=10_001,
        invalid_npi_count=2,
        invalid_npi_individual_records=2,
        invalid_npi_address_rows=2,
        invalid_npi_provider_plan_rows=2,
    )

    fact_count, evidence_count, counters, _blocks, _ranges = (
        store._validate_native_report(identity, native)
    )

    assert (fact_count, evidence_count) == (10_001, 9_999)
    assert counters["invalid_npi_count"] == 2


def test_structural_quarantine_is_a_bounded_subset_of_native_totals() -> None:
    identity = _identity()
    native = _native_report_with_quarantine(identity)
    native["counters"].update(
        raw_individual_records=3,
        raw_facility_records=1,
        invalid_npi_individual_records=0,
        invalid_npi_facility_records=1,
        invalid_npi_structure_count=1,
        invalid_npi_structure_individual_records=0,
        invalid_npi_structure_facility_records=1,
        invalid_npi_structure_address_rows=1,
        invalid_npi_structure_provider_plan_rows=1,
    )

    _fact_count, _evidence_count, counters, _blocks, _ranges = (
        store._validate_native_report(identity, native)
    )
    rejected = store._combined_counters(
        counters,
        UhcNpiEvidenceSummary(
            evidence_count=3,
            distinct_npis=3,
            duplicate_npi_groups=0,
            conflicting_npi_groups=0,
            conflict_counts={},
        ),
    )["rejected_counts"]

    assert rejected["invalid_npi_checksum"] == 0
    assert rejected["invalid_npi_structure"] == 1
    assert rejected["invalid_npi_structure_facility_records"] == 1


def test_native_report_rejects_partial_structural_counter_group() -> None:
    identity = _identity()
    native = _native_report_with_quarantine(identity)
    native["counters"]["invalid_npi_structure_count"] = 1

    with pytest.raises(
        store.UhcSemanticBuildError,
        match="native quarantine counters are invalid",
    ):
        store._validate_native_report(identity, native)


@pytest.mark.parametrize(
    "mutation",
    (
        "quarantine_count",
        "quarantine_digest",
        "evidence_count",
        "copy_rows",
        "dimension_balance",
        "rate_ceiling",
        "plan_quarantine",
    ),
)
def test_nonzero_quarantine_report_rejects_contract_drift(mutation):
    identity, native_report_by_field = _quarantine_report_for_mutation(
        mutation
    )

    with pytest.raises(store.UhcSemanticBuildError):
        store._validate_native_report(identity, native_report_by_field)


def _quarantine_report_for_mutation(mutation: str):
    identity = _identity()
    native_report_by_field = _native_report_with_quarantine(identity)
    top_level_change_by_name = {
        "quarantine_count": {"quarantine_count": 2},
        "quarantine_digest": {"quarantine_identity_set_sha256": "bad"},
        "evidence_count": {"evidence_count": 4},
        "copy_rows": {"copy_row_count": 8},
    }
    if mutation in top_level_change_by_name:
        native_report_by_field.update(top_level_change_by_name[mutation])
        return identity, native_report_by_field
    if mutation == "dimension_balance":
        native_report_by_field["counters"][
            "invalid_npi_address_rows"
        ] = 0
        return identity, native_report_by_field
    if mutation == "rate_ceiling":
        native_report_by_field.update(
            quarantine_count=2,
            evidence_count=2,
            copy_row_count=6,
        )
        native_report_by_field["counters"].update(
            invalid_npi_count=2,
            invalid_npi_individual_records=2,
            invalid_npi_address_rows=2,
            invalid_npi_provider_plan_rows=2,
        )
        return identity, native_report_by_field
    if mutation == "plan_quarantine":
        identity = _identity(collection_kind="plan_reference")
        native_report_by_field = _native_report_with_quarantine(identity)
        native_report_by_field["counters"].update(
            raw_provider_records=0,
            raw_plan_records=4,
            raw_individual_records=0,
            raw_address_rows=0,
            raw_provider_plan_rows=0,
        )
        return identity, native_report_by_field
    raise AssertionError(mutation)


def test_setwise_evidence_is_the_only_source_of_npi_group_counts() -> None:
    counter_by_field = {
        "raw_provider_records": 10,
        "raw_plan_records": 0,
        "raw_individual_records": 10,
        "raw_facility_records": 0,
        "raw_address_rows": 10,
        "raw_provider_plan_rows": 10,
        "invalid_npi_count": 0,
        "invalid_npi_individual_records": 0,
        "invalid_npi_facility_records": 0,
        "invalid_npi_address_rows": 0,
        "invalid_npi_provider_plan_rows": 0,
    }
    evidence = UhcNpiEvidenceSummary(
        evidence_count=10,
        distinct_npis=8,
        duplicate_npi_groups=2,
        conflicting_npi_groups=1,
        conflict_counts={"names": 1},
    )

    combined = store._combined_counters(counter_by_field, evidence)

    assert combined["distinct_npis"] == 8
    assert combined["duplicate_npi_groups"] == 2
    assert combined["conflicting_npi_groups"] == 1
    assert combined["unknown_field_counts"] == {}
    assert combined["intentional_drop_counts"] == {}


class _Transaction:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *_args: object) -> None:
        return None


class _ClaimConnection:
    def __init__(self, identity: store.UhcSemanticBuildIdentity) -> None:
        self.identity = identity
        self.row: dict[str, object] | None = None
        self.lease_active = False
        self.drop_count = 0
        self.create_count = 0

    def transaction(self) -> _Transaction:
        return _Transaction()

    async def fetchrow(self, query: str, *_args: object):
        if "provider_directory_uhc_source_binding" in query:
            return {"gate": True}
        if "provider_directory_uhc_semantic_build" in query:
            if self.row is None:
                return None
            row = deepcopy(self.row)
            row["lease_active"] = self.lease_active
            return row
        raise AssertionError(query)

    async def execute(self, query: str, *args: object) -> str:
        if "pg_advisory_xact_lock" in query:
            return "SELECT 1"
        if query.lstrip().startswith("DROP TABLE"):
            self.drop_count += 1
            return "DROP TABLE"
        if query.lstrip().startswith("CREATE TABLE"):
            self.create_count += 1
            return "CREATE TABLE"
        if "INSERT INTO" in query:
            return self._insert_build(args)
        if "UPDATE" in query and "attempt_count=attempt_count + 1" in query:
            return self._recover_build(args)
        raise AssertionError(query)

    def _insert_build(self, arguments: tuple[object, ...]) -> str:
        identity = self.identity
        self.row = {
            **store._identity_fields(identity),
            "semantic_build_id": identity.semantic_build_id,
            "status": "building",
            "attempt_count": 1,
            "lease_token": arguments[16],
            "stage_schema": arguments[18],
            "stage_relation": arguments[19],
        }
        self.lease_active = True
        return "INSERT 0 1"

    def _recover_build(self, arguments: tuple[object, ...]) -> str:
        assert self.row is not None
        self.row["status"] = "building"
        self.row["attempt_count"] = int(self.row["attempt_count"]) + 1
        self.row["lease_token"] = arguments[1]
        self.lease_active = True
        return "UPDATE 1"


def test_stale_build_recovery_recreates_only_its_stage_and_sealed_reuses(
    monkeypatch,
) -> None:
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")
    identity = _identity()
    connection = _ClaimConnection(identity)

    first = asyncio.run(store.claim_uhc_semantic_build(connection, identity))
    assert first.attempt_count == 1
    assert not first.sealed_reuse
    assert connection.drop_count == connection.create_count == 1

    connection.lease_active = False
    second = asyncio.run(store.claim_uhc_semantic_build(connection, identity))
    assert second.attempt_count == 2
    assert second.lease_token != first.lease_token
    assert connection.drop_count == connection.create_count == 2

    assert connection.row is not None
    connection.row["status"] = "sealed"
    connection.lease_active = False
    reused = asyncio.run(store.claim_uhc_semantic_build(connection, identity))
    assert reused.sealed_reuse
    assert reused.lease_token is None
    assert reused.attempt_count == 2
    assert connection.drop_count == connection.create_count == 2


def test_live_build_is_not_stolen(monkeypatch) -> None:
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")
    identity = _identity()
    connection = _ClaimConnection(identity)
    asyncio.run(store.claim_uhc_semantic_build(connection, identity))

    with pytest.raises(store.UhcSemanticBuildBusy, match="live lease"):
        asyncio.run(store.claim_uhc_semantic_build(connection, identity))


def _claim(*, sealed_reuse=False, lease_token="lease"):
    identity = _identity()
    return store.UhcSemanticBuildClaim(
        semantic_build_id=identity.semantic_build_id,
        lease_token=lease_token,
        attempt_count=1,
        stage_schema="mrf_test",
        stage_relation=identity.stage_relation,
        sealed_reuse=sealed_reuse,
    )


def _verifier_report(identity):
    native = _native_report(identity)
    report_by_field = {
        field: native[field]
        for field in (
            "fact_count",
            "evidence_count",
            "quarantine_count",
            "quarantine_identity_set_sha256",
            "fact_set_sha256",
            "record_identity_set_sha256",
            "evidence_identity_set_sha256",
            "evidence_layout_set_sha256",
            "output_bytes",
            "output_sha256",
            "copy_row_count",
        )
    }
    report_by_field["verifier_sha256"] = identity.semantic_verifier_sha256
    return report_by_field


def test_schema_identifier_and_hash_guards(monkeypatch):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "bad-schema")
    with pytest.raises(store.UhcSemanticBuildError, match="registry schema"):
        store._schema_name()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf_test")
    assert store._schema_name() == "mrf_test"
    assert store._quoted_identifier("valid_name") == '"valid_name"'
    with pytest.raises(store.UhcSemanticBuildError, match="identifier"):
        store._quoted_identifier("bad-name")
    with pytest.raises(ValueError, match="lowercase SHA"):
        store._require_sha256("bad", "hash")


@pytest.mark.parametrize(
    "overrides",
    [
        {"catalog_set_sha256": "bad"},
        {"source_file_id": "bad"},
        {"artifact_sha256": "bad"},
        {"encoder_sha256": "bad"},
        {"raw_contract_version": 0},
        {"raw_range_count": 3},
        {"raw_range_count": 257},
        {"raw_record_count": 0},
        {"raw_producer_build_id": ""},
        {"collection_kind": "unsupported"},
    ],
)
def test_identity_validation_rejects_every_invalid_dimension(overrides):
    with pytest.raises(ValueError):
        _identity(**overrides).validate()


def test_claim_stage_reference_and_identity_row_guard():
    claim = _claim()
    assert claim.stage_ref.startswith('"mrf_test".')
    identity = _identity()
    row = store._identity_fields(identity)
    store._assert_identity_row(row, identity)
    row["encoder_sha256"] = _digest("different")
    with pytest.raises(store.UhcSemanticBuildError, match="identity mismatch"):
        store._assert_identity_row(row, identity)


@pytest.mark.asyncio
async def test_active_layout_stage_and_recovery_guards():
    connection = SimpleNamespace(
        fetchrow=AsyncMock(return_value=None),
        execute=AsyncMock(return_value="UPDATE 0"),
    )
    with pytest.raises(store.UhcSemanticBuildError, match="active verified"):
        await store._assert_active_raw_layout(
            connection,
            _identity(),
            '"mrf"."binding"',
            '"mrf"."layout"',
        )
    identity = _identity()
    build_row_by_field = {
        **store._identity_fields(identity),
        "stage_schema": "wrong",
        "stage_relation": identity.stage_relation,
        "status": "building",
        "lease_active": False,
    }
    with pytest.raises(store.UhcSemanticBuildError, match="stage identity"):
        store._existing_build_claim(
            build_row_by_field,
            identity,
            "mrf_test",
            identity.stage_relation,
        )
    with pytest.raises(store.UhcSemanticBuildStale, match="during recovery"):
        await store._recover_semantic_build(
            connection,
            '"mrf"."build"',
            identity.semantic_build_id,
            "lease",
            300,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("lease_seconds", [29, 3601])
async def test_claim_rejects_invalid_lease_duration(lease_seconds):
    with pytest.raises(ValueError, match="30..=3600"):
        await store.claim_uhc_semantic_build(
            object(),
            _identity(),
            lease_seconds=lease_seconds,
        )


@pytest.mark.asyncio
async def test_heartbeat_covers_sealed_invalid_lost_and_success():
    for claim in (
        _claim(sealed_reuse=True),
        _claim(lease_token=None),
    ):
        with pytest.raises(store.UhcSemanticBuildStale, match="no lease"):
            await store.heartbeat_uhc_semantic_build(object(), claim)
    with pytest.raises(ValueError, match="30..=3600"):
        await store.heartbeat_uhc_semantic_build(
            object(),
            _claim(),
            lease_seconds=1,
        )
    connection = SimpleNamespace(execute=AsyncMock(return_value="UPDATE 0"))
    with pytest.raises(store.UhcSemanticBuildStale, match="lease was lost"):
        await store.heartbeat_uhc_semantic_build(connection, _claim())
    connection.execute.return_value = "UPDATE 1"
    await store.heartbeat_uhc_semantic_build(connection, _claim())


class _CopyConnection:
    def __init__(
        self,
        *,
        owns=True,
        copy_status="COPY 3",
        update_status="UPDATE 1",
    ):
        self.owns = owns
        self.copy_status = copy_status
        self.update_status = update_status

    def transaction(self):
        return _Transaction()

    async def fetchval(self, *_args):
        return self.owns

    async def copy_to_table(self, *_args, **_kwargs):
        return self.copy_status

    async def execute(self, *_args):
        return self.update_status


async def _empty_copy_stream():
    if False:
        yield b""


@pytest.mark.asyncio
async def test_copy_stage_covers_ownership_status_and_row_count_guards():
    with pytest.raises(store.UhcSemanticBuildStale, match="cannot be copied"):
        await store.copy_uhc_semantic_stage(
            object(),
            _claim(sealed_reuse=True),
            _empty_copy_stream(),
        )
    with pytest.raises(store.UhcSemanticBuildStale, match="lease is stale"):
        await store.copy_uhc_semantic_stage(
            _CopyConnection(owns=False),
            _claim(),
            _empty_copy_stream(),
        )
    with pytest.raises(store.UhcSemanticBuildError, match="row count is missing"):
        await store.copy_uhc_semantic_stage(
            _CopyConnection(copy_status="COPY"),
            _claim(),
            _empty_copy_stream(),
        )
    with pytest.raises(store.UhcSemanticBuildStale, match="lost during COPY"):
        await store.copy_uhc_semantic_stage(
            _CopyConnection(update_status="UPDATE 0"),
            _claim(),
            _empty_copy_stream(),
        )
    assert await store.copy_uhc_semantic_stage(
        _CopyConnection(copy_status="COPY 7"),
        _claim(),
        _empty_copy_stream(),
    ) == 7


@pytest.mark.asyncio
async def test_quarantine_covers_identifier_ownership_and_success():
    with pytest.raises(store.UhcSemanticBuildStale, match="cannot quarantine"):
        await store.quarantine_uhc_semantic_build(
            object(),
            _claim(sealed_reuse=True),
            failure_code="failure",
        )
    for failure_code in ("", "Uppercase", "x" * 129):
        with pytest.raises(ValueError, match="stable lowercase"):
            await store.quarantine_uhc_semantic_build(
                object(),
                _claim(),
                failure_code=failure_code,
            )
    connection = SimpleNamespace(execute=AsyncMock(return_value="UPDATE 0"))
    with pytest.raises(store.UhcSemanticBuildStale, match="before quarantine"):
        await store.quarantine_uhc_semantic_build(
            connection,
            _claim(),
            failure_code="native_failure",
        )
    connection.execute.return_value = "UPDATE 1"
    await store.quarantine_uhc_semantic_build(
        connection,
        _claim(),
        failure_code="native_failure",
    )


def test_report_primitive_guards():
    assert store._mapping({}, "field") == {}
    with pytest.raises(store.UhcSemanticBuildError, match="not an object"):
        store._mapping([], "field")
    for value in (True, "1", -1):
        with pytest.raises(store.UhcSemanticBuildError, match="count is invalid"):
            store._report_int({"count": value}, "count")
    with pytest.raises(store.UhcSemanticBuildError, match="count is invalid"):
        store._report_int({"count": 0}, "count", positive=True)
    assert store._report_int({"count": 0}, "count") == 0
    with pytest.raises(store.UhcSemanticBuildError, match="hash is invalid"):
        store._report_sha256({"hash": "bad"}, "hash")


def _mutated_native_report(mutation):
    identity = _identity()
    report = deepcopy(_native_report(identity))
    match mutation:
        case (
            "contract_id"
            | "contract_version"
            | "copy_format_id"
            | "source_id"
            | "encoder_sha256"
        ):
            report[mutation] = "wrong"
        case "lineage_type":
            report["lineage"] = []
        case "lineage_source_file":
            report["lineage"]["source_file_id"] = "wrong"
        case "lineage_artifact":
            report["lineage"]["artifact_sha256"] = _digest("wrong artifact")
        case "lineage_manifest":
            report["lineage"]["manifest_sha256"] = _digest("wrong manifest")
        case "lineage_range_set":
            report["lineage"]["range_set_sha256"] = _digest("wrong ranges")
        case "lineage_source_binding":
            report["lineage"]["source_binding_id"] = "wrong"
        case "lineage_collection_kind":
            report["lineage"]["collection_kind"] = "plan_reference"
        case "fact_count":
            report["fact_count"] = 0
        case "fact_count_layout":
            report["fact_count"] = identity.raw_record_count - 1
        case "evidence_count":
            report["evidence_count"] = 3
        case "quarantine_count_ceiling":
            report["quarantine_count"] = (
                store.UHC_PROVIDER_QUARANTINE_MAX_COUNT + 1
            )
        case "proof_hash":
            report["fact_set_sha256"] = "bad"
        case "output_bytes":
            report["output_bytes"] = 0
        case "copy_rows":
            report["copy_row_count"] += 1
        case "counters_type":
            report["counters"] = []
        case "fact_blocks_type":
            report["fact_blocks"] = {}
        case "fact_blocks_count":
            report["fact_blocks"] = []
        case "ranges_type":
            report["evidence_ranges"] = {}
        case "ranges_count":
            report["evidence_ranges"] = []
        case "counter_balance":
            report["counters"]["raw_provider_records"] += 1
        case "quarantine_counter_balance":
            report["counters"]["invalid_npi_count"] = 1
        case _:
            raise AssertionError(mutation)
    return identity, report


@pytest.mark.parametrize(
    "mutation",
    [
        "contract_id",
        "contract_version",
        "copy_format_id",
        "source_id",
        "encoder_sha256",
        "lineage_type",
        "lineage_source_file",
        "lineage_artifact",
        "lineage_manifest",
        "lineage_range_set",
        "lineage_source_binding",
        "lineage_collection_kind",
        "fact_count",
        "evidence_count",
        "proof_hash",
        "output_bytes",
        "copy_rows",
        "counters_type",
        "fact_blocks_type",
        "fact_blocks_count",
        "ranges_type",
        "ranges_count",
        "counter_balance",
    ],
)
def test_native_report_rejects_every_contract_mutation(mutation):
    identity, report = _mutated_native_report(mutation)
    with pytest.raises(store.UhcSemanticBuildError):
        store._validate_native_report(identity, report)


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (
            "fact_count_layout",
            "UHC semantic native fact count does not match admitted raw layout",
        ),
        (
            "quarantine_count_ceiling",
            "UHC semantic native quarantine count exceeds its ceiling",
        ),
        (
            "quarantine_counter_balance",
            "UHC semantic native quarantine counters do not balance",
        ),
    ],
)
def test_native_report_rejects_each_bounded_quarantine_guard(
    mutation,
    message,
):
    identity, report = _mutated_native_report(mutation)
    with pytest.raises(
        store.UhcSemanticBuildError,
        match=f"^{re.escape(message)}$",
    ):
        store._validate_native_report(identity, report)


def test_plan_reference_report_requires_zero_evidence():
    identity = _identity(collection_kind="plan_reference")
    report = _native_report(identity)
    report["evidence_count"] = 0
    report["copy_row_count"] = identity.raw_range_count
    report["counters"] = {
        "raw_provider_records": 0,
        "raw_plan_records": identity.raw_range_count,
        "raw_individual_records": 0,
        "raw_facility_records": 0,
        "raw_address_rows": 0,
        "raw_provider_plan_rows": 0,
        "invalid_npi_count": 0,
        "invalid_npi_individual_records": 0,
        "invalid_npi_facility_records": 0,
        "invalid_npi_address_rows": 0,
        "invalid_npi_provider_plan_rows": 0,
    }
    assert store._validate_native_report(identity, report)[1] == 0


def test_counter_proof_and_seal_proof_include_verifier_evidence():
    identity = _identity()
    native = _native_report(identity)
    verifier = _verifier_report(identity)
    evidence = UhcNpiEvidenceSummary(
        evidence_count=4,
        distinct_npis=4,
        duplicate_npi_groups=0,
        conflicting_npi_groups=0,
        conflict_counts={},
        proof_sha256=_digest("npi-proof"),
    )
    combined = store._combined_counters(native["counters"], evidence, verifier)
    assert combined["copy_proof"]["output_bytes"] == native["output_bytes"]
    assert combined["npi_evidence_proof_sha256"] == _digest("npi-proof")
    proof = store._semantic_seal_proof(identity, native, verifier)
    assert proof.verifier_sha256 == identity.semantic_verifier_sha256
    assert len(store._stage_index_sql(_claim())) == 4
    assert "rows_valid" in store._stage_shape_sql(_claim().stage_ref)


@pytest.mark.asyncio
async def test_prepare_indexes_covers_reuse_stale_and_success():
    with pytest.raises(store.UhcSemanticBuildStale, match="need no indexes"):
        await store.prepare_uhc_semantic_stage_indexes(
            object(),
            _claim(sealed_reuse=True),
        )
    connection = SimpleNamespace(
        transaction=lambda: _Transaction(),
        fetchval=AsyncMock(return_value=False),
        execute=AsyncMock(),
    )
    with pytest.raises(store.UhcSemanticBuildStale, match="lease is stale"):
        await store.prepare_uhc_semantic_stage_indexes(connection, _claim())
    connection.fetchval.return_value = True
    await store.prepare_uhc_semantic_stage_indexes(connection, _claim())
    assert connection.execute.await_count == 5


def _build_row(identity, **overrides):
    build_row_by_field = {
        **store._identity_fields(identity),
        "status": "building",
        "lease_token": "lease",
        "lease_active": True,
    }
    build_row_by_field.update(overrides)
    return build_row_by_field


@pytest.mark.asyncio
async def test_lock_for_seal_covers_missing_state_and_identity():
    identity = _identity()
    connection = SimpleNamespace(fetchrow=AsyncMock(return_value=None))
    with pytest.raises(store.UhcSemanticBuildStale, match="seal lease is stale"):
        await store._lock_build_for_seal(
            connection,
            _claim(),
            identity,
            '"mrf"."build"',
        )
    for overrides in (
        {"status": "sealed"},
        {"lease_token": "other"},
        {"lease_active": False},
    ):
        connection.fetchrow.return_value = _build_row(identity, **overrides)
        with pytest.raises(store.UhcSemanticBuildStale):
            await store._lock_build_for_seal(
                connection,
                _claim(),
                identity,
                '"mrf"."build"',
            )
    connection.fetchrow.return_value = _build_row(identity)
    assert await store._lock_build_for_seal(
        connection,
        _claim(),
        identity,
        '"mrf"."build"',
    ) == _build_row(identity)


@pytest.mark.asyncio
async def test_stage_shape_covers_every_summary_and_metadata_mismatch():
    identity = _identity()
    native = _native_report(identity)
    valid_shape_by_field = {
        "fact_block_count": 4,
        "fact_count": 4,
        "evidence_count": 4,
        "rows_valid": True,
    }
    for shape in (
        None,
        {**valid_shape_by_field, "fact_block_count": 3},
        {**valid_shape_by_field, "fact_count": 3},
        {**valid_shape_by_field, "evidence_count": 3},
        {**valid_shape_by_field, "rows_valid": False},
    ):
        connection = SimpleNamespace(
            fetchrow=AsyncMock(return_value=shape),
            fetch=AsyncMock(return_value=native["fact_blocks"]),
        )
        with pytest.raises(store.UhcSemanticBuildError, match="shape proof"):
            await store._assert_semantic_stage_shape(
                connection,
                _claim().stage_ref,
                identity,
                4,
                4,
                native["fact_blocks"],
            )
    connection = SimpleNamespace(
        fetchrow=AsyncMock(return_value=valid_shape_by_field),
        fetch=AsyncMock(return_value=[]),
    )
    with pytest.raises(store.UhcSemanticBuildError, match="metadata disagrees"):
        await store._assert_semantic_stage_shape(
            connection,
            _claim().stage_ref,
            identity,
            4,
            4,
            native["fact_blocks"],
        )
    connection.fetch.return_value = native["fact_blocks"]
    await store._assert_semantic_stage_shape(
        connection,
        _claim().stage_ref,
        identity,
        4,
        4,
        native["fact_blocks"],
    )


@pytest.mark.asyncio
async def test_store_seal_covers_expired_and_success():
    identity = _identity()
    native = _native_report(identity)
    proof = store._semantic_seal_proof(
        identity,
        native,
        _verifier_report(identity),
    )
    connection = SimpleNamespace(fetchrow=AsyncMock(return_value=None))
    with pytest.raises(store.UhcSemanticBuildStale, match="expired"):
        await store._store_semantic_seal(
            connection,
            '"mrf"."build"',
            _claim(),
            native,
            proof,
            {},
        )
    sealed_row_by_field = {
        "attempt_count": 1,
        "sealed_at": datetime.now(timezone.utc),
    }
    connection.fetchrow.return_value = sealed_row_by_field
    assert await store._store_semantic_seal(
        connection,
        '"mrf"."build"',
        _claim(),
        native,
        proof,
        {},
    ) == sealed_row_by_field


@pytest.mark.asyncio
async def test_seal_rejects_reuse_and_claim_identity_mismatch():
    identity = _identity()
    with pytest.raises(store.UhcSemanticBuildStale, match="already sealed"):
        await store.seal_uhc_semantic_build(
            object(),
            _claim(sealed_reuse=True),
            identity,
            {},
            {},
        )
    wrong_claim = store.UhcSemanticBuildClaim(
        semantic_build_id="wrong",
        lease_token="lease",
        attempt_count=1,
        stage_schema="mrf_test",
        stage_relation=identity.stage_relation,
        sealed_reuse=False,
    )
    with pytest.raises(store.UhcSemanticBuildError, match="claim identity"):
        await store.seal_uhc_semantic_build(
            object(),
            wrong_claim,
            identity,
            {},
            {},
        )


@pytest.mark.asyncio
async def test_load_sealed_build_covers_absent_invalid_and_valid_proof():
    identity = _identity()
    connection = SimpleNamespace(fetchrow=AsyncMock(return_value=None))
    assert await store.load_sealed_uhc_semantic_build(connection, identity) is None
    valid_build_by_field = {
        **store._identity_fields(identity),
        **{
            field: _digest(field)
            for field in (
                "fact_set_sha256",
                "record_identity_set_sha256",
                "evidence_identity_set_sha256",
                "evidence_layout_set_sha256",
                "verifier_sha256",
            )
        },
    }
    invalid_build_by_field = dict(valid_build_by_field)
    invalid_build_by_field["verifier_sha256"] = "bad"
    connection.fetchrow.return_value = invalid_build_by_field
    with pytest.raises(store.UhcSemanticBuildError, match="proof is invalid"):
        await store.load_sealed_uhc_semantic_build(connection, identity)
    connection.fetchrow.return_value = valid_build_by_field
    assert await store.load_sealed_uhc_semantic_build(
        connection,
        identity,
    ) == valid_build_by_field


def _postgres_json_bytes(encoded_value: object) -> bytes:
    return json.dumps(
        encoded_value,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode()


def _postgres_line_hash(encoded_values: list[bytes]) -> str:
    return hashlib.sha256(b"\n".join(encoded_values)).hexdigest()


def _postgres_signature_pack(encoded_values: list[str]) -> bytes:
    assert len(encoded_values) == 9
    return b"".join(
        hashlib.sha256(encoded_value.encode()).digest()
        for encoded_value in encoded_values
    )


def _postgres_fixture_fact(
    identity: store.UhcSemanticBuildIdentity,
    ordinal: int,
) -> tuple[tuple[object, ...], dict[str, object], bytes, object | None]:
    quarantine = None
    if ordinal == 1:
        fact_by_field = {
            "_healthporta_quarantine": {
                "contract_id": UHC_PROVIDER_QUARANTINE_CONTRACT_ID,
                "reason": UHC_PROVIDER_QUARANTINE_REASON_INVALID_NPI_CHECKSUM,
                "source_file_id": identity.source_file_id,
                "range_ordinal": ordinal,
                "occurrence_ordinal": ordinal,
                "record_sha256": _digest("rejected-source-record"),
            }
        }
        quarantine = validate_provider_quarantine_fact(
            fact_by_field,
            expected_source_file_id=identity.source_file_id,
            expected_range_ordinal=ordinal,
            expected_occurrence_ordinal=ordinal,
        )
        assert quarantine is not None
    else:
        fact_by_field = {"npi": "1003821380", "ordinal": ordinal}
    fact_payload = _postgres_json_bytes(fact_by_field)
    payload_hash = hashlib.sha256(fact_payload).hexdigest()
    fact_identity = _fact_identity(
        identity.source_file_id,
        "ProviderMembershipRecord",
        ordinal,
        payload_hash,
    )
    semantic_hash = hashlib.sha256(fact_identity).hexdigest()
    compressed = zlib.compress(fact_payload + b"\n", level=1)
    compressed_hash = hashlib.sha256(compressed).hexdigest()
    fact_block_by_field = {
        "range_ordinal": ordinal,
        "record_start": ordinal,
        "record_count": 1,
        "fact_count": 1,
        "compressed_bytes": len(compressed),
        "compressed_payload_sha256": compressed_hash,
        "semantic_block_sha256": semantic_hash,
    }
    stage_record = (
        1, ordinal, None, None, ordinal, 1, None, None,
        compressed_hash, semantic_hash, compressed,
    )
    return stage_record, fact_block_by_field, fact_identity, quarantine


def _postgres_fixture_evidence(
    ordinal: int,
) -> tuple[tuple[object, ...], dict[str, object], bytes]:
    signature_pack = _postgres_signature_pack(
        [
            '"accepting"', '[{\\"address\\":\\"1 Main St\\"}]',
            '"2026-07-01"', "null", "null", '"F"',
            '"Ada"' if ordinal < 2 else '"Augusta"',
            "INDIVIDUAL", '["Family Medicine"]',
        ]
    )
    evidence_by_field = {
        "occurrence_ordinal": ordinal,
        "npi": "1003821380",
        "conflict_signature_pack": signature_pack,
    }
    evidence_identity = _evidence_identity(evidence_by_field)
    stage_record = (
        2, ordinal, 0, ordinal, None, None, "1003821380",
        signature_pack, None, None, None,
    )
    layout_hash = hashlib.sha256(
        _postgres_json_bytes(
            [ordinal, 0, 1, hashlib.sha256(evidence_identity).hexdigest()]
        )
    ).hexdigest()
    evidence_range_by_field = {
        "range_ordinal": ordinal,
        "evidence_count": 1,
        "run_count": 1,
        "layout_sha256": layout_hash,
    }
    return stage_record, evidence_range_by_field, evidence_identity


def _postgres_fixture_counters() -> dict[str, int]:
    return {
        "raw_provider_records": 4, "raw_plan_records": 0,
        "raw_individual_records": 4, "raw_facility_records": 0,
        "raw_address_rows": 4, "raw_provider_plan_rows": 4,
        "raw_formulary_entries": 0, "named_facility_records": 0,
        "facility_type_values": 0, "dated_records": 4,
        "accepting_newpt_records": 4, "accepting_nopt_records": 0,
        "accepting_null_records": 0, "invalid_phone_count": 0,
        "valid_phone_count": 4, "multi_address_provider_records": 0,
        "plan_year_rows": 4, "invalid_npi_count": 1,
        "invalid_npi_individual_records": 1,
        "invalid_npi_facility_records": 0,
        "invalid_npi_address_rows": 1,
        "invalid_npi_provider_plan_rows": 1,
    }


def _postgres_fixture_proof_hash(
    proof_records: list[dict[str, object]],
    fields: tuple[str, ...],
    *,
    contract_prefix: bool,
) -> str:
    return _postgres_line_hash(
        [
            _postgres_json_bytes(
                [
                    *([store.UHC_SEMANTIC_CONTRACT_ID] if contract_prefix else []),
                    *(proof_record[field] for field in fields),
                ]
            )
            for proof_record in proof_records
        ]
    )


def _postgres_fixture_native_report(
    identity: store.UhcSemanticBuildIdentity,
    fact_blocks: list[dict[str, object]],
    evidence_ranges: list[dict[str, object]],
    fact_identities: list[bytes],
    evidence_identities: list[bytes],
    quarantines: list[UhcProviderQuarantine],
) -> dict[str, object]:
    fact_fields = (
        "range_ordinal", "record_start", "record_count", "fact_count",
        "compressed_payload_sha256", "semantic_block_sha256",
    )
    evidence_fields = (
        "range_ordinal", "evidence_count", "run_count", "layout_sha256",
    )
    return {
        "contract_id": store.UHC_SEMANTIC_CONTRACT_ID,
        "contract_version": store.UHC_SEMANTIC_CONTRACT_VERSION,
        "copy_format_id": store.UHC_SEMANTIC_COPY_FORMAT_ID,
        "source_id": UHC_PROVIDER_FILE_SOURCE_ID,
        "encoder_sha256": identity.encoder_sha256,
        "lineage": {
            "artifact_sha256": identity.artifact_sha256,
            "manifest_sha256": identity.manifest_sha256,
            "range_set_sha256": identity.range_set_sha256,
            "source_file_id": identity.source_file_id,
            "source_binding_id": (
                f"{identity.catalog_set_sha256}/{identity.source_file_id}"
            ),
            "collection_kind": identity.collection_kind,
        },
        "counters": _postgres_fixture_counters(),
        "fact_count": 4,
        "evidence_count": 3,
        "quarantine_count": 1,
        "quarantine_identity_set_sha256": quarantine_identity_set_sha256(
            quarantines
        ),
        "fact_set_sha256": _postgres_fixture_proof_hash(
            fact_blocks, fact_fields, contract_prefix=True
        ),
        "record_identity_set_sha256": _postgres_line_hash(fact_identities),
        "evidence_identity_set_sha256": _postgres_line_hash(
            evidence_identities
        ),
        "evidence_layout_set_sha256": _postgres_fixture_proof_hash(
            evidence_ranges, evidence_fields, contract_prefix=False
        ),
        "fact_blocks": fact_blocks,
        "evidence_ranges": evidence_ranges,
        "max_record_bytes": 1024 * 1024,
    }


def _postgres_semantic_fixture(
    identity: store.UhcSemanticBuildIdentity,
) -> tuple[list[tuple[object, ...]], dict[str, object]]:
    stage_records: list[tuple[object, ...]] = []
    fact_blocks: list[dict[str, object]] = []
    evidence_ranges: list[dict[str, object]] = []
    fact_identities: list[bytes] = []
    evidence_identities: list[bytes] = []
    quarantines: list[UhcProviderQuarantine] = []
    for ordinal in range(4):
        fact_record, fact_block, fact_identity, quarantine = (
            _postgres_fixture_fact(identity, ordinal)
        )
        stage_records.append(fact_record)
        fact_blocks.append(fact_block)
        fact_identities.append(fact_identity)
        if quarantine is not None:
            quarantines.append(quarantine)
            evidence_ranges.append(
                {
                    "range_ordinal": ordinal,
                    "evidence_count": 0,
                    "run_count": 0,
                    "layout_sha256": hashlib.sha256(b"").hexdigest(),
                }
            )
            continue
        evidence_record, evidence_range, evidence_identity = (
            _postgres_fixture_evidence(ordinal)
        )
        stage_records.append(evidence_record)
        evidence_ranges.append(evidence_range)
        evidence_identities.append(evidence_identity)
    native_report_by_field = _postgres_fixture_native_report(
        identity,
        fact_blocks,
        evidence_ranges,
        fact_identities,
        evidence_identities,
        quarantines,
    )
    assert all(
        len(stage_record) == len(store.UHC_SEMANTIC_COPY_COLUMNS)
        for stage_record in stage_records
    )
    return stage_records, native_report_by_field


def _postgres_binary_copy_field(index: int, field_value: object) -> bytes:
    if index == 0:
        return struct.pack(">h", int(field_value))
    if index in {1, 2, 3, 4, 5}:
        return struct.pack(">q", int(field_value))
    if index in {7, 10}:
        return bytes(field_value)
    return str(field_value).encode()


def _postgres_binary_copy(stage_records: list[tuple[object, ...]]) -> bytes:
    encoded = bytearray(b"PGCOPY\n\xff\r\n\0")
    encoded.extend(struct.pack(">ii", 0, 0))
    for stage_record in stage_records:
        encoded.extend(struct.pack(">h", len(stage_record)))
        for index, field_value in enumerate(stage_record):
            if field_value is None:
                encoded.extend(struct.pack(">i", -1))
                continue
            field_bytes = _postgres_binary_copy_field(index, field_value)
            encoded.extend(struct.pack(">i", len(field_bytes)))
            encoded.extend(field_bytes)
    encoded.extend(struct.pack(">h", -1))
    return bytes(encoded)


async def _postgres_copy_chunks(payload: bytes) -> AsyncIterator[bytes]:
    for offset in range(0, len(payload), 4096):
        yield payload[offset : offset + 4096]


async def _postgres_broken_chunks(payload: bytes) -> AsyncIterator[bytes]:
    yield payload[: max(20, len(payload) // 3)]
    raise RuntimeError("injected semantic COPY crash")


async def _postgres_install_semantic_identity(
    connection: asyncpg.Connection,
    identity: store.UhcSemanticBuildIdentity,
    schema: str,
) -> None:
    await connection.execute(
        f"""
        INSERT INTO "{schema}".provider_directory_uhc_source_binding (
            catalog_set_sha256, source_file_id, artifact_sha256,
            collection_kind, released_at
        ) VALUES ($1, $2, $3, $4, NULL)
        """,
        identity.catalog_set_sha256,
        identity.source_file_id,
        identity.artifact_sha256,
        identity.collection_kind,
    )
    await connection.execute(
        f"""
        INSERT INTO "{schema}".provider_directory_uhc_raw_layout (
            artifact_sha256, contract_version, range_count, record_count,
            producer_build_id, range_set_sha256, manifest_sha256, status
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, 'verified')
        """,
        identity.artifact_sha256,
        identity.raw_contract_version,
        identity.raw_range_count,
        identity.raw_record_count,
        identity.raw_producer_build_id,
        identity.range_set_sha256,
        identity.manifest_sha256,
    )


async def _postgres_crash_and_recover_semantic_build(
    connection: asyncpg.Connection,
    identity: store.UhcSemanticBuildIdentity,
    binary_copy_payload: bytes,
    schema: str,
):
    first_claim = await store.claim_uhc_semantic_build(connection, identity)
    with pytest.raises(RuntimeError, match="injected semantic COPY crash"):
        await store.copy_uhc_semantic_stage(
            connection,
            first_claim,
            _postgres_broken_chunks(binary_copy_payload),
        )
    assert await connection.fetchval(
        f"SELECT count(*) FROM {first_claim.stage_ref}"
    ) == 0
    await store.copy_uhc_semantic_stage(
        connection,
        first_claim,
        _postgres_copy_chunks(binary_copy_payload),
    )
    assert await connection.fetchval(
        f"SELECT count(*) FROM {first_claim.stage_ref}"
    ) == 7
    await connection.execute(
        f"""
        UPDATE "{schema}".provider_directory_uhc_semantic_build
           SET lease_expires_at=now() - interval '1 second'
         WHERE semantic_build_id=$1
        """,
        first_claim.semantic_build_id,
    )
    recovered_claim = await store.claim_uhc_semantic_build(
        connection,
        identity,
    )
    assert recovered_claim.attempt_count == 2
    assert await connection.fetchval(
        f"SELECT count(*) FROM {recovered_claim.stage_ref}"
    ) == 0
    return recovered_claim


async def _postgres_assert_overlap_rejected(
    connection: asyncpg.Connection,
    recovered_claim,
    identity: store.UhcSemanticBuildIdentity,
    native_report_by_field: dict[str, object],
    copy_observation_by_field: dict[str, object],
    quarantine_source: UhcProviderQuarantineRawSource,
) -> None:
    overlap_record = _postgres_fixture_evidence(1)[0]
    await connection.execute(
        f"""
        INSERT INTO {recovered_claim.stage_ref} (
            row_kind, range_ordinal, run_ordinal, occurrence_ordinal,
            record_start, record_count, npi, conflict_signature_pack,
            payload_hash, semantic_hash, payload_bytes
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        """,
        *overlap_record,
    )
    with pytest.raises(
        store.UhcSemanticBuildError,
        match="ordinal partition changed",
    ):
        await verify_uhc_semantic_stage(
            connection,
            recovered_claim,
            identity,
            native_report_by_field,
            copy_observation=copy_observation_by_field,
            quarantine_source=quarantine_source,
        )
    await connection.execute(
        f"DELETE FROM {recovered_claim.stage_ref} "
        "WHERE row_kind=2 AND occurrence_ordinal=1"
    )


async def _postgres_assert_census_drift_rejected(
    connection: asyncpg.Connection,
    recovered_claim,
    identity: store.UhcSemanticBuildIdentity,
    native_report_by_field: dict[str, object],
    copy_observation_by_field: dict[str, object],
    quarantine_source: UhcProviderQuarantineRawSource,
    monkeypatch,
) -> None:
    def _wrong_raw_census(source, quarantines, max_record_bytes):
        assert source is quarantine_source
        assert quarantines
        assert max_record_bytes == native_report_by_field["max_record_bytes"]
        return UhcProviderQuarantineRecordCensus(
            individual_records=1,
            address_rows=2,
            provider_plan_rows=1,
        )

    with monkeypatch.context() as context:
        context.setattr(
            stage_verifier,
            "verify_provider_quarantine_source_records",
            _wrong_raw_census,
        )
        with pytest.raises(
            store.UhcSemanticBuildError,
            match="raw census disagrees",
        ):
            await verify_uhc_semantic_stage(
                connection,
                recovered_claim,
                identity,
                native_report_by_field,
                copy_observation=copy_observation_by_field,
                quarantine_source=quarantine_source,
            )


async def _postgres_assert_sealed_semantic_build(
    connection: asyncpg.Connection,
    identity: store.UhcSemanticBuildIdentity,
    sealed,
) -> None:
    assert sealed.attempt_count == 2
    assert sealed.fact_count == 4
    assert sealed.evidence_count == 3
    assert sealed.source_summary["rejected_counts"] == {
        "invalid_npi_checksum": 1,
        "invalid_npi_checksum_individual_records": 1,
        "invalid_npi_checksum_facility_records": 0,
        "invalid_npi_checksum_address_rows": 1,
        "invalid_npi_checksum_provider_plan_rows": 1,
        "invalid_npi_structure": 0,
        "invalid_npi_structure_individual_records": 0,
        "invalid_npi_structure_facility_records": 0,
        "invalid_npi_structure_address_rows": 0,
        "invalid_npi_structure_provider_plan_rows": 0,
    }
    assert sealed.source_summary["distinct_npis"] == 1
    assert sealed.source_summary["duplicate_npi_groups"] == 1
    assert sealed.source_summary["conflicting_npi_groups"] == 1
    assert sealed.source_summary["conflict_counts"]["names"] == 1
    sealed_row = await store.load_sealed_uhc_semantic_build(
        connection,
        identity,
    )
    assert sealed_row
    sealed_verifier_report = await verify_sealed_uhc_semantic_build(
        connection,
        identity,
        sealed_row,
    )
    assert sealed_verifier_report["fact_count"] == 4
    assert sealed_verifier_report["evidence_count"] == 3
    assert sealed_verifier_report["quarantine_count"] == 1
    reused_claim = await store.claim_uhc_semantic_build(connection, identity)
    assert reused_claim.sealed_reuse
    assert reused_claim.attempt_count == 2


def _postgres_quarantine_source(
    identity: store.UhcSemanticBuildIdentity,
) -> UhcProviderQuarantineRawSource:
    """Return the exact typed raw identity used by the stage verifier test."""

    return UhcProviderQuarantineRawSource(
        raw_path=Path("/test/raw.json"),
        manifest_path=Path("/test/manifest.json"),
        artifact_sha256=identity.artifact_sha256,
        artifact_byte_count=1,
        raw_contract_version=identity.raw_contract_version,
        manifest_sha256=identity.manifest_sha256,
        range_set_sha256=identity.range_set_sha256,
        record_count=identity.raw_record_count,
        range_count=identity.raw_range_count,
        raw_producer_build_id=identity.raw_producer_build_id,
        source_file_id=identity.source_file_id,
    )


def _postgres_quarantine_census(
    native_report_by_field,
    quarantine_source: UhcProviderQuarantineRawSource,
):
    """Return a deterministic stand-in for already unit-proven raw replay."""

    def _verify_raw_quarantine(source, quarantines, max_record_bytes):
        """Validate stage invocation and return the expected raw census."""

        assert source is quarantine_source
        assert len(quarantines) == 1
        assert max_record_bytes == native_report_by_field["max_record_bytes"]
        assert quarantines[0].occurrence_ordinal == 1
        assert quarantines[0].record_sha256 == _digest(
            "rejected-source-record"
        )
        return UhcProviderQuarantineRecordCensus(
            individual_records=1,
            address_rows=1,
            provider_plan_rows=1,
        )

    return _verify_raw_quarantine


async def _postgres_prepare_quarantine_proof(
    connection: asyncpg.Connection,
    identity: store.UhcSemanticBuildIdentity,
    recovered_claim,
    binary_copy_payload: bytes,
    native_report_by_field: dict[str, object],
    monkeypatch,
) -> tuple[dict[str, object], UhcProviderQuarantineRawSource]:
    """Copy the recovered stage and bind its typed raw-verifier stand-in."""

    copied_row_count = await store.copy_uhc_semantic_stage(
        connection,
        recovered_claim,
        _postgres_copy_chunks(binary_copy_payload),
    )
    copy_observation_by_field = {
        "output_bytes": len(binary_copy_payload),
        "output_sha256": hashlib.sha256(binary_copy_payload).hexdigest(),
        "copy_row_count": copied_row_count,
    }
    native_report_by_field.update(copy_observation_by_field)
    await store.prepare_uhc_semantic_stage_indexes(connection, recovered_claim)
    quarantine_source = _postgres_quarantine_source(identity)
    monkeypatch.setattr(
        stage_verifier,
        "verify_provider_quarantine_source_records",
        _postgres_quarantine_census(
            native_report_by_field,
            quarantine_source,
        ),
    )
    return copy_observation_by_field, quarantine_source


async def _postgres_seal_and_reuse_semantic_build(
    connection: asyncpg.Connection,
    identity: store.UhcSemanticBuildIdentity,
    recovered_claim,
    binary_copy_payload: bytes,
    native_report_by_field: dict[str, object],
    monkeypatch,
) -> None:
    """Verify, seal, reread, and reuse one crash-recovered semantic build."""

    copy_observation_by_field, quarantine_source = (
        await _postgres_prepare_quarantine_proof(
            connection,
            identity,
            recovered_claim,
            binary_copy_payload,
            native_report_by_field,
            monkeypatch,
        )
    )

    await _postgres_assert_overlap_rejected(
        connection,
        recovered_claim,
        identity,
        native_report_by_field,
        copy_observation_by_field,
        quarantine_source,
    )
    await _postgres_assert_census_drift_rejected(
        connection,
        recovered_claim,
        identity,
        native_report_by_field,
        copy_observation_by_field,
        quarantine_source,
        monkeypatch,
    )
    verifier_report = await verify_uhc_semantic_stage(
        connection,
        recovered_claim,
        identity,
        native_report_by_field,
        copy_observation=copy_observation_by_field,
        quarantine_source=quarantine_source,
    )
    sealed = await store.seal_uhc_semantic_build(
        connection,
        recovered_claim,
        identity,
        native_report_by_field,
        verifier_report,
    )
    await _postgres_assert_sealed_semantic_build(
        connection,
        identity,
        sealed,
    )
