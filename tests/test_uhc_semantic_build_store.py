# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
from copy import deepcopy
from datetime import datetime, timezone
import hashlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

import process.uhc_semantic_build_store as store
from process.uhc_semantic_evidence import UhcNpiEvidenceSummary


def _digest(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


def _identity(**overrides: object) -> store.UhcSemanticBuildIdentity:
    identity_by_field = {
        "catalog_set_sha256": _digest("catalog"),
        "source_file_id": _digest("source"),
        "artifact_sha256": _digest("artifact"),
        "raw_contract_version": 2,
        "raw_range_count": 4,
        "collection_kind": "provider_membership",
        "encoder_sha256": _digest("encoder"),
    }
    identity_by_field.update(overrides)
    return store.UhcSemanticBuildIdentity(**identity_by_field)


def _native_report(identity: store.UhcSemanticBuildIdentity) -> dict[str, object]:
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
            "source_file_id": identity.source_file_id,
            "collection_kind": identity.collection_kind,
        },
        "fact_count": fact_count,
        "evidence_count": fact_count,
        "fact_set_sha256": _digest("facts"),
        "record_identity_set_sha256": _digest("records"),
        "evidence_identity_set_sha256": _digest("evidence"),
        "evidence_layout_set_sha256": _digest("evidence-layout"),
        "output_bytes": 1024,
        "output_sha256": _digest("copy-output"),
        "copy_row_count": fact_count + identity.raw_range_count,
        "counters": {
            "raw_provider_records": fact_count,
            "raw_plan_records": 0,
        },
        "fact_blocks": blocks,
        "evidence_ranges": evidence_ranges,
    }


def test_build_identity_is_exact_and_stage_is_private() -> None:
    identity = _identity()

    assert len(identity.semantic_build_id) == 64
    assert identity.semantic_build_id == _identity().semantic_build_id
    assert identity.semantic_build_id != _identity(
        encoder_sha256=_digest("other encoder")
    ).semantic_build_id
    assert identity.stage_relation.startswith("provider_directory_uhc_sem_")
    assert len(identity.stage_relation) <= 63
    assert len(store.UHC_SEMANTIC_COPY_COLUMNS) == 11
    create_sql = store._stage_create_sql('"mrf"."stage"')
    assert "CREATE TABLE" in create_sql
    assert "payload_bytes bytea" in create_sql
    assert "conflict_signature_pack bytea" in create_sql
    assert "PRIMARY KEY" not in create_sql
    assert "UNIQUE" not in create_sql


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
            "fact_set_sha256",
            "record_identity_set_sha256",
            "evidence_identity_set_sha256",
            "evidence_layout_set_sha256",
            "output_bytes",
            "output_sha256",
            "copy_row_count",
        )
    }
    verifier_by_field["verifier_sha256"] = _digest("verifier")
    assert store._assert_verifier_report(
        native,
        verifier_by_field,
    ) == _digest("verifier")
    verifier_by_field["fact_set_sha256"] = _digest("wrong")
    with pytest.raises(store.UhcSemanticBuildError, match="fact_set_sha256"):
        store._assert_verifier_report(native, verifier_by_field)


def test_setwise_evidence_is_the_only_source_of_npi_group_counts() -> None:
    counter_by_field = {"raw_provider_records": 10, "raw_plan_records": 0}
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
            "lease_token": arguments[11],
            "stage_schema": arguments[13],
            "stage_relation": arguments[14],
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
            "fact_set_sha256",
            "record_identity_set_sha256",
            "evidence_identity_set_sha256",
            "evidence_layout_set_sha256",
            "output_bytes",
            "output_sha256",
            "copy_row_count",
        )
    }
    report_by_field["verifier_sha256"] = _digest("verifier")
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
        case "lineage":
            report["lineage"]["source_file_id"] = "wrong"
        case "fact_count":
            report["fact_count"] = 0
        case "evidence_count":
            report["evidence_count"] = 3
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
        "lineage",
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


def test_plan_reference_report_requires_zero_evidence():
    identity = _identity(collection_kind="plan_reference")
    report = _native_report(identity)
    report["evidence_count"] = 0
    report["copy_row_count"] = identity.raw_range_count
    report["counters"] = {
        "raw_provider_records": 0,
        "raw_plan_records": identity.raw_range_count,
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
    assert proof.verifier_sha256 == _digest("verifier")
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
