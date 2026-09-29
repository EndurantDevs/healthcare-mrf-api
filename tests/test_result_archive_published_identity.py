# See LICENSE.

from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import result_archive_candidate_initialization as initialization
from process.ptg_parts import result_archive_published_identity as published_identity
from process.ptg_parts.ptg2_candidate_attestation import shared_source_set_metadata
from process.ptg_parts.ptg2_invalid_price_exclusion import (
    INVALID_PRICE_EXCLUSION_POLICY_FIELD,
    invalid_price_exclusion_evidence,
    invalid_price_exclusion_policy,
    invalid_price_exclusion_source,
    invalid_price_value_sha256,
)
from process.ptg_parts.ptg2_v4_finalizer_maps import PTG2_V4_FINALIZER_MAP_CONTRACT
from process.ptg_parts.ptg2_v4_snapshot_maps import PTG2_V4_MAP_FORMAT
from process.ptg_parts.result_archive_published_authority import (
    PtgPublishedResultSourceAuthorityError,
    _authority_from_identity,
    validate_ptg_published_result_source_authority,
)
from process.ptg_parts.result_archive_published_identity import validate_published_result_identity
from tests.test_ptg2_candidate_attestation import (
    EMPTY_PROVIDER_IDENTIFIER_QUARANTINE,
    _audit_sample,
    _source_witness,
)


def _published_row() -> dict:
    raw_hash = b"r" * 32
    scope = b"s" * 32
    map_digest = b"m" * 32
    finalizer_digest = b"f" * 32
    source_set = shared_source_set_metadata([raw_hash.hex()])
    serving_by_field = {
        "arch_version": "postgres_binary_v3",
        "type": "ptg2_shared_blocks_v4",
        "storage_generation": "shared_blocks_v4",
        "provider_scope_strategy": "postgres_packed_graph_v4",
        "shared_block_layout": "packed_snapshot_maps_v4",
        "shared_snapshot_key": 17,
        "snapshot_map": {"map_digest": map_digest.hex()},
        "coverage_scope_id": scope.hex(),
        "source_witness": _source_witness(source_set),
        "audit_sample": _audit_sample("ab" * 32),
        "provider_identifier_quarantine": EMPTY_PROVIDER_IDENTIFIER_QUARANTINE,
    }
    return {
        "snapshot_id": "published-snapshot",
        "status": "published",
        "import_run_id": "source-run",
        "import_month": "2026-09-01",
        "run_import_month": "2026-09-01",
        "run_source_key": "source_a",
        "manifest": {
            "activation": {
                "contract": "ptg2_candidate_activation_v1",
                "state": "activated",
                "source_key": "source_a",
            },
            "serving_index": {**serving_by_field, "source_set": source_set},
        },
        "layout_manifest": {"serving_index": {**serving_by_field, "source_count": 1}},
        "snapshot_key": 17,
        "layout_state": "sealed",
        "layout_generation": "shared_blocks_v4",
        "layout_mapping_digest": map_digest,
        "v4_root_state": "complete",
        "v4_root_map_digest": map_digest,
        "map_root_state": "complete",
        "map_format": PTG2_V4_MAP_FORMAT,
        "map_digest": map_digest,
        "finalizer_root_state": "complete",
        "finalizer_contract": PTG2_V4_FINALIZER_MAP_CONTRACT,
        "finalizer_map_format": PTG2_V4_MAP_FORMAT,
        "finalizer_map_digest": finalizer_digest,
        "plan_id": "12-3456789",
        "plan_market_type": "group",
        "coverage_scope_id": scope,
        "raw_container_sha256_values": [raw_hash],
        "plan_scopes": [["12-3456789", "group"]],
        "source_assignments": [{"source_key": 1, "source_type": "in_network", "raw_container_sha256": raw_hash.hex()}],
        "artifact_count": 0,
        "frozen_binding_payload": None,
    }


def test_published_result_identity_binds_sealed_result_without_frozen_files():
    identity = validate_published_result_identity(_published_row())
    assert identity["snapshot_id"] == "published-snapshot"
    assert identity["source_count"] == 1
    assert identity["source_set_digest"] == bytes.fromhex(
        _published_row()["manifest"]["serving_index"]["source_set"]["raw_container_sha256_digest"]
    )
    assert identity["finalizer_map_digest"] == b"f" * 32


def test_published_result_identity_rejects_a_sealed_legacy_v3_layout():
    row = _published_row()
    row["layout_generation"] = "shared_blocks_v3"
    for manifest in (row["manifest"], row["layout_manifest"]):
        manifest["serving_index"]["storage_generation"] = "shared_blocks_v3"
    with pytest.raises(ValueError, match="requires a sealed V4 layout"):
        validate_published_result_identity(row)


def test_published_result_candidate_preserves_authenticated_singleton_price_policy():
    published_record = _published_row()
    policy = invalid_price_exclusion_policy(
        [
            invalid_price_exclusion_source(
                raw_source_sha256=(b"r" * 32).hex(),
                entries=[
                    {
                        "object_ordinal": 0,
                        "rate_ordinal": 0,
                        "price_ordinal": 0,
                        "invalid_value_sha256": invalid_price_value_sha256("2027-02-30"),
                    }
                ],
                emptied_rate_count=0,
            )
        ]
    )
    evidence = invalid_price_exclusion_evidence(policy)
    published_record[INVALID_PRICE_EXCLUSION_POLICY_FIELD] = policy
    published_record["manifest"]["serving_index"]["invalid_price_exclusion"] = evidence
    published_record["layout_manifest"]["serving_index"]["invalid_price_exclusion"] = evidence
    assert validate_published_result_identity(published_record)["source_count"] == 1
    staged = initialization._AuthenticatedStagedCandidate(
        source_snapshot_id=published_record["snapshot_id"],
        source_manifest=published_record["manifest"],
        primary_plan_id=published_record["plan_id"],
        primary_plan_market_type=published_record["plan_market_type"],
        coverage_scope_id=published_record["coverage_scope_id"],
        plan_scopes=((published_record["plan_id"], published_record["plan_market_type"]),),
        source_records=(),
        invalid_price_exclusion_policy=policy,
    )
    _manifest, options = initialization._published_candidate_metadata(
        staged,
        {"identity": {"import_month": published_record["import_month"]}},
        "local-snapshot",
        "source_a",
        "local-run",
    )
    assert options[INVALID_PRICE_EXCLUSION_POLICY_FIELD] == policy
    assert options[INVALID_PRICE_EXCLUSION_POLICY_FIELD] is not policy


@pytest.mark.parametrize(
    "mutation",
    [
        lambda row: row.update(status="validated"),
        lambda row: row.update(run_source_key="other"),
        lambda row: row.update(run_import_month="2026-08-01"),
        lambda row: row.update(frozen_binding_payload={"protected": True}),
        lambda row: row["manifest"].update(frozen_rate_file_count=1),
        lambda row: row.update(finalizer_root_state="building"),
        lambda row: row.update(artifact_count=1),
        lambda row: row.update(raw_container_sha256_values=[b"x" * 32]),
        lambda row: row.update(plan_scopes=[["other", "group"]]),
        lambda row: row["source_assignments"][0].update(raw_container_sha256="00" * 32),
    ],
)
def test_published_result_identity_rejects_nonmatching_evidence(mutation):
    row = deepcopy(_published_row())
    mutation(row)
    with pytest.raises(ValueError):
        validate_published_result_identity(row)


def test_published_result_authority_receipt_binds_exact_pin_and_identity():
    authority = _authority_from_identity("operation-1", validate_published_result_identity(_published_row()))
    receipt = authority.as_dict()
    assert validate_ptg_published_result_source_authority(receipt) == receipt
    assert authority.retention_pin()["pin_id"].endswith(authority.owner_id)

    changed = deepcopy(receipt)
    changed["identity"]["source_set_digest"] = "00" * 32
    with pytest.raises(PtgPublishedResultSourceAuthorityError):
        validate_ptg_published_result_source_authority(changed)

    changed = deepcopy(receipt)
    changed["pin"]["owner_id"] = "00" * 32
    with pytest.raises(PtgPublishedResultSourceAuthorityError):
        validate_ptg_published_result_source_authority(changed)


@pytest.mark.parametrize(
    "mutation, reason",
    [
        (lambda receipt: receipt.update(operation_id=""), "operation is invalid"),
        (lambda receipt: receipt.update(identity=[]), "identity is invalid"),
        (lambda receipt: receipt["identity"].pop("snapshot_id"), "identity is invalid"),
        (lambda receipt: receipt["identity"].update(map_digest="invalid"), "digest is invalid"),
        (lambda receipt: receipt["identity"].update(plan_id=" "), "scope is invalid"),
        (lambda receipt: receipt["identity"].update(import_month="invalid"), "month is invalid"),
        (lambda receipt: receipt["identity"].update(import_month="2026-09-02"), "month is invalid"),
        (lambda receipt: receipt["identity"].update(source_count=True), "count is invalid"),
        (lambda receipt: receipt.update(unexpected=True), "receipt is invalid"),
    ],
)
def test_published_result_authority_rejects_malformed_receipts(mutation, reason):
    receipt = _authority_from_identity("operation-1", validate_published_result_identity(_published_row())).as_dict()
    mutation(receipt)
    with pytest.raises(PtgPublishedResultSourceAuthorityError, match=reason):
        validate_ptg_published_result_source_authority(receipt)


@pytest.mark.parametrize(
    "mutation",
    [
        lambda row: row.update(import_month="invalid"),
        lambda row: row.update(snapshot_id=""),
        lambda row: row.update(manifest=None),
    ],
)
def test_published_result_identity_rejects_missing_ownership(mutation):
    row = deepcopy(_published_row())
    mutation(row)
    with pytest.raises(ValueError):
        validate_published_result_identity(row)


@pytest.mark.asyncio
async def test_asyncpg_published_identity_uses_the_same_locked_evidence_query(monkeypatch):
    expected_map = {"snapshot_id": "source-snapshot", "manifest": {}, "plan_scopes": [["plan", "group"]]}
    connection = SimpleNamespace(
        fetch=AsyncMock(return_value=[{**expected_map, "manifest": "{}", "plan_scopes": '[["plan", "group"]]'}])
    )
    monkeypatch.setattr(published_identity, "validate_published_result_identity", lambda row: row)

    assert (
        await published_identity.load_published_result_identity_asyncpg(
            connection, schema_name="mrf", snapshot_id="source-snapshot", lock=True
        )
        == expected_map
    )
    query, snapshot_id = connection.fetch.await_args.args
    assert snapshot_id == "source-snapshot"
    assert "WHERE snapshot.snapshot_id = $1" in query
    assert "FOR KEY SHARE OF snapshot, internal_run, binding, scope, layout" in query
    assert 'LEFT JOIN "mrf".ptg2_frozen_source_file_binding AS frozen' in query
    assert "jsonb_build_array(plan_id, lower(plan_market_type))" in query
    assert "ORDER BY plan_id, lower(plan_market_type)" in query

    connection.fetch.return_value = []
    with pytest.raises(ValueError, match="missing or ambiguous"):
        await published_identity.load_published_result_identity_asyncpg(
            connection, schema_name="mrf", snapshot_id="source-snapshot"
        )


@pytest.mark.asyncio
async def test_asyncpg_published_identity_rejects_a_blank_snapshot_before_reading():
    connection = SimpleNamespace(fetch=AsyncMock())
    with pytest.raises(ValueError, match="snapshot_id is required"):
        await published_identity.load_published_result_identity_asyncpg(connection, schema_name="mrf", snapshot_id="  ")
    connection.fetch.assert_not_awaited()
