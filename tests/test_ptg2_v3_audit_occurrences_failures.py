# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import json
import types
from dataclasses import replace
from decimal import Decimal
from unittest.mock import AsyncMock

import orjson
import pytest
from sanic.exceptions import InvalidUsage

from api import ptg2_audit_occurrences as audit_api
from api.endpoint import pricing
from api.ptg2_types import PTG2ServingTables
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from process.ptg_parts.ptg2_shared_reuse import shared_source_set_metadata
from process.ptg_parts.ptg2_serving_binary_v3_types import PTG2V3PriceAtomRecord
from scripts.validation import ptg2_v3_source_api_audit as source_audit


SNAPSHOT_ID = "ptg3:test:snapshot"
PLAN_ID = "12-3456789"
OCCURRENCE_ONE = b"\x01" * 32
OCCURRENCE_TWO = b"\x02" * 32
SOURCE_PROVENANCE = {
    "source_key": 1,
    "source_type": "in_network",
    "identity_kind": "logical_json_sha256_v1",
    "identity_sha256": "1" * 64,
    "raw_container_sha256": "2" * 64,
    "logical_json_sha256": "1" * 64,
    "logical_hash_deferred": False,
    "source_trace_set_hash": "4" * 64,
    "source_trace": [{"source_file_version_id": "source-file-1"}],
}
SOURCE_SET = shared_source_set_metadata(["2" * 64, "3" * 64])
DATABASE_EVIDENCE = {
    "contract": "postgresql_session_v1",
    "server_version_num": 160004,
    "database_selected": True,
    "backend_session_active": True,
    "transaction_snapshot_observed": True,
}

from tests.ptg2_v3_audit_occurrences_support import (
    RecordingSession,
    Result,
    _args,
    _assert_exact_audit_items,
    _assert_exact_audit_metadata,
    _assert_exact_audit_query,
    _audit_digest_rows,
    _dictionary_values,
    _patch_resolution,
    _price_atom,
    _row,
    _serving_tables,
    _serving_tables_for_digest_rows,
)

@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"plan_id": ""}, "plan_id.*required"),
        ({"snapshot_id": ""}, "snapshot_id.*required"),
        ({"mode": "product_search"}, "mode.*exact_source"),
        ({"order_by": "npi"}, "order_by.*occurrence_id"),
        ({"order": "desc"}, "order.*asc"),
        ({"limit": "0"}, "limit.*>= 1"),
        ({"limit": "101"}, "limit.*<= 100"),
        ({"offset": "-1"}, "offset.*>= 0"),
    ],
)
async def test_audit_endpoint_rejects_non_strict_query_contract(overrides, message):
    with pytest.raises(InvalidUsage, match=message):
        await audit_api.audit_occurrences_payload(object(), _args(**overrides))


@pytest.mark.asyncio
async def test_audit_endpoint_rejects_snapshot_or_plan_scope_mismatch(monkeypatch):
    monkeypatch.setattr(
        audit_api,
        "current_snapshot_id",
        AsyncMock(return_value=None),
    )
    with pytest.raises(InvalidUsage, match="published sealed PTG V3 snapshot"):
        await audit_api.audit_occurrences_payload(object(), _args())

    _patch_resolution(monkeypatch)
    session = RecordingSession([{"scope_count": 0, "total": 0, "occurrence_id": None}])
    with pytest.raises(InvalidUsage, match="do not identify one plan scope"):
        await audit_api.audit_occurrences_payload(session, _args())


@pytest.mark.asyncio
async def test_audit_endpoint_rejects_logical_source_key_mismatch(monkeypatch):
    _patch_resolution(monkeypatch)

    with pytest.raises(InvalidUsage, match="source_key.*logical snapshot"):
        await audit_api.audit_occurrences_payload(
            object(),
            _args(source_key="another-logical-source"),
        )


@pytest.mark.asyncio
async def test_audit_endpoint_fails_closed_for_missing_atom_or_code(monkeypatch):
    session = RecordingSession([_row(OCCURRENCE_ONE, atom_ordinal=0)])
    _patch_resolution(monkeypatch, atoms={})
    with pytest.raises(PTG2ManifestArtifactError, match="missing price atom"):
        await audit_api.audit_occurrences_payload(session, _args())

    invalid_code = _row(OCCURRENCE_ONE, atom_ordinal=0)
    invalid_code["code_scope_matches"] = False
    session = RecordingSession([invalid_code])
    _patch_resolution(monkeypatch)
    with pytest.raises(PTG2ManifestArtifactError, match="out-of-scope code metadata"):
        await audit_api.audit_occurrences_payload(session, _args())

    invalid_provider_set = _row(OCCURRENCE_ONE, atom_ordinal=0)
    invalid_provider_set["provider_set_scope_matches"] = False
    session = RecordingSession([invalid_provider_set])
    _patch_resolution(monkeypatch)
    with pytest.raises(PTG2ManifestArtifactError, match="provider-set metadata"):
        await audit_api.audit_occurrences_payload(session, _args())


@pytest.mark.asyncio
async def test_audit_endpoint_rejects_source_rows_outside_sealed_source_set(
    monkeypatch,
):
    session = RecordingSession([_row(OCCURRENCE_ONE, atom_ordinal=0, total=1)])
    _patch_resolution(monkeypatch, sample_count=1)
    monkeypatch.setattr(
        audit_api,
        "fetch_snapshot_source_set_metadata",
        AsyncMock(
            return_value=shared_source_set_metadata(["2" * 64, "4" * 64])
        ),
    )

    with pytest.raises(PTG2ManifestArtifactError, match="sealed source set"):
        await audit_api.audit_occurrences_payload(session, _args())


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("serving_table_changes", "message"),
    (
        ({"shared_snapshot_key": None}, "sealed shared-block"),
        ({"source_set": None}, "sealed complete source set"),
        ({"database_evidence": None}, "PostgreSQL execution evidence"),
        ({"audit_sample": None}, "sealed persisted audit sample"),
    ),
)
async def test_audit_endpoint_rejects_incomplete_sealed_metadata(
    monkeypatch,
    serving_table_changes,
    message,
):
    _patch_resolution(monkeypatch)
    monkeypatch.setattr(
        audit_api,
        "snapshot_serving_tables",
        AsyncMock(return_value=replace(_serving_tables(), **serving_table_changes)),
    )

    with pytest.raises(PTG2ManifestArtifactError, match=message):
        await audit_api.audit_occurrences_payload(object(), _args())


@pytest.mark.asyncio
async def test_audit_endpoint_translates_source_set_reader_failure(monkeypatch):
    _patch_resolution(monkeypatch)
    monkeypatch.setattr(
        audit_api,
        "fetch_snapshot_source_set_metadata",
        AsyncMock(side_effect=audit_api.PTG2SharedBlockError("source set failed")),
    )

    with pytest.raises(PTG2ManifestArtifactError, match="source set failed"):
        await audit_api.audit_occurrences_payload(object(), _args())


@pytest.mark.asyncio
async def test_audit_endpoint_rejects_missing_contract_or_digest_rows(monkeypatch):
    _patch_resolution(monkeypatch, sample_count=1)
    with pytest.raises(PTG2ManifestArtifactError, match="no contract row"):
        await audit_api.audit_occurrences_payload(RecordingSession([]), _args())

    session = RecordingSession(
        [_row(OCCURRENCE_ONE, atom_ordinal=0, total=1)],
        digest_rows=[],
    )
    with pytest.raises(PTG2ManifestArtifactError, match="sealed sample count"):
        await audit_api.audit_occurrences_payload(session, _args())


@pytest.mark.asyncio
async def test_audit_endpoint_rejects_duplicate_digest_occurrence_ids(monkeypatch):
    duplicate_digest_rows = [
        _row(OCCURRENCE_ONE, atom_ordinal=0),
        _row(OCCURRENCE_ONE, atom_ordinal=0),
    ]
    _patch_resolution(monkeypatch)
    monkeypatch.setattr(
        audit_api,
        "snapshot_serving_tables",
        AsyncMock(return_value=_serving_tables_for_digest_rows(duplicate_digest_rows)),
    )
    session = RecordingSession(
        [_row(OCCURRENCE_ONE, atom_ordinal=0), _row(OCCURRENCE_TWO, atom_ordinal=1)],
        digest_rows=duplicate_digest_rows,
    )

    with pytest.raises(PTG2ManifestArtifactError, match="duplicate occurrence ids"):
        await audit_api.audit_occurrences_payload(session, _args())


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("row_changes", "message"),
    (
        ({"occurrence_id": b"short"}, "exactly 32 bytes"),
        ({"source_key": True}, "invalid source key"),
        ({"source_key": 2}, "invalid source key"),
        ({"npi": 0}, "invalid NPI"),
    ),
)
async def test_audit_endpoint_rejects_invalid_persisted_coordinates(
    monkeypatch,
    row_changes,
    message,
):
    page_row_by_field = {
        **_row(OCCURRENCE_ONE, atom_ordinal=0, total=1),
        **row_changes,
    }
    digest_row_by_field = dict(page_row_by_field)
    serving_tables = _serving_tables_for_digest_rows([digest_row_by_field])
    _patch_resolution(monkeypatch, sample_count=1)
    monkeypatch.setattr(
        audit_api,
        "snapshot_serving_tables",
        AsyncMock(return_value=serving_tables),
    )
    session = RecordingSession(
        [page_row_by_field],
        digest_rows=[digest_row_by_field],
    )

    with pytest.raises(PTG2ManifestArtifactError, match=message):
        await audit_api.audit_occurrences_payload(session, _args())


@pytest.mark.asyncio
async def test_audit_endpoint_translates_source_provenance_reader_failure(monkeypatch):
    session = RecordingSession([_row(OCCURRENCE_ONE, atom_ordinal=0, total=1)])
    _patch_resolution(monkeypatch, sample_count=1)
    monkeypatch.setattr(
        audit_api,
        "fetch_snapshot_source_provenance",
        AsyncMock(side_effect=audit_api.PTG2SharedBlockError("provenance failed")),
    )

    with pytest.raises(PTG2ManifestArtifactError, match="provenance failed"):
        await audit_api.audit_occurrences_payload(session, _args())


@pytest.mark.asyncio
async def test_audit_endpoint_reports_market_filter_without_optional_source_query(
    monkeypatch,
):
    session = RecordingSession([_row(OCCURRENCE_ONE, atom_ordinal=0, total=1)])
    _patch_resolution(monkeypatch, sample_count=1)

    payload = await audit_api.audit_occurrences_payload(
        session,
        _args(source_key="", plan_market_type="GROUP"),
    )

    assert payload["query"]["plan_market_type"] == "group"
    assert "source_key" not in payload["query"]
    assert "source_key" not in payload["provenance"]
    assert "logical_scope.plan_market_type = :plan_market_type" in session.calls[0][0]


@pytest.mark.asyncio
async def test_audit_endpoint_fails_closed_for_missing_exact_source_mapping(monkeypatch):
    session = RecordingSession([_row(OCCURRENCE_ONE, atom_ordinal=0, total=1)])
    _patch_resolution(monkeypatch, sample_count=1)
    monkeypatch.setattr(
        audit_api,
        "fetch_snapshot_source_provenance",
        AsyncMock(return_value={}),
    )

    with pytest.raises(PTG2ManifestArtifactError, match="source mapping is missing"):
        await audit_api.audit_occurrences_payload(session, _args())


@pytest.mark.parametrize(
    ("dictionary_field", "dictionary_value", "error_field"),
    [
        (("service_code", 3), {"01": True}, "service_code"),
        (("billing_code_modifier", 6), {"TC": True}, "billing_code_modifier"),
    ],
)
@pytest.mark.asyncio
async def test_audit_endpoint_rejects_container_valued_price_lists(
    monkeypatch,
    dictionary_field,
    dictionary_value,
    error_field,
):
    session = RecordingSession([_row(OCCURRENCE_ONE, atom_ordinal=0, total=1)])
    _patch_resolution(monkeypatch, sample_count=1)
    dictionary_values = _dictionary_values()
    dictionary_values[dictionary_field] = dictionary_value
    monkeypatch.setattr(
        audit_api,
        "_version_three_dictionary_values",
        AsyncMock(return_value=dictionary_values),
    )

    with pytest.raises(PTG2ManifestArtifactError, match=error_field):
        await audit_api.audit_occurrences_payload(session, _args())


@pytest.mark.asyncio
async def test_pricing_route_uses_dedicated_audit_path(monkeypatch):
    response_by_field = {
        "result_state": "no_matching_rates",
        "pricing_scope": "plan_scoped_ptg",
        "resolved_snapshot_id": SNAPSHOT_ID,
        "items": [],
        "pagination": {"total": 0, "limit": 100, "offset": 0, "has_more": False},
        "query": {
            "plan_id": PLAN_ID,
            "snapshot_id": SNAPSHOT_ID,
            "mode": "exact_source",
        },
        "provenance": {
            "arch_version": "postgres_binary_v3",
            "storage_generation": "shared_blocks_v3",
            "database_backend": "postgresql",
            "plan_id": PLAN_ID,
            "snapshot_id": SNAPSHOT_ID,
            "mode": "exact_source",
            "pricing_scope": "plan_scoped_ptg",
        },
        "audit_sample": {
            "contract": "persisted_served_occurrence_sample_v2",
            "method": "publish_time_stratified_v1",
            "sample_count": 0,
            "complete_population": False,
        },
    }
    dedicated = AsyncMock(return_value=response_by_field)
    search = AsyncMock()
    monkeypatch.setattr(pricing, "audit_occurrences_payload", dedicated)
    monkeypatch.setattr(pricing, "search_current_ptg2_index", search)
    request = types.SimpleNamespace(
        args=_args(),
        ctx=types.SimpleNamespace(sa_session=object()),
    )

    response = await pricing.list_ptg2_audit_occurrences(request)

    assert json.loads(response.body) == response_by_field
    dedicated.assert_awaited_once_with(
        request.ctx.sa_session,
        {**request.args, "plan_market_type": None},
    )
    search.assert_not_awaited()
