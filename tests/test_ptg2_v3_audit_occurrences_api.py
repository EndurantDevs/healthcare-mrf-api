# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import json
import types
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


class Result:
    def __init__(self, rows):
        self.rows = list(rows)

    def __iter__(self):
        return iter(self.rows)


class RecordingSession:
    def __init__(self, rows, *, digest_rows=None):
        self.rows = rows
        total = int(rows[0].get("total") or 0) if rows else 0
        self.digest_rows = (
            list(digest_rows)
            if digest_rows is not None
            else _audit_digest_rows(total)
        )
        self.calls = []

    async def execute(self, statement, params):
        self.calls.append((str(statement), dict(params)))
        return Result(self.rows if len(self.calls) == 1 else self.digest_rows)


def _audit_digest_rows(count):
    rows = []
    for index in range(count):
        if index == 0:
            occurrence_id = OCCURRENCE_ONE
        elif index == 1:
            occurrence_id = OCCURRENCE_TWO
        else:
            occurrence_id = (index + 1).to_bytes(32, "big")
        rows.append(_row(occurrence_id, atom_ordinal=index, total=count))
    return rows


def _serving_tables(*, sample_count=2, sample_digest=None) -> PTG2ServingTables:
    return PTG2ServingTables(
        snapshot_id=SNAPSHOT_ID,
        arch_version="postgres_binary_v3",
        storage="manifest_snapshot",
        shared_snapshot_key=41,
        storage_generation="shared_blocks_v3",
        cold_lookup_contract="ptg_v3_cold_v2",
        shared_block_layout="dense_shared_blocks_v3",
        source_count=2,
        source_key="logical-source",
        atom_key_bits=24,
        atom_key_block_span=4096,
        price_atom_constant_values={},
        source_set=dict(SOURCE_SET),
        database_evidence=dict(DATABASE_EVIDENCE),
        audit_sample={
            "contract": "persisted_served_occurrence_sample_v2",
            "format_version": 2,
            "method": "publish_time_stratified_v1",
            "sample_count": sample_count,
            "maximum_rows": 2560,
            "complete_population": False,
            "sample_digest": sample_digest
            or audit_api.persisted_audit_sample_digest(
                _audit_digest_rows(sample_count)
            ),
            "source_count": 2,
            "occurrence_identity": "sha256_candidate_ordinal_source_key_v2",
            "serving_multiplicity_semantics": "source_multiset_v1",
        },
    )


def _args(**overrides):
    values = {
        "plan_id": PLAN_ID,
        "snapshot_id": SNAPSHOT_ID,
        "mode": "exact_source",
        "order_by": "occurrence_id",
        "order": "asc",
        "limit": "100",
        "offset": "0",
        "source_key": "logical-source",
    }
    values.update(overrides)
    return values


def _row(occurrence_id, *, atom_ordinal, atom_key=9, total=2):
    return {
        "scope_count": 1,
        "total": total,
        "occurrence_id": occurrence_id,
        "code_key": 7,
        "provider_set_key": 5,
        "price_key": 11,
        "source_key": 1,
        "npi": 1234567890,
        "atom_ordinal": atom_ordinal,
        "atom_key": atom_key,
        "reported_code_system": "CPT",
        "reported_code": "99213",
        "negotiation_arrangement": "ffs",
        "billing_code_type_version": "2026",
        "source_name": "Office visit",
        "source_description": "Established patient office visit",
        "network_names": ["Exact Network B", "Exact Network A"],
        "code_scope_matches": True,
        "provider_set_scope_matches": True,
    }


def _price_atom(rate="123.4567890123456789"):
    return PTG2V3PriceAtomRecord(
        negotiated_rate=rate,
        attribute_keys=(1, 2, 3, 4, 5, 6, 7),
    )


def _dictionary_values():
    return {
        ("negotiated_type", 1): "negotiated",
        ("expiration_date", 2): "2027-12-31",
        ("service_code", 3): ["1", "01"],
        ("billing_class", 4): "professional",
        ("setting", 5): "office",
        ("billing_code_modifier", 6): ["tc", " TC "],
        ("additional_information", 7): "exact text",
    }


def _patch_resolution(monkeypatch, *, atoms=None, sample_count=2):
    monkeypatch.setattr(
        audit_api,
        "current_snapshot_id",
        AsyncMock(return_value=SNAPSHOT_ID),
    )
    monkeypatch.setattr(
        audit_api,
        "snapshot_serving_tables",
        AsyncMock(return_value=_serving_tables(sample_count=sample_count)),
    )
    atom_lookup = AsyncMock(
        return_value=atoms if atoms is not None else {9: _price_atom()}
    )
    monkeypatch.setattr(audit_api, "lookup_shared_price_atoms_from_db", atom_lookup)
    monkeypatch.setattr(
        audit_api,
        "_version_three_dictionary_values",
        AsyncMock(return_value=_dictionary_values()),
    )
    monkeypatch.setattr(
        audit_api,
        "fetch_snapshot_source_provenance",
        AsyncMock(return_value={1: dict(SOURCE_PROVENANCE)}),
    )
    monkeypatch.setattr(
        audit_api,
        "fetch_snapshot_source_set_metadata",
        AsyncMock(return_value=dict(SOURCE_SET)),
    )
    return atom_lookup


@pytest.mark.asyncio
async def test_audit_page_is_exact_ordered_and_preserves_duplicate_occurrences(
    monkeypatch,
):
    """Ensure audit pages preserve ordering, duplicates, and source provenance."""

    session = RecordingSession(
        [
            _row(OCCURRENCE_ONE, atom_ordinal=0),
            _row(OCCURRENCE_TWO, atom_ordinal=1),
        ]
    )
    atom_lookup = _patch_resolution(monkeypatch)

    payload = await audit_api.audit_occurrences_payload(session, _args())
    encoded = orjson.dumps(payload)
    decoded = json.loads(encoded, parse_float=Decimal, parse_int=int)

    assert [item["occurrence_id"] for item in decoded["items"]] == [
        OCCURRENCE_ONE.hex(),
        OCCURRENCE_TWO.hex(),
    ]
    assert decoded["items"][0]["tuple"] == {
        "code_system": "CPT",
        "code": "99213",
        "npi": 1234567890,
        "negotiation_arrangement": "FFS",
        "billing_code_type_version": "2026",
        "name": "Office visit",
        "description": "Established patient office visit",
        "network_names": ["Exact Network B", "Exact Network A"],
        "negotiated_type": "negotiated",
        "negotiated_rate": Decimal("123.4567890123456789"),
        "expiration_date": "2027-12-31",
        "service_code": ["01"],
        "billing_class": "professional",
        "setting": "office",
        "billing_code_modifier": ["TC"],
        "additional_information": "exact text",
    }
    assert decoded["items"][0]["tuple"] == decoded["items"][1]["tuple"]
    consumed = source_audit.extract_api_occurrence(decoded["items"][0])
    assert consumed.canonical_tuple.billing_code_type_version == "2026"
    assert consumed.canonical_tuple.name == "Office visit"
    assert consumed.canonical_tuple.description == "Established patient office visit"
    assert consumed.canonical_tuple.network_names == (
        "Exact Network A",
        "Exact Network B",
    )
    assert decoded["items"][0]["digest_coordinates"] == {
        "code_key": 7,
        "provider_set_key": 5,
        "price_key": 11,
        "source_artifact_key": 1,
        "npi": 1234567890,
        "atom_ordinal": 0,
        "atom_key": 9,
    }
    expected_source_payload = {
        **{
            key: value
            for key, value in SOURCE_PROVENANCE.items()
            if key != "source_key"
        },
        "source_key": "logical-source",
        "source_artifact_key": 1,
    }
    assert {
        key: decoded["items"][0][key] for key in expected_source_payload
    } == expected_source_payload
    assert decoded["pagination"] == {
        "total": 2,
        "limit": 100,
        "offset": 0,
        "has_more": False,
    }
    assert decoded["query"]["source_key"] == "logical-source"
    assert decoded["audit_sample"] == {
        "contract": "persisted_served_occurrence_sample_v2",
        "format_version": 2,
        "method": "publish_time_stratified_v1",
        "sample_count": 2,
        "maximum_rows": 2560,
        "sample_digest": audit_api.persisted_audit_sample_digest(
            _audit_digest_rows(2)
        ),
        "source_count": 2,
        "occurrence_identity": "sha256_candidate_ordinal_source_key_v2",
        "complete_population": False,
        "serving_multiplicity_semantics": "source_multiset_v1",
    }
    assert decoded["provenance"] == {
        "arch_version": "postgres_binary_v3",
        "storage_generation": "shared_blocks_v3",
        "database_backend": "postgresql",
        "plan_id": PLAN_ID,
        "snapshot_id": SNAPSHOT_ID,
        "mode": "exact_source",
        "pricing_scope": "plan_scoped_ptg",
        "database_evidence": DATABASE_EVIDENCE,
        "source_key": "logical-source",
    }
    assert decoded["source_set"] == SOURCE_SET
    sql, params = session.calls[0]
    assert "mrf.ptg2_v3_audit_occurrence" in sql
    assert "mrf.ptg2_v3_snapshot_scope" in sql
    assert "mrf.ptg2_v3_provider_set" in sql
    assert "provider_set.network_names" in sql
    assert "ORDER BY audit.occurrence_id ASC" in sql
    assert "audit.source_key" in sql
    assert "search-by-procedure" not in sql
    assert params["plan_ids"] == [PLAN_ID, "123456789"]
    assert atom_lookup.await_args.kwargs["atom_keys"] == {9}


@pytest.mark.asyncio
async def test_empty_page_keeps_persisted_sample_total(monkeypatch):
    session = RecordingSession(
        [
            {
                "scope_count": 1,
                "total": 2560,
                "occurrence_id": None,
            }
        ]
    )
    atom_lookup = _patch_resolution(monkeypatch, atoms={}, sample_count=2560)

    payload = await audit_api.audit_occurrences_payload(
        session,
        _args(limit="50", offset="2600"),
    )

    assert payload["items"] == []
    assert payload["pagination"] == {
        "total": 2560,
        "limit": 50,
        "offset": 2600,
        "has_more": False,
    }
    assert payload["result_state"] == "matched"
    assert atom_lookup.await_args.kwargs["atom_keys"] == set()


@pytest.mark.asyncio
async def test_audit_endpoint_rejects_rows_that_disagree_with_sealed_sample(
    monkeypatch,
):
    session = RecordingSession([_row(OCCURRENCE_ONE, atom_ordinal=0, total=1)])
    _patch_resolution(monkeypatch, sample_count=2)

    with pytest.raises(PTG2ManifestArtifactError, match="disagree with the sealed"):
        await audit_api.audit_occurrences_payload(session, _args())


@pytest.mark.asyncio
async def test_audit_endpoint_rejects_same_count_tampered_sample_digest(monkeypatch):
    tampered = _audit_digest_rows(2)
    tampered[1] = {**tampered[1], "price_key": 999}
    session = RecordingSession(
        [_row(OCCURRENCE_ONE, atom_ordinal=0), _row(OCCURRENCE_TWO, atom_ordinal=1)],
        digest_rows=tampered,
    )
    _patch_resolution(monkeypatch)

    with pytest.raises(PTG2ManifestArtifactError, match="sealed sample digest"):
        await audit_api.audit_occurrences_payload(session, _args())


@pytest.mark.asyncio
async def test_audit_endpoint_rejects_page_row_outside_validated_digest(monkeypatch):
    tampered_page_row = {
        **_row(OCCURRENCE_ONE, atom_ordinal=0),
        "price_key": 999,
    }
    session = RecordingSession(
        [tampered_page_row, _row(OCCURRENCE_TWO, atom_ordinal=1)],
        digest_rows=_audit_digest_rows(2),
    )
    _patch_resolution(monkeypatch)

    with pytest.raises(PTG2ManifestArtifactError, match="validated sample digest"):
        await audit_api.audit_occurrences_payload(session, _args())


def test_audit_numeric_fragment_matches_scanner_canonical_limit():
    numeric_text = "1" + ("0" * 600)

    encoded = orjson.dumps({"value": audit_api._numeric_json_fragment(numeric_text)})

    assert json.loads(encoded, parse_int=int)["value"] == int(numeric_text)


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
    payload = {
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
    dedicated = AsyncMock(return_value=payload)
    search = AsyncMock()
    monkeypatch.setattr(pricing, "audit_occurrences_payload", dedicated)
    monkeypatch.setattr(pricing, "search_current_ptg2_index", search)
    request = types.SimpleNamespace(
        args=_args(),
        ctx=types.SimpleNamespace(sa_session=object()),
    )

    response = await pricing.list_ptg2_audit_occurrences(request)

    assert json.loads(response.body) == payload
    dedicated.assert_awaited_once_with(
        request.ctx.sa_session,
        {**request.args, "plan_market_type": None},
    )
    search.assert_not_awaited()
