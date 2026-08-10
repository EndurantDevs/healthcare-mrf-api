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

    response_by_field = await audit_api.audit_occurrences_payload(session, _args())
    encoded = orjson.dumps(response_by_field)
    decoded = json.loads(encoded, parse_float=Decimal, parse_int=int)

    _assert_exact_audit_items(decoded)
    _assert_exact_audit_metadata(decoded)
    _assert_exact_audit_query(session, atom_lookup)


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

    response_by_field = await audit_api.audit_occurrences_payload(
        session,
        _args(limit="50", offset="2600"),
    )

    assert response_by_field["items"] == []
    assert response_by_field["pagination"] == {
        "total": 2560,
        "limit": 50,
        "offset": 2600,
        "has_more": False,
    }
    assert response_by_field["result_state"] == "matched"
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
    tampered_row_by_field = {
        **_row(OCCURRENCE_ONE, atom_ordinal=0),
        "price_key": 999,
    }
    session = RecordingSession(
        [tampered_row_by_field, _row(OCCURRENCE_TWO, atom_ordinal=1)],
        digest_rows=_audit_digest_rows(2),
    )
    _patch_resolution(monkeypatch)

    with pytest.raises(PTG2ManifestArtifactError, match="validated sample digest"):
        await audit_api.audit_occurrences_payload(session, _args())


def test_audit_numeric_fragment_matches_scanner_canonical_limit():
    numeric_text = "1" + ("0" * 600)

    encoded = orjson.dumps({"value": audit_api._numeric_json_fragment(numeric_text)})

    assert json.loads(encoded, parse_int=int)["value"] == int(numeric_text)


def test_audit_occurrence_low_level_contract_edges():
    driver_row = types.SimpleNamespace(_mapping={"value": 1})
    assert audit_api._row_mapping(driver_row) == {"value": 1}
    assert audit_api._row_mapping((("value", 2),)) == {"value": 2}

    for invalid_value in (None, "", "null"):
        with pytest.raises(InvalidUsage, match="limit.*required"):
            audit_api._required_integer(
                {"limit": invalid_value},
                "limit",
                minimum=1,
            )
    with pytest.raises(InvalidUsage, match="limit.*integer"):
        audit_api._required_integer({"limit": "not-an-integer"}, "limit", minimum=1)

    with pytest.raises(PTG2ManifestArtifactError, match="invalid negotiated rate"):
        audit_api._numeric_json_fragment("not-a-number")
    with pytest.raises(PTG2ManifestArtifactError, match="non-finite"):
        audit_api._numeric_json_fragment("NaN")
    assert orjson.dumps(audit_api._numeric_json_fragment("-0")) == b"0"
    with pytest.raises(PTG2ManifestArtifactError, match="too large"):
        audit_api._numeric_json_fragment("1e131072")

    with pytest.raises(PTG2ManifestArtifactError, match="invalid code identity"):
        audit_api._canonical_identity(
            {"reported_code_system": "", "reported_code": ""}
        )
    assert audit_api._optional_exact_text({}, "name") is None
    with pytest.raises(PTG2ManifestArtifactError, match="invalid name metadata"):
        audit_api._optional_exact_text({"name": 1}, "name")
    for invalid_network_names in ({}, ["network", 1]):
        with pytest.raises(PTG2ManifestArtifactError, match="network metadata"):
            audit_api._exact_network_names(
                {"network_names": invalid_network_names}
            )
    assert audit_api._exact_scalar_or_text_array(None, field_name="codes") == []
    assert audit_api._exact_scalar_or_text_array("01", field_name="codes") == [
        "01"
    ]


@pytest.mark.parametrize("provenance_source_key", (True, None, 2))
def test_audit_source_payload_rejects_inconsistent_physical_key(
    provenance_source_key,
):
    with pytest.raises(PTG2ManifestArtifactError, match="provenance is inconsistent"):
        audit_api._audit_source_payload(
            source_artifact_key=1,
            logical_source_key="logical-source",
            provenance={"source_key": provenance_source_key},
        )


def test_audit_source_payload_omits_empty_logical_source_key():
    assert audit_api._audit_source_payload(
        source_artifact_key=1,
        logical_source_key=None,
        provenance={"source_key": 1, "source_type": "in_network"},
    ) == {
        "source_artifact_key": 1,
        "source_type": "in_network",
    }
