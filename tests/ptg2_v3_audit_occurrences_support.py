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
    query_by_name = {
        "plan_id": PLAN_ID,
        "snapshot_id": SNAPSHOT_ID,
        "mode": "exact_source",
        "order_by": "occurrence_id",
        "order": "asc",
        "limit": "100",
        "offset": "0",
        "source_key": "logical-source",
    }
    query_by_name.update(overrides)
    return query_by_name


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


def _serving_tables_for_digest_rows(digest_rows):
    serving_tables = _serving_tables(sample_count=len(digest_rows))
    return replace(
        serving_tables,
        audit_sample={
            **serving_tables.audit_sample,
            "sample_digest": audit_api.persisted_audit_sample_digest(digest_rows),
        },
    )


def _assert_exact_audit_items(decoded):
    assert [
        occurrence_by_field["occurrence_id"]
        for occurrence_by_field in decoded["items"]
    ] == [OCCURRENCE_ONE.hex(), OCCURRENCE_TWO.hex()]
    expected_tuple_by_field = {
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
    assert decoded["items"][0]["tuple"] == expected_tuple_by_field
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


def _assert_exact_audit_metadata(decoded):
    expected_source_by_field = {
        **{
            source_field_name: source_field_value
            for source_field_name, source_field_value in SOURCE_PROVENANCE.items()
            if source_field_name != "source_key"
        },
        "source_key": "logical-source",
        "source_artifact_key": 1,
    }
    assert {
        source_field_name: decoded["items"][0][source_field_name]
        for source_field_name in expected_source_by_field
    } == expected_source_by_field
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


def _assert_exact_audit_query(session, atom_lookup):
    sql, params = session.calls[0]
    for required_sql in (
        "mrf.ptg2_v3_audit_occurrence",
        "mrf.ptg2_v3_snapshot_scope",
        "mrf.ptg2_v3_provider_set",
        "provider_set.network_names",
        "ORDER BY audit.occurrence_id ASC",
        "audit.source_key",
    ):
        assert required_sql in sql
    assert "search-by-procedure" not in sql
    assert params["plan_ids"] == [PLAN_ID, "123456789"]
    assert atom_lookup.await_args.kwargs["atom_keys"] == {9}
