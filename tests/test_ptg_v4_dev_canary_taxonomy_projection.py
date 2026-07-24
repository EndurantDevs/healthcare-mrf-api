# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from copy import deepcopy
from typing import Any, Mapping

import pytest

from process.ptg_parts.ptg2_v4_taxonomy_candidates import (
    PTG2_V4_INFERRED_TAXONOMY_CATALOG_CONTRACT,
    PTG2_V4_INFERRED_TAXONOMY_DIRECT_REPRESENTATION,
    PTG2_V4_INFERRED_TAXONOMY_OBSERVE_REPRESENTATION,
    PTG2_V4_INFERRED_TAXONOMY_OBSERVE_STATUS,
    PTG2_V4_INFERRED_TAXONOMY_PATTERN_CAP_REASON,
    PTG2_V4_INFERRED_TAXONOMY_PATTERN_REPRESENTATION,
    PTG2_V4_INFERRED_TAXONOMY_VECTOR_FORMAT,
    inferred_taxonomy_member_digest,
    inferred_taxonomy_pattern_member_digest,
    pack_inferred_taxonomy_npi_keys,
    pack_inferred_taxonomy_pattern_npi_keys,
)
from scripts import ptg_v4_dev_canary_db as canary_db
from scripts import ptg_v4_dev_canary_publication as publication
from scripts.ptg_v4_dev_canary_storage import relation_size_rows
from scripts.ptg_v4_dev_canary_storage_sql import _ownership_predicates


def _candidate_row(
    rule_byte: int,
    npi_keys: tuple[int, ...],
    *,
    npi_keys_by_pattern: Mapping[int, tuple[int, ...]] | None = None,
) -> dict[str, Any]:
    rule_digest = bytes([rule_byte]) * 32
    member_keys = pack_inferred_taxonomy_npi_keys(npi_keys)
    postings_by_pattern = dict(npi_keys_by_pattern or {})
    representation = (
        PTG2_V4_INFERRED_TAXONOMY_PATTERN_REPRESENTATION
        if postings_by_pattern
        else PTG2_V4_INFERRED_TAXONOMY_DIRECT_REPRESENTATION
    )
    pattern_payload = pack_inferred_taxonomy_pattern_npi_keys(
        postings_by_pattern
    )
    pattern_member_count = sum(
        len(pattern_npi_keys)
        for pattern_npi_keys in postings_by_pattern.values()
    )
    return {
        "rule_digest": rule_digest,
        "catalog_contract": PTG2_V4_INFERRED_TAXONOMY_CATALOG_CONTRACT,
        "catalog_digest": bytes([rule_byte + 10]) * 32,
        "vector_format": PTG2_V4_INFERRED_TAXONOMY_VECTOR_FORMAT,
        "member_count": len(npi_keys),
        "member_digest": inferred_taxonomy_member_digest(
            rule_digest,
            member_count=len(npi_keys),
            payload=member_keys,
        ),
        "member_keys": member_keys,
        "representation": representation,
        "pattern_count": len(postings_by_pattern),
        "pattern_member_count": pattern_member_count,
        "pattern_member_bytes": len(pattern_payload),
        "pattern_member_digest": inferred_taxonomy_pattern_member_digest(
            rule_digest,
            representation=representation,
            pattern_count=len(postings_by_pattern),
            pattern_member_count=pattern_member_count,
            payload=pattern_payload,
        ),
        "pattern_member_payload": pattern_payload,
    }


def _projection(*, pattern: bool = True) -> dict[str, Any]:
    first_patterns = {7: (2, 5)} if pattern else None
    second_patterns = {8: (3,)} if pattern else None
    return canary_db._shape_inferred_taxonomy_candidates(
        (
            _candidate_row(
                1,
                (2, 5),
                npi_keys_by_pattern=first_patterns,
            ),
            _candidate_row(
                2,
                (3,),
                npi_keys_by_pattern=second_patterns,
            ),
        ),
        npi_count=6,
        pattern_count=9 if pattern else 0,
    )


def _observe_row(
    rule_byte: int,
    npi_keys: tuple[int, ...],
) -> dict[str, Any]:
    rule_digest = bytes([rule_byte]) * 32
    member_keys = pack_inferred_taxonomy_npi_keys(npi_keys)
    return {
        "rule_digest": rule_digest,
        "catalog_contract": PTG2_V4_INFERRED_TAXONOMY_CATALOG_CONTRACT,
        "catalog_digest": bytes([rule_byte + 10]) * 32,
        "vector_format": PTG2_V4_INFERRED_TAXONOMY_VECTOR_FORMAT,
        "member_count": len(npi_keys),
        "member_digest": inferred_taxonomy_member_digest(
            rule_digest,
            member_count=len(npi_keys),
            payload=member_keys,
        ),
        "member_keys": member_keys,
        "representation": PTG2_V4_INFERRED_TAXONOMY_OBSERVE_REPRESENTATION,
        "observe_reason": PTG2_V4_INFERRED_TAXONOMY_PATTERN_CAP_REASON,
        "observe_count_lower_bound": 131_073,
        "pattern_count": 0,
        "pattern_member_count": 0,
        "pattern_member_bytes": 0,
        "pattern_member_digest": inferred_taxonomy_pattern_member_digest(
            rule_digest,
            representation=(
                PTG2_V4_INFERRED_TAXONOMY_OBSERVE_REPRESENTATION
            ),
            pattern_count=0,
            pattern_member_count=0,
            payload=b"",
        ),
        "pattern_member_payload": b"",
    }


def _snapshot(
    *,
    projection: object = None,
    advertised: bool = True,
    representation: str = PTG2_V4_INFERRED_TAXONOMY_PATTERN_REPRESENTATION,
) -> dict[str, Any]:
    provider_graph_map: dict[str, object] = {
        "representation": representation
    }
    if advertised:
        provider_graph_map["inferred_taxonomy_candidates"] = projection
    return {
        "layout_manifest": {
            "serving_index": {
                "serving_binary": {
                    "provider_graph_v4": provider_graph_map,
                }
            }
        }
    }


def _exact_counts(projection: Mapping[str, Any]) -> dict[str, int]:
    return {
        "inferred_taxonomy_rule_count": int(
            projection.get("rule_count", 0)
        ),
        "inferred_taxonomy_observe_only_rule_count": int(
            projection.get("observe_only_rule_count", 0)
        ),
        "inferred_taxonomy_member_count": int(
            projection.get("member_count", 0)
        ),
        "inferred_taxonomy_packed_byte_count": int(
            projection.get("packed_byte_count", 0)
        ),
        "inferred_taxonomy_pattern_count": int(
            projection.get("pattern_count", 0)
        ),
        "inferred_taxonomy_pattern_member_count": int(
            projection.get("pattern_member_count", 0)
        ),
        "inferred_taxonomy_pattern_payload_byte_count": int(
            projection.get("pattern_member_bytes", 0)
        ),
    }


def test_projection_evidence_authenticates_v2_pattern_counts_and_digest() -> None:
    projection = _projection()

    assert projection["rule_count"] == 2
    assert projection["member_count"] == 3
    assert projection["packed_byte_count"] == 12
    assert projection["pattern_count"] == 2
    assert projection["pattern_member_count"] == 3
    assert projection["pattern_member_bytes"] > 0
    assert len(projection["projection_digest"]) == 64
    assert {
        rule["representation"] for rule in projection["rules"]
    } == {PTG2_V4_INFERRED_TAXONOMY_PATTERN_REPRESENTATION}

    failures: list[str] = []
    summary = publication._validate_inferred_taxonomy_candidates(
        _snapshot(projection=projection),
        projection,
        _exact_counts(projection),
        failures,
        expected_representation=(
            PTG2_V4_INFERRED_TAXONOMY_PATTERN_REPRESENTATION
        ),
    )

    assert failures == []
    assert summary == {"advertised": True, **projection}


def test_projection_evidence_authenticates_online_and_observe_partition() -> None:
    projection = canary_db._shape_inferred_taxonomy_candidates(
        (
            _candidate_row(1, (2, 5), npi_keys_by_pattern={7: (2, 5)}),
            _observe_row(3, (3,)),
        ),
        npi_count=6,
        pattern_count=8,
    )
    failures: list[str] = []

    summary = publication._validate_inferred_taxonomy_candidates(
        _snapshot(projection=projection),
        projection,
        _exact_counts(projection),
        failures,
        expected_representation=(
            PTG2_V4_INFERRED_TAXONOMY_PATTERN_REPRESENTATION
        ),
    )

    assert failures == []
    assert summary == {"advertised": True, **projection}
    assert projection["rule_count"] == 1
    assert projection["observe_only_rule_count"] == 1
    observe_rule = projection["observe_only_rules"][0]
    assert observe_rule["status"] == PTG2_V4_INFERRED_TAXONOMY_OBSERVE_STATUS
    assert observe_rule["reason"] == PTG2_V4_INFERRED_TAXONOMY_PATTERN_CAP_REASON
    assert observe_rule["observed_count_lower_bound"] == 131_073


def test_advertised_projection_rejects_digest_cap_and_exact_count_drift() -> None:
    projection = _projection()
    advertised = deepcopy(projection)
    advertised["projection_digest"] = "0" * 64
    advertised["max_online_inferred_taxonomy_candidates"] = 1
    exact_counts = _exact_counts(projection)
    exact_counts["inferred_taxonomy_pattern_member_count"] += 1
    failures: list[str] = []

    publication._validate_inferred_taxonomy_candidates(
        _snapshot(projection=advertised),
        projection,
        exact_counts,
        failures,
        expected_representation=(
            PTG2_V4_INFERRED_TAXONOMY_PATTERN_REPRESENTATION
        ),
    )

    assert any("serving manifest is invalid" in failure for failure in failures)
    assert any("counts differ" in failure for failure in failures)


def test_pattern_layout_rejects_valid_nonempty_direct_projection() -> None:
    direct_projection = _projection(pattern=False)
    failures: list[str] = []

    publication._validate_inferred_taxonomy_candidates(
        _snapshot(
            projection=direct_projection,
            representation=PTG2_V4_INFERRED_TAXONOMY_PATTERN_REPRESENTATION,
        ),
        direct_projection,
        _exact_counts(direct_projection),
        failures,
        expected_representation=(
            PTG2_V4_INFERRED_TAXONOMY_PATTERN_REPRESENTATION
        ),
    )

    assert any(
        "selected graph representation" in failure for failure in failures
    )


@pytest.mark.parametrize(
    "tamper_field",
    ("representation", "pattern_member_count", "pattern_member_payload"),
)
def test_database_row_shaper_rejects_pattern_metadata_or_payload_tamper(
    tamper_field: str,
) -> None:
    row = _candidate_row(1, (2, 5), npi_keys_by_pattern={7: (2, 5)})
    tampered_candidate_map = dict(row)
    if tamper_field == "representation":
        tampered_candidate_map[tamper_field] = (
            PTG2_V4_INFERRED_TAXONOMY_DIRECT_REPRESENTATION
        )
    elif tamper_field == "pattern_member_count":
        tampered_candidate_map[tamper_field] = (
            int(tampered_candidate_map[tamper_field]) + 1
        )
    else:
        tampered_payload = bytearray(tampered_candidate_map[tamper_field])
        tampered_payload[-1] ^= 1
        tampered_candidate_map[tamper_field] = bytes(tampered_payload)

    with pytest.raises(RuntimeError):
        canary_db._shape_inferred_taxonomy_candidates(
            (tampered_candidate_map,),
            npi_count=6,
            pattern_count=8,
        )


def test_legacy_v4_without_projection_remains_valid_only_without_rows() -> None:
    failures: list[str] = []

    summary = publication._validate_inferred_taxonomy_candidates(
        _snapshot(advertised=False),
        {},
        _exact_counts({}),
        failures,
        expected_representation=(
            PTG2_V4_INFERRED_TAXONOMY_PATTERN_REPRESENTATION
        ),
    )

    assert failures == []
    assert summary == {"advertised": False}

    projection = _projection()
    publication._validate_inferred_taxonomy_candidates(
        _snapshot(advertised=False),
        projection,
        _exact_counts(projection),
        failures,
        expected_representation=(
            PTG2_V4_INFERRED_TAXONOMY_PATTERN_REPRESENTATION
        ),
    )
    assert any("rows exist without" in failure for failure in failures)


def test_candidate_relation_is_in_both_physical_scopes_and_snapshot_owned() -> None:
    relation_name = "ptg2_v4_inferred_taxonomy_candidate"

    assert relation_name in publication.REQUIRED_PHYSICAL_RELATIONS
    assert relation_name in publication.WHOLE_SNAPSHOT_PHYSICAL_RELATIONS
    target_filter, owned_filter, ownership_value = _ownership_predicates(
        relation_name,
        "ptg2:legacy-reference",
        501,
    )
    assert target_filter == '"snapshot_key" = $1::bigint'
    assert owned_filter == '"snapshot_key" IS NOT NULL'
    assert ownership_value == 501


class _ExactCountConnection:
    def __init__(self) -> None:
        self.query = ""

    async def fetchrow(self, query: str, snapshot_key: int) -> dict[str, int]:
        self.query = query
        assert snapshot_key == 501
        return {
            "map_pack_count": 0,
            "map_coordinate_count": 0,
            "map_entry_count": 0,
            "map_logical_byte_count": 0,
            "npi_count": 0,
            "component_count": 0,
            "pattern_count": 0,
            "relation_count": 0,
            "heavy_owner_count": 0,
            "prefix_owner_count": 0,
            "prefix_member_count": 0,
            "diagnostic_count": 1,
            "inferred_taxonomy_rule_count": 2,
            "inferred_taxonomy_observe_only_rule_count": 0,
            "inferred_taxonomy_member_count": 3,
            "inferred_taxonomy_packed_byte_count": 12,
            "inferred_taxonomy_pattern_count": 2,
            "inferred_taxonomy_pattern_member_count": 3,
            "inferred_taxonomy_pattern_payload_byte_count": 76,
        }


@pytest.mark.asyncio
async def test_exact_database_counts_include_all_v2_projection_totals() -> None:
    connection = _ExactCountConnection()

    counts = await canary_db._exact_counts(connection, "mrf", 501)

    assert counts["inferred_taxonomy_rule_count"] == 2
    assert counts["inferred_taxonomy_observe_only_rule_count"] == 0
    assert counts["inferred_taxonomy_member_count"] == 3
    assert counts["inferred_taxonomy_packed_byte_count"] == 12
    assert counts["inferred_taxonomy_pattern_count"] == 2
    assert counts["inferred_taxonomy_pattern_member_count"] == 3
    assert counts["inferred_taxonomy_pattern_payload_byte_count"] == 76
    assert "OCTET_LENGTH(pattern_member_payload)" in connection.query
    assert "representation = 'observe_v1'" in connection.query


class _CandidateConnection:
    def __init__(self, rows: tuple[Mapping[str, Any], ...]) -> None:
        self.rows = rows
        self.query = ""

    async def fetch(
        self,
        query: str,
        snapshot_key: int,
    ) -> tuple[Mapping[str, Any], ...]:
        self.query = query
        assert snapshot_key == 501
        return self.rows


@pytest.mark.asyncio
async def test_database_evidence_reads_every_v2_payload_field() -> None:
    connection = _CandidateConnection(
        (
            _candidate_row(
                1,
                (2, 5),
                npi_keys_by_pattern={7: (2, 5)},
            ),
        )
    )

    projection = await canary_db._inferred_taxonomy_candidates(
        connection,
        "mrf",
        501,
        npi_count=6,
        pattern_count=8,
    )

    assert projection["pattern_count"] == 1
    for field_name in (
        "representation",
        "observe_reason",
        "observe_count_lower_bound",
        "pattern_count",
        "pattern_member_count",
        "pattern_member_bytes",
        "pattern_member_digest",
        "pattern_member_payload",
    ):
        assert field_name in connection.query


class _RelationSizeConnection:
    def __init__(self) -> None:
        self.size_query = ""

    async def fetchval(self, query: str, *parameters: object) -> object:
        assert parameters == ("mrf", "ptg2_v4_inferred_taxonomy_candidate")
        if "pg_total_relation_size" in query:
            self.size_query = query
            return 8_192
        return True


@pytest.mark.asyncio
async def test_candidate_storage_uses_total_relation_size_including_indexes() -> None:
    connection = _RelationSizeConnection()

    rows = await relation_size_rows(
        connection,
        "mrf",
        ["ptg2_v4_inferred_taxonomy_candidate"],
    )

    assert "pg_total_relation_size" in connection.size_query
    assert rows == [
        {
            "relation": "ptg2_v4_inferred_taxonomy_candidate",
            "exists": True,
            "total_bytes": 8_192,
        }
    ]
