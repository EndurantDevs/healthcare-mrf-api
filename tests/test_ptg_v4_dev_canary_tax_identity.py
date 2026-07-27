# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Canary proof for provider-group tax-identity storage and integrity."""

from __future__ import annotations

import hashlib
import json
import struct
from typing import Any, Mapping

import pytest

from scripts import ptg_v4_dev_canary_db as canary_db
from scripts import ptg_v4_dev_canary_publication as publication


def _source_map_digest(shard_ids: tuple[str, ...]) -> str:
    digest = hashlib.sha256()
    digest.update(b"PTG2V4TAXORD\x01")
    digest.update(struct.pack(">I", len(shard_ids)))
    for ordinal, shard_id in enumerate(shard_ids):
        encoded_shard_id = shard_id.encode("utf-8")
        digest.update(struct.pack(">I", len(encoded_shard_id)))
        digest.update(encoded_shard_id)
        digest.update(struct.pack(">I", ordinal))
    return digest.hexdigest()


def _manifest_evidence() -> dict[str, Any]:
    shard_ids = ("file:a", "file:b")
    return {
        "row_count": 1,
        "fields": {
            "contract": "ptg2_provider_group_tax_identity_v1",
            "token_policy_id": "ptg-tin-hmac-sha256-v1:release-1",
            "token_policy_descriptor_sha256": (
                "a0c06f5494f80663686be6861038a8804"
                "d9509d0fdc2d2c8cc56c259e53d761c"
            ),
            "normalization_contract": "ein_ascii_digits_or_2_7_hyphen_v1",
            "hmac_contract": "hmac_sha256_ptg_tin_v1",
            "source_ordinal_contract": (
                "snapshot_shard_id_sorted_lsb0_bitmap_v1"
            ),
            "source_ordinal_map": [
                {"shard_id": shard_id, "ordinal": ordinal}
                for ordinal, shard_id in enumerate(shard_ids)
            ],
            "source_ordinal_map_digest": _source_map_digest(shard_ids),
            "source_shard_count": 2,
            "provider_group_count": 4,
            "tax_identity_count": 1,
            "matched_ein_count": 2,
            "missing_count": 1,
            "malformed_count": 1,
            "unsupported_type_count": 0,
            "content_digest": "22" * 32,
        },
    }


def _exact_counts() -> dict[str, int]:
    return {
        "provider_group_count": 4,
        "provider_tax_identity_manifest_count": 1,
        "provider_tax_identity_count": 1,
        "provider_group_tax_identity_count": 4,
        "provider_tax_matched_ein_count": 2,
        "provider_tax_missing_count": 1,
        "provider_tax_malformed_count": 1,
        "provider_tax_unsupported_type_count": 0,
        "provider_tax_referenced_identity_count": 1,
    }


def test_provider_tax_identity_manifest_reconciles_exact_rows() -> None:
    failures: list[str] = []

    publication._validate_provider_tax_identity(
        _manifest_evidence(),
        _exact_counts(),
        failures,
    )

    assert failures == []


@pytest.mark.parametrize(
    ("mutate", "expected_failure"),
    (
        (
            lambda evidence: evidence.update(row_count=0),
            "provider tax-identity manifest is missing or duplicated",
        ),
        (
            lambda evidence: evidence["fields"].update(contract="legacy"),
            "provider tax-identity manifest contract is invalid",
        ),
        (
            lambda evidence: evidence["fields"].update(token_policy_id="bad"),
            "provider tax-identity token policy is invalid",
        ),
        (
            lambda evidence: evidence["fields"].update(
                token_policy_descriptor_sha256="11" * 32
            ),
            "provider tax-identity token policy descriptor is invalid",
        ),
        (
            lambda evidence: evidence["fields"].update(content_digest="XYZ"),
            "provider tax-identity content_digest is invalid",
        ),
        (
            lambda evidence: evidence["fields"]["source_ordinal_map"].reverse(),
            "provider tax-identity source ordinal map is invalid",
        ),
        (
            lambda evidence: evidence["fields"].update(missing_count=2),
            "provider tax-identity manifest counts differ from rows",
        ),
    ),
)
def test_provider_tax_identity_manifest_fails_closed(
    mutate: Any,
    expected_failure: str,
) -> None:
    evidence = _manifest_evidence()
    mutate(evidence)
    failures: list[str] = []

    publication._validate_provider_tax_identity(
        evidence,
        _exact_counts(),
        failures,
    )

    assert expected_failure in failures


def test_provider_tax_identity_exact_cardinality_fails_closed() -> None:
    exact_counts = _exact_counts()
    exact_counts["provider_tax_referenced_identity_count"] = 0
    failures: list[str] = []

    publication._validate_provider_tax_identity(
        _manifest_evidence(),
        exact_counts,
        failures,
    )

    assert "provider tax-identity exact cardinality is invalid" in failures


class _ManifestConnection:
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
async def test_database_evidence_shapes_tax_identity_manifest() -> None:
    manifest_by_field = dict(_manifest_evidence()["fields"])
    source_ordinal_map = manifest_by_field.pop("source_ordinal_map")
    manifest_by_field["source_ordinal_map_json"] = json.dumps(
        source_ordinal_map
    )
    connection = _ManifestConnection((manifest_by_field,))

    evidence = await canary_db._provider_tax_identity(
        connection,
        "mrf",
        501,
    )

    assert evidence["row_count"] == 1
    assert evidence["fields"]["source_ordinal_map"] == source_ordinal_map
    assert "encode(content_digest, 'hex')" in connection.query


@pytest.mark.asyncio
async def test_database_evidence_reports_missing_or_duplicate_manifest() -> None:
    missing = await canary_db._provider_tax_identity(
        _ManifestConnection(()),
        "mrf",
        501,
    )
    duplicate = await canary_db._provider_tax_identity(
        _ManifestConnection(({}, {})),
        "mrf",
        501,
    )

    assert missing == {"row_count": 0, "fields": {}}
    assert duplicate == {"row_count": 2, "fields": {}}
