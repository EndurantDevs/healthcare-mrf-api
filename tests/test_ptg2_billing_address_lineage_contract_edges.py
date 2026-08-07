# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed edge contracts for exact billing address lineage."""

from __future__ import annotations

import pytest

from api import ptg2_billing_address_lineage as lineage
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from tests.ptg2_billing_geo_reader_support import (
    NPI_A,
    _location_row,
    _replace_location_payload,
)


def _provenance_entry(
    *,
    source_id: int = 1,
    source_record_id: str = "synthetic:record",
) -> dict[str, object]:
    dataset_by_source = {
        1: "cms_nppes_registry",
        2: "marketplace_provider_directory",
    }
    return {
        "dataset_id": dataset_by_source[source_id],
        "source_id": source_id,
        "source_record_id": source_record_id,
        "record_version_id": "20260101",
        "record_version_ids": ["20260101"],
        "retrieved_at": "2026-01-01T00:00:00Z",
    }


def test_internal_reference_and_uuid_contract_edges() -> None:
    assert lineage._optional_internal_ref(None, category="reference") is None
    assert lineage._canonical_uuid_key(None, category="site key", optional=True) is None

    with pytest.raises(PTG2ManifestArtifactError, match="reference is malformed"):
        lineage._optional_internal_ref(7, category="reference")
    with pytest.raises(PTG2ManifestArtifactError, match="reference is unavailable"):
        lineage._required_internal_ref(None, category="reference")
    with pytest.raises(PTG2ManifestArtifactError, match="key is malformed"):
        lineage._canonical_uuid_key(7, category="key", optional=False)
    with pytest.raises(PTG2ManifestArtifactError, match="key is malformed"):
        lineage._canonical_uuid_key("not-a-uuid", category="key", optional=False)


def test_provenance_text_contract_edges() -> None:
    with pytest.raises(PTG2ManifestArtifactError, match="dataset is malformed"):
        lineage._required_provenance_text(None, category="dataset")
    with pytest.raises(PTG2ManifestArtifactError, match="values is malformed"):
        lineage._provenance_text_tuple("not-a-list", category="values")
    with pytest.raises(PTG2ManifestArtifactError, match="values is malformed"):
        lineage._provenance_text_tuple(["same", "same"], category="values")


@pytest.mark.parametrize(
    ("updates", "error"),
    (
        ({"source_id": 0}, "provenance is malformed"),
        ({"dataset_id": "unexpected"}, "provenance is malformed"),
        ({"record_version_ids": ["20251231"]}, "provenance is malformed"),
        ({"retrieved_at": "not-a-time"}, "retrieval time is malformed"),
    ),
)
def test_provenance_entry_rejects_inconsistent_fields(updates, error) -> None:
    raw_entry = _provenance_entry()
    raw_entry.update(updates)

    with pytest.raises(PTG2ManifestArtifactError, match=error):
        lineage._provenance_entry(raw_entry)


def test_address_provenance_requires_canonical_entry_order() -> None:
    address_payload_by_field = {
        "address_provenance": [
            _provenance_entry(source_id=2, source_record_id="synthetic:second"),
            _provenance_entry(source_id=1, source_record_id="synthetic:first"),
        ]
    }

    with pytest.raises(PTG2ManifestArtifactError, match="is inconsistent"):
        lineage._address_provenance(
            address_payload_by_field,
            admitted_source_id=1,
        )


def test_distance_conversion_is_bounded_and_fail_closed(monkeypatch) -> None:
    assert lineage._distance_miles({"distance_miles": 3}) == 3.0

    def fail_conversion(_value):
        raise ValueError("synthetic conversion failure")

    monkeypatch.setattr(lineage, "float", fail_conversion, raising=False)
    with pytest.raises(PTG2ManifestArtifactError, match="distance is malformed"):
        lineage._distance_miles({"distance_miles": 3})


def test_provider_address_accepts_optional_site_key_and_positive_distance() -> None:
    row = _replace_location_payload(
        _location_row(NPI_A, distance=1.25),
        address_site_key=None,
    )

    address = lineage.provider_addresses_by_npi(
        [row],
        candidate_npis=frozenset({NPI_A}),
    )[NPI_A]

    assert address.address_site_key is None
    assert address.distance_miles == 1.25


@pytest.mark.parametrize(
    ("row", "error"),
    (
        (
            _replace_location_payload(
                _location_row(NPI_A),
                geo_evidence_level="unrecognized",
            ),
            "geo evidence lineage is unavailable",
        ),
        (
            {**_location_row(NPI_A), "location_hash": "unexpected"},
            "location witness is inconsistent",
        ),
    ),
)
def test_provider_address_rejects_unbound_lineage(row, error) -> None:
    with pytest.raises(PTG2ManifestArtifactError, match=error):
        lineage.provider_addresses_by_npi(
            [row],
            candidate_npis=frozenset({NPI_A}),
        )
