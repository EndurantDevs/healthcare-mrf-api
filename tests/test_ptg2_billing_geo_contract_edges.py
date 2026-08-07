# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed edge contracts for exact billing GEO values."""

from __future__ import annotations

from dataclasses import replace

import pytest

from api import ptg2_billing_geo_contract as contract
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from tests.ptg2_billing_geo_reader_support import NPI_A, _provider_rate


def _provenance() -> contract.BillingAddressProvenance:
    return contract.BillingAddressProvenance(
        dataset_id="cms_nppes_registry",
        source_id=1,
        source_record_id="synthetic:record",
        record_version_id="20260101",
        record_version_ids=("20260101",),
        retrieved_at="2026-01-01T00:00:00Z",
        issuer_names=(),
        source_urls=(),
    )


def _address(
    *,
    npi: int = NPI_A,
    distance_miles: float | None = None,
) -> contract.BillingProviderAddress:
    return contract.BillingProviderAddress(
        npi=npi,
        location_hash="entity_address_unified:" + "1" * 64,
        distance_miles=distance_miles,
        address_key="00000000-0000-0000-0000-000000000001",
        address_site_key=None,
        location_key="1" * 64,
        address_purpose="practice",
        display={"first_line": "10 Example Ave"},
        geo_evidence_level="nppes_registry_address",
        geo_evidence_source_id=1,
        provenance=(_provenance(),),
    )


def test_geo_value_objects_keep_redacted_stable_coordinates() -> None:
    address = _address()
    provider_rate = _provider_rate()
    geo_witness = contract.BillingProviderGeoWitness(provider_rate, address)
    selection = contract.BillingGeoSelection(True, (geo_witness,))
    priced = contract.BillingProviderGeoPriceWitness(
        geo_witness,
        ({"negotiated_rate": 10.0},),
    )

    assert "address=<redacted>" in repr(address)
    assert "rate=<redacted>" in repr(geo_witness)
    assert "witness_count=1" in repr(selection)
    assert priced.stable_sort_key == geo_witness.stable_sort_key
    assert "price_count=1" in repr(priced)


def test_bounded_tuple_stops_before_retaining_an_extra_value() -> None:
    with pytest.raises(PTG2ManifestArtifactError, match="synthetic limit"):
        contract.bounded_tuple(
            ("extra",),
            maximum_count=0,
            error_message="synthetic limit",
        )


@pytest.mark.parametrize(
    "address",
    (
        object(),
        replace(_address(), selection_contract="unexpected"),
        _address(npi=1234567890),
        _address(distance_miles=-0.5),
        _address(distance_miles=True),
    ),
    ids=("wrong-type", "wrong-contract", "invalid-npi", "negative", "boolean"),
)
def test_provider_address_validation_rejects_invalid_public_coordinates(
    address,
) -> None:
    with pytest.raises(PTG2ManifestArtifactError, match="address scope is invalid"):
        contract.require_valid_provider_address(address)


def test_provider_address_validation_accepts_valid_address() -> None:
    address = _address(distance_miles=1.5)

    assert contract.require_valid_provider_address(address) is address


@pytest.mark.parametrize("value", (True, "1.0"))
def test_finite_coordinate_rejects_non_numeric_values(value) -> None:
    with pytest.raises(ValueError, match="must be a finite number"):
        contract.finite_coordinate(value, category="coordinate")


@pytest.mark.parametrize("value", (float("nan"), float("inf")))
def test_finite_coordinate_rejects_non_finite_values(value) -> None:
    with pytest.raises(ValueError, match="must be a finite number"):
        contract.finite_coordinate(value, category="coordinate")


def test_geo_args_reject_conflicting_zip_aliases() -> None:
    with pytest.raises(ValueError, match="same exact ZIP"):
        contract.validated_geo_args({"zip": "25000", "zip5": "25001"})


def test_address_payload_accepts_mappings_without_reusing_the_input() -> None:
    raw_payload_by_field = {"city": "EXAMPLE"}

    payload_by_field = contract.decoded_address_payload(raw_payload_by_field)

    assert payload_by_field == raw_payload_by_field
    assert payload_by_field is not raw_payload_by_field


@pytest.mark.parametrize(
    "value",
    (
        "not-json",
        "[]",
        object(),
    ),
    ids=("invalid-json", "json-non-object", "unsupported-type"),
)
def test_address_payload_rejects_malformed_or_non_object_values(value) -> None:
    with pytest.raises(PTG2ManifestArtifactError, match="payload is malformed"):
        contract.decoded_address_payload(value)
