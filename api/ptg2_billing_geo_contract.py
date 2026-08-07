# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Immutable contracts and validation for exact billing GEO witnesses."""

from __future__ import annotations

import json
import math
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from api import ptg2_serving
from api.ptg2_billing_exact_contract import (
    BillingRateOccurrenceWitness,
    billing_rate_occurrence_sort_key,
)
from process.provider_directory_profile import is_valid_npi
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError

ZIP5_PATTERN = re.compile(r"[0-9]{5}")
LOCATION_KEY_PATTERN = re.compile(r"[0-9a-f]{64}")
MAX_PROVIDER_GROUPS = 2048
MAX_PROVIDER_RATE_WITNESSES = 32768
DEFAULT_RADIUS_MILES = 25.0
MAX_RADIUS_MILES = 100.0
MAX_ADDRESS_PROVENANCE_ENTRIES = 16
MAX_PROVENANCE_LIST_VALUES = 64
MAX_PROVENANCE_TEXT_LENGTH = 4096
BILLING_ADDRESS_SELECTION_CONTRACT = "ptg2_billing_provider_address_selection_v1"
GEO_EVIDENCE_SOURCE_ID_BY_LEVEL = {
    "nppes_registry_address": 1,
    "multi_issuer_marketplace_address": 2,
    "cms_doctors_source_with_nppes_identity_anchor": 3,
}
PHYSICAL_ADDRESS_PURPOSES = frozenset({"primary", "secondary", "practice", "site"})
PUBLIC_ADDRESS_FIELDS = (
    "first_line",
    "second_line",
    "city",
    "state",
    "postal_code",
    "country_code",
    "telephone_number",
    "fax_number",
    "phone_number",
    "phone_extension",
    "fax_number_digits",
    "fax_extension",
    "lat",
    "long",
)


@dataclass(frozen=True, slots=True, repr=False)
class BillingProviderRateWitness:
    """One exact rate occurrence expanded only through its own group members."""

    snapshot_key: int
    code_key: int
    source_key: int
    source_record_ordinal: int
    provider_group_ref: str
    provider_set_key: int
    price_key: int
    occurrence_ordinal: int
    npi: int

    @property
    def stable_rate_key(self) -> tuple[int, int, int, int, int, int, str]:
        """Return the complete internal key for one exact rate occurrence."""

        return (
            self.code_key,
            self.provider_set_key,
            self.price_key,
            self.source_key,
            self.source_record_ordinal,
            self.occurrence_ordinal,
            self.provider_group_ref,
        )

    def __repr__(self) -> str:
        return (
            "<billing-provider-rate-witness "
            f"snapshot_key={self.snapshot_key} npi={self.npi} "
            f"source_record_ordinal={self.source_record_ordinal} "
            "provider_group_ref=<redacted> "
            f"code_key={self.code_key} provider_set_key={self.provider_set_key} "
            f"price_key={self.price_key}>"
        )


@dataclass(frozen=True, slots=True, repr=False)
class BillingAddressProvenance:
    """One complete immutable source tuple for a selected provider address."""

    dataset_id: str
    source_id: int
    source_record_id: str
    record_version_id: str
    record_version_ids: tuple[str, ...]
    retrieved_at: str
    issuer_names: tuple[str, ...]
    source_urls: tuple[str, ...]

    def __repr__(self) -> str:
        return (
            "<billing-address-provenance "
            f"source_id={self.source_id} source=<redacted>>"
        )


@dataclass(frozen=True, slots=True, repr=False)
class BillingProviderAddress:
    """One selected provider-owned address with immutable source lineage."""

    npi: int
    location_hash: str
    distance_miles: float | None
    address_key: str
    address_site_key: str | None
    location_key: str
    address_purpose: str | None
    display: Mapping[str, Any]
    geo_evidence_level: str
    geo_evidence_source_id: int
    provenance: tuple[BillingAddressProvenance, ...]
    selection_contract: str = BILLING_ADDRESS_SELECTION_CONTRACT

    @property
    def stable_geo_key(self) -> tuple[int, float, int]:
        """Return the distance-first stable provider key."""

        return (
            1 if self.distance_miles is None else 0,
            0.0 if self.distance_miles is None else self.distance_miles,
            self.npi,
        )

    def __repr__(self) -> str:
        return (
            "<billing-provider-address "
            f"npi={self.npi} distance_miles={self.distance_miles} "
            "address=<redacted>>"
        )


@dataclass(frozen=True, slots=True, repr=False)
class BillingProviderGeoWitness:
    """One provider/rate witness joined only to that provider's own address."""

    provider_rate: BillingProviderRateWitness
    address: BillingProviderAddress

    @property
    def stable_sort_key(self) -> tuple[Any, ...]:
        """Return the complete internal GEO and rate sort coordinate."""

        return (*self.address.stable_geo_key, *self.provider_rate.stable_rate_key)

    def __repr__(self) -> str:
        return (
            "<billing-provider-geo-witness "
            f"npi={self.provider_rate.npi} "
            "rate=<redacted> address=<redacted>>"
        )


@dataclass(frozen=True, slots=True, repr=False)
class BillingGeoSelection:
    """GEO-filtered witnesses plus explicit address-projection availability."""

    address_projection_available: bool
    witnesses: tuple[BillingProviderGeoWitness, ...]

    def __repr__(self) -> str:
        return (
            "<billing-geo-selection "
            f"address_projection_available={self.address_projection_available} "
            f"witness_count={len(self.witnesses)}>"
        )


@dataclass(frozen=True, slots=True, repr=False)
class BillingProviderGeoPriceWitness:
    """One GEO witness with its ordered, filtered negotiated-price atoms."""

    geo_witness: BillingProviderGeoWitness
    prices: tuple[Mapping[str, Any], ...]

    @property
    def stable_sort_key(self) -> tuple[Any, ...]:
        """Return the underlying exact GEO witness sort coordinate."""

        return self.geo_witness.stable_sort_key

    def __repr__(self) -> str:
        return (
            "<billing-provider-geo-price-witness "
            f"npi={self.geo_witness.provider_rate.npi} "
            f"price_count={len(self.prices)} witness=<redacted>>"
        )


def validated_provider_npi(value: object, *, optional: bool = False) -> int | None:
    """Validate an optional exact checksum-valid NPI integer."""

    if optional and value is None:
        return None
    if type(value) is not int or not is_valid_npi(value):
        raise ValueError("provider NPI must be a checksum-valid 10-digit NPI")
    return value


def bounded_tuple(
    values: Iterable[Any],
    *,
    maximum_count: int,
    error_message: str,
) -> tuple[Any, ...]:
    """Retain a bounded iterable as a tuple without eager unbounded input."""

    retained_values: list[Any] = []
    for value in values:
        if len(retained_values) >= maximum_count:
            raise PTG2ManifestArtifactError(error_message)
        retained_values.append(value)
    return tuple(retained_values)


def validated_rate_witnesses(
    rate_witnesses: Iterable[BillingRateOccurrenceWitness],
) -> tuple[BillingRateOccurrenceWitness, ...]:
    """Require bounded, typed, canonically ordered rate witnesses."""

    witnesses = bounded_tuple(
        rate_witnesses,
        maximum_count=MAX_PROVIDER_RATE_WITNESSES,
        error_message="PTG2 exact billing provider expansion exceeds its rate limit",
    )
    if any(type(witness) is not BillingRateOccurrenceWitness for witness in witnesses):
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing provider expansion contains an invalid rate witness"
        )
    if witnesses != tuple(sorted(witnesses, key=billing_rate_occurrence_sort_key)):
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing rate witnesses are not canonically ordered"
        )
    return witnesses


def finite_coordinate(value: object, *, category: str) -> float:
    """Return a finite numeric coordinate without accepting booleans."""

    if type(value) not in {int, float}:
        raise ValueError(f"{category} must be a finite number")
    coordinate = float(value)
    if not math.isfinite(coordinate):
        raise ValueError(f"{category} must be a finite number")
    return coordinate


def validated_geo_args(geo_args: Mapping[str, Any]) -> dict[str, Any]:
    """Normalize one exact-ZIP or bounded coordinate GEO selector."""

    zip5_value = geo_args.get("zip5")
    legacy_zip = geo_args.get("zip")
    if zip5_value is not None and legacy_zip is not None and zip5_value != legacy_zip:
        raise ValueError("zip and zip5 must identify the same exact ZIP")
    zip5 = zip5_value if zip5_value is not None else legacy_zip
    if zip5 is not None and (
        type(zip5) is not str or ZIP5_PATTERN.fullmatch(zip5) is None
    ):
        raise ValueError("zip5 must contain exactly five ASCII digits")
    latitude_value = geo_args.get("lat")
    longitude_value = geo_args.get("long")
    if (latitude_value is None) != (longitude_value is None):
        raise ValueError("lat and long must be supplied together")
    geo_parameters_by_name = {
        "mode": ptg2_serving.PTG2_MODE_EXACT_SOURCE,
        "include_evidence": True,
    }
    if zip5 is not None:
        geo_parameters_by_name["zip5"] = zip5
    if latitude_value is not None:
        latitude = finite_coordinate(latitude_value, category="lat")
        longitude = finite_coordinate(longitude_value, category="long")
        if not -90.0 <= latitude <= 90.0 or not -180.0 <= longitude <= 180.0:
            raise ValueError("lat or long is outside its geographic range")
        radius = finite_coordinate(
            geo_args.get("radius_miles", DEFAULT_RADIUS_MILES),
            category="radius_miles",
        )
        if not 0.0 <= radius <= MAX_RADIUS_MILES:
            raise ValueError("radius_miles is outside its bounded range")
        geo_parameters_by_name.update(
            lat=latitude,
            long=longitude,
            radius_miles=radius,
        )
    elif geo_args.get("radius_miles") is not None:
        raise ValueError("radius_miles requires lat and long")
    if zip5 is None and latitude_value is None:
        raise ValueError("an exact zip5 or coordinate pair is required")
    return geo_parameters_by_name


def decoded_address_payload(value: object) -> dict[str, Any]:
    """Decode one JSON object address payload or fail closed."""

    if isinstance(value, Mapping):
        payload = dict(value)
    elif type(value) is str:
        try:
            payload = json.loads(value)
        except (TypeError, ValueError) as exc:
            raise PTG2ManifestArtifactError(
                "PTG2 exact billing provider address payload is malformed"
            ) from exc
    else:
        payload = None
    if not isinstance(payload, dict):
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing provider address payload is malformed"
        )
    return payload


__all__ = [
    "BILLING_ADDRESS_SELECTION_CONTRACT",
    "MAX_PROVIDER_GROUPS",
    "MAX_PROVIDER_RATE_WITNESSES",
    "BillingAddressProvenance",
    "BillingGeoSelection",
    "BillingProviderAddress",
    "BillingProviderGeoPriceWitness",
    "BillingProviderGeoWitness",
    "BillingProviderRateWitness",
    "bounded_tuple",
    "decoded_address_payload",
    "finite_coordinate",
    "validated_geo_args",
    "validated_provider_npi",
    "validated_rate_witnesses",
]
