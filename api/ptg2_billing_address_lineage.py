# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Validate immutable provider-address lineage for exact billing search."""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from datetime import datetime
from typing import Any
from uuid import UUID

from api import ptg2_serving
from api.ptg2_address_policy import (
    PTG_ADDRESS_KIND_PHYSICAL,
    classify_ptg_address_kind,
)
from api.ptg2_billing_geo_contract import (
    GEO_EVIDENCE_SOURCE_ID_BY_LEVEL,
    LOCATION_KEY_PATTERN,
    MAX_ADDRESS_PROVENANCE_ENTRIES,
    MAX_PROVENANCE_LIST_VALUES,
    MAX_PROVENANCE_TEXT_LENGTH,
    PHYSICAL_ADDRESS_PURPOSES,
    PUBLIC_ADDRESS_FIELDS,
    BillingAddressProvenance,
    BillingProviderAddress,
    decoded_address_payload,
)
from process.provider_directory_profile import is_valid_npi
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError


def _optional_internal_ref(value: object, *, category: str) -> str | None:
    if value is None:
        return None
    if type(value) is not str or not value or len(value) > 256:
        raise PTG2ManifestArtifactError(
            f"PTG2 exact billing provider {category} is malformed"
        )
    return value


def _required_internal_ref(value: object, *, category: str) -> str:
    normalized = _optional_internal_ref(value, category=category)
    if normalized is None:
        raise PTG2ManifestArtifactError(
            f"PTG2 exact billing provider {category} is unavailable"
        )
    return normalized


def _canonical_uuid_key(value: object, *, category: str, optional: bool) -> str | None:
    if value is None and optional:
        return None
    if type(value) is not str:
        raise PTG2ManifestArtifactError(
            f"PTG2 exact billing provider {category} is malformed"
        )
    try:
        parsed = UUID(value)
    except (ValueError, AttributeError) as exc:
        raise PTG2ManifestArtifactError(
            f"PTG2 exact billing provider {category} is malformed"
        ) from exc
    if str(parsed) != value:
        raise PTG2ManifestArtifactError(
            f"PTG2 exact billing provider {category} is malformed"
        )
    return value


def _required_location_key(value: object) -> str:
    if type(value) is not str or LOCATION_KEY_PATTERN.fullmatch(value) is None:
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing provider location key is malformed"
        )
    return value


def _required_provenance_text(value: object, *, category: str) -> str:
    if type(value) is not str or not value or len(value) > MAX_PROVENANCE_TEXT_LENGTH:
        raise PTG2ManifestArtifactError(
            f"PTG2 exact billing provider address {category} is malformed"
        )
    return value


def _provenance_text_tuple(
    value: object,
    *,
    category: str,
    required: bool = False,
) -> tuple[str, ...]:
    if value in (None, []) and not required:
        return ()
    if type(value) is not list or not value or len(value) > MAX_PROVENANCE_LIST_VALUES:
        raise PTG2ManifestArtifactError(
            f"PTG2 exact billing provider address {category} is malformed"
        )
    normalized_values = tuple(
        _required_provenance_text(member, category=category) for member in value
    )
    if len(normalized_values) != len(set(normalized_values)):
        raise PTG2ManifestArtifactError(
            f"PTG2 exact billing provider address {category} is malformed"
        )
    return normalized_values


def _provenance_entry(raw_entry: Mapping[str, Any]) -> BillingAddressProvenance:
    source_id = raw_entry.get("source_id")
    if type(source_id) is not int or not 1 <= source_id <= 32767:
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing provider address provenance is malformed"
        )
    dataset_id = _required_provenance_text(
        raw_entry.get("dataset_id"), category="dataset"
    )
    if dataset_id != ptg2_serving._ADDRESS_DATASET_ID_BY_SOURCE_ID.get(source_id):
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing provider address provenance is malformed"
        )
    record_version_id = _required_provenance_text(
        raw_entry.get("record_version_id"), category="record version"
    )
    record_version_ids = _provenance_text_tuple(
        raw_entry.get("record_version_ids"),
        category="record versions",
        required=True,
    )
    if record_version_ids[-1] != record_version_id:
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing provider address provenance is malformed"
        )
    retrieved_at = _required_provenance_text(
        raw_entry.get("retrieved_at"), category="retrieval time"
    )
    try:
        datetime.fromisoformat(
            retrieved_at[:-1] + "+00:00" if retrieved_at.endswith("Z") else retrieved_at
        )
    except ValueError as exc:
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing provider address retrieval time is malformed"
        ) from exc
    return BillingAddressProvenance(
        dataset_id=dataset_id,
        source_id=source_id,
        source_record_id=_required_provenance_text(
            raw_entry.get("source_record_id"), category="source record"
        ),
        record_version_id=record_version_id,
        record_version_ids=record_version_ids,
        retrieved_at=retrieved_at,
        issuer_names=_provenance_text_tuple(
            raw_entry.get("issuer_names"), category="issuer names"
        ),
        source_urls=_provenance_text_tuple(
            raw_entry.get("source_urls"), category="source URLs"
        ),
    )


def _address_provenance(
    address_payload: Mapping[str, Any],
    *,
    admitted_source_id: int,
) -> tuple[BillingAddressProvenance, ...]:
    raw_entries = address_payload.get("address_provenance")
    if (
        type(raw_entries) is not list
        or not raw_entries
        or len(raw_entries) > MAX_ADDRESS_PROVENANCE_ENTRIES
        or any(not isinstance(raw_entry, Mapping) for raw_entry in raw_entries)
    ):
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing provider address provenance is incomplete"
        )
    entries = tuple(_provenance_entry(raw_entry) for raw_entry in raw_entries)
    canonical_entries = tuple(
        sorted(
            entries,
            key=lambda entry: (
                entry.source_id,
                entry.source_record_id,
                entry.record_version_id,
            ),
        )
    )
    if entries != canonical_entries or len(set(entries)) != len(entries):
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing provider address provenance is inconsistent"
        )
    if sum(entry.source_id == admitted_source_id for entry in entries) != 1:
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing provider address provenance is incomplete"
        )
    return entries


def _distance_miles(location_row: Mapping[str, Any]) -> float | None:
    raw_distance = location_row.get("distance_miles")
    if raw_distance is None:
        return None
    if type(raw_distance) not in {int, float}:
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing provider distance is malformed"
        )
    try:
        distance = float(raw_distance)
    except (TypeError, ValueError) as exc:
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing provider distance is malformed"
        ) from exc
    if not math.isfinite(distance) or distance < 0:
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing provider distance is malformed"
        )
    return distance


def _provider_address(
    location_row: Mapping[str, Any],
    *,
    npi: int,
) -> BillingProviderAddress:
    address_payload = decoded_address_payload(location_row.get("address_payload"))
    evidence_level = address_payload.get("geo_evidence_level")
    evidence_source_id = GEO_EVIDENCE_SOURCE_ID_BY_LEVEL.get(evidence_level)
    if evidence_source_id is None:
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing provider geo evidence lineage is unavailable"
        )
    location_key = _required_location_key(address_payload.get("location_key"))
    location_hash = _required_internal_ref(
        location_row.get("location_hash"), category="location hash"
    )
    if location_hash != f"entity_address_unified:{location_key}":
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing provider location witness is inconsistent"
        )
    address_purpose = _optional_internal_ref(
        location_row.get("type"), category="address purpose"
    )
    display_by_field = {
        field: address_payload[field]
        for field in PUBLIC_ADDRESS_FIELDS
        if field in address_payload
    }
    if (
        address_purpose not in PHYSICAL_ADDRESS_PURPOSES
        or classify_ptg_address_kind(display_by_field) != PTG_ADDRESS_KIND_PHYSICAL
    ):
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing provider address is not a physical location"
        )
    return BillingProviderAddress(
        npi=npi,
        location_hash=location_hash,
        distance_miles=_distance_miles(location_row),
        address_key=_canonical_uuid_key(
            address_payload.get("address_key"), category="address key", optional=False
        ),
        address_site_key=_canonical_uuid_key(
            address_payload.get("address_site_key"),
            category="address site key",
            optional=True,
        ),
        location_key=location_key,
        address_purpose=address_purpose,
        display=display_by_field,
        geo_evidence_level=evidence_level,
        geo_evidence_source_id=evidence_source_id,
        provenance=_address_provenance(
            address_payload,
            admitted_source_id=evidence_source_id,
        ),
    )


def provider_addresses_by_npi(
    location_rows: Iterable[Mapping[str, Any]],
    *,
    candidate_npis: frozenset[int],
) -> dict[int, BillingProviderAddress]:
    """Validate one unique, provider-owned address for each returned NPI."""

    addresses_by_npi: dict[int, BillingProviderAddress] = {}
    for location_row in location_rows:
        npi = location_row.get("npi")
        if type(npi) is not int or npi not in candidate_npis or not is_valid_npi(npi):
            raise PTG2ManifestArtifactError(
                "PTG2 exact billing provider address escaped its NPI scope"
            )
        if npi in addresses_by_npi:
            raise PTG2ManifestArtifactError(
                "PTG2 exact billing provider address projection contains duplicates"
            )
        addresses_by_npi[npi] = _provider_address(location_row, npi=npi)
    return addresses_by_npi


__all__ = ["provider_addresses_by_npi"]
