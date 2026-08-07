# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Validate bounded identifiers used by the exact billing witness reader."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from api.ptg2_billing_entity_source_resolution import (
    BillingEntitySourceWitness,
    ResolvedBillingEntitySourceScope,
)
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from process.ptg_parts.ptg2_tax_identity_source_projection import (
    TaxIdentitySourceProjectionError,
    TaxIdentitySourcePublication,
    tax_identity_source_publication_from_metadata,
)

MAX_PROVIDER_GROUPS = 2048
MAX_SOURCE_WITNESSES = 8192
MAX_DENSE_KEY = 2**31 - 1
MAX_PRICE_KEY = 2**32 - 1


@dataclass(frozen=True, slots=True, repr=False)
class BillingRateOccurrenceWitness:
    """One internal exact billing path retained through the rate occurrence."""

    snapshot_key: int
    code_key: int
    source_key: int
    source_record_ordinal: int
    provider_group_ref: str
    provider_set_key: int
    price_key: int
    occurrence_ordinal: int

    def __repr__(self) -> str:
        return (
            "<billing-rate-occurrence-witness "
            f"snapshot_key={self.snapshot_key} "
            f"code_key={self.code_key} "
            f"source_key={self.source_key} "
            f"source_record_ordinal={self.source_record_ordinal} "
            "provider_group_ref=<redacted> "
            f"provider_set_key={self.provider_set_key} "
            f"price_key={self.price_key} "
            f"occurrence_ordinal={self.occurrence_ordinal}>"
        )


def billing_rate_occurrence_sort_key(
    witness: BillingRateOccurrenceWitness,
) -> tuple[int, int, int, int, int, int, str]:
    """Return the canonical coordinate shared by every witness stage."""

    return (
        witness.code_key,
        witness.provider_set_key,
        witness.price_key,
        witness.source_key,
        witness.source_record_ordinal,
        witness.occurrence_ordinal,
        witness.provider_group_ref,
    )


def validated_rate_occurrence_witness(
    witness: BillingRateOccurrenceWitness,
) -> BillingRateOccurrenceWitness:
    """Validate every coordinate carried across the exact-reader boundary."""

    if (
        type(witness) is not BillingRateOccurrenceWitness
        or type(witness.snapshot_key) is not int
        or not 1 <= witness.snapshot_key < 2**63
        or type(witness.source_record_ordinal) is not int
        or not 0 <= witness.source_record_ordinal < 2**63
    ):
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing rate witness has invalid coordinates"
        )
    try:
        normalized_dense_key(witness.code_key, category="code")
        normalized_dense_key(witness.source_key, category="source")
        canonical_ref(witness.provider_group_ref, category="provider group")
        normalized_dense_key(witness.provider_set_key, category="provider set")
        normalized_dense_key(
            witness.price_key,
            category="price",
            maximum=MAX_PRICE_KEY,
        )
        normalized_dense_key(witness.occurrence_ordinal, category="occurrence")
    except PTG2ManifestArtifactError as exc:
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing rate witness has invalid coordinates"
        ) from exc
    return witness


@dataclass(frozen=True, slots=True)
class ExactGroupProjection:
    snapshot_key: int
    source_count: int
    group_refs_by_source: dict[int, dict[str, int]]
    provider_group_refs: tuple[str, ...]
    sets_by_group: dict[str, tuple[str, ...]]
    group_keys_by_id: dict[str, int]


@dataclass(frozen=True, slots=True)
class ExactSetProjection:
    provider_set_keys_by_id: dict[str, int]
    group_keys_by_set: dict[int, tuple[int, ...]]


def distinct_dense_keys(
    values: Iterable[int],
    *,
    category: str,
    maximum_count: int,
) -> tuple[int, ...]:
    """Return unique sorted dense keys while bounding the raw input."""

    normalized_values: set[int] = set()
    value_count = 0
    for value in values:
        value_count += 1
        if value_count > maximum_count:
            raise PTG2ManifestArtifactError(
                f"PTG2 exact billing {category} scope exceeds its limit"
            )
        normalized_values.add(normalized_dense_key(value, category=category))
    return tuple(sorted(normalized_values))


def normalized_dense_key(
    value: int,
    *,
    category: str,
    maximum: int = MAX_DENSE_KEY,
) -> int:
    """Validate one exact integer key without accepting booleans."""

    if type(value) is not int or not 0 <= value <= maximum:
        raise PTG2ManifestArtifactError(
            f"PTG2 exact billing {category} key is out of range"
        )
    return value


def canonical_ref(value: object, *, category: str) -> str:
    """Validate one lowercase 128-bit hexadecimal graph reference."""

    if type(value) is not str or len(value) != 32 or value != value.lower():
        raise PTG2ManifestArtifactError(
            f"PTG2 exact billing {category} reference is malformed"
        )
    try:
        raw_value = bytes.fromhex(value)
    except ValueError as exc:
        raise PTG2ManifestArtifactError(
            f"PTG2 exact billing {category} reference is malformed"
        ) from exc
    if len(raw_value) != 16:
        raise PTG2ManifestArtifactError(
            f"PTG2 exact billing {category} reference is malformed"
        )
    return value


def _source_coordinate(
    witness: BillingEntitySourceWitness,
    *,
    source_count: int,
) -> tuple[int, int, str]:
    if (
        type(witness.source_key) is not int
        or not 0 <= witness.source_key < source_count
        or type(witness.source_record_ordinal) is not int
        or not 0 <= witness.source_record_ordinal < 2**63
    ):
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing source scope contains an invalid witness"
        )
    try:
        provider_group_ref = canonical_ref(
            witness.provider_group_ref,
            category="provider group",
        )
    except PTG2ManifestArtifactError as exc:
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing source scope contains an invalid witness"
        ) from exc
    return witness.source_key, witness.source_record_ordinal, provider_group_ref


def _require_source_geometry(
    source_scope: ResolvedBillingEntitySourceScope,
    *,
    snapshot_key: int,
    source_count: int,
    source_publication: TaxIdentitySourcePublication | None,
) -> None:
    if (
        type(source_scope) is not ResolvedBillingEntitySourceScope
        or type(source_scope.snapshot_key) is not int
        or source_scope.snapshot_key != snapshot_key
        or type(source_scope.witnesses) is not tuple
    ):
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing source scope belongs to another snapshot"
        )
    if (
        type(source_publication) is not TaxIdentitySourcePublication
        or type(source_scope.publication) is not TaxIdentitySourcePublication
    ):
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing source geometry is unavailable"
        )
    try:
        canonical_publication = tax_identity_source_publication_from_metadata(
            source_publication.as_dict()
        )
    except TaxIdentitySourceProjectionError as exc:
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing source geometry is unavailable"
        ) from exc
    if (
        canonical_publication != source_publication
        or source_scope.publication != source_publication
        or source_publication.source_count != source_count
    ):
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing source geometry does not match its scope"
        )


def _validated_source_coordinates(
    source_scope: ResolvedBillingEntitySourceScope,
    *,
    source_count: int,
) -> tuple[tuple[int, int, str], ...]:
    coordinates = tuple(
        _source_coordinate(witness, source_count=source_count)
        for witness in source_scope.witnesses[: MAX_SOURCE_WITNESSES + 1]
    )
    if not coordinates:
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing source scope contains no witnesses"
        )
    if len(coordinates) > MAX_SOURCE_WITNESSES:
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing source scope exceeds its witness limit"
        )
    if coordinates != tuple(sorted(coordinates)) or len(set(coordinates)) != len(
        coordinates
    ):
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing source scope contains inconsistent witnesses"
        )
    if len({(key, ordinal) for key, ordinal, _ref in coordinates}) != len(coordinates):
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing source scope contains inconsistent witnesses"
        )
    return coordinates


def source_groups(
    source_scope: ResolvedBillingEntitySourceScope,
    *,
    snapshot_key: int,
    source_count: int,
    source_publication: TaxIdentitySourcePublication | None,
) -> dict[int, dict[str, int]]:
    """Validate and index exact source, record, and provider-group witnesses."""

    _require_source_geometry(
        source_scope,
        snapshot_key=snapshot_key,
        source_count=source_count,
        source_publication=source_publication,
    )
    coordinates = _validated_source_coordinates(
        source_scope,
        source_count=source_count,
    )
    groups_by_source: dict[int, dict[str, int]] = {}
    for source_key, source_record_ordinal, provider_group_ref in coordinates:
        source_group_map = groups_by_source.setdefault(source_key, {})
        if provider_group_ref in source_group_map:
            raise PTG2ManifestArtifactError(
                "PTG2 exact billing source scope contains inconsistent witnesses"
            )
        source_group_map[provider_group_ref] = source_record_ordinal
    provider_group_refs = {
        provider_group_ref
        for source_group_map in groups_by_source.values()
        for provider_group_ref in source_group_map
    }
    if len(provider_group_refs) > MAX_PROVIDER_GROUPS:
        raise PTG2ManifestArtifactError(
            "PTG2 exact billing source scope exceeds its provider-group limit"
        )
    return groups_by_source


__all__ = [
    "BillingRateOccurrenceWitness",
    "ExactGroupProjection",
    "ExactSetProjection",
    "MAX_DENSE_KEY",
    "MAX_PRICE_KEY",
    "MAX_PROVIDER_GROUPS",
    "MAX_SOURCE_WITNESSES",
    "billing_rate_occurrence_sort_key",
    "canonical_ref",
    "distinct_dense_keys",
    "normalized_dense_key",
    "source_groups",
    "validated_rate_occurrence_witness",
]
