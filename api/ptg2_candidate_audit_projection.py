# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Project retained candidate prices into direct source-witness conditions."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Sequence

from api.ptg2_response import _price_response_fields, _response_wire_value
from api.ptg2_serving import _exact_source_rate_fields
from process.ptg_parts.ptg2_candidate_audit_batch_contract import (
    AuditBatchChallenge,
)
from process.ptg_parts.ptg2_candidate_audit_evidence import (
    canonical_tuple_digest_without_networks,
)
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from scripts.validation import ptg2_v3_source_api_audit as source_audit


_CandidateConditionKey = tuple[str, str, int, int, str]
_CandidateCoordinate = tuple[str, str, int, int]
_CandidateProjectionKey = tuple[str, str, int, int, int]


@dataclass(frozen=True)
class CandidatePriceData:
    """Retain forward occurrence keys and each decoded price payload once."""

    price_keys_by_occurrence: dict[tuple[int, int, int], tuple[int, ...]]
    atom_keys_by_price_key: dict[int, tuple[int, ...]]
    prices_by_key: dict[int, list[dict[str, Any]]]


@dataclass
class _CandidateAvailabilityState:
    tuple_digest_by_projection: dict[_CandidateProjectionKey, str] = field(
        default_factory=dict
    )
    network_sets_by_condition: dict[
        _CandidateConditionKey,
        set[frozenset[str]],
    ] = field(default_factory=dict)
    projection_deliveries: int = 0
    duplicate_availability_deliveries: int = 0

    def add_projection(
        self,
        *,
        query: source_audit.QueryKey,
        code_key: int,
        code_record: Mapping[str, Any],
        source_artifact_key: int,
        provider_network_digests: frozenset[str],
        price_payload: Mapping[str, Any],
    ) -> None:
        """Record one retained price delivery and reuse its tuple projection."""

        self.projection_deliveries += 1
        projection_key = (
            query.code_system,
            query.code,
            query.npi,
            code_key,
            id(price_payload),
        )
        tuple_digest = self.tuple_digest_by_projection.get(projection_key)
        if tuple_digest is None:
            candidate_tuple = _build_canonical_candidate_tuple(
                query,
                code_record,
                (),
                price_payload,
            )
            tuple_digest = canonical_tuple_digest_without_networks(candidate_tuple)
            self.tuple_digest_by_projection[projection_key] = tuple_digest
        condition_key = (
            query.code_system,
            query.code,
            query.npi,
            source_artifact_key,
            tuple_digest,
        )
        available_network_sets = self.network_sets_by_condition.setdefault(
            condition_key,
            set(),
        )
        if provider_network_digests in available_network_sets:
            self.duplicate_availability_deliveries += 1
        available_network_sets.add(provider_network_digests)

    def finalized(
        self,
    ) -> tuple[dict[_CandidateConditionKey, tuple[frozenset[str], ...]], dict[str, int]]:
        """Freeze the match index and expose truthful projection counters."""

        availability_by_condition = {
            condition_key: tuple(
                sorted(
                    network_digest_sets,
                    key=lambda digest_set: tuple(sorted(digest_set)),
                )
            )
            for condition_key, network_digest_sets in self.network_sets_by_condition.items()
        }
        projection_count = len(self.tuple_digest_by_projection)
        return availability_by_condition, {
            "candidate_occurrence_deliveries": self.projection_deliveries,
            "unique_candidate_projections": projection_count,
            "candidate_projection_builds": projection_count,
            "candidate_projection_reuse_deliveries": (
                self.projection_deliveries - projection_count
            ),
            "repeated_candidate_projection_builds": 0,
            "availability_condition_count": len(availability_by_condition),
            "duplicate_availability_deliveries": (
                self.duplicate_availability_deliveries
            ),
        }


def _requested_candidate_coordinates(
    challenges: Sequence[AuditBatchChallenge],
) -> tuple[_CandidateCoordinate, ...]:
    return tuple(
        sorted(
            {
                (
                    challenge.code_system,
                    challenge.code,
                    challenge.npi,
                    challenge.source_artifact_key,
                )
                for challenge in challenges
            }
        )
    )


def _record_coordinate_availability(
    state: _CandidateAvailabilityState,
    coordinate: _CandidateCoordinate,
    code_records_by_pair: Mapping[tuple[str, str], Sequence[Mapping[str, Any]]],
    provider_set_keys_by_npi: Mapping[int, tuple[int, ...]],
    network_digests_by_provider_set_key: Mapping[int, frozenset[str]],
    price_data: CandidatePriceData,
) -> None:
    code_system, code, npi, source_artifact_key = coordinate
    query = source_audit.QueryKey(code_system, code, npi)
    for code_record in code_records_by_pair[(code_system, code)]:
        code_key = int(code_record["code_key"])
        for provider_set_key in provider_set_keys_by_npi.get(npi, ()):
            occurrence_key = (code_key, provider_set_key, source_artifact_key)
            provider_network_digests = network_digests_by_provider_set_key[
                provider_set_key
            ]
            for price_key in price_data.price_keys_by_occurrence.get(occurrence_key, ()):
                for price_payload in price_data.prices_by_key.get(price_key, ()):
                    state.add_projection(
                        query=query,
                        code_key=code_key,
                        code_record=code_record,
                        source_artifact_key=source_artifact_key,
                        provider_network_digests=provider_network_digests,
                        price_payload=price_payload,
                    )


def candidate_availability_index(
    challenges: Sequence[AuditBatchChallenge],
    code_records_by_pair: Mapping[tuple[str, str], Sequence[Mapping[str, Any]]],
    provider_set_keys_by_npi: Mapping[int, tuple[int, ...]],
    network_digests_by_provider_set_key: Mapping[int, frozenset[str]],
    price_data: CandidatePriceData,
) -> tuple[dict[_CandidateConditionKey, tuple[frozenset[str], ...]], dict[str, int]]:
    """Project each unique candidate price once into a direct match index."""

    state = _CandidateAvailabilityState()
    for coordinate in _requested_candidate_coordinates(challenges):
        _record_coordinate_availability(
            state,
            coordinate,
            code_records_by_pair,
            provider_set_keys_by_npi,
            network_digests_by_provider_set_key,
            price_data,
        )
    return state.finalized()


def _build_canonical_candidate_tuple(
    query: source_audit.QueryKey,
    code_record: Mapping[str, Any],
    network_names: tuple[str, ...],
    price_payload: Mapping[str, Any],
) -> source_audit.CanonicalTuple:
    price_response_fields = _price_response_fields([price_payload])
    exact_source_fields = _exact_source_rate_fields(
        reported_code_system=code_record.get("reported_code_system"),
        reported_code=code_record.get("reported_code"),
        negotiation_arrangement=code_record.get("negotiation_arrangement"),
        billing_code_type_version=code_record.get("billing_code_type_version"),
        source_name=code_record.get("source_name"),
        source_description=code_record.get("source_description"),
        network_names=network_names,
        price_response_fields=price_response_fields,
    )
    wire_source_fields = _response_wire_value(exact_source_fields)
    api_item_fields_by_name = {"npi": query.npi, **wire_source_fields}
    normalized_prices = wire_source_fields["prices"]
    if len(normalized_prices) != 1:
        raise PTG2ManifestArtifactError(
            "PTG2 candidate price projection is not singular"
        )
    try:
        return source_audit.canonical_api_price_tuple(
            api_item_fields_by_name,
            normalized_prices[0],
            query,
        )
    except source_audit.ApiSchemaError as exc:
        raise PTG2ManifestArtifactError(
            "PTG2 candidate price projection violates the public API contract"
        ) from exc


__all__ = ["CandidatePriceData", "candidate_availability_index"]
