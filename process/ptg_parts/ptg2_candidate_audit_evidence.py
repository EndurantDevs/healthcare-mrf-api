# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Derive strict candidate-audit expectations only from authenticated raw evidence."""

from __future__ import annotations

import dataclasses
import hashlib
import json
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Mapping

from process.ptg_parts.ptg2_candidate_audit_contract import (
    FastCandidateAuditError,
)
from process.ptg_parts.ptg2_source_witness import SourceWitnessRecord
from scripts.validation import ptg2_v3_source_api_audit as source_audit


RATE_EXPECTED_CONTRACT = "ptg2_v3_source_rate_occurrence_expected_v2"
PROVIDER_EXPECTED_CONTRACT = "ptg2_v3_source_provider_expected_v2"
_TUPLE_FIELDS_EXCEPT_NETWORK = tuple(
    field.name
    for field in dataclasses.fields(source_audit.CanonicalTuple)
    if field.name != "network_names"
)


@dataclass(frozen=True)
class SourceChallenge:
    """One exact source occurrence to locate through the public pricing API."""

    query: source_audit.QueryKey
    expected_tuple: source_audit.CanonicalTuple
    required_network_names: tuple[str, ...]
    raw_source_sha256: str
    negotiated_rate: str
    service_codes: tuple[str, ...]
    modifiers: tuple[str, ...]
    fingerprint: str


def _json_object(raw_json: bytes, *, field_name: str) -> dict[str, Any]:
    try:
        decoded = json.loads(raw_json, parse_float=Decimal, parse_int=int)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise FastCandidateAuditError(f"{field_name}_json_invalid") from exc
    if not isinstance(decoded, dict):
        raise FastCandidateAuditError(f"{field_name}_not_object")
    return decoded


def _mapping(value: Any, *, field_name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise FastCandidateAuditError(f"{field_name}_invalid")
    return dict(value)


def _strict_index(value: Any, *, field_name: str) -> int:
    if type(value) is not int or value < 0:
        raise FastCandidateAuditError(f"{field_name}_invalid")
    return value


def _strict_provider_reference_id(value: Any, *, field_name: str) -> str:
    if type(value) is not int:
        raise FastCandidateAuditError(f"{field_name}_invalid")
    return str(value)


def _provider_groups(
    container: Mapping[str, Any],
    *,
    field_name: str,
) -> list[Any]:
    groups = container.get("provider_groups")
    if not isinstance(groups, list) or not groups:
        raise FastCandidateAuditError(f"{field_name}_invalid")
    return groups


def _npi_at_source_coordinate(
    groups: list[Any],
    evidence: Mapping[str, Any],
) -> int:
    group_ordinal = _strict_index(
        evidence.get("provider_group_ordinal"),
        field_name="source_provider_group_ordinal",
    )
    npi_ordinal = _strict_index(
        evidence.get("npi_ordinal"),
        field_name="source_provider_npi_ordinal",
    )
    if group_ordinal >= len(groups) or not isinstance(groups[group_ordinal], Mapping):
        raise FastCandidateAuditError("source_provider_group_coordinate_invalid")
    npi_values = groups[group_ordinal].get("npi")
    if not isinstance(npi_values, list) or npi_ordinal >= len(npi_values):
        raise FastCandidateAuditError("source_provider_npi_coordinate_invalid")
    selected_npi = npi_values[npi_ordinal]
    if type(selected_npi) is not int or not 1_000_000_000 <= selected_npi <= 9_999_999_999:
        raise FastCandidateAuditError("source_provider_npi_invalid")
    return selected_npi


def _inline_provider_evidence(
    raw_rate: Mapping[str, Any],
    evidence: Mapping[str, Any],
) -> tuple[int, list[str]]:
    if set(evidence) != {
        "source_kind",
        "provider_group_ordinal",
        "npi_ordinal",
    }:
        raise FastCandidateAuditError("source_inline_provider_evidence_invalid")
    groups = _provider_groups(raw_rate, field_name="source_inline_provider_groups")
    return _npi_at_source_coordinate(groups, evidence), []


def _referenced_provider_evidence(
    witness_record: SourceWitnessRecord,
    raw_rate: Mapping[str, Any],
    evidence: Mapping[str, Any],
) -> tuple[int, list[str]]:
    if set(evidence) != {
        "source_kind",
        "provider_reference_id",
        "provider_group_ordinal",
        "npi_ordinal",
    }:
        raise FastCandidateAuditError("source_referenced_provider_evidence_invalid")
    if witness_record.linked_provider_json is None:
        raise FastCandidateAuditError("source_linked_provider_missing")
    linked_provider = _json_object(
        witness_record.linked_provider_json,
        field_name="source_linked_provider",
    )
    evidence_reference = str(evidence.get("provider_reference_id") or "")
    linked_reference = _strict_provider_reference_id(
        linked_provider.get("provider_group_id"),
        field_name="source_linked_provider_id",
    )
    raw_references = raw_rate.get("provider_references")
    if not isinstance(raw_references, list) or not raw_references:
        raise FastCandidateAuditError("source_provider_references_invalid")
    source_references = {
        _strict_provider_reference_id(
            reference,
            field_name="source_provider_reference",
        )
        for reference in raw_references
    }
    if evidence_reference != linked_reference or linked_reference not in source_references:
        raise FastCandidateAuditError("source_provider_reference_link_mismatch")
    groups = _provider_groups(
        linked_provider,
        field_name="source_linked_provider_groups",
    )
    provider_network_names = source_audit.canonical_list(
        linked_provider.get("network_name")
    )
    return _npi_at_source_coordinate(groups, evidence), provider_network_names


def _source_npi_and_networks(
    witness_record: SourceWitnessRecord,
    raw_rate: Mapping[str, Any],
) -> tuple[int, tuple[str, ...]]:
    evidence = _mapping(
        witness_record.provider_evidence,
        field_name="source_provider_evidence",
    )
    source_kind = evidence.get("source_kind")
    if source_kind == "inline_provider_group":
        selected_npi, provider_network_names = _inline_provider_evidence(
            raw_rate,
            evidence,
        )
    elif source_kind == "provider_reference":
        selected_npi, provider_network_names = _referenced_provider_evidence(
            witness_record,
            raw_rate,
            evidence,
        )
    else:
        raise FastCandidateAuditError("source_provider_evidence_kind_invalid")
    required_network_names = source_audit.canonical_list(
        [
            *source_audit.canonical_list(raw_rate.get("network_names")),
            *provider_network_names,
        ]
    )
    return selected_npi, tuple(required_network_names)


def source_challenge(witness_record: SourceWitnessRecord) -> SourceChallenge:
    """Derive one API challenge without trusting scanner-authored field values."""

    if witness_record.kind != "rate_occurrence" or witness_record.procedure is None:
        raise FastCandidateAuditError("source_occurrence_witness_invalid")
    if dict(witness_record.expected) != {"contract": RATE_EXPECTED_CONTRACT}:
        raise FastCandidateAuditError("source_occurrence_expected_contract_invalid")
    raw_rate = _json_object(witness_record.raw_json, field_name="source_rate")
    prices = raw_rate.get("negotiated_prices")
    price_ordinal = witness_record.coordinate[2]
    if (
        not isinstance(prices, list)
        or price_ordinal >= len(prices)
        or not isinstance(prices[price_ordinal], Mapping)
    ):
        raise FastCandidateAuditError("source_rate_price_coordinate_invalid")
    raw_price_by_field = dict(prices[price_ordinal])
    procedure_by_field = dict(witness_record.procedure)
    code_system = source_audit.canonical_code_system(
        procedure_by_field.get("billing_code_type")
    )
    code = source_audit.canonical_catalog_code(
        code_system,
        procedure_by_field.get("billing_code"),
    )
    npi, required_network_names = _source_npi_and_networks(
        witness_record,
        raw_rate,
    )
    if not code_system or not code:
        raise FastCandidateAuditError("source_rate_query_invalid")
    query = source_audit.QueryKey(code_system, code, npi)
    try:
        canonical_tuple = source_audit.CanonicalTuple.from_parts(
            query,
            procedure_by_field.get("negotiation_arrangement"),
            raw_price_by_field,
            billing_code_type_version=procedure_by_field.get("billing_code_type_version"),
            name=procedure_by_field.get("name"),
            description=procedure_by_field.get("description"),
            network_names=required_network_names,
        )
    except (source_audit.SourceCoverageError, ValueError) as exc:
        raise FastCandidateAuditError("source_rate_tuple_invalid") from exc
    canonical_price = source_audit.canonical_price_payload(raw_price_by_field)
    fingerprint_payload = (
        f"{witness_record.raw_source_sha256}:{witness_record.raw_sha256}:"
        f"{witness_record.priority}:{witness_record.tie_breaker}:"
        f"{witness_record.coordinate}"
    )
    return SourceChallenge(
        query=query,
        expected_tuple=canonical_tuple,
        required_network_names=required_network_names,
        raw_source_sha256=witness_record.raw_source_sha256,
        negotiated_rate=canonical_price["negotiated_rate"],
        service_codes=tuple(canonical_price["service_code"]),
        modifiers=tuple(canonical_price["billing_code_modifier"]),
        fingerprint=hashlib.sha256(fingerprint_payload.encode("utf-8")).hexdigest()[:24],
    )


def validate_provider_witness(record: SourceWitnessRecord) -> None:
    """Validate the only provider claims retained by the V2 witness contract."""

    if record.kind != "provider_reference":
        raise FastCandidateAuditError("source_provider_witness_invalid")
    raw_provider = _json_object(record.raw_json, field_name="source_provider")
    raw_group_id = _strict_provider_reference_id(
        raw_provider.get("provider_group_id"),
        field_name="source_provider_id",
    )
    expected_by_field = dict(record.expected)
    if set(expected_by_field) != {"contract", "provider_group_id"} or (
        expected_by_field.get("contract") != PROVIDER_EXPECTED_CONTRACT
        or str(expected_by_field.get("provider_group_id") or "") != raw_group_id
    ):
        raise FastCandidateAuditError("source_provider_expected_contract_invalid")
    groups = _provider_groups(raw_provider, field_name="source_provider_groups")
    for group in groups:
        if not isinstance(group, Mapping):
            raise FastCandidateAuditError("source_provider_group_invalid")
        npi_values = group.get("npi")
        if not isinstance(npi_values, list):
            raise FastCandidateAuditError("source_provider_npi_invalid")
        if any(type(raw_npi) is not int for raw_npi in npi_values):
            raise FastCandidateAuditError("source_provider_npi_invalid")


def is_tuple_matching_challenge(
    occurrence_key: str,
    candidate_tuple: source_audit.CanonicalTuple,
    challenge: SourceChallenge,
) -> bool:
    """Match exact source fields while allowing networks from other linked references."""

    try:
        occurrence_payload = json.loads(occurrence_key)
    except json.JSONDecodeError:
        return False
    if not isinstance(occurrence_payload, dict) or (
        occurrence_payload.get("raw_container_sha256")
        != challenge.raw_source_sha256
    ):
        return False
    if any(
        getattr(candidate_tuple, field_name)
        != getattr(challenge.expected_tuple, field_name)
        for field_name in _TUPLE_FIELDS_EXCEPT_NETWORK
    ):
        return False
    return set(challenge.required_network_names).issubset(
        candidate_tuple.network_names
    )


__all__ = [
    "SourceChallenge",
    "source_challenge",
    "is_tuple_matching_challenge",
    "validate_provider_witness",
]
