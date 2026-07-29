# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""FHIR Organization extraction into protected TIN-to-NPI evidence."""

from __future__ import annotations

import datetime as dt
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from typing import Any

from process.tin_npi_connector_evidence import (
    FhirOrganizationEvidenceResult,
    FhirTinNpiEvidence,
    _strict_evidence_id,
    _verified_record_identity_sha256,
)
from process.tin_npi_connector_policy import (
    DEFAULT_FHIR_TIN_NPI_IDENTIFIER_POLICY,
    FhirTinNpiIdentifierPolicy,
    FhirTinNpiIdentifierRule,
)
from process.tin_npi_connector_security import (
    TinTaxIdentityToken,
    TinTokenProjector,
    canonical_token_policy_id,
    normalize_ein,
)
from process.tin_npi_connector_source import _strict_hash_hex
from process.tin_npi_connector_support import (
    FhirOrganizationEvidenceState,
    TinNpiConnectorError,
    _MalformedFhirIdentifierPeriod,
    _UnresolvedFhirIdentifierPeriod,
)
from process.tin_npi_connector_temporal import (
    _as_utc_datetime,
    _has_identifier_match,
    _is_identifier_effective,
    _normalize_npi,
    canonical_evidence_as_of,
)


@dataclass(frozen=True)
class _ExtractionContext:
    """Verified source and policy inputs shared by one extraction pass."""

    source_id: str
    source_endpoint_id: str
    source_dataset_id: str
    source_record_identity_sha256: bytes
    source_record_payload_hash: str
    token_projectors: tuple[TinTokenProjector, ...]
    evidence_as_of: dt.datetime | dt.date | str
    identifier_policy: FhirTinNpiIdentifierPolicy


@dataclass(frozen=True)
class _NormalizedIdentifierValues:
    """Canonical NPI set and sole EIN selected from one Organization."""

    npis: tuple[int, ...]
    normalized_ein: str


def _canonical_token_projectors(
    token_projectors: object,
) -> tuple[TinTokenProjector, ...]:
    """Validate exact policy ordering before any protected projection occurs."""

    if type(token_projectors) is not tuple or not token_projectors:
        raise TinNpiConnectorError("TIN token projectors are invalid")
    policy_ids: list[str] = []
    for token_projector in token_projectors:
        try:
            policy_ids.append(
                canonical_token_policy_id(token_projector.token_policy_id)
            )
        except (AttributeError, TinNpiConnectorError):
            raise TinNpiConnectorError("TIN token projectors are invalid") from None
    if policy_ids != sorted(set(policy_ids)):
        raise TinNpiConnectorError("TIN token projectors are duplicated or unordered")
    return token_projectors


def _state_result(
    state: FhirOrganizationEvidenceState,
) -> FhirOrganizationEvidenceResult:
    """Return one evidence-free terminal extraction result."""

    return FhirOrganizationEvidenceResult(state)


def _identifier_cutoff(
    evidence_as_of: dt.datetime | dt.date | str,
) -> tuple[str, dt.datetime]:
    """Return canonical cutoff text and its UTC datetime representation."""

    canonical_as_of = canonical_evidence_as_of(evidence_as_of)
    cutoff = _as_utc_datetime(
        dt.datetime.fromisoformat(canonical_as_of[:-1] + "+00:00")
    )
    if cutoff is None:
        raise TinNpiConnectorError("evidence cutoff is invalid")
    return canonical_as_of, cutoff


def _classify_effective_identifier(
    identifier: Mapping[str, Any],
    *,
    identifier_rule: FhirTinNpiIdentifierRule,
    evidence_cutoff: dt.datetime,
) -> tuple[str | None, FhirOrganizationEvidenceState | None]:
    """Classify one active identifier or return its terminal period error."""

    is_npi = _has_identifier_match(
        identifier,
        systems=identifier_rule.npi_systems,
        type_codings=identifier_rule.npi_type_codings,
    )
    is_ein = _has_identifier_match(
        identifier,
        systems=identifier_rule.ein_systems,
        type_codings=identifier_rule.ein_type_codings,
    )
    if is_npi and is_ein:
        return None, FhirOrganizationEvidenceState.CONFLICTING_IDENTIFIER_CLASS
    if not is_npi and not is_ein:
        return None, None
    try:
        is_effective = _is_identifier_effective(
            identifier,
            observed_at=evidence_cutoff,
            policy=identifier_rule,
        )
    except _UnresolvedFhirIdentifierPeriod:
        return None, FhirOrganizationEvidenceState.UNRESOLVED_IDENTIFIER_PERIOD
    except _MalformedFhirIdentifierPeriod:
        return None, FhirOrganizationEvidenceState.MALFORMED_IDENTIFIER_PERIOD
    if not is_effective:
        return None, None
    return ("npi" if is_npi else "ein"), None


def _select_effective_identifiers(
    identifiers: Sequence[Any],
    *,
    identifier_rule: FhirTinNpiIdentifierRule,
    evidence_cutoff: dt.datetime,
) -> (
    tuple[tuple[Mapping[str, Any], ...], tuple[Mapping[str, Any], ...]]
    | FhirOrganizationEvidenceResult
):
    """Select active NPI/EIN identifiers or return the first terminal error."""

    npi_identifiers: list[Mapping[str, Any]] = []
    ein_identifiers: list[Mapping[str, Any]] = []
    for identifier in identifiers:
        if not isinstance(identifier, Mapping):
            continue
        identifier_class, terminal_state = _classify_effective_identifier(
            identifier,
            identifier_rule=identifier_rule,
            evidence_cutoff=evidence_cutoff,
        )
        if terminal_state is not None:
            return _state_result(terminal_state)
        if identifier_class == "npi":
            npi_identifiers.append(identifier)
        elif identifier_class == "ein":
            ein_identifiers.append(identifier)
    if not npi_identifiers and not ein_identifiers:
        return _state_result(FhirOrganizationEvidenceState.MISSING_IDENTIFIERS)
    if not npi_identifiers:
        return _state_result(FhirOrganizationEvidenceState.MISSING_NPI)
    if not ein_identifiers:
        return _state_result(FhirOrganizationEvidenceState.MISSING_EIN)
    return tuple(npi_identifiers), tuple(ein_identifiers)


def _normalize_selected_identifiers(
    npi_identifiers: tuple[Mapping[str, Any], ...],
    ein_identifiers: tuple[Mapping[str, Any], ...],
) -> _NormalizedIdentifierValues | FhirOrganizationEvidenceResult:
    """Normalize selected values or return a non-sensitive terminal state."""

    try:
        npis = tuple(
            sorted(
                {
                    _normalize_npi(identifier.get("value"))
                    for identifier in npi_identifiers
                }
            )
        )
    except TinNpiConnectorError:
        return _state_result(FhirOrganizationEvidenceState.MALFORMED_NPI)
    try:
        normalized_eins = {
            normalize_ein(identifier.get("value")) for identifier in ein_identifiers
        }
    except TinNpiConnectorError:
        return _state_result(FhirOrganizationEvidenceState.MALFORMED_EIN)
    if len(normalized_eins) != 1:
        return _state_result(FhirOrganizationEvidenceState.AMBIGUOUS_EIN)
    return _NormalizedIdentifierValues(npis, next(iter(normalized_eins)))


def _project_token_rows(
    context: _ExtractionContext,
    *,
    normalized_ein: str,
    resource_id: str,
) -> tuple[tuple[TinTaxIdentityToken, bytes], ...]:
    """Project one EIN and record identity under every selected token policy."""

    token_rows: list[tuple[TinTaxIdentityToken, bytes]] = []
    for token_projector in context.token_projectors:
        token = token_projector.tokenize_ein(normalized_ein)
        if (
            type(token) is not TinTaxIdentityToken
            or token.token_policy_id != token_projector.token_policy_id
        ):
            raise TinNpiConnectorError("TIN token projector returned an invalid token")
        source_record_hmac = token_projector.pseudonymize_source_record(
            source_id=context.source_id,
            source_endpoint_id=context.source_endpoint_id,
            source_dataset_id=context.source_dataset_id,
            resource_id=resource_id,
        )
        if type(source_record_hmac) is not bytes or len(source_record_hmac) != 32:
            raise TinNpiConnectorError(
                "TIN token projector returned an invalid source-record identity"
            )
        token_rows.append((token, source_record_hmac))
    return tuple(token_rows)


def _materialize_evidence(
    context: _ExtractionContext,
    *,
    identifier_rule: FhirTinNpiIdentifierRule,
    canonical_as_of: str,
    normalized_values: _NormalizedIdentifierValues,
    token_rows: tuple[tuple[TinTaxIdentityToken, bytes], ...],
) -> tuple[FhirTinNpiEvidence, ...]:
    """Build the exact token-policy by NPI Cartesian evidence product."""

    return tuple(
        FhirTinNpiEvidence(
            token=token,
            npi=npi,
            source_id=context.source_id,
            source_endpoint_id=context.source_endpoint_id,
            source_dataset_id=context.source_dataset_id,
            source_record_hmac_sha256=source_record_hmac,
            source_record_identity_sha256=context.source_record_identity_sha256,
            source_record_payload_hash=context.source_record_payload_hash,
            evidence_as_of=canonical_as_of,
            identifier_policy_id=context.identifier_policy.policy_id,
            identifier_policy_sha256=context.identifier_policy.descriptor_sha256,
            identifier_rule_id=identifier_rule.rule_id,
            identifier_rule_sha256=identifier_rule.descriptor_sha256,
        )
        for token, source_record_hmac in token_rows
        for npi in normalized_values.npis
    )


def _matched_evidence_result(
    resource: Mapping[str, Any],
    context: _ExtractionContext,
    *,
    identifier_rule: FhirTinNpiIdentifierRule,
    canonical_as_of: str,
    normalized_values: _NormalizedIdentifierValues,
    canonical_projectors: tuple[TinTokenProjector, ...],
) -> FhirOrganizationEvidenceResult:
    """Project token rows and return the complete matched evidence product."""

    resource_id = _strict_evidence_id(resource.get("id"), "resource ID", limit=256)
    projection_context = replace(
        context,
        token_projectors=canonical_projectors,
    )
    token_rows = _project_token_rows(
        projection_context,
        normalized_ein=normalized_values.normalized_ein,
        resource_id=resource_id,
    )
    evidence_rows = _materialize_evidence(
        projection_context,
        identifier_rule=identifier_rule,
        canonical_as_of=canonical_as_of,
        normalized_values=normalized_values,
        token_rows=token_rows,
    )
    return FhirOrganizationEvidenceResult(
        state=FhirOrganizationEvidenceState.MATCHED,
        evidence=evidence_rows,
    )


def _extract_verified_organization_evidence(
    resource: Mapping[str, Any],
    context: _ExtractionContext,
) -> FhirOrganizationEvidenceResult:
    """Project identifiers after the source row payload identity is verified."""

    if (
        not isinstance(resource, Mapping)
        or resource.get("resourceType") != "Organization"
    ):
        return _state_result(FhirOrganizationEvidenceState.NOT_ORGANIZATION)
    if (
        type(context.source_record_identity_sha256) is not bytes
        or len(context.source_record_identity_sha256) != 32
    ):
        raise TinNpiConnectorError(
            "FHIR Organization source-record identity is invalid"
        )
    _strict_hash_hex(
        context.source_record_payload_hash,
        "FHIR Organization payload hash",
    )
    if type(context.identifier_policy) is not FhirTinNpiIdentifierPolicy:
        raise TinNpiConnectorError("FHIR identifier policy is invalid")
    identifier_rule = context.identifier_policy.rule_for(
        source_id=context.source_id,
        endpoint_id=context.source_endpoint_id,
    )
    canonical_projectors = _canonical_token_projectors(context.token_projectors)
    if resource.get("active") is False:
        return _state_result(FhirOrganizationEvidenceState.INACTIVE)
    canonical_as_of, evidence_cutoff = _identifier_cutoff(context.evidence_as_of)
    identifiers = resource.get("identifier")
    if not isinstance(identifiers, Sequence) or isinstance(
        identifiers,
        (str, bytes, bytearray),
    ):
        return _state_result(FhirOrganizationEvidenceState.MISSING_IDENTIFIERS)
    selected = _select_effective_identifiers(
        identifiers,
        identifier_rule=identifier_rule,
        evidence_cutoff=evidence_cutoff,
    )
    if isinstance(selected, FhirOrganizationEvidenceResult):
        return selected
    normalized = _normalize_selected_identifiers(*selected)
    if isinstance(normalized, FhirOrganizationEvidenceResult):
        return normalized
    return _matched_evidence_result(
        resource,
        context,
        identifier_rule=identifier_rule,
        canonical_as_of=canonical_as_of,
        normalized_values=normalized,
        canonical_projectors=canonical_projectors,
    )


def extract_organization_evidence_for_policies(
    resource: Mapping[str, Any],
    *,
    source_id: str,
    source_endpoint_id: str,
    source_dataset_id: str,
    resource_payload_hash: str,
    token_projectors: tuple[TinTokenProjector, ...],
    evidence_as_of: dt.datetime | dt.date | str,
    identifier_policy: FhirTinNpiIdentifierPolicy = (
        DEFAULT_FHIR_TIN_NPI_IDENTIFIER_POLICY
    ),
) -> FhirOrganizationEvidenceResult:
    """Verify one exact source payload, then project same-Organization evidence."""

    if (
        not isinstance(resource, Mapping)
        or resource.get("resourceType") != "Organization"
    ):
        return _state_result(FhirOrganizationEvidenceState.NOT_ORGANIZATION)
    canonical_payload_hash = _strict_hash_hex(
        resource_payload_hash,
        "FHIR Organization payload hash",
    )
    record_identity = _verified_record_identity_sha256(
        resource_id=resource.get("id"),
        payload=resource,
        payload_hash=canonical_payload_hash,
    )
    return _extract_verified_organization_evidence(
        resource,
        _ExtractionContext(
            source_id=source_id,
            source_endpoint_id=source_endpoint_id,
            source_dataset_id=source_dataset_id,
            source_record_identity_sha256=record_identity,
            source_record_payload_hash=canonical_payload_hash,
            token_projectors=token_projectors,
            evidence_as_of=evidence_as_of,
            identifier_policy=identifier_policy,
        ),
    )


def extract_fhir_organization_tin_npi_evidence(
    resource: Mapping[str, Any],
    *,
    source_id: str,
    source_endpoint_id: str,
    source_dataset_id: str,
    resource_payload_hash: str,
    token_projector: TinTokenProjector,
    evidence_as_of: dt.datetime | dt.date | str,
    identifier_policy: FhirTinNpiIdentifierPolicy = (
        DEFAULT_FHIR_TIN_NPI_IDENTIFIER_POLICY
    ),
) -> FhirOrganizationEvidenceResult:
    """Compatibility wrapper for a one-policy extraction pass."""

    return extract_organization_evidence_for_policies(
        resource,
        source_id=source_id,
        source_endpoint_id=source_endpoint_id,
        source_dataset_id=source_dataset_id,
        resource_payload_hash=resource_payload_hash,
        token_projectors=(token_projector,),
        evidence_as_of=evidence_as_of,
        identifier_policy=identifier_policy,
    )


extract_fhir_organization_tin_npi_evidence_for_policies = (
    extract_organization_evidence_for_policies
)
