# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Adapters from retained normalized Organization rows to strict extraction."""

from __future__ import annotations

import datetime as dt
from collections.abc import Mapping
from typing import Any

from process.tin_npi_connector_evidence import (
    FhirOrganizationEvidenceResult,
    _strict_evidence_id,
    _verified_record_identity_sha256,
)
from process.tin_npi_connector_extract import (
    _ExtractionContext,
    _extract_verified_organization_evidence,
)
from process.tin_npi_connector_policy import (
    DEFAULT_FHIR_TIN_NPI_IDENTIFIER_POLICY,
    FhirTinNpiIdentifierPolicy,
)
from process.tin_npi_connector_security import TinTokenProjector
from process.tin_npi_connector_source import _strict_hash_hex
from process.tin_npi_connector_support import (
    FhirOrganizationEvidenceState,
    TinNpiConnectorError,
)


def extract_normalized_organization_evidence_for_policies(
    organization_row: Mapping[str, Any],
    *,
    source_id: str,
    source_endpoint_id: str,
    source_dataset_id: str,
    token_projectors: tuple[TinTokenProjector, ...],
    evidence_as_of: dt.datetime | dt.date | str,
    identifier_policy: FhirTinNpiIdentifierPolicy = (
        DEFAULT_FHIR_TIN_NPI_IDENTIFIER_POLICY
    ),
) -> FhirOrganizationEvidenceResult:
    """Verify and adapt one immutable normalized Organization source row."""

    if not isinstance(organization_row, Mapping):
        return FhirOrganizationEvidenceResult(
            FhirOrganizationEvidenceState.NOT_ORGANIZATION
        )
    if organization_row.get("resource_type") != "Organization":
        return FhirOrganizationEvidenceResult(
            FhirOrganizationEvidenceState.NOT_ORGANIZATION
        )
    organization_payload = organization_row.get("payload_json")
    if not isinstance(organization_payload, Mapping):
        raise TinNpiConnectorError("FHIR Organization payload is invalid")
    resource_id = _strict_evidence_id(
        organization_row.get("resource_id"),
        "FHIR Organization resource ID",
        limit=256,
    )
    if organization_payload.get("resource_id") != resource_id:
        raise TinNpiConnectorError("FHIR Organization resource identity mismatch")
    canonical_payload_hash = _strict_hash_hex(
        organization_row.get("payload_hash"),
        "FHIR Organization payload hash",
    )
    record_identity = _verified_record_identity_sha256(
        resource_id=resource_id,
        payload=organization_payload,
        payload_hash=canonical_payload_hash,
    )
    return _extract_verified_organization_evidence(
        {
            "resourceType": "Organization",
            "id": resource_id,
            "active": organization_payload.get("active"),
            "identifier": organization_payload.get("identifiers"),
        },
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


def extract_normalized_organization_evidence(
    organization_row: Mapping[str, Any],
    *,
    source_id: str,
    source_endpoint_id: str,
    source_dataset_id: str,
    token_projector: TinTokenProjector,
    evidence_as_of: dt.datetime | dt.date | str,
    identifier_policy: FhirTinNpiIdentifierPolicy = (
        DEFAULT_FHIR_TIN_NPI_IDENTIFIER_POLICY
    ),
) -> FhirOrganizationEvidenceResult:
    """Compatibility wrapper for one normalized-row token policy."""

    return extract_normalized_organization_evidence_for_policies(
        organization_row,
        source_id=source_id,
        source_endpoint_id=source_endpoint_id,
        source_dataset_id=source_dataset_id,
        token_projectors=(token_projector,),
        evidence_as_of=evidence_as_of,
        identifier_policy=identifier_policy,
    )


extract_normalized_fhir_organization_tin_npi_evidence_for_policies = (
    extract_normalized_organization_evidence_for_policies
)
extract_normalized_fhir_organization_tin_npi_evidence = (
    extract_normalized_organization_evidence
)
