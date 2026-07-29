# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Shared constants, states, and errors for the protected TIN connector."""

from __future__ import annotations

import re
from enum import Enum

from typing import Any, Mapping, Protocol, Sequence


TIN_TOKEN_MESSAGE_DOMAIN = b"healthporta.ptg.tin.v1"
TIN_TOKEN_MESSAGE_FORMAT_ID = "healthporta.ptg.tin-hmac-message.v1"
TIN_TOKEN_POLICY_PREFIX = "ptg-tin-hmac-sha256-v1:"
TIN_TOKEN_POLICY_ID_MAX_BYTES = 55
TIN_TOKEN_POLICY_DESCRIPTOR_DOMAIN = b"PTG2V4TINPOLICY\x01"
TIN_TOKEN_NORMALIZATION_CONTRACT_ID = "ein_ascii_digits_or_2_7_hyphen_v1"
TIN_TOKEN_HMAC_CONTRACT_ID = "hmac_sha256_ptg_tin_v1"
TIN_TOKEN_ID_128_CONTRACT_ID = "tin_id_128=first_16_bytes(tin_hmac_sha256)"
TIN_TOKEN_FULL_HMAC_CONTRACT_ID = "tin_hmac_sha256_full_32_bytes_authoritative"
FHIR_SOURCE_RECORD_HMAC_MESSAGE_FORMAT_ID = (
    "healthporta.tin-npi.fhir-source-record-hmac.v1"
)
FHIR_TIN_NPI_IDENTIFIER_POLICY_ID = (
    "healthporta.provider-directory.tin-npi-identifiers.v1"
)
FHIR_SAME_ORGANIZATION_RELATIONSHIP = "same_organization_identifier"

_TIN_TOKEN_POLICY_PATTERN = re.compile(
    r"^ptg-tin-hmac-sha256-v1:[a-z0-9](?:[a-z0-9._-]{0,31})$"
)
_NORMALIZED_EIN_PATTERN = re.compile(r"^[0-9]{9}$")
_FHIR_EIN_INPUT_PATTERN = re.compile(r"^(?:[0-9]{9}|[0-9]{2}-[0-9]{7})$")
_NORMALIZED_NPI_PATTERN = re.compile(r"^[0-9]{10}$")
_PUBLIC_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]*$")
_FHIR_IDENTIFIER_CODING_SYSTEMS = (
    "http://terminology.hl7.org/CodeSystem/v2-0203",
    "http://hl7.org/fhir/v2/0203",
)
_FHIR_NPI_SYSTEM = "http://hl7.org/fhir/sid/us-npi"
_ALLOWED_NPI_SEPARATORS = frozenset(" -./")
_EVIDENCE_HASH_DOMAIN = b"healthporta.tin-npi.fhir-evidence.v2\0"
_EVIDENCE_SET_HASH_DOMAIN = b"healthporta.tin-npi.fhir-evidence-set.v1\0"
_SOURCE_RECORD_HMAC_DOMAIN = b"healthporta.tin-npi.fhir-source-record.v1"
_POLICY_BINDING_DOMAIN = b"healthporta.tin-npi.policy-binding.v1\0"
_NPI_MIN = 1_000_000_000
_NPI_MAX = 2_999_999_999
_NPI_LUHN_PREFIX_DIGIT_SUM = 24
_HASH_HEX_PATTERN = re.compile(r"^[0-9a-f]{64}$")
_SOURCE_VECTOR_HASH_DOMAIN = b"healthporta.tin-npi.source-vector.v1\0"
_SOURCE_ORDINAL_MAP_HASH_DOMAIN = b"healthporta.tin-npi.source-ordinal-map.v1\0"
_LOOKUP_ROW_HASH_DOMAIN = b"healthporta.tin-npi.lookup-row.v3\0"
_LOOKUP_BUCKET_HASH_DOMAIN = b"healthporta.tin-npi.lookup-bucket.v1\0"
_LOOKUP_SET_HASH_DOMAIN = b"healthporta.tin-npi.lookup-set.v4\0"
_SCAN_PROOF_HASH_DOMAIN = b"healthporta.tin-npi.fhir-organization-scan-proof.v2\0"
_GENERATION_HASH_DOMAIN = b"healthporta.tin-npi.generation.v3\0"
_IDENTIFIER_RULE_HASH_DOMAIN = b"healthporta.tin-npi.fhir-identifier-rule.v1\0"
_IDENTIFIER_POLICY_HASH_DOMAIN = b"healthporta.tin-npi.fhir-identifier-policy.v2\0"
_FHIR_ORGANIZATION_RECORD_BINDING_HASH_DOMAIN = (
    b"healthporta.tin-npi.fhir-organization-record-binding.v1\0"
)
_FHIR_DATE_PATTERN = re.compile(
    r"^(?P<year>[0-9]{4})(?:-(?P<month>[0-9]{2})(?:-(?P<day>[0-9]{2}))?)?$"
)
_FHIR_DATETIME_PATTERN = re.compile(
    r"^(?P<year>[0-9]{4})-"
    r"(?P<month>0[1-9]|1[0-2])-"
    r"(?P<day>0[1-9]|[1-2][0-9]|3[0-1])T"
    r"(?P<hour>[01][0-9]|2[0-3]):"
    r"(?P<minute>[0-5][0-9]):"
    r"(?P<second>[0-5][0-9]|60)"
    r"(?:\.(?P<fraction>[0-9]+))?"
    r"(?P<zone>Z|[+-](?:(?:0[0-9]|1[0-3]):[0-5][0-9]|14:00))$"
)

TIN_NPI_SOURCE_VECTOR_SCHEMA_VERSION = 3
TIN_NPI_LOOKUP_SCHEMA_VERSION = 2
TIN_NPI_LOOKUP_CONTRACT_ID = "healthporta.tin-npi.compact-lookup.v2"
TIN_NPI_PROJECTION_POLICY_ID = "healthporta.tin-npi.compact-same-organization-lookup.v3"
TIN_NPI_SOURCE_ORDINAL_CONTRACT_ID = "source_id_sorted_utf8_lsb0_bitmap_v1"
TIN_NPI_SOURCE_SCOPE_CONTRACT_ID = (
    "healthporta.tin-npi.all-current-published-organization-sources.v1"
)
TIN_NPI_TOKEN_POLICY_SCOPE_CONTRACT_ID = (
    "healthporta.tin-npi.all-retained-ptg-tax-policy-descriptors.v1"
)
TIN_NPI_SITE_RESOLUTION_CONTRACT_ID = (
    "healthporta.tin-npi.site-by-current-entity-address-unified.v1"
)
TIN_NPI_FHIR_ORGANIZATION_SCAN_CONTRACT_ID = (
    "healthporta.tin-npi.fhir-organization-scan.v2"
)
TIN_NPI_FHIR_ORGANIZATION_IDENTITY_CONTRACT_ID = (
    "provider_directory_dataset_resource_type_id_payload_hash_newline_v1"
)
TIN_NPI_FHIR_INPUT_RELATION = "provider_directory_dataset_resource"


class TinNpiConnectorError(ValueError):
    """Report a fail-closed connector identity or evidence error."""


def strict_evidence_text(candidate: object, field_name: str, *, limit: int) -> str:
    """Return one bounded printable identifier or fail without echoing it."""

    if (
        type(candidate) is not str
        or candidate != candidate.strip()
        or not 1 <= len(candidate) <= limit
        or not candidate.isprintable()
    ):
        raise TinNpiConnectorError(f"FHIR evidence {field_name} is invalid")
    return candidate


class _MalformedFhirIdentifierPeriod(TinNpiConnectorError):
    pass


class _UnresolvedFhirIdentifierPeriod(TinNpiConnectorError):
    pass


class FhirOrganizationEvidenceState(str, Enum):
    """Non-sensitive outcome of inspecting one FHIR Organization."""

    MATCHED = "matched"
    NOT_ORGANIZATION = "not_organization"
    INACTIVE = "inactive"
    MISSING_IDENTIFIERS = "missing_identifiers"
    MISSING_NPI = "missing_npi"
    MISSING_EIN = "missing_ein"
    MALFORMED_NPI = "malformed_npi"
    MALFORMED_EIN = "malformed_ein"
    AMBIGUOUS_EIN = "ambiguous_ein"
    CONFLICTING_IDENTIFIER_CLASS = "conflicting_identifier_class"
    MALFORMED_IDENTIFIER_PERIOD = "malformed_identifier_period"
    UNRESOLVED_IDENTIFIER_PERIOD = "unresolved_identifier_period"


FHIR_ORGANIZATION_SCAN_TERMINAL_STATES = tuple(
    state
    for state in FhirOrganizationEvidenceState
    if state is not FhirOrganizationEvidenceState.NOT_ORGANIZATION
)
