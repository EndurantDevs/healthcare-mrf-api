# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Immutable same-Organization TIN-to-NPI evidence contracts."""

from __future__ import annotations

import datetime as dt
import hashlib
import hmac
import json
import struct
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from process.tin_npi_connector_security import TinTaxIdentityToken
from process.tin_npi_connector_source import _strict_hash_hex
from process.tin_npi_connector_support import (
    _EVIDENCE_HASH_DOMAIN,
    _FHIR_ORGANIZATION_RECORD_BINDING_HASH_DOMAIN,
    _NPI_MAX,
    _NPI_MIN,
    FHIR_SAME_ORGANIZATION_RELATIONSHIP,
    FhirOrganizationEvidenceState,
    TinNpiConnectorError,
    strict_evidence_text,
)
from process.tin_npi_connector_temporal import (
    _normalize_npi,
    canonical_evidence_as_of,
)

_strict_evidence_id = strict_evidence_text


@dataclass(frozen=True, repr=False)
class FhirTinNpiEvidence:
    """One same-Organization TIN-token to NPI assertion."""

    token: TinTaxIdentityToken
    npi: int
    source_id: str
    source_endpoint_id: str
    source_dataset_id: str
    source_record_hmac_sha256: bytes
    source_record_identity_sha256: bytes
    source_record_payload_hash: str
    evidence_as_of: str
    identifier_policy_id: str
    identifier_policy_sha256: str
    identifier_rule_id: str
    identifier_rule_sha256: str
    relationship_class: str = FHIR_SAME_ORGANIZATION_RELATIONSHIP

    def __post_init__(self) -> None:
        if type(self.token) is not TinTaxIdentityToken:
            raise TinNpiConnectorError("FHIR evidence TIN token is invalid")
        if (
            type(self.npi) is not int
            or not _NPI_MIN <= self.npi <= _NPI_MAX
            or _normalize_npi(str(self.npi)) != self.npi
        ):
            raise TinNpiConnectorError("FHIR evidence NPI is invalid")
        _strict_evidence_id(self.source_id, "source ID", limit=64)
        _strict_evidence_id(self.source_endpoint_id, "endpoint ID", limit=64)
        _strict_evidence_id(self.source_dataset_id, "dataset ID", limit=128)
        _strict_hash_hex(
            self.source_record_payload_hash,
            "FHIR Organization payload hash",
        )
        if (
            type(self.source_record_hmac_sha256) is not bytes
            or len(self.source_record_hmac_sha256) != 32
            or type(self.source_record_identity_sha256) is not bytes
            or len(self.source_record_identity_sha256) != 32
        ):
            raise TinNpiConnectorError(
                "FHIR evidence source-record identity is invalid"
            )
        canonical_evidence_as_of(self.evidence_as_of)
        _strict_evidence_id(
            self.identifier_policy_id,
            "identifier policy ID",
            limit=128,
        )
        _strict_hash_hex(
            self.identifier_policy_sha256,
            "FHIR identifier policy hash",
        )
        _strict_evidence_id(
            self.identifier_rule_id,
            "identifier rule ID",
            limit=128,
        )
        _strict_hash_hex(
            self.identifier_rule_sha256,
            "FHIR identifier rule hash",
        )
        if self.relationship_class != FHIR_SAME_ORGANIZATION_RELATIONSHIP:
            raise TinNpiConnectorError("FHIR evidence relationship is invalid")

    @property
    def evidence_id(self) -> bytes:
        """Return the immutable binary identity of this exact evidence row."""

        policy_id = self.token.token_policy_id.encode("ascii")
        relationship = self.relationship_class.encode("ascii")
        if len(policy_id) > 0xFFFF or len(relationship) > 0xFFFF:
            raise TinNpiConnectorError("FHIR evidence identity is invalid")
        return hashlib.sha256(
            _EVIDENCE_HASH_DOMAIN
            + struct.pack(">H", len(policy_id))
            + policy_id
            + self.token.tin_hmac_sha256
            + struct.pack(">q", self.npi)
            + struct.pack(">H", len(relationship))
            + relationship
            + self.source_record_hmac_sha256
            + self.source_record_identity_sha256
            + bytes.fromhex(self.source_record_payload_hash)
            + bytes.fromhex(self.identifier_policy_sha256)
            + bytes.fromhex(self.identifier_rule_sha256)
        ).digest()

    def __repr__(self) -> str:
        return (
            "<fhir-tin-npi-evidence "
            f"source_id={self.source_id!r} "
            f"source_endpoint_id={self.source_endpoint_id!r} "
            f"npi={self.npi!r} token=<redacted>>"
        )


@dataclass(frozen=True)
class FhirOrganizationEvidenceResult:
    """One non-sensitive extraction result and zero or more NPI assertions."""

    state: FhirOrganizationEvidenceState
    evidence: tuple[FhirTinNpiEvidence, ...] = ()

    def __post_init__(self) -> None:
        if type(self.state) is not FhirOrganizationEvidenceState:
            raise TinNpiConnectorError("FHIR evidence state is invalid")
        if type(self.evidence) is not tuple or any(
            type(item) is not FhirTinNpiEvidence for item in self.evidence
        ):
            raise TinNpiConnectorError("FHIR evidence result is invalid")
        if (self.state is FhirOrganizationEvidenceState.MATCHED) != bool(self.evidence):
            raise TinNpiConnectorError("FHIR evidence result is inconsistent")


def _fhir_organization_identity_bytes(
    resource_id: object,
    payload_hash: object,
) -> bytes:
    canonical_resource_id = _strict_evidence_id(
        resource_id,
        "FHIR Organization resource ID",
        limit=256,
    )
    canonical_payload_hash = _strict_hash_hex(
        payload_hash,
        "FHIR Organization payload hash",
    )
    return json.dumps(
        ["Organization", canonical_resource_id, canonical_payload_hash],
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _provider_directory_json_default(value: object) -> object:
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    return str(value)


def canonical_provider_directory_payload_hash(
    payload: Mapping[str, Any],
) -> str:
    """Recompute the dataset-resource hash under the importer contract."""

    if not isinstance(payload, Mapping):
        raise TinNpiConnectorError("FHIR Organization payload is invalid")
    try:
        encoded = json.dumps(
            dict(payload),
            sort_keys=True,
            default=_provider_directory_json_default,
        ).encode("utf-8")
    except (TypeError, ValueError):
        raise TinNpiConnectorError("FHIR Organization payload is invalid") from None
    return hashlib.sha256(encoded).hexdigest()


def _fhir_organization_record_identity_sha256(
    resource_id: object,
    payload_hash: object,
) -> bytes:
    return hashlib.sha256(
        _FHIR_ORGANIZATION_RECORD_BINDING_HASH_DOMAIN
        + _fhir_organization_identity_bytes(resource_id, payload_hash)
    ).digest()


def _verified_record_identity_sha256(
    *,
    resource_id: object,
    payload: Mapping[str, Any],
    payload_hash: object,
) -> bytes:
    """Verify the retained payload hash and bind it to the Organization ID."""

    canonical_payload_hash = _strict_hash_hex(
        payload_hash,
        "FHIR Organization payload hash",
    )
    if not hmac.compare_digest(
        canonical_payload_hash,
        canonical_provider_directory_payload_hash(payload),
    ):
        raise TinNpiConnectorError("FHIR Organization payload hash mismatch")
    return _fhir_organization_record_identity_sha256(
        resource_id,
        canonical_payload_hash,
    )


_verified_fhir_organization_record_identity_sha256 = _verified_record_identity_sha256


def canonical_fhir_organization_identity_sha256(
    identities: Iterable[tuple[str, str]],
) -> str:
    """Hash exact ordered Organization identities using the dataset contract."""

    if isinstance(identities, (str, bytes, bytearray)):
        raise TinNpiConnectorError("FHIR Organization identities are invalid")
    digest = hashlib.sha256()
    previous_resource_id: str | None = None
    count = 0
    try:
        for resource_id, payload_hash in identities:
            canonical_resource_id = _strict_evidence_id(
                resource_id,
                "FHIR Organization resource ID",
                limit=256,
            )
            if (
                previous_resource_id is not None
                and canonical_resource_id <= previous_resource_id
            ):
                raise TinNpiConnectorError(
                    "FHIR Organization identities are not strictly ordered"
                )
            if count:
                digest.update(b"\n")
            digest.update(
                _fhir_organization_identity_bytes(
                    canonical_resource_id,
                    payload_hash,
                )
            )
            previous_resource_id = canonical_resource_id
            count += 1
    except (TypeError, ValueError):
        raise TinNpiConnectorError("FHIR Organization identities are invalid") from None
    return digest.hexdigest()
