# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Authenticated FHIR source fences and deterministic source vectors."""

from __future__ import annotations

import hashlib
import json
import struct
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

from process.tin_npi_connector_policy import FhirTinNpiIdentifierPolicy
from process.tin_npi_connector_security import TinTokenPolicyDescriptor
from process.tin_npi_connector_support import (
    _HASH_HEX_PATTERN,
    _SOURCE_ORDINAL_MAP_HASH_DOMAIN,
    _SOURCE_VECTOR_HASH_DOMAIN,
    FHIR_SOURCE_RECORD_HMAC_MESSAGE_FORMAT_ID,
    TIN_NPI_FHIR_INPUT_RELATION,
    TIN_NPI_LOOKUP_CONTRACT_ID,
    TIN_NPI_LOOKUP_SCHEMA_VERSION,
    TIN_NPI_PROJECTION_POLICY_ID,
    TIN_NPI_SITE_RESOLUTION_CONTRACT_ID,
    TIN_NPI_SOURCE_ORDINAL_CONTRACT_ID,
    TIN_NPI_SOURCE_SCOPE_CONTRACT_ID,
    TIN_NPI_SOURCE_VECTOR_SCHEMA_VERSION,
    TIN_NPI_TOKEN_POLICY_SCOPE_CONTRACT_ID,
    TinNpiConnectorError,
    strict_evidence_text,
)
from process.tin_npi_connector_source_validation import (
    validate_connector_source_vector,
    validate_fhir_dataset_fence,
)
from process.tin_npi_connector_temporal import canonical_evidence_as_of

_strict_evidence_id = strict_evidence_text


def _strict_hash_hex(candidate: object, field_name: str) -> str:
    if type(candidate) is not str or _HASH_HEX_PATTERN.fullmatch(candidate) is None:
        raise TinNpiConnectorError(f"{field_name} is invalid")
    return candidate


def _strict_optional_text(
    candidate: object,
    field_name: str,
    *,
    limit: int,
) -> str | None:
    if candidate is None:
        return None
    return _strict_evidence_id(candidate, field_name, limit=limit)


def _strict_string_tuple(
    candidate: object,
    field_name: str,
    *,
    limit: int,
) -> tuple[str, ...]:
    if type(candidate) is not tuple:
        raise TinNpiConnectorError(f"{field_name} is invalid")
    values = tuple(
        _strict_evidence_id(value, field_name, limit=limit) for value in candidate
    )
    if values != tuple(sorted(set(values))):
        raise TinNpiConnectorError(f"{field_name} is invalid")
    return values


def _canonical_source_ids(source_ids: Iterable[str]) -> tuple[str, ...]:
    if isinstance(source_ids, (str, bytes, bytearray)):
        raise TinNpiConnectorError("connector source ordinal map is invalid")
    try:
        values = tuple(
            _strict_evidence_id(source_id, "source ID", limit=64)
            for source_id in source_ids
        )
    except TypeError:
        raise TinNpiConnectorError("connector source ordinal map is invalid") from None
    if not values:
        raise TinNpiConnectorError("connector source ordinal map is invalid")
    return tuple(sorted(set(values), key=lambda value: value.encode("utf-8")))


def canonical_source_ordinal_map_json(source_ids: Iterable[str]) -> str:
    """Encode source ID ordinals as compact canonical UTF-8 JSON."""

    canonical_source_ids = _canonical_source_ids(source_ids)
    return json.dumps(
        [
            {"ordinal": ordinal, "source_id": source_id}
            for ordinal, source_id in enumerate(canonical_source_ids)
        ],
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def canonical_source_ordinal_map_digest(
    source_ids: Iterable[str],
) -> bytes:
    """Seal the canonical source ordinal map for independent DB verification."""

    return hashlib.sha256(
        _SOURCE_ORDINAL_MAP_HASH_DOMAIN
        + canonical_source_ordinal_map_json(source_ids).encode("utf-8")
    ).digest()


def _source_bitmap(
    source_ids: tuple[str, ...],
    *,
    source_ordinal_map: tuple[str, ...],
) -> bytes:
    ordinal_by_source_id = {
        source_id: ordinal for ordinal, source_id in enumerate(source_ordinal_map)
    }
    bitmap = bytearray((len(source_ordinal_map) + 7) // 8)
    try:
        for source_id in source_ids:
            ordinal = ordinal_by_source_id[source_id]
            bitmap[ordinal // 8] |= 1 << (ordinal % 8)
    except KeyError:
        raise TinNpiConnectorError(
            "forward lookup source IDs are outside the source ordinal map"
        ) from None
    return bytes(bitmap)


@dataclass(frozen=True)
class FhirDatasetFenceIdentity:
    """Immutable Provider Directory dataset selection used by one rebuild."""

    source_id: str
    endpoint_id: str
    dataset_id: str
    evidence_run_id: str
    selected_resources: tuple[str, ...]
    expected_resources: tuple[str, ...]
    status: str
    is_current: bool
    promote_on_cutover: bool
    dataset_hash: str
    resource_count: int
    organization_resource_count: int
    organization_resource_sha256: str
    source_summary_sha256: str
    identifier_rule_id: str
    identifier_rule_sha256: str
    recorded_expected_resources: tuple[str, ...] | None = None
    previous_dataset_id: str | None = None
    expected_incumbent_dataset_id: str | None = None
    validated_at: str | None = None

    def __post_init__(self) -> None:
        """Validate the complete immutable dataset and completeness fence."""

        validate_fhir_dataset_fence(self)

    def public_payload(self) -> dict[str, Any]:
        """Return every non-secret field bound into the source vector."""

        return {
            "dataset_hash": self.dataset_hash,
            "dataset_id": self.dataset_id,
            "endpoint_id": self.endpoint_id,
            "evidence_run_id": self.evidence_run_id,
            "expected_incumbent_dataset_id": self.expected_incumbent_dataset_id,
            "expected_resources": list(self.expected_resources),
            "identifier_rule_id": self.identifier_rule_id,
            "identifier_rule_sha256": self.identifier_rule_sha256,
            "is_current": self.is_current,
            "organization_resource_count": self.organization_resource_count,
            "organization_resource_sha256": self.organization_resource_sha256,
            "previous_dataset_id": self.previous_dataset_id,
            "promote_on_cutover": self.promote_on_cutover,
            "recorded_expected_resources": (
                list(self.recorded_expected_resources)
                if self.recorded_expected_resources is not None
                else None
            ),
            "resource_count": self.resource_count,
            "selected_resources": list(self.selected_resources),
            "source_id": self.source_id,
            "source_summary_sha256": self.source_summary_sha256,
            "status": self.status,
            "validated_at": self.validated_at,
        }


@dataclass(frozen=True)
class ConnectorRelationIdentity:
    """Physical relation fence used to reject an input swap during a build."""

    schema: str
    relation: str
    relation_oid: int
    relkind: str = "r"
    relpersistence: str = "p"

    def __post_init__(self) -> None:
        _strict_evidence_id(self.schema, "relation schema", limit=63)
        _strict_evidence_id(self.relation, "relation name", limit=63)
        if type(self.relation_oid) is not int or self.relation_oid <= 0:
            raise TinNpiConnectorError("relation OID is invalid")
        if self.relkind not in {"r", "p"}:
            raise TinNpiConnectorError("relation kind is invalid")
        if self.relpersistence != "p":
            raise TinNpiConnectorError("relation persistence is invalid")

    def public_payload(self) -> dict[str, Any]:
        """Return the physical relation identity used by publication CAS."""

        return {
            "relation": self.relation,
            "relation_oid": self.relation_oid,
            "relkind": self.relkind,
            "relpersistence": self.relpersistence,
            "schema": self.schema,
        }


@dataclass(frozen=True)
class TinNpiConnectorSourceVector:
    """Complete immutable input identity for one swappable same-entity build."""

    fhir_datasets: tuple[FhirDatasetFenceIdentity, ...]
    input_relations: tuple[ConnectorRelationIdentity, ...]
    token_policies: tuple[TinTokenPolicyDescriptor, ...]
    evidence_as_of: str
    identifier_policy: FhirTinNpiIdentifierPolicy
    lookup_contract_id: str = TIN_NPI_LOOKUP_CONTRACT_ID
    lookup_schema_version: int = TIN_NPI_LOOKUP_SCHEMA_VERSION
    projection_policy_id: str = TIN_NPI_PROJECTION_POLICY_ID
    schema_version: int = TIN_NPI_SOURCE_VECTOR_SCHEMA_VERSION

    def __post_init__(self) -> None:
        """Validate exact datasets, relations, policies, and cutoff identity."""

        validate_connector_source_vector(self)

    def public_payload(self) -> dict[str, Any]:
        """Return the canonical, secret-free generation input descriptor."""

        return {
            "fhir_datasets": sorted(
                (dataset.public_payload() for dataset in self.fhir_datasets),
                key=lambda payload: json.dumps(
                    payload,
                    sort_keys=True,
                    separators=(",", ":"),
                ),
            ),
            "evidence_as_of": self.evidence_as_of,
            "identifier_policy_id": self.identifier_policy.policy_id,
            "identifier_policy_sha256": (self.identifier_policy.descriptor_sha256),
            "input_relations": sorted(
                (relation.public_payload() for relation in self.input_relations),
                key=lambda payload: (
                    payload["schema"],
                    payload["relation"],
                    payload["relation_oid"],
                ),
            ),
            "lookup_contract_id": self.lookup_contract_id,
            "lookup_schema_version": self.lookup_schema_version,
            "projection_policy_id": self.projection_policy_id,
            "schema_version": self.schema_version,
            "site_resolution_contract_id": TIN_NPI_SITE_RESOLUTION_CONTRACT_ID,
            "source_scope_contract_id": TIN_NPI_SOURCE_SCOPE_CONTRACT_ID,
            "source_record_identity_contract_id": (
                FHIR_SOURCE_RECORD_HMAC_MESSAGE_FORMAT_ID
            ),
            "token_policies": sorted(
                (policy.public_payload() for policy in self.token_policies),
                key=lambda payload: payload["token_policy_id"],
            ),
            "token_policy_scope_contract_id": (TIN_NPI_TOKEN_POLICY_SCOPE_CONTRACT_ID),
            "token_policy_ids": sorted(self.token_policy_ids),
        }

    @property
    def token_policy_ids(self) -> tuple[str, ...]:
        """Return token-policy IDs in the manifest-declared order."""

        return tuple(policy.token_policy_id for policy in self.token_policies)

    @property
    def canonical_json(self) -> str:
        """Return stable JSON used to derive the source-vector identity."""

        return json.dumps(
            self.public_payload(),
            sort_keys=True,
            separators=(",", ":"),
        )

    @property
    def source_vector_id(self) -> str:
        """Return the domain-separated identity of all connector inputs."""

        return hashlib.sha256(
            _SOURCE_VECTOR_HASH_DOMAIN + self.canonical_json.encode("utf-8")
        ).hexdigest()
