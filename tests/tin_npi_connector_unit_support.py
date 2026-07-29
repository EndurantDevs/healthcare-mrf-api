# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Shared deterministic fixtures for TIN-to-NPI connector unit tests."""

from __future__ import annotations

import datetime as dt
from dataclasses import replace

from process.tin_npi_connector import (
    ConnectorRelationIdentity,
    FhirDatasetFenceIdentity,
    FhirOrganizationEvidenceState,
    FhirOrganizationScanRecord,
    FhirTinNpiIdentifierPolicy,
    FhirTinNpiIdentifierRule,
    TIN_TOKEN_POLICY_PREFIX,
    TinNpiConnectorSourceVector,
    TinTokenPolicyDescriptor,
    canonical_evidence_as_of,
    canonical_fhir_organization_identity_sha256,
    canonical_provider_directory_payload_hash,
    extract_fhir_organization_tin_npi_evidence,
    load_tin_token_policy,
)


TOKEN_POLICY_ID = f"{TIN_TOKEN_POLICY_PREFIX}2026-07-a"
RELEASE_1_TOKEN_POLICY_ID = f"{TIN_TOKEN_POLICY_PREFIX}release-1"
RELEASE_1_POLICY_DESCRIPTOR_SHA256 = (
    "a0c06f5494f80663686be6861038a8804d9509d0fdc2d2c8cc56c259e53d761c"
)
NPI_SYSTEM = "http://hl7.org/fhir/sid/us-npi"
TYPE_SYSTEM = "http://terminology.hl7.org/CodeSystem/v2-0203"
TEST_SECRET = bytes(range(32))
TEST_EIN = "01-2345678"
TEST_EIN_NORMALIZED = "012345678"
TEST_HMAC_HEX = "305973e3ec2e1fd407f17583d368b7bcb29df8f8869b63574797c836ed8b8a5a"
OBSERVED_AT = dt.datetime(2026, 7, 27, tzinfo=dt.timezone.utc)
EVIDENCE_AS_OF = canonical_evidence_as_of(OBSERVED_AT)
DEFAULT_ORGANIZATION_PAYLOAD_HASH = "c" * 64
REVIEWED_TAX_AS_EIN_RULE = FhirTinNpiIdentifierRule(
    rule_id="healthporta.test.fhir-tax-as-ein.source-a.v1",
    source_id="source-a",
    endpoint_id="endpoint-a",
    npi_systems=(NPI_SYSTEM,),
    npi_type_codings=((TYPE_SYSTEM, "NPI"),),
    ein_systems=(),
    ein_type_codings=((TYPE_SYSTEM, "TAX"),),
)
REVIEWED_TAX_AS_EIN_POLICY = FhirTinNpiIdentifierPolicy(
    policy_id="healthporta.test.fhir-tax-as-ein.v1",
    rules=(REVIEWED_TAX_AS_EIN_RULE,),
)


def identifier_rule(
    *,
    source_id: str = "source-a",
    endpoint_id: str = "endpoint-a",
) -> FhirTinNpiIdentifierRule:
    return replace(
        REVIEWED_TAX_AS_EIN_RULE,
        rule_id=f"healthporta.test.fhir-tax-as-ein.{source_id}.v1",
        source_id=source_id,
        endpoint_id=endpoint_id,
    )


def identifier_policy(
    fhir_datasets: tuple[FhirDatasetFenceIdentity, ...],
) -> FhirTinNpiIdentifierPolicy:
    identifier_rules = tuple(
        sorted(
            (
                identifier_rule(
                    source_id=fhir_dataset.source_id,
                    endpoint_id=fhir_dataset.endpoint_id,
                )
                for fhir_dataset in fhir_datasets
            ),
            key=lambda candidate_rule: (
                candidate_rule.source_id.encode(),
                candidate_rule.endpoint_id.encode(),
                candidate_rule.rule_id.encode(),
            ),
        )
    )
    return FhirTinNpiIdentifierPolicy(
        policy_id=REVIEWED_TAX_AS_EIN_POLICY.policy_id,
        rules=identifier_rules,
    )


def token_policy(
    temporary_path,
    secret: bytes = TEST_SECRET,
    policy_id: str = TOKEN_POLICY_ID,
):
    secret_path = temporary_path / "tin-token.key"
    if secret_path.exists():
        secret_path.chmod(0o600)
    secret_path.write_bytes(secret)
    secret_path.chmod(0o400)
    return load_tin_token_policy(
        token_policy_id=policy_id,
        secret_file=secret_path,
    )


class RecordingProjector:
    """Record every normalized EIN and source identity offered to a projector."""

    def __init__(
        self,
        delegate,
        *,
        declared_policy_id=None,
        returned_token=None,
        tokenize_error=None,
    ):
        self.delegate = delegate
        self.declared_policy_id = declared_policy_id or delegate.token_policy_id
        self.returned_token = returned_token
        self.tokenize_error = tokenize_error
        self.normalized_eins = []
        self.source_record_calls = []

    @property
    def token_policy_id(self):
        return self.declared_policy_id

    def tokenize_ein(self, candidate_ein):
        self.normalized_eins.append(candidate_ein)
        if self.tokenize_error is not None:
            raise self.tokenize_error
        if self.returned_token is not None:
            return self.returned_token
        return self.delegate.tokenize_ein(candidate_ein)

    def pseudonymize_source_record(self, **record_identity):
        self.source_record_calls.append(record_identity)
        return self.delegate.pseudonymize_source_record(**record_identity)


def npi_identifier(identifier_value, *, system=NPI_SYSTEM):
    return {"system": system, "value": identifier_value}


def typed_identifier(identifier_code, identifier_value, *, system=TYPE_SYSTEM):
    return {
        "type": {"coding": [{"system": system, "code": identifier_code}]},
        "value": identifier_value,
    }


def organization(*identifiers, active=True, resource_id="organization-1"):
    return {
        "resourceType": "Organization",
        "id": resource_id,
        "active": active,
        "identifier": list(identifiers),
    }


def extract_evidence(
    resource,
    temporary_path,
    *,
    payload_hash=None,
    identifier_policy_override=REVIEWED_TAX_AS_EIN_POLICY,
):
    canonical_payload_hash = canonical_provider_directory_payload_hash(resource)
    if payload_hash is not None:
        assert payload_hash == canonical_payload_hash
    return extract_fhir_organization_tin_npi_evidence(
        resource,
        source_id="source-a",
        source_endpoint_id="endpoint-a",
        source_dataset_id="dataset-a",
        resource_payload_hash=canonical_payload_hash,
        token_projector=token_policy(temporary_path),
        evidence_as_of=EVIDENCE_AS_OF,
        identifier_policy=identifier_policy_override,
    )


def fhir_dataset(
    *,
    source_id="source-a",
    endpoint_id="endpoint-a",
    dataset_id="dataset-a",
    dataset_hash="a" * 64,
    organization_identities=(("organization-1", DEFAULT_ORGANIZATION_PAYLOAD_HASH),),
    source_summary_sha256="d" * 64,
):
    reviewed_rule = identifier_rule(
        source_id=source_id,
        endpoint_id=endpoint_id,
    )
    return FhirDatasetFenceIdentity(
        source_id=source_id,
        endpoint_id=endpoint_id,
        dataset_id=dataset_id,
        evidence_run_id=f"run-{source_id}",
        selected_resources=("Organization",),
        expected_resources=("Location", "Organization"),
        recorded_expected_resources=("Location", "Organization"),
        status="published",
        is_current=True,
        promote_on_cutover=False,
        dataset_hash=dataset_hash,
        resource_count=10,
        organization_resource_count=len(organization_identities),
        organization_resource_sha256=(
            canonical_fhir_organization_identity_sha256(organization_identities)
        ),
        source_summary_sha256=source_summary_sha256,
        identifier_rule_id=reviewed_rule.rule_id,
        identifier_rule_sha256=reviewed_rule.descriptor_sha256,
        validated_at="2026-07-27 00:00:00",
    )


def connector_relation(
    *,
    relation="provider_directory_dataset_resource",
    oid=1001,
):
    return ConnectorRelationIdentity(
        schema="mrf",
        relation=relation,
        relation_oid=oid,
    )


def source_vector(
    *,
    fhir_datasets=None,
    input_relations=None,
    policy_ids=(TOKEN_POLICY_ID,),
    identifier_policy_override=None,
):
    selected_datasets = tuple(fhir_datasets or (fhir_dataset(),))
    return TinNpiConnectorSourceVector(
        fhir_datasets=selected_datasets,
        input_relations=tuple(input_relations or (connector_relation(),)),
        token_policies=tuple(
            TinTokenPolicyDescriptor.release_1(policy_id) for policy_id in policy_ids
        ),
        evidence_as_of=EVIDENCE_AS_OF,
        identifier_policy=(
            identifier_policy_override or identifier_policy(selected_datasets)
        ),
    )


def matched_scan(
    *extraction_results,
    source_id="source-a",
    endpoint_id="endpoint-a",
    dataset_id="dataset-a",
    resource_id="organization-1",
    payload_hash=None,
):
    assert extraction_results
    evidence_states = {extraction.state for extraction in extraction_results}
    assert len(evidence_states) == 1
    evidence_rows = tuple(
        sorted(
            (
                evidence_row
                for extraction in extraction_results
                for evidence_row in extraction.evidence
            ),
            key=lambda evidence_row: (
                evidence_row.token.token_policy_id,
                evidence_row.npi,
                evidence_row.evidence_id,
            ),
        )
    )
    selected_payload_hash = payload_hash or _single_payload_hash(evidence_rows)
    return FhirOrganizationScanRecord(
        source_id=source_id,
        source_endpoint_id=endpoint_id,
        source_dataset_id=dataset_id,
        resource_id=resource_id,
        payload_hash=selected_payload_hash,
        state=next(iter(evidence_states)),
        evidence=evidence_rows,
    )


def _single_payload_hash(evidence_rows):
    payload_hashes = {
        evidence_row.source_record_payload_hash for evidence_row in evidence_rows
    }
    if not payload_hashes:
        return DEFAULT_ORGANIZATION_PAYLOAD_HASH
    assert len(payload_hashes) == 1
    return next(iter(payload_hashes))


def unmatched_scan(
    *,
    source_id="source-a",
    endpoint_id="endpoint-a",
    dataset_id="dataset-a",
    resource_id="organization-1",
    payload_hash=DEFAULT_ORGANIZATION_PAYLOAD_HASH,
    state=FhirOrganizationEvidenceState.MISSING_IDENTIFIERS,
):
    return FhirOrganizationScanRecord(
        source_id=source_id,
        source_endpoint_id=endpoint_id,
        source_dataset_id=dataset_id,
        resource_id=resource_id,
        payload_hash=payload_hash,
        state=state,
    )
