# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import copy
import datetime as dt
import json
import os
import signal
from dataclasses import replace

import pytest

import process.tin_npi_connector as connector
from process.tin_npi_connector import (
    CompactTinNpiGeneration,
    DEFAULT_FHIR_TIN_NPI_IDENTIFIER_POLICY,
    ConnectorRelationIdentity,
    FHIR_ORGANIZATION_SCAN_TERMINAL_STATES,
    FHIR_SAME_ORGANIZATION_RELATIONSHIP,
    FhirDatasetFenceIdentity,
    FhirOrganizationEvidenceState,
    FhirOrganizationScanProof,
    FhirOrganizationScanRecord,
    FhirTinNpiEvidence,
    FhirTinNpiIdentifierPolicy,
    FhirTinNpiIdentifierRule,
    NpiTinLookupReference,
    NpiTinLookupRow,
    TIN_TOKEN_MESSAGE_FORMAT_ID,
    TIN_TOKEN_POLICY_PREFIX,
    TinNpiConnectorError,
    TinNpiConnectorSourceVector,
    TinNpiLookupRow,
    TinTaxIdentityToken,
    TinTokenPolicyDescriptor,
    assert_generation_reuse_compatible,
    build_compact_tin_npi_generation,
    canonical_evidence_as_of,
    canonical_fhir_organization_identity_sha256,
    canonical_fhir_evidence_set_digest,
    canonical_provider_directory_payload_hash,
    canonical_source_ordinal_map_digest,
    canonical_source_ordinal_map_json,
    canonical_token_policy_id,
    extract_fhir_organization_tin_npi_evidence,
    extract_fhir_organization_tin_npi_evidence_for_policies,
    extract_normalized_fhir_organization_tin_npi_evidence,
    load_tin_token_policy,
    normalize_ein,
    token_policy_descriptor_sha256,
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


def _identifier_rule(
    *,
    source_id="source-a",
    endpoint_id="endpoint-a",
):
    return replace(
        REVIEWED_TAX_AS_EIN_RULE,
        rule_id=f"healthporta.test.fhir-tax-as-ein.{source_id}.v1",
        source_id=source_id,
        endpoint_id=endpoint_id,
    )


def _identifier_policy_for_datasets(datasets):
    rules = tuple(
        sorted(
            (
                _identifier_rule(
                    source_id=dataset.source_id,
                    endpoint_id=dataset.endpoint_id,
                )
                for dataset in datasets
            ),
            key=lambda rule: (
                rule.source_id.encode(),
                rule.endpoint_id.encode(),
                rule.rule_id.encode(),
            ),
        )
    )
    return FhirTinNpiIdentifierPolicy(
        policy_id=REVIEWED_TAX_AS_EIN_POLICY.policy_id,
        rules=rules,
    )


def _policy(tmp_path, secret=TEST_SECRET, policy_id=TOKEN_POLICY_ID):
    secret_path = tmp_path / "tin-token.key"
    if secret_path.exists():
        secret_path.chmod(0o600)
    secret_path.write_bytes(secret)
    secret_path.chmod(0o400)
    return load_tin_token_policy(
        token_policy_id=policy_id,
        secret_file=secret_path,
    )


class _RecordingProjector:
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

    def tokenize_ein(self, candidate):
        self.normalized_eins.append(candidate)
        if self.tokenize_error is not None:
            raise self.tokenize_error
        if self.returned_token is not None:
            return self.returned_token
        return self.delegate.tokenize_ein(candidate)

    def pseudonymize_source_record(self, **identity):
        self.source_record_calls.append(identity)
        return self.delegate.pseudonymize_source_record(**identity)


def _npi_identifier(value, *, system=NPI_SYSTEM):
    return {"system": system, "value": value}


def _typed_identifier(code, value, *, system=TYPE_SYSTEM):
    return {
        "type": {"coding": [{"system": system, "code": code}]},
        "value": value,
    }


def _organization(*identifiers, active=True, resource_id="organization-1"):
    return {
        "resourceType": "Organization",
        "id": resource_id,
        "active": active,
        "identifier": list(identifiers),
    }


def _extract(
    resource,
    tmp_path,
    *,
    payload_hash=None,
    identifier_policy=REVIEWED_TAX_AS_EIN_POLICY,
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
        token_projector=_policy(tmp_path),
        evidence_as_of=EVIDENCE_AS_OF,
        identifier_policy=identifier_policy,
    )


def _dataset(
    *,
    source_id="source-a",
    endpoint_id="endpoint-a",
    dataset_id="dataset-a",
    dataset_hash="a" * 64,
    organization_identities=(("organization-1", DEFAULT_ORGANIZATION_PAYLOAD_HASH),),
    source_summary_sha256="d" * 64,
):
    identifier_rule = _identifier_rule(
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
        identifier_rule_id=identifier_rule.rule_id,
        identifier_rule_sha256=identifier_rule.descriptor_sha256,
        validated_at="2026-07-27 00:00:00",
    )


def _relation(*, relation="provider_directory_dataset_resource", oid=1001):
    return ConnectorRelationIdentity(
        schema="mrf",
        relation=relation,
        relation_oid=oid,
    )


def _source_vector(
    *,
    datasets=None,
    relations=None,
    policy_ids=(TOKEN_POLICY_ID,),
    identifier_policy=None,
):
    selected_datasets = tuple(datasets or (_dataset(),))
    return TinNpiConnectorSourceVector(
        fhir_datasets=selected_datasets,
        input_relations=tuple(relations or (_relation(),)),
        token_policies=tuple(
            TinTokenPolicyDescriptor.release_1(policy_id) for policy_id in policy_ids
        ),
        evidence_as_of=EVIDENCE_AS_OF,
        identifier_policy=(
            identifier_policy or _identifier_policy_for_datasets(selected_datasets)
        ),
    )


def _scan_record(
    *results,
    source_id="source-a",
    endpoint_id="endpoint-a",
    dataset_id="dataset-a",
    resource_id="organization-1",
    payload_hash=None,
):
    assert results
    states = {result.state for result in results}
    assert len(states) == 1
    evidence = tuple(
        sorted(
            (evidence for result in results for evidence in result.evidence),
            key=lambda item: (
                item.token.token_policy_id,
                item.npi,
                item.evidence_id,
            ),
        )
    )
    if payload_hash is None:
        evidence_payload_hashes = {item.source_record_payload_hash for item in evidence}
        if evidence_payload_hashes:
            assert len(evidence_payload_hashes) == 1
            payload_hash = next(iter(evidence_payload_hashes))
        else:
            payload_hash = DEFAULT_ORGANIZATION_PAYLOAD_HASH
    return FhirOrganizationScanRecord(
        source_id=source_id,
        source_endpoint_id=endpoint_id,
        source_dataset_id=dataset_id,
        resource_id=resource_id,
        payload_hash=payload_hash,
        state=next(iter(states)),
        evidence=evidence,
    )


def _unmatched_scan_record(
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


def test_token_policy_id_enforces_frozen_ascii_grammar_and_55_byte_limit():
    maximum = TIN_TOKEN_POLICY_PREFIX + "a" * 32

    assert len(maximum.encode("ascii")) == 55
    assert canonical_token_policy_id(maximum) == maximum

    for invalid in (
        TIN_TOKEN_POLICY_PREFIX,
        TIN_TOKEN_POLICY_PREFIX + "a" * 33,
        TIN_TOKEN_POLICY_PREFIX + "UPPER",
        TIN_TOKEN_POLICY_PREFIX + "é",
        "other:a",
    ):
        with pytest.raises(TinNpiConnectorError, match="policy ID is invalid"):
            canonical_token_policy_id(invalid)


def test_release_1_policy_descriptor_matches_cross_language_vector():
    descriptor = TinTokenPolicyDescriptor(
        token_policy_id=RELEASE_1_TOKEN_POLICY_ID,
        token_policy_descriptor_sha256=RELEASE_1_POLICY_DESCRIPTOR_SHA256,
    )

    assert (
        token_policy_descriptor_sha256(RELEASE_1_TOKEN_POLICY_ID)
        == RELEASE_1_POLICY_DESCRIPTOR_SHA256
    )
    assert descriptor.public_payload() == {
        "token_policy_descriptor_sha256": (RELEASE_1_POLICY_DESCRIPTOR_SHA256),
        "token_policy_id": RELEASE_1_TOKEN_POLICY_ID,
    }
    with pytest.raises(
        TinNpiConnectorError,
        match="policy descriptor is invalid",
    ):
        TinTokenPolicyDescriptor(
            token_policy_id=RELEASE_1_TOKEN_POLICY_ID,
            token_policy_descriptor_sha256="0" * 64,
        )


@pytest.mark.parametrize(
    "raw",
    (
        "012345678",
        "01-2345678",
        " \t01-2345678\r\n",
    ),
)
def test_ein_normalization_matches_ptg_ascii_alphanumeric_contract(raw):
    assert normalize_ein(raw) == TEST_EIN_NORMALIZED


@pytest.mark.parametrize(
    "raw",
    (
        None,
        "",
        "12345678",
        "1234567890",
        "12-34AB789",
        "01 2345678",
        "01.2345678",
        "01💥2345678",
        "０１２３４５６７８",
    ),
)
def test_ein_normalization_fails_closed_without_echoing_raw_value(raw):
    with pytest.raises(TinNpiConnectorError) as error:
        normalize_ein(raw)

    if str(raw):
        assert str(raw) not in str(error.value)


def test_token_wire_vector_uses_domain_nul_independent_u16be_lengths(tmp_path):
    policy = _policy(tmp_path)

    token = policy.tokenize_ein(TEST_EIN)

    assert token.token_policy_id == TOKEN_POLICY_ID
    assert token.tin_hmac_sha256.hex() == TEST_HMAC_HEX
    assert token.tin_id_128.hex() == TEST_HMAC_HEX[:32]
    assert token.matches_full_hmac(bytes.fromhex(TEST_HMAC_HEX))
    assert policy.public_descriptor() == {
        "message_format_id": TIN_TOKEN_MESSAGE_FORMAT_ID,
        "token_policy_descriptor_sha256": (
            token_policy_descriptor_sha256(TOKEN_POLICY_ID)
        ),
        "token_policy_id": TOKEN_POLICY_ID,
    }


def test_full_hmac_is_authoritative_after_128_bit_candidate_lookup(tmp_path):
    token = _policy(tmp_path).tokenize_ein(TEST_EIN)
    colliding_digest = token.tin_id_128 + b"\xff" * 16
    collision = TinTaxIdentityToken(
        token_policy_id=TOKEN_POLICY_ID,
        tin_id_128=token.tin_id_128,
        tin_hmac_sha256=colliding_digest,
    )

    assert collision.tin_id_128 == token.tin_id_128
    assert not collision.matches_full_hmac(token.tin_hmac_sha256)
    assert not token.matches_full_hmac(colliding_digest)


@pytest.mark.parametrize(
    "secret",
    (
        b"",
        b"x" * 31,
        b"x" * 33,
        b"x" * 32 + b"\n",
    ),
)
def test_secret_file_requires_exactly_32_raw_bytes(tmp_path, secret):
    secret_path = tmp_path / "tin-token.key"
    secret_path.write_bytes(secret)
    secret_path.chmod(0o400)

    with pytest.raises(TinNpiConnectorError, match="secret file is invalid"):
        load_tin_token_policy(
            token_policy_id=TOKEN_POLICY_ID,
            secret_file=secret_path,
        )


@pytest.mark.parametrize("mode", (0o600, 0o440, 0o444))
def test_secret_file_requires_exact_0400_mode(tmp_path, mode):
    secret_path = tmp_path / "tin-token.key"
    secret_path.write_bytes(TEST_SECRET)
    secret_path.chmod(mode)

    with pytest.raises(TinNpiConnectorError, match="secret file is invalid"):
        load_tin_token_policy(
            token_policy_id=TOKEN_POLICY_ID,
            secret_file=secret_path,
        )


def test_secret_file_rejects_nonregular_path(tmp_path):
    secret_directory = tmp_path / "tin-token.key"
    secret_directory.mkdir(mode=0o400)

    with pytest.raises(TinNpiConnectorError, match="secret file is"):
        load_tin_token_policy(
            token_policy_id=TOKEN_POLICY_ID,
            secret_file=secret_directory,
        )


def test_secret_file_accepts_projected_volume_style_symbolic_link(tmp_path):
    secret_path = tmp_path / "mounted-secret.key"
    secret_path.write_bytes(TEST_SECRET)
    secret_path.chmod(0o400)
    secret_link = tmp_path / "tin-token.key"
    secret_link.symlink_to(secret_path)

    policy = load_tin_token_policy(
        token_policy_id=TOKEN_POLICY_ID,
        secret_file=secret_link,
    )

    assert policy.token_policy_id == TOKEN_POLICY_ID


def test_secret_file_rejects_fifo_without_blocking(tmp_path):
    secret_fifo = tmp_path / "tin-token.key"
    os.mkfifo(secret_fifo, mode=0o400)

    def _fail_blocked_open(_signal_number, _frame):
        raise TimeoutError("secret FIFO open blocked")

    previous_handler = signal.signal(
        signal.SIGALRM,
        _fail_blocked_open,
    )
    signal.setitimer(signal.ITIMER_REAL, 1.0)
    try:
        with pytest.raises(TinNpiConnectorError, match="secret file is invalid"):
            load_tin_token_policy(
                token_policy_id=TOKEN_POLICY_ID,
                secret_file=secret_fifo,
            )
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)


def test_secret_capability_never_exposes_or_copies_protected_material(tmp_path):
    secret = b"raw-secret-material-is-32-bytes!"
    assert len(secret) == 32
    policy = _policy(tmp_path, secret=secret)
    token = policy.tokenize_ein(TEST_EIN)
    inert_copy = copy.copy(policy)

    public_text = json.dumps(policy.public_descriptor(), sort_keys=True)
    assert secret.decode("ascii") not in repr(policy)
    assert secret.decode("ascii") not in repr(token)
    assert secret.decode("ascii") not in public_text
    assert repr(inert_copy) == "<redacted-tin-token-policy>"
    assert not hasattr(inert_copy, "tokenize_ein")


def test_explicit_same_organization_identifiers_create_exact_evidence(tmp_path):
    result = _extract(
        _organization(
            _npi_identifier("1234567893"),
            _typed_identifier("TAX", TEST_EIN),
        ),
        tmp_path,
    )

    assert result.state is FhirOrganizationEvidenceState.MATCHED
    assert len(result.evidence) == 1
    evidence = result.evidence[0]
    assert evidence.npi == 1234567893
    assert evidence.source_id == "source-a"
    assert evidence.source_endpoint_id == "endpoint-a"
    assert evidence.source_dataset_id == "dataset-a"
    assert evidence.identifier_policy_id == (REVIEWED_TAX_AS_EIN_POLICY.policy_id)
    assert (
        evidence.identifier_policy_sha256
        == REVIEWED_TAX_AS_EIN_POLICY.descriptor_sha256
    )
    assert evidence.token.tin_hmac_sha256.hex() == TEST_HMAC_HEX
    assert len(evidence.source_record_hmac_sha256) == 32
    assert len(evidence.evidence_id) == 32
    assert TEST_EIN not in repr(evidence)
    assert TEST_EIN_NORMALIZED not in repr(evidence)


def test_evidence_id_matches_hardcoded_binary_vector():
    evidence = FhirTinNpiEvidence(
        token=TinTaxIdentityToken(
            token_policy_id=RELEASE_1_TOKEN_POLICY_ID,
            tin_id_128=bytes(range(16)),
            tin_hmac_sha256=bytes(range(32)),
        ),
        npi=1234567893,
        source_id="source-vector",
        source_endpoint_id="endpoint-vector",
        source_dataset_id="dataset-vector",
        source_record_hmac_sha256=b"\x11" * 32,
        source_record_identity_sha256=b"\x22" * 32,
        source_record_payload_hash="33" * 32,
        evidence_as_of=EVIDENCE_AS_OF,
        identifier_policy_id="policy-vector",
        identifier_policy_sha256="44" * 32,
        identifier_rule_id="rule-vector",
        identifier_rule_sha256="55" * 32,
    )

    assert evidence.evidence_id.hex() == (
        "5ecb13238da3c8fa0a595e4df70a6ee4" "d68cfab5b5f281a8a26e1ba74b94c7f2"
    )


def test_same_ein_can_return_sorted_deduplicated_npi_array_source(tmp_path):
    result = _extract(
        _organization(
            _npi_identifier("1234567893"),
            _typed_identifier("NPI", "1000000004"),
            _typed_identifier("NPI", "1 000 000 004"),
            _typed_identifier("TAX", TEST_EIN),
        ),
        tmp_path,
    )

    assert result.state is FhirOrganizationEvidenceState.MATCHED
    assert [evidence.npi for evidence in result.evidence] == [
        1000000004,
        1234567893,
    ]
    assert len({evidence.evidence_id for evidence in result.evidence}) == 2


def test_multi_projector_pass_normalizes_ein_once_for_every_policy(tmp_path):
    second_policy_id = f"{TIN_TOKEN_POLICY_PREFIX}2026-08-b"
    first = _RecordingProjector(
        _policy(tmp_path, policy_id=TOKEN_POLICY_ID),
    )
    second = _RecordingProjector(
        _policy(
            tmp_path,
            secret=bytes(reversed(TEST_SECRET)),
            policy_id=second_policy_id,
        ),
    )
    organization = _organization(
        _npi_identifier("1234567893"),
        _typed_identifier("TAX", TEST_EIN),
    )

    result = extract_fhir_organization_tin_npi_evidence_for_policies(
        organization,
        source_id="source-a",
        source_endpoint_id="endpoint-a",
        source_dataset_id="dataset-a",
        resource_payload_hash=canonical_provider_directory_payload_hash(organization),
        token_projectors=(first, second),
        evidence_as_of=EVIDENCE_AS_OF,
        identifier_policy=REVIEWED_TAX_AS_EIN_POLICY,
    )

    assert result.state is FhirOrganizationEvidenceState.MATCHED
    assert first.normalized_eins == [TEST_EIN_NORMALIZED]
    assert second.normalized_eins == [TEST_EIN_NORMALIZED]
    assert [row.token.token_policy_id for row in result.evidence] == [
        TOKEN_POLICY_ID,
        second_policy_id,
    ]
    assert len(first.source_record_calls) == 1
    assert len(second.source_record_calls) == 1


def test_multi_projector_pass_fails_without_returning_partial_evidence(tmp_path):
    second_policy_id = f"{TIN_TOKEN_POLICY_PREFIX}2026-08-b"
    first = _RecordingProjector(
        _policy(tmp_path, policy_id=TOKEN_POLICY_ID),
    )
    second = _RecordingProjector(
        _policy(
            tmp_path,
            secret=bytes(reversed(TEST_SECRET)),
            policy_id=second_policy_id,
        ),
        tokenize_error=TinNpiConnectorError("synthetic projector failure"),
    )
    organization = _organization(
        _npi_identifier("1234567893"),
        _typed_identifier("TAX", TEST_EIN),
    )

    with pytest.raises(
        TinNpiConnectorError,
        match="synthetic projector failure",
    ):
        extract_fhir_organization_tin_npi_evidence_for_policies(
            organization,
            source_id="source-a",
            source_endpoint_id="endpoint-a",
            source_dataset_id="dataset-a",
            resource_payload_hash=canonical_provider_directory_payload_hash(
                organization
            ),
            token_projectors=(first, second),
            evidence_as_of=EVIDENCE_AS_OF,
            identifier_policy=REVIEWED_TAX_AS_EIN_POLICY,
        )

    assert first.normalized_eins == [TEST_EIN_NORMALIZED]
    assert second.normalized_eins == [TEST_EIN_NORMALIZED]


def test_multi_projector_pass_rejects_token_for_wrong_policy(tmp_path):
    second_policy_id = f"{TIN_TOKEN_POLICY_PREFIX}2026-08-b"
    first_delegate = _policy(tmp_path, policy_id=TOKEN_POLICY_ID)
    wrong_token = first_delegate.tokenize_ein(TEST_EIN)
    wrong_policy_projector = _RecordingProjector(
        _policy(
            tmp_path,
            secret=bytes(reversed(TEST_SECRET)),
            policy_id=second_policy_id,
        ),
        returned_token=wrong_token,
    )
    organization = _organization(
        _npi_identifier("1234567893"),
        _typed_identifier("TAX", TEST_EIN),
    )

    with pytest.raises(TinNpiConnectorError, match="returned an invalid token"):
        extract_fhir_organization_tin_npi_evidence_for_policies(
            organization,
            source_id="source-a",
            source_endpoint_id="endpoint-a",
            source_dataset_id="dataset-a",
            resource_payload_hash=canonical_provider_directory_payload_hash(
                organization
            ),
            token_projectors=(wrong_policy_projector,),
            evidence_as_of=EVIDENCE_AS_OF,
            identifier_policy=REVIEWED_TAX_AS_EIN_POLICY,
        )


def test_multi_projector_pass_rejects_duplicate_or_unordered_policies(tmp_path):
    second_policy_id = f"{TIN_TOKEN_POLICY_PREFIX}2026-08-b"
    first = _policy(tmp_path, policy_id=TOKEN_POLICY_ID)
    second = _policy(
        tmp_path,
        secret=bytes(reversed(TEST_SECRET)),
        policy_id=second_policy_id,
    )
    organization = _organization(
        _npi_identifier("1234567893"),
        _typed_identifier("TAX", TEST_EIN),
    )
    common = {
        "source_id": "source-a",
        "source_endpoint_id": "endpoint-a",
        "source_dataset_id": "dataset-a",
        "resource_payload_hash": canonical_provider_directory_payload_hash(
            organization
        ),
        "evidence_as_of": EVIDENCE_AS_OF,
        "identifier_policy": REVIEWED_TAX_AS_EIN_POLICY,
    }

    for projectors in ((first, first), (second, first)):
        with pytest.raises(
            TinNpiConnectorError,
            match="duplicated or unordered",
        ):
            extract_fhir_organization_tin_npi_evidence_for_policies(
                organization,
                token_projectors=projectors,
                **common,
            )
    with pytest.raises(TinNpiConnectorError, match="projectors are invalid"):
        extract_fhir_organization_tin_npi_evidence_for_policies(
            organization,
            token_projectors=[first, second],
            **common,
        )


def test_normalized_organization_row_adapter_uses_explicit_identifiers(tmp_path):
    payload = {
        "resource_id": "normalized-organization",
        "active": True,
        "identifiers": [
            {
                "system": NPI_SYSTEM,
                "value": "1234567893",
            },
            {
                "type_codes": [{"system": TYPE_SYSTEM, "code": "TAX"}],
                "value": TEST_EIN,
            },
        ],
    }
    result = extract_normalized_fhir_organization_tin_npi_evidence(
        {
            "resource_type": "Organization",
            "resource_id": "normalized-organization",
            "payload_hash": canonical_provider_directory_payload_hash(payload),
            "payload_json": payload,
        },
        source_id="source-a",
        source_endpoint_id="endpoint-a",
        source_dataset_id="dataset-a",
        token_projector=_policy(tmp_path),
        evidence_as_of=EVIDENCE_AS_OF,
        identifier_policy=REVIEWED_TAX_AS_EIN_POLICY,
    )

    assert result.state is FhirOrganizationEvidenceState.MATCHED
    assert "normalized-organization" not in repr(result.evidence[0])


def test_extractors_recompute_dataset_payload_hash_before_evidence(tmp_path):
    resource = _organization(
        _npi_identifier("1234567893"),
        _typed_identifier("TAX", TEST_EIN),
    )
    original_hash = canonical_provider_directory_payload_hash(resource)
    tampered_resource = {
        **resource,
        "identifier": [
            _npi_identifier("1000000004"),
            _typed_identifier("TAX", TEST_EIN),
        ],
    }

    with pytest.raises(TinNpiConnectorError, match="payload hash mismatch"):
        extract_fhir_organization_tin_npi_evidence(
            tampered_resource,
            source_id="source-a",
            source_endpoint_id="endpoint-a",
            source_dataset_id="dataset-a",
            resource_payload_hash=original_hash,
            token_projector=_policy(tmp_path),
            evidence_as_of=EVIDENCE_AS_OF,
            identifier_policy=REVIEWED_TAX_AS_EIN_POLICY,
        )
    normalized_payload = {
        "resource_id": "organization-1",
        "active": True,
        "identifiers": resource["identifier"],
    }
    with pytest.raises(TinNpiConnectorError, match="payload hash mismatch"):
        extract_normalized_fhir_organization_tin_npi_evidence(
            {
                "resource_type": "Organization",
                "resource_id": "organization-1",
                "payload_hash": "0" * 64,
                "payload_json": normalized_payload,
            },
            source_id="source-a",
            source_endpoint_id="endpoint-a",
            source_dataset_id="dataset-a",
            token_projector=_policy(tmp_path),
            evidence_as_of=EVIDENCE_AS_OF,
            identifier_policy=REVIEWED_TAX_AS_EIN_POLICY,
        )


def test_generic_tax_code_is_not_ein_without_reviewed_source_policy(tmp_path):
    organization = _organization(
        _npi_identifier("1234567893"),
        _typed_identifier("TAX", TEST_EIN),
    )
    with pytest.raises(
        TinNpiConnectorError,
        match="does not cover source endpoint",
    ):
        extract_fhir_organization_tin_npi_evidence(
            organization,
            source_id="source-a",
            source_endpoint_id="endpoint-a",
            source_dataset_id="dataset-a",
            resource_payload_hash=canonical_provider_directory_payload_hash(
                organization
            ),
            token_projector=_policy(tmp_path),
            evidence_as_of=EVIDENCE_AS_OF,
            identifier_policy=DEFAULT_FHIR_TIN_NPI_IDENTIFIER_POLICY,
        )


def test_resource_id_npi_fallback_and_fuzzy_tax_descriptors_are_rejected(tmp_path):
    resource_id_only = _extract(
        _organization(
            _typed_identifier("TAX", TEST_EIN),
            resource_id="1234567893",
        ),
        tmp_path,
    )
    fuzzy_tax = _extract(
        _organization(
            _npi_identifier("1234567893"),
            {
                "system": "https://example.test/tin",
                "type": {"text": "Employer EIN"},
                "value": TEST_EIN,
            },
        ),
        tmp_path,
    )

    assert resource_id_only.state is FhirOrganizationEvidenceState.MISSING_NPI
    assert fuzzy_tax.state is FhirOrganizationEvidenceState.MISSING_EIN


def test_untrusted_resource_id_is_validated_but_never_retained(tmp_path):
    result = _extract(
        _organization(
            _npi_identifier("1234567893"),
            _typed_identifier("TAX", TEST_EIN),
            resource_id=TEST_EIN_NORMALIZED,
        ),
        tmp_path,
    )

    assert result.state is FhirOrganizationEvidenceState.MATCHED
    evidence = result.evidence[0]
    assert TEST_EIN_NORMALIZED not in repr(evidence)
    assert not hasattr(evidence, "resource_id")
    assert len(evidence.source_record_hmac_sha256) == 32
    assert len(evidence.source_record_identity_sha256) == 32


def test_distinct_source_records_remain_distinct_without_raw_resource_ids(
    tmp_path,
):
    identifiers = (
        _npi_identifier("1234567893"),
        _typed_identifier("TAX", TEST_EIN),
    )
    first = _extract(
        _organization(*identifiers, resource_id="organization-a"),
        tmp_path,
    ).evidence[0]
    second = _extract(
        _organization(*identifiers, resource_id="organization-b"),
        tmp_path,
    ).evidence[0]

    assert first.source_record_hmac_sha256 != second.source_record_hmac_sha256
    assert first.source_record_identity_sha256 != (second.source_record_identity_sha256)
    assert first.evidence_id != second.evidence_id
    identities = (
        ("organization-a", first.source_record_payload_hash),
        ("organization-b", second.source_record_payload_hash),
    )
    generation = build_compact_tin_npi_generation(
        (
            FhirOrganizationScanRecord(
                source_id="source-a",
                source_endpoint_id="endpoint-a",
                source_dataset_id="dataset-a",
                resource_id=identities[0][0],
                payload_hash=identities[0][1],
                state=FhirOrganizationEvidenceState.MATCHED,
                evidence=(first,),
            ),
            FhirOrganizationScanRecord(
                source_id="source-a",
                source_endpoint_id="endpoint-a",
                source_dataset_id="dataset-a",
                resource_id=identities[1][0],
                payload_hash=identities[1][1],
                state=FhirOrganizationEvidenceState.MATCHED,
                evidence=(second,),
            ),
        ),
        source_vector=_source_vector(
            datasets=(_dataset(organization_identities=identities),)
        ),
    )
    assert generation.evidence_count == 2
    assert generation.forward_rows[0].evidence_count == 2


def test_scan_record_rejects_evidence_from_a_different_resource_identity(
    tmp_path,
):
    evidence = _extract(
        _organization(
            _npi_identifier("1234567893"),
            _typed_identifier("TAX", TEST_EIN),
            resource_id="organization-a",
        ),
        tmp_path,
    ).evidence

    with pytest.raises(
        TinNpiConnectorError,
        match="scan evidence identity is inconsistent",
    ):
        FhirOrganizationScanRecord(
            source_id="source-a",
            source_endpoint_id="endpoint-a",
            source_dataset_id="dataset-a",
            resource_id="organization-a",
            payload_hash="b" * 64,
            state=FhirOrganizationEvidenceState.MATCHED,
            evidence=evidence,
        )


@pytest.mark.parametrize(
    ("identifier_kind", "identifier_changes", "expected_state"),
    (
        (
            "npi",
            {"use": "old"},
            FhirOrganizationEvidenceState.MISSING_NPI,
        ),
        (
            "ein",
            {"use": "old"},
            FhirOrganizationEvidenceState.MISSING_EIN,
        ),
        (
            "ein",
            {"period": {"end": "2025-12-31"}},
            FhirOrganizationEvidenceState.MISSING_EIN,
        ),
        (
            "npi",
            {"period": {"start": "2027-01-01"}},
            FhirOrganizationEvidenceState.MISSING_NPI,
        ),
        (
            "ein",
            {"period": {"start": "not-a-date"}},
            FhirOrganizationEvidenceState.MALFORMED_IDENTIFIER_PERIOD,
        ),
        (
            "ein",
            {"period": "not-an-object"},
            FhirOrganizationEvidenceState.MALFORMED_IDENTIFIER_PERIOD,
        ),
        (
            "ein",
            {"period": {"end": "2026-07-26T23:59:59.999999Z"}},
            FhirOrganizationEvidenceState.MISSING_EIN,
        ),
        (
            "ein",
            {"period": {"end": "2026-07-27T00:00:00"}},
            FhirOrganizationEvidenceState.MALFORMED_IDENTIFIER_PERIOD,
        ),
        (
            "ein",
            {"period": {"end": "2026-07-27 00:00:00Z"}},
            FhirOrganizationEvidenceState.MALFORMED_IDENTIFIER_PERIOD,
        ),
    ),
)
def test_identifier_use_and_period_are_evaluated_at_generation_cutoff(
    identifier_kind,
    identifier_changes,
    expected_state,
    tmp_path,
):
    npi_identifier = _npi_identifier("1234567893")
    ein_identifier = _typed_identifier("TAX", TEST_EIN)
    selected = npi_identifier if identifier_kind == "npi" else ein_identifier
    selected.update(identifier_changes)

    result = _extract(
        _organization(npi_identifier, ein_identifier),
        tmp_path,
    )

    assert result.state is expected_state
    assert result.evidence == ()


@pytest.mark.parametrize(
    "period_end",
    (
        "2026-07-27",
        "2026-07-27T00:00:00Z",
        "2026-07-27T00:00:00.000001Z",
    ),
)
def test_fhir_period_end_boundary_precision_is_explicit(period_end, tmp_path):
    result = _extract(
        _organization(
            _npi_identifier("1234567893"),
            {
                **_typed_identifier("TAX", TEST_EIN),
                "period": {"end": period_end},
            },
        ),
        tmp_path,
    )

    assert result.state is FhirOrganizationEvidenceState.MATCHED


@pytest.mark.parametrize("period_end", ("9999", "9999-12", "9999-12-31"))
def test_fhir_maximum_partial_period_end_is_inclusive(period_end, tmp_path):
    result = _extract(
        _organization(
            _npi_identifier("1234567893"),
            {
                **_typed_identifier("TAX", TEST_EIN),
                "period": {"end": period_end},
            },
        ),
        tmp_path,
    )

    assert result.state is FhirOrganizationEvidenceState.MATCHED


def test_generation_requires_an_explicit_evidence_cutoff(tmp_path):
    organization = _organization(
        _npi_identifier("1234567893"),
        _typed_identifier("TAX", TEST_EIN),
    )
    with pytest.raises(TinNpiConnectorError, match="cutoff is invalid"):
        extract_fhir_organization_tin_npi_evidence(
            organization,
            source_id="source-a",
            source_endpoint_id="endpoint-a",
            source_dataset_id="dataset-a",
            resource_payload_hash=canonical_provider_directory_payload_hash(
                organization
            ),
            token_projector=_policy(tmp_path),
            evidence_as_of=None,
            identifier_policy=REVIEWED_TAX_AS_EIN_POLICY,
        )


@pytest.mark.parametrize(
    ("resource", "expected_state"),
    (
        (
            {
                "resourceType": "Practitioner",
                "id": "p1",
                "identifier": [],
            },
            FhirOrganizationEvidenceState.NOT_ORGANIZATION,
        ),
        (
            _organization(
                _npi_identifier("1234567893"),
                _typed_identifier("TAX", TEST_EIN),
                active=False,
            ),
            FhirOrganizationEvidenceState.INACTIVE,
        ),
        (
            _organization(
                _npi_identifier("1234567890"),
                _typed_identifier("TAX", TEST_EIN),
            ),
            FhirOrganizationEvidenceState.MALFORMED_NPI,
        ),
        (
            _organization(
                _npi_identifier("1234567893"),
                _typed_identifier("TAX", "not-an-ein"),
            ),
            FhirOrganizationEvidenceState.MALFORMED_EIN,
        ),
        (
            _organization(
                _npi_identifier("1234567893"),
                _typed_identifier("TAX", TEST_EIN),
                _typed_identifier("TAX", "98-7654321"),
            ),
            FhirOrganizationEvidenceState.AMBIGUOUS_EIN,
        ),
        (
            _organization(
                {
                    "system": NPI_SYSTEM,
                    "type": {"coding": [{"system": TYPE_SYSTEM, "code": "TAX"}]},
                    "value": "1234567893",
                },
            ),
            FhirOrganizationEvidenceState.CONFLICTING_IDENTIFIER_CLASS,
        ),
    ),
)
def test_evidence_extraction_fails_closed_with_non_sensitive_states(
    resource,
    expected_state,
    tmp_path,
):
    result = _extract(resource, tmp_path)

    assert result.state is expected_state
    assert result.evidence == ()


def test_source_vector_is_order_invariant_and_binds_every_input():
    dataset_a = _dataset()
    dataset_b = _dataset(
        source_id="source-b",
        endpoint_id="endpoint-b",
        dataset_id="dataset-b",
        dataset_hash="b" * 64,
    )
    relation_a = _relation()
    second_policy = TIN_TOKEN_POLICY_PREFIX + "2026-08-b"
    forward_order = _source_vector(
        datasets=(dataset_a, dataset_b),
        relations=(relation_a,),
        policy_ids=(TOKEN_POLICY_ID, second_policy),
    )
    reverse_order = _source_vector(
        datasets=(dataset_b, dataset_a),
        relations=(relation_a,),
        policy_ids=(second_policy, TOKEN_POLICY_ID),
    )

    assert forward_order.source_vector_id == reverse_order.source_vector_id
    assert forward_order.canonical_json == reverse_order.canonical_json
    assert forward_order.canonical_json == json.dumps(
        forward_order.public_payload(),
        sort_keys=True,
        separators=(",", ":"),
    )
    assert (
        replace(
            forward_order,
            fhir_datasets=(replace(dataset_a, dataset_hash="c" * 64), dataset_b),
        ).source_vector_id
        != forward_order.source_vector_id
    )
    assert (
        replace(
            forward_order,
            input_relations=tuple(
                (
                    replace(relation, relation_oid=9999)
                    if relation == relation_a
                    else relation
                )
                for relation in forward_order.input_relations
            ),
        ).source_vector_id
        != forward_order.source_vector_id
    )
    assert (
        replace(
            forward_order,
            projection_policy_id=(
                "healthporta.tin-npi.compact-same-organization-lookup.v4"
            ),
        ).source_vector_id
        != forward_order.source_vector_id
    )
    assert (
        replace(
            forward_order,
            token_policies=(TinTokenPolicyDescriptor.release_1(TOKEN_POLICY_ID),),
        ).source_vector_id
        != forward_order.source_vector_id
    )
    assert (
        replace(
            forward_order,
            evidence_as_of=canonical_evidence_as_of(OBSERVED_AT + dt.timedelta(days=1)),
        ).source_vector_id
        != forward_order.source_vector_id
    )


def test_identifier_rule_bundle_resolves_one_exact_source_endpoint():
    source_a = _identifier_rule()
    source_b = _identifier_rule(
        source_id="source-b",
        endpoint_id="endpoint-b",
    )
    policy = FhirTinNpiIdentifierPolicy(
        policy_id=REVIEWED_TAX_AS_EIN_POLICY.policy_id,
        rules=(source_a, source_b),
    )

    assert (
        policy.rule_for(
            source_id="source-a",
            endpoint_id="endpoint-a",
        )
        is source_a
    )
    assert (
        policy.rule_for(
            source_id="source-b",
            endpoint_id="endpoint-b",
        )
        is source_b
    )
    assert policy.public_payload()["rules"] == [
        {
            **source_a.public_payload(),
            "identifier_rule_sha256": source_a.descriptor_sha256,
        },
        {
            **source_b.public_payload(),
            "identifier_rule_sha256": source_b.descriptor_sha256,
        },
    ]
    with pytest.raises(
        TinNpiConnectorError,
        match="does not cover source endpoint",
    ):
        policy.rule_for(
            source_id="source-a",
            endpoint_id="endpoint-b",
        )


def test_identifier_rule_bundle_rejects_unordered_or_ambiguous_rules():
    source_a = _identifier_rule()
    source_b = _identifier_rule(
        source_id="source-b",
        endpoint_id="endpoint-b",
    )
    with pytest.raises(TinNpiConnectorError, match="rules are not ordered"):
        FhirTinNpiIdentifierPolicy(
            policy_id=REVIEWED_TAX_AS_EIN_POLICY.policy_id,
            rules=(source_b, source_a),
        )
    with pytest.raises(TinNpiConnectorError, match="rules are duplicated"):
        FhirTinNpiIdentifierPolicy(
            policy_id=REVIEWED_TAX_AS_EIN_POLICY.policy_id,
            rules=(
                source_a,
                replace(
                    source_a,
                    rule_id="healthporta.test.fhir-tax-as-ein.source-a.v2",
                ),
            ),
        )
    with pytest.raises(TinNpiConnectorError, match="rules are duplicated"):
        FhirTinNpiIdentifierPolicy(
            policy_id=REVIEWED_TAX_AS_EIN_POLICY.policy_id,
            rules=(
                source_a,
                replace(source_b, rule_id=source_a.rule_id),
            ),
        )


def test_source_vector_accepts_multiple_rotation_policies_without_raw_tin(
    tmp_path,
):
    second_policy = TIN_TOKEN_POLICY_PREFIX + "2026-08-b"
    vector = _source_vector(policy_ids=(second_policy, TOKEN_POLICY_ID))

    payload = vector.public_payload()
    serialized = json.dumps(payload, sort_keys=True)

    assert payload["token_policy_ids"] == [TOKEN_POLICY_ID, second_policy]
    assert payload["source_scope_contract_id"] == (
        "healthporta.tin-npi." "all-current-published-organization-sources.v1"
    )
    assert payload["token_policy_scope_contract_id"] == (
        "healthporta.tin-npi." "all-retained-ptg-tax-policy-descriptors.v1"
    )
    assert payload["lookup_contract_id"] == ("healthporta.tin-npi.compact-lookup.v2")
    assert payload["lookup_schema_version"] == 2
    assert payload["schema_version"] == 3
    assert payload["input_relations"] == [
        {
            "relation": "provider_directory_dataset_resource",
            "relation_oid": 1001,
            "relkind": "r",
            "relpersistence": "p",
            "schema": "mrf",
        }
    ]
    assert "physical_projections" not in payload
    assert TEST_EIN not in serialized
    assert TEST_EIN_NORMALIZED not in serialized
    assert len(vector.source_vector_id) == 64
    with pytest.raises(
        TinNpiConnectorError,
        match="does not cover every token policy",
    ):
        result = _extract(
            _organization(
                _npi_identifier("1234567893"),
                _typed_identifier("TAX", TEST_EIN),
            ),
            tmp_path,
        )
        build_compact_tin_npi_generation(
            (_scan_record(result),),
            source_vector=vector,
        )


@pytest.mark.parametrize(
    "changes",
    (
        {"status": "validated", "is_current": False},
        {
            "is_current": False,
            "promote_on_cutover": True,
            "expected_incumbent_dataset_id": "dataset-old",
        },
    ),
)
def test_source_vector_rejects_noncurrent_or_staged_fhir_datasets(changes):
    with pytest.raises(
        TinNpiConnectorError,
        match="must already be current and published",
    ):
        replace(_dataset(), **changes)


def test_source_vector_requires_recorded_fhir_completeness_metadata():
    with pytest.raises(
        TinNpiConnectorError,
        match="requires validation evidence",
    ):
        replace(_dataset(), validated_at=None)
    with pytest.raises(
        TinNpiConnectorError,
        match="requires recorded expected resources",
    ):
        replace(_dataset(), recorded_expected_resources=None)
    with pytest.raises(
        TinNpiConnectorError,
        match="must select Organization",
    ):
        replace(_dataset(), selected_resources=("Location",))


def test_source_vector_rejects_ambiguous_source_or_endpoint_dataset_identity():
    dataset = _dataset()
    with pytest.raises(
        TinNpiConnectorError,
        match="source selects more than one dataset",
    ):
        _source_vector(
            datasets=(
                dataset,
                _dataset(
                    source_id=dataset.source_id,
                    endpoint_id="endpoint-b",
                    dataset_id="dataset-b",
                    dataset_hash="b" * 64,
                ),
            ),
            identifier_policy=REVIEWED_TAX_AS_EIN_POLICY,
        )
    with pytest.raises(
        TinNpiConnectorError,
        match="endpoint dataset identities conflict",
    ):
        _source_vector(
            datasets=(
                dataset,
                _dataset(
                    source_id="source-b",
                    endpoint_id=dataset.endpoint_id,
                    dataset_id="dataset-b",
                    dataset_hash="b" * 64,
                ),
            )
        )


def test_identifier_allowlist_descriptor_is_generation_bound():
    changed_rule = replace(
        REVIEWED_TAX_AS_EIN_RULE,
        ein_systems=("https://example.test/reviewed-ein",),
    )
    changed_policy = replace(
        REVIEWED_TAX_AS_EIN_POLICY,
        rules=(changed_rule,),
    )
    original = _source_vector()
    changed = replace(
        original,
        fhir_datasets=(
            replace(
                original.fhir_datasets[0],
                identifier_rule_sha256=changed_rule.descriptor_sha256,
            ),
        ),
        identifier_policy=changed_policy,
    )

    assert changed_policy.policy_id == REVIEWED_TAX_AS_EIN_POLICY.policy_id
    assert REVIEWED_TAX_AS_EIN_POLICY.descriptor_canonical_json == json.dumps(
        REVIEWED_TAX_AS_EIN_POLICY.public_payload(),
        sort_keys=True,
        separators=(",", ":"),
    )
    assert (
        changed_policy.descriptor_sha256 != REVIEWED_TAX_AS_EIN_POLICY.descriptor_sha256
    )
    assert changed.source_vector_id != original.source_vector_id


def test_source_vector_rejects_non_dataset_resource_input_relation():
    with pytest.raises(
        TinNpiConnectorError,
        match="FHIR input relation is invalid",
    ):
        TinNpiConnectorSourceVector(
            fhir_datasets=(_dataset(),),
            input_relations=(
                _relation(relation="provider_directory_physical_projection"),
            ),
            token_policies=(TinTokenPolicyDescriptor.release_1(TOKEN_POLICY_ID),),
            evidence_as_of=EVIDENCE_AS_OF,
            identifier_policy=REVIEWED_TAX_AS_EIN_POLICY,
        )


@pytest.mark.parametrize(
    "changes",
    (
        {"relation_oid": 0},
        {"relkind": "i"},
        {"relpersistence": "u"},
    ),
)
def test_source_vector_relation_fences_require_permanent_table_identity(changes):
    with pytest.raises(TinNpiConnectorError):
        replace(_relation(), **changes)


def test_compact_generation_factors_all_npis_for_one_tin_and_reverse_lookup(
    tmp_path,
):
    multi_source_policy = FhirTinNpiIdentifierPolicy(
        policy_id=REVIEWED_TAX_AS_EIN_POLICY.policy_id,
        rules=(
            _identifier_rule(),
            _identifier_rule(
                source_id="source-b",
                endpoint_id="endpoint-b",
            ),
        ),
    )
    organization = _organization(
        _npi_identifier("1234567893"),
        _typed_identifier("NPI", "1000000004"),
        _typed_identifier("TAX", TEST_EIN),
    )
    source_a_result = _extract(
        organization,
        tmp_path,
        identifier_policy=multi_source_policy,
    )
    source_b_organization = _organization(
        _npi_identifier("1234567893"),
        _typed_identifier("TAX", TEST_EIN),
        resource_id="organization-b",
    )
    source_b_payload_hash = canonical_provider_directory_payload_hash(
        source_b_organization
    )
    source_b_result = extract_fhir_organization_tin_npi_evidence(
        source_b_organization,
        source_id="source-b",
        source_endpoint_id="endpoint-b",
        source_dataset_id="dataset-b",
        resource_payload_hash=source_b_payload_hash,
        token_projector=_policy(tmp_path),
        evidence_as_of=EVIDENCE_AS_OF,
        identifier_policy=multi_source_policy,
    )
    vector = _source_vector(
        datasets=(
            _dataset(
                organization_identities=(
                    (
                        "organization-1",
                        source_a_result.evidence[0].source_record_payload_hash,
                    ),
                ),
            ),
            _dataset(
                source_id="source-b",
                endpoint_id="endpoint-b",
                dataset_id="dataset-b",
                dataset_hash="b" * 64,
                organization_identities=(("organization-b", source_b_payload_hash),),
            ),
        ),
        identifier_policy=multi_source_policy,
    )

    generation = build_compact_tin_npi_generation(
        (
            _scan_record(source_a_result),
            _scan_record(
                source_b_result,
                source_id="source-b",
                endpoint_id="endpoint-b",
                dataset_id="dataset-b",
                resource_id="organization-b",
                payload_hash=source_b_payload_hash,
            ),
        ),
        source_vector=vector,
    )

    assert generation.source_vector_id == vector.source_vector_id
    assert generation.generation_id != vector.source_vector_id
    assert generation.evidence_count == 3
    assert not hasattr(generation, "evidence_digest")
    assert generation.source_ordinal_map == ("source-a", "source-b")
    assert generation.source_ordinal_map_json == (
        '[{"ordinal":0,"source_id":"source-a"},' '{"ordinal":1,"source_id":"source-b"}]'
    )
    assert len(generation.source_ordinal_map_digest) == 32
    assert len(generation.lookup_digest) == 32
    assert len(generation.forward_rows) == 1
    forward = generation.forward_rows[0]
    assert forward.npis == (1000000004, 1234567893)
    assert forward.evidence_count == 3
    assert forward.source_ids == ("source-a", "source-b")
    assert forward.source_bitmap == b"\x03"
    assert forward.npi_source_bitmap_matrix == b"\x01\x03"
    assert forward.source_evidence_counts == (2, 1)
    assert forward.source_bitmap_for_npi(1000000004) == b"\x01"
    assert forward.source_bitmap_for_npi(1234567893) == b"\x03"
    assert forward.npis_supported_by_source_ordinal(0) == (
        1000000004,
        1234567893,
    )
    assert forward.npis_supported_by_source_ordinal(1) == (1234567893,)
    assert [row.npi for row in generation.reverse_rows] == [
        1000000004,
        1234567893,
    ]
    with pytest.raises(
        TinNpiConnectorError,
        match="generation is inconsistent",
    ):
        replace(generation, lookup_digest=b"\0" * 32)
    with pytest.raises(
        TinNpiConnectorError,
        match="generation is inconsistent",
    ):
        replace(generation, source_ordinal_map_digest=b"\0" * 32)
    with pytest.raises(
        TinNpiConnectorError,
        match="source bitmap is invalid",
    ):
        replace(
            generation,
            forward_rows=(replace(forward, source_bitmap=b"\x01"),),
        )
    with pytest.raises(
        TinNpiConnectorError,
        match="generation is inconsistent",
    ):
        replace(
            generation,
            forward_rows=(
                replace(
                    forward,
                    npi_source_bitmap_matrix=b"\x03\x01",
                ),
            ),
        )
    with pytest.raises(
        TinNpiConnectorError,
        match="reverse rows are invalid",
    ):
        replace(generation, reverse_rows=tuple(reversed(generation.reverse_rows)))


def test_lookup_row_requires_aligned_nonempty_per_npi_source_segments():
    token = TinTaxIdentityToken(
        token_policy_id=TOKEN_POLICY_ID,
        tin_id_128=bytes(range(16)),
        tin_hmac_sha256=bytes(range(32)),
    )
    valid = {
        "token": token,
        "relationship_class": FHIR_SAME_ORGANIZATION_RELATIONSHIP,
        "npis": (1000000004, 1234567893),
        "evidence_count": 2,
        "source_ids": ("source-a",),
        "source_bitmap": b"\x01",
        "npi_source_bitmap_matrix": b"\x01\x01",
        "source_evidence_counts": (2,),
    }

    for changes in (
        {"npi_source_bitmap_matrix": b"\x01"},
        {"npi_source_bitmap_matrix": b"\x01\x00"},
        {
            "evidence_count": 3,
            "source_ids": ("source-a", "source-b"),
            "source_bitmap": b"\x03",
            "npi_source_bitmap_matrix": b"\x03\x01",
            "source_evidence_counts": (1, 2),
        },
    ):
        with pytest.raises(
            TinNpiConnectorError,
            match="source bitmap is invalid",
        ):
            TinNpiLookupRow(**{**valid, **changes})

    row = TinNpiLookupRow(**valid)
    with pytest.raises(TinNpiConnectorError, match="NPI is unavailable"):
        row.source_bitmap_for_npi(1999999999)
    with pytest.raises(TinNpiConnectorError, match="source ordinal is invalid"):
        row.npis_supported_by_source_ordinal(1)


def test_source_ordinal_map_and_lsb0_bitmap_are_derived_from_source_vector(
    tmp_path,
):
    organization = _organization(
        _npi_identifier("1234567893"),
        _typed_identifier("TAX", TEST_EIN),
    )
    organization_payload_hash = canonical_provider_directory_payload_hash(organization)
    datasets = [
        _dataset(
            source_id=f"source-{index:02d}",
            endpoint_id=f"endpoint-{index:02d}",
            dataset_id=f"dataset-{index:02d}",
            dataset_hash=f"{index + 1:x}" * 64,
            organization_identities=(("organization-1", organization_payload_hash),),
        )
        for index in range(9)
    ]
    vector = _source_vector(datasets=tuple(reversed(datasets)))
    policy = _policy(tmp_path)
    matched_indexes = {0, 3, 8}
    scan_records = []
    for index in range(9):
        common = {
            "source_id": f"source-{index:02d}",
            "endpoint_id": f"endpoint-{index:02d}",
            "dataset_id": f"dataset-{index:02d}",
            "resource_id": "organization-1",
        }
        if index not in matched_indexes:
            scan_records.append(
                _unmatched_scan_record(
                    **common,
                    payload_hash=organization_payload_hash,
                )
            )
            continue
        result = extract_fhir_organization_tin_npi_evidence(
            organization,
            source_id=common["source_id"],
            source_endpoint_id=common["endpoint_id"],
            source_dataset_id=common["dataset_id"],
            resource_payload_hash=organization_payload_hash,
            token_projector=policy,
            evidence_as_of=EVIDENCE_AS_OF,
            identifier_policy=vector.identifier_policy,
        )
        scan_records.append(_scan_record(result, **common))

    generation = build_compact_tin_npi_generation(
        tuple(scan_records),
        source_vector=vector,
    )

    expected_source_ids = tuple(f"source-{index:02d}" for index in range(9))
    assert generation.source_ordinal_map == expected_source_ids
    assert generation.source_ordinal_map_digest == (
        canonical_source_ordinal_map_digest(reversed(expected_source_ids))
    )
    assert generation.forward_rows[0].source_ids == (
        "source-00",
        "source-03",
        "source-08",
    )
    assert generation.forward_rows[0].source_bitmap == b"\x09\x01"
    assert generation.forward_rows[0].npi_source_bitmap_matrix == b"\x09\x01"
    assert generation.forward_rows[0].source_evidence_counts == (
        1,
        0,
        0,
        1,
        0,
        0,
        0,
        0,
        1,
    )


def test_compact_generation_digest_matches_cross_language_binary_vector():
    source_ids = tuple(f"source-{index:02d}" for index in range(9))
    expected_source_json = (
        '[{"ordinal":0,"source_id":"source-00"},'
        '{"ordinal":1,"source_id":"source-01"},'
        '{"ordinal":2,"source_id":"source-02"},'
        '{"ordinal":3,"source_id":"source-03"},'
        '{"ordinal":4,"source_id":"source-04"},'
        '{"ordinal":5,"source_id":"source-05"},'
        '{"ordinal":6,"source_id":"source-06"},'
        '{"ordinal":7,"source_id":"source-07"},'
        '{"ordinal":8,"source_id":"source-08"}]'
    )
    source_ordinal_map_digest = bytes.fromhex(
        "1a26df8b2720ba342b888e1a2bc5a9a2" "a9ab99ac24f76e866263a1a0eaa4ad51"
    )
    lookup_digest = bytes.fromhex("00" * 32)
    token = TinTaxIdentityToken(
        token_policy_id=RELEASE_1_TOKEN_POLICY_ID,
        tin_id_128=bytes(range(16)),
        tin_hmac_sha256=bytes(range(32)),
    )
    forward = TinNpiLookupRow(
        token=token,
        relationship_class=FHIR_SAME_ORGANIZATION_RELATIONSHIP,
        npis=(1000000004, 1234567893),
        evidence_count=3,
        source_ids=("source-00", "source-03", "source-08"),
        source_bitmap=b"\x09\x01",
        npi_source_bitmap_matrix=b"\x09\x00\x00\x01",
        source_evidence_counts=(1, 0, 0, 1, 0, 0, 0, 0, 1),
    )
    reverse_reference = NpiTinLookupReference(
        token=token,
        relationship_class=FHIR_SAME_ORGANIZATION_RELATIONSHIP,
    )
    evidence_by_index = {}
    for index, npi in ((0, 1000000004), (3, 1000000004), (8, 1234567893)):
        source_id = f"source-{index:02d}"
        endpoint_id = f"endpoint-{index:02d}"
        identifier_rule = _identifier_rule(
            source_id=source_id,
            endpoint_id=endpoint_id,
        )
        evidence_by_index[index] = FhirTinNpiEvidence(
            token=token,
            npi=npi,
            source_id=source_id,
            source_endpoint_id=endpoint_id,
            source_dataset_id=f"dataset-{index:02d}",
            source_record_hmac_sha256=bytes([index]) * 32,
            source_record_identity_sha256=bytes([index + 1]) * 32,
            source_record_payload_hash=f"{index + 1:x}" * 64,
            evidence_as_of=EVIDENCE_AS_OF,
            identifier_policy_id=REVIEWED_TAX_AS_EIN_POLICY.policy_id,
            identifier_policy_sha256=(REVIEWED_TAX_AS_EIN_POLICY.descriptor_sha256),
            identifier_rule_id=identifier_rule.rule_id,
            identifier_rule_sha256=identifier_rule.descriptor_sha256,
        )
    evidence_rows = tuple(
        sorted(evidence_by_index.values(), key=lambda row: row.evidence_id)
    )

    scan_proofs = tuple(
        FhirOrganizationScanProof(
            source_id=source_id,
            endpoint_id=f"endpoint-{index:02d}",
            dataset_id=f"dataset-{index:02d}",
            source_summary_sha256=f"{index + 1:x}" * 64,
            identifier_rule_id=_identifier_rule(
                source_id=source_id,
                endpoint_id=f"endpoint-{index:02d}",
            ).rule_id,
            identifier_rule_sha256=_identifier_rule(
                source_id=source_id,
                endpoint_id=f"endpoint-{index:02d}",
            ).descriptor_sha256,
            organization_resource_count=1,
            organization_resource_sha256=f"{index + 1:x}" * 64,
            state_counts=tuple(
                (
                    state.value,
                    int(
                        state
                        is (
                            FhirOrganizationEvidenceState.MATCHED
                            if index in {0, 3, 8}
                            else FhirOrganizationEvidenceState.MISSING_IDENTIFIERS
                        )
                    ),
                )
                for state in sorted(
                    FHIR_ORGANIZATION_SCAN_TERMINAL_STATES,
                    key=lambda candidate: candidate.value,
                )
            ),
            matched_evidence_counts=(
                (
                    RELEASE_1_TOKEN_POLICY_ID,
                    int(index in {0, 3, 8}),
                ),
            ),
            matched_evidence_sha256=canonical_fhir_evidence_set_digest(
                ((evidence_by_index[index],) if index in evidence_by_index else ())
            ).hex(),
        )
        for index, source_id in enumerate(source_ids)
    )
    lookup_digest = connector._lookup_digest((forward,))
    scan_proof_digest = connector.canonical_fhir_organization_scan_proof_digest(
        scan_proofs
    )
    generation_id = connector._generation_id(
        source_vector_id="0" * 64,
        scan_proof_digest=scan_proof_digest,
        lookup_digest=lookup_digest,
    )
    generation = CompactTinNpiGeneration(
        generation_id=generation_id,
        source_vector_id="0" * 64,
        source_ordinal_map=source_ids,
        source_ordinal_map_digest=source_ordinal_map_digest,
        scan_proofs=scan_proofs,
        scan_proof_digest=scan_proof_digest,
        lookup_digest=lookup_digest,
        evidence_rows=evidence_rows,
        forward_rows=(forward,),
        reverse_rows=tuple(
            NpiTinLookupRow(
                npi=npi,
                tax_identities=(reverse_reference,),
            )
            for npi in forward.npis
        ),
    )
    assert canonical_source_ordinal_map_json(reversed(source_ids)) == (
        expected_source_json
    )
    assert canonical_source_ordinal_map_digest(source_ids) == (
        source_ordinal_map_digest
    )
    assert generation.source_ordinal_map_json == expected_source_json
    assert lookup_digest.hex() == (
        "b4f027a31ed2e3026a597fed9b43e92e" "8cf92d2a9cee792b9d9fbc522d39c1e0"
    )
    assert scan_proof_digest.hex() == (
        "188bf914acedad21579d316310b24e4e" "d5692d7051df904952a943aaa83cec33"
    )
    assert generation_id == (
        "daf9b03d6723970de7bf205867829040" "39f4adcd414ee0688ecab896a100f12f"
    )
    assert generation.lookup_digest == lookup_digest
    assert generation.evidence_count == 3


def test_generation_rejects_partial_or_empty_self_consistent_scan(
    tmp_path,
):
    result = _extract(
        _organization(
            _npi_identifier("1234567893"),
            _typed_identifier("NPI", "1000000004"),
            _typed_identifier("TAX", TEST_EIN),
        ),
        tmp_path,
    )
    vector = _source_vector(
        datasets=(
            _dataset(
                organization_identities=(
                    (
                        "organization-1",
                        result.evidence[0].source_record_payload_hash,
                    ),
                ),
            ),
        ),
    )
    complete = build_compact_tin_npi_generation(
        (_scan_record(result),),
        source_vector=vector,
    )
    assert complete.source_vector_id == vector.source_vector_id
    assert complete.organization_count == 1
    assert complete.matched_organization_count == 1
    assert assert_generation_reuse_compatible(complete, complete) is True
    with pytest.raises(
        TinNpiConnectorError,
        match="scan completeness proof mismatch",
    ):
        build_compact_tin_npi_generation((), source_vector=vector)


def test_zero_organization_dataset_has_complete_empty_generation():
    vector = _source_vector(
        datasets=(_dataset(organization_identities=()),),
    )

    generation = build_compact_tin_npi_generation(
        (),
        source_vector=vector,
    )

    assert generation.organization_count == 0
    assert generation.matched_organization_count == 0
    assert generation.evidence_count == 0
    assert generation.forward_rows == ()
    assert generation.reverse_rows == ()
    proof = generation.scan_proofs[0]
    assert proof.matched_evidence_counts == ((TOKEN_POLICY_ID, 0),)
    assert proof.matched_evidence_sha256 == (
        canonical_fhir_evidence_set_digest(()).hex()
    )


@pytest.mark.parametrize(
    ("terminal_state", "matched_evidence_count"),
    (
        (FhirOrganizationEvidenceState.MISSING_IDENTIFIERS, 1),
        (FhirOrganizationEvidenceState.MATCHED, 0),
    ),
)
def test_scan_proof_rejects_zero_evidence_state_inconsistency(
    terminal_state,
    matched_evidence_count,
):
    state_counts = tuple(
        (state.value, int(state is terminal_state))
        for state in sorted(
            FHIR_ORGANIZATION_SCAN_TERMINAL_STATES,
            key=lambda candidate: candidate.value,
        )
    )

    with pytest.raises(TinNpiConnectorError, match="scan proof is invalid"):
        FhirOrganizationScanProof(
            source_id="source-a",
            endpoint_id="endpoint-a",
            dataset_id="dataset-a",
            source_summary_sha256="d" * 64,
            identifier_rule_id=REVIEWED_TAX_AS_EIN_RULE.rule_id,
            identifier_rule_sha256=(REVIEWED_TAX_AS_EIN_RULE.descriptor_sha256),
            organization_resource_count=1,
            organization_resource_sha256="e" * 64,
            state_counts=state_counts,
            matched_evidence_counts=((TOKEN_POLICY_ID, matched_evidence_count),),
            matched_evidence_sha256=canonical_fhir_evidence_set_digest(()).hex(),
        )


def test_organization_scan_rejects_duplicate_or_out_of_order_rows():
    first_identity = ("organization-1", "1" * 64)
    second_identity = ("organization-2", "2" * 64)
    vector = _source_vector(
        datasets=(
            _dataset(
                organization_identities=(first_identity, second_identity),
            ),
        ),
    )
    first = FhirOrganizationScanRecord(
        source_id="source-a",
        source_endpoint_id="endpoint-a",
        source_dataset_id="dataset-a",
        resource_id=first_identity[0],
        payload_hash=first_identity[1],
        state=FhirOrganizationEvidenceState.MISSING_IDENTIFIERS,
    )
    second = replace(
        first,
        resource_id=second_identity[0],
        payload_hash=second_identity[1],
    )

    with pytest.raises(TinNpiConnectorError, match="not strictly ordered"):
        build_compact_tin_npi_generation(
            (second, first),
            source_vector=vector,
        )
    with pytest.raises(TinNpiConnectorError, match="not strictly ordered"):
        build_compact_tin_npi_generation(
            (first, first),
            source_vector=vector,
        )


def test_scan_proof_requires_every_selected_token_policy():
    state_counts = tuple(
        (
            state.value,
            int(state is FhirOrganizationEvidenceState.MISSING_IDENTIFIERS),
        )
        for state in sorted(
            FHIR_ORGANIZATION_SCAN_TERMINAL_STATES,
            key=lambda candidate: candidate.value,
        )
    )

    with pytest.raises(TinNpiConnectorError, match="scan proof is invalid"):
        FhirOrganizationScanProof(
            source_id="source-a",
            endpoint_id="endpoint-a",
            dataset_id="dataset-a",
            source_summary_sha256="d" * 64,
            identifier_rule_id=REVIEWED_TAX_AS_EIN_RULE.rule_id,
            identifier_rule_sha256=(REVIEWED_TAX_AS_EIN_RULE.descriptor_sha256),
            organization_resource_count=1,
            organization_resource_sha256="e" * 64,
            state_counts=state_counts,
            matched_evidence_counts=(),
            matched_evidence_sha256=canonical_fhir_evidence_set_digest(()).hex(),
        )


def test_scan_record_rejects_mixed_full_hmac_for_one_policy(tmp_path):
    result = _extract(
        _organization(
            _npi_identifier("1234567893"),
            _typed_identifier("NPI", "1000000004"),
            _typed_identifier("TAX", TEST_EIN),
        ),
        tmp_path,
    )
    original_token = result.evidence[0].token
    colliding_token = TinTaxIdentityToken(
        token_policy_id=original_token.token_policy_id,
        tin_id_128=original_token.tin_id_128,
        tin_hmac_sha256=original_token.tin_id_128 + b"\xff" * 16,
    )
    mixed_evidence = (
        result.evidence[0],
        replace(result.evidence[1], token=colliding_token),
    )

    with pytest.raises(
        TinNpiConnectorError,
        match="policy evidence is inconsistent",
    ):
        FhirOrganizationScanRecord(
            source_id="source-a",
            source_endpoint_id="endpoint-a",
            source_dataset_id="dataset-a",
            resource_id="organization-1",
            payload_hash=result.evidence[0].source_record_payload_hash,
            state=FhirOrganizationEvidenceState.MATCHED,
            evidence=mixed_evidence,
        )


def test_compact_generation_keeps_full_hmac_collision_candidates_isolated(
    tmp_path,
):
    collision_source_evidence = _extract(
        _organization(
            _npi_identifier("1234567893"),
            _typed_identifier("TAX", TEST_EIN),
            resource_id="organization-a",
        ),
        tmp_path,
    ).evidence[0]
    evidence = _extract(
        _organization(
            _npi_identifier("1234567893"),
            _typed_identifier("TAX", TEST_EIN),
            resource_id="organization-b",
        ),
        tmp_path,
    ).evidence[0]
    colliding_token = TinTaxIdentityToken(
        token_policy_id=evidence.token.token_policy_id,
        tin_id_128=evidence.token.tin_id_128,
        tin_hmac_sha256=evidence.token.tin_id_128 + b"\xff" * 16,
    )
    collision_evidence = replace(
        collision_source_evidence,
        token=colliding_token,
    )

    generation = build_compact_tin_npi_generation(
        (
            FhirOrganizationScanRecord(
                source_id="source-a",
                source_endpoint_id="endpoint-a",
                source_dataset_id="dataset-a",
                resource_id="organization-a",
                payload_hash=(collision_source_evidence.source_record_payload_hash),
                state=FhirOrganizationEvidenceState.MATCHED,
                evidence=(collision_evidence,),
            ),
            FhirOrganizationScanRecord(
                source_id="source-a",
                source_endpoint_id="endpoint-a",
                source_dataset_id="dataset-a",
                resource_id="organization-b",
                payload_hash=evidence.source_record_payload_hash,
                state=FhirOrganizationEvidenceState.MATCHED,
                evidence=(evidence,),
            ),
        ),
        source_vector=_source_vector(
            datasets=(
                _dataset(
                    organization_identities=(
                        (
                            "organization-a",
                            collision_source_evidence.source_record_payload_hash,
                        ),
                        (
                            "organization-b",
                            evidence.source_record_payload_hash,
                        ),
                    )
                ),
            )
        ),
    )

    assert len(generation.forward_rows) == 2
    assert (
        generation.forward_rows[0].token.tin_id_128
        == generation.forward_rows[1].token.tin_id_128
    )
    assert (
        generation.forward_rows[0].token.tin_hmac_sha256
        != generation.forward_rows[1].token.tin_hmac_sha256
    )


def test_compact_generation_rejects_evidence_outside_selected_dataset_or_policy(
    tmp_path,
):
    evidence = _extract(
        _organization(
            _npi_identifier("1234567893"),
            _typed_identifier("TAX", TEST_EIN),
        ),
        tmp_path,
    ).evidence[0]
    vector = _source_vector(
        datasets=(
            _dataset(
                organization_identities=(
                    ("organization-1", evidence.source_record_payload_hash),
                ),
            ),
        ),
    )

    def record_for(candidate):
        return FhirOrganizationScanRecord(
            source_id=candidate.source_id,
            source_endpoint_id=candidate.source_endpoint_id,
            source_dataset_id=candidate.source_dataset_id,
            resource_id="organization-1",
            payload_hash=candidate.source_record_payload_hash,
            state=FhirOrganizationEvidenceState.MATCHED,
            evidence=(candidate,),
        )

    with pytest.raises(
        TinNpiConnectorError,
        match="scan is outside its source vector",
    ):
        outside_dataset = replace(evidence, source_dataset_id="other-dataset")
        build_compact_tin_npi_generation(
            (record_for(outside_dataset),),
            source_vector=vector,
        )
    with pytest.raises(
        TinNpiConnectorError,
        match="scan is outside its source vector",
    ):
        outside_endpoint = replace(evidence, source_endpoint_id="other-endpoint")
        build_compact_tin_npi_generation(
            (record_for(outside_endpoint),),
            source_vector=vector,
        )
    with pytest.raises(
        TinNpiConnectorError,
        match="identifier policy mismatch",
    ):
        changed_rule = replace(
            REVIEWED_TAX_AS_EIN_RULE,
            ein_systems=("https://example.test/reviewed-ein",),
        )
        build_compact_tin_npi_generation(
            (record_for(evidence),),
            source_vector=replace(
                vector,
                fhir_datasets=(
                    replace(
                        vector.fhir_datasets[0],
                        identifier_rule_sha256=changed_rule.descriptor_sha256,
                    ),
                ),
                identifier_policy=replace(
                    REVIEWED_TAX_AS_EIN_POLICY,
                    rules=(changed_rule,),
                ),
            ),
        )
    with pytest.raises(
        TinNpiConnectorError,
        match="identifier policy mismatch",
    ):
        later_evidence = replace(
            evidence,
            evidence_as_of=canonical_evidence_as_of(OBSERVED_AT + dt.timedelta(days=1)),
        )
        build_compact_tin_npi_generation(
            (record_for(later_evidence),),
            source_vector=vector,
        )
