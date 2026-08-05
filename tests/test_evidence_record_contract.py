from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from process import evidence_record_contract as evidence
from process import evidence_source_release_contract as releases
from tests.evidence_record_tic_support import GROUP_ID, synthetic_tic_material

NPI = "1234567893"


def _sha(character: str) -> str:
    return character * 64


def _ref(prefix: str, character: str, *, short: bool = False) -> str:
    return prefix + character * (32 if short else 64)


def _release(
    source_kind: str, marker: str = "a", end_at: str | None = None
) -> releases.PublicEvidenceSourceReleaseDescriptor:
    policy = releases._SOURCE_POLICIES[source_kind]
    verified = dict.fromkeys(
        "artifact_bytes_verified public_access_verified "
        "processing_retention_rights_verified semantic_limits_verified "
        "completeness_verified".split(),
        True,
    )
    disabled = dict.fromkeys(
        "legal_ownership_claimed exact_rate_site_claimed redistribution_enabled "
        "export_enabled publication_enabled replacement_enabled".split(),
        False,
    )
    release_by_field = {
        "source_kind": source_kind,
        "authority_classification": policy.authority,
        "trust_classification": policy.trust,
        "semantic_limits": policy.semantic_limits,
        "artifact_identity": releases.ImmutablePublicSourceIdentity(
            policy.identity_kind,
            f"public-{source_kind.replace('_', '-')}-{marker}",
            _sha(marker),
        ),
        "completeness_proof": releases.PublicEvidenceCompletenessProof(
            policy.completeness_mode,
            None if policy.completeness_mode == "positive_evidence_only" else 7,
            7,
            _sha("b"),
        ),
        "rights_classification": policy.rights,
        "rights_proof_sha256": _sha("c"),
        "source_binding": (
            releases.OpaqueSourceBindingReference(
                releases.TAX_IDENTITY_SHADOW_SOURCE_BINDING_CONTRACT, _sha("d")
            )
            if source_kind == "tic"
            else None
        ),
        "observed_interval": releases.CanonicalUtcInterval(
            "2026-07-01T00:00:00Z", "2026-07-02T00:00:00Z"
        ),
        "effective_interval": releases.CanonicalUtcInterval(
            "2026-07-01T00:00:00Z", end_at
        ),
        "import_run_id": f"public-import-{marker}",
        "source_release_id": f"public-release-{source_kind.replace('_', '-')}-{marker}",
        **verified,
        **disabled,
    }
    return releases.build_public_evidence_source_release(release_by_field)


def _source_record(marker: str = "e") -> evidence.EvidenceSourceRecordReference:
    return evidence.EvidenceSourceRecordReference(
        _ref("esr1_", marker), _ref("esp1_", marker)
    )


def _tax(
    identity_type: str = "ein", marker: str = "a"
) -> evidence.OpaqueTaxIdentityReference:
    return evidence.OpaqueTaxIdentityReference(
        identity_type,
        _ref("tip1_", marker),
        1,
        _ref("til1_", marker, short=True),
        _ref("tih1_", marker),
    )


def _address(purpose: str, marker: int = 1) -> evidence.CanonicalAddressEvidence:
    return evidence.CanonicalAddressEvidence(
        f"ak1_00000000-0000-5000-8000-{marker:012d}",
        f"pk1_00000000-0000-5000-8000-{marker + 100:012d}",
        purpose,
    )


def _common(record_type: str, source_record=None) -> dict[str, object]:
    return {
        "record_type": record_type,
        "source_record": source_record or _source_record(),
        "observed_at": "2026-07-01T12:00:00Z",
        "effective_interval": releases.CanonicalUtcInterval(
            "2026-07-01T00:00:00Z", None
        ),
    }


def _record_case(record_type: str):
    source_by_type = {
        "fhir_same_organization_identifier": "public_provider_directory_fhir",
        "hospital_ein_type2_npi": "public_hpt",
        "npi_address": "nppes_entity_address",
        "provider_directory_network_location": "public_provider_directory_fhir",
        "direct_tax_identity_address": "public_hpt",
    }
    release = _release(source_by_type[record_type])
    raw = _common(record_type)
    if record_type == "fhir_same_organization_identifier":
        raw.update(
            tax_identity=_tax(),
            organization_npi=NPI,
            organization_resource_ref=_ref("org1_", "a"),
        )
        return release, raw
    if record_type == "hospital_ein_type2_npi":
        witness = evidence.OrganizationNpiWitness(
            "src1_" + release.contract_sha256, raw["source_record"], NPI
        )
        raw.update(
            tax_identity=_tax(),
            organization_npi=NPI,
            hospital_entity_ref=_ref("hpt1_", "a"),
            organization_witness=witness,
        )
        return release, raw
    if record_type == "npi_address":
        raw.update(npi=NPI, address=_address("nppes_practice_location"))
        return release, raw
    if record_type == "provider_directory_network_location":
        raw.update(
            npi=NPI,
            address=_address("provider_directory_location"),
            practitioner_role_ref=_ref("role1_", "a"),
            location_resource_ref=_ref("loc1_", "a"),
            network_resource_ref=_ref("net1_", "a"),
            insurance_plan_resource_ref=_ref("plan1_", "a"),
            role_active=True,
        )
        return release, raw
    raw.update(tax_identity=_tax(), address=_address("hospital_location_candidate"))
    return release, raw


def _build_case(record_type: str):
    if record_type == "tic_provider_group_member":
        material = synthetic_tic_material()
        common = _common(record_type)
        return material.release, evidence.build_tic_provider_group_member_evidence(
            material.release,
            material.bundle,
            scratch_root=material.scratch_root,
            provider_group_global_id_128=GROUP_ID,
            member_npi=NPI,
            source_record=common["source_record"],
            observed_at=common["observed_at"],
            effective_interval=common["effective_interval"],
        )
    release, raw = _record_case(record_type)
    return release, evidence.build_public_evidence_record(release, raw)


RECORD_CLASSES = {
    "tic_provider_group_member": evidence.TicProviderGroupMemberEvidence,
    "fhir_same_organization_identifier": evidence.FhirSameOrganizationIdentifierEvidence,
    "hospital_ein_type2_npi": evidence.HospitalEinType2NpiEvidence,
    "npi_address": evidence.NpiAddressEvidence,
    "provider_directory_network_location": evidence.ProviderDirectoryNetworkLocationEvidence,
    "direct_tax_identity_address": evidence.DirectTaxIdentityAddressEvidence,
}


@pytest.mark.parametrize("record_type", RECORD_CLASSES)
def test_builds_six_exact_frozen_positive_variants(record_type: str) -> None:
    release, record = _build_case(record_type)

    assert type(record) is RECORD_CLASSES[record_type]
    assert record.evidence_id.startswith("ev1_") and len(record.evidence_id) == 68
    assert record.release.contract_sha256 == release.contract_sha256
    assert record.positive_evidence_only is True
    assert record.serving_authority == "none"
    overclaim_flags = (
        record.legal_ownership_claimed,
        record.employment_claimed,
        record.facility_claimed,
        record.exact_rate_site_claimed,
        record.site_match_claimed,
        record.confidence_claimed,
        record.deletion_enabled,
        record.replacement_enabled,
        record.publication_enabled,
    )
    assert overclaim_flags == (False,) * 9
    assert record.evidence_id not in repr(record)
    rebuilt = evidence.validate_public_evidence_record(record)
    assert rebuilt == record and rebuilt is not record
    with pytest.raises(FrozenInstanceError):
        setattr(record, "evidence_id", _ref("ev1_", "f"))


def test_canonical_id_is_order_independent_and_binds_witnesses() -> None:
    release, raw = _record_case("fhir_same_organization_identifier")
    first = evidence.build_public_evidence_record(release, raw)
    second = evidence.build_public_evidence_record(
        release, dict(reversed(tuple(raw.items())))
    )
    changed_by_field = dict(raw)
    changed_by_field["source_record"] = _source_record("f")

    assert first.evidence_id == second.evidence_id
    assert first.evidence_id == (
        "ev1_489e877096ed4f481a6dba0da00e21d14d0e39908cdb930efc89ab324d1449c7"
    )
    assert (
        evidence.build_public_evidence_record(release, changed_by_field).evidence_id
        != first.evidence_id
    )


@pytest.mark.parametrize(
    "forbidden_field",
    "raw_tin tax_identity_value path url payload internal_group_key confidence "
    "legal_ownership_claimed employment_claimed facility exact_rate_site site_match".split(),
)
def test_exact_input_shape_rejects_sensitive_and_overclaim_fields(
    forbidden_field: str,
) -> None:
    release, raw = _record_case("fhir_same_organization_identifier")
    raw[forbidden_field] = "123456789"

    with pytest.raises(evidence.PublicEvidenceRecordError):
        evidence.build_public_evidence_record(release, raw)


def test_rejects_missing_unknown_and_nonexact_input_mappings() -> None:
    release, raw = _record_case("fhir_same_organization_identifier")
    del raw["organization_npi"]
    with pytest.raises(evidence.PublicEvidenceRecordError):
        evidence.build_public_evidence_record(release, raw)

    class InputDictionary(dict[str, object]):
        pass

    _, valid = _record_case("fhir_same_organization_identifier")
    with pytest.raises(evidence.PublicEvidenceRecordError):
        evidence.build_public_evidence_record(release, InputDictionary(valid))
    for discriminator in ("ownership", True, None):
        invalid_by_field = dict(valid, record_type=discriminator)
        with pytest.raises(evidence.PublicEvidenceRecordError):
            evidence.build_public_evidence_record(release, invalid_by_field)


@pytest.mark.parametrize("record_type", tuple(RECORD_CLASSES)[1:])
def test_rejects_each_variant_under_the_wrong_source(record_type: str) -> None:
    _, raw = _record_case(record_type)
    wrong_release = _release(
        "public_hpt"
        if record_type not in {"hospital_ein_type2_npi", "direct_tax_identity_address"}
        else "tic"
    )
    with pytest.raises(evidence.PublicEvidenceRecordError):
        evidence.build_public_evidence_record(wrong_release, raw)


@pytest.mark.parametrize(
    ("source_kind", "purpose"),
    [
        ("nppes_entity_address", "nppes_practice_location"),
        ("nppes_entity_address", "nppes_mailing"),
        ("public_provider_directory_fhir", "provider_directory_location"),
    ],
)
def test_npi_address_accepts_only_non_hpt_source_purposes(
    source_kind: str, purpose: str
) -> None:
    release = _release(source_kind)
    raw = _common("npi_address")
    raw.update(npi=NPI, address=_address(purpose))
    assert (
        evidence.build_public_evidence_record(release, raw).address.purpose == purpose
    )

    hpt_release = _release("public_hpt")
    raw["address"] = _address("hospital_location_candidate")
    with pytest.raises(evidence.PublicEvidenceRecordError):
        evidence.build_public_evidence_record(hpt_release, raw)
    raw["organization_witness"] = object()
    with pytest.raises(evidence.PublicEvidenceRecordError):
        evidence.build_public_evidence_record(hpt_release, raw)


def test_hpt_organization_link_requires_same_release_record_npi_and_ein() -> None:
    release, raw = _record_case("hospital_ein_type2_npi")
    baseline = raw["organization_witness"]
    invalid_witnesses = (
        evidence.OrganizationNpiWitness(
            _ref("src1_", "f"), baseline.source_record, NPI
        ),
        evidence.OrganizationNpiWitness(
            "src1_" + release.contract_sha256, _source_record("f"), NPI
        ),
        evidence.OrganizationNpiWitness(
            "src1_" + release.contract_sha256, baseline.source_record, "1245319599"
        ),
    )
    for witness in invalid_witnesses:
        changed_by_field = dict(raw, organization_witness=witness)
        with pytest.raises(evidence.PublicEvidenceRecordError):
            evidence.build_public_evidence_record(release, changed_by_field)
    with pytest.raises(evidence.PublicEvidenceRecordError):
        evidence.build_public_evidence_record(
            release, dict(raw, tax_identity=_tax("npi"))
        )


@pytest.mark.parametrize(
    ("field_name", "invalid_value"),
    [
        ("role_active", 1),
        ("practitioner_role_ref", _ref("other1_", "a")),
        ("location_resource_ref", "123456789"),
        ("network_resource_ref", _ref("net1_", "A")),
        ("insurance_plan_resource_ref", _ref("plan1_", "a")[:-1]),
    ],
)
def test_network_location_requires_active_exact_domain_witnesses(
    field_name: str, invalid_value: object
) -> None:
    release, raw = _record_case("provider_directory_network_location")
    raw[field_name] = invalid_value
    with pytest.raises(evidence.PublicEvidenceRecordError):
        evidence.build_public_evidence_record(release, raw)


@pytest.mark.parametrize(
    "record_type",
    (
        "fhir_same_organization_identifier",
        "hospital_ein_type2_npi",
        "npi_address",
        "provider_directory_network_location",
    ),
)
def test_rejects_checksum_invalid_npi_in_every_npi_role(record_type: str) -> None:
    release, raw = _record_case(record_type)
    npi_field = (
        "member_npi"
        if record_type.startswith("tic_")
        else (
            "organization_npi"
            if "organization" in record_type or "hospital" in record_type
            else "npi"
        )
    )
    raw[npi_field] = "1234567890"
    with pytest.raises(evidence.PublicEvidenceRecordError):
        evidence.build_public_evidence_record(release, raw)


@pytest.mark.parametrize(
    "observed_at",
    (
        "2026-06-30T23:59:59Z",
        "2026-07-02T00:00:01Z",
        "2026-02-30T00:00:00Z",
        "2026-07-01T00:00:00+00:00",
    ),
)
def test_observation_time_is_canonical_and_within_release(observed_at: str) -> None:
    release, raw = _record_case("npi_address")
    raw["observed_at"] = observed_at
    with pytest.raises(evidence.PublicEvidenceRecordError):
        evidence.build_public_evidence_record(release, raw)


@pytest.mark.parametrize(
    "effective_interval",
    (
        releases.CanonicalUtcInterval("2026-06-30T23:59:59Z", None),
        releases.CanonicalUtcInterval("2026-07-01T00:00:00Z", None),
        releases.CanonicalUtcInterval("2026-07-01T00:00:00Z", "2026-07-02T00:00:01Z"),
        object(),
    ),
)
def test_effective_interval_must_be_contained_by_release(
    effective_interval: object,
) -> None:
    release = _release("nppes_entity_address", end_at="2026-07-02T00:00:00Z")
    _, raw = _record_case("npi_address")
    raw["effective_interval"] = effective_interval
    with pytest.raises(evidence.PublicEvidenceRecordError):
        evidence.build_public_evidence_record(release, raw)


def test_direct_hpt_address_is_ein_candidate_only() -> None:
    release, raw = _record_case("direct_tax_identity_address")
    record = evidence.build_public_evidence_record(release, raw)
    assert record.candidate_only is True
    with pytest.raises(evidence.PublicEvidenceRecordError):
        evidence.build_public_evidence_record(
            release, dict(raw, tax_identity=_tax("npi"))
        )
    with pytest.raises(evidence.PublicEvidenceRecordError):
        evidence.build_public_evidence_record(
            release, dict(raw, address=_address("nppes_mailing"))
        )


def test_revalidation_rejects_forged_ids_fixed_state_and_candidate_state() -> None:
    release, raw = _record_case("npi_address")
    record = evidence.build_public_evidence_record(release, raw)
    object.__setattr__(record, "evidence_id", _ref("ev1_", "f"))
    with pytest.raises(evidence.PublicEvidenceRecordError):
        evidence.validate_public_evidence_record(record)
    record = evidence.build_public_evidence_record(release, raw)
    object.__setattr__(record, "legal_ownership_claimed", True)
    with pytest.raises(evidence.PublicEvidenceRecordError):
        evidence.validate_public_evidence_record(record)

    hpt_release, hpt_raw = _record_case("direct_tax_identity_address")
    candidate = evidence.build_public_evidence_record(hpt_release, hpt_raw)
    object.__setattr__(candidate, "candidate_only", False)
    with pytest.raises(evidence.PublicEvidenceRecordError):
        evidence.validate_public_evidence_record(candidate)
    with pytest.raises(evidence.PublicEvidenceRecordError):
        evidence.validate_public_evidence_record(object())


def test_batch_is_order_independent_release_bound_and_positive_only() -> None:
    release = _release("nppes_entity_address")
    first_raw = _common("npi_address", _source_record("a"))
    first_raw.update(npi=NPI, address=_address("nppes_practice_location", 1))
    second_raw = _common("npi_address", _source_record("b"))
    second_raw.update(npi=NPI, address=_address("nppes_mailing", 2))
    first = evidence.build_public_evidence_record(release, first_raw)
    second = evidence.build_public_evidence_record(release, second_raw)

    batch = evidence.build_public_evidence_batch(release, (second, first))
    reverse = evidence.build_public_evidence_batch(release, (first, second))
    assert batch == reverse
    assert batch.batch_id.startswith("evb1_") and batch.record_count == 2
    assert tuple(record.evidence_id for record in batch.records) == tuple(
        sorted((first.evidence_id, second.evidence_id))
    )
    assert batch.positive_evidence_only is True and batch.serving_authority == "none"
    assert evidence.validate_public_evidence_batch(batch) == batch
    assert evidence.build_public_evidence_batch(release, ()).record_count == 0


def test_batch_rejects_duplicates_mixed_releases_lists_and_tampering() -> None:
    release, raw = _record_case("npi_address")
    record = evidence.build_public_evidence_record(release, raw)
    with pytest.raises(evidence.PublicEvidenceRecordError):
        evidence.build_public_evidence_batch(release, (record, record))
    with pytest.raises(evidence.PublicEvidenceRecordError):
        evidence.build_public_evidence_batch(release, [record])
    other_release = _release("nppes_entity_address", "f")
    with pytest.raises(evidence.PublicEvidenceRecordError):
        evidence.build_public_evidence_batch(other_release, (record,))

    batch = evidence.build_public_evidence_batch(release, (record,))
    object.__setattr__(batch, "record_count", 2)
    with pytest.raises(evidence.PublicEvidenceRecordError):
        evidence.validate_public_evidence_batch(batch)
    batch = evidence.build_public_evidence_batch(release, (record,))
    object.__setattr__(batch, "publication_enabled", True)
    with pytest.raises(evidence.PublicEvidenceRecordError):
        evidence.validate_public_evidence_batch(batch)
    with pytest.raises(evidence.PublicEvidenceRecordError):
        evidence.validate_public_evidence_batch(object())


def test_builders_wrap_unexpected_failures(monkeypatch: pytest.MonkeyPatch) -> None:
    release, raw = _record_case("npi_address")
    monkeypatch.setattr(evidence, "_normalize_variant", lambda *_args: 1 / 0)
    with pytest.raises(
        evidence.PublicEvidenceRecordError, match="^public_evidence_record_invalid$"
    ):
        evidence.build_public_evidence_record(release, raw)

    monkeypatch.undo()
    record = evidence.build_public_evidence_record(release, raw)
    monkeypatch.setattr(evidence, "_rebuild_record", lambda *_args: 1 / 0)
    with pytest.raises(evidence.PublicEvidenceRecordError):
        evidence.validate_public_evidence_record(record)
