from __future__ import annotations

from dataclasses import replace
import hashlib
import inspect
from pathlib import Path

import pytest

from process import evidence_record_contract as evidence
from process import evidence_record_values as values
from process import evidence_source_release_contract as releases
from process import evidence_tic_binding_proof as proof
from process import evidence_tic_tax_identity_binding as binding
from process.tin_npi_connector_security import token_policy_descriptor_sha256
from tests.evidence_record_tic_support import (
    GROUP_ID,
    NPI_GROUP_ID,
    UNAVAILABLE_GROUP_ID,
    build_tic_release,
    make_tic_material,
    v2_sidecar_bytes,
)
from tests.test_evidence_record_contract import (
    NPI,
    _address,
    _common,
    _record_case,
    _source_record,
)


def _build_tic(
    root: Path,
    bundle: object,
    release: releases.PublicEvidenceSourceReleaseDescriptor,
    group_id: bytes = GROUP_ID,
    member_npi: str = NPI,
):
    common = _common("tic_provider_group_member")
    return evidence.build_tic_provider_group_member_evidence(
        release,
        bundle,
        scratch_root=root,
        provider_group_global_id_128=group_id,
        member_npi=member_npi,
        source_record=common["source_record"],
        observed_at=common["observed_at"],
        effective_interval=common["effective_interval"],
    )


def test_release_one_tax_reference_has_consistency_without_key_version() -> None:
    reference_by_field = {
        "identity_type": "ein",
        "token_policy_ref": "tip1_" + "a" * 64,
        "token_policy_version": 1,
        "locator": "til1_" + "b" * 32,
        "full_hmac": "tih1_" + "b" * 64,
    }
    reference = evidence.OpaqueTaxIdentityReference(**reference_by_field)

    assert reference.locator[5:] == reference.full_hmac[5:37]
    assert (
        "key_version"
        not in inspect.signature(evidence.OpaqueTaxIdentityReference).parameters
    )
    for changed in (
        dict(reference_by_field, token_policy_version=2),
        dict(reference_by_field, locator="til1_" + "c" * 32),
    ):
        with pytest.raises(evidence.PublicEvidenceRecordError):
            evidence.OpaqueTaxIdentityReference(**changed)


@pytest.mark.parametrize("npi", ("0000000006", "3000000000", "9999999995"))
def test_npi_must_be_cms_assignable_before_luhn_is_considered(npi: str) -> None:
    with pytest.raises(evidence.PublicEvidenceRecordError):
        values._strict_npi(npi)


def test_dedicated_builder_authenticates_exact_ein_or_npi_row_without_public_receipt(
    tmp_path: Path,
) -> None:
    root, bundle, release = make_tic_material(tmp_path)
    ein_record = _build_tic(root, bundle, release)
    npi_record = _build_tic(root, bundle, release, NPI_GROUP_ID)
    ein = ein_record._source_binding_receipt
    npi = npi_record._source_binding_receipt

    expected_policy_ref = "tip1_" + token_policy_descriptor_sha256(
        bundle.v2.token_policy_id
    )
    assert ein.identity_type == "ein" and npi.identity_type == "npi"
    assert ein.token_policy_ref == npi.token_policy_ref == expected_policy_ref
    assert ein.locator[5:] == ein.full_hmac[5:37] == "11" * 16
    assert npi.locator[5:] == npi.full_hmac[5:37] == "22" * 16
    assert ein.source_binding_sha256 == bundle.binding_sha256
    assert ein.release_contract_ref == "src1_" + release.contract_sha256
    assert ein.provider_group_ref == (
        "pg1_91475fb3fbb12e07c99074f1053dae91731c3d036ddc4922b01b07d5711f734f"
    )
    assert ein.receipt_ref == (
        proof._validate_tic_tax_identity_binding_receipt(ein).receipt_ref
    )
    assert ein.provider_group_ref != npi.provider_group_ref
    assert ein.full_hmac not in repr(ein)
    assert ein.authorization_granted is False and ein.publication_enabled is False
    assert ein_record.tax_identity.full_hmac == ein.full_hmac
    assert evidence.validate_public_evidence_record(ein_record) == ein_record
    assert "TicTaxIdentityBindingReceipt" not in evidence.__all__
    assert "resolve_tic_tax_identity_binding" not in evidence.__all__


def test_dedicated_builder_redacts_wrong_release_missing_unavailable_and_bad_group(
    tmp_path: Path,
) -> None:
    root, bundle, release = make_tic_material(tmp_path)
    wrong_release = build_tic_release("f" * 64)
    cases = (
        (wrong_release, GROUP_ID),
        (release, (99).to_bytes(16, "big")),
        (release, UNAVAILABLE_GROUP_ID),
        (release, b"short"),
    )
    for candidate_release, group_id in cases:
        with pytest.raises(
            evidence.PublicEvidenceRecordError,
            match="^public_evidence_record_invalid$",
        ):
            _build_tic(root, bundle, candidate_release, group_id)


def test_dedicated_builder_rejects_tampered_descriptor_artifact_and_policy(
    tmp_path: Path,
) -> None:
    root, bundle, release = make_tic_material(tmp_path)
    candidates = (
        replace(bundle, binding_sha256="f" * 64),
        replace(bundle, v1=replace(bundle.v1, sidecar_version=True)),
        replace(bundle, v2=replace(bundle.v2, row_count=True)),
        replace(
            bundle,
            v1=replace(bundle.v1, token_policy_id=bundle.v1.token_policy_id + "x"),
        ),
        replace(bundle, v2=replace(bundle.v2, path=Path("relative.bin"))),
    )
    for candidate in candidates:
        with pytest.raises(evidence.PublicEvidenceRecordError):
            _build_tic(root, candidate, release)

    Path(bundle.v2.path).write_bytes(Path(bundle.v2.path).read_bytes()[:-1] + b"x")
    with pytest.raises(evidence.PublicEvidenceRecordError):
        _build_tic(root, bundle, release)


@pytest.mark.parametrize("mutation", ("locator", "state", "order"))
def test_dedicated_builder_rejects_invalid_authenticated_v2_row_semantics(
    tmp_path: Path, mutation: str
) -> None:
    encoded = bytearray(v2_sidecar_bytes())
    record_start = 13 + len("ptg-tin-hmac-sha256-v1:synthetic-shadow")
    if mutation == "locator":
        encoded[record_start + 17] ^= 0xFF
    elif mutation == "state":
        encoded[record_start + 16] = 9
    else:
        second_record = record_start + 65
        encoded[second_record : second_record + 16] = bytes(16)
    root, bundle, release = make_tic_material(tmp_path, v2_bytes=bytes(encoded))

    with pytest.raises(evidence.PublicEvidenceRecordError):
        _build_tic(root, bundle, release)


def test_caller_receipts_never_authorize_generic_or_direct_tic_construction(
    tmp_path: Path,
) -> None:
    root, bundle, release = make_tic_material(tmp_path)
    tic_record = _build_tic(root, bundle, release)
    receipt = tic_record._source_binding_receipt
    assert proof._validate_tic_tax_identity_binding_receipt(receipt) is receipt
    with pytest.raises(proof.TicTaxIdentityBindingError):
        proof._TicTaxIdentityBindingReceipt()

    caller_raw = _common("tic_provider_group_member")
    caller_raw.update(
        tax_identity=tic_record.tax_identity,
        provider_group_ref=tic_record.provider_group_ref,
        member_npi=tic_record.member_npi,
        source_binding_receipt=receipt,
    )
    with pytest.raises(evidence.PublicEvidenceRecordError):
        evidence.build_public_evidence_record(release, caller_raw)
    with pytest.raises(evidence.PublicEvidenceRecordError):
        evidence.TicProviderGroupMemberEvidence(
            release=tic_record.release,
            source_record=tic_record.source_record,
            observed_at=tic_record.observed_at,
            effective_interval=tic_record.effective_interval,
            evidence_id=tic_record.evidence_id,
            tax_identity=tic_record.tax_identity,
            provider_group_ref=tic_record.provider_group_ref,
            member_npi=tic_record.member_npi,
            _source_binding_receipt=receipt,
        )
    parameters = inspect.signature(
        proof._issue_tic_tax_identity_binding_receipt
    ).parameters
    assert tuple(parameters) == ("release", "bundle", "policy_id", "authenticated_row")


@pytest.mark.parametrize(
    ("field_name", "invalid_value"),
    [
        ("serving_authority", "read"),
        ("authorization_granted", 0),
        ("publication_enabled", 0),
        ("locator", "til1_" + "f" * 32),
        ("receipt_ref", "tbr1_" + "f" * 64),
    ],
)
def test_internal_proof_fixed_state_and_locator_binding_are_exact(
    tmp_path: Path, field_name: str, invalid_value: object
) -> None:
    root, bundle, release = make_tic_material(tmp_path)
    record = _build_tic(root, bundle, release)
    object.__setattr__(record._source_binding_receipt, field_name, invalid_value)
    with pytest.raises(proof.TicTaxIdentityBindingError):
        proof._validate_tic_tax_identity_binding_receipt(record._source_binding_receipt)
    with pytest.raises(evidence.PublicEvidenceRecordError):
        evidence.validate_public_evidence_record(record)


def test_record_and_batch_fixed_state_reject_bool_int_and_equality_spoofs() -> None:
    class EqualitySpoof:
        def __eq__(self, _other: object) -> bool:
            return True

    release, raw = _record_case("npi_address")
    for field_name, invalid in (
        ("record_type", EqualitySpoof()),
        ("positive_evidence_only", 1),
        ("serving_authority", EqualitySpoof()),
        ("legal_ownership_claimed", 0),
        ("publication_enabled", 0),
    ):
        record = evidence.build_public_evidence_record(release, raw)
        object.__setattr__(record, field_name, invalid)
        with pytest.raises(evidence.PublicEvidenceRecordError):
            evidence.validate_public_evidence_record(record)

    record = evidence.build_public_evidence_record(release, raw)
    for field_name, invalid in (
        ("contract", EqualitySpoof()),
        ("positive_evidence_only", 1),
        ("serving_authority", EqualitySpoof()),
        ("publication_enabled", 0),
    ):
        batch = evidence.build_public_evidence_batch(release, (record,))
        object.__setattr__(batch, field_name, invalid)
        with pytest.raises(evidence.PublicEvidenceRecordError):
            evidence.validate_public_evidence_batch(batch)


def test_generic_builder_rejects_nonexact_keys_before_lookup_or_equality() -> None:
    class StringKey(str):
        pass

    class EqualityTrap:
        def __hash__(self) -> int:
            return 1

        def __eq__(self, _other: object) -> bool:
            raise AssertionError("key equality must not run")

    release, valid = _record_case("npi_address")
    discriminator = valid.pop("record_type")
    valid[StringKey("record_type")] = discriminator
    with pytest.raises(evidence.PublicEvidenceRecordError):
        evidence.build_public_evidence_record(release, valid)
    with pytest.raises(evidence.PublicEvidenceRecordError):
        evidence.build_public_evidence_record(release, {EqualityTrap(): object()})


def test_generic_builder_rejects_oversized_dict_before_key_materialization() -> None:
    release, _raw = _record_case("npi_address")
    oversized_by_field = {f"synthetic_{index}": object() for index in range(12)}
    with pytest.raises(evidence.PublicEvidenceRecordError):
        evidence.build_public_evidence_record(release, oversized_by_field)


def test_artifact_types_fail_before_path_methods_arithmetic_or_value_echo(
    tmp_path: Path,
) -> None:
    class DerivedPath(type(Path())):
        pass

    class ArithmeticTrap:
        def __mul__(self, _other: object) -> object:
            raise AssertionError("private-row-count")

    _root, bundle, _release = make_tic_material(tmp_path)
    candidates = (
        replace(bundle.v1, path=DerivedPath(bundle.v1.path)),
        replace(bundle.v1, row_count=ArithmeticTrap()),
        replace(bundle.v1, token_policy_id=type("Secret", (str,), {})("private")),
    )
    for candidate in candidates:
        with pytest.raises(binding.TicTaxIdentityBindingError) as captured:
            binding._validated_artifact(candidate, 1)
        assert str(captured.value) == binding._INVALID


def _address_record(
    release: releases.PublicEvidenceSourceReleaseDescriptor, index: int
) -> evidence.NpiAddressEvidence:
    source_hash = hashlib.sha256(f"source-{index}".encode("ascii")).hexdigest()
    payload_hash = hashlib.sha256(f"payload-{index}".encode("ascii")).hexdigest()
    raw = _common(
        "npi_address",
        evidence.EvidenceSourceRecordReference(
            "esr1_" + source_hash, "esp1_" + payload_hash
        ),
    )
    raw.update(npi=NPI, address=_address("nppes_practice_location"))
    return evidence.build_public_evidence_record(release, raw)


def test_batch_accepts_max_minus_one_and_max_records() -> None:
    from tests.test_evidence_record_contract import _release

    release = _release("nppes_entity_address")
    records = tuple(
        _address_record(release, index)
        for index in range(evidence.PUBLIC_EVIDENCE_BATCH_MAX_RECORDS)
    )
    assert evidence.build_public_evidence_batch(release, records[:-1]).record_count == (
        evidence.PUBLIC_EVIDENCE_BATCH_MAX_RECORDS - 1
    )
    assert evidence.build_public_evidence_batch(release, records).record_count == (
        evidence.PUBLIC_EVIDENCE_BATCH_MAX_RECORDS
    )


def test_batch_rejects_max_plus_one_before_record_work(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from tests.test_evidence_record_contract import _release

    release = _release("nppes_entity_address")
    oversized = (object(),) * (evidence.PUBLIC_EVIDENCE_BATCH_MAX_RECORDS + 1)
    monkeypatch.setattr(
        evidence,
        "validate_public_evidence_record",
        lambda _value: pytest.fail("oversized batch performed record validation"),
    )
    monkeypatch.setattr(
        evidence,
        "_validated_release",
        lambda _value: pytest.fail("oversized batch performed release validation"),
    )
    with pytest.raises(evidence.PublicEvidenceRecordError):
        evidence.build_public_evidence_batch(release, oversized)
    with pytest.raises(evidence.PublicEvidenceRecordError):
        evidence.PublicEvidenceBatch(
            release, oversized, len(oversized), "evb1_" + "a" * 64
        )


def test_internal_variant_dispatch_rejects_unknown_types_fail_closed() -> None:
    from tests.test_evidence_record_contract import _release, _tax

    hpt_release = _release("public_hpt")
    variant_by_field = {
        "tax_identity": _tax(),
        "address": _address("hospital_location_candidate"),
    }
    with pytest.raises(evidence.PublicEvidenceRecordError):
        values._variant_payload("future_relation", variant_by_field)
    with pytest.raises(evidence.PublicEvidenceRecordError):
        values._normalize_variant_semantics(
            "future_relation", hpt_release, _source_record(), variant_by_field
        )


def test_binding_validators_reject_wrong_types_and_damaged_nested_state(
    tmp_path: Path,
) -> None:
    _root, bundle, _release = make_tic_material(tmp_path)
    for operation in (
        lambda: binding._strict_sha256("bad"),
        lambda: binding._validated_state_counts(object()),
        lambda: binding._validated_artifact(object(), 1),
        lambda: binding.validate_tax_identity_shadow_bundle_descriptor(object()),
        lambda: proof._validate_tic_tax_identity_binding_receipt(object()),
    ):
        with pytest.raises(binding.TicTaxIdentityBindingError):
            operation()

    damaged_counts = replace(bundle.v1.state_counts)
    object.__delattr__(damaged_counts, "matched_ein")
    with pytest.raises(binding.TicTaxIdentityBindingError):
        binding._validated_state_counts(damaged_counts)

    damaged_artifact = replace(bundle.v1)
    object.__setattr__(damaged_artifact, "row_count", object())
    with pytest.raises(binding.TicTaxIdentityBindingError):
        binding._validated_artifact(damaged_artifact, 1)

    damaged_bundle = replace(bundle)
    object.__delattr__(damaged_bundle, "v1")
    with pytest.raises(binding.TicTaxIdentityBindingError):
        binding.validate_tax_identity_shadow_bundle_descriptor(damaged_bundle)


def test_binding_defensive_io_and_receipt_failures_are_redacted(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    class RaisingStream:
        def read(self, _count: int) -> bytes:
            raise OSError("synthetic private path")

    class ShortStream:
        def read(self, _count: int) -> bytes:
            return b""

    for stream in (RaisingStream(), ShortStream()):
        with pytest.raises(
            binding.TicTaxIdentityBindingError,
            match=f"^{binding._INVALID}$",
        ):
            binding._read_exact_record(stream)

    root, bundle, release = make_tic_material(tmp_path)
    monkeypatch.setattr(
        binding.shadow_files, "_is_held_artifact_pair_distinct", lambda *_args: False
    )
    with pytest.raises(binding.TicTaxIdentityBindingError):
        binding._resolve_tic_tax_identity_binding(
            release,
            bundle,
            scratch_root=root,
            provider_group_global_id_128=GROUP_ID,
        )

    monkeypatch.undo()
    receipt = binding._resolve_tic_tax_identity_binding(
        release,
        bundle,
        scratch_root=root,
        provider_group_global_id_128=GROUP_ID,
    )
    object.__delattr__(receipt, "release_contract_ref")
    with pytest.raises(proof.TicTaxIdentityBindingError):
        proof._validate_tic_tax_identity_binding_receipt(receipt)
    with pytest.raises(binding.TicTaxIdentityBindingError):
        binding._resolve_tic_tax_identity_binding(
            object(),
            bundle,
            scratch_root=root,
            provider_group_global_id_128=GROUP_ID,
        )


def test_authenticated_row_reader_rejects_extra_unvalidated_rows(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, bundle, _release = make_tic_material(tmp_path)
    shortened = replace(
        bundle,
        v2=replace(bundle.v2, row_count=bundle.v2.row_count - 1),
    )
    monkeypatch.setattr(
        binding.shadow_files, "_authenticate_held_artifact", lambda _held: None
    )
    with pytest.raises(binding.TicTaxIdentityBindingError):
        binding._read_authenticated_row(shortened, root, GROUP_ID)
