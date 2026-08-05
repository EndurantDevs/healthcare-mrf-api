from __future__ import annotations

from dataclasses import replace
from pathlib import Path

import pytest

from process import evidence_record_batch as batches
from process import evidence_record_contract as evidence
from process import evidence_record_values as values
from process import evidence_tic_binding_proof as proof
from process import evidence_tic_tax_identity_binding as binding
from process.ptg_parts import ptg2_tax_identity_shadow_admission as admission
from tests.evidence_record_tic_support import GROUP_ID, make_tic_material
from tests.test_evidence_record_contract import NPI, _common, _record_case


def _tic_record(tmp_path: Path):
    root, bundle, release = make_tic_material(tmp_path)
    common = _common("tic_provider_group_member")
    record = evidence.build_tic_provider_group_member_evidence(
        release,
        bundle,
        scratch_root=root,
        provider_group_global_id_128=GROUP_ID,
        member_npi=NPI,
        source_record=common["source_record"],
        observed_at=common["observed_at"],
        effective_interval=common["effective_interval"],
    )
    return root, bundle, release, record


def test_invalid_member_npi_is_rejected_before_tic_resolution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root, bundle, release = make_tic_material(tmp_path)
    common = _common("tic_provider_group_member")
    resolution_calls: list[object] = []

    def record_resolution(*args: object, **kwargs: object) -> object:
        resolution_calls.append((args, kwargs))
        raise AssertionError("TiC resolution must not run")

    monkeypatch.setattr(
        evidence, "_resolve_tic_tax_identity_binding", record_resolution
    )
    with pytest.raises(evidence.PublicEvidenceRecordError):
        evidence.build_tic_provider_group_member_evidence(
            release,
            bundle,
            scratch_root=root,
            provider_group_global_id_128=GROUP_ID,
            member_npi="1234567890",
            source_record=common["source_record"],
            observed_at=common["observed_at"],
            effective_interval=common["effective_interval"],
        )
    assert resolution_calls == []


@pytest.mark.parametrize(
    ("field_name", "invalid_value"),
    [
        ("byte_count", -1),
        ("byte_count", admission.TAX_IDENTITY_SHADOW_MAX_ARTIFACT_BYTES + 1),
        ("row_count", -1),
        ("row_count", admission.TAX_IDENTITY_SHADOW_MAX_ROWS + 1),
        ("provider_group_count", -1),
        ("provider_group_count", admission.TAX_IDENTITY_SHADOW_MAX_ROWS + 1),
    ],
)
def test_artifact_cardinality_is_bounded_before_contract_arithmetic(
    tmp_path: Path, field_name: str, invalid_value: int
) -> None:
    _root, bundle, _release = make_tic_material(tmp_path)
    candidate = replace(bundle.v1)
    object.__setattr__(candidate, field_name, invalid_value)
    with pytest.raises(binding.TicTaxIdentityBindingError):
        binding._validated_artifact(candidate, 1)


@pytest.mark.parametrize(
    "state_counts",
    [
        admission.TaxIdentityShadowStateCounts(
            admission.TAX_IDENTITY_SHADOW_MAX_ROWS + 1, 0, 0, 0, 0
        ),
        admission.TaxIdentityShadowStateCounts(60_000_000, 0, 60_000_000, 0, 0),
    ],
)
def test_state_count_components_and_total_are_bounded_before_sum_use(
    state_counts: admission.TaxIdentityShadowStateCounts,
) -> None:
    with pytest.raises(binding.TicTaxIdentityBindingError):
        binding._validated_state_counts(state_counts)


def test_contract_internal_dispatch_and_missing_discriminator_fail_closed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release, raw = _record_case("npi_address")
    raw.pop("record_type")
    with pytest.raises(evidence.PublicEvidenceRecordError):
        evidence.build_public_evidence_record(release, raw)
    with pytest.raises(evidence.PublicEvidenceRecordError):
        evidence._record_values(object(), "future_relation")
    with pytest.raises(evidence.PublicEvidenceRecordError):
        evidence._finish_record(object())

    def reject_release(_value: object) -> None:
        raise evidence.PublicEvidenceRecordError("public_evidence_record_invalid")

    monkeypatch.setattr(evidence, "_validated_release", reject_release)
    with pytest.raises(evidence.PublicEvidenceRecordError):
        evidence.build_tic_provider_group_member_evidence(
            object(),
            object(),
            scratch_root="/synthetic",
            provider_group_global_id_128=bytes(16),
            member_npi=NPI,
            source_record=object(),
            observed_at="synthetic",
            effective_interval=object(),
        )


def test_batch_validator_wraps_unexpected_rebuild_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    release, raw = _record_case("npi_address")
    record = evidence.build_public_evidence_record(release, raw)
    batch = evidence.build_public_evidence_batch(release, (record,))
    monkeypatch.setattr(batches, "_validated_release", lambda _value: 1 / 0)
    with pytest.raises(evidence.PublicEvidenceRecordError):
        evidence.validate_public_evidence_batch(batch)


def test_private_proof_helpers_and_record_binding_fail_closed(tmp_path: Path) -> None:
    _root, bundle, release, record = _tic_record(tmp_path)
    for operation in (
        lambda: proof._strict_sha256("bad"),
        lambda: proof._strict_prefixed_hex("bad", "src1_", proof._HEX_64_RE),
        lambda: proof._receipt_digest({}),
        lambda: proof._issue_tic_tax_identity_binding_receipt(
            release, bundle, bundle.v2.token_policy_id, object()
        ),
        lambda: proof._validate_tic_binding_for_record(
            record._source_binding_receipt,
            release,
            identity_type="npi",
            token_policy_ref=record.tax_identity.token_policy_ref,
            provider_group_ref=record.provider_group_ref,
            locator=record.tax_identity.locator,
            full_hmac=record.tax_identity.full_hmac,
        ),
    ):
        with pytest.raises(proof.TicTaxIdentityBindingError):
            operation()


def test_binding_descriptor_exception_and_fixed_state_edges(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _root, bundle, _release = make_tic_material(tmp_path)
    unicode_policy = replace(bundle.v1, token_policy_id="\N{SNOWMAN}")
    with pytest.raises(binding.TicTaxIdentityBindingError):
        binding._validated_artifact(unicode_policy, 1)
    with pytest.raises(binding.TicTaxIdentityBindingError):
        binding._validated_row(b"short")

    damaged_bundle = replace(bundle)
    object.__setattr__(damaged_bundle, "contract", True)
    with pytest.raises(binding.TicTaxIdentityBindingError):
        binding.validate_tax_identity_shadow_bundle_descriptor(damaged_bundle)

    monkeypatch.setattr(binding, "_artifact_contract_fields", lambda *_args: 1 / 0)
    with pytest.raises(binding.TicTaxIdentityBindingError):
        binding._validated_artifact(bundle.v1, 1)


def test_tic_semantics_reject_non_tic_release_before_variant_access() -> None:
    release, _raw = _record_case("npi_address")
    with pytest.raises(values.PublicEvidenceRecordError):
        values._normalize_variant_semantics(
            "tic_provider_group_member", release, object(), {}
        )
