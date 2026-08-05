from __future__ import annotations

import inspect
import traceback
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest

from process.ptg_parts import ptg2_tax_identity_shadow_admission as admission
from tests.ptg2_tax_identity_shadow_admission_support import (
    POLICY_ID,
    descriptor_for,
    make_sidecar_pair,
    refresh_descriptor,
    sidecar_bytes,
)


def _admit(
    scratch_root: Path,
    v1: dict[str, object],
    v2: dict[str, object],
    **limits: int,
) -> admission.TaxIdentityShadowBundleDescriptor:
    return admission.admit_tax_identity_shadow_bundle(
        scratch_root=scratch_root,
        v1_scanner_descriptor=v1,
        v2_scanner_descriptor=v2,
        **limits,
    )


def test_admits_immutable_relocation_stable_publication_disabled_shadow(tmp_path: Path) -> None:
    first_root, first_v1, first_v2 = make_sidecar_pair(tmp_path, directory_name="first")
    second_root, second_v1, second_v2 = make_sidecar_pair(tmp_path, directory_name="second")

    bundle = _admit(first_root, first_v1, first_v2)
    relocated = _admit(second_root, second_v1, second_v2)

    assert bundle.shadow_state == "SHADOW"
    assert bundle.contract == "ptg2_tax_identity_shadow_bundle_v1"
    assert bundle.projection_authority == "v1_only"
    assert bundle.publication_enabled is False
    assert bundle.v1.state_counts.total == bundle.v1.row_count == 6
    assert bundle.v2.state_counts.total == bundle.v2.row_count == 6
    assert bundle.binding_sha256 == relocated.binding_sha256
    assert bundle.binding_sha256 == (
        "b5a0afd98516a8f27cbef23cf0eec6165ae5bdbb4a9160c23bdf9b4e9f0eb7b3"
    )
    assert "publication_enabled" not in inspect.signature(
        admission.TaxIdentityShadowBundleDescriptor
    ).parameters
    with pytest.raises(FrozenInstanceError):
        setattr(bundle, "publication_enabled", True)
    with pytest.raises(TypeError):
        admission.TaxIdentityShadowBundleDescriptor(
            v1=bundle.v1,
            v2=bundle.v2,
            binding_sha256=bundle.binding_sha256,
            **{"publication_enabled": True},
        )


def test_binding_digest_changes_with_authenticated_content(tmp_path: Path) -> None:
    first_root, first_v1, first_v2 = make_sidecar_pair(tmp_path, directory_name="first")
    second_root, second_v1, second_v2 = make_sidecar_pair(
        tmp_path,
        directory_name="second",
        payload_seed=91,
    )

    assert _admit(first_root, first_v1, first_v2).binding_sha256 != _admit(
        second_root,
        second_v1,
        second_v2,
    ).binding_sha256


def test_arbitrary_rows_are_admitted_only_as_rust_owned_shadow_evidence(
    tmp_path: Path,
) -> None:
    scratch_root, v1, v2 = make_sidecar_pair(tmp_path)
    v2_path = Path(v2["path"])
    v2_path.write_bytes(sidecar_bytes(2, payload_seed=233))
    refresh_descriptor(v2)

    bundle = _admit(scratch_root, v1, v2)

    assert bundle.shadow_state == "SHADOW"
    assert bundle.projection_authority == "v1_only"
    assert bundle.publication_enabled is False


@pytest.mark.parametrize(
    ("version", "field", "invalid_value"),
    [
        (1, "version", True),
        (2, "record_bytes", True),
        (1, "row_count", True),
        (2, "format", "ptg2_provider_group_tax_identity_v1"),
        (1, "normalization_contract", "unknown"),
        (2, "token_message_contract", "unknown"),
        (1, "final", False),
        (2, "sha256", "A" * 64),
        (2, "sha256", "z" * 64),
        (1, "token_policy_id", "not a policy"),
        (1, "matched_ein_count", -1),
        (2, "bytes", 1),
        (1, "path", "relative.bin"),
    ],
)
def test_rejects_non_exact_descriptor_contracts(
    tmp_path: Path,
    version: int,
    field: str,
    invalid_value: object,
) -> None:
    scratch_root, v1, v2 = make_sidecar_pair(tmp_path)
    target = v1 if version == 1 else v2
    target[field] = invalid_value

    with pytest.raises(
        admission.TaxIdentityShadowAdmissionError,
        match="ptg2_tax_identity_shadow_descriptor_invalid",
    ):
        _admit(scratch_root, v1, v2)


@pytest.mark.parametrize("mutation", ["extra", "missing"])
def test_rejects_descriptor_field_set_drift(tmp_path: Path, mutation: str) -> None:
    scratch_root, v1, v2 = make_sidecar_pair(tmp_path)
    if mutation == "extra":
        v2["unexpected"] = "value"
    else:
        del v1["final"]

    with pytest.raises(
        admission.TaxIdentityShadowAdmissionError,
        match="ptg2_tax_identity_shadow_descriptor_invalid",
    ):
        _admit(scratch_root, v1, v2)


def test_descriptor_ceilings_fail_before_file_authentication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    scratch_root, v1, v2 = make_sidecar_pair(tmp_path)

    def unexpected_authentication(**_kwargs: object) -> None:
        pytest.fail("file authentication ran after descriptor ceiling rejection")

    monkeypatch.setattr(
        admission,
        "authenticate_shadow_artifact_pair",
        unexpected_authentication,
    )
    with pytest.raises(
        admission.TaxIdentityShadowAdmissionError,
        match="ptg2_tax_identity_shadow_ceiling_exceeded",
    ):
        _admit(scratch_root, v1, v2, max_row_count=5)
    with pytest.raises(
        admission.TaxIdentityShadowAdmissionError,
        match="ptg2_tax_identity_shadow_ceiling_exceeded",
    ):
        _admit(scratch_root, v1, v2, max_artifact_bytes=int(v1["bytes"]) - 1)


@pytest.mark.parametrize(
    ("limit_name", "limit_value"),
    [
        ("max_row_count", 0),
        ("max_row_count", admission.TAX_IDENTITY_SHADOW_MAX_ROWS + 1),
        ("max_artifact_bytes", 0),
        (
            "max_artifact_bytes",
            admission.TAX_IDENTITY_SHADOW_MAX_ARTIFACT_BYTES + 1,
        ),
    ],
)
def test_rejects_invalid_caller_ceilings(
    tmp_path: Path,
    limit_name: str,
    limit_value: int,
) -> None:
    scratch_root, v1, v2 = make_sidecar_pair(tmp_path)

    with pytest.raises(
        admission.TaxIdentityShadowAdmissionError,
        match="ptg2_tax_identity_shadow_ceiling_invalid",
    ):
        _admit(scratch_root, v1, v2, **{limit_name: limit_value})


def test_rejects_pair_policy_and_row_count_drift(tmp_path: Path) -> None:
    scratch_root, v1, v2 = make_sidecar_pair(tmp_path)
    alternate_policy = "ptg-tin-hmac-sha256-v1:synthetic-other"
    v2_path = Path(v2["path"])
    v2_path.write_bytes(sidecar_bytes(2, policy_id=alternate_policy))
    v2 = descriptor_for(v2_path, 2, policy_id=alternate_policy)
    with pytest.raises(
        admission.TaxIdentityShadowAdmissionError,
        match="ptg2_tax_identity_shadow_pair_invalid",
    ):
        _admit(scratch_root, v1, v2)

    v2_path.write_bytes(sidecar_bytes(2, row_count=5))
    v2 = descriptor_for(v2_path, 2)
    v2.update(
        {
            "row_count": 5,
            "provider_group_count": 5,
            "bytes": len(v2_path.read_bytes()),
            "matched_ein_count": 1,
            "matched_npi_count": 1,
            "missing_count": 1,
            "malformed_count": 1,
            "unsupported_type_count": 1,
        }
    )
    with pytest.raises(
        admission.TaxIdentityShadowAdmissionError,
        match="ptg2_tax_identity_shadow_pair_invalid",
    ):
        _admit(scratch_root, v1, v2)


def test_aggregate_transition_is_necessary_only_and_fail_closed() -> None:
    v1 = admission.TaxIdentityShadowStateCounts(1, 0, 1, 1, 3)
    feasible_v2 = admission.TaxIdentityShadowStateCounts(1, 1, 1, 2, 1)
    infeasible_v2 = admission.TaxIdentityShadowStateCounts(1, 1, 1, 0, 3)

    assert admission.aggregate_transition_feasible(v1, feasible_v2)
    assert not admission.aggregate_transition_feasible(v1, infeasible_v2)


@pytest.mark.parametrize(
    "field",
    ["matched_ein", "matched_npi", "missing", "malformed", "unsupported_type"],
)
@pytest.mark.parametrize("invalid_value", [-1, True])
def test_state_counts_reject_direct_invalid_construction(
    field: str,
    invalid_value: object,
) -> None:
    counts_by_field = {
        "matched_ein": 1,
        "matched_npi": 1,
        "missing": 1,
        "malformed": 1,
        "unsupported_type": 1,
    }
    counts_by_field[field] = invalid_value

    with pytest.raises(
        admission.TaxIdentityShadowAdmissionError,
        match="^ptg2_tax_identity_shadow_descriptor_invalid$",
    ):
        admission.TaxIdentityShadowStateCounts(**counts_by_field)


def test_repr_and_traceback_redact_paths_policy_and_digests(tmp_path: Path) -> None:
    scratch_root, v1, v2 = make_sidecar_pair(tmp_path)
    bundle = _admit(scratch_root, v1, v2)
    secrets = [str(scratch_root), POLICY_ID, str(v1["sha256"]), str(v2["sha256"])]
    rendered = repr(bundle) + repr(bundle.v1) + repr(bundle.v2)
    assert all(secret not in rendered for secret in secrets)

    v2["sha256"] = "not-a-digest"
    try:
        _admit(scratch_root, v1, v2)
    except admission.TaxIdentityShadowAdmissionError as error:
        error_text = "".join(traceback.format_exception(error))
    else:
        pytest.fail("invalid digest was admitted")
    assert all(secret not in error_text for secret in secrets)


@pytest.mark.parametrize("forbidden_field", ["tin", "raw_tin", "business_name"])
def test_rejects_and_redacts_raw_identity_descriptor_fields(
    tmp_path: Path,
    forbidden_field: str,
) -> None:
    scratch_root, v1, v2 = make_sidecar_pair(tmp_path)
    sentinel = f"private-{forbidden_field}-sentinel"
    v2[forbidden_field] = sentinel

    try:
        _admit(scratch_root, v1, v2)
    except admission.TaxIdentityShadowAdmissionError as error:
        rendered = repr(error) + "".join(traceback.format_exception(error))
    else:
        pytest.fail("raw identity descriptor field was admitted")
    assert sentinel not in rendered
    assert str(scratch_root) not in rendered
