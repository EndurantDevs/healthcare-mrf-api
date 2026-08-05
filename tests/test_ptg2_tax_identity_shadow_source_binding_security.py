from __future__ import annotations

import builtins
import traceback
from pathlib import Path
from types import SimpleNamespace

import pytest

from process.ptg_parts import ptg2_tax_identity_shadow_admission as admission
from process.ptg_parts import ptg2_tax_identity_shadow_source_binding as binding
from tests.ptg2_tax_identity_shadow_admission_support import make_sidecar_pair
from tests.test_ptg2_tax_identity_shadow_source_binding import (
    _admitted_bundle,
    _bind,
    _binding_error,
    _source_input,
)


@pytest.mark.parametrize(
    ("field", "invalid_value"),
    [("shadow_bundle", SimpleNamespace()), ("source", SimpleNamespace())],
)
def test_binder_rejects_non_authoritative_input_types(
    tmp_path: Path, field: str, invalid_value: object
) -> None:
    """Reject substitutes for both authoritative binding inputs."""

    shadow_bundle: object = _admitted_bundle(tmp_path)
    source_binding_input: object = _source_input()
    if field == "shadow_bundle":
        shadow_bundle = invalid_value
    else:
        source_binding_input = invalid_value
    with _binding_error():
        binding.bind_tax_identity_shadow_source(
            shadow_bundle=shadow_bundle,
            source=source_binding_input,
        )


def test_ambient_activation_and_file_access_cannot_change_binding(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Keep the source binding independent of ambient activation and file I/O."""

    shadow_bundle = _admitted_bundle(tmp_path)
    source_binding_input = _source_input()
    baseline = _bind(shadow_bundle, source_binding_input)
    for env_name in (
        "HLTHPRT_PTG2_MANIFEST_PROVIDER_GROUP_TAX_IDENTITY_V2_SIDECAR_PATH",
        "HLTHPRT_PTG2_PROVIDER_GRAPH_V4",
        "HLTHPRT_PTG2_AUTO_ACTIVATE_CANDIDATES",
    ):
        monkeypatch.setenv(env_name, "1")

    def unexpected_file_access(*_args: object, **_kwargs: object) -> object:
        pytest.fail("source binding attempted file access")

    monkeypatch.setattr(builtins, "open", unexpected_file_access)
    monkeypatch.setattr(Path, "open", unexpected_file_access)
    rebound = _bind(shadow_bundle, source_binding_input)

    assert rebound == baseline
    assert rebound.projection_authority == "v1_only"
    assert rebound.publication_enabled is False
    assert "os" not in binding.__dict__
    assert "config" not in binding.__dict__


def test_binding_is_metadata_only_and_later_admission_reauthenticates(
    tmp_path: Path,
) -> None:
    """Keep binding pathless while later admission reauthenticates changed bytes."""

    scratch_root, v1, v2 = make_sidecar_pair(tmp_path)
    bundle = admission.admit_tax_identity_shadow_bundle(
        scratch_root=scratch_root,
        v1_scanner_descriptor=v1,
        v2_scanner_descriptor=v2,
    )
    bound = _bind(bundle, _source_input())
    before_mutation = bound.binding_sha256
    v2_path = Path(v2["path"])
    changed = bytearray(v2_path.read_bytes())
    changed[-1] ^= 1
    v2_path.write_bytes(changed)

    assert bound.binding_sha256 == before_mutation
    with pytest.raises(
        admission.TaxIdentityShadowAdmissionError,
        match="ptg2_tax_identity_shadow_artifact_invalid",
    ):
        admission.admit_tax_identity_shadow_bundle(
            scratch_root=scratch_root,
            v1_scanner_descriptor=v1,
            v2_scanner_descriptor=v2,
        )


def test_repr_errors_and_tracebacks_redact_all_coordinates(tmp_path: Path) -> None:
    """Keep every private coordinate out of reprs, errors, and tracebacks."""

    shadow_bundle = _admitted_bundle(tmp_path)
    source_binding_input = _source_input()
    bound = _bind(shadow_bundle, source_binding_input)
    secrets = {
        str(shadow_bundle.v1.path),
        str(shadow_bundle.v2.path),
        shadow_bundle.binding_sha256,
        source_binding_input.physical_identity.source_type,
        source_binding_input.physical_identity.identity_sha256,
        source_binding_input.source_identity_hash,
        source_binding_input.source_file_version_id,
        source_binding_input.raw_container_sha256,
        source_binding_input.source_run_contract_sha256,
        source_binding_input.source_shard_id,
        source_binding_input.import_run_id,
        source_binding_input.snapshot_id,
    }
    rendered = repr(bound) + repr(bound.coordinates) + repr(source_binding_input)
    assert all(secret not in rendered for secret in secrets)

    sentinel = "private-coordinate-sentinel"
    try:
        _source_input(import_run_id=f"run\u0085{sentinel}")
    except binding.TaxIdentityShadowSourceBindingError as error:
        error_text = repr(error) + "".join(traceback.format_exception(error))
    else:
        pytest.fail("Unicode control coordinate was admitted")
    assert sentinel not in error_text
    assert all(secret not in error_text for secret in secrets)

    object.__setattr__(shadow_bundle.v1, "path", Path(sentinel))
    try:
        _bind(shadow_bundle, source_binding_input)
    except binding.TaxIdentityShadowSourceBindingError as error:
        canonical_error_text = repr(error) + "".join(traceback.format_exception(error))
    else:
        pytest.fail("tampered admission descriptor was bound")
    assert sentinel not in canonical_error_text
