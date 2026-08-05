from __future__ import annotations

from dataclasses import FrozenInstanceError
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from process.ptg_parts import ptg2_tax_identity_shadow_admission as admission
from process.ptg_parts import ptg2_tax_identity_shadow_source_binding as binding
from process.ptg_parts.ptg2_shared_reuse import SharedPhysicalArtifactIdentity
from tests.ptg2_tax_identity_shadow_admission_support import (
    assert_bound_descriptor_identity,
    assert_bound_descriptor_non_publishable,
    make_sidecar_pair,
)

RAW_SHA256 = "1" * 64
LOGICAL_SHA256 = "2" * 64
SOURCE_RUN_SHA256 = "3" * 64
SOURCE_IDENTITY_HASH = "4" * 16
SOURCE_FILE_VERSION_ID = "5" * 16


def _admitted_bundle(
    tmp_path: Path, *, directory_name: str = "shadow", payload_seed: int = 0
) -> admission.TaxIdentityShadowBundleDescriptor:
    scratch_root, v1, v2 = make_sidecar_pair(
        tmp_path, directory_name=directory_name, payload_seed=payload_seed
    )
    return admission.admit_tax_identity_shadow_bundle(
        scratch_root=scratch_root,
        v1_scanner_descriptor=v1,
        v2_scanner_descriptor=v2,
    )


def _source_input(**overrides: Any) -> binding.TaxIdentityShadowSourceBindingInput:
    fields_by_name: dict[str, Any] = {
        "physical_identity": SharedPhysicalArtifactIdentity(
            "in-network", "logical_json_sha256_v1", LOGICAL_SHA256
        ),
        "source_identity_hash": SOURCE_IDENTITY_HASH,
        "source_file_version_id": SOURCE_FILE_VERSION_ID,
        "raw_container_sha256": RAW_SHA256,
        "logical_json_sha256": LOGICAL_SHA256,
        "logical_hash_deferred": False,
        "source_shard_id": "file:17",
        "source_run_contract_sha256": SOURCE_RUN_SHA256,
        "import_run_id": "ptg2:synthetic-import",
        "snapshot_id": "ptg2:209901:synthetic",
    }
    fields_by_name.update(overrides)
    return binding.TaxIdentityShadowSourceBindingInput(**fields_by_name)


def _bind(
    shadow_bundle, source_binding_input
) -> binding.BoundTaxIdentityShadowBundleDescriptor:
    return binding.bind_tax_identity_shadow_source(
        shadow_bundle=shadow_bundle, source=source_binding_input
    )


def _binding_error():
    return pytest.raises(
        binding.TaxIdentityShadowSourceBindingError,
        match="^ptg2_tax_identity_shadow_source_binding_invalid$",
    )


def _coordinate_fields(**overrides: Any) -> dict[str, Any]:
    coordinates_by_name: dict[str, Any] = {
        "source_type": "in-network",
        "physical_identity_kind": "logical_json_sha256_v1",
        "physical_identity_sha256": LOGICAL_SHA256,
        "source_identity_hash": SOURCE_IDENTITY_HASH,
        "source_file_version_id": SOURCE_FILE_VERSION_ID,
        "raw_container_sha256": RAW_SHA256,
        "logical_json_sha256": LOGICAL_SHA256,
        "logical_hash_deferred": False,
        "source_shard_id": "file:17",
        "source_run_contract_sha256": SOURCE_RUN_SHA256,
        "import_run_id": "ptg2:synthetic-import",
        "snapshot_id": "ptg2:209901:synthetic",
    }
    coordinates_by_name.update(overrides)
    return coordinates_by_name


def test_binds_pathless_relocation_stable_fixed_authority_descriptor(
    tmp_path: Path,
) -> None:
    """Prove relocation stability and an immutable non-publishable surface."""

    source_binding_input = _source_input()
    first = _bind(
        _admitted_bundle(tmp_path, directory_name="first"), source_binding_input
    )
    second = _bind(
        _admitted_bundle(tmp_path, directory_name="second"), source_binding_input
    )

    assert_bound_descriptor_identity(first, second)
    assert_bound_descriptor_non_publishable(first)


@pytest.mark.parametrize(
    ("source_hash", "source_version", "source_shard"),
    [
        ("6" * 16, "7" * 16, "file:0"),
        ("6" * 32, "7" * 32, "manifest:19"),
        ("6" * 64, "7" * 32, f"manifest:{'8' * 16}"),
        ("6" * 16, "7" * 16, f"manifest:{'8' * 32}"),
        ("6" * 32, "7" * 32, f"manifest:{'8' * 64}"),
        ("6" * 16, "7" * 16, f"file:{'8' * 91}"),
        ("6" * 32, "7" * 32, f"manifest:{'8' * 87}"),
    ],
)
def test_accepts_current_source_hash_lengths_and_shard_forms(
    tmp_path: Path,
    source_hash: str,
    source_version: str,
    source_shard: str,
) -> None:
    result = _bind(
        _admitted_bundle(tmp_path),
        _source_input(
            source_identity_hash=source_hash,
            source_file_version_id=source_version,
            source_shard_id=source_shard,
        ),
    )

    assert result.coordinates.source_identity_hash == source_hash
    assert result.coordinates.source_file_version_id == source_version
    assert result.coordinates.source_shard_id == source_shard


def test_accepts_deferred_raw_container_identity(tmp_path: Path) -> None:
    result = _bind(
        _admitted_bundle(tmp_path),
        _source_input(
            physical_identity=SharedPhysicalArtifactIdentity(
                "in-network", "raw_container_sha256_v1", RAW_SHA256
            ),
            logical_json_sha256=None,
            logical_hash_deferred=True,
        ),
    )

    assert result.coordinates.logical_hash_deferred is True
    assert result.coordinates.logical_json_sha256 is None


@pytest.mark.parametrize(
    ("field", "coordinate"), [("import_run_id", "x" * 96), ("snapshot_id", "é" * 48)]
)
def test_accepts_exact_96_byte_run_coordinates(
    tmp_path: Path,
    field: str,
    coordinate: str,
) -> None:
    result = _bind(
        _admitted_bundle(tmp_path),
        _source_input(**{field: coordinate}),
    )

    assert getattr(result.coordinates, field) == coordinate


def test_every_bound_coordinate_changes_the_digest(tmp_path: Path) -> None:
    bundle = _admitted_bundle(tmp_path, directory_name="baseline")
    baseline_source = _source_input()
    baseline = _bind(bundle, baseline_source)
    changed_bundle = _admitted_bundle(
        tmp_path, directory_name="changed-bundle", payload_seed=37
    )
    source_variants = [
        {
            "physical_identity": SharedPhysicalArtifactIdentity(
                "allowed-amount", "logical_json_sha256_v1", LOGICAL_SHA256
            )
        },
        {
            "physical_identity": SharedPhysicalArtifactIdentity(
                "in-network", "logical_json_sha256_v1", "6" * 64
            ),
            "logical_json_sha256": "6" * 64,
        },
        {"source_identity_hash": "7" * 16},
        {"source_file_version_id": "8" * 16},
        {"raw_container_sha256": "9" * 64},
        {
            "physical_identity": SharedPhysicalArtifactIdentity(
                "in-network", "logical_json_sha256_v1", "a" * 64
            ),
            "logical_json_sha256": "a" * 64,
        },
        {
            "physical_identity": SharedPhysicalArtifactIdentity(
                "in-network", "raw_container_sha256_v1", RAW_SHA256
            ),
            "logical_json_sha256": None,
            "logical_hash_deferred": True,
        },
        {"source_shard_id": "file:18"},
        {"source_run_contract_sha256": "b" * 64},
        {"import_run_id": "ptg2:other-import"},
        {"snapshot_id": "ptg2:209902:synthetic"},
    ]

    assert (
        _bind(changed_bundle, baseline_source).binding_sha256 != baseline.binding_sha256
    )
    for variant_by_name in source_variants:
        assert (
            _bind(bundle, _source_input(**variant_by_name)).binding_sha256
            != baseline.binding_sha256
        )


@pytest.mark.parametrize(
    ("field", "invalid_value"),
    [
        ("source_type", None),
        ("source_type", "IN-NETWORK"),
        ("source_type", "a" * 65),
        ("physical_identity_kind", []),
        ("physical_identity_kind", "unknown"),
        ("physical_identity_sha256", "A" * 64),
        ("physical_identity_sha256", "a" * 63),
        ("source_identity_hash", None),
        ("source_identity_hash", "a" * 15),
        ("source_identity_hash", "Z" * 16),
        ("source_file_version_id", "a" * 64),
        ("source_file_version_id", "A" * 16),
        ("raw_container_sha256", "not-a-digest"),
        ("logical_hash_deferred", 1),
        ("source_shard_id", None),
        ("source_shard_id", "manifest:ABCDEF0123456789"),
        ("source_shard_id", "other:17"),
        ("source_shard_id", f"file:{'8' * 92}"),
        ("source_shard_id", f"manifest:{'8' * 88}"),
        ("source_run_contract_sha256", "a" * 32),
        ("import_run_id", None),
        ("import_run_id", ""),
        ("import_run_id", " surrounded "),
        ("import_run_id", "x" * 97),
        ("import_run_id", "é" * 49),
        ("import_run_id", "run\nvalue"),
        ("import_run_id", "run\u0085value"),
        ("import_run_id", "run\u009fvalue"),
        ("import_run_id", "run\ud800value"),
        ("snapshot_id", "snapshot\x00value"),
        ("snapshot_id", "x" * 97),
    ],
)
def test_direct_coordinates_reject_invalid_fields(
    field: str,
    invalid_value: object,
) -> None:
    coordinates_by_name = _coordinate_fields(**{field: invalid_value})

    with _binding_error():
        binding.TaxIdentityShadowSourceCoordinates(**coordinates_by_name)


@pytest.mark.parametrize(
    ("identity_kind", "physical_sha256", "logical_sha256", "is_deferred"),
    [
        ("logical_json_sha256_v1", LOGICAL_SHA256, None, False),
        ("raw_container_sha256_v1", LOGICAL_SHA256, LOGICAL_SHA256, False),
        ("logical_json_sha256_v1", "a" * 64, LOGICAL_SHA256, False),
        ("raw_container_sha256_v1", RAW_SHA256, LOGICAL_SHA256, True),
        ("logical_json_sha256_v1", RAW_SHA256, None, True),
        ("raw_container_sha256_v1", "b" * 64, None, True),
    ],
)
def test_direct_coordinates_reject_logical_identity_forgery(
    identity_kind: str,
    physical_sha256: str,
    logical_sha256: str | None,
    is_deferred: bool,
) -> None:
    with _binding_error():
        binding.TaxIdentityShadowSourceCoordinates(
            **_coordinate_fields(
                physical_identity_kind=identity_kind,
                physical_identity_sha256=physical_sha256,
                logical_json_sha256=logical_sha256,
                logical_hash_deferred=is_deferred,
            )
        )


def test_source_input_requires_untampered_physical_identity(tmp_path: Path) -> None:
    tampered = SharedPhysicalArtifactIdentity(
        "in-network", "logical_json_sha256_v1", LOGICAL_SHA256
    )
    object.__setattr__(tampered, "identity_kind", [])
    missing_slot = SharedPhysicalArtifactIdentity(
        "in-network", "logical_json_sha256_v1", LOGICAL_SHA256
    )
    object.__delattr__(missing_slot, "source_type")
    invalid_identities: list[object] = [SimpleNamespace(), tampered, missing_slot]

    for invalid_identity in invalid_identities:
        with _binding_error():
            _source_input(physical_identity=invalid_identity)

    source_binding_input = _source_input()
    with pytest.raises(FrozenInstanceError):
        source_binding_input.import_run_id = "other"
    assert SOURCE_IDENTITY_HASH not in repr(source_binding_input)
    object.__delattr__(source_binding_input, "snapshot_id")
    with _binding_error():
        _bind(_admitted_bundle(tmp_path), source_binding_input)


@pytest.mark.parametrize(
    ("field", "invalid_value"),
    [
        ("shadow_bundle_binding_sha256", None),
        ("shadow_bundle_binding_sha256", "A" * 64),
        ("coordinates", SimpleNamespace()),
        ("binding_sha256", "a" * 63),
        ("binding_sha256", "f" * 64),
    ],
)
def test_direct_bound_descriptor_rejects_forgery(
    tmp_path: Path, field: str, invalid_value: object
) -> None:
    admitted = _bind(_admitted_bundle(tmp_path), _source_input())
    descriptor_field_by_name: dict[str, object] = {
        "shadow_bundle_binding_sha256": admitted.shadow_bundle_binding_sha256,
        "coordinates": admitted.coordinates,
        "binding_sha256": admitted.binding_sha256,
    }
    descriptor_field_by_name[field] = invalid_value

    with _binding_error():
        binding.BoundTaxIdentityShadowBundleDescriptor(**descriptor_field_by_name)


def test_direct_bound_descriptor_accepts_only_matching_digest(tmp_path: Path) -> None:
    admitted = _bind(_admitted_bundle(tmp_path), _source_input())

    reconstructed = binding.BoundTaxIdentityShadowBundleDescriptor(
        shadow_bundle_binding_sha256=admitted.shadow_bundle_binding_sha256,
        coordinates=admitted.coordinates,
        binding_sha256=admitted.binding_sha256,
    )

    assert reconstructed == admitted
    assert reconstructed.coordinates is not admitted.coordinates


@pytest.mark.parametrize(
    ("field", "invalid_value"),
    [
        ("contract", "other"),
        ("shadow_state", "ACTIVE"),
        ("projection_authority", "v2"),
        ("publication_enabled", True),
        ("binding_sha256", "A" * 64),
        ("binding_sha256", "0" * 64),
    ],
)
def test_binder_rejects_tampered_admission_descriptor(
    tmp_path: Path, field: str, invalid_value: object
) -> None:
    shadow_bundle = _admitted_bundle(tmp_path)
    object.__setattr__(shadow_bundle, field, invalid_value)

    with _binding_error():
        _bind(shadow_bundle, _source_input())


@pytest.mark.parametrize("artifact_name", ["v1", "v2"])
def test_binder_rejects_direct_constructed_artifact_substitutes(
    tmp_path: Path, artifact_name: str
) -> None:
    admitted = _admitted_bundle(tmp_path)
    forged = admission.TaxIdentityShadowBundleDescriptor(
        v1=SimpleNamespace() if artifact_name == "v1" else admitted.v1,
        v2=SimpleNamespace() if artifact_name == "v2" else admitted.v2,
        binding_sha256=admitted.binding_sha256,
    )
    with _binding_error():
        _bind(forged, _source_input())


@pytest.mark.parametrize(
    ("artifact_name", "field", "invalid_value"),
    [
        ("v1", "sha256", "A" * 64),
        ("v2", "sha256", "0" * 64),
        ("v1", "record_bytes", True),
        ("v2", "provider_group_count", 0),
    ],
)
def test_binder_rejects_artifact_digest_and_scalar_tampering(
    tmp_path: Path, artifact_name: str, field: str, invalid_value: object
) -> None:
    admitted = _admitted_bundle(tmp_path)
    object.__setattr__(getattr(admitted, artifact_name), field, invalid_value)
    with _binding_error():
        _bind(admitted, _source_input())


@pytest.mark.parametrize(
    ("artifact_name", "field", "invalid_value"),
    [
        ("v2", "state_counts", SimpleNamespace()),
        ("v2", "missing", -1),
        ("v1", "matched_npi", 1),
    ],
)
def test_binder_rejects_nested_state_count_tampering(
    tmp_path: Path, artifact_name: str, field: str, invalid_value: object
) -> None:
    admitted = _admitted_bundle(tmp_path)
    artifact = getattr(admitted, artifact_name)
    owner = artifact if field == "state_counts" else artifact.state_counts
    object.__setattr__(owner, field, invalid_value)
    with _binding_error():
        _bind(admitted, _source_input())


def test_bound_descriptor_revalidates_recomputed_digest_coordinates(
    tmp_path: Path,
) -> None:
    admitted = _bind(_admitted_bundle(tmp_path), _source_input())
    coordinates = admitted.coordinates
    object.__setattr__(coordinates, "logical_hash_deferred", True)
    recomputed = binding._source_binding_sha256(
        admitted.shadow_bundle_binding_sha256, coordinates
    )
    with _binding_error():
        binding.BoundTaxIdentityShadowBundleDescriptor(
            admitted.shadow_bundle_binding_sha256, coordinates, recomputed
        )
