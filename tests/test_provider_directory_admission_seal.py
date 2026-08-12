# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Contracts for fixed-size Provider Directory dataset admission receipts."""

from __future__ import annotations

import hashlib
import importlib
import json
from pathlib import Path
import struct
import subprocess
import sys

import pytest

from process.provider_directory_admission_seal import (
    ADMISSION_GENERIC_PROOF_SUMMARY_KEY,
    ADMISSION_METADATA_SUMMARY_MAX_BYTES,
    AdmissionSealError,
    admission_seal_from_validated_metadata,
    validate_generic_admission_copy,
)
from process.provider_directory_fhir_subset_canonical import (
    canonical_payload_sha256,
)
importer = importlib.import_module("process.provider_directory_fhir")
from tests.test_provider_directory_dataset_selection_bounded_db import (
    _large_metadata_by_field,
    _proof_line_hash,
)
from tests.uhc_final_publication_test_support import final_publication_fixture


_COPY_SIGNATURE = b"PGCOPY\n\xff\r\n\x00"


def _binary_copy(metadata: dict[str, object], path: Path) -> None:
    payload = json.dumps(
        metadata,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode()
    _binary_copy_payload(payload, path)


def _binary_copy_payload(payload: bytes, path: Path) -> None:
    path.write_bytes(
        _COPY_SIGNATURE
        + struct.pack("!ii", 0, 0)
        + struct.pack("!h", 1)
        + struct.pack("!i", len(payload))
        + payload
        + struct.pack("!h", -1)
    )


def test_validated_metadata_receipt_excludes_raw_proof_and_binds_summary():
    metadata = _large_metadata_by_field(2)
    metadata["synthetic_parser_boundary"] = {
        "escaped": "quote\" slash\\ line\n tab\t",
        "nested": [None, True, False, 0, -0.0, 1.25, "žluťoučký"],
    }

    receipt = admission_seal_from_validated_metadata(metadata)

    assert receipt is not None
    assert receipt.admission_version == 1
    assert receipt.admission_kind == "generic"
    assert receipt.resource_types == ("Location",)
    assert "provider_directory_content_proof_v1" not in receipt.metadata_summary
    assert receipt.metadata_summary[ADMISSION_GENERIC_PROOF_SUMMARY_KEY] == {
        key: metadata["provider_directory_content_proof_v1"][key]
        for key in (
            "dataset_hash",
            "resource_count",
            "resource_hashes",
            "resource_counts",
        )
    }
    assert receipt.metadata_summary["synthetic_parser_boundary"] == (
        metadata["synthetic_parser_boundary"]
    )
    assert receipt.metadata_sha256 == canonical_payload_sha256(
        receipt.digest_envelope()
    )


def test_receipt_rejects_overlapping_proofs_and_unbounded_summary():
    metadata = _large_metadata_by_field(1)
    metadata["uhc_canonical_content_proof_v1"] = {"proof_sha256": "a" * 64}
    with pytest.raises(AdmissionSealError, match="proof_kind"):
        admission_seal_from_validated_metadata(metadata)

    metadata = _large_metadata_by_field(1)
    metadata["oversized"] = "x" * ADMISSION_METADATA_SUMMARY_MAX_BYTES
    with pytest.raises(AdmissionSealError, match="metadata_summary"):
        admission_seal_from_validated_metadata(metadata)

    metadata = _large_metadata_by_field(1)
    metadata[ADMISSION_GENERIC_PROOF_SUMMARY_KEY] = {"forged": True}
    with pytest.raises(AdmissionSealError, match="reserved_metadata_key"):
        admission_seal_from_validated_metadata(metadata)


def test_streaming_copy_revalidates_complete_generic_proof(tmp_path: Path):
    metadata = _large_metadata_by_field(32)
    copy_path = tmp_path / "metadata.copy"
    _binary_copy(metadata, copy_path)

    receipt = validate_generic_admission_copy(
        copy_path,
        dataset_id="dataset_shared",
        endpoint_id="endpoint_shared",
        evidence_run_id="root-shared",
        dataset_hash="e" * 64,
        resource_count=32,
        scratch_directory=tmp_path,
    )

    assert receipt.admission_kind == "generic"
    assert receipt.resource_types == ("Location",)
    expected_summary = {
        key: value
        for key, value in metadata.items()
        if key != "provider_directory_content_proof_v1"
    }
    expected_summary[ADMISSION_GENERIC_PROOF_SUMMARY_KEY] = {
        key: metadata["provider_directory_content_proof_v1"][key]
        for key in (
            "dataset_hash",
            "resource_count",
            "resource_hashes",
            "resource_counts",
        )
    }
    assert receipt.metadata_summary == expected_summary


def test_streaming_copy_preserves_exact_large_integral_numbers(tmp_path: Path):
    receipts = []
    for index, raw_number in enumerate(
        ("9007199254740992.0", "9007199254740993.0")
    ):
        metadata = _large_metadata_by_field(2)
        payload = json.dumps(metadata, separators=(",", ":")).encode()
        payload = payload[:-1] + b',"exact_number":' + raw_number.encode() + b"}"
        copy_path = tmp_path / f"metadata-{index}.copy"
        _binary_copy_payload(payload, copy_path)
        receipts.append(
            validate_generic_admission_copy(
                copy_path,
                dataset_id="dataset_shared",
                endpoint_id="endpoint_shared",
                evidence_run_id="root-shared",
                dataset_hash="e" * 64,
                resource_count=2,
                scratch_directory=tmp_path,
            )
        )

    assert receipts[0].metadata_summary["exact_number"] == 9007199254740992
    assert receipts[1].metadata_summary["exact_number"] == 9007199254740993
    assert receipts[0].metadata_sha256 != receipts[1].metadata_sha256


def test_streaming_copy_rejects_boolean_parent_resource_count(tmp_path: Path):
    metadata = _large_metadata_by_field(1)
    metadata["resource_count"] = True
    copy_path = tmp_path / "boolean-parent-count.copy"
    _binary_copy(metadata, copy_path)

    with pytest.raises(AdmissionSealError, match="parent_identity_invalid"):
        validate_generic_admission_copy(
            copy_path,
            dataset_id="dataset_shared",
            endpoint_id="endpoint_shared",
            evidence_run_id="root-shared",
            dataset_hash="e" * 64,
            resource_count=1,
            scratch_directory=tmp_path,
        )


def test_streaming_copy_rejects_inexact_decimal_and_reserved_key(
    tmp_path: Path,
):
    metadata = _large_metadata_by_field(2)
    payload = json.dumps(metadata, separators=(",", ":")).encode()
    payload = (
        payload[:-1]
        + b',"inexact":1.00000000000000000000000000001}'
    )
    copy_path = tmp_path / "inexact.copy"
    _binary_copy_payload(payload, copy_path)
    with pytest.raises(AdmissionSealError, match="number_invalid"):
        validate_generic_admission_copy(
            copy_path,
            dataset_id="dataset_shared",
            endpoint_id="endpoint_shared",
            evidence_run_id="root-shared",
            dataset_hash="e" * 64,
            resource_count=2,
            scratch_directory=tmp_path,
        )

    metadata = _large_metadata_by_field(2)
    metadata[ADMISSION_GENERIC_PROOF_SUMMARY_KEY] = {"forged": True}
    reserved_path = tmp_path / "reserved.copy"
    _binary_copy(metadata, reserved_path)
    with pytest.raises(AdmissionSealError, match="reserved_metadata_key"):
        validate_generic_admission_copy(
            reserved_path,
            dataset_id="dataset_shared",
            endpoint_id="endpoint_shared",
            evidence_run_id="root-shared",
            dataset_hash="e" * 64,
            resource_count=2,
            scratch_directory=tmp_path,
        )
    assert sorted(path.name for path in tmp_path.iterdir()) == [
        "inexact.copy",
        "reserved.copy",
    ]


@pytest.mark.parametrize(
    ("needle", "replacement"),
    [
        (b'"resource_count":2,', b'"resource_count":2.0,'),
        (b'"artifact_byte_count":1}', b'"artifact_byte_count":1.0}'),
    ],
)
def test_streaming_copy_rejects_decimal_proof_integers(
    tmp_path: Path,
    needle: bytes,
    replacement: bytes,
):
    metadata = _large_metadata_by_field(2)
    payload = json.dumps(metadata, separators=(",", ":")).encode()
    assert needle in payload
    payload = payload.replace(needle, replacement, 1)
    copy_path = tmp_path / "decimal-proof.copy"
    _binary_copy_payload(payload, copy_path)

    with pytest.raises(AdmissionSealError, match="proof_shape"):
        validate_generic_admission_copy(
            copy_path,
            dataset_id="dataset_shared",
            endpoint_id="endpoint_shared",
            evidence_run_id="root-shared",
            dataset_hash="e" * 64,
            resource_count=2,
            scratch_directory=tmp_path,
        )


def test_streaming_copy_rejects_oversized_integer_without_signal(
    tmp_path: Path,
):
    metadata = _large_metadata_by_field(2)
    payload = json.dumps(metadata, separators=(",", ":")).encode()
    payload = payload[:-1] + b',"oversized_integer":' + b"9" * 4301 + b"}"
    copy_path = tmp_path / "oversized-integer.copy"
    _binary_copy_payload(payload, copy_path)
    probe = """
import sys
from pathlib import Path
from process.provider_directory_admission_seal import (
    AdmissionSealError,
    validate_generic_admission_copy,
)
try:
    validate_generic_admission_copy(
        Path(sys.argv[1]),
        dataset_id="dataset_shared",
        endpoint_id="endpoint_shared",
        evidence_run_id="root-shared",
        dataset_hash="e" * 64,
        resource_count=2,
        scratch_directory=Path(sys.argv[2]),
    )
except AdmissionSealError:
    raise SystemExit(0)
raise SystemExit(3)
"""

    completed = subprocess.run(
        [sys.executable, "-c", probe, str(copy_path), str(tmp_path)],
        cwd=Path(__file__).parents[1],
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0, completed.stderr
    assert list(tmp_path.iterdir()) == [copy_path]


def test_streaming_copy_rejects_proof_and_root_growth_early(tmp_path: Path):
    metadata = _large_metadata_by_field(2)
    proof = metadata["provider_directory_content_proof_v1"]
    assert isinstance(proof, dict)
    proof["unexpected"] = "x"
    copy_path = tmp_path / "unknown-proof-key.copy"
    _binary_copy(metadata, copy_path)
    with pytest.raises(AdmissionSealError, match="proof_keyset"):
        validate_generic_admission_copy(
            copy_path,
            dataset_id="dataset_shared",
            endpoint_id="endpoint_shared",
            evidence_run_id="root-shared",
            dataset_hash="e" * 64,
            resource_count=2,
            scratch_directory=tmp_path,
        )

    metadata = _large_metadata_by_field(2)
    proof = metadata["provider_directory_content_proof_v1"]
    assert isinstance(proof, dict)
    shards = proof["shards"]
    assert isinstance(shards, list) and isinstance(shards[0], dict)
    shards[0]["resource_count"] = 65
    shards[0]["resource_counts"] = {
        f"Synthetic{index}": 1 for index in range(65)
    }
    type_path = tmp_path / "resource-types.copy"
    _binary_copy(metadata, type_path)
    with pytest.raises(AdmissionSealError, match="resource_types"):
        validate_generic_admission_copy(
            type_path,
            dataset_id="dataset_shared",
            endpoint_id="endpoint_shared",
            evidence_run_id="root-shared",
            dataset_hash="e" * 64,
            resource_count=65,
            scratch_directory=tmp_path,
        )

    metadata = _large_metadata_by_field(2)
    metadata.update(
        {f"synthetic_{index}": "x" * 1000 for index in range(1100)}
    )
    root_path = tmp_path / "root-summary.copy"
    _binary_copy(metadata, root_path)
    with pytest.raises(AdmissionSealError, match="metadata_summary_unbounded"):
        validate_generic_admission_copy(
            root_path,
            dataset_id="dataset_shared",
            endpoint_id="endpoint_shared",
            evidence_run_id="root-shared",
            dataset_hash="e" * 64,
            resource_count=2,
            scratch_directory=tmp_path,
        )
    assert sorted(path.suffix for path in tmp_path.iterdir()) == [
        ".copy",
        ".copy",
        ".copy",
    ]


def test_streaming_copy_rejects_resealed_descriptor_aggregate_drift(
    tmp_path: Path,
):
    metadata = _large_metadata_by_field(2)
    proof = metadata["provider_directory_content_proof_v1"]
    assert isinstance(proof, dict)
    proof["contract_id"] = importer.PROVIDER_DIRECTORY_CONTENT_PROOF_CONTRACT_ID
    for semantic_field in (
        "proof_resource_scope",
        "resource_hash_contract",
        "semantic_projection_as_of",
        "semantic_union",
    ):
        proof.pop(semantic_field)
    metadata.pop("proof_resource_scope")
    metadata.pop("semantic_projection_as_of")
    metadata["resource_hash_contract"] = importer.LEGACY_RESOURCE_HASH_CONTRACT
    shards = proof["shards"]
    assert isinstance(shards, list)
    assert isinstance(shards[0], dict)
    shards[0]["resource_count"] = 2
    proof["shard_set_sha256"] = _proof_line_hash(shards)
    unsigned = dict(proof)
    unsigned.pop("proof_sha256")
    proof["proof_sha256"] = hashlib.sha256(
        json.dumps(unsigned, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    copy_path = tmp_path / "metadata.copy"
    _binary_copy(metadata, copy_path)

    with pytest.raises(AdmissionSealError, match="shard"):
        validate_generic_admission_copy(
            copy_path,
            dataset_id="dataset_shared",
            endpoint_id="endpoint_shared",
            evidence_run_id="root-shared",
            dataset_hash="e" * 64,
            resource_count=2,
            scratch_directory=tmp_path,
        )


def test_streaming_copy_rejects_resealed_legacy_resource_scope_drift(
    tmp_path: Path,
):
    """Keep every header and shard family inside the selected legacy scope."""

    metadata = _large_metadata_by_field(2)
    proof = metadata["provider_directory_content_proof_v1"]
    assert isinstance(proof, dict)
    shards = proof["shards"]
    assert isinstance(shards, list)
    for shard in shards:
        assert isinstance(shard, dict)
        shard["resource_count"] = 2
        shard["resource_counts"] = {"Location": 1, "Practitioner": 1}
    proof["resource_count"] = 4
    proof["resource_counts"] = {"Location": 2, "Practitioner": 2}
    proof["resource_hashes"]["Practitioner"] = "b" * 64
    proof["shard_set_sha256"] = _proof_line_hash(shards)
    unsigned = dict(proof)
    unsigned.pop("proof_sha256")
    proof["proof_sha256"] = hashlib.sha256(
        json.dumps(unsigned, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    copy_path = tmp_path / "metadata.copy"
    _binary_copy(metadata, copy_path)

    with pytest.raises(AdmissionSealError, match="lineage|resource scope"):
        validate_generic_admission_copy(
            copy_path,
            dataset_id="dataset_shared",
            endpoint_id="endpoint_shared",
            evidence_run_id="root-shared",
            dataset_hash="e" * 64,
            resource_count=4,
            scratch_directory=tmp_path,
        )


@pytest.mark.parametrize(
    ("field_name", "invalid_scope"),
    [
        ("source_ids", {"source_primary": True, "source_sibling": True}),
        ("selected_resources", "Location"),
        ("proof_resource_scope", {"Location": True}),
    ],
)
def test_streaming_copy_requires_array_root_scopes(
    tmp_path: Path,
    field_name: str,
    invalid_scope: object,
):
    metadata = _large_metadata_by_field(2)
    metadata[field_name] = invalid_scope
    copy_path = tmp_path / "metadata.copy"
    _binary_copy(metadata, copy_path)

    with pytest.raises(AdmissionSealError, match="lineage"):
        validate_generic_admission_copy(
            copy_path,
            dataset_id="dataset_shared",
            endpoint_id="endpoint_shared",
            evidence_run_id="root-shared",
            dataset_hash="e" * 64,
            resource_count=2,
            scratch_directory=tmp_path,
        )


@pytest.mark.parametrize("invalid_item", [1, True, None])
@pytest.mark.parametrize(
    "field_name",
    ["source_ids", "selected_resources", "proof_resource_scope"],
)
def test_streaming_copy_requires_string_root_scope_items(
    tmp_path: Path,
    field_name: str,
    invalid_item: object,
):
    metadata = _large_metadata_by_field(2)
    metadata[field_name] = [invalid_item]
    copy_path = tmp_path / "metadata.copy"
    _binary_copy(metadata, copy_path)

    with pytest.raises(AdmissionSealError, match="lineage"):
        validate_generic_admission_copy(
            copy_path,
            dataset_id="dataset_shared",
            endpoint_id="endpoint_shared",
            evidence_run_id="root-shared",
            dataset_hash="e" * 64,
            resource_count=2,
            scratch_directory=tmp_path,
        )


def test_streaming_copy_rejects_explicit_null_hash_contract(tmp_path: Path):
    metadata = _large_metadata_by_field(2)
    metadata["resource_hash_contract"] = None
    copy_path = tmp_path / "metadata.copy"
    _binary_copy(metadata, copy_path)

    with pytest.raises(AdmissionSealError, match="contract"):
        validate_generic_admission_copy(
            copy_path,
            dataset_id="dataset_shared",
            endpoint_id="endpoint_shared",
            evidence_run_id="root-shared",
            dataset_hash="e" * 64,
            resource_count=2,
            scratch_directory=tmp_path,
        )


@pytest.mark.parametrize(
    "root_scope",
    [
        [" Location ", "Location"],
        ["Location", "Location"],
        ["Practitioner", "Location"],
    ],
)
def test_streaming_copy_requires_exact_semantic_root_proof_scope(
    tmp_path: Path,
    root_scope: list[str],
):
    metadata = _large_metadata_by_field(2)
    metadata["proof_resource_scope"] = root_scope
    copy_path = tmp_path / "metadata.copy"
    _binary_copy(metadata, copy_path)

    with pytest.raises(AdmissionSealError, match="lineage|resource scope"):
        validate_generic_admission_copy(
            copy_path,
            dataset_id="dataset_shared",
            endpoint_id="endpoint_shared",
            evidence_run_id="root-shared",
            dataset_hash="e" * 64,
            resource_count=2,
            scratch_directory=tmp_path,
        )


def test_streaming_copy_rejects_non_ascii_proof_value(tmp_path: Path):
    metadata = _large_metadata_by_field(
        2,
        source_id_list=["sourcé"],
    )
    copy_path = tmp_path / "metadata.copy"
    _binary_copy(metadata, copy_path)

    with pytest.raises(AdmissionSealError, match="non_ascii"):
        validate_generic_admission_copy(
            copy_path,
            dataset_id="dataset_shared",
            endpoint_id="endpoint_shared",
            evidence_run_id="root-shared",
            dataset_hash="e" * 64,
            resource_count=2,
            scratch_directory=tmp_path,
        )


@pytest.mark.parametrize(
    ("field_name", "dirty_scope"),
    [
        ("selected_resources", [" Location ", "Location"]),
        ("selected_resources", ["Location", "Location"]),
        ("source_ids", [" source_primary ", "source_primary"]),
        ("source_ids", ["source_primary", "source_primary"]),
    ],
)
def test_streaming_copy_rejects_noncanonical_root_lineage(
    tmp_path: Path,
    field_name: str,
    dirty_scope: list[str],
):
    metadata = _large_metadata_by_field(2)
    metadata[field_name] = dirty_scope
    copy_path = tmp_path / "metadata.copy"
    _binary_copy(metadata, copy_path)

    with pytest.raises(AdmissionSealError, match="lineage"):
        validate_generic_admission_copy(
            copy_path,
            dataset_id="dataset_shared",
            endpoint_id="endpoint_shared",
            evidence_run_id="root-shared",
            dataset_hash="e" * 64,
            resource_count=2,
            scratch_directory=tmp_path,
        )


def test_receipt_digest_preserves_large_integer_identity():
    first = 10000000000000000000000000001
    second = first + 1

    assert canonical_payload_sha256({"value": first}) != (
        canonical_payload_sha256({"value": second})
    )
    assert canonical_payload_sha256({"value": 10**4300})


def test_streaming_copy_accepts_zero_count_scoped_resource(tmp_path: Path):
    metadata = _large_metadata_by_field(2)
    metadata["proof_resource_scope"] = ["Location", "Practitioner"]
    proof = metadata["provider_directory_content_proof_v1"]
    assert isinstance(proof, dict)
    proof["proof_resource_scope"] = ["Location", "Practitioner"]
    proof["resource_counts"] = {"Location": 2, "Practitioner": 0}
    proof["resource_hashes"] = {
        "Location": "f" * 64,
        "Practitioner": hashlib.sha256(b"").hexdigest(),
    }
    unsigned = dict(proof)
    unsigned.pop("proof_sha256")
    proof["proof_sha256"] = hashlib.sha256(
        json.dumps(unsigned, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    copy_path = tmp_path / "metadata.copy"
    _binary_copy(metadata, copy_path)

    receipt = validate_generic_admission_copy(
        copy_path,
        dataset_id="dataset_shared",
        endpoint_id="endpoint_shared",
        evidence_run_id="root-shared",
        dataset_hash="e" * 64,
        resource_count=2,
        scratch_directory=tmp_path,
    )

    assert receipt.resource_types == ("Location", "Practitioner")


def test_streaming_copy_rejects_unbounded_tiny_item_capture_and_cleans_spool(
    tmp_path: Path,
):
    copy_path = tmp_path / "metadata.copy"
    payload = b'{"oversized":[' + (b"0," * (1024 * 1024 + 1)) + b"0]}"
    _binary_copy_payload(payload, copy_path)

    with pytest.raises(AdmissionSealError, match="capture_unbounded"):
        validate_generic_admission_copy(
            copy_path,
            dataset_id="dataset_shared",
            endpoint_id="endpoint_shared",
            evidence_run_id="root-shared",
            dataset_hash="e" * 64,
            resource_count=2,
            scratch_directory=tmp_path,
        )
    assert list(tmp_path.iterdir()) == [copy_path]


@pytest.mark.parametrize(
    ("field_name", "field_value"),
    [
        ("proof_resource_scope", None),
        ("semantic_projection_as_of", None),
    ],
)
def test_streaming_copy_ignores_legacy_semantic_parent_key_presence(
    tmp_path: Path,
    field_name: str,
    field_value: object,
):
    metadata = _large_metadata_by_field(2)
    proof = metadata["provider_directory_content_proof_v1"]
    assert isinstance(proof, dict)
    proof["contract_id"] = importer.PROVIDER_DIRECTORY_CONTENT_PROOF_CONTRACT_ID
    for semantic_field in (
        "proof_resource_scope",
        "resource_hash_contract",
        "semantic_projection_as_of",
        "semantic_union",
    ):
        proof.pop(semantic_field)
    metadata.pop("proof_resource_scope")
    metadata.pop("semantic_projection_as_of")
    metadata["resource_hash_contract"] = importer.LEGACY_RESOURCE_HASH_CONTRACT
    metadata[field_name] = field_value
    unsigned = dict(proof)
    unsigned.pop("proof_sha256")
    proof["proof_sha256"] = hashlib.sha256(
        json.dumps(unsigned, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    copy_path = tmp_path / "metadata.copy"
    _binary_copy(metadata, copy_path)

    receipt = validate_generic_admission_copy(
        copy_path,
        dataset_id="dataset_shared",
        endpoint_id="endpoint_shared",
        evidence_run_id="root-shared",
        dataset_hash="e" * 64,
        resource_count=2,
        scratch_directory=tmp_path,
    )

    assert receipt.admission_kind == "generic"


def test_streaming_copy_binds_optional_parent_root_run_id(tmp_path: Path):
    metadata = _large_metadata_by_field(2)
    metadata["acquisition_root_run_id"] = "root-conflict"
    copy_path = tmp_path / "metadata.copy"
    _binary_copy(metadata, copy_path)

    with pytest.raises(AdmissionSealError, match="parent_identity"):
        validate_generic_admission_copy(
            copy_path,
            dataset_id="dataset_shared",
            endpoint_id="endpoint_shared",
            evidence_run_id="root-shared",
            dataset_hash="e" * 64,
            resource_count=2,
            scratch_directory=tmp_path,
        )


@pytest.mark.parametrize(("flags", "extension_length"), [(1, 0), (0, 1)])
def test_streaming_copy_requires_exact_private_copy_header(
    tmp_path: Path,
    flags: int,
    extension_length: int,
):
    metadata = _large_metadata_by_field(2)
    payload = json.dumps(metadata, separators=(",", ":")).encode()
    copy_path = tmp_path / "metadata.copy"
    copy_path.write_bytes(
        _COPY_SIGNATURE
        + struct.pack("!ii", flags, extension_length)
        + struct.pack("!h", 1)
        + struct.pack("!i", len(payload))
        + payload
        + struct.pack("!h", -1)
    )

    with pytest.raises(AdmissionSealError, match="copy_header"):
        validate_generic_admission_copy(
            copy_path,
            dataset_id="dataset_shared",
            endpoint_id="endpoint_shared",
            evidence_run_id="root-shared",
            dataset_hash="e" * 64,
            resource_count=2,
            scratch_directory=tmp_path,
        )


def test_streaming_copy_validates_bounded_legacy_canonical_proof(tmp_path: Path):
    state, _expectation = final_publication_fixture(
        dataset_id="dataset_shared",
        endpoint_id="endpoint_shared",
        acquisition_root_run_id="root-shared",
    )
    metadata = state["publication_metadata_json"]
    copy_path = tmp_path / "metadata.copy"
    _binary_copy(metadata, copy_path)

    receipt = validate_generic_admission_copy(
        copy_path,
        dataset_id="dataset_shared",
        endpoint_id="endpoint_shared",
        evidence_run_id="root-shared",
        dataset_hash=state["dataset_hash"],
        resource_count=state["resource_count"],
        scratch_directory=tmp_path,
    )

    assert receipt.admission_kind == "uhc_canonical"
    assert receipt.proof_sha256 == metadata[
        "uhc_canonical_content_proof_v1"
    ]["proof_sha256"]
    assert "uhc_canonical_content_proof_v1" not in receipt.metadata_summary


@pytest.mark.parametrize(
    ("field_name", "changed_value"),
    [
        ("source_ids", ["tampered-source"]),
        ("selected_resources", ["Organization"]),
    ],
)
def test_streaming_copy_rejects_canonical_root_lineage_change(
    tmp_path: Path,
    field_name: str,
    changed_value: list[str],
):
    state, _expectation = final_publication_fixture(
        dataset_id="dataset_shared",
        endpoint_id="endpoint_shared",
        acquisition_root_run_id="root-shared",
    )
    metadata = state["publication_metadata_json"]
    metadata[field_name] = changed_value
    copy_path = tmp_path / "metadata.copy"
    _binary_copy(metadata, copy_path)

    with pytest.raises(AdmissionSealError, match="uhc_proof_invalid"):
        validate_generic_admission_copy(
            copy_path,
            dataset_id="dataset_shared",
            endpoint_id="endpoint_shared",
            evidence_run_id="root-shared",
            dataset_hash=state["dataset_hash"],
            resource_count=state["resource_count"],
            scratch_directory=tmp_path,
        )


def test_production_selection_prefers_fixed_receipts_and_caps_legacy_json():
    projection, publication_lateral = (
        importer._artifact_dataset_option_projection_parts(True)
    )
    candidate_ctes = importer._artifact_candidate_eligibility_ctes(
        "dataset_table"
    )
    fence_sql = importer._artifact_fence_dataset_rows_sql(for_update=False)

    for sql in (projection, publication_lateral, candidate_ctes, fence_sql):
        assert "publication_metadata_summary_json" in sql
        assert "content_proof_admission_version" in sql
    for sql in (publication_lateral, candidate_ctes, fence_sql):
        assert str(ADMISSION_METADATA_SUMMARY_MAX_BYTES) in sql
    assert "content_proof_resource_types" in projection
    reviewed_gate = importer._artifact_reviewed_candidate_eligibility_sql(
        "dataset_table",
        "source_table",
        metadata="dataset.eligibility_metadata_jsonb",
        content_proof_admitted="dataset.content_proof_admitted",
    )
    assert "provider_directory_content_proof_admission_summary_v1" in reviewed_gate
    assert "publication_metadata_sha256" in fence_sql


def test_terminal_writer_stores_complete_nullable_receipt_tuple():
    terminal_sql = importer._store_validated_endpoint_dataset_sql()

    for column_name in (
        "publication_metadata_summary_json",
        "publication_metadata_sha256",
        "content_proof_admission_version",
        "content_proof_admission_kind",
        "content_proof_admission_sha256",
        "content_proof_resource_types",
    ):
        assert f"{column_name} =" in terminal_sql
