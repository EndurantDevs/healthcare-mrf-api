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

from process import provider_directory_admission_seal as seal
from process.provider_directory_admission_seal import (
    ADMISSION_GENERIC_PROOF_SUMMARY_KEY,
    ADMISSION_METADATA_SUMMARY_MAX_BYTES,
    AdmissionSealError,
    admission_seal_from_validated_metadata,
    validate_generic_admission_copy,
)
from process.provider_directory_fhir_subset_canonical import canonical_payload_sha256
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


def _admit_with(**proof_updates: object):
    metadata = _large_metadata_by_field(1)
    assert isinstance(proof := metadata["provider_directory_content_proof_v1"], dict)
    proof.update(proof_updates)
    return admission_seal_from_validated_metadata(metadata)


@pytest.mark.parametrize(
    ("call", "error"),
    [
        (lambda: seal._require_ascii_canonical_json({1: None}), "proof_shape"),
        (lambda: seal._require_ascii_canonical_json(1.5), "proof_shape"),
        (lambda: seal._normalized_resource_types(None), "resource_types"),
        (lambda: seal._normalized_resource_types({"": 1}), "resource_types"),
        (lambda: seal._bounded_metadata_summary({"bad": object()}), "metadata_summary"),
        (lambda: _admit_with(dataset_hash="x"), "proof_summary"),
        (lambda: _admit_with(shards=[None]), "shard_summary"),
        (lambda: _admit_with(shards=[{}]), "shard_summary"),
        (lambda: _admit_with(shards=[]), "shard_summary"),
        (lambda: _admit_with(resource_count=2), "shard_summary"),
        (lambda: _admit_with(proof_sha256="x"), "proof_receipt"),
        (lambda: admission_seal_from_validated_metadata(None), "metadata_invalid"),
    ],
)
def test_validated_receipt_boundaries_fail_closed(call, error):
    """Reject malformed receipt fields at their owning validators."""
    with pytest.raises(AdmissionSealError, match=error):
        call()
    assert admission_seal_from_validated_metadata({}) is None


def test_streaming_copy_rejects_trailer_drift(tmp_path: Path):
    """Reject a missing terminator or bytes after the terminator."""
    copy_path = tmp_path / "metadata.copy"
    _binary_copy(_large_metadata_by_field(1), copy_path)
    valid_copy = copy_path.read_bytes()
    validation_by_field = {
        "dataset_id": "dataset_shared",
        "endpoint_id": "endpoint_shared",
        "evidence_run_id": "root-shared",
        "dataset_hash": "e" * 64,
        "resource_count": 1,
    }
    for invalid_copy in (valid_copy[:-2] + b"\0\0", valid_copy + b"x"):
        copy_path.write_bytes(invalid_copy)
        with pytest.raises(AdmissionSealError, match="copy_trailer_invalid"):
            validate_generic_admission_copy(copy_path, **validation_by_field)


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
    )

    assert receipt.admission_kind == "generic"
    assert receipt.resource_types == ("Location",)
    expected_summary_by_field = {
        key: metadata_value
        for key, metadata_value in metadata.items()
        if key != "provider_directory_content_proof_v1"
    }
    expected_summary_by_field[ADMISSION_GENERIC_PROOF_SUMMARY_KEY] = {
        key: metadata["provider_directory_content_proof_v1"][key]
        for key in (
            "dataset_hash",
            "resource_count",
            "resource_hashes",
            "resource_counts",
        )
    }
    assert receipt.metadata_summary == expected_summary_by_field


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
        )


def test_streaming_copy_rejects_inexact_decimal_and_reserved_key(
    tmp_path: Path,
):
    metadata = _large_metadata_by_field(2)
    raw_json = json.dumps(metadata, separators=(",", ":")).encode()
    raw_json = (
        raw_json[:-1]
        + b',"inexact":1.00000000000000000000000000001}'
    )
    copy_path = tmp_path / "inexact.copy"
    _binary_copy_payload(raw_json, copy_path)
    with pytest.raises(AdmissionSealError, match="number_invalid"):
        validate_generic_admission_copy(
            copy_path,
            dataset_id="dataset_shared",
            endpoint_id="endpoint_shared",
            evidence_run_id="root-shared",
            dataset_hash="e" * 64,
            resource_count=2,
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
        )


def test_streaming_copy_rejects_oversized_integer_without_signal(
    tmp_path: Path,
):
    metadata = _large_metadata_by_field(2)
    raw_json = json.dumps(metadata, separators=(",", ":")).encode()
    raw_json = raw_json[:-1] + b',"oversized_integer":' + b"9" * 4301 + b"}"
    copy_path = tmp_path / "oversized-integer.copy"
    _binary_copy_payload(raw_json, copy_path)
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


def test_streaming_copy_rejects_unknown_proof_key(tmp_path: Path):
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
        )
    assert list(tmp_path.iterdir()) == [copy_path]


def test_streaming_copy_rejects_resource_type_growth_early(tmp_path: Path):
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
        )
    assert list(tmp_path.iterdir()) == [type_path]


def test_streaming_copy_rejects_unbounded_root_summary_early(tmp_path: Path):
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
        )
    assert list(tmp_path.iterdir()) == [root_path]


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
    unsigned_proof_by_field = dict(proof)
    unsigned_proof_by_field.pop("proof_sha256")
    proof["proof_sha256"] = hashlib.sha256(
        json.dumps(
            unsigned_proof_by_field,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
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
    unsigned_proof_by_field = dict(proof)
    unsigned_proof_by_field.pop("proof_sha256")
    proof["proof_sha256"] = hashlib.sha256(
        json.dumps(
            unsigned_proof_by_field,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
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
        )
