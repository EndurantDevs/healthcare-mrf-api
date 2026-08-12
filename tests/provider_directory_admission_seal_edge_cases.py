# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Focused fail-closed branches for Provider Directory admission receipts."""

from __future__ import annotations

from contextlib import ExitStack
from decimal import Decimal
from pathlib import Path
import struct
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

import process.provider_directory_admission_seal as seal
import process.provider_directory_admission_stream as stream
import process.provider_directory_admission_validation as validation
from process.provider_directory_proof_store import (
    PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY,
    PROVIDER_DIRECTORY_PROOF_RESOURCE_SCOPE_METADATA_KEY,
)
from tests.test_provider_directory_admission_seal import _binary_copy_payload


def _descriptor(shard_id: str = "a" * 64, resource_type: str = "Location"):
    return {
        "shard_id": shard_id,
        "dataset_id": "dataset_shared",
        "endpoint_id": "endpoint_shared",
        "acquisition_root_run_id": "root-shared",
        "source_ids": ["source_primary"],
        "resource_count": 1,
        "resource_counts": {resource_type: 1},
        "first_identity": [resource_type, "first"],
        "last_identity": [resource_type, "last"],
        "input_sha256": "b" * 64,
        "artifact_sha256": "c" * 64,
        "artifact_byte_count": 1,
    }


def _copy_request(copy_path: Path, scratch_directory: Path):
    return stream._AdmissionCopyRequest(
        copy_path=copy_path,
        dataset_id="dataset_shared",
        endpoint_id="endpoint_shared",
        evidence_run_id="root-shared",
        dataset_hash="e" * 64,
        resource_count=1,
        scratch_directory=scratch_directory,
        expected_resource_hashes=None,
        expected_resource_counts=None,
    )


def _assert_invalid_aggregate_proofs() -> None:
    """Reject malformed descriptors and aggregate totals."""

    for proof_by_field in (
        {},
        {
            "shards": [None],
            "resource_count": 1,
            "resource_counts": {"Location": 1},
        },
        {
            "shards": [
                {
                    "resource_count": 1,
                    "resource_counts": {"Location": 1},
                }
            ],
            "resource_count": 2,
            "resource_counts": {"Location": 2},
        },
    ):
        with pytest.raises(seal.AdmissionSealError, match="shard_summary"):
            seal._require_exact_generic_descriptor_aggregates(proof_by_field)


def test_admission_seal_rejects_remaining_invalid_shapes(tmp_path: Path):
    """Reject every remaining malformed direct receipt input."""

    with pytest.raises(seal.AdmissionSealError, match="proof_shape"):
        seal._require_ascii_canonical_json({1: "value"})
    with pytest.raises(seal.AdmissionSealError, match="proof_shape"):
        seal._require_ascii_canonical_json(1.25)

    for resource_counts in ([], {"": 1}):
        with pytest.raises(seal.AdmissionSealError, match="resource_types"):
            seal._normalized_resource_types(resource_counts)
    with pytest.raises(seal.AdmissionSealError, match="metadata_summary"):
        seal._bounded_metadata_summary({"invalid": object()})
    with pytest.raises(seal.AdmissionSealError, match="proof_summary"):
        seal._generic_proof_summary({})
    with pytest.raises(seal.AdmissionSealError, match="shard_summary"):
        seal._descriptor_resource_counts({}, {"Location": 1})
    _assert_invalid_aggregate_proofs()

    with pytest.raises(seal.AdmissionSealError, match="proof_receipt"):
        seal._receipt(
            {},
            admission_kind="invalid",
            proof_sha256="a" * 64,
            resource_counts={},
        )
    with pytest.raises(seal.AdmissionSealError, match="metadata_invalid"):
        seal.admission_seal_from_validated_metadata([])
    with pytest.raises(seal.AdmissionSealError, match="proof_invalid"):
        seal.admission_seal_from_validated_metadata(
            {PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY: []}
        )
    with pytest.raises(TypeError, match="unexpected keyword"):
        seal.validate_generic_admission_copy(
            tmp_path / "unused.copy",
            dataset_id="dataset_shared",
            endpoint_id="endpoint_shared",
            evidence_run_id="root-shared",
            dataset_hash="e" * 64,
            resource_count=1,
            scratch_directory=tmp_path,
            unexpected_summary={},
        )


def _opened_stream(cleanup: ExitStack, tmp_path: Path):
    proof_stream = stream._GenericProofStream(tmp_path)
    cleanup.callback(proof_stream.close)
    return proof_stream


def _assert_invalid_stream_values(
    monkeypatch,
    tmp_path: Path,
) -> None:
    """Reject malformed captured values and shard descriptors."""

    for number in (Decimal("NaN"), Decimal("1e4096")):
        with pytest.raises(seal.AdmissionSealError, match="number_invalid"):
            stream._Capture(allow_payload_numbers=True).is_complete_after(
                "number", number
            )
    fractional_capture = stream._Capture(allow_payload_numbers=True)
    assert fractional_capture.is_complete_after("number", Decimal("1.25"))
    assert fractional_capture.builder.value == 1.25

    with pytest.raises(seal.AdmissionSealError, match="shard_shape"):
        stream._validated_descriptor_counts({})

    with ExitStack() as cleanup:
        invalid_summary = _opened_stream(cleanup, tmp_path)
        invalid_summary.capture = SimpleNamespace(
            builder=SimpleNamespace(value="value")
        )
        invalid_summary.capture_destination = ("root", "field")
        monkeypatch.setattr(
            stream,
            "canonical_payload_json",
            Mock(side_effect=ValueError("invalid")),
        )
        with pytest.raises(seal.AdmissionSealError, match="metadata_summary"):
            invalid_summary._store_capture()

        too_many_types = _opened_stream(cleanup, tmp_path)
        too_many_types.resource_counts = {
            f"Synthetic{index}": 1 for index in range(64)
        }
        with pytest.raises(seal.AdmissionSealError, match="resource_types"):
            too_many_types._store_descriptor(_descriptor(resource_type="Extra"))

        out_of_order = _opened_stream(cleanup, tmp_path)
        out_of_order.previous_shard_id = "f" * 64
        with pytest.raises(seal.AdmissionSealError, match="shard_order"):
            out_of_order._store_descriptor(_descriptor())


def _assert_invalid_stream_events(tmp_path: Path) -> None:
    """Reject invalid events in every parser state."""

    with ExitStack() as cleanup:
        invalid_root = _opened_stream(cleanup, tmp_path)
        with pytest.raises(seal.AdmissionSealError, match="metadata_invalid"):
            invalid_root.event("", "string", "invalid")

        duplicate_root = _opened_stream(cleanup, tmp_path)
        duplicate_root.event("", "start_map", None)
        duplicate_root.event("", "map_key", "field")
        with pytest.raises(seal.AdmissionSealError, match="metadata_duplicate"):
            duplicate_root.event("", "map_key", "field")

        invalid_proof = _opened_stream(cleanup, tmp_path)
        invalid_proof.event("", "start_map", None)
        invalid_proof.event(
            "", "map_key", PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY
        )
        with pytest.raises(seal.AdmissionSealError, match="proof_invalid"):
            invalid_proof.event("", "string", "invalid")

        duplicate_proof = _opened_stream(cleanup, tmp_path)
        duplicate_proof.mode = "proof"
        duplicate_proof.event("", "map_key", "contract_id")
        with pytest.raises(seal.AdmissionSealError, match="proof_duplicate"):
            duplicate_proof.event("", "map_key", "contract_id")

        invalid_shards = _opened_stream(cleanup, tmp_path)
        invalid_shards.mode = "proof"
        invalid_shards.event("", "map_key", "shards")
        with pytest.raises(seal.AdmissionSealError, match="shards_invalid"):
            invalid_shards.event("", "string", "invalid")

        invalid_shard = _opened_stream(cleanup, tmp_path)
        invalid_shard.mode = "shards"
        with pytest.raises(seal.AdmissionSealError, match="shard_shape"):
            invalid_shard.event("", "string", "invalid")


def test_admission_stream_rejects_remaining_invalid_events(
    monkeypatch,
    tmp_path: Path,
):
    """Cover the remaining stream fail-closed branches."""

    _assert_invalid_stream_values(monkeypatch, tmp_path)
    _assert_invalid_stream_events(tmp_path)


def test_admission_copy_rejects_remaining_framing_failures(tmp_path: Path):
    """Reject malformed COPY framing, trailers, and JSON."""

    header = validation._COPY_SIGNATURE + struct.pack("!ii", 0, 0)
    invalid_file_bytes_by_name = {
        "field-count.copy": header + struct.pack("!h", 2),
        "field-length.copy": header + struct.pack("!h", 1),
        "field-size.copy": header + struct.pack("!h", 1) + struct.pack("!i", -1),
    }
    for file_name, contents in invalid_file_bytes_by_name.items():
        copy_path = tmp_path / file_name
        copy_path.write_bytes(contents)
        with pytest.raises(seal.AdmissionSealError, match="copy_(shape|size)"):
            validation._copy_field_reader(copy_path)

    missing_trailer = tmp_path / "missing-trailer.copy"
    missing_trailer.write_bytes(
        header + struct.pack("!h", 1) + struct.pack("!i", 2) + b"{}"
    )
    with pytest.raises(seal.AdmissionSealError, match="copy_trailer"):
        validation._validate_generic_admission_copy(
            _copy_request(missing_trailer, tmp_path)
        )

    extra_trailer = tmp_path / "extra-trailer.copy"
    _binary_copy_payload(b"{}", extra_trailer)
    extra_trailer.write_bytes(extra_trailer.read_bytes() + b"x")
    with pytest.raises(seal.AdmissionSealError, match="copy_trailer"):
        validation._validate_generic_admission_copy(
            _copy_request(extra_trailer, tmp_path)
        )

    invalid_json = tmp_path / "invalid-json.copy"
    _binary_copy_payload(b"{", invalid_json)
    with pytest.raises(seal.AdmissionSealError, match="copy_parse"):
        validation._validate_generic_admission_copy(
            _copy_request(invalid_json, tmp_path)
        )


def _assert_invalid_validation_summaries(request) -> None:
    """Reject incomplete, malformed, and internally inconsistent summaries."""

    with pytest.raises(seal.AdmissionSealError, match="metadata_incomplete"):
        validation._validate_stream_summary(
            SimpleNamespace(complete=False, mode="expect_root"),
            request,
        )
    with pytest.raises(seal.AdmissionSealError, match="proof_keyset"):
        validation._validate_stream_summary(
            SimpleNamespace(complete=True, mode="root", proof_header={}),
            request,
        )
    exact_header_by_field = {
        field_name: None
        for field_name in stream._SEMANTIC_PROOF_FIELDS
        if field_name != "shards"
    }
    with pytest.raises(seal.AdmissionSealError, match="shard_summary"):
        validation._validate_stream_summary(
            SimpleNamespace(
                complete=True,
                mode="root",
                proof_header=exact_header_by_field,
                shard_count=0,
            ),
            request,
        )


def _lineage():
    return SimpleNamespace(
        source_ids=["source_primary"],
        selected_resources=["Location"],
        proof_resource_scope=["Location"],
    )


def _assert_invalid_validation_lineage(monkeypatch, request) -> None:
    """Reject a root proof scope that differs from its proof header."""

    lineage = _lineage()
    monkeypatch.setattr(
        validation.proof_store,
        "_validated_proof_lineage",
        Mock(return_value=lineage),
    )
    with pytest.raises(
        validation.ProviderDirectoryProofStoreError,
        match="resource scope changed",
    ):
        validation._validated_lineage(
            SimpleNamespace(
                metadata={
                    "source_ids": list(lineage.source_ids),
                    "selected_resources": list(lineage.selected_resources),
                },
                proof_header={
                    PROVIDER_DIRECTORY_PROOF_RESOURCE_SCOPE_METADATA_KEY: [
                        "Practitioner"
                    ]
                },
            ),
            request,
            is_legacy_contract=False,
            proof_scope=["Location"],
        )


def _assert_invalid_resource_totals(request) -> None:
    """Reject mismatched resource families and aggregate totals."""

    for proof_stream in (
        SimpleNamespace(
            proof_header={
                "resource_counts": {"Practitioner": 1},
                "resource_hashes": {"Location": "a" * 64},
            },
            resource_count=1,
            resource_counts={"Location": 1},
        ),
        SimpleNamespace(
            proof_header={
                "resource_counts": {"Location": 1},
                "resource_hashes": {"Location": "a" * 64},
            },
            resource_count=0,
            resource_counts={},
        ),
    ):
        with pytest.raises(
            validation.ProviderDirectoryProofStoreError,
            match="resource (scope|total)",
        ):
            validation._validate_resource_totals(
                proof_stream,
                request,
                {"Location"},
            )


def _assert_invalid_descriptor_scope(monkeypatch, tmp_path: Path, request) -> None:
    """Reject a shard descriptor outside the exact resource scope."""

    lineage = _lineage()
    descriptor_path = tmp_path / "descriptor.jsonl"
    descriptor_path.write_text("{}\n")
    monkeypatch.setattr(
        validation.proof_store,
        "_validated_shard_descriptor",
        Mock(return_value={"resource_counts": {"Practitioner": 1}}),
    )
    with pytest.raises(
        validation.ProviderDirectoryProofStoreError,
        match="shard resource scope",
    ):
        validation._validate_shard_descriptors(
            SimpleNamespace(descriptor_path=descriptor_path),
            request,
            lineage,
            {"Location"},
        )


def test_admission_validation_rejects_remaining_summary_and_lineage_failures(
    monkeypatch,
    tmp_path: Path,
):
    """Cover the remaining final-validation fail-closed branches."""

    request = _copy_request(tmp_path / "unused.copy", tmp_path)
    _assert_invalid_validation_summaries(request)
    _assert_invalid_validation_lineage(monkeypatch, request)
    _assert_invalid_resource_totals(request)
    _assert_invalid_descriptor_scope(monkeypatch, tmp_path, request)
