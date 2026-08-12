# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed coverage for Provider Directory admission boundaries."""

import importlib
from pathlib import Path
from types import SimpleNamespace

import pytest

from process import provider_directory_admission_copy as admission_copy
from process import provider_directory_admission_stream as admission_stream
from process import provider_directory_admission_validation as admission_validation
from process.provider_directory_admission_seal import (
    AdmissionSealError,
    admission_seal_from_validated_metadata,
)
from tests.test_provider_directory_dataset_selection_bounded_db import (
    _large_metadata_by_field,
)


importer = importlib.import_module("process.provider_directory_fhir")


def _copy_expectation():
    return admission_validation._AdmissionCopyExpectation(
        dataset_id="dataset_shared",
        endpoint_id="endpoint_shared",
        evidence_run_id="root-shared",
        dataset_hash="e" * 64,
        resource_count=1,
    )


def test_receipt_rejects_unknown_completion_field_and_nonobject_proof():
    with pytest.raises(TypeError, match="unexpected keyword"):
        admission_copy._completion_expectation(
            dataset_id="dataset_shared",
            endpoint_id="endpoint_shared",
            evidence_run_id="root-shared",
            dataset_hash="e" * 64,
            resource_count=1,
            completion_summaries={"unexpected": None},
        )
    with pytest.raises(AdmissionSealError, match="proof_invalid"):
        admission_seal_from_validated_metadata(
            {importer.PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY: []}
        )


def test_streaming_internal_guards_fail_closed(tmp_path: Path, monkeypatch):
    stream = admission_stream._GenericProofStream(tmp_path)
    try:
        with pytest.raises(AdmissionSealError, match="metadata_summary_invalid"):
            stream._store_root_capture("invalid", object())
        with pytest.raises(AdmissionSealError, match="shard_shape_invalid"):
            stream._validated_descriptor({})

        proof = _large_metadata_by_field(1)[
            importer.PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY
        ]
        assert isinstance(proof, dict)
        descriptor = proof["shards"][0]
        descriptor_by_field, count, counts = stream._validated_descriptor(descriptor)
        stream._record_descriptor(descriptor_by_field, count, counts)
        with pytest.raises(AdmissionSealError, match="shard_order_invalid"):
            stream._record_descriptor(descriptor_by_field, count, counts)

        monkeypatch.setattr(
            admission_stream,
            "_normalized_resource_types",
            lambda _counts: tuple(f"Synthetic{index}" for index in range(65)),
        )
        with pytest.raises(AdmissionSealError, match="resource_types_invalid"):
            stream._record_descriptor(descriptor_by_field, count, counts)
    finally:
        stream.close()


def test_completion_receipts_reject_cross_scope_state(monkeypatch):
    expected = _copy_expectation()
    with pytest.raises(AdmissionSealError, match="proof_kind_invalid"):
        admission_validation._uhc_receipt(
            SimpleNamespace(
                seen_root_keys={
                    importer.PROVIDER_DIRECTORY_CONTENT_PROOF_METADATA_KEY
                }
            ),
            expected,
        )

    monkeypatch.setattr(
        admission_validation,
        "_validate_uhc_contract",
        lambda _stream, _expected: {
            "dataset_hash": "f" * 64,
            "resource_count": 1,
        },
    )
    with pytest.raises(AdmissionSealError, match="completion_summary_invalid"):
        admission_validation._uhc_receipt(
            SimpleNamespace(seen_root_keys=set()),
            expected,
        )
    with pytest.raises(AdmissionSealError, match="proof_keyset_invalid"):
        admission_validation._require_generic_header_summaries(
            SimpleNamespace(proof_header={}),
            expected,
        )


def test_lineage_and_shard_guards_reject_scope_drift(tmp_path: Path, monkeypatch):
    expected = _copy_expectation()
    proof_scope_key = importer.PROVIDER_DIRECTORY_PROOF_RESOURCE_SCOPE_METADATA_KEY
    with pytest.raises(
        admission_validation.ProviderDirectoryProofStoreError,
        match="resource scope changed",
    ):
        admission_validation._require_unchanged_lineage(
            SimpleNamespace(
                metadata={
                    "source_ids": ["source_shared"],
                    "selected_resources": ["Location"],
                },
                proof_header={proof_scope_key: ["Practitioner"]},
            ),
            SimpleNamespace(
                source_ids=["source_shared"],
                selected_resources=["Location"],
                proof_resource_scope=["Location"],
            ),
            False,
            ["Location"],
        )

    descriptor_path = tmp_path / "descriptor.jsonl"
    descriptor_path.write_text("{}\n")
    monkeypatch.setattr(
        admission_validation.proof_store,
        "_validated_shard_descriptor",
        lambda *_args, **_kwargs: {"resource_counts": {"Practitioner": 1}},
    )
    with pytest.raises(
        admission_validation.ProviderDirectoryProofStoreError,
        match="shard resource scope changed",
    ):
        admission_validation._require_shard_scopes(
            SimpleNamespace(descriptor_path=descriptor_path),
            expected,
            SimpleNamespace(source_ids=("source_shared",)),
            {"Location"},
        )


def test_finished_stream_rejects_incomplete_proof(tmp_path: Path):
    stream = admission_stream._GenericProofStream(tmp_path)
    try:
        with pytest.raises(AdmissionSealError, match="metadata_incomplete"):
            admission_validation._validate_finished_stream(
                stream,
                _copy_expectation(),
            )
    finally:
        stream.close()
