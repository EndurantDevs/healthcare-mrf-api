# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Additional streaming admission-seal contracts."""

from __future__ import annotations

import hashlib
import importlib
import json
from pathlib import Path
import struct

import pytest

from process.provider_directory_admission_seal import (
    ADMISSION_METADATA_SUMMARY_MAX_BYTES,
    AdmissionSealError,
    validate_generic_admission_copy,
)
from process.provider_directory_fhir_subset_canonical import (
    canonical_payload_sha256,
)
from tests.test_provider_directory_admission_seal import (
    _COPY_SIGNATURE,
    _binary_copy,
    _binary_copy_payload,
)
from tests.test_provider_directory_dataset_selection_bounded_db import (
    _large_metadata_by_field,
)
from tests.uhc_final_publication_test_support import final_publication_fixture


importer = importlib.import_module("process.provider_directory_fhir")


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
