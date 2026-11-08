# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Witness projection preserves original bytes, proof inputs and the admitted seal."""

import hashlib
import json
import struct
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from ijson import JSONError

from process import network_fhir_source_witness_publication as publication
from process import provider_directory_admission_validation as admission
from process.network_fhir_source_witness_contract import (
    ADMITTED_LEGACY_PROFILE,
    ADMITTED_TRANSPORT_PROFILE,
    ADMITTED_UHC_PROFILE,
    OBSERVED_LEGACY_PROFILE,
    source_origin,
)
from process.provider_directory_admission_seal import AdmissionSealError, validate_generic_admission_copy
from process.provider_directory_proof_store import PROVIDER_DIRECTORY_CONTENT_PROOF_CONTRACT_ID
from process.provider_directory_source_summary import (
    SOURCE_SUMMARY_FHIR_SEMANTIC_CONTRACT_ID,
    SOURCE_SUMMARY_OUTCOME_COUNT_FIELDS,
    ProviderDirectorySourceSummaryBinding,
    build_source_summary,
)
from tests.test_network_fhir_source_witness_contract import origin_fields
from tests.test_network_fhir_source_witness_publication import _publication_header
from tests.test_provider_directory_admission_seal import _binary_copy, _binary_copy_payload
from tests.test_provider_directory_admission_seal_streaming_contracts import _reseal
from tests.test_provider_directory_dataset_selection_bounded_db import _large_metadata_by_field

SKIPPED_FIELDS = ("completion_proof_v1", "resource_diagnostics", "server_issued_subset_replay_evidence")


def _metadata():
    fields = origin_fields()
    metadata = _large_metadata_by_field(
        2,
        dataset_id=fields["dataset_id"],
        endpoint_id=fields["endpoint_id"],
        root_run_id=fields["root_run_id"],
        source_id_list=[fields["source_id"]],
    )
    proof = metadata["provider_directory_content_proof_v1"]
    proof["contract_id"] = PROVIDER_DIRECTORY_CONTENT_PROOF_CONTRACT_ID
    for key in ("proof_resource_scope", "resource_hash_contract", "semantic_projection_as_of", "semantic_union"):
        proof.pop(key)
    metadata.pop("proof_resource_scope")
    metadata.pop("semantic_projection_as_of")
    metadata["resource_hash_contract"] = "transport_bound_v1"
    _reseal(proof)
    metadata["source_summary_v1"] = build_source_summary(
        binding=ProviderDirectorySourceSummaryBinding(
            fields["dataset_id"], fields["endpoint_id"], fields["root_run_id"], proof["dataset_hash"]
        ),
        source_ids=[fields["source_id"]],
        selected_resources=["Location"],
        count_by_resource=proof["resource_counts"],
        hash_by_resource=proof["resource_hashes"],
        count_by_field=dict.fromkeys(SOURCE_SUMMARY_OUTCOME_COUNT_FIELDS, 0),
        count_by_category={"unknown_field_counts": {}, "intentional_drop_counts": {}},
        identity_by_field={"semantic_contract_id": SOURCE_SUMMARY_FHIR_SEMANTIC_CONTRACT_ID},
    )
    metadata.update({field: {"retained": [1, "small"]} for field in SKIPPED_FIELDS})
    return metadata


def _expectation(metadata):
    proof = metadata["provider_directory_content_proof_v1"]
    return admission._AdmissionCopyExpectation(
        proof["dataset_id"],
        proof["endpoint_id"],
        proof["acquisition_root_run_id"],
        proof["dataset_hash"],
        proof["resource_count"],
        proof["resource_hashes"],
        proof["resource_counts"],
    )


def _admitted_inputs(tmp_path, *, oversized=False):
    metadata = _metadata()
    path = tmp_path / "metadata.copy"
    _binary_copy(metadata, path)
    expected = _expectation(metadata)
    with publication._metadata_stream(path) as stream:
        original = admission._validate_finished_stream(stream, expected)
    if oversized:
        metadata.update({field: {"entries": ["x" * 1024] * 1100} for field in SKIPPED_FIELDS})
    encoded = json.dumps(metadata, ensure_ascii=False).encode()
    proof = metadata["provider_directory_content_proof_v1"]
    origin = source_origin(
        origin_fields()
        | {
            "dataset_sha256": proof["dataset_hash"],
            "resource_counts": tuple(proof["resource_counts"].items()),
            "resource_hashes": tuple(proof["resource_hashes"].items()),
            "descriptor_counts": tuple(proof["resource_counts"].items()),
            "admission_profile": ADMITTED_LEGACY_PROFILE,
            "semantic_projection_as_of": None,
            "metadata_bytes": len(encoded),
            "metadata_text_sha256": hashlib.sha256(encoded).hexdigest(),
            "summary_sha256": metadata["source_summary_v1"]["summary_sha256"],
            "proof_sha256": original.proof_sha256,
            "admission_metadata_sha256": original.metadata_sha256,
        }
    )
    header = _publication_header(origin, metadata["source_summary_v1"]) | {
        "hash_contract": "transport_bound_v1",
        "hash_contract_type": "string",
        "proof_absent": False,
        "admission_absent": False,
        "proof_sha256": original.proof_sha256,
        "content_proof_admission_version": 1,
        "content_proof_admission_kind": "generic",
        "content_proof_admission_sha256": original.proof_sha256,
        "content_proof_resource_types": list(original.resource_types),
        "publication_metadata_sha256": original.metadata_sha256,
        "admitted_summary": json.dumps(original.metadata_summary),
    }
    return encoded, origin, header


def _reader(encoded, *, trailer=None):
    start, rollback = AsyncMock(), AsyncMock()

    async def copy(query, dataset_id, *, output, **options):
        assert query.startswith("SELECT publication_metadata_json::text") and options == {"format": "binary"}
        payload = b"PGCOPY\n\xff\r\n\x00" + struct.pack("!iihi", 0, 0, 1, len(encoded)) + encoded
        await output(payload + (struct.pack("!h", -1) if trailer is None else trailer))
        return "COPY 1"

    return SimpleNamespace(
        transaction=lambda: SimpleNamespace(start=start, rollback=rollback), copy_from_query=copy
    ), rollback


@pytest.mark.asyncio
@pytest.mark.parametrize("oversized", [False, True])
async def test_admitted_witness_keeps_original_seal_and_complete_raw_authentication(tmp_path, oversized):
    encoded, origin, header = _admitted_inputs(tmp_path, oversized=oversized)
    reader, rollback = _reader(encoded)
    await publication.verify_publication(reader, origin, header)
    rollback.assert_awaited_once()
    path = tmp_path / "metadata.copy"
    _binary_copy_payload(encoded, path)
    with publication._metadata_stream(path, admission_profile=origin.admission_profile) as stream:
        assert not set(SKIPPED_FIELDS) & stream.metadata.keys()
        assert set(SKIPPED_FIELDS) <= stream.seen_root_keys
        projected = admission._validate_finished_stream(stream, _expectation(json.loads(encoded)))
        assert projected.proof_sha256 == origin.proof_sha256
        assert projected.metadata_sha256 != origin.admission_metadata_sha256
        publication._admitted_header(header, origin, "generic")
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.parametrize("profile", [None, OBSERVED_LEGACY_PROFILE, ADMITTED_UHC_PROFILE])
def test_other_profiles_and_admission_writer_keep_aggregate_capture_bound(tmp_path, profile):
    encoded, origin, _header = _admitted_inputs(tmp_path, oversized=True)
    path = tmp_path / "metadata.copy"
    _binary_copy_payload(encoded, path)
    with pytest.raises(AdmissionSealError, match="capture_unbounded"):
        with publication._metadata_stream(path, admission_profile=profile):
            pytest.fail("unprojected metadata admitted")
    with pytest.raises(AdmissionSealError, match="capture_unbounded"):
        validate_generic_admission_copy(
            path,
            dataset_id=origin.dataset_id,
            endpoint_id=origin.endpoint_id,
            evidence_run_id=origin.root_run_id,
            dataset_hash=origin.dataset_sha256,
            resource_count=2,
        )
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.parametrize("profile", [ADMITTED_LEGACY_PROFILE, ADMITTED_TRANSPORT_PROFILE])
@pytest.mark.parametrize(
    "value", [b"null", b"1.25", b'"unicode-\\u00e9"', b'[[{"x":1}]]', b"[" * 256 + b"0" + b"]" * 256]
)
def test_skipped_values_keep_scalar_numeric_unicode_and_nested_grammar(tmp_path, profile, value):
    path = tmp_path / "metadata.copy"
    _binary_copy_payload(b'{"resource_diagnostics":' + value + b',"following":true}', path)
    with publication._metadata_stream(path, admission_profile=profile) as stream:
        assert stream.metadata == {"following": True}
        assert stream.complete and stream.capture is None
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.parametrize(
    "payload,reason",
    [
        (b'{"resource_diagnostics":{},"resource_diagnostics":{}}', "metadata_duplicate"),
        (b'{"resource_diagnostics":[1,]}', None),
        (b'{"resource_diagnostics":[[[0]]}', None),
        (b'{"resource_diagnostics":"\xff"}', None),
        (b'{"resource_diagnostics":"\\ud800"}', None),
        (b'{"resource_diagnostics":1e4096}', "number_invalid"),
        (b'{"resource_diagnostics":1.00000000000000000000000000001}', "number_invalid"),
        (b'{"resource_diagnostics":NaN}', None),
        (b'{"resource_diagnostics":{},"provider_directory_content_proof_admission_summary_v1":{}}', "reserved"),
        (b'{"resource_diagnostics":{},"provider_directory_content_proof_v1":{"shards":null}}', "shards_invalid"),
    ],
)
def test_skipped_value_refusals_and_spool_cleanup(tmp_path, payload, reason):
    path = tmp_path / "metadata.copy"
    _binary_copy_payload(payload, path)
    with pytest.raises((AdmissionSealError, JSONError, UnicodeError), match=reason):
        with publication._metadata_stream(path, admission_profile=ADMITTED_LEGACY_PROFILE):
            pytest.fail("invalid metadata admitted")
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.parametrize("key", ["resource_diagnostics", "unrelated_field"])
def test_skipped_scalar_and_other_field_limits_are_unchanged(tmp_path, key):
    path = tmp_path / "metadata.copy"
    _binary_copy({key: "x" * (1024**2 + 1)}, path)
    with pytest.raises(AdmissionSealError, match="capture_unbounded"):
        with publication._metadata_stream(path, admission_profile=ADMITTED_LEGACY_PROFILE):
            pytest.fail("unbounded scalar admitted")
    assert list(tmp_path.iterdir()) == [path]


def test_skipped_event_budget_is_unchanged():
    capture = publication._SkippedMetadataCapture()
    assert not capture._is_complete_after_event("start_array", None)
    capture.event_count = 1024**2
    with pytest.raises(AdmissionSealError, match="capture_unbounded"):
        capture._is_complete_after_event("end_array", None)
    assert not hasattr(capture, "builder")


def test_skipped_aggregate_summary_keeps_complete_proof_parity(tmp_path):
    metadata = _metadata()
    metadata.update({field: {"entries": ["x" * 1024] * 600} for field in SKIPPED_FIELDS})
    path = tmp_path / "metadata.copy"
    _binary_copy(metadata, path)
    with pytest.raises(AdmissionSealError, match="metadata_summary_unbounded"):
        with publication._metadata_stream(path):
            pytest.fail("unprojected aggregate admitted")
    with publication._metadata_stream(path, admission_profile=ADMITTED_LEGACY_PROFILE) as stream:
        seal = admission._validate_finished_stream(stream, _expectation(metadata))
        assert seal.proof_sha256 == metadata["provider_directory_content_proof_v1"]["proof_sha256"]
    assert list(tmp_path.iterdir()) == [path]


@pytest.mark.asyncio
@pytest.mark.parametrize("corruption", ["raw_hash", "header_seal", "source_scope", "proof_hash", "copy_trailer"])
async def test_projection_does_not_bypass_raw_header_lineage_or_proof_closure(tmp_path, corruption):
    encoded, origin, header = _admitted_inputs(tmp_path, oversized=True)
    trailer = None
    if corruption == "raw_hash":
        encoded = encoded.replace(b'"entries"', b'"changed"', 1)
    elif corruption == "header_seal":
        header["admitted_summary"] = header["admitted_summary"].replace("small", "other")
    elif corruption in {"source_scope", "proof_hash"}:
        metadata = json.loads(encoded)
        if corruption == "source_scope":
            metadata["source_ids"] = ["source-other"]
        else:
            metadata["provider_directory_content_proof_v1"]["proof_sha256"] = "9" * 64
        encoded = json.dumps(metadata).encode()
        origin = replace(origin, metadata_bytes=len(encoded), metadata_text_sha256=hashlib.sha256(encoded).hexdigest())
        header.update(metadata_bytes=origin.metadata_bytes, metadata_sha256=origin.metadata_text_sha256)
    else:
        trailer = b"\x00\x00"
    reader, rollback = _reader(encoded, trailer=trailer)
    with pytest.raises((ValueError, AdmissionSealError)):
        await publication.verify_publication(reader, origin, header)
    rollback.assert_awaited_once()


def test_raw_metadata_cap_is_unchanged(tmp_path):
    path = tmp_path / "metadata.copy"
    path.write_bytes(b"PGCOPY\n\xff\r\n\x00" + struct.pack("!iihi", 0, 0, 1, 256 * 1024**2 + 1))
    with pytest.raises(AdmissionSealError, match="copy_size"):
        with publication._metadata_stream(path, admission_profile=ADMITTED_LEGACY_PROFILE):
            pytest.fail("oversized raw field admitted")
    assert list(tmp_path.iterdir()) == [path]
