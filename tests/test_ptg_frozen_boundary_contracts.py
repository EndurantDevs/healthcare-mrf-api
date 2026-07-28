# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Boundary contracts for fail-closed frozen multipart evidence."""

from __future__ import annotations

import datetime as dt
import json
from dataclasses import replace

import pytest

from process.ptg_parts import frozen_rate_binding as binding
from process.ptg_parts import frozen_rate_binding_store as binding_store
from process.ptg_parts import frozen_rate_candidate as candidate
from process.ptg_parts import frozen_rate_files as rate_files
from process.ptg_parts import frozen_rate_runtime as runtime
from process.ptg_parts.frozen_rate_files import (
    FrozenRateFileMismatchError,
    FrozenRateFileValidationError,
)
from tests.ptg_frozen_test_support import (
    frozen_artifacts,
    frozen_candidate_evidence,
    frozen_descriptor_by_ordinal,
    frozen_rate_file_set,
    protected_control_payload,
)


def _descriptors() -> list[dict[str, object]]:
    return [frozen_descriptor_by_ordinal(1), frozen_descriptor_by_ordinal(2)]


def _binding_fixture():
    params = protected_control_payload()["params"]
    frozen_binding = binding.frozen_rate_binding_from_params(params)
    assert frozen_binding is not None
    manifest, sources = frozen_candidate_evidence(params, frozen_binding)
    return params, frozen_binding, manifest, sources


@pytest.mark.parametrize(
    "bad_url",
    [
        None,
        "",
        "https://rates.example.com:bad/part.json.gz",
        "https://rates.example.com/",
    ],
)
def test_frozen_url_rejects_empty_invalid_port_and_root(bad_url):
    descriptors = _descriptors()
    descriptors[0]["canonical_url"] = bad_url

    with pytest.raises(FrozenRateFileValidationError, match="canonical"):
        rate_files.frozen_rate_file_set_sha256(descriptors)


def test_frozen_url_and_validator_byte_caps_are_enforced(monkeypatch):
    descriptors = _descriptors()
    monkeypatch.setattr(rate_files, "FROZEN_RATE_FILE_MAX_URL_BYTES", 8)
    with pytest.raises(FrozenRateFileValidationError, match="canonical"):
        rate_files.frozen_rate_file_set_sha256(descriptors)

    monkeypatch.setattr(rate_files, "FROZEN_RATE_FILE_MAX_URL_BYTES", 4096)
    descriptors[0]["etag"] = '"bad\nvalidator"'
    with pytest.raises(FrozenRateFileValidationError, match="etag"):
        rate_files.frozen_rate_file_set_sha256(descriptors)


@pytest.mark.parametrize(
    "bad_descriptor",
    [
        "not-an-object",
        {"source_type": "in_network"},
    ],
)
def test_frozen_descriptor_shape_is_exact(bad_descriptor):
    descriptors = _descriptors()
    descriptors[0] = bad_descriptor

    with pytest.raises(FrozenRateFileValidationError):
        rate_files.frozen_rate_file_set_sha256(descriptors)


def test_frozen_logical_flag_and_container_shape_are_strict():
    descriptors = _descriptors()
    descriptors[0]["logical_hash_deferred"] = 1
    with pytest.raises(FrozenRateFileValidationError, match="boolean"):
        rate_files.frozen_rate_file_set_sha256(descriptors)

    for frozen_set in (None, {}, [descriptors[0]]):
        with pytest.raises(FrozenRateFileValidationError):
            rate_files.frozen_rate_file_set_sha256(frozen_set)


@pytest.mark.parametrize("raw_limit", ["not-an-int", "0", "-1"])
def test_frozen_aggregate_limit_configuration_fails_closed(
    monkeypatch,
    raw_limit,
):
    monkeypatch.setenv(rate_files.FROZEN_RATE_FILE_TOTAL_MAX_BYTES_ENV, raw_limit)

    with pytest.raises(FrozenRateFileValidationError, match="positive integer"):
        rate_files.frozen_rate_file_set_sha256(_descriptors())


def test_all_canonical_entrypoints_enforce_request_cap(monkeypatch):
    descriptors = _descriptors()
    digest = rate_files.frozen_rate_file_set_sha256(descriptors)
    monkeypatch.setattr(
        rate_files,
        "FROZEN_RATE_FILE_SET_MAX_CANONICAL_BYTES",
        1,
    )

    with pytest.raises(FrozenRateFileValidationError, match="request-size"):
        rate_files.frozen_rate_file_set_sha256(descriptors)
    with pytest.raises(FrozenRateFileValidationError, match="request-size"):
        rate_files.canonical_frozen_rate_file_set_json(descriptors)
    with pytest.raises(FrozenRateFileValidationError, match="request-size"):
        rate_files.normalize_frozen_rate_file_set(descriptors, digest)


def test_frozen_digest_and_proof_shapes_are_strict():
    descriptors = _descriptors()
    with pytest.raises(FrozenRateFileValidationError, match="SHA-256"):
        rate_files.normalize_frozen_rate_file_set(descriptors, None)
    for proof in (None, [], ["not-an-object"], [{"contract": "wrong"}]):
        with pytest.raises(FrozenRateFileValidationError, match="proof"):
            rate_files.frozen_rate_file_proof_sha256(proof)


def test_binding_canonicalizes_datetime_sets_and_absence():
    params = protected_control_payload()["params"]
    params["import_month"] = dt.datetime(2026, 7, 31, 23, 59)
    params["plan_ids"] = None
    frozen_binding = binding.frozen_rate_binding_from_params(params)

    assert frozen_binding["import_month"] == "2026-07-01"
    assert frozen_binding["plan_ids"] == []


@pytest.mark.parametrize(
    ("field_name", "bad_value", "message"),
    [
        ("import_month", "not-a-month", "import_month"),
        ("plan_ids", "plan-a", "array"),
        ("source_key", "\x00", "source_key"),
    ],
)
def test_binding_rejects_invalid_scope_fields(
    field_name,
    bad_value,
    message,
):
    params = protected_control_payload()["params"]
    params[field_name] = bad_value

    with pytest.raises(FrozenRateFileValidationError, match=message):
        binding.frozen_rate_binding_from_params(params)


@pytest.mark.parametrize(
    ("field_name", "bad_value", "message"),
    [
        ("frozen_rate_file_set_contract", "wrong", "contract"),
        ("frozen_rate_file_count", True, "integer"),
        ("frozen_rate_file_count", 3, "does not match"),
    ],
)
def test_binding_rejects_marker_contract_and_count_drift(
    field_name,
    bad_value,
    message,
):
    params = protected_control_payload()["params"]
    params[field_name] = bad_value

    with pytest.raises(FrozenRateFileValidationError, match=message):
        binding.normalize_protected_frozen_rate_params(params)


def test_binding_internal_run_length_guard_is_independent(monkeypatch):
    monkeypatch.setattr(binding, "_required_text", lambda *_args, **_kwargs: "x" * 92)

    with pytest.raises(FrozenRateFileValidationError, match="invalid"):
        binding.frozen_internal_run_id("ignored")


def test_stored_binding_parses_json_and_rejects_ambiguous_rows():
    _, frozen_binding, _, _ = _binding_fixture()
    stored_options_by_name = {
        binding.FROZEN_RATE_FILE_BINDING_OPTION: json.dumps(frozen_binding)
    }
    binding.assert_existing_frozen_binding(
        stored_options_by_name,
        frozen_binding,
        row_exists=True,
    )
    binding.assert_existing_frozen_binding(None, None, row_exists=False)

    for candidate_options_by_name in (
        None,
        {binding.FROZEN_RATE_FILE_BINDING_OPTION: "{"},
    ):
        with pytest.raises(binding.FrozenRateFileBindingMismatchError):
            binding.assert_existing_frozen_binding(
                candidate_options_by_name,
                frozen_binding,
                row_exists=True,
            )


def test_candidate_rejects_marker_binding_and_proof_boundaries():
    _, frozen_binding, manifest, database_sources = _binding_fixture()
    cases = []

    incomplete_manifest_by_field = dict(manifest)
    incomplete_manifest_by_field.pop("frozen_rate_file_count")
    cases.append(
        (
            incomplete_manifest_by_field,
            frozen_binding,
            database_sources,
            "marker tuple",
        )
    )

    wrong_contract_manifest_by_field = dict(manifest)
    wrong_contract_manifest_by_field["frozen_rate_file_set_contract"] = "wrong"
    cases.append(
        (
            wrong_contract_manifest_by_field,
            frozen_binding,
            database_sources,
            "set contract",
        )
    )

    bad_count_manifest_by_field = dict(manifest)
    bad_count_manifest_by_field["frozen_rate_file_count"] = True
    cases.append(
        (
            bad_count_manifest_by_field,
            frozen_binding,
            database_sources,
            "count",
        )
    )

    unavailable_manifest_by_field = dict(manifest)
    cases.append(
        (
            unavailable_manifest_by_field,
            frozen_binding,
            None,
            "unavailable",
        )
    )

    for candidate_manifest, database_binding, database_sources, message in cases:
        with pytest.raises(FrozenRateFileMismatchError, match=message):
            candidate.validate_frozen_candidate_evidence(
                candidate_manifest,
                candidate_run_id="ptg2:source-file-import-001",
                database_binding=database_binding,
                database_sources=database_sources,
            )


def test_candidate_rejects_invalid_proof_and_version_objects():
    _, frozen_binding, manifest, sources = _binding_fixture()

    malformed_proof_manifest_by_field = dict(manifest)
    malformed_proof_manifest_by_field["frozen_rate_file_proof"] = [
        "bad",
        *manifest["frozen_rate_file_proof"][1:],
    ]
    with pytest.raises(FrozenRateFileMismatchError):
        candidate.validate_frozen_candidate_evidence(
            malformed_proof_manifest_by_field,
            candidate_run_id="ptg2:source-file-import-001",
            database_binding=frozen_binding,
            database_sources=sources,
        )

    malformed_version_manifest_by_field = dict(manifest)
    malformed_version_manifest_by_field["source_file_versions"] = [
        "bad",
        *manifest["source_file_versions"][1:],
    ]
    with pytest.raises(FrozenRateFileMismatchError, match="invalid"):
        candidate.validate_frozen_candidate_evidence(
            malformed_version_manifest_by_field,
            candidate_run_id="ptg2:source-file-import-001",
            database_binding=frozen_binding,
            database_sources=sources,
        )


def test_candidate_rejects_binding_identity_and_set_drift():
    _, frozen_binding, manifest, sources = _binding_fixture()
    wrong_run = "ptg2:another-import"
    with pytest.raises(binding.FrozenRateFileBindingMismatchError):
        candidate.validate_frozen_candidate_evidence(
            manifest,
            candidate_run_id=wrong_run,
            database_binding=frozen_binding,
            database_sources=sources,
        )

    drifted_binding_by_name = {
        **frozen_binding,
        "frozen_rate_file_count": 3,
    }
    drifted_manifest_by_name = {
        **manifest,
        binding.FROZEN_RATE_FILE_BINDING_OPTION: drifted_binding_by_name,
    }
    with pytest.raises(binding.FrozenRateFileBindingMismatchError):
        candidate.validate_frozen_candidate_evidence(
            drifted_manifest_by_name,
            candidate_run_id="ptg2:source-file-import-001",
            database_binding=drifted_binding_by_name,
            database_sources=sources,
        )


def test_runtime_head_and_artifact_boundaries(tmp_path):
    descriptor = frozen_descriptor_by_ordinal(1)
    raw_artifact, logical_artifact = frozen_artifacts(descriptor, tmp_path)

    runtime.validate_frozen_head(
        descriptor,
        replace(raw_artifact, reused=True, head=None),
    )
    with pytest.raises(FrozenRateFileMismatchError, match="metadata"):
        runtime.validate_frozen_head(
            descriptor,
            replace(raw_artifact, reused=False, head=None),
        )
    with pytest.raises(FrozenRateFileMismatchError, match="Last-Modified"):
        runtime.validate_frozen_head(
            descriptor,
            replace(
                raw_artifact,
                head=replace(raw_artifact.head, last_modified="changed"),
            ),
        )
    with pytest.raises(FrozenRateFileMismatchError, match="content length"):
        runtime.validate_frozen_head(
            descriptor,
            replace(
                raw_artifact,
                head=replace(raw_artifact.head, content_length=1),
            ),
        )
    with pytest.raises(FrozenRateFileMismatchError, match="evidence"):
        runtime.validate_frozen_head(
            descriptor,
            replace(
                raw_artifact,
                head=replace(raw_artifact.head, status=500),
            ),
        )
    with pytest.raises(FrozenRateFileMismatchError, match="canonical URL"):
        runtime.validate_frozen_artifacts(
            descriptor,
            replace(raw_artifact, canonical_url="https://rates.example.com/other"),
            logical_artifact,
        )


def test_runtime_processed_result_shape_boundaries():
    descriptors, _ = frozen_rate_file_set(2)
    good_results = [
        {
            "success": True,
            "source_type": descriptor["source_type"],
            "url": descriptor["canonical_url"],
            "summary": {
                **descriptor,
                "raw_byte_count": descriptor["content_length"],
                "verification_mode": "downloaded",
            },
        }
        for descriptor in descriptors
    ]

    for bad_results in (
        [{**good_results[0], "success": False}, good_results[1]],
        [{**good_results[0], "url": good_results[1]["url"]}, good_results[1]],
    ):
        with pytest.raises(FrozenRateFileMismatchError, match="cardinality"):
            runtime.validate_frozen_processed_results(descriptors, bad_results)

    wrong_type_results = [dict(file_result) for file_result in good_results]
    wrong_type_results[0] = {
        **wrong_type_results[0],
        "source_type": "wrong",
    }
    with pytest.raises(FrozenRateFileMismatchError, match="source type"):
        runtime.validate_frozen_processed_results(descriptors, wrong_type_results)

    wrong_summary_results = [dict(file_result) for file_result in good_results]
    wrong_summary_results[0] = {
        **wrong_summary_results[0],
        "summary": {
            **wrong_summary_results[0]["summary"],
            "raw_byte_count": 1,
        },
    }
    with pytest.raises(FrozenRateFileMismatchError, match="raw_byte_count"):
        runtime.validate_frozen_processed_results(
            descriptors,
            wrong_summary_results,
        )


def test_binding_store_row_boundaries():
    _, frozen_binding, _, _ = _binding_fixture()
    binding_row_by_name = {
        "source_file_import_id": frozen_binding["source_file_import_id"],
        "internal_run_id": binding.frozen_internal_run_id(
            frozen_binding["source_file_import_id"]
        ),
        "binding_sha256": binding.frozen_rate_binding_sha256(frozen_binding),
        "binding_payload": json.dumps(frozen_binding),
    }
    binding_store._assert_binding_row_integrity(
        binding_row_by_name,
        frozen_binding,
    )
    binding_store._assert_loaded_binding(
        None,
        None,
        requires_durable_row=False,
    )

    with pytest.raises(binding.FrozenRateFileBindingMismatchError):
        binding_store._assert_binding_row_integrity(
            {**binding_row_by_name, "binding_sha256": "wrong"},
            frozen_binding,
        )
    with pytest.raises(binding.FrozenRateFileBindingMismatchError, match="missing"):
        binding_store._assert_loaded_binding(
            None,
            frozen_binding,
            requires_durable_row=True,
        )


def test_remaining_frozen_drift_guards(monkeypatch):
    params, frozen_binding, manifest, database_sources = _binding_fixture()
    monkeypatch.setattr(binding, "_normalize_source_key", lambda _value: None)
    with pytest.raises(FrozenRateFileValidationError, match="source_key"):
        binding.frozen_rate_binding_from_params(params)

    drift_cases = []
    wrong_contract = json.loads(json.dumps(manifest))
    wrong_contract["frozen_rate_file_proof"][0]["contract"] = "wrong"
    drift_cases.append(wrong_contract)
    wrong_bytes = json.loads(json.dumps(manifest))
    wrong_bytes["frozen_rate_file_proof"][0]["raw_byte_count"] = 1
    drift_cases.append(wrong_bytes)
    wrong_count = json.loads(json.dumps(manifest))
    wrong_count["frozen_rate_file_count"] = 3
    drift_cases.append(wrong_count)
    wrong_version = json.loads(json.dumps(manifest))
    wrong_version["source_file_versions"][0]["raw_sha256"] = "f" * 64
    drift_cases.append(wrong_version)
    for drifted_manifest in drift_cases:
        with pytest.raises(FrozenRateFileMismatchError):
            candidate.validate_frozen_candidate_evidence(
                drifted_manifest,
                candidate_run_id="ptg2:source-file-import-001",
                database_binding=frozen_binding,
                database_sources=database_sources,
            )

    with pytest.raises(FrozenRateFileMismatchError):
        candidate._proof_by_version_id({}, 2)
    duplicate_proof = [dict(manifest["frozen_rate_file_proof"][0])] * 2
    with pytest.raises(FrozenRateFileMismatchError):
        candidate._proof_by_version_id(
            {"frozen_rate_file_proof": duplicate_proof},
            2,
        )
