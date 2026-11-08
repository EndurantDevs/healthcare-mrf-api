# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic policy declarations and binding-version compatibility checks."""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from dataclasses import FrozenInstanceError, replace

import pytest

from process.custom_import.processing_policy import BuildPolicy, ProcessingPolicy
from process.custom_import.segmented_capture_policy import SegmentedCapturePolicyError
from process.custom_import.snowflake_binding import (
    SOURCE_BINDING_CONTRACT,
    SOURCE_BINDING_V2_CONTRACT,
    SnowflakeSourceBinding,
    SnowflakeSourceBindingError,
)
from process.custom_import.snowflake_source_binding import (
    SnowflakeSourceBindingUnavailableError,
    _loaded_snowflake_source_binding,
)
from tests.test_custom_import_segmented_capture_policy import _document as _capture_policy_document
from tests.test_custom_import_snowflake_source_binding import _binding_document, _definition, _loaded_rows

_BUILD_CEILINGS = {
    "page_row_limit": 256,
    "page_byte_limit": 268_435_456,
    "statement_timeout_ms": 2_147_483_647,
    "lease_seconds": 3_600,
    "build_deadline_seconds": 86_400,
}


def _policy_document():
    return {
        "capture": _capture_policy_document(),
        "driver_timeout_seconds": 30,
        "build": {
            "page_row_limit": 2,
            "page_byte_limit": 4096,
            "statement_timeout_ms": 1000,
            "lease_seconds": 60,
            "build_deadline_seconds": 300,
        },
    }


def _v2_document():
    document = _binding_document(_definition())
    document.update(contract=SOURCE_BINDING_V2_CONTRACT, processing_policy=_policy_document())
    return document


def test_policy_roundtrip_copies_every_nested_declaration():
    document = _policy_document()
    expected = deepcopy(document)
    policy = ProcessingPolicy.from_mapping(document)
    document["capture"]["stream_budget"]["maximum_parts"] = 2
    document["build"]["page_row_limit"] = 3
    assert policy.to_mapping() == expected
    wire = policy.to_mapping()
    wire["capture"]["bundle_budget"]["maximum_parts"] = 1
    wire["build"]["page_row_limit"] = 4
    assert policy.to_mapping() == expected
    with pytest.raises(FrozenInstanceError):
        policy.driver_timeout_seconds = 40
    with pytest.raises(FrozenInstanceError):
        policy.build.lease_seconds = 120
    with pytest.raises(TypeError):
        policy.capture.stream_budget["maximum_parts"] = 2


@pytest.mark.parametrize("field", ["capture", "driver_timeout_seconds", "build"])
@pytest.mark.parametrize("change", ["missing", "unknown"])
def test_processing_policy_rejects_partial_or_open_shapes(field, change):
    document = _policy_document()
    if change == "missing":
        del document[field]
    else:
        document[field + "_extra"] = document[field]
    with pytest.raises(SegmentedCapturePolicyError):
        ProcessingPolicy.from_mapping(document)


@pytest.mark.parametrize("field", _BUILD_CEILINGS)
@pytest.mark.parametrize("change", ["missing", "unknown"])
def test_build_policy_rejects_partial_or_open_shapes(field, change):
    document = _policy_document()["build"]
    if change == "missing":
        del document[field]
    else:
        document[field + "_extra"] = document[field]
    with pytest.raises(SegmentedCapturePolicyError):
        BuildPolicy.from_mapping(document)


@pytest.mark.parametrize("field", _BUILD_CEILINGS)
@pytest.mark.parametrize("invalid", [None, True, False, 0, -1, 1.0, "1", [], {}])
def test_build_policy_requires_positive_integers(field, invalid):
    document = _policy_document()["build"]
    document[field] = invalid
    with pytest.raises(SegmentedCapturePolicyError):
        BuildPolicy.from_mapping(document)


@pytest.mark.parametrize("field,maximum", _BUILD_CEILINGS.items())
def test_build_policy_enforces_each_ceiling(field, maximum):
    document = _policy_document()["build"]
    for admitted in (1, maximum):
        document[field] = admitted
        assert getattr(BuildPolicy.from_mapping(document), field) == admitted
    document[field] = maximum + 1
    with pytest.raises(SegmentedCapturePolicyError):
        BuildPolicy.from_mapping(document)


@pytest.mark.parametrize("invalid", [None, True, False, 0, -1, 1.0, "1", [], {}, 121])
def test_driver_timeout_preserves_the_existing_cap(invalid):
    document = _policy_document()
    document["driver_timeout_seconds"] = invalid
    with pytest.raises(SegmentedCapturePolicyError):
        ProcessingPolicy.from_mapping(document)


@pytest.mark.parametrize("timeout", [1, 120])
def test_driver_timeout_accepts_exact_bounds(timeout):
    document = _policy_document()
    document["driver_timeout_seconds"] = timeout
    assert ProcessingPolicy.from_mapping(document).driver_timeout_seconds == timeout


@pytest.mark.parametrize("invalid", [None, "{}", [], {}, True])
def test_capture_policy_reuses_closed_capture_validation(invalid):
    document = _policy_document()
    document["capture"] = invalid
    with pytest.raises(SegmentedCapturePolicyError):
        ProcessingPolicy.from_mapping(document)


def test_capture_policy_reuses_coherence_checks():
    document = _policy_document()
    document["capture"]["bundle_budget"]["maximum_parts"] = 1
    with pytest.raises(SegmentedCapturePolicyError):
        ProcessingPolicy.from_mapping(document)


def test_constructor_rejects_unvalidated_nested_objects():
    valid = ProcessingPolicy.from_mapping(_policy_document())
    with pytest.raises(SegmentedCapturePolicyError):
        replace(valid, capture=valid.capture.to_mapping())
    with pytest.raises(SegmentedCapturePolicyError):
        replace(valid, build=valid.build.to_mapping())
    with pytest.raises(SegmentedCapturePolicyError):
        replace(valid.build, page_row_limit=0)


def test_v1_binding_keeps_exact_bytes_and_hash_domain():
    document = _binding_document(_definition())
    binding = SnowflakeSourceBinding.from_mapping(document)
    document["role"] = document["role"].upper()
    document["warehouse"] = document["warehouse"].upper()
    for stream in document["streams"]:
        stream["relation"] = [identifier.upper() for identifier in stream["relation"]]
        stream["source_snapshot_token_relation"] = [
            identifier.upper() for identifier in stream["source_snapshot_token_relation"]
        ]
        stream["source_snapshot_token_column_identifier"] = stream["source_snapshot_token_column_identifier"].upper()
        for column in stream["columns"]:
            column["column_identifier"] = column["column_identifier"].upper()
    expected = json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    assert binding.contract == SOURCE_BINDING_CONTRACT
    assert binding.processing_policy is None
    assert binding.canonical == expected
    assert binding.digest == hashlib.sha256((SOURCE_BINDING_CONTRACT + ":" + expected).encode()).hexdigest()
    assert binding.digest == "0fa5896a1f329396f287547c2269e4bddf03ee01ce9522289a38fb9d705bd1d5"
    assert replace(binding, processing_policy=None) == binding
    assert "processing_policy" not in json.loads(binding.canonical)


def test_v2_binding_pins_every_processing_limit_in_its_digest():
    document = _v2_document()
    binding = SnowflakeSourceBinding.from_mapping(document)
    assert binding.contract == SOURCE_BINDING_V2_CONTRACT
    assert binding.processing_policy.to_mapping() == document["processing_policy"]
    assert SnowflakeSourceBinding.from_json(binding.canonical) == binding
    assert binding.digest == hashlib.sha256((SOURCE_BINDING_V2_CONTRACT + ":" + binding.canonical).encode()).hexdigest()
    assert binding.bundle_components(_definition()) == SnowflakeSourceBinding.from_mapping(
        _binding_document(_definition())
    ).bundle_components(_definition())


@pytest.mark.parametrize("section", ["driver", "build", "capture"])
def test_changed_policy_has_a_distinct_binding_identity(section):
    document = _v2_document()
    original = SnowflakeSourceBinding.from_mapping(document)
    if section == "driver":
        document["processing_policy"]["driver_timeout_seconds"] += 1
    elif section == "build":
        document["processing_policy"]["build"]["lease_seconds"] += 1
    else:
        document["processing_policy"]["capture"]["acquisition_deadline_seconds"] += 1
    assert SnowflakeSourceBinding.from_mapping(document).digest != original.digest


@pytest.mark.parametrize("defect", ["missing", "null", "partial", "extra"])
def test_invalid_v2_never_falls_back_to_v1(defect):
    document = _v2_document()
    if defect == "missing":
        del document["processing_policy"]
    elif defect == "null":
        document["processing_policy"] = None
    elif defect == "partial":
        del document["processing_policy"]["build"]["lease_seconds"]
    else:
        document["processing_policy"]["extra"] = 1
    with pytest.raises(SnowflakeSourceBindingError):
        SnowflakeSourceBinding.from_mapping(document)


@pytest.mark.parametrize("contract", [SOURCE_BINDING_CONTRACT, "custom-import/source-binding/v3"])
def test_policy_requires_the_known_opt_in_version(contract):
    document = _v2_document()
    document["contract"] = contract
    with pytest.raises(SnowflakeSourceBindingError):
        SnowflakeSourceBinding.from_mapping(document)


def test_loaded_v2_requires_matching_retained_version():
    definition = _definition()
    binding = SnowflakeSourceBinding.from_mapping(_v2_document())
    rows = _loaded_rows(definition, binding)
    rows[0].binding_contract = SOURCE_BINDING_V2_CONTRACT
    loaded = _loaded_snowflake_source_binding(*rows)
    assert loaded.binding == binding
    assert loaded.binding.processing_policy == binding.processing_policy
    rows[0].binding_contract = SOURCE_BINDING_CONTRACT
    with pytest.raises(SnowflakeSourceBindingUnavailableError):
        _loaded_snowflake_source_binding(*rows)
