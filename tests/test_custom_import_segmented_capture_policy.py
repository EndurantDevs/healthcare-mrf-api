# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Explicit declaration checks; these synthetic limits are not admission values."""

from __future__ import annotations

import hashlib
import json
from dataclasses import FrozenInstanceError, asdict, replace

import pytest

from process.custom_import.capture_limits import CaptureLimits
from process.custom_import.definition import canonical_json
from process.custom_import.segmented_capture_policy import (
    SEGMENTED_CAPTURE_POLICY_CONTRACT,
    SegmentedCapturePolicy,
    SegmentedCapturePolicyError,
)

_MAX_BIGINT = (1 << 63) - 1
_PART_CEILINGS = {
    "maximum_compressed_bytes": 64 * 1024 * 1024,
    "maximum_decoded_bytes": 256 * 1024 * 1024,
    "maximum_record_bytes": 1024 * 1024,
    "maximum_records": 1_000_000,
    "maximum_fields_per_record": 1_024,
    "read_chunk_bytes": 64 * 1024 * 1024,
}
_SCALAR_CEILINGS = {
    "maximum_part_arrow_bytes": 256 * 1024 * 1024,
    "maximum_part_manifest_bytes": 2 * 1024 * 1024,
    "maximum_dataset_retained_bytes": _MAX_BIGINT,
    "acquisition_deadline_seconds": 86_400,
}
_BUDGET_KEYS = (
    "maximum_parts",
    "maximum_compressed_bytes",
    "maximum_decoded_bytes",
    "maximum_arrow_bytes",
    "maximum_records",
    "maximum_manifest_bytes",
)


def _document():
    stream_budget_by_limit = dict(zip(_BUDGET_KEYS, (3, 3_072, 12_288, 24_576, 30, 3_072), strict=True))
    return {
        "contract": SEGMENTED_CAPTURE_POLICY_CONTRACT,
        "part_limits": {
            "maximum_compressed_bytes": 1_024,
            "maximum_decoded_bytes": 4_096,
            "maximum_record_bytes": 256,
            "maximum_records": 10,
            "maximum_fields_per_record": 4,
            "read_chunk_bytes": 128,
        },
        "stream_budget": stream_budget_by_limit,
        "bundle_budget": {key: value * 2 for key, value in stream_budget_by_limit.items()},
        "maximum_part_arrow_bytes": 8_192,
        "maximum_part_manifest_bytes": 1_024,
        "maximum_dataset_retained_bytes": 16_384,
        "acquisition_deadline_seconds": 60,
    }


def _ceiling_document():
    budget_by_limit = {
        "maximum_parts": 131_072,
        "maximum_compressed_bytes": _PART_CEILINGS["maximum_compressed_bytes"],
        "maximum_decoded_bytes": _PART_CEILINGS["maximum_decoded_bytes"],
        "maximum_arrow_bytes": _SCALAR_CEILINGS["maximum_part_arrow_bytes"],
        "maximum_records": _PART_CEILINGS["maximum_records"],
        "maximum_manifest_bytes": _SCALAR_CEILINGS["maximum_part_manifest_bytes"],
    }
    return {
        "contract": SEGMENTED_CAPTURE_POLICY_CONTRACT,
        "part_limits": dict(_PART_CEILINGS),
        "stream_budget": dict(budget_by_limit),
        "bundle_budget": dict(budget_by_limit),
        **_SCALAR_CEILINGS,
    }


def test_roundtrip_and_identity_are_canonical_and_domain_separated():
    document = _document()
    policy = SegmentedCapturePolicy.from_mapping(document)
    reordered_by_key = {key: value for key, value in reversed(list(document.items()))}
    reordered_by_key["part_limits"] = dict(reversed(list(document["part_limits"].items())))
    assert SegmentedCapturePolicy.from_mapping(reordered_by_key) == policy
    assert policy.to_mapping() == document
    assert SegmentedCapturePolicy.from_mapping(json.loads(policy.canonical)) == policy
    assert policy.canonical == canonical_json(document)
    encoded = policy.canonical.encode("utf-8")
    assert policy.digest == hashlib.sha256(b"custom-import/segmented-capture-policy/v1:" + encoded).hexdigest()
    assert policy.digest != hashlib.sha256(encoded).hexdigest()
    assert policy.digest != hashlib.sha256(b"custom-import/source-binding/v1:" + encoded).hexdigest()


def test_policy_is_frozen_and_detached_from_input_and_output():
    document = _document()
    policy = SegmentedCapturePolicy.from_mapping(document)
    original = policy.to_mapping()
    document["part_limits"]["maximum_records"] = 20
    document["stream_budget"]["maximum_parts"] = 5
    output = policy.to_mapping()
    output["bundle_budget"]["maximum_records"] = 100
    assert policy.to_mapping() == original
    with pytest.raises(TypeError):
        policy.stream_budget["maximum_parts"] = 5
    with pytest.raises(TypeError):
        policy.bundle_budget["maximum_parts"] = 5
    with pytest.raises(FrozenInstanceError):
        policy.acquisition_deadline_seconds = 100
    with pytest.raises(FrozenInstanceError):
        policy.part_limits.maximum_records = 20


_NUMERIC_PATHS = (
    *(f"part_limits.{key}" for key in _PART_CEILINGS),
    *(f"{budget}.{key}" for budget in ("stream_budget", "bundle_budget") for key in _BUDGET_KEYS),
    *_SCALAR_CEILINGS,
)


def _set_value(document, path, value):
    if "." in path:
        parent, key = path.split(".")
        document[parent][key] = value
    else:
        document[path] = value


@pytest.mark.parametrize("path", _NUMERIC_PATHS)
@pytest.mark.parametrize("value", [None, True, False, 0, -1, 1.0, "1", _MAX_BIGINT + 1])
def test_every_numeric_input_requires_a_bounded_positive_integer(path, value):
    document = _document()
    _set_value(document, path, value)
    with pytest.raises(SegmentedCapturePolicyError):
        SegmentedCapturePolicy.from_mapping(document)


@pytest.mark.parametrize("parent", [None, "part_limits", "stream_budget", "bundle_budget"])
@pytest.mark.parametrize("change", ["missing", "unknown", "null", "list"])
def test_each_mapping_is_closed_and_required(parent, change):
    document = _document()
    mapping = document if parent is None else document[parent]
    if change == "missing":
        for key in tuple(mapping):
            changed = _document()
            target = changed if parent is None else changed[parent]
            del target[key]
            with pytest.raises(SegmentedCapturePolicyError):
                SegmentedCapturePolicy.from_mapping(changed)
    elif change == "unknown":
        mapping["extra"] = 1
        with pytest.raises(SegmentedCapturePolicyError):
            SegmentedCapturePolicy.from_mapping(document)
    else:
        value = None if change == "null" else list(mapping.items())
        if parent is None:
            document = value
        else:
            document[parent] = value
        with pytest.raises(SegmentedCapturePolicyError):
            SegmentedCapturePolicy.from_mapping(document)


@pytest.mark.parametrize("contract", [None, True, 1, b"custom-import/segmented-capture-policy/v1", "other/v1"])
def test_only_the_declared_contract_is_accepted(contract):
    document = _document()
    document["contract"] = contract
    with pytest.raises(SegmentedCapturePolicyError):
        SegmentedCapturePolicy.from_mapping(document)


def test_all_safety_ceilings_are_inclusive():
    policy = SegmentedCapturePolicy.from_mapping(_ceiling_document())
    assert policy.stream_budget["maximum_parts"] == 131_072
    assert policy.bundle_budget["maximum_parts"] == 131_072
    assert policy.maximum_dataset_retained_bytes == _MAX_BIGINT
    assert asdict(policy.part_limits) == _PART_CEILINGS


@pytest.mark.parametrize(
    "path,maximum",
    [
        *((f"part_limits.{key}", value) for key, value in _PART_CEILINGS.items()),
        *_SCALAR_CEILINGS.items(),
        ("stream_budget.maximum_parts", 131_072),
        ("bundle_budget.maximum_parts", 131_072),
    ],
)
def test_one_above_each_safety_ceiling_is_rejected(path, maximum):
    document = _ceiling_document()
    _set_value(document, path, maximum + 1)
    with pytest.raises(SegmentedCapturePolicyError):
        SegmentedCapturePolicy.from_mapping(document)


@pytest.mark.parametrize("key", _BUDGET_KEYS)
def test_bundle_must_cover_each_stream_budget(key):
    document = _document()
    document["bundle_budget"][key] = document["stream_budget"][key] - 1
    with pytest.raises(SegmentedCapturePolicyError):
        SegmentedCapturePolicy.from_mapping(document)


@pytest.mark.parametrize(
    "key,part",
    [
        ("maximum_compressed_bytes", 1_024),
        ("maximum_decoded_bytes", 4_096),
        ("maximum_arrow_bytes", 8_192),
        ("maximum_records", 10),
        ("maximum_manifest_bytes", 1_024),
    ],
)
def test_stream_must_cover_one_maximum_part(key, part):
    document = _document()
    document["stream_budget"][key] = part - 1
    with pytest.raises(SegmentedCapturePolicyError):
        SegmentedCapturePolicy.from_mapping(document)


def test_dataset_retained_bytes_cover_both_payload_and_manifest_without_overflow():
    document = _document()
    document["maximum_dataset_retained_bytes"] = (
        document["bundle_budget"]["maximum_compressed_bytes"] + document["bundle_budget"]["maximum_manifest_bytes"] - 1
    )
    with pytest.raises(SegmentedCapturePolicyError):
        SegmentedCapturePolicy.from_mapping(document)
    document = _document()
    document["bundle_budget"]["maximum_compressed_bytes"] = _MAX_BIGINT
    document["maximum_dataset_retained_bytes"] = _MAX_BIGINT
    with pytest.raises(SegmentedCapturePolicyError):
        SegmentedCapturePolicy.from_mapping(document)


def test_largest_nonoverflowing_retained_sum_is_accepted():
    document = _document()
    manifest_bytes = document["bundle_budget"]["maximum_manifest_bytes"]
    document["bundle_budget"]["maximum_compressed_bytes"] = _MAX_BIGINT - manifest_bytes
    document["maximum_dataset_retained_bytes"] = _MAX_BIGINT
    assert SegmentedCapturePolicy.from_mapping(document).maximum_dataset_retained_bytes == _MAX_BIGINT


def test_existing_part_limit_coherence_is_preserved_without_inventing_new_relationships():
    document = _document()
    document["part_limits"]["maximum_decoded_bytes"] = 255
    with pytest.raises(SegmentedCapturePolicyError):
        SegmentedCapturePolicy.from_mapping(document)
    document = _document()
    document["part_limits"]["read_chunk_bytes"] = 64 * 1024 * 1024
    document["maximum_part_arrow_bytes"] = 1
    assert SegmentedCapturePolicy.from_mapping(document).maximum_part_arrow_bytes == 1


@pytest.mark.parametrize("path", _NUMERIC_PATHS)
def test_each_declared_limit_contributes_to_identity(path):
    document = _document()
    original = SegmentedCapturePolicy.from_mapping(document)
    parent, _, key = path.partition(".")
    value = document[parent][key] if key else document[parent]
    _set_value(document, path, value + 1)
    changed = SegmentedCapturePolicy.from_mapping(document)
    assert changed.digest != original.digest
    assert changed.canonical != original.canonical


def test_direct_construction_cannot_bypass_validation():
    policy = SegmentedCapturePolicy.from_mapping(_document())
    with pytest.raises(SegmentedCapturePolicyError):
        replace(policy, acquisition_deadline_seconds=True)
    with pytest.raises(SegmentedCapturePolicyError):
        replace(policy, part_limits=CaptureLimits(maximum_compressed_bytes=65 * 1024 * 1024))
    with pytest.raises(SegmentedCapturePolicyError):
        replace(policy, part_limits={})


def test_legacy_capture_limits_defaults_and_behavior_are_unchanged():
    assert asdict(CaptureLimits()) == {
        "maximum_compressed_bytes": 64 * 1024 * 1024,
        "maximum_decoded_bytes": 256 * 1024 * 1024,
        "maximum_record_bytes": 1024 * 1024,
        "maximum_records": 1_000_000,
        "maximum_fields_per_record": 1_024,
        "read_chunk_bytes": 64 * 1024,
    }
    legacy = CaptureLimits(**{key: maximum + 1 for key, maximum in _PART_CEILINGS.items()})
    assert legacy.maximum_records == 1_000_001
    assert legacy.read_chunk_bytes == 64 * 1024 * 1024 + 1
