# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Explicit synthetic object policies, closed shapes and canonical identity."""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from dataclasses import FrozenInstanceError, asdict, replace

import pytest

from process.custom_import.capture_limits import CaptureLimits
from process.custom_import.definition import canonical_json
from process.custom_import.object_processing_policy import (
    OBJECT_PROCESSING_POLICY_CONTRACT,
    ObjectProcessingPolicy,
    ObjectProcessingPolicyError,
)
from process.custom_import.prepared_source_codec import prepared_source_row_byte_limit
from process.custom_import.processing_policy import BuildPolicy

_MAX_BIGINT = (1 << 63) - 1
_CAPTURE_CEILINGS = asdict(CaptureLimits())
_PREPARED_CEILINGS = {
    "part_byte_limit": 64 * 1024 * 1024,
    "part_row_limit": 1_000_000,
    "maximum_parts": 131_072,
    "maximum_total_bytes": _MAX_BIGINT,
}
_BUILD_CEILINGS = {
    "page_row_limit": 256,
    "page_byte_limit": 268_435_456,
    "statement_timeout_ms": 2_147_483_647,
    "lease_seconds": 3_600,
    "build_deadline_seconds": 86_400,
}
_CEILING_BY_PATH = {
    **{f"capture_limits.{name}": maximum for name, maximum in _CAPTURE_CEILINGS.items()},
    **{f"prepared.{name}": maximum for name, maximum in _PREPARED_CEILINGS.items()},
    **{f"build.{name}": maximum for name, maximum in _BUILD_CEILINGS.items()},
    "acquisition_timeout_seconds": 3_600,
    "maximum_dataset_retained_bytes": _MAX_BIGINT,
}


def _document():
    return {
        "contract": OBJECT_PROCESSING_POLICY_CONTRACT,
        "capture_limits": {
            "maximum_compressed_bytes": 1_024,
            "maximum_decoded_bytes": 4_096,
            "maximum_record_bytes": 256,
            "maximum_records": 10,
            "maximum_fields_per_record": 4,
            "read_chunk_bytes": 128,
        },
        "acquisition_timeout_seconds": 60,
        "maximum_dataset_retained_bytes": 16_384,
        "prepared": {
            "part_byte_limit": 2_048,
            "part_row_limit": 10,
            "maximum_parts": 3,
            "maximum_total_bytes": 8_192,
        },
        "build": {
            "page_row_limit": 2,
            "page_byte_limit": 4_096,
            "statement_timeout_ms": 1_000,
            "lease_seconds": 60,
            "build_deadline_seconds": 300,
        },
    }


def _set_value(document, path, value):
    parent, separator, name = path.partition(".")
    if separator:
        document[parent][name] = value
    else:
        document[parent] = value


def _ceiling_document():
    document = _document()
    for path, maximum in _CEILING_BY_PATH.items():
        _set_value(document, path, maximum)
    return document


def test_roundtrip_and_identity_are_canonical_and_domain_separated():
    document = _document()
    policy = ObjectProcessingPolicy.from_mapping(document)
    reordered_by_key = dict(reversed(list(document.items())))
    for name in ("capture_limits", "prepared", "build"):
        reordered_by_key[name] = dict(reversed(list(document[name].items())))
    assert ObjectProcessingPolicy.from_mapping(reordered_by_key) == policy
    assert ObjectProcessingPolicy.from_mapping(json.loads(policy.canonical)) == policy
    assert policy.to_mapping() == document
    assert policy.canonical == canonical_json(document)
    encoded = policy.canonical.encode("utf-8")
    assert policy.digest == hashlib.sha256(b"custom-import/object-processing-policy/v1:" + encoded).hexdigest()
    assert policy.digest == "43289d24177924142c4d45818bc2f2534904a3b5bc7d6010d2fd221ac7881a15"
    assert policy.digest != hashlib.sha256(encoded).hexdigest()
    assert policy.digest != hashlib.sha256(b"custom-import/segmented-capture-policy/v1:" + encoded).hexdigest()
    assert type(policy.capture_limits) is CaptureLimits
    assert type(policy.build) is BuildPolicy
    assert policy.build == BuildPolicy.from_mapping(document["build"])


def test_declaration_is_frozen_and_detached_from_input_and_output():
    document = _document()
    expected = deepcopy(document)
    policy = ObjectProcessingPolicy.from_mapping(document)
    document["capture_limits"]["maximum_records"] = 20
    document["prepared"]["maximum_parts"] = 4
    document["build"]["page_row_limit"] = 3
    returned = policy.to_mapping()
    returned["capture_limits"]["maximum_records"] = 30
    returned["prepared"]["maximum_parts"] = 5
    returned["build"]["page_row_limit"] = 4
    assert policy.to_mapping() == expected
    with pytest.raises(TypeError):
        policy.prepared["maximum_parts"] = 4
    with pytest.raises(FrozenInstanceError):
        policy.acquisition_timeout_seconds = 120
    with pytest.raises(FrozenInstanceError):
        policy.capture_limits.maximum_records = 20
    with pytest.raises(FrozenInstanceError):
        policy.build.page_row_limit = 3


@pytest.mark.parametrize("parent", [None, "capture_limits", "prepared", "build"])
@pytest.mark.parametrize("change", ["missing", "unknown"])
def test_every_mapping_is_explicit_and_closed(parent, change):
    document = _document()
    mapping = document if parent is None else document[parent]
    for name in mapping:
        changed = _document()
        target = changed if parent is None else changed[parent]
        if change == "missing":
            del target[name]
        else:
            target[name + "_extra"] = 1
        with pytest.raises(ObjectProcessingPolicyError):
            ObjectProcessingPolicy.from_mapping(changed)


@pytest.mark.parametrize("parent", [None, "capture_limits", "prepared", "build"])
@pytest.mark.parametrize("invalid", [None, True, [], "{}"])
def test_mappings_reject_wrong_container_types(parent, invalid):
    document = _document()
    if parent is None:
        document = invalid
    else:
        document[parent] = invalid
    with pytest.raises(ObjectProcessingPolicyError):
        ObjectProcessingPolicy.from_mapping(document)


@pytest.mark.parametrize("contract", [None, True, 1, b"custom-import/object-processing-policy/v1", "other/v1"])
def test_only_the_declared_contract_is_accepted(contract):
    document = _document()
    document["contract"] = contract
    with pytest.raises(ObjectProcessingPolicyError):
        ObjectProcessingPolicy.from_mapping(document)


@pytest.mark.parametrize("path", _CEILING_BY_PATH)
@pytest.mark.parametrize("invalid", [None, True, False, -1, 1.0, "1"])
def test_every_numeric_field_rejects_wrong_types_and_negative_values(path, invalid):
    document = _document()
    _set_value(document, path, invalid)
    with pytest.raises(ObjectProcessingPolicyError):
        ObjectProcessingPolicy.from_mapping(document)


@pytest.mark.parametrize("path", [path for path in _CEILING_BY_PATH if path != "maximum_dataset_retained_bytes"])
def test_positive_limits_reject_zero(path):
    document = _document()
    _set_value(document, path, 0)
    with pytest.raises(ObjectProcessingPolicyError):
        ObjectProcessingPolicy.from_mapping(document)


@pytest.mark.parametrize("path,maximum", _CEILING_BY_PATH.items())
def test_one_above_every_safety_ceiling_is_rejected(path, maximum):
    document = _ceiling_document()
    _set_value(document, path, maximum + 1)
    with pytest.raises(ObjectProcessingPolicyError):
        ObjectProcessingPolicy.from_mapping(document)


def test_every_maximum_is_inclusive():
    document = _ceiling_document()
    assert ObjectProcessingPolicy.from_mapping(document).to_mapping() == document
    assert document["capture_limits"]["read_chunk_bytes"] == 64 * 1024


def test_minimum_declaration_preserves_zero_retention_and_build_limits():
    document = _document()
    document["capture_limits"] = dict.fromkeys(_CAPTURE_CEILINGS, 1)
    document["build"] = dict.fromkeys(_BUILD_CEILINGS, 1)
    row_bound = prepared_source_row_byte_limit(CaptureLimits(**document["capture_limits"]))
    document["prepared"] = {
        "part_byte_limit": row_bound,
        "part_row_limit": 1,
        "maximum_parts": 1,
        "maximum_total_bytes": row_bound,
    }
    document["acquisition_timeout_seconds"] = 1
    document["maximum_dataset_retained_bytes"] = 0
    assert ObjectProcessingPolicy.from_mapping(document).to_mapping() == document


@pytest.mark.parametrize("defect", ["decoded_bytes", "prepared_total", "row_envelope"])
def test_required_budget_relationships_are_enforced(defect):
    document = _document()
    if defect == "decoded_bytes":
        document["capture_limits"]["maximum_decoded_bytes"] = document["capture_limits"]["maximum_record_bytes"] - 1
    elif defect == "prepared_total":
        document["prepared"]["maximum_total_bytes"] = document["prepared"]["part_byte_limit"] - 1
    else:
        document["prepared"]["part_byte_limit"] = (
            prepared_source_row_byte_limit(CaptureLimits(**document["capture_limits"])) - 1
        )
    with pytest.raises(ObjectProcessingPolicyError):
        ObjectProcessingPolicy.from_mapping(document)


def test_no_unstated_quota_or_capacity_relationships_are_added():
    document = _document()
    document["prepared"]["part_row_limit"] = 1
    document["prepared"]["maximum_parts"] = 1
    document["prepared"]["maximum_total_bytes"] = document["prepared"]["part_byte_limit"]
    document["maximum_dataset_retained_bytes"] = 0
    assert ObjectProcessingPolicy.from_mapping(document).to_mapping() == document


@pytest.mark.parametrize("path", _CEILING_BY_PATH)
def test_every_limit_contributes_to_identity(path):
    document = _document()
    original = ObjectProcessingPolicy.from_mapping(document)
    parent, separator, name = path.partition(".")
    value = document[parent][name] if separator else document[parent]
    _set_value(document, path, value + 1)
    changed = ObjectProcessingPolicy.from_mapping(document)
    assert changed.digest != original.digest
    assert changed.canonical != original.canonical


def test_direct_construction_cannot_bypass_validation():
    policy = ObjectProcessingPolicy.from_mapping(_document())
    for changes in (
        {"capture_limits": policy.to_mapping()["capture_limits"]},
        {"capture_limits": CaptureLimits(maximum_records=1_000_001)},
        {"prepared": {}},
        {"build": policy.build.to_mapping()},
        {"acquisition_timeout_seconds": True},
        {"maximum_dataset_retained_bytes": False},
        {"maximum_dataset_retained_bytes": _MAX_BIGINT + 1},
    ):
        with pytest.raises(ObjectProcessingPolicyError):
            replace(policy, **changes)


@pytest.mark.parametrize("field", ["driver_timeout_seconds", "maximum_part_arrow_bytes", "sql"])
def test_transport_specific_fields_do_not_enter_object_policy(field):
    document = _document()
    document[field] = 1
    with pytest.raises(ObjectProcessingPolicyError):
        ObjectProcessingPolicy.from_mapping(document)
