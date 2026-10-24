# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic reusable object approvals, without acquisition or credentials."""

from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import asdict, replace
from pathlib import Path

import pytest

from process.custom_import import https, s3, upload
from process.custom_import.capture_limits import CaptureLimits
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.object_source_binding import ObjectSourceBinding, ObjectSourceBindingError


def _policy():
    return {
        "contract": "custom-import/object-processing-policy/v1",
        "capture_limits": asdict(CaptureLimits()),
        "acquisition_timeout_seconds": 300,
        "maximum_dataset_retained_bytes": 1024 * 1024 * 1024,
        "prepared": {
            "part_byte_limit": 8 * 1024 * 1024,
            "part_row_limit": 256,
            "maximum_parts": 1024,
            "maximum_total_bytes": 512 * 1024 * 1024,
        },
        "build": {
            "page_row_limit": 128,
            "page_byte_limit": 8 * 1024 * 1024,
            "statement_timeout_ms": 1000,
            "lease_seconds": 60,
            "build_deadline_seconds": 3600,
        },
    }


def _definition():
    fixture = Path(__file__).with_name("fixtures") / "custom_import" / "v1_valid.json"
    return CustomImportDefinition.from_json(fixture.read_text())


def _document(connector="https"):
    locator = {
        "https": {"url": "https://DOWNLOADS.example:443/data%2Frecords?day=current"},
        "s3": {
            "endpoint": "https://OBJECTS.example:443/",
            "region": "us-east-1",
            "bucket": "synthetic-bucket",
            "key": "daily/é+%/../records.csv",
            "addressing": "path",
        },
        "upload": {"source_id": "approved_upload_channel"},
    }[connector]
    return {
        "contract": "custom-import/source-binding/v3",
        "connector": connector,
        "definition_sha256": "1" * 64,
        "schema_sha256": "2" * 64,
        "source_object": {"fingerprint_sha256": "3" * 64, "version": "approved-v1"},
        "streams": [{"stream_id": stream_id, **locator} for stream_id in ("rates", "providers")],
        "processing_policy": _policy(),
    }


@pytest.mark.parametrize("connector", ["https", "s3", "upload"])
def test_canonical_binding_keeps_exact_locations_and_separate_digest_domain(connector):
    document = _document(connector)
    expected = copy.deepcopy(document)
    expected["streams"].reverse()
    if connector == "s3":
        for stream in expected["streams"]:
            stream["endpoint"] = "https://objects.example"
    canonical = json.dumps(expected, ensure_ascii=False, allow_nan=False, separators=(",", ":"), sort_keys=True)
    binding = ObjectSourceBinding.from_mapping(document)
    assert binding.to_mapping() == expected
    assert binding.canonical == canonical
    assert binding.digest == hashlib.sha256(b"custom-import/source-binding/v3:" + canonical.encode()).hexdigest()
    assert ObjectSourceBinding.from_json(binding.canonical) == binding
    assert ObjectSourceBinding.from_json(binding.canonical.encode()) == binding
    document["streams"].reverse()
    assert ObjectSourceBinding.from_mapping(document) == binding


def test_nested_input_and_output_mutation_cannot_change_a_binding():
    document = _document()
    binding = ObjectSourceBinding.from_mapping(document)
    unchanged = binding.canonical
    document["source_object"]["version"] = "changed"
    document["streams"][0]["url"] = "https://changed.example"
    document["processing_policy"]["prepared"]["part_row_limit"] = 1
    exported = binding.to_mapping()
    exported["streams"][0]["url"] = "https://changed.example"
    exported["source_object"]["version"] = "changed"
    exported["processing_policy"]["build"]["page_row_limit"] = 1
    assert ObjectSourceBinding.from_json(unchanged) == binding
    with pytest.raises(TypeError):
        binding.streams[0]["url"] = "https://changed.example"
    with pytest.raises(TypeError):
        binding.source_object["version"] = "changed"


def test_definition_validation_requires_both_digests_and_exact_stream_coverage():
    definition = _definition()
    document = _document()
    document.update(definition_sha256=definition.digest, schema_sha256=definition.schema_digest)
    binding = ObjectSourceBinding.from_mapping(document)
    assert binding.validate_definition(definition) is None
    for changed in (
        replace(binding, definition_sha256="4" * 64),
        replace(binding, schema_sha256="4" * 64),
        replace(binding, streams=binding.streams[:1]),
        replace(binding, streams=(*binding.streams, {"stream_id": "extra", "url": "https://source.example"})),
    ):
        with pytest.raises(ObjectSourceBindingError, match="^object_source_binding_invalid$"):
            changed.validate_definition(definition)
    with pytest.raises(ObjectSourceBindingError):
        binding.validate_definition(None)


@pytest.mark.parametrize(
    "field,value",
    [
        ("contract", "custom-import/source-binding/v2"),
        ("connector", "snowflake_bundle"),
        ("connector", []),
        ("definition_sha256", "A" * 64),
        ("schema_sha256", None),
        ("source_object", {"fingerprint_sha256": "3" * 64, "version": "../private"}),
        ("source_object", {"fingerprint_sha256": "invalid", "version": "approved-v1"}),
        ("source_object", {"fingerprint_sha256": "3" * 64, "version": "approved-v1", "extra": 1}),
        ("streams", []),
        ("streams", ({"stream_id": "providers", "url": "https://source.example"},)),
        ("streams", [{"stream_id": "bad/name", "url": "https://source.example"}]),
        ("streams", [{"stream_id": "providers", "url": "https://source.example"}] * 2),
        ("streams", [{"stream_id": f"stream_{index}", "url": "https://source.example"} for index in range(10)]),
        ("processing_policy", {}),
    ],
)
def test_invalid_binding_shapes_are_redacted(field, value):
    document = _document()
    document[field] = value
    with pytest.raises(ObjectSourceBindingError, match="^object_source_binding_invalid$") as caught:
        ObjectSourceBinding.from_mapping(document)
    assert caught.value.__cause__ is None and caught.value.__suppress_context__


@pytest.mark.parametrize("connector", ["https", "s3", "upload"])
@pytest.mark.parametrize("field", ["expected_sha256", "expected_bytes", "source_snapshot_token", "version_id"])
def test_reusable_binding_rejects_per_run_pins(connector, field):
    document = _document(connector)
    document["streams"][0][field] = "synthetic"
    with pytest.raises(ObjectSourceBindingError):
        ObjectSourceBinding.from_mapping(document)


@pytest.mark.parametrize("field", ["role", "warehouse", "snapshot_token_mode", "decimal_conversions", "entity_limit"])
def test_snowflake_fields_cannot_enter_the_object_variant(field):
    document = _document()
    document[field] = "synthetic"
    with pytest.raises(ObjectSourceBindingError):
        ObjectSourceBinding.from_mapping(document)


@pytest.mark.parametrize(
    "connector,changes",
    [
        ("https", {"url": "http://source.example/private"}),
        ("https", {"url": "https://owner@source.example/private"}),
        ("https", {"url": "https://127.0.0.1/private"}),
        ("https", {"url": "https://source.example/private#fragment"}),
        ("s3", {"endpoint": "https://objects.example/prefix"}),
        ("s3", {"region": "region/*"}),
        ("s3", {"bucket": "bad..bucket"}),
        ("s3", {"key": "\x00"}),
        ("s3", {"addressing": "virtual"}),
        ("upload", {"source_id": "../private"}),
        ("upload", {"source_id": ""}),
        ("upload", {"source_id": "x" * 129}),
    ],
)
def test_source_location_rules_match_existing_transports(connector, changes):
    document = _document(connector)
    document["streams"][0].update(changes)
    with pytest.raises(ObjectSourceBindingError, match="^object_source_binding_invalid$"):
        ObjectSourceBinding.from_mapping(document)


@pytest.mark.parametrize("serialized", ["[]", "{", '{"contract":"one","contract":"two"}', "x" * (1024 * 1024 + 1)])
def test_json_boundary_is_bounded_and_rejects_duplicate_keys(serialized):
    with pytest.raises(ObjectSourceBindingError, match="^object_source_binding_invalid$"):
        ObjectSourceBinding.from_json(serialized)


def test_binding_validation_has_no_acquisition_or_credential_side_effects(monkeypatch):
    def forbidden(*_args, **_kwargs):
        raise AssertionError("binding validation must not acquire source data")

    for module, name in ((https, "acquire_https"), (s3, "acquire_s3"), (s3, "_signed_url"), (upload, "acquire_upload")):
        monkeypatch.setattr(module, name, forbidden)
    for connector in ("https", "s3", "upload"):
        binding = ObjectSourceBinding.from_mapping(_document(connector))
        assert "example" not in repr(binding)
        assert binding.connector == connector


def test_s3_locator_extraction_keeps_object_normalization_and_identity_bytes():
    document = _document("s3")
    locator = document["streams"][0]
    source = s3.S3Object(**locator, expected_sha256="4" * 64, expected_bytes=15)
    assert source.endpoint == s3._validated_locator(
        **{key: value for key, value in locator.items() if key != "stream_id"}
    )
    canonical = json.dumps(
        {"contract": "custom-import/s3-object/v1", **asdict(source)},
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    assert (
        s3.s3_object_identity_sha256(source)
        == hashlib.sha256(b"custom-import/s3-object/v1\0object\0" + canonical.encode()).digest()
    )
