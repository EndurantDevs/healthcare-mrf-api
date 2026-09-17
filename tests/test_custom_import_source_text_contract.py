# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Cross-boundary retained-text contracts for custom imports."""

from __future__ import annotations

import json
from io import BytesIO
from pathlib import Path
from typing import Any

import pytest

import process.custom_import.capture as capture_module
from process.custom_import import (
    CaptureError,
    CustomImportDefinition,
    DefinitionError,
    SourceSnapshotError,
    capture_stream,
    iter_records,
    load_json_definition,
    validate_source_snapshot_tokens,
)

FIXTURES = Path(__file__).with_name("fixtures") / "custom_import"


def _definition() -> CustomImportDefinition:
    return CustomImportDefinition.from_json((FIXTURES / "v1_valid.json").read_text())


def _raw_definition() -> dict[str, Any]:
    return load_json_definition((FIXTURES / "v1_valid.json").read_text())


def _source_token_observations(token: Any) -> dict[str, list[Any]]:
    return {"providers": [token], "rates": [token]}


@pytest.mark.parametrize(
    ("token", "capture_message", "family_message"),
    (
        (None, "non-empty string", "non-string snapshot token"),
        ("", "non-empty string", "lacks one snapshot token"),
        ("x" * 1025, "byte limit", "byte limit"),
        ("snapshot\u0085token", "control characters", "control characters"),
        ("\ud800", "valid UTF-8", "valid UTF-8"),
    ),
    ids=("non-string", "empty", "1025-utf8-bytes", "unicode-nonprintable", "surrogate"),
)
def test_snapshot_token_contract_matches_capture_and_family(token, capture_message, family_message):
    """Every retained snapshot token has the same acceptance boundary everywhere."""

    definition = _definition()
    stream = definition.source_streams[0]

    with pytest.raises(CaptureError, match=capture_message):
        capture_stream(
            BytesIO(b"Provider ID\n1234567893\n"),
            stream,
            source_snapshot_token=token,
        )
    with pytest.raises(SourceSnapshotError, match=family_message):
        validate_source_snapshot_tokens(definition, _source_token_observations(token))


@pytest.mark.parametrize(
    "token",
    (
        "snapshot caf\u00e9",
        "\u00e9" * 512,
    ),
    ids=("printable-unicode", "1024-utf8-bytes"),
)
def test_snapshot_token_contract_accepts_printable_utf8_at_the_exact_limit(token):
    """Printable Unicode remains valid through the exact shared token limit."""

    definition = _definition()
    capture = capture_stream(
        BytesIO(b"Provider ID\n1234567893\n"),
        definition.source_streams[0],
        source_snapshot_token=token,
    )

    assert capture.manifest.source_snapshot_token == token
    assert validate_source_snapshot_tokens(definition, _source_token_observations(token)) == token


@pytest.mark.parametrize(
    ("label", "is_valid"),
    (
        ("Source caf\u00e9", True),
        ("\u00e9" * 127 + "x", True),
        ("x" * 256, False),
        ("Not\u0085Printable", False),
        ("\ud800", False),
        ("", False),
    ),
    ids=("printable-unicode", "255-utf8-bytes", "256-utf8-bytes", "unicode-nonprintable", "surrogate", "empty"),
)
def test_source_label_contract_matches_definition_aliases_and_decoders(label, is_valid):
    """Definition aliases and decoded labels share one exact retained-text contract."""

    raw = _raw_definition()
    raw["streams"][0]["format"] = "json"
    raw["aliases"]["providers"] = {label: "npi"}
    payload = json.dumps([{label: "1234567893"}], ensure_ascii=True).encode("ascii")

    if not is_valid:
        with pytest.raises(DefinitionError, match="bounded printable"):
            CustomImportDefinition.from_mapping(raw)
        stream = _definition_for_json_decoder().source_streams[0]
        capture = capture_stream(BytesIO(payload), stream, source_snapshot_token="snapshot-1")
        with pytest.raises(CaptureError):
            list(iter_records(capture, stream))
        return

    definition = CustomImportDefinition.from_mapping(raw)
    capture = capture_stream(
        BytesIO(payload),
        definition.source_streams[0],
        source_snapshot_token="snapshot-1",
    )

    assert list(iter_records(capture, definition.source_streams[0]))[0].values[label] == "1234567893"


def _definition_for_json_decoder() -> CustomImportDefinition:
    """Build a stream shape used only to exercise decoder-side label validation."""

    raw = _raw_definition()
    raw["streams"][0]["format"] = "json"
    return CustomImportDefinition.from_mapping(raw)


def test_source_label_contract_rejects_non_string_values_at_each_boundary():
    """Definition input and the decoder-side wrapper both reject non-string labels."""

    raw = _raw_definition()
    raw["aliases"]["providers"] = {1: "npi"}

    with pytest.raises(CaptureError, match="non-empty strings"):
        capture_module._validated_source_label(1)
    with pytest.raises(DefinitionError, match="object keys must be strings"):
        CustomImportDefinition.from_mapping(raw)
