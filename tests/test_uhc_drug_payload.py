# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from pathlib import Path

import pytest

import process.formulary_fhir.uhc_drug_payload as payload
from process.formulary_fhir.uhc_drug_payload import UHCDrugPayloadError
from process.formulary_fhir.uhc_drug_payload import (
    uhc_drug_object_array_item_count,
)


def test_payload_accepts_complete_object_array(tmp_path: Path) -> None:
    source_path = tmp_path / "drugs.json"
    source_path.write_bytes(b'[{"nested":{"rows":[1,2]}},{"value":null}]')

    assert uhc_drug_object_array_item_count(source_path) == 2


@pytest.mark.parametrize(
    "payload_bytes",
    [
        b"{}",
        b"1",
        b"[]",
        b"[1]",
        b"[{",
        b"[{}]{}",
        b"[[{}]]",
    ],
)
def test_payload_rejects_incomplete_or_nonobject_arrays(
    tmp_path: Path,
    payload_bytes: bytes,
) -> None:
    source_path = tmp_path / "invalid.json"
    source_path.write_bytes(payload_bytes)

    with pytest.raises(UHCDrugPayloadError, match="JSON structure"):
        uhc_drug_object_array_item_count(source_path)


def test_payload_sanitizes_unavailable_file_error(tmp_path: Path) -> None:
    with pytest.raises(UHCDrugPayloadError, match="JSON structure"):
        uhc_drug_object_array_item_count(tmp_path / "missing.json")


@pytest.mark.parametrize(
    "payload_bytes",
    (
        b'[{"rxnorm_id":"1","rxnorm_id":"2"}]',
        b'[{"plans":[{"years":[2026],"years":[2027]}]}]',
        b'[{"extension":{"flag":true,"flag":false}}]',
    ),
)
def test_payload_rejects_duplicate_keys_at_every_object_depth(
    tmp_path: Path,
    payload_bytes: bytes,
) -> None:
    source_path = tmp_path / "duplicate.json"
    source_path.write_bytes(payload_bytes)

    with pytest.raises(UHCDrugPayloadError, match="JSON structure"):
        uhc_drug_object_array_item_count(source_path)


def test_payload_enforces_scalar_and_record_byte_budgets(
    monkeypatch,
    tmp_path: Path,
) -> None:
    scalar_path = tmp_path / "scalar.json"
    scalar_path.write_bytes(b'[{"value":"oversized"}]')
    monkeypatch.setattr(payload, "MAX_JSON_SCALAR_BYTES", 4)
    with pytest.raises(UHCDrugPayloadError, match="JSON structure"):
        uhc_drug_object_array_item_count(scalar_path)

    record_path = tmp_path / "record.json"
    record_path.write_bytes(b'[{"a":"b","c":"d"}]')
    monkeypatch.setattr(payload, "MAX_JSON_SCALAR_BYTES", 1_024)
    monkeypatch.setattr(payload, "MAX_JSON_RECORD_BYTES", 8)
    with pytest.raises(UHCDrugPayloadError, match="JSON structure"):
        uhc_drug_object_array_item_count(record_path)
