# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Legacy source-version boundaries for native hospital parser receipts."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from support import hospital_price_native_validation as validation
from tests.test_hospital_price_native import (
    _packed_summary,
    _validate_packed_summary,
)


@pytest.mark.parametrize("schema_version", ("2.2.0", "2.2.1", "3.0.0"))
def test_native_summary_accepts_supported_source_schema_versions(
    tmp_path, schema_version
):
    summary = _packed_summary(tmp_path)
    summary["schema_version"] = schema_version

    assert (
        _validate_packed_summary(summary, tmp_path).schema_version == schema_version
    )


@pytest.mark.parametrize("schema_version", ("1", "1.0.0", "2", "2.0.0"))
@pytest.mark.parametrize("source_format", ("csv-tall", "csv-wide"))
def test_native_summary_accepts_csv_v2_versions_without_admitting_json(
    tmp_path, source_format, schema_version
):
    summary = _packed_summary(tmp_path)
    summary.update(schema_version=schema_version, format=source_format)

    assert _validate_packed_summary(summary, tmp_path).schema_version == schema_version

    summary["format"] = "json"
    with pytest.raises(ValueError, match="contract"):
        _validate_packed_summary(summary, tmp_path)


def _v4_v2_summary(tmp_path, *, metadata=None):
    summary = _packed_summary(tmp_path)
    summary.update(schema_version="4.0.0", format="csv-tall")
    if metadata is None:
        metadata = (
            "\t".join(("a" * 64, "Example", "2025-04-25", "4.0.0",
                       validation._CMS_V2_AFFIRMATION_TEXT, "true", r"\N", r"\N"))
            + "\n"
        ).encode()
    for kind, payload, rows in (("npi", b"", 0), ("mrf", metadata, 1)):
        artifact = next(item for item in summary["artifacts"] if item["kind"] == kind)
        Path(artifact["path"]).write_bytes(payload)
        artifact.update(rows=rows, bytes=len(payload), sha256=hashlib.sha256(payload).hexdigest())
    return summary


@pytest.mark.parametrize("source_format", ("csv-tall", "csv-wide"))
def test_native_v4_v2_requires_exact_artifact_profile(tmp_path, source_format, monkeypatch):
    summary = _v4_v2_summary(tmp_path)
    summary["format"] = source_format
    receipt = _validate_packed_summary(summary, tmp_path)
    assert receipt.schema_version == "4.0.0"
    assert receipt.artifact("npi").rows == 0

    monkeypatch.setattr(validation, "_PROFILE_METADATA_MAX_BYTES", 10)
    with pytest.raises(ValueError, match="v3 NPI"):
        _validate_packed_summary(summary, tmp_path)


@pytest.mark.parametrize(
    ("field", "replacement"),
    [(0, "b" * 64), (3, "2.0.0"), (4, "not exact affirmation"), (5, "1")],
)
def test_native_v4_v2_rejects_wrong_metadata(tmp_path, field, replacement):
    summary = _v4_v2_summary(tmp_path)
    metadata = next(item for item in summary["artifacts"] if item["kind"] == "mrf")
    fields = Path(metadata["path"]).read_text().rstrip("\n").split("\t")
    fields[field] = replacement
    summary = _v4_v2_summary(tmp_path, metadata=("\t".join(fields) + "\n").encode())
    with pytest.raises(ValueError, match="v3 NPI"):
        _validate_packed_summary(summary, tmp_path)


def test_native_v4_v2_rejects_unbound_or_multiple_metadata_rows(tmp_path):
    summary = _v4_v2_summary(tmp_path)
    metadata = next(item for item in summary["artifacts"] if item["kind"] == "mrf")
    payload = Path(metadata["path"]).read_bytes()
    Path(metadata["path"]).write_bytes(payload.replace(b"Example", b"Changed"))
    with pytest.raises(ValueError, match="artifact mrf is invalid"):
        _validate_packed_summary(summary, tmp_path)
    for payload in (payload + payload, payload.rstrip(b"\n")):
        summary = _v4_v2_summary(tmp_path, metadata=payload)
        with pytest.raises(ValueError, match="v3 NPI"):
            _validate_packed_summary(summary, tmp_path)


@pytest.mark.parametrize("schema_version", ("3.0.1", "4.0.0"))
@pytest.mark.parametrize("source_format", ("csv-tall", "csv-wide"))
def test_native_summary_accepts_producer_declared_current_csv_only(
    tmp_path, source_format, schema_version
):
    summary = _packed_summary(tmp_path)
    summary.update(schema_version=schema_version, format=source_format)

    assert _validate_packed_summary(summary, tmp_path).schema_version == schema_version

    summary["format"] = "json"
    with pytest.raises(ValueError, match="contract"):
        _validate_packed_summary(summary, tmp_path)


def test_native_summary_allows_legacy_without_npi_but_keeps_v3_strict(tmp_path):
    summary = _packed_summary(tmp_path)
    npi = next(artifact for artifact in summary["artifacts"] if artifact["kind"] == "npi")
    Path(npi["path"]).write_bytes(b"")
    npi.update(rows=0, bytes=0, sha256=hashlib.sha256(b"").hexdigest())
    summary["schema_version"] = "2.2.0"

    assert _validate_packed_summary(summary, tmp_path).artifact("npi").rows == 0

    summary["schema_version"] = "3.0.0"
    with pytest.raises(ValueError, match="v3 NPI"):
        _validate_packed_summary(summary, tmp_path)

    for schema_version in ("3.0.1", "4.0.0"):
        summary.update(schema_version=schema_version, format="csv-tall")
        with pytest.raises(ValueError, match="v3 NPI"):
            _validate_packed_summary(summary, tmp_path)

    summary.update(schema_version="2.0.0", format="json")
    with pytest.raises(ValueError, match="contract"):
        _validate_packed_summary(summary, tmp_path)
