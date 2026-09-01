# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Legacy source-version boundaries for native hospital parser receipts."""

from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

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

    assert _validate_packed_summary(summary, tmp_path).schema_version == schema_version


@pytest.mark.parametrize("schema_version", ("2", "2.0.0"))
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

    summary["schema_version"] = "2.0.0"
    with pytest.raises(ValueError, match="contract"):
        _validate_packed_summary(summary, tmp_path)
