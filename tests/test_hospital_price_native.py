# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import gzip
import hashlib
import importlib.util
import json
import sys
import zipfile
from pathlib import Path

import pytest


MODULE_PATH = Path(__file__).parents[1] / "process/hospital_price_native.py"
MODULE_SPEC = importlib.util.spec_from_file_location(
    "hospital_price_native_isolated", MODULE_PATH
)
assert MODULE_SPEC is not None and MODULE_SPEC.loader is not None
native = importlib.util.module_from_spec(MODULE_SPEC)
sys.modules[MODULE_SPEC.name] = native
MODULE_SPEC.loader.exec_module(native)


def _csv(headers):
    return (
        "hospital_name,version\nExample,3.0.0\n"
        + ",".join(headers)
        + "\n"
    ).encode()


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        (b'\xef\xbb\xbf {"version":"3.0.0"}', "json"),
        (_csv(["description", "payer_name"]), "csv-tall"),
        (
            _csv([
                "description",
                "standard_charge|Payer|Plan|negotiated_dollar",
            ]),
            "csv-wide",
        ),
    ],
)
def test_format_detection_plain_and_gzip(tmp_path, payload, expected):
    plain = tmp_path / "input"
    plain.write_bytes(payload)
    compressed = tmp_path / "input.gz"
    compressed.write_bytes(gzip.compress(payload))
    archived = tmp_path / "input.zip"
    with zipfile.ZipFile(archived, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("folder/", b"")
        archive.writestr("prices.mrf", payload)

    assert native.detect_hospital_mrf_format(plain) == expected
    assert native.detect_hospital_mrf_format(compressed) == expected
    assert native.detect_hospital_mrf_format(archived) == expected


def test_format_detection_rejects_empty_multiple_and_encrypted_zips(tmp_path):
    empty = tmp_path / "empty.zip"
    zipfile.ZipFile(empty, "w").close()

    multiple = tmp_path / "multiple.zip"
    with zipfile.ZipFile(multiple, "w") as archive:
        archive.writestr("one.json", b"{}")
        archive.writestr("two.json", b"{}")

    encrypted = tmp_path / "encrypted.zip"
    with zipfile.ZipFile(encrypted, "w") as archive:
        archive.writestr("prices.json", b"{}")
    encrypted_bytes = bytearray(encrypted.read_bytes())
    central = encrypted_bytes.index(b"PK\x01\x02")
    encrypted_bytes[6] |= 1
    encrypted_bytes[central + 8] |= 1
    encrypted.write_bytes(encrypted_bytes)

    for path in (empty, multiple, encrypted):
        with pytest.raises(ValueError, match="ZIP"):
            native.detect_hospital_mrf_format(path)


def test_format_detection_bounds_zip_expansion_and_csv_headers(tmp_path):
    oversized_zip = tmp_path / "oversized.zip"
    with zipfile.ZipFile(oversized_zip, "w", zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("prices.csv", b"x" * 1024)
    with pytest.raises(ValueError, match="decompressed size"):
        native.detect_hospital_mrf_format(
            oversized_zip, max_decompressed_bytes=512
        )

    giant_header = tmp_path / "giant.csv.gz"
    payload = b"hospital_name\nExample\n" + b"x" * 1024 + b",payer_name\n"
    giant_header.write_bytes(gzip.compress(payload))
    with pytest.raises(ValueError):
        native.detect_hospital_mrf_format(
            giant_header, max_decompressed_bytes=512
        )


def test_ein_is_only_derived_from_a_filename_prefix():
    assert native.hospital_ein_from_mrf_url(
        "https://example/12-3456789_Hospital_standardcharges.json?download=1"
    ) == "123456789"
    assert native.hospital_ein_from_mrf_url(
        "https://example/Hospital_123456789_standardcharges.json"
    ) is None


def test_native_summary_is_path_and_contract_bound(tmp_path):
    artifacts = []
    for kind in native.HOSPITAL_MRF_COPY_COLUMNS:
        path = tmp_path / f"{kind}.copy"
        row_count = 1 if kind in native._REQUIRED_NONEMPTY_RELATIONS else 0
        artifact_bytes = b"row\n" if row_count else b""
        path.write_bytes(artifact_bytes)
        artifacts.append(
            {
                "kind": kind,
                "path": str(path),
                "rows": row_count,
                "bytes": len(artifact_bytes),
                "sha256": hashlib.sha256(artifact_bytes).hexdigest(),
            }
        )
    version_id = "a" * 64
    summary_by_field = {
        "contract": "hospital-mrf-copy-v3",
        "version_id": version_id,
        "schema_version": "3.0.0",
        "schema_revision": native.HOSPITAL_MRF_SCHEMA_REVISION,
        "format": "json",
        "compressed_input_bytes": 123,
        "max_fanout_rows": 100_000,
        "max_decompressed_bytes": 2048,
        "max_output_bytes": 1024,
        "artifacts": artifacts,
    }

    receipt = native.validate_hospital_parser_summary(
        json.dumps(summary_by_field).encode(),
        version_id=version_id,
        source_format="json",
        input_bytes=123,
        output_directory=tmp_path,
        max_decompressed_bytes=2048,
        max_output_bytes=1024,
    )

    assert receipt.version_id == version_id
    assert receipt.source_format == "json"
    assert len(receipt.artifacts) == 11
    assert receipt.max_fanout_rows == 100_000

    summary_by_field["artifacts"][0]["path"] = str(tmp_path.parent / "mrf.copy")
    with pytest.raises(ValueError, match="unsafe path"):
        native.validate_hospital_parser_summary(
            json.dumps(summary_by_field).encode(),
            version_id=version_id,
            source_format="json",
            input_bytes=123,
            output_directory=tmp_path,
            max_decompressed_bytes=2048,
            max_output_bytes=1024,
        )


def test_version_identity_is_bound_to_content_and_parser_contract():
    content = "b" * 64

    assert native.hospital_price_version_id(content) == native.hospital_price_version_id(content)
    assert native.hospital_price_version_id(content) != native.hospital_price_version_id("c" * 64)
    with pytest.raises(ValueError, match="SHA-256"):
        native.hospital_price_version_id("not-a-digest")
