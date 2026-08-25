# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import gzip
import hashlib
import importlib.util
import io
import json
import sys
import zipfile
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from tests.hospital_price_control_support import acquisition_module


MODULE_PATH = Path(__file__).parents[1] / "process/hospital_price_native.py"
MODULE_SPEC = importlib.util.spec_from_file_location(
    "hospital_price_native_isolated", MODULE_PATH
)
assert MODULE_SPEC is not None and MODULE_SPEC.loader is not None
native = importlib.util.module_from_spec(MODULE_SPEC)
sys.modules[MODULE_SPEC.name] = native
MODULE_SPEC.loader.exec_module(native)
_acquisition_module = acquisition_module


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
    with pytest.raises(ValueError, match="read limit"):
        native.detect_hospital_mrf_format(
            giant_header, max_decompressed_bytes=512
        )


def test_format_detection_rejects_unsupported_and_malformed_payloads(tmp_path):
    unsupported = tmp_path / "unsupported.zip"
    with zipfile.ZipFile(unsupported, "w", zipfile.ZIP_BZIP2) as archive:
        archive.writestr("prices.json", b"{}")
    invalid_zip = tmp_path / "invalid.zip"
    invalid_zip.write_bytes(b"not a ZIP")
    empty = tmp_path / "empty"
    empty.write_bytes(b"")
    incomplete_csv = tmp_path / "incomplete.csv"
    incomplete_csv.write_bytes(b"hospital_name\nExample\n")
    unknown_csv = tmp_path / "unknown.csv"
    unknown_csv.write_bytes(_csv(["description", "gross_charge"]))

    for path, message in (
        (unsupported, "compression method"),
        (invalid_zip, "input is invalid"),
        (empty, "input is empty"),
        (incomplete_csv, "three header rows"),
        (unknown_csv, "payer layout"),
    ):
        with pytest.raises(ValueError, match=message):
            native.detect_hospital_mrf_format(path)


def test_bounded_payload_and_decompression_limit_edges(monkeypatch):
    bounded = native._BoundedPayload(io.BytesIO(b""), 0)
    assert bounded.readinto(bytearray()) == 0
    assert bounded.readinto(bytearray(1)) == 0

    for value in ("invalid", "0"):
        monkeypatch.setenv(native.HOSPITAL_MRF_MAX_DECOMPRESSED_BYTES_ENV, value)
        with pytest.raises(ValueError, match="positive integer"):
            native._max_decompressed_bytes()


def test_ein_is_only_derived_from_a_filename_prefix():
    assert native.hospital_ein_from_mrf_url(
        "https://example/12-3456789_Hospital_standardcharges.json?download=1"
    ) == "123456789"
    assert native.hospital_ein_from_mrf_url(
        "https://example/Hospital_123456789_standardcharges.json"
    ) is None


def test_acquisition_helper_contracts(monkeypatch):
    acquisition = _acquisition_module()

    assert acquisition.schema_name() == "mrf"
    monkeypatch.setenv("HOSPITAL_TEST_LIMIT", "7")
    assert acquisition.positive_env("HOSPITAL_TEST_LIMIT", 3) == 7
    monkeypatch.setenv("HOSPITAL_TEST_LIMIT", "invalid")
    assert acquisition.positive_env("HOSPITAL_TEST_LIMIT", 3) == 3
    assert acquisition.error_details(ValueError()) == ("value", "value")


@pytest.mark.asyncio
async def test_registry_sync_copies_deduplicated_locators_and_checks_storage():
    acquisition = _acquisition_module()
    driver = SimpleNamespace(copy_records_to_table=AsyncMock())
    connection = SimpleNamespace(
        raw_connection=SimpleNamespace(driver_connection=driver),
        scalar=AsyncMock(return_value=True), status=AsyncMock(),
    )

    @asynccontextmanager
    async def acquire():
        yield connection

    acquisition.db.acquire = acquire
    hospitals = (
        {"hospital_id": "a", "name": "A", "cms_hpt_url": "https://a/cms-hpt.txt"},
        {"hospital_id": "b", "name": "B", "cms_hpt_url": "https://a/cms-hpt.txt"},
    )
    await acquisition.sync_registry(hospitals)

    copied_record_counts = [
        len(call.kwargs["records"])
        for call in driver.copy_records_to_table.await_args_list
    ]
    assert copied_record_counts == [1, 2]
    assert connection.status.await_count == 5

    connection.scalar.return_value = False
    with pytest.raises(RuntimeError, match="migration is not installed"):
        await acquisition.sync_registry(hospitals)


@pytest.mark.asyncio
async def test_locator_observation_persists_bounded_evidence():
    acquisition = _acquisition_module()
    acquisition.db.status = AsyncMock()
    raw = SimpleNamespace(
        head=SimpleNamespace(url="https://a/final", status=302),
        raw_sha256="a" * 64, byte_count=12,
    )

    await acquisition._record_locator_observation(
        "https://a/cms-hpt.txt", "locator", "observation", "verified", raw
    )
    await acquisition._record_locator_observation(
        "https://b/cms-hpt.txt", "locator-2", "observation-2", "fetch_failed"
    )

    first_call, second_call = acquisition.db.status.await_args_list
    assert first_call.kwargs["final_url"] == "https://a/final"
    assert first_call.kwargs["http_status"] == 302
    assert second_call.kwargs["final_url"] is None
    assert second_call.kwargs["sha256"] is None


def test_invalid_locator_becomes_an_explicit_hospital_candidate():
    acquisition = _acquisition_module()
    locator_result = acquisition.LocatorResult(
        "https://a/cms-hpt.txt", "locator", "observation",
        ({"hospital_id": "a", "name": "Hospital A"},), None,
    )

    candidate = acquisition.candidates_from_locators((locator_result,))[0]

    assert candidate.hospital_id == "a"
    assert candidate.locator_name == "Hospital A"
    assert candidate.initial_error_code == "locator_invalid"


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
    assert receipt.artifact("mrf").kind == "mrf"

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


def test_native_summary_rejects_invalid_receipts_and_artifacts(tmp_path):
    version_id = "a" * 64
    options_by_name = {
        "version_id": version_id,
        "source_format": "json",
        "input_bytes": 1,
        "output_directory": tmp_path,
        "max_decompressed_bytes": 2,
        "max_output_bytes": 1,
    }
    for summary_bytes, message in (
        (b"{}", "shape"),
        (b"not json", "invalid JSON"),
        (b"x" * 1_000_001, "oversized"),
    ):
        with pytest.raises(ValueError, match=message):
            native.validate_hospital_parser_summary(
                summary_bytes, **options_by_name
            )
    with pytest.raises(ValueError, match="identity"):
        native.validate_hospital_parser_summary(
            b"{}", **(options_by_name | {"version_id": "x"})
        )

    contract_by_field = {
        "contract": "hospital-mrf-copy-v3",
        "version_id": version_id,
        "schema_version": "3.0.0",
        "schema_revision": native.HOSPITAL_MRF_SCHEMA_REVISION,
        "format": "json",
        "compressed_input_bytes": 1,
        "max_fanout_rows": 1,
        "max_decompressed_bytes": 2,
        "max_output_bytes": 1,
        "artifacts": [],
    }
    with pytest.raises(ValueError, match="contract"):
        native.validate_hospital_parser_summary(
            json.dumps(contract_by_field).encode(), **options_by_name
        )

    with pytest.raises(ValueError, match="is missing"):
        native._parser_artifact(None, kind="mrf", output_directory=tmp_path)
    artifact_path = tmp_path / "mrf.copy"
    artifact_path.write_bytes(b"x")
    artifact_by_field = {
        "kind": "mrf", "path": str(artifact_path), "rows": -1,
        "bytes": 1, "sha256": hashlib.sha256(b"x").hexdigest(),
    }
    with pytest.raises(ValueError, match="is invalid"):
        native._parser_artifact(
            artifact_by_field, kind="mrf", output_directory=tmp_path
        )


def test_native_summary_enforces_total_output_limit(tmp_path):
    artifacts = []
    for kind in native.HOSPITAL_MRF_COPY_COLUMNS:
        path = tmp_path / f"{kind}.copy"
        path.write_bytes(b"x")
        artifacts.append({
            "kind": kind, "path": str(path), "rows": 1, "bytes": 1,
            "sha256": hashlib.sha256(b"x").hexdigest(),
        })
    summary_by_field = {
        "contract": "hospital-mrf-copy-v3", "version_id": "a" * 64,
        "schema_version": "3.0.0",
        "schema_revision": native.HOSPITAL_MRF_SCHEMA_REVISION,
        "format": "json", "compressed_input_bytes": 1,
        "max_fanout_rows": 1, "max_decompressed_bytes": 2,
        "max_output_bytes": 10, "artifacts": artifacts,
    }

    with pytest.raises(ValueError, match="output limit"):
        native.validate_hospital_parser_summary(
            json.dumps(summary_by_field).encode(), version_id="a" * 64,
            source_format="json", input_bytes=1, output_directory=tmp_path,
            max_decompressed_bytes=2, max_output_bytes=10,
        )


@pytest.mark.asyncio
async def test_native_runner_reports_process_and_spawn_failures(tmp_path, monkeypatch):
    acquisition = _acquisition_module()
    binary = tmp_path / "release" / "ptg2_scanner"
    monkeypatch.setattr(acquisition, "_ptg2_rust_scanner_binary", lambda: binary)
    monkeypatch.setattr(
        acquisition, "_ptg2_scanner_binary_profile", lambda _path: "release"
    )

    class FailedProcess:
        returncode = 2

        async def communicate(self):
            return b"", b"parser failed"

    async def spawn_process(*_args, **_kwargs):
        return FailedProcess()

    monkeypatch.setattr(acquisition.asyncio, "create_subprocess_exec", spawn_process)
    with pytest.raises(RuntimeError, match="exited 2: parser failed"):
        await acquisition.run_native_parser(
            tmp_path / "input", tmp_path / "output", "a" * 64,
            "json", 1, 2, 3,
        )

    async def fail_spawn(*_args, **_kwargs):
        raise ValueError("spawn failed")

    monkeypatch.setattr(acquisition.asyncio, "create_subprocess_exec", fail_spawn)
    with pytest.raises(ValueError, match="spawn failed"):
        await acquisition.run_native_parser(
            tmp_path / "input", tmp_path / "output", "a" * 64,
            "json", 1, 2, 3,
        )


def test_version_identity_is_bound_to_content_and_parser_contract():
    content = "b" * 64

    assert native.hospital_price_version_id(content) == native.hospital_price_version_id(content)
    assert native.hospital_price_version_id(content) != native.hospital_price_version_id("c" * 64)
    with pytest.raises(ValueError, match="SHA-256"):
        native.hospital_price_version_id("not-a-digest")
