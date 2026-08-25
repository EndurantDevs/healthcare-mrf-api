# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Focused unit proof for hospital-price acquisition and COPY publication."""

from __future__ import annotations

import asyncio
import hashlib
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from tests.hospital_price_control_support import (
    ROOT,
    acquisition_module as _acquisition_module,
    native_module as _native_module,
    store_module as _store_module,
)


class _CopyDriver:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def copy_to_table(self, table: str, **kwargs: Any) -> str:
        payload = kwargs["source"].read()
        self.calls.append({"table": table, "payload": payload, **kwargs})
        return "COPY 1"


class _CopyConnection:
    def __init__(self) -> None:
        self.driver = _CopyDriver()
        self.raw_connection = SimpleNamespace(driver_connection=self.driver)
        self.statements: list[str] = []

    async def status(self, statement: str, **_params: Any) -> int:
        self.statements.append(statement)
        return 1


class _AdmissionDriver:
    def __init__(self) -> None:
        self.records: list[tuple[Any, ...]] = []

    async def copy_records_to_table(self, _table: str, **kwargs: Any) -> None:
        self.records = list(kwargs["records"])


class _AdmissionConnection:
    def __init__(self) -> None:
        self.driver = _AdmissionDriver()
        self.raw_connection = SimpleNamespace(driver_connection=self.driver)
        self.statements: list[str] = []

    async def status(self, statement: str, **_params: Any) -> int:
        self.statements.append(statement)
        return 1

    async def all(self, statement: str, **_params: Any) -> list[tuple[Any, ...]]:
        self.statements.append(statement)
        return [("hospital-a", "attempt-a", 3)]


def _receipt(native: Any, directory: Path) -> Any:
    artifacts = []
    for kind in native.HOSPITAL_MRF_COPY_COLUMNS:
        payload = f"{kind}\n".encode()
        path = directory / f"{kind}.copy"
        path.write_bytes(payload)
        artifacts.append(
            native.HospitalParserArtifact(
                kind, path, 1, len(payload), hashlib.sha256(payload).hexdigest()
            )
        )
    return native.HospitalParserReceipt(
        "a" * 64, "json", "b" * 64, 100_000, 2048, 1024,
        tuple(artifacts),
    )


def test_registry_sync_does_not_guess_canonical_facility_identity():
    acquisition = _acquisition_module()
    hospital_by_field = {
        "hospital_id": "hospital-a",
        "name": "Same Name Hospital",
        "cms_hpt_url": "https://hospital.example/cms-hpt.txt",
    }

    registry_row = acquisition._registry_records((hospital_by_field,))[0]

    assert registry_row[0] == "hospital-a"
    assert registry_row[1] is None
    assert registry_row[3:] == ("Same Name Hospital", 1)
    assert "facility_anchor_id=EXCLUDED.facility_anchor_id" not in (
        ROOT / "process/hospital_price_acquisition.py"
    ).read_text()


def test_exact_locator_name_survives_candidate_and_location_binding():
    acquisition = _acquisition_module()
    store, _native = _store_module()
    locator_url = "https://hospital.example/cms-hpt.txt"
    mrf_url = "https://hospital.example/prices.csv"
    hospital_by_field = {
        "hospital_id": "hospital-a",
        "name": "Catalog Display Name",
        "locator_name": "Exact Locator Name",
        "cms_hpt_url": locator_url,
    }
    locator_result = acquisition.LocatorResult(
        locator_url,
        "locator-a",
        "observation-a",
        (hospital_by_field,),
        (acquisition.HospitalHptLocatorRecord("Exact Locator Name", mrf_url),),
    )

    candidate = acquisition.candidates_from_locators((locator_result,))[0]
    attempt = SimpleNamespace(
        hospital_id=candidate.hospital_id,
        hospital_name=candidate.hospital_name,
        locator_name=candidate.locator_name,
    )

    assert candidate.locator_name == "Exact Locator Name"
    assert store._location_ordinals((attempt,), ((7, "Exact Locator Name"),)) == {
        "hospital-a": 7
    }


@pytest.mark.asyncio
async def test_copy_stages_use_exact_v3_columns_and_private_temp_tables(tmp_path):
    store, native = _store_module()
    receipt = _receipt(native, tmp_path)
    connection = _CopyConnection()
    stage_by_kind = {kind: f"stage_{kind}" for kind in native.HOSPITAL_MRF_COPY_COLUMNS}

    await store._copy_stages(connection, receipt, stage_by_kind)

    columns_by_stage = {
        call["table"]: tuple(call["columns"])
        for call in connection.driver.calls
    }
    assert columns_by_stage["stage_modifier"] == (
        "version_id", "modifier_ordinal", "code", "description", "setting",
        "additional_generic_notes",
    )
    assert columns_by_stage["stage_modifier_payer"] == (
        "version_id", "modifier_ordinal", "payer_ordinal", "payer_name",
        "plan_name", "description", "standard_charge_dollar",
        "standard_charge_percentage", "standard_charge_algorithm",
    )
    assert set(columns_by_stage) == {
        f"stage_{kind}" for kind in native.HOSPITAL_MRF_COPY_COLUMNS
    }
    assert all("CREATE TEMP TABLE" in statement for statement in connection.statements)
    assert all("ON COMMIT DROP" in statement for statement in connection.statements)
    assert all("WITH NO DATA" in statement for statement in connection.statements)


@pytest.mark.asyncio
async def test_attempt_admission_locks_generation_and_rejects_active_attempts():
    store, _native = _store_module()
    connection = _AdmissionConnection()

    @asynccontextmanager
    async def acquire():
        yield connection

    store.db.acquire = acquire
    candidate = SimpleNamespace(
        hospital_id="hospital-a", locator_id="locator-a",
        observation_id="observation-a", source_url="https://a/prices.json",
    )

    rows = await store.admit_attempts(
        (candidate,), lease_owner="hospital-prices:test", lease_seconds=300
    )

    sql = "\n".join(connection.statements)
    assert rows == [("hospital-a", "attempt-a", 3)]
    assert "ON COMMIT DROP" in sql
    assert "FOR UPDATE OF current" in sql
    assert "NOT IN ('queued', 'running', 'verified')" in sql
    assert "error_code='lease_expired'" in sql
    assert "attempt.lease_expires_at <= clock_timestamp()" in sql
    assert "heartbeat_at, lease_expires_at" in sql
    assert "SET latest_attempt_id=inserted.attempt_id" in sql
    assert connection.driver.records[0][0] == "hospital-a"


@pytest.mark.asyncio
async def test_attempt_heartbeat_cannot_resurrect_an_expired_lease():
    store, _native = _store_module()
    statements = []

    async def expired(statement: str, **_params: Any) -> tuple[int, int, int]:
        statements.append(statement)
        return 0, 1, 0

    store.db.first = expired
    with pytest.raises(RuntimeError, match="lease was lost"):
        await store.renew_attempt_leases(
            (SimpleNamespace(attempt_id="attempt-a"),),
            lease_owner="hospital-prices:test",
            lease_seconds=300,
        )

    assert "attempt.lease_expires_at > lease_clock.now" in statements[0]


class _EvidenceConnection:
    def __init__(self) -> None:
        self.statements: list[str] = []

    async def status(self, statement: str, **_params: Any) -> int:
        self.statements.append(statement)
        return 2 if "SET status='verified'" in statement else 1

    async def scalar(self, statement: str, **_params: Any) -> int:
        self.statements.append(statement)
        return 0

    async def all(self, statement: str, **_params: Any) -> list[tuple[str]]:
        self.statements.append(statement)
        return [("published",), ("superseded",), ("unchanged",)]


@pytest.mark.asyncio
async def test_evidence_is_immutable_and_publication_is_one_generation_cas():
    store, _native = _store_module()
    connection = _EvidenceConnection()

    await store._bind_evidence(connection, '"stage"', "a" * 64, "b" * 64, 2)
    published, superseded, unchanged = await store._cas_publish(
        connection, '"stage"', "a" * 64
    )

    sql = "\n".join(connection.statements)
    assert "hospital_price_version_hospital (version_id, hospital_id" in sql
    assert "ON CONFLICT DO NOTHING" in sql
    assert (
        "hospital_price_hospital_npi "
        "(hospital_id, version_id, source_ordinal, npi, source_kind)" in sql
    )
    assert "'mrf_header_file'" in sql
    assert "tin_type, tin_value, source_kind" in sql
    assert "current.generation=staged.expected_generation" in sql
    assert "current.latest_attempt_id=staged.attempt_id" in sql
    assert "current.version_id=:version" in sql
    assert "current.version_id IS DISTINCT FROM :version" in sql
    assert "WHEN unchanged.hospital_id IS NOT NULL THEN 'unchanged'" in sql
    assert (published, superseded, unchanged) == (1, 1, 1)


@pytest.mark.asyncio
async def test_locator_fetch_is_fresh_exact_and_bounded(tmp_path, monkeypatch):
    acquisition = _acquisition_module()
    locator_path = tmp_path / "cms-hpt.txt"
    locator_path.write_text(
        "location-name: Hospital A\n"
        "mrf-url: https://hospital.example/12-3456789_prices.json\n"
    )
    options_by_name: dict[str, Any] = {}
    raw = SimpleNamespace(
        raw_path=str(locator_path), raw_sha256="a" * 64,
        byte_count=locator_path.stat().st_size,
        head=SimpleNamespace(url="https://hospital.example/cms-hpt.txt", status=200),
    )

    async def download(url: str, **kwargs: Any) -> Any:
        options_by_name.update({"url": url, **kwargs})
        return raw

    async def record(*_args: Any, **_kwargs: Any) -> None:
        return None

    monkeypatch.setattr(acquisition, "download_raw_artifact", download)
    monkeypatch.setattr(acquisition, "_record_locator_observation", record)
    hospital_by_field = {
        "hospital_id": "hospital-a", "name": "Hospital A",
        "cms_hpt_url": "https://hospital.example/cms-hpt.txt",
    }

    locator_result = await acquisition.fetch_locator(
        (
            hospital_by_field["cms_hpt_url"],
            (hospital_by_field,),
        ),
        object(),
    )

    assert options_by_name["reuse_raw_artifacts"] is False
    assert options_by_name["exact_get_evidence"] is True
    assert options_by_name["keep_partial_artifacts"] is False
    assert options_by_name["max_bytes"] == 1_000_000
    assert locator_result.records[0].mrf_url.endswith("12-3456789_prices.json")


@pytest.mark.asyncio
async def test_locator_fetch_records_download_parse_and_cancellation_errors(
    tmp_path, monkeypatch
):
    acquisition = _acquisition_module()
    hospital_by_field = {"hospital_id": "a", "name": "Hospital A"}
    locator_input = ("https://a/cms-hpt.txt", (hospital_by_field,))
    observations = []

    async def record(*args, **kwargs):
        observations.append((args, kwargs))

    async def fail_download(*_args, **_kwargs):
        raise ValueError("download failed")

    monkeypatch.setattr(acquisition, "_record_locator_observation", record)
    monkeypatch.setattr(acquisition, "download_raw_artifact", fail_download)
    failed = await acquisition.fetch_locator(locator_input, object())
    assert failed.error_code == "value"
    assert observations[-1][0][3] == "fetch_failed"

    locator_path = tmp_path / "cms-hpt.txt"
    locator_path.write_bytes(b"invalid")
    raw = SimpleNamespace(raw_path=str(locator_path), head=None)

    async def download(*_args, **_kwargs):
        return raw

    monkeypatch.setattr(acquisition, "download_raw_artifact", download)
    invalid = await acquisition.fetch_locator(locator_input, object())
    assert invalid.error_code == "hospitalhptlocator"
    assert observations[-1][0][3] == "invalid"

    async def cancel(*_args, **_kwargs):
        raise asyncio.CancelledError

    monkeypatch.setattr(acquisition, "download_raw_artifact", cancel)
    with pytest.raises(asyncio.CancelledError):
        await acquisition.fetch_locator(locator_input, object())


@pytest.mark.asyncio
async def test_source_download_updates_shared_attempts_and_reports_errors(monkeypatch):
    acquisition = _acquisition_module()
    attempt = acquisition.Attempt("attempt", "a", "Hospital A", "https://a/mrf", 1)
    raw = SimpleNamespace(head=SimpleNamespace(url="https://a/final", status=200))

    async def download(*_args, **_kwargs):
        return raw

    monkeypatch.setattr(acquisition, "download_raw_artifact", download)
    downloaded_source = await acquisition.download_source(
        ("https://a/mrf", (attempt,)), object(), 1024
    )
    assert downloaded_source.raw is raw
    assert attempt.final_source_url == "https://a/final"
    assert attempt.source_http_status == 200
    raw.head = None
    await acquisition.download_source(("https://a/mrf", (attempt,)), object(), 1024)

    async def fail(*_args, **_kwargs):
        raise ValueError("failed")

    monkeypatch.setattr(acquisition, "download_raw_artifact", fail)
    failed = await acquisition.download_source(
        ("https://a/mrf", (attempt,)), object(), 1024
    )
    assert failed.raw is None
    assert failed.error_code == "value"

    async def cancel(*_args, **_kwargs):
        raise asyncio.CancelledError

    monkeypatch.setattr(acquisition, "download_raw_artifact", cancel)
    with pytest.raises(asyncio.CancelledError):
        await acquisition.download_source(
            ("https://a/mrf", (attempt,)), object(), 1024
        )


@pytest.mark.asyncio
async def test_native_runner_rejects_debug_binary(tmp_path, monkeypatch):
    acquisition = _acquisition_module()
    monkeypatch.setattr(
        acquisition, "_ptg2_rust_scanner_binary",
        lambda: tmp_path / "debug" / "ptg2_scanner",
    )
    monkeypatch.setattr(
        acquisition, "_ptg2_scanner_binary_profile", lambda _path: "debug"
    )

    with pytest.raises(RuntimeError, match="release Rust parser"):
        await acquisition.run_native_parser(
            tmp_path / "input.json", tmp_path / "output", "a" * 64,
            "json", 1, 2048, 1024,
        )


@pytest.mark.asyncio
async def test_native_runner_drains_cleanup_after_repeated_cancel(
    tmp_path, monkeypatch
):
    acquisition = _acquisition_module()
    communicate_started = asyncio.Event()
    cleanup_started = asyncio.Event()
    allow_cleanup = asyncio.Event()
    cleanup_finished = asyncio.Event()

    class Process:
        returncode = None

        async def communicate(self) -> tuple[bytes, bytes]:
            communicate_started.set()
            await asyncio.Future()
            raise AssertionError("cancelled parser communication returned")

    process = Process()

    async def spawn(*_args: Any, **_kwargs: Any) -> Process:
        return process

    async def terminate(_process: Process) -> None:
        cleanup_started.set()
        await allow_cleanup.wait()
        cleanup_finished.set()

    binary = tmp_path / "release" / "ptg2_scanner"
    monkeypatch.setattr(acquisition, "_ptg2_rust_scanner_binary", lambda: binary)
    monkeypatch.setattr(
        acquisition, "_ptg2_scanner_binary_profile", lambda _path: "release"
    )
    monkeypatch.setattr(acquisition.asyncio, "create_subprocess_exec", spawn)
    monkeypatch.setattr(acquisition, "_terminate_asyncio_subprocess_group", terminate)
    operation = asyncio.create_task(
        acquisition.run_native_parser(
            tmp_path / "input.json", tmp_path / "output", "a" * 64,
            "json", 1, 2048, 1024,
        )
    )
    await asyncio.wait_for(communicate_started.wait(), timeout=1)

    operation.cancel()
    await asyncio.wait_for(cleanup_started.wait(), timeout=1)
    operation.cancel()
    await asyncio.sleep(0)
    assert not operation.done()
    allow_cleanup.set()

    with pytest.raises(asyncio.CancelledError):
        await operation
    assert cleanup_finished.is_set()


@pytest.mark.asyncio
async def test_native_runner_passes_and_validates_exact_output_cap(
    tmp_path, monkeypatch
):
    acquisition = _acquisition_module()
    call_by_name: dict[str, Any] = {}
    expected_receipt = object()

    class Process:
        returncode = 0

        async def communicate(self) -> tuple[bytes, bytes]:
            return b"{}", b""

    async def spawn(*args: Any, **kwargs: Any) -> Process:
        call_by_name["args"] = args
        call_by_name["kwargs"] = kwargs
        return Process()

    def validate(payload: bytes, **kwargs: Any) -> object:
        call_by_name["payload"] = payload
        call_by_name["validation"] = kwargs
        return expected_receipt

    binary = tmp_path / "release" / "ptg2_scanner"
    monkeypatch.setattr(acquisition, "_ptg2_rust_scanner_binary", lambda: binary)
    monkeypatch.setattr(
        acquisition, "_ptg2_scanner_binary_profile", lambda _path: "release"
    )
    monkeypatch.setattr(acquisition.asyncio, "create_subprocess_exec", spawn)
    monkeypatch.setattr(acquisition, "validate_hospital_parser_summary", validate)

    receipt = await acquisition.run_native_parser(
        tmp_path / "input.json", tmp_path / "output", "a" * 64,
        "json", 123, 8192, 4096,
    )

    assert receipt is expected_receipt
    assert call_by_name["args"][-2:] == ("8192", "4096")
    assert call_by_name["validation"]["max_decompressed_bytes"] == 8192
    assert call_by_name["validation"]["max_output_bytes"] == 4096
