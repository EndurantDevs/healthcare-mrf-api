# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Focused unit proof for hospital-price acquisition and COPY publication."""

from __future__ import annotations

import hashlib
import importlib.util
import sys
import types
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest


ROOT = Path(__file__).resolve().parents[1]


def _load_path(
    name: str, relative_path: str, replacements: dict[str, types.ModuleType]
) -> Any:
    prior_module_by_name = {
        module_name: sys.modules.get(module_name)
        for module_name in (name, *replacements)
    }
    sys.modules.update(replacements)
    spec = importlib.util.spec_from_file_location(name, ROOT / relative_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
        return module
    finally:
        for module_name, prior_module in prior_module_by_name.items():
            if prior_module is None:
                sys.modules.pop(module_name, None)
            else:
                sys.modules[module_name] = prior_module


def _module(name: str, **attributes: Any) -> types.ModuleType:
    module = types.ModuleType(name)
    for attribute, value in attributes.items():
        setattr(module, attribute, value)
    return module


def _native_module() -> Any:
    return _load_path(
        "hospital_price_native_control_test",
        "process/hospital_price_native.py",
        {},
    )


def _store_module() -> tuple[Any, Any]:
    native = _native_module()
    fake_db = SimpleNamespace()
    replacement_by_name = {
        "db.models": _module("db.models", db=fake_db),
        "process.hospital_hpt_locator": _module(
            "process.hospital_hpt_locator",
            normalized_hospital_location_name=lambda value: " ".join(value.split()).casefold(),
        ),
        "process.hospital_price_acquisition": _module(
            "process.hospital_price_acquisition",
            REGISTRY_VERSION=1, Attempt=object, Candidate=object,
            schema_name=lambda: "mrf",
        ),
        "process.hospital_price_native": native,
        "process.ptg_parts.db_tables": _module(
            "process.ptg_parts.db_tables",
            _quote_ident=lambda value: f'"{value}"',
        ),
    }
    return (
        _load_path(
            "hospital_price_store_control_test",
            "process/hospital_price_store.py",
            replacement_by_name,
        ),
        native,
    )


def _acquisition_module() -> Any:
    native = _native_module()
    locator = _load_path(
        "hospital_hpt_locator_control_test",
        "process/hospital_hpt_locator.py",
        {},
    )

    class HospitalPriceVersion:
        __table__ = SimpleNamespace(schema="mrf")

    async def noop(*_args: Any, **_kwargs: Any) -> None:
        return None

    replacement_by_name = {
        "db.models": _module(
            "db.models", HospitalPriceVersion=HospitalPriceVersion,
            db=SimpleNamespace(),
        ),
        "process.control_cancel": _module(
            "process.control_cancel", ImportCancelledError=RuntimeError
        ),
        "process.hospital_hpt_locator": locator,
        "process.hospital_hpt_registry": _module(
            "process.hospital_hpt_registry",
            load_hospital_hpt_registry=lambda: (),
        ),
        "process.hospital_price_native": native,
        "process.ptg_parts.artifacts": _module(
            "process.ptg_parts.artifacts", PTG2ArtifactStore=object
        ),
        "process.ptg_parts.db_tables": _module(
            "process.ptg_parts.db_tables", _quote_ident=lambda value: value
        ),
        "process.ptg_parts.rust_scanner": _module(
            "process.ptg_parts.rust_scanner",
            _ptg2_rust_scanner_binary=lambda: None,
            _ptg2_scanner_binary_profile=lambda _path: "release",
            _subprocess_session_options=lambda _spawn: {},
            _terminate_asyncio_subprocess_group=noop,
        ),
        "process.ptg_parts.source_download": _module(
            "process.ptg_parts.source_download",
            PTG2_DEFAULT_MAX_BYTES=64 * 1024**3,
            download_raw_artifact=noop,
        ),
    }
    return _load_path(
        "hospital_price_acquisition_control_test",
        "process/hospital_price_acquisition.py",
        replacement_by_name,
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
    hospital = {
        "hospital_id": "hospital-a",
        "name": "Catalog Display Name",
        "locator_name": "Exact Locator Name",
        "cms_hpt_url": locator_url,
    }
    result = acquisition.LocatorResult(
        locator_url,
        "locator-a",
        "observation-a",
        (hospital,),
        (acquisition.HospitalHptLocatorRecord("Exact Locator Name", mrf_url),),
    )

    candidate = acquisition.candidates_from_locators((result,))[0]
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
