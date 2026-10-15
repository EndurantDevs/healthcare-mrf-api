# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Portable checks for the opt-in installed HTTPS proof; no network or database."""

import hashlib
import json
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from tests import custom_import_installed_acquisition as support
from tests import custom_import_installed_https as proof

URL = (
    "https://raw.githubusercontent.com/EndurantDevs/healthcare-mrf-api/"
    + "1" * 40
    + "/tests/fixtures/custom_import/https/root_source.parquet"
)


def test_fixture_contains_only_existing_synthetic_rows_and_binds_exact_bytes(tmp_path):
    payload = proof.fixture_bytes()
    assert len(payload) < 65_536
    assert pq.read_table(BytesIO(payload)).to_pylist() == [{"ROOT_NPI": npi} for npi in support.NPIS]
    path = tmp_path / "root_source.parquet"
    path.write_bytes(payload)
    assert proof.checked_fixture(path, URL) == {
        "stream_id": "root_source",
        "url": URL,
        "expected_sha256": hashlib.sha256(payload).hexdigest(),
        "expected_bytes": len(payload),
    }
    path.write_bytes(payload + b"x" * 65_536)
    with pytest.raises(ValueError, match="fixture exceeds"):
        proof.checked_fixture(path, URL)


def test_fixture_accepts_different_encoding_and_binds_actual_bytes(tmp_path):
    path = tmp_path / "root_source.parquet"
    pq.write_table(
        pa.table({"ROOT_NPI": support.NPIS}),
        path,
        compression="NONE",
        use_dictionary=False,
        row_group_size=1,
    )
    payload = path.read_bytes()
    assert payload != proof.fixture_bytes()
    checked = proof.checked_fixture(path, URL)
    assert checked["expected_sha256"] == hashlib.sha256(payload).hexdigest()
    assert checked["expected_bytes"] == len(payload)


@pytest.mark.parametrize(
    "columns",
    (
        {"ROOT_NPI": [support.NPIS[0], support.NPIS[0]]},
        {"ROOT_NPI": [*support.NPIS, support.NPIS[0]]},
        {"ROOT_NPI": support.NPIS, "extra": ["synthetic", "synthetic"]},
        {"ROOT_NPI": [int(npi) for npi in support.NPIS]},
    ),
)
def test_fixture_rejects_changed_or_extra_data(tmp_path, columns):
    path = tmp_path / "root_source.parquet"
    pq.write_table(pa.table(columns), path)
    with pytest.raises(ValueError, match="fixture data differ"):
        proof.checked_fixture(path, URL)


@pytest.mark.parametrize(
    "url",
    (
        URL.replace("https:", "http:"),
        URL.replace("1" * 40, "main"),
        URL.replace("1" * 40, "0" * 40),
        URL.replace("raw.githubusercontent.com", "example.invalid"),
        URL + "?token=synthetic",
        URL + "#fragment",
        URL.replace("root_source.parquet", "other.parquet"),
        URL.replace("https://", "https://user@"),
    ),
)
def test_fixture_pin_rejects_unreviewed_destinations_before_open(tmp_path, url):
    with pytest.raises(ValueError, match="fixture URL must identify"):
        proof.checked_fixture(tmp_path / "absent", url)


def test_fixture_generation_never_overwrites_and_reports_only_identity(tmp_path, monkeypatch, capsys):
    path = tmp_path / "root_source.parquet"
    monkeypatch.setattr(proof.sys, "argv", ["proof", "fixture", str(path)])
    assert proof.main() == 0
    output = capsys.readouterr()
    assert output.err == ""
    assert json.loads(output.out) == {
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "bytes": path.stat().st_size,
    }
    assert proof.main() == 1
    output = capsys.readouterr()
    assert output.out == "" and str(path) not in output.err
    assert path.read_bytes() == proof.fixture_bytes()


def test_connection_observer_records_real_public_https_without_issuing_io():
    connections = []
    observe = proof.observe_public_connections(connections, support.loopback_database_only(5432))
    observe("socket.connect", (None, ("127.0.0.1", 5432)))
    observe("socket.getaddrinfo", ("raw.githubusercontent.com", 443))
    assert not connections
    observe("socket.connect", (None, ("8.8.8.8", 443)))
    assert connections == [True]
    for address in (("192.168.1.1", 443), ("8.8.8.8", 80), ("127.0.0.1", 6379), "/tmp/synthetic.sock"):
        with pytest.raises(AssertionError):
            observe("socket.connect", (None, address))
    for event, arguments in (
        ("socket.getaddrinfo", ("synthetic.invalid", 443)),
        ("socket.sendto", (None, ("8.8.8.8", 443))),
    ):
        with pytest.raises(AssertionError):
            observe(event, arguments)


@pytest.mark.parametrize("mode", ("execute", "interrupt", "resume"))
def test_installed_bootstrap_keeps_transport_and_uses_offline_guard(tmp_path, monkeypatch, mode):
    from process.custom_import import https, runner

    installed = tmp_path / "opt"
    installed.mkdir()
    entry = installed / "custom_import_cli.py"
    configuration = tmp_path / "registered.json"
    registered_dict = {"dataset_id": 1, "definition_revision_id": 2, "schema_revision_id": 3}
    configuration.write_text(json.dumps(registered_dict))
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("HLTHPRT_DB_PORT", "5432")
    monkeypatch.setattr(proof.sys, "argv", ["proof", "run", str(entry), str(configuration), mode, "execute"])
    monkeypatch.setattr(proof.sys, "path", list(proof.sys.path))
    guards = []
    monkeypatch.setattr(proof.sys, "addaudithook", guards.append)
    for module in (https, runner):
        monkeypatch.setattr(module, "__file__", str(installed / "process" / (module.__name__ + ".py")))
    read = AsyncMock()
    monkeypatch.setattr(
        proof,
        "_support",
        lambda: {
            "loopback_database_only": support.loopback_database_only,
            "verify_installed_read": read,
        },
    )
    interrupt = Mock()
    monkeypatch.setattr(proof, "interrupt_before_materialization", interrupt)
    acquire = https.acquire_https

    def invoke(path, *, run_name):
        assert path == str(entry) and run_name == "__main__"
        assert proof.sys.argv == [str(entry), "execute"]
        assert https.acquire_https is acquire
        if mode == "resume":
            with pytest.raises(AssertionError):
                guards[0]("socket.connect", (None, ("8.8.8.8", 443)))
        else:
            guards[0]("socket.connect", (None, ("8.8.8.8", 443)))
        raise SystemExit(0)

    monkeypatch.setattr(proof.runpy, "run_path", invoke)
    proof.run_installed({"phase": "bootstrap", "network_event": None})
    assert interrupt.call_count == (1 if mode == "interrupt" else 0)
    assert read.await_count == (0 if mode == "interrupt" else 1)
    if mode != "interrupt":
        read.assert_awaited_once_with(registered_dict)


@pytest.mark.asyncio
async def test_interruption_expires_exact_execution_then_calls_original_materializer():
    session = AsyncMock()
    session.__aenter__.return_value = session
    transaction = AsyncMock()
    session.begin = Mock(return_value=transaction)
    session_factory = Mock(return_value=session)
    original = AsyncMock(return_value="lease_lost")
    runner = SimpleNamespace(_materialize_candidate=original)
    proof.interrupt_before_materialization(runner)
    candidate, grant, admitted = object(), SimpleNamespace(execution_id=17), object()
    assert await runner._materialize_candidate(session_factory, candidate, grant, admitted) == "lease_lost"
    session.execute.assert_awaited_once()
    statement = session.execute.call_args.args[0].compile()
    assert statement.params == {"execution_id_1": 17}
    assert "expires_at=clock_timestamp()" in str(statement)
    transaction.__aexit__.assert_awaited_once()
    original.assert_awaited_once_with(session_factory, candidate, grant, admitted)


def test_bootstrap_failure_uses_existing_redacted_diagnostic(monkeypatch, capsys):
    monkeypatch.setattr(proof.sys, "argv", ["proof", "run", "entry", "config", "execute", "execute"])

    def fail(_diagnostic):
        raise ValueError("synthetic-unreported-detail")

    monkeypatch.setattr(proof, "run_installed", fail)
    assert proof.main() == 1
    captured = capsys.readouterr()
    assert captured.out == "" and "synthetic-unreported-detail" not in captured.err
    assert support.failure_metadata(b"", captured.err.encode()) == {
        "code": "installed_acquisition_failed",
        "phase": "bootstrap",
        "network_event": None,
    }
