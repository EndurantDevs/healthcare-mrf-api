# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Portable checks that the installed proof's synthetic boundary fails closed."""

import pytest

from tests import custom_import_installed_acquisition as upstream
from tests.custom_import_installed_acquisition import (
    NPIS,
    SNAPSHOT,
    SyntheticTransport,
    loopback_database_only,
    write_synthetic_credentials,
)


def test_bootstrap_failure_reports_only_fixed_phase_and_network_event(monkeypatch, capsys):
    def denied(diagnostic):
        diagnostic["phase"] = "imports"
        upstream.loopback_database_only(5432, diagnostic)("socket.connect", (None, ("private.invalid", 9999)))

    monkeypatch.setattr(upstream, "run_installed_acquisition", denied)
    assert upstream.main() == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert upstream.failure_metadata(b"", captured.err.encode()) == {
        "code": "installed_acquisition_failed",
        "phase": "imports",
        "network_event": "socket.connect",
    }
    assert "private.invalid" not in captured.err and "9999" not in captured.err


@pytest.mark.parametrize(
    "stderr",
    [
        b"private exception text",
        b'{"code":"private_code"}',
        b'{"code":"installed_acquisition_failed","phase":"private_phase","network_event":null}',
        b'{"code":"installed_acquisition_failed","phase":"imports","network_event":"private_event"}',
        b"\xff",
        b"[]",
        b'{"code":"installed_acquisition_failed","phase":[],"network_event":null}',
        b'{"code":"installed_acquisition_failed","phase":"imports","network_event":[]}',
        b'{"code":"installed_acquisition_failed","phase":"imports"}',
        b"[" * 50000 + b"]" * 50000,
    ],
)
def test_failure_metadata_does_not_forward_unknown_child_output(stderr):
    assert upstream.failure_metadata(b"private stdout", stderr) == {
        "code": "unclassified",
        "stdout_lines": 1,
        "stderr_lines": 1,
    }


def test_bootstrap_preserves_operator_cancellation(monkeypatch, capsys):
    def cancel(diagnostic):
        raise SystemExit(130)

    monkeypatch.setattr(upstream, "run_installed_acquisition", cancel)
    assert upstream.main() == 130
    assert capsys.readouterr().err == ""


def _options():
    return {
        "account": "synthetic_account",
        "user": "synthetic_user",
        "role": "SYNTHETIC_READER",
        "warehouse": "SYNTHETIC_LOAD",
        "authenticator": "SNOWFLAKE_JWT",
        "private_key": b"synthetic-der",
        "autocommit": False,
        "client_session_keep_alive": False,
    }


def test_transport_requires_exact_query_consumption_and_cleanup():
    transport = SyntheticTransport("SELECT synthetic")
    connection = transport.connect(**_options())
    cursor = connection.cursor()
    with pytest.raises(AssertionError):
        transport.verify()
    with pytest.raises(AssertionError):
        cursor.fetchone()
    cursor.execute("USE SECONDARY ROLES NONE")
    with pytest.raises(AssertionError):
        cursor.execute("SELECT other")
    cursor.execute("SELECT synthetic")
    assert cursor.fetchone() == (0, 1, "root_source", SNAPSHOT, None)
    assert [cursor.fetchone()[-1] for _ in NPIS] == list(NPIS)
    with pytest.raises(AssertionError):
        transport.verify()
    assert cursor.fetchone() is None
    cursor.close()
    with pytest.raises(AssertionError):
        transport.verify()
    connection.close()
    transport.verify()
    with pytest.raises(AssertionError):
        transport.connect(**_options())


@pytest.mark.parametrize("change", [{"account": "other"}, {"private_key": b""}, {"autocommit": True}])
def test_transport_rejects_wrong_connection_settings(change):
    with pytest.raises(AssertionError):
        SyntheticTransport("SELECT synthetic").connect(**(_options() | change))


@pytest.mark.parametrize("address", [("example.invalid", 5432), ("127.0.0.1", 6379), "/tmp/synthetic.sock"])
def test_network_guard_rejects_other_services(address):
    with pytest.raises(AssertionError):
        loopback_database_only(5432)("socket.connect", (None, address))


def test_network_guard_allows_only_database_and_loopback_resolution():
    guard = loopback_database_only(5432)
    guard("socket.connect", (None, ("127.0.0.1", 5432)))
    guard("socket.connect", (None, ("::1", 5432, 0, 0)))
    for host in ("localhost", "127.0.0.1", "::1"):
        guard("socket.getaddrinfo", (host, 5432))
    with pytest.raises(AssertionError):
        guard("socket.getaddrinfo", ("example.invalid", 5432))
    with pytest.raises(AssertionError):
        guard("socket.getaddrinfo", ("localhost", 6379))


@pytest.mark.parametrize(
    "event,arguments",
    [
        ("socket.sendto", (None, ("203.0.113.1", 5432))),
        ("socket.sendmsg", (None, ("203.0.113.1", 5432))),
        ("socket.gethostbyname", ("example.invalid",)),
        ("socket.gethostbyaddr", ("203.0.113.1",)),
        ("socket.getnameinfo", (("203.0.113.1", 5432), 0)),
    ],
)
def test_network_guard_rejects_datagrams_and_legacy_dns(event, arguments):
    with pytest.raises(AssertionError):
        loopback_database_only(5432)(event, arguments)


def test_real_connector_seals_the_synthetic_upstream(tmp_path, monkeypatch):
    import json
    from io import BytesIO
    from pathlib import Path
    from tempfile import TemporaryDirectory

    import pyarrow.parquet as pq
    import snowflake.connector

    from process.custom_import.definition import CustomImportDefinition
    from process.custom_import.snowflake import FixedLocalKeyPairCredentialProvider
    from process.custom_import.snowflake_bundle import SnowflakeBundleAcquisitionConnector, replayable_parquet_captures
    from process.custom_import.snowflake_python import SnowflakePythonConnectorAdapter
    from process.custom_import.snowflake_source_binding import SnowflakeSourceBinding
    from tests.test_custom_import_installed_acquisition_postgres import _searchable_registration

    document = _searchable_registration()
    definition = CustomImportDefinition.from_mapping(document["definition"])
    binding = SnowflakeSourceBinding.from_json(json.dumps(document["source_binding"]))
    approved, bindings = binding.bundle_components(definition)
    with TemporaryDirectory(dir=tmp_path) as temporary:
        mount = Path(temporary) / "operator"
        write_synthetic_credentials(mount)
        with FixedLocalKeyPairCredentialProvider(mount) as provider:
            connector = SnowflakeBundleAcquisitionConnector(
                approved_relations=approved,
                credential_provider=provider,
                adapter=SnowflakePythonConnectorAdapter(role=binding.role, warehouse=binding.warehouse),
            )
            request = connector.prepare_request(definition, bindings=bindings)
            transport = SyntheticTransport(connector.build_statement(request).sql)
            monkeypatch.setattr(snowflake.connector, "connect", transport.connect)
            acquired = connector.acquire(request)
    assert not mount.exists()
    transport.verify()
    captures = replayable_parquet_captures(acquired)
    assert len(captures) == 1 and captures[0].receipt.source_snapshot_token == SNAPSHOT
    captured_rows = [captured for part in captures[0].parts for captured in pq.read_table(BytesIO(part)).to_pylist()]
    assert captured_rows == [{"npi": npi} for npi in NPIS]
