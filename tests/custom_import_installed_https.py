# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic fixture and installed-process boundary for an opt-in HTTPS proof."""

from __future__ import annotations

import asyncio
import hashlib
import ipaddress
import json
import os
import re
import runpy
import sys
from io import BytesIO
from pathlib import Path

_FIXTURE_URL = re.compile(
    r"https://raw\.githubusercontent\.com/EndurantDevs/healthcare-mrf-api/"
    r"(?P<revision>[0-9a-f]{40})/tests/fixtures/custom_import/https/root_source\.parquet\Z"
)


def _support():
    return runpy.run_path(str(Path(__file__).with_name("custom_import_installed_acquisition.py")))


def fixture_bytes():
    import pyarrow as pa
    import pyarrow.parquet as pq

    destination = BytesIO()
    pq.write_table(pa.table({"ROOT_NPI": _support()["NPIS"]}), destination, compression="NONE", store_schema=False)
    return destination.getvalue()


def checked_fixture(path, url):
    import pyarrow as pa
    import pyarrow.parquet as pq

    match = _FIXTURE_URL.fullmatch(url)
    if match is None or set(match["revision"]) == {"0"}:
        raise ValueError("fixture URL must identify the reviewed immutable public object")
    with Path(path).open("rb") as source:
        payload = source.read(65_537)
    if len(payload) > 65_536:
        raise ValueError("fixture exceeds the complete synthetic object bound")
    parquet = pq.ParquetFile(BytesIO(payload))
    npis = _support()["NPIS"]
    if (
        parquet.schema_arrow != pa.schema([("ROOT_NPI", pa.string())])
        or parquet.metadata.num_rows != len(npis)
        or parquet.read(use_threads=False).to_pylist() != [{"ROOT_NPI": npi} for npi in npis]
    ):
        raise ValueError("fixture data differ from the complete synthetic object")
    return {
        "stream_id": "root_source",
        "url": url,
        "expected_sha256": hashlib.sha256(payload).hexdigest(),
        "expected_bytes": len(payload),
    }


def observe_public_connections(connections, database_only):
    """Observe actual sockets without replacing the resolver, TLS or acquisition."""

    def audit(event, arguments):
        if event == "socket.getaddrinfo" and arguments[:2] == ("raw.githubusercontent.com", 443):
            return
        if event == "socket.connect":
            address = arguments[1]
            if (
                isinstance(address, tuple)
                and len(address) >= 2
                and address[1] == 443
                and ipaddress.ip_address(address[0]).is_global
            ):
                connections.append(True)
                return
        database_only(event, arguments)

    return audit


def interrupt_before_materialization(runner):
    """Reuse the lease-loss seam only after real acquisition has durably bound."""
    from sqlalchemy import func, update

    from db.models.custom_import import CustomImportLease

    original = runner._materialize_candidate

    async def expire(session_factory, candidate, grant, admitted):
        async with session_factory() as session, session.begin():
            await session.execute(
                update(CustomImportLease)
                .where(CustomImportLease.execution_id == grant.execution_id)
                .values(expires_at=func.clock_timestamp())
            )
        return await original(session_factory, candidate, grant, admitted)

    runner._materialize_candidate = expire


def run_installed(diagnostic):
    entry, configuration, mode, *arguments = sys.argv[2:]
    entry = Path(entry).resolve()
    assert entry.name == "custom_import_cli.py" and mode in {"execute", "interrupt", "resume"}
    assert Path.cwd() != entry.parent and not (entry.parent / "tests").exists()
    registered = json.loads(Path(configuration).read_text())
    support = _support()
    sys.path.insert(0, str(entry.parent))
    connections = []
    database_only = support["loopback_database_only"](int(os.environ["HLTHPRT_DB_PORT"]), diagnostic)
    guard = database_only if mode == "resume" else observe_public_connections(connections, database_only)
    sys.addaudithook(guard)
    diagnostic["phase"] = "imports"
    from process.custom_import import https, runner

    for module in (https, runner):
        assert Path(module.__file__).resolve().is_relative_to(entry.parent)
    if mode == "interrupt":
        interrupt_before_materialization(runner)
    sys.argv[:] = [str(entry), *arguments]
    diagnostic["phase"] = "execute"
    try:
        runpy.run_path(str(entry), run_name="__main__")
    except SystemExit as result:
        if result.code != 0:
            raise
    else:
        raise AssertionError("installed entry did not return an exit status")
    if mode != "resume":
        assert connections
    if mode != "interrupt":
        diagnostic["phase"] = "verify_reads"
        asyncio.run(support["verify_installed_read"](registered))


def main():
    diagnostic_dict = {"phase": "bootstrap", "network_event": None}
    try:
        if len(sys.argv) == 3 and sys.argv[1] == "fixture":
            payload = fixture_bytes()
            with Path(sys.argv[2]).open("xb") as destination:
                destination.write(payload)
            print(json.dumps({"sha256": hashlib.sha256(payload).hexdigest(), "bytes": len(payload)}))
        elif len(sys.argv) >= 6 and sys.argv[1] == "run":
            run_installed(diagnostic_dict)
        else:
            raise ValueError("invalid proof arguments")
    except (Exception, SystemExit) as error:
        if isinstance(error, SystemExit) and error.code == 130:
            return 130
        print(json.dumps({"code": "installed_acquisition_failed", **diagnostic_dict}), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
