# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed synthetic upstream for an otherwise real installed operator process."""

from __future__ import annotations

import asyncio
import json
import os
import runpy
import sys
import warnings
from pathlib import Path
from types import SimpleNamespace

NPIS = ("1003000126", "1234567893")
SNAPSHOT = "synthetic-fresh-snapshot"
_PHASES = {"bootstrap", "imports", "execute", "verify_upstream", "verify_reads"}
_DENIED_EVENTS = {
    "socket.connect",
    "socket.getaddrinfo",
    "socket.sendto",
    "socket.sendmsg",
    "socket.gethostbyname",
    "socket.gethostbyaddr",
    "socket.getnameinfo",
}


def write_synthetic_credentials(directory):
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    directory.mkdir(mode=0o700)
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    credentials = directory / "snowflake-key-pair.json"
    credentials.write_text(
        json.dumps(
            {
                "account": "synthetic_account",
                "user": "synthetic_user",
                "private_key_pem": key.private_bytes(
                    serialization.Encoding.PEM,
                    serialization.PrivateFormat.PKCS8,
                    serialization.NoEncryption(),
                ).decode(),
            }
        )
    )
    credentials.chmod(0o400)


class SyntheticCursor:
    def __init__(self, sql):
        self.expected = ("USE SECONDARY ROLES NONE", sql)
        self.executed = []
        self.closed = False
        self.exhausted = False
        self.sfqid = "synthetic-fresh-query"
        self.rows = iter([(0, 1, "root_source", SNAPSHOT, None), *((1, 1, "root_source", None, npi) for npi in NPIS)])
        self.description = tuple(
            SimpleNamespace(name=name, type_name=kind, is_nullable=nullable, precision=precision, scale=scale)
            for name, kind, nullable, precision, scale in (
                ("__ci_bundle_row_kind", "FIXED", False, 1, 0),
                ("__ci_stream_ordinal", "FIXED", False, 2, 0),
                ("__ci_stream_id", "TEXT", False, None, None),
                ("__ci_source_snapshot_token", "TEXT", True, None, None),
                ("npi", "TEXT", False, None, None),
            )
        )

    def execute(self, sql):
        assert not self.closed and len(self.executed) < len(self.expected)
        assert sql == self.expected[len(self.executed)]
        self.executed.append(sql)

    def fetchone(self):
        assert not self.closed and tuple(self.executed) == self.expected
        row = next(self.rows, None)
        self.exhausted = row is None
        return row

    def close(self):
        self.closed = True


class SyntheticTransport:
    def __init__(self, sql):
        self.result = SyntheticCursor(sql)
        self.connections = 0
        self.closed = False

    def connect(self, **options):
        assert self.connections == 0
        assert options["account"] == "synthetic_account" and options["user"] == "synthetic_user"
        assert options["role"] == "SYNTHETIC_READER" and options["warehouse"] == "SYNTHETIC_LOAD"
        assert options["authenticator"] == "SNOWFLAKE_JWT"
        assert isinstance(options["private_key"], bytes) and options["private_key"]
        assert options["autocommit"] is False and options["client_session_keep_alive"] is False
        self.connections += 1
        return self

    def cursor(self):
        return self.result

    def close(self):
        self.closed = True

    def verify(self):
        assert self.connections == 1 and self.closed
        assert self.result.closed and self.result.exhausted
        assert tuple(self.result.executed) == self.result.expected


def loopback_database_only(port, diagnostic=None):
    """Fail closed if the installed process tries another network destination."""

    def audit(event, arguments):
        is_allowed = True
        if event == "socket.connect":
            address = arguments[1]
            is_allowed = (
                isinstance(address, tuple)
                and len(address) >= 2
                and address[0] in {"127.0.0.1", "::1"}
                and address[1] == port
            )
        elif event == "socket.getaddrinfo":
            try:
                is_allowed = arguments[0] in {"localhost", "127.0.0.1", "::1"} and int(arguments[1]) == port
            except TypeError, ValueError:
                is_allowed = False
        elif event in _DENIED_EVENTS:
            is_allowed = False
        if not is_allowed:
            if diagnostic is not None:
                diagnostic["network_event"] = event
            raise AssertionError("unexpected network operation")

    return audit


async def verify_installed_read(registered):
    from db.connection import db
    from db.models.custom_import import CustomImportCurrentGeneration
    from process.custom_import.cli import _receipt_only_database_output
    from process.custom_import.read_core import (
        CustomImportReadService,
        ExtensionReadAuthorization,
        ExtensionReadScope,
        PinnedReadTarget,
        ReadFilter,
        SearchRequest,
    )

    class Authorizer:
        def authorize(self, authorization, *, target):
            assert authorization.credential == "synthetic-read" and target.dataset_id == registered["dataset_id"]
            return ExtensionReadScope("synthetic:read")

    with _receipt_only_database_output(db):
        try:
            await db.connect()
            async with db.session() as session:
                pointer = await session.get(CustomImportCurrentGeneration, registered["dataset_id"])
                assert pointer is not None and pointer.pointer_version == 1
                read_target = PinnedReadTarget(
                    dataset_id=registered["dataset_id"],
                    generation_id=pointer.generation_id,
                    definition_revision_id=registered["definition_revision_id"],
                    schema_revision_id=registered["schema_revision_id"],
                    profile_id="synthetic_profile",
                )
            service = CustomImportReadService(authorizer=Authorizer(), cursor_secret=b"r" * 32)
            authorization = ExtensionReadAuthorization("synthetic-read")
            cursor = None
            for index, npi in enumerate(NPIS):
                async with db.session() as session:
                    page = await service.search(
                        session,
                        authorization=authorization,
                        request=SearchRequest(read_target, page_size=1, cursor=cursor),
                    )
                    assert page.total == len(NPIS) and len(page.items) == 1
                    await verify_family_projection(service, session, authorization, read_target, page.items[0], npi)
                    cursor = page.next_cursor
                    assert (cursor is not None) == (index == 0)
            async with db.session() as session:
                filtered = await service.search(
                    session,
                    authorization=authorization,
                    request=SearchRequest(read_target, filters=(ReadFilter("npi", "eq", NPIS[1]),)),
                )
                assert filtered.total == 1 and filtered.items[0].root_fields[0].value == NPIS[1]
        finally:
            await db.disconnect()


async def verify_family_projection(service, session, authorization, read_target, search_item, npi):
    assert [(field.field_id, field.state, field.value) for field in search_item.root_fields] == [("npi", "value", npi)]
    detail = await service.root_detail(
        session, authorization=authorization, target=read_target, winner=search_item.winner
    )
    assert detail.root_fields == search_item.root_fields and detail.children == ()


def run_installed_acquisition(diagnostic):
    entry, configuration, mount, *arguments = sys.argv[1:]
    entry = Path(entry).resolve()
    assert entry.name == "custom_import_snowflake_operator.py"
    assert Path.cwd() != entry.parent and not (entry.parent / "tests").exists()
    sys.path.insert(0, str(entry.parent))
    sys.addaudithook(loopback_database_only(int(os.environ["HLTHPRT_DB_PORT"]), diagnostic))
    configuration = json.loads(Path(configuration).read_text())
    diagnostic["phase"] = "imports"
    with warnings.catch_warnings(action="ignore"):
        import snowflake.connector

        from process.custom_import import admission_launch, snowflake_operator_cli

    assert Path(snowflake_operator_cli.__file__).resolve().is_relative_to(entry.parent)
    mount = Path(mount).resolve()
    assert {path.name for path in mount.iterdir()} == {"snowflake-key-pair.json"}
    # Relocate the fixed operator mount, retaining its real file validation and
    # genuinely absent hosted-stage files. Only the upstream driver is replaced.
    snowflake_operator_cli.FIXED_CREDENTIAL_DIRECTORY = mount
    admission_launch.FIXED_ADMISSION_DIRECTORY = mount
    transport = SyntheticTransport(configuration["sql"])
    snowflake.connector.connect = transport.connect
    sys.argv[:] = [str(entry), *arguments]
    diagnostic["phase"] = "execute"
    try:
        runpy.run_path(str(entry), run_name="__main__")
    except SystemExit as result:
        if result.code != 0:
            raise
    else:
        raise AssertionError("installed entry did not return an exit status")
    diagnostic["phase"] = "verify_upstream"
    transport.verify()
    diagnostic["phase"] = "verify_reads"
    asyncio.run(verify_installed_read(configuration["registered"]))


def failure_metadata(stdout, stderr):
    """Surface only fixed fixture codes, never child output or exception values."""
    for line in stderr.splitlines():
        try:
            value = json.loads(line)
        except ValueError, RecursionError:
            continue
        if (
            type(value) is dict
            and set(value) == {"code", "phase", "network_event"}
            and value.get("code") == "installed_acquisition_failed"
            and isinstance(value.get("phase"), str)
            and value.get("phase") in _PHASES
            and (
                value["network_event"] is None
                or (isinstance(value["network_event"], str) and value["network_event"] in _DENIED_EVENTS)
            )
        ):
            return {key: value[key] for key in ("code", "phase", "network_event")}
    return {"code": "unclassified", "stdout_lines": len(stdout.splitlines()), "stderr_lines": len(stderr.splitlines())}


def main():
    diagnostic_by_key = {"phase": "bootstrap", "network_event": None}
    try:
        run_installed_acquisition(diagnostic_by_key)
    except (Exception, SystemExit) as error:
        if isinstance(error, SystemExit) and error.code == 130:
            return 130
        print(
            json.dumps({"code": "installed_acquisition_failed", **diagnostic_by_key}),
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
