# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Typed CMS NPD staging with a stand-in encoder process and fake COPY connections (no database)."""

import asyncio
import json
import os
import sys
from dataclasses import replace

import pytest

from process import cms_npd_typed_stage as stage
from process.cms_npd_typed_stage import CmsNpdTypedImportError, _verified_counts, file_plan
from tests.test_cms_npd_typed_import import write_release

PAYLOAD = b"synthetic-copy-bytes"
_SCANNER = r"""
import json, os, sys
data = sys.stdin.buffer.read()
outputs = sys.argv[3:]
for path in outputs:
    with open(path, "wb") as output:
        output.write(os.environb.get(b"FAKE_PAYLOAD", b""))
    os.close(int(path.rsplit("/", 1)[1]))  # opening /dev/fd/N may duplicate N: close the inherited end too
with open(os.environ["FAKE_SCANNER_LOG"], "w") as log:
    json.dump({"argv": sys.argv[1:], "threads": os.environ.get("RAYON_NUM_THREADS")}, log)
if os.environ.get("FAKE_SCANNER_MODE") == "hang":
    import time
    time.sleep(60)
if os.environ.get("FAKE_SCANNER_MODE") == "fail":
    sys.stderr.write("first line\nPTG2_SCANNER_ERROR\tline 3\n")
    sys.exit(2)
print("progress line")
print(json.dumps({"resource_type": sys.argv[2], "rows": 2, "address_rows": 1 if len(outputs) == 2 else 0,
                  "input_bytes": len(data)}))
"""


class FakeCopyConnection:
    """Drains the binary COPY source like asyncpg does, then reports a COPY status."""

    def __init__(self, status="COPY 2", error=None):
        self.status, self.error = status, error
        self.received, self.copied_into = b"", None
        self.closed = self.terminated = False

    async def copy_to_table(self, table, *, schema_name, source, **options):
        self.copied_into = (schema_name, table, options["format"])
        async for chunk in source:
            self.received += chunk
        if self.error is not None:
            raise self.error
        return self.status

    async def close(self):
        self.closed = True

    def terminate(self):
        self.terminated = True


@pytest.fixture
def scanner(tmp_path, monkeypatch):
    script = tmp_path / "fake_scanner.py"
    script.write_text(_SCANNER)
    wrapper = tmp_path / "fake-scanner"
    wrapper.write_text(f'#!/bin/sh\nexec "{sys.executable}" "{script}" "$@"\n')
    wrapper.chmod(0o755)
    monkeypatch.setenv("FAKE_PAYLOAD", PAYLOAD.decode())
    monkeypatch.setenv("FAKE_SCANNER_LOG", str(tmp_path / "scanner.json"))
    return wrapper


@pytest.fixture
def plans(tmp_path):
    return file_plan(tmp_path / "release", write_release(tmp_path / "release", 1))


def connector(*connections):
    pending_connections = list(connections)

    async def connect():
        return pending_connections.pop(0)

    return connect


async def test_file_streams_through_the_encoder_into_one_copy_per_table(tmp_path, scanner, plans):
    main, address = FakeCopyConnection("COPY 2"), FakeCopyConnection("COPY 1")
    counts = await stage.copy_file_into_tables(plans[0], "cms_npd_x", connector(main, address), scanner, 3)
    assert counts == {"practitioner": 2, "practitioner_address": 1}
    assert main.copied_into == ("cms_npd_x", "practitioner", "binary")
    assert address.copied_into == ("cms_npd_x", "practitioner_address", "binary")
    assert main.received == address.received == PAYLOAD
    assert main.closed and address.closed and not main.terminated
    log = json.loads((tmp_path / "scanner.json").read_text())
    assert log["argv"][:2] == ["--cms-npd-typed-copy", "Practitioner"] and log["threads"] == "3"
    assert len(log["argv"]) == 4 and all(path.startswith("/dev/fd/") for path in log["argv"][2:])


async def test_single_table_file_uses_one_connection(scanner, plans):
    role = FakeCopyConnection("COPY 2")
    assert await stage.copy_file_into_tables(plans[1], "s", connector(role), scanner, 1) == {"practitioner_role": 2}


async def test_download_size_mismatch_fails_before_spawning(scanner, plans):
    async def never_connect():
        raise AssertionError("no connection for a short download")

    with pytest.raises(CmsNpdTypedImportError) as caught:
        await stage.copy_file_into_tables(replace(plans[1], compressed_bytes=1), "s", never_connect, scanner, 1)
    assert caught.value.control_error_code == "cms_npd_typed_download_size_mismatch"


async def test_failed_encoder_reports_its_last_stderr_line(scanner, plans, monkeypatch):
    monkeypatch.setenv("FAKE_SCANNER_MODE", "fail")
    role = FakeCopyConnection("COPY 0")
    with pytest.raises(CmsNpdTypedImportError) as caught:
        await stage.copy_file_into_tables(plans[1], "s", connector(role), scanner, 1)
    assert caught.value.control_error_code == "cms_npd_typed_scanner_failed"
    assert str(caught.value).endswith(f"{plans[1].name}: PTG2_SCANNER_ERROR\tline 3")
    assert role.terminated and not role.closed


async def test_copy_failure_is_reraised_when_the_encoder_succeeded(scanner, plans):
    role = FakeCopyConnection(error=RuntimeError("synthetic copy failure"))
    with pytest.raises(RuntimeError, match="synthetic copy failure"):
        await stage.copy_file_into_tables(plans[1], "s", connector(role), scanner, 1)
    assert role.terminated


async def test_copy_failure_reports_the_encoder_failure_first(scanner, plans, monkeypatch):
    monkeypatch.setenv("FAKE_SCANNER_MODE", "fail")
    role = FakeCopyConnection(error=RuntimeError("synthetic copy failure"))
    with pytest.raises(CmsNpdTypedImportError) as caught:
        await stage.copy_file_into_tables(plans[1], "s", connector(role), scanner, 1)
    assert caught.value.control_error_code == "cms_npd_typed_scanner_failed"
    assert isinstance(caught.value.__cause__, ExceptionGroup)


async def test_unexpected_copy_status_is_rejected(scanner, plans):
    with pytest.raises(CmsNpdTypedImportError) as caught:
        await stage.copy_file_into_tables(plans[1], "s", connector(FakeCopyConnection("INSERT 0")), scanner, 1)
    assert caught.value.control_error_code == "cms_npd_typed_copy_status_invalid"


async def test_stuck_encoder_is_killed_after_a_copy_failure(scanner, plans, monkeypatch):
    monkeypatch.setenv("FAKE_SCANNER_MODE", "hang")
    role = FakeCopyConnection(error=RuntimeError("synthetic copy failure"))
    started = asyncio.get_running_loop().time()
    with pytest.raises(RuntimeError, match="synthetic copy failure"):
        await stage.copy_file_into_tables(plans[1], "s", connector(role), scanner, 1)
    assert asyncio.get_running_loop().time() - started < 10  # killed, not waited for
    assert role.terminated


def test_feeder_stops_quietly_when_the_encoder_closed_its_input(plans):
    read_fd, write_fd = os.pipe()
    os.close(read_fd)
    assert stage._decompress_into(plans[1].path, write_fd) == 0
    with pytest.raises(OSError):
        os.fstat(write_fd)  # the feeder closed its end


async def test_corrupt_zstd_of_the_right_size_is_rejected(scanner, plans):
    plans[1].path.write_bytes(b"\0" * plans[1].compressed_bytes)
    role = FakeCopyConnection("COPY 2")
    with pytest.raises(CmsNpdTypedImportError) as caught:
        await stage.copy_file_into_tables(plans[1], "s", connector(role), scanner, 1)
    assert caught.value.control_error_code == "cms_npd_typed_zstd_invalid"
    assert role.terminated


async def test_failed_connect_releases_earlier_connections(scanner, plans):
    first = FakeCopyConnection()
    handed_out_connections = []

    async def connect():
        if handed_out_connections:
            raise OSError("synthetic connect failure")
        handed_out_connections.append(first)
        return first

    with pytest.raises(OSError):
        await stage.copy_file_into_tables(plans[0], "s", connect, scanner, 1)
    assert first.terminated or first.closed


def test_summary_must_name_the_file_type_and_empty_address_stream(plans):
    role = plans[1]

    def summary(**fields):
        by_field = {
            "resource_type": "PractitionerRole",
            "rows": 2,
            "address_rows": 0,
            "input_bytes": role.original_bytes,
        }
        return json.dumps({**by_field, **fields}).encode()

    assert _verified_counts(role, 0, summary(), b"", [2], role.original_bytes) == {"practitioner_role": 2}
    for fields, code in (
        ({"resource_type": "Location"}, "cms_npd_typed_scanner_summary_invalid"),
        ({"address_rows": 1}, "cms_npd_typed_copy_count_mismatch"),
    ):
        with pytest.raises(CmsNpdTypedImportError) as caught:
            _verified_counts(role, 0, summary(**fields), b"", [2], role.original_bytes)
        assert caught.value.control_error_code == code
    with pytest.raises(CmsNpdTypedImportError) as caught:
        _verified_counts(role, 1, b"", b"", [2], role.original_bytes)
    assert str(caught.value).endswith(": no message")


def test_manifest_type_must_match_the_staging_slot(tmp_path):
    observed = write_release(tmp_path, 1)
    files = tuple(
        replace(spec, resource_type="Location") if spec.resource_type == "Endpoint" else spec
        for spec in observed.manifest.files
    )
    observed = replace(observed, manifest=replace(observed.manifest, files=files))
    with pytest.raises(CmsNpdTypedImportError) as caught:
        file_plan(tmp_path, observed)
    assert caught.value.control_error_code == "cms_npd_typed_manifest_mismatch"


async def test_stage_files_bounds_concurrency_and_reports_each_file(plans, monkeypatch):
    load_by_name, calls = {"running": 0, "peak": 0}, []

    async def fake_copy(plan, schema, connect, scanner, threads):
        load_by_name["running"] += 1
        load_by_name["peak"] = max(load_by_name["peak"], load_by_name["running"])
        await asyncio.sleep(0)
        load_by_name["running"] -= 1
        calls.append((plan.name, schema, threads))
        return {table: 1 for table in plan.tables}

    async def on_file_done(plan, done):
        progress.append(done)

    progress = []
    monkeypatch.setattr(stage, "copy_file_into_tables", fake_copy)
    monkeypatch.setattr(stage.os, "cpu_count", lambda: 8)
    counts = await stage.stage_files(plans, "s", None, "scanner", concurrency=2, on_file_done=on_file_done)
    assert load_by_name["peak"] == 2 and progress == list(range(1, len(plans) + 1))
    assert set(counts) == {table for plan in plans for table in plan.tables}
    assert {(schema, threads) for _, schema, threads in calls} == {("s", 4)}
    assert await stage.stage_files(plans[:1], "s", None, "scanner") == {"practitioner": 1, "practitioner_address": 1}


async def test_stage_files_raises_the_first_file_failure(plans, monkeypatch):
    async def fake_copy(plan, *_args):
        raise CmsNpdTypedImportError("cms_npd_typed_scanner_failed", plan.name)

    monkeypatch.setattr(stage, "copy_file_into_tables", fake_copy)
    with pytest.raises(CmsNpdTypedImportError) as caught:
        await stage.stage_files(plans[:2], "s", None, "scanner", concurrency=1)
    assert caught.value.control_error_code == "cms_npd_typed_scanner_failed"
    assert isinstance(caught.value.__cause__, ExceptionGroup)
