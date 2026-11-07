# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Real file consumption and module entry; terminal database transport is doubled."""

import os
import runpy
import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from db import connection
from process.custom_import import family_shard_handoff as handoff
from process.custom_import import family_shard_owner_cli as owner_cli
from process.custom_import import family_shard_retirement_cli as cli
from process.custom_import import family_shard_retirement_files as files
from process.custom_import import family_shard_retirement_handoff as custody
from process.custom_import import family_shard_retirement_native as native
from process.custom_import.family_shard_owner_lock import acquire_owner_job_lock
from process.custom_import.family_shard_retirement_policy import RetiredFilePolicy
from tests.owner_attempt_support import recovery


@pytest.fixture
def committed_inventory(tmp_path, monkeypatch):
    value = recovery(tmp_path, monkeypatch)
    root = value.root / "work/owner"
    root.mkdir(mode=0o700)
    lock = acquire_owner_job_lock(root, identity=(root.stat().st_dev, root.stat().st_ino))
    (root / "source").mkdir(mode=0o700)
    path = root / "source/output.mpk"
    path.write_bytes(b"synthetic original output")
    path.chmod(0o400)
    value.native = SimpleNamespace(sha256="e" * 64)
    value.document = custody.stage(value.root, value.parent, value.native, value.profile)
    value.context = custody.context(value.document, work=value.root / "work")
    policy = RetiredFilePolicy(root)
    policy.work(root / "source")
    files.seal_inventory(value.context, policy, lock.check)
    lock.close()
    value.identifier = handoff.identity(value.parent)
    value.intent = custody.identity(value.document)
    value.database = SimpleNamespace(connect=AsyncMock(), disconnect=AsyncMock(), session_factory=object())
    limits = owner_cli.source_limits(value.profile["source_limits"])
    monkeypatch.setattr(handoff, "OWNER_INPUT", value.root)
    monkeypatch.setattr(handoff, "OWNER_PARENT", value.root / "work")
    monkeypatch.setattr(cli, "profile", lambda: (value.profile, value.public, limits))
    monkeypatch.setattr(cli, "read_retired_scope", AsyncMock(return_value=value.native))
    return value


@pytest.mark.asyncio
async def test_resume_consumes_only_original_sealed_files_and_is_idempotent(committed_inventory):
    value = committed_inventory
    expected_by_field = {
        "status": "retired_files_cleaned",
        "handoff_id": value.identifier,
        "intent_sha256": value.intent,
    }
    assert await cli.run_cleanup(value.identifier, value.intent, value.database) == expected_by_field
    assert not value.context.root.exists()
    assert set(os.listdir(value.context.state)) == {"inventory.mpk", "sealed", "drained", "done", "completion.json"}
    assert await cli.run_cleanup(value.identifier, value.intent, value.database) == expected_by_field
    assert value.database.connect.await_count == value.database.disconnect.await_count == 2


@pytest.mark.asyncio
async def test_drained_lock_unlink_interruption_resumes_without_recreating_lock(committed_inventory, monkeypatch):
    value = committed_inventory
    original = os.rmdir

    def interrupted(path, *args, **kwargs):
        if Path(path) == value.context.root:
            raise OSError("synthetic interruption after original lock unlink")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(os, "rmdir", interrupted)
    with pytest.raises(OSError):
        await cli.run_cleanup(value.identifier, value.intent, value.database)
    assert value.context.root.exists() and not list(value.context.root.iterdir())
    assert (value.context.state / "drained").exists()
    monkeypatch.setattr(os, "rmdir", original)
    assert (await cli.run_cleanup(value.identifier, value.intent, value.database))["status"] == "retired_files_cleaned"
    assert not value.context.root.exists()


@pytest.mark.asyncio
async def test_native_retirement_change_cannot_consume_a_sealed_inventory(committed_inventory, monkeypatch):
    value = committed_inventory
    monkeypatch.setattr(cli, "read_retired_scope", AsyncMock(return_value=SimpleNamespace(sha256="f" * 64)))
    with pytest.raises(ValueError):
        await cli.run_cleanup(value.identifier, value.intent, value.database)
    assert (value.context.root / "source/output.mpk").exists()
    value.database.disconnect.assert_awaited_once()


def test_actual_module_entry_success_uses_standard_async_runner_and_safe_receipt(
    committed_inventory, monkeypatch, capsys
):
    value = committed_inventory
    limits = owner_cli.source_limits(value.profile["source_limits"])
    monkeypatch.setattr(owner_cli, "profile", lambda: (value.profile, value.public, limits))
    monkeypatch.setattr(native, "read_retired_scope", AsyncMock(return_value=value.native))
    monkeypatch.setattr(connection, "db", value.database)
    monkeypatch.setattr(sys, "argv", ["retired-owner", value.identifier, value.intent])
    with pytest.raises(SystemExit) as result:
        runpy.run_path(cli.__file__, run_name="__main__")
    assert result.value.code == 0
    output = capsys.readouterr()
    assert output.err == ""
    assert handoff.document(output.out.rstrip("\n").encode()) == {
        "status": "retired_files_cleaned",
        "handoff_id": value.identifier,
        "intent_sha256": value.intent,
    }
    assert not value.context.root.exists()


def test_real_python_module_unavailable_receipt_never_echoes_credentials(tmp_path):
    environment_by_key = dict(os.environ)
    environment_by_key.pop(owner_cli.PROFILE_ENV, None)
    environment_by_key["HLTHPRT_DB_PASSWORD"] = "synthetic-never-log-this"
    environment_by_key["PYTHONDONTWRITEBYTECODE"] = "1"
    environment_by_key["PYTHONPYCACHEPREFIX"] = str(tmp_path / "pycache")
    environment_by_key["PYTHONWARNINGS"] = "error,ignore:'return' in a 'finally' block:SyntaxWarning"
    result = subprocess.run(
        [sys.executable, "-B", "-m", "process.custom_import.family_shard_retirement_cli", "a" * 64, "b" * 64],
        check=False,
        capture_output=True,
        text=True,
        env=environment_by_key,
        timeout=30,
    )
    assert result.returncode == 1
    assert result.stdout == '{"status":"retired_files_unavailable"}\n'
    assert result.stderr == ""
    assert "synthetic-never-log-this" not in result.stdout + result.stderr


def _replace_declared_inventory_with_producer(value):
    """Replace this fixture's synthetic declaration with a real protected producer seal."""
    from process.custom_import import family_shard_terminal_files as producer

    for name in ("inventory.mpk", "sealed"):
        (value.context.state / name).unlink()
    (value.root / custody.INTENT).unlink()
    value.native.scope = (7, b"synthetic-scope")
    value.native.head = SimpleNamespace(revision=3, sha256=b"h" * 32)
    lock = acquire_owner_job_lock(value.context.root, identity=value.context.root_identity[:2])
    original = producer.original_context(value.context.root, value.parent, value.profile, value.native, lock)
    policy = RetiredFilePolicy(value.context.root)
    policy.work(value.context.root / "source")
    entries, digest = producer.seal(original, policy, lock.check)
    return producer, original, lock, entries, digest


@pytest.mark.asyncio
@pytest.mark.parametrize("remaining", ["partial", "empty_root", "absent_root", "compact_only"])
async def test_native_gated_cli_joins_producer_and_finishes_each_cleanup_boundary(committed_inventory, remaining):
    value = committed_inventory
    producer, original, lock, entries, _digest = _replace_declared_inventory_with_producer(value)
    try:
        (value.context.root / "source/output.mpk").unlink()
        if remaining != "partial":
            files._consume_entries(original, entries, lock.check)
            producer.mark_drained(original, lock.check)
            if remaining == "empty_root":
                os.unlink(value.context.root / ".owner-job.lock")
            else:
                lock.remove_terminal_root()
        if remaining == "compact_only":
            (original.state / producer.INVENTORY).unlink()
            (original.state / producer.DRAINED).unlink()
    finally:
        lock.close()
    value.document = custody.stage(value.root, value.parent, value.native, value.profile)
    value.intent = custody.identity(value.document)
    observed = await cli.run_cleanup(value.identifier, value.intent, value.database)
    assert observed["status"] == "retired_files_cleaned"
    assert not value.context.root.exists()
    assert set(os.listdir(value.context.state)) == {"inventory.mpk", "sealed", "drained", "done", "completion.json"}
    assert await cli.run_cleanup(value.identifier, value.intent, value.database) == observed


@pytest.mark.asyncio
async def test_producer_census_cannot_replace_native_retirement_authority(committed_inventory, monkeypatch):
    value = committed_inventory
    _producer, _original, lock, _entries, _digest = _replace_declared_inventory_with_producer(value)
    lock.close()
    value.document = custody.stage(value.root, value.parent, value.native, value.profile)
    monkeypatch.setattr(cli, "read_retired_scope", AsyncMock(side_effect=ValueError("native still active")))
    with pytest.raises(ValueError, match="native still active"):
        await cli.run_cleanup(value.identifier, custody.identity(value.document), value.database)
    assert (value.context.root / "source/output.mpk").exists()
    assert not (value.context.state / "sealed").exists()
