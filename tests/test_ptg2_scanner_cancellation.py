from __future__ import annotations

import asyncio
import hashlib
import os
import threading
import time
from pathlib import Path

import pytest

from process.ptg_parts import rust_scanner
from process.ptg_parts.ptg2_manifest_artifacts import (
    PTG2_MANIFEST_MEMBERSHIP_FORMAT,
)
from process.ptg_parts.ptg2_shared_graph import (
    MembershipArtifact,
    SharedGraphShardBundle,
)
from tests.ptg2_cancellation_test_binaries import (
    write_blocking_scanner_binary,
    write_retrying_converter_binary,
    write_retrying_scanner_binary,
)


pytestmark = pytest.mark.skipif(
    os.name != "posix", reason="process-group cancellation requires POSIX"
)


async def _wait_for_file(path: Path, timeout: float = 5.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not path.exists():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError(f"timed out waiting for {path}")
        await asyncio.sleep(0.01)


async def _wait_for_pid_file(path: Path, timeout: float = 5.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not path.exists() or not path.read_text().strip():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError(f"timed out waiting for process id in {path}")
        await asyncio.sleep(0.01)


def _is_pid_alive(process_id: int) -> bool:
    try:
        os.kill(process_id, 0)
    except ProcessLookupError:
        return False
    return True


def _assert_pid_exits(process_id: int, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while _is_pid_alive(process_id) and time.monotonic() < deadline:
        time.sleep(0.02)
    assert not _is_pid_alive(process_id), f"process {process_id} survived cancellation"


def _scanner_invocation(tmp_path: Path) -> dict[str, object]:
    return {
        "path": tmp_path / "rates.json.gz",
        "raw_source_sha256": "a" * 64,
        "snapshot_id": "snapshot",
        "plan_id": "plan",
        "coverage_scope_id": "c" * 64,
        "plan_month_id": "month",
        "source_trace_set_hash": "trace",
        "v3_serving_run_directory": tmp_path / "runs",
    }


def _shared_graph_bundle(tmp_path: Path) -> SharedGraphShardBundle:
    artifacts = []
    for artifact_index, marker in enumerate((b"a", b"b", b"c", b"d")):
        artifact_path = tmp_path / f"graph-{artifact_index}.sidecar"
        artifact_path.write_bytes(marker)
        artifacts.append(
            MembershipArtifact(
                path=artifact_path,
                metadata={
                    "record_format": PTG2_MANIFEST_MEMBERSHIP_FORMAT,
                    "sha256": hashlib.sha256(marker).hexdigest(),
                    "byte_count": len(marker),
                    "owner_count": 1,
                    "member_count": 1,
                },
            )
        )
    return SharedGraphShardBundle("shard-01", *artifacts)


def _reader_threads() -> list[threading.Thread]:
    return [
        thread
        for thread in threading.enumerate()
        if thread.is_alive()
        and thread.name in {"ptg2-rust-compact-stdout", "ptg2-rust-compact-stderr"}
    ]


async def _cancel_twice_after_term_signal(
    task: asyncio.Task[object],
    *,
    parent_pid_path: Path,
    child_pid_path: Path,
    term_seen_path: Path,
) -> tuple[int, int]:
    await _wait_for_pid_file(parent_pid_path)
    await _wait_for_pid_file(child_pid_path)
    parent_pid = int(parent_pid_path.read_text())
    child_pid = int(child_pid_path.read_text())
    task.cancel()
    await _wait_for_file(term_seen_path)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(asyncio.shield(task), timeout=5)
    return parent_pid, child_pid


@pytest.mark.asyncio
async def test_async_scanner_cancellation_reaps_executing_reader_process_group(
    tmp_path,
    monkeypatch,
):
    scanner_binary = write_blocking_scanner_binary(tmp_path)
    parent_pid_path = tmp_path / "scanner-parent.pid"
    child_pid_path = tmp_path / "scanner-child.pid"
    monkeypatch.setenv("PTG_SCANNER_PARENT_PID", str(parent_pid_path))
    monkeypatch.setenv("PTG_SCANNER_CHILD_PID", str(child_pid_path))
    monkeypatch.setattr(
        rust_scanner, "_ptg2_rust_scanner_binary", lambda: scanner_binary
    )
    monkeypatch.setattr(rust_scanner, "_PROCESS_GROUP_TERM_TIMEOUT_SECONDS", 0.1)
    monkeypatch.setattr(rust_scanner, "_PROCESS_GROUP_KILL_TIMEOUT_SECONDS", 2.0)

    scanner_records = rust_scanner._aiter_compact_serving_records_rust(
        **_scanner_invocation(tmp_path)
    )
    next_record = asyncio.create_task(scanner_records.__anext__())
    await _wait_for_pid_file(parent_pid_path)
    await _wait_for_pid_file(child_pid_path)
    parent_pid = int(parent_pid_path.read_text())
    child_pid = int(child_pid_path.read_text())

    next_record.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(next_record, timeout=5)

    _assert_pid_exits(parent_pid)
    _assert_pid_exits(child_pid)
    assert not rust_scanner._is_process_group_alive(parent_pid)
    assert not _reader_threads()


@pytest.mark.asyncio
async def test_repeated_cancellation_finishes_cleanup_before_same_path_retry(
    tmp_path,
    monkeypatch,
):
    """Repeated cancellation must finish cleanup before a same-path retry."""

    scanner_binary = write_retrying_scanner_binary(tmp_path)
    parent_pid_path = tmp_path / "scanner-parent.pid"
    child_pid_path = tmp_path / "scanner-child.pid"
    term_seen_path = tmp_path / "scanner-term-seen"
    output_path = tmp_path / "scanner-output"
    attempts_path = tmp_path / "scanner-attempts"
    monkeypatch.setenv("PTG_SCANNER_PARENT_PID", str(parent_pid_path))
    monkeypatch.setenv("PTG_SCANNER_CHILD_PID", str(child_pid_path))
    monkeypatch.setenv("PTG_SCANNER_TERM_SEEN", str(term_seen_path))
    monkeypatch.setenv("PTG_SCANNER_OUTPUT", str(output_path))
    monkeypatch.setenv("PTG_SCANNER_ATTEMPTS", str(attempts_path))
    monkeypatch.setattr(
        rust_scanner, "_ptg2_rust_scanner_binary", lambda: scanner_binary
    )
    monkeypatch.setattr(rust_scanner, "_PROCESS_GROUP_TERM_TIMEOUT_SECONDS", 0.3)
    monkeypatch.setattr(rust_scanner, "_PROCESS_GROUP_KILL_TIMEOUT_SECONDS", 2.0)
    invocation = _scanner_invocation(tmp_path)

    scanner_records = rust_scanner._aiter_compact_serving_records_rust(**invocation)
    first_record = asyncio.create_task(scanner_records.__anext__())
    parent_pid, child_pid = await _cancel_twice_after_term_signal(
        first_record,
        parent_pid_path=parent_pid_path,
        child_pid_path=child_pid_path,
        term_seen_path=term_seen_path,
    )

    is_cleanup_finished_before_return = (
        not _is_pid_alive(parent_pid)
        and not _is_pid_alive(child_pid)
        and not rust_scanner._is_process_group_alive(parent_pid)
        and not _reader_threads()
    )
    _assert_pid_exits(parent_pid)
    _assert_pid_exits(child_pid)
    assert is_cleanup_finished_before_return

    retry_records = [
        scanner_record
        async for scanner_record in rust_scanner._aiter_compact_serving_records_rust(
            **invocation
        )
    ]
    await asyncio.sleep(0.35)

    assert [record_kind for record_kind, _payload in retry_records] == [
        "scanner_config",
        "scanner_summary",
    ]
    assert retry_records[-1][1]["attempt"] == 2
    assert attempts_path.read_text() == "2"
    assert output_path.read_text() == "retry-complete"
    assert not _reader_threads()


@pytest.mark.asyncio
async def test_shared_graph_converter_double_cancel_allows_same_path_retry(
    tmp_path,
    monkeypatch,
):
    """Converter cancellation must reap descendants and remove its manifest."""

    converter_binary = write_retrying_converter_binary(tmp_path)
    environment_path_by_name = {
        "PTG_CONVERTER_ATTEMPTS": tmp_path / "converter-attempts",
        "PTG_CONVERTER_PARENT_PID": tmp_path / "converter-parent.pid",
        "PTG_CONVERTER_CHILD_PID": tmp_path / "converter-child.pid",
        "PTG_CONVERTER_TERM_SEEN": tmp_path / "converter-term-seen",
    }
    for environment_name, environment_path in environment_path_by_name.items():
        monkeypatch.setenv(environment_name, str(environment_path))
    monkeypatch.setattr(
        rust_scanner, "_ptg2_rust_scanner_binary", lambda: converter_binary
    )
    monkeypatch.setattr(rust_scanner, "_PROCESS_GROUP_TERM_TIMEOUT_SECONDS", 0.2)
    monkeypatch.setattr(rust_scanner, "_PROCESS_GROUP_KILL_TIMEOUT_SECONDS", 2.0)
    monkeypatch.setattr(
        rust_scanner,
        "_parse_shared_graph_summary_frame",
        lambda *_args, **_kwargs: "converted",
    )
    provider_map_path = tmp_path / "provider-set-map.tsv"
    provider_map_path.write_text(f"{'01' * 16}\t1\n", encoding="ascii")
    output_directory = tmp_path / "shared-graph-output"
    manifest_path = tmp_path / "shared-graph-output.manifest.json"

    conversion_task = asyncio.create_task(
        rust_scanner.convert_v3_provider_membership_shards_to_shared_graph_rust(
            shards=(_shared_graph_bundle(tmp_path),),
            provider_set_key_map_path=provider_map_path,
            output_directory=output_directory,
        )
    )
    parent_pid, child_pid = await _cancel_twice_after_term_signal(
        conversion_task,
        parent_pid_path=environment_path_by_name["PTG_CONVERTER_PARENT_PID"],
        child_pid_path=environment_path_by_name["PTG_CONVERTER_CHILD_PID"],
        term_seen_path=environment_path_by_name["PTG_CONVERTER_TERM_SEEN"],
    )

    _assert_pid_exits(parent_pid)
    _assert_pid_exits(child_pid)
    assert not manifest_path.exists()
    assert not output_directory.exists()
    retry_result = (
        await rust_scanner.convert_v3_provider_membership_shards_to_shared_graph_rust(
            shards=(_shared_graph_bundle(tmp_path),),
            provider_set_key_map_path=provider_map_path,
            output_directory=output_directory,
        )
    )
    assert retry_result == "converted"
    assert environment_path_by_name["PTG_CONVERTER_ATTEMPTS"].read_text() == "2"
    assert not manifest_path.exists()
