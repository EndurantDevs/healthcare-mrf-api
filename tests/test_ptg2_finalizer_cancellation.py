from __future__ import annotations

import asyncio
import hashlib
import os
import time
from pathlib import Path

import pytest

from process.ptg_parts import ptg2_shared_finalize as finalizer
from process.ptg_parts import rust_scanner
from tests.ptg2_cancellation_test_binaries import (
    write_retrying_finalizer_binary,
)


pytestmark = pytest.mark.skipif(
    os.name != "posix", reason="process-group cancellation requires POSIX"
)


class _TestResourceConfiguration:
    def command_arguments(self) -> tuple[str, ...]:
        return (
            "--workers",
            "1",
            "--identity-map-max-bytes",
            "67108864",
            "--total-sort-memory-bytes",
            "67108864",
        )

    def contract_metadata(self) -> dict[str, object]:
        return {
            "contract": finalizer.PTG2_V3_FINALIZER_RESOURCE_CONTRACT,
            "workers": 1,
            "identity_map_max_bytes": 67108864,
            "total_sort_memory_bytes": 67108864,
            "sort_memory_scope": "process_total_across_workers_v1",
        }

    def validation_metadata(self) -> dict[str, object]:
        return {"test_resource_configuration": True}


async def _wait_for_file(path: Path, timeout: float = 5.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while not path.exists():
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError(f"timed out waiting for {path}")
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


def _finalizer_inputs(tmp_path: Path):
    """Build authenticated finalizer inputs for cancellation tests."""

    source_identity_map = {
        "source_type": "in_network",
        "identity_kind": "logical_json_sha256_v1",
        "identity_sha256": "a" * 64,
    }
    serving_path = tmp_path / "run.ready"
    serving_path.write_bytes(b"r" * 52)
    serving_entries = finalizer.attach_v3_source_run_contract(
        [
            {
                "path": str(serving_path),
                "row_count": 1,
                "bytes": 52,
                "partition": 0,
                "partition_count": 1,
                "format": "ptg2_v3_serving_run",
                "version": 1,
                "sha256": hashlib.sha256(serving_path.read_bytes()).hexdigest(),
            }
        ],
        source_identity=source_identity_map,
        scanner_summary={
            "serving_run_files": 1,
            "serving_run_rows": 1,
            "serving_run_bytes": 52,
        },
        scanner_config={"serving_run_partition_count": 1},
    )
    code_path = tmp_path / "codes.ready"
    code_path.write_bytes(b"c" * 64)
    code_entries = finalizer.attach_v3_dictionary_contract(
        [
            {
                "path": str(code_path),
                "row_count": 1,
                "bytes": 64,
                "format": "ptg2_v3_serving_code_dictionary",
                "version": 4,
                "sha256": hashlib.sha256(code_path.read_bytes()).hexdigest(),
            }
        ],
        source_identity=source_identity_map,
        source_run_contract_sha256=serving_entries[0][
            "source_run_contract_sha256"
        ],
        scanner_summary={
            "serving_code_dictionary_files": 1,
            "serving_code_dictionary_rows": 1,
            "serving_code_dictionary_bytes": 64,
        },
    )
    provider_metadata_path = tmp_path / "provider-metadata.copy"
    provider_metadata_payload = b"00000000000000000000000000000001\t1\t{}\n"
    provider_metadata_path.write_bytes(provider_metadata_payload)
    provider_metadata_entries = [
        {
            "path": str(provider_metadata_path),
            "row_count": 1,
            "bytes": len(provider_metadata_payload),
            "sha256": hashlib.sha256(provider_metadata_payload).hexdigest(),
            "format": "ptg2_v3_provider_set_metadata_copy",
            "version": 1,
            **source_identity_map,
            "source_run_contract_sha256": serving_entries[0][
                "source_run_contract_sha256"
            ],
        }
    ]
    price_key_map = tmp_path / "price-key-map.copy"
    price_key_map.write_bytes(b"map")
    return (
        source_identity_map,
        serving_entries,
        code_entries,
        provider_metadata_entries,
        price_key_map,
    )


@pytest.mark.parametrize("cancel_mode", ["cancel", "timeout"])
@pytest.mark.asyncio
async def test_finalizer_cancellation_reaps_group_and_allows_same_directory_retry(
    tmp_path,
    monkeypatch,
    cancel_mode,
):
    """Cancel or time out a process group, then prove the workdir is reusable."""

    binary = write_retrying_finalizer_binary(tmp_path)
    work_directory = tmp_path / "work"
    parent_pid_path = tmp_path / "finalizer-parent.pid"
    child_pid_path = tmp_path / "finalizer-child.pid"
    monkeypatch.setenv("PTG_FINALIZER_ATTEMPTS", str(tmp_path / "attempts"))
    monkeypatch.setenv("PTG_FINALIZER_PARENT_PID", str(parent_pid_path))
    monkeypatch.setenv("PTG_FINALIZER_CHILD_PID", str(child_pid_path))
    monkeypatch.setattr(finalizer, "_ptg2_rust_scanner_binary", lambda: binary)
    monkeypatch.setattr(
        finalizer,
        "_load_v3_finalizer_resource_configuration",
        _TestResourceConfiguration,
    )
    monkeypatch.setattr(rust_scanner, "_PROCESS_GROUP_TERM_TIMEOUT_SECONDS", 0.1)
    monkeypatch.setattr(rust_scanner, "_PROCESS_GROUP_KILL_TIMEOUT_SECONDS", 2.0)
    (
        identity,
        serving_entries,
        code_entries,
        provider_metadata_entries,
        price_key_map,
    ) = _finalizer_inputs(tmp_path)
    finalizer_argument_map = dict(
        work_directory=work_directory,
        serving_run_entries=serving_entries,
        code_dictionary_entries=code_entries,
        provider_set_metadata_entries=provider_metadata_entries,
        expected_source_identities=[identity],
        price_key_map_input=price_key_map,
        price_key_map_row_count=1,
    )

    first_attempt = asyncio.create_task(
        finalizer.run_v3_direct_finalizer(**finalizer_argument_map)
    )
    await _wait_for_file(parent_pid_path)
    await _wait_for_file(child_pid_path)
    parent_pid = int(parent_pid_path.read_text())
    child_pid = int(child_pid_path.read_text())

    if cancel_mode == "cancel":
        first_attempt.cancel()
        with pytest.raises(asyncio.CancelledError):
            await first_attempt
    else:
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(first_attempt, timeout=0.01)

    _assert_pid_exits(parent_pid)
    _assert_pid_exits(child_pid)
    assert not (work_directory / "finalized").exists()
    assert not (work_directory / "scanner-summary.json").exists()
    assert not list(work_directory.glob(".finalized.ptg2-finalizer-*.tmp"))

    summary = await finalizer.run_v3_direct_finalizer(**finalizer_argument_map)

    assert summary["output_directory"] == str((work_directory / "finalized").resolve())
    assert (work_directory / "finalized" / "committed.marker").read_text() == "2"
    assert (work_directory / "scanner-summary.json").is_file()
