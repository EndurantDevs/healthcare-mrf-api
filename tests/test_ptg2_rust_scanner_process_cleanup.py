from __future__ import annotations

import io
import json
from pathlib import Path

from process.ptg_parts import rust_scanner


class _RunningScannerProcess:
    def __init__(self, stdout: bytes, *, stderr: bytes = b""):
        self.stdout = io.BytesIO(stdout)
        self.stderr = io.BytesIO(stderr)
        self.returncode: int | None = None
        self.wait_calls = 0

    def poll(self) -> int | None:
        return self.returncode

    def wait(self, timeout: float | None = None) -> int:
        del timeout
        self.wait_calls += 1
        assert self.returncode is not None
        return self.returncode


def _frame(record_kind: str, payload: dict[str, object]) -> bytes:
    encoded = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return (
        record_kind.encode("ascii")
        + b"\t"
        + str(len(encoded)).encode("ascii")
        + b"\n"
        + encoded
        + b"\n"
    )


def test_compact_scanner_reaps_process_still_running_after_stdout(
    tmp_path,
    monkeypatch,
):
    """Prove framed EOF cannot leave the scanner process or scratch alive."""

    binary = tmp_path / "ptg2_scanner"
    binary.write_text("fake", encoding="ascii")
    binary.chmod(0o755)
    scanner_config_map = {
        "snapshot_arch": "postgres_binary_v3",
        "storage_generation": "shared_blocks_v3",
        "serving_row_semantics": "source_multiset_v1",
        "serving_run_format": "ptg2_v3_serving_run",
        "serving_run_version": 1,
    }
    process = _RunningScannerProcess(
        _frame("scanner_config", scanner_config_map) + _frame("scanner_summary", {})
    )
    terminated_process_list = []
    observed_scratch_paths: list[Path] = []

    def terminate(running_process):
        terminated_process_list.append(running_process)
        running_process.returncode = -15
        return running_process.returncode

    monkeypatch.setattr(rust_scanner, "_ptg2_rust_scanner_binary", lambda: binary)

    def fake_popen(*_args, **kwargs):
        scratch_path = Path(
            kwargs["env"][rust_scanner._SOURCE_WITNESS_SCRATCH_DIR_ENV]
        )
        assert scratch_path.is_dir()
        assert rust_scanner._source_witness_scratch_marker(scratch_path).is_file()
        observed_scratch_paths.append(scratch_path)
        return process

    monkeypatch.setattr(
        rust_scanner.subprocess,
        "Popen",
        fake_popen,
    )
    monkeypatch.setattr(rust_scanner, "_terminate_subprocess_group", terminate)

    scanner_records = list(
        rust_scanner._iter_compact_serving_records_rust(
            tmp_path / "input.json",
            raw_source_sha256="a" * 64,
            snapshot_id="snapshot",
            plan_id="plan",
            coverage_scope_id="cc" * 32,
            plan_month_id="month",
            source_trace_set_hash="trace",
            v3_serving_run_directory=tmp_path / "runs",
        )
    )

    assert [record_kind for record_kind, _payload in scanner_records] == [
        "scanner_config",
        "scanner_summary",
    ]
    assert terminated_process_list == [process]
    assert process.wait_calls == 1
    assert len(observed_scratch_paths) == 1
    assert not observed_scratch_paths[0].exists()


def test_compact_scanner_removes_scratch_when_spawn_fails(tmp_path, monkeypatch):
    binary = tmp_path / "ptg2_scanner"
    binary.write_text("fake", encoding="ascii")
    binary.chmod(0o755)
    observed_scratch_paths: list[Path] = []

    def fail_spawn(*_args, **kwargs):
        scratch_path = Path(
            kwargs["env"][rust_scanner._SOURCE_WITNESS_SCRATCH_DIR_ENV]
        )
        assert scratch_path.is_dir()
        observed_scratch_paths.append(scratch_path)
        raise OSError("spawn failed")

    monkeypatch.setattr(rust_scanner, "_ptg2_rust_scanner_binary", lambda: binary)
    monkeypatch.setattr(rust_scanner.subprocess, "Popen", fail_spawn)

    try:
        list(
            rust_scanner._iter_compact_serving_records_rust(
                tmp_path / "input.json",
                raw_source_sha256="a" * 64,
                snapshot_id="snapshot",
                plan_id="plan",
                coverage_scope_id="cc" * 32,
                plan_month_id="month",
                source_trace_set_hash="trace",
                v3_serving_run_directory=tmp_path / "runs",
            )
        )
    except OSError as exc:
        assert "spawn failed" in str(exc)
    else:
        raise AssertionError("scanner spawn failure was not propagated")

    assert len(observed_scratch_paths) == 1
    assert not observed_scratch_paths[0].exists()


def test_scratch_orphan_reaper_requires_authenticated_dead_owner(
    tmp_path,
    monkeypatch,
):
    run_directory = tmp_path / "runs"
    run_directory.mkdir()
    orphan = run_directory / f"{rust_scanner._SOURCE_WITNESS_SCRATCH_PREFIX}orphan"
    orphan.mkdir()
    rust_scanner._source_witness_scratch_marker(orphan).write_text(
        json.dumps(
            {
                "contract": "healthporta_ptg2_source_witness_scratch_v1",
                "pid": 424242,
            }
        ),
        encoding="ascii",
    )
    unmarked = run_directory / f"{rust_scanner._SOURCE_WITNESS_SCRATCH_PREFIX}unmarked"
    unmarked.mkdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    symlink = run_directory / f"{rust_scanner._SOURCE_WITNESS_SCRATCH_PREFIX}symlink"
    symlink.symlink_to(outside, target_is_directory=True)
    monkeypatch.setattr(rust_scanner, "_is_live_local_process", lambda _pid: False)

    created = rust_scanner._prepare_source_witness_scratch_directory(run_directory)

    assert not orphan.exists()
    assert unmarked.is_dir()
    assert symlink.is_symlink()
    assert outside.is_dir()
    assert created.is_dir()
    assert rust_scanner._is_source_witness_scratch_removed(
        created,
        require_orphan=False,
    )


def test_top_level_scanner_frames_and_reaps_process(
    tmp_path,
    monkeypatch,
):
    """Frame top-level records and reap a fake scanner without a native binary."""

    binary = tmp_path / "ptg2_scanner"
    binary.write_text("fake", encoding="ascii")
    binary.chmod(0o755)
    frame_by_field = {"negotiation_arrangement": "ffs"}
    process = _RunningScannerProcess(
        _frame("in_network", frame_by_field),
        stderr=b"scanner warning\n",
    )
    terminated_process_list = []

    def terminate(running_process):
        terminated_process_list.append(running_process)
        running_process.returncode = 0
        return running_process.returncode

    monkeypatch.setattr(rust_scanner, "_ptg2_rust_scanner_binary", lambda: binary)
    monkeypatch.setattr(
        rust_scanner.subprocess,
        "Popen",
        lambda *_args, **_kwargs: process,
    )
    monkeypatch.setattr(rust_scanner, "_terminate_subprocess_group", terminate)

    scanner_records = list(
        rust_scanner._iter_top_level_object_bytes_rust(
            tmp_path / "input.json",
            {"in_network"},
            live_progress_context={},
        )
    )

    assert scanner_records == [
        ("in_network", json.dumps(frame_by_field, separators=(",", ":")).encode())
    ]
    assert terminated_process_list == [process]
    assert process.wait_calls == 1
