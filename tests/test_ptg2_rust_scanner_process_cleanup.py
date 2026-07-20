from __future__ import annotations

import io
import json

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

    def terminate(running_process):
        terminated_process_list.append(running_process)
        running_process.returncode = -15
        return running_process.returncode

    monkeypatch.setattr(rust_scanner, "_ptg2_rust_scanner_binary", lambda: binary)
    monkeypatch.setattr(
        rust_scanner.subprocess,
        "Popen",
        lambda *_args, **_kwargs: process,
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
