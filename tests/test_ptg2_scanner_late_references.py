# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import hashlib
import importlib.util
import io
import json
import os
import shutil
import signal
import subprocess
import time
from pathlib import Path

import pytest

_SUPPORT_PATH = Path(__file__).with_name("test_ptg2_scanner_parallelism.py")
_SUPPORT_SPEC = importlib.util.spec_from_file_location("ptg2_scanner_late_reference_support", _SUPPORT_PATH)
assert _SUPPORT_SPEC is not None and _SUPPORT_SPEC.loader is not None
_SUPPORT = importlib.util.module_from_spec(_SUPPORT_SPEC)
_SUPPORT_SPEC.loader.exec_module(_SUPPORT)


def _single_frame(run: dict, record_kind: str) -> dict:
    matches = [payload for kind, payload in run["frames"] if kind == record_kind]
    assert len(matches) == 1
    return matches[0]


@pytest.fixture(scope="module")
def scanner_plain_late_reference_runs(tmp_path_factory):
    scanner_binary = _SUPPORT._built_scanner_binary()
    base = tmp_path_factory.mktemp("ptg2-scanner-plain-late-references")
    fixture = _SUPPORT._scanner_fixture_payload(procedure_count=240)
    artifact = base / "rates.json"
    artifact.write_text(
        json.dumps(
            {
                "reporting_entity_name": fixture["reporting_entity_name"],
                "in_network": fixture["in_network"],
                "provider_references": fixture["provider_references"],
            },
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )
    return {
        workers: _SUPPORT._run_parallel_scanner(
            scanner_binary,
            artifact,
            base / f"workers-{workers}",
            workers=workers,
            sidecar_kinds=tuple(_SUPPORT.SIDECAR_ENV_BY_KIND),
        )
        for workers in (1, 8)
    }


@pytest.fixture(scope="module")
def scanner_gzip_late_reference_runs(tmp_path_factory):
    scanner_binary = _SUPPORT._built_scanner_binary()
    base = tmp_path_factory.mktemp("ptg2-scanner-gzip-late-references")
    fixture = _SUPPORT._scanner_fixture_payload(procedure_count=240)
    artifact = base / "rates.json.gz"
    _SUPPORT._write_gzip_json(
        artifact,
        {
            "reporting_entity_name": fixture["reporting_entity_name"],
            "in_network": fixture["in_network"],
            "provider_references": fixture["provider_references"],
        },
    )
    return {
        workers: _SUPPORT._run_parallel_scanner(
            scanner_binary,
            artifact,
            base / f"workers-{workers}",
            workers=workers,
            sidecar_kinds=tuple(_SUPPORT.SIDECAR_ENV_BY_KIND),
            env_overrides={
                "HLTHPRT_PTG2_RUST_RAPIDGZIP_ENABLED": "true",
                "HLTHPRT_PTG2_RUST_RAPIDGZIP_THREADS": str(workers),
                "HLTHPRT_PTG2_RUST_INDEXED_RANGE_PRODUCERS": str(workers),
            },
        )
        for workers in (1, 8, 16)
    }


def test_late_plain_provider_references_are_reordered_and_processed_in_parallel(
    scanner_plain_late_reference_runs,
):
    baseline = scanner_plain_late_reference_runs[1]
    candidate = scanner_plain_late_reference_runs[8]

    assert candidate["copy_rows"] == baseline["copy_rows"]
    assert candidate["copy_digests"] == baseline["copy_digests"]
    assert candidate["sidecar_digests"] == baseline["sidecar_digests"]
    assert candidate["serving_records"] == baseline["serving_records"]
    assert candidate["serving_digest"] == baseline["serving_digest"]
    assert candidate["ids"] == baseline["ids"]
    assert _single_frame(candidate, "dedupe_summary") == _single_frame(baseline, "dedupe_summary")

    config = _single_frame(candidate, "scanner_config")
    summary = _single_frame(candidate, "scanner_summary")
    assert config["execution_mode"] == "parallel_top_level_bytes_plain_range_reorder"
    assert config["provider_reference_order"] == "after_in_network"
    assert config["plain_range_reorder"] is True
    assert config["plain_provider_range_bytes"] > 0
    assert config["plain_in_network_range_bytes"] > 0
    assert config["plain_in_network_object_count"] == 240
    assert config["order_probe_partial_pass"] is True
    assert sum(worker["rates_seen"] for worker in summary["workers"]) == 240 * 8
    assert sum(worker["rates_seen"] > 0 for worker in summary["workers"]) >= 2


def test_late_gzip_provider_references_are_indexed_and_byte_identical(
    scanner_gzip_late_reference_runs,
):
    baseline = scanner_gzip_late_reference_runs[1]
    for workers in (8, 16):
        candidate = scanner_gzip_late_reference_runs[workers]
        assert candidate["copy_rows"] == baseline["copy_rows"]
        assert candidate["copy_digests"] == baseline["copy_digests"]
        assert candidate["sidecar_digests"] == baseline["sidecar_digests"]
        assert candidate["serving_records"] == baseline["serving_records"]
        assert candidate["serving_digest"] == baseline["serving_digest"]
        assert candidate["ids"] == baseline["ids"]
        assert _single_frame(candidate, "dedupe_summary") == _single_frame(baseline, "dedupe_summary")

        config = _single_frame(candidate, "scanner_config")
        summary = _single_frame(candidate, "scanner_summary")
        assert config["execution_mode"] == "parallel_top_level_bytes_indexed_reorder"
        assert config["decompression"] == "rapidgzip"
        assert config["provider_reference_order"] == "after_in_network"
        assert config["rapidgzip_index_bytes"] > 0
        assert config["indexed_range_producers_requested"] == workers
        assert config["indexed_range_producers_selected"] == workers
        assert config["indexed_range_count"] == workers
        assert config["order_probe_partial_pass"] is True
        assert sum(worker["rates_seen"] for worker in summary["workers"]) == 240 * 8
        assert sum(worker["rates_seen"] > 0 for worker in summary["workers"]) >= 2


def _write_blocking_rapidgzip_wrapper(wrapper: Path) -> None:
    wrapper.write_text(
        """#!/bin/sh
set -eu
printf 'phase=start\n' > "$RAPIDGZIP_TEST_STATE/wrapper-$$" || :
trap 'exit_status=$?; printf "exit=%s\n" "$exit_status" >> "$RAPIDGZIP_TEST_STATE/wrapper-$$" || :' 0
uses_import_index=false
previous_argument=
range_spec=
for argument in "$@"; do
    if [ "$previous_argument" = "--ranges" ]; then range_spec=$argument; fi
    previous_argument=$argument
    if [ "$argument" = "--import-index" ]; then
        uses_import_index=true
    fi
done
if [ "$uses_import_index" != "true" ]; then
    printf 'phase=direct-exec\n' >> "$RAPIDGZIP_TEST_STATE/wrapper-$$" || :
    exec "$REAL_RAPIDGZIP" "$@"
fi
if [ ! -e "$RAPIDGZIP_TEST_STATE/provider-range-complete" ]; then
    printf 'phase=provider\n' >> "$RAPIDGZIP_TEST_STATE/wrapper-$$" || :
    "$REAL_RAPIDGZIP" "$@"
    status=$?
    : > "$RAPIDGZIP_TEST_STATE/provider-range-complete"
    printf 'phase=provider-complete\n' >> "$RAPIDGZIP_TEST_STATE/wrapper-$$" || :
    exit "$status"
fi
if [ "${range_spec##*@}" = "$RAPIDGZIP_TEST_BLOCK_OFFSET" ]; then
    sleep 300 &
    descendant_pid=$!
    printf 'parent=%s\n' "$$" > "$RAPIDGZIP_TEST_STATE/descendant-$descendant_pid" || :
    printf '%s %s\n' "$$" "$descendant_pid" > "$RAPIDGZIP_TEST_STATE/blocked-pids"
    printf 'phase=blocked\n' >> "$RAPIDGZIP_TEST_STATE/wrapper-$$" || :
    wait "$descendant_pid"
fi
printf 'phase=peer-wait\n' >> "$RAPIDGZIP_TEST_STATE/wrapper-$$" || :
while [ ! -s "$RAPIDGZIP_TEST_STATE/blocked-pids" ]; do
    sleep 0.01
done
printf 'phase=peer-fail\n' >> "$RAPIDGZIP_TEST_STATE/wrapper-$$" || :
exit 42
""",
        encoding="utf-8",
    )
    wrapper.chmod(0o755)


@pytest.mark.skipif(os.name != "posix", reason="process-group cleanup is POSIX-specific")
@pytest.mark.parametrize("blocked_first", [False, True])
def test_decoder_roles_follow_range_offsets(tmp_path, blocked_first):
    """One declared range blocks regardless of which producer starts first."""
    state_dir = tmp_path / "state"
    state_dir.mkdir()
    (state_dir / "provider-range-complete").touch()
    wrapper = tmp_path / "wrapper"
    _write_blocking_rapidgzip_wrapper(wrapper)
    environment_by_name = {
        **os.environ,
        "RAPIDGZIP_TEST_STATE": str(state_dir),
        "RAPIDGZIP_TEST_BLOCK_OFFSET": "56",
        "REAL_RAPIDGZIP": "/usr/bin/true",
    }
    processes = []
    try:
        for offset in (56, 3022) if blocked_first else (3022, 56):
            processes.append(
                subprocess.Popen(
                    [str(wrapper), "--import-index", "synthetic", "--ranges", f"100@{offset}"],
                    env=environment_by_name,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    start_new_session=True,
                )
            )
        blocked, peer = processes if blocked_first else processes[::-1]
        assert peer.wait(timeout=5) == 42
        blocked_pids = _recorded_pids(state_dir / "blocked-pids")
        assert blocked_pids[0] == blocked.pid and len(blocked_pids) == 2
        assert blocked.poll() is None
        phases = [path.read_text() for path in state_dir.glob("wrapper-*")]
        assert sum("phase=blocked\n" in phase for phase in phases) == 1
        assert sum("phase=peer-fail\n" in phase for phase in phases) == 1
    finally:
        for child in processes:
            if child.poll() is None:
                try:
                    os.killpg(child.pid, signal.SIGKILL)
                except ProcessLookupError:
                    child.poll()
            child.wait(timeout=5)


def _recorded_pids(path: Path) -> tuple[int, ...]:
    record = path.read_text(encoding="utf-8")
    if not record.endswith("\n"):
        raise ValueError("incomplete process ID record")
    return tuple(int(value) for value in record.split())


def _process_numeric_state(process_id):
    state_by_field = {"pid": process_id, "pgid": None, "state": None, "start_ticks": None}
    try:
        state_by_field["pgid"] = os.getpgid(process_id)
    except OSError:
        return state_by_field
    try:
        with Path(f"/proc/{process_id}/stat").open("rb") as stream:
            content = stream.read(4097)
    except OSError:
        return state_by_field
    if len(content) > 4096:
        raise ValueError("process state exceeds diagnostic limit")
    fields = content.rsplit(b") ", 1)[1].split()
    if fields[0] in {b"R", b"S", b"D", b"Z", b"T", b"t", b"X", b"x", b"K", b"W", b"P", b"I"}:
        state_by_field["state"] = fields[0].decode("ascii")
    state_by_field["start_ticks"] = int(fields[19])
    return state_by_field


def _wrapper_phase_markers(state_dir):
    phases = dict.fromkeys(
        ("start", "direct-exec", "provider", "provider-complete", "blocked", "peer-wait", "peer-fail"), False
    )
    for index, path in enumerate(state_dir.glob("wrapper-*")):
        if index >= 8 or path.is_symlink():
            raise ValueError("wrapper state exceeds diagnostic limit")
        with path.open("rb") as stream:
            content = stream.read(1025)
        if len(content) > 1024:
            raise ValueError("wrapper log exceeds diagnostic limit")
        for phase in phases:
            phases[phase] |= b"phase=" + phase.encode() in content.splitlines()
    return phases


def _blocked_decoder_timeout_diagnostic(state_dir: Path, error) -> str:
    """Retain bounded fingerprints and fixed markers without emitting process contents."""
    stderr = error.stderr or b""
    sample = stderr[:65536]
    pid_path = state_dir / "blocked-pids"
    try:
        with pid_path.open("rb") as stream:
            raw = stream.read(129)
    except FileNotFoundError:
        raw = b""
    try:
        process_ids = tuple(int(encoded_pid) for encoded_pid in raw.split())
    except ValueError:
        process_ids = ()
    has_complete_pair = (
        len(raw) <= 128
        and raw.endswith(b"\n")
        and len(process_ids) == 2
        and len(set(process_ids)) == 2
        and min(process_ids) > 1
    )
    return json.dumps(
        {
            "stderr_bytes": len(stderr),
            "stderr_sample_bytes": len(sample),
            "stderr_sample_sha256": hashlib.sha256(sample).hexdigest(),
            "error_markers": {
                name: name.encode() in sample
                for name in ("indexed range producer", "rapidgzip failed", "exit status: 42")
            },
            "state_markers": {
                name: (state_dir / name).exists() for name in ("provider-range-complete", "blocked-pids")
            },
            "phases": _wrapper_phase_markers(state_dir),
            "complete_pid_pair": has_complete_pair,
            "processes": [_process_numeric_state(process_id) for process_id in process_ids]
            if has_complete_pair
            else [],
        },
        sort_keys=True,
    )


@pytest.mark.parametrize("pid_text", (None, "43210 4", "43210 unfinished\n", "43210 43211\n"))
@pytest.mark.parametrize("capture_error", [None, RuntimeError, KeyboardInterrupt])
def test_timeout_projection_precedes_cleanup(tmp_path, monkeypatch, pid_text, capture_error):
    """Neither capture failures nor partial markers may prevent the original cleanup."""
    state_dir = tmp_path / "state"
    if pid_text is not None:
        state_dir.mkdir()
        (state_dir / "blocked-pids").write_text(pid_text, encoding="ascii")
        (state_dir / "wrapper-pending").write_text("phase=sta", encoding="ascii")
    error = subprocess.TimeoutExpired(
        ["/synthetic/command", "--synthetic"],
        20,
        output=b"config=" + b"x" * 20000 + b" stdout-tail",
        stderr=b"progress=" + b"y" * 20000 + b" stderr-tail",
    )
    events = []
    original_diagnostic = _blocked_decoder_timeout_diagnostic
    capture_exception = capture_error("synthetic capture error") if capture_error else None

    def fail_run(*_args, **_kwargs):
        raise error

    def capture_diagnostic(state_dir, error):
        events.append("diagnostic")
        if capture_exception is not None:
            raise capture_exception
        return original_diagnostic(state_dir, error)

    monkeypatch.setattr(shutil, "which", lambda _name: "rapidgzip")
    monkeypatch.setattr(_SUPPORT, "_built_scanner_binary", lambda: Path("scanner"))
    monkeypatch.setitem(globals(), "_blocked_decoder_scanner_input", lambda *_args: (Path("fixture.gz"), state_dir, {}))
    monkeypatch.setitem(globals(), "_blocked_decoder_timeout_diagnostic", capture_diagnostic)
    monkeypatch.setitem(
        globals(), "_process_numeric_state", lambda pid: {"pid": pid, "pgid": None, "state": None, "start_ticks": None}
    )
    monkeypatch.setattr(subprocess, "run", fail_run)
    monkeypatch.setattr(os, "killpg", lambda pid, _signal: events.append(pid))
    expected_error = KeyboardInterrupt if capture_error is KeyboardInterrupt else pytest.fail.Exception
    with pytest.raises(expected_error) as failure:
        test_indexed_peer_failure_interrupts_blocked_decoder_and_descendants(tmp_path)
    assert events == (["diagnostic", 43210, 43211] if pid_text == "43210 43211\n" else ["diagnostic"])
    assert failure.value.__context__ is None
    if capture_error is KeyboardInterrupt:
        assert failure.value is capture_exception and failure.value.__suppress_context__
        return
    diagnostic = json.loads(str(failure.value).split("\n", 1)[1])
    for forbidden_text in ("stdout-tail", "stderr-tail", "phase=sta", "/synthetic/command", "synthetic capture error"):
        assert forbidden_text not in str(failure.value)
    if capture_error:
        assert diagnostic == {"capture_error": True}
    else:
        assert diagnostic["stderr_bytes"] == diagnostic["stderr_sample_bytes"] == len(error.stderr)
        assert diagnostic["stderr_sample_sha256"] == hashlib.sha256(error.stderr).hexdigest()
        assert diagnostic["state_markers"]["blocked-pids"] is (pid_text is not None)
        assert diagnostic["complete_pid_pair"] is (pid_text == "43210 43211\n")
        assert [process_state["pid"] for process_state in diagnostic["processes"]] == (
            [43210, 43211] if diagnostic["complete_pid_pair"] else []
        )
        assert not any(diagnostic["phases"].values())


@pytest.mark.parametrize("missing_process", [False, True])
def test_blocked_decoder_timeout_diagnostic_captures_only_recorded_processes(
    tmp_path,
    monkeypatch,
    missing_process,
):
    (tmp_path / "blocked-pids").write_text("43210 43211\n", encoding="ascii")
    (tmp_path / "wrapper-43210").write_text("phase=blocked\n/synthetic/not-recorded\n", encoding="ascii")
    (tmp_path / "wrapper-99999").write_text("phase=peer-wait\n", encoding="ascii")
    (tmp_path / "descendant-43211").write_text("parent=43210\n", encoding="ascii")
    original_open = Path.open
    probed_pids = []

    def getpgid(process_id):
        probed_pids.append(process_id)
        assert process_id in (43210, 43211)
        if missing_process and process_id == 43211:
            raise ProcessLookupError("synthetic process already exited")
        return 43210

    def open_stat(path, *args, **kwargs):
        if path in (Path("/proc/43210/stat"), Path("/proc/43211/stat")):
            fields = [b"S", b"1", b"43210", b"43210", *([b"0"] * 15), b"12345"]
            return io.BytesIO(path.parent.name.encode() + b" (synthetic-command-name) " + b" ".join(fields))
        return original_open(path, *args, **kwargs)

    def readlink(_path):
        pytest.fail("diagnostics must not inspect descriptor targets")

    monkeypatch.setattr(os, "getpgid", getpgid)
    monkeypatch.setattr(Path, "open", open_stat)
    monkeypatch.setattr(os, "readlink", readlink)
    error = subprocess.TimeoutExpired(
        ["scanner"],
        20,
        output=b"synthetic-stdout",
        stderr=b"rapidgzip failed\n" + b"x" * 70000 + b"synthetic-stderr-tail",
    )

    diagnostic = json.loads(_blocked_decoder_timeout_diagnostic(tmp_path, error))

    assert probed_pids == [43210, 43211]
    assert [process_state["pid"] for process_state in diagnostic["processes"]] == [43210, 43211]
    assert diagnostic["processes"][0] == {"pid": 43210, "pgid": 43210, "state": "S", "start_ticks": 12345}
    assert diagnostic["processes"][1]["pgid"] == (None if missing_process else 43210)
    assert diagnostic["stderr_sample_bytes"] == 65536 and diagnostic["stderr_bytes"] == len(error.stderr)
    assert diagnostic["stderr_sample_sha256"] == hashlib.sha256(error.stderr[:65536]).hexdigest()
    assert diagnostic["error_markers"]["rapidgzip failed"] is True
    assert diagnostic["phases"]["blocked"] and diagnostic["phases"]["peer-wait"]
    for forbidden_text in (
        "synthetic-stdout",
        "synthetic-stderr-tail",
        "/synthetic/not-recorded",
        "synthetic-command-name",
        "/proc/",
        "fds",
    ):
        assert forbidden_text not in json.dumps(diagnostic)


def _is_process_running(process_id: int) -> bool:
    try:
        os.kill(process_id, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _wait_for_process_cleanup(process_ids: tuple[int, ...]) -> None:
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if not any(_is_process_running(process_id) for process_id in process_ids):
            return
        time.sleep(0.02)
    assert not [process_id for process_id in process_ids if _is_process_running(process_id)]


def _blocked_decoder_scanner_input(tmp_path, real_rapidgzip):
    fixture = _SUPPORT._scanner_fixture_payload(procedure_count=96)
    artifact = tmp_path / "rates.json.gz"
    content_by_field = {
        "reporting_entity_name": fixture["reporting_entity_name"],
        "in_network": fixture["in_network"],
        "provider_references": fixture["provider_references"],
    }
    serialized = json.dumps(content_by_field, separators=(",", ":")).encode("utf-8")
    first_range_offset = serialized.index(b'"in_network":[') + len(b'"in_network":[')
    _SUPPORT._write_gzip_json(artifact, content_by_field)
    state_dir = tmp_path / "rapidgzip-state"
    state_dir.mkdir()
    wrapper = tmp_path / "rapidgzip-wrapper"
    _write_blocking_rapidgzip_wrapper(wrapper)
    run_dir = tmp_path / "scanner-run"
    run_dir.mkdir()
    scanner_environment = _SUPPORT._scanner_environment(8, None, run_dir / "serving-runs")
    scanner_environment.update(
        {
            _SUPPORT.COPY_ENV_BY_KIND["price_atom"]: str(run_dir / "price-atom.copy"),
            "HLTHPRT_PTG2_RUST_RAPIDGZIP_ENABLED": "true",
            "HLTHPRT_PTG2_RUST_RAPIDGZIP_BIN": str(wrapper),
            "HLTHPRT_PTG2_RUST_RAPIDGZIP_THREADS": "2",
            "HLTHPRT_PTG2_RUST_INDEXED_RANGE_PRODUCERS": "2",
            "REAL_RAPIDGZIP": real_rapidgzip,
            "RAPIDGZIP_TEST_STATE": str(state_dir),
            "RAPIDGZIP_TEST_BLOCK_OFFSET": str(first_range_offset),
        }
    )
    return artifact, state_dir, scanner_environment


@pytest.mark.skipif(os.name != "posix", reason="process-group cleanup is POSIX-specific")
def test_indexed_peer_failure_interrupts_blocked_decoder_and_descendants(tmp_path):
    """A failed range must cancel a blocked peer and its descendant without hanging."""
    real_rapidgzip = shutil.which("rapidgzip")
    if real_rapidgzip is None:
        pytest.skip("rapidgzip is not installed")

    artifact, state_dir, scanner_environment = _blocked_decoder_scanner_input(
        tmp_path,
        real_rapidgzip,
    )
    scanner_binary = _SUPPORT._built_scanner_binary()
    started_at = time.monotonic()
    timeout_diagnostic = capture_interruption = None
    try:
        completed = subprocess.run(
            [str(scanner_binary), "--compact-serving", str(artifact)],
            check=False,
            env=scanner_environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=20,
        )
    except subprocess.TimeoutExpired as error:
        try:
            timeout_diagnostic = _blocked_decoder_timeout_diagnostic(state_dir, error)
        except Exception:
            timeout_diagnostic = json.dumps({"capture_error": True})
        except BaseException as interruption:
            capture_interruption = interruption
        finally:
            pid_path = state_dir / "blocked-pids"
            try:
                process_ids = _recorded_pids(pid_path)
            except OSError, ValueError:
                process_ids = ()
            for process_id in process_ids:
                try:
                    os.killpg(process_id, signal.SIGKILL)
                except OSError:
                    continue
    if capture_interruption is not None:
        capture_interruption.__context__ = None
        raise capture_interruption from None
    if timeout_diagnostic is not None:
        pytest.fail(
            f"scanner hung after indexed rapidgzip peer failure\n{timeout_diagnostic}",
            pytrace=False,
        )

    assert time.monotonic() - started_at < 10
    assert completed.returncode != 0
    stderr = completed.stderr.decode("utf-8", errors="replace")
    assert "indexed range producer" in stderr
    assert "rapidgzip failed" in stderr
    assert "exit status: 42" in stderr
    blocked_pids = _recorded_pids(state_dir / "blocked-pids")
    assert len(blocked_pids) == 2
    _wait_for_process_cleanup(blocked_pids)
