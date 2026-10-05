"""Lifecycle and progress contracts for the native V4 graph compiler."""

import asyncio
import io
from pathlib import Path
import struct
from unittest.mock import AsyncMock, Mock

import pytest

from process.ptg_parts import ptg2_v4_graph_compiler as compiler
from process.ptg_parts.ptg2_v4_graph_compiler import (
    compile_provider_graph_v4_rust,
)
from tests.test_ptg2_v4_graph_compiler import (
    _HeartbeatProcess,
    _PendingCompilerProcess,
    _PendingNpiScopeProcess,
    _binary,
    _compiler_inputs,
    _fixture,
    _progress_event,
)


def test_scope_prepass_batches_owner_rows_and_preserves_validation() -> None:
    owner_count = 4_097
    owners = b"".join(
        bytes(8)
        + (1_000_000_000 + index).to_bytes(8, "big")
        + index.to_bytes(8, "little")
        + (1).to_bytes(4, "little")
        for index in range(owner_count)
    )
    scope = io.BytesIO()
    compiler._write_npi_scope_rows(
        io.BytesIO(owners),
        scope,
        owner_count=owner_count,
        member_count=owner_count,
    )
    expected = compiler._PG_COPY_HEADER + b"".join(
        struct.pack(">hIq", 1, 8, 1_000_000_000 + index)
        for index in range(owner_count)
    ) + struct.pack(">h", -1)
    assert scope.getvalue() == expected

    with pytest.raises(RuntimeError, match="index is truncated"):
        compiler._write_npi_scope_rows(
            io.BytesIO(owners[:-1]),
            io.BytesIO(),
            owner_count=owner_count,
            member_count=owner_count,
        )
    invalid = bytearray(owners[:28])
    invalid[-4:] = bytes(4)
    with pytest.raises(RuntimeError, match="index changed"):
        compiler._write_npi_scope_rows(
            io.BytesIO(invalid),
            io.BytesIO(),
            owner_count=1,
            member_count=1,
        )


@pytest.mark.asyncio
async def test_scope_prepass_refuses_reciprocal_replacement_at_open(
    tmp_path: Path,
    monkeypatch,
) -> None:
    artifacts, _provider_map = _fixture(tmp_path)
    reciprocal_path = next(
        Path(str(artifact["path"]))
        for artifact in artifacts
        if artifact["name"] == "provider_npi_group"
    )
    replacement_path = tmp_path / "replacement-reciprocal.bin"
    replacement_path.write_bytes(reciprocal_path.read_bytes())
    original_open = compiler._open_reciprocal_descriptor
    pending_replacement_paths = {reciprocal_path}

    def replace_before_open(path):
        opened_path = Path(path)
        if opened_path in pending_replacement_paths:
            replacement_path.replace(reciprocal_path)
            pending_replacement_paths.remove(opened_path)
        return original_open(path)

    monkeypatch.setattr(
        compiler,
        "_open_reciprocal_descriptor",
        replace_before_open,
    )
    with pytest.raises(RuntimeError, match="changed before extraction"):
        await compiler.prepare_provider_graph_v4_npi_scope_rust(
            graph_artifact_entries=artifacts,
            output_path=tmp_path / "replaced-scope.copy",
            binary_path=_binary(),
        )


@pytest.mark.asyncio
async def test_scope_prepass_emits_authenticated_start_and_completion(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Fast prepasses expose both boundaries without waiting for a heartbeat."""

    artifacts, _provider_map = _fixture(tmp_path)
    progress_events: list[dict[str, object]] = []

    async def capture_progress(**progress_by_field):
        progress_events.append(progress_by_field)

    monkeypatch.setattr(compiler, "_emit_compile_progress", capture_progress)
    preparation = await compiler.prepare_provider_graph_v4_npi_scope_rust(
        graph_artifact_entries=artifacts,
        output_path=tmp_path / "scope-progress.copy",
        binary_path=_binary(),
    )

    assert [event["stage_pct"] for event in progress_events] == [0.0, 100.0]
    assert {event["stage_id"] for event in progress_events} == {
        "ptg2_v4_provider_graph_npi_scope"
    }
    assert progress_events[-1]["done"] == preparation.manifest["row_count"]
    assert progress_events[-1]["total"] == preparation.manifest["row_count"]
    preparation.cleanup()


@pytest.mark.asyncio
async def test_scope_prepass_failure_never_emits_completion(
    tmp_path: Path,
    monkeypatch,
) -> None:
    artifacts, _provider_map = _fixture(tmp_path)
    progress_events: list[dict[str, object]] = []
    binary = tmp_path / "failed-npi-scope"
    binary.write_text("#!/bin/sh\nexit 1\n", encoding="ascii")
    binary.chmod(0o755)

    async def capture_progress(**progress_by_field):
        progress_events.append(progress_by_field)

    monkeypatch.setattr(compiler, "_emit_compile_progress", capture_progress)
    with pytest.raises(RuntimeError, match="NPI scope preparation failed"):
        await compiler.prepare_provider_graph_v4_npi_scope_rust(
            graph_artifact_entries=artifacts,
            output_path=tmp_path / "failed-scope.copy",
            binary_path=binary,
        )

    assert progress_events[0]["stage_pct"] == 0.0
    assert all(event["stage_pct"] != 100.0 for event in progress_events)


@pytest.mark.asyncio
async def test_scope_prepass_cancellation_never_emits_completion(
    tmp_path: Path,
    monkeypatch,
) -> None:
    artifacts, _provider_map = _fixture(tmp_path)
    progress_events: list[dict[str, object]] = []
    process = _PendingNpiScopeProcess()
    binary = tmp_path / "pending-npi-scope"
    binary.write_text("#!/bin/sh\nexit 0\n", encoding="ascii")
    binary.chmod(0o755)

    async def capture_progress(**progress_by_field):
        progress_events.append(progress_by_field)

    async def terminate_process(pending_process):
        pending_process.returncode = -15
        pending_process.finished.set()

    monkeypatch.setattr(
        compiler.asyncio,
        "create_subprocess_exec",
        process.create_subprocess,
    )
    monkeypatch.setattr(compiler, "_emit_compile_progress", capture_progress)
    monkeypatch.setattr(compiler, "_terminate_process", terminate_process)
    task = asyncio.create_task(
        compiler.prepare_provider_graph_v4_npi_scope_rust(
            graph_artifact_entries=artifacts,
            output_path=tmp_path / "canceled-scope.copy",
            binary_path=binary,
        )
    )
    while not progress_events:
        await asyncio.sleep(0)
    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task
    assert all(event["stage_pct"] != 100.0 for event in progress_events)


@pytest.mark.asyncio
async def test_progress_consumer_accepts_monotonic_events_and_logs_malformed_lines(
    monkeypatch,
) -> None:
    stream = asyncio.StreamReader()
    stream.feed_data(b"compiler diagnostic\n")
    oversized_diagnostic = b"x" * compiler.PTG2_V4_PROGRESS_MAX_LINE_BYTES + b"\n"
    stream.feed_data(oversized_diagnostic)
    stream.feed_data(_progress_event(1, "resource_admission", 0, 1))
    stream.feed_data(compiler.PTG2_V4_PROGRESS_PREFIX + b"{broken-json}\n")
    stream.feed_data(_progress_event(99, "resource_admission", 1, 1))
    stream.feed_data(_progress_event(2, "resource_admission", 1, 1, elapsed_ms=5))
    stream.feed_data(
        _progress_event(
            3,
            "complete",
            1,
            1,
            unit="compile",
            elapsed_ms=10,
            terminal=True,
        )
    )
    stream.feed_eof()
    observed_events: list[dict[str, object]] = []

    async def fake_emit(**progress_by_field):
        observed_events.append(progress_by_field)

    monkeypatch.setattr(compiler, "_emit_compile_progress", fake_emit)
    state = compiler._CompilerProgressState()
    diagnostics = io.BytesIO()
    await compiler._consume_compiler_stderr(
        stream,
        diagnostics,
        state=state,
        emit_lock=asyncio.Lock(),
        input_bytes=123,
        input_factor_edges=456,
        input_factor_owners=78,
    )

    assert [
        progress_by_field["compiler_progress_seq"]
        for progress_by_field in observed_events
    ] == [1, 2, 3]
    assert observed_events[-1]["pct"] == 95.0
    assert observed_events[-1]["compiler_terminal"] is True
    assert state.terminal is True
    logged = diagnostics.getvalue()
    assert b"compiler diagnostic" in logged
    assert oversized_diagnostic in logged
    assert b"broken-json" in logged
    assert b'"seq":99' in logged


@pytest.mark.asyncio
async def test_progress_heartbeat_preserves_last_real_child_counters(
    monkeypatch,
) -> None:
    state = compiler._CompilerProgressState()
    assert state.is_accepted(
        {
            "version": 1,
            "seq": 1,
            "phase": "derive_patterns",
            "done": 50,
            "total": 100,
            "unit": "groups",
            "elapsed_ms": 1_000,
            "terminal": False,
        }
    )
    observed_events: list[dict[str, object]] = []

    async def fake_emit(**progress_by_field):
        observed_events.append(progress_by_field)

    monkeypatch.setattr(compiler, "_emit_compile_progress", fake_emit)
    heartbeat = compiler.replace(state, elapsed_ms=5_000)
    await compiler._publish_compiler_progress_state(
        heartbeat,
        emit_lock=asyncio.Lock(),
        input_bytes=123,
        input_factor_edges=456,
        input_factor_owners=78,
        checkpoint_reused=False,
        heartbeat=True,
    )

    assert len(observed_events) == 1
    assert observed_events[0]["done"] == 50
    assert observed_events[0]["total"] == 100
    assert observed_events[0]["unit"] == "groups"
    assert 92.0 < observed_events[0]["pct"] < 95.0
    assert observed_events[0]["compiler_progress_seq"] == 1
    assert observed_events[0]["message"].endswith("; active")


@pytest.mark.asyncio
@pytest.mark.parametrize("child_progress", [False, True], ids=["fallback", "child"])
async def test_wrapper_heartbeat_reports_current_compiler_progress(
    tmp_path: Path,
    monkeypatch,
    child_progress: bool,
) -> None:
    """Heartbeat counters remain meaningful before and after child progress."""

    artifacts, provider_map = _fixture(tmp_path)
    factor_edge_count = sum(
        int(artifact.get("member_count") or artifact.get("row_count") or 0)
        for artifact in artifacts
        if artifact.get("name") != "provider_npi_scope"
    )
    npi_scope, inferred_taxonomy = await _compiler_inputs(tmp_path, artifacts)
    output = tmp_path / "compiled"
    binary = tmp_path / "fake-compiler"
    binary.write_text("#!/bin/sh\nexit 1\n", encoding="ascii")
    binary.chmod(0o755)
    fake_process = _HeartbeatProcess(
        child_progress=child_progress,
        factor_edge_count=factor_edge_count,
    )

    monkeypatch.setattr(
        compiler.asyncio, "create_subprocess_exec", fake_process.create_subprocess
    )
    monkeypatch.setattr(
        compiler,
        "_emit_compile_progress",
        fake_process.capture_progress,
    )
    monkeypatch.setattr(compiler, "PTG2_V4_GRAPH_HEARTBEAT_SECONDS", 0.05)

    with pytest.raises(RuntimeError, match="exited with status 1"):
        await compile_provider_graph_v4_rust(
            graph_artifact_entries=artifacts,
            provider_set_key_map_path=provider_map,
            npi_scope=npi_scope,
            inferred_taxonomy=inferred_taxonomy,
            output_directory=output,
            binary_path=binary,
        )

    heartbeat = fake_process.active_heartbeat()
    assert heartbeat["done"] == (4 if child_progress else 0)
    assert heartbeat["total"] == factor_edge_count
    assert heartbeat["unit"] == ("groups" if child_progress else "factor_edges")
    assert not output.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("stderr_stalls", [False, True])
async def test_wrapper_cancellation_terminates_child_and_drains_progress(
    tmp_path: Path,
    monkeypatch,
    stderr_stalls: bool,
) -> None:
    """Cancel the native child, drain progress, and remove partial outputs."""

    artifacts, provider_map = _fixture(tmp_path)
    npi_scope, inferred_taxonomy = await _compiler_inputs(tmp_path, artifacts)
    output = tmp_path / "compiled"
    binary = tmp_path / "fake-compiler"
    binary.write_text("#!/bin/sh\nexit 0\n", encoding="ascii")
    binary.chmod(0o755)
    fake_process = _PendingCompilerProcess()
    if stderr_stalls:
        monkeypatch.setattr(fake_process.stderr, "feed_eof", lambda: None)
    stderr_finished = asyncio.Event()
    consume = compiler._consume_compiler_stderr

    async def consume_until_finished(*args, **kwargs):
        try:
            await consume(*args, **kwargs)
        finally:
            stderr_finished.set()

    monkeypatch.setattr(compiler, "_consume_compiler_stderr", consume_until_finished)
    monkeypatch.setattr(
        compiler.asyncio,
        "create_subprocess_exec",
        fake_process.create_subprocess,
    )
    monkeypatch.setattr(
        compiler,
        "_terminate_process",
        fake_process.terminate,
    )
    task = asyncio.create_task(
        compile_provider_graph_v4_rust(
            graph_artifact_entries=artifacts,
            provider_set_key_map_path=provider_map,
            npi_scope=npi_scope,
            inferred_taxonomy=inferred_taxonomy,
            output_directory=output,
            binary_path=binary,
        )
    )
    await fake_process.created.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert fake_process.terminated.is_set()
    assert stderr_finished.is_set()
    assert not output.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["stderr", "terminal", "wait"])
async def test_wrapper_failed_child_contract_cleans_owned_output(tmp_path, monkeypatch, failure):
    """A child protocol or wait failure cannot leave outputs eligible for reuse."""
    artifacts, provider_map = _fixture(tmp_path)
    npi_scope, inferred_taxonomy = await _compiler_inputs(tmp_path, artifacts)
    output = tmp_path / "compiled"
    binary = tmp_path / "fake-compiler"
    binary.write_text("#!/bin/sh\nexit 0\n", encoding="ascii")
    binary.chmod(0o755)
    process = _PendingCompilerProcess()
    if failure == "stderr":
        process.stderr = None
    elif failure == "terminal":
        process.returncode = 0
        process.stderr.feed_eof()
        process.finished.set()
    else:
        process.wait = AsyncMock(side_effect=OSError("synthetic child wait failure"))
    terminated = AsyncMock()

    async def terminate(child):
        assert child is process
        process.returncode = -15
        await terminated()

    monkeypatch.setattr(compiler.asyncio, "create_subprocess_exec", process.create_subprocess)
    monkeypatch.setattr(compiler, "_terminate_process", terminate)
    expected_message = {
        "stderr": "did not expose its progress stream",
        "terminal": "without a terminal progress event",
        "wait": "synthetic child wait failure",
    }[failure]
    with pytest.raises((RuntimeError, OSError), match=expected_message):
        await compile_provider_graph_v4_rust(
            graph_artifact_entries=artifacts, provider_set_key_map_path=provider_map,
            npi_scope=npi_scope, inferred_taxonomy=inferred_taxonomy,
            output_directory=output, binary_path=binary,
        )
    assert terminated.await_count == int(failure != "terminal")
    assert not output.exists()
    assert not list(tmp_path.glob("compiled.*"))


@pytest.mark.parametrize("failure", ["absent", "removed", "replaced"])
def test_reciprocal_descriptor_authentication_rejects_path_changes(tmp_path, failure):
    """An opened reciprocal stays authenticated through the final read."""
    artifacts, _provider_map = _fixture(tmp_path)
    reciprocal = next(artifact for artifact in artifacts if artifact["name"] == "provider_npi_group")
    path = Path(str(reciprocal["path"]))
    replacement = tmp_path / "replacement.bin"
    replacement.write_bytes(path.read_bytes())
    if failure == "absent":
        path.unlink()
    with pytest.raises(RuntimeError, match="unavailable|changed during extraction"):
        with compiler._open_authenticated_reciprocal({"path": path, "metadata": reciprocal}) as stream:
            assert stream.read(8) == b"PTG2MNDS"
            path.unlink()
            if failure == "replaced":
                replacement.rename(path)
    if failure == "replaced":
        assert path.read_bytes().startswith(b"PTG2MNDS")


def test_scratch_owner_fsync_failure_removes_only_new_output(tmp_path, monkeypatch):
    output = tmp_path / "compiled"
    sentinel = tmp_path / "unrelated"
    sentinel.write_bytes(b"preserve")
    monkeypatch.setattr(compiler.os, "fsync", Mock(side_effect=OSError("synthetic fsync failure")))
    with pytest.raises(OSError, match="synthetic fsync failure"):
        compiler._create_compiler_output(output)
    assert not output.exists()
    assert sentinel.read_bytes() == b"preserve"


@pytest.mark.asyncio
async def test_progress_sink_failure_does_not_fail_compilation(monkeypatch):
    sink = Mock(side_effect=OSError("synthetic progress failure"))
    monkeypatch.setattr(compiler, "write_live_progress", sink)
    await compiler._emit_compile_progress(done=1, total=2)
    sink.assert_called_once()


@pytest.mark.parametrize("failure", ["format", "existing", "header", "members"])
def test_scope_extraction_preserves_existing_output_and_rejects_metadata_drift(tmp_path, failure):
    artifacts, _provider_map = _fixture(tmp_path)
    reciprocal = next(artifact for artifact in artifacts if artifact["name"] == "provider_npi_group")
    output = tmp_path / "scope.copy"
    if failure == "format":
        reciprocal["record_format"] = "unknown"
    elif failure == "existing":
        output.write_bytes(b"preserve")
    elif failure == "header":
        reciprocal["owner_count"] += 1
    else:
        reciprocal["member_count"] += 1
    with pytest.raises(RuntimeError, match="dense format|already exists|header changed|member count changed"):
        compiler._write_npi_scope_from_reciprocal(
            output, reciprocal={"path": reciprocal["path"], "metadata": reciprocal}
        )
    assert not output.with_suffix(".copy.partial").exists()
    if failure == "existing":
        assert output.read_bytes() == b"preserve"
    else:
        assert not output.exists()


def test_scope_link_cleanup_failure_removes_both_owned_files(tmp_path, monkeypatch):
    """Failure after publishing a hard link rolls back the whole owned output."""
    artifacts, _provider_map = _fixture(tmp_path)
    reciprocal = next(artifact for artifact in artifacts if artifact["name"] == "provider_npi_group")
    output = tmp_path / "scope.copy"
    partial = output.with_suffix(".copy.partial")
    unlink = Path.unlink
    failed_paths = set()

    def fail_first_partial_unlink(path, *args, **kwargs):
        if path == partial and path not in failed_paths:
            failed_paths.add(path)
            raise OSError("synthetic partial cleanup failure")
        return unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", fail_first_partial_unlink)
    with pytest.raises(OSError, match="synthetic partial cleanup failure"):
        compiler._write_npi_scope_from_reciprocal(
            output, reciprocal={"path": reciprocal["path"], "metadata": reciprocal}
        )
    assert failed_paths == {partial}
    assert not partial.exists()
    assert not output.exists()
    assert Path(str(reciprocal["path"])).is_file()


def test_binary_copy_iterator_preserves_nullable_and_empty_fields(tmp_path):
    path = tmp_path / "canonical.copy"
    record = struct.pack(">hi", 3, 8) + struct.pack(">qii", 7, -1, 0)
    path.write_bytes(compiler._PG_COPY_HEADER + record + struct.pack(">h", -1))
    assert list(compiler._iter_pg_binary_rows(
        path, expected_field_count=3, nullable_field_indices=frozenset({1})
    )) == [(struct.pack(">q", 7), None, b"")]
    with pytest.raises(RuntimeError, match="invalid NULL COPY field"):
        list(compiler._iter_pg_binary_rows(path, expected_field_count=3))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure,message",
    [
        ("binary", "requires ptg2_provider_graph_v4"),
        ("parent", "parent is not private"),
        ("owner", "scratch ownership changed"),
        ("file", "not a safe directory"),
    ],
)
async def test_compiler_rejects_unsafe_launch_paths_without_touching_files(
    tmp_path, monkeypatch, failure, message
):
    """Trust checks precede subprocess creation and any scratch replacement."""
    binary = tmp_path / "compiler"
    binary.write_text("#!/bin/sh\nexit 0\n", encoding="ascii")
    binary.chmod(0o644 if failure == "binary" else 0o755)
    output = tmp_path / "compiled"
    if failure == "parent":
        parent = tmp_path / "shared"
        parent.mkdir(mode=0o755)
        parent.chmod(0o755)
        output = parent / "compiled"
    elif failure == "owner":
        compiler._create_compiler_output(output)
        compiler._compiler_output_owner_path(output).write_bytes(b"changed")
    elif failure == "file":
        output.write_bytes(b"preserve")
    contents_by_path = {path: path.read_bytes() for path in tmp_path.rglob("*") if path.is_file()}
    start = AsyncMock()
    monkeypatch.setattr(compiler.asyncio, "create_subprocess_exec", start)
    with pytest.raises(RuntimeError, match=message):
        await compile_provider_graph_v4_rust(
            graph_artifact_entries=(), provider_set_key_map_path=tmp_path / "map",
            npi_scope=None, inferred_taxonomy={}, output_directory=output, binary_path=binary,
        )
    start.assert_not_awaited()
    assert {path: path.read_bytes() for path in tmp_path.rglob("*") if path.is_file()} == contents_by_path


def test_artifact_manifest_requires_explicit_record_format(tmp_path):
    artifacts, _provider_map = _fixture(tmp_path)
    artifact = artifacts[0]
    manifest, _byte_count = compiler._artifact_manifest(artifact)
    assert manifest["metadata"]["record_format"] == artifact["record_format"]
    with pytest.raises(RuntimeError, match="invalid record_format"):
        compiler._artifact_manifest({**artifact, "record_format": ""})
