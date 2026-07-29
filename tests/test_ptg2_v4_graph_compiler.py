import asyncio
from copy import deepcopy
import io
import json
from pathlib import Path

import pytest

from process.ptg_parts import ptg2_v4_graph_compiler as compiler
from process.ptg_parts.ptg2_v4_graph_compiler import (
    V4GraphResourceAdmissionError,
    compile_provider_graph_v4_rust,
)
from tests.ptg2_v4_graph_compiler_test_support import (
    compiler_fixture as _fixture,
    scanner_binary as _binary,
)


def _progress_event(
    seq: int,
    phase: str,
    done: int,
    total: int,
    *,
    unit: str = "stage",
    elapsed_ms: int = 0,
    terminal: bool = False,
) -> bytes:
    return compiler.PTG2_V4_PROGRESS_PREFIX + json.dumps(
        {
            "version": compiler.PTG2_V4_PROGRESS_VERSION,
            "seq": seq,
            "phase": phase,
            "done": done,
            "total": total,
            "unit": unit,
            "elapsed_ms": elapsed_ms,
            "terminal": terminal,
        },
        separators=(",", ":"),
    ).encode("ascii") + b"\n"


class _HeartbeatProcess:
    """Hold a fake compiler open until its wrapper publishes one heartbeat."""

    def __init__(self, *, child_progress: bool, factor_edge_count: int) -> None:
        self.pid = 99_999_998
        self.returncode = None
        self.stderr = asyncio.StreamReader()
        if child_progress:
            self.stderr.feed_data(
                _progress_event(
                    1,
                    "derive_patterns",
                    4,
                    factor_edge_count,
                    unit="groups",
                )
            )
        self.finished = asyncio.Event()
        self.observed_events: list[dict[str, object]] = []

    async def wait(self):
        await self.finished.wait()
        return self.returncode

    async def create_subprocess(self, *_args, **_kwargs):
        return self

    async def capture_progress(self, **progress_by_field):
        self.observed_events.append(progress_by_field)
        if str(progress_by_field.get("message", "")).endswith("; active"):
            self.returncode = 1
            self.stderr.feed_eof()
            self.finished.set()

    def active_heartbeat(self) -> dict[str, object]:
        return next(
            event
            for event in self.observed_events
            if str(event.get("message", "")).endswith("; active")
        )


def _assert_summary_rejected(
    summary_by_field: dict,
    validation_arguments_by_name: dict,
    expected_message: str,
) -> None:
    """Assert that one mutated compiler summary fails authentication."""

    with pytest.raises(RuntimeError, match=expected_message):
        compiler._validate_compiler_summary(
            summary_by_field,
            **validation_arguments_by_name,
        )


def _tampered_summary_cases(
    summary_by_field: dict,
    expected_options: dict,
) -> tuple[tuple[dict, str], ...]:
    """Build representative sealed-option and diagnostic drift cases."""

    changed_limit = deepcopy(summary_by_field)
    changed_limit["max_set_components_per_fallback_set"] += 1
    changed_provider_page = deepcopy(summary_by_field)
    changed_provider_page["provider_expansion_rate_page_rows"] += 1
    inconsistent = deepcopy(summary_by_field)
    inconsistent["observe"]["pattern_overflow_set_count"] = 1
    unsafe_second_hop = deepcopy(summary_by_field)
    unsafe_second_hop["observe"][
        "maximum_online_group_npi_batch_work"
    ] = (
        expected_options["max_online_group_npi_batches_per_set"] + 1
    )
    changed_decision = deepcopy(summary_by_field)
    changed_decision["pattern_layout_serving_degree_eligible"] = not bool(
        changed_decision["pattern_layout_serving_degree_eligible"]
    )
    return (
        (changed_limit, "fallback-degree limit changed"),
        (changed_provider_page, "option .* changed"),
        (inconsistent, "diagnostics are inconsistent"),
        (unsafe_second_hop, "diagnostics are inconsistent"),
        (changed_decision, "serving-degree decision changed"),
    )


@pytest.mark.asyncio
async def test_wrapper_discards_mismatched_checkpoint_and_rebuilds(tmp_path: Path) -> None:
    artifacts, provider_map = _fixture(tmp_path)
    output = tmp_path / "compiled"
    await compile_provider_graph_v4_rust(
        graph_artifact_entries=artifacts,
        provider_set_key_map_path=provider_map,
        output_directory=output,
        binary_path=_binary(),
    )
    (output / "v4-complete.json").write_text("{}\n")

    rebuilt = await compile_provider_graph_v4_rust(
        graph_artifact_entries=artifacts,
        provider_set_key_map_path=provider_map,
        output_directory=output,
        binary_path=_binary(),
    )
    assert rebuilt.checkpoint_reused is False


@pytest.mark.asyncio
async def test_wrapper_rejects_component_fallback_summary_tampering(
    tmp_path: Path,
) -> None:
    """Reject sealed limits, second-hop work, and layout decision drift."""

    artifacts, provider_map = _fixture(tmp_path)
    output = tmp_path / "compiled"
    compilation = await compile_provider_graph_v4_rust(
        graph_artifact_entries=artifacts,
        provider_set_key_map_path=provider_map,
        output_directory=output,
        binary_path=_binary(),
    )
    summary_by_field = deepcopy(compilation.summary)
    expected_options = compiler._effective_compiler_options(None)
    validation_arguments_by_name = {
        "output_directory": output,
        "expected_input_bytes": int(summary_by_field["input_byte_count"]),
        "expected_factor_edges": int(
            summary_by_field["resource_admission"]["factor_edge_count"]
        ),
        "expected_factor_owners": int(
            summary_by_field["resource_admission"]["factor_owner_count"]
        ),
        "expected_options": expected_options,
        "allow_checkpoint": True,
    }

    for tampered_summary, expected_message in _tampered_summary_cases(
        summary_by_field,
        expected_options,
    ):
        _assert_summary_rejected(
            tampered_summary,
            validation_arguments_by_name,
            expected_message,
        )


@pytest.mark.asyncio
async def test_wrapper_surfaces_typed_resource_admission_failure(tmp_path: Path) -> None:
    artifacts, provider_map = _fixture(tmp_path)
    output = tmp_path / "compiled"

    with pytest.raises(V4GraphResourceAdmissionError, match="factor edge count"):
        await compile_provider_graph_v4_rust(
            graph_artifact_entries=artifacts,
            provider_set_key_map_path=provider_map,
            output_directory=output,
            binary_path=_binary(),
            options={"max_factor_edges": 1},
        )
    assert not output.exists()

    with pytest.raises(RuntimeError, match="unknown option"):
        await compile_provider_graph_v4_rust(
            graph_artifact_entries=artifacts,
            provider_set_key_map_path=provider_map,
            output_directory=output,
            binary_path=_binary(),
            options={"max_npi_prefix_override_owner": 250_001},
        )


@pytest.mark.asyncio
async def test_progress_consumer_accepts_monotonic_events_and_logs_malformed_lines(
    monkeypatch,
) -> None:
    stream = asyncio.StreamReader()
    stream.feed_data(b"compiler diagnostic\n")
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
    assert b"broken-json" in logged
    assert b'"seq":99' in logged


@pytest.mark.asyncio
async def test_progress_heartbeat_preserves_last_real_child_counters(monkeypatch) -> None:
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
    )
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
            output_directory=output,
            binary_path=binary,
        )

    heartbeat = fake_process.active_heartbeat()
    assert heartbeat["done"] == (4 if child_progress else 0)
    assert heartbeat["total"] == factor_edge_count
    assert heartbeat["unit"] == ("groups" if child_progress else "factor_edges")
    assert not output.exists()


@pytest.mark.asyncio
async def test_wrapper_cancellation_terminates_child_and_drains_progress(
    tmp_path: Path,
    monkeypatch,
) -> None:
    artifacts, provider_map = _fixture(tmp_path)
    output = tmp_path / "compiled"
    binary = tmp_path / "fake-compiler"
    binary.write_text("#!/bin/sh\nexit 0\n", encoding="ascii")
    binary.chmod(0o755)
    created = asyncio.Event()
    terminated = asyncio.Event()

    class FakeProcess:
        def __init__(self) -> None:
            self.pid = 99_999_999
            self.returncode = None
            self.stderr = asyncio.StreamReader()
            self.stderr.feed_data(
                _progress_event(1, "resource_admission", 0, 1)
            )
            self.finished = asyncio.Event()

        async def wait(self):
            await self.finished.wait()
            return self.returncode

    fake_process = FakeProcess()

    async def fake_create_subprocess_exec(*_args, **_kwargs):
        created.set()
        return fake_process

    async def fake_terminate(process):
        assert process is fake_process
        process.returncode = -15
        process.stderr.feed_eof()
        process.finished.set()
        terminated.set()

    monkeypatch.setattr(
        compiler.asyncio, "create_subprocess_exec", fake_create_subprocess_exec
    )
    monkeypatch.setattr(compiler, "_terminate_process", fake_terminate)
    task = asyncio.create_task(
        compile_provider_graph_v4_rust(
            graph_artifact_entries=artifacts,
            provider_set_key_map_path=provider_map,
            output_directory=output,
            binary_path=binary,
        )
    )
    await created.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert terminated.is_set()
    assert not output.exists()
