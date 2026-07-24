import asyncio
from copy import deepcopy
import hashlib
import io
import json
import struct
from pathlib import Path

import pytest

from process.ptg_parts import ptg2_v4_graph_compiler as compiler
from process.ptg_parts.ptg2_v4_graph_compiler import (
    V4GraphResourceAdmissionError,
    compile_provider_graph_v4_rust,
)


_STANDARD_FORMAT = (
    "magic8:uint32_le_version:uint64_le_entry_count:"
    "index(owner16:uint64_le_offset:uint32_le_count):members16"
)


def _global(domain: int, value: int) -> bytes:
    return bytes([domain]) + bytes(7) + value.to_bytes(8, "big")


def _npi(value: int) -> bytes:
    return bytes(8) + value.to_bytes(8, "big")


def _write_membership(
    path: Path, *, name: str, shard_id: str, pairs: list[tuple[bytes, bytes]]
) -> dict[str, object]:
    by_owner: dict[bytes, set[bytes]] = {}
    for owner, member in pairs:
        by_owner.setdefault(owner, set()).add(member)
    memberships = [
        (owner, sorted(members)) for owner, members in sorted(by_owner.items())
    ]
    membership_payload = bytearray(b"PTG2MNSC")
    membership_payload.extend(struct.pack("<IQ", 1, len(memberships)))
    offset = 0
    for owner, members in memberships:
        membership_payload.extend(owner)
        membership_payload.extend(struct.pack("<QI", offset, len(members)))
        offset += len(members)
    for _, members in memberships:
        for member in members:
            membership_payload.extend(member)
    path.write_bytes(membership_payload)
    return {
        "name": name,
        "source_shard_id": shard_id,
        "path": str(path),
        "record_format": _STANDARD_FORMAT,
        "sha256": hashlib.sha256(membership_payload).hexdigest(),
        "byte_count": len(membership_payload),
        "owner_count": len(memberships),
        "member_count": offset,
    }


def _fixture(tmp_path: Path) -> tuple[list[dict[str, object]], Path]:
    shard_id = "shard-a"
    provider_set = _global(1, 1)
    component = _global(2, 1)
    groups = [_global(3, 1), _global(3, 2)]
    provider_npi = _npi(1_234_567_890)
    artifacts = [
        _write_membership(
            tmp_path / "set-component.sidecar",
            name="provider_set_component",
            shard_id=shard_id,
            pairs=[(provider_set, component)],
        ),
        _write_membership(
            tmp_path / "component-group.sidecar",
            name="provider_component_group",
            shard_id=shard_id,
            pairs=[(component, group) for group in groups],
        ),
        _write_membership(
            tmp_path / "group-npi.sidecar",
            name="provider_group_npi",
            shard_id=shard_id,
            pairs=[(group, provider_npi) for group in groups],
        ),
        _write_membership(
            tmp_path / "npi-group.sidecar",
            name="provider_npi_group",
            shard_id=shard_id,
            pairs=[(provider_npi, group) for group in groups],
        ),
    ]
    provider_map = tmp_path / "provider-set-map.tsv"
    provider_map.write_text(f"{provider_set.hex()}\t1\n")
    return artifacts, provider_map


def _binary() -> Path:
    return (
        Path(__file__).resolve().parents[1]
        / "support"
        / "ptg2_scanner"
        / "target"
        / "debug"
        / "ptg2_provider_graph_v4"
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
async def test_wrapper_authenticates_and_reuses_complete_checkpoint(tmp_path: Path) -> None:
    artifacts, provider_map = _fixture(tmp_path)
    output = tmp_path / "compiled"

    first = await compile_provider_graph_v4_rust(
        graph_artifact_entries=artifacts,
        provider_set_key_map_path=provider_map,
        output_directory=output,
        binary_path=_binary(),
    )
    assert first.checkpoint_reused is False
    assert first.resource_admission["factor_edge_count"] == 7
    assert (output / "v4-complete.json").is_file()

    second = await compile_provider_graph_v4_rust(
        graph_artifact_entries=artifacts,
        provider_set_key_map_path=provider_map,
        output_directory=output,
        binary_path=_binary(),
    )
    assert second.checkpoint_reused is True
    assert second.selected_layout == first.selected_layout
    assert second.selected_encoded_bytes == first.selected_encoded_bytes


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
