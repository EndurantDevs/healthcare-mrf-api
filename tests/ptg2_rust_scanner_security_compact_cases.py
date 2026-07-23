from __future__ import annotations

from tests.ptg2_rust_scanner_security_support import (
    asyncio,
    io,
    json,
    os,
    signal,
    Path,
    SimpleNamespace,
    pytest,
    rust_scanner,
    WitnessPayloadLimitError,
    _SHARED_GRAPH_DIRECTION_SPECS,
    _COMPACT_OPTIONAL_PATH_NAMES,
    _frame,
    _Process,
    _binary,
    _config,
    _compact_kwargs,
    _factor_frame_process,
    _valid_shared_graph_summary,
)

@pytest.mark.asyncio
async def test_shared_graph_conversion_precondition_failures(tmp_path, monkeypatch) -> None:
    """Reject unavailable tools, inputs, and occupied shared-graph destinations."""
    monkeypatch.setattr(rust_scanner, "_ptg2_rust_scanner_binary", lambda: None)
    with pytest.raises(RuntimeError, match="requires the PTG2 Rust scanner"):
        await rust_scanner.convert_membership_shards_to_shared_graph_rust(
            shards=[],
            provider_set_key_map_path=tmp_path / "map",
            output_directory=tmp_path / "output",
        )
    unavailable = tmp_path / "missing-binary"
    monkeypatch.setattr(rust_scanner, "_ptg2_rust_scanner_binary", lambda: unavailable)
    with pytest.raises(RuntimeError, match="binary is unavailable"):
        await rust_scanner.convert_membership_shards_to_shared_graph_rust(
            shards=[],
            provider_set_key_map_path=tmp_path / "map",
            output_directory=tmp_path / "output",
        )
    binary = _binary(tmp_path)
    monkeypatch.setattr(rust_scanner, "_ptg2_rust_scanner_binary", lambda: binary)
    with pytest.raises(RuntimeError, match="provider-set key map"):
        await rust_scanner.convert_membership_shards_to_shared_graph_rust(
            shards=[],
            provider_set_key_map_path=tmp_path / "map",
            output_directory=tmp_path / "output",
        )

    provider_map = tmp_path / "map"
    provider_map.write_bytes(b"x")
    output = tmp_path / "output"
    output.mkdir()
    with pytest.raises(RuntimeError, match="already exists"):
        await rust_scanner.convert_membership_shards_to_shared_graph_rust(
            shards=[],
            provider_set_key_map_path=provider_map,
            output_directory=output,
        )
    output.rmdir()
    with pytest.raises(RuntimeError, match="parent directory"):
        await rust_scanner.convert_membership_shards_to_shared_graph_rust(
            shards=[],
            provider_set_key_map_path=provider_map,
            output_directory=tmp_path / "missing-parent" / "output",
        )

    monkeypatch.setattr(
        rust_scanner,
        "_shared_graph_manifest",
        lambda **_kwargs: (
            {},
            rust_scanner._SharedGraphExpected(0, 0, 0, 0),
        ),
    )
    manifest_path = tmp_path / "output.manifest.json"
    manifest_path.write_text("occupied", encoding="ascii")
    with pytest.raises(RuntimeError, match="manifest already exists"):
        await rust_scanner.convert_membership_shards_to_shared_graph_rust(
            shards=[],
            provider_set_key_map_path=provider_map,
            output_directory=tmp_path / "output",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("returncode", [0, 2])
async def test_shared_graph_converter_stderr_and_failure_paths(
    tmp_path,
    monkeypatch,
    returncode,
) -> None:
    binary = _binary(tmp_path)
    provider_map = tmp_path / "map"
    provider_map.write_bytes(b"x")
    expected = rust_scanner._SharedGraphExpected(0, 0, 0, 0)

    class Process:
        pid = 123

        def __init__(self):
            self.returncode = returncode

        async def communicate(self):
            return b"summary", b"converter note"

    async def spawn(*_args, **_kwargs):
        return Process()

    monkeypatch.setattr(rust_scanner, "_ptg2_rust_scanner_binary", lambda: binary)
    monkeypatch.setattr(
        rust_scanner,
        "_shared_graph_manifest",
        lambda **_kwargs: ({}, expected),
    )
    monkeypatch.setattr(rust_scanner.asyncio, "create_subprocess_exec", spawn)
    sentinel = object()
    monkeypatch.setattr(
        rust_scanner,
        "_parse_shared_graph_summary_frame",
        lambda *_args, **_kwargs: sentinel,
    )
    if returncode:
        with pytest.raises(RuntimeError, match="exit code 2"):
            await rust_scanner.convert_membership_shards_to_shared_graph_rust(
                shards=[],
                provider_set_key_map_path=provider_map,
                output_directory=tmp_path / "failed-output",
            )
    else:
        assert (
            await rust_scanner.convert_membership_shards_to_shared_graph_rust(
                shards=[],
                provider_set_key_map_path=provider_map,
                output_directory=tmp_path / "success-output",
            )
            is sentinel
        )


def test_strict_shared_graph_scalar_and_path_validation(tmp_path) -> None:
    with pytest.raises(RuntimeError, match="invalid count"):
        rust_scanner._shared_graph_strict_int(True, "count")
    with pytest.raises(RuntimeError, match="out-of-range count"):
        rust_scanner._shared_graph_strict_int(2**63, "count")
    expected = tmp_path / "expected"
    with pytest.raises(RuntimeError, match="invalid file"):
        rust_scanner._shared_graph_summary_path({}, "file", expected_path=expected)
    relative_path_by_field = {"file": "relative"}
    with pytest.raises(RuntimeError, match="unexpected file"):
        rust_scanner._shared_graph_summary_path(
            relative_path_by_field,
            "file",
            expected_path=expected,
        )
    expected.write_text("x", encoding="ascii")
    assert (
        rust_scanner._shared_graph_summary_path(
            {"file": str(expected.resolve())},
            "file",
            expected_path=expected.resolve(),
        )
        == expected.resolve()
    )
    with pytest.raises(RuntimeError, match="invalid metric"):
        rust_scanner._shared_graph_metric_object(
            {},
            field_name="metric",
            expected_fields=frozenset({"x"}),
        )


def test_compact_no_stderr_and_setup_cleanup_paths(tmp_path, monkeypatch) -> None:
    binary = _binary(tmp_path)
    valid = _frame("scanner_config", _config()) + _frame("scanner_summary", {})
    process = _Process(valid, stderr=None, returncode=0)
    monkeypatch.setattr(rust_scanner, "_ptg2_rust_scanner_binary", lambda: binary)
    monkeypatch.setattr(rust_scanner.subprocess, "Popen", lambda *_args, **_kwargs: process)
    assert len(
        list(
            rust_scanner._iter_compact_serving_records_rust(
                tmp_path / "input",
                **_compact_kwargs(tmp_path),
            )
        )
    ) == 2

    process = _Process(None, stderr=b"", returncode=0)
    monkeypatch.setattr(rust_scanner.subprocess, "Popen", lambda *_args, **_kwargs: process)
    monkeypatch.setattr(
        rust_scanner,
        "_is_source_witness_scratch_removed",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("busy")),
    )
    with pytest.raises(RuntimeError, match="stdout is unavailable"):
        list(
            rust_scanner._iter_compact_serving_records_rust(
                tmp_path / "input-2",
                **_compact_kwargs(tmp_path),
            )
        )


def test_compact_setup_joins_started_thread_on_late_setup_failure(
    tmp_path,
    monkeypatch,
) -> None:
    binary = _binary(tmp_path)
    process = _Process(b"", stderr=b"", returncode=0)

    class Thread:
        def __init__(self, **_kwargs):
            self.joined = False

        def start(self):
            raise RuntimeError("thread start failed")

        def join(self, *_args, **_kwargs):
            self.joined = True

    monkeypatch.setattr(rust_scanner, "_ptg2_rust_scanner_binary", lambda: binary)
    monkeypatch.setattr(rust_scanner.subprocess, "Popen", lambda *_args, **_kwargs: process)
    monkeypatch.setattr(rust_scanner.threading, "Thread", Thread)
    with pytest.raises(RuntimeError, match="thread start failed"):
        list(
            rust_scanner._iter_compact_serving_records_rust(
                tmp_path / "input",
                **_compact_kwargs(tmp_path),
            )
        )


def test_compact_mid_frame_timeout_reports_running_status(tmp_path, monkeypatch) -> None:
    binary = _binary(tmp_path)

    class TimeoutProcess(_Process):
        def wait(self, timeout=None):
            if timeout is not None:
                self.returncode = 0
                raise rust_scanner.subprocess.TimeoutExpired("scanner", timeout)
            return 0

    process = TimeoutProcess(b"item\t2\nx", stderr=None, returncode=None)
    monkeypatch.setattr(rust_scanner, "_ptg2_rust_scanner_binary", lambda: binary)
    monkeypatch.setattr(rust_scanner.subprocess, "Popen", lambda *_args, **_kwargs: process)
    with pytest.raises(RuntimeError, match="still running after stdout closed"):
        list(
            rust_scanner._iter_compact_serving_records_rust(
                tmp_path / "input",
                **_compact_kwargs(tmp_path),
            )
        )


@pytest.mark.asyncio
async def test_async_bridge_retries_full_queue_and_handles_iterators_without_close(
    tmp_path,
    monkeypatch,
) -> None:
    real_queue = rust_scanner.queue.Queue

    class FullOnceQueue(real_queue):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.failed_once = False

        def put(self, queue_item, *args, **kwargs):
            if not self.failed_once:
                self.failed_once = True
                raise rust_scanner.queue.Full
            return super().put(queue_item, *args, **kwargs)

    monkeypatch.setattr(rust_scanner.queue, "Queue", FullOnceQueue)
    monkeypatch.setattr(
        rust_scanner,
        "_iter_compact_serving_records_rust",
        lambda *_args, **_kwargs: iter([("one", {})]),
    )
    observed_records = [
        record_entry
        async for record_entry in rust_scanner._aiter_compact_serving_records_rust(
            tmp_path / "input",
            **_compact_kwargs(tmp_path),
        )
    ]
    assert observed_records == [("one", {})]


@pytest.mark.asyncio
@pytest.mark.parametrize("message", ["generator already executing", "other close failure"])
async def test_async_bridge_close_value_error_contract(
    tmp_path,
    monkeypatch,
    message,
) -> None:
    class Iterator:
        def __init__(self):
            self.used = False

        def __iter__(self):
            return self

        def __next__(self):
            if self.used:
                raise StopIteration
            self.used = True
            return "one", {}

        def close(self):
            raise ValueError(message)

    monkeypatch.setattr(
        rust_scanner,
        "_iter_compact_serving_records_rust",
        lambda *_args, **_kwargs: Iterator(),
    )
    if message == "generator already executing":
        observed_records = [
            record_entry
            async for record_entry in rust_scanner._aiter_compact_serving_records_rust(
                tmp_path / "input",
                **_compact_kwargs(tmp_path),
            )
        ]
        assert observed_records == [("one", {})]
    else:
        partial_records = []
        with pytest.raises(ValueError, match="other close failure"):
            async for partial_record in rust_scanner._aiter_compact_serving_records_rust(
                tmp_path / "input",
                **_compact_kwargs(tmp_path),
            ):
                partial_records.append(partial_record)
        assert partial_records == [("one", {})]


def test_return_code_label_known_signal() -> None:
    assert "SIGTERM" in rust_scanner._scanner_return_code_label(-signal.SIGTERM)


def test_remaining_process_branch_shortcuts(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(rust_scanner.os, "name", "posix")
    assert rust_scanner._subprocess_session_options() == {"start_new_session": True}

    process = _Process(b"", returncode=0)
    monkeypatch.setattr(
        rust_scanner.os,
        "killpg",
        lambda *_args: (_ for _ in ()).throw(PermissionError()),
    )
    rust_scanner._signal_process_group(process, signal.SIGTERM)
    assert process.signals == []

    process = _Process(b"", returncode=0)
    monkeypatch.setattr(rust_scanner, "_signal_process_group", lambda *_args: None)
    wait_call_counts = [0]

    def timeout_then_finished(*_args):
        wait_call_counts[0] += 1
        if wait_call_counts[0] == 1:
            process.returncode = 0
        raise TimeoutError

    monkeypatch.setattr(
        rust_scanner,
        "_wait_for_subprocess_group_exit",
        timeout_then_finished,
    )
    with pytest.raises(RuntimeError, match="did not exit"):
        rust_scanner._terminate_subprocess_group(process)
    assert not process.killed

    run_dir = tmp_path / "runs"
    run_dir.mkdir()
    (run_dir / "unrelated").write_text("x", encoding="ascii")
    scratch = rust_scanner._prepare_source_witness_scratch_directory(run_dir)
    assert scratch.is_dir()
    assert rust_scanner._is_source_witness_scratch_removed(
        scratch,
        require_orphan=False,
    )


@pytest.mark.asyncio
async def test_async_kill_timeout_skips_kill_for_finished_process(monkeypatch) -> None:
    class AsyncProcess(_Process):
        async def wait(self):
            return 0

    process = AsyncProcess(b"", returncode=None)
    monkeypatch.setattr(rust_scanner, "_signal_process_group", lambda *_args: None)
    wait_call_counts = [0]

    async def timeout(*_args):
        wait_call_counts[0] += 1
        if wait_call_counts[0] == 1:
            process.returncode = 0
        raise TimeoutError

    monkeypatch.setattr(
        rust_scanner,
        "_wait_for_asyncio_subprocess_group_exit",
        timeout,
    )
    with pytest.raises(RuntimeError, match="did not exit"):
        await rust_scanner._terminate_asyncio_subprocess_group(process)
    assert not process.killed


def test_default_binary_search_selects_release(monkeypatch) -> None:
    monkeypatch.delenv(rust_scanner.PTG2_RUST_SCANNER_BIN_ENV, raising=False)
    monkeypatch.delenv(rust_scanner.PTG2_RUST_REQUIRE_RELEASE_ENV, raising=False)
    monkeypatch.setattr(Path, "exists", lambda self: "release" in self.parts)
    monkeypatch.setattr(rust_scanner.os, "access", lambda *_args: True)
    selected = rust_scanner._ptg2_rust_scanner_binary()
    assert selected is not None and "release" in selected.parts


def test_top_level_scanner_without_stderr(tmp_path, monkeypatch) -> None:
    binary = _binary(tmp_path)
    process = _Process(_frame("item", {}), stderr=None, returncode=0)
    monkeypatch.setattr(rust_scanner, "_ptg2_rust_scanner_binary", lambda: binary)
    monkeypatch.setattr(rust_scanner.subprocess, "Popen", lambda *_args, **_kwargs: process)
    assert list(
        rust_scanner._iter_top_level_object_bytes_rust(
            tmp_path / "input",
            {"item"},
            live_progress_context={},
        )
    )
