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

@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (ProcessLookupError(), False),
        (PermissionError(), True),
        (None, True),
    ],
)
def test_live_process_probe_is_fail_closed(monkeypatch, error, expected) -> None:
    def kill(_pid, _signal):
        if error is not None:
            raise error

    monkeypatch.setattr(rust_scanner.os, "kill", kill)
    assert rust_scanner._is_live_local_process(0) is False
    assert rust_scanner._is_live_local_process(123) is expected


def test_scratch_owner_parser_and_live_orphan_guard(tmp_path, monkeypatch) -> None:
    scratch = tmp_path / f"{rust_scanner._SOURCE_WITNESS_SCRATCH_PREFIX}x"
    scratch.mkdir()
    marker = rust_scanner._source_witness_scratch_marker(scratch)
    marker.write_text("{bad", encoding="ascii")
    assert rust_scanner._validated_source_witness_scratch_owner(scratch) is None
    marker.write_text(json.dumps({"contract": "wrong", "pid": 7}), encoding="ascii")
    assert rust_scanner._validated_source_witness_scratch_owner(scratch) is None
    marker.write_text(
        json.dumps(
            {
                "contract": "healthporta_ptg2_source_witness_scratch_v1",
                "pid": True,
            }
        ),
        encoding="ascii",
    )
    assert rust_scanner._validated_source_witness_scratch_owner(scratch) is None
    marker.write_text(
        json.dumps(
            {
                "contract": "healthporta_ptg2_source_witness_scratch_v1",
                "pid": 7,
            }
        ),
        encoding="ascii",
    )
    monkeypatch.setattr(rust_scanner, "_is_live_local_process", lambda _pid: True)
    assert not rust_scanner._is_source_witness_scratch_removed(
        scratch,
        require_orphan=True,
    )
    assert scratch.is_dir()


def test_scratch_prepare_tolerates_reap_error_and_cleans_creation_failure(
    tmp_path,
    monkeypatch,
) -> None:
    run_dir = tmp_path / "runs"
    run_dir.mkdir()
    stale = run_dir / f"{rust_scanner._SOURCE_WITNESS_SCRATCH_PREFIX}stale"
    stale.mkdir()
    original_remove = rust_scanner._is_source_witness_scratch_removed
    monkeypatch.setattr(
        rust_scanner,
        "_is_source_witness_scratch_removed",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("busy")),
    )
    created = rust_scanner._prepare_source_witness_scratch_directory(run_dir)
    assert created.is_dir()
    monkeypatch.setattr(
        rust_scanner,
        "_is_source_witness_scratch_removed",
        original_remove,
    )
    assert original_remove(created, require_orphan=False)
    stale.rmdir()

    created_paths: list[Path] = []
    original_mkdtemp = rust_scanner.tempfile.mkdtemp

    def tracked_mkdtemp(**kwargs):
        path = Path(original_mkdtemp(**kwargs))
        created_paths.append(path)
        return str(path)

    class BadMarker:
        def write_text(self, *_args, **_kwargs):
            raise OSError("disk full")

    monkeypatch.setattr(rust_scanner.tempfile, "mkdtemp", tracked_mkdtemp)
    monkeypatch.setattr(rust_scanner, "_source_witness_scratch_marker", lambda _path: BadMarker())
    with pytest.raises(OSError, match="disk full"):
        rust_scanner._prepare_source_witness_scratch_directory(run_dir)
    assert created_paths and not created_paths[-1].exists()


def test_signal_group_fallback_and_terminal_shortcuts(monkeypatch) -> None:
    process = _Process(b"", returncode=None)
    monkeypatch.setattr(
        rust_scanner.os,
        "killpg",
        lambda *_args: (_ for _ in ()).throw(ProcessLookupError()),
    )
    rust_scanner._signal_process_group(process, signal.SIGTERM)
    assert process.signals == [signal.SIGTERM]

    process.returncode = 0
    process.signals.clear()
    rust_scanner._signal_process_group(process, signal.SIGTERM)
    assert process.signals == []

    process.returncode = None
    monkeypatch.setattr(
        rust_scanner.os,
        "killpg",
        lambda *_args: (_ for _ in ()).throw(PermissionError()),
    )
    rust_scanner._signal_process_group(process, signal.SIGTERM)
    assert process.signals == [signal.SIGTERM]

    process.send_signal = lambda _signal: (_ for _ in ()).throw(ProcessLookupError())
    rust_scanner._signal_process_group(process, signal.SIGTERM)


def test_sync_group_kill_timeout_uses_last_resort_kill(monkeypatch) -> None:
    process = _Process(b"", returncode=None)
    monkeypatch.setattr(rust_scanner, "_signal_process_group", lambda *_args: None)
    monkeypatch.setattr(
        rust_scanner,
        "_wait_for_subprocess_group_exit",
        lambda *_args: (_ for _ in ()).throw(TimeoutError()),
    )
    with pytest.raises(RuntimeError, match="did not exit"):
        rust_scanner._terminate_subprocess_group(process)
    assert process.killed
    assert process.wait_count == 1


@pytest.mark.asyncio
async def test_async_group_kill_timeout_uses_last_resort_kill(monkeypatch) -> None:
    class AsyncProcess(_Process):
        async def wait(self):
            self.wait_count += 1
            return self.returncode if self.returncode is not None else 0

    process = AsyncProcess(b"", returncode=None)
    monkeypatch.setattr(rust_scanner, "_signal_process_group", lambda *_args: None)

    async def timeout(*_args):
        raise TimeoutError

    monkeypatch.setattr(
        rust_scanner,
        "_wait_for_asyncio_subprocess_group_exit",
        timeout,
    )
    with pytest.raises(RuntimeError, match="did not exit"):
        await rust_scanner._terminate_asyncio_subprocess_group(process)
    assert process.killed
    assert process.wait_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("spawn_outcome", ["cancel", "error"])
async def test_failed_conversion_cleanup_handles_unavailable_spawn(
    tmp_path,
    spawn_outcome,
) -> None:
    async def spawn():
        if spawn_outcome == "cancel":
            raise asyncio.CancelledError
        raise RuntimeError("spawn failed")

    spawn_task = asyncio.create_task(spawn())
    output = tmp_path / "output"
    output.mkdir()
    manifest = tmp_path / "manifest"
    manifest.write_text("x", encoding="ascii")
    await rust_scanner._cleanup_failed_shared_graph_conversion(
        process=None,
        spawn_task=spawn_task,
        output_directory=output,
        manifest_path=manifest,
    )
    assert not output.exists()
    assert not manifest.exists()


def test_default_binary_search_skips_debug_when_release_is_required(
    monkeypatch,
) -> None:
    monkeypatch.delenv(rust_scanner.PTG2_RUST_SCANNER_BIN_ENV, raising=False)
    monkeypatch.setenv(rust_scanner.PTG2_RUST_REQUIRE_RELEASE_ENV, "true")
    monkeypatch.setattr(
        Path,
        "exists",
        lambda self: "debug" in self.parts,
    )
    monkeypatch.setattr(rust_scanner.os, "access", lambda *_args: True)
    assert rust_scanner._ptg2_rust_scanner_binary() is None


def test_scanner_error_frames_and_progress_field_parser_cover_malformed_edges() -> None:
    error = rust_scanner._scanner_failure_exception(
        "scanner",
        2,
        [
            rust_scanner._SCANNER_ERROR_PREFIX + "{bad",
            rust_scanner._SCANNER_ERROR_PREFIX + "[]",
            rust_scanner._SCANNER_ERROR_PREFIX
            + json.dumps({"code": "witness_payload_limit", "message": ""}),
        ],
    )
    assert type(error) is RuntimeError

    typed = rust_scanner._scanner_failure_exception(
        "scanner",
        2,
        [
            "prefix " + rust_scanner._SCANNER_ERROR_PREFIX
            + json.dumps({"code": "witness_payload_limit", "message": "too large"})
        ],
    )
    assert isinstance(typed, WitnessPayloadLimitError)
    assert rust_scanner._scanner_progress_fields("not progress") is None
    assert rust_scanner._scanner_progress_fields(
        rust_scanner._SCANNER_PROGRESS_PREFIX + "bad\t =x\tok= 2 "
    ) == {"ok": "2"}


def test_progress_parsing_and_messages_cover_all_basis_and_counter_shapes() -> None:
    fallback = rust_scanner._parse_scanner_progress({"progress_basis": "custom"})
    assert fallback.progress_basis == "custom"
    assert rust_scanner._format_object_rate(None) is None
    assert rust_scanner._format_object_rate(2_000_000).startswith("2.00M")
    assert rust_scanner._format_object_rate(2_000).startswith("2.0K")
    assert rust_scanner._format_object_rate(2) == "2 objects/s"

    message, rate = rust_scanner._scanner_object_progress_message(
        "scan",
        {"negotiated_rates": 3, "provider_references": 2, "in_network": 1},
        object_count=6,
        elapsed_seconds=2,
        total_bytes=1024,
    )
    assert rate == 3
    assert all(word in message for word in ("rates=3", "provider refs=2", "code groups=1"))
    empty_message, _ = rust_scanner._scanner_object_progress_message(
        "scan",
        {},
        object_count=7,
        elapsed_seconds=None,
        total_bytes=None,
    )
    assert "objects=7" in empty_message

    progress = rust_scanner._ScannerProgress(
        compressed_bytes=None,
        total_bytes=None,
        percent=None,
        eta_seconds=None,
        elapsed_seconds=2,
        compressed_mib_s=1.5,
        object_count=3,
        scanner_object_count_by_kind={"provider_references": 2},
        scanner_path=None,
        is_done=False,
        progress_basis="indexed_objects",
        work_done=4,
        work_total=None,
    )
    bounded, work_rate = rust_scanner._scanner_bounded_progress_message("scan", progress)
    assert work_rate == 2
    assert "indexed objects=4" in bounded
    assert "objects=3" in bounded
    assert "provider references=2" in bounded


def test_progress_and_metric_observer_failures_are_nonfatal(monkeypatch) -> None:
    monkeypatch.setattr(
        rust_scanner,
        "write_live_progress",
        lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("sink down")),
    )
    rust_scanner._emit_scanner_live_progress(
        rust_scanner._SCANNER_PROGRESS_PREFIX
        + "objects=2\telapsed_seconds=1\tprogress_basis=objects",
        phase="scan",
    )
    rust_scanner._emit_scanner_live_progress(
        rust_scanner._SCANNER_PROGRESS_PREFIX + "percent=1",
        phase="scan",
        progress_observer=lambda _payload: (_ for _ in ()).throw(RuntimeError("observer")),
    )
    rust_scanner._emit_scanner_metric_progress(
        "scanner_summary",
        {
            "execution_mode": "fast",
            "elapsed_seconds": 1,
            "producer_nonblocked_seconds": 0.5,
            "producer_blocked_seconds": 0.5,
            "producer_nonblocked_raw_mib_s": 2,
        },
    )
    rust_scanner._emit_scanner_metric_progress("not-metric", {})


@pytest.mark.parametrize(
    "payload",
    [
        None,
        {},
        {"factor_mode": True, "provider_graph_v4_factor_mode": False},
    ],
)
def test_factor_mode_summary_validation(payload) -> None:
    if payload is None:
        with pytest.raises(RuntimeError, match="invalid scanner_summary"):
            rust_scanner._validate_v3_scanner_summary(payload)
    elif not payload:
        assert rust_scanner._validate_v3_scanner_summary(payload) == {}
    else:
        with pytest.raises(RuntimeError, match="did not confirm"):
            rust_scanner._validate_v3_scanner_summary(payload, expect_factor_mode=True)


def test_factor_mode_environment_requires_paired_outputs(monkeypatch) -> None:
    environment_by_name = {rust_scanner._PROVIDER_GRAPH_V4_FACTORS_ENV: "stale"}
    monkeypatch.setattr(rust_scanner, "_env_bool", lambda *_args: False)
    with pytest.raises(RuntimeError, match="require both"):
        rust_scanner._use_v4_provider_factors(
            environment_by_name,
            provider_set_component_path="one",
            provider_component_group_path=None,
        )
    assert not rust_scanner._use_v4_provider_factors(
        environment_by_name,
        provider_set_component_path=None,
        provider_component_group_path=None,
    )
    assert rust_scanner._PROVIDER_GRAPH_V4_FACTORS_ENV not in environment_by_name

    monkeypatch.setattr(rust_scanner, "_env_bool", lambda *_args: True)
    with pytest.raises(RuntimeError, match="requires both"):
        rust_scanner._use_v4_provider_factors(
            {},
            provider_set_component_path=None,
            provider_component_group_path=None,
        )
    enabled_environment_by_name: dict[str, str] = {}
    assert rust_scanner._use_v4_provider_factors(
        enabled_environment_by_name,
        provider_set_component_path="one",
        provider_component_group_path="two",
    )
    assert enabled_environment_by_name[rust_scanner._PROVIDER_GRAPH_V4_ENV] == "true"
    assert (
        enabled_environment_by_name[rust_scanner._PROVIDER_GRAPH_V4_FACTORS_ENV]
        == "true"
    )


def test_top_level_scanner_malformed_frames_and_typed_failure(
    tmp_path,
    monkeypatch,
) -> None:
    binary = _binary(tmp_path)
    monkeypatch.setattr(rust_scanner, "_ptg2_rust_scanner_binary", lambda: binary)

    malformed_streams = [
        b"bad header\n",
        b"item\t2\nx",
        b"item\t1\nx!",
    ]
    for stdout in malformed_streams:
        process = _Process(stdout, returncode=0)
        monkeypatch.setattr(rust_scanner.subprocess, "Popen", lambda *_a, **_k: process)
        with pytest.raises(RuntimeError):
            list(
                rust_scanner._iter_top_level_object_bytes_rust(
                    tmp_path / "input",
                    {"item"},
                    live_progress_context={},
                )
            )

    stderr = (
        rust_scanner._SCANNER_ERROR_PREFIX
        + json.dumps({"code": "witness_payload_limit", "message": "too much"})
        + "\n"
    ).encode()
    process = _Process(b"", stderr=stderr, returncode=2)
    monkeypatch.setattr(rust_scanner.subprocess, "Popen", lambda *_a, **_k: process)
    with pytest.raises(WitnessPayloadLimitError, match="too much"):
        list(
            rust_scanner._iter_top_level_object_bytes_rust(
                tmp_path / "input",
                {"item"},
                live_progress_context={},
            )
        )
