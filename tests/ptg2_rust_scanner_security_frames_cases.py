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

def test_compact_scanner_all_optional_paths_and_factor_frames(
    tmp_path,
    monkeypatch,
) -> None:
    """Verify every optional compact-scanner output and factor frame contract."""
    binary = _binary(tmp_path)
    process = _factor_frame_process()
    observed_environment_by_name: dict[str, str] = {}

    def popen(*_args, **kwargs):
        observed_environment_by_name.update(kwargs["env"])
        return process

    monkeypatch.setattr(rust_scanner, "_ptg2_rust_scanner_binary", lambda: binary)
    monkeypatch.setattr(rust_scanner.subprocess, "Popen", popen)
    monkeypatch.setenv(rust_scanner._PROVIDER_GRAPH_V4_ENV, "true")
    kwargs = _compact_kwargs(tmp_path)
    kwargs.update({name: tmp_path / name for name in _COMPACT_OPTIONAL_PATH_NAMES})
    kwargs["manifest_only"] = True
    kwargs["source_network_names"] = ["network"]

    observed_records = list(
        rust_scanner._iter_compact_serving_records_rust(
            tmp_path / "input",
            **kwargs,
        )
    )
    assert [kind for kind, _payload in observed_records] == [
        "scanner_config",
        "v3_serving_run_partition_file",
        "v3_serving_code_dictionary_file",
        "scanner_summary",
    ]
    assert (
        observed_records[-1][1]["serving_run_partition_files"][0]["path"]
        == "partition.bin"
    )
    assert (
        observed_records[-1][1]["serving_run_code_dictionary_files"][0]["path"]
        == "dictionary.bin"
    )
    assert (
        observed_environment_by_name[rust_scanner._PROVIDER_GRAPH_V4_FACTORS_ENV]
        == "true"
    )
    assert observed_environment_by_name["HLTHPRT_PTG2_MANIFEST_ONLY"] == "true"
    assert (
        "HLTHPRT_PTG2_MANIFEST_PROVIDER_SET_COMPONENT_SIDECAR_PATH"
        in observed_environment_by_name
    )


@pytest.mark.parametrize("attach_failure", [False, True])
def test_compact_scanner_setup_failure_reaps_process_and_scratch(
    tmp_path,
    monkeypatch,
    attach_failure,
) -> None:
    binary = _binary(tmp_path)
    process = _Process(None, returncode=None)
    observed_scratch_paths: list[Path] = []

    def popen(*_args, **kwargs):
        observed_scratch_paths.append(
            Path(kwargs["env"][rust_scanner._SOURCE_WITNESS_SCRATCH_DIR_ENV])
        )
        return process

    class Control:
        is_termination_requested = False

        def attach(self, _process):
            if attach_failure:
                raise RuntimeError("attach failed")

        def terminate(self):
            process.returncode = -15
            return -15

    def terminate(_process):
        process.returncode = -15
        return -15

    monkeypatch.setattr(rust_scanner, "_ptg2_rust_scanner_binary", lambda: binary)
    monkeypatch.setattr(rust_scanner.subprocess, "Popen", popen)
    monkeypatch.setattr(rust_scanner, "_terminate_subprocess_group", terminate)
    with pytest.raises(RuntimeError, match="attach failed|stdout is unavailable"):
        list(
            rust_scanner._iter_compact_serving_records_rust(
                tmp_path / "input",
                **_compact_kwargs(tmp_path),
                _process_control=Control(),
            )
        )
    assert observed_scratch_paths and not observed_scratch_paths[0].exists()


@pytest.mark.asyncio
async def test_async_compact_bridge_forwards_records_and_reader_failure(
    tmp_path,
    monkeypatch,
) -> None:
    def records(*_args, **_kwargs):
        yield "one", {"value": 1}
        yield "two", {"value": 2}

    monkeypatch.setattr(rust_scanner, "_iter_compact_serving_records_rust", records)
    observed_records = [
        scanner_record
        async for scanner_record in rust_scanner._aiter_compact_serving_records_rust(
            tmp_path / "input",
            **_compact_kwargs(tmp_path),
        )
    ]
    assert observed_records == [("one", {"value": 1}), ("two", {"value": 2})]

    def failed(*_args, **_kwargs):
        yield "one", {}
        raise RuntimeError("reader failed")

    monkeypatch.setattr(rust_scanner, "_iter_compact_serving_records_rust", failed)
    partial_records = []
    with pytest.raises(RuntimeError, match="reader failed"):
        async for partial_record in rust_scanner._aiter_compact_serving_records_rust(
            tmp_path / "input",
            **_compact_kwargs(tmp_path),
        ):
            partial_records.append(partial_record)
    assert partial_records == [("one", {})]


def test_return_code_labels_cover_unknown_signal() -> None:
    assert rust_scanner._scanner_return_code_label(3) == "exit code 3"
    assert rust_scanner._scanner_return_code_label(-999) == "signal 999"


def test_subprocess_session_process_group_and_control_edge_paths(monkeypatch) -> None:
    monkeypatch.setattr(rust_scanner.os, "name", "nt")
    assert rust_scanner._subprocess_session_options() == {}
    assert rust_scanner._is_process_group_alive(7) is False
    monkeypatch.setattr(rust_scanner.os, "name", "posix")

    class NoSession:
        def __call__(self, required):
            del required

    assert rust_scanner._subprocess_session_options(NoSession()) == {}
    monkeypatch.setattr(
        rust_scanner.inspect,
        "signature",
        lambda _spawn: (_ for _ in ()).throw(TypeError("opaque")),
    )
    assert rust_scanner._subprocess_session_options(object()) == {"start_new_session": True}

    monkeypatch.setattr(
        rust_scanner.os,
        "killpg",
        lambda *_args: (_ for _ in ()).throw(ProcessLookupError()),
    )
    assert rust_scanner._is_process_group_alive(7) is False
    monkeypatch.setattr(
        rust_scanner.os,
        "killpg",
        lambda *_args: (_ for _ in ()).throw(PermissionError()),
    )
    assert rust_scanner._is_process_group_alive(7) is True

    process = _Process(b"", returncode=0)
    monkeypatch.setattr(rust_scanner, "_is_process_group_alive", lambda _pid: False)
    assert rust_scanner._terminate_subprocess_group(process) == 0

    control = rust_scanner._ScannerProcessControl()
    control.terminate()
    monkeypatch.setattr(rust_scanner, "_terminate_subprocess_group", lambda _process: -15)
    control.attach(_Process(b"", returncode=None))
    with pytest.raises(RuntimeError, match="already attached"):
        control.attach(_Process(b""))


@pytest.mark.asyncio
async def test_async_terminate_terminal_shortcut(monkeypatch) -> None:
    class AsyncProcess(_Process):
        async def wait(self):
            self.wait_count += 1
            return 0

    process = AsyncProcess(b"", returncode=0)
    monkeypatch.setattr(rust_scanner, "_is_process_group_alive", lambda _pid: False)
    assert await rust_scanner._terminate_asyncio_subprocess_group(process) == 0


def test_binary_profiles_configured_selection_and_strict_validators(
    tmp_path,
    monkeypatch,
) -> None:
    release = tmp_path / "target" / "release" / "ptg2_scanner"
    debug = tmp_path / "target" / "debug" / "ptg2_scanner"
    custom = tmp_path / "scanner"
    for path in (release, debug, custom):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("x", encoding="ascii")
        path.chmod(0o755)
    assert rust_scanner._ptg2_scanner_binary_profile(release) == "release"
    assert rust_scanner._ptg2_scanner_binary_profile(debug) == "debug"
    assert rust_scanner._ptg2_scanner_binary_profile(custom) == "custom"

    monkeypatch.setenv(rust_scanner.PTG2_RUST_SCANNER_BIN_ENV, str(custom))
    monkeypatch.delenv(rust_scanner.PTG2_RUST_REQUIRE_RELEASE_ENV, raising=False)
    assert rust_scanner._ptg2_rust_scanner_binary() == custom
    monkeypatch.setenv(rust_scanner.PTG2_RUST_SCANNER_BIN_ENV, str(tmp_path / "missing"))
    assert rust_scanner._ptg2_rust_scanner_binary() is None
    monkeypatch.setenv(rust_scanner.PTG2_RUST_SCANNER_BIN_ENV, str(debug))
    monkeypatch.setenv(rust_scanner.PTG2_RUST_REQUIRE_RELEASE_ENV, "true")
    assert rust_scanner._ptg2_rust_scanner_binary() is None

    for invalid_count in (True, "bad", -1):
        with pytest.raises(RuntimeError):
            rust_scanner._strict_non_negative_int(invalid_count, "count")
    with pytest.raises(RuntimeError):
        rust_scanner._strict_sha256("A" * 64, "digest")
    with pytest.raises(RuntimeError):
        rust_scanner._validated_v3_file_frame(
            [],
            label="file",
            fields=("path",),
            expected_format="format",
            expected_version=1,
        )
    with pytest.raises(RuntimeError, match="activate"):
        rust_scanner._validate_v3_scanner_config(
            _config(factors=False),
            expect_factor_mode=True,
        )


def test_json_fallback_and_progress_basis_formatting_branches(monkeypatch) -> None:
    monkeypatch.setattr(rust_scanner, "orjson", None)
    assert rust_scanner._json_loads('{"x":1}') == {"x": 1}
    assert rust_scanner._format_progress_mib(None) == "unknown"
    assert rust_scanner._format_progress_duration(65) == "1m 05s"

    indexed = rust_scanner._parse_scanner_progress(
        {
            "progress_basis": "indexed_objects",
            "indexed_objects_completed": "4",
            "indexed_objects_total": "8",
        }
    )
    plain = rust_scanner._parse_scanner_progress(
        {
            "progress_basis": "plain_reordered_objects",
            "in_network_objects_completed": "3",
            "in_network_objects_total": "6",
        }
    )
    compressed = rust_scanner._parse_scanner_progress(
        {"compressed_bytes": "2", "total_bytes": "4"}
    )
    objects = rust_scanner._parse_scanner_progress({"objects": "5"})
    assert (indexed.work_done, indexed.work_total) == (4, 8)
    assert (plain.work_done, plain.work_total) == (3, 6)
    assert compressed.progress_basis == "compressed_bytes"
    assert objects.progress_basis == "objects"

    message, _rate = rust_scanner._scanner_bounded_progress_message(
        "scan",
        indexed,
    )
    assert "4/8" in message
    assert rust_scanner._scanner_failure_exception("scan", 1, ["no marker"]).__class__ is RuntimeError


def test_live_progress_derived_percent_and_metric_observer(monkeypatch) -> None:
    observed_progress_events = []
    rust_scanner._emit_scanner_live_progress("not progress", phase="scan")
    rust_scanner._emit_scanner_live_progress(
        rust_scanner._SCANNER_PROGRESS_PREFIX
        + "progress_basis=indexed_objects\tindexed_objects_completed=1"
        "\tindexed_objects_total=4",
        phase="scan",
        progress_observer=observed_progress_events.append,
    )
    rust_scanner._emit_scanner_metric_progress(
        "scanner_config",
        {
            "execution_mode": "fallback",
            "provider_reference_order": "source",
            "top_level_byte_scan_selected": False,
            "top_level_byte_scan_fallback_reason": "shape",
        },
        progress_observer=observed_progress_events.append,
    )
    assert observed_progress_events[0]["phase_pct"] == 25.0
    assert observed_progress_events[1]["scanner_fallback_reason"] == "shape"


def test_top_level_scanner_success_stderr_and_missing_binary(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(rust_scanner, "_ptg2_rust_scanner_binary", lambda: None)
    with pytest.raises(RuntimeError, match="no scanner binary"):
        list(rust_scanner._iter_top_level_object_bytes_rust(tmp_path / "input", {"x"}))

    binary = _binary(tmp_path)
    stderr_lines = [
        "",
        *[f"warning-{index}" for index in range(21)],
        rust_scanner._SCANNER_PROGRESS_PREFIX
        + "progress_basis=indexed_objects\tindexed_objects_completed=1"
        "\tindexed_objects_total=2",
    ]
    process = _Process(
        _frame("item", {"x": 1}),
        stderr=("\n".join(stderr_lines) + "\n").encode(),
        returncode=0,
    )
    monkeypatch.setattr(rust_scanner, "_ptg2_rust_scanner_binary", lambda: binary)
    monkeypatch.setattr(rust_scanner, "current_live_progress_context", lambda: {"stage_id": "x"})
    monkeypatch.setattr(rust_scanner.subprocess, "Popen", lambda *_args, **_kwargs: process)
    observed_records = list(
        rust_scanner._iter_top_level_object_bytes_rust(
            tmp_path / "input",
            {"item"},
        )
    )
    assert observed_records and process.wait_count == 1


def test_compact_missing_binary_and_run_directory(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(rust_scanner, "_ptg2_rust_scanner_binary", lambda: None)
    with pytest.raises(RuntimeError, match="no scanner binary"):
        list(
            rust_scanner._iter_compact_serving_records_rust(
                tmp_path / "input",
                **_compact_kwargs(tmp_path),
            )
        )

    monkeypatch.setattr(rust_scanner, "_ptg2_rust_scanner_binary", lambda: _binary(tmp_path))
    kwargs = _compact_kwargs(tmp_path)
    kwargs["v3_serving_run_directory"] = None
    with pytest.raises(RuntimeError, match="serving-run directory"):
        list(rust_scanner._iter_compact_serving_records_rust(tmp_path / "input", **kwargs))


def test_compact_spawn_failure_tolerates_scratch_cleanup_error(
    tmp_path,
    monkeypatch,
) -> None:
    monkeypatch.setattr(rust_scanner, "_ptg2_rust_scanner_binary", lambda: _binary(tmp_path))
    monkeypatch.setattr(
        rust_scanner.subprocess,
        "Popen",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("spawn failed")),
    )
    monkeypatch.setattr(
        rust_scanner,
        "_is_source_witness_scratch_removed",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(OSError("busy")),
    )
    with pytest.raises(RuntimeError, match="spawn failed"):
        list(
            rust_scanner._iter_compact_serving_records_rust(
                tmp_path / "input",
                **_compact_kwargs(tmp_path),
            )
        )


@pytest.mark.parametrize(
    "stdout",
    [
        b"bad header\n",
        b"item\t2\nx",
        b"item\t1\nx!",
        _frame("scanner_config", _config()),
    ],
)
def test_compact_rejects_malformed_or_incomplete_frame_contract(
    tmp_path,
    monkeypatch,
    stdout,
) -> None:
    binary = _binary(tmp_path)
    process = _Process(stdout, returncode=0)
    monkeypatch.setattr(rust_scanner, "_ptg2_rust_scanner_binary", lambda: binary)
    monkeypatch.setattr(rust_scanner.subprocess, "Popen", lambda *_args, **_kwargs: process)
    with pytest.raises(RuntimeError):
        list(
            rust_scanner._iter_compact_serving_records_rust(
                tmp_path / "input",
                **_compact_kwargs(tmp_path),
            )
        )


def test_compact_stderr_tail_failure_and_cleanup_warning(
    tmp_path,
    monkeypatch,
) -> None:
    binary = _binary(tmp_path)
    stdout = _frame("scanner_config", _config()) + _frame("scanner_summary", {})
    stderr = (
        "\n"
        + "\n".join(f"warning-{index}" for index in range(22))
        + "\n"
        + rust_scanner._SCANNER_PROGRESS_PREFIX
        + "progress_basis=indexed_objects\tindexed_objects_completed=1"
        "\tindexed_objects_total=2\n"
    ).encode()
    process = _Process(stdout, stderr=stderr, returncode=2)
    cleanup_call_counts = [0]
    original_remove = rust_scanner._is_source_witness_scratch_removed

    def remove(*args, **kwargs):
        cleanup_call_counts[0] += 1
        if cleanup_call_counts[0] == 1:
            raise OSError("busy")
        return original_remove(*args, **kwargs)

    monkeypatch.setattr(rust_scanner, "_ptg2_rust_scanner_binary", lambda: binary)
    monkeypatch.setattr(rust_scanner.subprocess, "Popen", lambda *_args, **_kwargs: process)
    monkeypatch.setattr(rust_scanner, "_is_source_witness_scratch_removed", remove)
    with pytest.raises(RuntimeError, match="failed with exit code 2"):
        list(
            rust_scanner._iter_compact_serving_records_rust(
                tmp_path / "input",
                **_compact_kwargs(tmp_path),
            )
        )
