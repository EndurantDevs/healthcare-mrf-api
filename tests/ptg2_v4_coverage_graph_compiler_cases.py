from __future__ import annotations

from tests.ptg2_v4_coverage_support import (
    Path,
    compiler,
    hashlib,
    intersection,
    json,
    pytest,
    struct,
)

def _assert_compiler_scalar_validation(monkeypatch) -> None:
    assert compiler._load_json_bytes(b'{"a":1}', label="test") == {"a": 1}
    with pytest.raises(RuntimeError, match="repeats field"):
        compiler._load_json_bytes(b'{"a":1,"a":2}', label="test")
    with pytest.raises(RuntimeError, match="invalid JSON"):
        compiler._load_json_bytes(b"{", label="test")
    for invalid_count in (True, -1, "1"):
        with pytest.raises(RuntimeError, match="invalid count"):
            compiler._strict_nonnegative_int(invalid_count, label="count")
    with pytest.raises(RuntimeError, match="out-of-range"):
        compiler._strict_nonnegative_int(2**63, label="count")
    assert compiler._strict_nonnegative_int(7, label="count") == 7

    monkeypatch.setenv("PTG_TEST_OPTION", "7")
    assert compiler._positive_env_int("PTG_TEST_OPTION", 3) == 7
    monkeypatch.setenv("PTG_TEST_OPTION", "broken")
    with pytest.raises(RuntimeError, match="not an integer"):
        compiler._positive_env_int("PTG_TEST_OPTION", 3)
    monkeypatch.setenv("PTG_TEST_OPTION", "0")
    with pytest.raises(RuntimeError, match="out of range"):
        compiler._positive_env_int("PTG_TEST_OPTION", 3)

    assert compiler._strict_sha256("ab" * 32, label="digest") == "ab" * 32
    for digest in ("AB" * 32, "xx" * 32, "short"):
        with pytest.raises(RuntimeError, match="invalid digest"):
            compiler._strict_sha256(digest, label="digest")


def _assert_artifact_manifest(tmp_path: Path) -> None:
    artifact = tmp_path / "artifact.bin"
    artifact.write_bytes(b"payload")
    artifact_entry_by_field = {
        "path": str(artifact),
        "record_format": "test",
        "sha256": hashlib.sha256(b"payload").hexdigest(),
        "byte_count": 7,
        "owner_count": 1,
        "member_count": 2,
        "member_global_count": 2,
        "name": "factor",
        "source_shard_id": "shard",
    }
    manifest, byte_count = compiler._artifact_manifest(artifact_entry_by_field)
    assert byte_count == 7
    assert manifest["metadata"]["member_global_count"] == 2
    with pytest.raises(RuntimeError, match="lacks a path"):
        compiler._artifact_manifest({})
    with pytest.raises(RuntimeError, match="byte count changed"):
        compiler._artifact_manifest({**artifact_entry_by_field, "byte_count": 8})
    with pytest.raises(RuntimeError, match="invalid name"):
        compiler._artifact_manifest({**artifact_entry_by_field, "name": 7})


def _assert_copy_file_validation(tmp_path: Path) -> None:
    copy_path = tmp_path / "rows.copy"
    copy_path.write_bytes(
        compiler._PG_COPY_HEADER
        + struct.pack(">hih", 1, 2, compiler.PTG2_V4_SHARED_FORMAT_VERSION)
        + struct.pack(">h", -1)
    )
    assert compiler._count_pg_binary_rows(
        copy_path,
        expected_field_count=1,
        validate_shared_version=True,
    ) == 1
    copy_path.write_bytes(b"bad")
    with pytest.raises(RuntimeError, match="invalid COPY header"):
        compiler._count_pg_binary_rows(copy_path, expected_field_count=1)


def _assert_reference_manifest(tmp_path: Path) -> None:
    references = tmp_path / "references.jsonl"
    references.write_text(
        json.dumps(
            {
                "object_kind": "v4_members_v1",
                "block_key": 0,
                "fragment_no": 0,
                "hash": "ab" * 32,
                "codec": "none",
            }
        )
        + "\n",
        encoding="ascii",
    )
    compiler._validate_reference_manifest(references, 1)
    with pytest.raises(RuntimeError, match="row count"):
        compiler._validate_reference_manifest(references, 2)
    references.write_text(
        json.dumps(
            {
                "object_kind": "wrong",
                "block_key": 0,
                "fragment_no": 0,
                "hash": "ab" * 32,
                "codec": "none",
            }
        )
        + "\n",
        encoding="ascii",
    )
    with pytest.raises(RuntimeError, match="object_kind"):
        compiler._validate_reference_manifest(references, 1)


def _assert_sorted_intersection() -> None:
    assert intersection.intersect_sorted_u32((1, 3, 5), (0, 1, 5, 9)) == (
        1,
        5,
    )
    with pytest.raises(ValueError, match="strictly increasing"):
        intersection._strict_sorted_u32((1, 1))
    with pytest.raises(ValueError, match="uint32"):
        intersection._strict_sorted_u32((-1,))


def test_compiler_and_intersection_validation_edges(tmp_path: Path, monkeypatch) -> None:
    """Validate compiler input records and sorted intersection edge cases."""
    _assert_compiler_scalar_validation(monkeypatch)
    _assert_artifact_manifest(tmp_path)
    _assert_copy_file_validation(tmp_path)
    _assert_reference_manifest(tmp_path)
    _assert_sorted_intersection()


def _invalid_progress_events():
    baseline_event_by_field = {
        "version": 1,
        "seq": 1,
        "phase": "resource_admission",
        "done": 0,
        "total": 1,
        "unit": "stage",
        "elapsed_ms": 0,
        "terminal": False,
    }
    return (
        None,
        {},
        {**baseline_event_by_field, "version": 99},
        {**baseline_event_by_field, "seq": 2},
        {**baseline_event_by_field, "phase": "missing"},
        {**baseline_event_by_field, "done": 2},
        {**baseline_event_by_field, "unit": ""},
        {**baseline_event_by_field, "terminal": True},
    )


def _assert_compiler_progress_events() -> None:
    state = compiler._CompilerProgressState()
    assert all(not state.is_accepted(event) for event in _invalid_progress_events())
    valid_progress_event_by_field = {
        "version": 1,
        "seq": 1,
        "phase": "resource_admission",
        "done": 0,
        "total": 1,
        "unit": "stage",
        "elapsed_ms": 1,
        "terminal": False,
    }
    assert state.is_accepted(valid_progress_event_by_field)
    for update in (
        {**valid_progress_event_by_field, "seq": 2, "done": 0, "total": 2},
        {**valid_progress_event_by_field, "seq": 2, "done": 0, "elapsed_ms": 0},
    ):
        assert not state.is_accepted(update)


def _assert_bounded_compiler_files(tmp_path: Path) -> None:
    oversized = tmp_path / "oversized"
    oversized.write_bytes(b"1234")
    with pytest.raises(RuntimeError, match="exceeds"):
        compiler._read_bounded(oversized, 3, label="test")
    assert compiler._read_bounded(oversized, 4, label="test") == b"1234"
    error_log = tmp_path / "error.log"
    error_log.write_bytes(b"x" * (compiler.PTG2_V4_GRAPH_ERROR_TAIL_BYTES + 5))
    assert len(compiler._read_error_tail(error_log)) == (
        compiler.PTG2_V4_GRAPH_ERROR_TAIL_BYTES
    )


def _assert_compiler_binary_resolution(tmp_path: Path, monkeypatch) -> Path:
    executable = tmp_path / "compiler"
    executable.write_text("#!/bin/sh\nexit 0\n", encoding="ascii")
    executable.chmod(0o755)
    monkeypatch.setenv(compiler.PTG2_V4_GRAPH_COMPILER_BIN_ENV, str(executable))
    assert compiler._resolve_v4_graph_compiler_binary() == executable.resolve()
    monkeypatch.setenv(
        compiler.PTG2_V4_GRAPH_COMPILER_BIN_ENV,
        str(tmp_path / "missing"),
    )
    assert compiler._resolve_v4_graph_compiler_binary() is None
    return executable


def _assert_summary_path_validation(tmp_path: Path, executable: Path) -> None:
    with pytest.raises(RuntimeError, match="invalid path"):
        compiler._summary_path({}, "path", executable)
    with pytest.raises(RuntimeError, match="unexpected path"):
        compiler._summary_path(
            {"path": str(tmp_path / "relative")},
            "path",
            executable,
        )
    with pytest.raises(RuntimeError, match="unavailable"):
        compiler._summary_path(
            {"path": str((tmp_path / "missing").resolve())},
            "path",
            tmp_path / "missing",
        )


def test_compiler_progress_and_file_validation_branch_matrix(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Validate compiler progress events and required source-file metadata."""
    _assert_compiler_progress_events()
    _assert_bounded_compiler_files(tmp_path)
    executable = _assert_compiler_binary_resolution(tmp_path, monkeypatch)
    _assert_summary_path_validation(tmp_path, executable)
