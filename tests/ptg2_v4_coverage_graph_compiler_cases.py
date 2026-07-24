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


def _copy_structure_error_cases():
    return (
        (compiler._PG_COPY_HEADER, "truncates COPY rows", 1, False),
        (
            compiler._PG_COPY_HEADER + struct.pack(">h", -1) + b"x",
            "trailing COPY bytes",
            1,
            False,
        ),
        (
            compiler._PG_COPY_HEADER + struct.pack(">h", 2),
            "wrong COPY width",
            1,
            False,
        ),
        (
            compiler._PG_COPY_HEADER + struct.pack(">h", 1) + b"\0",
            "truncates COPY field",
            1,
            False,
        ),
        (
            compiler._PG_COPY_HEADER + struct.pack(">hi", 1, -1),
            "NULL COPY field",
            1,
            False,
        ),
        (
            compiler._PG_COPY_HEADER + struct.pack(">hi", 1, 8) + b"x",
            "truncates COPY field",
            1,
            False,
        ),
    )


def _copy_version_error_cases():
    return (
        (
            compiler._PG_COPY_HEADER
            + struct.pack(">hi", 2, 0)
            + struct.pack(">i", 1)
            + b"\1",
            "invalid format-version width",
            2,
            True,
        ),
        (
            compiler._PG_COPY_HEADER
            + struct.pack(">hi", 2, 0)
            + struct.pack(">ih", 2, compiler.PTG2_V4_SHARED_FORMAT_VERSION + 1),
            "changed the shared CAS wire version",
            2,
            True,
        ),
    )


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

    malformed_cases = _copy_structure_error_cases() + _copy_version_error_cases()
    for copy_payload, message, field_count, validate_shared_version in malformed_cases:
        copy_path.write_bytes(copy_payload)
        with pytest.raises(RuntimeError, match=message):
            compiler._count_pg_binary_rows(
                copy_path,
                expected_field_count=field_count,
                validate_shared_version=validate_shared_version,
            )

    copy_path.write_bytes(
        compiler._PG_COPY_HEADER
        + struct.pack(">hi", 2, 0)
        + struct.pack(">ih", 2, compiler.PTG2_V4_SHARED_FORMAT_VERSION)
        + struct.pack(">h", -1)
    )
    assert (
        compiler._count_pg_binary_rows(
            copy_path,
            expected_field_count=2,
            validate_shared_version=True,
        )
        == 1
    )


def _prefix_metadata_payload(
    *rows: tuple[int, int, bytes],
    trailing: bytes = b"",
) -> bytes:
    prefix_payload = bytearray(compiler._PG_COPY_HEADER)
    for provider_set_key, member_count, digest in rows:
        prefix_payload.extend(struct.pack(">h", 3))
        for field in (
            struct.pack(">i", provider_set_key),
            struct.pack(">i", member_count),
            digest,
        ):
            prefix_payload.extend(struct.pack(">i", len(field)))
            prefix_payload.extend(field)
    prefix_payload.extend(struct.pack(">h", -1))
    prefix_payload.extend(trailing)
    return bytes(prefix_payload)


def _assert_prefix_metadata_validation(tmp_path: Path) -> None:
    metadata_path = tmp_path / "prefix-metadata.copy"
    digest = b"d" * 32
    metadata_path.write_bytes(_prefix_metadata_payload((1, 2, digest)))
    assert compiler._read_prefix_override_metadata(
        metadata_path,
        prefix_target=2,
    ) == {1: (2, digest)}

    malformed_by_message = (
        (b"bad", "invalid COPY header"),
        (compiler._PG_COPY_HEADER, "truncates COPY rows"),
        (_prefix_metadata_payload(trailing=b"x"), "trailing bytes"),
        (
            compiler._PG_COPY_HEADER + struct.pack(">h", 2),
            "invalid row width",
        ),
        (
            compiler._PG_COPY_HEADER + struct.pack(">h", 3) + b"\0",
            "truncates field width",
        ),
        (
            compiler._PG_COPY_HEADER + struct.pack(">hi", 3, 3),
            "invalid field width",
        ),
        (
            compiler._PG_COPY_HEADER + struct.pack(">hi", 3, 4) + b"\0",
            "truncates field",
        ),
        (_prefix_metadata_payload((-1, 0, digest)), "not canonical"),
        (_prefix_metadata_payload((1, -1, digest)), "not canonical"),
        (_prefix_metadata_payload((1, 3, digest)), "not canonical"),
        (
            _prefix_metadata_payload((1, 1, digest), (1, 1, digest)),
            "not canonical",
        ),
    )
    for prefix_payload, message in malformed_by_message:
        metadata_path.write_bytes(prefix_payload)
        with pytest.raises(RuntimeError, match=message):
            compiler._read_prefix_override_metadata(
                metadata_path,
                prefix_target=2,
            )


def _assert_reference_manifest(tmp_path: Path) -> None:
    references = tmp_path / "references.jsonl"
    valid_record_by_field = {
        "object_kind": "v4_members_v1",
        "block_key": 0,
        "fragment_no": 0,
        "hash": "ab" * 32,
        "codec": "none",
    }
    references.write_text(
        json.dumps(valid_record_by_field) + "\n",
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

    references.write_bytes(b"x" * (64 * 1024 + 1) + b"\n")
    with pytest.raises(RuntimeError, match="exceeds 64 KiB"):
        compiler._validate_reference_manifest(references, 1)
    references.write_text("[]\n", encoding="ascii")
    with pytest.raises(RuntimeError, match="not an object"):
        compiler._validate_reference_manifest(references, 1)
    references.write_text(
        "\n".join(
            (
                json.dumps(valid_record_by_field),
                json.dumps(valid_record_by_field),
            )
        )
        + "\n",
        encoding="ascii",
    )
    with pytest.raises(RuntimeError, match="strict coordinate order"):
        compiler._validate_reference_manifest(references, 2)
    references.write_text(
        json.dumps({**valid_record_by_field, "codec": "gzip"}) + "\n",
        encoding="ascii",
    )
    with pytest.raises(RuntimeError, match="unexpectedly compressed"):
        compiler._validate_reference_manifest(references, 1)


def _assert_observe_counter_validation() -> None:
    assert compiler._normalized_observe_value(
        "npi_prefix_worst_provider_set_key",
        None,
    ) is None
    assert compiler._normalized_observe_value(
        "npi_prefix_worst_member_digest",
        None,
    ) is None
    assert compiler._normalized_observe_value(
        "npi_prefix_worst_online_member_digest",
        "ab" * 32,
    ) == "ab" * 32
    assert compiler._normalized_observe_value(
        "npi_prefix_worst_provider_set_uses_override",
        True,
    ) is True
    with pytest.raises(RuntimeError, match="invalid observe"):
        compiler._normalized_observe_value(
            "npi_prefix_worst_provider_set_uses_override",
            1,
        )
    assert compiler._normalized_observe_value("ordinary_counter", 3) == 3
    with pytest.raises(RuntimeError, match="invalid observe counters"):
        compiler._normalize_observe_counters(())
    assert compiler._normalize_observe_counters(
        {
            "npi_prefix_worst_online_provider_set_key": None,
            "ordinary_counter": 4,
        }
    ) == {
        "npi_prefix_worst_online_provider_set_key": None,
        "ordinary_counter": 4,
    }


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
    _assert_prefix_metadata_validation(tmp_path)
    _assert_reference_manifest(tmp_path)
    _assert_observe_counter_validation()
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
    with pytest.raises(
        RuntimeError,
        match=r"is 4 bytes; maximum is 3 bytes",
    ):
        compiler._read_bounded(oversized, 3, label="test")
    assert compiler._read_bounded(oversized, 4, label="test") == b"1234"
    formerly_oversized = tmp_path / "formerly-oversized"
    formerly_oversized.write_bytes(b"x" * (2 * 1024 * 1024 + 1))
    assert (
        len(
            compiler._read_bounded(
                formerly_oversized,
                compiler.PTG2_V4_GRAPH_SUMMARY_MAX_BYTES,
                label="summary",
            )
        )
        == 2 * 1024 * 1024 + 1
    )
    over_limit = tmp_path / "over-limit"
    with over_limit.open("wb") as output:
        output.truncate(compiler.PTG2_V4_GRAPH_SUMMARY_MAX_BYTES + 1)
    with pytest.raises(
        RuntimeError,
        match=(
            rf"is {compiler.PTG2_V4_GRAPH_SUMMARY_MAX_BYTES + 1} bytes; "
            rf"maximum is {compiler.PTG2_V4_GRAPH_SUMMARY_MAX_BYTES} bytes"
        ),
    ):
        compiler._read_bounded(
            over_limit,
            compiler.PTG2_V4_GRAPH_SUMMARY_MAX_BYTES,
            label="summary",
        )
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
    symlink = tmp_path / "compiler-symlink"
    symlink.symlink_to(executable)
    with pytest.raises(RuntimeError, match="unexpected path"):
        compiler._summary_path(
            {"path": str(symlink.resolve(strict=False).parent / symlink.name)},
            "path",
            executable,
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
