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

def test_shared_graph_digest_directory_and_total_mismatch_guards(
    tmp_path,
) -> None:
    expected_file = tmp_path / "missing"
    with pytest.raises(RuntimeError, match="output is unavailable"):
        rust_scanner._shared_graph_summary_path(
            {"file": str(expected_file.resolve())},
            "file",
            expected_path=expected_file.resolve(),
        )

    output, summary_by_field, expected = _valid_shared_graph_summary(tmp_path)
    missing_output = tmp_path / "missing-output"
    missing_summary_by_field = dict(summary_by_field)
    missing_summary_by_field["scratch_directory"] = str(missing_output.resolve())
    missing_summary_by_field["output_directory"] = str(missing_output.resolve())
    with pytest.raises(RuntimeError, match="output directory is unavailable"):
        rust_scanner._shared_graph_result_from_summary(
            missing_summary_by_field,
            expected_output_directory=missing_output,
            expected=expected,
        )

    for digest in ("not-hex", "ab"):
        invalid_summary_by_field = dict(summary_by_field)
        invalid_summary_by_field["support_digest"] = digest
        with pytest.raises(RuntimeError, match="invalid support_digest"):
            rust_scanner._shared_graph_result_from_summary(
                invalid_summary_by_field,
                expected_output_directory=output,
                expected=expected,
            )
    for field_name in ("block_count", "owner_count", "raw_block_byte_count"):
        invalid_summary_by_field = dict(summary_by_field)
        invalid_summary_by_field[field_name] = (
            int(summary_by_field[field_name]) + 1
        )
        with pytest.raises(RuntimeError):
            rust_scanner._shared_graph_result_from_summary(
                invalid_summary_by_field,
                expected_output_directory=output,
                expected=expected,
            )
