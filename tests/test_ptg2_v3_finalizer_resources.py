from __future__ import annotations

import json

import pytest

from process.ptg_parts import ptg2_shared_finalize as finalizer_module
from process.ptg_parts.config import (
    PTG2_V3_FINALIZER_IDENTITY_MAP_MAX_BYTES_ENV,
    PTG2_V3_FINALIZER_LEGACY_MEMORY_RECORDS_ENV,
    PTG2_V3_FINALIZER_TOTAL_SORT_MEMORY_BYTES_ENV,
    PTG2_V3_FINALIZER_WORKERS_ENV,
)
from process.ptg_parts.ptg2_shared_finalize import (
    PTG2_V3_FINALIZER_RESOURCE_CONTRACT,
    attach_v3_dictionary_contract,
    attach_v3_source_run_contract,
    run_v3_direct_finalizer,
    validate_v3_finalizer_summary,
)


_MIB = 1024 * 1024
_GIB = 1024 * _MIB
_RESOURCE_ENVIRONMENT_NAMES = (
    PTG2_V3_FINALIZER_WORKERS_ENV,
    PTG2_V3_FINALIZER_IDENTITY_MAP_MAX_BYTES_ENV,
    PTG2_V3_FINALIZER_TOTAL_SORT_MEMORY_BYTES_ENV,
    PTG2_V3_FINALIZER_LEGACY_MEMORY_RECORDS_ENV,
)


def _set_resource_environment(
    monkeypatch,
    *,
    workers=4,
    identity_map_max_bytes=8 * _GIB,
    total_sort_memory_bytes=4 * _GIB,
):
    """Set one complete resource profile and remove stale legacy input."""

    for environment_name in _RESOURCE_ENVIRONMENT_NAMES:
        monkeypatch.delenv(environment_name, raising=False)
    monkeypatch.setenv(PTG2_V3_FINALIZER_WORKERS_ENV, str(workers))
    monkeypatch.setenv(
        PTG2_V3_FINALIZER_IDENTITY_MAP_MAX_BYTES_ENV,
        str(identity_map_max_bytes),
    )
    monkeypatch.setenv(
        PTG2_V3_FINALIZER_TOTAL_SORT_MEMORY_BYTES_ENV,
        str(total_sort_memory_bytes),
    )


def _resource_contract_metadata():
    """Return the expected contract for the standard focused-test profile."""

    return {
        "contract": PTG2_V3_FINALIZER_RESOURCE_CONTRACT,
        "workers": 4,
        "identity_map_max_bytes": 8 * _GIB,
        "total_sort_memory_bytes": 4 * _GIB,
        "sort_memory_scope": "process_total_across_workers_v1",
    }


def _summary_metadata(output_directory):
    """Build the minimum valid Rust summary used by invocation tests."""

    return {
        "format": "ptg2_v3_direct_finalizer_v3",
        "storage_generation": "shared_blocks_v3",
        "cold_lookup_contract": "ptg_v3_cold_v2",
        "shared_block_layout": "dense_shared_blocks_v3",
        "source_count": 1,
        "output_directory": str(output_directory.resolve()),
        "resource_configuration": _resource_contract_metadata(),
        "price_key_map": {
            "copy_format": "postgresql_binary_copy",
            "row_count": 1,
            "dense_price_ordering": (
                "minimum_negotiated_rate_then_global_id_128_v1"
            ),
            "keys_unique_dense_contiguous": True,
            "source_ids_exact_match": True,
        },
        "dense_keys": {
            "price": {
                "count": 1,
                "ordering": "minimum_negotiated_rate_then_global_id_128_v1",
            }
        },
        "blocks": {
            "serving": {
                "artifact_record_counts": {
                    "by_code_provider_shard_v1": 1,
                    "by_code_price_page_v4": 1,
                    "provider_set_count_dictionary": 1,
                    "provider_set_codes_v3": 1,
                    "provider_set_page_v3_s2": 1,
                }
            },
            "price_dictionary": {
                "artifact_record_counts": {"by_code_price_dictionary": 1}
            },
        },
    }


def _framed_summary(summary_metadata):
    """Encode one summary using the production length-framed protocol."""

    encoded_summary = json.dumps(summary_metadata, separators=(",", ":")).encode(
        "ascii"
    )
    return (
        b"v3_finalizer_summary\t"
        + str(len(encoded_summary)).encode("ascii")
        + b"\n"
        + encoded_summary
    )


def _physical_identity():
    """Return a synthetic public-safe physical source identity."""

    return {
        "source_type": "in_network",
        "identity_kind": "logical_json_sha256_v1",
        "identity_sha256": "a" * 64,
    }


def _finalizer_inputs(tmp_path):
    """Create a complete one-source finalizer input set."""

    serving_path = tmp_path / "run.ready"
    serving_path.write_bytes(b"x" * 52)
    serving_entries = attach_v3_source_run_contract(
        [
            {
                "path": str(serving_path),
                "row_count": 1,
                "bytes": 52,
                "partition": 0,
                "partition_count": 1,
                "format": "ptg2_v3_serving_run",
                "version": 1,
            }
        ],
        source_identity=_physical_identity(),
        scanner_summary={
            "serving_run_files": 1,
            "serving_run_rows": 1,
            "serving_run_bytes": 52,
        },
        scanner_config={"serving_run_partition_count": 1},
    )
    code_path = tmp_path / "codes.ready"
    code_path.write_bytes(b"c" * 64)
    code_entries = attach_v3_dictionary_contract(
        [
            {
                "path": str(code_path),
                "row_count": 1,
                "bytes": 64,
                "format": "ptg2_v3_serving_code_dictionary",
                "version": 4,
            }
        ],
        source_identity=_physical_identity(),
        source_run_contract_sha256=serving_entries[0][
            "source_run_contract_sha256"
        ],
        scanner_summary={
            "serving_code_dictionary_files": 1,
            "serving_code_dictionary_rows": 1,
            "serving_code_dictionary_bytes": 64,
        },
    )
    price_key_map_path = tmp_path / "price-key-map.copy"
    price_key_map_path.write_bytes(b"map")
    return serving_entries, code_entries, price_key_map_path


def _successful_subprocess(stdout, invocation_by_key):
    """Return a subprocess factory that captures the finalizer command."""

    class SuccessfulProcess:
        returncode = 0

        async def communicate(self):
            """Return one successful finalizer frame."""

            return stdout, b""

    async def create_subprocess(*command_arguments, **process_options):
        """Capture the command and return the successful fake process."""

        invocation_by_key["command_arguments"] = command_arguments
        invocation_by_key["process_options"] = process_options
        return SuccessfulProcess()

    return create_subprocess


def test_finalizer_resource_configuration_is_explicit_and_process_wide(monkeypatch):
    """Bind every Rust resource argument to one validated process budget."""

    _set_resource_environment(monkeypatch)
    monkeypatch.setattr(
        finalizer_module,
        "_finite_process_memory_limit_bytes",
        lambda: 32 * _GIB,
    )

    configuration = finalizer_module._load_v3_finalizer_resource_configuration()

    assert configuration.command_arguments() == (
        "--workers",
        "4",
        "--identity-map-max-bytes",
        str(8 * _GIB),
        "--total-sort-memory-bytes",
        str(4 * _GIB),
    )
    assert configuration.contract_metadata() == _resource_contract_metadata()
    assert configuration.validation_metadata() == {
        "configured_memory_budget_bytes": 12 * _GIB,
        "sort_memory_bytes_per_worker": _GIB,
        "finite_process_memory_limit_bytes": 32 * _GIB,
        "configured_memory_limit_fraction": "3/4",
    }


@pytest.mark.parametrize(
    ("environment_name", "configured_value", "expected_error"),
    [
        (PTG2_V3_FINALIZER_WORKERS_ENV, "4.0", "positive decimal"),
        (PTG2_V3_FINALIZER_WORKERS_ENV, "65", "between 1 and 64"),
        (
            PTG2_V3_FINALIZER_IDENTITY_MAP_MAX_BYTES_ENV,
            str(63 * _MIB),
            "MiB-aligned",
        ),
        (
            PTG2_V3_FINALIZER_TOTAL_SORT_MEMORY_BYTES_ENV,
            str(63 * _MIB),
            "at least",
        ),
        (
            PTG2_V3_FINALIZER_TOTAL_SORT_MEMORY_BYTES_ENV,
            str(65 * _MIB),
            "divide evenly",
        ),
    ],
)
def test_finalizer_resource_configuration_rejects_invalid_values(
    monkeypatch,
    environment_name,
    configured_value,
    expected_error,
):
    """Reject malformed, excessive, or internally inconsistent lane values."""

    _set_resource_environment(
        monkeypatch,
        identity_map_max_bytes=64 * _MIB,
        total_sort_memory_bytes=64 * _MIB,
    )
    monkeypatch.setenv(environment_name, configured_value)
    monkeypatch.setattr(
        finalizer_module,
        "_finite_process_memory_limit_bytes",
        lambda: None,
    )

    with pytest.raises(RuntimeError, match=expected_error):
        finalizer_module._load_v3_finalizer_resource_configuration()


def test_finalizer_resource_configuration_requires_every_value(monkeypatch):
    """Refuse hidden finalizer defaults when a lane omits one required value."""

    _set_resource_environment(monkeypatch)
    monkeypatch.delenv(PTG2_V3_FINALIZER_WORKERS_ENV)

    with pytest.raises(RuntimeError, match="requires.*FINALIZER_WORKERS"):
        finalizer_module._load_v3_finalizer_resource_configuration()


def test_finalizer_resource_configuration_rejects_legacy_record_limit(monkeypatch):
    """Prevent the legacy record limit from multiplying with worker count."""

    _set_resource_environment(monkeypatch)
    monkeypatch.setenv(PTG2_V3_FINALIZER_LEGACY_MEMORY_RECORDS_ENV, "4000000")

    with pytest.raises(RuntimeError, match="multiply with worker count"):
        finalizer_module._load_v3_finalizer_resource_configuration()


def test_finalizer_resource_configuration_reserves_process_headroom(monkeypatch):
    """Leave bounded process memory for finalizer structures outside the knobs."""

    _set_resource_environment(
        monkeypatch,
        identity_map_max_bytes=8 * _GIB,
        total_sort_memory_bytes=2 * _GIB,
    )
    monkeypatch.setattr(
        finalizer_module,
        "_finite_process_memory_limit_bytes",
        lambda: 12 * _GIB,
    )

    with pytest.raises(RuntimeError, match="safe process budget"):
        finalizer_module._load_v3_finalizer_resource_configuration()


def test_finalizer_summary_must_echo_invoked_resource_configuration(tmp_path):
    """Fail publication unless Rust confirms the exact wrapper invocation."""

    expected_resources = _resource_contract_metadata()
    summary_metadata = _summary_metadata(output_directory=tmp_path)
    summary_metadata.pop("resource_configuration")
    with pytest.raises(RuntimeError, match="did not confirm"):
        validate_v3_finalizer_summary(
            summary_metadata,
            expected_resource_configuration=expected_resources,
        )

    summary_metadata["resource_configuration"] = {**expected_resources, "workers": 3}
    with pytest.raises(RuntimeError, match="did not confirm"):
        validate_v3_finalizer_summary(
            summary_metadata,
            expected_resource_configuration=expected_resources,
        )


@pytest.mark.asyncio
async def test_finalizer_invocation_passes_and_reports_resource_contract(
    tmp_path,
    monkeypatch,
):
    """Pass aggregate limits to Rust and retain them in progress/report data."""

    _set_resource_environment(monkeypatch)
    monkeypatch.setattr(
        finalizer_module,
        "_finite_process_memory_limit_bytes",
        lambda: 32 * _GIB,
    )
    monkeypatch.setattr(
        finalizer_module,
        "_ptg2_rust_scanner_binary",
        lambda: tmp_path / "ptg2_scanner",
    )
    output_directory = tmp_path / "work" / "finalized"
    invocation_by_key = {}
    subprocess_factory = _successful_subprocess(
        _framed_summary(_summary_metadata(output_directory)), invocation_by_key
    )
    monkeypatch.setattr(
        finalizer_module.asyncio,
        "create_subprocess_exec",
        subprocess_factory,
    )
    serving_entries, code_entries, price_key_map_path = _finalizer_inputs(tmp_path)

    summary_metadata = await run_v3_direct_finalizer(
        work_directory=tmp_path / "work",
        serving_run_entries=serving_entries,
        code_dictionary_entries=code_entries,
        expected_source_identities=[_physical_identity()],
        price_key_map_input=price_key_map_path,
    )

    command_arguments = invocation_by_key["command_arguments"]
    assert command_arguments[command_arguments.index("--workers") + 1] == "4"
    assert command_arguments[
        command_arguments.index("--identity-map-max-bytes") + 1
    ] == str(8 * _GIB)
    assert command_arguments[
        command_arguments.index("--total-sort-memory-bytes") + 1
    ] == str(4 * _GIB)
    assert "--memory-records" not in command_arguments
    manifest_metadata = json.loads(
        (tmp_path / "work" / "scanner-summary.json").read_text()
    )
    assert manifest_metadata["resource_configuration"] == (
        _resource_contract_metadata()
    )
    assert manifest_metadata["resource_validation"][
        "sort_memory_bytes_per_worker"
    ] == _GIB
    assert summary_metadata["resource_configuration"] == _resource_contract_metadata()
    assert summary_metadata["resource_validation"][
        "configured_memory_budget_bytes"
    ] == 12 * _GIB
