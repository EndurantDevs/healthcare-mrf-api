# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import copy
from pathlib import Path
import resource
from types import SimpleNamespace

import pytest

from process.ptg_parts import ptg2_shared_finalize as finalizer
from tests.ptg2_shared_finalize_test_support import (
    _canonical_sha256,
    _contracted_entries,
    _entry,
    _identity,
)


def _serving_entries(tmp_path: Path, *, partitions: int = 1):
    raw_entries = [
        _entry(
            tmp_path / f"run-{partition}.ready",
            row_count=1,
            bytes=finalizer.PTG2_V3_SERVING_RUN_RECORD_BYTES,
            partition=partition,
            partition_count=partitions,
            format=finalizer.PTG2_V3_SERVING_RUN_FORMAT,
            version=finalizer.PTG2_V3_SERVING_RUN_VERSION,
        )
        for partition in range(partitions)
    ]
    return _contracted_entries(
        raw_entries,
        _identity("a"),
        partition_count=partitions,
    )


def _rehash_contract(entries: list[dict[str, object]]) -> None:
    digest = _canonical_sha256(entries[0]["source_run_contract"])
    for entry in entries:
        entry["source_run_contract_sha256"] = digest


def test_validated_entries_require_unique_nonempty_files(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="at least one"):
        finalizer._validated_entries([], label="artifact")
    with pytest.raises(RuntimeError, match="missing path"):
        finalizer._validated_entries([{}], label="artifact")
    with pytest.raises(RuntimeError, match="missing or empty"):
        finalizer._validated_entries(
            [{"path": str(tmp_path / "missing")}],
            label="artifact",
        )

    path = tmp_path / "same"
    path.write_bytes(b"x")
    with pytest.raises(RuntimeError, match="repeats path"):
        finalizer._validated_entries(
            [{"path": str(path)}, {"path": str(path)}],
            label="artifact",
        )


@pytest.mark.parametrize(
    ("value", "message"),
    (
        (True, "invalid"),
        ("1", "invalid"),
        (-1, "negative"),
        (1 << 65, "oversized"),
    ),
)
def test_finalizer_integer_contract_rejects_noncanonical_values(
    value: object,
    message: str,
) -> None:
    with pytest.raises(RuntimeError, match=message):
        finalizer._required_non_negative_integer(value, field_name="count")


@pytest.mark.parametrize(
    ("updates", "message"),
    (
        ({"format": "wrong"}, "incompatible format"),
        ({"version": 2}, "incompatible version"),
        ({"row_count": 0}, "does not match"),
        ({"bytes": 0}, "does not match"),
        ({"bytes": 51}, "does not match"),
    ),
)
def test_finalizer_file_metadata_binds_format_version_rows_and_bytes(
    tmp_path: Path,
    updates: dict[str, object],
    message: str,
) -> None:
    entry = _entry(
        tmp_path / "artifact",
        row_count=1,
        bytes=52,
        format=finalizer.PTG2_V3_SERVING_RUN_FORMAT,
        version=finalizer.PTG2_V3_SERVING_RUN_VERSION,
    )
    entry.update(updates)
    with pytest.raises(RuntimeError, match=message):
        finalizer._validate_file_metadata(
            entry,
            label="serving-run",
            expected_format=finalizer.PTG2_V3_SERVING_RUN_FORMAT,
            expected_version=finalizer.PTG2_V3_SERVING_RUN_VERSION,
        )


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        (
            lambda entry, summary, config: config.update(
                serving_run_partition_count=0
            ),
            "reported no serving-run partitions",
        ),
        (
            lambda entry, summary, config: entry.update(partition_count=2),
            "partition metadata disagrees",
        ),
        (
            lambda entry, summary, config: entry.update(partition=1),
            "partition metadata disagrees",
        ),
        (
            lambda entry, summary, config: entry.update(row_count=2),
            "row and byte counts",
        ),
        (
            lambda entry, summary, config: entry.update(
                source_type="different"
            ),
            "conflicting physical identity",
        ),
        (
            lambda entry, summary, config: summary.update(
                serving_run_files=2
            ),
            "aggregate summary",
        ),
        (
            lambda entry, summary, config: summary.update(
                serving_run_rows=2
            ),
            "aggregate summary",
        ),
        (
            lambda entry, summary, config: summary.update(
                serving_run_bytes=104
            ),
            "aggregate summary",
        ),
    ),
)
def test_source_run_contract_rejects_independent_scanner_drift(
    tmp_path: Path,
    mutation: object,
    message: str,
) -> None:
    entry = _entry(
        tmp_path / "run.ready",
        row_count=1,
        bytes=52,
        partition=0,
        partition_count=1,
        format=finalizer.PTG2_V3_SERVING_RUN_FORMAT,
        version=finalizer.PTG2_V3_SERVING_RUN_VERSION,
    )
    scanner_summary_by_field = {
        "serving_run_files": 1,
        "serving_run_rows": 1,
        "serving_run_bytes": 52,
    }
    scanner_config_by_field = {"serving_run_partition_count": 1}
    mutation(entry, scanner_summary_by_field, scanner_config_by_field)

    with pytest.raises(RuntimeError, match=message):
        finalizer.attach_v3_source_run_contract(
            [entry],
            source_identity=_identity("a"),
            scanner_summary=scanner_summary_by_field,
            scanner_config=scanner_config_by_field,
        )


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        (
            lambda entries: entries[0].update(
                identity_sha256="b" * 64
            ),
            "not part of the complete physical input set",
        ),
        (
            lambda entries: entries[0].update(partition_count=0),
            "partition metadata is invalid",
        ),
        (
            lambda entries: entries[0].update(partition=1),
            "partition metadata is invalid",
        ),
        (
            lambda entries: entries[0].update(row_count=2),
            "row and byte counts",
        ),
        (
            lambda entries: entries[0].update(
                source_run_file_sha256="00" * 32
            ),
            "content digest",
        ),
        (
            lambda entries: entries[0].update(
                source_run_contract_sha256="00" * 32
            ),
            "source contract digest is invalid",
        ),
        (
            lambda entries: entries[0].update(source_run_contract=[]),
            "must appear exactly once",
        ),
        (
            lambda entries: entries[0].pop("source_run_contract"),
            "missing a complete source-run contract",
        ),
    ),
)
def test_prepared_serving_entries_reject_untrusted_entry_bindings(
    tmp_path: Path,
    mutation: object,
    message: str,
) -> None:
    entries = _serving_entries(tmp_path)
    mutation(entries)
    with pytest.raises(RuntimeError, match=message):
        finalizer._prepare_serving_entries(
            entries,
            expected_source_identities=[_identity("a")],
        )


def test_prepared_serving_entries_require_every_expected_source(
    tmp_path: Path,
) -> None:
    entries = _serving_entries(tmp_path)
    with pytest.raises(RuntimeError, match="complete dense source keys"):
        finalizer._prepare_serving_entries(
            entries,
            expected_source_identities=[_identity("a"), _identity("b")],
        )


@pytest.mark.parametrize(
    ("contract_update", "message"),
    (
        ({"unexpected": True}, "fields are incompatible"),
        ({"version": 2}, "version is incompatible"),
        ({"partition_count": 2}, "incomplete partition coverage"),
        ({"partition_rows": []}, "incomplete partition coverage"),
        ({"partition_rows": [2]}, "partition rows"),
        ({"file_count": 2}, "aggregates"),
        ({"row_count": 2}, "aggregates"),
        ({"byte_count": 104}, "aggregates"),
        ({"files": None}, "missing file digests"),
        ({"files": [None]}, "invalid file digest"),
        (
            {
                "files": [
                    {
                        "partition": 0,
                        "row_count": 1,
                        "bytes": 52,
                        "sha256": "a" * 64,
                        "unexpected": True,
                    }
                ]
            },
            "file fields are incompatible",
        ),
        (
            {
                "files": [
                    {
                        "partition": 0,
                        "row_count": 1,
                        "bytes": 52,
                        "sha256": "00" * 32,
                    }
                ]
            },
            "file digests do not match",
        ),
    ),
)
def test_prepared_serving_entries_recompute_complete_source_contract(
    tmp_path: Path,
    contract_update: dict[str, object],
    message: str,
) -> None:
    entries = _serving_entries(tmp_path)
    entries[0]["source_run_contract"].update(contract_update)
    _rehash_contract(entries)

    with pytest.raises(RuntimeError, match=message):
        finalizer._prepare_serving_entries(
            entries,
            expected_source_identities=[_identity("a")],
        )


def test_source_contract_index_rejects_missing_noncanonical_and_duplicate_ids() -> None:
    valid_contract_by_field = {
        "source_identity": _identity("a"),
        "source_key": 0,
        "contract_sha256": "11" * 32,
    }
    with pytest.raises(RuntimeError, match="missing its physical identity"):
        finalizer._source_contracts_by_identity([{"source_identity": None}])
    with pytest.raises(RuntimeError, match="not canonical"):
        finalizer._source_contracts_by_identity(
            [
                {
                    **valid_contract_by_field,
                    "source_identity": {
                        **_identity("a"),
                        "source_type": "IN_NETWORK",
                    },
                }
            ]
        )
    with pytest.raises(RuntimeError, match="repeat physical identity"):
        finalizer._source_contracts_by_identity(
            [valid_contract_by_field, valid_contract_by_field]
        )


@pytest.mark.parametrize(
    ("stdout", "message"),
    (
        (b"no-newline", "incomplete frame header"),
        (b"bad\n{}", "invalid frame header"),
        (b"wrong\t2\n{}", "unexpected record kind"),
        (b"v3_finalizer_summary\t1\n{}", "unexpected record kind"),
        (b"v3_finalizer_summary\t9\n{}", "truncated summary"),
        (b"v3_finalizer_summary\t2\n[]", "incompatible summary"),
        (b"v3_finalizer_summary\t2\n{x", "invalid JSON"),
    ),
)
def test_finalizer_stdout_frame_rejects_ambiguous_or_partial_output(
    stdout: bytes,
    message: str,
) -> None:
    with pytest.raises(RuntimeError, match=message):
        finalizer.parse_v3_finalizer_stdout(stdout)


def _resource_contract() -> dict[str, object]:
    return {
        "contract": finalizer.PTG2_V3_FINALIZER_RESOURCE_CONTRACT,
        "workers": 2,
        "identity_map_max_bytes": 16,
        "total_sort_memory_bytes": 32,
        "sort_memory_scope": finalizer._FINALIZER_SORT_MEMORY_SCOPE,
    }


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        (lambda value: value.pop("workers"), "incomplete"),
        (lambda value: value.update(contract="wrong"), "incompatible"),
        (lambda value: value.update(sort_memory_scope="wrong"), "incompatible"),
        (lambda value: value.update(workers=True), "invalid workers"),
        (lambda value: value.update(identity_map_max_bytes=0), "invalid identity"),
        (lambda value: value.update(total_sort_memory_bytes="32"), "invalid total"),
    ),
)
def test_finalizer_resource_contract_is_exact_and_positive(
    mutation: object,
    message: str,
) -> None:
    contract = _resource_contract()
    mutation(contract)
    with pytest.raises(RuntimeError, match=message):
        finalizer._validated_finalizer_resource_contract(
            contract, label="resource contract"
        )


def test_finite_process_limit_uses_smallest_cgroup_or_address_space(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    values = ("", "max", "not-a-number", "4096")
    paths = []
    for index, value in enumerate(values):
        path = tmp_path / f"limit-{index}"
        path.write_text(value)
        paths.append(path)
    monkeypatch.setattr(finalizer, "_CGROUP_MEMORY_LIMIT_PATHS", tuple(paths))
    monkeypatch.setattr(
        finalizer.resource,
        "getrlimit",
        lambda _kind: (2048, resource.RLIM_INFINITY),
    )

    assert finalizer._finite_process_memory_limit_bytes() == 2048


def test_finalizer_process_usage_and_output_high_water_marks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / "output"
    output.mkdir()
    (output / "one").write_bytes(b"123")
    (output / "two").write_bytes(b"45")
    assert finalizer._finalizer_output_usage(output, process_id=7) == (5, 2)

    monkeypatch.setattr(finalizer.sys, "platform", "darwin")
    monkeypatch.setattr(
        finalizer.subprocess,
        "run",
        lambda *_args, **_kwargs: SimpleNamespace(stdout="01:02.500"),
    )
    assert finalizer._finalizer_process_usage(7) == (62_500, 0)


@pytest.mark.asyncio
async def test_finalizer_progress_reports_each_monotonic_resource(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    stop = asyncio.Event()
    stop.set()
    monkeypatch.setattr(
        finalizer, "_finalizer_output_usage", lambda *_args, **_kwargs: (8, 2)
    )
    monkeypatch.setattr(
        finalizer, "_finalizer_process_usage", lambda _pid: (5, 7)
    )
    resource_samples: list[tuple[str, int]] = []

    await finalizer._monitor_finalizer_output(
        output_directory=tmp_path,
        process_id=7,
        stop=stop,
        progress_callback=lambda name, value: resource_samples.append(
            (name, value)
        ),
        poll_seconds=0.01,
    )

    assert resource_samples == [
        ("artifact_bytes", 8),
        ("artifact_files", 2),
        ("process_cpu_milliseconds", 5),
        ("process_io_bytes", 7),
    ]
