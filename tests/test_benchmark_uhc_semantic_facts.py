# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from argparse import Namespace
import hashlib
from pathlib import Path

import pytest

from process.uhc_semantic_build_store import UHC_SEMANTIC_COPY_COLUMNS
from support.benchmark_uhc_semantic_facts import (
    _native_command,
    _proof_identity,
    _public_result,
    _quoted,
)


def _arguments() -> Namespace:
    return Namespace(
        native=Path("/tmp/uhc_semantic_facts"),
        input=Path("/tmp/raw.json"),
        manifest=Path("/tmp/manifest.json"),
        artifact_sha256=hashlib.sha256(b"artifact").hexdigest(),
        artifact_byte_count=123,
        manifest_sha256=hashlib.sha256(b"manifest").hexdigest(),
        range_set_sha256=hashlib.sha256(b"ranges").hexdigest(),
        record_count=20,
        range_count=4,
        source_file_id=hashlib.sha256(b"source").hexdigest(),
        source_binding_id="benchmark",
        collection_kind="provider_membership",
        native_workers=4,
    )


def test_native_command_passes_every_database_bound_identity() -> None:
    arguments = _arguments()
    encoder_sha256 = hashlib.sha256(b"encoder").hexdigest()

    command, identity = _native_command(arguments, encoder_sha256)

    assert command[0] == str(arguments.native)
    for flag in (
        "--artifact-byte-count",
        "--record-count",
        "--range-count",
        "--source-file-id",
        "--source-binding-id",
        "--collection-kind",
        "--workers",
    ):
        assert flag in command
    assert identity.encoder_sha256 == encoder_sha256
    assert identity.raw_range_count == 4
    assert len(UHC_SEMANTIC_COPY_COLUMNS) == 11


def test_benchmark_identifiers_fail_closed() -> None:
    assert _quoted("mrf_benchmark") == '"mrf_benchmark"'
    with pytest.raises(ValueError, match="unsafe"):
        _quoted("mrf; drop schema")


def test_proof_identity_ignores_timings_but_binds_semantics() -> None:
    report_by_field = {
        "contract_id": "contract",
        "contract_version": 2,
        "copy_format_id": "copy",
        "counters": {"raw_provider_records": 1},
        "encoder_sha256": "a" * 64,
        "evidence_count": 1,
        "evidence_identity_set_sha256": "b" * 64,
        "evidence_layout_set_sha256": "c" * 64,
        "evidence_ranges": [],
        "fact_blocks": [],
        "fact_count": 1,
        "fact_set_sha256": "d" * 64,
        "lineage": {"source_file_id": "e" * 64},
        "max_record_bytes": 1024,
        "record_identity_set_sha256": "f" * 64,
        "source_id": "source",
        "elapsed_seconds": 1.0,
    }
    timing_change_by_field = {
        **report_by_field,
        "elapsed_seconds": 99.0,
    }
    fact_change_by_field = {
        **report_by_field,
        "fact_set_sha256": "0" * 64,
    }

    assert _proof_identity(report_by_field) == _proof_identity(
        timing_change_by_field
    )
    assert _proof_identity(report_by_field) != _proof_identity(
        fact_change_by_field
    )


def test_public_result_removes_monotonic_clock_values() -> None:
    benchmark_result_by_field = {
        "trial": 1,
        "landing_started": 10.0,
        "landing_finished": 11.0,
        "full_finished": 12.0,
        "fact_set_sha256": "a" * 64,
    }

    assert _public_result(benchmark_result_by_field) == {
        "trial": 1,
        "fact_set_sha256": "a" * 64,
    }
