# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Sparse raw-record verifier adapter for the UHC semantic benchmark."""

from __future__ import annotations

from collections.abc import Mapping
import hashlib
import json
from typing import Any

from process.uhc_provider_quarantine_raw_verifier import (
    UhcProviderQuarantineRawSource,
)


def benchmark_proof_identity(report_by_field: Mapping[str, Any]) -> str:
    """Hash semantic proof fields while excluding benchmark timings."""

    stable_proof_by_field = {
        key: report_by_field[key]
        for key in (
            "contract_id",
            "contract_version",
            "copy_format_id",
            "counters",
            "encoder_sha256",
            "evidence_count",
            "evidence_identity_set_sha256",
            "evidence_layout_set_sha256",
            "evidence_ranges",
            "fact_blocks",
            "fact_count",
            "fact_set_sha256",
            "lineage",
            "max_record_bytes",
            "quarantine_count",
            "quarantine_identity_set_sha256",
            "record_identity_set_sha256",
            "source_id",
        )
    }
    encoded = json.dumps(
        stable_proof_by_field,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return hashlib.sha256(encoded).hexdigest()


def benchmark_quarantine_source(
    arguments: Any,
) -> UhcProviderQuarantineRawSource:
    """Bind sparse verification to the benchmark's exact raw identity."""

    return UhcProviderQuarantineRawSource(
        raw_path=arguments.input,
        manifest_path=arguments.manifest,
        artifact_sha256=arguments.artifact_sha256,
        artifact_byte_count=arguments.artifact_byte_count,
        raw_contract_version=2,
        manifest_sha256=arguments.manifest_sha256,
        range_set_sha256=arguments.range_set_sha256,
        record_count=arguments.record_count,
        range_count=arguments.range_count,
        raw_producer_build_id=arguments.producer_build_id,
        source_file_id=arguments.source_file_id,
    )
