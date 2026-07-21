# See LICENSE.

from __future__ import annotations

import hashlib

from process.ptg_parts import ptg2_source_witness_bundle as witness_bundle
from process.ptg_parts.ptg2_source_witness_contract import (
    PTG2_V3_SOURCE_WITNESS_MAX_BUNDLE_BYTES,
    PTG2_V3_SOURCE_WITNESS_MAX_FILE_BYTES,
    PTG2_V3_SOURCE_WITNESS_MAX_PART_BYTES,
    PTG2_V3_SOURCE_WITNESS_MAX_PAYLOAD_BYTES,
    SOURCE_BUNDLE_MAGIC,
    source_witness_targets,
)
from process.ptg_parts.ptg2_source_witness_selection import (
    local_source_witness_targets,
)


def test_scanner_bundle_and_logical_payload_have_independent_bounds(
    tmp_path,
    monkeypatch,
):
    bundle_payload = SOURCE_BUNDLE_MAGIC + (b"x" * 64)
    bundle_path = tmp_path / "candidate.bin"
    bundle_path.write_bytes(bundle_payload)
    bundle_entry_by_field = {
        "path": str(bundle_path),
        "sha256": hashlib.sha256(bundle_payload).hexdigest(),
        "byte_count": len(bundle_payload),
    }
    monkeypatch.setattr(
        witness_bundle,
        "PTG2_V3_SOURCE_WITNESS_MAX_BUNDLE_BYTES",
        len(bundle_payload),
    )

    assert PTG2_V3_SOURCE_WITNESS_MAX_FILE_BYTES == (
        PTG2_V3_SOURCE_WITNESS_MAX_PAYLOAD_BYTES
    )
    assert PTG2_V3_SOURCE_WITNESS_MAX_BUNDLE_BYTES == (
        PTG2_V3_SOURCE_WITNESS_MAX_PAYLOAD_BYTES
    )
    assert PTG2_V3_SOURCE_WITNESS_MAX_PART_BYTES < (
        PTG2_V3_SOURCE_WITNESS_MAX_PAYLOAD_BYTES
    )
    assert (
        witness_bundle._authenticated_bundle_payload(bundle_entry_by_field)
        == bundle_payload
    )


def test_local_targets_preserve_independent_global_bottom_k_cohorts():
    source_populations = (
        (10_000, 80),
        (2_040, 8),
        (10, 100),
    )
    occurrence_population_total = sum(
        occurrence_count
        for occurrence_count, provider_count in source_populations
    )
    provider_population_total = sum(
        provider_count
        for occurrence_count, provider_count in source_populations
    )
    global_occurrence_target, global_provider_target, global_total_target = (
        source_witness_targets(
            occurrence_population=occurrence_population_total,
            provider_population=provider_population_total,
        )
    )

    for occurrence_population, provider_population in source_populations:
        local_target_counts = local_source_witness_targets(
            {"population_count": occurrence_population},
            {"population_count": provider_population},
        )
        assert local_target_counts[0] >= min(
            occurrence_population,
            global_occurrence_target,
        )
        assert local_target_counts[1] >= min(
            provider_population,
            global_provider_target,
        )

    assert global_total_target == 10_188
    assert source_witness_targets(
        occurrence_population=10,
        provider_population=10_000,
    ) == (10, 1_000, 1_010)
