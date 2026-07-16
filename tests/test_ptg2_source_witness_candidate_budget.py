# See LICENSE.

from __future__ import annotations

import hashlib

from process.ptg_parts import ptg2_source_witness_codec as witness_codec
from process.ptg_parts.ptg2_source_witness_contract import (
    PTG2_V3_SOURCE_WITNESS_MAX_BUNDLE_BYTES,
    PTG2_V3_SOURCE_WITNESS_MAX_FILE_BYTES,
    SOURCE_BUNDLE_MAGIC,
    source_witness_targets,
)
from process.ptg_parts.ptg2_source_witness_selection import (
    local_source_witness_targets,
)


def test_scanner_bundle_has_a_separate_larger_intermediate_budget(
    tmp_path,
    monkeypatch,
):
    payload = SOURCE_BUNDLE_MAGIC + (b"x" * 64)
    bundle_path = tmp_path / "candidate.bin"
    bundle_path.write_bytes(payload)
    bundle_entry_by_field = {
        "path": str(bundle_path),
        "sha256": hashlib.sha256(payload).hexdigest(),
        "byte_count": len(payload),
    }
    monkeypatch.setattr(
        witness_codec,
        "PTG2_V3_SOURCE_WITNESS_MAX_BUNDLE_BYTES",
        len(payload),
    )

    assert PTG2_V3_SOURCE_WITNESS_MAX_BUNDLE_BYTES > (
        PTG2_V3_SOURCE_WITNESS_MAX_FILE_BYTES
    )
    assert (
        witness_codec._authenticated_bundle_payload(bundle_entry_by_field)
        == payload
    )


def test_local_targets_preserve_provider_backfill_and_global_bottom_k():
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

    assert global_total_target == 2_048
    assert source_witness_targets(
        occurrence_population=10,
        provider_population=10_000,
    ) == (10, 2_038, 2_048)
