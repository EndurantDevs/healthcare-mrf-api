# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL seed support for the neutral v5 HTTP-410 profile."""

from __future__ import annotations

from tests.provider_directory_fhir_subset_terminal_disposition_v4_pg_support import (
    _insert_checkpoints,
    _insert_proof_shards,
    _insert_resources,
    _insert_source_and_candidate,
)
from tests.provider_directory_fhir_subset_terminal_disposition_v5_support import (
    direct_v5_inputs,
)


async def seed_direct_v5_terminal_root(scenario) -> None:
    """Seed one failed v5 root without enacting its disposition."""

    source_by_field, candidate_by_field, checkpoint_rows = direct_v5_inputs()
    await _insert_source_and_candidate(
        scenario,
        source_by_field,
        candidate_by_field,
    )
    await _insert_resources(scenario, candidate_by_field, checkpoint_rows)
    await _insert_proof_shards(
        scenario,
        source_by_field,
        candidate_by_field,
        checkpoint_rows,
    )
    await _insert_checkpoints(scenario, checkpoint_rows)
    await scenario.connection.execute(
        "SET CONSTRAINTS ALL IMMEDIATE; SET CONSTRAINTS ALL DEFERRED;"
    )


__all__ = ("seed_direct_v5_terminal_root",)
