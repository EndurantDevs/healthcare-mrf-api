# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Deterministic SQL ordering proofs for projection stage census extrema."""

from __future__ import annotations

import re

import pytest

import process.provider_directory_projection_stage as projection_stage
from tests.provider_directory_projection_materializer_context import (
    synthetic_projection_context,
)


class _CensusDatabase:
    """Capture the exact census SQL and return an empty valid census."""

    def __init__(self) -> None:
        self.statement = ""
        self.parameters: dict[str, str] = {}

    async def first(self, statement: str, **parameters):
        """Capture one census query for contract inspection."""

        self.statement = statement
        self.parameters = parameters
        return {
            "resource_count": 0,
            "resource_count_map": {},
            "first_identity": None,
            "last_identity": None,
        }


@pytest.mark.asyncio
async def test_stage_census_extrema_use_c_collation_for_both_identity_fields() -> None:
    """Both min/max identities must match Rust byte-lexical ordering."""

    context = synthetic_projection_context("ndjson")
    database = _CensusDatabase()

    await projection_stage._stage_partition_census(
        context.stage,
        context.claim,
        database,
    )

    extrema_expressions = re.findall(
        r'(?:min|max)\(ARRAY\[\s*resource_type COLLATE "C",\s*'
        r'resource_id COLLATE "C"\s*\]\)',
        database.statement,
    )
    assert len(extrema_expressions) == 2
    assert database.statement.count('resource_type COLLATE "C"') == 2
    assert database.statement.count('resource_id COLLATE "C"') == 2
    assert database.parameters == {
        "physical_projection_id": context.claim.recipe_lease.recipe.recipe_id,
        "proof_partition_id": context.claim.shard.partition_id,
    }
