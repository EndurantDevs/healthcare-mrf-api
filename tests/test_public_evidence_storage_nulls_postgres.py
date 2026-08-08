# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL NULL-semantics proof for public-evidence release checks."""

from __future__ import annotations

import asyncpg
import pytest

from tests.public_evidence_storage_postgres_support import (
    connect,
    insert_source_release,
    public_evidence_schema,
    release_parameters_without_declared_count,
    release_parameters_without_required_binding,
)


@pytest.mark.asyncio
async def test_release_checks_reject_canonically_rehashed_null_bypasses() -> None:
    """Reject validly hashed rows that exploit SQL CHECK unknown results."""

    async with public_evidence_schema() as (
        _engine,
        database_url,
        schema_name,
        _migration,
    ):
        connection = await connect(database_url)
        try:
            invalid_cases = (
                (
                    release_parameters_without_required_binding(),
                    "public_evidence_source_release_policy_check",
                ),
                (
                    release_parameters_without_declared_count(),
                    "public_evidence_source_release_count_check",
                ),
            )
            for parameters, expected_constraint in invalid_cases:
                with pytest.raises(
                    asyncpg.CheckViolationError,
                    match=expected_constraint,
                ):
                    await insert_source_release(
                        connection,
                        schema_name,
                        "tic",
                        parameters,
                    )
        finally:
            await connection.close()
