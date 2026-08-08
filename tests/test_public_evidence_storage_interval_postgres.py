# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL timestamp-domain proof for the public-evidence catalog."""

from __future__ import annotations

import asyncpg
import pytest

from tests.public_evidence_storage_postgres_support import (
    connect,
    public_evidence_schema,
    quoted,
    release_parameters,
)


_ALLOWED_TIMESTAMP_EXPRESSIONS = {
    "TIMESTAMPTZ '2026-07-01 00:00:00 BC'",
    "TIMESTAMPTZ '2026-07-02 00:00:00 BC'",
    "TIMESTAMPTZ '10000-07-01 00:00:00+00'",
    "TIMESTAMPTZ '10000-07-02 00:00:00+00'",
}
_TIMESTAMP_COLUMNS = {
    "observed_start_at",
    "observed_end_at",
    "effective_start_at",
    "effective_end_at",
}


async def _insert_release_with_timestamp_expressions(
    connection,
    schema_name: str,
    timestamp_expressions: dict[str, str],
) -> None:
    """Inject only fixed test timestamps while binding every other value."""

    parameters = release_parameters("tic")
    assert set(timestamp_expressions) <= _TIMESTAMP_COLUMNS
    assert set(timestamp_expressions.values()) <= _ALLOWED_TIMESTAMP_EXPRESSIONS
    schema = quoted(schema_name)
    await connection.execute(
        f"""INSERT INTO {schema}.public_evidence_source_identity (
            identity_ref, identity_kind, content_identity_kind, content_sha256
        ) VALUES ($1, $2, $3, $4)
        ON CONFLICT (identity_ref) DO NOTHING""",
        parameters["artifact_identity_ref"],
        parameters["artifact_identity_kind"],
        parameters["artifact_content_identity_kind"],
        parameters["artifact_content_sha256"],
    )
    column_names = tuple(parameters)
    bound_values = []
    value_expressions = []
    for column_name in column_names:
        timestamp_expression = timestamp_expressions.get(column_name)
        if timestamp_expression is not None:
            value_expressions.append(timestamp_expression)
            continue
        bound_values.append(parameters[column_name])
        value_expressions.append(f"${len(bound_values)}")
    await connection.execute(
        f"INSERT INTO {schema}.public_evidence_source_release "
        f"({', '.join(column_names)}) VALUES "
        f"({', '.join(value_expressions)})",
        *bound_values,
    )


@pytest.mark.asyncio
async def test_rejects_timestamps_outside_python_canonical_years() -> None:
    """Reject BC and five-digit years that Python cannot canonicalize."""

    async with public_evidence_schema() as (
        _engine,
        database_url,
        schema_name,
        _migration,
    ):
        connection = await connect(database_url)
        try:
            invalid_timestamp_sets = (
                {
                    "observed_start_at": "TIMESTAMPTZ '2026-07-01 00:00:00 BC'",
                    "observed_end_at": "TIMESTAMPTZ '2026-07-02 00:00:00 BC'",
                    "effective_start_at": ("TIMESTAMPTZ '2026-07-01 00:00:00 BC'"),
                },
                {
                    "observed_start_at": ("TIMESTAMPTZ '10000-07-01 00:00:00+00'"),
                    "observed_end_at": ("TIMESTAMPTZ '10000-07-02 00:00:00+00'"),
                    "effective_start_at": ("TIMESTAMPTZ '10000-07-01 00:00:00+00'"),
                    "effective_end_at": ("TIMESTAMPTZ '10000-07-02 00:00:00+00'"),
                },
            )
            for timestamp_expressions in invalid_timestamp_sets:
                with pytest.raises(
                    asyncpg.CheckViolationError,
                    match="public_evidence_source_release_interval_check",
                ):
                    await _insert_release_with_timestamp_expressions(
                        connection,
                        schema_name,
                        timestamp_expressions,
                    )
        finally:
            await connection.close()
