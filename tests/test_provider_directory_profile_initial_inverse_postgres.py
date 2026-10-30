# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native existing-guard transition proof; no full ten-family inverse admission is claimed."""

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from process import provider_directory_profile_initial_history as history
from tests import provider_directory_initial_inverse_postgres_support as support


@pytest.mark.asyncio
@pytest.mark.parametrize("json_columns", [False, True], ids=["migration-jsonb", "model-json-vectors"])
async def test_fresh_inverse_restores_expired_initial(monkeypatch, json_columns):
    async with support.inverse_guard_case(monkeypatch, json_columns=json_columns) as case:
        authority = await support.seed_inverse(case)
        retained = await support.immutable_state(case)
        async with case.builder.connect() as connection:
            assert await connection.scalar(
                text(
                    f'SELECT expires_at<clock_timestamp() FROM "{support.SCHEMA}".provider_directory_profile_capacity_lease_consumption '
                    "WHERE attestation_id=:id"
                ),
                {"id": case.initial_consumed["attestation_id"]},
            )
        with pytest.raises(DBAPIError, match="initial_receipt_required"):
            await support.restore(case, authority)
        async with case.builder.connect() as connection:
            previous_catalog = await support.historical._catalog(connection, support.SERVING)
        await support.migrate(case.administrator, support.SUCCESSOR)
        async with case.builder.connect() as connection:
            current_catalog = await support.historical._catalog(connection, support.SERVING)
        assert current_catalog != previous_catalog
        assert history.previous_serving_fingerprint_payload(current_catalog, schema=support.SCHEMA) == previous_catalog
        await support.restore(case, authority)
        async with case.builder.connect() as connection:
            assert await support.profile_row(connection) == case.before
            assert (
                await connection.scalar(
                    text(f"SELECT count(*) FROM \"{support.SCHEMA}\".{support.JOURNAL} WHERE phase='recovered'")
                )
                == 1
            )
        assert await support.immutable_state(case) == retained


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    ["missing_preflight", "run", "producer", "scope_owner", "expired_inverse", "writer", "metadata", "insert", "event"],
)
async def test_initial_inverse_refuses_unbound_authority(monkeypatch, fault):
    async with support.inverse_guard_case(monkeypatch) as case:
        await support.migrate(case.administrator, support.SUCCESSOR)
        authority = await support.seed_inverse(case, fault=fault)
        retained = await support.immutable_state(case)
        with pytest.raises(DBAPIError, match="initial_receipt_required"):
            await support.restore(case, authority, fault=fault)
        async with case.builder.connect() as connection:
            assert await support.profile_row(connection) == case.after
            assert (
                await connection.scalar(
                    text(f"SELECT count(*) FROM \"{support.SCHEMA}\".{support.JOURNAL} WHERE phase='recovered'")
                )
                == 0
            )
        assert await support.immutable_state(case) == retained


@pytest.mark.asyncio
async def test_stale_initial_receipt_cannot_restore_alone(monkeypatch):
    async with support.inverse_guard_case(monkeypatch) as case:
        await support.migrate(case.administrator, support.SUCCESSOR)
        retained = await support.immutable_state(case)
        with pytest.raises(DBAPIError, match="initial_receipt_required"):
            async with case.builder.begin() as connection:
                await support.replace_profile(connection, case.before)
                await connection.execute(text("SET CONSTRAINTS ALL IMMEDIATE"))
        async with case.builder.connect() as connection:
            assert await support.profile_row(connection) == case.after
        assert await support.immutable_state(case) == retained
