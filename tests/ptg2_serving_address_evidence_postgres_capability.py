# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from api.ptg2_geo_policy import is_provider_address_geo_capability_available
from tests.ptg2_serving_address_evidence_postgres_support import (
    ZCTA5_ZIP_INDEX_NAME,
    _temporary_schema,
)


@pytest.mark.asyncio
async def test_geo_capability_probe_requires_usable_zcta_zip_index():
    async with _temporary_schema() as (database, schema):
        async with database.session() as session:
            assert isinstance(session, AsyncSession)
            assert await is_provider_address_geo_capability_available(
                session,
                schema_name=schema,
                reference_schema=schema,
            )

        await database.status(
            f"DROP INDEX {schema}.{ZCTA5_ZIP_INDEX_NAME}"
        )

        async with database.session() as session:
            assert not await is_provider_address_geo_capability_available(
                session,
                schema_name=schema,
                reference_schema=schema,
            )
