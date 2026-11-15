# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""An ordinary Source publisher reads retained claims by name without owning the retention schema."""

from uuid import uuid4

import pytest
from sqlalchemy import text

from process.entity_address_snapshot_adoption import _is_registered_snapshot
from process.nucc_retention_read_view import _CLAIM_SOURCE_SQL
from tests.provider_directory_source_postgres_support import source_component_database

pytestmark = pytest.mark.asyncio


async def test_ordinary_publisher_reads_named_claims_by_name(monkeypatch):
    async with source_component_database(monkeypatch) as component:
        schema, publisher = component.policy.schema, component.database
        assert await publisher.scalar("SELECT current_user::text") == component.publisher
        assert await publisher.scalar(_CLAIM_SOURCE_SQL) == "named"
        assert await _is_registered_snapshot(publisher, schema, ("entity_address_unified",)) is False
        claim_by_field = {"schema": schema, "generation": uuid4()}
        async with component.admin_engine.begin() as admin:
            await admin.execute(
                text(
                    "INSERT INTO hp_snapshot_retention.relation (schema_name,relation_name,generation_id) "
                    "VALUES (:schema,'entity_address_unified',:generation)"
                ),
                claim_by_field,
            )
        try:
            assert await _is_registered_snapshot(publisher, schema, ("entity_address_unified",)) is True
        finally:
            async with component.admin_engine.begin() as admin:
                await admin.execute(
                    text("DELETE FROM hp_snapshot_retention.relation WHERE schema_name=:schema"), claim_by_field
                )
