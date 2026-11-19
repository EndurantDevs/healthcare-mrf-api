# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Address follows published compact views using the same logical named-edition contract."""

from contextlib import asynccontextmanager

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import create_async_engine

from process import provider_directory_compact_edition as edition_layout
from process import provider_directory_compact_payload as compact
from process.entity_address_cutover_contract import postgres_sqlstate
from process.provider_directory_generic_candidate import RESOURCE_COLUMNS
from process.provider_directory_named_edition import HEADER
from tests.provider_directory_entities_postgres_support import _database_url
from tests.test_entity_address_named_edition_postgres import (
    _assert_named_followup_through_cutover,
    _assert_replacement_keeps_named_peer,
    _named_edition,
)

pytestmark = pytest.mark.asyncio
RESOURCE = "provider_directory_dataset_resource"


async def _logical_rows(connection, edition):
    return [
        dict(row)
        for row in (
            await connection.execute(
                text(
                    f"SELECT dataset_id,resource_type,resource_id,payload_hash,payload_json::jsonb AS payload_json,"
                    f'acquired_resource_sha256 FROM "{edition}".{RESOURCE} ORDER BY resource_type,resource_id'
                )
            )
        ).mappings()
    ]


async def _install_compact_view(connection, edition, dataset_id):
    """COPY identical rows into real backing tables before replacing the plain fixture relation."""
    original = await _logical_rows(connection, edition)
    assert original and all(set(row) == set(RESOURCE_COLUMNS) for row in original)
    tables = compact.CompactPayloadTables(
        edition, edition_layout.OUTPUT_ALIASES, edition_layout.REFERENCE_SETS, edition_layout.BODY_BLOCKS
    )
    for statement in compact.compact_payload_ddl(tables):
        await connection.execute(text(statement))
    driver = (await connection.get_raw_connection()).driver_connection
    assert await compact.append_compact_payload_batch(
        driver, tables, original, dataset_id=dataset_id, batch_ordinal=1, format_id=compact.FORMAT
    ) == {"rows": len(original), "is_replay": False}
    await connection.execute(text(f'DROP TABLE "{edition}".{RESOURCE}'))
    await connection.execute(text(f'CREATE VIEW "{edition}".{RESOURCE} AS ' + compact.compact_resource_sql(tables)))
    await connection.execute(text(f'CREATE TABLE "{edition}".{edition_layout.FORMAT_TABLE}(format_id text NOT NULL)'))
    await connection.execute(
        text(f'INSERT INTO "{edition}".{edition_layout.FORMAT_TABLE} VALUES (:format)'), {"format": compact.FORMAT}
    )
    assert await edition_layout.compact_edition_tables(driver, edition) == tables
    assert await _logical_rows(connection, edition) == original


async def _assert_published_writes_denied(case, edition):
    for relation in (
        HEADER,
        edition_layout.OUTPUT_ALIASES,
        edition_layout.REFERENCE_SETS,
        edition_layout.BODY_BLOCKS,
        edition_layout.FORMAT_TABLE,
    ):
        with pytest.raises(DBAPIError) as denied:
            await case.database.status(f'DELETE FROM "{edition}".{relation} WHERE false')
        assert postgres_sqlstate(denied.value) == "42501"
    with pytest.raises(DBAPIError) as denied:
        await case.database.status(f'CREATE TABLE "{edition}".unexpected_write(id integer)')
    assert postgres_sqlstate(denied.value) == "42501"


@asynccontextmanager
async def _compact_named_edition(case, dataset_id, endpoint_id, *, legacy_dataset_id):
    administrator = create_async_engine(_database_url())
    try:
        async with _named_edition(case, dataset_id, endpoint_id, legacy_dataset_id=legacy_dataset_id) as edition:
            async with administrator.begin() as connection:
                await _install_compact_view(connection, edition, dataset_id)
                await connection.execute(text(f'ALTER TABLE "{edition}".{HEADER} OWNER TO CURRENT_USER'))
                await connection.execute(text(f'ALTER SCHEMA "{edition}" OWNER TO CURRENT_USER'))
                await connection.execute(
                    text(f'REVOKE ALL ON ALL TABLES IN SCHEMA "{edition}" FROM PUBLIC,"{case.role}"')
                )
                await connection.execute(text(f'REVOKE ALL ON SCHEMA "{edition}" FROM PUBLIC,"{case.role}"'))
                await connection.execute(text(f'GRANT USAGE ON SCHEMA "{edition}" TO "{case.role}"'))
                await connection.execute(text(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{edition}" TO "{case.role}"'))
            await _assert_published_writes_denied(case, edition)
            yield edition
            await _assert_published_writes_denied(case, edition)
    finally:
        await administrator.dispose()


async def test_compact_named_followup_runs_overlay_and_partial_address_through_cutover(monkeypatch):
    await _assert_named_followup_through_cutover(monkeypatch, _compact_named_edition)


async def test_published_replacement_keeps_a_compact_named_peer(monkeypatch):
    await _assert_replacement_keeps_named_peer(monkeypatch, _compact_named_edition)
