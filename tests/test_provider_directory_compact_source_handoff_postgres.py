# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native physical-key reduction and compact heap finalization boundaries."""

from types import SimpleNamespace

import asyncpg
import pytest

from db.plan_release_storage import storage_relation
from process import provider_directory_compact_payload as compact
from process import provider_directory_compact_source_completion as compact_completion
from process import provider_directory_compact_source_storage as layout
from process import provider_directory_generic_source_storage as storage
from tests.test_provider_directory_compact_completion_postgres import _append, _candidate, _complete, _payloads, _stored
from tests.test_provider_directory_compact_intake_postgres import _prepare
from tests.test_provider_directory_compact_intake_postgres import intake_case as intake_case
from tests.test_provider_directory_compact_payload import resource
from tests.test_provider_directory_compact_payload_postgres import storage as payload_storage

pytestmark = pytest.mark.asyncio


async def test_unique_keys_do_not_reconstruct_json_during_divergence_detection():
    async with payload_storage() as (driver, tables):
        async with driver.transaction():
            await compact.append_compact_payload_batch(
                driver, tables, [resource("unique")], dataset_id="dataset", batch_ordinal=1, format_id=compact.FORMAT
            )
            # An invalid body would fail reconstruction; key-only discovery must not read it.
            await driver.execute("UPDATE compact_test.blocks SET bodies='[0]'::jsonb")
            with pytest.raises(asyncpg.InvalidTextRepresentationError):
                async with driver.transaction():
                    await driver.fetch(compact.compact_resource_sql(tables))
            binding_by_field = {
                "resets": {},
                "tables": {
                    root: {"name": getattr(tables, field)}
                    for root, field in (
                        ("resource_aliases", "aliases"),
                        ("resource_reference_sets", "reference_sets"),
                        ("resource_body_blocks", "body_blocks"),
                    )
                },
            }
            conflicts = await compact_completion._conflicting_keys(
                driver, SimpleNamespace(schema="compact_test"), binding_by_field
            )
            assert await driver.fetchval(f"SELECT count(*) FROM {conflicts}") == 0


async def test_compact_finalization_uses_native_heap_bounds_and_preserves_views(intake_case):
    case = await _candidate(intake_case, "finalization")
    await _prepare(case, payload_format=compact.FORMAT)
    await _append(case, _payloads())
    prepared = await _complete(case)
    before = await _stored(case, prepared)
    async with case.connection.transaction():
        await layout.prepare_final_tables(case.connection, SimpleNamespace(schema="source_schema"), prepared)
    assert await _stored(case, prepared) == before
    assert len(before) == 7
    aliases = storage_relation("source_schema", prepared["tables"]["resource_aliases"]["name"])
    columns = (*compact._ALIAS_COLUMNS, "npi_text")
    fields = ",".join(columns)
    values = ",".join("'wrong'" if column == "dataset_id" else column for column in columns)
    with pytest.raises(asyncpg.CheckViolationError):
        async with case.connection.transaction():
            await case.connection.execute(f"INSERT INTO {aliases} ({fields}) SELECT {values} FROM {aliases} LIMIT 1")
    assert await _stored(case, prepared) == before
