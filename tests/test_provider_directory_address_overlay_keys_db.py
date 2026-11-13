# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib
from unittest.mock import AsyncMock

import pytest

from tests.test_provider_directory_practitioner_address_overlay_db import (
    _create_fixture_tables,
    _install_canonical_functions,
    _seed_cms_role_location_rows,
    _temporary_schema,
)

importer = importlib.import_module("process.provider_directory_fhir")
overlay_keys = importlib.import_module("process.provider_directory_address_overlay_keys")
address_canon = importlib.import_module("process.ext.address_canon")

PUBLISHED_ROWS_SQL = (
    "SELECT source_record_id, address_key::text, first_line, city_name, state_code, postal_code "
    'FROM "{schema}"."{stage}" ORDER BY source_record_id'
)


def _python_location_batch(addresses):
    """Stand in for the native batch with the parity-tested Python key."""
    return [
        (str(address_key) if (address_key := address_canon.address_key_v1(*address)) else None, None, None)
        for address in addresses
    ]


async def _seed_affiliation(database, schema_name):
    await database.status(
        f'CREATE TABLE "{schema_name}"."provider_directory_organization" ('
        "source_id varchar, resource_id varchar, npi bigint, active boolean, "
        "address_json jsonb, telecom jsonb, updated_at timestamp)"
    )
    await database.status(
        f'CREATE TABLE "{schema_name}"."provider_directory_organization_affiliation" ('
        "source_id varchar, resource_id varchar, active boolean, organization_ref varchar, "
        "participating_organization_ref varchar, location_refs jsonb, healthcare_service_refs jsonb, "
        "telecom jsonb, last_seen_run_id varchar, updated_at timestamp)"
    )
    await database.status(
        f'INSERT INTO "{schema_name}"."provider_directory_organization" '
        "VALUES ('cms-npd', 'org-1', 1234567890, true, '[]'::jsonb, '[]'::jsonb, now())"
    )
    await database.status(
        f'INSERT INTO "{schema_name}"."provider_directory_organization_affiliation" '
        "VALUES ('cms-npd', 'aff-1', true, 'Organization/org-1', NULL, "
        "'[\"Location/physical\"]'::jsonb, '[]'::jsonb, '[]'::jsonb, 'run-current', now())"
    )


@pytest.mark.asyncio
async def test_batch_keyed_locations_publish_the_same_overlay_rows(monkeypatch):
    """Keying Locations once in batches must not change one published row."""

    async with _temporary_schema(monkeypatch) as (database, schema_name):
        await _install_canonical_functions(database, schema_name)
        await _create_fixture_tables(database, schema_name, "sql_stage")
        await database.status(importer.provider_directory_address_overlay_table_sql(schema_name, "batch_stage"))
        await _seed_cms_role_location_rows(database, schema_name)
        await _seed_affiliation(database, schema_name)
        components = ("practitioner_role", "organization_affiliation")
        scope_by_name = {"source_ids": ["cms-npd"]}
        for component in components:
            await database.status(
                importer._address_overlay_component_insert_sql(
                    schema_name, "sql_stage", component=component, source_ids=["cms-npd"]
                ),
                **scope_by_name,
            )
        native_batch = overlay_keys._native_location_batch(importer)
        key_tables = []
        stage_table_name = importer._address_overlay_stage_table_name

        def owned_key_table(run_id):
            key_tables.append((run_id, stage_table_name(run_id)))
            return key_tables[-1][1]

        monkeypatch.setattr(importer, "_address_overlay_stage_table_name", owned_key_table)
        monkeypatch.setattr(importer, "db", database)
        monkeypatch.setattr(
            overlay_keys, "_native_location_batch", lambda _fhir: native_batch or _python_location_batch
        )
        monkeypatch.setattr(overlay_keys, "LOCATION_KEY_PAGE_ROWS", 2)
        step_messages = []

        async def record_step(*, message):
            step_messages.append(message)

        with overlay_keys.build_scope("run-control", record_step):
            inserted_by_component, _seconds = await overlay_keys.insert_components(
                importer, schema_name, "batch_stage", None, ["cms-npd"], scope_by_name, components
            )

        assert inserted_by_component == {"practitioner_role": 1, "organization_affiliation": 1}
        assert "keyed locations rows=3" in step_messages[0]
        # The control run owns the key table although the build's query scope has no run id.
        assert key_tables[0][0] == "run-control"
        assert (
            key_tables[0][1].rsplit("_", 1)[-1].startswith(importer._address_overlay_stage_owner_token("run-control"))
        )
        batch_rows = await database.all(PUBLISHED_ROWS_SQL.format(schema=schema_name, stage="batch_stage"))
        sql_rows = await database.all(PUBLISHED_ROWS_SQL.format(schema=schema_name, stage="sql_stage"))
        assert list(map(tuple, batch_rows)) == list(map(tuple, sql_rows))
        assert len(batch_rows) == 2 and all(published[1] for published in batch_rows)
        leftover_tables = await database.scalar(
            "SELECT count(*) FROM pg_tables WHERE schemaname = :schema_name AND tablename LIKE :stage_pattern",
            schema_name=schema_name,
            stage_pattern="provider_directory_address_overlay_stage%",
        )
        assert leftover_tables == 0


@pytest.mark.asyncio
async def test_overlay_without_native_keys_keeps_the_sql_expression(monkeypatch):
    """No native module, or an admitted build, must leave the templates on addr_key_v1."""

    monkeypatch.setattr(overlay_keys, "_native_location_batch", lambda _fhir: None)
    status = AsyncMock(return_value="INSERT 0 0")
    monkeypatch.setattr(importer.db, "status", status)

    await overlay_keys.insert_components(
        importer, "mrf", "overlay_stage", None, ["source-a"], {}, ("practitioner_role",)
    )

    insert_sql = status.await_args.args[0]
    assert status.await_count == 1
    assert overlay_keys.EMPTY_LOCATION_KEYS_SQL in insert_sql
    assert '"mrf".addr_key_v1(' in insert_sql


@pytest.mark.asyncio
async def test_formatted_label_is_compared_after_its_constant_metadata(monkeypatch):
    """A fresh row differs on metadata first, so the renderer runs once, in SET."""

    status = AsyncMock(return_value="UPDATE 0")
    monkeypatch.setattr(importer.db, "status", status)

    await importer._backfill_address_overlay_stage_formatted_addresses("mrf", '"mrf"."overlay_stage"')

    predicate_sql = status.await_args.args[0].split("WHERE", 1)[1]
    assert predicate_sql.index("ROW(stage_row.formatted_address_version") < predicate_sql.index(
        "stage_row.formatted_address IS DISTINCT FROM"
    )
    assert predicate_sql.count("addr_formatted_address_v2(") == 1
