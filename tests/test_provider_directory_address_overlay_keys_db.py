# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib
import json
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


async def _seed_repeated_unit_locations(database, schema_name):
    """Add repeated units after a double space, a Unicode space and a control character."""
    for resource_id, first_line in (
        ("repeat", "100 Main Street Suite 200  A"),
        ("narrow", "100 Main Street Suite 200\u202f A"),
        ("control", "100 Main Street Suite 200\x1cA"),
    ):
        await database.status(
            f'INSERT INTO "{schema_name}"."provider_directory_location" '
            "(source_id, resource_id, mode, addresses, first_line, second_line, city_name, state_name, postal_code) "
            "VALUES ('cms-npd', :resource_id, 'instance', CAST(:addresses AS jsonb), "
            ":first_line, '200 A', 'Austin', 'TX', '78701')",
            resource_id=resource_id,
            first_line=first_line,
            addresses='[{"use": "work", "type": "physical"}]',
        )
    location_refs = [
        "Location/kind",
        "Location/postal",
        "Location/physical",
        "Location/repeat",
        "Location/narrow",
        "Location/control",
    ]
    for table_name in ("provider_directory_practitioner_role", "provider_directory_organization_affiliation"):
        await database.status(
            f'UPDATE "{schema_name}"."{table_name}" SET location_refs = CAST(:location_refs AS jsonb)',
            location_refs=json.dumps(location_refs),
        )


async def _seed_sql_keyed_stage(database, schema_name, components, scope_by_name):
    """Seed the fixture and publish it once with the SQL key expression."""
    await _install_canonical_functions(database, schema_name)
    await _create_fixture_tables(database, schema_name, "sql_stage")
    await database.status(importer.provider_directory_address_overlay_table_sql(schema_name, "batch_stage"))
    await _seed_cms_role_location_rows(database, schema_name)
    await _seed_affiliation(database, schema_name)
    await _seed_repeated_unit_locations(database, schema_name)
    for component in components:
        await database.status(
            importer._address_overlay_component_insert_sql(
                schema_name, "sql_stage", component=component, source_ids=["cms-npd"]
            ),
            **scope_by_name,
        )


def _recorded_batch_key_tables(monkeypatch, database):
    """Batch-key on this database in two-row pages and record each key table's owner."""
    native_batch = overlay_keys._native_location_batch(importer)
    key_tables = []
    stage_table_name = importer._address_overlay_stage_table_name

    def owned_key_table(run_id):
        key_tables.append((run_id, stage_table_name(run_id)))
        return key_tables[-1][1]

    monkeypatch.setattr(importer, "_address_overlay_stage_table_name", owned_key_table)
    monkeypatch.setattr(importer, "db", database)
    monkeypatch.setattr(overlay_keys, "_native_location_batch", lambda _fhir: native_batch or _python_location_batch)
    monkeypatch.setattr(overlay_keys, "LOCATION_KEY_PAGE_ROWS", 2)
    return key_tables


@pytest.mark.asyncio
async def test_batch_keyed_locations_publish_the_same_overlay_rows(monkeypatch):
    """Keying Locations once in batches must not change one published row."""

    async with _temporary_schema(monkeypatch) as (database, schema_name):
        components = ("practitioner_role", "organization_affiliation")
        scope_by_name = {"source_ids": ["cms-npd"]}
        await _seed_sql_keyed_stage(database, schema_name, components, scope_by_name)
        key_tables = _recorded_batch_key_tables(monkeypatch, database)
        step_messages = []

        async def record_step(*, message):
            step_messages.append(message)

        with overlay_keys.build_scope("run-control", record_step):
            inserted_by_component, _seconds = await overlay_keys.insert_components(
                importer, schema_name, "batch_stage", None, ["cms-npd"], scope_by_name, components
            )

        assert inserted_by_component == {"practitioner_role": 4, "organization_affiliation": 4}
        # The double-spaced Location is batch keyed now that SQL normalizes spacing too; the Unicode-space
        # and control-character Locations stay out of the batch and publish through the SQL key.
        assert "keyed locations rows=4" in step_messages[0]
        # The control run owns the key table although the build's query scope has no run id.
        assert key_tables[0][0] == "run-control"
        assert (
            key_tables[0][1].rsplit("_", 1)[-1].startswith(importer._address_overlay_stage_owner_token("run-control"))
        )
        batch_rows = await database.all(PUBLISHED_ROWS_SQL.format(schema=schema_name, stage="batch_stage"))
        sql_rows = await database.all(PUBLISHED_ROWS_SQL.format(schema=schema_name, stage="sql_stage"))
        assert list(map(tuple, batch_rows)) == list(map(tuple, sql_rows))
        assert len(batch_rows) == 8 and all(published[1] for published in batch_rows)
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


async def _publish_sql_keyed_rows(database, schema_name, stage_name, components, scope_by_name):
    """Publish the current Locations into a fresh stage with the SQL key expression and read them back."""
    await database.status(importer.provider_directory_address_overlay_table_sql(schema_name, stage_name))
    for component in components:
        await database.status(
            importer._address_overlay_component_insert_sql(
                schema_name, stage_name, component=component, source_ids=["cms-npd"]
            ),
            **scope_by_name,
        )
    return list(map(tuple, await database.all(PUBLISHED_ROWS_SQL.format(schema=schema_name, stage=stage_name))))


@pytest.mark.asyncio
async def test_location_changed_after_key_staging_keeps_the_sql_key(monkeypatch):
    """A Location updated between key staging and the inserts must not reuse its staged key."""

    async with _temporary_schema(monkeypatch) as (database, schema_name):
        components = ("practitioner_role", "organization_affiliation")
        scope_by_name = {"source_ids": ["cms-npd"]}
        await _seed_sql_keyed_stage(database, schema_name, components, scope_by_name)
        _recorded_batch_key_tables(monkeypatch, database)
        stage_pages = overlay_keys._stage_location_key_pages

        async def stage_then_move_location(*arguments):
            keyed_count = await stage_pages(*arguments)
            await database.status(
                f'UPDATE "{schema_name}"."provider_directory_location" '
                "SET first_line = '200 Oak Street' WHERE resource_id = 'physical'"
            )
            return keyed_count

        monkeypatch.setattr(overlay_keys, "_stage_location_key_pages", stage_then_move_location)
        with overlay_keys.build_scope("run-control", None):
            await overlay_keys.insert_components(
                importer, schema_name, "batch_stage", None, ["cms-npd"], scope_by_name, components
            )

        batch_rows = list(
            map(tuple, await database.all(PUBLISHED_ROWS_SQL.format(schema=schema_name, stage="batch_stage")))
        )
        moved_rows = await _publish_sql_keyed_rows(database, schema_name, "sql_after_move", components, scope_by_name)
        before_rows = list(
            map(tuple, await database.all(PUBLISHED_ROWS_SQL.format(schema=schema_name, stage="sql_stage")))
        )
        assert batch_rows == moved_rows
        assert batch_rows != before_rows
