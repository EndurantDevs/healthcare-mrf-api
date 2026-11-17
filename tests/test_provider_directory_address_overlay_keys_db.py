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
)

importer = importlib.import_module("process.provider_directory_fhir")
overlay_keys = importlib.import_module("process.provider_directory_address_overlay_keys")

PUBLISHED_ROWS_SQL = (
    "SELECT source_record_id, address_key::text, first_line, city_name, state_code, postal_code "
    'FROM "{schema}"."{stage}" ORDER BY source_record_id'
)


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


@pytest.mark.asyncio
async def test_batch_keyed_locations_publish_the_same_overlay_rows(monkeypatch):
    """Compare scoped keys and scratch cleanup through the prepared native batch."""
    from tests import provider_directory_address_batch_support as support

    async with support.location_overlay_fixture(monkeypatch) as (database, schema_name, components):
        scope_by_name = {"source_ids": ["cms-npd"]}
        stage_table_name = importer._address_overlay_stage_table_name
        owner_runs = []

        def remember_stage_owner(run_id):
            owner_runs.append(run_id)
            return stage_table_name(run_id)

        monkeypatch.setattr(importer, "_address_overlay_stage_table_name", remember_stage_owner)
        with overlay_keys.build_scope("run-control", None):
            async with support.batch.scope(importer, schema_name, None, ["cms-npd"], components):
                inserted_by_component, _seconds = await overlay_keys.insert_components(
                    importer, schema_name, "batch_stage", None, ["cms-npd"], scope_by_name, components
                )

        assert inserted_by_component == {"practitioner_role": 4, "organization_affiliation": 4}
        assert owner_runs[:2] == ["run-control", "run-control"]
        batch_rows = await database.all(PUBLISHED_ROWS_SQL.format(schema=schema_name, stage="batch_stage"))
        sql_rows = await database.all(PUBLISHED_ROWS_SQL.format(schema=schema_name, stage="sql_stage"))
        assert list(map(tuple, batch_rows)) == list(map(tuple, sql_rows))
        assert len(batch_rows) == 8 and all(published[1] for published in batch_rows)
        assert await support.remaining_batch_tables(database, schema_name) == 0


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
