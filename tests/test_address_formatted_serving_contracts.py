# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib
from unittest.mock import AsyncMock

import pytest

from api.endpoint import npi

unified = importlib.import_module("process.entity_address_unified")
directory = importlib.import_module("process.provider_directory_fhir")


FORMATTED_COLUMNS = {
    "formatted_address",
    "formatted_address_version",
    "formatted_address_source",
}


def test_overlay_schema_separates_source_and_archive_columns():
    storage_columns = set(directory._provider_directory_address_overlay_columns())
    source_columns = set(
        directory._provider_directory_address_overlay_source_columns()
    )
    table_sql = directory.provider_directory_address_overlay_table_sql("mrf")
    alter_sql = directory.address_overlay_formatted_address_columns_sql("mrf")

    assert FORMATTED_COLUMNS <= storage_columns
    assert FORMATTED_COLUMNS.isdisjoint(source_columns)
    assert "formatted_address varchar" in table_sql
    assert "formatted_address_version smallint" in table_sql
    assert "formatted_address_source varchar(32)" in table_sql
    assert "ADD COLUMN IF NOT EXISTS formatted_address varchar" in alter_sql
    assert "ADD COLUMN IF NOT EXISTS formatted_address_version smallint" in alter_sql
    assert "ADD COLUMN IF NOT EXISTS formatted_address_source varchar(32)" in alter_sql


@pytest.mark.asyncio
async def test_overlay_stage_hydrates_formatted_metadata_from_archive(monkeypatch):
    monkeypatch.setattr(
        directory,
        "_is_table_present",
        AsyncMock(return_value=True),
    )
    status = AsyncMock(return_value="UPDATE 3")
    monkeypatch.setattr(directory.db, "status", status)

    changed_rows = await directory._backfill_address_overlay_stage_formatted_addresses(
        "mrf",
        '"mrf"."overlay_stage"',
    )

    assert changed_rows == 3
    hydrate_sql = status.await_args.args[0]
    assert 'FROM "mrf"."address_archive_v2" AS archive' in hydrate_sql
    assert "archive.address_key = stage_row.address_key" in hydrate_sql
    assert "archive.merged_into IS NULL" in hydrate_sql
    assert "formatted_address = archive.formatted_address" in hydrate_sql
    assert "formatted_address_version = archive.formatted_address_version" in hydrate_sql
    assert "formatted_address_source = archive.formatted_address_source" in hydrate_sql
    assert "IS DISTINCT FROM ROW" in hydrate_sql


@pytest.mark.asyncio
async def test_overlay_stage_skips_hydration_without_archive(monkeypatch):
    monkeypatch.setattr(
        directory,
        "_is_table_present",
        AsyncMock(return_value=False),
    )
    status = AsyncMock()
    monkeypatch.setattr(directory.db, "status", status)

    changed_rows = await directory._backfill_address_overlay_stage_formatted_addresses(
        "mrf",
        '"mrf"."overlay_stage"',
    )

    assert changed_rows == 0
    status.assert_not_awaited()


@pytest.mark.asyncio
async def test_overlay_population_hydrates_after_alias_rewrite(monkeypatch):
    events: list[str] = []
    monkeypatch.setattr(
        directory,
        "_copy_existing_address_overlay",
        AsyncMock(return_value=0),
    )
    monkeypatch.setattr(
        directory,
        "_insert_address_overlay_components",
        AsyncMock(return_value=({}, {})),
    )
    monkeypatch.setattr(
        directory,
        "_normalize_address_overlay_stage_countries",
        AsyncMock(return_value=0),
    )
    monkeypatch.setattr(
        directory,
        "_materialize_address_overlay_aliases",
        AsyncMock(side_effect=lambda *_: events.append("aliases") or {}),
    )
    monkeypatch.setattr(
        directory,
        "_backfill_address_overlay_stage_formatted_addresses",
        AsyncMock(side_effect=lambda *_: events.append("formatted") or 2),
    )
    monkeypatch.setattr(
        directory,
        "_backfill_address_overlay_stage_coordinates",
        AsyncMock(side_effect=lambda *_: events.append("coordinates") or 0),
    )
    monkeypatch.setattr(
        directory,
        "_dedupe_address_overlay_stage",
        AsyncMock(side_effect=lambda *_args, **_kwargs: events.append("dedupe") or 0),
    )
    monkeypatch.setattr(
        directory,
        "_create_address_overlay_stage_indexes",
        AsyncMock(),
    )
    monkeypatch.setattr(directory.db, "scalar", AsyncMock(return_value=1))
    monkeypatch.setattr(directory.db, "status", AsyncMock())

    metrics = await directory._populate_address_overlay_stage(
        "mrf",
        "overlay_stage",
        '"mrf"."overlay_stage"',
        None,
        [],
        {},
    )

    assert events == ["aliases", "formatted", "coordinates", "dedupe"]
    assert metrics["archive_formatted_address_backfill_rows"] == 2


def test_unified_archive_enrichment_carries_formatted_metadata():
    enrichment_sql = unified._enrich_raw_stage_sql(
        "mrf",
        "entity_address_unified_stage_raw",
    )

    assert "a.formatted_address::varchar AS archive_formatted_address" in enrichment_sql
    assert "a.formatted_address_version::smallint" in enrichment_sql
    assert "a.formatted_address_source::varchar" in enrichment_sql
    assert "formatted_address = COALESCE(k.archive_formatted_address" in enrichment_sql
    assert "formatted_address_version = CASE" in enrichment_sql
    assert "formatted_address_source = CASE" in enrichment_sql


def test_unified_aggregation_prefers_archive_formatted_label():
    aggregate_sql = unified._materialize_from_raw_sql(
        "mrf",
        "entity_address_unified_stage",
        "entity_address_unified_stage_raw",
    )
    archive_precedence = (
        "ORDER BY (formatted_address IS NULL), "
        "((formatted_address_version IS NULL) AND "
        "(formatted_address_source IS NULL)), "
        "formatted_address_version DESC NULLS LAST, source_priority ASC"
    )

    assert archive_precedence in aggregate_sql
    assert "formatted_address_version," in aggregate_sql
    assert "formatted_address_source," in aggregate_sql
    assert ")[1]::smallint AS formatted_address_version" in aggregate_sql
    assert ")[1]::varchar AS formatted_address_source" in aggregate_sql


def test_overlay_source_and_api_select_only_persisted_label():
    overlay_source_sql = unified._PROVIDER_DIRECTORY_PARTIAL_OVERLAY_SOURCE_TEMPLATE
    serving_sql = npi._provider_directory_overlay_query_sql(
        {"lat", "long", "formatted_address"}
    )

    assert "overlay.formatted_address::varchar AS formatted_address" in overlay_source_sql
    assert "MAX(NULLIF(BTRIM(formatted_address), ''))::varchar" in serving_sql
    assert "address_archive_v2" not in serving_sql
    assert "addr_formatted_address" not in serving_sql
    assert "render_formatted_address" not in serving_sql


def test_overlay_api_falls_back_when_formatted_column_is_absent():
    serving_sql = npi._provider_directory_overlay_query_sql({"lat", "long"})

    assert "NULL::varchar AS formatted_address" in serving_sql
    assert "MAX(NULLIF(BTRIM(formatted_address), ''))" not in serving_sql
