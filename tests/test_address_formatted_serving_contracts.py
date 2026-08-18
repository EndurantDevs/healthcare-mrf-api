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
async def test_overlay_stage_renders_from_its_own_components(monkeypatch):
    status = AsyncMock(return_value="UPDATE 3")
    monkeypatch.setattr(directory.db, "status", status)

    changed_rows = await directory._backfill_address_overlay_stage_formatted_addresses(
        "mrf",
        '"mrf"."overlay_stage"',
    )

    assert changed_rows == 3
    hydrate_sql = status.await_args.args[0]
    assert '"mrf".addr_formatted_address_v2(' in hydrate_sql
    for component in (
        "first_line",
        "second_line",
        "city_name",
        "state_name",
        "postal_code",
        "country_code",
    ):
        assert f"stage_row.{component}" in hydrate_sql
    assert "formatted_address_version = 2" in hydrate_sql
    assert "formatted_address_source = 'canonical_v2'" in hydrate_sql
    assert "address_archive_v2" not in hydrate_sql
    assert "IS DISTINCT FROM ROW" in hydrate_sql
    assert "formatted_source_ids" not in hydrate_sql
    assert status.await_args.kwargs == {}


@pytest.mark.asyncio
async def test_overlay_stage_rendering_does_not_require_archive(monkeypatch):
    monkeypatch.setattr(
        directory,
        "_is_table_present",
        AsyncMock(return_value=False),
    )
    status = AsyncMock(return_value="UPDATE 2")
    monkeypatch.setattr(directory.db, "status", status)

    changed_rows = await directory._backfill_address_overlay_stage_formatted_addresses(
        "mrf",
        '"mrf"."overlay_stage"',
    )

    assert changed_rows == 2
    status.assert_awaited_once()


@pytest.mark.asyncio
async def test_overlay_stage_rendering_scopes_selected_sources(monkeypatch):
    status = AsyncMock(return_value="UPDATE 1")
    monkeypatch.setattr(directory.db, "status", status)

    changed_rows = await directory._backfill_address_overlay_stage_formatted_addresses(
        "mrf",
        '"mrf"."overlay_stage"',
        source_ids=["source-a"],
    )

    assert changed_rows == 1
    assert (
        "stage_row.source_id = ANY(CAST(:formatted_source_ids AS varchar[]))"
        in status.await_args.args[0]
    )
    assert status.await_args.kwargs == {"formatted_source_ids": ["source-a"]}


def _record_overlay_event(events, name, value):
    def record(*_args, **_kwargs):
        events.append(name)
        return value

    return record


def _stub_overlay_population_dependencies(monkeypatch, events):
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
        AsyncMock(side_effect=_record_overlay_event(events, "aliases", {})),
    )
    for function_name, event_name, changed_rows in (
        ("_backfill_address_overlay_stage_premise_keys", "premise", 1),
        ("_backfill_address_overlay_stage_formatted_addresses", "formatted", 2),
        ("_backfill_address_overlay_stage_coordinates", "coordinates", 0),
    ):
        monkeypatch.setattr(
            directory,
            function_name,
            AsyncMock(
                side_effect=_record_overlay_event(events, event_name, changed_rows)
            ),
        )
    monkeypatch.setattr(
        directory,
        "_dedupe_address_overlay_stage",
        AsyncMock(side_effect=_record_overlay_event(events, "dedupe", 0)),
    )
    monkeypatch.setattr(
        directory,
        "_create_address_overlay_stage_indexes",
        AsyncMock(),
    )
    monkeypatch.setattr(directory.db, "scalar", AsyncMock(return_value=1))
    monkeypatch.setattr(directory.db, "status", AsyncMock())


@pytest.mark.asyncio
async def test_overlay_population_hydrates_after_alias_rewrite(monkeypatch):
    events: list[str] = []
    _stub_overlay_population_dependencies(monkeypatch, events)

    metrics = await directory._populate_address_overlay_stage(
        "mrf",
        "overlay_stage",
        '"mrf"."overlay_stage"',
        None,
        [],
        {},
    )

    assert events == ["aliases", "premise", "formatted", "coordinates", "dedupe"]
    assert metrics["archive_premise_key_backfill_rows"] == 1
    assert metrics["archive_formatted_address_backfill_rows"] == 2
    formatted = directory._backfill_address_overlay_stage_formatted_addresses
    assert formatted.await_args.kwargs == {"source_ids": []}


def test_unified_archive_enrichment_never_copies_formatted_labels():
    enrichment_sql = unified._enrich_raw_stage_sql(
        "mrf",
        "entity_address_unified_stage_raw",
    )

    assert "archive_formatted_address" not in enrichment_sql
    assert "formatted_address =" not in enrichment_sql
    assert "formatted_address_version =" not in enrichment_sql
    assert "formatted_address_source =" not in enrichment_sql


def test_unified_aggregation_renders_selected_components_locally():
    raw_load_sql = unified._insert_raw_from_source_sql(
        "mrf",
        "entity_address_unified_stage_raw",
        "SELECT * FROM mrf.source_address",
    )
    aggregate_sql = unified._materialize_from_raw_sql(
        "mrf",
        "entity_address_unified_stage",
        "entity_address_unified_stage_raw",
    )
    insert_columns = aggregate_sql.split("WITH aggregated AS", 1)[0]

    assert "addr_formatted_address_v2(" not in raw_load_sql
    assert "TRIM(formatted_address)" not in raw_load_sql
    assert "NULL::varchar AS formatted_address" in raw_load_sql
    assert "NULL::smallint AS formatted_address_version" in raw_load_sql
    assert " AS formatted_address" not in insert_columns
    assert "mrf.addr_formatted_address_v2(" in aggregate_sql
    assert "2::smallint AS formatted_address_version" in aggregate_sql
    assert "'canonical_v2'::varchar AS formatted_address_source" in aggregate_sql


def test_unified_generation_receipt_fences_formatted_address_version():
    expected_version = (
        f"address_archive_v2:v2+fmt-v{unified.ADDRESS_FORMAT_VERSION}"
    )

    assert unified.BASE_ADDRESS_VERSION == expected_version
    assert unified.ALIAS_BASE_ADDRESS_VERSION_PREFIX == (
        f"{expected_version}+alias-v1:g"
    )


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


def test_public_address_finalizer_rejects_an_external_sibling_unit_label():
    public_address = npi._finalize_public_provider_address(
        {
            "first_line": "4007 Clarksville Pike Suite 301",
            "second_line": "Ste 301",
            "city_name": "NASHVILLE",
            "state_name": "TN",
            "postal_code": "37218",
            "country_code": "US",
            "formatted_address": (
                "4007 CLARKSVILLE PIKE, 101, NASHVILLE, TN 37218"
            ),
            "formatted_address_version": None,
            "formatted_address_source": None,
        },
        include_sources=False,
        include_evidence=False,
    )

    assert public_address["formatted_address"] == (
        "4007 Clarksville Pike, Suite 301, Nashville, TN 37218"
    )
    assert "101" not in public_address["formatted_address"]
    assert "United States" not in public_address["formatted_address"]
    assert public_address["formatted_address_version"] == 2
    assert public_address["formatted_address_source"] == "canonical_v2"


def test_public_address_finalizer_suppresses_conflicting_site_keys():
    public_address = npi._finalize_public_provider_address(
        {
            "first_line": "100 MAIN ST",
            "city_name": "CITY",
            "state_name": "MO",
            "postal_code": "64055",
            "country_code": "US",
            "premise_key": "site-a",
            "address_site_key": "site-a",
            "_address_site_key_status": "conflicting",
        },
        include_sources=False,
        include_evidence=False,
        suppress_conflicting_site_key=True,
    )

    assert public_address["formatted_address"] == (
        "100 Main Street, City, MO 64055"
    )
    assert "premise_key" not in public_address
    assert "address_site_key" not in public_address


def test_shared_public_redaction_renders_every_nested_address_locally():
    payload = {
        "address_list": [
            {
                "first_line": "3800 S WHITNEY AVE",
                "city_name": "INDEPENDENCE",
                "state_name": "MO",
                "postal_code": "64055",
                "country_code": "US",
                "formatted_address": "external label",
            },
            {"formatted_address": "external-only label"},
        ]
    }

    npi._redact_internal_address_fields(payload)

    assert payload["address_list"][0]["formatted_address"] == (
        "3800 South Whitney Avenue, Independence, MO 64055"
    )
    assert payload["address_list"][1]["formatted_address"] is None
