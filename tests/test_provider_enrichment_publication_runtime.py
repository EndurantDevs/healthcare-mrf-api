"""Runtime cutover coverage for Provider Enrichment publication."""

import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, call

import pytest

pytest.importorskip("pytz")

enrichment = importlib.import_module("process.provider_enrichment")


class _AsyncTransaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, _exc_type, _exc, _traceback):
        return False


@pytest.mark.asyncio
async def test_address_resolution_covers_disabled_and_enabled_sources(monkeypatch):
    ffs_table = enrichment.ProviderEnrollmentFFS.__main_table__
    address_table = enrichment.ProviderEnrollmentFFSAddress.__main_table__
    staging_table_map = {
        ffs_table: SimpleNamespace(__tablename__="ffs_stage"),
        address_table: SimpleNamespace(__tablename__="address_stage"),
    }
    source_enabled = Mock(return_value=False)
    resolve_stage = AsyncMock()
    monkeypatch.setattr(enrichment, "source_enabled", source_enabled)
    monkeypatch.setattr(enrichment, "_resolve_provider_enrichment_stage_address", resolve_stage)

    assert await enrichment._resolve_provider_enrichment_addresses(
        staging_table_map,
        "provider_test",
    ) == {}
    resolve_stage.assert_not_awaited()
    source_enabled.return_value = True
    resolve_stage.side_effect = ({"rows": 3}, {"rows": 4})

    resolution_by_source = await enrichment._resolve_provider_enrichment_addresses(
        staging_table_map,
        "provider_test",
    )

    assert resolution_by_source == {
        "provider_enrollment_ffs": {"rows": 3},
        "provider_enrollment_ffs_address": {"rows": 4},
    }
    assert [resolution_call.args[0] for resolution_call in resolve_stage.await_args_list] == [
        staging_table_map[ffs_table],
        staging_table_map[address_table],
    ]


@pytest.mark.asyncio
async def test_resolve_stage_address_stamps_then_resolves(monkeypatch):
    stamp_keys = AsyncMock()
    resolve_archive = AsyncMock(return_value=SimpleNamespace(rows=5, unresolved=1))
    monkeypatch.setattr(enrichment, "stamp_address_keys", stamp_keys)
    monkeypatch.setattr(enrichment, "resolve_into_archive", resolve_archive)
    stage_model = SimpleNamespace(__tablename__="address_stage")
    address_field_map = {"zip": "zip_code"}

    address_resolution = await enrichment._resolve_provider_enrichment_stage_address(
        stage_model,
        address_field_map,
        "provider_test",
    )

    assert address_resolution == {"rows": 5, "unresolved": 1}
    stamp_keys.assert_awaited_once_with(
        "address_stage",
        address_field_map,
        schema="provider_test",
    )
    resolve_archive.assert_awaited_once_with(
        "address_stage",
        address_field_map,
        source_bit=4,
        priority=2,
        schema="provider_test",
    )


@pytest.mark.asyncio
async def test_create_indexes_covers_optional_using_and_where(monkeypatch):
    class IndexedModel:
        __main_table__ = "indexed"
        __my_additional_indexes__ = [
            {
                "name": "payload",
                "index_elements": ["payload"],
                "using": "gin",
                "where": "payload IS NOT NULL",
            },
            {"index_elements": ["npi"]},
        ]

    monkeypatch.setattr(enrichment, "PROCESSING_CLASSES", (IndexedModel,))
    monkeypatch.setattr(enrichment.db, "transaction", lambda: _AsyncTransaction())
    db_status = AsyncMock()
    monkeypatch.setattr(enrichment.db, "status", db_status)

    await enrichment._create_provider_enrichment_indexes(
        {"indexed": SimpleNamespace(__tablename__="indexed_stage")},
        "provider_test",
    )

    assert (
        "USING gin (payload) WHERE payload IS NOT NULL"
        in db_status.await_args_list[0].args[0]
    )
    assert "indexed_stage_idx_npi" in db_status.await_args_list[1].args[0]
    assert "USING" not in db_status.await_args_list[1].args[0]
    assert " WHERE " not in db_status.await_args_list[1].args[0]


@pytest.mark.asyncio
async def test_refresh_statistics_and_archive_index(monkeypatch):
    class FirstModel:
        __main_table__ = "first"

    class SecondModel:
        __main_table__ = "second"

    refresh_one_stage = enrichment._refresh_enrichment_stage_statistics
    refresh_stage = AsyncMock()
    monkeypatch.setattr(enrichment, "PROCESSING_CLASSES", (FirstModel, SecondModel))
    monkeypatch.setattr(enrichment, "_refresh_enrichment_stage_statistics", refresh_stage)
    staging_table_map = {
        "first": SimpleNamespace(__tablename__="first_stage"),
        "second": SimpleNamespace(__tablename__="second_stage"),
    }

    await enrichment._refresh_all_enrichment_statistics(staging_table_map, "provider_test")

    assert refresh_stage.await_args_list == [
        call(staging_table_map["first"], "provider_test"),
        call(staging_table_map["second"], "provider_test"),
    ]
    execute_ddl = AsyncMock()
    monkeypatch.setattr(enrichment.db, "execute_ddl", execute_ddl)
    await refresh_one_stage(staging_table_map["first"], "provider_test")
    execute_ddl.assert_awaited_once_with("ANALYZE provider_test.first_stage;")
    db_status = AsyncMock()
    monkeypatch.setattr(enrichment.db, "status", db_status)
    await enrichment._archive_provider_enrichment_index("provider_test", "first_idx_primary")
    assert db_status.await_args_list == [
        call("DROP INDEX IF EXISTS provider_test.first_idx_primary_old;"),
        call(
            "ALTER INDEX IF EXISTS provider_test.first_idx_primary "
            "RENAME TO first_idx_primary_old;"
        ),
    ]


@pytest.mark.asyncio
async def test_publish_tables_moves_primary_and_declared_indexes(monkeypatch):
    class PublishedModel:
        __main_table__ = "published"
        __my_initial_indexes__ = [{"name": "npi", "index_elements": ["npi"]}]
        __my_additional_indexes__ = [{"name": "state", "index_elements": ["state"]}]

    monkeypatch.setattr(enrichment, "PROCESSING_CLASSES", (PublishedModel,))
    monkeypatch.setattr(enrichment.db, "transaction", lambda: _AsyncTransaction())
    db_status = AsyncMock()
    archive_index = AsyncMock()
    monkeypatch.setattr(enrichment.db, "status", db_status)
    monkeypatch.setattr(enrichment, "_archive_provider_enrichment_index", archive_index)
    stage_model = SimpleNamespace(__main_table__="published", __tablename__="published_stage")

    await enrichment._publish_provider_enrichment_tables(
        {"published": stage_model},
        "provider_test",
    )

    assert archive_index.await_args_list == [
        call("provider_test", "published_idx_primary"),
        call("provider_test", "published_idx_npi"),
        call("provider_test", "published_idx_state"),
    ]
    published_sql = "\n".join(status_call.args[0] for status_call in db_status.await_args_list)
    assert "DROP TABLE IF EXISTS provider_test.published_old" in published_sql
    assert "published_stage RENAME TO published" in published_sql
    assert "published_stage_idx_primary" in published_sql
    assert "published_stage_idx_npi" in published_sql
    assert "published_stage_idx_state" in published_sql


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "address_stats_by_source",
    ({}, {"provider_enrollment_ffs": {"resolved": 3}}),
)
async def test_complete_run_reports_optional_address_stats(monkeypatch, address_stats_by_source):
    mark_run = AsyncMock()
    monkeypatch.setattr(enrichment, "mark_control_run", mark_run)
    context_map = {
        "audit": {
            "dataset_stats": {"hospital": [{}]},
            "rows_accepted": 4,
            "rows_dropped_missing_npi": 1,
        }
    }

    await enrichment._complete_provider_enrichment_run(
        "run-enrichment",
        context_map,
        {"hospital": 4},
        3,
        address_stats_by_source,
    )

    metrics_by_name = mark_run.await_args.kwargs["metrics"]
    assert metrics_by_name["stage_rows"] == {"hospital": 4}
    assert metrics_by_name["summary_rows"] == 3
    assert metrics_by_name["datasets"] == 1
    assert metrics_by_name["rows_accepted"] == 4
    assert metrics_by_name["rows_dropped_missing_npi"] == 1
    assert ("address_resolve" in metrics_by_name) is bool(address_stats_by_source)


@pytest.mark.asyncio
async def test_shutdown_without_run_skips_publication(monkeypatch):
    ensure_database = AsyncMock()
    staging_tables = AsyncMock()
    monkeypatch.setattr(enrichment, "ensure_database", ensure_database)
    monkeypatch.setattr(enrichment, "_provider_enrichment_staging_tables", staging_tables)

    await enrichment.shutdown({"context": {"run": 0}})

    ensure_database.assert_not_awaited()
    staging_tables.assert_not_awaited()


def _install_publish_orchestration(monkeypatch, event_names):
    async def async_event(name, returned_value=None):
        event_names.append(name)
        return returned_value

    step_fields = (
        ("ensure_database", "ensure", None),
        ("_provider_enrichment_staging_tables", "staging", {"hospital": SimpleNamespace()}),
        ("_required_provider_enrichment_stage_counts", "counts", {"hospital": 5}),
        ("_resolve_provider_enrichment_addresses", "addresses", {"hospital": {"resolved": 4}}),
        ("_materialize_provider_enrichment_summary", "summary", 4),
        ("_create_provider_enrichment_indexes", "indexes", None),
        ("_refresh_all_enrichment_statistics", "statistics", None),
        ("_publish_provider_enrichment_tables", "publish", None),
        ("_complete_provider_enrichment_run", "complete", None),
    )
    for attribute_name, event_name, returned_value in step_fields:
        async def step(*_args, _name=event_name, _value=returned_value, **_kwargs):
            return await async_event(_name, _value)

        monkeypatch.setattr(enrichment, attribute_name, step)


@pytest.mark.asyncio
async def test_shutdown_publishes_in_exact_orchestration_order(monkeypatch):
    event_names = []
    _install_publish_orchestration(monkeypatch, event_names)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "provider_test")
    monkeypatch.setattr(
        enrichment,
        "print_time_info",
        lambda start: event_names.append(f"time:{start}"),
    )
    worker_context_map = {
        "import_date": "20260801",
        "control_run_id": "run-top",
        "context": {
            "run": 1,
            "test_mode": True,
            "control_run_id": " run-context ",
            "start": "started-at",
        },
    }

    await enrichment.shutdown(worker_context_map)

    assert event_names == [
        "ensure",
        "staging",
        "counts",
        "addresses",
        "summary",
        "indexes",
        "statistics",
        "publish",
        "complete",
        "time:started-at",
    ]
