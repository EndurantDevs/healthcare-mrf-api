import importlib
import asyncio
import json

import pytest

from process.control_cancel import ImportCancelledError


openaddresses = importlib.import_module("process.openaddresses")
control_imports = importlib.import_module("api.control_imports")


@pytest.mark.parametrize(
    ("properties", "reason"),
    (
        (None, "not_dict"),
        ({"street": "Main Street", "region": "MO", "postcode": "64055"}, "missing_house_number"),
        (
            {"number": "100", "region": "MO", "postcode": "64055"},
            "missing_street",
        ),
        (
            {
                "number": "100",
                "street": "---",
                "region": "MO",
                "postcode": "64055",
            },
            "missing_street_match_key",
        ),
        (
            {"number": "100", "street": "Main Street", "postcode": "64055"},
            "missing_state",
        ),
    ),
)
def test_feature_import_records_report_invalid_components(properties, reason):
    feature = [] if properties is None else {
        "properties": properties,
        "geometry": {"type": "Point", "coordinates": [-94.5, 39.0]},
    }
    assert openaddresses._feature_import_records(
        feature,
        source_name=None,
        data_id=None,
        job_id=None,
        updated=None,
        restore_shards=1,
    ) == (None, None, reason)


def test_openaddresses_record_uses_us_source_state_and_canonical_keys():
    feature_by_field = {
        "type": "Feature",
        "properties": {
            "number": "123",
            "street": "Main Street",
            "unit": "Suite 200",
            "city": "Austin",
            "postcode": "78701-1234",
            "id": "OA-1",
            "accuracy": "rooftop",
        },
        "geometry": {"type": "Point", "coordinates": [-97.7431, 30.2672]},
    }

    address_record = openaddresses._record_from_feature(
        feature_by_field,
        source_name="us/tx/austin",
        data_id=10,
        job_id=20,
        updated=1781288662893,
    )

    assert address_record is not None
    assert address_record["house_number"] == "123"
    assert address_record["street_match_key"] == "mainst"
    assert address_record["state_code"] == "TX"
    assert address_record["zip5"] == "78701"
    assert address_record["address_key"] is not None
    assert address_record["zip5_source"] == "openaddresses_postcode"
    assert address_record["zip5_restored_at"] is None


def test_openaddresses_missing_zip_point_is_staged_for_zip_recovery():
    feature_by_field = {
        "type": "Feature",
        "properties": {
            "number": "123",
            "street": "Main Street",
            "unit": "Suite 200",
            "city": "Austin",
            "region": "TX",
            "id": "OA-1",
            "accuracy": "rooftop",
        },
        "geometry": {"type": "Point", "coordinates": [-97.7431, 30.2672]},
    }

    recovery_record, reason = openaddresses._zip_recovery_record_from_feature(
        feature_by_field,
        source_name="us/tx/austin",
        data_id=10,
        job_id=20,
        updated=1781288662893,
        restore_shards=16,
    )

    assert reason == "missing_zip5"
    assert recovery_record is not None
    assert recovery_record["house_number"] == "123"
    assert recovery_record["street_match_key"] == "mainst"
    assert recovery_record["state_code"] == "TX"
    assert recovery_record["lat"] == 30.2672
    assert recovery_record["long"] == -97.7431
    assert 0 <= recovery_record["restore_bucket"] < 16


def test_openaddresses_record_rejects_non_us_coordinates():
    feature_by_field = {
        "type": "Feature",
        "properties": {
            "number": "123",
            "street": "Main Street",
            "region": "TX",
            "postcode": "78701",
        },
        "geometry": {"type": "Point", "coordinates": [4.31653, 50.83595]},
    }

    assert (
        openaddresses._record_from_feature(
            feature_by_field,
            source_name="us/tx/austin",
            data_id=10,
            job_id=20,
            updated=None,
        )
        is None
    )


def test_openaddresses_lookup_params_strip_house_number_and_normalize_street():
    params = openaddresses.lookup_params_from_address(
        {
            "first_line": "123 Main Street",
            "second_line": "",
            "city_name": "Austin",
            "state_name": "Texas",
            "postal_code": "78701-1234",
            "country_code": "US",
        }
    )

    assert params["house_number"] == "123"
    assert params["street_match_key"] == "mainst"
    assert params["city_norm"] == "austin"
    assert params["state_code"] == "TX"
    assert params["zip5"] == "78701"


def test_openaddresses_iter_geojson_features_reads_line_delimited_features(tmp_path):
    features = [
        {
            "type": "Feature",
            "properties": {"id": "1"},
            "geometry": {"type": "Point", "coordinates": [-97.7431, 30.2672]},
        },
        {
            "type": "Feature",
            "properties": {"id": "2"},
            "geometry": {"type": "Point", "coordinates": [-87.6298, 41.8781]},
        },
    ]
    path = tmp_path / "source.geojson"
    path.write_text("\n".join(json.dumps(feature) for feature in features), encoding="utf-8")

    assert list(openaddresses._iter_geojson_features(path)) == features


def test_openaddresses_lookup_sql_uses_strict_fuzzy_guards():
    sql = openaddresses.fuzzy_lookup_sql("mrf")

    assert "state_code = :state_code" in sql
    assert "zip5 = :zip5" in sql
    assert "house_number = :house_number" in sql
    assert "similarity(street_match_key, :street_match_key) >= :fuzzy_threshold" in sql
    assert "score - next_score >= :fuzzy_margin" in sql


def test_openaddresses_exact_lookup_sql_uses_city_when_available():
    sql = openaddresses.exact_lookup_sql("mrf")

    assert "state_code = :state_code" in sql
    assert "zip5 = :zip5" in sql
    assert "house_number = :house_number" in sql
    assert "street_match_key = :street_match_key" in sql
    assert ":city_norm IS NULL" in sql
    assert "addr_city_norm_v1(city_name)" in sql
    assert "= :city_norm" in sql


def test_openaddresses_relaxed_lookup_sql_uses_city_zip_guards():
    sql = openaddresses.relaxed_lookup_sql("mrf")

    assert "zip5 = :zip5" in sql
    assert "house_number = :house_number" in sql
    assert "addr_city_norm_v1(city_name)" in sql
    assert "= :city_norm" in sql
    assert "similarity(street_match_key, :street_match_key) >= :relaxed_threshold" in sql
    assert "score - next_score >= :relaxed_margin" in sql


def test_archive_match_components_extracts_house_number_without_postgres_word_boundary():
    sql = openaddresses._archive_match_components_cte("mrf", "address_archive_v2")

    assert "substring(first_line from '^\\s*([0-9]+[A-Za-z]?)')" in sql
    assert "([0-9]+[A-Za-z]?)\\b" not in sql


def test_openaddresses_backfill_ctes_include_state_and_zip_shard_filters():
    archive_sql = openaddresses._archive_match_components_cte(
        "mrf",
        "address_archive_v2",
        state_code="CA",
        zip_prefix="90",
    )
    grouped_sql = openaddresses._openaddresses_grouped_cte(
        "mrf",
        "openaddresses_geocode",
        state_code="CA",
        zip_prefix="90",
    )
    city_grouped_sql = openaddresses._openaddresses_city_grouped_cte(
        "mrf",
        "openaddresses_geocode",
        state_code="CA",
        zip_prefix="90",
    )

    for sql in (archive_sql, grouped_sql, city_grouped_sql):
        assert "state_code = :backfill_state_code" in sql
        assert "zip5 >= :backfill_zip_lower" in sql
        assert "zip5 < :backfill_zip_upper" in sql


def test_openaddresses_backfill_source_contains_city_scoped_exact_phase():
    source = openaddresses.refresh_archive_geocodes_from_openaddresses.__code__.co_consts
    sql_text = "\n".join(const for const in source if isinstance(const, str))

    assert "openaddresses_exact_city" in sql_text
    assert "missing.city_norm IS NOT NULL" in sql_text
    assert "addr_city_norm_v1(oa.city_name)" in sql_text
    assert "formatted_address" not in sql_text


def test_openaddresses_backfill_match_modes_parser():
    assert openaddresses._normalize_backfill_match_modes(None) == {"exact", "fuzzy", "relaxed"}
    assert openaddresses._normalize_backfill_match_modes(" exact, fuzzy ") == {"exact", "fuzzy"}
    assert openaddresses._normalize_backfill_match_modes(["relaxed"]) == {"relaxed"}
    assert openaddresses._normalize_backfill_match_modes("all") == {"exact", "fuzzy", "relaxed"}

    with pytest.raises(ValueError, match="bogus"):
        openaddresses._normalize_backfill_match_modes("exact,bogus")


def test_openaddresses_load_progress_payload(monkeypatch):
    events = []
    monkeypatch.setattr(openaddresses, "enqueue_live_progress", lambda **payload: events.append(payload))

    openaddresses._emit_load_progress(
        processed_files=12,
        total_files=100,
        processed_rows=3456,
        accepted_rows=1234,
        label="us/tx/example",
        run_id="run_openaddresses",
    )

    assert events == [
        {
            "run_id": "run_openaddresses",
            "importer": "openaddresses",
            "status": "running",
            "unit": "sources",
            "done": 12,
            "total": 100,
            "pct": 12.0,
            "phase": "loading OpenAddresses sources",
            "message": "12/100 sources; 3,456 rows processed; 1,234 rows accepted",
            "label": "us/tx/example",
            "step": "us/tx/example",
            "source": "openaddresses-load-progress",
            "confidence": "live",
        }
    ]


def test_openaddresses_backfill_progress_payload(monkeypatch):
    events = []
    monkeypatch.setattr(openaddresses, "enqueue_live_progress", lambda **payload: events.append(payload))

    openaddresses._emit_backfill_progress(
        completed_shards=2,
        total_shards=5,
        stats=openaddresses.OpenAddressesBackfillStats(exact_updates=3, fuzzy_updates=2, relaxed_updates=1),
        label="TX ZIP 75*",
        total_candidates=1234,
        run_id="run_openaddresses",
    )

    assert events == [
        {
            "run_id": "run_openaddresses",
            "importer": "openaddresses",
            "status": "running",
            "unit": "shards",
            "done": 2,
            "total": 5,
            "pct": 40.0,
            "phase": "backfilling address archive from OpenAddresses",
            "message": (
                "2/5 shards; 6 archive rows updated "
                "(exact=3, fuzzy=2, relaxed=1); 1,234 candidate rows"
            ),
            "label": "TX ZIP 75*",
            "step": "TX ZIP 75*",
            "source": "openaddresses-backfill-progress",
            "confidence": "live",
        }
    ]


def test_openaddresses_progress_run_id_prefers_task_then_context():
    assert (
        openaddresses._progress_run_id(
            {"control_run_id": "run_ctx", "context": {"control_run_id": "run_nested"}},
            {"run_id": " run_task "},
        )
        == "run_task"
    )
    assert (
        openaddresses._progress_run_id(
            {"control_run_id": "run_ctx", "context": {"control_run_id": "run_nested"}},
            {},
        )
        == "run_ctx"
    )
    assert (
        openaddresses._progress_run_id(
            {"context": {"control_run_id": "run_nested"}},
            {},
        )
        == "run_nested"
    )


@pytest.mark.asyncio
async def test_openaddresses_backfill_plans_state_zip_prefix_shards(monkeypatch):
    seen_by_field = {}

    class FakeDb:
        async def all(self, stmt, **params):
            seen_by_field["stmt"] = stmt
            seen_by_field["params"] = params
            return [
                {"state_code": "TX", "zip_prefix": "75", "candidate_count": 100},
                {"state_code": "CA", "zip_prefix": "90", "candidate_count": 50},
            ]

    monkeypatch.setattr(openaddresses, "db", FakeDb())

    shards = await openaddresses._plan_openaddresses_backfill_shards(
        schema="mrf",
        archive_table="address_archive_v2",
        zip_prefix_length=2,
    )

    assert seen_by_field["params"] == {"backfill_state_code": None, "backfill_zip_prefix_length": 2}
    assert "substring(zip5 from 1 for :backfill_zip_prefix_length)" in seen_by_field["stmt"]
    assert shards == [
        openaddresses.OpenAddressesBackfillShard(state_code="TX", zip_prefix="75", candidate_count=100),
        openaddresses.OpenAddressesBackfillShard(state_code="CA", zip_prefix="90", candidate_count=50),
    ]


@pytest.mark.asyncio
async def test_openaddresses_backfill_plan_skips_invalid_archive_state_shards(monkeypatch, caplog):
    class FakeDb:
        async def all(self, _stmt, **_params):
            return [
                {"state_code": "TE", "zip_prefix": "12", "candidate_count": 4},
                {"state_code": "TX", "zip_prefix": "75", "candidate_count": 10},
            ]

    monkeypatch.setattr(openaddresses, "db", FakeDb())

    shards = await openaddresses._plan_openaddresses_backfill_shards(
        schema="mrf",
        archive_table="address_archive_v2",
        zip_prefix_length=2,
    )

    assert shards == [
        openaddresses.OpenAddressesBackfillShard(state_code="TX", zip_prefix="75", candidate_count=10),
    ]
    assert "Skipping OpenAddresses archive backfill shard" in caplog.text


@pytest.mark.asyncio
async def test_openaddresses_sharded_backfill_uses_bounded_concurrency_and_aggregates(monkeypatch):
    concurrency_by_metric = {"active": 0, "max_active": 0}
    progress = []
    shards = [
        openaddresses.OpenAddressesBackfillShard(state_code="TX", zip_prefix="75", candidate_count=10),
        openaddresses.OpenAddressesBackfillShard(state_code="CA", zip_prefix="90", candidate_count=20),
        openaddresses.OpenAddressesBackfillShard(state_code="NY", zip_prefix="10", candidate_count=30),
    ]

    async def is_table_present(_schema, _table):
        return True

    async def has_table_column(_schema, _table, _column):
        return True

    async def fake_plan(**_kwargs):
        return shards

    async def fake_refresh(**kwargs):
        concurrency_by_metric["active"] += 1
        concurrency_by_metric["max_active"] = max(
            concurrency_by_metric["max_active"],
            concurrency_by_metric["active"],
        )
        await asyncio.sleep(0.01)
        concurrency_by_metric["active"] -= 1
        return openaddresses.OpenAddressesBackfillStats(
            exact_updates=int(kwargs["zip_prefix"]),
            fuzzy_updates=1,
            relaxed_updates=2,
        )

    monkeypatch.setattr(openaddresses, "_is_table_present", is_table_present)
    monkeypatch.setattr(openaddresses, "_has_table_column", has_table_column)
    monkeypatch.setattr(openaddresses, "_plan_openaddresses_backfill_shards", fake_plan)
    monkeypatch.setattr(openaddresses, "refresh_archive_geocodes_from_openaddresses", fake_refresh)
    monkeypatch.setattr(openaddresses, "enqueue_live_progress", lambda **payload: progress.append(payload))

    stats = await openaddresses.refresh_archive_geocodes_from_openaddresses_sharded(
        schema="mrf",
        concurrency=2,
        run_id="run_openaddresses",
    )

    assert concurrency_by_metric["max_active"] == 2
    assert stats == openaddresses.OpenAddressesBackfillStats(
        exact_updates=175,
        fuzzy_updates=3,
        relaxed_updates=6,
    )
    assert [event["done"] for event in progress] == [0, 1, 2, 3]
    assert all(event["run_id"] == "run_openaddresses" for event in progress)


@pytest.mark.asyncio
async def test_openaddresses_backfill_exact_match_mode_skips_fuzzy_relaxed(monkeypatch):
    statements = []

    async def is_table_present(_schema, _table):
        return True

    async def has_table_column(_schema, _table, _column):
        return True

    class FakeDb:
        async def status(self, stmt, **_params):
            statements.append(stmt)
            return "UPDATE 1" if "UPDATE" in stmt else "CREATE EXTENSION"

    monkeypatch.setattr(openaddresses, "_is_table_present", is_table_present)
    monkeypatch.setattr(openaddresses, "_has_table_column", has_table_column)
    monkeypatch.setattr(openaddresses, "db", FakeDb())

    stats = await openaddresses.refresh_archive_geocodes_from_openaddresses(
        schema="mrf",
        archive_table="address_archive_v2",
        source_table="openaddresses_geocode",
        state_code="TX",
        zip_prefix="75",
        match_modes="exact",
    )

    sql_text = "\n".join(statements)
    assert stats == openaddresses.OpenAddressesBackfillStats(exact_updates=2, fuzzy_updates=0, relaxed_updates=0)
    assert "openaddresses_exact_city" in sql_text
    assert "openaddresses_exact" in sql_text
    assert "openaddresses_fuzzy_zip" not in sql_text
    assert "openaddresses_relaxed_city_zip" not in sql_text
