import importlib
import asyncio
import json

import pytest

from process.control_cancel import ImportCancelledError


openaddresses = importlib.import_module("process.openaddresses")
control_imports = importlib.import_module("api.control_imports")


def test_openaddresses_record_uses_us_source_state_and_canonical_keys():
    feature = {
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

    record = openaddresses._record_from_feature(  # pylint: disable=protected-access
        feature,
        source="us/tx/austin",
        data_id=10,
        job_id=20,
        updated=1781288662893,
    )

    assert record is not None
    assert record["house_number"] == "123"
    assert record["street_match_key"] == "mainst"
    assert record["state_code"] == "TX"
    assert record["zip5"] == "78701"
    assert record["address_key"] is not None


def test_openaddresses_record_rejects_non_us_coordinates():
    feature = {
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
        openaddresses._record_from_feature(  # pylint: disable=protected-access
            feature,
            source="us/tx/austin",
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

    assert list(openaddresses._iter_geojson_features(path)) == features  # pylint: disable=protected-access


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
    sql = openaddresses._archive_match_components_cte("mrf", "address_archive_v2")  # pylint: disable=protected-access

    assert "substring(first_line from '^\\s*([0-9]+[A-Za-z]?)')" in sql
    assert "([0-9]+[A-Za-z]?)\\b" not in sql


def test_openaddresses_backfill_ctes_include_state_and_zip_shard_filters():
    archive_sql = openaddresses._archive_match_components_cte(  # pylint: disable=protected-access
        "mrf",
        "address_archive_v2",
        state_code="CA",
        zip_prefix="90",
    )
    grouped_sql = openaddresses._openaddresses_grouped_cte(  # pylint: disable=protected-access
        "mrf",
        "openaddresses_geocode",
        state_code="CA",
        zip_prefix="90",
    )
    city_grouped_sql = openaddresses._openaddresses_city_grouped_cte(  # pylint: disable=protected-access
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


def test_openaddresses_load_progress_payload(monkeypatch):
    events = []
    monkeypatch.setattr(openaddresses, "enqueue_live_progress", lambda **payload: events.append(payload))

    openaddresses._emit_load_progress(  # pylint: disable=protected-access
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


def test_openaddresses_progress_run_id_prefers_task_then_context():
    assert (
        openaddresses._progress_run_id(  # pylint: disable=protected-access
            {"control_run_id": "run_ctx", "context": {"control_run_id": "run_nested"}},
            {"run_id": " run_task "},
        )
        == "run_task"
    )
    assert (
        openaddresses._progress_run_id(  # pylint: disable=protected-access
            {"control_run_id": "run_ctx", "context": {"control_run_id": "run_nested"}},
            {},
        )
        == "run_ctx"
    )
    assert (
        openaddresses._progress_run_id(  # pylint: disable=protected-access
            {"context": {"control_run_id": "run_nested"}},
            {},
        )
        == "run_nested"
    )


class _FakeDownloadContent:
    def __init__(self, chunks):
        self._chunks = list(chunks)

    async def iter_chunked(self, _chunk_size):
        for chunk in self._chunks:
            yield chunk


class _FakeDownloadResponse:
    def __init__(self, status, *, body="", chunks=()):
        self.status = status
        self._body = body
        self.content = _FakeDownloadContent(chunks)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return False

    async def text(self):
        return self._body


class _FakeDownloadClient:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.responses.pop(0)


@pytest.mark.asyncio
async def test_openaddresses_download_retries_transient_http_status(monkeypatch, tmp_path):
    sleeps = []
    client = _FakeDownloadClient(
        [
            _FakeDownloadResponse(504, body="gateway timeout"),
            _FakeDownloadResponse(200, chunks=[b"abc", b"def"]),
        ]
    )

    async def fake_sleep(delay):
        sleeps.append(delay)

    monkeypatch.setattr(openaddresses.asyncio, "sleep", fake_sleep)
    path = tmp_path / "source.geojson.gz"

    await openaddresses._download_file(  # pylint: disable=protected-access
        client,
        "https://openaddresses.test/source.geojson.gz",
        path,
        "token",
        task={"download_retries": 1},
    )

    assert path.read_bytes() == b"abcdef"
    assert len(client.calls) == 2
    assert sleeps == [openaddresses.DEFAULT_DOWNLOAD_RETRY_BASE_SECONDS]


@pytest.mark.asyncio
async def test_openaddresses_download_does_not_retry_non_transient_status(tmp_path):
    client = _FakeDownloadClient([_FakeDownloadResponse(404, body="missing")])

    with pytest.raises(RuntimeError, match="HTTP 404"):
        await openaddresses._download_file(  # pylint: disable=protected-access
            client,
            "https://openaddresses.test/source.geojson.gz",
            tmp_path / "source.geojson.gz",
            "token",
            task={"download_retries": 1},
        )

    assert len(client.calls) == 1


@pytest.mark.asyncio
async def test_openaddresses_flush_uses_copy_first(monkeypatch):
    calls = []

    async def fake_push_objects(rows, cls, *, rewrite, use_copy):
        calls.append((list(rows), cls, rewrite, use_copy))

    rows = [{"row_hash": "a" * 64}]
    monkeypatch.setattr(openaddresses, "push_objects", fake_push_objects)

    accepted = await openaddresses._flush_rows(rows, object)  # pylint: disable=protected-access

    assert accepted == 1
    assert rows == []
    assert calls == [([{"row_hash": "a" * 64}], object, True, True)]


@pytest.mark.asyncio
async def test_openaddresses_repairs_legacy_stage_row_hash_width(monkeypatch):
    statuses = []

    class FakeDb:
        async def first(self, _stmt, **params):
            assert params == {"schema": "mrf", "table_name": "openaddresses_geocode_202606151230024"}
            return {"data_type": "character varying", "character_maximum_length": 32}

        async def status(self, stmt, **_params):
            statuses.append(stmt)

    monkeypatch.setattr(openaddresses, "db", FakeDb())

    await openaddresses._ensure_openaddresses_stage_schema(  # pylint: disable=protected-access
        "openaddresses_geocode_202606151230024",
        "mrf",
    )

    assert statuses == ['ALTER TABLE "mrf"."openaddresses_geocode_202606151230024" ALTER COLUMN row_hash TYPE varchar(64);']


@pytest.mark.asyncio
async def test_openaddresses_keeps_current_stage_row_hash_width(monkeypatch):
    statuses = []

    class FakeDb:
        async def first(self, _stmt, **_params):
            return {"data_type": "character varying", "character_maximum_length": 64}

        async def status(self, stmt, **_params):
            statuses.append(stmt)

    monkeypatch.setattr(openaddresses, "db", FakeDb())

    await openaddresses._ensure_openaddresses_stage_schema(  # pylint: disable=protected-access
        "openaddresses_geocode_202606151230024",
        "mrf",
    )

    assert statuses == []


@pytest.mark.asyncio
async def test_openaddresses_local_files_load_in_parallel(monkeypatch, tmp_path):
    paths = []
    for index in range(3):
        path = tmp_path / f"source-{index}.geojson"
        path.write_text("{}", encoding="utf-8")
        paths.append(path)

    active = 0
    max_active = 0

    async def fake_load_file(path, **_kwargs):
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        await asyncio.sleep(0.01)
        active -= 1
        return 10, 5

    monkeypatch.setattr(openaddresses, "_load_file", fake_load_file)
    monkeypatch.setattr(openaddresses, "_emit_load_progress", lambda **_payload: None)

    stats = await openaddresses._load_openaddresses_data(  # pylint: disable=protected-access
        {"context": {"test_mode": False}},
        {"local_files": [str(path) for path in paths], "source_concurrency": 3},
        object,
    )

    assert stats == {"processed_files": 3, "processed_rows": 30, "accepted_rows": 15}
    assert max_active > 1


@pytest.mark.asyncio
async def test_openaddresses_remote_sources_load_in_parallel(monkeypatch):
    items = [
        {"source": f"us/tx/source-{index}", "layer": "addresses", "output": {"output": True}, "id": index, "job": index}
        for index in range(1, 4)
    ]
    active = 0
    max_active = 0

    async def fake_fetch_json(_client, _url, _token):
        return items

    async def fake_load_source_item(**kwargs):
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        await asyncio.sleep(0.01)
        active -= 1
        return kwargs["item"]["source"], 10, 5

    monkeypatch.setenv("HLTHPRT_OPENADDRESSES_API_TOKEN", "test-token")
    monkeypatch.setattr(openaddresses, "_fetch_json", fake_fetch_json)
    monkeypatch.setattr(openaddresses, "_load_source_item", fake_load_source_item)
    monkeypatch.setattr(openaddresses, "_emit_load_progress", lambda **_payload: None)

    stats = await openaddresses._load_openaddresses_data(  # pylint: disable=protected-access
        {"context": {"test_mode": False}},
        {"source_concurrency": 3, "max_files": 3},
        object,
    )

    assert stats == {"processed_files": 3, "processed_rows": 30, "accepted_rows": 15}
    assert max_active > 1


@pytest.mark.asyncio
async def test_openaddresses_remote_tempdir_ignores_cleanup_errors(monkeypatch, tmp_path):
    tempdir_kwargs = []
    items = [{"source": "us/tx/source-1", "layer": "addresses", "output": {"output": True}, "id": 1, "job": 1}]

    class FakeTemporaryDirectory:
        def __init__(self, **kwargs):
            tempdir_kwargs.append(kwargs)

        def __enter__(self):
            return str(tmp_path)

        def __exit__(self, *_args):
            return False

    async def fake_fetch_json(_client, _url, _token):
        return items

    async def fake_load_source_item(**kwargs):
        return kwargs["item"]["source"], 10, 5

    monkeypatch.setenv("HLTHPRT_OPENADDRESSES_API_TOKEN", "test-token")
    monkeypatch.setattr(openaddresses.tempfile, "TemporaryDirectory", FakeTemporaryDirectory)
    monkeypatch.setattr(openaddresses, "_fetch_json", fake_fetch_json)
    monkeypatch.setattr(openaddresses, "_load_source_item", fake_load_source_item)
    monkeypatch.setattr(openaddresses, "_emit_load_progress", lambda **_payload: None)

    stats = await openaddresses._load_openaddresses_data(  # pylint: disable=protected-access
        {"context": {"test_mode": False}},
        {"source_concurrency": 1, "max_files": 1},
        object,
    )

    assert stats == {"processed_files": 1, "processed_rows": 10, "accepted_rows": 5}
    assert tempdir_kwargs == [{"ignore_cleanup_errors": True}]


@pytest.mark.asyncio
async def test_openaddresses_remote_test_mode_honors_source_concurrency(monkeypatch):
    items = [
        {"source": f"us/ca/test-{index}", "layer": "addresses", "output": {"output": True}, "id": index, "job": index}
        for index in range(1, 4)
    ]
    active = 0
    max_active = 0

    async def fake_fetch_json(_client, _url, _token):
        return items

    async def fake_load_source_item(**kwargs):
        nonlocal active, max_active
        assert kwargs["test_mode"] is True
        assert kwargs["test_row_limit"] == 10
        active += 1
        max_active = max(max_active, active)
        await asyncio.sleep(0.01)
        active -= 1
        return kwargs["item"]["source"], 10, 5

    monkeypatch.setenv("HLTHPRT_OPENADDRESSES_API_TOKEN", "test-token")
    monkeypatch.setattr(openaddresses, "_fetch_json", fake_fetch_json)
    monkeypatch.setattr(openaddresses, "_load_source_item", fake_load_source_item)
    monkeypatch.setattr(openaddresses, "_emit_load_progress", lambda **_payload: None)

    stats = await openaddresses._load_openaddresses_data(  # pylint: disable=protected-access
        {"context": {"test_mode": True}},
        {"source_concurrency": 2, "test_file_limit": 3, "test_row_limit": 10},
        object,
    )

    assert stats == {"processed_files": 3, "processed_rows": 30, "accepted_rows": 15}
    assert max_active > 1


@pytest.mark.asyncio
async def test_openaddresses_task_import_id_controls_stage_suffix():
    ctx = {"context": {}, "import_date": "old"}

    await openaddresses.process_data(
        ctx,
        {"publish_only": True, "import_id": "oa-dev-2026/06/19"},
    )

    assert ctx["import_date"] == "oadev20260619"
    assert ctx["context"]["import_date"] == "oadev20260619"
    assert ctx["context"]["publish_only"] is True


@pytest.mark.asyncio
async def test_openaddresses_shutdown_uses_job_import_id_from_shared_context(monkeypatch):
    seen = {}

    async def fake_ensure_database(_test_mode):
        return None

    async def fake_table_exists(schema, table_name):
        seen["table_exists"] = (schema, table_name)
        return True

    async def fake_create_indexes(table_name, schema):
        seen["create_indexes"] = (schema, table_name)

    async def fake_refresh_archive_geocodes_from_openaddresses(**_kwargs):
        return openaddresses.OpenAddressesBackfillStats(exact_updates=0, fuzzy_updates=0, relaxed_updates=0)

    class FakeTransaction:
        async def __aenter__(self):
            return None

        async def __aexit__(self, *_exc):
            return False

    class FakeDb:
        async def scalar(self, _stmt, **_params):
            return 3

        async def execute_ddl(self, _stmt):
            return None

        async def status(self, _stmt, **_params):
            return None

        def transaction(self):
            return FakeTransaction()

    monkeypatch.setattr(openaddresses, "ensure_database", fake_ensure_database)
    monkeypatch.setattr(openaddresses, "_table_exists", fake_table_exists)
    monkeypatch.setattr(openaddresses, "_create_indexes", fake_create_indexes)
    monkeypatch.setattr(openaddresses, "refresh_archive_geocodes_from_openaddresses", fake_refresh_archive_geocodes_from_openaddresses)
    monkeypatch.setattr(openaddresses, "db", FakeDb())
    monkeypatch.setattr(openaddresses, "print_time_info", lambda _started_at: None)

    await openaddresses.shutdown(
        {
            "import_date": "startupwrong",
            "context": {
                "run": 1,
                "test_mode": True,
                "import_date": "oadev20260619",
            },
        }
    )

    assert seen["table_exists"] == ("mrf", "openaddresses_geocode_oadev20260619")
    assert seen["create_indexes"] == ("mrf", "openaddresses_geocode_oadev20260619")


@pytest.mark.asyncio
async def test_openaddresses_load_file_stops_when_control_run_cancelled(tmp_path):
    class FakeRedis:
        async def get(self, key):
            assert key == "cancel:run_1"
            return "1"

    path = tmp_path / "source.geojson"
    path.write_text(
        json.dumps(
            {
                "type": "Feature",
                "properties": {
                    "number": "123",
                    "street": "Main Street",
                    "region": "TX",
                    "postcode": "78701",
                },
                "geometry": {"type": "Point", "coordinates": [-97.7431, 30.2672]},
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ImportCancelledError):
        await openaddresses._load_file(  # pylint: disable=protected-access
            path,
            stage_cls=object,
            batch_size=5000,
            ctx={"redis": FakeRedis()},
            task={"run_id": "run_1"},
        )


def test_openaddresses_import_control_registration():
    adapter = control_imports._SINGLE_JOB_ADAPTERS["openaddresses"]  # pylint: disable=protected-access

    assert adapter["queue"] == "arq:OpenAddresses"
    assert adapter["target_module"] == "process.openaddresses"
    assert adapter["target_function"] == "process_data"
    assert "openaddresses" in control_imports._CANCELABLE_IMPORTERS  # pylint: disable=protected-access
