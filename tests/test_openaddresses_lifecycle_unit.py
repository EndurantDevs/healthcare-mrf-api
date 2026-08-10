import importlib
import asyncio
import json

import pytest

from process.control_cancel import ImportCancelledError


openaddresses = importlib.import_module("process.openaddresses")
control_imports = importlib.import_module("api.control_imports")

@pytest.mark.asyncio
async def test_openaddresses_task_import_id_controls_stage_suffix():
    context_by_field = {"context": {}, "import_date": "old"}

    await openaddresses.process_data(
        context_by_field,
        {"publish_only": True, "import_id": "oa-dev-2026/06/19"},
    )

    assert context_by_field["import_date"] == "oadev20260619"
    assert context_by_field["context"]["import_date"] == "oadev20260619"
    assert context_by_field["context"]["publish_only"] is True


@pytest.mark.asyncio
async def test_openaddresses_shutdown_uses_job_import_id_from_shared_context(monkeypatch):
    """Verify openaddresses shutdown uses job import id from shared context."""
    seen_by_field = {}

    async def fake_ensure_database(_test_mode):
        return None

    async def is_table_present(schema, table_name):
        seen_by_field["table_exists"] = (schema, table_name)
        return True

    async def fake_create_indexes(table_name, schema):
        seen_by_field["create_indexes"] = (schema, table_name)
    async def fake_refresh_archive_geocodes_sharded(**_kwargs):
        return openaddresses.OpenAddressesBackfillStats(exact_updates=0, fuzzy_updates=0, relaxed_updates=0)
    async def fake_restore_zip5_from_tiger(**_kwargs):
        return openaddresses.OpenAddressesZipRestoreStats()
    class FakeTransaction:
        async def __aenter__(self):
            return None
        async def __aexit__(self, *_exc):
            return False
    class FakeDb:
        async def scalar(self, stmt, **_params):
            if "WHERE zip5 IS NULL" in stmt:
                return 0
            return 3
        async def execute_ddl(self, _stmt):
            return None
        async def status(self, _stmt, **_params):
            return None
        def transaction(self):
            return FakeTransaction()
    monkeypatch.setattr(openaddresses, "ensure_database", fake_ensure_database)
    monkeypatch.setattr(openaddresses, "_is_table_present", is_table_present)
    monkeypatch.setattr(openaddresses, "_create_indexes", fake_create_indexes)
    monkeypatch.setattr(
        openaddresses,
        "refresh_archive_geocodes_from_openaddresses_sharded",
        fake_refresh_archive_geocodes_sharded,
    )
    monkeypatch.setattr(
        openaddresses,
        "restore_openaddresses_zips",
        fake_restore_zip5_from_tiger,
    )
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
    assert seen_by_field["table_exists"] == ("mrf", "openaddresses_geocode_oadev20260619")
    assert seen_by_field["create_indexes"] == ("mrf", "openaddresses_geocode_oadev20260619")


@pytest.mark.asyncio
async def test_openaddresses_shutdown_is_idempotent_after_stage_publish(monkeypatch):
    calls = []

    async def fake_ensure_database(_test_mode):
        calls.append("ensure_database")

    async def is_table_present(_schema, table_name):
        return table_name == openaddresses.OpenAddressesGeocode.__main_table__

    monkeypatch.setattr(openaddresses, "ensure_database", fake_ensure_database)
    monkeypatch.setattr(openaddresses, "_is_table_present", is_table_present)

    await openaddresses.shutdown(
        {
            "import_date": "oadev20260619",
            "context": {
                "run": 1,
                "openaddresses_stage_published": True,
            },
        }
    )

    assert calls == ["ensure_database"]


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
        await openaddresses._load_file(
            path,
            stage_cls=object,
            batch_size=5000,
            ctx={"redis": FakeRedis()},
            task={"run_id": "run_1"},
        )


def test_openaddresses_operator_registration():
    adapter = control_imports._SINGLE_JOB_ADAPTERS["openaddresses"]

    assert adapter["queue"] == "arq:OpenAddresses"
    assert adapter["target_module"] == "process.openaddresses"
    assert adapter["target_function"] == "process_data"
    assert "openaddresses" in control_imports._CANCELABLE_IMPORTERS


@pytest.mark.parametrize(
    ("raw_value", "expected_value"),
    [
        (None, 1.5),
        ("invalid", 1.5),
        ("0", 1.5),
        ("2.75", 2.75),
    ],
)
def test_openaddresses_float_environment_guard(
    monkeypatch,
    raw_value,
    expected_value,
):
    environment_name = "HLTHPRT_OPENADDRESSES_TEST_FLOAT"
    if raw_value is None:
        monkeypatch.delenv(environment_name, raising=False)
    else:
        monkeypatch.setenv(environment_name, raw_value)

    assert openaddresses._env_float(environment_name, 1.5) == expected_value


@pytest.mark.parametrize(
    ("raw_value", "default", "expected_value"),
    [
        (None, True, True),
        ("yes", False, True),
        ("off", True, False),
    ],
)
def test_openaddresses_boolean_environment_guard(
    monkeypatch,
    raw_value,
    default,
    expected_value,
):
    environment_name = "HLTHPRT_OPENADDRESSES_TEST_FLAG"
    if raw_value is None:
        monkeypatch.delenv(environment_name, raising=False)
    else:
        monkeypatch.setenv(environment_name, raw_value)

    assert (
        openaddresses._is_env_flag_enabled(environment_name, default)
        is expected_value
    )


def test_openaddresses_identifier_helpers_are_stable_and_bounded():
    short_identifier = "short_name"
    long_identifier = "table_" + ("x" * 100)

    assert openaddresses._bounded_identifier(short_identifier) == short_identifier
    assert len(openaddresses._bounded_identifier(long_identifier)) == 63
    assert len(openaddresses._archived_identifier(long_identifier)) == 63
    assert openaddresses._archived_identifier(long_identifier).endswith("_old")


@pytest.mark.parametrize(
    ("raw_status", "expected_count"),
    [
        (None, 0),
        (7, 7),
        ("12", 12),
        ("UPDATE 5", 5),
        ("unknown", 0),
    ],
)
def test_openaddresses_status_count_accepts_database_status_shapes(
    raw_status,
    expected_count,
):
    assert openaddresses._status_count(raw_status) == expected_count


def test_openaddresses_address_component_helpers_fail_closed():
    assert openaddresses._normalize_house_number(None) is None
    assert openaddresses._normalize_house_number("Building A-12") == "buildinga12"
    assert openaddresses._normalize_house_number("---") is None
    assert openaddresses._street_after_house(None) is None
    assert openaddresses._street_after_house("Main Street") == "Main Street"
    assert openaddresses._valid_us_coordinate("invalid", -90) is None
    assert openaddresses._valid_us_coordinate(90, -90) is None
    assert openaddresses._source_state("ca/example") is None
    assert openaddresses._source_updated("invalid") is None


def test_openaddresses_lookup_params_reject_incomplete_and_non_us_addresses():
    assert openaddresses.lookup_params_from_address({}) is None
    assert (
        openaddresses.lookup_params_from_address(
            {
                "first_line": "1 Main Street",
                "country_code": "CA",
                "state_name": "Ontario",
                "postal_code": "K1A 0B1",
            }
        )
        is None
    )
    assert (
            openaddresses.lookup_params_from_address(
                {
                    "first_line": "1 Main Street",
                    "country_code": "US",
                    "postal_code": "78701",
                }
            )
        is None
    )


def test_openaddresses_source_filter_and_task_bounds(monkeypatch):
    source_items = [
        {"source": "ca/on", "layer": "addresses"},
        {"source": "us/tx", "layer": "buildings"},
        {"source": "us/ca", "layer": "addresses", "output": {"output": False}},
        {"source": "us/ny", "layer": "addresses", "output": {"output": True}},
    ]
    monkeypatch.setenv(
        "HLTHPRT_OPENADDRESSES_LOCAL_FILES",
        "/tmp/first.geojson, /tmp/second.geojson",
    )

    assert openaddresses._us_data_items(source_items) == [source_items[3]]
    assert openaddresses._local_files_from_env() == [
        openaddresses.Path("/tmp/first.geojson"),
        openaddresses.Path("/tmp/second.geojson"),
    ]
    assert openaddresses._local_files_from_task_or_env(
        {"local_files": "/tmp/one.geojson, /tmp/two.geojson"}
    ) == [
        openaddresses.Path("/tmp/one.geojson"),
        openaddresses.Path("/tmp/two.geojson"),
    ]
    assert (
        openaddresses._task_or_env_int_range(
            {"workers": 99},
            "workers",
            "HLTHPRT_OPENADDRESSES_WORKERS",
            2,
            minimum=1,
            maximum=8,
        )
        == 8
    )
    assert openaddresses._is_task_or_env_flag_enabled(
        {"enabled": "yes"},
        "enabled",
        "HLTHPRT_OPENADDRESSES_ENABLED",
    )
    assert not openaddresses._is_task_or_env_flag_enabled(
        {"enabled": "off"},
        "enabled",
        "HLTHPRT_OPENADDRESSES_ENABLED",
        default=True,
    )
