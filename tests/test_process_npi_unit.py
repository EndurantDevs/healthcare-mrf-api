import importlib
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import os
import datetime
from contextlib import asynccontextmanager

import pytest

os.environ.setdefault("HLTHPRT_REDIS_ADDRESS", "redis://localhost")

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in __import__("sys").path:
    __import__("sys").path.insert(0, str(ROOT))

pytest.importorskip("sqlalchemy")
pytest.importorskip("asyncpg")


@pytest.fixture
def npi_module():
    return importlib.import_module("process.npi")


def _build_minimal_row(npi: str) -> dict[str, str]:
    row: dict[str, str] = {
        "NPI": npi,
        "Entity Type Code": "2",
        "Provider Organization Name (Legal Business Name)": "Example Org",
        "Provider First Line Business Practice Location Address": "123 Main St",
        "Provider Second Line Business Practice Location Address": "",
        "Provider Business Practice Location Address City Name": "Austin",
        "Provider Business Practice Location Address State Name": "TX",
        "Provider Business Practice Location Address Postal Code": "78701",
        "Provider Business Practice Location Address Country Code (If outside U.S.)": "US",
        "Provider Business Practice Location Address Telephone Number": "5125550100",
        "Provider Business Practice Location Address Fax Number": "",
        "Provider First Line Business Mailing Address": "PO Box 1",
        "Provider Second Line Business Mailing Address": "",
        "Provider Business Mailing Address City Name": "Austin",
        "Provider Business Mailing Address State Name": "TX",
        "Provider Business Mailing Address Postal Code": "78702",
        "Provider Business Mailing Address Country Code (If outside U.S.)": "US",
        "Provider Business Mailing Address Telephone Number": "5125550199",
        "Provider Business Mailing Address Fax Number": "",
        "Last Update Date": "",
    }

    for idx in range(1, 16):
        row[f"Healthcare Provider Taxonomy Code_{idx}"] = ""
        row[f"Provider License Number_{idx}"] = ""
        row[f"Provider License Number State Code_{idx}"] = ""
        row[f"Healthcare Provider Primary Taxonomy Switch_{idx}"] = ""
        row[f"Healthcare Provider Taxonomy Group_{idx}"] = ""

    for idx in range(1, 51):
        row[f"Other Provider Identifier_{idx}"] = ""
        row[f"Other Provider Identifier Type Code_{idx}"] = ""
        row[f"Other Provider Identifier State_{idx}"] = ""
        row[f"Other Provider Identifier Issuer_{idx}"] = ""

    return row


def _fake_make_class_factory(schema: str = "mrf"):
    def _factory(base_cls, suffix):
        table_name = f"{base_cls.__tablename__}_{suffix}"
        return SimpleNamespace(
            __main_table__=getattr(base_cls, "__main_table__", base_cls.__tablename__),
            __tablename__=table_name,
            __table__=SimpleNamespace(name=table_name, schema=schema),
            __my_index_elements__=list(getattr(base_cls, "__my_index_elements__", [])),
            __my_additional_indexes__=list(getattr(base_cls, "__my_additional_indexes__", [])),
            __my_initial_indexes__=list(getattr(base_cls, "__my_initial_indexes__", [])),
            npi=SimpleNamespace(),
        )

    return _factory


@pytest.mark.asyncio
async def test_process_npi_chunk_enqueues_basic_payload(monkeypatch, npi_module):

    fake_redis = SimpleNamespace(enqueue_job=AsyncMock())
    ctx = {"redis": fake_redis, "import_date": "20251104"}

    npi_csv_map = {
        "NPI": "npi",
        "Entity Type Code": "entity_type_code",
        "Provider Organization Name (Legal Business Name)": "provider_organization_name",
    }
    npi_csv_map_reverse = {value: key for key, value in npi_csv_map.items()}

    row = _build_minimal_row("1215387113")

    task = {
        "npi_csv_map": npi_csv_map,
        "npi_csv_map_reverse": npi_csv_map_reverse,
        "row_list": [row],
    }

    await npi_module.process_npi_chunk(ctx, task)

    fake_redis.enqueue_job.assert_awaited_once()
    payload = fake_redis.enqueue_job.await_args.args[1]

    assert payload["npi_obj_list"][0]["npi"] == 1215387113
    address_by_type = {entry["type"]: entry for entry in payload["npi_address_list"]}
    assert address_by_type["primary"]["city_name"] == "AUSTIN"
    assert address_by_type["mail"]["first_line"] == "PO Box 1"


@pytest.mark.asyncio
async def test_process_npi_chunk_populates_taxonomy_variants(monkeypatch, npi_module):

    fake_redis = SimpleNamespace(enqueue_job=AsyncMock())
    ctx = {"redis": fake_redis, "import_date": "20251105"}

    npi_csv_map = {
        "NPI": "npi",
        "Entity Type Code": "entity_type_code",
        "Provider Organization Name (Legal Business Name)": "provider_organization_name",
    }
    npi_csv_map_reverse = {value: key for key, value in npi_csv_map.items()}

    row = _build_minimal_row("1415980663")
    row["Entity Type Code"] = "<UNAVAIL>"
    row["Last Update Date"] = "2024-01-15"
    row["Healthcare Provider Taxonomy Code_1"] = "1223D0001X"
    row["Healthcare Provider Primary Taxonomy Switch_1"] = "Y"
    row["Provider License Number_1"] = "12345"
    row["Provider License Number State Code_1"] = "TX"
    row["Healthcare Provider Taxonomy Group_1"] = "Special Group"
    row["Other Provider Identifier_1"] = "ALT123"
    row["Other Provider Identifier Type Code_1"] = "05"
    row["Other Provider Identifier State_1"] = "TX"
    row["Other Provider Identifier Issuer_1"] = "Issuer"

    task = {
        "npi_csv_map": npi_csv_map,
        "npi_csv_map_reverse": npi_csv_map_reverse,
        "row_list": [row],
    }

    await npi_module.process_npi_chunk(ctx, task)

    fake_redis.enqueue_job.assert_awaited_once()
    payload = fake_redis.enqueue_job.await_args.args[1]

    taxonomy_entry = payload["npi_taxonomy_list"][0]
    assert taxonomy_entry["healthcare_provider_taxonomy_code"] == "1223D0001X"
    assert taxonomy_entry["provider_license_number_state_code"] == "TX"

    other_identifier = payload["npi_other_id_list"][0]
    assert other_identifier["other_provider_identifier"] == "ALT123"

    taxonomy_group = payload["npi_taxonomy_group_list"][0]
    assert taxonomy_group["healthcare_provider_taxonomy_group"] == "Special Group"


@pytest.mark.asyncio
async def test_save_npi_data_dispatch(monkeypatch, npi_module):

    push_calls = []

    async def fake_push(objects, cls, rewrite=False):
        push_calls.append((cls.__tablename__, rewrite, objects))

    monkeypatch.setattr(npi_module, "push_objects", fake_push)

    ctx = {"import_date": "20251106"}
    task = {
        "npi_obj_list": [{"npi": 1, "entity_type_code": 2}],
        "npi_taxonomy_list": [{"npi": 1, "checksum": 10}],
        "npi_other_id_list": [{"npi": 1, "checksum": 11}],
        "npi_taxonomy_group_list": [{"npi": 1, "checksum": 12}],
        "npi_address_list": [{"npi": 1, "checksum": 13, "type": "primary"}],
        "unexpected": [{"value": 99}],
    }

    await npi_module.save_npi_data(ctx, task)

    table_flags = {name: flag for name, flag, _ in push_calls}
    assert table_flags == {
        "npi_20251106": True,
        "npi_taxonomy_20251106": True,
        "npi_other_identifier_20251106": False,
        "npi_taxonomy_group_20251106": True,
        "npi_address_20251106": True,
    }


@pytest.mark.asyncio
async def test_process_data_no_remote_files(monkeypatch, npi_module):

    monkeypatch.setenv("HLTHPRT_NPPES_DOWNLOAD_URL_DIR", "https://example.com/")
    monkeypatch.setenv("HLTHPRT_NPPES_DOWNLOAD_URL_FILE", "feed.html")

    download_mock = AsyncMock(return_value="")
    monkeypatch.setattr(npi_module, "download_it", download_mock)

    ctx = {"context": {}, "redis": SimpleNamespace(enqueue_job=AsyncMock()), "import_date": "20251107"}

    await npi_module.process_data(ctx)

    assert ctx["context"]["run"] == 1
    download_mock.assert_awaited()


@pytest.mark.asyncio
async def test_startup_initializes_tables(monkeypatch, npi_module):

    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "testschema")

    my_init_mock = AsyncMock()
    monkeypatch.setattr(npi_module, "my_init_db", my_init_mock)

    make_mock = _fake_make_class_factory("testschema")
    monkeypatch.setattr(npi_module, "make_class", make_mock)

    create_mock = AsyncMock()
    status_mock = AsyncMock()
    monkeypatch.setattr(npi_module.db, "create_table", create_mock)
    monkeypatch.setattr(npi_module.db, "status", status_mock)

    ctx: dict[str, object] = {}
    await npi_module.startup(ctx)

    assert ctx["import_date"]
    assert ctx["context"]["run"] == 0
    my_init_mock.assert_awaited_once()
    assert create_mock.await_count >= 1
    assert status_mock.await_count >= 1


@pytest.mark.asyncio
async def test_shutdown_handles_rotation(monkeypatch, npi_module):

    monkeypatch.setenv("DB_SCHEMA", "testschema")
    monkeypatch.setattr(npi_module, "make_class", _fake_make_class_factory("testschema"))
    monkeypatch.setattr(npi_module, "print_time_info", lambda start: None)

    scalar_mock = AsyncMock(return_value=6_000_000)
    status_mock = AsyncMock()
    execute_mock = AsyncMock()
    monkeypatch.setattr(npi_module.db, "scalar", scalar_mock)
    monkeypatch.setattr(npi_module.db, "status", status_mock)
    monkeypatch.setattr(npi_module.db, "execute_ddl", execute_mock)

    @asynccontextmanager
    async def dummy_tx():
        yield SimpleNamespace()

    monkeypatch.setattr(npi_module.db, "transaction", lambda: dummy_tx())

    class DummyInsert:
        def values(self, *args, **kwargs):
            return self

        def on_conflict_do_update(self, **kwargs):
            return self

        async def status(self):
            return None

    monkeypatch.setattr(npi_module.db, "insert", lambda *args, **kwargs: DummyInsert())
    monkeypatch.setattr(npi_module.db, "func", SimpleNamespace(now=lambda: "NOW"))

    ctx = {
        "context": {"run": 1, "start": datetime.datetime.utcnow()},
        "import_date": "20251108",
    }

    await npi_module.shutdown(ctx)

    scalar_mock.assert_awaited()
    assert execute_mock.await_count >= 1
    assert status_mock.await_count >= 1


@pytest.mark.asyncio
async def test_main_enqueues_process_job(monkeypatch, npi_module):

    fake_pool = SimpleNamespace(enqueue_job=AsyncMock())

    monkeypatch.setenv("HLTHPRT_REDIS_ADDRESS", "redis://localhost")
    monkeypatch.setattr(
        npi_module,
        "create_pool",
        AsyncMock(return_value=fake_pool),
    )

    class DummyRedisSettings:
        @staticmethod
        def from_dsn(dsn):
            return ("settings", dsn)

    monkeypatch.setattr(npi_module, "RedisSettings", DummyRedisSettings)

    await npi_module.main()

    fake_pool.enqueue_job.assert_awaited_once_with("process_data", _queue_name="arq:NPI")
