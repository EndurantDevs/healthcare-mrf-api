# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import os
import datetime
import uuid
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
    npi_row_map: dict[str, str] = {
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
        npi_row_map[f"Healthcare Provider Taxonomy Code_{idx}"] = ""
        npi_row_map[f"Provider License Number_{idx}"] = ""
        npi_row_map[f"Provider License Number State Code_{idx}"] = ""
        npi_row_map[f"Healthcare Provider Primary Taxonomy Switch_{idx}"] = ""
        npi_row_map[f"Healthcare Provider Taxonomy Group_{idx}"] = ""

    for idx in range(1, 51):
        npi_row_map[f"Other Provider Identifier_{idx}"] = ""
        npi_row_map[f"Other Provider Identifier Type Code_{idx}"] = ""
        npi_row_map[f"Other Provider Identifier State_{idx}"] = ""
        npi_row_map[f"Other Provider Identifier Issuer_{idx}"] = ""

    return npi_row_map


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


def test_index_requires_postgis_matches_geo_idx_and_expressions(npi_module):
    assert npi_module._index_requires_postgis(
        {
            "name": "geo_idx",
            "index_elements": ("Geography(ST_MakePoint(long, lat))",),
        }
    )
    assert npi_module._index_requires_postgis({"name": "pricing_proc_peer_stats_geo_idx"})
    assert not npi_module._index_requires_postgis({"name": "taxonomy_array", "index_elements": ("taxonomy_array",)})


def test_npi_requires_nucc_defaults_to_full_imports_only(monkeypatch, npi_module):
    monkeypatch.delenv("HLTHPRT_NPI_REQUIRE_NUCC", raising=False)
    monkeypatch.delenv("HLTHPRT_NPI_REQUIRE_NUCC_IN_TEST", raising=False)

    assert npi_module._npi_requires_nucc({}) is True
    assert npi_module._npi_requires_nucc({"test_mode": True}) is False

    monkeypatch.setenv("HLTHPRT_NPI_REQUIRE_NUCC", "0")
    assert npi_module._npi_requires_nucc({}) is False

    monkeypatch.setenv("HLTHPRT_NPI_REQUIRE_NUCC_IN_TEST", "1")
    assert npi_module._npi_requires_nucc({"test_mode": True}) is True


@pytest.mark.asyncio
async def test_assert_nucc_ready_rejects_missing_table(monkeypatch, npi_module):
    async def fake_scalar(_sql):
        return None

    monkeypatch.setattr(npi_module.db, "scalar", fake_scalar)

    with pytest.raises(npi_module.NPIPrerequisiteError, match="nucc_taxonomy"):
        await npi_module._assert_nucc_ready("mrf")


@pytest.mark.asyncio
async def test_assert_nucc_ready_rejects_empty_or_unusable_taxonomy(monkeypatch, npi_module):
    values = iter(["mrf.nucc_taxonomy", 883, 0])

    async def fake_scalar(_sql):
        return next(values)

    monkeypatch.setattr(npi_module.db, "scalar", fake_scalar)

    with pytest.raises(npi_module.NPIPrerequisiteError, match="pharmacist_rows=0"):
        await npi_module._assert_nucc_ready("mrf")


@pytest.mark.asyncio
async def test_assert_nucc_ready_accepts_populated_taxonomy(monkeypatch, npi_module):
    values = iter(["mrf.nucc_taxonomy", 883, 18])

    async def fake_scalar(_sql):
        return next(values)

    monkeypatch.setattr(npi_module.db, "scalar", fake_scalar)

    await npi_module._assert_nucc_ready("mrf")


@pytest.mark.asyncio
async def test_assert_nppes_canonical_ready_rejects_missing_sql_function(monkeypatch, npi_module):
    monkeypatch.setenv("HLTHPRT_ADDRESS_CANON_SOURCES", "nppes")

    async def fake_scalar(_sql):
        return None

    monkeypatch.setattr(npi_module.db, "scalar", fake_scalar)

    with pytest.raises(npi_module.NPIPrerequisiteError, match="addr_key_v1"):
        await npi_module._assert_nppes_canonical_ready("mrf")


@pytest.mark.asyncio
async def test_rebuild_phone_staffing_skips_missing_target(monkeypatch, npi_module):
    status_mock = AsyncMock()

    async def fake_scalar(_sql):
        return None

    monkeypatch.setattr(npi_module.db, "scalar", fake_scalar)
    monkeypatch.setattr(npi_module.db, "status", status_mock)

    await npi_module.rebuild_phone_staffing_table(
        target_table="npi_phone_staffing_20260603",
        address_table="npi_address_20260603",
        schema="mrf",
    )

    status_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_rebuild_phone_staffing_rejects_missing_nucc(monkeypatch, npi_module):
    values = iter(["mrf.npi_phone_staffing_20260603", "mrf.npi_address_20260603", None])

    async def fake_scalar(_sql):
        return next(values)

    monkeypatch.setattr(npi_module.db, "scalar", fake_scalar)
    monkeypatch.setattr(npi_module.db, "status", AsyncMock())

    with pytest.raises(npi_module.NPIPrerequisiteError, match="nucc_taxonomy"):
        await npi_module.rebuild_phone_staffing_table(
            target_table="npi_phone_staffing_20260603",
            address_table="npi_address_20260603",
            schema="mrf",
        )


@pytest.mark.asyncio
async def test_process_data_rejects_missing_nucc_before_download(monkeypatch, npi_module):
    monkeypatch.setenv("HLTHPRT_NPPES_DOWNLOAD_URL_DIR", "https://example.com/")
    monkeypatch.setenv("HLTHPRT_NPPES_DOWNLOAD_URL_FILE", "feed.html")
    monkeypatch.delenv("HLTHPRT_ADDRESS_CANON_SOURCES", raising=False)

    download_mock = AsyncMock()
    monkeypatch.setattr(npi_module, "download_it", download_mock)
    monkeypatch.setattr(npi_module, "ensure_database", AsyncMock())
    monkeypatch.setattr(npi_module.db, "status", AsyncMock())

    async def fake_scalar(_sql):
        return None

    monkeypatch.setattr(npi_module.db, "scalar", fake_scalar)

    worker_context_map = {
        "context": {},
        "redis": SimpleNamespace(enqueue_job=AsyncMock()),
        "import_date": "20251107",
    }

    with pytest.raises(npi_module.NPIPrerequisiteError, match="nucc_taxonomy"):
        await npi_module.process_data(worker_context_map)

    download_mock.assert_not_awaited()


@pytest.mark.asyncio
async def test_process_npi_chunk_enqueues_basic_payload(monkeypatch, npi_module):
    monkeypatch.delenv("HLTHPRT_ADDRESS_CANON_SOURCES", raising=False)

    fake_redis = SimpleNamespace(enqueue_job=AsyncMock())
    worker_context_map = {"redis": fake_redis, "import_date": "20251104"}

    npi_csv_map = {
        "NPI": "npi",
        "Entity Type Code": "entity_type_code",
        "Provider Organization Name (Legal Business Name)": "provider_organization_name",
    }
    npi_csv_map_reverse = {
        column_name: source_name
        for source_name, column_name in npi_csv_map.items()
    }

    npi_row_map = _build_minimal_row("1215387113")

    chunk_task_map = {
        "npi_csv_map": npi_csv_map,
        "npi_csv_map_reverse": npi_csv_map_reverse,
        "taxonomy_int_code_map": {"1223D0001X": 4101},
        "row_list": [npi_row_map],
    }

    await npi_module.process_npi_chunk(worker_context_map, chunk_task_map)

    fake_redis.enqueue_job.assert_awaited_once()
    enqueue_payload_map = fake_redis.enqueue_job.await_args.args[1]

    assert enqueue_payload_map["npi_obj_list"][0]["npi"] == 1215387113
    address_by_type = {
        entry["type"]: entry
        for entry in enqueue_payload_map["npi_address_list"]
    }
    assert address_by_type["primary"]["city_name"] == "AUSTIN"
    assert address_by_type["mail"]["first_line"] == "PO Box 1"


@pytest.mark.asyncio
async def test_process_npi_chunk_precomputes_address_key_when_enabled(monkeypatch, npi_module):
    monkeypatch.setenv("HLTHPRT_ADDRESS_CANON_SOURCES", "nppes")

    fake_redis = SimpleNamespace(enqueue_job=AsyncMock())
    worker_context_map = {"redis": fake_redis, "import_date": "20251104"}

    npi_csv_map = {
        "NPI": "npi",
        "Entity Type Code": "entity_type_code",
        "Provider Organization Name (Legal Business Name)": "provider_organization_name",
    }
    npi_csv_map_reverse = {
        column_name: source_name
        for source_name, column_name in npi_csv_map.items()
    }

    npi_row_map = _build_minimal_row("1215387113")

    await npi_module.process_npi_chunk(
        worker_context_map,
        {
            "npi_csv_map": npi_csv_map,
            "npi_csv_map_reverse": npi_csv_map_reverse,
            "row_list": [npi_row_map],
        },
    )

    enqueue_payload_map = fake_redis.enqueue_job.await_args.args[1]
    address_by_type = {
        entry["type"]: entry
        for entry in enqueue_payload_map["npi_address_list"]
    }
    assert address_by_type["primary"]["address_key"] == npi_module.address_key_v1(
        "123 Main St",
        "",
        "AUSTIN",
        "TX",
        "78701",
        "US",
    )
    assert address_by_type["mail"]["address_key"] == npi_module.address_key_v1(
        "PO Box 1",
        "",
        "AUSTIN",
        "TX",
        "78702",
        "US",
    )


@pytest.mark.asyncio
async def test_process_npi_chunk_batches_address_key_precompute(monkeypatch, npi_module):
    monkeypatch.setenv("HLTHPRT_ADDRESS_CANON_SOURCES", "nppes")

    seen_batches = []

    def fake_canonicalize_batch(address_rows):
        address_row_list = list(address_rows)
        seen_batches.append(address_row_list)
        return [
            {"address_key": "00000000-0000-4000-8000-000000000001"},
            {"address_key": "00000000-0000-4000-8000-000000000002"},
        ]

    monkeypatch.setattr(npi_module, "canonicalize_address_batch", fake_canonicalize_batch)
    fake_redis = SimpleNamespace(enqueue_job=AsyncMock())
    worker_context_map = {"redis": fake_redis, "import_date": "20251104"}

    npi_csv_map = {
        "NPI": "npi",
        "Entity Type Code": "entity_type_code",
        "Provider Organization Name (Legal Business Name)": "provider_organization_name",
    }
    npi_csv_map_reverse = {
        column_name: source_name
        for source_name, column_name in npi_csv_map.items()
    }

    await npi_module.process_npi_chunk(
        worker_context_map,
        {
            "npi_csv_map": npi_csv_map,
            "npi_csv_map_reverse": npi_csv_map_reverse,
            "row_list": [_build_minimal_row("1215387113")],
        },
    )

    enqueue_payload_map = fake_redis.enqueue_job.await_args.args[1]
    address_by_type = {
        entry["type"]: entry
        for entry in enqueue_payload_map["npi_address_list"]
    }
    assert len(seen_batches) == 1
    assert seen_batches[0] == [
        ("123 Main St", "", "AUSTIN", "TX", "78701", "US"),
        ("PO Box 1", "", "AUSTIN", "TX", "78702", "US"),
    ]
    assert address_by_type["primary"]["address_key"] == uuid.UUID("00000000-0000-4000-8000-000000000001")
    assert address_by_type["mail"]["address_key"] == uuid.UUID("00000000-0000-4000-8000-000000000002")


@pytest.mark.asyncio
async def test_process_npi_chunk_batches_contact_normalization(monkeypatch, npi_module):
    monkeypatch.delenv("HLTHPRT_ADDRESS_CANON_SOURCES", raising=False)

    seen_batches = []

    def fake_canonicalize_contact_batch(contact_rows):
        contact_row_list = list(contact_rows)
        seen_batches.append(contact_row_list)
        return [
            {
                "phone_number": "5125550100",
                "phone_extension": None,
                "fax_number_digits": None,
                "fax_extension": None,
            },
            {
                "phone_number": "5125550199",
                "phone_extension": None,
                "fax_number_digits": None,
                "fax_extension": None,
            },
        ]

    monkeypatch.setattr(npi_module, "canonicalize_contact_batch", fake_canonicalize_contact_batch)
    fake_redis = SimpleNamespace(enqueue_job=AsyncMock())
    worker_context_map = {"redis": fake_redis, "import_date": "20251104"}

    npi_csv_map = {
        "NPI": "npi",
        "Entity Type Code": "entity_type_code",
        "Provider Organization Name (Legal Business Name)": "provider_organization_name",
    }
    npi_csv_map_reverse = {
        column_name: source_name
        for source_name, column_name in npi_csv_map.items()
    }

    await npi_module.process_npi_chunk(
        worker_context_map,
        {
            "npi_csv_map": npi_csv_map,
            "npi_csv_map_reverse": npi_csv_map_reverse,
            "row_list": [_build_minimal_row("1215387113")],
        },
    )

    enqueue_payload_map = fake_redis.enqueue_job.await_args.args[1]
    address_by_type = {
        entry["type"]: entry
        for entry in enqueue_payload_map["npi_address_list"]
    }
    assert seen_batches == [
        [
            ("5125550100", "", "US"),
            ("5125550199", "", "US"),
        ]
    ]
    assert address_by_type["primary"]["phone_number"] == "5125550100"
    assert address_by_type["mail"]["phone_number"] == "5125550199"


@pytest.mark.asyncio
async def test_process_npi_chunk_populates_taxonomy_variants(monkeypatch, npi_module):
    monkeypatch.delenv("HLTHPRT_ADDRESS_CANON_SOURCES", raising=False)

    fake_redis = SimpleNamespace(enqueue_job=AsyncMock())
    worker_context_map = {"redis": fake_redis, "import_date": "20251105"}

    npi_csv_map = {
        "NPI": "npi",
        "Entity Type Code": "entity_type_code",
        "Provider Organization Name (Legal Business Name)": "provider_organization_name",
    }
    npi_csv_map_reverse = {
        column_name: source_name
        for source_name, column_name in npi_csv_map.items()
    }

    npi_row_map = _build_minimal_row("1415980663")
    npi_row_map["Entity Type Code"] = "<UNAVAIL>"
    npi_row_map["Last Update Date"] = "2024-01-15"
    npi_row_map["Healthcare Provider Taxonomy Code_1"] = "1223D0001X"
    npi_row_map["Healthcare Provider Primary Taxonomy Switch_1"] = "Y"
    npi_row_map["Provider License Number_1"] = "12345"
    npi_row_map["Provider License Number State Code_1"] = "TX"
    npi_row_map["Healthcare Provider Taxonomy Group_1"] = "Special Group"
    npi_row_map["Other Provider Identifier_1"] = "ALT123"
    npi_row_map["Other Provider Identifier Type Code_1"] = "05"
    npi_row_map["Other Provider Identifier State_1"] = "TX"
    npi_row_map["Other Provider Identifier Issuer_1"] = "Issuer"

    chunk_task_map = {
        "npi_csv_map": npi_csv_map,
        "npi_csv_map_reverse": npi_csv_map_reverse,
        "taxonomy_int_code_map": {"1223D0001X": 4101},
        "row_list": [npi_row_map],
    }

    await npi_module.process_npi_chunk(worker_context_map, chunk_task_map)

    fake_redis.enqueue_job.assert_awaited_once()
    enqueue_payload_map = fake_redis.enqueue_job.await_args.args[1]

    taxonomy_entry = enqueue_payload_map["npi_taxonomy_list"][0]
    assert taxonomy_entry["healthcare_provider_taxonomy_code"] == "1223D0001X"
    assert taxonomy_entry["provider_license_number_state_code"] == "TX"

    other_identifier = enqueue_payload_map["npi_other_id_list"][0]
    assert other_identifier["other_provider_identifier"] == "ALT123"

    taxonomy_group = enqueue_payload_map["npi_taxonomy_group_list"][0]
    assert taxonomy_group["healthcare_provider_taxonomy_group"] == "Special Group"

    address_by_type = {
        entry["type"]: entry
        for entry in enqueue_payload_map["npi_address_list"]
    }
    assert address_by_type["primary"]["taxonomy_array"] == [4101]
    assert address_by_type["mail"]["taxonomy_array"] == [4101]


@pytest.mark.asyncio
async def test_save_npi_data_dispatch(monkeypatch, npi_module):

    push_calls = []

    async def fake_push(objects, cls, rewrite=False):
        push_calls.append((cls.__tablename__, rewrite, objects))

    monkeypatch.setattr(npi_module, "push_objects", fake_push)
    monkeypatch.setattr(npi_module, "ensure_database", AsyncMock())

    worker_context_map = {"import_date": "20251106"}
    save_task_map = {
        "npi_obj_list": [{"npi": 1, "entity_type_code": 2}],
        "npi_taxonomy_list": [{"npi": 1, "checksum": 10}],
        "npi_other_id_list": [{"npi": 1, "checksum": 11}],
        "npi_taxonomy_group_list": [{"npi": 1, "checksum": 12}],
        "npi_address_list": [{"npi": 1, "checksum": 13, "type": "primary"}],
        "unexpected": [{"value": 99}],
    }

    await npi_module.save_npi_data(worker_context_map, save_task_map)

    rewrite_flags_by_table = {name: flag for name, flag, _ in push_calls}
    assert rewrite_flags_by_table == {
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
    monkeypatch.setattr(npi_module, "ensure_database", AsyncMock())
    monkeypatch.setattr(npi_module, "_ensure_required_extensions", AsyncMock())
    monkeypatch.setattr(npi_module, "_assert_nucc_ready", AsyncMock())
    monkeypatch.setattr(npi_module, "_assert_nppes_canonical_ready", AsyncMock())
    monkeypatch.setattr(npi_module, "_load_nucc_taxonomy_int_code_map", AsyncMock(return_value={}))
    monkeypatch.setattr(npi_module.db, "status", AsyncMock())

    worker_context_map = {
        "context": {},
        "redis": SimpleNamespace(enqueue_job=AsyncMock()),
        "import_date": "20251107",
    }

    await npi_module.process_data(worker_context_map)

    assert worker_context_map["context"]["run"] == 1
    download_mock.assert_awaited()


@pytest.mark.asyncio
async def test_process_data_failure_does_not_mark_run(monkeypatch, npi_module):

    monkeypatch.setenv("HLTHPRT_NPPES_DOWNLOAD_URL_DIR", "https://example.com/")
    monkeypatch.setenv("HLTHPRT_NPPES_DOWNLOAD_URL_FILE", "feed.html")

    monkeypatch.setattr(npi_module, "download_it", AsyncMock(side_effect=RuntimeError("boom")))
    monkeypatch.setattr(npi_module, "ensure_database", AsyncMock())
    monkeypatch.setattr(npi_module, "_ensure_required_extensions", AsyncMock())
    monkeypatch.setattr(npi_module, "_assert_nucc_ready", AsyncMock())
    monkeypatch.setattr(npi_module, "_assert_nppes_canonical_ready", AsyncMock())
    monkeypatch.setattr(npi_module, "_load_nucc_taxonomy_int_code_map", AsyncMock(return_value={}))
    monkeypatch.setattr(npi_module.db, "status", AsyncMock())

    worker_context_map = {
        "context": {},
        "redis": SimpleNamespace(enqueue_job=AsyncMock()),
        "import_date": "20251107",
    }

    with pytest.raises(RuntimeError):
        await npi_module.process_data(worker_context_map)

    assert worker_context_map["context"].get("run", 0) == 0


def test_nppes_listing_regex_is_v2_only(npi_module):
    html = """
    <a href="NPPES_Data_Dissemination_March_2026.zip">legacy</a>
    <a href="NPPES_Data_Dissemination_20260301_20260331_V2.zip">current</a>
    """
    matches = npi_module.re.findall(r'(NPPES_Data_Dissemination.*_V2.zip)', html)
    assert matches == ["NPPES_Data_Dissemination_20260301_20260331_V2.zip"]


@pytest.mark.asyncio
async def test_startup_initializes_tables(monkeypatch, npi_module):

    monkeypatch.delenv("HLTHPRT_IMPORT_ID_OVERRIDE", raising=False)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "testschema")

    my_init_mock = AsyncMock()
    monkeypatch.setattr(npi_module, "my_init_db", my_init_mock)
    monkeypatch.setattr(npi_module, "ensure_database", AsyncMock())

    make_mock = _fake_make_class_factory("testschema")
    monkeypatch.setattr(npi_module, "make_class", make_mock)

    create_mock = AsyncMock()
    status_mock = AsyncMock()
    monkeypatch.setattr(npi_module.db, "create_table", create_mock)
    monkeypatch.setattr(npi_module.db, "status", status_mock)

    startup_context_map: dict[str, object] = {}
    await npi_module.startup(startup_context_map)

    assert startup_context_map["import_date"]
    assert startup_context_map["context"]["run"] == 0
    my_init_mock.assert_awaited_once()
    assert create_mock.await_count >= 1
    assert status_mock.await_count >= 1


@pytest.mark.asyncio
async def test_startup_honors_import_id_override(monkeypatch, npi_module):

    monkeypatch.setenv("HLTHPRT_IMPORT_ID_OVERRIDE", "addrcanon_npi_timing")
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "testschema")

    monkeypatch.setattr(npi_module, "my_init_db", AsyncMock())
    monkeypatch.setattr(npi_module, "ensure_database", AsyncMock())
    monkeypatch.setattr(npi_module, "make_class", _fake_make_class_factory("testschema"))
    monkeypatch.setattr(npi_module.db, "create_table", AsyncMock())
    monkeypatch.setattr(npi_module.db, "status", AsyncMock())

    startup_context_map: dict[str, object] = {}
    await npi_module.startup(startup_context_map)

    assert startup_context_map["import_date"] == "addrcanon_npi_timing"


@pytest.mark.asyncio
async def test_shutdown_handles_rotation(monkeypatch, npi_module):

    """Verify shutdown handles rotation."""
    monkeypatch.setenv("DB_SCHEMA", "testschema")
    monkeypatch.setenv("HLTHPRT_ADDRESS_CANON_SOURCES", "nppes")
    monkeypatch.setattr(npi_module, "make_class", _fake_make_class_factory("testschema"))
    monkeypatch.setattr(npi_module, "print_time_info", lambda start: None)
    monkeypatch.setattr(npi_module, "ensure_database", AsyncMock())

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
    stamp_address_keys = AsyncMock()
    monkeypatch.setattr(npi_module, "stamp_address_keys", stamp_address_keys)
    monkeypatch.setattr(
        npi_module,
        "resolve_into_archive",
        AsyncMock(return_value=SimpleNamespace(staged=1, distinct_keys=1, inserted=1, elapsed_seconds=0.1)),
    )
    openaddresses_backfill = AsyncMock()
    monkeypatch.delenv("HLTHPRT_NPI_OPENADDRESSES_BACKFILL", raising=False)
    monkeypatch.setattr(npi_module, "refresh_archive_geocodes_from_openaddresses_sharded", openaddresses_backfill)
    progress_events: list[dict[str, object]] = []
    control_updates: list[tuple[str, dict[str, object]]] = []

    monkeypatch.setattr(npi_module, "enqueue_live_progress", lambda **payload: progress_events.append(payload))

    async def fake_mark_control_run(run_id, **kwargs):
        control_updates.append((run_id, kwargs))

    monkeypatch.setattr(npi_module, "mark_control_run", fake_mark_control_run)

    shutdown_context_map = {
        "context": {"run": 1, "start": datetime.datetime.utcnow(), "control_run_id": "npi-run-1"},
        "import_date": "20251108",
    }

    await npi_module.shutdown(shutdown_context_map)

    scalar_mock.assert_awaited()
    stamp_address_keys.assert_awaited()
    assert stamp_address_keys.await_args.kwargs["update_existing"] is False
    assert execute_mock.await_count >= 1
    assert status_mock.await_count >= 1
    assert control_updates[-1][0] == "npi-run-1"
    metrics = control_updates[-1][1]["metrics"]
    timings = metrics["npi_shutdown_phase_timings"]
    openaddresses_backfill.assert_not_awaited()
    assert metrics["openaddresses_backfill_enabled"] is False
    assert any(phase_timing["phase"] == "canonical_address_resolve" for phase_timing in timings)
    assert not any(phase_timing["phase"] == "openaddresses_archive_backfill" for phase_timing in timings)
    assert any(str(phase_timing["phase"]).startswith("vacuum_analyze:") for phase_timing in timings)
    assert any(str(phase_timing["phase"]).startswith("publish_swap:") for phase_timing in timings)
    assert all("elapsed_seconds" in phase_timing for phase_timing in timings)
    assert any(event.get("phase") == "npi shutdown canonical_address_resolve" for event in progress_events)


@pytest.mark.asyncio
async def test_resolve_npi_address_archive_skips_sql_stamp_when_keys_loaded(monkeypatch, npi_module):
    stamp_address_keys = AsyncMock()
    resolve_into_archive = AsyncMock(return_value=SimpleNamespace(staged=10, distinct_keys=5))

    monkeypatch.setattr(npi_module.db, "scalar", AsyncMock(return_value=0))
    monkeypatch.setattr(npi_module, "stamp_address_keys", stamp_address_keys)
    monkeypatch.setattr(npi_module, "resolve_into_archive", resolve_into_archive)

    stats = await npi_module.resolve_npi_address_archive(
        staging_table="npi_address_20260613",
        field_map={"first_line": "first_line"},
        schema="mrf",
        cancel_check=AsyncMock(),
    )

    assert stats.staged == 10
    stamp_address_keys.assert_not_awaited()
    resolve_into_archive.assert_awaited_once()


@pytest.mark.asyncio
async def test_resolve_npi_address_archive_uses_single_shard_for_small_missing_set(monkeypatch, npi_module):
    stamp_address_keys = AsyncMock()
    resolve_into_archive = AsyncMock(return_value=SimpleNamespace(staged=10, distinct_keys=5))

    monkeypatch.setenv("HLTHPRT_ADDRESS_CANON_NPI_SHARDS", "24")
    monkeypatch.setattr(npi_module.db, "scalar", AsyncMock(return_value=42))
    monkeypatch.setattr(npi_module, "stamp_address_keys", stamp_address_keys)
    monkeypatch.setattr(npi_module, "resolve_into_archive", resolve_into_archive)

    await npi_module.resolve_npi_address_archive(
        staging_table="npi_address_20260613",
        field_map={"first_line": "first_line"},
        schema="mrf",
        cancel_check=AsyncMock(),
    )

    stamp_address_keys.assert_awaited_once()
    assert stamp_address_keys.await_args.kwargs["shards"] == 1
    assert stamp_address_keys.await_args.kwargs["update_existing"] is False
    assert stamp_address_keys.await_args.kwargs["honor_env_override"] is False


@pytest.mark.asyncio
async def test_resolve_npi_address_archive_repairs_only_on_mismatch(monkeypatch, npi_module):
    stamp_address_keys = AsyncMock(return_value=7)
    resolve_into_archive = AsyncMock(
        side_effect=[
            RuntimeError(f"{npi_module.ADDRESS_KEY_MISMATCH_MESSAGE}: stale"),
            SimpleNamespace(staged=10, distinct_keys=5),
        ]
    )

    monkeypatch.setattr(npi_module.db, "scalar", AsyncMock(return_value=0))
    monkeypatch.setattr(npi_module, "stamp_address_keys", stamp_address_keys)
    monkeypatch.setattr(npi_module, "resolve_into_archive", resolve_into_archive)

    stats = await npi_module.resolve_npi_address_archive(
        staging_table="npi_address_20260613",
        field_map={"first_line": "first_line"},
        schema="mrf",
        cancel_check=AsyncMock(),
    )

    assert stats.staged == 10
    assert resolve_into_archive.await_count == 2
    stamp_address_keys.assert_awaited_once()
    assert stamp_address_keys.await_args.kwargs["update_existing"] is True


@pytest.mark.asyncio
async def test_main_enqueues_process_job(monkeypatch, npi_module):

    fake_pool = SimpleNamespace(enqueue_job=AsyncMock())

    monkeypatch.setattr(
        npi_module,
        "create_pool",
        AsyncMock(return_value=fake_pool),
    )

    monkeypatch.setattr(npi_module, "build_redis_settings", lambda: ("settings", "redis://localhost"))

    await npi_module.main()

    fake_pool.enqueue_job.assert_awaited_once_with("process_data", {"test_mode": False}, _queue_name="arq:NPI")


@pytest.mark.asyncio
async def test_main_enqueues_process_job_test_mode(monkeypatch, npi_module):

    fake_pool = SimpleNamespace(enqueue_job=AsyncMock())

    monkeypatch.setattr(
        npi_module,
        "create_pool",
        AsyncMock(return_value=fake_pool),
    )

    monkeypatch.setattr(npi_module, "build_redis_settings", lambda: ("settings", "redis://localhost"))

    await npi_module.main(test_mode=True)

    fake_pool.enqueue_job.assert_awaited_once_with("process_data", {"test_mode": True}, _queue_name="arq:NPI")
