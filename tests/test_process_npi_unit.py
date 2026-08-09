# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import importlib
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import os
import datetime
import uuid
from contextlib import asynccontextmanager

import pytest

from process.nppes_public_evidence_import import NPPES_RIGHTS_PROOF_SHA256

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


@pytest.mark.parametrize(
    ("raw_value", "expected"),
    [("7", 7), ("0", 3), ("-2", 3), ("invalid", 3)],
)
def test_npi_positive_integer_configuration_is_fail_safe(
    monkeypatch,
    npi_module,
    raw_value,
    expected,
):
    """Worker concurrency settings accept only positive integer overrides."""

    monkeypatch.setenv("HLTHPRT_NPI_TEST_LIMIT", raw_value)
    assert npi_module._env_positive_int("HLTHPRT_NPI_TEST_LIMIT", 3) == expected


def test_npi_archived_identifier_stays_inside_postgres_limit(npi_module):
    """Long archive names are deterministic and remain valid PostgreSQL identifiers."""

    assert npi_module._archived_identifier("npi") == "npi_old"
    archived_name = npi_module._archived_identifier("npi_" + "x" * 80)
    assert len(archived_name) == npi_module.POSTGRES_IDENTIFIER_MAX_LENGTH
    assert archived_name.endswith("_old")


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
async def test_assert_nppes_canonical_ready_validates_each_archive_prerequisite(
    monkeypatch,
    npi_module,
):
    """Canonical NPPES mode fails closed on missing archive table or key column."""

    monkeypatch.delenv("HLTHPRT_ADDRESS_CANON_SOURCES", raising=False)
    scalar = AsyncMock()
    monkeypatch.setattr(npi_module.db, "scalar", scalar)
    await npi_module._assert_nppes_canonical_ready("mrf")
    scalar.assert_not_awaited()

    monkeypatch.setenv("HLTHPRT_ADDRESS_CANON_SOURCES", "nppes")
    scalar.side_effect = ["addr_key_v1", None]
    with pytest.raises(npi_module.NPIPrerequisiteError, match="address_archive"):
        await npi_module._assert_nppes_canonical_ready("mrf")

    scalar.side_effect = ["addr_key_v1", "address_archive_v2", False]
    with pytest.raises(npi_module.NPIPrerequisiteError, match="address_key"):
        await npi_module._assert_nppes_canonical_ready("mrf")

    scalar.side_effect = ["addr_key_v1", "address_archive_v2", True]
    await npi_module._assert_nppes_canonical_ready("mrf")


def test_npi_address_and_contact_helpers_preserve_empty_and_canonical_shapes(
    monkeypatch,
    npi_module,
):
    """Address helpers preserve empty input and attach typed canonical keys."""

    expected_key = uuid.uuid4()
    monkeypatch.setattr(
        npi_module,
        "canonicalize_address_batch",
        lambda _rows: [{"address_key": str(expected_key)}],
    )
    address_map = {"first_line": "123 Main", "country_code": "US"}

    assert npi_module.is_test_mode({"context": {"test_mode": True}})
    assert not npi_module.is_test_mode({})
    assert npi_module._attach_npi_contact_fields([]) == []
    assert npi_module._attach_all_npi_address_keys(
        [address_map.copy()],
        canonical_enabled=False,
    ) == [address_map]
    attached = npi_module._attach_npi_address_key(
        address_map.copy(),
        canonical_enabled=True,
    )
    assert attached["address_key"] == expected_key


@pytest.mark.asyncio
async def test_npi_taxonomy_code_map_requires_a_table_and_filters_bad_rows(
    monkeypatch,
    npi_module,
):
    """Taxonomy mapping returns only complete code-to-integer pairs."""

    scalar = AsyncMock(return_value=None)
    all_rows = AsyncMock()
    monkeypatch.setattr(npi_module.db, "scalar", scalar)
    monkeypatch.setattr(npi_module.db, "all", all_rows)
    assert await npi_module._load_nucc_taxonomy_int_code_map("mrf") == {}
    all_rows.assert_not_awaited()

    scalar.return_value = "mrf.nucc_taxonomy"
    all_rows.return_value = [
        ("207Q00000X", 41),
        None,
        (None, 42),
        ("207R00000X", None),
    ]
    assert await npi_module._load_nucc_taxonomy_int_code_map("mrf") == {
        "207Q00000X": 41
    }


def test_npi_taxonomy_array_is_distinct_sorted_and_fail_safe(npi_module):
    """Address taxonomy arrays retain known distinct integers or the zero sentinel."""

    npi_row_map = {
        "Healthcare Provider Taxonomy Code_1": "B",
        "Healthcare Provider Taxonomy Code_2": "A",
        "Healthcare Provider Taxonomy Code_3": "B",
        "Healthcare Provider Taxonomy Code_4": "UNKNOWN",
        "Healthcare Provider Taxonomy Code_5": "",
    }

    assert npi_module._taxonomy_array_from_npi_row(npi_row_map, {"A": 1, "B": 2}) == [1, 2]
    assert npi_module._taxonomy_array_from_npi_row(npi_row_map, {"C": 3}) == [0]
    assert npi_module._taxonomy_array_from_npi_row(npi_row_map, None) == [0]
    full_npi_row_map = {
        f"Healthcare Provider Taxonomy Code_{index}": "A"
        for index in range(1, 16)
    }
    assert npi_module._taxonomy_array_from_npi_row(full_npi_row_map, {"A": 1}) == [1]


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
async def test_rebuild_phone_staffing_rejects_missing_address_or_pharmacist_rows(
    monkeypatch,
    npi_module,
):
    """Phone staffing requires both the address stage and usable pharmacist taxonomy."""

    scalar = AsyncMock(
        side_effect=[
            "target",
            None,
            "target",
            "address",
            "nucc",
            0,
        ]
    )
    status = AsyncMock()
    monkeypatch.setattr(npi_module.db, "scalar", scalar)
    monkeypatch.setattr(npi_module.db, "status", status)

    await npi_module.rebuild_phone_staffing_table(
        target_table="npi_phone_staffing_stage",
        address_table="npi_address_stage",
        schema="mrf",
    )
    with pytest.raises(npi_module.NPIPrerequisiteError, match="has no Pharmacist rows"):
        await npi_module.rebuild_phone_staffing_table(
            target_table="npi_phone_staffing_stage",
            address_table="npi_address_stage",
            schema="mrf",
        )

    status.assert_not_awaited()


@pytest.mark.asyncio
async def test_process_data_rejects_missing_nucc_before_download(monkeypatch, npi_module):
    monkeypatch.setenv("HLTHPRT_NPPES_DOWNLOAD_URL_DIR", "https://example.com/")
    monkeypatch.setenv("HLTHPRT_NPPES_DOWNLOAD_URL_FILE", "feed.html")
    monkeypatch.delenv("HLTHPRT_ADDRESS_CANON_SOURCES", raising=False)

    download_mock = AsyncMock()
    monkeypatch.setattr(npi_module, "download_it", download_mock)
    monkeypatch.setattr(npi_module, "ensure_database", AsyncMock())
    monkeypatch.setattr(npi_module.db, "status", AsyncMock())
    acquire_lease = AsyncMock()
    assert_lease = AsyncMock()
    release_lease = AsyncMock()
    monkeypatch.setattr(npi_module, "_acquire_npi_import_lease", acquire_lease)
    monkeypatch.setattr(npi_module, "_assert_npi_import_lease", assert_lease)
    monkeypatch.setattr(npi_module, "_release_npi_import_lease", release_lease)

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

    acquire_lease.assert_awaited_once()
    assert_lease.assert_awaited_once()
    release_lease.assert_awaited_once()
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
        "Employer Identification Number (EIN)": "employer_identification_number",
        "Parent Organization TIN": "parent_organization_tin",
    }
    npi_csv_map_reverse = {
        column_name: source_name
        for source_name, column_name in npi_csv_map.items()
    }

    npi_row_map = _build_minimal_row("1215387113")
    npi_row_map["Employer Identification Number (EIN)"] = "private-ein-sentinel"
    npi_row_map["Parent Organization TIN"] = "private-tin-sentinel"

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
    assert enqueue_payload_map["npi_obj_list"][0]["employer_identification_number"] is None
    assert enqueue_payload_map["npi_obj_list"][0]["parent_organization_tin"] is None
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

    call_order_events: list[str] = []

    async def empty_listing(*_args, **_kwargs):
        call_order_events.append("listing")
        return ""

    download_mock = AsyncMock(side_effect=empty_listing)
    monkeypatch.setattr(npi_module, "download_it", download_mock)
    monkeypatch.setattr(npi_module, "ensure_database", AsyncMock())
    monkeypatch.setattr(npi_module, "_ensure_required_extensions", AsyncMock())
    monkeypatch.setattr(npi_module, "_assert_nucc_ready", AsyncMock())
    monkeypatch.setattr(npi_module, "_assert_nppes_canonical_ready", AsyncMock())
    monkeypatch.setattr(npi_module, "_load_nucc_taxonomy_int_code_map", AsyncMock(return_value={}))
    monkeypatch.setattr(npi_module, "_prepare_npi_staging", AsyncMock())
    monkeypatch.setattr(npi_module.db, "status", AsyncMock())
    acquire_lease = AsyncMock(
        side_effect=lambda _context: call_order_events.append("lease")
    )
    assert_lease = AsyncMock(
        side_effect=lambda _context: call_order_events.append("assert")
    )
    release_lease = AsyncMock()
    monkeypatch.setattr(npi_module, "_acquire_npi_import_lease", acquire_lease)
    monkeypatch.setattr(npi_module, "_assert_npi_import_lease", assert_lease)
    monkeypatch.setattr(npi_module, "_release_npi_import_lease", release_lease)

    worker_context_map = {
        "context": {},
        "redis": SimpleNamespace(enqueue_job=AsyncMock()),
        "import_date": "20251107",
    }

    with pytest.raises(
        npi_module.NPIPrerequisiteError,
        match="No NPPES source archives",
    ):
        await npi_module.process_data(worker_context_map)

    assert worker_context_map["context"]["run"] == 0
    download_mock.assert_awaited()
    npi_module._prepare_npi_staging.assert_not_awaited()
    assert call_order_events[:3] == ["lease", "assert", "listing"]
    release_lease.assert_awaited_once_with(
        worker_context_map["context"],
        suppress_errors=True,
    )


@pytest.mark.asyncio
async def test_process_data_required_test_mode_cannot_write_immutable_evidence(
    monkeypatch,
    npi_module,
):
    monkeypatch.setenv("HLTHPRT_NPPES_PUBLIC_EVIDENCE_MODE", "required")
    monkeypatch.setenv(
        "HLTHPRT_NPPES_RIGHTS_PROOF_SHA256",
        NPPES_RIGHTS_PROOF_SHA256,
    )
    prepare_chain = AsyncMock()
    import_chain = AsyncMock()
    ensure_database = AsyncMock()
    monkeypatch.setattr(npi_module, "prepare_nppes_release_chain", prepare_chain)
    monkeypatch.setattr(
        npi_module,
        "import_nppes_public_evidence_chain",
        import_chain,
    )
    monkeypatch.setattr(npi_module, "ensure_database", ensure_database)
    worker_context_map = {
        "context": {},
        "redis": SimpleNamespace(enqueue_job=AsyncMock()),
        "import_date": "20260809",
    }

    with pytest.raises(
        npi_module.NPIPrerequisiteError,
        match="cannot admit immutable public evidence",
    ):
        await npi_module.process_data(worker_context_map, {"test_mode": True})

    assert worker_context_map["context"]["run"] == 0
    prepare_chain.assert_not_awaited()
    import_chain.assert_not_awaited()
    ensure_database.assert_not_awaited()


@pytest.mark.asyncio
async def test_npi_import_lease_is_session_owned_and_released(
    monkeypatch,
    npi_module,
):
    connection = SimpleNamespace(
        fetchval=AsyncMock(side_effect=[True, 731, 731, True, True])
    )
    exit_events: list[tuple[object, object, object]] = []

    @asynccontextmanager
    async def lease_manager():
        try:
            yield connection
        finally:
            exit_events.append((None, None, None))

    manager = lease_manager()
    monkeypatch.setattr(npi_module.db, "acquire_driver", lambda: manager)
    worker_context_by_key: dict[str, object] = {}
    await npi_module._acquire_npi_import_lease(worker_context_by_key)
    await npi_module._assert_npi_import_lease(worker_context_by_key)
    with pytest.raises(npi_module.NPIPrerequisiteError, match="already held"):
        await npi_module._acquire_npi_import_lease(worker_context_by_key)
    await npi_module._release_npi_import_lease(
        worker_context_by_key,
        suppress_errors=False,
    )
    assert npi_module._NPI_IMPORT_LEASE_KEY not in worker_context_by_key
    assert connection.fetchval.await_count == 5
    assert len(exit_events) == 1


@pytest.mark.asyncio
async def test_npi_import_lease_rejects_a_missing_advisory_lock(npi_module):
    connection = SimpleNamespace(
        fetchval=AsyncMock(side_effect=[941, False])
    )
    worker_context_by_key = {
        npi_module._NPI_IMPORT_LEASE_KEY: npi_module._NpiImportLease(
            manager=object(),
            connection=connection,
            backend_pid=941,
        )
    }

    with pytest.raises(npi_module.NPIPrerequisiteError, match="lease was lost"):
        await npi_module._assert_npi_import_lease(worker_context_by_key)

    lock_query = connection.fetchval.await_args_list[1].args[0]
    assert "pg_catalog.pg_locks" in lock_query
    for required_fragment in (
        "held_lock.granted",
        "held_lock.mode = 'ExclusiveLock'",
        "held_lock.database",
        "current_database()",
        "held_lock.classid",
        "held_lock.objid",
        "held_lock.objsubid = 1",
        "held_lock.pid = pg_backend_pid()",
    ):
        assert required_fragment in lock_query


@pytest.mark.asyncio
async def test_nppes_runtime_accepts_the_proved_postgres_configuration(npi_module):
    connection = SimpleNamespace(
        fetchrow=AsyncMock(return_value=(180002, "on", "on", "on", "pglz"))
    )
    worker_context_by_key = {
        npi_module._NPI_IMPORT_LEASE_KEY: npi_module._NpiImportLease(
            manager=object(),
            connection=connection,
            backend_pid=941,
        )
    }

    await npi_module._assert_nppes_postgres_runtime(worker_context_by_key)

    assert "current_setting('wal_compression')" in connection.fetchrow.await_args.args[0]


@pytest.mark.parametrize(
    "settings",
    (
        (170999, "on", "on", "on", "pglz"),
        (180002, "off", "on", "on", "pglz"),
        (180002, "on", "off", "on", "pglz"),
        (180002, "on", "on", "off", "pglz"),
        (180002, "on", "on", "on", "off"),
    ),
)
@pytest.mark.asyncio
async def test_nppes_runtime_rejects_unproved_postgres_settings(
    npi_module,
    settings,
):
    connection = SimpleNamespace(fetchrow=AsyncMock(return_value=settings))
    worker_context_by_key = {
        npi_module._NPI_IMPORT_LEASE_KEY: npi_module._NpiImportLease(
            manager=object(),
            connection=connection,
            backend_pid=941,
        )
    }

    with pytest.raises(
        npi_module.NPIPrerequisiteError,
        match="durability configuration is invalid",
    ):
        await npi_module._assert_nppes_postgres_runtime(worker_context_by_key)


@pytest.mark.asyncio
async def test_nppes_catalog_preflight_uses_the_lease_connection(
    monkeypatch,
    npi_module,
):
    connection = object()
    worker_context_by_key = {
        npi_module._NPI_IMPORT_LEASE_KEY: npi_module._NpiImportLease(
            manager=object(),
            connection=connection,
            backend_pid=941,
        )
    }
    assert_catalog = AsyncMock()
    monkeypatch.setattr(npi_module, "assert_nppes_admission_catalog", assert_catalog)

    await npi_module._assert_nppes_storage_catalog(
        worker_context_by_key,
        "mrf",
    )

    assert_catalog.assert_awaited_once_with(connection, "mrf")


@pytest.mark.asyncio
async def test_npi_import_lease_rejects_a_parallel_attempt_and_closes_connection(
    monkeypatch,
    npi_module,
):
    connection = SimpleNamespace(fetchval=AsyncMock(side_effect=[False, 811]))
    exit_events: list[None] = []

    @asynccontextmanager
    async def lease_manager():
        try:
            yield connection
        finally:
            exit_events.append(None)

    monkeypatch.setattr(npi_module.db, "acquire_driver", lease_manager)
    worker_context_by_key: dict[str, object] = {}
    with pytest.raises(npi_module.NPIPrerequisiteError, match="already active"):
        await npi_module._acquire_npi_import_lease(worker_context_by_key)
    assert npi_module._NPI_IMPORT_LEASE_KEY not in worker_context_by_key
    assert exit_events == [None]


@pytest.mark.asyncio
async def test_staged_write_failure_cancels_and_drains_its_sibling(npi_module):
    sibling_started = asyncio.Event()

    async def waiting_write() -> None:
        sibling_started.set()
        await asyncio.Event().wait()

    async def failing_write() -> None:
        await sibling_started.wait()
        raise RuntimeError("synthetic write failure")

    waiting_task = asyncio.create_task(waiting_write())
    failing_task = asyncio.create_task(failing_write())
    owned_tasks = [waiting_task, failing_task]
    with pytest.raises(RuntimeError, match="synthetic write failure"):
        await npi_module._drain_npi_save_tasks(owned_tasks)
    assert waiting_task.cancelled()
    assert failing_task.done()
    assert owned_tasks == []


@pytest.mark.asyncio
async def test_controlled_test_mode_fails_before_database_or_staging(
    monkeypatch,
    npi_module,
):
    ensure_database = AsyncMock()
    staging_reset = AsyncMock()
    release_lease = AsyncMock()
    monkeypatch.setattr(npi_module, "ensure_database", ensure_database)
    monkeypatch.setattr(npi_module, "_prepare_npi_staging", staging_reset)
    monkeypatch.setattr(npi_module, "_release_npi_import_lease", release_lease)
    worker_context_by_key = {
        "context": {"control_run_id": "run_synthetic"},
        "import_date": "20260809",
    }
    with pytest.raises(npi_module.NPIPrerequisiteError, match="isolated publication"):
        await npi_module.process_data(
            worker_context_by_key,
            {"test_mode": True, "run_id": "run_synthetic"},
        )
    ensure_database.assert_not_awaited()
    staging_reset.assert_not_awaited()
    assert worker_context_by_key["context"]["run"] == 0


@pytest.mark.asyncio
async def test_shutdown_rejects_test_mode_before_database_or_publication(
    monkeypatch,
    npi_module,
):
    assert_lease = AsyncMock()
    release_lease = AsyncMock()
    ensure_database = AsyncMock()
    monkeypatch.setattr(npi_module, "_assert_npi_import_lease", assert_lease)
    monkeypatch.setattr(npi_module, "_release_npi_import_lease", release_lease)
    monkeypatch.setattr(npi_module, "ensure_database", ensure_database)
    worker_context_map = {
        "context": {
            "run": 1,
            "test_mode": True,
            "control_run_id": "run_test_mode",
            "_control_attempt_id": "run_test_mode:" + "a" * 32,
            "_control_attempt_started_at": "2026-08-09T00:00:00.000000+00:00",
        },
        "import_date": "20260809",
    }
    with pytest.raises(npi_module.NPIPrerequisiteError, match="cannot publish"):
        await npi_module.shutdown(worker_context_map)
    assert_lease.assert_awaited_once()
    ensure_database.assert_not_awaited()
    release_lease.assert_awaited_once()


@pytest.mark.asyncio
async def test_shutdown_normalizes_missing_required_evidence_receipt(
    monkeypatch,
    npi_module,
):
    monkeypatch.setenv("HLTHPRT_NPPES_PUBLIC_EVIDENCE_MODE", "required")
    monkeypatch.setenv(
        "HLTHPRT_NPPES_RIGHTS_PROOF_SHA256",
        NPPES_RIGHTS_PROOF_SHA256,
    )
    monkeypatch.setattr(npi_module, "_assert_npi_import_lease", AsyncMock())
    monkeypatch.setattr(npi_module, "_release_npi_import_lease", AsyncMock())
    ensure_database = AsyncMock()
    monkeypatch.setattr(npi_module, "ensure_database", ensure_database)
    worker_context_map = {
        "context": {
            "run": 1,
            "test_mode": False,
            "control_run_id": "run_missing_receipt",
            "_control_attempt_id": "run_missing_receipt:" + "b" * 32,
            "_control_attempt_started_at": "2026-08-09T00:00:00.000000+00:00",
        },
        "import_date": "20260809",
    }
    with pytest.raises(npi_module.NPIPrerequisiteError) as caught:
        await npi_module.shutdown(worker_context_map)
    assert str(caught.value) == "NPPES public-evidence admission receipt is invalid"
    assert caught.value.__cause__ is None
    assert caught.value.__context__ is None
    ensure_database.assert_not_awaited()


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
    monkeypatch.setattr(npi_module, "_acquire_npi_import_lease", AsyncMock())
    monkeypatch.setattr(npi_module, "_assert_npi_import_lease", AsyncMock())
    monkeypatch.setattr(npi_module, "_release_npi_import_lease", AsyncMock())

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
    staging_reset = AsyncMock()
    monkeypatch.setattr(npi_module, "_prepare_npi_staging", staging_reset)

    startup_context_map: dict[str, object] = {}
    await npi_module.startup(startup_context_map)

    assert startup_context_map["import_date"]
    assert startup_context_map["context"]["run"] == 0
    my_init_mock.assert_awaited_once()
    assert create_mock.await_count >= 1
    assert status_mock.await_count >= 1
    staging_reset.assert_not_awaited()


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


class _ShutdownRawConnection:
    def __init__(self, count_by_stage: dict[str, int]):
        self.count_by_stage = count_by_stage
        self.events: list[str] = []

    @asynccontextmanager
    async def transaction(self):
        self.events.append("transaction:begin")
        try:
            yield self
        except BaseException:
            self.events.append("transaction:rollback")
            raise
        self.events.append("transaction:commit")

    async def execute(self, statement: str, *_args):
        self.events.append(statement)
        return "OK"

    async def fetchval(self, statement: str, *_args):
        self.events.append(statement)
        if "count(*)::bigint" in statement:
            return next(
                count
                for stage_name, count in self.count_by_stage.items()
                if stage_name in statement
            )
        return 1


class _AmbiguousPublicationConnection:
    @asynccontextmanager
    async def transaction(self):
        yield self
        raise RuntimeError("synthetic ambiguous commit")


def _shutdown_stage_classes(npi_module):
    return tuple(
        SimpleNamespace(__main_table__=table_name, __tablename__=table_name)
        for table_name in npi_module.NPI_CANONICAL_TABLES
    )


def _install_shutdown_success_collaborators(monkeypatch, npi_module, raw_connection):
    monkeypatch.setenv("DB_SCHEMA", "testschema")
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "testschema")
    monkeypatch.setattr(npi_module, "_NPI_STAGING_CLASSES", _shutdown_stage_classes(npi_module))
    monkeypatch.setattr(npi_module, "make_class", _fake_make_class_factory("testschema"))
    monkeypatch.setattr(npi_module, "source_enabled", lambda _source: False)
    monkeypatch.setattr(npi_module, "_npi_requires_nucc", lambda _context: False)
    monkeypatch.setattr(npi_module, "_nppes_evidence_runtime_config", lambda _context: SimpleNamespace(required=True))
    monkeypatch.setattr(npi_module, "_required_nppes_evidence_receipt", lambda _context: SimpleNamespace(chain_ref="penpc1_" + "a" * 43))
    monkeypatch.setattr(npi_module, "_nppes_evidence_metrics", lambda _receipt: {"status": "complete"})
    monkeypatch.setattr(npi_module, "ensure_database", AsyncMock())
    monkeypatch.setattr(npi_module, "_assert_npi_import_lease", AsyncMock())
    monkeypatch.setattr(npi_module, "_release_npi_import_lease", AsyncMock())
    monkeypatch.setattr(npi_module, "raise_if_cancelled", AsyncMock())
    monkeypatch.setattr(npi_module, "lock_npi_publication_attempt", AsyncMock())
    monkeypatch.setattr(npi_module, "canonical_relation_oids", AsyncMock(return_value=(11, 12, 13, 14, 15, 16)))
    publication_receipt = SimpleNamespace(publication_ref="nppub1_" + "b" * 43)
    monkeypatch.setattr(npi_module, "insert_npi_publication_receipt", AsyncMock(return_value=publication_receipt))
    monkeypatch.setattr(npi_module, "npi_publication_metrics", lambda receipt: {"publication_ref": receipt.publication_ref})
    publication_commit = npi_module.NpiCanonicalPublicationCommit(
        publication_receipt,
        "2026-08-09T02:03:04.000000+00:00",
        "2026-08-09T02:03:04.000000+00:00",
    )
    monkeypatch.setattr(
        npi_module,
        "mark_npi_publication_succeeded",
        AsyncMock(return_value=publication_commit),
    )
    monkeypatch.setattr(npi_module, "print_time_info", lambda _start: None)
    monkeypatch.setattr(npi_module.db, "scalar", AsyncMock(return_value=6_000_000))
    monkeypatch.setattr(npi_module.db, "status", AsyncMock())
    monkeypatch.setattr(npi_module.db, "execute_ddl", AsyncMock())
    monkeypatch.setattr(npi_module.func, "count", lambda _value: "count")
    monkeypatch.setattr(npi_module, "select", lambda *_values: "count query")

    @asynccontextmanager
    async def database_transaction():
        yield SimpleNamespace()

    monkeypatch.setattr(npi_module.db, "transaction", database_transaction)
    return publication_receipt


@pytest.mark.asyncio
async def test_publication_transaction_reconciles_only_an_exact_commit(
    monkeypatch,
    npi_module,
):
    receipt = SimpleNamespace(publication_ref="nppub1_" + "d" * 43)
    commit = npi_module.NpiCanonicalPublicationCommit(
        receipt,
        "2026-08-09T02:03:04.000000+00:00",
        "2026-08-09T02:03:04.000000+00:00",
    )
    lease = npi_module._NpiImportLease(
        object(),
        _AmbiguousPublicationConnection(),
        731,
    )
    state_by_name = {
        "commit": commit,
        "progress": {"phase": "npi published"},
        "metrics": {"npi_canonical_publication": {"publication_ref": receipt.publication_ref}},
    }
    reconcile = AsyncMock(return_value=commit)
    monkeypatch.setattr(
        npi_module,
        "_reconcile_npi_commit_after_error",
        reconcile,
    )
    committed_context_by_name = {}

    async with npi_module._npi_publication_transaction(
        lease=lease,
        schema="testschema",
        context=committed_context_by_name,
        publication_state_by_name=state_by_name,
    ):
        state_by_name["first_body_entered"] = True
    assert state_by_name["commit"] == commit
    assert committed_context_by_name["control_run_terminal_committed"] is True
    reconcile.assert_awaited_once()

    reconcile.return_value = None
    with pytest.raises(RuntimeError, match="npi_canonical_publication_invalid"):
        async with npi_module._npi_publication_transaction(
            lease=lease,
            schema="testschema",
            context={},
            publication_state_by_name=state_by_name,
        ):
            state_by_name["second_body_entered"] = True


@pytest.mark.asyncio
async def test_shutdown_handles_rotation(monkeypatch, npi_module):
    """Seal stage census, table rotation, receipt, and terminal state together."""
    stage_count_by_table = {
        f"{table_name}_20251108": ordinal
        for ordinal, table_name in enumerate(npi_module.NPI_CANONICAL_TABLES, 1)
    }
    raw_connection = _ShutdownRawConnection(stage_count_by_table)
    publication_receipt = _install_shutdown_success_collaborators(
        monkeypatch,
        npi_module,
        raw_connection,
    )
    lease = npi_module._NpiImportLease(object(), raw_connection, 731)
    shutdown_context_map = {
        "context": {
            "run": 1,
            "start": datetime.datetime.utcnow(),
            "control_run_id": "npi-run-1",
            "_control_attempt_id": "npi-run-1:" + "c" * 32,
            "_control_attempt_started_at": "2026-08-09T00:00:00.000000+00:00",
            npi_module._NPI_IMPORT_LEASE_KEY: lease,
        },
        "import_date": "20251108",
    }
    shutdown_result_by_name = await npi_module.shutdown(shutdown_context_map)

    receipt_mock = npi_module.insert_npi_publication_receipt
    publication_input = receipt_mock.await_args.kwargs["publication_input"]
    assert publication_input.row_counts == (1, 2, 3, 4, 5, 6)
    assert publication_input.relation_oids == (11, 12, 13, 14, 15, 16)
    npi_module.mark_npi_publication_succeeded.assert_awaited_once()
    npi_module.raise_if_cancelled.assert_awaited()
    first_swap = next(
        index for index, event in enumerate(raw_connection.events)
        if "DROP TABLE IF EXISTS testschema.npi_old" in event
    )
    final_count = max(
        index for index, event in enumerate(raw_connection.events)
        if "count(*)::bigint" in event
    )
    assert final_count < first_swap
    assert raw_connection.events[-1] == "transaction:commit"
    assert shutdown_context_map["context"][npi_module._NPI_CONTROL_TERMINAL_COMMITTED_KEY] is True
    assert shutdown_context_map["context"][
        npi_module._NPI_CONTROL_COMMITTED_FINISHED_AT_KEY
    ] == "2026-08-09T02:03:04.000000+00:00"
    assert shutdown_result_by_name["npi_canonical_publication"][
        "publication_ref"
    ] == publication_receipt.publication_ref
    npi_module._release_npi_import_lease.assert_awaited_once_with(
        shutdown_context_map["context"],
        suppress_errors=True,
    )


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
async def test_main_creates_one_controlled_import_run(monkeypatch, npi_module):
    control_imports = importlib.import_module("api.control_imports")
    create_run = AsyncMock(
        return_value=({"run_id": "run_npi", "status": "queued"}, True)
    )
    ensure_table = AsyncMock()
    monkeypatch.setattr(control_imports, "create_import_run", create_run)
    monkeypatch.setattr(control_imports, "ensure_import_run_table", ensure_table)

    result = await npi_module.main()

    assert result == {"run_id": "run_npi", "status": "queued"}
    ensure_table.assert_awaited_once_with()
    create_run.assert_awaited_once_with(
        {
            "importer": "npi",
            "params": {},
            "triggered_by": "manual",
        }
    )


@pytest.mark.asyncio
async def test_main_rejects_live_queue_test_mode(monkeypatch, npi_module):
    with pytest.raises(ValueError, match="isolated database"):
        await npi_module.main(test_mode=True)
