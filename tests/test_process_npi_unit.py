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

def test_nppes_listing_regex_is_v2_only(npi_module):
    html = """
    <a href="NPPES_Data_Dissemination_March_2026.zip">legacy</a>
    <a href="NPPES_Data_Dissemination_20260301_20260331_V2.zip">current</a>
    """
    matches = npi_module.re.findall(r'(NPPES_Data_Dissemination.*_V2.zip)', html)
    assert matches == ["NPPES_Data_Dissemination_20260301_20260331_V2.zip"]

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
