# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""The CMS NPD provider address phase, its importer wiring and its unified-address source, without a database."""

import importlib
import json
import re

import pytest

from process import cms_npd_typed_address_source as address_source
from process import cms_npd_typed_addresses as addresses
from process import cms_npd_typed_import as typed_import
from process.cms_npd_typed_schema import release_schema_name
from tests import test_cms_npd_typed_run as run_tests
from tests.test_cms_npd_typed_run import OLD_ID, FakeConnection, recorded, run_import

native = importlib.import_module("process.entity_address_unified")
harness = run_tests.harness
REFRESH = addresses.REFRESH_KEY


class CountingConnection(FakeConnection):
    """Answers the stage row count."""

    async def fetchval(self, sql, *args):
        self._record(sql, args)
        return 3 if "cms_npd_address_stage" in sql else await super().fetchval(sql, *args)


async def _build(connection, monkeypatch, *, reader="api_reader"):
    if reader:
        monkeypatch.setenv("HLTHPRT_DB_READER_USER", reader)
    else:
        monkeypatch.delenv("HLTHPRT_DB_READER_USER", raising=False)

    async def resolve(stage_table, schema):
        connection.statements.append(f"RESOLVE {schema}.{stage_table}")

    return await addresses.build(connection, "mrf", "cms_npd_0123456789abcdef", "a" * 64, resolve=resolve)


async def test_build_stages_resolves_indexes_swaps_and_records_in_order(monkeypatch):
    connection = CountingConnection()
    assert await _build(connection, monkeypatch) == 3
    stage = '"mrf".cms_npd_address_stage'
    statements = connection.statements
    assert statements[:4] == ["BEGIN", f"DROP TABLE IF EXISTS {stage}", statements[2], statements[3]]
    assert statements[2].startswith(f"CREATE TABLE {stage} (") and statements[3].startswith(f"INSERT INTO {stage}")
    assert '"cms_npd_0123456789abcdef".practitioner_address' in statements[3]
    swap_start = statements.index("BEGIN", 1)
    assert statements[4:swap_start] == [
        "COMMIT",
        "RESOLVE mrf.cms_npd_address_stage",
        f"ALTER TABLE {stage} ADD CONSTRAINT cms_npd_address_stage_pkey PRIMARY KEY (npi, source_record_id)",
        f"CREATE INDEX cms_npd_address_stage_address_key ON {stage} (address_key) WHERE address_key IS NOT NULL",
        f"ANALYZE {stage}",
        f"SELECT count(*) FROM {stage}",
    ]
    assert statements[swap_start + 1 :] == [
        "SET LOCAL lock_timeout = '60s'",
        'DROP TABLE IF EXISTS "mrf".cms_npd_address_old',
        'ALTER TABLE IF EXISTS "mrf".cms_npd_address RENAME TO cms_npd_address_old',
        'ALTER INDEX IF EXISTS "mrf".cms_npd_address_pkey RENAME TO cms_npd_address_old_pkey',
        'ALTER INDEX IF EXISTS "mrf".cms_npd_address_address_key RENAME TO cms_npd_address_old_address_key',
        'ALTER TABLE IF EXISTS "mrf".cms_npd_address_stage RENAME TO cms_npd_address',
        'ALTER INDEX IF EXISTS "mrf".cms_npd_address_stage_pkey RENAME TO cms_npd_address_pkey',
        'ALTER INDEX IF EXISTS "mrf".cms_npd_address_stage_address_key RENAME TO cms_npd_address_address_key',
        'GRANT SELECT ON "mrf".cms_npd_address TO "api_reader"',
        "UPDATE \"mrf\".cms_npd_release SET row_counts = (row_counts - 'provider_address_refresh') || "
        "jsonb_build_object('provider_address', $1::bigint) WHERE status = 'current' AND release_id = $2",
        "COMMIT",
    ]
    assert connection.last_args == (3, "a" * 64)


async def test_build_without_reader_grants_nothing(monkeypatch):
    connection = CountingConnection()
    await _build(connection, monkeypatch, reader=None)
    assert not any(statement.startswith("GRANT") for statement in connection.statements)


def test_stage_sql_selects_work_physical_addresses_role_locations_and_npi_organizations():
    insert = " ".join(addresses.INSERT_STAGE.split())
    assert insert.count("a.use = 'work' AND a.type IN ('physical', 'both')") == 2
    assert "WHERE o.npi IS NOT NULL" in insert and "unnest(r.location_ids)" in insert
    assert "l.phones[1], l.faxes[1], l.latitude, l.longitude" in insert
    assert "SELECT DISTINCT ON (npi, address_hash)" in insert
    assert "ORDER BY npi, address_hash, (latitude IS NULL OR longitude IS NULL), kind_rank" in insert
    assert "'cms_npd:' || npi || ':' || address_hash" in insert
    assert not re.search(r"\b(REFERENCES|TRIGGER|FUNCTION)\b", addresses.CREATE_STAGE + insert, re.IGNORECASE)


def test_country_is_part_of_the_address_identity():
    insert = addresses.INSERT_STAGE
    assert insert.count(addresses.COUNTRY_CODE) == 2
    assert f"{addresses.COUNTRY_CODE} AS country_code" in insert
    assert f"5),\n                         {addresses.COUNTRY_CODE})) AS address_hash" in insert
    assert "IN ('', 'US', 'USA', 'UNITED STATES') THEN 'US'" in addresses.COUNTRY_CODE


def test_has_pending_refresh_until_admitted():
    assert addresses.has_pending_refresh({"row_counts": {"provider_address": 0}})
    assert addresses.has_pending_refresh({"row_counts": None})
    assert not addresses.has_pending_refresh({"row_counts": {"provider_address_refresh": 1}})


def test_has_pending_addresses_until_the_row_count_is_recorded():
    assert addresses.has_pending_addresses({"row_counts": {"practitioner": 1}})
    assert addresses.has_pending_addresses({"row_counts": None})
    assert not addresses.has_pending_addresses({"row_counts": {"provider_address": 0}})


async def test_archive_resolution_follows_the_canon_source_switch(monkeypatch):
    from process.ext import address_canon

    calls = []

    async def stamp(stage_table, field_map, *, schema):
        calls.append(("stamp", stage_table, field_map, schema))

    async def resolve(stage_table, field_map, *, source_bit, priority, schema):
        calls.append(("resolve", stage_table, source_bit, priority, schema))
        return "stats"

    monkeypatch.setattr(address_canon, "stamp_address_keys", stamp)
    monkeypatch.setattr(address_canon, "resolve_into_archive", resolve)
    monkeypatch.delenv("HLTHPRT_ADDRESS_CANON_SOURCES", raising=False)
    assert await addresses.resolve_archive("cms_npd_address_stage", "mrf") is None and calls == []
    monkeypatch.setenv("HLTHPRT_ADDRESS_CANON_SOURCES", "cms_doctors,cms_npd")
    assert await addresses.resolve_archive("cms_npd_address_stage", "mrf") == "stats"
    assert calls == [
        ("stamp", "cms_npd_address_stage", addresses.ADDRESS_FIELDS, "mrf"),
        ("resolve", "cms_npd_address_stage", 256, 7, "mrf"),
    ]


RECORD_REFRESH_SQL = (
    "UPDATE \"mrf\".cms_npd_release SET row_counts = row_counts || jsonb_build_object('provider_address_refresh', 1) "
    "WHERE status = 'current' AND release_id = $1"
)


async def test_refresh_enqueue_reports_queued_existing_and_failed():
    requests = []
    connection = FakeConnection()

    async def create_run(request):
        requests.append(request)
        return {"run_id": "run-9", "idempotency_key": request["idempotency_key"]}, len(requests) == 1

    queued = await addresses.enqueue_refresh(connection, "mrf", "b" * 64, create_run=create_run)
    assert queued == {"status": "queued", "run_id": "run-9"}
    assert (await addresses.enqueue_refresh(connection, "mrf", "b" * 64, create_run=create_run))["status"] == "existing"
    assert connection.statements == [RECORD_REFRESH_SQL] * 2 and connection.last_args == ("b" * 64,)
    assert requests[0]["params"] == {
        "refresh_mode": "full",
        "trigger_source_key": "cms-npd",
        "trigger_snapshot_id": "b" * 64,
    }
    assert requests[0]["importer"] == "entity-address-unified"
    assert requests[0]["idempotency_key"] == "entity-address-unified:cms-npd:" + "b" * 64

    async def failing(_request):
        raise RuntimeError("queue unavailable")

    failed = await addresses.enqueue_refresh(connection, "mrf", "b" * 64, create_run=failing)
    assert failed["status"] == "enqueue_failed" and failed["error"] == "queue unavailable"
    assert len(connection.statements) == 2


async def test_unrelated_active_run_defers():
    connection = FakeConnection()

    async def create_run(_request):
        return {"run_id": "run-other", "idempotency_key": "entity-address-unified:nightly"}, False

    deferred = await addresses.enqueue_refresh(connection, "mrf", "b" * 64, create_run=create_run)
    assert deferred == {"status": "deferred", "run_id": "run-other"} and connection.statements == []


async def test_default_enqueue_uses_the_control_plane(monkeypatch):
    import api.control_imports

    async def create_import_run(request):
        return {"run_id": request["import_id"]}, True

    monkeypatch.setattr(api.control_imports, "create_import_run", create_import_run)
    result = await addresses.enqueue_refresh(FakeConnection(), "mrf", "c" * 64)
    assert result == {"status": "queued", "run_id": "entity-address-unified:cms-npd:" + "c" * 16}


@pytest.fixture
def phase(monkeypatch):
    """Record address builds and refresh requests made by the importer."""
    calls = []

    async def build(connection, control, schema, release_id, *, resolve):
        calls.append(("build", control, schema, release_id, resolve))
        return 4

    async def enqueue(connection, control, release_id):
        calls.append(("enqueue", release_id))
        return {"status": "queued"}

    monkeypatch.setattr(addresses, "build", build)
    monkeypatch.setattr(addresses, "enqueue_refresh", enqueue)
    return calls


async def test_current_release_with_addresses_is_unchanged(harness, phase):
    release_id = harness["observed"].vector_sha256
    lock = FakeConnection(releases=[recorded(release_id, "current", {"provider_address": 2, REFRESH: 1})])
    summary = await run_import(harness, lock)
    assert summary["status"] == "unchanged" and "address_rows" not in summary
    assert set(summary["seconds"]) == {"observe"} and phase == []


async def test_missing_table_is_rebuilt(harness, phase):
    release_id = harness["observed"].vector_sha256
    current = recorded(release_id, "current", {"provider_address": 2, REFRESH: 1})
    lock = FakeConnection(releases=[current], address_table=False)
    summary = await run_import(harness, lock)
    assert summary["address_rows"] == 4 and [call[0] for call in phase] == ["build", "enqueue"]
    assert "SELECT to_regclass($1) IS NOT NULL" in lock.statements


async def test_unadmitted_refresh_retries_only_enqueue(harness, phase):
    release_id = harness["observed"].vector_sha256
    lock = FakeConnection(releases=[recorded(release_id, "current", {"provider_address": 2})])
    summary = await run_import(harness, lock)
    assert summary["address_refresh"] == {"status": "queued"} and "address_rows" not in summary
    assert set(summary["seconds"]) == {"observe"} and phase == [("enqueue", release_id)]


async def test_opt_out_leaves_refresh_pending(harness, phase):
    release_id = harness["observed"].vector_sha256
    lock = FakeConnection(releases=[recorded(release_id, "current", {"provider_address": 2})])
    summary = await typed_import.run(
        {},
        {"refresh_addresses": False},
        "run-1",
        source=harness["source"],
        connect=lambda: _connection(lock),
        scanner="scanner-bin",
    )
    assert summary["status"] == "unchanged" and "address_refresh" not in summary and phase == []


async def test_current_release_without_addresses_runs_only_the_address_phase(harness, phase):
    release_id = harness["observed"].vector_sha256
    lock = FakeConnection(releases=[recorded(release_id, "current", {"practitioner": 5})])
    summary = await run_import(harness, lock)
    assert summary["status"] == "unchanged" and summary["address_rows"] == 4
    assert summary["address_refresh"] == {"status": "queued"}
    assert set(summary["seconds"]) == {"observe", "addresses"} and harness["staged"] == []
    assert phase == [
        ("build", "mrf", release_schema_name(release_id), release_id, addresses.resolve_archive),
        ("enqueue", release_id),
    ]
    phases = [name for _, name, unit, *_ in harness["progress"] if unit == "phase"]
    assert phases == ["cms-npd-typed-observe", "cms-npd-typed-addresses"]


async def test_published_release_builds_addresses_after_publish_and_refresh_can_be_skipped(harness, phase):
    lock = FakeConnection(releases=[recorded(OLD_ID, "current", {"practitioner": 5})])

    async def resolve(stage_table, schema):
        return None

    summary = await typed_import.run(
        {},
        {"refresh_addresses": False},
        "run-1",
        source=harness["source"],
        connect=lambda: _connection(lock),
        scanner="scanner-bin",
        resolve_archive=resolve,
    )
    assert summary["status"] == "published" and summary["address_rows"] == 4
    assert "address_refresh" not in summary and "addresses_pending" not in summary
    assert list(summary["seconds"])[-2:] == ["publish", "addresses"]
    assert phase == [("build", "mrf", summary["schema"], summary["release_id"], resolve)]


async def _connection(connection):
    return connection


def _select_aliases(select_sql):
    """Output column names of one source select, in order."""
    projection = select_sql.split("SELECT", 1)[1].split("\n              FROM ", 1)[0]
    return [line.rsplit(" AS ", 1)[1].rstrip(",").strip() for line in projection.strip().splitlines()]


def _available(**tables):
    return {"npi": True, "npi_address": True, **tables}


def test_eau_source_matches_the_cms_doctors_row_shape_and_identity():
    selects = native._source_selects("sample", _available(doctor_clinician_address=True, cms_npd_address=True))
    doctors = next(sql for sql in selects if "'cms_doctors'::varchar AS address_source" in sql)
    cms = next(sql for sql in selects if "'cms_npd'::varchar AS address_source" in sql)
    assert _select_aliases(cms) == _select_aliases(doctors)
    assert "'npi'::varchar AS entity_type" in cms and "c.npi::varchar AS entity_id" in cms
    assert "'practice'::varchar AS type" in cms
    assert "FROM sample.cms_npd_address AS c" in cms and "LEFT JOIN sample.npi AS n ON n.npi = c.npi" in cms
    assert "c.source_record_id::varchar AS source_record_id" in cms
    assert "pa.npi = c.npi AND pa.type = 'primary'" in cms


def test_eau_source_absent_or_bounded():
    assert address_source.eau_source_selects(native, "sample", _available()) == []
    assert not any("cms_npd_address" in sql for sql in native._source_selects("sample", _available()))
    bare = address_source.eau_source_selects(native, "sample", {"cms_npd_address": True})[0]
    assert "ARRAY[0]::int[] AS taxonomy_array" in bare and "LEFT JOIN" not in bare
    limited = native._source_selects("sample", {"cms_npd_address": True}, test_limit_per_source=5)
    assert limited == [f"(\nSELECT * FROM (\n{bare.strip()}\n) AS src LIMIT 5\n)"]


def test_eau_label_rank_id_and_mask():
    assert "WHEN label = 'cms_npd' THEN 7 " in native._source_priority_expr("label")
    assert "WHEN label = 'cms_npd' THEN 9 " in native._source_id_expr("label")
    assert "WHEN label = 'cms_npd' THEN 256::bigint " in native._source_mask_expr("label")
    assert "source_id IN (1, 2, 3, 4, 5, 6, 8, 9) THEN source_mask" in native._enriched_raw_keyed_cte_sql(
        archive_available=True
    )
    prefiltered = native._prefilter_npi_source_select_sql(
        "sample", address_source.eau_source_selects(native, "sample", {"cms_npd_address": True})[0], "groups"
    )
    assert "ON affected_npi.entity_npi = c.npi" in prefiltered


def test_serving_provenance_maps_the_label_to_its_source_id():
    from api import ptg2_serving

    assert "WHEN sources.source_name = 'cms_npd' THEN 9" in ptg2_serving._ADDRESS_PROVENANCE_SQL_TEMPLATE


def test_release_rows_keep_the_recorded_address_count():
    row = recorded(OLD_ID, "current", {"provider_address": 7})
    assert not addresses.has_pending_addresses({**row, "row_counts": json.loads(row["row_counts"])})


def test_role_locations_skip_inactive_and_ended_roles():
    role_part = addresses._CANDIDATES.split("UNION ALL")[1]
    assert "r.active IS NOT FALSE" in role_part
    assert "r.period_end IS NULL OR r.period_end >= CURRENT_DATE" in role_part
