# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Stage, finish, check and publish synthetic CMS NPD releases through the real encoder and PostgreSQL."""

import json
import os
from uuid import uuid4

import asyncpg
import pytest

from process.cms_npd_typed_addresses import enqueue_refresh
from process.cms_npd_typed_import import LOCK_KEY, run
from process.cms_npd_typed_stage import CmsNpdTypedImportError
from process.ptg_parts.rust_scanner import _ptg2_rust_scanner_binary
from tests.test_cms_npd_typed_import import LocalSource, write_release

DSN_ENV = "HLTHPRT_CMS_NPD_TYPED_IMPORT_TEST_DSN"
SCANNER = _ptg2_rust_scanner_binary()
pytestmark = [
    pytest.mark.skipif(not os.getenv(DSN_ENV), reason=f"{DSN_ENV} names a disposable PostgreSQL database"),
    pytest.mark.skipif(
        SCANNER is None,
        reason="build support/ptg2_scanner first (cargo build --release --manifest-path support/ptg2_scanner/Cargo.toml)",
    ),
]


async def _connect():
    return await asyncpg.connect(os.environ[DSN_ENV])


@pytest.fixture
async def control(monkeypatch):
    """A fresh, empty control schema (the importer creates its release table); drops every schema it recorded."""
    schema = "cms_npd_import_ctl_" + uuid4().hex[:12]
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.delenv("HLTHPRT_DB_READER_USER", raising=False)
    admin = await _connect()
    await admin.execute(f'CREATE SCHEMA "{schema}"')
    try:
        yield admin, schema
    finally:
        for row in await admin.fetch(f'SELECT schema_name FROM "{schema}".cms_npd_release'):
            await admin.execute(f'DROP SCHEMA IF EXISTS "{row["schema_name"]}" CASCADE')
        await admin.execute(f'DROP SCHEMA "{schema}" CASCADE')
        await admin.close()


async def _import(source, resolve_archive=None):
    task_by_field = {"refresh_addresses": False}
    return await run(
        {}, task_by_field, "", source=source, connect=_connect, scanner=SCANNER, resolve_archive=resolve_archive
    )


async def _statuses(admin, control_schema):
    rows = await admin.fetch(f'SELECT release_id, status FROM "{control_schema}".cms_npd_release')
    return {row["release_id"]: row["status"] for row in rows}


async def _schema_exists(admin, schema):
    return await admin.fetchval("SELECT to_regnamespace($1) IS NOT NULL", schema)


async def test_releases_publish_roll_and_retire(control, tmp_path):
    admin, control_schema = control
    first = LocalSource(tmp_path / "r1", write_release(tmp_path / "r1", 2, "2026-01-01"))
    published = await _import(first)
    first_id, first_schema = published["release_id"], published["schema"]
    assert published["status"] == "published" and published["previous_release_id"] is None
    assert published["row_counts"] == {
        "practitioner": 2,
        "practitioner_address": 2,
        "practitioner_role": 2,
        "organization": 3,
        "organization_address": 2,
        "location": 2,
        "endpoint": 2,
        "organization_affiliation": 2,
        "healthcare_service": 2,
        "insurance_plan": 2,
        "network": 1,
    }
    assert published["unresolved_counts"]["endpoint.organization"] == 2
    assert sum(published["unresolved_counts"].values()) == 2
    assert set(published["seconds"]) == {"observe", "download", "stage", "finish", "check", "publish", "addresses"}
    assert await admin.fetchval(f'SELECT count(*) FROM "{first_schema}".practitioner_role') == 2
    assert await admin.fetchval(f'SELECT count(*) FROM "{first_schema}".network') == 1
    typed_references = f"""SELECT
        (SELECT count(*) FROM "{first_schema}".organization WHERE other_type_codes = '{{ein}}'),
        (SELECT count(*) FROM "{first_schema}".location WHERE part_of = id),
        (SELECT count(*) FROM "{first_schema}".insurance_plan WHERE administered_by = 'net-1'
            AND cardinality(coverage_area_ids) = 1 AND jsonb_array_length(nested_references) = 3),
        (SELECT count(*) FROM "{first_schema}".practitioner_role WHERE network_ids = '{{net-1}}'
            AND cardinality(insurance_plan_ids) = 1),
        (SELECT count(*) FROM "{first_schema}".practitioner WHERE npi_explicit)"""
    assert tuple(await admin.fetchrow(typed_references)) == (2, 2, 2, 2, 0)
    recorded = await admin.fetchrow(f'SELECT * FROM "{control_schema}".cms_npd_release')
    assert (recorded["release_id"], recorded["status"], recorded["generated_on"].isoformat()) == (
        first_id,
        "current",
        "2026-01-01",
    )
    assert json.loads(recorded["files"])["06-Practitioner.ndjson"]["etag"] == '"etag-06-Practitioner.ndjson"'
    assert json.loads(recorded["row_counts"]) == {**published["row_counts"], "provider_address": 0}
    assert first.kept == {first_id}

    unchanged = await _import(first)
    assert (unchanged["status"], unchanged["release_id"]) == ("unchanged", first_id)

    second = await _import(LocalSource(tmp_path / "r2", write_release(tmp_path / "r2", 3, "2026-02-01")))
    assert second["previous_release_id"] == first_id and second["dropped_release_id"] is None
    assert await _statuses(admin, control_schema) == {first_id: "previous", second["release_id"]: "current"}
    assert await _schema_exists(admin, first_schema)

    third_source = LocalSource(tmp_path / "r3", write_release(tmp_path / "r3", 4, "2026-03-01"))
    third = await _import(third_source)
    assert third["dropped_release_id"] == first_id
    assert await _statuses(admin, control_schema) == {second["release_id"]: "previous", third["release_id"]: "current"}
    assert not await _schema_exists(admin, first_schema)
    assert third_source.kept == {second["release_id"], third["release_id"]}
    assert await admin.fetchval(f'SELECT count(*) FROM "{third["schema"]}".practitioner') == 4


async def _assert_failed_run(admin, control_schema, source, code, current_id):
    with pytest.raises(CmsNpdTypedImportError) as error:
        await _import(source)
    assert error.value.control_error_code == code
    assert await _statuses(admin, control_schema) == {current_id: "current"}
    candidate = "cms_npd_" + source.observed.vector_sha256[:16]
    assert not await _schema_exists(admin, candidate)


async def test_failed_runs_drop_their_candidate_and_keep_the_current_release(control, tmp_path):
    admin, control_schema = control
    current = await _import(LocalSource(tmp_path / "good", write_release(tmp_path / "good", 5)))
    current_id = current["release_id"]

    bad = LocalSource(tmp_path / "bad", write_release(tmp_path / "bad", 5, "2026-02-01", bad_type="Location"))
    await _assert_failed_run(admin, control_schema, bad, "cms_npd_typed_scanner_failed", current_id)

    small = LocalSource(tmp_path / "small", write_release(tmp_path / "small", 1, "2026-03-01"))
    await _assert_failed_run(admin, control_schema, small, "cms_npd_typed_table_shrunk", current_id)

    holder = await _connect()
    try:
        assert await holder.fetchval("SELECT pg_try_advisory_lock($1)", LOCK_KEY)
        busy = LocalSource(tmp_path / "busy", write_release(tmp_path / "busy", 6, "2026-04-01"))
        await _assert_failed_run(admin, control_schema, busy, "cms_npd_typed_import_in_progress", current_id)
    finally:
        await holder.close()


def _address(line, use="work", kind="physical"):
    return {"use": use, "type": kind, "line": [line], "city": "Exampleville", "state": "ZZ", "postalCode": "00001-1234"}


def _with_addresses(resource_type, index, record):
    """Practitioner, role location and NPI organization addresses, with excluded and duplicate entries."""
    if resource_type == "Practitioner":
        address_list = [
            _address("10 Main St"),
            _address("10 MAIN ST."),  # the same normalized address
            _address("20 Oak Ave", kind="both"),
            _address("40 Mail Box Rd", kind="postal"),
            _address("50 Billing Way", use="billing"),
        ]
        return {**record, "address": address_list}
    if resource_type == "Location":
        location_by_field = {key: value for key, value in _address("10 Main St").items() if key not in {"use", "type"}}
        position_by_field = {"longitude": -70.5 - index, "latitude": 40.25}
        telecom_list = [{"system": "phone", "value": "555-0100"}, {"system": "fax", "value": "555-0101"}]
        return {**record, "address": location_by_field, "position": position_by_field, "telecom": telecom_list}
    if resource_type == "Organization" and record["id"] != "Organization-net-1":
        npi_identifier_by_field = {
            "system": "http://terminology.hl7.org/NamingSystem/npi",
            "value": record["id"].split("-")[1],
        }
        address_list = [_address("30 Elm Rd"), _address("60 Postal Ln", kind="postal")]
        return {**record, "identifier": [npi_identifier_by_field], "address": address_list}
    return record


async def _stamp_with_own_connection(stage_table, schema):
    """Stand-in for archive resolution: a separate connection must see the committed stage."""
    connection = await _connect()
    try:
        await connection.execute(
            f'UPDATE "{schema}".{stage_table} SET address_key = md5(lower(first_line || city_name))::uuid'
        )
    finally:
        await connection.close()


async def _address_rows(admin, control_schema, table="cms_npd_address"):
    rows = await admin.fetch(
        f"""SELECT npi, source_kind, first_line, latitude IS NOT NULL AS located, telephone_number,
                   source_record_id, address_key IS NOT NULL AS keyed
            FROM "{control_schema}".{table} ORDER BY npi, first_line"""
    )
    return [dict(row) for row in rows]


async def test_release_builds_the_provider_address_table_and_swaps_keeping_old(control, tmp_path):
    admin, control_schema = control
    first = await _import(
        LocalSource(tmp_path / "a1", write_release(tmp_path / "a1", 2, patch=_with_addresses)),
        _stamp_with_own_connection,
    )
    assert first["address_rows"] == 6
    first_rows = await _address_rows(admin, control_schema)
    assert [(address["npi"], address["source_kind"], address["first_line"]) for address in first_rows] == [
        (npi, source_kind, first_line)
        for npi in (1000000001, 1000000002)
        for source_kind, first_line in (
            ("role_location", "10 Main St"),
            ("practitioner", "20 Oak Ave"),
            ("organization", "30 Elm Rd"),
        )
    ]
    assert [address["located"] for address in first_rows] == [True, False, False] * 2
    assert [address["telephone_number"] for address in first_rows] == ["555-0100", None, None] * 2
    assert all(
        address["keyed"] and address["source_record_id"].startswith(f"cms_npd:{address['npi']}:")
        for address in first_rows
    )
    recorded = await admin.fetchval(f'SELECT row_counts FROM "{control_schema}".cms_npd_release')
    assert json.loads(recorded)["provider_address"] == 6

    second = await _import(
        LocalSource(tmp_path / "a2", write_release(tmp_path / "a2", 3, "2026-02-01", patch=_with_addresses)),
        _stamp_with_own_connection,
    )
    assert second["address_rows"] == 9
    assert len(await _address_rows(admin, control_schema)) == 9
    old_rows = await _address_rows(admin, control_schema, "cms_npd_address_old")
    assert old_rows == first_rows  # the same addresses keep the same source_record_id across releases
    indexes = await admin.fetch(
        "SELECT indexname FROM pg_indexes WHERE schemaname = $1 AND tablename LIKE 'cms_npd_address%' ORDER BY 1",
        control_schema,
    )
    assert [address["indexname"] for address in indexes] == [
        "cms_npd_address_address_key",
        "cms_npd_address_old_address_key",
        "cms_npd_address_old_pkey",
        "cms_npd_address_pkey",
    ]
    unchanged = await _import(
        LocalSource(tmp_path / "a2", write_release(tmp_path / "a2", 3, "2026-02-01", patch=_with_addresses))
    )
    assert unchanged["status"] == "unchanged" and "address_rows" not in unchanged

    await admin.execute(
        f"UPDATE \"{control_schema}\".cms_npd_release SET row_counts = row_counts - 'provider_address' "
        "WHERE status = 'current'"
    )
    pending = await _import(
        LocalSource(tmp_path / "a2", write_release(tmp_path / "a2", 3, "2026-02-01", patch=_with_addresses))
    )
    assert (pending["status"], pending["address_rows"]) == ("unchanged", 9)
    assert len(await _address_rows(admin, control_schema, "cms_npd_address_old")) == 9


def _with_countries(resource_type, index, record):
    """One practitioner street in three countries (US under two spellings), without city, state or postal code."""
    if resource_type != "Practitioner":
        return record
    address_list = [
        {"use": "work", "type": "physical", "line": ["70 Border Rd"], "country": country}
        for country in ("CA", "MX", "US", "USA")
    ]
    return {**record, "address": address_list}


async def test_country_splits_addresses_and_missing_table_rebuilds(control, tmp_path):
    admin, control_schema = control
    source = LocalSource(tmp_path / "c1", write_release(tmp_path / "c1", 1, patch=_with_countries))
    first = await _import(source)
    rows = await admin.fetch(
        f'SELECT country_code FROM "{control_schema}".cms_npd_address WHERE first_line = $1 ORDER BY 1', "70 Border Rd"
    )
    assert [row["country_code"] for row in rows] == ["CA", "MX", "US"]

    await admin.execute(f'DROP TABLE "{control_schema}".cms_npd_address')
    rebuilt = await _import(source)
    assert (rebuilt["status"], rebuilt["address_rows"]) == ("unchanged", first["address_rows"])
    assert await admin.fetchval(f"SELECT to_regclass('\"{control_schema}\".cms_npd_address') IS NOT NULL")

    async def admitted(request):
        return {"run_id": "run-1", "idempotency_key": request["idempotency_key"]}, True

    release_id = first["release_id"]
    assert (await enqueue_refresh(admin, control_schema, release_id, create_run=admitted))["status"] == "queued"
    recorded = await admin.fetchval(f'SELECT row_counts FROM "{control_schema}".cms_npd_release')
    assert json.loads(recorded)["provider_address_refresh"] == 1
