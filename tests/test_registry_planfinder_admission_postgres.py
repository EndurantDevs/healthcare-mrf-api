# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Complete Plan Finder editions use bounded COPY and one native set reduction."""

import asyncio
import json
import threading
from dataclasses import replace
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from process import cms_planfinder_workbook_input as decoder
from process import registry_source_admission as admission
from process.registry_source_admission import RegistrySourceEdition, admit_cms_planfinder_edition
from process.registry_source_observation_store import RegistryObservationError, persist_registry_source_observations
from tests.test_cms_planfinder_workbook_input import _NS, _workbook
from tests.test_network_serving_schema_postgres import serving_schema as serving_schema
from tests.test_registry_source_import_postgres import _arguments, _configured_pool, _invoke, _source_totals

pytest.importorskip("ptg2_address_canon")


def _edition(workbook_digest):
    return RegistrySourceEdition(
        uuid4(),
        "cms",
        "plan-finder",
        "synthetic-quarter",
        "https://example.test/source.zip",
        "a" * 64,
        workbook_digest,
        "plan-finder-workbook-v1",
        2026,
        datetime(2026, 9, 15, tzinfo=timezone.utc),
    )


@pytest.mark.asyncio
async def test_actual_native_workbook_admission_and_exact_replay(serving_schema, tmp_path):
    connection, schema, _ = serving_schema
    path, digest = _workbook(tmp_path, count=4)
    edition = _edition(digest)
    async with connection.transaction():
        receipt = await admit_cms_planfinder_edition(connection, path, edition, control_schema=schema)
    assert receipt["observations"] == receipt["accepted"] == receipt["resolved_issuers"] == 4
    assert receipt["identifiers"] == 4 and receipt["group_assertions"] == 0
    assert receipt["identity_materialization"]["companies_created"] == 1
    assert receipt["identity_materialization"]["groups_created"] == 0
    assert receipt["copy_batches"] == 1 and receipt["native_counts"]["input_rows"] == 4
    retained = json.loads(
        await connection.fetchval(f'SELECT observation_json FROM "{schema}".registry_source_observation LIMIT 1')
    )
    assert set(retained["raw_fields"]) == set(decoder.HEADERS)
    assert retained["normalized_ein"] == "012345678"
    assert retained["raw_fields"]["federal_ein"] == "12345678"
    assert retained["raw_fields"]["databasecompanyid"] == "37.0"
    assert retained["source_evidence"]["workbook_sha256"] == digest
    assert retained["source_evidence"]["raw_values"][10] == "46000.5"
    before = await _source_totals(connection, schema)
    async with connection.transaction():
        replay = await admit_cms_planfinder_edition(connection, path, edition, control_schema=schema)
    assert replay["replayed"] and replay["copy_sha256"] == receipt["copy_sha256"]
    assert replay["identity_materialization"]["companies_created"] == 0
    assert await _source_totals(connection, schema) == before
    assert tuple(
        await connection.fetchrow(f'SELECT draft_revision,approved_revision FROM "{schema}".registry_revision_control')
    ) == (0, 0)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("overrides", "issuer_count"),
    (({8: ("n", "0", "0")}, 1), ({0: ("n", "0", "0")}, 0), ({3: ("inlineStr", "ZZ", "0")}, 0)),
)
async def test_valid_issuer_fields_survive_invalid_company_identifiers(
    serving_schema, tmp_path, overrides, issuer_count
):
    connection, schema, _ = serving_schema
    path, digest = _workbook(tmp_path, count=1, overrides=overrides)
    async with connection.transaction():
        receipt = await admit_cms_planfinder_edition(connection, path, _edition(digest), control_schema=schema)
    assert receipt["observations"] == receipt["rejected"] == 1
    assert receipt["resolved_issuers"] == 0 and receipt["issuer_assertions"] == issuer_count
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".hios_issuer_registry') == issuer_count
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".company_registry') == 0


@pytest.mark.asyncio
async def test_conflicting_company_name_in_second_batch_never_materializes(serving_schema, tmp_path):
    connection, schema, _ = serving_schema

    def change_last_company(documents_by_path):
        sheet = documents_by_path["xl/worksheets/sheet1.xml"]
        cell = sheet.find(f"{{{_NS}}}sheetData/{{{_NS}}}row[@r='5002']/{{{_NS}}}c[@r='B5002']")
        cell.set("t", "inlineStr")
        cell.remove(cell.find(f"{{{_NS}}}v"))
        from xml.etree import ElementTree

        ElementTree.SubElement(
            ElementTree.SubElement(cell, f"{{{_NS}}}is"), f"{{{_NS}}}t"
        ).text = "Different Legal Company"

    path, digest = _workbook(tmp_path, count=5001, mutation=change_last_company)
    async with connection.transaction():
        receipt = await admit_cms_planfinder_edition(connection, path, _edition(digest), control_schema=schema)
    assert receipt["observations"] == 5001 and receipt["copy_batches"] == 2
    assert receipt["identity_materialization"]["companies_created"] == 0
    assert receipt["identity_materialization"]["company_conflicts"] == 1
    assert receipt["conflicting_issuers"] == 5001 and receipt["resolved_issuers"] == 0
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".company_registry') == 0


@pytest.mark.asyncio
async def test_last_batch_failure_rolls_back_all_source_and_identity_writes(serving_schema, tmp_path, monkeypatch):
    connection, schema, _ = serving_schema
    path, digest = _workbook(tmp_path, count=5001)
    encode = admission._encode_edition
    calls = []

    def fail_second_batch(input_bytes, edition):
        calls.append(None)
        if len(calls) == 2:
            raise ValueError("synthetic late failure")
        return encode(input_bytes, edition)

    monkeypatch.setattr(admission, "_encode_edition", fail_second_batch)
    async with connection.transaction():
        with pytest.raises(ValueError, match="late failure"):
            await admit_cms_planfinder_edition(connection, path, _edition(digest), control_schema=schema)
    assert len(calls) == 2 and not any((await _source_totals(connection, schema)).values())
    assert (
        await connection.fetchval(
            "SELECT count(*) FROM pg_class WHERE relnamespace=pg_my_temp_schema() AND relname LIKE 'registry_landing_%'"
        )
        == 0
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ("raw_fields", "artifact", "workbook", "row_gap"))
async def test_closed_landing_rejects_fabricated_source_fields_and_pins(serving_schema, tmp_path, change):
    connection, schema, _ = serving_schema
    path, digest = _workbook(tmp_path, count=2)
    edition = _edition(digest)
    async with connection.transaction():
        await admission._register_edition(connection, f'"{schema}"', edition)
        landing, *_ = await admission._load_planfinder_landing(connection, f'"{schema}"', path, edition)
        table = f'"{landing.table_name}"'
        if change == "raw_fields":
            await connection.execute(
                f"UPDATE {table} SET observation_json=jsonb_set("
                "observation_json,'{raw_fields,mr_submission_template_id}','\"fabricated\"')"
            )
        elif change in {"artifact", "workbook"}:
            await connection.execute(
                f"UPDATE {table} SET observation_json=jsonb_set("
                f"observation_json,'{{source_evidence,{change}_sha256}}',to_jsonb($1::text))",
                "b" * 64,
            )
        else:
            await connection.execute(f"DELETE FROM {table} WHERE source_row_number=2")
            landing = replace(landing, expected_rows=1)
        with pytest.raises(RegistryObservationError):
            await persist_registry_source_observations(connection, landing, control_schema=schema)
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".registry_source_observation') == 0


@pytest.mark.asyncio
async def test_cancelled_decoder_drains_thread_and_closes_iterator(serving_schema, tmp_path, monkeypatch):
    connection, schema, _ = serving_schema
    path, digest = _workbook(tmp_path, count=1)
    entered, release, closed = threading.Event(), threading.Event(), threading.Event()

    def delayed_batches(*args, **kwargs):
        try:
            entered.set()
            assert release.wait(5)
            yield b"not consumed after cancellation"
        finally:
            closed.set()

    monkeypatch.setattr(decoder, "iter_cms_planfinder_issuer_batches", delayed_batches)

    async def import_source():
        async with connection.transaction():
            await admit_cms_planfinder_edition(connection, path, _edition(digest), control_schema=schema)

    task = asyncio.create_task(import_source())
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert closed.is_set()
        assert not any((await _source_totals(connection, schema)).values())
    finally:
        release.set()
        if not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_planfinder_cli_uses_configured_pool_and_receipt_only(serving_schema, tmp_path, monkeypatch, capsys):
    connection, schema, engine = serving_schema
    path, digest = _workbook(tmp_path, count=3)
    edition = _edition(digest)
    databases = _configured_pool(monkeypatch, engine, schema)
    status, receipt = await _invoke(_arguments(path, edition), capsys)
    assert status == 0 and receipt["observations"] == receipt["resolved_issuers"] == 3
    assert receipt["copy_batches"] == 1 and all(database.engine is None for database in databases)
    serialized = json.dumps(receipt)
    assert len(serialized.encode()) <= 8192
    assert (
        str(path) not in serialized
        and edition.source_url not in serialized
        and "Example Legal Company" not in serialized
    )
    assert await connection.fetchval(f'SELECT count(*) FROM "{schema}".company_registry') == 1
