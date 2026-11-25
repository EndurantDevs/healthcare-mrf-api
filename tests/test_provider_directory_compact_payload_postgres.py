# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic native storage proof; no importer or publication is activated."""

import json
from contextlib import asynccontextmanager

import asyncpg
import pytest

from process import provider_directory_compact_payload as compact
from process import provider_directory_generic_candidate as candidate
from tests.cms_npd_admission_postgres_support import _database_url, _owned_database
from tests.test_provider_directory_compact_payload import resource


@asynccontextmanager
async def storage(*, shared_bodies=False, reference_tokens=False):
    async with _owned_database(_database_url().set(database="postgres")) as (url, _admin):
        connection = await asyncpg.connect(url.set(drivername="postgresql").render_as_string(hide_password=False))
        tables = compact.CompactPayloadTables(
            "compact_test",
            "aliases",
            "sets",
            "blocks",
            "shared_bodies" if shared_bodies else None,
            "reference_tokens" if reference_tokens else None,
        )
        try:
            await connection.execute("CREATE SCHEMA compact_test")
            for statement in compact.compact_payload_ddl(tables):
                await connection.execute(statement)
            await connection.execute(
                "CREATE TABLE compact_test.checkpoint(id integer PRIMARY KEY,rows_processed bigint)"
            )
            await connection.execute("INSERT INTO compact_test.checkpoint VALUES(1,0)")
            yield connection, tables
        finally:
            await connection.close()


async def append(connection, tables, observations, ordinal):
    return await compact.append_compact_payload_batch(
        connection, tables, observations, dataset_id="dataset", batch_ordinal=ordinal, format_id=tables.format
    )


async def counts(connection):
    return [
        await connection.fetchval(f"SELECT count(*) FROM compact_test.{name}") for name in ("aliases", "sets", "blocks")
    ] + [await connection.fetchval("SELECT rows_processed FROM compact_test.checkpoint")]


def observations(count):
    first, second = "HealthcareService/" + "0" * 32, "HealthcareService/" + "f" * 32
    shapes = (
        [first, second, first],
        [],
        None,
        [first, "HealthcareService/" + "A" * 32, first],
        ["", "Other/ref", "caf\u00e9", "caf\u00e9"],
    )
    return [
        resource(
            str(number % 7),
            healthcare_service_refs=shapes[number % len(shapes)],
            npi="1000000000" if number % 2 else 1000000000,
            active=number % 3 == 0,
            insurance_plan_refs=["InsurancePlan/plan"],
            ordinal_evidence=number,
        )
        for number in range(count)
    ]


async def read_all(connection, tables):
    pages, cursor_values = [], ("", "", 0, -1)
    while True:
        page = await compact.read_compact_payload_page(connection, tables, "dataset", after=cursor_values, limit=40)
        if not page:
            return pages
        pages.extend(page)
        last = page[-1]
        cursor_values = tuple(last[field] for field in ("resource_type", "resource_id", "batch_ordinal", "row_ordinal"))


@pytest.mark.asyncio
async def test_compact_observations_resume_replay_and_native_reads():
    original = observations(135)
    async with storage() as (connection, tables):
        with pytest.raises(candidate.GenericCandidateError, match="transaction_required"):
            await append(connection, tables, original[:70], 1)
        async with connection.transaction():
            assert await append(connection, tables, original[:70], 1) == {"rows": 70, "is_replay": False}
            await connection.execute("UPDATE compact_test.checkpoint SET rows_processed=70")
        prefix = await counts(connection)
        with pytest.raises(asyncpg.UniqueViolationError):
            async with connection.transaction():
                await append(connection, tables, original[70:], 2)
                await connection.execute("UPDATE compact_test.checkpoint SET rows_processed=135")
                await connection.execute("INSERT INTO compact_test.checkpoint VALUES(1,0)")
        assert await counts(connection) == prefix
        async with connection.transaction():
            assert await append(connection, tables, original[:70], 1) == {"rows": 70, "is_replay": True}
            await append(connection, tables, original[70:], 2)
            await connection.execute("UPDATE compact_test.checkpoint SET rows_processed=135")
        resumed = await counts(connection)
        async with connection.transaction():
            assert await append(connection, tables, original[70:], 2) == {"rows": 65, "is_replay": True}
        assert await counts(connection) == resumed
        await assert_native_reads(connection, tables, original)
        changed_rows = [*original[70:]]
        changed_rows[0] = resource("changed")
        with pytest.raises(candidate.GenericCandidateError, match="batch_replay_changed"):
            async with connection.transaction():
                await append(connection, tables, changed_rows, 2)
        assert await counts(connection) == resumed


@pytest.mark.asyncio
async def test_shared_bodies_preserve_each_observation_across_copy_and_resume():
    original = resource("same", npi=1000000000, value=-0.0, fhir_meta={"lastUpdated": "old"})
    replay = resource("same", npi=1000000000, value=-0.0, fhir_meta={"lastUpdated": "new"})
    changed = resource("same", npi=1000000000, value=1.0, fhir_fetch_url=None)
    async with storage(shared_bodies=True) as (connection, tables):
        async with connection.transaction():
            await append(connection, tables, [original], 1)
            await connection.execute("UPDATE compact_test.checkpoint SET rows_processed=1")
        with pytest.raises(asyncpg.UniqueViolationError):
            async with connection.transaction():
                await append(connection, tables, [replay, changed], 2)
                await connection.execute("INSERT INTO compact_test.checkpoint VALUES(1,3)")
        assert await connection.fetchval("SELECT count(*) FROM compact_test.shared_bodies") == 1
        assert await counts(connection) == [1, 0, 1, 1]

        async with connection.transaction():
            await append(connection, tables, [replay, changed], 2)
            await connection.execute("UPDATE compact_test.checkpoint SET rows_processed=3")
            assert (await append(connection, tables, [replay, changed], 2))["is_replay"] is True
        async with connection.transaction(readonly=True):
            actual = await compact.read_compact_payload_page(connection, tables, "dataset", npi="1000000000")

        assert await connection.fetchval("SELECT count(*) FROM compact_test.shared_bodies") == 2
        assert [(row["batch_ordinal"], row["row_ordinal"]) for row in actual] == [(1, 0), (2, 0), (2, 1)]
        assert [compact._json(json.loads(row["payload_json"])) for row in actual] == [
            compact._json(row["payload_json"]) for row in (original, replay, changed)
        ]


async def assert_native_reads(connection, tables, original):
    expected_rows = [
        (batch, ordinal, row)
        for batch, batch_rows in ((1, original[:70]), (2, original[70:]))
        for ordinal, row in enumerate(batch_rows)
    ]
    expected_rows.sort(key=lambda entry: (entry[2]["resource_type"], entry[2]["resource_id"], entry[0], entry[1]))
    async with connection.transaction(isolation="repeatable_read", readonly=True):
        actual = await read_all(connection, tables)
        assert len(actual) == len(expected_rows) == 135
        for stored, (batch, ordinal, row) in zip(actual, expected_rows, strict=True):
            restored_by_field = {name: stored[name] for name in candidate.RESOURCE_COLUMNS}
            restored_by_field["payload_json"] = json.loads(restored_by_field["payload_json"])
            assert restored_by_field == row
            assert compact._json(restored_by_field) == compact._json(row)
            assert (stored["batch_ordinal"], stored["row_ordinal"]) == (batch, ordinal)
        matches = await compact.read_compact_payload_page(connection, tables, "dataset", npi="1000000000", limit=32)
        assert len(matches) == 32
        assert [tuple(row[field] for field in ("resource_id", "batch_ordinal", "row_ordinal")) for row in matches] == [
            (row["resource_id"], batch, ordinal) for batch, ordinal, row in expected_rows[:32]
        ]
    assert await connection.fetchval("SELECT count(*) FROM compact_test.sets") == 4
    assert await connection.fetchval("SELECT max(jsonb_array_length(bodies)) FROM compact_test.blocks") == 64


@pytest.mark.asyncio
async def test_compact_existing_reference_collision_refuses(monkeypatch):
    monkeypatch.setattr(compact, "_reference_digest", lambda _refs: b"x" * 32)
    async with storage() as (connection, tables):
        async with connection.transaction():
            await append(connection, tables, [resource(healthcare_service_refs=["one"])], 1)
        before = await counts(connection)
        with pytest.raises(candidate.GenericCandidateError, match="reference_hash_collision"):
            async with connection.transaction():
                await append(connection, tables, [resource(healthcare_service_refs=["two"])], 2)
        assert await counts(connection) == before


@pytest.mark.asyncio
async def test_compact_floats_keep_canonical_sign_type_and_replay():
    original = resource(
        numbers=[-0.0, 1.0, 1e100, {"nested": 1e-7}],
        npi=1000000000,
        healthcare_service_refs=["HealthcareService/" + "f" * 32],
    )
    async with storage() as (connection, tables):
        async with connection.transaction():
            await append(connection, tables, [original], 1)
        async with connection.transaction():
            assert (await append(connection, tables, [original], 1))["is_replay"]
        async with connection.transaction(readonly=True):
            page = await compact.read_compact_payload_page(connection, tables, "dataset", npi="1000000000")
            assert len(page) == 1
            assert compact._json(json.loads(page[0]["payload_json"])) == compact._json(original["payload_json"])


@pytest.mark.asyncio
async def test_compact_replay_refuses_stale_npi_and_extra_population():
    original_rows = [resource(npi="1000000000")]
    async with storage() as (connection, tables):
        async with connection.transaction():
            await append(connection, tables, original_rows, 1)
        before = await counts(connection)
        with pytest.raises(candidate.GenericCandidateError, match="replay_npi_changed"):
            async with connection.transaction():
                await connection.execute("UPDATE compact_test.aliases SET npi_text='different'")
                await append(connection, tables, original_rows, 1)
        assert await connection.fetchval("SELECT npi_text FROM compact_test.aliases") == "1000000000"
        with pytest.raises(candidate.GenericCandidateError, match="batch_replay_changed"):
            async with connection.transaction():
                await connection.execute("INSERT INTO compact_test.blocks VALUES('dataset',1,999,'[{}]')")
                await append(connection, tables, original_rows, 1)
        async with connection.transaction():
            assert (await append(connection, tables, original_rows, 1))["is_replay"]
        assert await counts(connection) == before


@pytest.mark.asyncio
async def test_compact_block_byte_bound_and_missing_data_refuse(monkeypatch):
    async with storage() as (connection, tables):
        monkeypatch.setattr(candidate, "COPY_MAX_BYTES", 2000)
        rows = [resource(str(number), text="x" * 400) for number in range(9)]
        async with connection.transaction():
            await append(connection, tables, rows, 1)
        assert await connection.fetchval("SELECT count(*) FROM compact_test.blocks") > 1
        async with connection.transaction(readonly=True):
            page = await compact.read_compact_payload_page(connection, tables, "dataset", limit=9)
            assert 0 < len(page) < 9
        before = await counts(connection)
        with pytest.raises(candidate.GenericCandidateError, match="copy_record_too_large"):
            async with connection.transaction():
                await append(connection, tables, [resource(text="x" * 2000)], 2)
                await connection.execute("UPDATE compact_test.checkpoint SET rows_processed=100")
        assert await counts(connection) == before
        with pytest.raises(candidate.GenericCandidateError, match="read_rows_missing"):
            async with connection.transaction():
                await connection.execute("DELETE FROM compact_test.blocks WHERE block_ordinal=0")
                await compact.read_compact_payload_page(connection, tables, "dataset", limit=9)
        assert await counts(connection) == before


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fields",
    [
        pytest.param({}, id="absent"),
        pytest.param({"healthcare_service_refs": None}, id="null"),
        pytest.param({"healthcare_service_refs": []}, id="empty"),
        pytest.param({"healthcare_service_refs": ["HealthcareService/" + "a" * 32]}, id="canonical"),
    ],
)
async def test_compact_reader_refuses_truncated_body_positions(fields):
    original_rows = [resource(str(number), **fields) for number in range(2)]
    async with storage() as (connection, tables):
        async with connection.transaction():
            await append(connection, tables, original_rows, 1)
        before = await counts(connection)
        with pytest.raises(candidate.GenericCandidateError, match="read_rows_missing"):
            async with connection.transaction():
                await connection.execute("UPDATE compact_test.blocks SET bodies=bodies-1 WHERE block_ordinal=0")
                await compact.read_compact_payload_page(connection, tables, "dataset", limit=2)
        assert await counts(connection) == before
        async with connection.transaction(readonly=True):
            restored = await read_all(connection, tables)
        assert [json.loads(row["payload_json"]) for row in restored] == [row["payload_json"] for row in original_rows]


@pytest.mark.asyncio
async def test_compact_intake_preserves_an_omitted_acquisition_hash():
    original_by_field = resource()
    original_by_field.pop("acquired_resource_sha256")
    async with storage() as (connection, tables):
        async with connection.transaction():
            await append(connection, tables, [original_by_field], 1)
        async with connection.transaction(readonly=True):
            stored_rows = await read_all(connection, tables)
        assert len(stored_rows) == 1
        assert stored_rows[0]["acquired_resource_sha256"] is None
        assert json.loads(stored_rows[0]["payload_json"]) == original_by_field["payload_json"]


@pytest.mark.asyncio
async def test_reference_tokens_pack_network_refs_losslessly_and_reject_missing_tokens():
    absent = resource("absent")
    absent["payload_json"].pop("network_refs")
    network_shapes = [
        absent,
        resource("null-net", network_refs=None, npi=True),
        resource("empty-net", network_refs=[], npi=1000000000),
        resource(
            "ordered-net",
            network_refs=["Organization/net-a", "Organization/net-b", "Organization/net-a", "Organization/caf\u00e9"],
            numbers=[-0.0, 1.0, 1e-7, 1e100],
            npi="1000000000",
        ),
        resource("inline-net", network_refs="inline", npi={"nested": 1}),
        resource("mixed-net", network_refs=[1, None]),
    ]
    replay_row = resource(
        "ordered-net",
        network_refs=["Organization/net-a", "Organization/net-b", "Organization/net-a", "Organization/caf\u00e9"],
        numbers=[-0.0, 1.0, 1e-7, 1e100],
        npi="1000000000",
        fhir_meta={"lastUpdated": "2026-11-26T00:00:00Z"},
    )
    async with storage(shared_bodies=True, reference_tokens=True) as (connection, tables):
        async with connection.transaction():
            assert await append(connection, tables, network_shapes, 1) == {"rows": 6, "is_replay": False}
        async with connection.transaction():
            assert await append(connection, tables, network_shapes, 1) == {"rows": 6, "is_replay": True}
            assert await append(connection, tables, [replay_row], 2) == {"rows": 1, "is_replay": False}
        assert await connection.fetchval("SELECT count(*) FROM compact_test.reference_tokens") == 3
        assert await connection.fetchval("SELECT count(*) FROM compact_test.shared_bodies") == 6
        async with connection.transaction(readonly=True):
            restored = await read_all(connection, tables)
            restored_by_obs = {(entry["batch_ordinal"], entry["row_ordinal"]): entry for entry in restored}
            expected_by_obs = {(1, idx): entry for idx, entry in enumerate(network_shapes)} | {(2, 0): replay_row}
            assert set(restored_by_obs) == set(expected_by_obs)
            for key, expected in expected_by_obs.items():
                assert compact._json(json.loads(restored_by_obs[key]["payload_json"])) == compact._json(
                    expected["payload_json"]
                )
            npi_matches = await compact.read_compact_payload_page(connection, tables, "dataset", npi="1000000000")
            assert len(npi_matches) == 3
        with pytest.raises(candidate.GenericCandidateError, match="read_rows_missing"):
            async with connection.transaction():
                await connection.execute("DELETE FROM compact_test.reference_tokens WHERE token_id=1")
                await compact.read_compact_payload_page(connection, tables, "dataset", limit=10)
