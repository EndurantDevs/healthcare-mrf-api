# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Optional native SQL and atomic-publication proof in an owned database."""

import asyncio
import copy
import importlib
import json
import os
import uuid
from contextlib import asynccontextmanager
from types import SimpleNamespace

import asyncpg
import pytest
from sqlalchemy import JSON, literal, select, text
from sqlalchemy.engine import make_url
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db.connection import Database
from process import new_york_profile_store as module
from process import provider_profile_source_store as shared
from tests.test_new_york_profile_acquisition import source_session
from tests.test_new_york_profile_store import (
    RUN_ID,
    _legacy_inventory_oracle,
    _refresh_artifact,
    _substitute_corroboration_receipt,
    captured_case,
    witnessed_case,
)


@asynccontextmanager
async def _database(monkeypatch):
    dsn = os.getenv("HLTHPRT_PROVIDER_DIRECTORY_PROFILE_POSTGRES_DSN")
    if not dsn:
        pytest.skip("set the profile PostgreSQL DSN for native store tests")
    admin_url = make_url(dsn).set(drivername="postgresql")
    name = "ny_profile_store_test_" + uuid.uuid4().hex
    admin = await asyncpg.connect(admin_url.render_as_string(hide_password=False), timeout=10)
    engine, is_creation_attempted = None, False
    try:
        assert await admin.fetchval("SELECT count(*) FROM pg_database WHERE datname=$1", name) == 0
        is_creation_attempted = True
        await admin.execute(f'CREATE DATABASE "{name}" TEMPLATE template0')
        engine = create_async_engine(admin_url.set(database=name, drivername="postgresql+asyncpg"))
        database = Database(engine=engine, session_factory=async_sessionmaker(engine, expire_on_commit=False))
        assert await database.scalar("SELECT current_database()") == name
        monkeypatch.setattr(shared, "db", database)
        florida = importlib.import_module("process.florida_mqa_profile")
        monkeypatch.setattr(florida, "db", database)
        await shared.ensure_tables()
        yield database
    finally:
        try:
            if engine is not None:
                await engine.dispose()
            if is_creation_attempted:
                await admin.execute(f'DROP DATABASE IF EXISTS "{name}"')
                assert await admin.fetchval("SELECT count(*) FROM pg_database WHERE datname=$1", name) == 0
                assert await admin.fetchval("SELECT count(*) FROM pg_stat_activity WHERE datname=$1", name) == 0
        finally:
            await admin.close(timeout=5)
            assert admin.is_closed()


async def _retain(database, case):
    await module.store.claim_run(case.run)
    for model, stored_rows in (
        (shared.ProviderProfileArtifact, [case.artifact]),
        (shared.ProviderProfileSourceRecord, case.records),
        (shared.ProviderProfileFact, case.facts),
    ):
        await database.insert(model.__table__).values(stored_rows).status()


async def _assert_unpublished(database):
    source_run = await module.store._read_run(RUN_ID)
    assert source_run["status"] == "running"
    assert await module.store.read_publication() is None
    fact_table = shared.ProviderProfileFact.__table__
    assert await database.first(select(fact_table).where(fact_table.c.published_at.is_not(None))) is None


async def _assert_support_substitution_cannot_publish(database, case):
    tampered = copy.deepcopy(case)
    _substitute_corroboration_receipt(tampered)
    record_table, artifact_table = (
        shared.ProviderProfileSourceRecord.__table__,
        shared.ProviderProfileArtifact.__table__,
    )
    async with database.transaction() as transaction:
        await (
            database.update(record_table)
            .where(record_table.c.record_id == tampered.records[0]["record_id"])
            .values(match_evidence=tampered.records[0]["match_evidence"])
            .status()
        )
        await (
            database.update(artifact_table)
            .where(artifact_table.c.run_id == RUN_ID)
            .values(
                **{field: tampered.artifact[field] for field in ("content_sha256", "content_bytes", "metadata_json")}
            )
            .status()
        )
        actual = await database.first(
            select(record_table).where(record_table.c.record_id == tampered.records[0]["record_id"])
        )
        assert module._hash(dict(actual._mapping)) == tampered.profiles["111111"]["record_sha256"]
        assert (await module.store.retained_counts(RUN_ID))["invalid_bundle_records"] == 1
        with pytest.raises(RuntimeError, match="retained_integrity_invalid"):
            await module.store.publish_run(RUN_ID, expected_current_run_id=None, metrics=case.metrics)
        await _assert_unpublished(database)
        await transaction.rollback()


async def test_native_store_checks_actual_rows_and_rolls_back_cancelled_publication(captured_case, monkeypatch):
    case = captured_case
    async with _database(monkeypatch) as database:
        await _retain(database, case)
        counts = await module.store.retained_counts(RUN_ID)
        assert counts["acquired_profiles"] == counts["retained_source_records"] == 2
        assert counts["held_attempts"] == counts["matched_public_providers"] == 1
        assert module.store._completion_metrics(case.run, case.metrics, counts)["retained_facts"] == 2
        await _assert_support_substitution_cannot_publish(database, case)
        fact_table = shared.ProviderProfileFact.__table__
        async with database.transaction() as transaction:
            await (
                database.update(fact_table)
                .where(fact_table.c.fact_id == case.facts[0]["fact_id"])
                .values(value_json={})
                .status()
            )
            with pytest.raises(RuntimeError, match="retained_integrity_invalid"):
                await module.store.publish_run(RUN_ID, expected_current_run_id=None, metrics=case.metrics)
            await _assert_unpublished(database)
            await transaction.rollback()
        original_complete = type(module.store)._complete_run

        async def cancelled_after_update(self, run_id, metrics):
            await original_complete(self, run_id, metrics)
            raise asyncio.CancelledError

        with monkeypatch.context() as patch:
            patch.setattr(type(module.store), "_complete_run", cancelled_after_update)
            with pytest.raises(asyncio.CancelledError):
                await module.store.publish_run(RUN_ID, expected_current_run_id=None, metrics=case.metrics)
        await _assert_unpublished(database)
        published = await module.store.publish_run(RUN_ID, expected_current_run_id=None, metrics=case.metrics)
        assert published["published"] is True
        assert (await module.store.read_publication())["current_run_id"] == RUN_ID
        assert (await module.store.retained_counts(RUN_ID))["invalid_bundle_facts"] == 0
        source_table = shared.ProviderProfileSourceRecord.__table__
        unmatched = await database.first(select(source_table).where(source_table.c.license_number == "222222"))
        assert unmatched.matched_npi is None and unmatched.match_status == "identity_conflict"
        assert await database.first(select(source_table).where(source_table.c.license_number == "333333")) is None


def _legacy_codec_case(case, fault):
    fact, source_record = case.facts[0], case.records[0]
    fact["value_json"]["codec"] = {
        "integer": 1,
        "float": 1.0,
        "zero": -0.0,
        "unicode": "Łódź\n學😀\u0000",
        "small": 5e-324,
        "large": 1.7976931348623157e308,
        "precise_integer": 9007199254740993,
    }
    fact["value_json"]["duplicate"] = 2
    source_record["match_evidence"]["registry_binding"]["npi"] = float(source_record["matched_npi"])
    case.profiles["111111"]["facts"][fact["fact_id"]] = module._fact_hash(fact)
    case.profiles["111111"]["record_sha256"] = module._hash(source_record)
    if fault in {"float", "zero", "unicode"}:
        fact["value_json"]["codec"][fault] = {"float": 1, "zero": 0.0, "unicode": "changed"}[fault]
    if fault == "receipt":
        case.profiles["111111"]["facts"][fact["fact_id"]] = "0" * 64
    if fault == "lineage":
        source_record["normalized_payload"]["profile_capture"]["artifact_id"] = "0" * 64
        case.profiles["111111"]["record_sha256"] = module._hash(source_record)
    if fault == "support":
        _substitute_corroboration_receipt(case)
    if fault == "missing_record":
        case.records.pop()
    if fault == "missing_fact":
        case.facts.pop()
    if fault == "extra_record":
        case.records.append(
            {
                **source_record,
                "record_id": "0" * 64,
                "license_number": "333333",
                "source_record_key": "held:333333",
            }
        )
    _refresh_artifact(case)
    raw_value = '{"duplicate": 0,' + json.dumps(fact["value_json"], ensure_ascii=True, indent=2)[1:]
    return raw_value.replace('"duplicate": 2', '"duplicate": 1e999') if fault == "nonfinite" else raw_value


@pytest.mark.parametrize(
    "fault",
    [
        None,
        "float",
        "zero",
        "unicode",
        "receipt",
        "lineage",
        "support",
        "missing_record",
        "missing_fact",
        "extra_record",
        "nonfinite",
    ],
)
async def test_native_legacy_witness_keeps_decoded_codec_and_original_receipts(captured_case, monkeypatch, fault):
    case = captured_case
    raw_value = _legacy_codec_case(case, fault)
    async with _database(monkeypatch) as database:
        await _retain(database, case)
        fact = shared.ProviderProfileFact.__table__
        await (
            database.update(fact)
            .where(fact.c.fact_id == case.facts[0]["fact_id"])
            .values(value_json=literal(raw_value).cast(JSON))
            .status()
        )
        if fault == "nonfinite":
            with pytest.raises(ValueError, match="Out of range float"):
                await module.store.retained_counts(RUN_ID)
        else:
            counts = await module.store.retained_counts(RUN_ID)
            if fault is None:
                assert counts["invalid_bundle_records"] == counts["invalid_bundle_facts"] == 0
                assert module.store._completion_metrics(case.run, case.metrics, counts)["retained_facts"] == 2
            else:
                assert counts["invalid_bundle_records"] + counts["invalid_bundle_facts"] > 0
                with pytest.raises(RuntimeError, match="retained_(integrity_invalid|count_mismatch)"):
                    module.store._completion_metrics(case.run, case.metrics, counts)
        assert (
            await database.scalar(
                "SELECT count(*) FROM pg_class WHERE relnamespace=pg_my_temp_schema() AND relname LIKE 'ny_legacy_witness_%'"
            )
            == 0
        )
        await _assert_unpublished(database)


async def _legacy_cancelled_driver(factory, session, methods):
    driver = await factory(session, methods)

    async def interrupted_copy(*copy_args, **copy_options):
        await driver.copy_records_to_table(*copy_args, **copy_options)
        raise asyncio.CancelledError

    return SimpleNamespace(copy_records_to_table=interrupted_copy, terminate=driver.terminate)


async def _assert_legacy_transfer_rollback(database, case, monkeypatch, fault):
    from process import reference_family_archive as native

    original_copy, original_counts = native.native_copy_record_batch, module._legacy_witness_counts
    fact_table = shared.ProviderProfileFact.__table__
    before_facts = [
        dict(fact._mapping) for fact in await database.all(select(fact_table).order_by(fact_table.c.fact_id))
    ]

    async def copy_records(*args, **kwargs):
        await original_copy(*args, **kwargs)
        if fault in {"copy", "cancel"}:
            raise asyncio.CancelledError if fault == "cancel" else RuntimeError("after_real_copy")
        if fault == "writer":
            async with database.engine.connect() as connection:
                await connection.execute(text("SET LOCAL lock_timeout='50ms'"))
                with pytest.raises(DBAPIError) as error:
                    await connection.execute(
                        text("UPDATE mrf.provider_profile_source_record SET match_status=match_status")
                    )
                assert error.value.orig.sqlstate == "55P03"

    async def counts(*args):
        result = await original_counts(*args)
        if fault == "counts":
            raise RuntimeError("after_real_counts")
        return result

    driver_factory = native._native_model_copy_driver

    with monkeypatch.context() as patch:
        patch.setattr(module, "_LEGACY_TRANSFER_ROWS", 1)
        patch.setattr(native, "native_copy_record_batch", copy_records)
        patch.setattr(module, "_legacy_witness_counts", counts)
        if fault == "driver_cancel":
            patch.setattr(
                native,
                "_native_model_copy_driver",
                lambda session, methods: _legacy_cancelled_driver(driver_factory, session, methods),
            )
        if fault == "writer":
            assert (await module.store.retained_counts(RUN_ID))["invalid_bundle_records"] == 0
        else:
            with pytest.raises(asyncio.CancelledError if fault in {"cancel", "driver_cancel"} else RuntimeError):
                await module.store.retained_counts(RUN_ID)
    assert [
        dict(fact._mapping) for fact in await database.all(select(fact_table).order_by(fact_table.c.fact_id))
    ] == before_facts
    assert (
        await database.scalar(
            "SELECT count(*) FROM pg_class WHERE relnamespace=pg_my_temp_schema() AND relname LIKE 'ny_legacy_witness_%'"
        )
        == 0
    )
    await _assert_unpublished(database)
    assert (await module.store.retained_counts(RUN_ID))["invalid_bundle_facts"] == 0


@pytest.mark.parametrize("fault", ["copy", "cancel", "driver_cancel", "counts", "writer"])
async def test_native_legacy_witness_closes_source_writes_and_rolls_back_scratch(captured_case, monkeypatch, fault):
    async with _database(monkeypatch) as database:
        await _retain(database, captured_case)
        await _assert_legacy_transfer_rollback(database, captured_case, monkeypatch, fault)


async def test_native_legacy_npi_equality_keeps_python_numeric_edges(monkeypatch):
    pairs = [
        (None, None),
        (0, False),
        (1, True),
        (0, -0.0),
        (2, 1.9),
        (1000000004, 1000000004.0),
        (9007199254740993, 9007199254740992.0),
        (2**63 - 1, float(2**63 - 1)),
        (-(2**63), float(-(2**63))),
    ]
    values = [{"matched_npi": expected, "binding": {"npi": observed}} for expected, observed in pairs]
    predicate = module._legacy_npi_equality("value->'binding'", "value")
    async with _database(monkeypatch) as database:
        actual = await database.scalar(
            f"SELECT array_agg(({predicate}) ORDER BY ordinal) "
            "FROM json_array_elements(CAST(:values AS json)) WITH ORDINALITY AS rows(value,ordinal)",
            values=module.encoded_json(values).decode(),
        )
        assert actual == [expected == observed for expected, observed in pairs]


async def test_native_legacy_malformed_json_cannot_replace_receipt(captured_case, monkeypatch):
    async with _database(monkeypatch) as database:
        await _retain(database, captured_case)
        fact = shared.ProviderProfileFact.__table__
        with pytest.raises(DBAPIError):
            async with database.transaction():
                await (
                    database.update(fact)
                    .where(fact.c.fact_id == captured_case.facts[0]["fact_id"])
                    .values(value_json=literal('{"malformed": }').cast(JSON))
                    .status()
                )
        assert (await module.store.retained_counts(RUN_ID))["invalid_bundle_facts"] == 0
        await _assert_unpublished(database)


async def test_native_legacy_metadata_nul_preserves_historical_acceptance(captured_case, monkeypatch):
    case = captured_case
    unmatched = case.records[1]
    unmatched["match_evidence"]["unused_metadata"] = ["\u0000", "\\u0000", "\\", "學"]
    case.facts = [fact for fact in case.facts if fact["source_record_id"] != unmatched["record_id"]]
    case.profiles["222222"]["facts"] = {}
    case.profiles["222222"]["record_sha256"] = module._hash(unmatched)
    case.metrics["facts"] = 1
    _refresh_artifact(case)
    expected = _legacy_inventory_oracle(
        case.run,
        case.artifact["metadata_json"],
        {
            "records": case.records,
            "facts": [
                {key: field_value for key, field_value in fact.items() if key != "published_at"} for fact in case.facts
            ],
        },
        case.facts,
    )
    assert expected == {"invalid_bundle_records": 0, "invalid_bundle_facts": 0}
    async with _database(monkeypatch) as database:
        await _retain(database, case)
        counts = await module.store.retained_counts(RUN_ID)
        assert {key: counts[key] for key in expected} == expected
        metadata = module.encoded_json(module._legacy_metadata_values(unmatched["match_evidence"]["unused_metadata"]))
        assert (
            await database.scalar(
                "SELECT count(DISTINCT value::text) FROM json_array_elements(CAST(:metadata AS json))",
                metadata=metadata.decode(),
            )
            == 4
        )
        await _assert_unpublished(database)


def _change_witness(case, fault):
    """Mutate declared preimages, preserving independent acquisition evidence for the verifier."""
    profile = case.profiles["111111"]
    fact = profile["fact_values"][0]
    fact["value_json"]["native_json_types"] = {"integer": 1, "decimal": 1.0, "zero": -0.0, "text": "Łódź\n學"}
    if fault == "coercion":
        fact["npi"] = str(fact["npi"])
    profile["facts"][fact["fact_id"]] = module._hash(fact)
    if fault in {"support", "support_type"}:
        field, changed_value = ("receipt_sha256", "0" * 64) if fault == "support" else ("license_number", 111111)
        profile["record_values"]["match_evidence"]["registry_binding"]["nysed_corroboration"][field] = changed_value
        profile["record_sha256"] = module._hash(profile["record_values"])
    elif fault == "canonical":
        profile["record_sha256"] = "0" * 64
    elif fault == "incomplete":
        profile["fact_values"].clear()
    case.artifact["content_sha256"] = module._hash(case.artifact["metadata_json"])
    case.artifact["content_bytes"] = len(module.encoded_json(case.artifact["metadata_json"]))
    case.compact_metrics["bundle"] = module.bundle_reference(case.artifact)


async def _verify_native_witness(database, case, ownership, session, fault):
    """Exercise actual model-derived binary COPY, indexes and complete producer set checks."""
    from process import source_profile_result_archive as archive

    source_copy = archive.native.ReferenceFamilySourceCopy(archive.native.native_copy_projection, 16 * 1024 * 1024, 300)
    await archive._load_witnessed_source(
        session,
        {"importer_id": module.IMPORTER, "schema_name": "mrf", "source_run_id": RUN_ID},
        ownership,
        source_copy,
        asyncio.get_running_loop().time() + 300,
    )
    if fault == "cancel":
        raise asyncio.CancelledError
    await archive.complete_restore(session, ownership)
    if fault == "stored_json":
        await session.execute(
            text(
                f'UPDATE "{ownership.schema_name}".provider_profile_fact '
                "SET value_json=value_json::jsonb::json WHERE fact_id=:fact"
            ),
            {"fact": case.facts[0]["fact_id"]},
        )
    artifact, bundle = await module.read_witness_bundle(
        session, ownership.schema_name, case.run, case.compact_metrics["bundle"]
    )
    counts_by_field = await module.native_witness_counts(session, ownership.schema_name, case.run, bundle)
    counts_by_field["bundle_reference"] = module.bundle_reference(artifact)
    final = module.store._completion_metrics(case.run, case.compact_metrics, counts_by_field)
    assert final["retained_facts"] == final["retained_source_records"] == 2
    actual_json = await session.scalar(
        text(f'SELECT value_json::text FROM "{ownership.schema_name}".provider_profile_fact WHERE fact_id=:fact'),
        {"fact": case.facts[0]["fact_id"]},
    )
    assert actual_json.encode("utf-8") == module.encoded_json(case.profiles["111111"]["fact_values"][0]["value_json"])


@pytest.mark.parametrize(
    "fault", [None, "canonical", "coercion", "support", "support_type", "incomplete", "stored_json", "cancel"]
)
async def test_native_witness_copy_preserves_canonical_types_and_closes_failures(witnessed_case, monkeypatch, fault):
    from process import source_profile_result_archive as archive

    case = witnessed_case
    _change_witness(case, fault)
    dataset_id = uuid.uuid4()

    async def forbidden(*args, **kwargs):
        raise AssertionError("witness reached retained-row hashing")

    monkeypatch.setattr(type(module.store), "_inventory_counts", forbidden)
    async with _database(monkeypatch) as database:
        await module.store.claim_run(case.run)
        await (
            database.insert(shared.ProviderProfileArtifact.__table__)
            .values(
                **{
                    **case.artifact,
                    "metadata_json": literal(module.encoded_json(case.artifact["metadata_json"]).decode("utf-8")).cast(
                        JSON
                    ),
                }
            )
            .status()
        )

        async def validate():
            async with database.transaction() as session:
                ownership = await archive.precreate_restore(
                    session, module.IMPORTER, dataset_id, contract=archive.CONTRACT
                )
                await _verify_native_witness(database, case, ownership, session, fault)
                await session.rollback()

        if fault:
            with pytest.raises(asyncio.CancelledError if fault == "cancel" else (ValueError, RuntimeError)):
                await validate()
        else:
            await validate()
        assert await database.scalar("SELECT to_regnamespace(:schema) IS NULL", schema=archive.stage_schema(dataset_id))
        await _assert_unpublished(database)
