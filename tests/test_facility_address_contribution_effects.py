# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native parity and row-CAS proof for destination-only facility effects."""

import importlib
import os
from dataclasses import replace
from functools import partial
from uuid import uuid4

import asyncpg
import pytest
from sqlalchemy import text

from db.models import db
from process import facility_address_contribution_capture as capture
from process import facility_address_contribution_effects as effects
from process import reference_family_archive as family
from process.ext import address_canon
from tests.test_facility_address_contribution_capture import _seed_aliased_facility_source

anchors = importlib.import_module("process.facility_anchors")
STAGE = "facility_effect_source_fixture"
ARTIFACT = "facility_effect_artifact_fixture"
STAGE_SCHEMA = "facility_effect_incoming"
FIELD_MAP = {
    "first_line": "address_line1",
    "second_line": "NULL",
    "city": "city",
    "state": "state",
    "zip": "zip_code",
    "country": "'US'",
}


async def _archive_images():
    snapshots = await db.all("SELECT address_key::text,to_jsonb(a) FROM mrf.address_archive_v2 a ORDER BY address_key")
    return {snapshot[0]: snapshot[1] for snapshot in snapshots}


def _without_timestamps(snapshots):
    return {
        key: {
            column: entry
            for column, entry in snapshot.items()
            if column not in ("first_seen_at", "last_seen_at", "geocoded_at")
        }
        for key, snapshot in snapshots.items()
    }


async def _seed_existing_and_unrelated():
    await db.status("CREATE TABLE mrf.facility_effect_prior (LIKE mrf.facility_effect_source_fixture)")
    await db.status(
        "INSERT INTO mrf.facility_effect_prior VALUES "
        "(NULL,'555 Existing Way','Austin','TX','78702',20,-80,'Hospital'),"
        "(NULL,'444 Unrelated Way','Austin','TX','78702',21,-81,'Hospital')"
    )
    await address_canon.stamp_address_keys("facility_effect_prior", FIELD_MAP, schema="mrf", shards=1)
    await address_canon.resolve_into_archive(
        "facility_effect_prior", FIELD_MAP, schema="mrf", source_bit=16, priority=2
    )
    await anchors._refresh_archive_geocodes_from_facility_anchors("facility_effect_prior", "mrf")
    await db.status(
        f"INSERT INTO mrf.{STAGE} SELECT * FROM mrf.facility_effect_prior WHERE address_line1='555 Existing Way'"
    )
    await db.status(f"UPDATE mrf.{STAGE} SET latitude=35,longitude=-100 WHERE address_line1='555 Existing Way'")
    await db.status(f"INSERT INTO mrf.{STAGE} VALUES (NULL,'999 New Way','Austin','TX','78702',22,-82,'Hospital')")
    await address_canon.stamp_address_keys(STAGE, FIELD_MAP, schema="mrf", shards=1)


async def _native_capture_fixture():
    await _seed_aliased_facility_source("mrf", STAGE, ARTIFACT, FIELD_MAP)
    await _seed_existing_and_unrelated()
    before = await _archive_images()
    await db.status("CREATE TABLE mrf.facility_effect_baseline AS SELECT * FROM mrf.address_archive_v2")
    await address_canon.resolve_into_archive(
        STAGE,
        FIELD_MAP,
        schema="mrf",
        source_bit=8,
        priority=4,
        source_capture=partial(capture.capture_canonical_observations, contribution_table=ARTIFACT),
    )
    await anchors._refresh_archive_geocodes_from_facility_anchors(STAGE, "mrf", contribution_table=ARTIFACT)
    after = await _archive_images()
    await db.status(f"CREATE SCHEMA {STAGE_SCHEMA}")
    await db.status(f"CREATE TABLE {STAGE_SCHEMA}.facility_address_contribution (LIKE mrf.{ARTIFACT} INCLUDING ALL)")
    await db.status(f"INSERT INTO {STAGE_SCHEMA}.facility_address_contribution SELECT * FROM mrf.{ARTIFACT}")
    await db.status("CREATE SCHEMA hp_snapshot_retention")
    await db.status(
        "CREATE TABLE hp_snapshot_retention.facility_address_effect ("
        "operation_id uuid,address_key uuid,before_image jsonb,after_image jsonb,"
        "before_xmin text,after_xmin text,applied bool NOT NULL DEFAULT false,"
        "reverted bool NOT NULL DEFAULT false,PRIMARY KEY(operation_id,address_key))"
    )
    return before, after


async def _restore_baseline():
    columns = await db.all(
        "SELECT attname FROM pg_attribute WHERE attrelid='mrf.address_archive_v2'::regclass "
        "AND attnum>0 AND NOT attisdropped AND attname <> 'address_key' ORDER BY attnum"
    )
    names = ",".join('"' + column[0] + '"' for column in columns)
    async with db.transaction():
        await db.status(
            "DELETE FROM mrf.address_archive_v2 WHERE address_key NOT IN "
            "(SELECT address_key FROM mrf.facility_effect_baseline)"
        )
        await db.status(
            f"UPDATE mrf.address_archive_v2 a SET ({names})=(SELECT {names} "
            "FROM mrf.facility_effect_baseline b WHERE b.address_key=a.address_key)"
        )


async def _prepare_effect():
    async with db.transaction() as session:
        await session.execute(text("SET LOCAL TimeZone TO 'Pacific/Honolulu'"))
        return await effects.prepare_facility_address_effects(
            session,
            operation_id=uuid4(),
            stage_schema=STAGE_SCHEMA,
            schema="mrf",
        )


async def _assert_native_replay(before, native_after):
    await _restore_baseline()
    assert await _archive_images() == before
    receipt = await _prepare_effect()
    assert await _archive_images() == before
    async with db.transaction() as session:
        await session.execute(text("SET LOCAL TimeZone TO 'Europe/Prague'"))
        await effects.apply_facility_address_effects(session, receipt)
    replay_after = await _archive_images()
    assert _without_timestamps(replay_after) == _without_timestamps(native_after)
    assert len(replay_after) == len(before) + 1
    existing = next(snapshot for snapshot in replay_after.values() if snapshot["first_line"] == "555 Existing Way")
    assert (existing["source_bits"], existing["lat"], existing["long"]) == (24, 20, -80)
    unrelated_key = next(key for key, snapshot in before.items() if snapshot["first_line"] == "444 Unrelated Way")
    assert replay_after[unrelated_key] == before[unrelated_key]
    async with db.transaction() as session:
        await session.execute(text("SET LOCAL TimeZone TO 'Asia/Tokyo'"))
        await effects.rollback_facility_address_effects(session, receipt)
    assert await _archive_images() == before


async def _assert_later_writer_fences(before):
    receipt = await _prepare_effect()
    await db.status(
        "UPDATE mrf.address_archive_v2 SET source_bits=source_bits|32 WHERE first_line='Accumulated target display'"
    )
    changed_before = await _archive_images()
    with pytest.raises(RuntimeError, match="destination changed"):
        async with db.transaction() as session:
            await effects.apply_facility_address_effects(session, receipt)
    assert await _archive_images() == changed_before
    await _restore_baseline()
    assert await _archive_images() == before
    receipt = await _prepare_effect()
    async with db.transaction() as session:
        await effects.apply_facility_address_effects(session, receipt)
    await db.status(
        "UPDATE mrf.address_archive_v2 SET source_bits=source_bits|32 WHERE first_line='Accumulated target display'"
    )
    changed_after = await _archive_images()
    with pytest.raises(RuntimeError, match="destination changed"):
        async with db.transaction() as session:
            await effects.rollback_facility_address_effects(session, receipt)
    assert await _archive_images() == changed_after


async def _assert_atomic_failure_and_alias_fence(before):
    await _restore_baseline()
    receipt = await _prepare_effect()
    with pytest.raises(RuntimeError, match="synthetic post-apply failure"):
        async with db.transaction() as session:
            await effects.apply_facility_address_effects(session, receipt)
            raise RuntimeError("synthetic post-apply failure")
    assert await _archive_images() == before
    assert not await db.scalar(
        "SELECT bool_or(applied) FROM hp_snapshot_retention.facility_address_effect "
        f"WHERE operation_id='{receipt['operation_id']}'"
    )
    with pytest.raises(RuntimeError, match="alias identity changed"):
        async with db.transaction() as session:
            await db.status("UPDATE mrf.address_alias_state_v1 SET generation=generation+1")
            await effects.apply_facility_address_effects(session, receipt)
    assert await _archive_images() == before


async def _assert_concurrent_writer_lock(before):
    receipt = await _prepare_effect()
    connection = await asyncpg.connect(
        host=os.environ["HLTHPRT_DB_HOST"],
        port=int(os.environ["HLTHPRT_DB_PORT"]),
        user=os.environ["HLTHPRT_DB_USER"],
        database=os.environ["HLTHPRT_DB_DATABASE"],
    )
    try:
        transaction = connection.transaction()
        await transaction.start()
        await connection.execute(
            "UPDATE mrf.address_archive_v2 SET source_bits=source_bits WHERE first_line='Accumulated target display'"
        )
        with pytest.raises(Exception, match="lock timeout"):
            async with db.transaction() as session:
                await effects.apply_facility_address_effects(session, receipt)
        await transaction.rollback()
        assert await _archive_images() == before
    finally:
        await connection.close()


async def _assert_invalid_geocode_rejected():
    """Missing JSON fields must not pass validation through SQL NULL comparisons."""
    with pytest.raises(RuntimeError, match="payload is invalid"):
        async with db.transaction() as session:
            await db.status(
                f"UPDATE {STAGE_SCHEMA}.facility_address_contribution SET payload='{{}}'::jsonb WHERE kind='geocode'"
            )
            await effects.prepare_facility_address_effects(session, operation_id=uuid4(), stage_schema=STAGE_SCHEMA)


async def _assert_stage_binding(before):
    receipt = await _prepare_effect()
    ownership = family.ReferenceFamilyStageOwnership(
        "facility-anchors",
        uuid4(),
        STAGE_SCHEMA,
        receipt["stage_schema_oid"],
        (("facility_address_contribution", receipt["contribution_oid"]),),
    )
    incumbent = family.ReferenceFamilyIncumbent("facility-anchors", "mrf", ())
    await db.status("CREATE SCHEMA facility_effect_other")
    await db.status(
        f"CREATE TABLE facility_effect_other.facility_address_contribution "
        f"AS TABLE {STAGE_SCHEMA}.facility_address_contribution"
    )
    schema_oid, relation_oid = (
        await db.all(
            "SELECT relnamespace,oid FROM pg_class "
            "WHERE oid='facility_effect_other.facility_address_contribution'::regclass"
        )
    )[0]
    other = replace(
        ownership,
        schema_name="facility_effect_other",
        schema_oid=schema_oid,
        relation_oids=(("facility_address_contribution", relation_oid),),
    )
    for candidate, destination in (
        (other, incumbent),
        (replace(ownership, schema_oid=schema_oid), incumbent),
        (replace(ownership, relation_oids=other.relation_oids), incumbent),
        (ownership, replace(incumbent, schema_name="facility_effect_other")),
    ):
        with pytest.raises(family.ReferenceFamilyArchiveError, match="stage or destination differs"):
            async with db.transaction() as session:
                await family._apply_validated_contribution(session, candidate, destination, receipt)
        assert await _archive_images() == before
    async with db.transaction() as session:
        await family._apply_validated_contribution(session, ownership, incumbent, receipt)
    async with db.transaction() as session:
        await effects.rollback_facility_address_effects(session, receipt)
    assert await _archive_images() == before


@pytest.mark.asyncio(loop_scope="session")
async def test_native_facility_replay_parity_rollback_and_writer_fences(monkeypatch):
    if os.getenv("HP_FACILITY_CAPTURE_POSTGRES_TEST") != "1" or "test" not in os.getenv("HLTHPRT_DB_DATABASE", ""):
        pytest.skip("requires an explicitly enabled disposable migrated test database")
    assert os.getenv("HLTHPRT_DB_SCHEMA", "mrf") == "mrf"
    monkeypatch.setenv("HLTHPRT_ADDRESS_CANON_RUST_MATERIALIZE", "false")
    monkeypatch.setenv("HLTHPRT_ADDRESS_CANON_ZIP_RESTORE_ENABLED", "false")
    try:
        before, native_after = await _native_capture_fixture()
        await _assert_native_replay(before, native_after)
        await _assert_stage_binding(before)
        await _assert_later_writer_fences(before)
        await _assert_atomic_failure_and_alias_fence(before)
        await _assert_concurrent_writer_lock(before)
        await _assert_invalid_geocode_rejected()
    finally:
        await db.disconnect()
