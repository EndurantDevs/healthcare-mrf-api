# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded Geo reduction, native COPY and one original transactional heap cut."""

import asyncio
import os
import time
from dataclasses import dataclass
from uuid import uuid4

from sqlalchemy import text

from db.models import GeoZipLookup
from db.plan_release_storage import storage_identifier, storage_relation
from process import reference_family_result_generation as generation
from process.geo_source_storage import ROOT, publish_geo_generation, replay_geo_reads, require, require_geo_storage
from process.native_copy_cleanup import close_interrupted_copy, invalidate_copy_owner
from process.provider_directory_generic_source_policy import bound_publisher, source_transaction
from process.provider_directory_generic_source_storage import _close_grants

MAX_ROWS = 2000
MAX_BYTES = 16 * 1024 * 1024
MAX_SECONDS = 300
COLUMNS = tuple(str(column.name) for column in GeoZipLookup.__table__.columns)


@dataclass(frozen=True)
class GeoCandidate:
    """Ephemeral identity on one original authenticated native transaction."""

    schema: str
    raw: str
    stage: str
    old_oid: int
    authority: object
    deadline: float


def record_bytes(record):
    """Bound binary field framing conservatively before entering COPY."""
    return 64 + sum(32 + len(str(value).encode("utf-8")) for value in record)


async def _copy(session, name, columns, records, deadline):
    require(0 < len(records) <= MAX_ROWS and sum(map(record_bytes, records)) <= MAX_BYTES, "copy_budget_exceeded")
    remaining = deadline - time.monotonic()
    require(0 < remaining <= MAX_SECONDS, "deadline_exceeded")
    driver = bound_publisher().connection
    try:
        async with asyncio.timeout(remaining):
            result = await driver.copy_records_to_table(
                name,
                schema_name=bound_publisher().policy.schema,
                columns=columns,
                records=records,
                timeout=remaining,
            )
    except asyncio.CancelledError, TimeoutError:
        await close_interrupted_copy(driver)
        await invalidate_copy_owner(session, driver)
        raise
    require(result == f"COPY {len(records)}", "copy_count_changed")


async def _prepare(session, schema, deadline):
    oid = await require_geo_storage(session, schema_name=schema)
    authority = await generation.read_reference_family_result_generation_authority(
        session,
        importer_id="geo",
        schema_name=schema,
    )
    require(authority.relation_oids is None or authority.relation_oids == (oid,), "incumbent_changed")
    token = uuid4().hex
    candidate = GeoCandidate(schema, "geo_raw_" + token, "geo_candidate_" + token, oid, authority, deadline)
    root = storage_relation(schema, ROOT)
    await session.execute(
        text(
            f"CREATE TABLE {storage_relation(schema, candidate.raw)} "
            f"(LIKE {root} INCLUDING CONSTRAINTS EXCLUDING INDEXES,"
            "source_ordinal bigint NOT NULL,source_batch bigint NOT NULL)"
        )
    )
    await session.execute(
        text(
            f"CREATE TABLE {storage_relation(schema, candidate.stage)} (LIKE {root} INCLUDING CONSTRAINTS EXCLUDING INDEXES)"
        )
    )
    for name in (candidate.raw, candidate.stage):
        await _close_grants(bound_publisher().connection, bound_publisher().policy, name)
    return candidate


async def _load(session, candidate, rows):
    records, total, size = [], 0, 0
    for row in rows:
        require(time.monotonic() < candidate.deadline, "deadline_exceeded")
        record = tuple(row[name] for name in COLUMNS) + (total, total // MAX_ROWS)
        footprint = record_bytes(record)
        require(footprint <= MAX_BYTES, "copy_row_oversized")
        if records and (len(records) == MAX_ROWS or size + footprint > MAX_BYTES):
            await _copy(
                session, candidate.raw, (*COLUMNS, "source_ordinal", "source_batch"), records, candidate.deadline
            )
            records, size = [], 0
        records.append(record)
        size += footprint
        total += 1
    if records:
        await _copy(session, candidate.raw, (*COLUMNS, "source_ordinal", "source_batch"), records, candidate.deadline)
    return total


async def _reduce(session, candidate):
    raw = storage_relation(candidate.schema, candidate.raw)
    await session.execute(text(f"CREATE INDEX ON {raw} (source_batch,zip_code)"))
    await session.execute(text(f"CREATE INDEX ON {raw} (zip_code,source_ordinal DESC)"))
    duplicates = await session.scalar(
        text(f"SELECT EXISTS(SELECT FROM {raw} GROUP BY source_batch,zip_code HAVING count(*)>1)")
    )
    require(not duplicates, "duplicate_zip_in_batch")
    columns = ",".join(storage_identifier(name) for name in COLUMNS)
    last = None
    while True:
        records = (
            await session.execute(
                text(
                    f"SELECT DISTINCT ON(zip_code) {columns} FROM {raw} "
                    "WHERE CAST(:last AS varchar) IS NULL OR zip_code>:last "
                    "ORDER BY zip_code,source_ordinal DESC LIMIT 2000"
                ),
                {"last": last},
            )
        ).all()
        if not records:
            return
        await _copy(session, candidate.stage, COLUMNS, [tuple(row) for row in records], candidate.deadline)
        last = records[-1][0]


async def _finish(session, candidate):
    stage = storage_relation(candidate.schema, candidate.stage)
    await session.execute(text(f"ALTER TABLE {stage} ADD PRIMARY KEY(zip_code)"))
    for index in GeoZipLookup.__my_additional_indexes__:
        columns = ",".join(storage_identifier(name) for name in index["index_elements"])
        await session.execute(text(f"CREATE INDEX ON {stage} ({columns})"))
    await require_geo_storage(session, schema_name=candidate.schema, relation_name=candidate.stage)
    columns = ",".join(storage_identifier(name) for name in COLUMNS)
    raw = storage_relation(candidate.schema, candidate.raw)
    differs = await session.scalar(
        text(
            f"WITH expected AS(SELECT DISTINCT ON(zip_code) {columns} FROM {raw} ORDER BY zip_code,source_ordinal DESC) "
            f"SELECT EXISTS((SELECT {columns} FROM expected EXCEPT SELECT {columns} FROM {stage}) UNION ALL "
            f"(SELECT {columns} FROM {stage} EXCEPT SELECT {columns} FROM expected))"
        )
    )
    require(not differs, "reduction_changed")


async def _rename_indexes(session, schema, relation, prefix):
    indexes = (
        (
            await session.execute(
                text(
                    "SELECT c.relname FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid "
                    "WHERE i.indrelid=to_regclass(:relation) ORDER BY i.indexrelid"
                ),
                {"relation": relation},
            )
        )
        .scalars()
        .all()
    )
    for ordinal, name in enumerate(indexes):
        await session.execute(
            text(f"ALTER INDEX {storage_relation(schema, name)} RENAME TO {storage_identifier(prefix + str(ordinal))}")
        )


async def _canonical_indexes(session, candidate):
    indexes = (
        (
            await session.execute(
                text(
                    "SELECT c.relname FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid "
                    "WHERE i.indrelid=to_regclass(:relation) ORDER BY i.indisprimary DESC,i.indexrelid"
                ),
                {"relation": storage_relation(candidate.schema, ROOT)},
            )
        )
        .scalars()
        .all()
    )
    names = [ROOT + "_pkey", *(ROOT + "_idx_" + index["name"] for index in GeoZipLookup.__my_additional_indexes__)]
    require(len(indexes) == len(names), "indexes_changed")
    for before, after in zip(indexes, names, strict=True):
        await session.execute(
            text(f"ALTER INDEX {storage_relation(candidate.schema, before)} RENAME TO {storage_identifier(after)}")
        )


async def _publish(session, candidate):
    root = storage_relation(candidate.schema, ROOT)
    stage = storage_relation(candidate.schema, candidate.stage)
    await session.execute(text(f"LOCK TABLE ONLY {root},{stage} IN ACCESS EXCLUSIVE MODE NOWAIT"))
    await require_geo_storage(session, schema_name=candidate.schema, expected_oid=candidate.old_oid)
    await require_geo_storage(session, schema_name=candidate.schema, relation_name=candidate.stage)
    authority = await generation.read_reference_family_result_generation_authority(
        session,
        importer_id="geo",
        schema_name=candidate.schema,
        lock=True,
    )
    require(authority.as_dict() == candidate.authority.as_dict(), "incumbent_changed")
    await _close_grants(bound_publisher().connection, bound_publisher().policy, candidate.stage, readable=True)
    await replay_geo_reads(session, candidate.old_oid, stage)
    token = candidate.stage.removeprefix("geo_candidate_")
    await _rename_indexes(session, candidate.schema, root, "geo_previous_" + token + "_")
    await session.execute(text(f"ALTER TABLE {root} RENAME TO {storage_identifier('geo_previous_' + token)}"))
    await session.execute(text(f"ALTER TABLE {stage} RENAME TO {storage_identifier(ROOT)}"))
    await _canonical_indexes(session, candidate)
    await publish_geo_generation(session, schema_name=candidate.schema, expected_authority=authority)
    require(
        await require_geo_storage(session, schema_name=candidate.schema) != candidate.old_oid, "publication_changed"
    )
    await session.execute(text(f"DROP TABLE {storage_relation(candidate.schema, candidate.raw)}"))


async def _require_test_target(session, test_mode):
    """Never let a genuine base-database factory erase the existing test suffix."""
    from process.ext.utils import TEST_DATABASE_SUFFIX

    if test_mode and TEST_DATABASE_SUFFIX:
        expected = os.getenv("HLTHPRT_DB_DATABASE", "postgres") + TEST_DATABASE_SUFFIX
        require(await session.scalar(text("SELECT current_database()")) == expected, "test_database_changed")


async def publish_geo_rows(database, rows, expected_count, *, deadline, test_mode=False):
    """Fail atomically before serving mutation unless every bounded stage check passes."""
    async with source_transaction(database, isolation="repeatable read") as session:
        async with session.begin_nested(), asyncio.timeout_at(deadline):
            await _require_test_target(session, test_mode)
            candidate = await _prepare(session, bound_publisher().policy.schema, deadline)
            total = await _load(session, candidate, rows)
            from process.geo_import import _validate_stable_source_row_count

            _validate_stable_source_row_count(total, expected_count)
            require(total > 0, "source_empty")
            await _reduce(session, candidate)
            await _finish(session, candidate)
            await _publish(session, candidate)
