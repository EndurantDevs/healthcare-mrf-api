# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native PostgreSQL capture permissions and shared retention quotas."""

from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from dataclasses import asdict
from types import SimpleNamespace

import pytest
import pytest_asyncio
from sqlalchemy import insert, text
from sqlalchemy.exc import DBAPIError

from db.models.custom_import import CustomImportCaptureParquetPart, CustomImportCaptureUsage
from process.custom_import import capture_retention, raw_capture_store
from process.custom_import.capture_pending import append_pending_parquet_part
from process.custom_import.capture_store import CaptureBundleConflict, CaptureStoreError
from process.custom_import.definition import canonical_json
from process.custom_import.segmented_capture_policy import SegmentedCapturePolicy
from tests.custom_import_postgres_support import isolated_publication_case
from tests.test_custom_import_capture_pending_postgres import _parts, _start_attempt
from tests.test_custom_import_capture_store_postgres import (
    _build_replayable_capture_set,
    _parquet_definition,
    _register_replayable_bundle,
    _seed_parquet_case,
)
from tests.test_custom_import_raw_capture_postgres import (
    _identity_kwargs,
    _mixed_policy,
    _register_raw,
    _sealed_originals,
    _seed_raw_case,
)
from tests.test_custom_import_segmented_capture_postgres import _POLICY, _part, _pending

pytestmark = pytest.mark.asyncio(loop_scope="module")


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def capture_case():
    async with isolated_publication_case(is_snapshot_storage_enabled=False) as case:
        yield case


@asynccontextmanager
async def _role(case):
    role = "capture_reader_" + uuid.uuid4().hex[:16]
    async with case.engine.begin() as connection:
        await connection.execute(text(f'CREATE ROLE "{role}" NOLOGIN NOSUPERUSER INHERIT'))
    try:
        yield role
    finally:
        async with case.engine.begin() as connection:
            await connection.execute(text(f'DROP OWNED BY "{role}"'))
            await connection.execute(text(f'DROP ROLE "{role}"'))


async def test_readonly_capture_permissions_are_enforced_by_postgres(capture_case):
    async with _role(capture_case) as role:
        relation = f'"{capture_case.schema_name}".custom_import_capture_raw_object'
        async with capture_case.sessions() as session, session.begin():
            await session.execute(text(f'GRANT USAGE ON SCHEMA "{capture_case.schema_name}" TO "{role}"'))
            await session.execute(text(f'GRANT SELECT (dataset_id, byte_count) ON {relation} TO "{role}"'))
            await session.execute(text(f'SET LOCAL ROLE "{role}"'))
            await session.execute(text(f"SELECT dataset_id, byte_count FROM {relation} LIMIT 0"))
        for statement in (
            f"INSERT INTO {relation} DEFAULT VALUES",
            f"UPDATE {relation} SET byte_count=0",
            f"DELETE FROM {relation}",
            f"TRUNCATE {relation}",
        ):
            await _denied_statement(capture_case, role, text(statement))


@pytest.mark.parametrize(
    "denied",
    (
        "bundle_insert",
        "header_insert",
        "raw_insert",
        "raw_select",
        "header_lock",
        "quota",
        "sequence",
    ),
)
async def test_raw_preflight_checks_effective_access_without_changing_storage(capture_case, denied):
    schema = f'"{capture_case.schema_name}"'
    bundle = f"{schema}.custom_import_capture_bundle"
    async with _role(capture_case) as role:
        await _legacy_grants(capture_case, role)
        async with capture_case.sessions() as session, session.begin():
            sequence = await session.scalar(
                text("SELECT pg_get_serial_sequence(:relation,'capture_bundle_id')"), {"relation": bundle}
            )
            await session.execute(text(f'GRANT USAGE ON SEQUENCE {sequence} TO "{role}"'))
            await session.execute(text(f'GRANT SELECT ON {schema}.custom_import_capture_usage TO "{role}"'))
            await session.execute(text(f'GRANT SELECT,INSERT ON {schema}.custom_import_capture_raw_object TO "{role}"'))
        async with capture_case.sessions() as session:
            before = (await session.execute(text(f"SELECT last_value,is_called FROM {sequence}"))).one()
            counts = await _preflight_capture_counts(session, schema)
        async with capture_case.sessions() as session, session.begin():
            await session.execute(text(f'SET LOCAL ROLE "{role}"'))
            await raw_capture_store.require_raw_capture_write_access(session)
        privilege, relation = {
            "bundle_insert": ("INSERT", bundle),
            "header_insert": ("INSERT", f"{schema}.custom_import_capture"),
            "raw_insert": ("INSERT", f"{schema}.custom_import_capture_raw_object"),
            "raw_select": ("SELECT", f"{schema}.custom_import_capture_raw_object"),
            "header_lock": ("UPDATE", f"{schema}.custom_import_capture"),
            "quota": ("SELECT", f"{schema}.custom_import_capture_usage"),
            "sequence": ("USAGE", f"SEQUENCE {sequence}"),
        }[denied]
        async with capture_case.sessions() as session, session.begin():
            await session.execute(text(f'REVOKE {privilege} ON {relation} FROM "{role}"'))
        with pytest.raises((DBAPIError, CaptureStoreError)) as rejected:
            async with capture_case.sessions() as session, session.begin():
                await session.execute(text(f'SET LOCAL ROLE "{role}"'))
                await raw_capture_store.require_raw_capture_write_access(session)
        if denied != "sequence":
            assert rejected.value.orig.sqlstate == "42501"
        async with capture_case.sessions() as session, session.begin():
            restored = "UPDATE" if denied == "sequence" else privilege
            await session.execute(text(f'GRANT {restored} ON {relation} TO "{role}"'))
            await session.execute(text(f'SET LOCAL ROLE "{role}"'))
            await raw_capture_store.require_raw_capture_write_access(session)
        async with capture_case.sessions() as session:
            assert (await session.execute(text(f"SELECT last_value,is_called FROM {sequence}"))).one() == before
            assert await _preflight_capture_counts(session, schema) == counts


async def _preflight_capture_counts(session, schema):
    return tuple(
        [
            await session.scalar(text(f"SELECT count(*) FROM {schema}.{relation}"))
            for relation in (
                "custom_import_capture_bundle",
                "custom_import_capture",
                "custom_import_capture_raw_object",
                "custom_import_capture_usage",
                "custom_import_capture_parquet_part",
            )
        ]
    )


async def _legacy_grants(case, role):
    schema = f'"{case.schema_name}"'
    async with case.engine.begin() as connection:
        await connection.execute(text(f'GRANT USAGE ON SCHEMA {schema} TO "{role}"'))
        await connection.execute(text(f'GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO "{role}"'))
        for relation in ("custom_import_capture_usage", "custom_import_capture_raw_object"):
            await connection.execute(text(f'REVOKE ALL ON {schema}.{relation} FROM "{role}"'))
        for relation in ("custom_import_capture_bundle", "custom_import_capture", "custom_import_capture_parquet_part"):
            await connection.execute(text(f'GRANT INSERT ON {schema}.{relation} TO "{role}"'))
        # Existing lifecycle/header row locks require an UPDATE privilege too.
        for relation in (
            "custom_import_dataset",
            "custom_import_execution",
            "custom_import_lease",
            "custom_import_capture_bundle",
            "custom_import_capture",
        ):
            await connection.execute(text(f'GRANT UPDATE ON {schema}.{relation} TO "{role}"'))


async def _append_as(case, role, request, registered, part):
    capture, records, arrow_bytes = part
    async with case.sessions() as session, session.begin():
        await session.execute(text(f'SET LOCAL ROLE "{role}"'))
        return await append_pending_parquet_part(
            session,
            request=request,
            capture_bundle_id=registered.capture_bundle_id,
            capture=capture,
            ordinal=1,
            record_count=records,
            arrow_byte_count=arrow_bytes,
        )


async def _metadata_grant(case, role, relation, columns):
    async with case.engine.begin() as connection:
        await connection.execute(text(f'GRANT SELECT ({columns}) ON "{case.schema_name}".{relation} TO "{role}"'))


async def _denied_statement(case, role, statement):
    with pytest.raises(DBAPIError) as rejected:
        async with case.sessions() as session, session.begin():
            await session.execute(text(f'SET LOCAL ROLE "{role}"'))
            await session.execute(statement)
    assert rejected.value.orig.sqlstate == "42501"


async def test_metadata_provisioning_preserves_restricted_segmented_writes(capture_case):
    async with _role(capture_case) as role:
        await _legacy_grants(capture_case, role)
        direct_attempt = await _pending(capture_case)
        async with capture_case.sessions() as session, session.begin():
            await session.execute(text(f'SET LOCAL ROLE "{role}"'))
            await session.execute(insert(CustomImportCaptureParquetPart).values(**_part(direct_attempt)))
        request, registered = await _start_attempt(capture_case)
        part = next(iter(_parts(request).values()))[0]
        for relation, columns in (
            ("custom_import_capture_usage", "dataset_id, retained_bytes"),
            ("custom_import_capture_raw_object", "dataset_id, byte_count"),
        ):
            with pytest.raises(DBAPIError) as rejected:
                await _append_as(capture_case, role, request, registered, part)
            assert rejected.value.orig.sqlstate == "42501"
            await _metadata_grant(capture_case, role, relation, columns)
        assert await _append_as(capture_case, role, request, registered, part) == 1
        assert await _append_as(capture_case, role, request, registered, part) == 0
        await _metadata_remains_read_only(capture_case, role)


@pytest.mark.parametrize("missing_relation", ("custom_import_capture_usage", "custom_import_capture_raw_object"))
async def test_retention_preflight_requires_both_metadata_grants(capture_case, missing_relation):
    columns_by_relation = {
        "custom_import_capture_usage": "dataset_id, retained_bytes",
        "custom_import_capture_raw_object": "dataset_id, byte_count",
    }
    async with _role(capture_case) as role:
        await _legacy_grants(capture_case, role)
        for relation, columns in columns_by_relation.items():
            if relation != missing_relation:
                await _metadata_grant(capture_case, role, relation, columns)
        with pytest.raises(DBAPIError) as rejected:
            async with capture_case.sessions() as session, session.begin():
                await session.execute(text(f'SET LOCAL ROLE "{role}"'))
                await capture_retention.require_capture_retention_read_access(session)
        assert rejected.value.orig.sqlstate == "42501"
        await _metadata_grant(capture_case, role, missing_relation, columns_by_relation[missing_relation])
        async with capture_case.sessions() as session, session.begin():
            await session.execute(text(f'SET LOCAL ROLE "{role}"'))
            await capture_retention.require_capture_retention_read_access(session)
        await _metadata_remains_read_only(capture_case, role)


async def _metadata_remains_read_only(case, role):
    schema = f'"{case.schema_name}"'
    await _denied_statement(case, role, text(f"SELECT payload FROM {schema}.custom_import_capture_raw_object"))
    await _denied_statement(case, role, text(f"UPDATE {schema}.custom_import_capture_usage SET retained_bytes=0"))
    await _denied_statement(case, role, text(f"DELETE FROM {schema}.custom_import_capture_raw_object"))
    async with case.sessions() as session, session.begin():
        await session.execute(text(f'REVOKE INSERT ON {schema}.custom_import_capture_parquet_part FROM "{role}"'))
    await _denied_statement(
        case, role, insert(CustomImportCaptureParquetPart).values(capture_bundle_id=1, stream_slot=1, part_ordinal=1)
    )


async def test_existing_segmented_writer_does_not_block_permitted_raw_capture(capture_case):
    async with _role(capture_case) as role:
        await _legacy_grants(capture_case, role)
        seed, captures = await _seed_raw_case(capture_case)
        async with capture_case.sessions() as session, session.begin():
            registered = await _register_raw(session, seed, captures)
        assert registered.created
        async with capture_case.sessions() as session:
            replayed = await raw_capture_store.load_replayable_raw_bundle(
                session, capture_bundle_id=registered.capture_bundle_id, **_identity_kwargs(seed)
            )
            assert replayed == captures


def _originals(captures, snapshot):
    return _sealed_originals(
        _parquet_definition(), {capture.receipt.stream_id: capture.parts[0] for capture in captures}, snapshot
    )


async def _mixed_history(case, first_kind):
    seed = await _seed_parquet_case(case)
    legacy = _build_replayable_capture_set()
    originals = _originals(legacy, "synthetic-mixed-original")
    canonical_bytes = sum(len(part) for capture in legacy for part in capture.parts)
    raw_bytes = sum(len(capture.payload) for capture in originals)
    for kind in (first_kind, "legacy" if first_kind == "raw" else "raw"):
        if kind == "legacy":
            await _register_replayable_bundle(case, seed, legacy)
        else:
            async with case.sessions() as session, session.begin():
                await _register_raw(session, seed, originals, canonical_bytes + raw_bytes)
    async with case.sessions() as session, session.begin():
        usage = await session.get(CustomImportCaptureUsage, seed.dataset_id)
        assert usage.retained_bytes == canonical_bytes
        await capture_retention.validate_dataset_retained_bytes(
            session, dataset_id=seed.dataset_id, new_byte_count=0, maximum_retained_bytes=canonical_bytes + raw_bytes
        )
    return seed, legacy, canonical_bytes + raw_bytes


@pytest.mark.parametrize("first_kind", ("raw", "legacy"))
async def test_mixed_history_charges_both_orders_without_changing_v1(capture_case, first_kind):
    seed, legacy, retained_bytes = await _mixed_history(capture_case, first_kind)
    next_originals = _originals(legacy, "synthetic-next-original")
    new_bytes = sum(len(capture.payload) for capture in next_originals)
    with pytest.raises(CaptureBundleConflict, match="quota"):
        async with capture_case.sessions() as session, session.begin():
            await _register_raw(session, seed, next_originals, retained_bytes + new_bytes - 1)
    policy = SegmentedCapturePolicy.from_mapping(_POLICY)
    part = next(
        iter(_parts(SimpleNamespace(policy=policy, source_snapshot_token="synthetic-pending-snapshot")).values())
    )[0]
    charge = len(part[0].payload) + len(canonical_json(asdict(part[0].manifest)).encode())
    policy = SegmentedCapturePolicy.from_mapping(
        _mixed_policy(*part).to_mapping() | {"maximum_dataset_retained_bytes": retained_bytes + charge - 1}
    )
    bounded_request, bounded = await _start_attempt(capture_case, seed=seed, policy=policy)
    with pytest.raises(CaptureBundleConflict, match="quota"):
        async with capture_case.sessions() as session, session.begin():
            await append_pending_parquet_part(
                session,
                request=bounded_request,
                capture_bundle_id=bounded.capture_bundle_id,
                capture=part[0],
                ordinal=1,
                record_count=part[1],
                arrow_byte_count=part[2],
            )
    async with capture_case.sessions() as session, session.begin():
        replayed = await _register_raw(session, seed, _originals(legacy, "synthetic-mixed-original"), 0)
        assert not replayed.created
