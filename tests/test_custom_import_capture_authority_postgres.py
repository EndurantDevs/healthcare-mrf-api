# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native trusted capture, legacy writer and metadata-only privilege proofs."""

from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from sqlalchemy import insert, text
from sqlalchemy.exc import DBAPIError

from db.models.custom_import import CustomImportCaptureParquetPart, CustomImportCaptureUsage
from process.custom_import import capture_retention, https_candidate
from process.custom_import.capture_authority import require_raw_capture_authority
from process.custom_import.capture_pending import append_pending_parquet_part
from process.custom_import.capture_store import CaptureBundleConflict, CaptureStoreError
from process.custom_import.definition import canonical_json
from process.custom_import.definition_store import register_definition
from process.custom_import.segmented_capture_policy import SegmentedCapturePolicy
from tests.custom_import_postgres_support import isolated_publication_case
from tests.test_custom_import_capture_pending_postgres import _parts, _start_attempt
from tests.test_custom_import_capture_store_postgres import (
    _build_replayable_capture_set,
    _parquet_definition,
    _register_replayable_bundle,
    _seed_parquet_case,
)
from tests.test_custom_import_https_originals import _original_request
from tests.test_custom_import_raw_capture_postgres import _mixed_policy, _register_raw, _sealed_originals
from tests.test_custom_import_segmented_capture_postgres import _POLICY, _part, _pending

pytestmark = pytest.mark.asyncio(loop_scope="module")


@pytest_asyncio.fixture(scope="module", loop_scope="module")
async def capture_case():
    async with isolated_publication_case(is_snapshot_storage_enabled=False) as case:
        yield case


@asynccontextmanager
async def _roles(case):
    created_roles = []
    try:
        for _ in range(2):
            role = "capture_reader_" + uuid.uuid4().hex[:16]
            async with case.engine.begin() as connection:
                await connection.execute(text(f'CREATE ROLE "{role}" NOLOGIN NOSUPERUSER INHERIT'))
            created_roles.append(role)
        yield SimpleNamespace(writer=created_roles[0], member=created_roles[1])
    finally:
        for role in reversed(created_roles):
            async with case.engine.begin() as connection:
                await connection.execute(text(f'DROP OWNED BY "{role}"'))
                await connection.execute(text(f'DROP ROLE "{role}"'))


@pytest.mark.parametrize(
    ("relation", "privilege"),
    (
        ("custom_import_capture_bundle", "INSERT"),
        ("custom_import_capture", "UPDATE"),
        ("custom_import_capture_parquet_part", "INSERT"),
        ("custom_import_capture_usage", "UPDATE"),
        ("custom_import_capture_raw_object", "DELETE"),
        ("custom_import_capture_raw_object", "TRUNCATE"),
        ("custom_import_capture_parquet_part", "INSERT (payload)"),
        ("custom_import_capture_parquet_part", "UPDATE (byte_count)"),
    ),
)
async def test_nonowner_table_and_column_writes_block_originals(capture_case, relation, privilege):
    async with _roles(capture_case) as roles:
        async with capture_case.sessions() as session, session.begin():
            await require_raw_capture_authority(session)
            await session.execute(
                text(f'GRANT {privilege} ON "{capture_case.schema_name}".{relation} TO "{roles.writer}"')
            )
            with pytest.raises(CaptureStoreError, match="closed capture write grants"):
                await require_raw_capture_authority(session)


async def test_inherited_column_grant_is_checked_at_its_root(capture_case):
    async with _roles(capture_case) as roles:
        relation = f'"{capture_case.schema_name}".custom_import_capture_parquet_part'
        async with capture_case.sessions() as session, session.begin():
            await session.execute(text(f'GRANT INSERT (payload) ON {relation} TO "{roles.writer}"'))
            await session.execute(text(f'GRANT "{roles.writer}" TO "{roles.member}"'))
            assert await session.scalar(
                text("SELECT has_column_privilege(:role, :relation, 'payload', 'INSERT')"),
                {"role": roles.member, "relation": relation},
            )
            with pytest.raises(CaptureStoreError, match="closed capture write grants"):
                await require_raw_capture_authority(session)


@pytest.mark.parametrize("grant_root", ("owner_assumable", "superuser"))
async def test_privileged_grant_root_cannot_leak_writes_to_restricted_member(capture_case, grant_root):
    async with _roles(capture_case) as roles:
        relation = f'"{capture_case.schema_name}".custom_import_capture_raw_object'
        async with capture_case.sessions() as session, session.begin():
            owner = await session.scalar(
                text("SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid = to_regclass(:relation)"),
                {"relation": relation},
            )
            if grant_root == "owner_assumable":
                await session.execute(text(f'GRANT "{owner}" TO "{roles.writer}" WITH INHERIT FALSE, SET TRUE'))
            else:
                await session.execute(text(f'ALTER ROLE "{roles.writer}" SUPERUSER'))
            await session.execute(text(f'GRANT "{roles.writer}" TO "{roles.member}" WITH INHERIT TRUE, SET FALSE'))
            await session.execute(text(f'GRANT DELETE ON {relation} TO "{roles.writer}"'))
            parameters_by_name = {"role": roles.member, "owner": owner, "relation": relation}
            assert await session.scalar(
                text("SELECT has_table_privilege(:role, :relation, 'DELETE')"), parameters_by_name
            )
            assert not await session.scalar(text("SELECT pg_has_role(:role, :owner, 'USAGE')"), parameters_by_name)
            assert not await session.scalar(text("SELECT pg_has_role(:role, :owner, 'SET')"), parameters_by_name)
            with pytest.raises(CaptureStoreError, match="closed capture write grants"):
                await require_raw_capture_authority(session)


async def test_public_grant_refuses_and_rollback_restores_authority(capture_case):
    async with capture_case.sessions() as session:
        try:
            await session.begin()
            await session.execute(
                text(f'GRANT INSERT ON "{capture_case.schema_name}".custom_import_capture_parquet_part TO PUBLIC')
            )
            with pytest.raises(CaptureStoreError, match="closed capture write grants"):
                await require_raw_capture_authority(session)
        finally:
            await session.rollback()
        async with session.begin():
            await require_raw_capture_authority(session)


async def test_nonowner_cannot_enable_raw_with_read_permissions(capture_case):
    async with _roles(capture_case) as roles, capture_case.sessions() as session, session.begin():
        await session.execute(text(f'GRANT USAGE ON SCHEMA "{capture_case.schema_name}" TO "{roles.writer}"'))
        await session.execute(
            text(
                f'GRANT SELECT (dataset_id, byte_count) ON "{capture_case.schema_name}".custom_import_capture_raw_object TO "{roles.writer}"'
            )
        )
        await session.execute(text(f'SET LOCAL ROLE "{roles.writer}"'))
        with pytest.raises(CaptureStoreError, match="trusted registrar"):
            await require_raw_capture_authority(session)


async def _legacy_grants(case, role):
    schema = f'"{case.schema_name}"'
    async with case.engine.begin() as connection:
        await connection.execute(text(f'GRANT USAGE ON SCHEMA {schema} TO "{role}"'))
        await connection.execute(text(f'GRANT SELECT ON ALL TABLES IN SCHEMA {schema} TO "{role}"'))
        for relation in (
            "custom_import_capture_usage",
            "custom_import_capture_raw_object",
            "custom_import_prepared_source",
            "custom_import_prepared_source_part",
        ):
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
    async with _roles(capture_case) as roles:
        await _legacy_grants(capture_case, roles.writer)
        direct_attempt = await _pending(capture_case)
        async with capture_case.sessions() as session, session.begin():
            await session.execute(text(f'SET LOCAL ROLE "{roles.writer}"'))
            await session.execute(insert(CustomImportCaptureParquetPart).values(**_part(direct_attempt)))
        request, registered = await _start_attempt(capture_case)
        part = next(iter(_parts(request).values()))[0]
        for relation, columns in (
            ("custom_import_capture_usage", "dataset_id, retained_bytes"),
            ("custom_import_capture_raw_object", "dataset_id, byte_count"),
            ("custom_import_prepared_source", "dataset_id, retained_byte_count"),
        ):
            with pytest.raises(DBAPIError) as rejected:
                await _append_as(capture_case, roles.writer, request, registered, part)
            assert rejected.value.orig.sqlstate == "42501"
            await _metadata_grant(capture_case, roles.writer, relation, columns)
        assert await _append_as(capture_case, roles.writer, request, registered, part) == 1
        assert await _append_as(capture_case, roles.writer, request, registered, part) == 0
        await _metadata_remains_read_only(capture_case, roles.writer)


@pytest.mark.parametrize(
    "missing_relation",
    ("custom_import_capture_usage", "custom_import_capture_raw_object", "custom_import_prepared_source"),
)
async def test_retention_preflight_requires_every_metadata_grant(capture_case, missing_relation):
    columns_by_relation = {
        "custom_import_capture_usage": "dataset_id, retained_bytes",
        "custom_import_capture_raw_object": "dataset_id, byte_count",
        "custom_import_prepared_source": "dataset_id, retained_byte_count",
    }
    async with _roles(capture_case) as roles:
        await _legacy_grants(capture_case, roles.writer)
        for relation, columns in columns_by_relation.items():
            if relation != missing_relation:
                await _metadata_grant(capture_case, roles.writer, relation, columns)
        with pytest.raises(DBAPIError) as rejected:
            async with capture_case.sessions() as session, session.begin():
                await session.execute(text(f'SET LOCAL ROLE "{roles.writer}"'))
                await capture_retention.require_capture_retention_read_access(session)
        assert rejected.value.orig.sqlstate == "42501"
        await _metadata_grant(capture_case, roles.writer, missing_relation, columns_by_relation[missing_relation])
        async with capture_case.sessions() as session, session.begin():
            await session.execute(text(f'SET LOCAL ROLE "{roles.writer}"'))
            await capture_retention.require_capture_retention_read_access(session)
        await _metadata_remains_read_only(capture_case, roles.writer)


async def _metadata_remains_read_only(case, role):
    schema = f'"{case.schema_name}"'
    await _denied_statement(case, role, text(f"SELECT payload FROM {schema}.custom_import_capture_raw_object"))
    await _denied_statement(case, role, text(f"UPDATE {schema}.custom_import_capture_usage SET retained_bytes=0"))
    await _denied_statement(case, role, text(f"DELETE FROM {schema}.custom_import_capture_raw_object"))
    for column in ("canonical_processing_policy", "canonical_progress", "canonical_summary"):
        await _denied_statement(case, role, text(f"SELECT {column} FROM {schema}.custom_import_prepared_source"))
    await _denied_statement(case, role, text(f"SELECT payload FROM {schema}.custom_import_prepared_source_part"))
    await _denied_statement(case, role, text(f"UPDATE {schema}.custom_import_prepared_source SET byte_count=0"))
    await _denied_statement(case, role, text(f"DELETE FROM {schema}.custom_import_prepared_source"))
    for relation in ("custom_import_prepared_source", "custom_import_prepared_source_part"):
        await _denied_statement(case, role, text(f"INSERT INTO {schema}.{relation} DEFAULT VALUES"))
    async with case.sessions() as session, session.begin():
        with pytest.raises(CaptureStoreError, match="closed capture write grants"):
            await require_raw_capture_authority(session)
    async with case.sessions() as session, session.begin():
        await session.execute(text(f'REVOKE INSERT ON {schema}.custom_import_capture_parquet_part FROM "{role}"'))
    await _denied_statement(
        case, role, insert(CustomImportCaptureParquetPart).values(capture_bundle_id=1, stream_slot=1, part_ordinal=1)
    )


async def test_open_canonical_grant_stops_raw_before_source_io(capture_case, monkeypatch):
    request, _payloads = _original_request("ndjson", "none")
    async with capture_case.sessions() as session, session.begin():
        seed = await register_definition(session, "synthetic_authority_" + uuid.uuid4().hex[:16], request.definition)
    request = replace(
        request,
        dataset_id=seed.dataset_id,
        definition_revision_id=seed.definition_revision_id,
        schema_revision_id=seed.schema_revision_id,
    )
    acquire = AsyncMock(side_effect=AssertionError("untrusted schema must not access the source"))
    monkeypatch.setattr(https_candidate.https, "acquire_https", acquire)
    async with _roles(capture_case) as roles:
        await _legacy_grants(capture_case, roles.writer)
        with pytest.raises(https_candidate.HttpsCandidateError):
            await https_candidate.run_https_candidate(capture_case.sessions, request)
    acquire.assert_not_awaited()


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
