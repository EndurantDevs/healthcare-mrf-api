# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native statement-snapshot retention and source-free fenced replay."""

from __future__ import annotations

import datetime as dt
import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from sqlalchemy import func, text, update

from db.models.custom_import import CustomImportCaptureBundle, CustomImportLease
from process.custom_import import snowflake_bundle, snowflake_python
from process.custom_import.capture import iter_records
from process.custom_import.capture_store import open_segmented_parquet_parts
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.processing_policy import BuildPolicy, ProcessingPolicy
from process.custom_import.snowflake import DEFAULT_CAPTURE_LIMITS, SnowflakeConnectorError, SnowflakeRelation
from process.custom_import.snowflake_bundle_replay import prepare_bundle_replay
from process.custom_import.snowflake_candidate import SnowflakeBundleCandidateRequest
from process.custom_import.snowflake_capture import acquire_segmented_snowflake_capture
from process.custom_import.snowflake_source_binding import register_snowflake_source_binding
from tests import test_custom_import_runner_postgres as runner_fixture
from tests import test_custom_import_snowflake_single_root_query_identity as identity_fixture
from tests.custom_import_postgres_support import isolated_publication_case
from tests.test_custom_import_segmented_runner_postgres import _binding
from tests.test_custom_import_snowflake_capture import _policy
from tests.test_custom_import_snowflake_shared_capture import _runtime, _shared_row
from tests.test_custom_import_snowflake_source_binding import _processing_policy_migration


async def _registered_statement(case, connector, bundle, policy):
    async with case.engine.begin() as connection:
        await connection.run_sync(lambda conn: _processing_policy_migration(conn, case.schema_name).upgrade())
    binding = _binding(connector.build_statement(bundle), policy)
    async with case.sessions() as session, session.begin():
        registered = await register_snowflake_source_binding(
            session, dataset_key="synthetic_statement_snapshot", definition=bundle.definition, binding=binding
        )
    return SnowflakeBundleCandidateRequest(
        registered.dataset_id,
        registered.definition_revision_id,
        registered.schema_revision_id,
        bundle.definition,
        bundle,
        "synthetic-statement-snapshot",
        b"synthetic-source-owner",
        source_binding_revision_id=registered.source_binding_revision_id,
        source_binding_sha256=registered.source_binding_sha256,
    )


async def _capture_statement(case, connector, request, policy):
    return await acquire_segmented_snowflake_capture(
        case.sessions,
        request,
        statement_builder=connector,
        adapter=connector._adapter,
        credential_provider=connector._credential_provider,
        policy=policy.capture,
        driver_timeout_seconds=policy.driver_timeout_seconds,
        processing_policy=policy,
    )


async def _replay_and_expire(case, request, captured, policy, query_id):
    streams_by_id = {stream.stream_id: stream for stream in request.definition.source_streams}
    counts_by_stream = dict.fromkeys(streams_by_id, 0)
    async with case.sessions() as session, session.begin():
        bundle = await session.get(CustomImportCaptureBundle, captured.capture_bundle_id)
        assert bundle.snapshot_token == f"snowflake-query:{query_id}"
        assert bundle.source_binding_sha256 == request.source_binding_sha256
        async with open_segmented_parquet_parts(
            session,
            capture_bundle_id=captured.capture_bundle_id,
            dataset_id=request.dataset_id,
            definition_revision_id=request.definition_revision_id,
            schema_revision_id=request.schema_revision_id,
        ) as parts:
            seen_stream_ids = set()
            async for part in parts:
                stream_id = part.receipt.stream_id
                seen_stream_ids.add(stream_id)
                source_by_field = json.loads(part.receipt.canonical_manifest)["source"]
                assert source_by_field["query_id"] == query_id
                assert source_by_field["request_sha256"] == request.bundle_request.request_sha256
                counts_by_stream[stream_id] += sum(
                    1 for _ in iter_records(part.capture, streams_by_id[stream_id], limits=policy.capture.part_limits)
                )
            assert seen_stream_ids == set(streams_by_id)
        await session.execute(
            update(CustomImportLease)
            .where(CustomImportLease.execution_id == captured.execution_id)
            .values(expires_at=func.clock_timestamp() - dt.timedelta(seconds=1))
        )
    return counts_by_stream


@pytest.mark.parametrize("has_rows", [False, True])
@pytest.mark.parametrize("interleaved", [False, True])
async def test_statement_capture_and_bound_replay(monkeypatch, has_rows, interleaved):
    policy = ProcessingPolicy(_policy(), 17, BuildPolicy(2, 4096, 1000, 60, 300))
    rows = (_shared_row(),) if has_rows else ()
    if interleaved:
        rows = tuple((*row, None, None, None) for row in rows)
    connector, bundle, adapter, cursor, connection = _runtime(
        monkeypatch, rows, interleaved=interleaved, processing_policy=policy, snapshot_token_mode="statement_query_id"
    )
    monkeypatch.setattr(adapter, "_connect", lambda _credentials, **_options: (connection, cursor))
    async with isolated_publication_case() as case:
        request = await _registered_statement(case, connector, bundle, policy)
        captured = await _capture_statement(case, connector, request, policy)
        assert captured.status == "capture_sealed"
        counts = await _replay_and_expire(case, request, captured, policy, cursor.sfqid)
        assert counts["root_source"] == counts["detail_source"] == int(has_rows)
        if interleaved:
            assert counts["other_source"] == 0
        forbidden = Mock(side_effect=AssertionError("retained replay must not access a source"))
        monkeypatch.setattr(connector._credential_provider, "load_key_pair", forbidden)
        monkeypatch.setattr(adapter, "_connect", forbidden)
        resumed = await _capture_statement(
            case, connector, replace(request, lease_token=b"synthetic-next-owner"), policy
        )
        assert resumed.status == "capture_bound" and resumed.fence == 2
        assert resumed.capture_bundle_id == captured.capture_bundle_id
        forbidden.assert_not_called()
    assert cursor.executed == [connector.build_statement(bundle).sql]
    assert cursor.closed and connection.closed


class _NativeCursor:
    """Bridge declared relation spelling and metadata; keep the generated read shape."""

    sfqid = "synthetic-statement-1"

    def __init__(self, cursor, statement, database):
        self._cursor = cursor
        self._statement = statement
        self._native_relations = {
            relation.quoted_sql: f'"{database}"."{relation.schema.lower()}"."{relation.name.lower()}"'
            for binding in statement.request.bindings
            for relation in (binding.relation, binding.source_snapshot_token_relation)
        }
        self.executed = []
        self.closed = False

    def execute(self, sql):
        assert sql == self._statement.sql
        self.executed.append(sql)
        for declared_relation, native_relation in self._native_relations.items():
            sql = sql.replace(declared_relation, native_relation)
        self._cursor.execute(sql)
        assert tuple(column[1] for column in self._cursor.description) == (23, 23, 25, 25, 25, 25, 25, 25, 25)
        self.description = tuple(
            SimpleNamespace(
                name=column[0],
                type_name="FIXED" if index in (0, 1) else "TEXT",
                precision=30,
                scale=0,
                is_nullable=index >= 3,
            )
            for index, column in enumerate(self._cursor.description)
        )

    def fetchone(self):
        native_row = self._cursor.fetchone()
        return None if native_row is None else tuple(native_row)

    def close(self):
        self._cursor.close()
        self.closed = True


async def _source_statement(case, *, child_token="snapshot-1"):
    schema = case.schema_name
    async with case.engine.begin() as connection:
        for name, columns in (
            ("providers", '"NPI" TEXT, "DISPLAY_NAME" TEXT'),
            ("rates", '"RATE_NPI" TEXT, "SERVICE_CODE" TEXT, "AMOUNT" TEXT'),
            ("root_token", '"SNAPSHOT_TOKEN" TEXT'),
            ("child_token", '"SNAPSHOT_TOKEN" TEXT'),
        ):
            await connection.execute(text(f'CREATE TABLE "{schema}"."{name}" ({columns})'))
        await connection.execute(text(f"INSERT INTO \"{schema}\".\"providers\" VALUES ('1234567893', 'Before')"))
        await connection.execute(text(f"INSERT INTO \"{schema}\".\"rates\" VALUES ('1234567893', 'A', 4)"))
        await connection.execute(text(f'INSERT INTO "{schema}"."root_token" VALUES (\'snapshot-1\')'))
        await connection.execute(text(f'INSERT INTO "{schema}"."child_token" VALUES (:token)'), {"token": child_token})
    database = case.engine.url.database
    definition_document = json.loads(runner_fixture._snowflake_bundle_definition().canonical)
    definition_document["schema"]["children"][0]["fields"][2]["type"] = "string"
    original = runner_fixture._snowflake_bundle_statement(CustomImportDefinition.from_mapping(definition_document))
    bindings = tuple(
        replace(
            binding,
            relation=SnowflakeRelation(database=database, schema=schema, name=source),
            source_snapshot_token_relation=SnowflakeRelation(database=database, schema=schema, name=token_table),
        )
        for binding, source, token_table in zip(
            original.request.bindings, ("providers", "rates"), ("root_token", "child_token"), strict=True
        )
    )
    return replace(original, request=replace(original.request, bindings=bindings))


def _fetch_native_bundle(sync_connection, statement, monkeypatch):
    cursor = _NativeCursor(sync_connection.connection.cursor(), statement, sync_connection.engine.url.database)
    owner = SimpleNamespace(closed=False)
    owner.close = lambda: setattr(owner, "closed", True)
    adapter = snowflake_python.SnowflakePythonConnectorAdapter(role="synthetic_reader", warehouse="synthetic_load")
    monkeypatch.setattr(adapter, "_connect", lambda _credentials: (owner, cursor))
    try:
        bundle_result = adapter.fetch_bundle(statement, identity_fixture._credentials())
    except SnowflakeConnectorError:
        assert cursor.closed and owner.closed
        assert cursor.executed == [statement.sql]
        raise
    return bundle_result, cursor, owner


async def _commit_source_change(case):
    schema = case.schema_name
    async with case.engine.begin() as writer:
        await writer.execute(text(f'UPDATE "{schema}"."providers" SET "DISPLAY_NAME" = \'After\''))
        await writer.execute(text(f'UPDATE "{schema}"."rates" SET "AMOUNT" = 99'))
        for token_table in ("root_token", "child_token"):
            await writer.execute(text(f'UPDATE "{schema}"."{token_table}" SET "SNAPSHOT_TOKEN" = \'snapshot-2\''))


@pytest.mark.asyncio
async def test_bundle_keeps_one_native_statement_snapshot_after_source_commit(monkeypatch):
    async with isolated_publication_case() as case:
        statement = await _source_statement(case)
        async with case.engine.connect() as connection:
            bundle_result, cursor, owner = await connection.run_sync(
                lambda sync: _fetch_native_bundle(sync, statement, monkeypatch)
            )
            try:
                await _commit_source_change(case)
                acquisition = snowflake_bundle._seal_bundle(statement, bundle_result, DEFAULT_CAPTURE_LIMITS)
            finally:
                bundle_result.close()
            assert cursor.executed == [statement.sql]
            assert cursor.closed and owner.closed
        replay = prepare_bundle_replay(acquisition)
        assert replay.source_snapshot_token == "snapshot-1"
        assert dict(replay.streams[0].records[0]) == {"npi": "1234567893", "display_name": "Before"}
        assert dict(replay.streams[1].records[0]) == {
            "rate_npi": "1234567893",
            "service_code": "A",
            "amount": "4",
        }
        async with case.engine.connect() as observer:
            assert await observer.scalar(text(f'SELECT "AMOUNT" FROM "{case.schema_name}"."rates"')) == "99"
            assert (
                await observer.scalar(text(f'SELECT "SNAPSHOT_TOKEN" FROM "{case.schema_name}"."root_token"'))
                == "snapshot-2"
            )


@pytest.mark.asyncio
@pytest.mark.parametrize("child_token", ("snapshot-2", None))
async def test_native_bundle_rejects_inconsistent_tokens_and_closes_cursor(monkeypatch, child_token):
    async with isolated_publication_case() as case:
        statement = await _source_statement(case, child_token=child_token)
        async with case.engine.connect() as connection:
            with pytest.raises(SnowflakeConnectorError, match="shared semantic snapshot token"):
                await connection.run_sync(lambda sync: _fetch_native_bundle(sync, statement, monkeypatch))
