# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native source reads exercise the generated one-statement bundle protocol."""

from __future__ import annotations

import json
from dataclasses import replace
from types import SimpleNamespace

import pytest
from sqlalchemy import text

from process.custom_import import snowflake_bundle, snowflake_python
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.snowflake import DEFAULT_CAPTURE_LIMITS, SnowflakeConnectorError, SnowflakeRelation
from process.custom_import.snowflake_bundle_replay import prepare_bundle_replay
from tests import test_custom_import_runner_postgres as runner_fixture
from tests import test_custom_import_snowflake_single_root_query_identity as identity_fixture
from tests.custom_import_postgres_support import isolated_publication_case


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
