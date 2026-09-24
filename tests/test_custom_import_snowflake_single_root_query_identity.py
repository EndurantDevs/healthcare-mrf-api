# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic contracts for a single-root query-identity Snowflake capture."""

from __future__ import annotations

import json
from dataclasses import replace
from io import BytesIO
from types import SimpleNamespace

import pytest

import process.custom_import.snowflake_bundle as snowflake_bundle
import process.custom_import.snowflake_bundle_replay as snowflake_bundle_replay
import process.custom_import.snowflake_operator_cli as operator_cli
import process.custom_import.snowflake_python as snowflake_python
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.snowflake import SnowflakeKeyPairCredentials
from process.custom_import.snowflake_bundle import (
    SnowflakeBundleAcquisitionConnector,
    SnowflakeBundleError,
    SnowflakeBundleRequest,
    SnowflakeBundleStreamMetadata,
    reconstruct_replayable_parquet_bundle,
    replayable_parquet_captures,
)
from process.custom_import.snowflake_source_binding import (
    SnowflakeSourceBinding,
    SnowflakeSourceBindingError,
)


def _definition(*, multi_stream: bool = False) -> CustomImportDefinition:
    definition_by_key = {
        "contract": "custom-import/v1",
        "revision": {"definition": 1, "schema": 1},
        "refresh_mode": "snapshot",
        "streams": [
            {
                "id": "root_source",
                "kind": "root",
                "format": "parquet",
                "compression": "none",
                "snapshot_token": "source_snapshot",
            }
        ],
        "schema": {
            "root": {
                "logical_key": ["npi"],
                "entity": {"adapter": "npi", "field": "npi"},
                "fields": [{"id": "npi", "slot": 1, "type": "string", "nullable": False}],
            },
            "children": [],
        },
        "aliases": {"root_source": {"PROVIDER_ID": "npi"}},
        "query": {"root_fields": [], "order": []},
        "selection_profiles": [],
    }
    if multi_stream:
        definition_by_key["streams"].append(
            {
                "id": "detail_source",
                "kind": "child",
                "child": "details",
                "format": "parquet",
                "compression": "none",
                "snapshot_token": "detail_snapshot",
            }
        )
        definition_by_key["schema"]["children"] = [
            {
                "name": "details",
                "parent_key": [{"child": "detail_npi", "root": "npi"}],
                "child_key": ["detail_id"],
                "fields": [
                    {"id": "detail_npi", "slot": 2, "type": "string", "nullable": False},
                    {"id": "detail_id", "slot": 3, "type": "string", "nullable": False},
                ],
            }
        ]
        definition_by_key["aliases"]["detail_source"] = {"DETAIL_NPI": "detail_npi", "DETAIL_ID": "detail_id"}
    return CustomImportDefinition.from_mapping(definition_by_key)


def _binding_document(definition: CustomImportDefinition, *, query_identity: bool) -> dict[str, object]:
    root_snapshot_relation = None if query_identity else ["synthetic", "public", "root_snapshots"]
    root_snapshot_column = None if query_identity else "snapshot_token"
    streams: list[dict[str, object]] = [
        {
            "stream_id": "root_source",
            "relation": ["synthetic", "public", "providers"],
            "source_snapshot_token_relation": root_snapshot_relation,
            "semantic_token_metadata_key": "source_snapshot",
            "source_snapshot_token_column_identifier": root_snapshot_column,
            "columns": [{"field_id": "npi", "column_identifier": "provider_id"}],
        }
    ]
    if len(definition.source_streams) == 2:
        streams.append(
            {
                "stream_id": "detail_source",
                "relation": ["synthetic", "public", "details"],
                "source_snapshot_token_relation": ["synthetic", "public", "detail_snapshots"],
                "semantic_token_metadata_key": "detail_snapshot",
                "source_snapshot_token_column_identifier": "snapshot_token",
                "columns": [
                    {"field_id": "detail_npi", "column_identifier": "detail_npi"},
                    {"field_id": "detail_id", "column_identifier": "detail_id"},
                ],
            }
        )
    return {
        "contract": "custom-import/source-binding/v1",
        "connector": "snowflake_bundle",
        "definition_sha256": definition.digest,
        "schema_sha256": definition.schema_digest,
        "source_object": {"fingerprint_sha256": "1" * 64, "version": "source-v1"},
        "role": "synthetic_reader",
        "warehouse": "synthetic_load",
        "streams": streams,
    }


def _credentials() -> SnowflakeKeyPairCredentials:
    return SnowflakeKeyPairCredentials(
        account="synthetic-account",
        user="synthetic-user",
        private_key_pem=b"-----BEGIN PRIVATE KEY-----\nsynthetic\n-----END PRIVATE KEY-----",
    )


def _connector(
    definition: CustomImportDefinition,
    binding: SnowflakeSourceBinding,
    adapter: object,
) -> tuple[SnowflakeBundleAcquisitionConnector, SnowflakeBundleRequest]:
    approved_relations, bindings = binding.bundle_components(definition)
    connector = SnowflakeBundleAcquisitionConnector(
        approved_relations=approved_relations,
        credential_provider=SimpleNamespace(load_key_pair=_credentials),
        adapter=adapter,
    )
    return connector, connector.prepare_request(definition, bindings=bindings)


def _statement(definition: CustomImportDefinition, binding: SnowflakeSourceBinding):
    connector, request = _connector(definition, binding, SimpleNamespace(fetch_bundle=lambda *_args: None))
    return connector.build_statement(request)


def test_single_root_null_pair_is_canonical_and_seals_query_identity_sql():
    definition = _definition()
    binding = SnowflakeSourceBinding.from_mapping(_binding_document(definition, query_identity=True))
    explicit = SnowflakeSourceBinding.from_mapping(_binding_document(definition, query_identity=False))

    statement = _statement(definition, binding)

    assert binding.digest != explicit.digest
    assert binding.streams[0].snapshot.relation is None
    assert binding.streams[0].snapshot.column_identifier is None
    assert json.loads(binding.canonical)["streams"][0]["source_snapshot_token_relation"] is None
    assert json.loads(statement.request.canonical_request)["bindings"][0]["source_snapshot_token_relation"] is None
    assert statement.source_snapshot_token_columns_by_stream == (None,)
    assert 'FROM "SYNTHETIC"."PUBLIC"."PROVIDERS"' in statement.sql
    assert 'CAST(NULL AS TEXT) AS "__ci_source_snapshot_token"' in statement.sql
    assert "ROOT_SNAPSHOTS" not in statement.sql
    assert 'SELECT "SNAPSHOT_TOKEN"' not in statement.sql
    rebuilt_request, rebuilt_statement = snowflake_bundle_replay._rebuilt_bundle_statement(statement)
    assert rebuilt_request == statement.request
    assert rebuilt_statement == statement

    explicit_statement = _statement(definition, explicit)
    assert 'FROM "SYNTHETIC"."PUBLIC"."ROOT_SNAPSHOTS"' in explicit_statement.sql

    expected_token = "snowflake-query:query-identity-1"
    bundle_result = SimpleNamespace(
        query_id="query-identity-1",
        stream_results=(
            SimpleNamespace(
                metadata=SnowflakeBundleStreamMetadata(
                    stream_id="root_source",
                    semantic_token_metadata_key="source_snapshot",
                    source_snapshot_tokens=(expected_token,),
                )
            ),
        ),
    )
    assert snowflake_bundle._bundle_snapshot_token(statement, bundle_result) == expected_token
    bundle_result.stream_results[0].metadata = SnowflakeBundleStreamMetadata(
        stream_id="root_source",
        semantic_token_metadata_key="source_snapshot",
        source_snapshot_tokens=("synthetic-other-token",),
    )
    with pytest.raises(SnowflakeBundleError, match="query-identity token"):
        snowflake_bundle._bundle_snapshot_token(statement, bundle_result)


def test_null_pair_requires_exactly_one_root_and_rejects_partial_pairs():
    definition = _definition()
    for relation, column in ((None, "snapshot_token"), (["synthetic", "public", "snapshots"], None)):
        document = _binding_document(definition, query_identity=True)
        document["streams"][0]["source_snapshot_token_relation"] = relation
        document["streams"][0]["source_snapshot_token_column_identifier"] = column
        with pytest.raises(SnowflakeSourceBindingError):
            SnowflakeSourceBinding.from_mapping(document)

    multi_stream_definition = _definition(multi_stream=True)
    multi_stream_binding = SnowflakeSourceBinding.from_mapping(
        _binding_document(multi_stream_definition, query_identity=True)
    )
    with pytest.raises(SnowflakeSourceBindingError, match="exactly one root"):
        multi_stream_binding.bundle_components(multi_stream_definition)

    explicit_binding = SnowflakeSourceBinding.from_mapping(
        _binding_document(multi_stream_definition, query_identity=False)
    )
    _approved_relations, bindings = explicit_binding.bundle_components(multi_stream_definition)
    with pytest.raises(SnowflakeBundleError, match="exactly one root"):
        SnowflakeBundleRequest(
            multi_stream_definition,
            (replace(bindings[0], source_snapshot_token_relation=None), bindings[1]),
        )


class _Cursor:
    def __init__(
        self,
        *,
        query_id: str = "query-identity-1",
        metadata_token: object = None,
        rows: list[tuple[object, ...]] | None = None,
    ) -> None:
        self.description = (
            SimpleNamespace(name="__ci_bundle_row_kind", type_name="FIXED", is_nullable=False, precision=1, scale=0),
            SimpleNamespace(name="__ci_stream_ordinal", type_name="FIXED", is_nullable=False, precision=2, scale=0),
            SimpleNamespace(name="__ci_stream_id", type_name="TEXT", is_nullable=False),
            SimpleNamespace(name="__ci_source_snapshot_token", type_name="TEXT", is_nullable=True),
            SimpleNamespace(name="npi", type_name="TEXT", is_nullable=False),
        )
        self.sfqid = query_id
        self.executed: list[str] = []
        self.rows = [(0, 1, "root_source", metadata_token, None)] if rows is None else rows
        self.closed = False

    def execute(self, sql: str) -> None:
        self.executed.append(sql)

    def fetchone(self):
        return self.rows.pop(0) if self.rows else None

    def close(self) -> None:
        self.closed = True


class _Connection:
    def __init__(self, cursor: _Cursor) -> None:
        self.cursor_value = cursor
        self.closed = False

    def cursor(self) -> _Cursor:
        return self.cursor_value

    def close(self) -> None:
        self.closed = True


def test_python_connector_uses_the_generated_query_identity_without_a_snapshot_read(monkeypatch):
    definition = _definition()
    statement = _statement(
        definition,
        SnowflakeSourceBinding.from_mapping(_binding_document(definition, query_identity=True)),
    )
    cursor = _Cursor()
    connection = _Connection(cursor)
    monkeypatch.setattr(snowflake_python, "_private_key_der", lambda _credentials: b"private-key-der")
    monkeypatch.setattr(snowflake_python.snowflake.connector, "connect", lambda **_kwargs: connection)

    result = snowflake_python.SnowflakePythonConnectorAdapter(
        role="synthetic_reader",
        warehouse="synthetic_load",
    ).fetch_bundle(
        statement,
        _credentials(),
    )

    assert result.query_id == "query-identity-1"
    assert result.stream_results[0].metadata.source_snapshot_tokens == ("snowflake-query:query-identity-1",)
    assert result.stream_results[0].parquet_result.source_snapshot_token == "snowflake-query:query-identity-1"
    assert cursor.executed == ["USE SECONDARY ROLES NONE", statement.sql]
    result.close()
    assert cursor.closed and connection.closed


@pytest.mark.parametrize(
    ("query_id", "metadata_token", "message"),
    ((" ", None, "statement identity"), ("query-identity-1", "unexpected", "metadata row")),
)
def test_python_connector_rejects_query_identity_errors_without_leaking_resources(
    monkeypatch,
    query_id,
    metadata_token,
    message,
):
    definition = _definition()
    statement = _statement(
        definition,
        SnowflakeSourceBinding.from_mapping(_binding_document(definition, query_identity=True)),
    )
    cursor = _Cursor(query_id=query_id, metadata_token=metadata_token)
    connection = _Connection(cursor)
    monkeypatch.setattr(snowflake_python, "_private_key_der", lambda _credentials: b"private-key-der")
    monkeypatch.setattr(snowflake_python.snowflake.connector, "connect", lambda **_kwargs: connection)

    with pytest.raises(snowflake_python.SnowflakeConnectorError, match=message):
        snowflake_python.SnowflakePythonConnectorAdapter(
            role="synthetic_reader",
            warehouse="synthetic_load",
        ).fetch_bundle(statement, _credentials())

    assert cursor.closed and connection.closed


def test_query_identity_capture_seals_and_replays_a_synthetic_root(monkeypatch):
    definition = _definition()
    binding = SnowflakeSourceBinding.from_mapping(_binding_document(definition, query_identity=True))
    cursor = _Cursor(
        rows=[
            (0, 1, "root_source", None, None),
            (1, 1, "root_source", None, "synthetic-npi"),
        ]
    )
    connection = _Connection(cursor)
    monkeypatch.setattr(snowflake_python, "_private_key_der", lambda _credentials: b"private-key-der")
    monkeypatch.setattr(snowflake_python.snowflake.connector, "connect", lambda **_kwargs: connection)
    connector, request = _connector(
        definition,
        binding,
        snowflake_python.SnowflakePythonConnectorAdapter(role="synthetic_reader", warehouse="synthetic_load"),
    )

    acquisition = connector.acquire(request)
    replay = reconstruct_replayable_parquet_bundle(
        acquisition.statement,
        replayable_parquet_captures(acquisition),
    )

    assert acquisition.source_snapshot_token == "snowflake-query:query-identity-1"
    assert replay.source_snapshot_token == acquisition.source_snapshot_token
    assert replay.roots == ({"npi": "synthetic-npi"},)
    assert cursor.executed == ["USE SECONDARY ROLES NONE", acquisition.statement.sql]
    assert cursor.closed and connection.closed

    with pytest.raises(SnowflakeBundleError, match="requires a statement identity"):
        replace(acquisition, diagnostic_query_id=None)
    with pytest.raises(SnowflakeBundleError, match="query-identity token does not match"):
        replace(acquisition, diagnostic_query_id="unrelated-query")


def test_operator_registration_accepts_the_canonical_single_root_null_pair():
    definition = _definition()
    binding = SnowflakeSourceBinding.from_mapping(_binding_document(definition, query_identity=True))
    stream = BytesIO(
        json.dumps(
            {
                "dataset_key": "synthetic_single_root",
                "definition": json.loads(definition.canonical),
                "source_binding": json.loads(binding.canonical),
            }
        ).encode("utf-8")
    )

    _dataset_key, registered_definition, registered_binding = operator_cli._registration_from_stdin(stream)

    assert registered_definition == definition
    assert registered_binding == binding
