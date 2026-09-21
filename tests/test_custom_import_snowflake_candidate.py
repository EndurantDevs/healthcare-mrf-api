# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Focused replay contracts for the root-only Snowflake candidate bridge."""

from __future__ import annotations

import json
from dataclasses import replace
from io import BytesIO

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from process.custom_import import snowflake, snowflake_candidate
from process.custom_import.capture import capture_stream
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.snowflake_candidate import (
    SnowflakeCandidateError,
    SnowflakeCandidateRequest,
    _prepare_candidate,
    run_snowflake_candidate,
)


def _definition(*, with_child: bool = False, with_nullable_root_field: bool = False) -> CustomImportDefinition:
    child_collections = []
    root_fields = [
        {"id": "npi", "slot": 1, "type": "string", "nullable": False},
        {"id": "display_name", "slot": 2, "type": "string", "nullable": False},
    ]
    if with_nullable_root_field:
        root_fields.append({"id": "note", "slot": 3, "type": "string", "nullable": True})
    streams = [
        {
            "id": "snowflake_result",
            "kind": "root",
            "format": "parquet",
            "compression": "none",
            "snapshot_token": "source_snapshot",
        }
    ]
    if with_child:
        child_collections.append(
            {
                "name": "details",
                "parent_key": [{"child": "detail_npi", "root": "npi"}],
                "child_key": ["detail_id"],
                "fields": [
                    {"id": "detail_npi", "slot": 3, "type": "string", "nullable": False},
                    {"id": "detail_id", "slot": 4, "type": "string", "nullable": False},
                ],
            }
        )
        streams.append(
            {
                "id": "details",
                "kind": "child",
                "child": "details",
                "format": "parquet",
                "compression": "none",
                "snapshot_token": "source_snapshot",
            }
        )
    return CustomImportDefinition.from_mapping(
        {
            "contract": "custom-import/v1",
            "revision": {"definition": 1, "schema": 1},
            "refresh_mode": "snapshot",
            "streams": streams,
            "schema": {
                "root": {
                    "logical_key": ["npi"],
                    "entity": {"adapter": "npi", "field": "npi"},
                    "fields": root_fields,
                },
                "children": child_collections,
            },
            "aliases": {stream["id"]: {} for stream in streams},
            "query": {"root_fields": [], "order": []},
            "selection_profiles": [],
        }
    )


def _capture(definition: CustomImportDefinition, npi: object, display_name: str):
    destination = BytesIO()
    pq.write_table(pa.table({"npi": [npi], "display_name": [display_name]}), destination)
    return capture_stream(
        BytesIO(destination.getvalue()),
        snowflake.SNOWFLAKE_RESULT_STREAM,
        source_snapshot_token="synthetic-snapshot",
    )


def _acquisition(
    definition: CustomImportDefinition,
    *,
    first_npi: object = "1234567893",
) -> snowflake.SnowflakeAcquisition:
    request = snowflake.SnowflakeReadRequest(
        relation=snowflake.SnowflakeRelation(database="synthetic", schema="public", name="root_records"),
        selected_columns=(
            snowflake.SnowflakeDeclaredColumn(field_id="npi", column_identifier="npi"),
            snowflake.SnowflakeDeclaredColumn(field_id="display_name", column_identifier="display_name"),
        ),
        definition_sha256=definition.digest,
        schema_sha256=definition.schema_digest,
    )
    statement = snowflake.SnowflakeReadStatement(request=request)
    captures = (
        _capture(definition, first_npi, "Synthetic One"),
        _capture(definition, "1234567893", "Synthetic Two"),
    )
    receipts = tuple(
        snowflake._capture_receipt(
            capture,
            ordinal=ordinal,
            source_snapshot_token="synthetic-snapshot",
        )
        for ordinal, capture in enumerate(captures, start=1)
    )
    content_hasher = snowflake._ResultContentHasher()
    for receipt in receipts:
        content_hasher.add(receipt)
    _, schema_fingerprint = snowflake._identity_sha256(
        "result-schema",
        {
            "columns": [
                {"field_id": "npi", "nullable": False, "source_type": "TEXT"},
                {"field_id": "display_name", "nullable": False, "source_type": "TEXT"},
            ],
            "contract": snowflake.CONNECTOR_CONTRACT,
            "format": snowflake.PARQUET_RESULT_FORMAT,
        },
    )
    return snowflake.SnowflakeAcquisition(
        statement=statement,
        manifest=snowflake.SnowflakeAcquisitionManifest(
            request_sha256=request.request_sha256,
            statement_sha256=statement.statement_sha256,
            source_snapshot_token="synthetic-snapshot",
            schema_fingerprint=schema_fingerprint,
            result_partitions=receipts,
            content_sha256=content_hasher.hexdigest(),
        ),
        parquet_captures=captures,
    )


def _request(
    definition: CustomImportDefinition, acquisition: snowflake.SnowflakeAcquisition
) -> SnowflakeCandidateRequest:
    return SnowflakeCandidateRequest(
        dataset_id=1,
        definition_revision_id=1,
        schema_revision_id=1,
        definition=definition,
        acquisition=acquisition,
        idempotency_key="synthetic-snowflake-candidate",
        lease_token="synthetic-snowflake-lease",
    )


def test_bridge_replays_each_partition_before_preparing_a_root_candidate():
    definition = _definition()
    acquisition = _acquisition(definition)

    prepared = _prepare_candidate(_request(definition, acquisition))

    assert [root["display_name"] for root in prepared.roots] == ["Synthetic One", "Synthetic Two"]
    assert prepared.receipt.content_sha256 == acquisition.manifest.content_sha256
    with pytest.raises(SnowflakeCandidateError, match="root-only"):
        _prepare_candidate(_request(_definition(with_child=True), acquisition))
    with pytest.raises(SnowflakeCandidateError, match="aggregate record"):
        _prepare_candidate(
            _request(
                definition, replace(acquisition, capture_limits=replace(acquisition.capture_limits, maximum_records=1))
            )
        )
    with pytest.raises(SnowflakeCandidateError, match="NPI strings"):
        _prepare_candidate(_request(definition, _acquisition(definition, first_npi=1234567893)))
    object.__setattr__(acquisition, "parquet_captures", tuple(reversed(acquisition.parquet_captures)))
    with pytest.raises(SnowflakeCandidateError, match="seal"):
        _prepare_candidate(_request(definition, acquisition))


@pytest.mark.parametrize("target", ("request", "manifest"))
def test_bridge_rejects_stale_nested_identity_seals(target):
    definition = _definition()
    acquisition = _acquisition(definition)
    if target == "request":
        object.__setattr__(acquisition.statement.request, "definition_sha256", "f" * 64)
    else:
        object.__setattr__(acquisition.manifest, "canonical_manifest", "{}")

    with pytest.raises(SnowflakeCandidateError, match="seal"):
        _prepare_candidate(_request(definition, acquisition))


@pytest.mark.asyncio
async def test_bridge_rejects_omitted_nullable_root_field_before_storage():
    definition = _definition(with_nullable_root_field=True)

    def session_factory():
        raise AssertionError("storage must not open for an incomplete root projection")

    with pytest.raises(SnowflakeCandidateError, match="exactly cover"):
        await run_snowflake_candidate(
            session_factory,
            _request(definition, _acquisition(definition)),
        )


def test_bridge_rejects_malformed_request_and_acquisition_boundaries():
    definition = _definition()
    acquisition = _acquisition(definition)
    request = _request(definition, acquisition)

    for malformed in (
        object(),
        replace(request, definition=object()),
        replace(request, lease_token=""),
        replace(request, dataset_id=0),
    ):
        with pytest.raises(SnowflakeCandidateError):
            snowflake_candidate._validated_request(malformed)
    with pytest.raises(SnowflakeCandidateError, match="acquisition is invalid"):
        snowflake_candidate._verified_acquisition(object())


def test_bridge_rejects_definition_binding_and_replay_shape_edges():
    definition = _definition()
    acquisition = _acquisition(definition)
    revised_document = json.loads(definition.canonical)
    revised_document["refresh_mode"] = "upsert"
    revised = CustomImportDefinition.from_mapping(revised_document)
    with pytest.raises(SnowflakeCandidateError, match="definition identity"):
        _prepare_candidate(_request(revised, acquisition))

    destination = BytesIO()
    pq.write_table(pa.table({"npi": ["1234567893"]}), destination)
    incomplete_capture = capture_stream(
        BytesIO(destination.getvalue()),
        snowflake.SNOWFLAKE_RESULT_STREAM,
        source_snapshot_token="synthetic-snapshot",
    )
    incomplete_acquisition = replace(acquisition)
    object.__setattr__(incomplete_acquisition, "parquet_captures", (incomplete_capture,))
    with pytest.raises(SnowflakeCandidateError):
        snowflake_candidate._decode_roots(
            incomplete_acquisition,
            entity_field="npi",
            selected_field_ids=("npi", "display_name"),
        )
    invalid_acquisition = replace(acquisition)
    object.__setattr__(invalid_acquisition, "parquet_captures", (object(),))
    with pytest.raises(SnowflakeCandidateError, match="cannot be replayed"):
        snowflake_candidate._decode_roots(
            invalid_acquisition,
            entity_field="npi",
            selected_field_ids=("npi", "display_name"),
        )


@pytest.mark.asyncio
async def test_bridge_rejects_session_factory_and_registry_mismatch(monkeypatch):
    definition = _definition()
    request = _request(definition, _acquisition(definition))
    with pytest.raises(SnowflakeCandidateError, match="session factory"):
        await run_snowflake_candidate(None, request)

    class Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        def begin(self):
            return self

    async def reject(*_args):
        raise snowflake_candidate.CandidateRunnerError("synthetic mismatch")

    monkeypatch.setattr(snowflake_candidate, "validate_revision_identity", reject)
    with pytest.raises(SnowflakeCandidateError, match="does not match"):
        await run_snowflake_candidate(Session, request)
