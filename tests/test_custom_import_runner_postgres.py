# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL proof for the generic custom-import candidate runner."""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal
from io import BytesIO
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import func, select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

import process.custom_import.runner as runner
import process.custom_import.runner_graph as runner_graph
import process.custom_import.snowflake_candidate as snowflake_candidate
from db.models.custom_import import (
    CustomImportCapture,
    CustomImportCaptureBundle,
    CustomImportCaptureParquetPart,
    CustomImportChildCollection,
    CustomImportChildRevision,
    CustomImportChildScalar,
    CustomImportCurrentGeneration,
    CustomImportDataset,
    CustomImportDefinitionRevision,
    CustomImportEntityBinding,
    CustomImportExecution,
    CustomImportFamilyRevision,
    CustomImportField,
    CustomImportFieldAlias,
    CustomImportFieldSlot,
    CustomImportGeneration,
    CustomImportGenerationFamily,
    CustomImportGenerationSeal,
    CustomImportLease,
    CustomImportNoChangeSeal,
    CustomImportPack,
    CustomImportRejection,
    CustomImportRootRevision,
    CustomImportRootScalar,
    CustomImportSchemaRevision,
    CustomImportSelectionProfile,
    CustomImportSourceStream,
    CustomImportWinner,
)
from process.custom_import import snowflake
from process.custom_import.capture import capture_stream
from process.custom_import.definition import CustomImportDefinition, canonical_json, canonical_sha256
from process.custom_import.execution import (
    IdempotencyConflict,
    cancel_execution_request,
    claim_execution,
    create_execution,
    finish_execution,
    lookup_execution_request,
    request_cancellation,
    reserve_execution,
)
from process.custom_import.family import assemble_root_families
from process.custom_import.runner import CandidateRunnerError, CandidateRunRequest, CandidateRunResult, run_candidate
from process.custom_import.runner_codec import child_key_hash, new_family_hash
from process.custom_import.snowflake_bundle import (
    SnowflakeBundleAcquisition,
    SnowflakeBundleAcquisitionConnector,
    SnowflakeBundleBinding,
    SnowflakeBundleRequest,
    SnowflakeBundleStatement,
    SnowflakeBundleStreamCapture,
)
from process.custom_import.snowflake_candidate import (
    SnowflakeBundleCandidateRequest,
    SnowflakeCandidateRequest,
    run_snowflake_bundle_candidate,
    run_snowflake_candidate,
)
from process.custom_import.snowflake_python import _parquet_reader
from tests.custom_import_postgres_support import digest, isolated_publication_case

_FIXTURE = Path(__file__).with_name("fixtures") / "custom_import" / "v1_valid.json"


@dataclass(frozen=True)
class _Seed:
    dataset_id: int
    definition_revision_id: int
    schema_revision_id: int
    capture_bundle_id: int
    definition: CustomImportDefinition


def _definition() -> CustomImportDefinition:
    return CustomImportDefinition.from_json(_FIXTURE.read_text())


def _snowflake_definition() -> CustomImportDefinition:
    document = json.loads(_FIXTURE.read_text())
    document["refresh_mode"] = "snapshot"
    document["streams"] = [
        {
            "id": "snowflake_result",
            "kind": "root",
            "format": "parquet",
            "compression": "none",
            "snapshot_token": "source_snapshot",
        }
    ]
    document["schema"]["children"] = []
    document["aliases"] = {"snowflake_result": {}}
    document["query"] = {"root_fields": ["npi", "display_name"], "order": []}
    document["selection_profiles"] = []
    return CustomImportDefinition.from_mapping(document)


def _snowflake_bundle_definition() -> CustomImportDefinition:
    document = json.loads(_FIXTURE.read_text())
    document["refresh_mode"] = "snapshot"
    for stream in document["streams"]:
        stream.update(format="parquet", compression="none", snapshot_token="source_snapshot")
    document["aliases"] = {
        "providers": {"NPI": "npi", "DISPLAY_NAME": "display_name"},
        "rates": {"RATE_NPI": "rate_npi", "SERVICE_CODE": "service_code", "AMOUNT": "amount"},
    }
    return CustomImportDefinition.from_mapping(document)


def _snowflake_bundle_statement(definition: CustomImportDefinition) -> SnowflakeBundleStatement:
    snapshot_relation = snowflake.SnowflakeRelation(database="synthetic", schema="public", name="snapshots")
    root_relation = snowflake.SnowflakeRelation(database="synthetic", schema="public", name="providers")
    child_relation = snowflake.SnowflakeRelation(database="synthetic", schema="public", name="rates")
    snapshot_column = snowflake.SnowflakeDeclaredColumn(field_id="source_snapshot", column_identifier="snapshot_token")
    root_columns = (
        snowflake.SnowflakeDeclaredColumn(field_id="npi", column_identifier="NPI"),
        snowflake.SnowflakeDeclaredColumn(field_id="display_name", column_identifier="DISPLAY_NAME"),
    )
    child_columns = (
        snowflake.SnowflakeDeclaredColumn(field_id="rate_npi", column_identifier="RATE_NPI"),
        snowflake.SnowflakeDeclaredColumn(field_id="service_code", column_identifier="SERVICE_CODE"),
        snowflake.SnowflakeDeclaredColumn(field_id="amount", column_identifier="AMOUNT"),
    )
    request = SnowflakeBundleRequest(
        definition=definition,
        bindings=(
            SnowflakeBundleBinding(
                stream_id="providers",
                relation=root_relation,
                source_snapshot_token_relation=snapshot_relation,
                selected_field_ids=("npi", "display_name"),
                semantic_token_metadata_key="source_snapshot",
            ),
            SnowflakeBundleBinding(
                stream_id="rates",
                relation=child_relation,
                source_snapshot_token_relation=snapshot_relation,
                selected_field_ids=("rate_npi", "service_code", "amount"),
                semantic_token_metadata_key="source_snapshot",
            ),
        ),
    )
    return SnowflakeBundleStatement(
        request=request,
        selected_columns_by_stream=(root_columns, child_columns),
        source_snapshot_token_columns_by_stream=(snapshot_column, snapshot_column),
    )


def _snowflake_bundle_acquisition(
    definition: CustomImportDefinition,
    *,
    root_rows: tuple[tuple[object, object], ...] = (("1234567893", "Synthetic Bundle"),),
    child_rows: tuple[tuple[object, object, object], ...] = (("1234567893", "SYNTHETIC", Decimal("12.50")),),
    snapshot_token: str = "synthetic-snowflake-bundle",
    amount_source_type: str = "FIXED(30,12)",
) -> SnowflakeBundleAcquisition:
    statement = _snowflake_bundle_statement(definition)
    root_schema = (
        snowflake.SnowflakeResultColumn(field_id="npi", source_type="TEXT", nullable=False),
        snowflake.SnowflakeResultColumn(field_id="display_name", source_type="TEXT", nullable=False),
    )
    child_schema = (
        snowflake.SnowflakeResultColumn(field_id="rate_npi", source_type="TEXT", nullable=False),
        snowflake.SnowflakeResultColumn(field_id="service_code", source_type="TEXT", nullable=False),
        snowflake.SnowflakeResultColumn(field_id="amount", source_type=amount_source_type, nullable=True),
    )
    root_stream, child_stream = definition.source_streams
    root_capture = capture_stream(
        BytesIO(_parquet_reader(root_rows, root_schema).getvalue()),
        root_stream,
        source_snapshot_token=snapshot_token,
        limits=snowflake.DEFAULT_CAPTURE_LIMITS,
    )
    child_capture = capture_stream(
        BytesIO(_parquet_reader(child_rows, child_schema).getvalue()),
        child_stream,
        source_snapshot_token=snapshot_token,
        limits=snowflake.DEFAULT_CAPTURE_LIMITS,
    )
    return SnowflakeBundleAcquisition(
        statement=statement,
        source_snapshot_token=snapshot_token,
        stream_captures=(
            SnowflakeBundleStreamCapture(
                stream_id=root_stream.stream_id,
                schema=root_schema,
                captures=(root_capture,),
            ),
            SnowflakeBundleStreamCapture(
                stream_id=child_stream.stream_id,
                schema=child_schema,
                captures=(child_capture,),
            ),
        ),
        capture_limits=snowflake.DEFAULT_CAPTURE_LIMITS,
    )


class _ClosedBundleAcquirer:
    def __init__(self, acquisition: SnowflakeBundleAcquisition) -> None:
        self._acquisition = acquisition
        self.calls = 0
        self.source_closed = False

    def build_statement(self, request: SnowflakeBundleRequest) -> SnowflakeBundleStatement:
        assert request == self._acquisition.statement.request
        return self._acquisition.statement

    def acquire(
        self,
        request: SnowflakeBundleRequest,
        *,
        prepared_statement: SnowflakeBundleStatement,
    ) -> SnowflakeBundleAcquisition:
        assert request == self._acquisition.statement.request
        assert prepared_statement == self._acquisition.statement
        self.calls += 1
        self.source_closed = True
        return self._acquisition


class _PreparedStatementOnlyAcquirer:
    """A preflight-only connector proving conflicts stop before source work."""

    def __init__(self, statement: SnowflakeBundleStatement) -> None:
        self._statement = statement
        self.acquire_calls = 0

    def build_statement(self, request: SnowflakeBundleRequest) -> SnowflakeBundleStatement:
        assert request == self._statement.request
        return self._statement

    def acquire(
        self,
        _request: SnowflakeBundleRequest,
        *,
        prepared_statement: SnowflakeBundleStatement,
    ) -> SnowflakeBundleAcquisition:
        assert prepared_statement == self._statement
        self.acquire_calls += 1
        raise AssertionError("an idempotency conflict must stop before source acquisition")


def _physical_mapping_drift_statement(definition: CustomImportDefinition) -> SnowflakeBundleStatement:
    """Keep logical request inputs fixed while changing one approved physical column."""

    statement = _snowflake_bundle_statement(definition)
    request = statement.request
    snapshot_relation = request.bindings[0].source_snapshot_token_relation
    connector = SnowflakeBundleAcquisitionConnector(
        approved_relations=(
            snowflake.SnowflakeApprovedRelation(
                relation=snapshot_relation,
                columns=(
                    snowflake.SnowflakeDeclaredColumn(
                        field_id="source_snapshot",
                        column_identifier="SNAPSHOT_TOKEN_RENAMED",
                    ),
                ),
            ),
            snowflake.SnowflakeApprovedRelation(
                relation=request.bindings[0].relation,
                columns=statement.selected_columns_by_stream[0],
            ),
            snowflake.SnowflakeApprovedRelation(
                relation=request.bindings[1].relation,
                columns=statement.selected_columns_by_stream[1],
            ),
        ),
        credential_provider=SimpleNamespace(load_key_pair=lambda: None),
        adapter=SimpleNamespace(fetch_bundle=lambda *_arguments: None),
    )
    return connector.build_statement(request)


def _snowflake_bundle_candidate_request(
    seed: _Seed,
    *,
    suffix: str,
    root_rows: tuple[tuple[object, object], ...],
    child_rows: tuple[tuple[object, object, object], ...],
    amount_source_type: str = "FIXED(30,12)",
) -> tuple[_ClosedBundleAcquirer, SnowflakeBundleCandidateRequest]:
    acquisition = _snowflake_bundle_acquisition(
        seed.definition,
        root_rows=root_rows,
        child_rows=child_rows,
        snapshot_token=f"synthetic-snowflake-{suffix}",
        amount_source_type=amount_source_type,
    )
    return _ClosedBundleAcquirer(acquisition), SnowflakeBundleCandidateRequest(
        dataset_id=seed.dataset_id,
        definition_revision_id=seed.definition_revision_id,
        schema_revision_id=seed.schema_revision_id,
        definition=seed.definition,
        bundle_request=acquisition.statement.request,
        idempotency_key=f"synthetic-snowflake-family-{suffix}",
        lease_token=f"synthetic-snowflake-family-{suffix}-lease",
    )


class _SnowflakePartitionSources:
    def __init__(self, reader) -> None:
        self._reader = reader
        self._consumed = False

    def __iter__(self):
        if self._consumed:
            raise RuntimeError("synthetic source was consumed twice")
        self._consumed = True
        yield self._reader

    def close(self) -> None:
        self._reader.close()


def _snowflake_acquisition(
    definition: CustomImportDefinition,
    *,
    root_rows: tuple[tuple[object, object], ...] = (("1234567893", "Synthetic Bridge"),),
    snapshot_token: str = "synthetic-snowflake-snapshot",
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
    schema = (
        snowflake.SnowflakeResultColumn(field_id="npi", source_type="TEXT", nullable=False),
        snowflake.SnowflakeResultColumn(field_id="display_name", source_type="TEXT", nullable=False),
    )
    result = snowflake.SnowflakeParquetResult(
        source_snapshot_token=snapshot_token,
        schema=schema,
        partition_sources=_SnowflakePartitionSources(_parquet_reader(root_rows, schema)),
    )
    try:
        return snowflake._seal_acquisition(statement, result, capture_limits=snowflake.DEFAULT_CAPTURE_LIMITS)
    finally:
        result.close()


def _snapshot_definition() -> CustomImportDefinition:
    document = json.loads(_FIXTURE.read_text())
    document["refresh_mode"] = "snapshot"
    return CustomImportDefinition.from_json(json.dumps(document))


def _schema_mismatch_definition() -> CustomImportDefinition:
    document = json.loads(_FIXTURE.read_text())
    document["schema"]["root"]["fields"][1]["nullable"] = True
    return CustomImportDefinition.from_json(json.dumps(document))


def _noncanonical_definition() -> CustomImportDefinition:
    return replace(_definition(), refresh_mode="snapshot")


def _decimal_root_key_definition() -> CustomImportDefinition:
    document = json.loads(_FIXTURE.read_text())
    root = document["schema"]["root"]
    root["logical_key"] = ["npi", "rank"]
    root["fields"].append({"id": "rank", "slot": 6, "type": "decimal", "nullable": False})
    rates = document["schema"]["children"][0]
    rates["parent_key"].append({"child": "rate_rank", "root": "rank"})
    rates["fields"].append({"id": "rate_rank", "slot": 7, "type": "decimal", "nullable": False})
    return CustomImportDefinition.from_json(json.dumps(document))


def _decimal_child_key_definition() -> CustomImportDefinition:
    document = json.loads(_FIXTURE.read_text())
    rates = document["schema"]["children"][0]
    rates["child_key"] = ["amount"]
    rates["fields"][2]["nullable"] = False
    return CustomImportDefinition.from_json(json.dumps(document))


def _timestamp_child_key_definition() -> CustomImportDefinition:
    document = json.loads(_FIXTURE.read_text())
    rates = document["schema"]["children"][0]
    rates["child_key"] = ["captured_at"]
    rates["fields"].append({"id": "captured_at", "slot": 6, "type": "timestamp", "nullable": False})
    return CustomImportDefinition.from_json(json.dumps(document))


def _root(npi: str, name: str) -> dict[str, object]:
    return {"npi": npi, "display_name": name}


def _rate(npi: str, code: str, amount: object) -> dict[str, object]:
    return {"rate_npi": npi, "service_code": code, "amount": amount}


def _root_with_rank(npi: str, name: object, rank: object) -> dict[str, object]:
    return {**_root(npi, name), "rank": rank}


def _rate_with_rank(npi: str, code: str, amount: object, rank: object) -> dict[str, object]:
    return {**_rate(npi, code, amount), "rate_rank": rank}


def _key_shape(collection) -> dict[str, object]:
    return {
        "child_key": list(collection.child_key),
        "parent_key": [{"child": part.child_field, "root": part.root_field} for part in collection.parent_key],
    }


async def _seed_identity(
    session: AsyncSession,
    suffix: str,
    definition: CustomImportDefinition | None = None,
) -> _Seed:
    """Persist one generic definition registry and sealed capture bundle."""

    definition = _definition() if definition is None else definition
    dataset_id, schema_revision_id = await _seed_dataset_schema(session, definition, suffix)
    collection_slots_by_name = await _seed_schema_registry(
        session,
        definition,
        dataset_id,
        schema_revision_id,
    )
    definition_revision_id = await _seed_definition_revision(
        session,
        definition,
        dataset_id,
        schema_revision_id,
    )
    stream_slots_by_id = await _seed_source_registry(
        session,
        definition,
        dataset_id,
        schema_revision_id,
        definition_revision_id,
        collection_slots_by_name,
    )
    capture_bundle_id = await _seed_capture_bundle(
        session,
        definition,
        suffix,
        dataset_id,
        schema_revision_id,
        definition_revision_id,
        stream_slots_by_id,
    )
    return _Seed(
        dataset_id=dataset_id,
        definition_revision_id=definition_revision_id,
        schema_revision_id=schema_revision_id,
        capture_bundle_id=capture_bundle_id,
        definition=definition,
    )


async def _seed_dataset_schema(
    session: AsyncSession,
    definition: CustomImportDefinition,
    suffix: str,
) -> tuple[int, int]:
    """Persist the dataset and one exact immutable schema document."""

    dataset = CustomImportDataset(dataset_key=f"synthetic_runner_{suffix}")
    session.add(dataset)
    await session.flush()
    schema = CustomImportSchemaRevision(
        dataset_id=dataset.dataset_id,
        revision_number=definition.schema_revision,
        canonical_schema=definition.schema_canonical,
        schema_sha256=bytes.fromhex(definition.schema_digest),
    )
    session.add(schema)
    await session.flush()
    return dataset.dataset_id, schema.schema_revision_id


async def _seed_schema_registry(
    session: AsyncSession,
    definition: CustomImportDefinition,
    dataset_id: int,
    schema_revision_id: int,
) -> dict[str, int]:
    """Persist collection slots and typed field slots for one schema."""

    collection_slots_by_name = {
        collection.name: index for index, collection in enumerate(definition.child_collections, start=1)
    }
    session.add_all(
        CustomImportChildCollection(
            schema_revision_id=schema_revision_id,
            dataset_id=dataset_id,
            collection_slot=collection_slots_by_name[collection.name],
            collection_name=collection.name,
            canonical_key_shape=canonical_json(_key_shape(collection)),
            key_shape_sha256=bytes.fromhex(canonical_sha256(_key_shape(collection), domain="schema")),
        )
        for collection in definition.child_collections
    )
    session.add_all(
        CustomImportFieldSlot(
            dataset_id=dataset_id,
            field_slot=field.field_slot,
            field_id=field.field_id,
        )
        for field in definition.fields
    )
    await session.flush()
    session.add_all(
        CustomImportField(
            schema_revision_id=schema_revision_id,
            dataset_id=dataset_id,
            field_slot=field.field_slot,
            collection_slot=0 if field.collection is None else collection_slots_by_name[field.collection],
            field_name=field.field_id,
            field_type=field.value_type,
            is_nullable=field.nullable,
            projection_slot=field.projection_slot or 0,
        )
        for field in definition.fields
    )
    await session.flush()
    return collection_slots_by_name


async def _seed_definition_revision(
    session: AsyncSession,
    definition: CustomImportDefinition,
    dataset_id: int,
    schema_revision_id: int,
) -> int:
    """Persist one definition revision bound to the seeded schema."""

    revision = CustomImportDefinitionRevision(
        dataset_id=dataset_id,
        schema_revision_id=schema_revision_id,
        revision_number=definition.definition_revision,
        contract_version="custom-import/v1",
        refresh_mode=definition.refresh_mode,
        canonical_definition=definition.canonical,
        definition_sha256=bytes.fromhex(definition.digest),
    )
    session.add(revision)
    await session.flush()
    return revision.definition_revision_id


async def _seed_source_registry(
    session: AsyncSession,
    definition: CustomImportDefinition,
    dataset_id: int,
    schema_revision_id: int,
    definition_revision_id: int,
    collection_slots_by_name: dict[str, int],
) -> dict[str, int]:
    """Persist source streams and aliases for the exact definition revision."""

    stream_slots_by_id = {stream.stream_id: index for index, stream in enumerate(definition.source_streams, start=1)}
    session.add_all(
        CustomImportSourceStream(
            definition_revision_id=definition_revision_id,
            dataset_id=dataset_id,
            schema_revision_id=schema_revision_id,
            stream_slot=stream_slots_by_id[stream.stream_id],
            stream_id=stream.stream_id,
            record_kind=stream.record_kind,
            collection_slot=None
            if stream.child_collection is None
            else collection_slots_by_name[stream.child_collection],
            decoder=stream.format,
            compression=stream.compression,
            snapshot_token_selector=stream.snapshot_token,
            record_path=stream.record_path,
        )
        for stream in definition.source_streams
    )
    await session.flush()
    session.add_all(
        CustomImportFieldAlias(
            definition_revision_id=definition_revision_id,
            dataset_id=dataset_id,
            schema_revision_id=schema_revision_id,
            stream_slot=stream_slots_by_id[alias.stream_id],
            alias_name=alias.source_label,
            field_slot=definition.fields_by_id[alias.field_id].field_slot,
        )
        for alias in definition.aliases
    )
    await session.flush()
    return stream_slots_by_id


async def _seed_capture_bundle(
    session: AsyncSession,
    definition: CustomImportDefinition,
    suffix: str,
    dataset_id: int,
    schema_revision_id: int,
    definition_revision_id: int,
    stream_slots_by_id: dict[str, int],
) -> int:
    """Persist one sealed synthetic capture for every declared source stream."""

    capture_bundle = CustomImportCaptureBundle(
        dataset_id=dataset_id,
        definition_revision_id=definition_revision_id,
        schema_revision_id=schema_revision_id,
        snapshot_token=f"synthetic-runner-snapshot-{suffix}",
        snapshot_token_sha256=digest(f"snapshot:{suffix}"),
        canonical_manifest=canonical_json({"contract": "synthetic-capture/v1", "suffix": suffix}),
        manifest_sha256=digest(f"bundle-manifest:{suffix}"),
        stream_count=len(definition.source_streams),
    )
    session.add(capture_bundle)
    await session.flush()
    session.add_all(
        CustomImportCapture(
            capture_bundle_id=capture_bundle.capture_bundle_id,
            dataset_id=dataset_id,
            definition_revision_id=definition_revision_id,
            schema_revision_id=schema_revision_id,
            stream_slot=stream_slots_by_id[stream.stream_id],
            content_sha256=digest(f"capture:{suffix}:{stream.stream_id}"),
            byte_count=0,
            canonical_manifest=canonical_json({"contract": "synthetic-capture-stream/v1", "stream": stream.stream_id}),
            manifest_sha256=digest(f"capture-manifest:{suffix}:{stream.stream_id}"),
        )
        for stream in definition.source_streams
    )
    await session.flush()
    return capture_bundle.capture_bundle_id


async def _new_execution(case, seed: _Seed, suffix: str) -> tuple[int, str]:
    token = f"synthetic-runner-token-{suffix}"
    async with case.sessions() as session, session.begin():
        submission = await create_execution(
            session,
            dataset_id=seed.dataset_id,
            definition_revision_id=seed.definition_revision_id,
            schema_revision_id=seed.schema_revision_id,
            idempotency_key=f"synthetic-runner-execution-{suffix}",
            mechanism="local",
            capture_bundle_id=seed.capture_bundle_id,
        )
    return submission.execution_id, token


def _request(
    seed: _Seed,
    execution_id: int,
    token: str,
    roots: list[dict[str, object]],
    children: list[dict[str, object]],
    *,
    complete_scope: bool = False,
) -> CandidateRunRequest:
    return CandidateRunRequest(
        dataset_id=seed.dataset_id,
        definition_revision_id=seed.definition_revision_id,
        schema_revision_id=seed.schema_revision_id,
        execution_id=execution_id,
        lease_token=token,
        definition=seed.definition,
        roots=roots,
        children_by_collection={"rates": children},
        complete_scope=complete_scope,
    )


async def _seed_case(case, suffix: str, definition: CustomImportDefinition | None = None) -> _Seed:
    async with case.sessions() as session, session.begin():
        return await _seed_identity(session, suffix, definition)


async def _seed_mapping_drift_execution(
    case,
    seed: _Seed,
    request: SnowflakeBundleCandidateRequest,
    statement: SnowflakeBundleStatement,
    stored_identity_state: str,
) -> tuple[int, bytes]:
    """Persist one legacy, live, expired, or terminal bundle reservation."""

    baseline_identity = snowflake_candidate.bundle_request_identity_sha256(request.bundle_request, statement)
    async with case.sessions() as session, session.begin():
        submission = await reserve_execution(
            session,
            dataset_id=seed.dataset_id,
            definition_revision_id=seed.definition_revision_id,
            schema_revision_id=seed.schema_revision_id,
            idempotency_key=request.idempotency_key,
            mechanism="local",
            request_identity_sha256=None if stored_identity_state == "null" else baseline_identity,
        )
        grant = None
        if stored_identity_state != "null":
            grant = await claim_execution(
                session,
                execution_id=submission.execution_id,
                token=request.lease_token,
                lease_seconds=60,
            )
    if stored_identity_state == "expired":
        assert grant is not None
        async with case.sessions() as session, session.begin():
            await session.execute(
                update(CustomImportLease)
                .where(CustomImportLease.execution_id == submission.execution_id)
                .values(expires_at=func.clock_timestamp())
            )
    elif stored_identity_state == "terminal":
        assert grant is not None
        async with case.sessions() as session, session.begin():
            await request_cancellation(session, execution_id=submission.execution_id)
            terminal = await finish_execution(
                session,
                execution_id=submission.execution_id,
                fence=grant.fence,
                token=request.lease_token,
                terminal_state="canceled",
            )
        assert terminal.state == "canceled"
    return submission.execution_id, baseline_identity


@pytest.mark.asyncio
async def test_runner_materializes_seals_and_activates_typed_candidate_rows():
    async with isolated_publication_case() as case:
        seed = await _seed_case(case, "success")
        execution_id, token = await _new_execution(case, seed, "success")

        run_result = await run_candidate(
            case.sessions,
            _request(
                seed,
                execution_id,
                token,
                [_root("1234567893", "Synthetic Clinic")],
                [_rate("1234567893", "SYNTHETIC", Decimal("12.50"))],
            ),
        )

        assert run_result.status == "activated"
        assert run_result.generation_id is not None
        async with case.sessions() as session:
            pointer = await session.get(CustomImportCurrentGeneration, seed.dataset_id)
            seal = await session.get(CustomImportGenerationSeal, run_result.generation_id)
            assert pointer is not None
            assert pointer.generation_id == run_result.generation_id
            assert seal is not None
            assert seal.root_count == 1
            assert seal.family_count == 1
            assert len((await session.scalars(select(CustomImportRootScalar))).all()) == 2
            assert len((await session.scalars(select(CustomImportChildScalar))).all()) == 2
            assert len((await session.scalars(select(CustomImportWinner))).all()) == 1


@pytest.mark.asyncio
async def test_snowflake_bridge_registers_and_activates_one_durable_root_candidate():
    async with isolated_publication_case() as case:
        seed = await _seed_case(case, "snowflake_bridge", _snowflake_definition())

        run_result = await run_snowflake_candidate(
            case.sessions,
            SnowflakeCandidateRequest(
                dataset_id=seed.dataset_id,
                definition_revision_id=seed.definition_revision_id,
                schema_revision_id=seed.schema_revision_id,
                definition=seed.definition,
                acquisition=_snowflake_acquisition(seed.definition),
                idempotency_key="synthetic-snowflake-bridge",
                lease_token="synthetic-snowflake-bridge-lease",
            ),
        )

        assert run_result.status == "activated"
        assert run_result.generation_id is not None
        async with case.sessions() as session:
            pointer = await session.get(CustomImportCurrentGeneration, seed.dataset_id)
            execution = await session.get(CustomImportExecution, run_result.execution_id)
            assert pointer is not None and pointer.generation_id == run_result.generation_id
            assert execution is not None and execution.capture_bundle_id is not None
            bundle = await session.get(CustomImportCaptureBundle, execution.capture_bundle_id)
            assert bundle is not None and bundle.snapshot_token == "synthetic-snowflake-snapshot"


def _bundle_family_expectation(definition: CustomImportDefinition):
    child_values = _rate("1234567893", "SYNTHETIC", Decimal("12.50"))
    expected = assemble_root_families(
        definition,
        [_root("1234567893", "Synthetic Bundle")],
        {"rates": [child_values]},
    )
    return child_values, expected


@pytest.mark.asyncio
async def test_snowflake_bundle_rehydrates_durable_streams_before_exact_activation():
    """Reserve before source acquisition, then replay only durable closed bytes."""
    async with isolated_publication_case() as case:
        seed = await _seed_case(case, "snowflake_bundle", _snowflake_bundle_definition())
        acquirer, candidate_request = _snowflake_bundle_candidate_request(
            seed,
            suffix="bundle",
            root_rows=(("1234567893", "Synthetic Bundle"),),
            child_rows=(("1234567893", "SYNTHETIC", Decimal("12.50")),),
        )
        opened_sessions, source_closed_flags = [], []

        @asynccontextmanager
        async def sessions_for_lifecycle():
            source_closed_flags.append(acquirer.source_closed)
            async with case.sessions() as session:
                opened_sessions.append(session)
                yield session

        run_result = await run_snowflake_bundle_candidate(
            sessions_for_lifecycle,
            acquirer,
            candidate_request,
        )

        assert acquirer.calls == 1 and not source_closed_flags[0]
        assert any(source_closed_flags) and all(source_closed_flags[source_closed_flags.index(True) :])
        assert len({id(session) for session in opened_sessions}) == len(opened_sessions)
        assert run_result.status == "activated"
        assert run_result.accepted_family_count == 1
        assert run_result.generation_id is not None
        child_values, expected = _bundle_family_expectation(seed.definition)
        async with case.sessions() as session:
            execution = await session.get(CustomImportExecution, run_result.execution_id)
            assert execution is not None and execution.capture_bundle_id is not None
            bundle = await session.get(CustomImportCaptureBundle, execution.capture_bundle_id)
            assert bundle is not None
            assert bundle.snapshot_token == "synthetic-snowflake-bundle"
            parts = (
                await session.scalars(
                    select(CustomImportCaptureParquetPart).where(
                        CustomImportCaptureParquetPart.capture_bundle_id == bundle.capture_bundle_id
                    )
                )
            ).all()
            family = (await session.scalars(select(CustomImportFamilyRevision))).one()
            child = (await session.scalars(select(CustomImportChildRevision))).one()
            binding = (await session.scalars(select(CustomImportEntityBinding))).one()
            pointer = await session.get(CustomImportCurrentGeneration, seed.dataset_id)
        assert len(parts) == 2 and all(part.payload for part in parts)
        assert family.child_count == 1
        assert family.family_sha256 == new_family_hash(seed.definition, expected.families[0])
        assert child.child_key_sha256 == child_key_hash(seed.definition, "rates", child_values)
        assert child.root_record_id == family.root_record_id
        assert binding.adapter_id == "npi" and binding.canonical_value == "1234567893"
        async with case.sessions() as session:
            assert len((await session.scalars(select(CustomImportRootScalar))).all()) == 2
            assert len((await session.scalars(select(CustomImportChildScalar))).all()) == 2
            assert len((await session.scalars(select(CustomImportWinner))).all()) == 1
        assert pointer is not None and pointer.generation_id == run_result.generation_id


@pytest.mark.asyncio
async def test_snowflake_bundle_cancel_first_retains_tombstone_without_source_contact():
    async with isolated_publication_case() as case:
        seed = await _seed_case(case, "snowflake_bundle_cancel_first", _snowflake_bundle_definition())
        acquirer, request = _snowflake_bundle_candidate_request(
            seed,
            suffix="cancel_first",
            root_rows=(("1234567893", "Canceled Bundle"),),
            child_rows=(("1234567893", "SYNTHETIC", Decimal("12.50")),),
        )
        identity = snowflake_candidate.bundle_request_identity_sha256(
            request.bundle_request, acquirer.build_statement(request.bundle_request)
        )
        exact_request_by_name = {
            "dataset_id": seed.dataset_id,
            "definition_revision_id": seed.definition_revision_id,
            "schema_revision_id": seed.schema_revision_id,
            "idempotency_key": request.idempotency_key,
            "mechanism": "local",
            "request_identity_sha256": identity,
        }
        async with case.sessions() as session, session.begin():
            canceled = await cancel_execution_request(session, **exact_request_by_name)

        result = await run_snowflake_bundle_candidate(case.sessions, acquirer, request)
        async with case.sessions() as session, session.begin():
            lookup = await lookup_execution_request(session, **exact_request_by_name)
        assert result.status == "not_claimed" and result.execution_id == canceled.execution_id
        assert lookup is not None and lookup.execution_id == canceled.execution_id
        assert lookup.state == "canceled" and acquirer.calls == 0


@pytest.mark.asyncio
async def test_snowflake_bundle_pre_reserved_first_claim_reads_source_once():
    async with isolated_publication_case() as case:
        seed = await _seed_case(case, "snowflake_bundle_pre_reserved", _snowflake_bundle_definition())
        acquirer, request = _snowflake_bundle_candidate_request(
            seed,
            suffix="pre_reserved",
            root_rows=(("1234567893", "Reserved Bundle"),),
            child_rows=(("1234567893", "SYNTHETIC", Decimal("12.50")),),
        )
        identity = snowflake_candidate.bundle_request_identity_sha256(
            request.bundle_request, acquirer.build_statement(request.bundle_request)
        )
        async with case.sessions() as session, session.begin():
            reserved = await reserve_execution(
                session,
                dataset_id=seed.dataset_id,
                definition_revision_id=seed.definition_revision_id,
                schema_revision_id=seed.schema_revision_id,
                idempotency_key=request.idempotency_key,
                mechanism="local",
                request_identity_sha256=identity,
            )

        result = await run_snowflake_bundle_candidate(case.sessions, acquirer, request)
        assert result.status == "activated" and result.execution_id == reserved.execution_id
        assert acquirer.calls == 1


@pytest.mark.asyncio
async def test_snowflake_bundle_resume_replays_durable_capture_without_acquiring(monkeypatch):
    """An expired owner reuses its bound capture instead of reading Snowflake again."""

    async with isolated_publication_case() as case:
        seed = await _seed_case(case, "snowflake_bundle_resume", _snowflake_bundle_definition())
        initial_acquirer, initial_request = _snowflake_bundle_candidate_request(
            seed,
            suffix="resume_initial",
            root_rows=(("1234567893", "Initial Bundle"),),
            child_rows=(("1234567893", "SYNTHETIC", Decimal("12.50")),),
        )
        original_materialize = runner._materialize_candidate

        async def expire_lease_before_materialization(session_factory, candidate_request, grant, admitted):
            async with case.sessions() as session, session.begin():
                await session.execute(
                    update(CustomImportLease)
                    .where(CustomImportLease.execution_id == grant.execution_id)
                    .values(expires_at=func.clock_timestamp())
                )
            return await original_materialize(session_factory, candidate_request, grant, admitted)

        monkeypatch.setattr(runner, "_materialize_candidate", expire_lease_before_materialization)
        initial_run = await run_snowflake_bundle_candidate(case.sessions, initial_acquirer, initial_request)
        monkeypatch.setattr(runner, "_materialize_candidate", original_materialize)

        recovery_acquirer, _ = _snowflake_bundle_candidate_request(
            seed,
            suffix="resume_unused",
            root_rows=(("1234567893", "Unexpected Bundle"),),
            child_rows=(("1234567893", "UNEXPECTED", Decimal("99.00")),),
        )
        recovery_request = replace(initial_request, lease_token="synthetic-snowflake-bundle-recovery-lease")
        resumed_run = await run_snowflake_bundle_candidate(case.sessions, recovery_acquirer, recovery_request)
        duplicate_run = await run_snowflake_bundle_candidate(case.sessions, recovery_acquirer, recovery_request)

        assert initial_run.status == "lease_lost"
        assert initial_acquirer.calls == 1
        assert resumed_run.status == "activated"
        assert resumed_run.execution_id == initial_run.execution_id
        assert duplicate_run.status == "not_claimed"
        assert recovery_acquirer.calls == 0


@pytest.mark.asyncio
async def test_snowflake_bundle_lost_lease_rolls_back_new_capture_binding(monkeypatch):
    """A stale claimant cannot attach or retain a newly acquired durable capture."""

    async with isolated_publication_case() as case:
        seed = await _seed_case(case, "snowflake_bundle_stale", _snowflake_bundle_definition())
        acquirer, candidate_request = _snowflake_bundle_candidate_request(
            seed,
            suffix="stale",
            root_rows=(("1234567893", "Stale Bundle"),),
            child_rows=(("1234567893", "SYNTHETIC", Decimal("12.50")),),
        )
        original_registration = snowflake_candidate._register_bundle_captures

        async def expire_lease_before_capture_binding(session_factory, request, grant, captures):
            async with case.sessions() as session, session.begin():
                await session.execute(
                    update(CustomImportLease)
                    .where(CustomImportLease.execution_id == grant.execution_id)
                    .values(expires_at=func.clock_timestamp())
                )
            return await original_registration(session_factory, request, grant, captures)

        monkeypatch.setattr(snowflake_candidate, "_register_bundle_captures", expire_lease_before_capture_binding)
        run_result = await run_snowflake_bundle_candidate(case.sessions, acquirer, candidate_request)

        assert run_result.status == "lease_lost"
        assert acquirer.calls == 1
        async with case.sessions() as session:
            execution = await session.get(CustomImportExecution, run_result.execution_id)
            capture_bundles = (
                await session.scalars(
                    select(CustomImportCaptureBundle).where(CustomImportCaptureBundle.dataset_id == seed.dataset_id)
                )
            ).all()
        assert execution is not None and execution.capture_bundle_id is None
        assert {bundle.capture_bundle_id for bundle in capture_bundles} == {seed.capture_bundle_id}


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ("cancellation", "lease_loss"))
async def test_snowflake_bundle_stops_after_source_when_lease_changes(monkeypatch, mode):
    """Cancellation or expiry after source work cannot retain or publish a bundle."""

    async with isolated_publication_case() as case:
        seed = await _seed_case(case, f"snowflake_bundle_{mode}", _snowflake_bundle_definition())
        acquirer, candidate_request = _snowflake_bundle_candidate_request(
            seed,
            suffix=mode,
            root_rows=(("1234567893", "Interrupted Bundle"),),
            child_rows=(("1234567893", "SYNTHETIC", Decimal("12.50")),),
        )
        original_renewal = snowflake_candidate._renew_bundle_lease
        renewal_calls = []

        async def change_lease_after_source(session_factory, request, grant):
            renewal_calls.append(grant)
            if len(renewal_calls) == 1:
                return await original_renewal(session_factory, request, grant)
            async with case.sessions() as session, session.begin():
                if mode == "cancellation":
                    await request_cancellation(session, execution_id=grant.execution_id)
                else:
                    await session.execute(
                        update(CustomImportLease)
                        .where(CustomImportLease.execution_id == grant.execution_id)
                        .values(expires_at=func.clock_timestamp())
                    )
            return await original_renewal(session_factory, request, grant)

        monkeypatch.setattr(snowflake_candidate, "_renew_bundle_lease", change_lease_after_source)
        run_result = await run_snowflake_bundle_candidate(case.sessions, acquirer, candidate_request)

        assert run_result.status == ("canceled" if mode == "cancellation" else "lease_lost")
        assert acquirer.calls == 1 and len(renewal_calls) == 2
        async with case.sessions() as session:
            execution = await session.get(CustomImportExecution, run_result.execution_id)
            capture_bundles = (
                await session.scalars(
                    select(CustomImportCaptureBundle).where(CustomImportCaptureBundle.dataset_id == seed.dataset_id)
                )
            ).all()
            generations = (await session.scalars(select(CustomImportGeneration))).all()
        assert execution is not None and execution.capture_bundle_id is None
        assert execution.state == ("canceled" if mode == "cancellation" else "running")
        assert {bundle.capture_bundle_id for bundle in capture_bundles} == {seed.capture_bundle_id}
        assert generations == []


@pytest.mark.asyncio
async def test_snowflake_bundle_unbound_capture_is_not_acquired_twice(monkeypatch):
    """A post-source interruption fails closed rather than rereading the same execution."""

    async with isolated_publication_case() as case:
        seed = await _seed_case(case, "snowflake_bundle_unbound", _snowflake_bundle_definition())
        acquirer, initial_request = _snowflake_bundle_candidate_request(
            seed,
            suffix="unbound",
            root_rows=(("1234567893", "Unbound Bundle"),),
            child_rows=(("1234567893", "SYNTHETIC", Decimal("12.50")),),
        )
        original_registration = snowflake_candidate._register_bundle_captures

        async def interrupt_before_capture_binding(*_arguments):
            raise RuntimeError("synthetic post-source interruption")

        monkeypatch.setattr(snowflake_candidate, "_register_bundle_captures", interrupt_before_capture_binding)
        with pytest.raises(RuntimeError, match="post-source interruption"):
            await run_snowflake_bundle_candidate(case.sessions, acquirer, initial_request)
        monkeypatch.setattr(snowflake_candidate, "_register_bundle_captures", original_registration)
        assert acquirer.calls == 1

        async with case.sessions() as session:
            execution = (await session.scalars(select(CustomImportExecution))).one()
            execution_id = execution.execution_id
        async with case.sessions() as session, session.begin():
            await session.execute(
                update(CustomImportLease)
                .where(CustomImportLease.execution_id == execution_id)
                .values(expires_at=func.clock_timestamp())
            )
        recovery_request = replace(initial_request, lease_token="synthetic-snowflake-unbound-recovery-lease")
        with pytest.raises(snowflake_candidate.SnowflakeCandidateError, match="unbound source capture"):
            await run_snowflake_bundle_candidate(case.sessions, acquirer, recovery_request)

        assert acquirer.calls == 1
        async with case.sessions() as session:
            execution = (await session.scalars(select(CustomImportExecution))).one()
        assert execution.execution_id == execution_id and execution.state == "failed"
        assert execution.capture_bundle_id is None


@pytest.mark.asyncio
@pytest.mark.parametrize("stored_identity_state", ("null", "live", "expired", "terminal"))
async def test_snowflake_bundle_resolved_mapping_drift_conflicts_before_claim_or_source(stored_identity_state):
    """A key cannot switch physical columns across legacy, live, expired, or terminal rows."""

    async with isolated_publication_case() as case:
        seed = await _seed_case(
            case, f"snowflake_mapping_drift_{stored_identity_state}", _snowflake_bundle_definition()
        )
        baseline_acquirer, baseline_request = _snowflake_bundle_candidate_request(
            seed,
            suffix=f"mapping_drift_{stored_identity_state}",
            root_rows=(("1234567893", "Baseline Bundle"),),
            child_rows=(("1234567893", "SYNTHETIC", Decimal("12.50")),),
        )
        baseline_statement = baseline_acquirer.build_statement(baseline_request.bundle_request)
        drifting_statement = _physical_mapping_drift_statement(seed.definition)
        drifting_acquirer = _PreparedStatementOnlyAcquirer(drifting_statement)
        assert drifting_statement.request.request_sha256 == baseline_statement.request.request_sha256
        assert drifting_statement.statement_sha256 != baseline_statement.statement_sha256
        execution_id, baseline_identity = await _seed_mapping_drift_execution(
            case,
            seed,
            baseline_request,
            baseline_statement,
            stored_identity_state,
        )

        drift_request = replace(
            baseline_request,
            lease_token=f"synthetic-mapping-drift-{stored_identity_state}-lease",
        )
        with pytest.raises(IdempotencyConflict):
            await run_snowflake_bundle_candidate(case.sessions, drifting_acquirer, drift_request)

        assert drifting_acquirer.acquire_calls == 0
        async with case.sessions() as session:
            execution = await session.get(CustomImportExecution, execution_id)
            lease = await session.get(CustomImportLease, execution_id)
        assert execution is not None and lease is not None
        assert execution.request_identity_sha256 == (None if stored_identity_state == "null" else baseline_identity)
        assert lease.fence == (0 if stored_identity_state == "null" else 1)
        assert (
            execution.state
            == {
                "null": "queued",
                "live": "running",
                "expired": "running",
                "terminal": "canceled",
            }[stored_identity_state]
        )


@pytest.mark.asyncio
async def test_snowflake_bundle_retains_only_the_family_with_an_invalid_child():
    """Retain one prior family while publishing an independent valid bundle update."""

    async with isolated_publication_case() as case:
        seed = await _seed_case(case, "snowflake_bundle_retention", _snowflake_bundle_definition())
        prior_acquirer, prior_request = _snowflake_bundle_candidate_request(
            seed,
            suffix="prior",
            root_rows=(("1234567893", "Prior First"), ("1003000126", "Prior Second")),
            child_rows=(
                ("1234567893", "FIRST", Decimal("10.00")),
                ("1003000126", "SECOND", Decimal("20.00")),
            ),
        )
        prior = await run_snowflake_bundle_candidate(case.sessions, prior_acquirer, prior_request)
        assert prior.status == "activated"

        update_acquirer, update_request = _snowflake_bundle_candidate_request(
            seed,
            suffix="update",
            root_rows=(("1234567893", "Rejected First"), ("1003000126", "Accepted Second")),
            child_rows=(("1234567893", "FIRST", "invalid"), ("1003000126", "SECOND", "21.00")),
            amount_source_type="TEXT",
        )
        update_run = await run_snowflake_bundle_candidate(case.sessions, update_acquirer, update_request)

        assert update_run.status == "activated"
        assert update_run.generation_id is not None
        assert update_run.accepted_family_count == 1
        assert update_run.rejection_count == 1
        async with case.sessions() as session:
            rejections = (
                await session.scalars(
                    select(CustomImportRejection).where(CustomImportRejection.execution_id == update_run.execution_id)
                )
            ).all()
            root_revisions = await _generation_root_revisions(session, update_run.generation_id)
        assert {rejection.code for rejection in rejections} == {"field_type_invalid"}
        assert {
            json.loads(root_revision.canonical_payload)["fields"][1]["value"]["value"]
            for root_revision in root_revisions
        } == {"Prior First", "Accepted Second"}


@pytest.mark.asyncio
async def test_runner_validates_execution_identity_before_claim_mutation():
    async with isolated_publication_case() as case:
        seed = await _seed_case(case, "identity")
        execution_id, token = await _new_execution(case, seed, "identity")
        request = _request(
            seed,
            execution_id,
            token,
            [_root("1234567893", "Synthetic Clinic")],
            [_rate("1234567893", "SYNTHETIC", Decimal("12.50"))],
        )

        with pytest.raises(CandidateRunnerError, match="identity does not match"):
            await run_candidate(
                case.sessions,
                replace(request, definition_revision_id=request.definition_revision_id + 1),
            )

        async with case.sessions() as session:
            execution = await session.get(CustomImportExecution, execution_id)
            lease = await session.get(CustomImportLease, execution_id)
            assert execution is not None and execution.state == "queued" and execution.started_at is None
            assert lease is not None and lease.fence == 0 and lease.token_sha256 is None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("definition_factory", "error"),
    (
        (_noncanonical_definition, "does not match its canonical form"),
        (_snapshot_definition, "persisted definition or schema"),
        (_schema_mismatch_definition, "persisted definition or schema"),
    ),
)
async def test_runner_validates_supplied_definition_and_schema_before_claim_mutation(definition_factory, error):
    async with isolated_publication_case() as case:
        seed = await _seed_case(case, "canonical")
        execution_id, token = await _new_execution(case, seed, "canonical")
        request = _request(
            seed,
            execution_id,
            token,
            [_root("1234567893", "Synthetic Clinic")],
            [_rate("1234567893", "SYNTHETIC", Decimal("12.50"))],
        )

        with pytest.raises(CandidateRunnerError, match=error):
            await run_candidate(case.sessions, replace(request, definition=definition_factory()))

        async with case.sessions() as session:
            execution = await session.get(CustomImportExecution, execution_id)
            lease = await session.get(CustomImportLease, execution_id)
            assert execution is not None and execution.state == "queued" and execution.started_at is None
            assert lease is not None and lease.fence == 0 and lease.token_sha256 is None


@pytest.mark.asyncio
async def test_runner_retains_prior_family_for_invalid_child_and_records_rejection():
    async with isolated_publication_case() as case:
        seed = await _seed_case(case, "retain")
        await _run_initial_retention_candidate(case, seed)
        second_execution, update_run = await _run_invalid_child_update(case, seed)
        assert update_run.status == "activated"
        assert update_run.generation_id is not None
        assert update_run.accepted_family_count == 1
        assert update_run.rejection_count == 2
        await _assert_retained_generation(case, update_run.generation_id, second_execution)


@pytest.mark.asyncio
async def test_runner_retains_prior_family_for_rejected_canonical_root_duplicate():
    async with isolated_publication_case() as case:
        seed = await _seed_case(case, "canonical_root", _decimal_root_key_definition())
        first_execution, first_token = await _new_execution(case, seed, "canonical_root_first")
        first = await run_candidate(
            case.sessions,
            _request(
                seed,
                first_execution,
                first_token,
                [_root_with_rank("1234567893", "Prior", 1)],
                [_rate_with_rank("1234567893", "PRIOR", Decimal("10"), 1)],
            ),
        )
        assert first.status == "activated"

        second_execution, second_token = await _new_execution(case, seed, "canonical_root_second")
        second = await run_candidate(
            case.sessions,
            _request(
                seed,
                second_execution,
                second_token,
                [
                    _root_with_rank("1234567893", "Replacement", 1),
                    {"npi": "1234567893", "rank": "1.0", "display_name": True},
                ],
                [_rate_with_rank("1234567893", "REPLACEMENT", Decimal("20"), 1)],
            ),
        )

        assert second.status == "no_change"
        assert second.generation_id is not None
        assert second.accepted_family_count == 0
        assert second.rejection_count == 2
        async with case.sessions() as session:
            rejections = (
                await session.scalars(
                    select(CustomImportRejection).where(CustomImportRejection.execution_id == second_execution)
                )
            ).all()
            root_revisions = await _generation_root_revisions(session, second.generation_id)
        assert {rejection.code for rejection in rejections} == {"duplicate_root_key", "field_type_invalid"}
        assert {
            json.loads(root_revision.canonical_payload)["fields"][1]["value"]["value"]
            for root_revision in root_revisions
        } == {"Prior"}


@pytest.mark.asyncio
async def test_runner_rejects_canonical_child_duplicate_without_blocking_other_roots():
    async with isolated_publication_case() as case:
        seed = await _seed_case(case, "canonical_child", _decimal_child_key_definition())
        execution_id, token = await _new_execution(case, seed, "canonical_child")

        run_result = await run_candidate(
            case.sessions,
            _request(
                seed,
                execution_id,
                token,
                [
                    _root("1234567893", "Duplicate Child"),
                    _root("1003000126", "Valid Child"),
                ],
                [
                    _rate("1234567893", "FIRST", 1),
                    _rate("1234567893", "SECOND", "1.0"),
                    _rate("1003000126", "VALID", Decimal("2")),
                ],
            ),
        )

        assert run_result.status == "activated"
        assert run_result.generation_id is not None
        assert run_result.accepted_family_count == 1
        assert run_result.rejection_count == 1
        async with case.sessions() as session:
            rejections = (
                await session.scalars(
                    select(CustomImportRejection).where(CustomImportRejection.execution_id == execution_id)
                )
            ).all()
            root_revisions = await _generation_root_revisions(session, run_result.generation_id)
        assert {rejection.code for rejection in rejections} == {"duplicate_child_key"}
        assert {
            json.loads(root_revision.canonical_payload)["fields"][1]["value"]["value"]
            for root_revision in root_revisions
        } == {"Valid Child"}


@pytest.mark.asyncio
async def test_runner_rejects_same_instant_timestamp_child_duplicate_without_blocking_other_roots():
    async with isolated_publication_case() as case:
        seed = await _seed_case(case, "timestamp_duplicate", _timestamp_child_key_definition())
        execution_id, token = await _new_execution(case, seed, "timestamp_duplicate")
        local = datetime(2026, 11, 1, 1, 30, tzinfo=ZoneInfo("America/New_York"), fold=0)

        run_result = await run_candidate(
            case.sessions,
            _request(
                seed,
                execution_id,
                token,
                [
                    _root("1234567893", "Duplicate Timestamp"),
                    _root("1003000126", "Valid Timestamp"),
                ],
                [
                    {**_rate("1234567893", "FIRST", Decimal("12.50")), "captured_at": local},
                    {
                        **_rate("1234567893", "SECOND", Decimal("12.50")),
                        "captured_at": local.astimezone(UTC),
                    },
                    {
                        **_rate("1003000126", "VALID", Decimal("12.50")),
                        "captured_at": datetime(2026, 11, 1, 7, 30, tzinfo=UTC),
                    },
                ],
            ),
        )

        assert run_result.status == "activated"
        assert run_result.generation_id is not None
        assert run_result.accepted_family_count == 1
        assert run_result.rejection_count == 1
        async with case.sessions() as session:
            rejections = (
                await session.scalars(
                    select(CustomImportRejection).where(CustomImportRejection.execution_id == execution_id)
                )
            ).all()
            root_revisions = await _generation_root_revisions(session, run_result.generation_id)
        assert {rejection.code for rejection in rejections} == {"duplicate_child_key"}
        assert {
            json.loads(root_revision.canonical_payload)["fields"][1]["value"]["value"]
            for root_revision in root_revisions
        } == {"Valid Timestamp"}


@pytest.mark.asyncio
async def test_runner_accepts_distinct_timestamp_folds():
    async with isolated_publication_case() as case:
        seed = await _seed_case(case, "timestamp_folds", _timestamp_child_key_definition())
        execution_id, token = await _new_execution(case, seed, "timestamp_folds")
        new_york = ZoneInfo("America/New_York")

        run_result = await run_candidate(
            case.sessions,
            _request(
                seed,
                execution_id,
                token,
                [_root("1234567893", "Distinct Timestamps")],
                [
                    {
                        **_rate("1234567893", "FIRST", Decimal("12.50")),
                        "captured_at": datetime(2026, 11, 1, 1, 30, tzinfo=new_york, fold=0),
                    },
                    {
                        **_rate("1234567893", "SECOND", Decimal("12.50")),
                        "captured_at": datetime(2026, 11, 1, 1, 30, tzinfo=new_york, fold=1),
                    },
                ],
            ),
        )

        assert run_result.status == "activated"
        assert run_result.generation_id is not None
        assert run_result.accepted_family_count == 1
        assert run_result.rejection_count == 0
        async with case.sessions() as session:
            children = (await session.scalars(select(CustomImportChildRevision))).all()
        assert len(children) == 2


async def _run_initial_retention_candidate(case, seed: _Seed) -> None:
    """Create the prior generation that an invalid update must retain."""

    execution_id, token = await _new_execution(case, seed, "retain_first")
    initial_run = await run_candidate(
        case.sessions,
        _request(
            seed,
            execution_id,
            token,
            [_root("1234567893", "Synthetic First"), _root("1003000126", "Synthetic Second")],
            [
                _rate("1234567893", "FIRST", Decimal("10.00")),
                _rate("1003000126", "SECOND", Decimal("20.00")),
            ],
        ),
    )
    assert initial_run.status == "activated"


async def _run_invalid_child_update(case, seed: _Seed) -> tuple[int, CandidateRunResult]:
    """Run an update with one rejected root family and one valid replacement."""

    execution_id, token = await _new_execution(case, seed, "retain_second")
    update_run = await run_candidate(
        case.sessions,
        _request(
            seed,
            execution_id,
            token,
            [
                _root("1234567893", "Synthetic First Changed"),
                _root("1003000126", "Synthetic Second Changed"),
                {"npi": 1003000126, "display_name": "Malformed Rejection"},
            ],
            [
                _rate("1234567893", "FIRST", True),
                _rate("1003000126", "SECOND", Decimal("21.00")),
                {"rate_npi": 1003000126, "service_code": "MALFORMED", "amount": Decimal("22.00")},
            ],
        ),
    )
    return execution_id, update_run


async def _assert_retained_generation(case, generation_id: int, execution_id: int) -> None:
    """Verify that rejected roots retain prior data and exact rejection evidence."""

    async with case.sessions() as session:
        generation_families = (
            await session.scalars(
                select(CustomImportGenerationFamily).where(CustomImportGenerationFamily.generation_id == generation_id)
            )
        ).all()
        rejections = (
            await session.scalars(
                select(CustomImportRejection).where(CustomImportRejection.execution_id == execution_id)
            )
        ).all()
        root_revisions = await _generation_root_revisions(session, generation_id)
    retained_names = {
        json.loads(root_revision.canonical_payload)["fields"][1]["value"]["value"] for root_revision in root_revisions
    }
    assert len(generation_families) == 2
    assert {rejection.code for rejection in rejections} == {"field_type_invalid"}
    assert sum(rejection.root_key_sha256 is None for rejection in rejections) == 1
    assert retained_names == {"Synthetic First", "Synthetic Second Changed"}


async def _generation_root_revisions(session: AsyncSession, generation_id: int) -> list[CustomImportRootRevision]:
    """Load exact root payload revisions for one generation membership set."""

    return list(
        (
            await session.scalars(
                select(CustomImportRootRevision)
                .join(
                    CustomImportFamilyRevision,
                    CustomImportFamilyRevision.root_revision_id == CustomImportRootRevision.root_revision_id,
                )
                .join(
                    CustomImportGenerationFamily,
                    CustomImportGenerationFamily.family_revision_id == CustomImportFamilyRevision.family_revision_id,
                )
                .where(CustomImportGenerationFamily.generation_id == generation_id)
            )
        ).all()
    )


@pytest.mark.asyncio
async def test_runner_records_no_change_without_moving_the_current_pointer():
    async with isolated_publication_case() as case:
        seed = await _seed_case(case, "no_change")
        first_execution, first_token = await _new_execution(case, seed, "no_change_first")
        first = await run_candidate(
            case.sessions,
            _request(
                seed,
                first_execution,
                first_token,
                [_root("1234567893", "Synthetic Clinic")],
                [_rate("1234567893", "SYNTHETIC", Decimal("12.50"))],
            ),
        )
        assert first.status == "activated"

        second_execution, second_token = await _new_execution(case, seed, "no_change_second")
        second = await run_candidate(
            case.sessions,
            _request(
                seed,
                second_execution,
                second_token,
                [_root("1234567893", "Synthetic Clinic")],
                [_rate("1234567893", "SYNTHETIC", Decimal("12.50"))],
            ),
        )

        assert second.status == "no_change"
        assert second.generation_id is not None
        async with case.sessions() as session:
            pointer = await session.get(CustomImportCurrentGeneration, seed.dataset_id)
            execution = await session.get(CustomImportExecution, second_execution)
            no_change = await session.get(CustomImportNoChangeSeal, second_execution)
            assert pointer is not None
            assert pointer.generation_id == first.generation_id
            assert execution is not None and execution.state == "no_change"
            assert no_change is not None and no_change.candidate_generation_id == second.generation_id


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ("cancellation", "lease_loss"))
async def test_runner_stops_before_materialization_when_canceled_or_fence_is_lost(monkeypatch, mode):
    async with isolated_publication_case() as case:
        seed = await _seed_case(case, mode)
        execution_id, token = await _new_execution(case, seed, mode)
        original_materialize = runner._materialize_candidate

        async def interrupt_before_graph(session_factory, request, grant, admitted):
            async with case.sessions() as session, session.begin():
                if mode == "cancellation":
                    await request_cancellation(session, execution_id=request.execution_id)
                else:
                    await session.execute(
                        update(CustomImportLease)
                        .where(CustomImportLease.execution_id == request.execution_id)
                        .values(expires_at=func.clock_timestamp())
                    )
            return await original_materialize(session_factory, request, grant, admitted)

        monkeypatch.setattr(runner, "_materialize_candidate", interrupt_before_graph)
        run_result = await run_candidate(
            case.sessions,
            _request(
                seed,
                execution_id,
                token,
                [_root("1234567893", "Synthetic Clinic")],
                [_rate("1234567893", "SYNTHETIC", Decimal("12.50"))],
            ),
        )

        assert run_result.status == ("canceled" if mode == "cancellation" else "lease_lost")
        async with case.sessions() as session:
            assert len((await session.scalars(select(CustomImportGeneration))).all()) == 0
            execution = await session.get(CustomImportExecution, execution_id)
            assert execution is not None
            assert execution.state == ("canceled" if mode == "cancellation" else "running")


@pytest.mark.asyncio
async def test_runner_cancels_rejected_candidate_when_cancellation_wins(monkeypatch):
    async with isolated_publication_case() as case:
        seed = await _seed_case(case, "rejected_cancellation", _snapshot_definition())
        execution_id, token = await _new_execution(case, seed, "rejected_cancellation")
        original_finish = runner._finish_rejected_candidate

        async def cancel_then_finish(session_factory, request, grant, admitted):
            async with case.sessions() as session, session.begin():
                transition = await request_cancellation(session, execution_id=request.execution_id)
            assert transition.state == "canceling"
            return await original_finish(session_factory, request, grant, admitted)

        monkeypatch.setattr(runner, "_finish_rejected_candidate", cancel_then_finish)
        run_result = await run_candidate(
            case.sessions,
            _request(
                seed,
                execution_id,
                token,
                [_root("1234567893", "Synthetic Clinic")],
                [_rate("1234567893", "SYNTHETIC", Decimal("12.50"))],
            ),
        )

        assert run_result.status == "canceled"
        async with case.sessions() as session:
            execution = await session.get(CustomImportExecution, execution_id)
            assert execution is not None and execution.state == "canceled"
            assert len((await session.scalars(select(CustomImportGeneration))).all()) == 0


@pytest.mark.asyncio
async def test_runner_rolls_back_partial_materialization_if_projection_fails(monkeypatch):
    async with isolated_publication_case() as case:
        seed = await _seed_case(case, "rollback")
        execution_id, token = await _new_execution(case, seed, "rollback")
        original_persist = runner_graph.persist_projections_and_winners

        async def fail_after_projection(*args, **kwargs):
            await original_persist(*args, **kwargs)
            raise CandidateRunnerError("synthetic projection failure")

        monkeypatch.setattr(runner_graph, "persist_projections_and_winners", fail_after_projection)
        with pytest.raises(CandidateRunnerError, match="synthetic projection failure"):
            await run_candidate(
                case.sessions,
                _request(
                    seed,
                    execution_id,
                    token,
                    [_root("1234567893", "Synthetic Clinic")],
                    [_rate("1234567893", "SYNTHETIC", Decimal("12.50"))],
                ),
            )

        async with case.sessions() as session:
            for model in (
                CustomImportPack,
                CustomImportGeneration,
                CustomImportRootRevision,
                CustomImportChildRevision,
                CustomImportRootScalar,
                CustomImportChildScalar,
                CustomImportWinner,
                CustomImportSelectionProfile,
            ):
                assert len((await session.scalars(select(model))).all()) == 0
