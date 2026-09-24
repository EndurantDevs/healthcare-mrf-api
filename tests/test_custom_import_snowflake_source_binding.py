# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic contracts for immutable Snowflake source-binding evidence."""

from __future__ import annotations

import asyncio
import hashlib
import json
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import func, select

import process.custom_import.snowflake_candidate as snowflake_candidate
import process.custom_import.snowflake_source_binding as source_binding
from db.models.custom_import import CustomImportExecution, CustomImportSourceBindingRevision
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.execution import (
    ExecutionSubmission,
    IdempotencyConflict,
    lookup_execution_request,
    request_cancellation,
    reserve_execution,
)
from process.custom_import.snowflake_bundle import SnowflakeBundleAcquisitionConnector
from process.custom_import.snowflake_candidate import SnowflakeBundleCandidateRequest, SnowflakeCandidateError
from tests.custom_import_postgres_support import isolated_publication_case, transaction_session


def _definition() -> CustomImportDefinition:
    return CustomImportDefinition.from_mapping(
        {
            "contract": "custom-import/v1",
            "revision": {"definition": 1, "schema": 1},
            "refresh_mode": "snapshot",
            "streams": [
                {
                    "id": "root_source",
                    "kind": "root",
                    "format": "parquet",
                    "compression": "none",
                    "snapshot_token": "root_snapshot",
                },
                {
                    "id": "detail_source",
                    "kind": "child",
                    "child": "details",
                    "format": "parquet",
                    "compression": "none",
                    "snapshot_token": "detail_snapshot",
                },
            ],
            "schema": {
                "root": {
                    "logical_key": ["npi"],
                    "entity": {"adapter": "npi", "field": "npi"},
                    "fields": [
                        {"id": "npi", "slot": 1, "type": "string", "nullable": False},
                        {"id": "score", "slot": 2, "type": "integer", "nullable": False},
                    ],
                },
                "children": [
                    {
                        "name": "details",
                        "parent_key": [{"child": "detail_npi", "root": "npi"}],
                        "child_key": ["detail_id"],
                        "fields": [
                            {"id": "detail_npi", "slot": 3, "type": "string", "nullable": False},
                            {"id": "detail_id", "slot": 4, "type": "string", "nullable": False},
                        ],
                    }
                ],
            },
            "aliases": {
                "root_source": {"ROOT_NPI": "npi", "ROOT_SCORE": "score"},
                "detail_source": {"DETAIL_NPI": "detail_npi", "DETAIL_ID": "detail_id"},
            },
            "query": {"root_fields": [], "order": []},
            "selection_profiles": [],
        }
    )


def _binding_document(definition: CustomImportDefinition) -> dict[str, object]:
    return {
        "contract": source_binding.SOURCE_BINDING_CONTRACT,
        "connector": source_binding.SNOWFLAKE_SOURCE_BINDING_CONNECTOR,
        "definition_sha256": definition.digest,
        "schema_sha256": definition.schema_digest,
        "source_object": {"fingerprint_sha256": "1" * 64, "version": "snapshot-20260923"},
        "role": "synthetic_reader",
        "warehouse": "synthetic_load",
        "streams": [
            {
                "stream_id": "root_source",
                "relation": ["synthetic", "public", "root_records"],
                "source_snapshot_token_relation": ["synthetic", "public", "root_snapshots"],
                "semantic_token_metadata_key": "root_snapshot",
                "source_snapshot_token_column_identifier": "root_snapshot_token",
                "columns": [
                    {"field_id": "npi", "column_identifier": "root_npi"},
                    {"field_id": "score", "column_identifier": "root_score"},
                ],
            },
            {
                "stream_id": "detail_source",
                "relation": ["synthetic", "public", "detail_records"],
                "source_snapshot_token_relation": ["synthetic", "public", "detail_snapshots"],
                "semantic_token_metadata_key": "detail_snapshot",
                "source_snapshot_token_column_identifier": "detail_snapshot_token",
                "columns": [
                    {"field_id": "detail_npi", "column_identifier": "detail_npi"},
                    {"field_id": "detail_id", "column_identifier": "detail_id"},
                ],
            },
        ],
    }


def _binding(definition: CustomImportDefinition) -> source_binding.SnowflakeSourceBinding:
    return source_binding.SnowflakeSourceBinding.from_json(json.dumps(_binding_document(definition)))


def _binding_at_version(
    definition: CustomImportDefinition,
    version: str,
) -> source_binding.SnowflakeSourceBinding:
    document = _binding_document(definition)
    document["source_object"]["version"] = version
    return source_binding.SnowflakeSourceBinding.from_json(json.dumps(document))


def _loaded_rows(definition: CustomImportDefinition, binding: source_binding.SnowflakeSourceBinding):
    schema_row = SimpleNamespace(
        dataset_id=11,
        schema_revision_id=13,
        revision_number=definition.schema_revision,
        canonical_schema=definition.schema_canonical,
        schema_sha256=bytes.fromhex(definition.schema_digest),
    )
    definition_row = SimpleNamespace(
        dataset_id=11,
        definition_revision_id=12,
        schema_revision_id=13,
        revision_number=definition.definition_revision,
        contract_version="custom-import/v1",
        canonical_definition=definition.canonical,
        definition_sha256=bytes.fromhex(definition.digest),
    )
    binding_row = SimpleNamespace(
        dataset_id=11,
        definition_revision_id=12,
        schema_revision_id=13,
        source_binding_revision_id=14,
        revision_number=1,
        binding_contract=source_binding.SOURCE_BINDING_CONTRACT,
        connector_kind=source_binding.SNOWFLAKE_SOURCE_BINDING_CONNECTOR,
        canonical_binding=binding.canonical,
        binding_sha256=bytes.fromhex(binding.digest),
        definition_sha256=bytes.fromhex(definition.digest),
        schema_sha256=bytes.fromhex(definition.schema_digest),
        source_object_fingerprint_sha256=bytes.fromhex(binding.source_object.fingerprint_sha256),
        source_object_version=binding.source_object.version,
    )
    return binding_row, definition_row, schema_row


def test_binding_derives_complete_approved_relations_and_generated_bundle_identity():
    definition = _definition()
    binding = _binding(definition)

    approved_relations, bindings = binding.bundle_components(definition)
    connector = SnowflakeBundleAcquisitionConnector(
        approved_relations=approved_relations,
        credential_provider=SimpleNamespace(load_key_pair=lambda: None),
        adapter=SimpleNamespace(fetch_bundle=lambda *_args: None),
    )
    statement = connector.build_statement(connector.prepare_request(definition, bindings=bindings))

    replayed = source_binding.SnowflakeSourceBinding.from_json(binding.canonical)
    assert binding.canonical == replayed.canonical
    assert binding.digest == replayed.digest
    canonical_document = json.loads(binding.canonical)
    assert "snapshot" not in canonical_document
    assert all("source_snapshot_token_column_identifier" in stream for stream in canonical_document["streams"])
    assert tuple(bundle_binding.stream_id for bundle_binding in bindings) == ("root_source", "detail_source")
    assert tuple(
        (bundle_binding.source_snapshot_token_relation.name, bundle_binding.semantic_token_metadata_key)
        for bundle_binding in bindings
    ) == (("ROOT_SNAPSHOTS", "root_snapshot"), ("DETAIL_SNAPSHOTS", "detail_snapshot"))
    assert {relation.relation.name for relation in approved_relations} == {
        "ROOT_SNAPSHOTS",
        "DETAIL_SNAPSHOTS",
        "ROOT_RECORDS",
        "DETAIL_RECORDS",
    }
    assert 'FROM "SYNTHETIC"."PUBLIC"."ROOT_RECORDS"' in statement.sql
    assert 'FROM "SYNTHETIC"."PUBLIC"."DETAIL_RECORDS"' in statement.sql
    assert 'FROM "SYNTHETIC"."PUBLIC"."ROOT_SNAPSHOTS"' in statement.sql
    assert 'FROM "SYNTHETIC"."PUBLIC"."DETAIL_SNAPSHOTS"' in statement.sql
    assert 'SELECT "ROOT_SNAPSHOT_TOKEN"' in statement.sql
    assert 'SELECT "DETAIL_SNAPSHOT_TOKEN"' in statement.sql
    assert "DROP" not in statement.sql


def test_binding_rejects_unknown_keys_paths_and_incomplete_field_mapping():
    definition = _definition()
    document = _binding_document(definition)
    document["sql"] = "SELECT"
    with pytest.raises(source_binding.SnowflakeSourceBindingError):
        source_binding.SnowflakeSourceBinding.from_json(json.dumps(document))

    document = _binding_document(definition)
    document["source_object"]["version"] = "../untrusted"
    with pytest.raises(source_binding.SnowflakeSourceBindingError):
        source_binding.SnowflakeSourceBinding.from_json(json.dumps(document))

    document = _binding_document(definition)
    document["snapshot"] = {
        "relation": document["streams"][0].pop("source_snapshot_token_relation"),
        "selector": document["streams"][0].pop("semantic_token_metadata_key"),
        "column_identifier": document["streams"][0].pop("source_snapshot_token_column_identifier"),
    }
    for field_name in (
        "source_snapshot_token_relation",
        "semantic_token_metadata_key",
        "source_snapshot_token_column_identifier",
    ):
        document["streams"][1].pop(field_name)
    with pytest.raises(source_binding.SnowflakeSourceBindingError):
        source_binding.SnowflakeSourceBinding.from_json(json.dumps(document))

    document = _binding_document(definition)
    document["streams"][0]["columns"].pop()
    with pytest.raises(source_binding.SnowflakeSourceBindingError, match="coverage"):
        source_binding.SnowflakeSourceBinding.from_json(json.dumps(document)).bundle_components(definition)

    document = _binding_document(definition)
    document["streams"][1]["semantic_token_metadata_key"] = "root_snapshot"
    with pytest.raises(source_binding.SnowflakeSourceBindingError, match="selector"):
        source_binding.SnowflakeSourceBinding.from_json(json.dumps(document)).bundle_components(definition)

    document = _binding_document(definition)
    document["streams"][1]["source_snapshot_token_relation"] = document["streams"][0]["source_snapshot_token_relation"]
    document["streams"][1]["source_snapshot_token_column_identifier"] = "root_snapshot_token"
    with pytest.raises(source_binding.SnowflakeSourceBindingError, match="physical column mapping"):
        source_binding.SnowflakeSourceBinding.from_json(json.dumps(document)).bundle_components(definition)


def test_snapshot_mapping_changes_binding_digest_and_legacy_rows_fail_closed():
    definition = _definition()
    binding = _binding(definition)
    changed_document = _binding_document(definition)
    changed_document["streams"][1]["source_snapshot_token_column_identifier"] = "changed_detail_snapshot_token"
    changed_binding = source_binding.SnowflakeSourceBinding.from_json(json.dumps(changed_document))

    assert changed_binding.digest != binding.digest

    legacy_document = _binding_document(definition)
    legacy_document["snapshot"] = {
        "relation": legacy_document["streams"][0].pop("source_snapshot_token_relation"),
        "selector": legacy_document["streams"][0].pop("semantic_token_metadata_key"),
        "column_identifier": legacy_document["streams"][0].pop("source_snapshot_token_column_identifier"),
    }
    for field_name in (
        "source_snapshot_token_relation",
        "semantic_token_metadata_key",
        "source_snapshot_token_column_identifier",
    ):
        legacy_document["streams"][1].pop(field_name)
    binding_row, definition_row, schema_row = _loaded_rows(definition, binding)
    binding_row.canonical_binding = json.dumps(legacy_document)
    with pytest.raises(source_binding.SnowflakeSourceBindingUnavailableError):
        source_binding._loaded_snowflake_source_binding(binding_row, definition_row, schema_row)


def test_persisted_binding_checks_canonical_rows_and_binds_candidate_identity():
    definition = _definition()
    binding = _binding(definition)
    loaded = source_binding._loaded_snowflake_source_binding(*_loaded_rows(definition, binding))
    connector = SnowflakeBundleAcquisitionConnector(
        approved_relations=loaded.approved_relations,
        credential_provider=SimpleNamespace(load_key_pair=lambda: None),
        adapter=SimpleNamespace(fetch_bundle=lambda *_args: None),
    )
    bundle_request = connector.prepare_request(loaded.definition, bindings=loaded.bundle_bindings)
    statement = connector.build_statement(bundle_request)
    request = SnowflakeBundleCandidateRequest(
        dataset_id=loaded.dataset_id,
        definition_revision_id=loaded.definition_revision_id,
        schema_revision_id=loaded.schema_revision_id,
        definition=loaded.definition,
        bundle_request=bundle_request,
        idempotency_key="synthetic-binding",
        lease_token="synthetic-lease",
        source_binding_revision_id=loaded.source_binding_revision_id,
        source_binding_sha256=loaded.source_binding_sha256,
    )
    unbound = SnowflakeBundleCandidateRequest(
        dataset_id=loaded.dataset_id,
        definition_revision_id=loaded.definition_revision_id,
        schema_revision_id=loaded.schema_revision_id,
        definition=loaded.definition,
        bundle_request=bundle_request,
        idempotency_key="synthetic-binding",
        lease_token="synthetic-lease",
    )

    assert source_binding._loaded_snowflake_source_binding(
        *_loaded_rows(definition, binding)
    ).source_binding_sha256 == bytes.fromhex(binding.digest)
    assert snowflake_candidate.bundle_request_identity_sha256(
        request.bundle_request, statement, source_binding_sha256=request.source_binding_sha256
    ) != snowflake_candidate.bundle_request_identity_sha256(unbound.bundle_request, statement)
    with pytest.raises(SnowflakeCandidateError, match="source binding identity"):
        snowflake_candidate._validated_bundle_request(
            SnowflakeBundleCandidateRequest(
                **{**request.__dict__, "source_binding_sha256": None},
            )
        )

    binding_row, definition_row, schema_row = _loaded_rows(definition, binding)
    binding_row.binding_sha256 = b"x" * 32
    with pytest.raises(source_binding.SnowflakeSourceBindingUnavailableError):
        source_binding._loaded_snowflake_source_binding(binding_row, definition_row, schema_row)


@pytest.mark.asyncio
async def test_bundle_reservation_carries_the_retained_binding_revision(monkeypatch):
    definition = _definition()
    binding = _binding(definition)
    loaded = source_binding._loaded_snowflake_source_binding(*_loaded_rows(definition, binding))
    connector = SnowflakeBundleAcquisitionConnector(
        approved_relations=loaded.approved_relations,
        credential_provider=SimpleNamespace(load_key_pair=lambda: None),
        adapter=SimpleNamespace(fetch_bundle=lambda *_args: None),
    )
    request = SnowflakeBundleCandidateRequest(
        dataset_id=loaded.dataset_id,
        definition_revision_id=loaded.definition_revision_id,
        schema_revision_id=loaded.schema_revision_id,
        definition=loaded.definition,
        bundle_request=connector.prepare_request(loaded.definition, bindings=loaded.bundle_bindings),
        idempotency_key="synthetic-binding",
        lease_token="synthetic-lease",
        source_binding_revision_id=loaded.source_binding_revision_id,
        source_binding_sha256=loaded.source_binding_sha256,
    )
    captured_by_key = {}

    class _Session:
        @asynccontextmanager
        async def begin(self):
            yield self

    @asynccontextmanager
    async def session_factory():
        yield _Session()

    async def validate(*_args):
        return None

    async def load(_session, **_kwargs):
        return loaded

    async def reserve(_session, **arguments):
        captured_by_key.update(arguments)
        return ExecutionSubmission(execution_id=15, state="queued", created=True)

    async def claim(*_args, **_kwargs):
        return None

    monkeypatch.setattr(snowflake_candidate, "validate_revision_identity", validate)
    monkeypatch.setattr(snowflake_candidate, "load_snowflake_source_binding", load)
    monkeypatch.setattr(snowflake_candidate, "reserve_execution", reserve)
    monkeypatch.setattr(snowflake_candidate, "claim_execution", claim)

    submission, grant = await snowflake_candidate._reserve_bundle_execution(
        session_factory, request, connector.build_statement(request.bundle_request), b"x" * 32
    )

    assert submission.execution_id == 15
    assert grant is None
    assert captured_by_key["source_binding_revision_id"] == loaded.source_binding_revision_id
    assert captured_by_key["request_identity_sha256"] == b"x" * 32


@pytest.mark.asyncio
async def test_source_binding_registration_reloads_its_exact_immutable_receipt():
    definition = _definition()
    binding = _binding(definition)

    async with transaction_session() as session, session.begin():
        first = await source_binding.register_snowflake_source_binding(
            session,
            dataset_key="synthetic_source_binding",
            definition=definition,
            binding=binding,
        )
        loaded = await source_binding.load_snowflake_source_binding(
            session,
            definition_revision_id=first.definition_revision_id,
            source_binding_revision_id=first.source_binding_revision_id,
        )
        replay = await source_binding.register_snowflake_source_binding(
            session,
            dataset_key="synthetic_source_binding",
            definition=definition,
            binding=binding,
        )

        assert first.created is True
        assert replay == source_binding.SnowflakeSourceBindingReceipt(
            dataset_id=first.dataset_id,
            definition_revision_id=first.definition_revision_id,
            schema_revision_id=first.schema_revision_id,
            source_binding_revision_id=first.source_binding_revision_id,
            revision_number=1,
            source_binding_sha256=bytes.fromhex(binding.digest),
            created=False,
        )
        assert loaded.definition == definition
        assert loaded.binding == binding
        assert loaded.source_binding_sha256 == first.source_binding_sha256
        assert await session.scalar(select(func.count()).select_from(CustomImportSourceBindingRevision)) == 1


async def _serialized_binding_registrations(
    definition: CustomImportDefinition,
    first_binding: source_binding.SnowflakeSourceBinding,
    second_binding: source_binding.SnowflakeSourceBinding,
):
    """Register two distinct bindings while the first holds the dataset lock."""
    first_locked = asyncio.Event()
    release_first = asyncio.Event()
    second_started = asyncio.Event()
    async with isolated_publication_case() as case:

        async def register_first():
            async with case.sessions() as session, session.begin():
                receipt = await source_binding.register_snowflake_source_binding(
                    session,
                    dataset_key="synthetic_source_binding",
                    definition=definition,
                    binding=first_binding,
                )
                first_locked.set()
                await release_first.wait()
                return receipt

        async def register_second():
            await first_locked.wait()
            async with case.sessions() as session, session.begin():
                second_started.set()
                return await source_binding.register_snowflake_source_binding(
                    session,
                    dataset_key="synthetic_source_binding",
                    definition=definition,
                    binding=second_binding,
                )

        first_task = asyncio.create_task(register_first())
        second_task = asyncio.create_task(register_second())
        try:
            await asyncio.wait_for(second_started.wait(), timeout=2)
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(asyncio.shield(second_task), timeout=0.1)
        except BaseException:
            release_first.set()
            first_task.cancel()
            second_task.cancel()
            await asyncio.gather(first_task, second_task, return_exceptions=True)
            raise
        release_first.set()
        first, second = await asyncio.gather(first_task, second_task)
        async with case.sessions() as session:
            first_loaded = await source_binding.load_snowflake_source_binding(
                session,
                definition_revision_id=first.definition_revision_id,
                source_binding_revision_id=first.source_binding_revision_id,
            )
            second_loaded = await source_binding.load_snowflake_source_binding(
                session,
                definition_revision_id=second.definition_revision_id,
                source_binding_revision_id=second.source_binding_revision_id,
            )
    return first, second, first_loaded, second_loaded


@pytest.mark.asyncio
async def test_source_binding_registration_locks_and_appends_distinct_revisions():
    definition = _definition()
    first_binding = _binding_at_version(definition, "snapshot-20260923")
    second_binding = _binding_at_version(definition, "snapshot-20260924")

    first, second, first_loaded, second_loaded = await _serialized_binding_registrations(
        definition,
        first_binding,
        second_binding,
    )

    assert first.created is True
    assert first.revision_number == 1
    assert second.created is True
    assert second.revision_number == 2
    assert second.source_binding_revision_id != first.source_binding_revision_id
    assert first_loaded.binding == first_binding
    assert second_loaded.binding == second_binding


async def _registered_bundle_candidate(case):
    definition = _definition()
    binding = _binding(definition)
    async with case.sessions() as session, session.begin():
        registration = await source_binding.register_snowflake_source_binding(
            session, dataset_key="synthetic_bound_request", definition=definition, binding=binding
        )
    approved_relations, bundle_bindings = binding.bundle_components(definition)
    connector = SnowflakeBundleAcquisitionConnector(
        approved_relations=approved_relations,
        credential_provider=SimpleNamespace(
            load_key_pair=lambda: pytest.fail("canceled run must not load credentials")
        ),
        adapter=SimpleNamespace(fetch_bundle=lambda *_args: pytest.fail("canceled run must not fetch")),
    )
    request = SnowflakeBundleCandidateRequest(
        dataset_id=registration.dataset_id,
        definition_revision_id=registration.definition_revision_id,
        schema_revision_id=registration.schema_revision_id,
        definition=definition,
        bundle_request=connector.prepare_request(definition, bindings=bundle_bindings),
        idempotency_key="synthetic-bound-run",
        lease_token="synthetic-bound-lease",
        source_binding_revision_id=registration.source_binding_revision_id,
        source_binding_sha256=registration.source_binding_sha256,
    )
    return connector, request


@pytest.mark.asyncio
async def test_bound_candidate_rejects_mismatched_retained_digest_and_bundle_before_reservation():
    async with isolated_publication_case() as case:
        connector, request = await _registered_bundle_candidate(case)
        document = _binding_document(request.definition)
        document["source_object"]["version"] = "snapshot-20260924"
        document["streams"][0]["relation"] = ["synthetic", "public", "other_root_records"]
        other_binding = source_binding.SnowflakeSourceBinding.from_json(json.dumps(document))
        async with case.sessions() as session, session.begin():
            other = await source_binding.register_snowflake_source_binding(
                session,
                dataset_key="synthetic_bound_request",
                definition=request.definition,
                binding=other_binding,
            )

        with pytest.raises(SnowflakeCandidateError, match="source binding identity"):
            await snowflake_candidate.run_snowflake_bundle_candidate(
                case.sessions,
                connector,
                replace(request, source_binding_sha256=other.source_binding_sha256),
            )

        approved_relations, bundle_bindings = other_binding.bundle_components(request.definition)
        other_connector = SnowflakeBundleAcquisitionConnector(
            approved_relations=approved_relations,
            credential_provider=SimpleNamespace(load_key_pair=lambda: pytest.fail("must not load credentials")),
            adapter=SimpleNamespace(fetch_bundle=lambda *_args: pytest.fail("must not fetch")),
        )
        with pytest.raises(SnowflakeCandidateError, match="source binding identity"):
            await snowflake_candidate.run_snowflake_bundle_candidate(
                case.sessions,
                other_connector,
                replace(
                    request,
                    bundle_request=other_connector.prepare_request(request.definition, bindings=bundle_bindings),
                ),
            )

        document = _binding_document(request.definition)
        document["streams"][0]["source_snapshot_token_column_identifier"] = "other_snapshot_token"
        wrong_column_binding = source_binding.SnowflakeSourceBinding.from_json(json.dumps(document))
        approved_relations, same_bundle_bindings = wrong_column_binding.bundle_components(request.definition)
        assert same_bundle_bindings == request.bundle_request.bindings
        wrong_column_connector = SnowflakeBundleAcquisitionConnector(
            approved_relations=approved_relations,
            credential_provider=SimpleNamespace(load_key_pair=lambda: pytest.fail("must not load credentials")),
            adapter=SimpleNamespace(fetch_bundle=lambda *_args: pytest.fail("must not fetch")),
        )
        with pytest.raises(SnowflakeCandidateError, match="source binding identity"):
            await snowflake_candidate.run_snowflake_bundle_candidate(case.sessions, wrong_column_connector, request)

        async with case.sessions() as session:
            assert await session.scalar(select(func.count()).select_from(CustomImportExecution)) == 0


@pytest.mark.asyncio
async def test_bound_request_replay_and_cancellation_keep_exact_binding_identity():
    async with isolated_publication_case() as case:
        connector, request = await _registered_bundle_candidate(case)
        statement = connector.build_statement(request.bundle_request)
        request_digest = snowflake_candidate.bundle_request_identity_sha256(
            request.bundle_request, statement, source_binding_sha256=request.source_binding_sha256
        )
        exact_request_by_field = {
            "dataset_id": request.dataset_id,
            "definition_revision_id": request.definition_revision_id,
            "schema_revision_id": request.schema_revision_id,
            "idempotency_key": request.idempotency_key,
            "mechanism": "local",
            "request_identity_sha256": request_digest,
        }
        async with case.sessions() as session, session.begin():
            reserved = await reserve_execution(
                session, **exact_request_by_field, source_binding_revision_id=request.source_binding_revision_id
            )
            canceled = await request_cancellation(session, execution_id=reserved.execution_id)
        for _ in range(2):
            replay = await snowflake_candidate.run_snowflake_bundle_candidate(case.sessions, connector, request)
            assert replay.status == "not_claimed" and replay.execution_id == canceled.execution_id
        async with case.sessions() as session:
            execution = await session.get(CustomImportExecution, reserved.execution_id)
            assert execution.state == "canceled"
            assert execution.source_binding_revision_id == request.source_binding_revision_id
            assert execution.request_identity_sha256 == request_digest
        with pytest.raises(IdempotencyConflict):
            async with case.sessions() as session, session.begin():
                await lookup_execution_request(session, **exact_request_by_field)
        with pytest.raises(SnowflakeCandidateError, match="source binding identity"):
            await snowflake_candidate.run_snowflake_bundle_candidate(
                case.sessions, connector, replace(request, source_binding_sha256=b"x" * 32)
            )


def test_bundle_identity_preserves_unbound_digest_and_validates_optional_binding_digest():
    definition = _definition()
    binding = _binding(definition)
    approved_relations, bundle_bindings = binding.bundle_components(definition)
    connector = SnowflakeBundleAcquisitionConnector(
        approved_relations=approved_relations,
        credential_provider=SimpleNamespace(load_key_pair=lambda: None),
        adapter=SimpleNamespace(fetch_bundle=lambda *_args: None),
    )
    bundle_request = connector.prepare_request(definition, bindings=bundle_bindings)
    statement = connector.build_statement(bundle_request)
    digest = hashlib.sha256(b"custom-import/snowflake-bundle-request-identity/v1\x00")
    digest.update(bytes.fromhex(bundle_request.request_sha256))
    digest.update(bytes.fromhex(statement.statement_sha256))
    assert snowflake_candidate.bundle_request_identity_sha256(bundle_request, statement) == digest.digest()
    digest.update(bytes.fromhex(binding.digest))
    assert (
        snowflake_candidate.bundle_request_identity_sha256(
            bundle_request, statement, source_binding_sha256=bytes.fromhex(binding.digest)
        )
        == digest.digest()
    )
    for invalid_digest in (b"x" * 31, "x" * 32, True):
        with pytest.raises(SnowflakeCandidateError, match="source binding identity"):
            snowflake_candidate.bundle_request_identity_sha256(
                bundle_request, statement, source_binding_sha256=invalid_digest
            )


@pytest.mark.asyncio
async def test_retained_source_binding_scan_rejects_foreign_or_malformed_revisions():
    definition = _definition()
    binding = _binding(definition)
    row = _loaded_rows(definition, binding)[0]
    registration = SimpleNamespace(dataset_id=11, definition_revision_id=12, schema_revision_id=13)

    def session_with(*rows):
        result = SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: rows))
        return SimpleNamespace(execute=AsyncMock(return_value=result))

    assert await source_binding._locked_source_binding_revisions(session_with(row), registration) == (row,)
    for field, value in (
        ("dataset_id", 99),
        ("definition_revision_id", 99),
        ("schema_revision_id", 99),
        ("revision_number", True),
        ("revision_number", "1"),
        ("revision_number", 0),
        ("revision_number", source_binding.MAX_REVISION_NUMBER + 1),
    ):
        malformed = SimpleNamespace(**{**vars(row), field: value})
        with pytest.raises(source_binding.SnowflakeSourceBindingUnavailableError, match="revision state"):
            await source_binding._locked_source_binding_revisions(session_with(malformed), registration)


def test_source_binding_replay_rejects_ambiguous_or_corrupt_retained_identity():
    definition = _definition()
    binding = _binding(definition)
    row = _loaded_rows(definition, binding)[0]

    assert source_binding._matching_binding_revision((row,), binding) is row
    with pytest.raises(source_binding.SnowflakeSourceBindingUnavailableError, match="ambiguous"):
        source_binding._matching_binding_revision((row, row), binding)
    corrupt_digest = SimpleNamespace(**{**vars(row), "binding_sha256": b"x" * 32})
    with pytest.raises(source_binding.SnowflakeSourceBindingUnavailableError, match="canonical state"):
        source_binding._matching_binding_revision((corrupt_digest,), binding)
    unrelated = SimpleNamespace(binding_sha256=b"x" * 32, canonical_binding="{}")
    assert source_binding._matching_binding_revision((unrelated,), binding) is None

    assert source_binding._next_binding_revision(()) == 1
    assert source_binding._next_binding_revision((row,)) == 2
    exhausted = SimpleNamespace(revision_number=source_binding.MAX_REVISION_NUMBER)
    with pytest.raises(source_binding.SnowflakeSourceBindingUnavailableError, match="limit"):
        source_binding._next_binding_revision((exhausted,))


def test_source_binding_document_rejects_malformed_identity_and_stream_shapes():
    definition = _definition()
    for key, value in (
        ("contract", "unsupported"),
        ("definition_sha256", 1),
        ("role", "invalid role"),
        ("streams", {}),
    ):
        document = _binding_document(definition)
        document[key] = value
        with pytest.raises(source_binding.SnowflakeSourceBindingError):
            source_binding.SnowflakeSourceBinding.from_mapping(document)

    for key, value in (
        ("relation", "synthetic.public.root_records"),
        ("semantic_token_metadata_key", "invalid selector"),
        ("columns", {}),
    ):
        document = _binding_document(definition)
        document["streams"][0][key] = value
        with pytest.raises(source_binding.SnowflakeSourceBindingError):
            source_binding.SnowflakeSourceBinding.from_mapping(document)


def test_source_binding_value_objects_reject_invalid_relationships():
    binding = _binding(_definition())
    stream = binding.streams[0]
    for candidate in (
        lambda: replace(stream.snapshot, relation=object()),
        lambda: replace(stream, relation=object()),
        lambda: replace(stream, snapshot=object()),
        lambda: replace(stream, columns=[]),
        lambda: replace(stream, columns=(stream.columns[0], stream.columns[0])),
        lambda: replace(binding, source_object=object()),
        lambda: replace(binding, streams=[]),
        lambda: replace(binding, streams=(stream, stream)),
    ):
        with pytest.raises(source_binding.SnowflakeSourceBindingError):
            candidate()


def test_source_binding_registration_rejects_incompatible_declarations():
    definition = _definition()
    binding = _binding(definition)
    for invalid_definition, invalid_binding in ((None, binding), (definition, None)):
        with pytest.raises(source_binding.SnowflakeSourceBindingError, match="registration inputs"):
            source_binding._validated_registration_binding(invalid_definition, invalid_binding)
    with pytest.raises(source_binding.SnowflakeSourceBindingError, match="definition is invalid"):
        binding.bundle_components(object())

    for stream_index, key, value in (
        (0, "semantic_token_metadata_key", "npi"),
        (1, "stream_id", "unexpected"),
        (
            0,
            "columns",
            [
                {"field_id": "npi", "column_identifier": "wrong"},
                {"field_id": "score", "column_identifier": "root_score"},
            ],
        ),
    ):
        document = _binding_document(definition)
        document["streams"][stream_index][key] = value
        with pytest.raises(source_binding.SnowflakeSourceBindingError):
            source_binding.SnowflakeSourceBinding.from_mapping(document).bundle_components(definition)


@pytest.mark.asyncio
async def test_source_binding_load_requires_one_exact_row():
    for rows in ((), ((object(), object(), object()),) * 2):
        result = SimpleNamespace(all=lambda: rows)
        session = SimpleNamespace(execute=AsyncMock(return_value=result))
        with pytest.raises(source_binding.SnowflakeSourceBindingUnavailableError, match="unavailable"):
            await source_binding.load_snowflake_source_binding(
                session, definition_revision_id=12, source_binding_revision_id=14
            )


@pytest.mark.asyncio
async def test_source_binding_receipt_requires_exact_readback_identity(monkeypatch):
    definition = _definition()
    binding = _binding(definition)
    binding_row, definition_row, schema_row = _loaded_rows(definition, binding)
    loaded = source_binding._loaded_snowflake_source_binding(binding_row, definition_row, schema_row)
    registration = SimpleNamespace(dataset_id=11, definition_revision_id=12, schema_revision_id=13)
    loader = AsyncMock(return_value=loaded)
    monkeypatch.setattr(source_binding, "load_snowflake_source_binding", loader)
    session = object()

    async def readback():
        return await source_binding._readback_receipt(
            session,
            definition=definition,
            binding=binding,
            definition_registration=registration,
            binding_revision=binding_row,
            created=False,
        )

    receipt = await readback()
    assert receipt.source_binding_revision_id == 14
    assert receipt.source_binding_sha256 == bytes.fromhex(binding.digest)
    assert receipt.created is False
    loader.assert_awaited_with(session, definition_revision_id=12, source_binding_revision_id=14)

    for field, wrong_value in (
        ("dataset_id", 99),
        ("definition_revision_id", 99),
        ("schema_revision_id", 99),
        ("source_binding_revision_id", 99),
        ("source_binding_sha256", b"x" * 32),
        ("definition", object()),
        ("binding", object()),
    ):
        loader.return_value = replace(loaded, **{field: wrong_value})
        with pytest.raises(source_binding.SnowflakeSourceBindingUnavailableError, match="readback"):
            await readback()
