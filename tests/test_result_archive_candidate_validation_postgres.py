# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native PostgreSQL proof for adopted-candidate validation and audit handoff."""

from __future__ import annotations

import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy import MetaData, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from db.connection import db
from db.models._legacy import Base
from process.ptg_candidate_audit import load_candidate_audit_target
from process.ptg_parts import result_archive_candidate_initialization as initialization
from process.ptg_parts import result_archive_candidate_preparation as preparation
from process.ptg_parts import result_archive_candidate_validation as validation
from process.ptg_parts.ptg2_provider_quarantine import provider_identifier_quarantine_payload
from process.ptg_parts.ptg2_source_witness import source_set_digest
from process.ptg_parts.result_archive_adoption import (
    RESULT_ARCHIVE_ADOPTION_CONTRACT,
    PreparedResultArchiveLayout,
)
from tests.test_result_archive_candidate_initialization_postgres import (
    _caller_transaction,
    _NativeFixture,
    native_candidate,
)

_VALIDATION_TABLES = (
    "ptg2_v3_snapshot_layout",
    "ptg2_v4_snapshot_map_root",
    "ptg2_current_source_snapshot",
)
_MAPPING_DIGEST = bytes.fromhex("ab" * 32)
_DESTINATION_SNAPSHOT_KEY = 1701


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "rows", [[], [(None,)], [(" ",)], [("a",), ("b",)], [("a" * 97,)], [("é" * 49,)], [("source\na",)]]
)
async def test_candidate_source_key_rejects_missing_or_ambiguous_scope(rows) -> None:
    """Invalid source scope fails before a lifecycle lock can be selected."""

    session = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(all=Mock(return_value=rows))))
    with pytest.raises(validation.ResultArchiveCandidateValidationError, match="source scope is unavailable"):
        await validation._candidate_source_key(session, schema_name="mrf", snapshot_id="candidate")


async def _install_validation_tables(fixture: _NativeFixture) -> None:
    """Add the local layout and incumbent relations used by this boundary."""

    assert db.engine is not None
    metadata = MetaData(schema=fixture.destination_schema)
    async with db.engine.begin() as connection:
        for table_name in _VALIDATION_TABLES:
            source_table = Base.metadata.tables[f"mrf.{table_name}"]
            target_table = source_table.to_metadata(
                metadata,
                schema=fixture.destination_schema,
            )
            statement = str(
                CreateTable(
                    target_table,
                    include_foreign_key_constraints=[],
                ).compile(dialect=postgresql.dialect())
            )
            await connection.exec_driver_sql(statement)


def _serving_index(fixture: _NativeFixture) -> dict[str, object]:
    frozen_files = fixture.frozen_params["frozen_rate_files"]
    assert isinstance(frozen_files, list)
    raw_digests = tuple(str(file_record["raw_sha256"]) for file_record in frozen_files)
    source_digest = source_set_digest(raw_digests)
    return {
        "arch_version": "postgres_binary_v3",
        "type": "ptg2_shared_blocks_v4",
        "storage_generation": "shared_blocks_v4",
        "provider_scope_strategy": "postgres_packed_graph_v4",
        "shared_block_layout": "packed_snapshot_maps_v4",
        "shared_snapshot_key": _DESTINATION_SNAPSHOT_KEY,
        "source_key": "source_a",
        "snapshot_map": {
            "contract": "ptg_v4_packed_snapshot_map_v1",
            "map_digest": _MAPPING_DIGEST.hex(),
        },
        "source_set": {
            "contract": "sorted_raw_container_sha256_bytes_v1",
            "source_count": len(raw_digests),
            "raw_container_sha256_digest": source_digest,
        },
        "source_witness": {
            "contract": "ptg2_v3_source_witness_payload_v5",
            "source_count": len(raw_digests),
            "source_set_digest": source_digest,
        },
        "audit_sample": {
            "contract": "persisted_served_occurrence_sample_v2",
            "sample_digest": "de" * 32,
        },
        "provider_identifier_quarantine": provider_identifier_quarantine_payload({}),
    }


async def _seed_local_layout(session, fixture: _NativeFixture) -> None:
    """Represent a production layout whose source set lives in the snapshot."""

    schema = '"' + fixture.destination_schema + '"'
    serving_index = _serving_index(fixture)
    serving_index.pop("source_set")
    serving_index.pop("source_key")
    await session.execute(
        text(
            f"""
            INSERT INTO {schema}.ptg2_v3_snapshot_layout
                (snapshot_key, build_token, generation, state,
                 mapping_digest, support_digest, layout_manifest)
            VALUES (:snapshot_key, 'local-layout', 'shared_blocks_v4', 'sealed',
                    :mapping_digest, :support_digest, CAST(:layout_manifest AS jsonb))
            """
        ),
        {
            "snapshot_key": _DESTINATION_SNAPSHOT_KEY,
            "mapping_digest": _MAPPING_DIGEST,
            "support_digest": b"s" * 32,
            "layout_manifest": json.dumps({"serving_index": serving_index}),
        },
    )
    await session.execute(
        text(
            f"""
            INSERT INTO {schema}.ptg2_v4_snapshot_map_root
                (snapshot_key, state, format_version, map_format,
                 representation, projection_id_scope, map_digest,
                 object_kind_count, map_pack_count, coordinate_count,
                 entry_count, completed_at)
            VALUES (:snapshot_key, 'complete', 1,
                    'packed_coordinate_hash_v1', 'direct_v1',
                    'snapshot_local_v1', :mapping_digest, 1, 1, 1, 1, now())
            """
        ),
        {
            "snapshot_key": _DESTINATION_SNAPSHOT_KEY,
            "mapping_digest": _MAPPING_DIGEST,
        },
    )
    await session.execute(
        text(
            f"INSERT INTO {schema}.ptg2_v3_snapshot_binding "
            "(snapshot_id, snapshot_key) VALUES ('local-candidate', :snapshot_key)"
        ),
        {"snapshot_key": _DESTINATION_SNAPSHOT_KEY},
    )


async def _prepare_candidate_and_layout(
    session,
    fixture: _NativeFixture,
) -> tuple[preparation.PreparedResultArchiveCandidate, PreparedResultArchiveLayout]:
    """Run local initialization and evidence preparation in the caller transaction."""

    await initialization.initialize_result_archive_candidate(
        session,
        schema_name=fixture.destination_schema,
        staging_schema_name=fixture.stage_schema,
        source_snapshot_key=71,
        destination_snapshot_id="local-candidate",
        frozen_binding_params=fixture.frozen_params,
        authenticated_source_archive_metadata=fixture.authority,
    )
    prepared_candidate = await preparation.prepare_result_archive_candidate_evidence(
        session,
        schema_name=fixture.destination_schema,
        staging_schema_name=fixture.stage_schema,
        source_snapshot_key=71,
        destination_snapshot_id="local-candidate",
        frozen_binding_params=fixture.frozen_params,
    )
    await _seed_local_layout(session, fixture)
    return prepared_candidate, PreparedResultArchiveLayout(
        RESULT_ARCHIVE_ADOPTION_CONTRACT,
        "local-candidate",
        _DESTINATION_SNAPSHOT_KEY,
        71,
        _MAPPING_DIGEST,
        True,
    )


async def _seed_incumbent(fixture: _NativeFixture) -> None:
    schema = '"' + fixture.destination_schema + '"'
    await db.status(
        f"""
        INSERT INTO {schema}.ptg2_current_source_snapshot
            (source_key, snapshot_id, import_month, updated_at)
        VALUES ('source_a', 'destination-incumbent', DATE '2026-08-01', now())
        """
    )


async def _seed_foreign_attestation(session, fixture: _NativeFixture) -> None:
    schema = '"' + fixture.destination_schema + '"'
    await session.execute(
        text(
            f"""
            INSERT INTO {schema}.ptg2_v3_candidate_audit_attestation
                (snapshot_id, snapshot_key, source_key, plan_id,
                 plan_market_type, coverage_scope_id, source_set_digest,
                 audit_sample_digest, contract, tool_name, tool_version,
                 report_digest, report, activation_intent,
                 attestation_digest, attested_at, expires_at)
            VALUES ('local-candidate', :snapshot_key, 'source_a', 'plan-a',
                    'market-a', :coverage_scope_id, :source_set_digest,
                    :audit_sample_digest, 'ptg2_candidate_audit_attestation_v4',
                    'foreign-auditor', '1', :report_digest, '{{}}'::jsonb,
                    'audit_only', :attestation_digest, now(), now() + interval '1 hour')
            """
        ),
        {
            "snapshot_key": _DESTINATION_SNAPSHOT_KEY,
            "coverage_scope_id": b"c" * 32,
            "source_set_digest": b"1" * 32,
            "audit_sample_digest": b"2" * 32,
            "report_digest": b"3" * 32,
            "attestation_digest": b"4" * 32,
        },
    )


def _assert_audit_handoff(handoff, replay, committed_audit_target) -> None:
    assert replay == handoff
    assert committed_audit_target.snapshot_id == handoff.destination_snapshot_id
    assert committed_audit_target.snapshot_key == handoff.destination_snapshot_key
    assert committed_audit_target.expected_current_snapshot_id == "destination-incumbent"
    assert handoff.status == "audit_required"
    assert handoff.expected_current_snapshot_id == "destination-incumbent"
    assert handoff.next_importer == "ptg-candidate-audit"
    assert handoff.next_parameters == {
        "candidate_run_id": "ptg2:local-filing",
        "snapshot_id": "local-candidate",
        "candidate_audit_mode": "audit_only",
    }


async def _assert_persisted_candidate(fixture: _NativeFixture) -> None:
    schema = '"' + fixture.destination_schema + '"'
    snapshot = await db.first(
        f"SELECT status, previous_snapshot_id, manifest FROM {schema}.ptg2_snapshot "
        "WHERE snapshot_id = 'local-candidate'"
    )
    assert snapshot is not None
    snapshot_by_name = dict(snapshot._mapping)
    assert snapshot_by_name["status"] == "validated"
    assert snapshot_by_name["previous_snapshot_id"] == "destination-incumbent"
    assert snapshot_by_name["manifest"]["serving_index"] == _serving_index(fixture)
    assert snapshot_by_name["manifest"]["activation"] == {
        "contract": "ptg2_candidate_activation_v1",
        "state": "validated",
        "source_key": "source_a",
        "expected_previous_snapshot_id": "destination-incumbent",
    }
    local_run = await db.first(
        f"SELECT status, report FROM {schema}.ptg2_import_run WHERE import_run_id = 'ptg2:local-filing'"
    )
    assert local_run is not None
    assert local_run[0] == "validated"
    assert local_run[1] == snapshot_by_name["manifest"]
    assert await db.scalar(f"SELECT COUNT(*) FROM {schema}.ptg2_v3_candidate_audit_attestation") == 0
    assert (
        await db.scalar(
            f"SELECT COUNT(*) FROM {schema}.ptg2_current_source_snapshot "
            "WHERE source_key = 'source_a' AND snapshot_id = 'destination-incumbent'"
        )
        == 1
    )


@pytest.mark.asyncio
async def test_validation_requires_caller_transaction_before_database_access() -> None:
    with pytest.raises(
        validation.ResultArchiveCandidateValidationError,
        match="requires an already-open caller transaction",
    ):
        await validation.validate_result_archive_candidate_for_audit(
            object(),
            schema_name="candidate_destination",
            prepared_candidate=object(),
            prepared_layout=object(),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("activation_source_key", ["source_a", " Source-A "])
async def test_native_validation_stages_local_manifest_and_replays(native_candidate, activation_source_key) -> None:
    """Retain the incumbent while producing a replayable post-commit audit request."""

    fixture = native_candidate
    await _install_validation_tables(fixture)
    await _seed_incumbent(fixture)
    async with _caller_transaction() as session:
        prepared_candidate, prepared_layout = await _prepare_candidate_and_layout(
            session,
            fixture,
        )
        await session.execute(
            text(
                f'UPDATE "{fixture.destination_schema}".ptg2_snapshot '
                "SET manifest = jsonb_set(manifest::jsonb, '{activation,source_key}', CAST(:source_key AS jsonb)) "
                "WHERE snapshot_id = 'local-candidate'"
            ),
            {"source_key": json.dumps(activation_source_key)},
        )
        handoff = await validation.validate_result_archive_candidate_for_audit(
            session,
            schema_name=fixture.destination_schema,
            prepared_candidate=prepared_candidate,
            prepared_layout=prepared_layout,
        )
    async with _caller_transaction() as session:
        replay = await validation.validate_result_archive_candidate_for_audit(
            session,
            schema_name=fixture.destination_schema,
            prepared_candidate=prepared_candidate,
            prepared_layout=prepared_layout,
        )
    committed_audit_target = await load_candidate_audit_target(
        candidate_run_id="ptg2:local-filing",
        snapshot_id="local-candidate",
    )

    _assert_audit_handoff(handoff, replay, committed_audit_target)
    await _assert_persisted_candidate(fixture)


@pytest.mark.asyncio
async def test_native_validation_wrong_layout_rolls_back_candidate(native_candidate) -> None:
    fixture = native_candidate
    await _install_validation_tables(fixture)
    with pytest.raises(
        validation.ResultArchiveCandidateValidationError,
        match="layout differs from its preparation receipt",
    ):
        async with _caller_transaction() as session:
            prepared_candidate, prepared_layout = await _prepare_candidate_and_layout(
                session,
                fixture,
            )
            await validation.validate_result_archive_candidate_for_audit(
                session,
                schema_name=fixture.destination_schema,
                prepared_candidate=prepared_candidate,
                prepared_layout=replace(prepared_layout, mapping_digest=b"z" * 32),
            )
    schema = '"' + fixture.destination_schema + '"'
    assert await db.scalar(f"SELECT COUNT(*) FROM {schema}.ptg2_snapshot") == 0
    assert await db.scalar(f"SELECT COUNT(*) FROM {schema}.ptg2_v3_snapshot_layout") == 0


@pytest.mark.asyncio
async def test_native_validation_rejects_conflicting_sealed_source_set(native_candidate) -> None:
    """A layout cannot override the source set reconstructed from copied rows."""

    fixture = native_candidate
    await _install_validation_tables(fixture)
    with pytest.raises(
        validation.ResultArchiveCandidateValidationError,
        match="sealed source set differs from local evidence",
    ):
        async with _caller_transaction() as session:
            prepared_candidate, prepared_layout = await _prepare_candidate_and_layout(
                session,
                fixture,
            )
            schema = '"' + fixture.destination_schema + '"'
            conflicting_source_set_by_field = {
                "contract": "sorted_raw_container_sha256_bytes_v1",
                "source_count": 2,
                "raw_container_sha256_digest": "00" * 32,
            }
            await session.execute(
                text(
                    f"""
                    UPDATE {schema}.ptg2_v3_snapshot_layout
                       SET layout_manifest = jsonb_set(
                               layout_manifest,
                               '{{serving_index,source_set}}',
                               CAST(:source_set AS jsonb)
                           )
                     WHERE snapshot_key = :snapshot_key
                    """
                ),
                {
                    "snapshot_key": _DESTINATION_SNAPSHOT_KEY,
                    "source_set": json.dumps(conflicting_source_set_by_field),
                },
            )
            await validation.validate_result_archive_candidate_for_audit(
                session,
                schema_name=fixture.destination_schema,
                prepared_candidate=prepared_candidate,
                prepared_layout=prepared_layout,
            )
    schema = '"' + fixture.destination_schema + '"'
    assert await db.scalar(f"SELECT COUNT(*) FROM {schema}.ptg2_snapshot") == 0
    assert await db.scalar(f"SELECT COUNT(*) FROM {schema}.ptg2_v3_snapshot_layout") == 0


@pytest.mark.asyncio
async def test_native_validation_rejects_conflicting_sealed_source_key(native_candidate) -> None:
    """A layout cannot override the destination's locked logical source scope."""

    fixture = native_candidate
    await _install_validation_tables(fixture)
    with pytest.raises(
        validation.ResultArchiveCandidateValidationError,
        match="sealed source key differs from local scope",
    ):
        async with _caller_transaction() as session:
            prepared_candidate, prepared_layout = await _prepare_candidate_and_layout(
                session,
                fixture,
            )
            schema = '"' + fixture.destination_schema + '"'
            await session.execute(
                text(
                    f"""
                    UPDATE {schema}.ptg2_v3_snapshot_layout
                       SET layout_manifest = jsonb_set(
                               layout_manifest,
                               '{{serving_index,source_key}}',
                               '"wrong_source"'::jsonb
                           )
                     WHERE snapshot_key = :snapshot_key
                    """
                ),
                {"snapshot_key": _DESTINATION_SNAPSHOT_KEY},
            )
            await validation.validate_result_archive_candidate_for_audit(
                session,
                schema_name=fixture.destination_schema,
                prepared_candidate=prepared_candidate,
                prepared_layout=prepared_layout,
            )
    schema = '"' + fixture.destination_schema + '"'
    assert await db.scalar(f"SELECT COUNT(*) FROM {schema}.ptg2_snapshot") == 0
    assert await db.scalar(f"SELECT COUNT(*) FROM {schema}.ptg2_v3_snapshot_layout") == 0


@pytest.mark.asyncio
async def test_native_validation_rejects_nonlocal_attestation_and_rolls_back(native_candidate) -> None:
    fixture = native_candidate
    await _install_validation_tables(fixture)
    with pytest.raises(
        validation.ResultArchiveCandidateValidationError,
        match="requires a fresh destination attestation",
    ):
        async with _caller_transaction() as session:
            prepared_candidate, prepared_layout = await _prepare_candidate_and_layout(
                session,
                fixture,
            )
            await _seed_foreign_attestation(session, fixture)
            await validation.validate_result_archive_candidate_for_audit(
                session,
                schema_name=fixture.destination_schema,
                prepared_candidate=prepared_candidate,
                prepared_layout=prepared_layout,
            )
    schema = '"' + fixture.destination_schema + '"'
    assert await db.scalar(f"SELECT COUNT(*) FROM {schema}.ptg2_snapshot") == 0
    assert await db.scalar(f"SELECT COUNT(*) FROM {schema}.ptg2_v3_candidate_audit_attestation") == 0
