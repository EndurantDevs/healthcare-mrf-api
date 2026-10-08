# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Candidate controls refuse changed custody, metadata and publication catalogs."""

import json
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest
import sqlalchemy as sa

from db import migration_ptg2_v4_attempt_audit as attempt_audit
from db import models
from db.migration_adoption import _expected_foreign_key_identity
from db.migration_expression_adoption import _normalized_expression
from db.migration_ptg2_legacy_v3_guard_sql import common_attempt_guard_sql
from db.migration_ptg2_v4_attempt_fence import _LIFECYCLE_FUNCTION_BODY
from process.ptg_parts import ptg2_physical_binding as native
from process.ptg_parts import result_archive_candidate_initialization as initialization
from process.ptg_parts import result_archive_candidate_validation as validation
from process.ptg_parts.frozen_rate_files import FrozenRateFileMismatchError
from process.ptg_parts.result_archive_published_authority import _authority_from_identity
from process.ptg_parts.result_archive_published_identity import validate_published_result_identity
from process.ptg_parts.result_archive_source_authority import PtgResultArchiveSourceAuthority
from tests.ptg2_v4_attempt_migration_postgres_support import FENCE_MIGRATION, migration
from tests.test_ptg2_local_physical_read_view import _view_fixture
from tests.test_ptg2_local_preparation_authority import (
    _control_fixture,
    _ownership,
    _published_control_fixture,
)
from tests.test_ptg2_physical_binding import _binding, _serving_scope
from tests.test_result_archive_candidate_preparation_postgres import _candidate_manifest, _frozen_params
from tests.test_result_archive_published_identity import _published_row


def _result(rows=(), scalar=None):
    result = MagicMock()
    result.__iter__.side_effect = lambda: iter(rows)
    result.mappings.return_value = result
    result.all.return_value = rows
    result.first.return_value = rows[0] if rows else None
    result.one_or_none.return_value = rows[0] if rows else None
    result.scalar_one.return_value = scalar
    result.scalar_one_or_none.return_value = scalar
    return result


def _session(*rows, scalars=()):
    return SimpleNamespace(
        in_transaction=lambda: True,
        execute=AsyncMock(side_effect=[_result(value) for value in rows]),
        scalar=AsyncMock(side_effect=scalars),
    )


def _source_fixture():
    row = _published_row()
    receipt = _authority_from_identity("source-operation", validate_published_result_identity(row)).as_dict()
    scope = _serving_scope()
    scope.update(
        snapshot_id=row["snapshot_id"],
        source_key=row["run_source_key"],
        coverage_scope_id=row["coverage_scope_id"].hex(),
        primary_plan=[row["plan_id"], row["plan_market_type"]],
        plan_scopes=row["plan_scopes"],
    )
    scope["source_assignments"][0]["raw_container_sha256"] = (b"r" * 32).hex()
    metadata = {
        "source_snapshot_id": row["snapshot_id"],
        "source_snapshot_key": row["snapshot_key"],
        "source_publication": receipt,
        "closure_metadata": {"serving_scope": scope, "invalid_price_exclusion_policy": None},
    }
    return row, metadata


def _layout():
    return {
        "generation": "shared_blocks_v4",
        "mapping_digest": b"m" * 32,
        "support_digest": b"s" * 32,
        "layout_manifest": {"serving_index": {"shared_snapshot_key": 19}},
        "logical_byte_count": 0,
        "storage_shard_id": 2,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", [None, "snapshot", "source", "manifest"])
async def test_local_source_rejoins_receipt_and_keeps_portable_rows(drift):
    row, metadata = _source_fixture()
    if drift == "snapshot":
        metadata["source_snapshot_id"] = "other-snapshot"
    elif drift == "source":
        metadata["closure_metadata"]["serving_scope"]["source_key"] = "other_source"
    elif drift == "manifest":
        row["manifest"]["changed"] = True
    sources = [{"source_key": 0, "raw_container_sha256": (b"r" * 32).hex()}]
    session = _session([{"manifest": row["manifest"]}], sources)
    if drift:
        with pytest.raises(initialization.ResultArchiveCandidateInitializationError, match="source manifest differs"):
            await initialization._local_data_candidate_source(session, _ownership(_binding()), metadata)
        assert session.execute.await_count == 1
    else:
        staged, receipt = await initialization._local_data_candidate_source(session, _ownership(_binding()), metadata)
        assert staged.source_records == tuple(sources)
        assert staged.plan_scopes == ((row["plan_id"], row["plan_market_type"]),)
        assert receipt == metadata["source_publication"]
        assert "FOR KEY SHARE" in str(session.execute.await_args_list[0].args[0])


@pytest.mark.asyncio
async def test_policy_capture_reads_only_locked_source_data():
    session = _session([{"manifest": {}, "policy": None}])
    assert (
        await initialization.capture_local_candidate_policy(
            session,
            schema_name="candidate",
            snapshot_id="source-snapshot",
            source_assignments=[{"raw_container_sha256": "a" * 64}],
        )
        is None
    )
    query = str(session.execute.await_args.args[0])
    assert "FOR KEY SHARE OF snapshot, run" in query
    assert "run.options->'invalid_price_exclusion_policy'" in query


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("generation", "shared_blocks_v3"),
        ("mapping_digest", None),
        ("support_digest", b"short"),
        ("layout_manifest", []),
        ("logical_byte_count", True),
        ("logical_byte_count", -1),
    ],
)
async def test_layout_metadata_refuses_invalid_source_before_insert(field, value):
    source = _layout()
    source[field] = value
    session = _session([source])
    with pytest.raises(
        initialization.ResultArchiveCandidateInitializationError, match="payload layout metadata differs"
    ):
        await initialization._local_metadata_layout(session, "mrf", "candidate", 19)
    assert session.execute.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", [None, "metadata", True, 0, 2**63])
async def test_layout_metadata_reuse_requires_exact_complete_postimage(drift):
    source = _layout()
    destination_by_field = {**deepcopy(source), "snapshot_key": 701}
    if drift == "metadata":
        destination_by_field["storage_shard_id"] += 1
    elif drift is not None:
        destination_by_field["snapshot_key"] = drift
    session = _session([source], [], [destination_by_field])
    if drift is not None:
        with pytest.raises(
            initialization.ResultArchiveCandidateInitializationError, match="metadata differs|key is invalid"
        ):
            await initialization._local_metadata_layout(session, "mrf", "candidate", 19)
    else:
        assert await initialization._local_metadata_layout(session, "mrf", "candidate", 19) == 701
    insert = str(session.execute.await_args_list[1].args[0])
    assert "ON CONFLICT (generation,mapping_digest,support_digest)" in insert
    assert "WHERE state='sealed'" in insert and "DO NOTHING" in insert


@pytest.mark.asyncio
@pytest.mark.parametrize("observed", [701, 702])
async def test_metadata_binding_never_replaces_another_layout(observed):
    session = _session([], [{"snapshot_key": observed}])
    if observed == 701:
        await initialization._bind_local_metadata_layout(session, "mrf", "candidate-snapshot", 701)
    else:
        with pytest.raises(initialization.ResultArchiveCandidateInitializationError, match="binding differs"):
            await initialization._bind_local_metadata_layout(session, "mrf", "candidate-snapshot", 701)
    assert "ON CONFLICT (snapshot_id) DO NOTHING" in str(session.execute.await_args_list[0].args[0])


def _initialization_session(staged, *, reused=False, incomplete=False):
    persisted_by_kind = {}

    async def execute_candidate_statement(statement, parameters=None):
        query = str(statement)
        if query.startswith("SELECT pg_advisory"):
            return _result()
        if "INSERT INTO" in query and ".ptg2_import_run" in query:
            persisted_by_kind["run"] = {**parameters, "options": json.loads(parameters["options"])}
            return _result([] if reused else [{"import_run_id": parameters["import_run_id"]}])
        if "INSERT INTO" in query and ".ptg2_snapshot" in query:
            persisted_by_kind["snapshot"] = {**parameters, "manifest": json.loads(parameters["manifest"])}
            return _result([] if reused or incomplete else [{"snapshot_id": parameters["snapshot_id"]}])
        if "SELECT import_run_id, import_month" in query:
            return _result([{**persisted_by_kind["run"], "status": "running", "started_at": 1, "heartbeat_at": 1}])
        if "SELECT snapshot_id, import_run_id, import_month" in query:
            return _result([{**persisted_by_kind["snapshot"], "status": "building", "created_at": 1}])
        if "INSERT INTO" in query:
            return _result()
        if "SELECT plan_id, plan_market_type, coverage_scope_id" in query:
            return _result(
                [
                    {
                        "plan_id": staged.primary_plan_id,
                        "plan_market_type": staged.primary_plan_market_type,
                        "coverage_scope_id": staged.coverage_scope_id,
                    }
                ]
            )
        if "SELECT plan_id, plan_market_type" in query:
            return _result([{"plan_id": plan, "plan_market_type": market} for plan, market in staged.plan_scopes])
        if ".ptg2_v3_candidate_audit_attestation" in query:
            return _result()
        if "WHERE import_run_id = :import_run_id" in query:
            return _result([{"snapshot_id": persisted_by_kind["snapshot"]["snapshot_id"]}])
        raise AssertionError("unexpected candidate SQL")

    return SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock(side_effect=execute_candidate_statement))


@pytest.mark.asyncio
@pytest.mark.parametrize("reused,incomplete", [(False, False), (True, False), (False, True)])
async def test_local_control_creation_and_retry_leave_payload_and_source_graph_untouched(reused, incomplete):
    row, metadata = _source_fixture()
    staged = initialization._AuthenticatedStagedCandidate(
        row["snapshot_id"],
        row["manifest"],
        row["plan_id"],
        row["plan_market_type"],
        row["coverage_scope_id"],
        tuple(tuple(plan) for plan in row["plan_scopes"]),
        (),
    )
    session = _initialization_session(staged, reused=reused, incomplete=incomplete)
    manifest, options = initialization._published_candidate_metadata(
        staged, metadata["source_publication"], "local-snapshot", "source_a", "local-run"
    )
    if incomplete:
        with pytest.raises(initialization.ResultArchiveCandidateInitializationError, match="attempt is incomplete"):
            await initialization._persist_local_data_controls(
                session, "mrf", "local-snapshot", "local-run", manifest, options, staged, None
            )
    else:
        assert await initialization._persist_local_data_controls(
            session, "mrf", "local-snapshot", "local-run", manifest, options, staged, None
        ) is (not reused)
    assert all("ptg2_source_" not in str(call.args[0]) for call in session.execute.await_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", [None, "schema", "source-schema", "snapshot", "source"])
async def test_local_initializer_requires_new_reviewed_coordinates(monkeypatch, drift):
    source_by_field, metadata = _source_fixture()
    ownership = _ownership(_binding())
    schema, snapshot, source_key = "mrf", "local-snapshot", "source_a"
    if drift == "schema":
        schema = "other"
    elif drift == "source-schema":
        ownership = replace(ownership, schema_name=schema)
    elif drift == "snapshot":
        snapshot = metadata["source_snapshot_id"]
    elif drift == "source":
        source_key = "other_source"
    family = AsyncMock()
    monkeypatch.setattr(native, "verify_local_data_family", family)
    staged = initialization._AuthenticatedStagedCandidate(
        source_by_field["snapshot_id"],
        source_by_field["manifest"],
        source_by_field["plan_id"],
        source_by_field["plan_market_type"],
        source_by_field["coverage_scope_id"],
        tuple(tuple(plan) for plan in source_by_field["plan_scopes"]),
        (),
    )
    source_lookup = AsyncMock(return_value=(staged, metadata["source_publication"]))
    monkeypatch.setattr(initialization, "_local_data_candidate_source", source_lookup)
    layout = AsyncMock(return_value=701)
    monkeypatch.setattr(initialization, "_local_metadata_layout", layout)
    session = _initialization_session(staged)
    binding = AsyncMock()
    monkeypatch.setattr(initialization, "_bind_local_metadata_layout", binding)
    if drift:
        with pytest.raises(
            initialization.ResultArchiveCandidateInitializationError, match="coordinates differ|source differs"
        ):
            await initialization.initialize_local_data_candidate(
                session,
                schema_name=schema,
                ownership=ownership,
                metadata=metadata,
                destination_snapshot_id=snapshot,
                reviewed_source_key=source_key,
            )
        session.execute.assert_not_awaited()
        layout.assert_not_awaited()
    else:
        initialized, digest = await initialization.initialize_local_data_candidate(
            session,
            schema_name=schema,
            ownership=ownership,
            metadata=metadata,
            destination_snapshot_id=snapshot,
            reviewed_source_key=source_key,
        )
        assert initialized.destination_layout_key == 701 and initialized.frozen_binding_sha256 is None
        assert initialized.source_snapshot_id == source_by_field["snapshot_id"] and not initialized.reused
        assert len(digest) == 64
        binding.assert_awaited_once_with(session, schema, snapshot, 701)
    assert family.await_count == int(drift not in {"schema", "source-schema", "snapshot"})


def _cleanup_fixture(*, staged=False):
    physical_binding, candidate, _scope, initialized = _control_fixture()
    snapshot = "snapshot-archive-11111111-2222-4333-8444-555555555555"
    physical_binding = replace(physical_binding, snapshot_id=snapshot)
    initialized = replace(initialized, destination_snapshot_id=snapshot)
    candidate["snapshot_id"] = snapshot
    if staged:
        attributes = validation._candidate_attributes(
            candidate,
            source_key="source_a",
            serving_index=candidate["layout_manifest"]["serving_index"],
            exact_published=True,
        )
        candidate.update(attributes, run_status="validated", run_report=attributes["manifest"])
    evidence_by_field = {
        "ownership": {"dataset_id": str(physical_binding.dataset_id)},
        "data": {
            "payload_snapshot_id": physical_binding.payload_snapshot_id,
            "payload_snapshot_key": physical_binding.payload_snapshot_key,
        },
        "initialization": asdict(initialized),
        "control_sha256": initialization._local_control_sha256(
            candidate["manifest"], candidate["options"], (("12-3456789", "group"),)
        ),
    }
    if staged:
        evidence_by_field["activation_evidence"] = {"control_sha256": evidence_by_field["control_sha256"]}
    return physical_binding, candidate, evidence_by_field


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "staged,drift",
    [
        (staged, drift)
        for staged in (False, True)
        for drift in (None, "duplicates", "status", "run", "source", "marker", "digest")
    ]
    + [(True, "report"), (True, "activation")],
)
async def test_cleanup_requires_the_exact_building_or_staged_control_preimage(staged, drift):
    _physical, candidate, evidence = _cleanup_fixture(staged=staged)
    changes_by_drift = {
        "status": (candidate, "status", "published"),
        "run": (candidate, "import_run_id", "other-run"),
        "source": (candidate["options"], "source_key", "other_source"),
        "marker": (candidate["manifest"]["local_data_preparation"], "payload_snapshot_key", 20),
        "digest": (evidence, "control_sha256", "0" * 64),
        "report": (candidate, "run_report", {}),
        "activation": (evidence.get("activation_evidence", {}), "control_sha256", "0" * 64),
    }
    if drift in changes_by_drift:
        target, field, value = changes_by_drift[drift]
        target[field] = value
    controls = [candidate] * (2 if drift == "duplicates" else 1)
    session = _session([{"plan_id": "12-3456789", "plan_market_type": "group"}])
    if drift:
        with pytest.raises(initialization.ResultArchiveCandidateInitializationError, match="controls differ"):
            await initialization._require_local_cleanup_controls(
                session, evidence, evidence["initialization"], controls, "source_a", "mrf"
            )
    else:
        assert (
            await initialization._require_local_cleanup_controls(
                session, evidence, evidence["initialization"], controls, "source_a", "mrf"
            )
            == 701
        )


@pytest.mark.parametrize("layout_key", [None, 1, 2**63 - 1, True, 0, -1, 2**63])
def test_cleanup_marker_preserves_payload_and_only_accepts_native_metadata_keys(layout_key):
    _physical, _candidate, evidence = _cleanup_fixture()
    evidence["initialization"]["destination_layout_key"] = layout_key
    if layout_key is not None and (type(layout_key) is not int or not 0 < layout_key < 2**63):
        with pytest.raises(initialization.ResultArchiveCandidateInitializationError, match="layout key differs"):
            initialization._local_cleanup_marker(evidence, evidence["initialization"])
    else:
        marker, observed = initialization._local_cleanup_marker(evidence, evidence["initialization"])
        assert observed == layout_key and marker["payload_snapshot_key"] == 19
        assert ("destination_layout_key" in marker) is (layout_key is not None)


@pytest.mark.asyncio
@pytest.mark.parametrize("refusal", [None, "absent", "partial", "destination", "references", "binding"])
async def test_called_cleanup_refuses_partial_referenced_or_foreign_controls(monkeypatch, refusal):
    physical, candidate, evidence = _cleanup_fixture()
    snapshot = physical.snapshot_id
    if refusal == "destination":
        evidence["initialization"]["destination_snapshot_id"] = "other-snapshot"
    controls = [] if refusal in {"absent", "partial"} else [candidate]
    query_rows = [[], controls]
    if controls:
        query_rows.extend(
            [
                [{"plan_id": "12-3456789", "plan_market_type": "group"}],
                [{"snapshot_id": snapshot}],
                [],
                [{"snapshot_key": 702 if refusal == "binding" else 701}],
                [],
                [],
                [],
                [],
                [],
            ]
        )
    scalars = [refusal == "partial"] if not controls else [refusal == "references", False, False, False]
    session = _session(*query_rows, scalars=scalars)
    lifecycle = AsyncMock()
    monkeypatch.setattr("process.ptg_parts.ptg2_lifecycle_lock.acquire_ptg2_source_lifecycle_lock", lifecycle)
    if refusal in {"partial", "destination", "references", "binding"}:
        with pytest.raises(initialization.ResultArchiveCandidateInitializationError):
            await initialization.cleanup_local_data_candidate(
                session, evidence, operation_id=snapshot.removeprefix("snapshot-archive-"), source_key="source_a"
            )
    else:
        await initialization.cleanup_local_data_candidate(
            session, evidence, operation_id=snapshot.removeprefix("snapshot-archive-"), source_key="source_a"
        )
    deletes = [str(call.args[0]) for call in session.execute.await_args_list if str(call.args[0]).startswith("DELETE")]
    if refusal:
        assert deletes == []
    else:
        assert len(deletes) == 5
        assert all("snapshot_id=:snapshot_id" in query or "import_run_id=:run_id" in query for query in deletes)
        assert not any("DROP" in query or "ptg2_source_identity" in query for query in deletes)


@pytest.mark.asyncio
@pytest.mark.parametrize("present", [False, True])
async def test_legacy_cleanup_has_no_metadata_binding_to_delete(present):
    session = _session([{"snapshot_key": 701}] if present else [])
    if present:
        with pytest.raises(initialization.ResultArchiveCandidateInitializationError, match="metadata binding differs"):
            await initialization._delete_local_metadata_binding(session, '"mrf"', "local-snapshot", None)
    else:
        await initialization._delete_local_metadata_binding(session, '"mrf"', "local-snapshot", None)
    assert session.execute.await_count == 1


@pytest.mark.parametrize(
    "safe,definition", [(True, "CHECK (value > 0)"), (False, "CHECK (value > 0)"), (True, "CHECK (value >= 0)")]
)
def test_publication_checks_refuse_executable_or_semantic_drift(safe, definition):
    rows = [
        {"expected": True, "conname": "value_check", "definition": "CHECK (value > 0)", "safe": True},
        {"expected": False, "conname": "value_check", "definition": definition, "safe": safe},
    ]
    connection = SimpleNamespace(execute=Mock(return_value=_result(rows)))
    if safe and definition == rows[0]["definition"]:
        validation._require_local_publication_checks(
            connection, "mrf", "candidate", "temporary", _normalized_expression
        )
    else:
        with pytest.raises(ValueError, match="control checks differ"):
            validation._require_local_publication_checks(
                connection, "mrf", "candidate", "temporary", _normalized_expression
            )
    assert "pg_depend" in str(connection.execute.call_args.args[0])


@pytest.mark.parametrize("layout", [False, True])
@pytest.mark.parametrize("proof", [True, False, None])
def test_publication_indexes_require_positive_native_catalog_proof(layout, proof):
    table = models.PTG2V3SnapshotLayout.__table__ if layout else models.PTG2Snapshot.__table__
    results = [_result(), _result(scalar="state = 'sealed'")] if layout else []
    connection = SimpleNamespace(execute=Mock(side_effect=[*results, _result(scalar=proof)]))
    if proof is True:
        validation._require_local_publication_indexes(connection, "mrf", table, "temporary")
    else:
        with pytest.raises(ValueError, match="native keys or semantic tuple differ"):
            validation._require_local_publication_indexes(connection, "mrf", table, "temporary")
    parameters = connection.execute.call_args.args[1]
    assert parameters["predicate"] == ("state = 'sealed'" if layout else None)
    query = str(connection.execute.call_args.args[0])
    assert "NOT i.indisvalid OR NOT i.indisready" in query and "op.opcnamespace" in query


def _attempt_functions():
    guard = (
        common_attempt_guard_sql(
            guard='"mrf"."guard_ptg2_v4_attempt"',
            legacy_audit='"mrf"."ptg2_legacy_v3_metadata_reconcile_audit"',
            snapshot='"mrf"."ptg2_snapshot"',
            internal_run='"mrf"."ptg2_import_run"',
            fence='"mrf"."ptg2_v4_attempt_fence"',
        )
        .split("AS $$", 1)[1]
        .rsplit("$$", 1)[0]
    )
    return [
        {
            "proname": name,
            "prosrc": body,
            "pronargs": arguments,
            "argument_types": types,
            "result_type": result,
            "lanname": "plpgsql",
            "safe": True,
        }
        for name, body, arguments, types, result in (
            ("lock_ptg2_v4_attempt_lifecycle", _LIFECYCLE_FUNCTION_BODY, 0, "", "trigger"),
            ("guard_ptg2_v4_attempt", guard, 3, "25 25 16", "void"),
        )
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "drift", [None, "missing", "safe", "lanname", "pronargs", "argument_types", "result_type", "prosrc"]
)
async def test_attempt_function_catalog_authenticates_full_native_signature(drift):
    functions = _attempt_functions()
    if drift == "missing":
        functions.pop()
    elif drift:
        functions[1][drift] = False if drift == "safe" else "different"
    session = _session(functions)
    excluded = (models.PTG2V3SnapshotLayout, models.PTG2V4AttemptFence, models.PTG2V4AttemptStage)
    if drift:
        with pytest.raises(ValueError, match="coordinate guard differs"):
            await validation._require_local_publication_attempt_guards(session, "mrf", excluded)
    else:
        await validation._require_local_publication_attempt_guards(session, "mrf", excluded)
    assert session.execute.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", [None, "unavailable", "status"])
async def test_publication_receipt_rechecks_actual_postimage(monkeypatch, drift):
    physical, candidate, evidence, expected = _published_control_fixture()
    if drift == "status":
        candidate["status"] = "validated"
    monkeypatch.setattr(
        native, "_local_candidate_control", AsyncMock(return_value=None if drift == "unavailable" else candidate)
    )
    session = _session([{"plan_id": "12-3456789", "plan_market_type": "group"}])
    if drift:
        with pytest.raises(native.PTG2PhysicalBindingError):
            await validation.local_data_publication_receipt(session, evidence, physical)
    else:
        assert await validation.local_data_publication_receipt(session, evidence, physical) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [0, 1, 256, 257])
async def test_serving_source_dictionary_is_bounded_and_reports_complete_key_statistics(count):
    physical = _binding()
    rows = [{"source_key": index} for index in range(count)]
    session = _session(rows)
    if count in {0, 257}:
        with pytest.raises(native.PTG2PhysicalBindingError, match="source dictionary differs"):
            await validation._local_serving_source_identity(session, physical)
    else:
        result = await validation._local_serving_source_identity(session, physical)
        assert result["source_identity_rows"] == rows
        assert result["source_row_count"] == result["distinct_source_key_count"] == count
        assert result["minimum_source_key"] == 0 and result["maximum_source_key"] == count - 1
    assert "ORDER BY source_key LIMIT 257" in str(session.execute.await_args.args[0])


def _frozen_source():
    _parameters, source_parameters, descriptors = _frozen_params()
    manifest = _candidate_manifest(frozen_params=source_parameters, descriptors=descriptors)
    receipt = PtgResultArchiveSourceAuthority(
        "source-operation",
        "source-snapshot",
        source_parameters["source_file_import_id"],
        "source_a",
        initialization.result_archive_manifest_sha256(manifest),
        initialization.frozen_rate_binding_sha256(manifest["frozen_rate_file_binding"]),
    ).as_dict()
    source_records = tuple(
        {
            "source_key": index,
            "source_file_version_count": 1,
            "source_file_version_id": descriptor["engine_source_file_version_id"],
            "raw_container_sha256": descriptor["raw_sha256"],
            "version_source_identity_hash": descriptor["engine_source_identity_hash"],
            **{
                "version_" + field: descriptor[field]
                for field in (
                    "source_type",
                    "canonical_url",
                    "raw_sha256",
                    "logical_sha256",
                    "content_length",
                    "etag",
                    "last_modified",
                )
            },
            "version_verification_mode": "downloaded",
            "version_payload": {"raw_byte_count": descriptor["content_length"], "logical_hash_deferred": False},
        }
        for index, descriptor in enumerate(descriptors)
    )
    staged = initialization._AuthenticatedStagedCandidate(
        "source-snapshot",
        manifest,
        "plan-a",
        "market-a",
        b"c" * 32,
        (("plan-a", "market-a"),),
        source_records,
    )
    return staged, receipt


@pytest.mark.parametrize("drift", [None, "binding", "records"])
def test_frozen_attempt_metadata_rebinds_only_the_filing_and_checks_source_evidence(drift):
    staged, receipt = _frozen_source()
    if drift == "binding":
        staged.source_manifest["frozen_rate_file_binding"]["plan_ids"] = ["other-plan"]
    elif drift == "records":
        staged = replace(staged, source_records=staged.source_records[:1])
    arguments = (
        staged,
        receipt,
        "mrf",
        "local-snapshot",
        "source_a",
        _ownership(_binding()),
        {"source_snapshot_key": 19},
    )
    if drift:
        with pytest.raises(
            (initialization.ResultArchiveCandidateInitializationError, FrozenRateFileMismatchError),
            match="binding differs|cardinality changed",
        ):
            initialization._local_data_attempt_metadata(*arguments)
    else:
        run, manifest, options, parameters = initialization._local_data_attempt_metadata(*arguments)
        assert run != initialization.frozen_internal_run_id(receipt["source_file_import_id"])
        assert parameters["source_file_import_id"] != receipt["source_file_import_id"]
        assert options["auto_activate_candidates"] is False
        assert manifest["snapshot_id"] == "local-snapshot"
        assert manifest["activation"]["state"] == "building"
        assert manifest["frozen_rate_files"] == staged.source_manifest["frozen_rate_files"]


@pytest.mark.asyncio
@pytest.mark.parametrize("conflict", [False, True])
async def test_local_frozen_controls_refuse_a_changed_stored_admission(monkeypatch, conflict):
    staged, receipt = _frozen_source()
    run, manifest, options, parameters = initialization._local_data_attempt_metadata(
        staged, receipt, "mrf", "local-snapshot", "source_a", _ownership(_binding()), {"source_snapshot_key": 19}
    )
    session = _initialization_session(staged)

    @asynccontextmanager
    async def bound(selected_session):
        assert selected_session is session
        yield

    binding = initialization.frozen_rate_binding_from_params(parameters)
    store = AsyncMock(return_value={**binding, "source_key": "other_source"} if conflict else binding)
    monkeypatch.setattr(initialization.db, "bind_existing_session", bound)
    monkeypatch.setattr(initialization, "insert_or_compare_frozen_binding", store)
    if conflict:
        with pytest.raises(initialization.ResultArchiveCandidateInitializationError, match="frozen binding differs"):
            await initialization._persist_local_data_controls(
                session, "mrf", "local-snapshot", run, manifest, options, staged, parameters
            )
    else:
        assert (
            await initialization._persist_local_data_controls(
                session, "mrf", "local-snapshot", run, manifest, options, staged, parameters
            )
            is True
        )
    store.assert_awaited_once_with(initialization.db, parameters)


def _model_inspector(table):
    foreign_keys = []
    for constraint in table.constraints:
        if isinstance(constraint, sa.ForeignKeyConstraint):
            columns, schema, target, referred, ondelete = _expected_foreign_key_identity(constraint, table.schema)
            foreign_keys.append(
                {
                    "name": constraint.name,
                    "constrained_columns": columns,
                    "referred_schema": schema,
                    "referred_table": target,
                    "referred_columns": referred,
                    "options": {"ondelete": ondelete},
                }
            )
    return SimpleNamespace(
        get_columns=Mock(
            return_value=[{"name": column.name, "type": column.type, "nullable": column.nullable} for column in table.c]
        ),
        get_pk_constraint=Mock(return_value={"constrained_columns": list(table.primary_key.columns.keys())}),
        get_unique_constraints=Mock(
            return_value=[
                {"name": constraint.name, "column_names": list(constraint.columns.keys())}
                for constraint in table.constraints
                if isinstance(constraint, sa.UniqueConstraint)
            ]
        ),
        get_foreign_keys=Mock(return_value=foreign_keys),
    )


@pytest.mark.parametrize("drift", [None, "missing", "type", "nullable", "checks"])
def test_model_catalog_checks_native_columns_and_always_drops_temporary_tables(monkeypatch, drift):
    table = models.PTG2SnapshotPin.__table__
    inspector = _model_inspector(table)
    columns = inspector.get_columns.return_value
    if drift == "missing":
        columns.pop()
    elif drift == "type":
        columns[0]["type"] = sa.Integer()
    elif drift == "nullable":
        columns[0]["nullable"] = not columns[0]["nullable"]
    monkeypatch.setattr(sa, "inspect", lambda connection: inspector)
    checks = [
        {
            "expected": expected,
            "conname": constraint.name,
            "definition": str(constraint.sqltext),
            "safe": drift != "checks",
        }
        for constraint in table.constraints
        if isinstance(constraint, sa.CheckConstraint)
        for expected in (False, True)
    ]
    guard_by_field = {
        "tgname": attempt_audit._AUDIT_TRIGGER,
        "tgtype": 27,
        "tgenabled": "O",
        "proname": attempt_audit._AUDIT_FUNCTION,
        "function_schema": "mrf",
        "lanname": "plpgsql",
        "prosrc": attempt_audit._FINAL_AUDIT_BODY,
    }

    def execute(statement, _parameters=None):
        query = str(statement)
        if "FROM pg_constraint c" in query:
            return _result(checks)
        if "FROM pg_class c" in query:
            return _result(scalar=True)
        if "FROM pg_trigger AS trigger_record" in query:
            return _result([guard_by_field])
        return _result()

    connection = SimpleNamespace(execute=Mock(side_effect=execute))
    if drift:
        with pytest.raises(ValueError, match="columns differ|checks differ"):
            validation._require_local_publication_model_catalog(connection, "mrf", (models.PTG2SnapshotPin,))
    else:
        validation._require_local_publication_model_catalog(connection, "mrf", (models.PTG2SnapshotPin,))
    statements = [str(call.args[0]) for call in connection.execute.call_args_list]
    temporary_statements = [query for query in statements if query.startswith("CREATE TEMPORARY")]
    if temporary_statements:
        assert sum(query.startswith("DROP TABLE") for query in statements) == 1
        assert all("ON COMMIT DROP" in query for query in temporary_statements)
    else:
        connection.execute.assert_not_called()


@pytest.mark.parametrize(
    "field,value",
    [
        ("snapshot_id", ""),
        ("snapshot_id", "x" * 97),
        ("source_key", "UPPER"),
        ("coverage_scope_id", "g" * 64),
        ("plan_scopes", []),
        ("source_assignments", []),
        ("plan_scopes", [[" plan-a", "individual"]]),
        ("plan_scopes", [["plan-a", "INDIVIDUAL"]]),
        ("plan_scopes", [["plan-a"]]),
        ("plan_scopes", [["plan-a", "individual"], ["plan-a", "individual"]]),
    ],
)
def test_serving_scope_refuses_malformed_or_noncanonical_coordinates(field, value):
    scope = _serving_scope()
    scope[field] = value
    with pytest.raises(native.PTG2PhysicalBindingError, match="scope is invalid|scope is incomplete"):
        native.validate_local_serving_scope(scope)


@pytest.mark.parametrize(
    "field,value", [("source_key", True), ("source_key", -1), ("source_type", "x" * 33), ("identity_sha256", "short")]
)
def test_serving_scope_refuses_invalid_native_source_assignment(field, value):
    scope = _serving_scope()
    scope["source_assignments"][0][field] = value
    with pytest.raises(native.PTG2PhysicalBindingError, match="assignment"):
        native.validate_local_serving_scope(scope)


@pytest.mark.parametrize("oversized", [False, True])
def test_serving_scope_refuses_unsorted_or_oversized_native_source_dictionary(oversized):
    scope = _serving_scope()
    assignment = scope["source_assignments"][0]
    scope["source_assignments"] = [
        {**assignment, "source_key": index, "raw_container_sha256": f"{index:064x}"}
        for index in range(128 if oversized else 2)
    ]
    if not oversized:
        scope["source_assignments"].reverse()
    with pytest.raises(native.PTG2PhysicalBindingError, match="incomplete or oversized"):
        native.validate_local_serving_scope(scope)


@pytest.mark.asyncio
async def test_stage_refuses_auxiliary_custody_before_candidate_reads(monkeypatch):
    physical, _candidate, scope, initialized = _control_fixture()
    ownership = replace(_ownership(physical), auxiliary_oid=99)
    monkeypatch.setattr(native, "validate_local_serving_scope", lambda value: value)
    monkeypatch.setattr(validation, "acquire_ptg2_source_lifecycle_lock", AsyncMock())
    monkeypatch.setattr("process.ptg_parts.result_archive_candidate_preparation._local_audit_control", AsyncMock())
    reader = AsyncMock()
    monkeypatch.setattr(native, "_local_candidate_control", reader)
    with pytest.raises(validation.ResultArchiveCandidateValidationError, match="auxiliary custody differs"):
        await validation.stage_local_data_candidate_for_audit(
            _session(),
            ownership=ownership,
            metadata={
                "source_snapshot_id": physical.payload_snapshot_id,
                "source_snapshot_key": physical.payload_snapshot_key,
                "closure_metadata": {"serving_scope": scope},
            },
            initialized=initialized,
            audit={"control_sha256": "a" * 64},
            owner_oid=physical.owner_oid,
        )
    reader.assert_not_awaited()


@pytest.mark.parametrize(
    "drift", [None, "snapshot", "contract", "audit-contract", "audit-identity", "audit-model", "catalog", "control"]
)
def test_read_authority_preserves_complete_protected_validation(drift):
    physical, authority = _view_fixture()
    evidence = authority["native_validation"]
    changes_by_drift = {
        "snapshot": (evidence["initialization"], "destination_snapshot_id", "other-snapshot"),
        "contract": (evidence, "contract", "other-contract"),
        "audit-contract": (evidence["native_audit"], "contract", "other-contract"),
        "audit-identity": (evidence["native_audit"], "identity", []),
        "audit-model": (evidence["native_audit"], "model_sha256", "0" * 64),
        "catalog": (evidence["native_audit"], "catalog_sha256", "0" * 64),
        "control": (evidence["activation_evidence"], "control_sha256", "0" * 64),
    }
    if drift:
        target, field, value = changes_by_drift[drift]
        target[field] = value
    if drift:
        with pytest.raises(native.PTG2PhysicalBindingError, match="read authority differs"):
            validation._local_read_authority_binding(
                authority, physical.snapshot_id, physical.owner_oid, is_prepared=True
            )
    else:
        assert (
            validation._local_read_authority_binding(
                authority, physical.snapshot_id, physical.owner_oid, is_prepared=True
            )[1]
            == physical
        )


@pytest.mark.parametrize("drift", [None, "layout", "owner"])
def test_installed_read_binding_rejoins_actual_publication_inventory(drift):
    physical, authority = _view_fixture()
    _published, _candidate, _evidence, publication = _published_control_fixture()
    installed_by_field = {**dict.fromkeys(native._local_read_view_columns(False)), **authority}
    installed_by_field.update(contract="ptg.installed-physical-binding-read.v1", native_publication=publication)
    if drift == "layout":
        publication["destination_layout_key"] += 1
    elif drift == "owner":
        publication["owner_oid"] += 1
    if drift:
        with pytest.raises(native.PTG2PhysicalBindingError):
            validation._local_read_authority_binding(
                installed_by_field, physical.snapshot_id, physical.owner_oid, is_prepared=False
            )
    else:
        assert (
            validation._local_read_authority_binding(
                installed_by_field, physical.snapshot_id, physical.owner_oid, is_prepared=False
            )[1]
            == physical
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("publisher,prepared,count", [(False, False, 0), (False, True, 2), (True, True, 1)])
async def test_read_authority_requires_one_row_from_the_fixed_role_view(monkeypatch, publisher, prepared, count):
    monkeypatch.setattr(native, "local_preparation_catalog_owner", AsyncMock(return_value=73))
    reader, writer = AsyncMock(return_value=73), AsyncMock(return_value=73)
    monkeypatch.setattr(native, "require_local_physical_read_view", reader)
    monkeypatch.setattr(native, "require_local_physical_publisher_view", writer)
    session = _session([{"owner_oid": 73}] * count, scalars=[publisher])
    if count != 1:
        with pytest.raises(native.PTG2PhysicalBindingError, match="authority is unavailable"):
            await validation._local_read_authority(session, "local-snapshot", is_prepared=prepared)
    else:
        assert await validation._local_read_authority(session, "local-snapshot", is_prepared=prepared) == (
            True,
            73,
            {"owner_oid": 73},
        )
    assert reader.await_count == int(not publisher) and writer.await_count == int(publisher)
    query = str(session.execute.await_args.args[0])
    assert "LIMIT 2" in query
    assert ("prepared_physical_binding" in query) is prepared


@pytest.mark.asyncio
@pytest.mark.parametrize("transaction", [False, None])
async def test_qualified_payload_reader_requires_an_open_caller_transaction(monkeypatch, transaction):
    monkeypatch.setattr(native, "PREPARED_LOCAL_READ_VIEW_SHA256", "a" * 64)
    session = _session()
    session.in_transaction = (lambda: False) if transaction is False else None
    with pytest.raises(native.PTG2PhysicalBindingError, match="caller transaction"):
        await validation.local_data_physical_read_state(session, "local-snapshot", is_prepared=True)
    session.execute.assert_not_awaited()


def _transition_rows(table):
    revision = migration(FENCE_MIGRATION)
    attachment = next(
        entry
        for entry in (*revision.ATTEMPT_STATE_TABLES, *revision.ATTEMPT_ATTACHMENTS)
        if entry.table_name == table.name
    )
    body = revision._trigger_function_sql("mrf", attachment).split("AS $$", 1)[1].rsplit("$$", 1)[0]
    return [
        {
            "tgname": table.name + "_attempt_" + suffix,
            "tgtype": mask,
            "tgenabled": "O",
            "tgoldtable": old,
            "tgnewtable": new,
            "proname": function,
            "prosrc": body,
            "nspname": "mrf",
            "safe": True,
        }
        for suffix, mask, old, new, function in (
            ("lifecycle_lock", 30, None, None, "lock_ptg2_v4_attempt_lifecycle"),
            ("insert_guard", 4, None, "attempt_new_rows", "guard_" + table.name + "_attempt"),
            ("update_guard", 16, "attempt_old_rows", "attempt_new_rows", "guard_" + table.name + "_attempt"),
            ("delete_guard", 8, "attempt_old_rows", None, "guard_" + table.name + "_attempt"),
        )
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", [False, True])
async def test_attempt_admission_requires_every_historical_transition_guard(missing):
    guards = _transition_rows(models.PTG2Snapshot.__table__)
    if missing:
        guards.pop()
    session = _session(_attempt_functions(), guards)
    if missing:
        with pytest.raises(ValueError, match="transition guard differs"):
            await validation._require_local_publication_attempt_guards(session, "mrf", (models.PTG2Snapshot,))
    else:
        await validation._require_local_publication_attempt_guards(session, "mrf", (models.PTG2Snapshot,))
    assert session.execute.await_count == 2


@pytest.mark.parametrize("scope", [None, {}, {"contract": "other-contract"}])
def test_serving_scope_requires_the_exact_tagged_shape(scope):
    with pytest.raises(native.PTG2PhysicalBindingError, match="serving scope is invalid"):
        native.validate_local_serving_scope(scope)


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["header", "binding", "catalog", "control"])
async def test_payload_reader_refuses_changed_preimages_before_serving(monkeypatch, drift):
    physical, authority = _view_fixture()
    is_publisher = drift in {"header", "binding"}
    monkeypatch.setattr(native, "PREPARED_LOCAL_READ_VIEW_SHA256", "a" * 64)
    monkeypatch.setattr(native, "local_preparation_catalog_owner", AsyncMock(return_value=physical.owner_oid))
    monkeypatch.setattr(native, "require_local_physical_read_view", AsyncMock(return_value=physical.owner_oid))
    monkeypatch.setattr(native, "require_local_physical_publisher_view", AsyncMock(return_value=physical.owner_oid))
    header_binding = replace(physical, destination_layout_key=702) if drift == "binding" else physical
    header_by_field = {"validation_sha256": "0" * 64 if drift == "header" else authority["validation_sha256"]}
    monkeypatch.setattr(native, "_prepared_local_header", AsyncMock(return_value=(header_by_field, {}, header_binding)))
    monkeypatch.setattr(native, "verify_local_data_family", AsyncMock())
    monkeypatch.setattr(native, "_require_closed_local_custody", AsyncMock())
    monkeypatch.setattr(
        native, "local_data_catalog_digest", AsyncMock(return_value="0" * 64 if drift == "catalog" else "c" * 64)
    )
    control = AsyncMock(return_value=None)
    monkeypatch.setattr(native, "_local_candidate_control", control)
    session = _session([authority], *[[] for _ in physical.relation_oids], scalars=[is_publisher])
    session.info = {}
    with pytest.raises(
        native.PTG2PhysicalBindingError, match="preimage differs|catalog changed|control is unavailable"
    ):
        await validation.local_data_physical_read_state(session, physical.snapshot_id, is_prepared=True)
    assert session.info == {}
    assert control.await_count == int(drift == "control")
    locks = [
        str(call.args[0]) for call in session.execute.await_args_list if str(call.args[0]).startswith("LOCK TABLE ONLY")
    ]
    assert len(locks) == (0 if is_publisher else len(physical.relation_oids))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "prepared,witness", [(False, None), (True, None), (True, {"contract": "witness-contract", "provider_count": 2})]
)
async def test_serving_row_uses_the_pinned_payload_source_dictionary(monkeypatch, prepared, witness):
    physical, candidate, _evidence, _publication = _published_control_fixture()
    attested_by_field = {
        "attested_source_key": "source_a",
        "attested_coverage_scope_id": b"s" * 32,
        "attested_source_set_digest": b"r" * 32,
        "attested_audit_sample_digest": b"a" * 32,
    }
    candidate.update(attested_by_field)
    monkeypatch.setattr(
        validation, "local_data_physical_read_state", AsyncMock(return_value=({}, {}, physical, candidate))
    )
    session = _session(([witness] if witness else []) if prepared else [{"source_key": 0}])
    original_by_field = {"snapshot_id": physical.snapshot_id}
    row, observed = await validation.local_data_serving_row(
        session, physical.snapshot_id, original_by_field, is_prepared=prepared
    )
    assert observed == physical and original_by_field == {"snapshot_id": physical.snapshot_id}
    assert row["bound_snapshot_key"] == physical.destination_layout_key
    if prepared:
        assert row.get("persisted_witness_contract") == (witness["contract"] if witness else None)
        assert session.execute.await_args.args[1]["snapshot_key"] == physical.payload_snapshot_key
    else:
        assert row["source_identity_rows"] == [{"source_key": 0}]
        assert session.execute.await_args.args[1]["snapshot_id"] == physical.payload_snapshot_id
    assert all(row[key] == value for key, value in attested_by_field.items())
    assert physical.schema_name in str(session.execute.await_args.args[0])


@pytest.mark.asyncio
async def test_installed_reader_checks_the_published_control_before_caching(monkeypatch):
    physical, authority = _view_fixture()
    _physical, candidate, published_evidence, publication = _published_control_fixture()
    evidence = authority["native_validation"]
    for field in ("initialization", "data", "native_audit", "activation_evidence"):
        evidence[field].update(published_evidence[field])
    evidence["control_sha256"] = published_evidence["control_sha256"]
    evidence["activation_evidence"]["control_sha256"] = evidence["control_sha256"]
    installed_by_field = {**dict.fromkeys(native._local_read_view_columns(False)), **authority}
    installed_by_field.update(contract="ptg.installed-physical-binding-read.v1", native_publication=publication)
    monkeypatch.setattr(native, "INSTALLED_LOCAL_READ_VIEW_SHA256", "a" * 64)
    monkeypatch.setattr(native, "local_preparation_catalog_owner", AsyncMock(return_value=physical.owner_oid))
    monkeypatch.setattr(native, "require_local_physical_read_view", AsyncMock(return_value=physical.owner_oid))
    monkeypatch.setattr(native, "verify_local_data_family", AsyncMock())
    monkeypatch.setattr(native, "_require_closed_local_custody", AsyncMock())
    monkeypatch.setattr(native, "local_data_catalog_digest", AsyncMock(return_value="c" * 64))
    reader = AsyncMock(return_value=candidate)
    monkeypatch.setattr(native, "_local_candidate_control", reader)
    session = _session(
        [installed_by_field],
        *[[] for _ in physical.relation_oids],
        [{"plan_id": "12-3456789", "plan_market_type": "group"}],
        scalars=[False],
    )
    session.info = {}
    assert (await validation.local_data_physical_read_state(session, physical.snapshot_id, is_prepared=False))[
        3
    ] == candidate
    assert session.info["ptg2_local_read_bindings"][physical.schema_name] == physical
    assert session.info["ptg2_local_read_catalog_sha256"][physical.schema_name] == "c" * 64
    assert reader.await_args.kwargs == {"lock_controls": False}


def _complete_catalog_connection(table_types):
    tables_by_name = {model.__tablename__: model.__table__ for model in table_types}
    inspectors_by_name = {name: _model_inspector(table) for name, table in tables_by_name.items()}
    inspector = SimpleNamespace(
        get_columns=lambda name, **kwargs: inspectors_by_name[name].get_columns(name, **kwargs),
        get_pk_constraint=lambda name, **kwargs: inspectors_by_name[name].get_pk_constraint(name, **kwargs),
        get_unique_constraints=lambda name, **kwargs: inspectors_by_name[name].get_unique_constraints(name, **kwargs),
        get_foreign_keys=lambda name, **kwargs: inspectors_by_name[name].get_foreign_keys(name, **kwargs),
    )

    def execute_catalog_statement(statement, parameters=None):
        query = str(statement)
        if "FROM pg_constraint c" in query:
            table = tables_by_name[parameters["actual"].rsplit(".", 1)[1].strip('"')]
            return _result(
                [
                    {
                        "expected": expected,
                        "conname": constraint.name,
                        "definition": str(constraint.sqltext),
                        "safe": True,
                    }
                    for constraint in table.constraints
                    if isinstance(constraint, sa.CheckConstraint)
                    for expected in (False, True)
                ]
            )
        if "SELECT pg_get_expr" in query:
            return _result(scalar="state = 'sealed'")
        if "FROM pg_class c" in query:
            return _result(scalar=True)
        if "FROM pg_trigger AS trigger_record" in query:
            return _result(
                [
                    {
                        "tgname": attempt_audit._AUDIT_TRIGGER,
                        "tgtype": 27,
                        "tgenabled": "O",
                        "proname": attempt_audit._AUDIT_FUNCTION,
                        "function_schema": "mrf",
                        "lanname": "plpgsql",
                        "prosrc": attempt_audit._FINAL_AUDIT_BODY,
                    }
                ]
            )
        return _result()

    connection = SimpleNamespace(execute=Mock(side_effect=execute_catalog_statement))
    return inspector, connection


@pytest.mark.asyncio
async def test_publication_controls_authenticate_the_complete_model_and_guard_catalog(monkeypatch):
    table_types = (
        models.PTG2ImportRun,
        models.PTG2Snapshot,
        models.PTG2V3SnapshotLayout,
        models.PTG2V3SnapshotBinding,
        models.PTG2V3SnapshotScope,
        models.PTG2V3SnapshotPlanScope,
        models.PTG2V3CandidateAuditAttestation,
        models.PTG2SnapshotPin,
        models.PTG2CurrentSourceSnapshot,
        models.PTG2CurrentPlanSource,
        models.PTG2V4AttemptFence,
        models.PTG2V4AttemptStage,
    )
    inspector, connection = _complete_catalog_connection(table_types)
    monkeypatch.setattr(sa, "inspect", lambda selected: inspector)
    connection.run_sync = AsyncMock(side_effect=lambda callback: callback(connection))
    guard_rows = [
        _transition_rows(model.__table__)
        for model in table_types
        if model not in {models.PTG2V3SnapshotLayout, models.PTG2V4AttemptFence, models.PTG2V4AttemptStage}
    ]
    session = _session([], _attempt_functions(), *guard_rows)
    session.connection = AsyncMock(return_value=connection)
    await validation.require_local_publication_controls(session)
    assert connection.run_sync.await_count == 1
    statements = [str(call.args[0]) for call in connection.execute.call_args_list]
    assert sum(query.startswith("CREATE TEMPORARY") for query in statements) == len(table_types)
    assert sum(query.startswith("DROP TABLE") for query in statements) == len(table_types)
    lock = str(session.execute.await_args_list[0].args[0])
    assert all(model.__tablename__ in lock for model in table_types)
    assert "ACCESS SHARE MODE NOWAIT" in lock
