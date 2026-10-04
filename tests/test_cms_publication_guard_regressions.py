# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Publication ownership, native receipt continuity, and bounded preflight failures."""

import contextvars
import datetime
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import provider_directory_cms_preflight as preflight
from process import provider_directory_cms_publication as publication
from process import provider_directory_cms_serving as serving
from process import provider_directory_cms_serving_receipt as receipts
from process import provider_directory_profile_capacity_preflight_contract as preflight_contract
from process import provider_directory_profile_selection as selection
from process import provider_directory_profile_serving_receipt as continuity
from tests.provider_directory_cms_capacity_test_support import cms_execution, cms_plan
from tests.provider_directory_profile_execution_test_support import _wal_tracker_admission
from tests.test_provider_directory_cms_preflight import (
    _database_observation,
    _fresh_guard,
    _inputs,
    _issue_stubs,
    _request,
)
from tests.test_provider_directory_cms_preflight import (
    fhir as _preflight_fhir,
)
from tests.test_provider_directory_cms_serving import _fhir as _serving_fhir
from tests.test_provider_directory_cms_serving_receipt import _archive_result, _payload
from tests.test_provider_directory_profile_capacity_attestation import VALIDATION_TIME, _trust
from tests.test_provider_directory_profile_selection_desired import _desired, _selection_rows

pytestmark = pytest.mark.asyncio


@pytest.fixture(autouse=True)
def configured_node(monkeypatch):
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")


def _snapshot(payload):
    return deepcopy({key: payload[key] for key in ("profile", "address", "doctors", "alias_generation", "overlay_oid")})


def _native_payload():
    payload = _payload()
    for family in ("address", "doctors"):
        payload[family].update(local_generation=1, origin_generation=1)
    return payload


def _execution(payload, *, operation="publish", selected=True, expected=None):
    return SimpleNamespace(
        attestation=SimpleNamespace(
            operation=operation,
            pairs=deepcopy(payload["desired_datasets"]) if selected else [],
            desired_cms_dataset=deepcopy(payload["desired_datasets"][0]) if selected else None,
            expected_cms_incumbent=expected,
            proof_id=payload["selection"]["proof_id"],
            selection_fingerprint=payload["selection"]["fingerprint"],
            catalog_digest=payload["selection"]["catalog_digest"],
        )
    )


def _retained_execution():
    catalog, sources, rows, _candidate = _selection_rows(_desired(current=True))
    computed = selection._computed_selection_from_rows(
        catalog, node_id="dev-node", source_rows=sources, dataset_rows=rows
    )
    identity_by_field = {**computed.identity_payload, "authority_revision": 7}
    attestation = selection.validated_profile_selection_attestation(
        {**identity_by_field, "proof_id": selection._proof_id(identity_by_field)}
    )
    return selection.ProviderDirectoryProfileExecution(attestation, 11)


def _row_result(row):
    return SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: row))


def _native_session(
    snapshot, *, predecessor=None, history=False, child=False, advisory=True, verified=True, payload_bytes=4096
):
    """Supply transport responses while retaining the production SQL/receipt guards."""

    async def scalar(statement, parameters=None):
        sql = str(statement)
        if "pg_try_advisory_xact_lock" in sql:
            return advisory
        if "cms_serving_native_snapshot()" in sql:
            return deepcopy(snapshot)
        if "cms_serving_native_matches(" in sql:
            return True
        if "publication_xid IS DISTINCT FROM" in sql:
            return verified
        if "pg_column_size" in sql:
            return payload_bytes
        if sql.lstrip().startswith("INSERT INTO"):
            return "d" * 64
        if "WHERE predecessor_receipt_id=:receipt_id" in sql:
            return child
        if "SELECT EXISTS (SELECT 1 FROM" in sql:
            return history
        raise AssertionError("unexpected native scalar: " + sql)

    async def execute(statement, parameters=None):
        if "cms_serving_current_receipt_matches" in str(statement):
            return _row_result(predecessor)
        return None

    return SimpleNamespace(
        in_transaction=lambda: True, scalar=AsyncMock(side_effect=scalar), execute=AsyncMock(side_effect=execute)
    )


def _publication_fhir(session, *, bound=False, commit_error=None, observed_wal=0):
    active = [session] if bound else []

    @asynccontextmanager
    async def transaction():
        nested = bool(active)
        active.append(session)
        try:
            yield session
        finally:
            active.pop()
        if not nested and commit_error is not None:
            raise commit_error

    @asynccontextmanager
    async def read_session():
        yield session

    async def scalar(statement, **parameters):
        if "to_regclass" in statement:
            return True
        if "current_setting" in statement:
            return "5s"
        if "pg_wal_lsn_diff" in statement:
            return observed_wal
        if "pg_current_wal_insert_lsn" in statement:
            return "0/1"
        raise AssertionError("unexpected database scalar: " + statement)

    database = SimpleNamespace(
        transaction=transaction,
        session=read_session,
        _transaction_binding=lambda: SimpleNamespace(session=active[-1]) if active else None,
        scalar=AsyncMock(side_effect=scalar),
        status=AsyncMock(),
    )
    return SimpleNamespace(
        db=database,
        profile_initial=_preflight_fhir.profile_initial,
        profile_artifact=_preflight_fhir.profile_artifact,
        _schema=lambda: "synthetic",
        _qt=lambda schema, table: f'"{schema}"."{table}"',
        _provider_directory_artifact_transaction_timeout_seconds=lambda *_args, **_kwargs: 5,
        _ordered_provider_directory_artifact_bundle=lambda stages: stages,
        _provider_directory_artifact_bundle_context=lambda *_args: ("synthetic", (), "1s", "5s"),
        _configure_provider_directory_artifact_promotion=AsyncMock(),
        _lock_and_verify_artifact_dataset_fence=AsyncMock(),
        _sql_string_literal=lambda value: "'" + value + "'",
    )


def _prepared(fhir, *, datasets=(), archive=None):
    return SimpleNamespace(
        fhir=fhir,
        fence=SimpleNamespace(datasets=datasets),
        stages=(),
        profile_delta=None,
        nonprofile_admission=None,
        archive_delta=archive,
        metrics={},
        assert_ready=AsyncMock(),
        mark_committed=AsyncMock(),
    )


@pytest.mark.parametrize("failure", ["selection", "proof", None])
async def test_common_receipt_requires_exact_selected_or_retained_cms_proof(failure):
    payload = _native_payload()
    predecessor_by_field = {"receipt_id": "c" * 64, "payload": payload}
    execution = _execution(payload, selected=False)
    proof = deepcopy(payload["cms"])
    if failure == "proof":
        proof["dataset_hash"] = "e" * 64
    original = deepcopy(payload)
    if failure:
        with pytest.raises(RuntimeError, match="selection_invalid|candidate_coverage_changed"):
            publication._receipt_payload(
                execution, proof, None if failure == "selection" else predecessor_by_field, _snapshot(payload)
            )
    else:
        result = publication._receipt_payload(execution, proof, predecessor_by_field, _snapshot(payload))
        assert result["cms"] == payload["cms"]
        assert result["desired_datasets"] == []
        assert result["expected_incumbent"] == {key: payload["cms"][key] for key in receipts._PIN_FIELDS}
    assert payload == original


@pytest.mark.parametrize(
    "selected,expected,has_predecessor,accepted",
    [
        (False, None, True, True),
        (True, None, False, True),
        (True, None, True, False),
        (True, "current", True, True),
        (True, "other", True, False),
    ],
)
async def test_incumbent_authority_is_required_only_for_a_fresh_cms_selection(
    selected, expected, has_predecessor, accepted
):
    payload = _native_payload()
    pin_by_field = {key: payload["cms"][key] for key in receipts._PIN_FIELDS}
    expected_pin = (
        None
        if expected is None
        else {**pin_by_field, **({"dataset_id": "other-dataset"} if expected == "other" else {})}
    )
    execution = _execution(payload, selected=selected, expected=expected_pin)
    predecessor = {"receipt_id": "c" * 64, "payload": payload} if has_predecessor else None
    if accepted:
        publication._assert_expected_incumbent(execution, predecessor)
    else:
        with pytest.raises(RuntimeError, match="incumbent_changed"):
            publication._assert_expected_incumbent(execution, predecessor)


@pytest.mark.parametrize("observations", [(True, True), (False, True), (False, False)])
async def test_published_coverage_is_resealed_if_needed_and_must_match_afterward(observations):
    fhir = _publication_fhir(SimpleNamespace())
    session = SimpleNamespace(scalar=AsyncMock(side_effect=observations))
    proof = _native_payload()["cms"]
    fhir.ENDPOINT_DATASET_PUBLISHED = "published"
    fhir.db.first = AsyncMock(return_value={"dataset_id": proof["dataset_id"]})
    if observations[-1]:
        await publication._seal_published_coverage(fhir, session, proof)
    else:
        with pytest.raises(RuntimeError, match="coverage_cutover_changed"):
            await publication._seal_published_coverage(fhir, session, proof)
    assert session.scalar.await_count == 2
    assert fhir.db.first.await_count == int(not observations[0])
    if not observations[0]:
        assert fhir.db.first.await_args.kwargs == {
            "dataset_id": proof["dataset_id"],
            "endpoint_id": proof["endpoint_id"],
            "published": "published",
            "release_id": proof["release_id"],
            "dataset_hash": proof["dataset_hash"],
            "proof_version": 2,
        }


async def test_coverage_seal_requires_the_exact_published_dataset_row_before_rechecking():
    fhir = _publication_fhir(SimpleNamespace())
    fhir.ENDPOINT_DATASET_PUBLISHED = "published"
    fhir.db.first = AsyncMock(return_value=None)
    session = SimpleNamespace(scalar=AsyncMock(return_value=False))
    with pytest.raises(RuntimeError, match="coverage_cutover_changed"):
        await publication._seal_published_coverage(fhir, session, _native_payload()["cms"])
    assert session.scalar.await_count == 1
    assert fhir.db.first.await_count == 1


@pytest.mark.parametrize("failure", ["no-transaction", "release-busy"])
async def test_complete_candidate_coverage_checks_require_owner_transaction_and_release_authority(monkeypatch, failure):
    monkeypatch.setattr(publication.coverage, "_schema_name", lambda: '"synthetic"')
    session = SimpleNamespace(
        in_transaction=lambda: failure != "no-transaction", execute=AsyncMock(), scalar=AsyncMock(return_value=False)
    )
    if failure == "no-transaction":
        with pytest.raises(ValueError, match="coverage_requires_transaction"):
            await publication.coverage.validate_cms_candidate_coverage(session, "synthetic-dataset", "a" * 64)
        session.execute.assert_not_awaited()
        session.scalar.assert_not_awaited()
    else:
        with pytest.raises(publication.coverage.DirectoryReadError) as caught:
            await publication.coverage.validate_cms_candidate_coverage(session, "synthetic-dataset", "a" * 64)
        assert caught.value.status == 503
        assert any("LOCK TABLE" in str(call.args[0]) for call in session.execute.await_args_list)
        assert "pg_try_advisory_xact_lock" in str(session.scalar.await_args.args[0])
    assert all(
        not str(call.args[0]).lstrip().startswith(("INSERT", "UPDATE", "DELETE"))
        for call in session.execute.await_args_list
    )


@pytest.mark.parametrize("failure", ["borrowed", "doctors-without-address", "doctors-purge"])
async def test_publication_rejects_borrowed_transactions_and_incomplete_doctors_preparation_before_sql(failure):
    payload = _native_payload()
    session = _native_session(_snapshot(payload))
    fhir = _publication_fhir(session, bound=failure == "borrowed")
    prepared = _prepared(fhir)
    predecessor_by_field = {"receipt_id": "c" * 64, "payload": payload}
    execution = _execution(payload, operation="purge", selected=False)
    with pytest.raises(RuntimeError, match="requires_own_transaction|doctors_preparation_invalid"):
        await publication.commit_prepared_serving_generation(
            fhir,
            execution,
            prepared,
            address=SimpleNamespace() if failure == "doctors-purge" else None,
            candidate_proof=payload["cms"],
            native_dependencies=_snapshot(payload),
            predecessor=predecessor_by_field,
            doctors=SimpleNamespace(),
        )
    session.execute.assert_not_awaited()
    session.scalar.assert_not_awaited()
    prepared.mark_committed.assert_not_awaited()


@pytest.mark.parametrize("failure", ["tip", "incumbent", None])
async def test_publication_rechecks_common_tip_and_incumbent_before_prepared_writes(failure):
    receipt_payload = _native_payload()
    predecessor_by_field = {"receipt_id": "c" * 64, "payload": receipt_payload}
    actual = {**predecessor_by_field, "receipt_id": "d" * 64} if failure == "tip" else predecessor_by_field
    session = _native_session(_snapshot(receipt_payload), predecessor=actual)
    fhir = _publication_fhir(session)
    archive = SimpleNamespace(before_lock=AsyncMock())
    prepared = _prepared(fhir, datasets=(SimpleNamespace(dataset_id="selected"),), archive=archive)
    incumbent_by_field = {key: receipt_payload["cms"][key] for key in receipts._PIN_FIELDS}
    if failure == "incumbent":
        incumbent_by_field["dataset_id"] = "other-dataset"
    execution = _execution(receipt_payload, expected=incumbent_by_field)
    has_applied = False
    if failure:
        with pytest.raises(RuntimeError, match="predecessor_changed|incumbent_changed"):
            async with publication._publication_transaction(
                fhir, execution, prepared, _snapshot(receipt_payload), predecessor_by_field, None
            ):
                has_applied = True
        prepared.assert_ready.assert_not_awaited()
    else:
        async with publication._publication_transaction(
            fhir, execution, prepared, _snapshot(receipt_payload), predecessor_by_field, None
        ) as (owner_session, _timeout):
            assert owner_session is session
            assert fhir.db._transaction_binding().session is session
            has_applied = True
        prepared.assert_ready.assert_awaited_once_with(cutover=True)
    assert has_applied == (failure is None)
    assert fhir.db._transaction_binding() is None
    fhir._lock_and_verify_artifact_dataset_fence.assert_awaited_once_with(prepared.fence)
    archive.before_lock.assert_awaited_once_with(fhir, session)
    assert all(
        not str(call.args[0]).lstrip().startswith(("INSERT", "UPDATE", "DELETE"))
        for call in session.execute.await_args_list
    )


@pytest.mark.parametrize("commit_error", [None, "lost acknowledgement"])
async def test_unproved_common_commit_never_consumes_preparations_and_preserves_publication_error(
    monkeypatch, commit_error
):
    receipt_payload = _native_payload()
    predecessor_by_field = {"receipt_id": "c" * 64, "payload": receipt_payload}
    session = _native_session(_snapshot(receipt_payload), predecessor=predecessor_by_field, verified=False)
    original_error = OSError(commit_error) if commit_error else None
    fhir = _publication_fhir(session, commit_error=original_error)
    prepared = _prepared(fhir)
    execution = _execution(receipt_payload, operation="purge", selected=False)
    result_payload = publication._receipt_payload(
        execution, receipt_payload["cms"], predecessor_by_field, _snapshot(receipt_payload)
    )
    applied = AsyncMock(return_value=("d" * 64, result_payload))
    monkeypatch.setattr(publication, "_apply_prepared_results", applied)
    with pytest.raises(OSError if commit_error else RuntimeError, match=commit_error or "commit_unproved") as caught:
        await publication.commit_prepared_serving_generation(
            fhir,
            execution,
            prepared,
            address=None,
            candidate_proof=receipt_payload["cms"],
            native_dependencies=_snapshot(receipt_payload),
            predecessor=predecessor_by_field,
        )
    if original_error:
        assert caught.value is original_error
    applied.assert_awaited_once()
    prepared.mark_committed.assert_not_awaited()
    assert prepared.metrics == {}
    assert fhir.db._transaction_binding() is None
    assert any("publication_xid IS DISTINCT FROM" in str(call.args[0]) for call in session.scalar.await_args_list)


async def test_verified_common_receipt_consumes_archive_and_profile_even_without_address_rebuild():
    payload = _native_payload()
    payload["archive"] = _archive_result()
    session = _native_session(_snapshot(payload), verified=True)
    fhir = _publication_fhir(session)
    archive = SimpleNamespace(mark_committed=AsyncMock())
    prepared = _prepared(fhir, archive=archive)
    assert (
        await publication._finalize_committed_publication(fhir, prepared, None, None, "d" * 64, payload, True)
        == "d" * 64
    )
    archive.mark_committed.assert_awaited_once_with(fhir, payload["archive"])
    prepared.mark_committed.assert_awaited_once_with(profile_result=payload["profile"])
    assert prepared.metrics["cms_serving"] == {
        "receipt_id": "d" * 64,
        "dataset_id": payload["cms"]["dataset_id"],
        "profile_generation_id": payload["profile"]["generation_id"],
        "address_generation": 1,
        "doctors_generation": 1,
        "recovered_commit": True,
    }


def _composite_session(snapshot):
    """Record live locks and receipt writes while retaining real SQL guards."""
    evidence_by_field = {"admission_sha256": "e" * 64, "metadata_sha256": "f" * 64, "relationship_count": 3}
    session = _native_session(snapshot)
    native_scalar, native_execute = session.scalar.side_effect, session.execute.side_effect
    events = []

    async def scalar(statement, parameters=None):
        sql = str(statement)
        if "SELECT c.relkind" in sql:
            return None if parameters["name"] == "provider_directory_profile_evidence" else "r"
        if "d.status='published'" in sql:
            return True
        if sql.lstrip().startswith("INSERT INTO"):
            events.append("common-receipt")
        return await native_scalar(statement, parameters)

    async def execute(statement, parameters=None):
        sql = str(statement)
        if (
            "SELECT admission_sha256, metadata_sha256, relationship_count" in sql
            or "d.content_proof_admission_sha256" in sql
        ):
            return _row_result(evidence_by_field)
        if "IN ACCESS EXCLUSIVE MODE" in sql:
            events.append("all-live-locked")
        if sql == "SET CONSTRAINTS ALL IMMEDIATE":
            events.append("constraints-checked")
        return await native_execute(statement, parameters)

    session.scalar.side_effect, session.execute.side_effect = scalar, execute
    return session, events


def _composite_initial_stages():
    build = SimpleNamespace(
        schema="synthetic", evidence_stage="initial_evidence_stage", profile_stage="initial_profile_stage",
        materialization_mode="full_swap",
    )
    return tuple(
        _preflight_fhir.ProviderDirectoryPreparedArtifactStage(
            schema=build.schema, stage_table=stage_table, target_relation=target,
            rename_stage_indexes=AsyncMock(), profile_initial_build=build,
        )
        for target, stage_table in (
            (_preflight_fhir.profile_artifact.PROFILE_EVIDENCE_TABLE, build.evidence_stage),
            (_preflight_fhir.profile_artifact.PROFILE_TABLE, build.profile_stage),
        )
    )


async def _lock_composite_delta(monkeypatch, session, profile_delta):
    async def status(sql):
        await session.execute(publication.text(sql))

    with monkeypatch.context() as context:
        context.setattr(_preflight_fhir, "db", SimpleNamespace(status=status))
        await _preflight_fhir._lock_profile_delta_relations(_preflight_fhir._profile_delta_relations(profile_delta))


def _composite_prepared(fhir, with_forecast):
    admission = _wal_tracker_admission()
    fhir._provider_directory_profile_capacity_admission = lambda: admission
    fhir._validate_profile_delta_total_wal = AsyncMock()
    prepared = _prepared(fhir)
    prepared.stages = () if with_forecast else _composite_initial_stages()
    prepared.profile_delta = SimpleNamespace(
        schema="synthetic", evidence_stage="delta_evidence_stage", profile_stage="delta_profile_stage",
        affected_npi_stage="delta_affected_stage",
    ) if with_forecast else None
    prepared.archive_delta = SimpleNamespace(apply=AsyncMock(return_value=_archive_result()))
    prepared.nonprofile_admission = SimpleNamespace(assert_cutover_complete=AsyncMock())
    return prepared


def _assert_composite_delta_locks(session):
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    exclusive_index = next(index for index, sql in enumerate(statements) if "IN ACCESS EXCLUSIVE MODE" in sql)
    for name in ("provider_directory_profile_evidence", "provider_directory_profile"):
        writer_lock = f'LOCK TABLE "synthetic"."{name}" IN SHARE ROW EXCLUSIVE MODE NOWAIT;'
        assert statements.index(writer_lock) < exclusive_index
    for name in ("delta_evidence_stage", "delta_profile_stage", "delta_affected_stage"):
        stage_lock = f'LOCK TABLE "synthetic"."{name}" IN SHARE MODE NOWAIT;'
        assert statements.index(stage_lock) < exclusive_index


def _assert_composite_completion(fhir, session, prepared, has_doctors, forecast):
    """Require complete live locks and validate archive, admission, and WAL completion."""
    locks = [
        str(call.args[0]) for call in session.execute.await_args_list if "IN ACCESS EXCLUSIVE MODE" in str(call.args[0])
    ]
    assert len(locks) == 1 and '"entity_address_unified"' in locks[0]
    assert '"provider_directory_profile_evidence"' not in locks[0]
    if has_doctors:
        assert all(f'"{name}"' in locks[0] for name in publication.RELATION_NAMES_BY_IMPORTER["cms-doctors"])
    prepared.archive_delta.apply.assert_awaited_once_with(fhir, session)
    prepared.nonprofile_admission.assert_cutover_complete.assert_awaited_once()
    if forecast is not None:
        assert '"provider_directory_profile"' not in locks[0]
        _assert_composite_delta_locks(session)
        prepared.assert_ready.assert_not_awaited()
        fhir._validate_profile_delta_total_wal.assert_awaited_once_with(
            fhir._provider_directory_profile_capacity_admission(), forecast
        )
    else:
        assert '"provider_directory_profile"' in locks[0]
        prepared.assert_ready.assert_awaited_once_with(cutover=True)
        fhir._validate_profile_delta_total_wal.assert_not_awaited()


@pytest.mark.parametrize("with_doctors,with_forecast", [(False, False), (False, True), (True, False), (True, True)])
async def test_composite_publication_locks_live_set_before_swaps_and_receipt(monkeypatch, with_doctors, with_forecast):
    """Fence every family swap and append exactly one validated common receipt."""
    receipt_payload = _native_payload()
    session, events = _composite_session(_snapshot(receipt_payload))
    fhir = _publication_fhir(session, bound=True)
    prepared = _composite_prepared(fhir, with_forecast)
    address = SimpleNamespace(
        swaps=(SimpleNamespace(live_cls=SimpleNamespace(__main_table__="entity_address_unified")),)
    )
    doctors = SimpleNamespace() if with_doctors else None
    forecast = SimpleNamespace() if with_forecast else None
    prior_events = ["delta-targets-locked", "profile-applied"] if with_forecast else []

    async def apply_bundle(_fhir, stages, **options):
        assert stages is prepared.stages
        assert options["settings_configured"] is True
        assert options["cutover_timeout"] is (None if with_forecast else timeout)
        if with_forecast:
            await _lock_composite_delta(monkeypatch, session, prepared.profile_delta)
            events.extend(prior_events)
        await options["before_swaps"]()
        assert events[len(prior_events)] == "all-live-locked"
        if with_doctors:
            assert events[len(prior_events) + 1] == "doctors-applied"
        if not with_forecast:
            events.append("profile-applied")
        return forecast

    async def apply_doctors(actual):
        assert actual is doctors and events == [*prior_events, "all-live-locked"]
        events.append("doctors-applied")

    async def apply_address(actual):
        last_delta_event = "doctors-applied" if with_doctors else "all-live-locked"
        assert actual is address and events[-1] == (last_delta_event if with_forecast else "profile-applied")
        events.append("address-applied")

    monkeypatch.setattr(publication, "apply_prepared_artifact_bundle", apply_bundle)
    monkeypatch.setattr(publication, "apply_prepared_cms_doctors_generation", apply_doctors)
    monkeypatch.setattr(publication, "publish_prepared_entity_address_generation", apply_address)
    timeout = object()
    receipt_id, receipt_result_by_field = await publication._apply_prepared_results(
        fhir, _execution(receipt_payload), prepared, address, receipt_payload["cms"], None, timeout, doctors
    )
    assert receipt_id == "d" * 64 and receipt_result_by_field["archive"] == _archive_result()
    assert events == [
        *prior_events,
        "all-live-locked",
        *(["doctors-applied"] if with_doctors else []),
        *([] if with_forecast else ["profile-applied"]),
        "address-applied",
        "common-receipt",
        "constraints-checked",
    ]
    _assert_composite_completion(fhir, session, prepared, with_doctors, forecast)


@pytest.mark.parametrize("schema", [None, "9schema", "schema.other", "a" * 64])
async def test_invalid_receipt_schema_never_reaches_the_database(schema):
    session = SimpleNamespace(execute=AsyncMock())
    with pytest.raises(ValueError, match="schema_invalid"):
        await receipts.read_current_receipt(session, schema)
    session.execute.assert_not_awaited()


@pytest.mark.parametrize(
    "mutation,reason",
    [
        ("boolean-contract", "contract_invalid"),
        ("new-contract", "contract_invalid"),
        ("tuple-vector", "vector_invalid"),
        ("oversized-vector", "vector_invalid"),
        ("empty-pin", "pin_invalid"),
        ("nonstring-pin", "pin_invalid"),
        ("oversized-pin", "pin_invalid"),
    ],
)
async def test_receipt_shape_rejection_never_issues_an_append(mutation, reason):
    payload = _native_payload()
    if mutation == "boolean-contract":
        payload["contract_version"] = True
    elif mutation == "new-contract":
        payload["contract_version"] = 2
    elif mutation == "tuple-vector":
        payload["desired_datasets"] = tuple(payload["desired_datasets"])
    elif mutation == "oversized-vector":
        payload["desired_datasets"] *= 257
    else:
        payload["desired_datasets"][0]["dataset_id"] = {
            "empty-pin": "",
            "nonstring-pin": 42,
            "oversized-pin": "a" * 257,
        }[mutation]
    session = SimpleNamespace(in_transaction=lambda: True, scalar=AsyncMock())
    with pytest.raises(ValueError, match=reason):
        await receipts.append_serving_receipt(session, "synthetic", payload)
    session.scalar.assert_not_awaited()


@pytest.mark.parametrize("failure", ["no-transaction", "alias-busy", None])
async def test_native_dependency_capture_requires_owned_transaction_and_alias_lock(failure):
    payload = _native_payload()
    session = _native_session(_snapshot(payload), advisory=failure != "alias-busy")
    session.in_transaction = lambda: failure != "no-transaction"
    if failure:
        with pytest.raises(
            ValueError if failure == "no-transaction" else RuntimeError, match="requires_transaction|dependencies_busy"
        ):
            await receipts.capture_native_dependencies(session, "synthetic", lock=True)
        assert not any("cms_serving_native_snapshot()" in str(call.args[0]) for call in session.scalar.await_args_list)
        assert not any("LOCK TABLE" in str(call.args[0]) for call in session.execute.await_args_list)
    else:
        assert await receipts.capture_native_dependencies(session, "synthetic", lock=True) == _snapshot(payload)
        assert any("LOCK TABLE" in str(call.args[0]) for call in session.execute.await_args_list)
        assert len([call for call in session.execute.await_args_list if "FOR SHARE NOWAIT" in str(call.args[0])]) == 5


@pytest.mark.parametrize("proof", [None, "sealed"])
async def test_serving_capture_requires_one_sealed_candidate_proof(proof):
    payload = _native_payload()
    row = (
        None
        if proof is None
        else {
            key: payload["cms"][key]
            for key in ("dataset_id", "endpoint_id", "dataset_hash", "release_id", "proof_version")
        }
    )
    session = SimpleNamespace(execute=AsyncMock(return_value=_row_result(row)))
    fhir = _serving_fhir([])
    if proof is None:
        with pytest.raises(RuntimeError, match="candidate_coverage_unavailable"):
            await serving._candidate_proof(fhir, session, _execution(payload))
    else:
        result = await serving._candidate_proof(fhir, session, _execution(payload))
        assert result == row and result is not row
    assert "proof_version=2" in str(session.execute.await_args.args[0])
    assert session.execute.await_args.args[1] == payload["desired_datasets"][0]


@pytest.mark.parametrize("history", [False, True])
async def test_purge_without_a_current_common_tip_is_only_allowed_before_history(monkeypatch, history):
    payload = _native_payload()
    session = _native_session(_snapshot(payload), history=history)
    fhir = _publication_fhir(session)
    prepare = AsyncMock()
    monkeypatch.setattr(serving, "prepare_serving_artifacts", prepare)
    if history:
        with pytest.raises(RuntimeError, match="current_receipt_unavailable"):
            await serving._purge_common_profile(
                fhir, _execution(payload, operation="purge", selected=False), "run-a", None, {}
            )
    else:
        assert (
            await serving._purge_common_profile(
                fhir, _execution(payload, operation="purge", selected=False), "run-a", None, {}
            )
            is None
        )
    prepare.assert_not_called()
    assert fhir.db._transaction_binding() is None
    assert not any("cms_serving_native_snapshot()" in str(call.args[0]) for call in session.scalar.await_args_list)


def _continuity_fhir(session, execution, *, admission=None, fence=None):
    fhir = _publication_fhir(session, bound=True)
    fhir._PROVIDER_DIRECTORY_PROFILE_SELECTION_EXECUTION = contextvars.ContextVar(
        "receipt-execution", default=execution
    )
    fhir._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE = contextvars.ContextVar("receipt-fence", default=fence)
    fhir._provider_directory_profile_capacity_admission = lambda: admission
    fhir.PROVIDER_DIRECTORY_PROFILE_CUTOVER_FIXED_ROW_LOCK_COUNT = 5
    fhir._profile_cutover_lock_count = lambda _fence: 6
    fhir._reserve_provider_directory_profile_wal_budget = AsyncMock()
    fhir._assert_provider_directory_profile_wal_budget = AsyncMock()
    fhir._provider_directory_profile_current_wal_bytes = AsyncMock(return_value=0)
    return fhir


@pytest.mark.parametrize("failure", ["unattested", "no-admission", "no-delta", "fresh-cms", "no-fence"])
async def test_ordinary_profile_with_common_history_requires_its_attested_delta_before_locks(failure):
    payload = _native_payload()
    session = _native_session(_snapshot(payload), history=True)
    execution = cms_execution() if failure == "fresh-cms" else _retained_execution()
    if failure == "unattested":
        execution = SimpleNamespace(attestation=execution.attestation)
    fhir = _continuity_fhir(
        session,
        execution,
        admission=None if failure == "no-admission" else _wal_tracker_admission(),
        fence=None if failure == "no-fence" else SimpleNamespace(datasets=()),
    )
    with pytest.raises(
        RuntimeError, match="attested_delta_required|desired_cms_requires_preparation|dataset_fence_missing"
    ):
        await continuity._capture_predecessor(
            fhir, "synthetic", None if failure == "no-delta" else object(), "1s", "5s"
        )
    fhir._configure_provider_directory_artifact_promotion.assert_not_awaited()
    fhir._reserve_provider_directory_profile_wal_budget.assert_not_awaited()
    session.execute.assert_not_awaited()


@pytest.mark.parametrize("failure", ["missing-tip", "existing-successor", "cms-changed"])
async def test_ordinary_profile_rejects_history_and_incumbent_drift_before_reservation(monkeypatch, failure):
    execution = _retained_execution()
    payload = _native_payload()
    selected = next(pair for pair in execution.attestation.pairs if pair["source_id"] == "cms-npd")
    payload["cms"].update({key: selected[key] for key in receipts._PIN_FIELDS})
    if failure == "cms-changed":
        payload["cms"]["dataset_id"] = "other-dataset"
    predecessor_by_field = {"receipt_id": "c" * 64, "payload": payload}
    session = _native_session(
        _snapshot(payload),
        predecessor=None if failure == "missing-tip" else predecessor_by_field,
        history=True,
        child=failure == "existing-successor",
    )
    fhir = _continuity_fhir(session, execution, admission=_wal_tracker_admission(), fence=SimpleNamespace(datasets=()))
    reserve = AsyncMock()
    monkeypatch.setattr(continuity, "_reserve_receipt_mutation", reserve)
    with pytest.raises(RuntimeError, match="history_inconsistent|cms_incumbent_changed"):
        await continuity._capture_predecessor(fhir, "synthetic", object(), "1s", "5s")
    reserve.assert_not_awaited()
    fhir._lock_and_verify_artifact_dataset_fence.assert_awaited_once()
    assert any("LOCK TABLE" in str(call.args[0]) for call in session.execute.await_args_list)
    assert fhir.db.status.await_args.args[0] == "SET LOCAL lock_timeout = '5s';"


def _successor_capture(failure):
    """Capture a native predecessor and inject one publication boundary failure."""
    receipt_payload = _native_payload()
    before = _snapshot(receipt_payload)
    after = deepcopy(before)
    if failure in {"address", "doctors"}:
        after[failure]["local_generation"] += 1
    elif failure == "alias":
        after["alias_generation"] += 1
    elif failure != "unchanged":
        after["profile"]["generation_id"] = "next-profile-generation"
    session = _native_session(
        after,
        payload_bytes=continuity.capacity.METADATA_PAYLOAD_UPPER_BOUND_BYTES + 1 if failure == "payload" else 4096,
    )
    admission = _wal_tracker_admission()
    wal_projection = 4096
    observed = (
        -1
        if failure == "negative-wal"
        else wal_projection + continuity.capacity.CONTROL_WAL_ROW_LOCK_UPPER_BOUND_BYTES_PER_TUPLE + 1
        if failure == "excess-wal"
        else wal_projection
    )
    fhir = _continuity_fhir(session, _execution(receipt_payload), admission=admission)
    fhir.db.scalar = AsyncMock(
        side_effect=lambda statement, **_parameters: observed if "pg_wal_lsn_diff" in statement else "0/1"
    )
    if failure == "transaction":
        other_session = SimpleNamespace(in_transaction=lambda: True)
        fhir.db._transaction_binding = lambda: SimpleNamespace(session=other_session)
    if failure == "final-wal":
        fhir._provider_directory_profile_current_wal_bytes.return_value = (
            admission.geometry.reservation_bytes_by_storage_class["wal"]
        )
    captured = (
        session,
        {"receipt_id": "c" * 64, "payload": receipt_payload},
        before,
        _execution(receipt_payload),
        admission,
        wal_projection,
    )
    return fhir, session, captured


@pytest.mark.parametrize(
    "failure",
    [
        "transaction",
        "address",
        "doctors",
        "alias",
        "unchanged",
        "payload",
        "negative-wal",
        "excess-wal",
        "final-wal",
        None,
    ],
)
async def test_ordinary_successor_preserves_native_authority_and_enforces_capacity_bounds(failure):
    """Keep native authority fixed and reject unsafe receipt or WAL growth."""
    fhir, session, captured = _successor_capture(failure)
    reason_by_failure = {
        "transaction": "transaction_changed",
        "address": "native_dependencies_changed",
        "doctors": "native_dependencies_changed",
        "alias": "native_dependencies_changed",
        "payload": "metadata_payload_exceeded",
        "negative-wal": "wal_projection_exceeded",
        "excess-wal": "wal_projection_exceeded",
        "final-wal": "final_wal_exceeded",
    }
    if failure in reason_by_failure:
        with pytest.raises(RuntimeError, match=reason_by_failure[failure]):
            await continuity._append_successor(fhir, "synthetic", captured)
    else:
        assert await continuity._append_successor(fhir, "synthetic", captured) is None
    appends = [call for call in session.scalar.await_args_list if str(call.args[0]).lstrip().startswith("INSERT INTO")]
    assert len(appends) == int(failure in {"negative-wal", "excess-wal", "final-wal", None})
    if appends:
        assert any(str(call.args[0]) == "SET CONSTRAINTS ALL IMMEDIATE" for call in session.execute.await_args_list)
    if failure in {"transaction", "address", "doctors", "alias", "unchanged", "payload"}:
        fhir._assert_provider_directory_profile_wal_budget.assert_not_awaited()
    if failure in {"negative-wal", "excess-wal"}:
        fhir._provider_directory_profile_current_wal_bytes.assert_not_awaited()


async def test_real_signed_profile_pair_must_leave_the_required_build_window(monkeypatch):
    profile_lease = cms_plan()[2]
    remaining = int((profile_lease.max_build_deadline - VALIDATION_TIME).total_seconds())
    request = _request(limits={"required_build_seconds": remaining + 1})
    fhir = SimpleNamespace(_profile_capacity_preflight_clock=AsyncMock(return_value=VALIDATION_TIME))
    monkeypatch.setattr(preflight.capacity_runtime, "configured_capacity_lease_trust", _trust)
    with pytest.raises(RuntimeError, match="paired_profile_deadline_too_short"):
        await preflight._verified_pair(fhir, request)


@pytest.mark.parametrize("failure", ["missing", "changed", None])
async def test_paired_preflight_checks_complete_receipt_before_open_authority(failure):
    profile_lease = cms_plan()[2]
    expected = profile_lease.signing_preflight_guard["healthcare_receipt"]
    actual = deepcopy(expected)
    if failure == "changed":
        actual["issued_at"] = "2026-07-30T12:01:01Z"
    receipt_by_field = {"receipt": actual}
    fhir = SimpleNamespace(
        db=SimpleNamespace(first=AsyncMock(return_value=None if failure == "missing" else receipt_by_field)),
        _schema=lambda: "synthetic",
        _profile_capacity_preflight_receipt_ref=lambda _schema: (
            '"synthetic"."provider_directory_profile_capacity_preflight_receipt"'
        ),
        _pagination_checkpoint_row_mapping=lambda value: value,
        _profile_capacity_preflight_stored_receipt=lambda value, _lease: value["receipt"],
        _assert_profile_capacity_receipt_open=Mock(),
        _profile_capacity_preflight_clock=AsyncMock(return_value=VALIDATION_TIME),
    )
    if failure:
        with pytest.raises(RuntimeError, match="paired_preflight_missing|paired_preflight_changed"):
            await preflight._paired_preflight(fhir, profile_lease, lock=True)
        fhir._assert_profile_capacity_receipt_open.assert_not_called()
    else:
        assert await preflight._paired_preflight(fhir, profile_lease, lock=True) == expected
        fhir._assert_profile_capacity_receipt_open.assert_called_once_with(
            receipt_by_field, profile_lease, VALIDATION_TIME
        )
    assert fhir.db.first.await_args.args[0].endswith(" FOR UPDATE")


async def test_missing_quiescence_observation_never_becomes_zero_counts():
    request = _request()
    profile_lease = cms_plan()[2]
    fhir = SimpleNamespace(
        db=SimpleNamespace(first=AsyncMock(return_value=None)),
        _schema=lambda: "synthetic",
        _profile_capacity_quiescence_sql=lambda _schema: "SELECT :request_sha256",
        _PROFILE_ACTIVE_RUN_STATUSES=("running",),
        PROFILE_EXECUTION_CONTRACT_ID="synthetic-contract",
    )
    with pytest.raises(RuntimeError, match="quiescence_missing"):
        await preflight._quiescence(fhir, request, profile_lease, VALIDATION_TIME)
    assert (
        fhir.db.first.await_args.kwargs["paired_request_sha256"]
        == profile_lease.signing_preflight_guard["healthcare_receipt"]["request_sha256"]
    )


@pytest.mark.parametrize("outlives_pair", [False, True])
async def test_validated_preflight_expiry_cannot_outlive_the_original_verified_profile_pair(monkeypatch, outlives_pair):
    raw = _fresh_guard()["healthcare_request"]
    original_profile = cms_plan()[2]
    expiry = original_profile.expires_at + datetime.timedelta(seconds=int(outlives_pair))
    raw["signing_guard"]["expires_at"] = expiry.strftime("%Y-%m-%dT%H:%M:%SZ")
    request = preflight_contract.validated_capacity_preflight_request(raw)
    inputs, profile_lease = _inputs(request)
    verify_pair = preflight._verified_pair
    _issue_stubs(monkeypatch, request, inputs, profile_lease)
    monkeypatch.setattr(preflight, "_verified_pair", verify_pair)
    monkeypatch.setattr(preflight.capacity_runtime, "configured_capacity_lease_trust", _trust)
    if outlives_pair:
        with pytest.raises(RuntimeError, match="paired_profile_expiry_too_short"):
            await preflight._issue_receipt(_preflight_fhir, request, inputs, profile_lease)
        _preflight_fhir.db.status.assert_not_awaited()
        _preflight_fhir._profile_capacity_preflight_receipt_layout.assert_not_awaited()
    else:
        receipt = await preflight._issue_receipt(_preflight_fhir, request, inputs, profile_lease)
        assert receipt["expires_at"] == raw["signing_guard"]["expires_at"]
        _preflight_fhir.db.status.assert_awaited_once()


@pytest.mark.parametrize("failure", ["transaction", "overrides"])
async def test_preflight_cannot_borrow_transaction_or_staged_serving_names(failure):
    session = SimpleNamespace()
    fhir = _publication_fhir(session, bound=failure == "transaction")
    fhir._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES = contextvars.ContextVar(
        "preflight-overrides", default={"profile": "staged-profile"} if failure == "overrides" else {}
    )
    fhir._profile_capacity_preflight_clock = AsyncMock()
    with pytest.raises(RuntimeError, match="own_transaction_required"):
        await preflight.capacity_preflight(fhir, _request())
    fhir._profile_capacity_preflight_clock.assert_not_awaited()
    fhir.db.scalar.assert_not_awaited()


@pytest.mark.parametrize("changed", ["runtime", "serving-payload", "serving-digest", None])
async def test_fresh_preflight_keeps_complete_runtime_and_serving_identity_through_issuance(monkeypatch, changed):
    request = _request()
    inputs, profile_lease = _inputs(request)
    payload = _native_payload()
    native = _snapshot(payload)
    inputs = replace(inputs, native_dependencies=native)
    session = _native_session(native)
    fhir = _publication_fhir(session, bound=True)
    fhir.assert_profile_selection_current_in_transaction = AsyncMock()
    fhir._provider_directory_profile_selection_catalog = lambda: {}
    observed = _database_observation(inputs, profile_lease)
    runtime_by_field = {**inputs.runtime, **({"healthcare_source_commit": "ab" * 20} if changed == "runtime" else {})}
    actual_serving = SimpleNamespace(
        payload={"changed": True} if changed == "serving-payload" else inputs.serving.payload,
        payload_sha256="ab" * 32 if changed == "serving-digest" else inputs.serving.payload_sha256,
    )
    fhir._profile_capacity_preflight_serving = AsyncMock(return_value=actual_serving)
    monkeypatch.setattr(preflight, "assert_native_address_input_fence", AsyncMock())
    monkeypatch.setattr(preflight, "_database_observation", AsyncMock(return_value=observed))
    monkeypatch.setattr(preflight, "observe_profile_runtime", AsyncMock(return_value=runtime_by_field))
    if changed:
        with pytest.raises(RuntimeError, match="preflight_inputs_changed"):
            await preflight._assert_current_inputs(fhir, request, inputs, profile_lease, session)
    else:
        await preflight._assert_current_inputs(fhir, request, inputs, profile_lease, session)
    fhir._lock_and_verify_artifact_dataset_fence.assert_awaited_once_with(inputs.fence)
    assert any("pg_try_advisory_xact_lock" in str(call.args[0]) for call in session.scalar.await_args_list)
    fhir.db.status.assert_not_awaited()
