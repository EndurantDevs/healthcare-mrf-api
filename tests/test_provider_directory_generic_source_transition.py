# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""A successor producer may adopt only one verified, detached stopped intake."""

import asyncio
import errno
import hashlib
import os
import re
from contextlib import asynccontextmanager, nullcontext
from copy import deepcopy
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import provider_directory_generic_source_policy as policy_module
from process import provider_directory_generic_source_storage as storage
from process import provider_directory_source_storage_binding as binding
from process.provider_directory_generic_candidate import GenericCandidateError
from process.provider_directory_generic_source_policy import GenericSourcePolicy
from process.vsp_directory_contract import RESOURCE_TYPES, SOURCE_ID
from tests.test_provider_directory_generic_source_storage import _case

ROOT = "run_" + "a" * 32
FAILED = "run_" + "b" * 32
DATASET = "synthetic-dataset"
ENDPOINT = "synthetic-endpoint"


def _candidate_storage_evidence():
    """Build a private, unpublished candidate and two issued storage seals."""
    _, _, header = _case()
    old_storage_by_field = {
        **asdict(
            binding.FHIRSourceStorageBinding(
                "catalog_schema",
                "source_schema",
                10,
                20,
                30,
                100,
                101,
                "d" * 64,
            )
        ),
        "producer": ["a" * 40, "sha256:" + "b" * 64],
    }
    new_storage_by_field = {
        **old_storage_by_field,
        "enrollment_sha256": "e" * 64,
        "producer": ["c" * 40, "sha256:" + "f" * 64],
    }
    document = header["candidate_storage_json"]
    document.update(contract=storage.ISOLATED_CONTRACT, storage=old_storage_by_field, admission=None)
    document["identity"].update(
        dataset_id=DATASET,
        endpoint_id=ENDPOINT,
        acquisition_root_run_id=ROOT,
    )
    document["identity"]["metadata"].update(expected_resources=["Organization"])
    header.update(
        dataset_id=DATASET,
        endpoint_id=ENDPOINT,
        acquisition_root_run_id=ROOT,
        import_run_id=FAILED,
        published_at=None,
    )
    return header, old_storage_by_field, new_storage_by_field


def _completed_input_evidence():
    """Keep the failed child, checkpoint and raw-row census aligned."""
    run_rows = [
        dict(
            run_id=FAILED,
            status="failed",
            finished_at="now",
            retry_of_run_id=ROOT,
            root_run_id=ROOT,
            source_ids=["cms-npd"],
        ),
        dict(
            run_id=ROOT,
            status="failed",
            finished_at="before",
            retry_of_run_id=None,
            root_run_id=ROOT,
            source_ids=["cms-npd"],
        ),
    ]
    checkpoint_rows = [
        dict(
            resource_type="Organization",
            state="complete",
            owner_run_id=FAILED,
            dataset_id=DATASET,
            acquisition_root_run_id=ROOT,
            rows_processed=3,
            pages_processed=1,
            completed_at="now",
            canonical_api_base="https://example.test/fhir",
            start_url_hash=hashlib.sha256(b"https://example.test/fhir/Organization?_count=100").hexdigest(),
            next_url=None,
        )
    ]
    input_rows = [dict(dataset_id=DATASET, resource_type="Organization", total=3, active=3)]
    return run_rows, checkpoint_rows, input_rows


def _update_test_transition_header(header, args):
    if storage._json(args[2]) != header["candidate_storage_json"] or args[4] != header["status"]:
        return "UPDATE 0"
    header.update(candidate_storage_json=storage._json(args[1]), status=args[3])
    return "UPDATE 1"


@pytest.fixture
def case(monkeypatch):
    """Expose a transaction-shaped fake and native proof seams."""
    policy = GenericSourcePolicy("source_schema", "source_owner", "source_publisher", ("source_reader",))
    header, old_storage_by_field, new_storage_by_field = _candidate_storage_evidence()
    run_rows, checkpoint_rows, input_rows = _completed_input_evidence()
    bulk_checkpoints, bulk_outputs = [], []
    writes = []

    async def fetchrow(sql, *args):
        if sql.startswith("SELECT session_user"):
            return dict(session="administrator", current="administrator", administrator=True)
        return deepcopy(header)

    async def fetch(sql, *args):
        placeholders = {int(value) for value in re.findall(r"\$(\d+)", sql)}
        assert placeholders == set(range(1, len(args) + 1))
        if 'FROM "catalog_schema"."import_run"' in sql:
            return deepcopy(run_rows)
        if "provider_directory_pagination_checkpoint" in sql:
            return deepcopy(checkpoint_rows)
        if "provider_directory_bulk_acquisition_checkpoint" in sql:
            return deepcopy(bulk_checkpoints)
        if "provider_directory_bulk_output_checkpoint" in sql:
            return deepcopy(bulk_outputs)
        return deepcopy(input_rows)

    async def execute(sql, *args):
        writes.append((sql, args))
        return _update_test_transition_header(header, args)

    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetchrow=AsyncMock(side_effect=fetchrow),
        fetchval=AsyncMock(
            side_effect=lambda sql, *args: "serializable" if sql == "SHOW transaction_isolation" else True
        ),
        fetch=AsyncMock(side_effect=fetch),
        execute=AsyncMock(side_effect=execute),
    )
    native, tables = AsyncMock(), AsyncMock()
    monkeypatch.setattr(binding, "verify_isolated_source_storage_coordinates", native)
    monkeypatch.setattr(storage, "_verify_tables", tables)
    return SimpleNamespace(
        connection=connection,
        policy=policy,
        header=header,
        old=old_storage_by_field,
        new=new_storage_by_field,
        runs=run_rows,
        checkpoints=checkpoint_rows,
        bulk_checkpoints=bulk_checkpoints,
        bulk_outputs=bulk_outputs,
        input_rows=input_rows,
        writes=writes,
        native=native,
        tables=tables,
    )


async def _transition(case, expected_counts=None, *, stopped_run_id=FAILED, input_census=None):
    return await storage.transition_isolated_loading_candidate_storage(
        case.connection,
        policy=case.policy,
        identity_by_field={
            "dataset_id": DATASET,
            "endpoint_id": ENDPOINT,
            "acquisition_root_run_id": ROOT,
            "failed_run_id": stopped_run_id,
        },
        old_storage=case.old,
        new_storage=case.new,
        expected_resource_counts={"Organization": 3} if expected_counts is None else expected_counts,
        **({"input_census": input_census} if input_census is not None else {}),
    )


def _census_lock_queries(case, backend_by_field):
    """Evaluate retained lock evidence while requiring every native SQL predicate."""
    input_oids = sorted(table["oid"] for table in case.header["candidate_storage_json"]["tables"].values())
    locks = [dict(relation=oid, mode="ShareLock", granted=True) for oid in input_oids]
    original_fetchval = case.connection.fetchval.side_effect

    def retained_locks(query, *arguments):
        if "FROM pg_locks l JOIN" not in query:
            return original_fetchval(query, *arguments)
        assert all(
            predicate in query
            for predicate in (
                "SELECT count(*)=$1",
                "a.pid=l.pid",
                "a.pid=$2",
                "a.datid=$3::oid",
                "a.backend_start=$4",
                "a.xact_start=$5",
                "l.virtualtransaction=$6",
                "l.database=$3::oid",
                "l.relation=ANY($7::oid[])",
                "l.locktype='relation'",
                "l.mode='ShareLock'",
                "l.granted",
            )
        )
        assert arguments == (
            len(input_oids),
            *(
                backend_by_field[name]
                for name in ("pid", "database_oid", "backend_start", "xact_start", "virtualtransaction")
            ),
            input_oids,
        )
        return (
            sum(lock["relation"] in arguments[6] and lock["mode"] == "ShareLock" and lock["granted"] for lock in locks)
            == arguments[0]
        )

    case.connection.fetchval.side_effect = retained_locks
    original_execute = case.connection.execute.side_effect

    async def final_write(query, *arguments):
        if query.startswith("LOCK TABLE "):
            return "LOCK TABLE"
        return await original_execute(query, *arguments)

    case.connection.execute.side_effect = final_write
    return locks


def _census_connection(case):
    """Expose native transaction and lock identity without replacing the complete census."""
    state_by_field = {"active": False}
    backend_by_field = dict(
        database_name="synthetic",
        database_oid=10,
        pid=7,
        backend_start="start",
        xact_start="transaction",
        virtualtransaction="3/7",
        session="administrator",
        current="administrator",
    )
    counts_by_type = {"Organization": 3}
    identity_by_field = dict(
        dataset_id=DATASET, endpoint_id=ENDPOINT, acquisition_root_run_id=ROOT, failed_run_id=FAILED
    )
    locks = _census_lock_queries(case, backend_by_field)

    class Transaction:
        async def start(self):
            state_by_field["active"] = True

        async def rollback(self):
            state_by_field["active"] = False

    async def census_header(query, *arguments):
        if query == storage._TRANSITION_CENSUS_IDENTITY_SQL:
            return dict(backend_by_field)
        return await case.connection.fetchrow(query, *arguments)

    transaction = Transaction()
    connection = SimpleNamespace(
        transaction=lambda **_options: transaction,
        is_in_transaction=lambda: state_by_field["active"],
        is_closed=lambda: False,
        fetchrow=AsyncMock(side_effect=census_header),
        fetchval=case.connection.fetchval,
        fetch=case.connection.fetch,
        execute=AsyncMock(),
    )
    context = storage.isolated_source_transition_census(
        connection,
        policy=case.policy,
        identity_by_field=identity_by_field,
        old_storage=case.old,
        new_storage=case.new,
        expected_resource_counts=counts_by_type,
    )
    return context, SimpleNamespace(
        backend=backend_by_field,
        connection=connection,
        transaction=transaction,
        counts=counts_by_type,
        identity=identity_by_field,
        locks=locks,
    )


@pytest.mark.asyncio
async def test_census_rollback_timeout_aborts_exact_transport():
    """A stuck driver cancellation cannot retain the owned socket indefinitely."""
    terminated, finished = asyncio.Event(), asyncio.Event()

    async def wait_for_native_cancellation():
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            await terminated.wait()
        finally:
            finished.set()

    connection = SimpleNamespace(is_closed=lambda: False, terminate=Mock(side_effect=terminated.set), _transport=Mock())
    transaction = SimpleNamespace(rollback=wait_for_native_cancellation)
    operation = asyncio.create_task(storage._rollback_transition_census(connection, transaction))
    try:
        completed, _ = await asyncio.wait((operation,), timeout=2)
        assert operation in completed
        with pytest.raises(TimeoutError):
            operation.result()
    finally:
        terminated.set()
        operation.cancel()
        await asyncio.gather(operation, return_exceptions=True)
    assert finished.is_set()
    connection.terminate.assert_called_once_with()
    connection._transport.abort.assert_called_once_with()


@pytest.mark.asyncio
@pytest.mark.parametrize("rollback_fails", [False, True])
async def test_canceled_census_begin_rolls_back_or_aborts_exact_transport(case, monkeypatch, rollback_fails):
    """A canceled BEGIN cannot leave its caller's backend in an uncertain transaction."""
    context, evidence = _census_connection(case)
    entered = asyncio.Event()
    original_start = evidence.transaction.start

    async def start():
        await original_start()
        entered.set()
        await asyncio.Event().wait()

    rollback = AsyncMock(side_effect=RuntimeError("uncertain BEGIN") if rollback_fails else None)
    monkeypatch.setattr(evidence.transaction, "start", start)
    monkeypatch.setattr(evidence.transaction, "rollback", rollback)
    evidence.connection.terminate = Mock()
    evidence.connection._transport = Mock()
    operation = asyncio.create_task(context.__aenter__())
    await asyncio.wait_for(entered.wait(), 1)
    operation.cancel()
    with pytest.raises(asyncio.CancelledError):
        await operation
    rollback.assert_awaited_once_with()
    assert evidence.connection.terminate.call_count == int(rollback_fails)
    assert evidence.connection._transport.abort.call_count == int(rollback_fails)
    evidence.connection.execute.assert_not_awaited()
    assert case.writes == []


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["begin", "after_census"])
async def test_census_freezes_validated_counts_and_identity_before_await(case, monkeypatch, phase):
    """Caller mutation cannot change what was verified or the evidence later handed off."""
    context, evidence = _census_connection(case)
    target, name = (evidence.transaction, "start") if phase == "begin" else (storage, "_verify_transition_witness")
    original = getattr(target, name)

    async def mutate_caller(*args, **kwargs):
        result = await original(*args, **kwargs)
        evidence.counts["Organization"] = 4
        evidence.identity["endpoint_id"] = "different-endpoint"
        return result

    monkeypatch.setattr(target, name, mutate_caller)
    async with context as census:
        assert storage._json(census.counts) == {"Organization": 3}
        assert storage._json(census.identity)["endpoint_id"] == ENDPOINT
        receipt = await _transition(case, input_census=census)
    assert receipt["rows"] == 3


@pytest.mark.asyncio
async def test_complete_census_transfers_only_with_live_native_custody(case):
    """The full grouped census runs once; final controls remain independently revalidated."""
    context, _ = _census_connection(case)
    async with context as census:
        receipt = await _transition(case, input_census=census)
    assert receipt["rows"] == 3 and receipt["already_applied"] is False and census.active is False
    census_calls = [
        call for call in case.connection.fetch.await_args_list if "GROUP BY dataset_id,resource_type" in call.args[0]
    ]
    assert len(census_calls) == 1
    assert any(
        "provider_directory_pagination_checkpoint" in call.args[0] for call in case.connection.fetch.await_args_list
    )
    assert sum("FROM pg_locks l JOIN" in call.args[0] for call in case.connection.fetchval.await_args_list) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field", ["database_oid", "pid", "backend_start", "xact_start", "virtualtransaction", "session"]
)
async def test_census_rejects_changed_original_backend_identity(case, field):
    """An identical row count cannot substitute for continuous physical transaction custody."""
    context, evidence = _census_connection(case)
    async with context as census:
        evidence.backend[field] = "changed"
        with pytest.raises(GenericCandidateError, match="transition_census_backend_changed"):
            await _transition(case, input_census=census)
    assert case.writes == []


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["expired", "counts", "task"])
async def test_census_refuses_stale_or_substituted_evidence(case, drift):
    """Preparation is scoped to exact counts, one live task and both native heap locks."""
    context, _ = _census_connection(case)
    async with context as census:
        if drift == "counts":
            census.counts = storage._encoded({"Organization": 4})
        elif drift == "task":
            census.task = object()
        if drift != "expired":
            with pytest.raises(GenericCandidateError, match="transition_census_(?:changed|backend_changed)"):
                await _transition(case, input_census=census)
    if drift == "expired":
        with pytest.raises(GenericCandidateError, match="transition_census_changed"):
            await _transition(case, input_census=census)
    assert case.writes == []


@pytest.mark.asyncio
@pytest.mark.parametrize("heap", [0, 1])
@pytest.mark.parametrize("drift", ["missing", "mode", "ungranted"])
async def test_census_requires_both_granted_native_share_locks(case, heap, drift):
    context, evidence = _census_connection(case)
    async with context as census:
        if drift == "missing":
            evidence.locks.pop(heap)
        elif drift == "mode":
            evidence.locks[heap]["mode"] = "AccessShareLock"
        else:
            evidence.locks[heap]["granted"] = False
        with pytest.raises(GenericCandidateError, match="transition_census_backend_changed"):
            await _transition(case, input_census=census)
    assert case.writes == []


def _include_completed_empty_collection(case):
    """Model an empty selected resource with its own completed checkpoint."""
    metadata = case.header["candidate_storage_json"]["identity"]["metadata"]
    metadata["selected_resources"] = ["Organization", "InsurancePlan"]
    metadata["expected_resources"] = ["Organization", "InsurancePlan"]
    case.header["publication_metadata_json"] = deepcopy(metadata)
    case.checkpoints.append({**case.checkpoints[0], "resource_type": "InsurancePlan", "rows_processed": 0})


@pytest.mark.asyncio
async def test_completed_empty_collection_retains_isolated_candidate(case):
    _include_completed_empty_collection(case)
    result = await _transition(case, {"Organization": 3, "InsurancePlan": 0})
    assert result["rows"] == 3 and result["already_applied"] is False
    assert case.header["candidate_storage_json"]["storage"] == case.new


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["missing_positive", "foreign_dataset", "unexpected_type"])
async def test_empty_collection_still_rejects_foreign_or_missing_input(case, drift):
    _include_completed_empty_collection(case)
    if drift == "missing_positive":
        case.input_rows.clear()
    else:
        case.input_rows.append(
            {
                "dataset_id": "other" if drift == "foreign_dataset" else DATASET,
                "resource_type": "InsurancePlan" if drift == "foreign_dataset" else "Endpoint",
                "total": 1,
                "active": 1,
            }
        )
    with pytest.raises(GenericCandidateError, match="transition_input_changed"):
        await _transition(case, {"Organization": 3, "InsurancePlan": 0})
    assert case.writes == []


@pytest.mark.asyncio
async def test_exact_detached_candidate_changes_only_its_storage_binding(case):
    before = deepcopy(case.header["candidate_storage_json"])
    result = await _transition(case)
    assert result["rows"] == 3 and result["already_applied"] is False
    assert result["lineage_run_ids"] == sorted([ROOT, FAILED])
    assert case.header["candidate_storage_json"] == {**before, "storage": case.new}
    assert len(case.writes) == 1
    assert "candidate_storage_json=$2::jsonb" in case.writes[0][0]
    case.native.assert_awaited_once()
    case.tables.assert_awaited_once()
    assert (await _transition(case))["already_applied"] is True
    assert len(case.writes) == 1


@pytest.fixture
def vsp_retry_candidate(case, monkeypatch):
    binding_document = case.header["candidate_storage_json"]
    metadata = binding_document["identity"]["metadata"]
    metadata.update(
        source_ids=[SOURCE_ID], selected_resources=list(RESOURCE_TYPES), expected_resources=list(RESOURCE_TYPES)
    )
    case.header["publication_metadata_json"] = deepcopy(metadata)
    binding_document.update(ordinal=0, resets={"*": 0})
    case.checkpoints.clear()
    publisher = SimpleNamespace(connection=case.connection, policy=case.policy)
    monkeypatch.setattr(storage, "source_owner_transaction", lambda _database: nullcontext(publisher))
    monkeypatch.setattr(policy_module, "bound_publisher", lambda: publisher)
    directory = SimpleNamespace(db=object(), _endpoint_dataset_candidate_metadata=lambda _candidate: metadata)
    candidate = SimpleNamespace(dataset_id=DATASET, endpoint_id=ENDPOINT, acquisition_root_run_id=ROOT)
    return directory, candidate


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("retained", "error"),
    [
        (None, None),
        ("input", "transition_input_changed"),
        ("witness_or_proof", "transition_witness_changed"),
        ("producer", "storage_binding_changed"),
    ],
)
async def test_vsp_capture_retry_proves_empty_storage_under_the_current_producer(
    case, monkeypatch, vsp_retry_candidate, retained, error
):
    directory, candidate = vsp_retry_candidate
    case.header["status"] = "failed"
    if retained != "input":
        case.input_rows.clear()
    if retained == "witness_or_proof":
        case.connection.fetchval.return_value = False
        case.connection.fetchval.side_effect = None
    monkeypatch.setattr(storage, "_require_candidate_storage", lambda *_args: True)
    monkeypatch.setattr(
        storage, "storage_document", lambda _publisher: case.new if retained == "producer" else case.old
    )
    if error:
        with pytest.raises(GenericCandidateError, match=f"^provider_directory_generic_source_{error}$"):
            await storage.has_empty_isolated_vsp_storage(directory, candidate, case.header)
    else:
        assert await storage.has_empty_isolated_vsp_storage(directory, candidate, case.header) is True
        case.tables.assert_awaited_once()
    assert not case.writes


@pytest.mark.asyncio
async def test_vsp_capture_retry_rejects_acquiring_legacy_storage(case, monkeypatch, vsp_retry_candidate):
    directory, candidate = vsp_retry_candidate
    case.header["status"] = "acquiring"
    binding_document = case.header["candidate_storage_json"]
    binding_document["contract"] = storage.CONTRACT
    del binding_document["storage"], binding_document["admission"]
    case.input_rows.clear()
    monkeypatch.setattr(storage, "_require_candidate_storage", lambda *_args: None)
    with pytest.raises(GenericCandidateError, match="^provider_directory_generic_source_vsp_retry_candidate_changed$"):
        await storage.has_empty_isolated_vsp_storage(directory, candidate, case.header)
    case.tables.assert_not_awaited()
    assert not case.writes


def _retain_original_root(case, status):
    """An original run has no retry parent or explicit reference to itself."""
    case.runs[:] = [case.runs[-1]]
    case.runs[0].update(status=status, root_run_id=None)
    case.header["import_run_id"] = ROOT
    for checkpoint in (*case.checkpoints, *case.bulk_checkpoints):
        checkpoint["owner_run_id"] = ROOT


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("retained", "error"),
    [
        (None, None),
        ("checkpoint", "transition_checkpoint_changed"),
        ("input", "transition_input_changed"),
        ("different-failure", "transition_checkpoint_changed"),
        ("canceled", "transition_checkpoint_changed"),
    ],
)
async def test_vsp_pre_capture_failure_transitions_only_an_empty_original_candidate(case, retained, error):
    _retain_original_root(case, "canceled" if retained == "canceled" else "failed")
    case.runs[0]["source_ids"] = [SOURCE_ID]
    candidate = case.header["candidate_storage_json"]
    candidate.update(ordinal=0, resets={"*": 0})
    metadata = candidate["identity"]["metadata"]
    metadata.update(
        source_ids=[SOURCE_ID], selected_resources=list(RESOURCE_TYPES), expected_resources=list(RESOURCE_TYPES)
    )
    case.header.update(
        status="failed",
        resource_count=0,
        publication_metadata_json={**metadata, "error": "vsp_prepared_candidate_required"},
    )
    if retained != "checkpoint":
        case.checkpoints.clear()
    if retained != "input":
        case.input_rows.clear()
    if retained == "different-failure":
        case.header["publication_metadata_json"]["error"] = "unrelated failure"
    expected_counts = dict.fromkeys(RESOURCE_TYPES, 0)
    if error:
        with pytest.raises(GenericCandidateError, match=f"^provider_directory_generic_source_{error}$"):
            await _transition(case, expected_counts, stopped_run_id=ROOT)
        assert not case.writes
        return
    receipt = await _transition(case, expected_counts, stopped_run_id=ROOT)
    assert receipt["rows"] == 0 and receipt["lineage_run_ids"] == [ROOT]
    assert case.header["status"] == "acquiring"
    assert case.header["candidate_storage_json"] == {**candidate, "storage": case.new}
    assert (await _transition(case, expected_counts, stopped_run_id=ROOT))["already_applied"] is True
    assert len(case.writes) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("drift", "error"),
    [
        (None, None),
        ("wrong_error", "transition_run_changed"),
        ("active_child", "transition_run_changed"),
        ("retained_input", "transition_input_changed"),
    ],
)
async def test_rearmed_vsp_root_transitions_after_exact_pre_capture_child_failure(case, drift, error):
    case.runs[0]["error_message"] = (
        "unrelated error"
        if drift == "wrong_error"
        else "provider_directory_generic_source_isolated_relation_override_changed"
    )
    case.runs[1]["root_run_id"] = None
    for run in case.runs:
        run["source_ids"] = [SOURCE_ID]
    if drift == "active_child":
        case.runs[0]["status"] = "running"
    candidate = case.header["candidate_storage_json"]
    candidate.update(ordinal=0, resets={"*": 0})
    metadata = candidate["identity"]["metadata"]
    metadata.update(
        source_ids=[SOURCE_ID], selected_resources=list(RESOURCE_TYPES), expected_resources=list(RESOURCE_TYPES)
    )
    case.header.update(
        import_run_id=ROOT,
        status="acquiring",
        resource_count=0,
        publication_metadata_json={**metadata, "error": "vsp_prepared_candidate_required"},
    )
    case.checkpoints.clear()
    if drift != "retained_input":
        case.input_rows.clear()
    counts_by_resource = dict.fromkeys(RESOURCE_TYPES, 0)
    if error:
        with pytest.raises(GenericCandidateError, match=f"^provider_directory_generic_source_{error}$"):
            await _transition(case, counts_by_resource)
        assert case.writes == []
        return
    receipt = await _transition(case, counts_by_resource)
    assert receipt["rows"] == 0 and receipt["lineage_run_ids"] == [ROOT, FAILED]
    assert case.header["status"] == "acquiring" and case.header["import_run_id"] == ROOT
    assert case.header["candidate_storage_json"] == {**candidate, "storage": case.new}
    assert (await _transition(case, counts_by_resource))["already_applied"] is True
    assert len(case.writes) == 1


def _empty_vsp_artifact_failure(case):
    """Retain the original root, a preattachment child and the latest attached child."""
    candidate = case.header["candidate_storage_json"]
    candidate.update(ordinal=0, resets={"*": 0})
    metadata = candidate["identity"]["metadata"]
    metadata.update(
        source_ids=[SOURCE_ID], selected_resources=list(RESOURCE_TYPES), expected_resources=list(RESOURCE_TYPES)
    )
    case.runs[0]["error_message"] = "provider_directory_generic_source_isolated_relation_override_changed"
    case.runs[-1]["root_run_id"] = None
    case.runs[-1]["error_message"] = "vsp_prepared_candidate_required"
    case.runs.insert(0, {**case.runs[0], "run_id": "run_" + "c" * 32, "retry_of_run_id": FAILED})
    case.runs[0]["error_message"] = "vsp_artifact_root_invalid"
    for run in case.runs:
        run["source_ids"] = [SOURCE_ID]
    case.header.update(
        status="failed",
        resource_count=0,
        import_run_id=case.runs[0]["run_id"],
        publication_metadata_json={**deepcopy(metadata), "error": "vsp_artifact_root_invalid"},
    )
    case.checkpoints.clear()
    case.input_rows.clear()
    return case.runs[0]["run_id"]


def _empty_vsp_permission_failure(case, monkeypatch, tmp_path):
    previous_run_id = _empty_vsp_artifact_failure(case)
    root = tmp_path / "artifacts"
    monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_ARTIFACT_ROOT", str(root))
    error = str(PermissionError(errno.EACCES, os.strerror(errno.EACCES), str(root / "vsp-directory" / "tmp")))
    failed_run_id = "run_" + "d" * 32
    case.runs.insert(
        0,
        {
            **case.runs[0],
            "run_id": failed_run_id,
            "retry_of_run_id": previous_run_id,
            "error_message": error,
        },
    )
    case.header["import_run_id"] = failed_run_id
    case.header["publication_metadata_json"]["error"] = error
    return failed_run_id


@pytest.mark.asyncio
@pytest.mark.parametrize("historical", [False, True])
async def test_vsp_artifact_failure_rearms_only_the_latest_attached_child_and_replays(
    case, monkeypatch, tmp_path, historical
):
    failed_run_id = (
        _empty_vsp_permission_failure(case, monkeypatch, tmp_path) if historical else _empty_vsp_artifact_failure(case)
    )
    before = deepcopy(case.header)
    runs = deepcopy(case.runs)
    counts = dict.fromkeys(RESOURCE_TYPES, 0)
    result = await _transition(case, counts, stopped_run_id=failed_run_id)
    assert result["rows"] == 0 and result["lineage_run_ids"] == sorted(run["run_id"] for run in runs)
    assert case.header == {
        **before,
        "status": "acquiring",
        "candidate_storage_json": {**before["candidate_storage_json"], "storage": case.new},
    }
    assert (await _transition(case, counts, stopped_run_id=failed_run_id))["already_applied"] is True
    assert len(case.writes) == 1 and case.runs == runs
    assert not case.checkpoints and not case.bulk_checkpoints and not case.input_rows


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["header_error", "run_error", "capture_ancestor", "sibling", "wrong_path"])
async def test_vsp_permission_failure_keeps_exact_error_and_pre_capture_lineage(case, monkeypatch, tmp_path, drift):
    failed_run_id = _empty_vsp_permission_failure(case, monkeypatch, tmp_path)
    match drift:
        case "header_error":
            case.header["publication_metadata_json"]["error"] = "vsp_artifact_root_invalid"
        case "run_error":
            case.runs[0]["error_message"] = "vsp_artifact_root_invalid"
        case "capture_ancestor":
            case.runs[1]["error_message"] = "vsp_http_status"
        case "sibling":
            case.runs.insert(1, {**case.runs[0], "run_id": "run_" + "e" * 32})
        case "wrong_path":
            monkeypatch.setenv("HLTHPRT_PROVIDER_DIRECTORY_ARTIFACT_ROOT", str(tmp_path / "different"))
    before = deepcopy((case.header, case.runs, case.input_rows, case.checkpoints))
    with pytest.raises(GenericCandidateError):
        await _transition(case, dict.fromkeys(RESOURCE_TYPES, 0), stopped_run_id=failed_run_id)
    assert not case.writes
    assert (case.header, case.runs, case.input_rows, case.checkpoints) == before


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "drift",
    ["capture", "root_capture", "unknown", "unfinished", "canceled", "sibling", "preparation", "preattachment"],
)
async def test_vsp_artifact_failure_rejects_unproven_lineage(case, drift):
    failed_run_id = _empty_vsp_artifact_failure(case)
    ancestor = case.runs[1]
    match drift:
        case "capture" | "root_capture":
            case.runs[-1 if drift == "root_capture" else 1]["error_message"] = "vsp_http_status"
        case "unknown":
            ancestor["error_message"] = None
        case "unfinished":
            ancestor["finished_at"] = None
        case "canceled":
            ancestor["status"] = "canceled"
        case "preparation":
            ancestor["error_message"] = "vsp_prepared_candidate_required"
        case "sibling" | "preattachment":
            other_run_id = "run_" + "d" * 32
            case.runs.insert(2, {**ancestor, "run_id": other_run_id, "error_message": "vsp_artifact_root_invalid"})
            if drift == "preattachment":
                ancestor["retry_of_run_id"] = other_run_id
    before = deepcopy((case.header, case.runs, case.input_rows, case.checkpoints, case.bulk_checkpoints))
    with pytest.raises(GenericCandidateError, match="^provider_directory_generic_source_transition_run_changed$"):
        await _transition(case, dict.fromkeys(RESOURCE_TYPES, 0), stopped_run_id=failed_run_id)
    assert not case.writes
    assert (case.header, case.runs, case.input_rows, case.checkpoints, case.bulk_checkpoints) == before


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "drift",
    [
        "header_error",
        "run_error",
        "owner",
        "endpoint",
        "root",
        "source",
        "metadata",
        "producer",
        "acquiring",
        "canceled",
        "running",
        "unfinished",
        "latest",
        "parent",
        "ordinal",
        "reset",
        "relationship",
        "header_rows",
        "input",
        "checkpoint",
        "bulk_checkpoint",
        "witness",
    ],
)
async def test_vsp_artifact_failure_refuses_changed_failure_identity_or_retained_state(case, drift):
    failed_run_id = _empty_vsp_artifact_failure(case)
    candidate = case.header["candidate_storage_json"]
    match drift:
        case "header_error":
            case.header["publication_metadata_json"]["error"] = "vsp_http_status"
        case "run_error":
            case.runs[0]["error_message"] = "vsp_http_status"
        case "owner":
            case.header["import_run_id"] = FAILED
        case "endpoint" | "root":
            case.header["endpoint_id" if drift == "endpoint" else "acquisition_root_run_id"] = "other"
        case "source":
            candidate["identity"]["metadata"]["source_ids"] = ["other-source"]
        case "metadata":
            case.header["publication_metadata_json"]["resource_hash_contract"] = "changed"
        case "producer":
            candidate["storage"] = {**case.old, "producer": ["d" * 40, "sha256:" + "e" * 64]}
        case "acquiring":
            case.header["status"] = "acquiring"
        case "canceled" | "running":
            case.runs[0]["status"] = drift
        case "unfinished":
            case.runs[0]["finished_at"] = None
        case "latest":
            case.runs.insert(0, {**case.runs[0], "run_id": "run_" + "d" * 32})
        case "parent":
            case.runs[0]["retry_of_run_id"] = "run_" + "d" * 32
        case "ordinal":
            candidate["ordinal"] = 1
        case "reset":
            candidate["resets"] = {"Organization": 0}
        case "relationship":
            candidate["relationship_sha256"] = "d" * 64
        case "header_rows":
            case.header["resource_count"] = 1
        case "input":
            case.input_rows.append(dict(dataset_id=DATASET, resource_type="Organization", total=1, active=1))
        case "checkpoint":
            case.checkpoints.append({})
        case "bulk_checkpoint":
            case.bulk_checkpoints.append({})
        case "witness":
            case.connection.fetchval.side_effect = lambda sql, *_args: (
                "serializable" if sql == "SHOW transaction_isolation" else not sql.startswith("SELECT NOT EXISTS")
            )
    before = deepcopy(case.header)
    with pytest.raises(GenericCandidateError):
        await _transition(case, dict.fromkeys(RESOURCE_TYPES, 0), stopped_run_id=failed_run_id)
    assert not case.writes and case.header == before


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fixture_name,status", [("case", "failed"), ("bulk_case", "failed"), ("bulk_case", "canceled")]
)
async def test_original_root_without_self_reference_transitions_and_replays(request, fixture_name, status):
    case = request.getfixturevalue(fixture_name)
    _retain_original_root(case, status)
    expected_counts_by_resource = (
        {"Organization": 3} if fixture_name == "case" else {"Organization": 3, "Location": 2, "Endpoint": 0}
    )
    evidence = deepcopy((case.checkpoints, case.bulk_checkpoints, case.bulk_outputs, case.input_rows, case.runs))
    before = deepcopy(case.header["candidate_storage_json"])
    result = await _transition(case, expected_counts_by_resource, stopped_run_id=ROOT)
    assert result["lineage_run_ids"] == [ROOT] and result["rows"] == sum(expected_counts_by_resource.values())
    assert result["already_applied"] is False
    assert case.header["candidate_storage_json"] == {**before, "storage": case.new}
    assert (await _transition(case, expected_counts_by_resource, stopped_run_id=ROOT))["already_applied"] is True
    assert len(case.writes) == 1
    assert (case.checkpoints, case.bulk_checkpoints, case.bulk_outputs, case.input_rows, case.runs) == evidence


@pytest.mark.asyncio
@pytest.mark.parametrize("root_reference", [None, "run_" + "c" * 32])
async def test_retry_child_still_requires_its_exact_root_reference(case, root_reference):
    case.runs[0]["root_run_id"] = root_reference
    with pytest.raises(GenericCandidateError, match="transition_run_changed"):
        await _transition(case)
    assert case.writes == []


@pytest.mark.asyncio
async def test_original_root_rejects_a_foreign_self_reference(case):
    _retain_original_root(case, "failed")
    case.runs[0]["root_run_id"] = "run_" + "c" * 32
    with pytest.raises(GenericCandidateError, match="transition_run_changed"):
        await _transition(case, stopped_run_id=ROOT)
    assert case.writes == []


def _include_active_collection(case):
    """Retain an opaque cursor and the exact already-committed input census."""
    metadata = case.header["candidate_storage_json"]["identity"]["metadata"]
    metadata["selected_resources"] = ["Organization", "Location"]
    metadata["expected_resources"] = ["Organization", "Location"]
    case.header["publication_metadata_json"] = deepcopy(metadata)
    case.checkpoints.append(
        {
            **case.checkpoints[0],
            "resource_type": "Location",
            "state": "active",
            "rows_processed": 2,
            "completed_at": None,
            "start_url_hash": hashlib.sha256(b"https://example.test/fhir/Location?_count=100").hexdigest(),
            "next_url": "https://example.test/fhir/Location?_getpages=opaque%2Btoken&_count=100",
        }
    )
    case.input_rows.append(dict(dataset_id=DATASET, resource_type="Location", total=2, active=2))


@pytest.mark.asyncio
async def test_mixed_checkpoint_transition_and_replay_preserve_all_acquisition_evidence(case):
    _include_active_collection(case)
    checkpoints, inputs, runs = deepcopy((case.checkpoints, case.input_rows, case.runs))
    before = deepcopy(case.header["candidate_storage_json"])
    result = await _transition(case, {"Organization": 3, "Location": 2})
    assert result["rows"] == 5 and result["already_applied"] is False
    assert case.header["candidate_storage_json"] == {**before, "storage": case.new}
    assert (await _transition(case, {"Organization": 3, "Location": 2}))["already_applied"] is True
    assert len(case.writes) == 1
    assert (case.checkpoints, case.input_rows, case.runs) == (checkpoints, inputs, runs)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("state", "failed"),
        ("completed_at", "now"),
        ("pages_processed", 0),
        ("pages_processed", -1),
        ("rows_processed", 1),
        ("dataset_id", "another-dataset"),
        ("owner_run_id", ROOT),
        ("next_url", None),
        ("next_url", ""),
        ("next_url", " https://example.test/fhir/Location?cursor=one"),
        ("next_url", "https://example.test/fhir/Location?cursor=one\n"),
        ("next_url", "/fhir/Location?cursor=one"),
        ("next_url", "http://example.test/fhir/Location?cursor=one"),
        ("next_url", "https://elsewhere.test/fhir/Location?cursor=one"),
        ("next_url", "https://user@example.test/fhir/Location?cursor=one"),
        ("next_url", "https://example.test/fhir/Location?cursor=one#fragment"),
        ("next_url", "https://example.test:invalid/fhir/Location?cursor=one"),
        ("next_url", "https://[invalid/fhir/Location?cursor=one"),
        ("next_url", "https://example.test/fhir/Location?_count=100"),
    ],
)
async def test_malformed_active_checkpoint_refuses_before_any_candidate_write(case, field, value):
    _include_active_collection(case)
    case.checkpoints[1][field] = value
    with pytest.raises(GenericCandidateError, match="transition_checkpoint_changed"):
        await _transition(case, {"Organization": 3, "Location": 2})
    assert case.writes == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "drift", ["missing", "duplicate", "complete_without_time", "complete_without_pages", "inactive"]
)
async def test_mixed_transition_preserves_complete_checkpoint_and_full_input_requirements(case, drift):
    _include_active_collection(case)
    reason = "transition_checkpoint_changed"
    if drift == "missing":
        case.checkpoints.pop()
    elif drift == "duplicate":
        case.checkpoints.append(deepcopy(case.checkpoints[1]))
    elif drift == "complete_without_time":
        case.checkpoints[0]["completed_at"] = None
    elif drift == "complete_without_pages":
        case.checkpoints[0]["pages_processed"] = 0
    else:
        case.input_rows[1]["active"] = 1
        reason = "transition_input_changed"
    with pytest.raises(GenericCandidateError, match=reason):
        await _transition(case, {"Organization": 3, "Location": 2})
    assert case.writes == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "drift,reason",
    [
        ("active", "transition_run_changed"),
        ("newest", "transition_run_changed"),
        ("cycle", "transition_run_changed"),
        ("checkpoint", "transition_checkpoint_changed"),
        ("row_count", "transition_input_changed"),
        ("metadata", "transition_candidate_changed"),
        ("header_owner", "transition_candidate_changed"),
        ("producer", "transition_storage_changed"),
        ("identity", "transition_candidate_changed"),
    ],
)
async def test_drift_refuses_without_candidate_write(case, drift, reason):
    match drift:
        case "active":
            case.runs[1]["status"] = "running"
        case "newest":
            case.runs.reverse()
        case "cycle":
            case.runs[0]["retry_of_run_id"] = FAILED
        case "checkpoint":
            case.checkpoints[0]["rows_processed"] = 2
        case "row_count":
            case.input_rows[0]["total"] = 2
        case "metadata":
            case.header["publication_metadata_json"] = {}
        case "header_owner":
            case.header["import_run_id"] = ROOT
        case "producer":
            case.new["catalog_schema"] = "other_schema"
        case "identity":
            case.header["endpoint_id"] = "other-endpoint"
    with pytest.raises(GenericCandidateError, match=reason):
        await _transition(case)
    assert case.writes == []


@pytest.mark.asyncio
async def test_legacy_empty_candidate_refuses_without_candidate_write(case):
    case.header.update(status="acquiring", resource_count=0)
    candidate = case.header["candidate_storage_json"]
    candidate["contract"] = storage.CONTRACT
    del candidate["storage"], candidate["admission"]

    with pytest.raises(GenericCandidateError, match="^provider_directory_generic_source_transition_candidate_changed$"):
        await _transition(case)
    assert case.writes == []


@pytest.mark.asyncio
async def test_native_table_identity_failure_refuses_before_write(case):
    case.tables.side_effect = GenericCandidateError("native_table_changed")
    with pytest.raises(GenericCandidateError, match="native_table_changed"):
        await _transition(case)
    assert case.writes == []


@pytest.mark.asyncio
async def test_candidate_compare_race_refuses_without_replacing_storage(case):
    before = deepcopy(case.header["candidate_storage_json"])
    case.connection.execute = AsyncMock(return_value="UPDATE 0")
    with pytest.raises(GenericCandidateError, match="transition_header_compare_failed"):
        await _transition(case)
    assert case.header["candidate_storage_json"] == before


@pytest.mark.asyncio
async def test_transition_requires_independent_serializable_administrator(case):
    case.connection.fetchval = AsyncMock(return_value="read committed")
    with pytest.raises(GenericCandidateError, match="transition_isolation_required"):
        await _transition(case)
    assert case.writes == []


@pytest.mark.asyncio
async def test_failed_run_worker_action_lock_refuses_a_concurrent_launch(case):
    async def fetchval(sql, *args):
        if sql == "SHOW transaction_isolation":
            return "serializable"
        return not (args and args[0] == "control-run-worker-action:v1:" + FAILED)

    case.connection.fetchval = AsyncMock(side_effect=fetchval)
    with pytest.raises(GenericCandidateError, match="transition_worker_action_busy"):
        await _transition(case)
    assert case.writes == []


def _bulk_evidence(resource, state, counts, lengths, committed):
    """Build URL-free completed and partial export evidence for the same candidate."""
    directory = storage.import_module("process.provider_directory_fhir")
    start = "https://example.test/fhir/$export?_type=" + resource
    identity_by_field = dict(
        canonical_api_base="https://example.test/fhir",
        resource_type=resource,
        source_scope_hash="5" * 64,
        strategy_version=directory.BULK_EXPORT_CHECKPOINT_STRATEGY_VERSION,
        acquisition_root_run_id=ROOT,
        endpoint_id=ENDPOINT,
        dataset_id=DATASET,
        start_url_hash=hashlib.sha256(start.encode()).hexdigest(),
    )
    checkpoint_id = directory._identity_hash(identity_by_field)
    outputs = [
        dict(
            checkpoint_id=checkpoint_id,
            output_id=hashlib.sha256(f"{resource}:{index}".encode()).hexdigest(),
            output_index=index,
            resource_type=resource,
            output_url_hash=hashlib.sha256(f"synthetic-output:{resource}:{index}".encode()).hexdigest(),
            state="complete" if size == length else "streaming" if count else "pending",
            rows_written=count,
            content_length_bytes=length,
            committed_bytes=size,
            etag_hash="6" * 64,
            validator_checked_at="before",
            completed_at="now" if size == length else None,
            error=None,
            has_output_url=size != length,
            has_etag=size != length,
        )
        for index, (count, length, size) in enumerate(zip(counts, lengths, committed, strict=True))
    ]
    checkpoint_by_field = dict(
        **identity_by_field,
        checkpoint_id=checkpoint_id,
        owner_run_id=FAILED,
        state=state,
        rows_written=sum(counts),
        manifest_hash="7" * 64,
        failed_at=None,
        completed_at="now" if state == "complete" else None,
        error=None,
        manifest_json=dict(
            transactionTime="2026-01-01T00:00:00Z",
            request_hash=identity_by_field["start_url_hash"],
            requiresAccessToken=False,
            output=[dict(type=resource, url_hash=output["output_url_hash"]) for output in outputs],
            deleted=[],
        ),
    )
    return checkpoint_by_field, outputs


@pytest.fixture
def bulk_case(case):
    case.checkpoints.clear()
    metadata = case.header["candidate_storage_json"]["identity"]["metadata"]
    metadata.update(
        selected_resources=["Organization", "Location", "Endpoint"],
        expected_resources=["Organization", "Location", "Endpoint"],
    )
    case.header["publication_metadata_json"] = deepcopy(metadata)
    for evidence in (
        _bulk_evidence("Organization", "complete", [1, 2], [60, 120], [60, 120]),
        _bulk_evidence("Location", "streaming", [2, 0], [360, 240], [120, 0]),
    ):
        checkpoint, outputs = evidence
        case.bulk_checkpoints.append(checkpoint)
        case.bulk_outputs.extend(outputs)
    case.input_rows.append(dict(dataset_id=DATASET, resource_type="Location", total=2, active=2))
    return case


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["failed", "canceled"])
async def test_bulk_transition_preserves_completed_partial_and_unstarted_resources(bulk_case, status):
    bulk_case.runs[0]["status"] = status
    before = deepcopy((bulk_case.bulk_checkpoints, bulk_case.bulk_outputs, bulk_case.input_rows, bulk_case.runs))
    candidate = deepcopy(bulk_case.header["candidate_storage_json"])
    expected_counts_by_resource = {"Organization": 3, "Location": 2, "Endpoint": 0}
    receipt = await _transition(bulk_case, expected_counts_by_resource)
    assert receipt["rows"] == 5 and receipt["already_applied"] is False
    assert bulk_case.header["candidate_storage_json"] == {**candidate, "storage": bulk_case.new}
    assert (await _transition(bulk_case, expected_counts_by_resource))["already_applied"] is True
    assert len(bulk_case.writes) == 1
    assert (bulk_case.bulk_checkpoints, bulk_case.bulk_outputs, bulk_case.input_rows, bulk_case.runs) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("completed_owner", [ROOT, "run_" + "c" * 32])
async def test_bulk_transition_reuses_completed_export_from_verified_ancestor(bulk_case, completed_owner):
    predecessor = "run_" + "c" * 32
    bulk_case.runs[0]["retry_of_run_id"] = predecessor
    bulk_case.runs.insert(1, {**bulk_case.runs[-1], "run_id": predecessor, "retry_of_run_id": ROOT})
    bulk_case.bulk_checkpoints[0]["owner_run_id"] = completed_owner
    before = deepcopy((bulk_case.bulk_checkpoints, bulk_case.bulk_outputs))

    receipt = await _transition(bulk_case, {"Organization": 3, "Location": 2, "Endpoint": 0})

    assert receipt["lineage_run_ids"] == sorted([ROOT, predecessor, FAILED])
    assert receipt["rows"] == 5
    assert (bulk_case.bulk_checkpoints, bulk_case.bulk_outputs) == before


@pytest.mark.asyncio
async def test_bulk_transition_rejects_completed_export_owned_by_sibling_retry(bulk_case):
    sibling = "run_" + "c" * 32
    bulk_case.runs.insert(1, {**bulk_case.runs[0], "run_id": sibling, "retry_of_run_id": ROOT})
    bulk_case.bulk_checkpoints[0]["owner_run_id"] = sibling

    with pytest.raises(GenericCandidateError, match="transition_bulk_checkpoint_changed"):
        await _transition(bulk_case, {"Organization": 3, "Location": 2, "Endpoint": 0})
    assert bulk_case.writes == []


@pytest.mark.asyncio
async def test_bulk_transition_receipt_retains_terminal_siblings_for_worker_presence_checks(bulk_case):
    sibling = "run_" + "c" * 32
    bulk_case.runs.insert(1, {**bulk_case.runs[0], "run_id": sibling, "retry_of_run_id": ROOT})
    bulk_case.bulk_checkpoints[0]["owner_run_id"] = ROOT

    receipt = await _transition(bulk_case, {"Organization": 3, "Location": 2, "Endpoint": 0})

    assert receipt["lineage_run_ids"] == sorted([ROOT, FAILED, sibling])


def _retain_reordered_bulk_manifest(transition_case, *, omit_format, legacy, query_suffix=""):
    """Use genuine parser/audit output while preserving the saved file boundaries."""
    directory = storage.import_module("process.provider_directory_fhir")
    checkpoint = transition_case.bulk_checkpoints[1]
    start = directory._bulk_export_start_url(
        {
            "api_base": checkpoint["canonical_api_base"],
            "metadata_json": {"provider_directory_bulk_export_omit_output_format": omit_format},
        },
        "Location",
    )
    start += query_suffix
    parsed_request = directory.urllib.parse.urlsplit(start)
    query = directory.urllib.parse.urlencode(list(reversed(directory.urllib.parse.parse_qsl(parsed_request.query))))
    manifest_by_field = dict(
        transactionTime="2026-01-01T00:00:00Z",
        requiresAccessToken=False,
        request=directory.urllib.parse.urlunsplit(parsed_request._replace(query=query)),
        output=[dict(type="Location", url=f"https://example.test/output/{i}") for i in range(2)],
    )
    manifest = directory._bulk_export_manifest_from_payload(manifest_by_field, "Location", expected_request_url=start)
    audit = directory._bulk_manifest_audit_payload(manifest.payload)
    if legacy:
        audit.pop("normalized_request_hash")
    checkpoint.update(
        start_url_hash=hashlib.sha256(start.encode()).hexdigest(),
        manifest_json=audit,
        manifest_hash=manifest.identity_hash,
    )
    fields = (
        "canonical_api_base",
        "resource_type",
        "source_scope_hash",
        "strategy_version",
        "acquisition_root_run_id",
        "endpoint_id",
        "dataset_id",
        "start_url_hash",
    )
    checkpoint["checkpoint_id"] = directory._identity_hash({key: checkpoint[key] for key in fields})
    for output, manifest_output in zip(transition_case.bulk_outputs[2:], manifest.outputs, strict=True):
        output.update(checkpoint_id=checkpoint["checkpoint_id"], output_url_hash=manifest_output.url_hash)


@pytest.mark.asyncio
@pytest.mark.parametrize("omit_format", [False, True])
@pytest.mark.parametrize("legacy", [False, True])
async def test_bulk_transition_preserves_reordered_request_without_decryption_keys(
    bulk_case, monkeypatch, omit_format, legacy
):
    monkeypatch.delenv("HLTHPRT_PROVIDER_DIRECTORY_CHECKPOINT_KEY", raising=False)
    monkeypatch.delenv("HLTHPRT_CONTROL_API_TOKEN", raising=False)
    _retain_reordered_bulk_manifest(bulk_case, omit_format=omit_format, legacy=legacy)
    before = deepcopy((bulk_case.bulk_checkpoints, bulk_case.bulk_outputs))

    receipt = await _transition(bulk_case, {"Organization": 3, "Location": 2, "Endpoint": 0})

    assert receipt["rows"] == 5 and receipt["already_applied"] is False
    assert (bulk_case.bulk_checkpoints, bulk_case.bulk_outputs) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("commitment", [None, "invalid", "8" * 64])
async def test_bulk_transition_never_falls_back_from_invalid_canonical_commitment(bulk_case, commitment):
    checkpoint = bulk_case.bulk_checkpoints[1]
    checkpoint["manifest_json"]["normalized_request_hash"] = commitment

    with pytest.raises(GenericCandidateError, match="transition_bulk_checkpoint_changed"):
        await _transition(bulk_case, {"Organization": 3, "Location": 2, "Endpoint": 0})
    assert bulk_case.writes == []


@pytest.mark.asyncio
async def test_bulk_transition_rejects_unrecognized_start_even_with_consistent_manifest(bulk_case):
    """A retained delta query must not stand in for either supported full export."""
    _retain_reordered_bulk_manifest(
        bulk_case, omit_format=False, legacy=False, query_suffix="&_since=2025-12-31T00:00:00Z"
    )
    before = deepcopy((bulk_case.header, bulk_case.bulk_checkpoints, bulk_case.bulk_outputs, bulk_case.input_rows))
    with pytest.raises(
        GenericCandidateError, match="^provider_directory_generic_source_transition_bulk_checkpoint_changed$"
    ):
        await _transition(bulk_case, {"Organization": 3, "Location": 2, "Endpoint": 0})
    assert bulk_case.writes == []
    assert (bulk_case.header, bulk_case.bulk_checkpoints, bulk_case.bulk_outputs, bulk_case.input_rows) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("original_root", [False, True])
async def test_canceled_rest_candidate_remains_ineligible(case, original_root):
    if original_root:
        _retain_original_root(case, "canceled")
    else:
        case.runs[0]["status"] = "canceled"
    with pytest.raises(GenericCandidateError, match="transition_run_changed"):
        await _transition(case, stopped_run_id=ROOT if original_root else FAILED)
    assert case.writes == []


@pytest.mark.asyncio
async def test_bulk_transition_selects_only_the_requested_manifest_resource(bulk_case):
    for checkpoint in bulk_case.bulk_checkpoints:
        checkpoint["manifest_json"]["output"].insert(0, {"type": "Practitioner", "url_hash": "a" * 64})
    receipt = await _transition(bulk_case, {"Organization": 3, "Location": 2, "Endpoint": 0})
    assert receipt["rows"] == 5 and receipt["already_applied"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["running", "canceling", "succeeded", "dead_letter"])
async def test_bulk_transition_rejects_other_run_states(bulk_case, status):
    bulk_case.runs[0]["status"] = status
    with pytest.raises(GenericCandidateError, match="transition_run_changed"):
        await _transition(bulk_case, {"Organization": 3, "Location": 2, "Endpoint": 0})
    assert bulk_case.writes == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value,expected_error",
    [
        ("acquisition_root_run_id", "run_" + "c" * 32, "transition_bulk_checkpoint_changed"),
        ("dataset_id", "another-dataset", "transition_bulk_checkpoint_changed"),
        ("endpoint_id", "another-endpoint", "transition_bulk_checkpoint_changed"),
        ("owner_run_id", ROOT, "transition_bulk_checkpoint_changed"),
        ("resource_type", "Practitioner", "transition_bulk_checkpoint_changed"),
        ("source_scope_hash", "8" * 64, "transition_bulk_checkpoint_changed"),
        ("canonical_api_base", "https://elsewhere.test/fhir", "transition_bulk_checkpoint_changed"),
        ("checkpoint_id", "9" * 64, "transition_bulk_output_changed"),
        ("start_url_hash", "9" * 64, "transition_bulk_checkpoint_changed"),
        ("strategy_version", "different", "transition_bulk_checkpoint_changed"),
        ("manifest_hash", None, "transition_bulk_checkpoint_changed"),
        ("manifest_json", {}, "transition_bulk_checkpoint_changed"),
        ("state", "failed", "transition_bulk_checkpoint_changed"),
        ("state", "starting", "transition_bulk_checkpoint_changed"),
        ("completed_at", "now", "transition_bulk_checkpoint_changed"),
        ("failed_at", "now", "transition_bulk_checkpoint_changed"),
        ("error", "invalid_ndjson", "transition_bulk_checkpoint_changed"),
        ("error", {}, "transition_bulk_checkpoint_changed"),
    ],
)
async def test_bulk_transition_rejects_foreign_or_unresumable_exports(bulk_case, field, value, expected_error):
    bulk_case.bulk_checkpoints[1][field] = value
    with pytest.raises(GenericCandidateError, match=rf"^provider_directory_generic_source_{expected_error}$"):
        await _transition(bulk_case, {"Organization": 3, "Location": 2, "Endpoint": 0})
    assert bulk_case.writes == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("checkpoint_id", "8" * 64),
        ("output_id", "invalid"),
        ("output_index", 1),
        ("resource_type", "Practitioner"),
        ("output_url_hash", "9" * 64),
        ("state", "failed"),
        ("completed_at", "now"),
        ("rows_written", 3),
        ("rows_written", -1),
        ("committed_bytes", -1),
        ("committed_bytes", 361),
        ("committed_bytes", 0),
        ("content_length_bytes", 0),
        ("etag_hash", "bad"),
        ("validator_checked_at", None),
        ("has_output_url", False),
        ("has_etag", False),
        ("error", "invalid_ndjson"),
    ],
)
async def test_bulk_transition_rejects_changed_file_evidence(bulk_case, field, value):
    bulk_case.bulk_outputs[2][field] = value
    with pytest.raises(GenericCandidateError, match="transition_bulk_output_changed"):
        await _transition(bulk_case, {"Organization": 3, "Location": 2, "Endpoint": 0})
    assert bulk_case.writes == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "drift,expected_error",
    [
        ("mixed", "transition_checkpoint_changed"),
        ("missing_export", "transition_bulk_checkpoint_changed"),
        ("missing_file", "transition_bulk_output_changed"),
        ("duplicate_export", "transition_bulk_checkpoint_changed"),
        ("duplicate_file", "transition_bulk_output_changed"),
        ("unstarted_rows", "transition_bulk_checkpoint_changed"),
        ("manifest_file", "transition_bulk_output_changed"),
        ("manifest_time", "transition_bulk_checkpoint_changed"),
        ("manifest_request", "transition_bulk_checkpoint_changed"),
        ("parent_total", "transition_bulk_output_changed"),
        ("completed_short", "transition_bulk_output_changed"),
        ("completed_unfinished", "transition_bulk_output_changed"),
        ("inactive_input", "transition_input_changed"),
    ],
)
async def test_bulk_transition_requires_the_complete_unchanged_census(bulk_case, drift, expected_error):
    expected_counts_by_resource = {"Organization": 3, "Location": 2, "Endpoint": 0}
    match drift:
        case "mixed":
            bulk_case.checkpoints.extend(_completed_input_evidence()[1])
        case "missing_export":
            bulk_case.bulk_checkpoints.pop()
        case "missing_file":
            bulk_case.bulk_outputs.pop()
        case "duplicate_export":
            bulk_case.bulk_checkpoints.append(deepcopy(bulk_case.bulk_checkpoints[0]))
        case "duplicate_file":
            bulk_case.bulk_outputs.append(deepcopy(bulk_case.bulk_outputs[0]))
        case "unstarted_rows":
            expected_counts_by_resource["Endpoint"] = 1
        case "manifest_file":
            bulk_case.bulk_checkpoints[1]["manifest_json"]["output"][0]["url_hash"] = "8" * 64
        case "manifest_time":
            bulk_case.bulk_checkpoints[1]["manifest_json"]["transactionTime"] = "invalid"
        case "manifest_request":
            bulk_case.bulk_checkpoints[1]["manifest_json"]["request_hash"] = "8" * 64
        case "parent_total":
            bulk_case.bulk_checkpoints[1]["rows_written"] = 1
        case "completed_short":
            bulk_case.bulk_outputs[0]["committed_bytes"] = 59
        case "completed_unfinished":
            bulk_case.bulk_outputs[0].update(state="streaming", completed_at=None, has_output_url=True, has_etag=True)
        case "inactive_input":
            bulk_case.input_rows[1]["active"] = 1
    with pytest.raises(GenericCandidateError, match=rf"^provider_directory_generic_source_{expected_error}$"):
        await _transition(bulk_case, expected_counts_by_resource)
    assert bulk_case.writes == []
