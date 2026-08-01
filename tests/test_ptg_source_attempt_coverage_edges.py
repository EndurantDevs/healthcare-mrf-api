"""Edge contracts for durable PTG source-attempt admission helpers."""
from __future__ import annotations
from contextlib import asynccontextmanager
from typing import Any
import pytest
from api import control_workers
from process.ptg_parts import ptg_source_attempt_actions as actions
from process.ptg_parts import ptg_source_attempt_guard as guard
from process.ptg_parts import ptg_source_worker_admission as worker_admission
SOURCE_ID = "synthetic-source-attempt-coverage"
RUN_ID = "synthetic-run-coverage"
class MappingResult:
    def __init__(self, outer_by_field: dict[str, Any] | None) -> None:
        self.outer_by_field = outer_by_field
    def mappings(self):
        return self
    def one_or_none(self):
        return self.outer_by_field
class ScalarResult:
    def __init__(self, scalar_value: Any) -> None:
        self.scalar_value = scalar_value
    def scalar(self):
        return self.scalar_value
class ExecuteAdapter:
    def __init__(self, returned_value: Any = None) -> None:
        self.returned_value = returned_value
        self.sql_calls: list[tuple[str, dict[str, Any]]] = []
    async def execute(self, statement, parameters):
        self.sql_calls.append((str(statement), parameters))
        return self.returned_value
class StatusAdapter:
    def __init__(
        self,
        returned_value: Any = None,
        *,
        rejects_kwargs: bool = False,
        error_text: str | None = None,
    ) -> None:
        self.returned_value = returned_value
        self.rejects_kwargs = rejects_kwargs
        self.error_text = error_text
        self.sql_calls: list[tuple[str, dict[str, Any]]] = []
    async def status(self, statement, **parameters):
        self.sql_calls.append((str(statement), parameters))
        if self.error_text is not None:
            raise TypeError(self.error_text)
        if self.rejects_kwargs and parameters:
            raise TypeError("unexpected keyword argument 'source_id'")
        return self.returned_value
def outer_run_by_field(
    *,
    importer: str = "ptg",
    status: str = "running",
    source_id: str | None = SOURCE_ID,
) -> dict[str, Any]:
    outer_by_field = {
        "run_id": RUN_ID,
        "importer": importer,
        "status": status,
        "params": {},
        "metrics": {},
        "import_id": source_id,
        "retry_of_run_id": None,
        "heartbeat_at": None,
    }
    if source_id is not None:
        outer_by_field["source_file_import_id"] = source_id
    return outer_by_field
def admission_contract(
    *,
    expected_source_id: str | None | object = actions._UNSET_SOURCE_ID,
    worker_selection: actions.PTGWorkerActionSelection | None = None,
    state_updates: dict[str, Any] | None = None,
) -> actions._SourceActionAdmission:
    return actions._SourceActionAdmission(
        event_kind="ensure_admitted",
        attempt_id="synthetic-attempt",
        state_updates=state_updates,
        expected_source_file_import_id=expected_source_id,
        worker_selection=worker_selection,
    )
def transaction_for(session):
    @asynccontextmanager
    async def transaction():
        yield session
    return transaction
@pytest.mark.parametrize(
    "invalid_source_id", (None, 1, "", " source", "source ", "x" * 65)
)
def test_identity_validation_rejects_noncanonical_values(invalid_source_id):
    with pytest.raises(ValueError, match="source_file_import_id"):
        guard.normalize_source_file_import_id(invalid_source_id)
def test_identity_payload_handles_missing_nested_and_conflicting_views():
    assert guard.source_file_import_id_from_payload(
        {"params": [], "metrics": "invalid"}, required=False
    ) is None
    with pytest.raises(ValueError, match="identity is required"):
        guard.source_file_import_id_from_payload({}, required=True)
    assert guard.source_file_import_id_from_payload(
        {
            "source_file_import_id": SOURCE_ID,
            "params": {"import_id": SOURCE_ID},
            "metrics": {"source_file_import_id": SOURCE_ID},
        },
        required=True,
    ) == SOURCE_ID
    with pytest.raises(ValueError, match="identity views conflict"):
        guard.source_file_import_id_from_payload(
            {"source_file_import_id": SOURCE_ID, "metrics": {"import_id": "different"}},
            required=True,
        )
def test_digest_lock_key_and_table_names_are_stable():
    assert guard.canonical_digest({"b": 2, "a": 1}) == guard.canonical_digest({"a": 1, "b": 2})
    assert guard.source_attempt_lock_key(SOURCE_ID).endswith(f":{SOURCE_ID}")
    expected_table = '"synthetic_schema"."synthetic_table"'
    assert guard._schema_table("synthetic_schema", "synthetic_table") == expected_table
    assert actions._schema_table("synthetic_schema", "synthetic_table") == expected_table
@pytest.mark.asyncio
@pytest.mark.parametrize("execute_adapter", (guard._execute, actions._execute_statement))
async def test_sql_adapters_support_execute_status_and_bound_fallback(execute_adapter):
    direct = ExecuteAdapter("execute-result")
    assert await execute_adapter(direct, "SELECT :source_id", {"source_id": SOURCE_ID}) == "execute-result"
    status = StatusAdapter("status-result")
    assert await execute_adapter(status, "SELECT :source_id", {"source_id": SOURCE_ID}) == "status-result"
    fallback = StatusAdapter("bound-result", rejects_kwargs=True)
    assert await execute_adapter(
        fallback, guard.text("SELECT :source_id"), {"source_id": SOURCE_ID}
    ) == "bound-result"
    assert len(fallback.sql_calls) == 2
@pytest.mark.asyncio
@pytest.mark.parametrize("execute_adapter", (guard._execute, actions._execute_statement))
async def test_sql_adapters_fail_closed_for_unsupported_executors(execute_adapter):
    with pytest.raises(TypeError, match="cannot execute SQL"):
        await execute_adapter(object(), "SELECT 1", {})
    with pytest.raises(TypeError, match="synthetic status failure"):
        await execute_adapter(StatusAdapter(error_text="synthetic status failure"), "SELECT 1", {})
@pytest.mark.asyncio
async def test_scalar_adapter_supports_dictionary_keyword_and_result_shapes():
    class DictionaryScalar:
        async def scalar(self, _statement, parameters):
            return parameters["scalar_value"]
    class KeywordScalar:
        async def scalar(self, _statement, *args, **parameters):
            if args:
                raise TypeError("dictionary shape unsupported")
            return parameters["scalar_value"]
    assert await guard._scalar(DictionaryScalar(), "SELECT 1", {"scalar_value": 3}) == 3
    assert await guard._scalar(KeywordScalar(), "SELECT 1", {"scalar_value": 4}) == 4
    assert await guard._scalar(ExecuteAdapter(ScalarResult(5)), "SELECT 1", {}) == 5
@pytest.mark.asyncio
async def test_attempt_guard_translates_only_known_fence_errors():
    successful = ExecuteAdapter()
    await guard.guard_source_attempt(successful, source_file_import_id=SOURCE_ID, schema_name="synthetic_schema")
    assert '"synthetic_schema".guard_ptg_source_attempt' in successful.sql_calls[0][0]
    class RejectingExecutor:
        def __init__(self, message: str) -> None:
            self.message = message
        async def execute(self, _statement, _parameters):
            raise RuntimeError(self.message)
    with pytest.raises(guard.PTGSourceAttemptFencedError):
        await guard.guard_source_attempt(
            RejectingExecutor("PTG2_LEGACY_V3_ATTEMPT_RECONCILED"), source_file_import_id=SOURCE_ID
        )
    with pytest.raises(RuntimeError, match="unrelated failure"):
        await guard.guard_source_attempt(
            RejectingExecutor("unrelated failure"), source_file_import_id=SOURCE_ID
        )
@pytest.mark.asyncio
async def test_capability_guard_requires_exact_requested_services():
    class CapabilityExecutor:
        def __init__(self, matched_count: int) -> None:
            self.matched_count = matched_count
            self.parameters: dict[str, Any] = {}
        async def scalar(self, _statement, parameters):
            self.parameters = parameters
            return self.matched_count
    healthcare = CapabilityExecutor(1)
    await guard.require_source_attempt_capabilities(
        healthcare, require_attempt_authority=False, schema_name="synthetic_schema"
    )
    assert healthcare.parameters["required_services"] == [guard.HEALTHCARE_SERVICE_NAME]
    authority = CapabilityExecutor(2)
    await guard.require_source_attempt_capabilities(authority, require_attempt_authority=True)
    assert authority.parameters["required_services"] == [
        guard.HEALTHCARE_SERVICE_NAME, guard.ATTEMPT_AUTHORITY_SERVICE_NAME
    ]
    with pytest.raises(RuntimeError, match="CAPABILITY_UNAVAILABLE"):
        await guard.require_source_attempt_capabilities(
            CapabilityExecutor(0), require_attempt_authority=False
        )
@pytest.mark.asyncio
async def test_outer_loader_renders_lock_and_handles_missing_rows():
    present = ExecuteAdapter(MappingResult(outer_run_by_field()))
    loaded = await actions._load_outer_run(
        present, outer_run_table='"synthetic"."import_run"', run_id=RUN_ID, lock_row=True
    )
    assert loaded == outer_run_by_field()
    assert "FOR UPDATE" in present.sql_calls[0][0]
    missing = ExecuteAdapter(MappingResult(None))
    assert await actions._load_outer_run(
        missing, outer_run_table='"synthetic"."import_run"', run_id=RUN_ID, lock_row=False
    ) is None
    assert "FOR UPDATE" not in missing.sql_calls[0][0]
@pytest.mark.asyncio
async def test_event_recording_validates_contract_and_normalizes_attempt(monkeypatch):
    event_calls: list[dict[str, Any]] = []
    async def capture_event(_executor, **event_by_field):
        event_calls.append(event_by_field)
    monkeypatch.setattr(actions, "_insert_action_event", capture_event)
    invalid_cases = [
        ("unknown", outer_run_by_field(), "event kind"),
        ("start_admitted", outer_run_by_field(source_id="different"), "identity changed"),
        ("start_admitted", {**outer_run_by_field(), "run_id": ""}, "requires outer run_id"),
    ]
    for event_kind, outer_by_field, error_match in invalid_cases:
        with pytest.raises(ValueError, match=error_match):
            await actions.record_source_attempt_event(
                object(), source_file_import_id=SOURCE_ID, event_kind=event_kind, outer_run=outer_by_field
            )
    digest = await actions.record_source_attempt_event(
        object(), source_file_import_id=SOURCE_ID, event_kind="start_admitted",
        outer_run={**outer_run_by_field(), "params": [], "metrics": "invalid"},
        attempt_id=" synthetic-attempt ", schema_name="synthetic_schema"
    )
    assert len(digest) == 64
    assert event_calls[0]["attempt_id"] == "synthetic-attempt"
    assert event_calls[0]["state_digest"] == digest
@pytest.mark.asyncio
async def test_event_insert_targets_resolved_append_only_table():
    executor = ExecuteAdapter()
    await actions._insert_action_event(
        executor, schema_name="synthetic_schema", source_file_import_id=SOURCE_ID,
        event_kind="retry_admitted", outer_run_id=RUN_ID, attempt_id=None, state_digest="a" * 64
    )
    statement, parameters = executor.sql_calls[0]
    assert 'INSERT INTO "synthetic_schema"."ptg_source_attempt_event"' in statement
    assert parameters["event_kind"] == "retry_admitted"
@pytest.mark.asyncio
async def test_state_updates_validate_and_encode_progress():
    outer_by_field = outer_run_by_field()
    no_updates = ExecuteAdapter()
    await actions._apply_state_updates(
        no_updates, outer_run_table='"synthetic"."import_run"', outer_run=outer_by_field,
        run_id=RUN_ID, state_updates=None
    )
    assert no_updates.sql_calls == []
    with pytest.raises(ValueError, match="not allowed"):
        await actions._apply_state_updates(
            no_updates, outer_run_table='"synthetic"."import_run"', outer_run=outer_by_field,
            run_id=RUN_ID, state_updates={"unexpected": True}
        )
    status_only = ExecuteAdapter()
    await actions._apply_state_updates(
        status_only, outer_run_table='"synthetic"."import_run"', outer_run=outer_by_field,
        run_id=RUN_ID, state_updates={"status": "running"}
    )
    progress_update = ExecuteAdapter()
    await actions._apply_state_updates(
        progress_update, outer_run_table='"synthetic"."import_run"', outer_run=outer_by_field,
        run_id=RUN_ID, state_updates={"status": "finalizing", "progress": {"total": 2, "done": 1}}
    )
    statement, parameters = progress_update.sql_calls[0]
    assert '"progress" = CAST(:progress AS json)' in statement
    assert parameters["progress"] == '{"done":1,"total":2}'
def test_locked_outer_run_rejects_identity_and_terminal_drift():
    cases = [
        (outer_run_by_field(source_id="different"), admission_contract(), RuntimeError),
        (outer_run_by_field(), admission_contract(expected_source_id="different"), actions.PTGSourceAttemptIdentityError),
        (outer_run_by_field(status="FAILED"), admission_contract(), actions.PTGSourceAttemptTerminalError),
    ]
    for outer_by_field, admission, error_type in cases:
        with pytest.raises(error_type):
            actions._validate_locked_outer_run(
                outer_by_field, source_file_import_id=SOURCE_ID, admission=admission
            )
@pytest.mark.parametrize(
    ("outer_by_field", "request_importer", "importers", "roles", "is_accepted"),
    (
        (outer_run_by_field(), "ptg", {"ptg"}, {"start"}, True),
        (outer_run_by_field(status="finalizing"), None, {"ptg"}, {"finish"}, True),
        (outer_run_by_field(importer=""), None, {"ptg"}, {"start"}, False),
        (outer_run_by_field(), "mrf", {"ptg"}, {"start"}, False),
        (outer_run_by_field(), None, {"mrf"}, {"start"}, False),
        (outer_run_by_field(), None, {"ptg"}, {"finish"}, False),
    ),
)
def test_worker_selector_requires_exact_importer_and_role(
    outer_by_field, request_importer, importers, roles, is_accepted
):
    selection = actions.PTGWorkerActionSelection(
        request_importer=request_importer,
        allowed_importers=frozenset(importers),
        allowed_roles=frozenset(roles),
    )
    if is_accepted:
        actions._validate_worker_selection(outer_by_field, selection)
    else:
        with pytest.raises(actions.PTGSourceAttemptIdentityError):
            actions._validate_worker_selection(outer_by_field, selection)
    actions._validate_worker_selection(outer_by_field, None)
@pytest.mark.asyncio
async def test_source_backed_action_handles_disappearance_and_success(monkeypatch):
    phase_names: list[str] = []
    async def mark_phase(phase_name):
        phase_names.append(phase_name)
    async def load_missing(*_args, **_kwargs):
        phase_names.append("load")
        return None
    monkeypatch.setattr(actions, "require_source_attempt_capabilities", lambda *_a, **_k: mark_phase("capability"))
    monkeypatch.setattr(actions, "guard_source_attempt", lambda *_a, **_k: mark_phase("guard"))
    monkeypatch.setattr(actions, "_load_outer_run", load_missing)
    arguments_by_name = dict(
        outer_run_table='"synthetic"."import_run"', run_id=RUN_ID,
        source_file_import_id=SOURCE_ID, admission=admission_contract()
    )
    assert await actions._admit_source_backed_action(object(), **arguments_by_name) is None
    assert phase_names == ["capability", "guard", "load"]
    outer_by_field = outer_run_by_field()
    monkeypatch.setattr(actions, "_load_outer_run", lambda *_a, **_k: mark_phase("load"))
    async def load_present(*_args, **_kwargs):
        phase_names.append("load")
        return outer_by_field
    monkeypatch.setattr(actions, "_load_outer_run", load_present)
    monkeypatch.setattr(actions, "_apply_state_updates", lambda *_a, **_k: mark_phase("update"))
    monkeypatch.setattr(actions, "record_source_attempt_event", lambda *_a, **_k: mark_phase("event"))
    phase_names.clear()
    assert await actions._admit_source_backed_action(object(), **arguments_by_name) is outer_by_field
    assert phase_names == ["capability", "guard", "load", "update", "event"]
@pytest.mark.asyncio
async def test_existing_outer_action_covers_early_returns_and_delegate(monkeypatch):
    with pytest.raises(ValueError, match="run_id is required"):
        await actions.admit_existing_outer_run_action(run_id=" ", event_kind="start_admitted")
    outer_rows = [None, outer_run_by_field(importer="mrf"), outer_run_by_field(source_id=None)]
    async def load_next(*_args, **_kwargs):
        return outer_rows.pop(0)
    monkeypatch.setattr(actions, "_load_outer_run", load_next)
    monkeypatch.setattr(actions.db, "transaction", transaction_for(object()))
    assert await actions.admit_existing_outer_run_action(run_id=RUN_ID, event_kind="start_admitted") is None
    assert (await actions.admit_existing_outer_run_action(run_id=RUN_ID, event_kind="start_admitted"))["importer"] == "mrf"
    assert (await actions.admit_existing_outer_run_action(run_id=RUN_ID, event_kind="start_admitted"))["importer"] == "ptg"
@pytest.mark.asyncio
async def test_existing_outer_action_rejects_request_drift_and_delegates(monkeypatch):
    async def load_outer(*_args, **_kwargs):
        return outer_run_by_field()
    admitted_contracts: list[actions._SourceActionAdmission] = []
    async def admit_source(*_args, **kwargs):
        admitted_contracts.append(kwargs["admission"])
        return outer_run_by_field()
    monkeypatch.setattr(actions, "_load_outer_run", load_outer)
    monkeypatch.setattr(actions, "_admit_source_backed_action", admit_source)
    monkeypatch.setattr(actions.db, "transaction", transaction_for(object()))
    with pytest.raises(actions.PTGSourceAttemptIdentityError):
        await actions.admit_existing_outer_run_action(
            run_id=RUN_ID, event_kind="start_admitted", expected_source_file_import_id="different"
        )
    outcome = await actions.admit_existing_outer_run_action(
        run_id=RUN_ID, event_kind="retry_admitted", expected_source_file_import_id=SOURCE_ID
    )
    assert outcome["source_file_import_id"] == SOURCE_ID
    assert admitted_contracts[0].event_kind == "retry_admitted"
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("run_id", "task_payload", "admitted_run", "reason"),
    (
        ("", {"source_file_import_id": " bad"}, None, "source_attempt_id_invalid"),
        (RUN_ID, {"source_file_import_id": " bad"}, None, "source_attempt_identity_mismatch"),
        ("", {"source_file_import_id": SOURCE_ID}, None, "source_attempt_run_id_required"),
        (RUN_ID, {}, None, "run_missing"),
        (RUN_ID, {}, {"importer": "mrf"}, "run_importer_mismatch"),
    ),
)
async def test_worker_start_returns_specific_skip_reasons(
    monkeypatch, run_id, task_payload, admitted_run, reason
):
    async def admit(**_kwargs):
        return admitted_run
    monkeypatch.setattr(worker_admission.source_actions, "admit_existing_outer_run_action", admit)
    outcome = await worker_admission.guard_ptg_worker_start(
        task_payload, run_id=run_id, attempt_id="synthetic-attempt"
    )
    expected_by_field = {"status": "skipped", "reason": reason}
    if run_id:
        expected_by_field["run_id"] = run_id
    assert outcome == expected_by_field
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("admission_error", "reason"),
    (
        (actions.PTGSourceAttemptIdentityError("identity"), "source_attempt_identity_mismatch"),
        (actions.PTGSourceAttemptFencedError("fenced"), "source_attempt_reconciled"),
        (actions.PTGSourceAttemptTerminalError("terminal"), "source_attempt_reconciled"),
    ),
)
async def test_worker_start_maps_guard_failures(monkeypatch, admission_error, reason):
    async def reject(**_kwargs):
        raise admission_error
    monkeypatch.setattr(worker_admission.source_actions, "admit_existing_outer_run_action", reject)
    assert await worker_admission.guard_ptg_worker_start({}, run_id=RUN_ID, attempt_id=None) == {
        "status": "skipped", "reason": reason, "run_id": RUN_ID
    }
@pytest.mark.asyncio
async def test_worker_start_allows_ordinary_and_exact_ptg_runs(monkeypatch):
    assert await worker_admission.guard_ptg_worker_start({}, run_id="", attempt_id=None) is None
    admission_calls: list[dict[str, Any]] = []
    async def admit(**kwargs):
        admission_calls.append(kwargs)
        return {"importer": "ptg"}
    monkeypatch.setattr(worker_admission.source_actions, "admit_existing_outer_run_action", admit)
    assert await worker_admission.guard_ptg_worker_start(
        {"source_file_import_id": SOURCE_ID}, run_id=RUN_ID, attempt_id="synthetic-attempt"
    ) is None
    assert admission_calls[0]["expected_source_file_import_id"] == SOURCE_ID
@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("admitted_run", "expected_message"),
    ((None, "control run was not found"), ({"importer": "mrf"}, "source-attempt identity requires a PTG import")),
)
async def test_worker_ensure_fails_closed_for_durable_mismatch(
    monkeypatch, admitted_run, expected_message
):
    async def admit(**_kwargs):
        return admitted_run
    monkeypatch.setattr(control_workers, "admit_existing_outer_run_action", admit)
    outcome = await control_workers._admit_worker_ensure(
        {"source_file_import_id": SOURCE_ID}, run_id=RUN_ID, importer="ptg",
        selected_specs=[control_workers._BY_QUEUE["arq:PTG"]]
    )
    assert outcome["status"] == "failed"
    assert outcome["message"] == expected_message
@pytest.mark.asyncio
async def test_worker_ensure_maps_terminal_and_allows_ordinary_run(monkeypatch):
    async def identity_error(**_kwargs):
        raise control_workers.PTGSourceAttemptIdentityError("identity")
    monkeypatch.setattr(control_workers, "admit_existing_outer_run_action", identity_error)
    invalid = await control_workers._admit_worker_ensure(
        {}, run_id=RUN_ID, importer="ptg", selected_specs=[control_workers._BY_QUEUE["arq:PTG"]]
    )
    assert invalid["message"] == "PTG source-attempt identity is invalid or changed"
    async def terminal(**_kwargs):
        raise control_workers.PTGSourceAttemptTerminalError("terminal run")
    monkeypatch.setattr(control_workers, "admit_existing_outer_run_action", terminal)
    rejected = await control_workers._admit_worker_ensure(
        {}, run_id=RUN_ID, importer="ptg", selected_specs=[control_workers._BY_QUEUE["arq:PTG"]]
    )
    assert rejected["message"] == "terminal run"
    monkeypatch.setattr(
        control_workers, "admit_existing_outer_run_action",
        lambda **_kwargs: _async_value({"importer": "mrf"})
    )
    assert await control_workers._admit_worker_ensure(
        {}, run_id=RUN_ID, importer="mrf", selected_specs=[control_workers._BY_QUEUE["arq:MRF"]]
    ) is None
async def _async_value(returned_value):
    return returned_value
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "worker_payload", ({"importer": "mrf", "queue": "arq:PTG"}, {"importer": "ptg", "queue": "arq:MRF"})
)
async def test_guarded_ensure_rejects_selector_conflicts(worker_payload):
    outcome = await control_workers.guarded_ensure_worker(worker_payload)
    assert outcome["status"] == "failed"
    assert outcome["message"] == "PTG worker selector conflicts with importer"
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "worker_payload", ({"source_file_import_id": SOURCE_ID}, {"source_file_import_id": " invalid"})
)
async def test_guarded_ensure_requires_run_for_source_identity(worker_payload):
    outcome = await control_workers.guarded_ensure_worker(worker_payload)
    assert outcome["status"] == "failed"
    assert outcome["message"] == "source-attempt worker launch requires run_id"
@pytest.mark.asyncio
async def test_guarded_ensure_delegates_or_returns_admission_failure(monkeypatch):
    async def admit_success(*_args, **_kwargs):
        return None
    async def thread_call(function, worker_payload):
        assert function is control_workers.ensure_worker
        return {"status": "already_running", "items": [], "payload": worker_payload}
    monkeypatch.setattr(control_workers, "_admit_worker_ensure", admit_success)
    monkeypatch.setattr(control_workers.asyncio, "to_thread", thread_call)
    request_by_field = {"run_id": RUN_ID, "importer": "ptg"}
    assert (await control_workers.guarded_ensure_worker(request_by_field))["payload"] == request_by_field
    rejection_by_field = {"status": "failed", "items": [], "message": "synthetic"}
    monkeypatch.setattr(control_workers, "_admit_worker_ensure", lambda *_a, **_k: _async_value(rejection_by_field))
    assert await control_workers.guarded_ensure_worker(request_by_field) is rejection_by_field
    assert (await control_workers.guarded_ensure_worker({}))["status"] == "already_running"
def test_worker_selection_and_response_preserve_run_identity():
    selection = control_workers._worker_action_selection(
        "", [control_workers._BY_QUEUE["arq:ClaimsPricing"], control_workers._BY_QUEUE["arq:ClaimsPricing_finish"]]
    )
    assert selection.request_importer is None
    assert selection.allowed_importers == frozenset({"claims-pricing", "claims-procedures"})
    assert selection.allowed_roles == frozenset({"start", "finish"})
    response = control_workers._failed_worker_admission({"run_id": RUN_ID}, "synthetic failure")
    assert response["contract_id"] == control_workers.WORKER_ENSURE_RUN_IDENTITY_CONTRACT
    assert response["run_id"] == RUN_ID
    assert control_workers._worker_ensure_response({}, status="inactive", items=[])["status"] == "inactive"
