# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed serving lineage, ordinary admission and native receipt comparisons."""

import asyncio
import copy
import importlib
import json
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from process import provider_directory_serving_recovery as recovery
from process import provider_directory_serving_recovery_receipts as receipts
from tests.test_source_publication_read_committed import owner_body as owner_body

PIN = dict(
    source_id="source_a", endpoint_id="endpoint_a", dataset_id="dataset_a", dataset_hash="a" * 64, parent_run_id="run_a"
)
GENERATION = "6b75740a-7c8a-556a-8e3c-21a9718c88f1"


@pytest.mark.parametrize("run_id", [None, "run_example"])
def test_recovery_reports_through_live_progress(monkeypatch, run_id):
    from process import live_progress

    events = []
    monkeypatch.setattr(
        live_progress, "enqueue_live_progress", lambda **progress_by_field: events.append(progress_by_field)
    )
    recovery._progress(run_id, "Preparing previous provider addresses")
    assert events == (
        [
            {
                "run_id": run_id,
                "status": "running",
                "phase": "preparing serving recovery receipts",
                "message": "Preparing previous provider addresses",
                "basis": "semantic_work",
                "denominator_state": "unknown",
            }
        ]
        if run_id
        else []
    )


def test_generation_is_canonical_and_bound_to_every_publication_field():
    """Changing any immutable coordinate changes the opaque generation."""
    assert recovery.publication_generation(PIN) == GENERATION
    assert recovery.publication_generation(dict(reversed(list(PIN.items())))) == GENERATION
    for field in PIN:
        changed_pin_by_field = {**PIN, field: "b" * 64 if field == "dataset_hash" else PIN[field] + "_other"}
        assert recovery.publication_generation(changed_pin_by_field) != GENERATION


@pytest.mark.parametrize(
    "value", [None, "", 1, "00000000-0000-0000-0000-000000000000", GENERATION.upper(), " " + GENERATION]
)
def test_generation_refuses_noncanonical_or_nil_input(value):
    with pytest.raises(ValueError, match="generation_invalid"):
        recovery.canonical_generation(value)


def test_only_explicit_source_publication_adds_descriptor_lineage():
    """Legacy descriptors and recovery outcomes cannot start a new forward chain."""
    descriptor_by_field = dict(PIN)
    assert recovery.publication_descriptor({}, descriptor_by_field) is descriptor_by_field
    assert (
        recovery.publication_descriptor({recovery.OPERATION_PARAM: "recover"}, descriptor_by_field)
        is descriptor_by_field
    )
    assert recovery.publication_descriptor({"publish_source_dataset_only": True}, descriptor_by_field) == {
        **PIN,
        recovery.DESCRIPTOR_FIELD: GENERATION,
    }
    assert descriptor_by_field == PIN


def request():
    """Construct the complete fixed operator request."""
    return {"source_ids": ["source_a"], recovery.OPERATION_PARAM: "recover", recovery.GENERATION_PARAM: GENERATION}


@pytest.mark.parametrize("field", [recovery.OPERATION_PARAM, recovery.GENERATION_PARAM])
def test_present_null_markers_are_rejected(field):
    with pytest.raises(ValueError, match="serving_recovery_"):
        recovery.is_recovery_request("provider-directory-fhir", {field: None})


def test_null_recovery_retry_is_rejected():
    from api.control_imports import _retry_child_params

    run_map = {"importer": "provider-directory-fhir", "params": {recovery.OPERATION_PARAM: None}}
    with pytest.raises(ValueError, match="request_invalid"):
        _retry_child_params(run_map, "run_original", {})


@pytest.mark.parametrize(
    "damage", ["importer", "two_sources", "missing_generation", "other_operation", "table", "profile", "acquisition"]
)
def test_recovery_rejects_other_authority_or_scope(damage):
    params, importer = request(), "provider-directory-fhir"
    match damage:
        case "importer":
            importer = "entity-address-unified"
        case "two_sources":
            params["source_ids"].append("source_b")
        case "missing_generation":
            del params[recovery.GENERATION_PARAM]
        case "other_operation":
            params[recovery.OPERATION_PARAM] = "publish"
        case "table":
            params["table"] = "other"
        case "profile":
            params["provider_directory_profile_generation"] = 1
        case "acquisition":
            params["import_resources"] = True
    with pytest.raises(ValueError, match="serving_recovery_"):
        recovery.is_recovery_request(importer, params)


def test_recovery_and_followup_markers_are_distinct_closed_operations():
    assert recovery.is_recovery_request("provider-directory-fhir", request())
    assert recovery.is_recovery_request("provider-directory-fhir", {}) is False
    with pytest.raises(ValueError, match="marker_scope_invalid"):
        recovery.is_recovery_request("provider-directory-fhir", {recovery.GENERATION_PARAM: GENERATION})
    overlay_params_by_field = dict(
        publish_artifacts_only=True,
        publish_artifacts_targets="address_overlay",
        publish_corroboration=False,
        import_resources=False,
        source_ids=["source_a"],
        **{recovery.GENERATION_PARAM: GENERATION},
    )
    assert recovery.is_recovery_request("provider-directory-fhir", overlay_params_by_field) is False


def test_overlay_marker_is_recomputed_from_the_actual_selected_source():
    dataset = SimpleNamespace(
        **{key: PIN[key] for key in PIN if key != "parent_run_id"}, evidence_run_id=PIN["parent_run_id"]
    )
    fence = SimpleNamespace(datasets=(dataset,))
    selected = SimpleNamespace(
        metrics={recovery.GENERATION_PARAM: GENERATION},
        publish_artifacts_targets={"address_overlay"},
        run_id="run_overlay",
    )
    assert recovery.overlay_request(fence, selected) == {
        "generation": GENERATION,
        "source": PIN,
        "run_id": "run_overlay",
    }
    selected.metrics[recovery.GENERATION_PARAM] = str(uuid4())
    with pytest.raises(ValueError, match="publication_changed"):
        recovery.overlay_request(fence, selected)


def native_receipt():
    """Use native-shaped metadata without asserting database custody."""
    return {
        "native": {
            "oid": 101,
            "relname": receipts.ROOTS[0],
            "relowner": 201,
            "columns": [{"name": "value"}],
            "acl": {"table": [], "columns": []},
            "indexes": [{"oid": 102, "name": "live_key", "valid": True}],
            "constraints": [{"oid": 103, "name": "live_key", "definition": "PRIMARY KEY (value)"}],
        },
        "rows": 2,
        "data_sha256": "c" * 64,
    }


@pytest.mark.parametrize("damage", ["rows", "data", "owner", "index", "constraint", "column"])
def test_retained_receipt_refuses_data_or_native_catalog_tamper(damage):
    expected, actual = native_receipt(), native_receipt()
    match damage:
        case "rows":
            actual["rows"] = 3
        case "data":
            actual["data_sha256"] = "d" * 64
        case "owner":
            actual["native"]["relowner"] += 1
        case "index":
            actual["native"]["indexes"][0]["valid"] = False
        case "constraint":
            actual["native"]["constraints"][0]["definition"] = "UNIQUE (value)"
        case "column":
            actual["native"]["columns"][0]["name"] = "other"
    with pytest.raises(ValueError, match="changed"):
        receipts.require_relation(expected, actual, renamed=True)


def test_only_authenticated_oid_preserving_names_are_normalized():
    expected, actual = native_receipt(), copy.deepcopy(native_receipt())
    actual["native"]["relname"] = receipts.retained_name(101)
    actual["native"]["indexes"][0]["name"] = receipts.retained_name(102, index=True)
    actual["native"]["constraints"][0]["name"] = receipts.retained_name(102, index=True)
    receipts.require_relation(expected, actual, renamed=True)
    with pytest.raises(ValueError, match="relation_changed"):
        receipts.require_relation(expected, actual)


def serving_states():
    """Construct metadata-only unit inputs; native producer custody is qualified separately."""
    root_by_name = {name: native_receipt() for name in receipts.ROOTS}
    for number, (name, receipt) in enumerate(root_by_name.items(), 1):
        receipt["native"].update(relname=name, oid=100 + number)
    relation_oids = [102, 301, 302, 303, 304, 305, 306]
    address_by_field = dict(
        singleton=True,
        local_lineage_id="bb7b24a0-2195-4fc7-8401-b5321ad42767",
        local_generation=3,
        relation_oids=relation_oids,
    )
    baseline_by_field = {
        "roots": root_by_name,
        "metadata": {
            "address": address_by_field,
            "relations": relation_oids,
            "alias": {"generation": 0},
            "overlay_alias": {"generation": 0},
            "profile": None,
            "doctors": None,
            "geo": None,
            "common_receipt": None,
        },
    }
    overlay_by_field = copy.deepcopy(baseline_by_field)
    overlay_by_field["roots"][receipts.ROOTS[0]]["native"]["oid"] = 401
    published_by_field = copy.deepcopy(overlay_by_field)
    published_by_field["roots"][receipts.ROOTS[1]]["native"]["oid"] = 402
    published_by_field["metadata"]["relations"][0] = 402
    published_by_field["metadata"]["address"]["local_generation"] = 4
    return baseline_by_field, overlay_by_field, published_by_field


def test_complete_events_require_native_generation_advance_and_fixed_delta():
    baseline, overlay, published = serving_states()
    recovery._require_transition("overlay", baseline, overlay)
    recovery._require_transition("published", overlay, published)
    recovered = copy.deepcopy(baseline)
    recovered["metadata"]["address"]["local_generation"] = 5
    recovery._require_transition("recovered", published, recovered)


@pytest.mark.parametrize("damage", ["generation", "profile", "support", "root_name", "common_chain"])
def test_event_refuses_counter_reset_unrelated_changes_or_forged_receipt(damage):
    _baseline, overlay, published = serving_states()
    match damage:
        case "generation":
            published["metadata"]["address"]["local_generation"] = 3
        case "profile":
            published["metadata"]["profile"] = {"generation_id": "other"}
        case "support":
            published["metadata"]["relations"][1] = 999
        case "root_name":
            published["roots"][receipts.ROOTS[1]]["native"]["relname"] = "caller_table"
        case "common_chain":
            overlay["metadata"]["common_receipt"] = {"receipt_id": "c" * 64}
            published["metadata"]["common_receipt"] = {"payload": {"predecessor_receipt_id": "d" * 64}}
    with pytest.raises(ValueError, match="serving_recovery_"):
        recovery._require_transition("published", overlay, published)


@pytest.mark.asyncio
async def test_recovery_dispatch_clears_source_authority_and_never_emits_followup(monkeypatch):
    """The actual dispatcher gives the ordinary child no ambient Source capability."""
    from process import provider_directory_generic_source_policy as source

    parent_factory = object()
    token = source._FACTORY.set(parent_factory)
    directory = SimpleNamespace(db=SimpleNamespace(_transaction_binding=lambda: None), ensure_database=AsyncMock())
    runtime_context_by_field = {"context": {}}

    async def recover(actual, task):
        assert actual is directory and task == request()
        assert source._FACTORY.get() is None and source._PUBLISHER.get() is None and source._OWNER.get() is None
        await asyncio.sleep(0)
        return {"serving_recovery": {"generation": GENERATION}}

    monkeypatch.setattr(recovery, "_recover", recover)
    try:
        result = await recovery.maybe_recover(directory, runtime_context_by_field, request())
        assert source._FACTORY.get() is parent_factory
    finally:
        source._FACTORY.reset(token)
    assert result == runtime_context_by_field["context"]["audit"]
    assert "dataset_followup" not in result


@pytest.mark.asyncio
@pytest.mark.parametrize("retry_of_run_id", [None, "run_previous_recovery"])
async def test_control_adapter_dispatches_exact_recovery_request_through_worker(monkeypatch, retry_of_run_id):
    from api import control_imports

    directory = importlib.import_module("process.provider_directory_fhir")

    run_by_field = {"run_id": "run_recovery", "importer": "provider-directory-fhir", "family": "provider"}
    if retry_of_run_id is not None:
        run_by_field["retry_of_run_id"] = retry_of_run_id
    payload_by_field = control_imports._adapter_payload(
        control_imports._SINGLE_JOB_ADAPTERS["provider-directory-fhir"], run_by_field, request()
    )
    task_by_field = {**payload_by_field["task"], "run_id": payload_by_field["run_id"]}
    context_by_field = {"control_run_id": payload_by_field["run_id"], "context": {}}
    outcome_by_field = {"serving_recovery": {"generation": GENERATION}}
    recover = AsyncMock(return_value=outcome_by_field)
    monkeypatch.setattr(directory.db, "_transaction_binding", lambda: None)
    monkeypatch.setattr(directory, "ensure_database", AsyncMock())
    monkeypatch.setattr(recovery, "_recover", recover)

    assert (
        await getattr(directory, payload_by_field["target_function"])(context_by_field, task_by_field)
        == outcome_by_field
    )

    assert payload_by_field["task"] == request()
    assert task_by_field == {**request(), "run_id": "run_recovery"}
    recover.assert_awaited_once_with(directory, task_by_field)
    assert context_by_field["context"]["audit"] == outcome_by_field
    directory.ensure_database.assert_awaited_once_with(False)


@pytest.mark.parametrize("extra", [{"test_mode": False}, {"retry_of_run_id": "caller_run"}, {"extra": True}])
def test_recovery_adapter_keeps_caller_extra_fields_invalid(extra):
    from api import control_imports

    run_by_field = {"run_id": "run_recovery", "importer": "provider-directory-fhir", "family": "provider"}
    with pytest.raises(ValueError, match="provider_directory_serving_recovery_request_invalid"):
        control_imports._adapter_payload(
            control_imports._SINGLE_JOB_ADAPTERS["provider-directory-fhir"], run_by_field, {**request(), **extra}
        )


@pytest.mark.asyncio
async def test_recovery_refuses_to_abandon_an_existing_writer(monkeypatch):
    directory = SimpleNamespace(db=SimpleNamespace(_transaction_binding=lambda: object()), ensure_database=AsyncMock())
    execute = AsyncMock()
    monkeypatch.setattr(recovery, "_recover", execute)
    with pytest.raises(ValueError, match="ordinary_binding_required"):
        await recovery.maybe_recover(directory, {"context": {}}, request())
    execute.assert_not_called()
    directory.ensure_database.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("committed", [True, False])
async def test_uncertain_commit_observes_only_and_preserves_real_failure(monkeypatch, committed):
    """A retry never performs a second inverse to decide whether the first committed."""
    failure = ConnectionError("synthetic lost acknowledgment")
    attempt = AsyncMock(side_effect=failure)
    outcome_by_field = {"serving_recovery": {"generation": GENERATION}}
    observer = AsyncMock(return_value=outcome_by_field if committed else None)
    monkeypatch.setattr(recovery, "_recover_once", attempt)
    monkeypatch.setattr(recovery, "_verify_recovered", observer)
    if committed:
        assert await recovery._recover(object(), request()) == outcome_by_field
    else:
        with pytest.raises(ConnectionError) as caught:
            await recovery._recover(object(), request())
        assert caught.value is failure
    attempt.assert_awaited_once()
    observer.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "override",
    [
        {},
        {"source_ids": ["source_b"]},
        {recovery.GENERATION_PARAM: "2f61cbd1-25b4-4d96-9cd0-b295a1982919"},
        {recovery.OPERATION_PARAM: "publish"},
        {"retry_of_run_id": "other"},
    ],
)
async def test_actual_retry_endpoint_preserves_exact_recovery_scope(monkeypatch, override):
    """Operator retry retains its three fields and rejects acquisition or identity overrides."""
    from sanic.exceptions import BadRequest

    from api import control, control_imports

    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "synthetic-token")
    original = request()
    monkeypatch.setattr(
        control_imports,
        "get_import_run",
        AsyncMock(return_value={"run_id": "parent", "importer": "provider-directory-fhir", "params": original}),
    )
    create = AsyncMock(return_value=({"run_id": "child"}, True))
    monkeypatch.setattr(control_imports, "create_import_run", create)
    monkeypatch.setattr(control, "retry_import_run", control_imports.retry_import_run)
    http_request = SimpleNamespace(
        headers={"Authorization": "Bearer synthetic-token"}, args={}, json={"retry_params": override}
    )
    if override:
        with pytest.raises(BadRequest, match="retry_scope_changed"):
            await control.control_retry_import(http_request, "parent")
        create.assert_not_called()
    else:
        response = await control.control_retry_import(http_request, "parent")
        assert response.status == 201 and json.loads(response.body)["run_id"] == "child"
        assert create.await_args.args[0]["params"] == original
        assert create.await_args.args[0]["retry_of_run_id"] == "parent"


@pytest.mark.asyncio
async def test_metadata_drift_after_preparation_refuses_overlay_cutover(monkeypatch):
    """The final comparison runs after real metadata locking, before any table move."""
    from process import provider_directory_generic_source_artifacts as artifacts

    baseline, _overlay, _published = serving_states()
    changed = copy.deepcopy(baseline)
    changed["metadata"]["profile"] = {"generation_id": "another_publication"}
    directory = SimpleNamespace(db=object(), _schema=lambda: "serving")
    selected = SimpleNamespace(recovery={"before": baseline, "proofs": object()})
    events = []

    async def lock_metadata(database, schema):
        assert database is directory.db and schema == "serving"
        events.append("locked")

    async def prepared_state(database, schema, proofs):
        assert events == ["locked"] and proofs is selected.recovery["proofs"]
        return changed

    monkeypatch.setattr(artifacts, "current", lambda actual: selected if actual is directory else None)
    monkeypatch.setattr(receipts, "lock_metadata", lock_metadata)
    monkeypatch.setattr(receipts, "prepared_state", prepared_state)
    with pytest.raises(ValueError, match="current_serving_changed"):
        await recovery.before_overlay(directory)


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", [None, "session", "task", "transaction", "lock", "catalog"])
async def test_prepared_digest_requires_original_frozen_transaction(monkeypatch, damage):
    """A content receipt cannot survive changed native identity or be reused by another task."""
    original = native_receipt()
    session = object()
    actor_by_field = {"backend": 401, "xid": "901"}
    driver = SimpleNamespace(fetchval=AsyncMock(return_value=damage != "lock"))
    proof = receipts.PreparedRelation(
        session,
        asyncio.current_task(),
        recovery.encoded(actor_by_field),
        "serving",
        recovery.encoded(original),
        0.1,
        1024,
        2048,
    )
    actual = copy.deepcopy(original)
    if damage == "session":
        session = object()
    elif damage == "task":
        proof = replace(proof, task=object())
    elif damage == "transaction":
        actor_by_field = {**actor_by_field, "xid": "902"}
    elif damage == "catalog":
        actual["native"]["columns"][0]["name"] = "changed"
    ordinary = AsyncMock(return_value=(session, driver, actor_by_field))
    relation = AsyncMock(return_value={"native": actual["native"]})
    monkeypatch.setattr(receipts, "ordinary", ordinary)
    monkeypatch.setattr(receipts, "relation", relation)
    if damage is not None:
        with pytest.raises(ValueError, match="serving_recovery_"):
            await receipts.recheck_prepared(object(), proof, receipts.ROOTS[0])
    else:
        assert await receipts.recheck_prepared(object(), proof, receipts.ROOTS[0]) == original
        relation.assert_awaited_once()
        assert relation.await_args.kwargs == {"content": False}


@pytest.mark.asyncio
async def test_preparation_keeps_short_sql_timeout_after_hash(monkeypatch):
    """The expensive digest temporarily gets preparation capacity on its original session."""
    from process import entity_address_cutover_contract as contract
    from process import entity_address_snapshot_receipt as snapshot

    session, database = object(), SimpleNamespace(status=AsyncMock())
    events = []

    @asynccontextmanager
    async def preserve(actual, names, quote):
        assert actual is database and names == ["statement_timeout"]
        events.append("save")
        yield
        events.append("restore")

    async def row_identity(actual, schema, name):
        assert actual is session and schema == "serving" and name == receipts.ROOTS[0]
        database.status.assert_awaited_once_with("SET LOCAL statement_timeout='7200000ms'")
        events.append("hash")
        return 2, "c" * 64

    monkeypatch.setattr(receipts, "ordinary", AsyncMock(return_value=(session, object(), {})))
    monkeypatch.setattr(receipts, "catalog", AsyncMock(return_value={receipts.ROOTS[0]: native_receipt()["native"]}))
    monkeypatch.setattr(contract, "preserve_transaction_sql_settings", preserve)
    monkeypatch.setattr(snapshot, "_normalize_receipt_session", AsyncMock())
    monkeypatch.setattr(snapshot, "_row_identity", row_identity)
    assert await receipts.relation(database, "serving", receipts.ROOTS[0]) == native_receipt()
    assert events == ["save", "hash", "restore"]


@pytest.mark.asyncio
async def test_address_short_cutover_wall_includes_commit(monkeypatch):
    """A slow acknowledgment cannot outlive the marked writer's short commit budget."""
    from process import entity_address_cutover_contract as contract

    @asynccontextmanager
    async def transaction(*args):
        yield
        await asyncio.sleep(0.05)

    monkeypatch.setattr(contract, "entity_address_cutover_transaction", transaction)
    context_by_field = {recovery.GENERATION_PARAM: GENERATION}
    with pytest.raises(TimeoutError):
        async with recovery.address_transaction(object(), "500ms", str, context_by_field):
            context_by_field["serving_recovery_wall"].reschedule(asyncio.get_running_loop().time() + 0.001)
    assert "serving_recovery_wall" not in context_by_field


def _address_retry_case(monkeypatch, phase, error, marked):
    """Stub native IO while retaining the real publisher, preparation and retry control flow."""
    from process import entity_address_snapshot_adoption as adoption
    from process import provider_directory_artifact_read_access as access
    from tests.test_entity_address_unified_publication import _RecordingDB, _StageTable

    address = importlib.import_module("process.entity_address_unified")
    directory = importlib.import_module("process.provider_directory_fhir")
    database = _RecordingDB(advisory_values=(False, True) if phase == "advisory" else (True, True))
    context_by_field = {
        "partial_provider_directory_source_ids": [PIN["source_id"]],
        "partial_provider_directory_dataset_id": PIN["dataset_id"],
        "partial_provider_directory_run_id": PIN["parent_run_id"],
    }
    if marked:
        context_by_field[recovery.GENERATION_PARAM] = GENERATION
    current, _overlay, _published = serving_states()
    monkeypatch.setattr(address, "db", database)
    monkeypatch.setattr(directory, "db", database)
    monkeypatch.setattr(address, "_cutover_retry_settings", lambda: (3, 0, 0))
    monkeypatch.setattr(address, "ordinary_address_receipt_callbacks", lambda *args: None)
    monkeypatch.setattr(address, "_assert_provider_directory_overlay_alias_fence", AsyncMock())
    monkeypatch.setattr(adoption, "_prepare_protected_ordinary_rotation", AsyncMock())
    monkeypatch.setattr(adoption, "_require_protected_cutover", AsyncMock())
    monkeypatch.setattr(
        address.result_generation, "publish_cutover_entity_address_generation", AsyncMock(return_value=None)
    )
    monkeypatch.setattr(recovery, "after_address", AsyncMock())
    monkeypatch.setattr(
        recovery, "_load_history", AsyncMock(return_value={"overlay": {"document": {"source": PIN, "after": current}}})
    )
    monkeypatch.setattr(recovery, "_source_fence", AsyncMock())
    monkeypatch.setattr(receipts, "lock", AsyncMock())
    monkeypatch.setattr(receipts, "lock_metadata", AsyncMock())
    monkeypatch.setattr(receipts, "ordinary", AsyncMock(return_value=(object(), object(), {})))
    monkeypatch.setattr(receipts, "prepared_state", AsyncMock(return_value=current))
    monkeypatch.setattr(receipts, "retain", AsyncMock())
    monkeypatch.setattr(access, "prepare_ordinary_read_access", AsyncMock(return_value=(object(),)))
    monkeypatch.setattr(access, "recheck_ordinary_read_access", AsyncMock())

    async def digest(*args):
        assert context_by_field["serving_recovery_preparation_started"] is True
        if phase == "digest":
            raise error
        return object()

    prepare = AsyncMock(side_effect=digest)
    monkeypatch.setattr(receipts, "prepare_relation", prepare)
    _inject_address_cutover_failure(monkeypatch, address, database, context_by_field, phase, error)
    return address, database, _StageTable, context_by_field, prepare


def _inject_address_cutover_failure(monkeypatch, address, database, context_by_field, phase, error):
    """Fail only the selected original transaction boundary on its first attempt."""

    async def source_fence(*args):
        if phase == "before" and context_by_field["cutover_attempts"] == 1:
            raise error

    monkeypatch.setattr(address, "lock_address_source_fence", source_fence)
    original_status = database.status

    async def status(statement, **params):
        if phase == "cutover" and "ACCESS EXCLUSIVE" in statement and context_by_field["cutover_attempts"] == 1:
            raise error
        return await original_status(statement, **params)

    monkeypatch.setattr(database, "status", status)

    @asynccontextmanager
    async def transaction():
        database.transaction_events.append("BEGIN")
        try:
            yield
            if phase == "commit":
                raise error
        except BaseException:
            database.transaction_events.append("ROLLBACK")
            raise
        else:
            database.transaction_events.append("COMMIT")

    monkeypatch.setattr(database, "transaction", transaction)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "marked,phase,error_kind,attempts,digests",
    [
        (True, "cutover", "lock", 1, 3),
        (True, "advisory", "lock", 1, 3),
        (True, "commit", "wrapped_lock", 1, 3),
        (True, "digest", "lock", 1, 1),
        (True, "cutover", "other", 1, 3),
        (True, "before", "lock", 2, 3),
        (False, "cutover", "lock", 2, 0),
    ],
)
async def test_actual_address_retry_loop_does_not_repeat_started_marked_digests(
    monkeypatch, marked, phase, error_kind, attempts, digests
):
    """Exercise the real publisher, cutover, preparation and retry classifier together."""
    from asyncpg import LockNotAvailableError

    error = RuntimeError("synthetic non-lock failure") if error_kind == "other" else LockNotAvailableError("locked")
    if error_kind == "wrapped_lock":
        wrapped = RuntimeError("wrapped lock failure")
        wrapped.__cause__ = error
        error = wrapped
    address, database, stage, context_by_field, prepare = _address_retry_case(monkeypatch, phase, error, marked)
    publish = address._publish_staged_entity_address_tables(
        "mrf", stage, {}, partial_support_patch=False, affected_group_table="", context=context_by_field
    )
    if marked and phase != "before":
        with pytest.raises(RuntimeError) as caught:
            await publish
        if error_kind == "other":
            assert caught.value is error
        else:
            assert "retry the operation explicitly" in str(caught.value)
            assert caught.value.__cause__ is None
            assert not address._is_retryable_cutover_lock_error(caught.value)
        assert database.transaction_events == ["BEGIN", "ROLLBACK"]
    else:
        await publish
        assert database.transaction_events == ["BEGIN", "ROLLBACK", "BEGIN", "COMMIT"]
    assert context_by_field["cutover_attempts"] == attempts
    assert prepare.await_count == digests
    assert "serving_recovery_preparation_started" not in context_by_field
    for key in ("wall", "proofs", "stage", "read_access", "before", "writer"):
        assert "serving_recovery_" + key not in context_by_field


@pytest.mark.asyncio
async def test_cancelled_marked_address_clears_preparation_marker(monkeypatch):
    from process import entity_address_cutover_contract as contract

    @asynccontextmanager
    async def transaction(*args):
        yield

    monkeypatch.setattr(contract, "entity_address_cutover_transaction", transaction)
    context_by_field = {recovery.GENERATION_PARAM: GENERATION}
    with pytest.raises(asyncio.CancelledError):
        async with recovery.address_transaction(object(), "500ms", str, context_by_field):
            context_by_field["serving_recovery_preparation_started"] = True
            raise asyncio.CancelledError
    assert context_by_field == {recovery.GENERATION_PARAM: GENERATION}


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", [None, "transaction", "schema"])
async def test_source_overlay_requires_validated_original_publication_before_skipping_ordinary_reads(
    monkeypatch, damage
):
    """CMS keeps its existing native read replay; an ambient Source marker grants nothing."""
    from process import provider_directory_artifact_read_access as access
    from process import provider_directory_generic_source_policy as source
    from process import provider_directory_output_snapshot_custody as outputs

    directory = SimpleNamespace(db=object(), _schema=lambda: "serving")
    stage = SimpleNamespace(schema="serving", target_relation=receipts.ROOTS[0])
    publisher = SimpleNamespace(policy=SimpleNamespace(schema="other" if damage == "schema" else "serving"))
    verifier = AsyncMock(
        side_effect=RuntimeError("original publication changed") if damage == "transaction" else None,
        return_value=publisher,
    )
    ordinary_reads = AsyncMock(side_effect=AssertionError("ordinary reads cannot authorize a Source owner"))
    monkeypatch.setattr(source, "require_source_publication_transaction", verifier)
    monkeypatch.setattr(access, "prepare_ordinary_artifact_reads", ordinary_reads)
    token = source._PUBLISHER.set(object())
    output_token = outputs._OUTPUT.set(None)
    try:
        if damage:
            with pytest.raises((RuntimeError, ValueError), match="publication changed|overlay_scope_changed"):
                await recovery.retain_overlay(directory, stage)
        else:
            await recovery.retain_overlay(directory, stage)
    finally:
        source._PUBLISHER.reset(token)
        outputs._OUTPUT.reset(output_token)
    verifier.assert_awaited_once_with(directory.db)
    ordinary_reads.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("is_missing", [False, True])
async def test_required_support_heaps_lock_before_metadata(monkeypatch, is_missing):
    """All six support names are pinned before metadata reads; a missing heap aborts admission."""
    from asyncpg import UndefinedTableError

    expected = (
        "LOCK TABLE "
        + ",".join(receipts.qualified("serving", name) for name in receipts.generations.RELATION_NAMES[1:])
        + " IN ACCESS SHARE MODE NOWAIT"
    )
    statements = []

    async def execute(statement):
        statements.append(statement)
        assert statement == expected
        if is_missing:
            raise UndefinedTableError("synthetic missing required support heap")

    async def lookup(*args):
        assert statements == [expected]
        return None

    driver = SimpleNamespace(
        execute=AsyncMock(side_effect=execute),
        fetchval=AsyncMock(side_effect=lookup),
        fetch=AsyncMock(return_value=[1]),
    )
    monkeypatch.setattr(receipts, "ordinary", AsyncMock(return_value=(object(), driver, {})))
    authority = AsyncMock()
    monkeypatch.setattr(receipts.generations, "_locked_authority", authority)
    if is_missing:
        with pytest.raises(UndefinedTableError, match="missing required support"):
            await receipts.lock_metadata(object(), "serving")
        driver.fetchval.assert_not_called()
        authority.assert_not_called()
    else:
        await receipts.lock_metadata(object(), "serving")
        assert driver.fetchval.await_count == 4
        authority.assert_awaited_once()
        assert driver.fetch.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", [None, "address", "overlay", "geo"])
async def test_narrow_metadata_writer_locks_require_existing_authority(monkeypatch, missing):
    """Column UPDATE locks mandatory rows; optional empty claims retain their table fence."""
    statements = []
    database = object()

    async def execute(statement):
        statements.append(statement)

    async def locked_authority(actual, schema):
        assert actual is database and schema == "serving"
        if missing == "address":
            raise RuntimeError("entity-address result generation singleton is unavailable")

    async def fetch(statement):
        statements.append(statement)
        if (
            missing in {"overlay", "geo"}
            and ("artifact_state" if missing == "overlay" else "geo_assurance") in statement
        ):
            return []
        return [1]

    driver = SimpleNamespace(execute=execute, fetch=fetch, fetchval=AsyncMock(return_value=123))
    monkeypatch.setattr(receipts, "ordinary", AsyncMock(return_value=(object(), driver, {})))
    monkeypatch.setattr(receipts.generations, "_locked_authority", locked_authority)
    if missing:
        with pytest.raises((RuntimeError, ValueError), match="singleton is unavailable|metadata_authority_missing"):
            await receipts.lock_metadata(database, "serving")
    else:
        await receipts.lock_metadata(database, "serving")
        for table in (
            "reference_family_result_generation",
            "provider_directory_profile_serving_generation",
            "provider_directory_cms_serving_receipt",
        ):
            assert f'LOCK TABLE "serving"."{table}" IN SHARE MODE NOWAIT' in statements
    for table in (
        "entity_address_result_generation",
        "address_alias_artifact_state_v1",
        "entity_address_geo_assurance_state",
    ):
        assert f'LOCK TABLE "serving"."{table}" IN SHARE MODE NOWAIT' not in statements


def _source_address_swaps():
    return tuple(
        SimpleNamespace(
            live_cls=SimpleNamespace(__main_table__=name),
            stage_cls=SimpleNamespace(__tablename__=name + "_stage"),
        )
        for name in ("entity_address_unified", "entity_address_evidence")
    )


@pytest.mark.asyncio
async def test_source_address_preserves_every_swap_in_original_owner(owner_body, monkeypatch):
    from process import native_publication_read_access as access
    from process import provider_directory_generic_source_policy as source

    preserve = AsyncMock()
    monkeypatch.setattr(access, "preserve_publication_reads", preserve)
    ordinary = AsyncMock(side_effect=AssertionError("Source publication must retain its original Owner"))
    monkeypatch.setattr(receipts, "ordinary", ordinary)
    swaps = _source_address_swaps()
    async with owner_body() as body:
        await recovery.before_address(body.database, body.publisher.policy.schema, swaps, {})
        assert source.source_copy_session() is body.session
        assert preserve.await_count == len(swaps)
        for call, swap in zip(preserve.await_args_list, swaps, strict=True):
            assert call.args == (body.connection,)
            assert call.kwargs == dict(
                schema=body.publisher.policy.schema,
                live_table=swap.live_cls.__main_table__,
                stage_table=swap.stage_cls.__tablename__,
            )
    ordinary.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["schema", "generation", "null_generation"])
async def test_source_address_refuses_marked_or_foreign_scope(owner_body, monkeypatch, damage):
    from process import native_publication_read_access as access

    preserve = AsyncMock()
    monkeypatch.setattr(access, "preserve_publication_reads", preserve)
    context_by_field = (
        {} if damage == "schema" else {recovery.GENERATION_PARAM: None if damage == "null_generation" else GENERATION}
    )
    async with owner_body() as body:
        schema = "other_schema" if damage == "schema" else body.publisher.policy.schema
        with pytest.raises(ValueError, match="address_scope_(changed|invalid)"):
            await recovery.before_address(body.database, schema, _source_address_swaps(), context_by_field)
    preserve.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage", ["unissued", "session", "driver", "task", "backend", "xid", "actor", "transaction", "isolation"]
)
async def test_source_address_rechecks_issued_original_publication(owner_body, monkeypatch, damage):
    from process import native_publication_read_access as access
    from process import provider_directory_generic_source_policy as source
    from process.provider_directory_generic_candidate import GenericCandidateError

    preserve = AsyncMock()
    monkeypatch.setattr(access, "preserve_publication_reads", preserve)
    original_driver = source._copy_driver
    async with owner_body(final=damage != "unissued") as body:
        database = body.database
        original_native = vars(body.native).copy()
        if damage == "session":
            database = SimpleNamespace(_transaction_binding=lambda: SimpleNamespace(session=object()))
        elif damage == "driver":
            monkeypatch.setattr(source, "_copy_driver", AsyncMock(return_value=object()))
        elif damage in {"backend", "xid", "actor", "transaction", "isolation"}:
            field, changed = {
                "backend": ("pid", 4321),
                "xid": ("xid", "43"),
                "actor": ("actor", "source_publisher"),
                "transaction": ("active", False),
                "isolation": ("isolation", "repeatable read"),
            }[damage]
            setattr(body.native, field, changed)
        try:
            with pytest.raises(GenericCandidateError):
                operation = recovery.before_address(database, body.publisher.policy.schema, _source_address_swaps(), {})
                await asyncio.create_task(operation) if damage == "task" else await operation
        finally:
            vars(body.native).update(original_native)
            monkeypatch.setattr(source, "_copy_driver", original_driver)
    preserve.assert_not_awaited()


def _source_address_native_execute(body, damage, error):
    original_execute = body.connection.execute.side_effect

    async def execute(statement):
        if statement.startswith("LOCK TABLE") and damage == "lock":
            raise error
        if statement.startswith("GRANT"):
            if damage == "cancel":
                raise error
            if damage == "xid_after_grant":
                body.native.xid = "43"
        if statement.startswith("SET LOCAL ROLE") or statement == "RESET ROLE":
            await original_execute(statement)

    return execute


async def _source_address_outcome(operation, damage, error):
    if damage in {"lock", "cancel"}:
        with pytest.raises(type(error)) as caught:
            await operation
        assert caught.value is error
    elif damage == "grantor":
        with pytest.raises(RuntimeError, match="publication_read_access_grant_changed"):
            await operation
    elif damage == "xid_after_grant":
        from process.provider_directory_generic_candidate import GenericCandidateError

        with pytest.raises(GenericCandidateError, match="publication_transaction_changed"):
            await operation
    else:
        await operation


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", [None, "lock", "grantor", "cancel", "xid_after_grant"])
async def test_source_address_uses_native_acl_and_lock_guards(owner_body, damage):
    from asyncpg import LockNotAvailableError

    from tests.test_native_publication_read_access import _grant, _relations

    swap = _source_address_swaps()[0]
    relations = _relations()
    for relation_by_field, name in zip(
        relations, (swap.live_cls.__main_table__, swap.stage_cls.__tablename__), strict=True
    ):
        relation_by_field["name"] = name
    grant_by_field = dict(_grant(), rolname="source_reader", grantor=99 if damage == "grantor" else 19)
    error = LockNotAvailableError("synthetic lock refusal") if damage == "lock" else asyncio.CancelledError("cancelled")
    async with owner_body() as body:
        original_execute = body.connection.execute.side_effect
        body.connection.fetch = AsyncMock(side_effect=[relations, relations, [grant_by_field]])

        body.connection.execute.side_effect = _source_address_native_execute(body, damage, error)
        body.connection.execute.reset_mock()
        try:
            operation = recovery.before_address(body.database, body.publisher.policy.schema, (swap,), {})
            await _source_address_outcome(operation, damage, error)
            statements = [call.args[0] for call in body.connection.execute.await_args_list]
            assert statements[0].startswith("LOCK TABLE")
            grants = [statement for statement in statements if statement.startswith("GRANT")]
            assert grants == (
                []
                if damage in {"lock", "grantor"}
                else ['GRANT SELECT ON TABLE "source_schema"."entity_address_unified_stage" TO "source_reader"']
            )
        finally:
            body.native.xid = "42"
            body.connection.execute.side_effect = original_execute
