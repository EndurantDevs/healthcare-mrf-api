# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed model COPY, source generation and abandoned-candidate boundaries."""

import json
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import reference_family_archive as archive
from process import reference_family_result_generation as generation
from tests.test_reference_family_source_copy import _nucc_completed_handoff, _nucc_stage_receipt


def _run_copy_arguments():
    return {
        "spec": archive.ReferenceFamilySpec(
            "synthetic", (archive.models.ProviderProfileSourceRecord, archive.models.ProviderProfileFact)
        ),
        "source_schema": "source_candidate",
        "target_schema": "received_candidate",
        "target_names": ("received_record", "received_fact"),
        "run_scope": (("run_id", "run_id"), ("a" * 32, "b" * 64)),
        "source_copy": archive.ReferenceFamilySourceCopy(AsyncMock(), 100, 30),
        "deadline": 1000,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("run_scope", None),
        ("run_scope", ()),
        ("run_scope", (("run_id", "run_id"), ())),
        ("run_scope", (("run_id", "run_id"), ("a" * 32,) * 65)),
        ("run_scope", (("run_id", "run_id"), ("a" * 32, "a" * 32))),
        ("run_scope", (("run_id", "run_id"), (1,))),
        ("run_scope", (("run_id", "run_id"), ("A" * 32,))),
        ("run_scope", (("run_id",), ("a" * 32,))),
        ("run_scope", ((None, "run_id"), ("a" * 32,))),
        ("run_scope", (("absent", "run_id"), ("a" * 32,))),
        ("spec", None),
        ("spec", archive.ReferenceFamilySpec("synthetic", ())),
        ("source_copy", None),
        ("target_names", None),
        ("target_names", ("received_record",)),
        ("target_names", ("same", "same")),
        ("target_names", ("bad-name", "received_fact")),
        ("source_schema", None),
        ("source_schema", "source;DROP"),
        ("target_schema", "received-candidate"),
        ("deadline", True),
        ("deadline", float("nan")),
        ("deadline", float("inf")),
    ],
)
async def test_model_run_copy_refuses_untrusted_scope_before_copy(monkeypatch, field, value):
    arguments = _run_copy_arguments()
    arguments[field] = value
    copy = AsyncMock()
    monkeypatch.setattr(archive, "_copy_source_projection", copy)
    with pytest.raises(archive.ReferenceFamilyArchiveError):
        await archive._copy_model_run_scope(object(), **arguments)
    copy.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_first", [False, True])
async def test_model_run_copy_shares_one_budget_and_exact_model_columns(monkeypatch, fail_first):
    arguments = _run_copy_arguments()
    copy = AsyncMock(side_effect=TimeoutError("expired") if fail_first else [60, 20])
    monkeypatch.setattr(archive, "_copy_source_projection", copy)
    if fail_first:
        with pytest.raises(TimeoutError, match="expired"):
            await archive._copy_model_run_scope(object(), **arguments)
        assert copy.await_count == 1
        return
    assert await archive._copy_model_run_scope(object(), **arguments) == 20
    assert [call.args[-2] for call in copy.await_args_list] == [100, 60]
    for model, name, call in zip(
        arguments["spec"].model_types, arguments["target_names"], copy.await_args_list, strict=True
    ):
        columns = tuple(column.name for column in model.__table__.columns)
        query = call.args[2]
        assert call.args[3:6] == (arguments["target_schema"], name, columns)
        assert call.args[-1] == arguments["deadline"]
        assert f'FROM "source_candidate"."{model.__tablename__}"' in query
        assert 'WHERE "run_id"=ANY(ARRAY[' in query and query.endswith("::text[])")
        assert all(f'"{column}"' in query for column in columns)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "family", "generation", "relations", "storage"])
async def test_immutable_source_capture_requires_exact_generation_and_sealed_heap(monkeypatch, failure):
    serving = object()
    authority = SimpleNamespace(
        serving_generation=None if failure == "generation" else serving,
        relation_oids=None if failure == "relations" else (41,),
    )
    read = AsyncMock(return_value=authority)
    storage = AsyncMock(side_effect=RuntimeError("storage changed") if failure == "storage" else None)
    monkeypatch.setattr(archive, "read_reference_family_result_generation_authority", read)
    monkeypatch.setattr(generation, "require_immutable_nucc_storage", storage)
    spec = archive.reference_family_spec("plan-attributes" if failure == "family" else "nucc")
    if failure:
        with pytest.raises((archive.ReferenceFamilyArchiveError, RuntimeError)):
            await archive._capture_source_generation(
                object(), spec, "mrf", {}, archive.IMMUTABLE_NUCC_SOURCE_CAPTURE_CONTRACT
            )
        assert storage.await_count == int(failure == "storage")
        return
    assert (
        await archive._capture_source_generation(
            object(), spec, "mrf", {}, archive.IMMUTABLE_NUCC_SOURCE_CAPTURE_CONTRACT
        )
        is serving
    )
    read.assert_awaited_once_with(read.await_args.args[0], importer_id="nucc", schema_name="mrf", lock=False)
    storage.assert_awaited_once_with(read.await_args.args[0], schema_name="mrf", expected_relation_oid=41)


@pytest.mark.asyncio
@pytest.mark.parametrize("importer", ["claims-pricing", "drug-claims"])
@pytest.mark.parametrize("failure", [None, "contract", "metadata", "preimage"])
async def test_terminal_source_capture_uses_complete_run_preimage(monkeypatch, importer, failure):
    metadata = {"observed_run_id": "synthetic", "content": {"count": 1}}
    observed = deepcopy(metadata)
    if failure == "preimage":
        observed["content"]["count"] = 2
    capture = AsyncMock(return_value=observed)
    monkeypatch.setattr(archive, "capture_terminal_reference_source", capture)
    arguments = (object(), archive.reference_family_spec(importer), "mrf")
    if failure:
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="terminal reference"):
            await archive._capture_source_generation(
                *arguments, None if failure == "metadata" else metadata, "foreign" if failure == "contract" else None
            )
        assert capture.await_count == int(failure == "preimage")
        return
    assert await archive._capture_source_generation(*arguments, metadata, None) is None
    capture.assert_awaited_once_with(arguments[0], importer_id=importer, schema_name="mrf", run_id="synthetic")


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "missing", "closure"])
async def test_facility_capture_validates_the_existing_observation_set(monkeypatch, failure):
    from process import facility_address_contribution_merge as facilities

    serving = None if failure == "missing" else object()
    monkeypatch.setattr(archive, "_source_serving_generation", AsyncMock(return_value=serving))
    validate = AsyncMock(side_effect=RuntimeError("closure changed") if failure == "closure" else None)
    monkeypatch.setattr(facilities, "validate_observations", validate)
    session = object()
    if failure:
        with pytest.raises((archive.ReferenceFamilyArchiveError, RuntimeError)):
            await archive._capture_source_generation(
                session, archive.reference_family_spec("facility-anchors"), "mrf", {}, None
            )
        assert validate.await_count == int(failure == "closure")
        return
    assert (
        await archive._capture_source_generation(
            session, archive.reference_family_spec("facility-anchors"), "mrf", {}, None
        )
        is serving
    )
    validate.assert_awaited_once_with(session, stage_schema="mrf", schema="mrf", bind_alias=False)


@pytest.mark.asyncio
@pytest.mark.parametrize("importer,relations", [("label", None), ("nucc", (41,)), ("nucc", None)])
async def test_untracked_source_generation_never_invents_an_origin(monkeypatch, importer, relations):
    monkeypatch.setattr(archive, "_has_source_generation_authority", AsyncMock(return_value=True))
    monkeypatch.setattr(
        archive,
        "read_reference_family_result_generation_authority",
        AsyncMock(return_value=SimpleNamespace(serving_generation=None, relation_oids=relations)),
    )
    capture = AsyncMock()
    monkeypatch.setattr(archive, "capture_reference_family_serving_generation", capture)
    spec = archive.ReferenceFamilySpec(importer, ())
    if importer == "label" or relations is not None:
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="incomplete"):
            await archive._source_serving_generation(object(), spec, "mrf")
    else:
        assert await archive._source_serving_generation(object(), spec, "mrf") is None
    capture.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "capture"])
async def test_tracked_source_preserves_capture_failure_cause(monkeypatch, failure):
    monkeypatch.setattr(archive, "_has_source_generation_authority", AsyncMock(return_value=True))
    monkeypatch.setattr(
        archive,
        "read_reference_family_result_generation_authority",
        AsyncMock(return_value=SimpleNamespace(serving_generation=object(), relation_oids=(41,))),
    )
    serving = object()
    cause = RuntimeError("origin changed")
    monkeypatch.setattr(
        archive,
        "capture_reference_family_serving_generation",
        AsyncMock(side_effect=cause if failure else None, return_value=serving),
    )
    if failure:
        with pytest.raises(archive.ReferenceFamilyArchiveError, match="drifted") as caught:
            await archive._source_serving_generation(object(), archive.reference_family_spec("nucc"), "mrf")
        assert caught.value.__cause__ is cause
    else:
        assert (
            await archive._source_serving_generation(object(), archive.reference_family_spec("nucc"), "mrf") is serving
        )


def _record_inputs():
    original = _nucc_stage_receipt()
    params_by_field = {"source": "official"}
    original["source_contract_sha256"] = archive._nucc_source_contract(original, params_by_field)
    original["stage_sha256"] = archive.nucc_native_digest(
        {key: value for key, value in original.items() if key != "stage_sha256"}
    )
    handoff = _nucc_completed_handoff(original)
    ctx_by_field = {
        "control_run_id": original["run_id"],
        "import_date": original["import_date"],
        "context": {
            "_control_attempt_id": original["attempt_id"],
            "_control_attempt_started_at": original["attempt_started_at"],
            "nucc_native_stage": original,
            "nucc_native_predecessor": original["incumbent"],
        },
    }
    run_by_field = {
        "node_id": original["node_id"],
        "params": params_by_field,
        "metrics": {"nucc_native_stage": original},
    }
    return ctx_by_field, run_by_field, handoff


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "date", "name", "predecessor", "persisted", "indexed", "cas"])
async def test_native_handoff_records_only_the_exact_closed_attempt(monkeypatch, failure):
    ctx, run, handoff = _record_inputs()
    if failure == "date":
        ctx["import_date"] = "foreign"
    if failure == "predecessor":
        ctx["context"]["nucc_native_predecessor"] = None
    if failure == "persisted":
        run["metrics"] = {}
    ready_by_field = {
        "row_count": handoff["row_count"],
        "relation_oid": 99 if failure == "indexed" else handoff["stage"]["relation_oid"],
    }
    model = SimpleNamespace(__tablename__="foreign" if failure == "name" else handoff["stage"]["table_name"])
    connection = SimpleNamespace(exec_driver_sql=AsyncMock())
    session = SimpleNamespace(
        scalar=AsyncMock(side_effect=[handoff["database_oid"], None if failure == "cas" else handoff["run_id"]]),
        connection=AsyncMock(return_value=connection),
    )
    monkeypatch.setattr(archive, "_nucc_locked_attempt", AsyncMock(return_value=run))
    monkeypatch.setattr(
        archive,
        "read_reference_family_result_generation_authority",
        AsyncMock(return_value=archive._nucc_result_authority(handoff["incumbent"], allow_untracked=True)),
    )
    monkeypatch.setattr(archive, "_nucc_native_stage", AsyncMock(return_value=handoff["stage"]))
    monkeypatch.setattr(archive, "_relation_oid", AsyncMock(return_value=handoff["import_run_oid"]))
    if failure:
        with pytest.raises(archive.ReferenceFamilyArchiveError):
            await archive.record_nucc_native_handoff(
                session, ctx, schema_name="mrf", stage_model=model, ready=ready_by_field
            )
        assert connection.exec_driver_sql.await_count == int(failure == "cas")
        return
    assert (
        await archive.record_nucc_native_handoff(
            session, ctx, schema_name="mrf", stage_model=model, ready=ready_by_field
        )
        == handoff
    )
    comment = connection.exec_driver_sql.await_args.args[0]
    marker = json.loads(comment.split(" IS '", 1)[1][:-1])
    assert marker == {key: handoff[key] for key in ("contract", "run_id", "attempt_id", "handoff_sha256")}
    query, parameters = session.scalar.await_args.args
    assert "status='running'" in str(query) and "metrics->'nucc_handoff' IS NULL" in str(query)
    assert "attempt_started_at" in str(query) and "finished_at IS NULL" in str(query)
    assert parameters["handoff"] == archive._canonical_json(handoff).decode("ascii")


def _cleanup_inputs():
    _ctx, original_run, handoff = _record_inputs()
    run_by_field = {
        **original_run,
        "status": "canceled",
        "finished_at": "2026-10-01T00:01:00Z",
        "error": None,
        "phase_detail": "canceled",
        "progress": {key: handoff[key] for key in ("attempt_id", "attempt_started_at")},
    }
    abandonment_by_field = {
        "contract": archive.NUCC_ABANDONMENT_CONTRACT,
        "preparation_id": "292088cc-c6c4-4c46-822a-a36826213cd8",
        "generation_id": "46165f4c-62df-49bf-a456-ac994a90e7bb",
        "publication_fence": "a2e47c32-dd13-45e3-b5d3-05d65e3e8fd8",
        "admission_sha256": "b" * 64,
        "reason": "terminal-cancellation",
        "stage_disposition": "retained-unpublished",
        "physical_cleanup_completed": False,
        "same_run_readmission": "requires-exact-candidate-cleanup-and-ledger-retirement",
        "observed_run": {
            "run_id": handoff["run_id"],
            "node_id": handoff["node_id"],
            "status": run_by_field["status"],
            "progress": run_by_field["progress"],
            "handoff_sha256": handoff["handoff_sha256"],
            "phase_detail": run_by_field["phase_detail"],
            "finished_at": run_by_field["finished_at"],
        },
        "candidate_custody": {
            key: handoff[key]
            for key in (
                "database_oid",
                "import_run_oid",
                "schema_name",
                "attempt_id",
                "attempt_started_at",
                "handoff_sha256",
                "source_contract_sha256",
                "stage",
            )
        },
        "released_pins": [],
    }
    run_by_field["metrics"] = {
        "nucc_handoff": handoff,
        "nucc_native_abandonments": {abandonment_by_field["preparation_id"]: abandonment_by_field},
    }
    return run_by_field, handoff, abandonment_by_field


@pytest.mark.parametrize("reason", ["terminal-cancellation", "completed-handoff-cancellation", "replaced-attempt"])
@pytest.mark.parametrize("failure", [None, "progress", "status", "finish", "identity", "source"])
def test_cleanup_requires_authenticated_terminal_or_replacement_state(reason, failure):
    run, handoff, _abandonment = _cleanup_inputs()
    current = deepcopy(handoff)
    if reason == "completed-handoff-cancellation":
        run.update(status="canceling", finished_at=None, phase_detail="cancel requested")
        run["progress"]["message"] = "cancel requested"
    if reason == "replaced-attempt":
        current.update(attempt_id="synthetic:" + "2" * 32, attempt_started_at="2026-10-02T00:00:00+00:00")
        current["source_contract_sha256"] = archive._nucc_source_contract(current, run["params"])
        run.update(status="finalizing", finished_at=None, phase_detail=archive.NUCC_HANDOFF_PHASE)
        run["progress"] = {key: current[key] for key in ("attempt_id", "attempt_started_at")}
    if failure == "progress":
        run["progress"] = {}
    if failure == "status":
        run["status"] = "running"
    if failure == "finish":
        run["finished_at"] = None if reason == "terminal-cancellation" else "2026-10-02T00:01:00Z"
    if failure == "identity":
        current["attempt_id"] = handoff["attempt_id"] if reason == "replaced-attempt" else "foreign"
    if failure == "source":
        if reason == "replaced-attempt":
            run["params"] = {"source": "foreign"}
        else:
            current["source_contract_sha256"] = "f" * 64
    if failure:
        with pytest.raises(archive.ReferenceFamilyArchiveError):
            archive._require_nucc_cleanup_terminal(run, handoff, current, reason)
    else:
        archive._require_nucc_cleanup_terminal(run, handoff, current, reason)


def _cleanup_catalog_session(run, handoff, failure):
    catalog_result = Mock()
    catalog_result.mappings.return_value = catalog_result
    catalog_result.one_or_none.return_value = run
    transaction_state = SimpleNamespace(is_active=True)
    xacts = []

    async def scalar(statement, *_args):
        query = str(statement)
        if query == "SHOW search_path":
            return "synthetic"
        if "pg_current_xact_id" in query:
            xacts.append(query)
            return "other" if failure == "xact" and len(xacts) == 2 else "original"
        if "pg_database" in query:
            return handoff["database_oid"]
        if "obj_description" in query:
            return json.dumps({key: handoff[key] for key in ("contract", "run_id", "attempt_id", "handoff_sha256")})
        if query.startswith("SELECT NOT EXISTS"):
            return failure != "serving"
        if query.startswith("SELECT count(*)"):
            return failure != "owner"
        raise AssertionError("unexpected catalog query")

    session = SimpleNamespace(
        in_transaction=lambda: transaction_state.is_active,
        scalar=scalar,
        execute=AsyncMock(return_value=catalog_result),
    )
    return session, transaction_state


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure", [None, "first_reference", "late_reference", "transaction", "xact", "serving", "owner"]
)
async def test_cleanup_rechecks_custody_and_references_before_one_restrict_drop(monkeypatch, failure):
    run, handoff, abandonment = _cleanup_inputs()
    session, transaction_state = _cleanup_catalog_session(run, handoff, failure)
    monkeypatch.setattr(archive, "_nucc_native_publisher_owner", AsyncMock(return_value=53))
    monkeypatch.setattr(archive, "_relation_oid", AsyncMock(return_value=handoff["import_run_oid"]))
    monkeypatch.setattr(archive, "_nucc_native_stage", AsyncMock(return_value=handoff["stage"]))
    columns = AsyncMock()
    monkeypatch.setattr(archive, "_require_nucc_native_columns", columns)
    checks = []

    async def is_unreferenced(actual_session, actual_handoff, actual_abandonment):
        assert actual_session is session and actual_handoff == handoff and actual_abandonment == abandonment
        checks.append(1)
        if failure == "transaction" and len(checks) == 2:
            transaction_state.is_active = False
        return not (failure == "first_reference" or failure == "late_reference" and len(checks) == 2)

    arguments_by_field = {
        "abandonment": abandonment,
        "runtime_owner_oids": (handoff["stage"]["owner_oid"],),
        "assert_unreferenced": is_unreferenced,
    }
    if failure:
        with pytest.raises(archive.ReferenceFamilyArchiveError):
            await archive.cleanup_nucc_native_handoff(session, handoff, **arguments_by_field)
    else:
        receipt = await archive.cleanup_nucc_native_handoff(session, handoff, **arguments_by_field)
        assert receipt["physical_cleanup_completed"] is True
        assert receipt["stage"] == handoff["stage"]
        assert receipt["abandonment_sha256"] == archive.nucc_native_digest(abandonment)
    queries = [str(call.args[0]) for call in session.execute.await_args_list]
    drops = [query for query in queries if query.startswith("DROP")]
    assert drops == ([] if failure else [f'DROP TABLE "mrf"."{handoff["stage"]["table_name"]}" RESTRICT'])
    assert queries[-1] == "SELECT set_config('search_path',:path,true)"
    assert session.execute.await_args.args[1] == {"path": "synthetic"}
    assert len(checks) == (1 if failure in {"first_reference", "serving", "owner"} else 2)
    assert columns.await_count == int(failure not in {"first_reference", "serving", "owner"})
