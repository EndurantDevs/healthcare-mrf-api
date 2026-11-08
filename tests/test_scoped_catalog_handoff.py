# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Attempt-fenced producer handoff, same-session publication and cancellation checks."""

import asyncio
import json
from contextlib import asynccontextmanager
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from sqlalchemy import update
from sqlalchemy.dialects import postgresql

from db.models import ImportRun
from process import control_lifecycle
from process import reference_family_archive as native
from process import scoped_catalog_handoff as handoff


def _value():
    candidate_by_field = {
        "contract": handoff.CONTRACT,
        "importer_id": "code-sets",
        "schema_name": "serving",
        "run_id": "run-1",
        "attempt_id": "attempt-1",
        "attempt_started_at": "2026-01-01T00:00:00Z",
        "node_id": "node-1",
        "database_oid": 11,
        "import_run_oid": 12,
        "input_owner_oid": 13,
        "input": {
            "dataset_id": str(UUID(int=1)),
            "schema_name": native.reference_family_stage_schema(UUID(int=1)),
            "schema_oid": 14,
            "relation_oids": [["code_catalog", 15]],
        },
        "tables": [{"table": "code_catalog", "row_count": 3, "row_sha256": "a" * 64, "schema_sha256": "b" * 64}],
        "expected_generations": {"code-sets": {"local_generation": 0}},
        "include_relationships": False,
        "test_mode": False,
        "metrics": {"pos_rows": 1},
        "source_contract_sha256": handoff._digest(
            {
                "importer": "code-sets",
                "params": {},
                "options": {"include_relationships": False, "test_mode": False},
                "metrics": {"pos_rows": 1},
            }
        ),
    }
    return _sign(candidate_by_field)


def _sign(candidate):
    unsigned_by_field = {key: value for key, value in candidate.items() if key != "handoff_sha256"}
    return {**unsigned_by_field, "handoff_sha256": handoff._digest(unsigned_by_field)}


def _producer_value(importer, include_relationships=False):
    candidate = _value()
    candidate.update(importer_id=importer, include_relationships=include_relationships)
    names = handoff.publication.input_spec(importer).table_names
    candidate["input"]["relation_oids"] = [[name, 15 + index] for index, name in enumerate(sorted(names))]
    candidate["tables"] = [{**candidate["tables"][0], "table": name} for name in names]
    candidate["expected_generations"] = {importer: {"local_generation": 0}}
    candidate["source_contract_sha256"] = handoff._digest(
        dict(
            importer=importer,
            params={},
            options=dict(include_relationships=include_relationships, test_mode=False),
            metrics=candidate["metrics"],
        )
    )
    return handoff.validate_catalog_handoff(_sign(candidate))


def _run(candidate):
    return {
        "run_id": candidate["run_id"],
        "node_id": candidate["node_id"],
        "params": {},
        "metrics": {handoff.METRIC: candidate},
        "phase_detail": handoff.PHASE,
    }


def _session():
    return SimpleNamespace(in_transaction=lambda: True, scalar=AsyncMock(return_value=11), execute=AsyncMock())


@pytest.mark.parametrize(
    "field,bad",
    [
        ("database_oid", True),
        ("import_run_oid", 0),
        ("input_owner_oid", 2**32),
        ("run_id", ""),
        ("attempt_id", 1),
        ("attempt_started_at", ""),
        ("node_id", ""),
        ("include_relationships", 1),
        ("test_mode", True),
        ("expected_generations", {}),
    ],
)
def test_handoff_refuses_re_signed_bad_identity(field, bad):
    candidate = _value()
    candidate[field] = bad
    with pytest.raises(native.ReferenceFamilyArchiveError):
        handoff.validate_catalog_handoff(_sign(candidate))


@pytest.mark.parametrize("change", ["extra", "digest", "input_namespace", "input_oid", "table"])
def test_handoff_refuses_unbound_input(change):
    candidate = _value()
    if change == "extra":
        candidate["extra"] = 1
    elif change == "digest":
        candidate["handoff_sha256"] = "0" * 64
    elif change == "input_namespace":
        candidate["input"]["schema_name"] = "serving"
    elif change == "input_oid":
        candidate["input"]["schema_oid"] = True
    else:
        candidate["input"]["relation_oids"] = [["other", 15]]
    with pytest.raises(native.ReferenceFamilyArchiveError):
        handoff.validate_catalog_handoff(candidate)


@pytest.mark.parametrize(
    "path,replacement",
    [
        (("input", "extra"), 1),
        (("tables", 0, "row_count"), True),
        (("tables", 0, "row_count"), -1),
        (("tables", 0, "schema_sha256"), "x" * 64),
        (("source_contract_sha256",), "x" * 64),
        (("tables", 0, "extra"), 1),
        (("tables",), []),
        (("expected_generations", "other"), {}),
    ],
)
def test_handoff_refuses_re_signed_receipt_shape_drift(path, replacement):
    candidate = _value()
    parent = candidate
    for key in path[:-1]:
        parent = parent[key]
    parent[path[-1]] = replacement
    with pytest.raises(native.ReferenceFamilyArchiveError):
        handoff.validate_catalog_handoff(_sign(candidate))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changed", [None, "node", "params", "metrics", "phase", "database", "control_oid", "rows", "owner"]
)
async def test_handoff_requires_actual_attempt_and_candidate(monkeypatch, changed):
    candidate, session = _value(), _session()
    run = _run(candidate)
    monkeypatch.setattr(handoff, "_native_control_run", AsyncMock(return_value=run))
    monkeypatch.setattr(native, "_relation_oid", AsyncMock(return_value=12))
    monkeypatch.setattr(handoff, "_input_receipts", AsyncMock(return_value=candidate["tables"]))
    monkeypatch.setattr(native, "_verify_stage_owner", AsyncMock())
    if changed in {"node", "phase"}:
        run["node_id" if changed == "node" else "phase_detail"] = "other"
    if changed in {"params", "metrics"}:
        run[changed] = {"changed": True}
    if changed == "database":
        session.scalar.return_value = 99
    if changed == "control_oid":
        native._relation_oid.return_value = 99
    if changed == "rows":
        handoff._input_receipts.return_value = []
    if changed == "owner":
        native._verify_stage_owner.side_effect = native.ReferenceFamilyArchiveError("owner differs")
    if changed:
        with pytest.raises(native.ReferenceFamilyArchiveError):
            await handoff.require_catalog_handoff(session, candidate)
    else:
        authenticated, incoming = await handoff.require_catalog_handoff(session, candidate)
        assert authenticated is candidate
        native._verify_stage_owner.assert_awaited_once_with(session, incoming.ownership, 13)
        handoff._native_control_run.assert_awaited_once_with(session, candidate, ("finalizing",))
    session.execute.assert_not_awaited()


def _database(session, *, commit_error=None):
    @asynccontextmanager
    async def transaction():
        yield session
        if commit_error:
            raise commit_error

    return SimpleNamespace(transaction=transaction)


@pytest.mark.asyncio
@pytest.mark.parametrize("persisted", [True, False])
async def test_real_commit_reconciliation_survives_caller_cancellation(persisted):
    """Cancellation cannot abandon the locked readback or turn a mismatched receipt into success."""
    candidate, session = _value(), _session()
    entered, release = asyncio.Event(), asyncio.Event()

    async def read_receipt(statement, parameters):
        assert "FOR SHARE" in str(statement)
        assert parameters == {"run_id": candidate["run_id"]}
        entered.set()
        await release.wait()
        return candidate if persisted else {**candidate, "node_id": "other"}

    session.scalar = read_receipt
    pending = asyncio.create_task(handoff._reconcile_catalog_handoff(_database(session), candidate))
    await entered.wait()
    pending.cancel()
    await asyncio.sleep(0)
    assert not pending.done()
    release.set()
    assert await pending is persisted
    assert str(session.execute.await_args.args[0]) == "SET LOCAL lock_timeout='30s'"


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [RuntimeError("readback unavailable"), asyncio.CancelledError()])
async def test_real_commit_reconciliation_propagates_readback_failure(error):
    """A failed or independently canceled readback is not evidence of a committed handoff."""
    session = _session()
    session.scalar.side_effect = error
    with pytest.raises(type(error)):
        await handoff._reconcile_catalog_handoff(_database(session), _value())


@pytest.mark.asyncio
@pytest.mark.parametrize("importer", ["code-sets", "ms-drg"])
async def test_candidate_receipts_verify_all_owned_models_before_row_identity(monkeypatch, importer):
    """Handoff receipts authenticate native ownership and include every producer model in order."""
    from process import ms_drg_result_generation as drg

    candidate, session = _producer_value(importer), _session()
    incoming = handoff._input_from_handoff(candidate)
    lock, owned, catalog = AsyncMock(), AsyncMock(), AsyncMock()
    monkeypatch.setattr(native, "_lock_family", lock)
    monkeypatch.setattr(native, "verify_model_family_stage_ownership", owned)
    monkeypatch.setattr(native, "require_native_read_catalog", catalog)
    rows = AsyncMock(return_value=(3, "a" * 64))
    shape = AsyncMock(return_value="b" * 64)
    monkeypatch.setattr(handoff, "_projected_row_identity", rows)
    monkeypatch.setattr(drg, "_table_shape", shape)
    assert await handoff._input_receipts(session, incoming) == candidate["tables"]
    owned.assert_awaited_once_with(session, incoming.spec, incoming.ownership)
    catalog.assert_awaited_once_with(session, tuple(oid for _, oid in incoming.ownership.relation_oids))
    assert [call.args[2] for call in rows.await_args_list] == list(incoming.spec.table_names)
    assert [call.args[2] for call in shape.await_args_list] == list(incoming.spec.model_types)
    lock.assert_awaited_once_with(
        session, incoming.ownership.schema_name, incoming.spec.table_names, "SHARE", nowait=True
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "missing_attempt", "cas"])
async def test_record_handoff_requires_real_attempt_and_fences_its_transition(monkeypatch, failure):
    """Only the actual running attempt may durably transition its exact prepared input to finalizing."""
    candidate, session = _value(), _session()
    incoming = handoff._input_from_handoff(candidate)
    context_by_field = {
        "context": dict(
            control_run_id="run-1", _control_attempt_id="attempt-1", _control_attempt_started_at="2026-01-01T00:00:00Z"
        )
    }
    if failure == "missing_attempt":
        context_by_field["context"].pop("_control_attempt_id")
    run = AsyncMock(return_value=_run(candidate))
    monkeypatch.setattr(handoff, "_native_control_run", run)
    family = AsyncMock()
    monkeypatch.setattr(handoff.publication, "_current_family", family)
    monkeypatch.setattr(
        handoff.publication, "_current_generations", AsyncMock(return_value=candidate["expected_generations"])
    )
    monkeypatch.setattr(native, "_relation_oid", AsyncMock(return_value=12))
    monkeypatch.setattr(handoff, "_input_receipts", AsyncMock(return_value=candidate["tables"]))
    session.scalar.side_effect = [11, 13, None if failure == "cas" else "run-1"]
    arguments_by_name = dict(
        importer="code-sets",
        schema="serving",
        incoming=incoming,
        options=dict(include_relationships=False, test_mode=False),
        metrics=candidate["metrics"],
    )
    if failure:
        with pytest.raises(native.ReferenceFamilyArchiveError, match="managed attempt|attempt changed"):
            await handoff.record_catalog_handoff(session, context_by_field, **arguments_by_name)
    else:
        assert await handoff.record_catalog_handoff(session, context_by_field, **arguments_by_name) == candidate
        family.assert_awaited_once_with(session, "serving", "code-sets", read_only=True)
    if failure == "missing_attempt":
        run.assert_not_awaited()
        session.scalar.assert_not_awaited()
    else:
        statement, parameters = session.scalar.await_args.args
        assert "status='running' AND finished_at IS NULL" in str(statement)
        assert "metrics->'scoped_catalog_handoff' IS NULL" in str(statement)
        assert parameters["handoff"] == handoff.canonical_metadata(candidate)
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [None, "run", "node", "handoff"])
async def test_catalog_cancel_request_never_retargets_a_different_attempt(monkeypatch, changed):
    """Cancellation authenticates the stored run, node and candidate before writing its request."""
    candidate, session = _value(), _session()
    current_by_field = dict(run_id=candidate["run_id"], metrics={handoff.METRIC: candidate})
    run = _run(candidate)
    if changed == "run":
        run["run_id"] = "other"
    if changed == "node":
        run["node_id"] = "other"
    if changed == "handoff":
        run["metrics"] = {}
    monkeypatch.setattr(handoff, "_native_control_run", AsyncMock(return_value=run))
    if changed:
        with pytest.raises(native.ReferenceFamilyArchiveError, match="cancellation attempt differs"):
            await handoff.request_catalog_cancel(session, current_by_field)
        session.execute.assert_not_awaited()
    else:
        await handoff.request_catalog_cancel(session, current_by_field)
        statement, parameters = session.execute.await_args.args
        assert "SET status='canceling'" in str(statement)
        assert parameters == {"run_id": candidate["run_id"]}


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "contract", "predecessor", "owner"])
async def test_publication_readback_binds_retained_authority_before_origin(monkeypatch, failure):
    """Readback accepts only the exact retained predecessor, owner and preserved native schema."""
    from process import scoped_catalog_retention as retention
    from tests.test_scoped_catalog_retention import _receipt, _signed

    candidate, session = _value(), _session()
    retained = _receipt()
    retained["publication_handoff_sha256"] = candidate["handoff_sha256"]
    if failure == "predecessor":
        retained["previous_generations"]["code-sets"]["local_generation"] = 9
    retained = _signed(retained)
    receipt_by_field = {"handoff": candidate, "retained": retained}
    if failure == "contract":
        receipt_by_field["extra"] = True
    authority, preserved, origin = AsyncMock(), AsyncMock(), AsyncMock()
    monkeypatch.setattr(retention, "require_retained_catalog", authority)
    incumbent = SimpleNamespace(relation_oids=(("code_catalog", 23),))
    monkeypatch.setattr(
        handoff.publication,
        "_current_family",
        AsyncMock(return_value=(object(), incumbent, 99 if failure == "owner" else retained["owner_oid"])),
    )
    monkeypatch.setattr(handoff.publication, "require_preserved_catalog_schema", preserved)
    monkeypatch.setattr(handoff, "_require_published_origin", origin)
    if failure:
        with pytest.raises(
            native.ReferenceFamilyArchiveError, match="contract differs|predecessor differs|owner differs"
        ):
            await handoff.require_catalog_publication(session, receipt_by_field)
        preserved.assert_not_awaited()
        origin.assert_not_awaited()
    else:
        assert await handoff.require_catalog_publication(session, receipt_by_field) is receipt_by_field
        authority.assert_awaited_once_with(session, retained)
        preserved.assert_awaited_once_with(session, (retained["schema_name"], 13), ("serving", 23))
        origin.assert_awaited_once_with(session, candidate, retained)
    if failure in {"contract", "predecessor"}:
        authority.assert_not_awaited()
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("importer", ["code-sets", "ms-drg"])
@pytest.mark.parametrize("read_only", [False, True])
async def test_standalone_preparation_is_unsealed_and_copies_exact_authority(monkeypatch, importer, read_only):
    """Standalone COPY retains authenticated authority and a signed preparation, never publication."""
    from process import scoped_catalog_retention as retention

    candidate, session = _producer_value(importer), _session()
    incoming = handoff._input_from_handoff(candidate)
    options, metrics, payloads = {"test_mode": False, "include_relationships": False}, {"pos_rows": 1}, {"rows": []}
    monkeypatch.setattr(handoff.publication, "precreate_catalog_input", AsyncMock(return_value=incoming))
    copy = AsyncMock()
    monkeypatch.setattr(handoff.publication, "copy_catalog_records", copy)
    monkeypatch.setattr(handoff.publication.binding, "pin_catalog_source", AsyncMock(return_value=read_only))
    previous_by_field = {"local_generation": 0}
    authority = AsyncMock(return_value=previous_by_field)
    monkeypatch.setattr(retention, "copy_generation_authority", authority)
    monkeypatch.setattr(handoff, "_input_receipts", AsyncMock(return_value=candidate["tables"]))
    monkeypatch.setattr(native, "_schema_oid", AsyncMock(return_value=90))
    monkeypatch.setattr(native, "_relation_oid", AsyncMock(return_value=91))
    activate, publisher = AsyncMock(), AsyncMock()
    monkeypatch.setattr(handoff.publication, "activate_catalog_family", activate)
    monkeypatch.setattr(native, "protected_publisher_owner", publisher)
    outcome = await handoff.prepare_catalog_handoff(
        _database(session),
        None,
        importer=importer,
        schema="serving",
        payloads=payloads,
        options=options,
        metrics=metrics,
    )
    assert outcome["status"] == "prepared" and outcome["pos_rows"] == 1
    receipt = outcome["scoped_catalog_preparation"]
    unsigned_by_field = {key: field_value for key, field_value in receipt.items() if key != "preparation_sha256"}
    assert receipt["preparation_sha256"] == handoff._digest(unsigned_by_field)
    assert receipt["contract"] == "scoped-catalog-prepared.v1" and receipt["previous_generation"] == previous_by_field
    assert receipt["input"] == candidate["input"] and receipt["tables"] == candidate["tables"]
    assert (receipt["control_schema_oid"], receipt["generation_oid"]) == (90, 91)
    authority.assert_awaited_once_with(session, importer, "serving", receipt["control_schema"], read_only=read_only)
    copy.assert_awaited_once_with(session, incoming, payloads)
    assert "CREATE SCHEMA" in str(session.execute.await_args_list[0].args[0])
    assert "SET retained_family=CAST(:receipt AS jsonb)" in str(session.execute.await_args.args[0])
    assert json.loads(session.execute.await_args.args[1]["receipt"]) == receipt
    activate.assert_not_awaited()
    publisher.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("include_relationships", [False, True])
@pytest.mark.parametrize("cas_changed", [False, True])
async def test_ms_drg_publication_uses_real_generation_callback_and_attempt_cas(
    monkeypatch, include_relationships, cas_changed
):
    """The MS-DRG callback retains its relationship option and cleanup follows only a successful CAS."""
    from process import ms_drg_result_generation as drg

    candidate, session = _producer_value("ms-drg", include_relationships), _session()
    incoming = handoff._input_from_handoff(candidate)
    prepared = SimpleNamespace(generations=candidate["expected_generations"])
    monkeypatch.setattr(native, "protected_publisher_owner", AsyncMock())
    monkeypatch.setattr(handoff, "require_catalog_handoff", AsyncMock(return_value=(candidate, incoming)))
    monkeypatch.setattr(handoff.publication, "compose_catalog_family", AsyncMock(return_value=prepared))
    publish = AsyncMock(return_value={"local_generation": 1})
    monkeypatch.setattr(drg, "publish_local_generation", publish)

    async def activate(observed_session, observed_prepared, callback, *, publication_handoff_sha256):
        assert observed_session is session and observed_prepared is prepared
        assert publication_handoff_sha256 == candidate["handoff_sha256"]
        assert await callback() == {"local_generation": 1}
        return {"local_generation": 1}, {"retained": True}

    monkeypatch.setattr(handoff.publication, "activate_catalog_family", activate)
    cleanup = AsyncMock()
    monkeypatch.setattr(native, "cleanup_model_family_stage", cleanup)
    session.scalar.return_value = None if cas_changed else candidate["run_id"]
    if cas_changed:
        with pytest.raises(native.ReferenceFamilyArchiveError, match="publication attempt changed"):
            await handoff.publish_catalog_handoff(session, candidate, source_copy=object())
        cleanup.assert_not_awaited()
    else:
        outcome = await handoff.publish_catalog_handoff(session, candidate, source_copy=object())
        assert outcome == {"handoff": candidate, "retained": {"retained": True}}
        cleanup.assert_awaited_once_with(session, incoming.spec, incoming.ownership)
    publish.assert_awaited_once_with(session, "serving", include_relationships=include_relationships)
    assert "progress->>'attempt_id'=:attempt_id" in str(session.scalar.await_args.args[0])


@pytest.mark.asyncio
@pytest.mark.parametrize("include_relationships", [False, True])
@pytest.mark.parametrize("failure", [None, "option", "custody", "binding"])
async def test_ms_drg_published_origin_requires_options_custody_and_native_binding(
    monkeypatch, include_relationships, failure
):
    """Ordinary MS-DRG publication authenticates typed lineage and the unchanged relationship contract."""
    from process import ms_drg_result_generation as drg
    from process import scoped_catalog_binding as binding

    candidate, session = _value(), _session()
    lineage = str(UUID(int=3))
    candidate.update(importer_id="ms-drg", include_relationships=include_relationships)
    candidate["expected_generations"] = {"ms-drg": {"local_lineage_id": lineage, "local_generation": 0}}
    current_by_field = dict(
        local_lineage_id=lineage,
        origin_lineage_id=lineage,
        local_generation=1,
        origin_generation=1,
        published_at="2026-01-01T00:00:00+00:00",
        include_relationships=not include_relationships if failure == "option" else include_relationships,
    )
    retained_by_field = dict(current_generation=current_by_field, generation_table=drg.TABLE)
    monkeypatch.setattr(binding, "_lock_generation", AsyncMock(return_value=30))
    custody = AsyncMock(side_effect=RuntimeError("custody refused") if failure == "custody" else None)
    monkeypatch.setattr(binding, "require_closed_catalog_binding", custody)
    proof = AsyncMock(side_effect=RuntimeError("binding refused") if failure == "binding" else None)
    monkeypatch.setattr(binding, "require_ms_drg_binding", proof)
    if failure:
        with pytest.raises(RuntimeError, match="option differs|custody refused|binding refused"):
            await handoff._require_published_origin(session, candidate, retained_by_field)
        if failure != "binding":
            proof.assert_not_awaited()
    else:
        await handoff._require_published_origin(session, candidate, retained_by_field)
        observed = proof.await_args.args[2]
        assert observed["local_lineage_id"] == observed["origin_lineage_id"] == UUID(lineage)
        assert observed["published_at"] == datetime.fromisoformat(current_by_field["published_at"])
        assert observed["include_relationships"] is include_relationships
    custody.assert_awaited_once_with(session, "serving", ((drg.TABLE, 30),))
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "copy", "lost_commit", "rolled_back", "unknown"])
async def test_preparation_never_claims_publication(monkeypatch, failure):
    candidate, session, context = _value(), _session(), {"context": {}}
    incoming = handoff._input_from_handoff(candidate)
    monkeypatch.setattr(handoff.publication, "precreate_catalog_input", AsyncMock(return_value=incoming))
    monkeypatch.setattr(
        handoff.publication,
        "copy_catalog_records",
        AsyncMock(side_effect=RuntimeError("copy failed") if failure == "copy" else None),
    )
    monkeypatch.setattr(handoff, "record_catalog_handoff", AsyncMock(return_value=candidate))
    reconcile = AsyncMock(
        return_value=failure == "lost_commit", side_effect=RuntimeError("unavailable") if failure == "unknown" else None
    )
    monkeypatch.setattr(handoff, "_reconcile_catalog_handoff", reconcile)
    database = _database(
        session, commit_error=RuntimeError("commit uncertain") if failure not in {None, "copy"} else None
    )
    arguments_by_name = dict(
        importer="code-sets", schema="serving", payloads={}, options={"test_mode": False}, metrics={"pos_rows": 1}
    )
    if failure in {"copy", "rolled_back", "unknown"}:
        with pytest.raises(RuntimeError):
            await handoff.prepare_catalog_handoff(database, context, **arguments_by_name)
        assert not context["context"].get("control_run_handoff_committed")
        assert context["context"].get("scoped_catalog_commit_unknown", False) is (failure == "unknown")
    else:
        outcome = await handoff.prepare_catalog_handoff(database, context, **arguments_by_name)
        assert outcome == {"pos_rows": 1, "status": "finalizing", handoff.METRIC: candidate}
        assert context["context"]["_control_committed_result"] is outcome
        assert context["context"]["control_run_handoff_committed"] is True
    if failure in {None, "copy"}:
        reconcile.assert_not_awaited()


@pytest.mark.asyncio
async def test_managed_bounded_run_refuses_before_preparation(monkeypatch):
    create = AsyncMock()
    monkeypatch.setattr(handoff.publication, "precreate_catalog_input", create)
    with pytest.raises(native.ReferenceFamilyArchiveError, match="bounded"):
        await handoff.prepare_catalog_handoff(
            object(), {}, importer="code-sets", schema="serving", payloads={}, options={"test_mode": True}, metrics={}
        )
    create.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("stale", [False, True])
async def test_publisher_uses_authenticated_handoff_and_same_session(monkeypatch, stale):
    candidate, session, events = _value(), _session(), []
    incoming = handoff._input_from_handoff(candidate)
    prepared = SimpleNamespace(generations={} if stale else candidate["expected_generations"])
    monkeypatch.setattr(native, "protected_publisher_owner", AsyncMock())
    monkeypatch.setattr(handoff, "require_catalog_handoff", AsyncMock(return_value=(candidate, incoming)))
    compose = AsyncMock(return_value=prepared)
    monkeypatch.setattr(handoff.publication, "compose_catalog_family", compose)
    from process import code_sets_result_archive as codes

    monkeypatch.setattr(codes, "publish_local_generation", AsyncMock(return_value="current"))

    async def activate(observed_session, observed_prepared, callback, *, publication_handoff_sha256):
        assert observed_session is session and observed_prepared is prepared
        assert publication_handoff_sha256 == candidate["handoff_sha256"]
        events.append("activate")
        assert await callback() == "current"
        return "current", {"retained": True}

    monkeypatch.setattr(handoff.publication, "activate_catalog_family", activate)
    monkeypatch.setattr(native, "cleanup_model_family_stage", AsyncMock())
    session.scalar.return_value = candidate["run_id"]
    capability = object()
    if stale:
        with pytest.raises(native.ReferenceFamilyArchiveError, match="predecessor"):
            await handoff.publish_catalog_handoff(session, candidate, source_copy=capability)
        assert events == []
        session.scalar.assert_not_awaited()
    else:
        receipt = await handoff.publish_catalog_handoff(session, candidate, source_copy=capability)
        assert receipt == {"handoff": candidate, "retained": {"retained": True}}
        assert compose.await_args.args[-1] is capability
        assert compose.await_args.args[0] is session
        statement, parameters = session.scalar.await_args.args
        assert "status='succeeded'" in str(statement) and "scoped_catalog_publication" in str(statement)
        assert "attempt_started_at" in str(statement) and "scoped_catalog_handoff" in str(statement)
        assert parameters["handoff"] == handoff.canonical_metadata(candidate)
        native.cleanup_model_family_stage.assert_awaited_once_with(session, incoming.spec, incoming.ownership)


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [False, True])
async def test_cancellation_cleans_only_authenticated_candidate(monkeypatch, changed):
    candidate, session = _value(), _session()
    incoming = handoff._input_from_handoff(candidate)
    monkeypatch.setattr(native, "protected_publisher_owner", AsyncMock())
    monkeypatch.setattr(handoff, "require_catalog_handoff", AsyncMock(return_value=(candidate, incoming)))
    monkeypatch.setattr(native, "cleanup_model_family_stage", AsyncMock())
    session.scalar.return_value = None if changed else candidate["run_id"]
    if changed:
        with pytest.raises(native.ReferenceFamilyArchiveError, match="attempt changed"):
            await handoff.cancel_catalog_handoff(session, candidate)
    else:
        assert (await handoff.cancel_catalog_handoff(session, candidate))["status"] == "canceled"
    handoff.require_catalog_handoff.assert_awaited_once_with(session, candidate, canceling=True)
    native.cleanup_model_family_stage.assert_awaited_once_with(session, incoming.spec, incoming.ownership)
    assert "status='canceling'" in str(session.scalar.await_args.args[0])
    assert not hasattr(session, "commit")


def test_worker_terminal_and_heartbeat_writes_preserve_handoff():
    statement = control_lifecycle._where_no_places_handoff(update(ImportRun))
    compiled = statement.compile(dialect=postgresql.dialect())
    assert "scoped_catalog_handoff" in compiled.params.values()
    assert ["code-sets", "ms-drg"] in compiled.params.values()
    assert {"process.code_sets", "process.ms_drg"} <= control_lifecycle._HANDOFF_MODULES
    assert control_lifecycle._has_unknown_native_commit(
        {"context": {"scoped_catalog_commit_unknown": True}}, "process.code_sets"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["finalizing", "canceling", "succeeded", "canceled"])
async def test_commit_readback_requires_real_native_outcome(monkeypatch, status):
    candidate, session = _value(), _session()
    publication_by_field = {"handoff": candidate, "retained": {"protected": True}}
    cancellation_by_field = {"handoff": candidate, "status": "canceled"}
    run_by_field = {
        **_run(candidate),
        "status": status,
        "finished_at": None if status in {"finalizing", "canceling"} else "finished",
    }
    run_by_field["metrics"].update(candidate["metrics"])
    if status == "succeeded":
        run_by_field["phase_detail"] = "catalog published"
        run_by_field["metrics"]["scoped_catalog_publication"] = publication_by_field
    if status == "canceled":
        run_by_field["phase_detail"] = "catalog preparation abandoned"
        run_by_field["metrics"]["scoped_catalog_cancellation"] = cancellation_by_field
    monkeypatch.setattr(native, "protected_publisher_owner", AsyncMock())
    monkeypatch.setattr(handoff, "_read_outcome_run", AsyncMock(return_value=run_by_field))
    pending = AsyncMock()
    native_proof = AsyncMock()
    monkeypatch.setattr(handoff, "require_catalog_handoff", pending)
    monkeypatch.setattr(handoff, "require_catalog_publication", native_proof)
    session.scalar.return_value = None
    outcome = await handoff.read_catalog_handoff_outcome(session, candidate)
    if status in {"finalizing", "canceling"}:
        assert outcome is None
        pending.assert_awaited_once_with(session, candidate, canceling=status == "canceling")
        session.scalar.assert_not_awaited()
    if status == "succeeded":
        assert outcome is publication_by_field
        native_proof.assert_awaited_once_with(session, publication_by_field)
    if status == "canceled":
        assert outcome == cancellation_by_field
        native_proof.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changed", ["unfinished", "input", "status", "phase", "handoff", "metrics", "native", "cancel_receipt"]
)
async def test_commit_readback_never_accepts_terminal_marker_alone(monkeypatch, changed):
    candidate, session = _value(), _session()
    receipt_by_field = {"handoff": candidate, "retained": {"protected": True}}
    run_by_field = {
        **_run(candidate),
        "status": "succeeded",
        "finished_at": "finished",
        "phase_detail": "catalog published",
    }
    run_by_field["metrics"].update({**candidate["metrics"], "scoped_catalog_publication": receipt_by_field})
    session.scalar.return_value = None
    if changed == "unfinished":
        run_by_field["finished_at"] = None
    if changed == "input":
        session.scalar.return_value = 99
    if changed == "status":
        run_by_field["status"] = "failed"
    if changed == "phase":
        run_by_field["phase_detail"] = "other"
    if changed == "handoff":
        receipt_by_field["handoff"] = {}
    if changed == "metrics":
        run_by_field["metrics"]["pos_rows"] = 99
    if changed == "cancel_receipt":
        run_by_field.update(status="canceled", phase_detail="catalog preparation abandoned")
    monkeypatch.setattr(native, "protected_publisher_owner", AsyncMock())
    monkeypatch.setattr(handoff, "_read_outcome_run", AsyncMock(return_value=run_by_field))
    monkeypatch.setattr(
        handoff,
        "require_catalog_publication",
        AsyncMock(side_effect=RuntimeError("native refusal") if changed == "native" else None),
    )
    with pytest.raises(RuntimeError):
        await handoff.read_catalog_handoff_outcome(session, candidate)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changed", [None, "database", "control_oid", "engine", "attempt", "params", "node", "handoff", "error"]
)
async def test_outcome_row_binds_real_control_attempt(monkeypatch, changed):
    candidate, session = _value(), _session()
    run_by_field = {
        **_run(candidate),
        "importer": candidate["importer_id"],
        "engine": "healthcare-mrf-api",
        "error": None,
        "progress": {key: candidate[key] for key in ("attempt_id", "attempt_started_at")},
    }
    session.execute.return_value = SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: run_by_field))
    monkeypatch.setattr(native, "_relation_oid", AsyncMock(return_value=12))
    monkeypatch.setattr(native, "require_native_read_catalog", AsyncMock())
    if changed == "database":
        session.scalar.return_value = 99
    if changed == "control_oid":
        native._relation_oid.return_value = 99
    if changed == "engine":
        run_by_field["engine"] = "other"
    if changed == "attempt":
        run_by_field["progress"]["attempt_id"] = "other"
    if changed == "params":
        run_by_field["params"] = {"changed": True}
    if changed == "node":
        run_by_field["node_id"] = "other"
    if changed == "handoff":
        run_by_field["metrics"][handoff.METRIC] = {}
    if changed == "error":
        run_by_field["error"] = {"message": "failed"}
    if changed:
        with pytest.raises(native.ReferenceFamilyArchiveError):
            await handoff._read_outcome_run(session, candidate)
    else:
        assert await handoff._read_outcome_run(session, candidate) is run_by_field
        assert "FOR UPDATE" in str(session.execute.await_args.args[0])


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [None, "lineage", "generation", "origin", "bool"])
async def test_success_receipt_requires_ordinary_origin_and_native_current_binding(monkeypatch, changed):
    from process import code_sets_result_archive as codes
    from process import scoped_catalog_binding as binding

    candidate, session = _value(), _session()
    lineage = str(UUID(int=3))
    candidate["expected_generations"]["code-sets"] = {"local_lineage_id": lineage, "local_generation": 0}
    current_by_field = dict(
        local_lineage_id=lineage,
        local_generation=1,
        origin_lineage_id=lineage,
        origin_generation=1,
        published_at="2026-01-01T00:00:00+00:00",
        code_catalog_oid=20,
        row_count=3,
        row_sha256="a" * 64,
    )
    if changed == "lineage":
        current_by_field["local_lineage_id"] = str(UUID(int=4))
    if changed == "generation":
        current_by_field["local_generation"] = 2
    if changed == "origin":
        current_by_field["origin_lineage_id"] = str(UUID(int=4))
    if changed == "bool":
        current_by_field["origin_generation"] = True
    retained_by_field = dict(
        current_generation=current_by_field, generation_table=codes.TABLE, tables=[{"relation_oid": 10}]
    )
    monkeypatch.setattr(binding, "_lock_generation", AsyncMock(return_value=30))
    monkeypatch.setattr(binding, "require_closed_catalog_binding", AsyncMock())
    monkeypatch.setattr(codes, "_column_signature", AsyncMock(return_value=("shape",)))
    current_binding = AsyncMock()
    monkeypatch.setattr(binding, "require_code_sets_binding", current_binding)
    if changed:
        with pytest.raises(native.ReferenceFamilyArchiveError, match="ordinary publication origin"):
            await handoff._require_published_origin(session, candidate, retained_by_field)
        current_binding.assert_not_awaited()
    else:
        await handoff._require_published_origin(session, candidate, retained_by_field)
        assert current_binding.await_args.args[:2] == (session, "serving")
        assert current_binding.await_args.args[2].origin_lineage_id == UUID(lineage)
        assert current_binding.await_args.kwargs == {"schema_sha256": codes._schema_digest(("shape",))}


@pytest.mark.asyncio
async def test_publication_receipt_cannot_be_reused_for_another_handoff(monkeypatch):
    from process import scoped_catalog_retention as retention
    from tests.test_scoped_catalog_retention import _receipt, _signed

    candidate = _value()
    retained = _receipt()
    retained["publication_handoff_sha256"] = "f" * 64
    protected_proof = AsyncMock()
    monkeypatch.setattr(retention, "require_retained_catalog", protected_proof)
    with pytest.raises(native.ReferenceFamilyArchiveError, match="publication handoff differs"):
        await handoff.require_catalog_publication(_session(), {"handoff": candidate, "retained": _signed(retained)})
    protected_proof.assert_not_awaited()
