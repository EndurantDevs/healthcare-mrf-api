# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Logical attempt fencing, bounded content contracts and uncertain-COMMIT recovery."""

from __future__ import annotations

from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import reference_family_archive as native
from process import scoped_catalog_handoff as handoff
from process import scoped_catalog_named_generation as generations
from process import scoped_catalog_named_publication as named


def _generation():
    return dict(
        local_lineage_id=str(UUID(int=7)),
        local_generation=0,
        origin_lineage_id=None,
        origin_generation=None,
        published_at=None,
    )


def _published_generation():
    return {
        **_generation(),
        "origin_lineage_id": str(UUID(int=8)),
        "origin_generation": 1,
        "published_at": "2026-01-01T00:00:00+00:00",
    }


@pytest.mark.parametrize(
    ("field", "field_value", "error"),
    [
        ("local_lineage_id", str(UUID(int=0)), "logical generation"),
        ("local_generation", True, "logical generation"),
        ("local_generation", 2**63, "logical generation"),
        ("origin_lineage_id", str(UUID(int=0)), "logical origin"),
        ("origin_generation", 0, "logical origin"),
        ("origin_generation", True, "logical origin"),
        ("origin_generation", 2**63, "logical origin"),
        ("published_at", datetime(2026, 1, 1), "publication time"),
        ("published_at", "2026-01-01T00:00:00", "publication time"),
        ("published_at", 1, "publication time"),
    ],
)
def test_logical_generation_refuses_invalid_revision_or_origin(field, field_value, error):
    generation_by_field = {**_published_generation(), field: field_value}
    with pytest.raises(RuntimeError, match=error):
        generations.logical_generation(generation_by_field)


@pytest.mark.parametrize("field", ["origin_lineage_id", "origin_generation", "published_at"])
def test_logical_generation_refuses_partial_publication(field):
    with pytest.raises(RuntimeError, match="origin is incomplete"):
        generations.logical_generation({**_published_generation(), field: None})


@pytest.mark.parametrize("publication", [datetime(2026, 1, 1, tzinfo=timezone.utc), "2026-01-01T00:00:00+00:00"])
def test_logical_generation_preserves_complete_portable_revision(publication):
    generation_by_field = {
        **_published_generation(),
        "local_lineage_id": UUID(int=7),
        "local_generation": 2**63 - 1,
        "origin_lineage_id": UUID(int=8),
        "published_at": publication,
    }
    before = deepcopy(generation_by_field)
    assert generations.logical_generation(generation_by_field) == {
        **_published_generation(),
        "local_generation": 2**63 - 1,
    }
    assert generation_by_field == before


def _generation_session(generation_rows):
    return SimpleNamespace(
        execute=AsyncMock(return_value=SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: generation_rows)))
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("importer", ["code-sets", "ms-drg", "unsupported"])
async def test_generation_read_refuses_missing_or_unsupported_producer(monkeypatch, importer):
    relation = AsyncMock(return_value=None)
    monkeypatch.setattr(native, "_relation_oid", relation)
    session = _generation_session([])
    with pytest.raises(RuntimeError, match="producer is unsupported|generation is unavailable"):
        await generations.read_generations(session, "serving", importer=importer)
    session.execute.assert_not_awaited()
    if importer == "unsupported":
        relation.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("generation_rows", [[], [{"id": 2}], [{"id": 1}, {"id": 1}]])
async def test_generation_read_refuses_invalid_singleton(monkeypatch, generation_rows):
    monkeypatch.setattr(native, "_relation_oid", AsyncMock(return_value=1))
    session = _generation_session(generation_rows)
    with pytest.raises(RuntimeError, match="generation singleton differs"):
        await generations.read_generations(session, "serving")
    assert session.execute.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("importer", ["code-sets", "ms-drg"])
@pytest.mark.parametrize("lock", [False, True])
async def test_generation_read_keeps_optional_companion_and_lock_semantics(monkeypatch, importer, lock):
    monkeypatch.setattr(
        native,
        "_relation_oid",
        AsyncMock(side_effect=lambda _session, _schema, name: 1 if name == generations.TABLES[importer] else None),
    )
    session = _generation_session([{"id": 1, **_generation()}])
    assert await generations.read_generations(session, "serving", lock=lock, importer=importer) == {
        importer: _generation()
    }
    statement = str(session.execute.await_args.args[0])
    assert statement.startswith("SELECT id,") and f'"serving"."{generations.TABLES[importer]}"' in statement
    assert statement.endswith(" FOR UPDATE") is lock


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("importer", "count", "digest", "exhausted"),
    [
        ("code-sets", 1, "a" * 64, True),
        ("ms-drg", 1, "a" * 64, True),
        ("code-sets", True, "a" * 64, False),
        ("code-sets", -1, "a" * 64, False),
        ("code-sets", 1, "A" * 64, False),
    ],
)
async def test_generation_publish_refuses_before_update(monkeypatch, importer, count, digest, exhausted):
    from process import code_sets_result_archive as codes

    prior_by_field = {**_generation(), "local_generation": 2**63 - 1 if exhausted else 0}
    read = AsyncMock(return_value={importer: prior_by_field})
    content = AsyncMock(return_value=(count, digest, None))
    monkeypatch.setattr(generations, "read_generations", read)
    monkeypatch.setattr(codes, "scope_receipt", content)
    session = _generation_session([])
    with pytest.raises(RuntimeError, match="generation is exhausted|source content is invalid"):
        await generations.publish_generation(session, "serving", importer=importer)
    session.execute.assert_not_awaited()
    read.assert_awaited_once_with(session, "serving", lock=True, importer=importer)
    if exhausted:
        content.assert_not_awaited()
    else:
        content.assert_awaited_once_with(session, "serving")


@pytest.mark.asyncio
@pytest.mark.parametrize("active", [False, True])
async def test_published_code_set_content_refuses_changed_receipt(monkeypatch, active):
    from process import code_sets_result_archive as codes

    session = SimpleNamespace(
        execute=AsyncMock(
            return_value=SimpleNamespace(
                mappings=lambda: SimpleNamespace(one=lambda: {"row_count": 2, "row_sha256": "b" * 64})
            )
        )
    )
    content = AsyncMock(return_value=(1, "a" * 64, None))
    monkeypatch.setattr(codes, "_slice_identity", content)
    vector_by_importer = {"code-sets": _published_generation() if active else _generation()}
    with pytest.raises(RuntimeError, match="published source content differs"):
        await generations.validate_published_content(session, "serving", vector_by_importer)
    content.assert_awaited_once_with(session, "serving", complete=active)


@pytest.mark.asyncio
async def test_untracked_companion_refuses_a_stored_content_receipt(monkeypatch):
    family = AsyncMock()
    monkeypatch.setattr(handoff.storage, "current_models", family)
    session = SimpleNamespace(scalar=AsyncMock(return_value={"content_sha256": "a" * 64}))
    with pytest.raises(RuntimeError, match="untracked companion catalog content"):
        await generations.validate_published_content(session, "serving", {"ms-drg": _generation()})
    family.assert_not_awaited()


def _sign(value):
    unsigned_by_field = {key: item for key, item in value.items() if key != "handoff_sha256"}
    return {**unsigned_by_field, "handoff_sha256": named.digest(unsigned_by_field)}


def _value(importer="code-sets"):
    tables = [
        {"table": name, "row_count": 3, "row_sha256": "a" * 64, "schema_sha256": "b" * 64}
        for name in handoff.publication.input_spec(importer).table_names
    ]
    value_by_field = dict(
        contract=named.CONTRACT,
        importer_id=importer,
        schema_name="serving",
        run_id="run-1",
        attempt_id="attempt-1",
        attempt_started_at="2026-01-01T00:00:00+00:00",
        node_id="node-1",
        input={"dataset_id": str(UUID(int=1)), "schema_name": native.reference_family_stage_schema(UUID(int=1))},
        tables=tables,
        expected_tables=tables,
        expected_generations={importer: _generation()},
        include_relationships=False,
        test_mode=False,
        metrics={"rows": 3},
    )
    value_by_field["source_contract_sha256"] = named.digest(
        {
            "importer": importer,
            "params": {},
            "options": {"include_relationships": False, "test_mode": False},
            "metrics": value_by_field["metrics"],
        }
    )
    return _sign(value_by_field)


def _attempt_context(candidate):
    return {
        "context": {
            "control_run_id": candidate["run_id"],
            "_control_attempt_id": candidate["attempt_id"],
            "_control_attempt_started_at": candidate["attempt_started_at"],
        }
    }


def _database_session(session):
    @asynccontextmanager
    async def transaction():
        yield session

    return SimpleNamespace(transaction=transaction)


@pytest.mark.parametrize(
    ("field", "field_value", "error"),
    [
        ("tables", [], "table receipt inventory"),
        ("source_contract_sha256", "g" * 64, "source digest"),
        ("metrics", {"padding": "x" * 65536}, "exceeds its bound"),
    ],
)
def test_handoff_refuses_invalid_or_oversized_data_receipts(field, field_value, error):
    candidate = _sign({**_value(), field: field_value})
    with pytest.raises(RuntimeError, match=error):
        named.validate_handoff(candidate)


def test_handoff_signature_detects_changed_metrics():
    candidate = _value()
    candidate["metrics"]["rows"] += 1
    with pytest.raises(RuntimeError, match="source content differs"):
        named.validate_handoff(candidate)


def test_handoff_input_cannot_overlap_serving_schema():
    candidate = _value()
    candidate = _sign({**candidate, "schema_name": candidate["input"]["schema_name"]})
    with pytest.raises(RuntimeError, match="overlaps serving data"):
        named.validate_handoff(candidate)


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["importer", "producer", "attempt", "node", "status"])
async def test_capture_refuses_changed_acquisition_before_catalog_access(monkeypatch, fault):
    candidate = _value()
    ctx = _attempt_context(candidate)
    run_by_field = dict(importer="code-sets", metrics={named.METRIC: candidate}, node_id="node-1", status="finalizing")
    if fault == "producer":
        run_by_field["importer"] = "ms-drg"
    if fault == "attempt":
        run_by_field["metrics"][named.METRIC] = _sign({**candidate, "attempt_id": "other"})
    if fault == "node":
        run_by_field["node_id"] = "other"
    if fault == "status":
        run_by_field["status"] = "running"
    read = AsyncMock(return_value=run_by_field)
    family = AsyncMock()
    monkeypatch.setattr(named, "_run", read)
    monkeypatch.setattr(named.storage, "current_models", family)
    session = _generation_session([])
    with pytest.raises(RuntimeError, match="unsupported|producer changed|attempt changed|status changed"):
        await named.capture_expected(
            _database_session(session),
            ctx,
            importer="unknown" if fault == "importer" else "code-sets",
            schema="serving",
        )
    family.assert_not_awaited()
    assert ctx["context"].get("control_run_handoff_committed") is None
    if fault == "importer":
        read.assert_not_awaited()
        session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("payloads", [{}, {"code_catalog": [{"unexpected": 1}]}])
async def test_prepare_input_refuses_wrong_family_or_columns_before_copy(monkeypatch, payloads):
    stage, copied, complete = AsyncMock(return_value="input"), AsyncMock(), AsyncMock()
    monkeypatch.setattr(named.storage, "create_stage", stage)
    monkeypatch.setattr(native, "native_copy_record_batch", copied)
    monkeypatch.setattr(named.storage, "complete_stage", complete)
    with pytest.raises(RuntimeError, match="input family differs|input columns differ"):
        await named._prepare_input(_generation_session([]), "code-sets", "serving", payloads)
    copied.assert_not_awaited()
    complete.assert_not_awaited()
    if not payloads:
        stage.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["importer", "test-mode", "uncaptured"])
async def test_managed_prepare_refuses_invalid_mode_before_database(monkeypatch, fault):
    copied = AsyncMock()
    monkeypatch.setattr(named, "_prepare_input", copied)
    session = _generation_session([])
    with pytest.raises(RuntimeError, match="unsupported|bounded catalog runs|captured before acquisition"):
        await named.prepare(
            _database_session(session),
            _attempt_context(_value()),
            importer="other" if fault == "importer" else "code-sets",
            schema="serving",
            payloads={},
            options={"test_mode": fault == "test-mode"},
            metrics={},
        )
    session.execute.assert_not_awaited()
    copied.assert_not_awaited()


@pytest.mark.asyncio
async def test_unmanaged_prepare_returns_private_input_without_managed_handoff(monkeypatch):
    candidate = _value()
    copied = AsyncMock(return_value=(candidate["input"], candidate["tables"]))
    record = AsyncMock()
    monkeypatch.setattr(named, "_prepare_input", copied)
    monkeypatch.setattr(named, "_record_handoff", record)
    session = _generation_session([])
    assert await named.prepare(
        _database_session(session),
        None,
        importer="code-sets",
        schema="serving",
        payloads={},
        options={},
        metrics={"rows": 3},
    ) == {
        "rows": 3,
        "status": "prepared",
        "scoped_catalog_preparation": {"input": candidate["input"], "tables": candidate["tables"]},
    }
    copied.assert_awaited_once_with(session, "code-sets", "serving", {})
    record.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["importer", "finished", "handoff"])
async def test_prepare_rechecks_managed_attempt_before_copy(monkeypatch, fault):
    candidate = _value()
    ctx = _attempt_context(candidate)
    ctx["context"][named._CONTEXT_EXPECTED] = {
        "generations": candidate["expected_generations"],
        "tables": candidate["expected_tables"],
    }
    run_by_field = dict(importer="code-sets", finished_at=None, metrics={})
    if fault == "importer":
        run_by_field["importer"] = "ms-drg"
    if fault == "finished":
        run_by_field["finished_at"] = candidate["attempt_started_at"]
    if fault == "handoff":
        run_by_field["metrics"][named.METRIC] = candidate
    copied = AsyncMock()
    monkeypatch.setattr(named, "_run", AsyncMock(return_value=run_by_field))
    monkeypatch.setattr(named, "_prepare_input", copied)
    with pytest.raises(RuntimeError, match="preparation attempt changed"):
        await named.prepare(
            _database_session(_generation_session([])),
            ctx,
            importer="code-sets",
            schema="serving",
            payloads={},
            options={"test_mode": False},
            metrics={},
        )
    copied.assert_not_awaited()
    assert ctx["context"].get("control_run_handoff_committed") is None


@pytest.mark.asyncio
async def test_handoff_record_requires_successful_attempt_compare_and_swap():
    candidate = _value()
    session = SimpleNamespace(scalar=AsyncMock(return_value=None))
    with pytest.raises(RuntimeError, match="preparation attempt changed"):
        await named._record_handoff(
            session, "serving", named.attempt_from_context(_attempt_context(candidate)), candidate
        )
    assert session.scalar.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["scope", "attempt"])
async def test_managed_continuation_refuses_wrong_source_or_attempt(monkeypatch, fault):
    candidate = _value()
    attempt = SimpleNamespace(
        **{name: candidate[name] for name in named._ATTEMPT_FIELDS},
        expected_lineage_id=_generation()["local_lineage_id"],
        expected_generation=0,
    )
    settings = SimpleNamespace(schema="other" if fault == "scope" else "serving")
    if fault == "attempt":
        attempt.attempt_id = "other"
    run_by_field = {"metrics": {named.METRIC: candidate}, "importer": candidate["importer_id"]}
    monkeypatch.setattr(named, "_run", AsyncMock(return_value=run_by_field))
    with pytest.raises(RuntimeError, match="continuation source differs|continuation attempt differs"):
        await named.publish_managed_attempt(_generation_session([]), attempt, settings, source_copy=object())


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["run", "status", "finished"])
async def test_cancel_request_refuses_changed_persisted_attempt(monkeypatch, fault):
    candidate = _value()
    current_by_field = {"run_id": candidate["run_id"], "metrics": {named.METRIC: candidate}}
    run_by_field = {"run_id": candidate["run_id"], "status": "finalizing", "finished_at": None}
    run_by_field[{"run": "run_id", "status": "status", "finished": "finished_at"}[fault]] = "other"
    monkeypatch.setattr(handoff, "_read_outcome_run", AsyncMock(return_value=run_by_field))
    session = _generation_session([])
    with pytest.raises(RuntimeError, match="cancellation attempt differs"):
        await handoff.request_catalog_cancel(session, current_by_field)
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_pending_outcome_refuses_changed_input_receipts(monkeypatch):
    candidate = _value()
    monkeypatch.setattr(
        handoff, "_read_outcome_run", AsyncMock(return_value={"status": "finalizing", "finished_at": None})
    )
    inventory = AsyncMock()
    monkeypatch.setattr(handoff.storage, "require_inventory", inventory)
    monkeypatch.setattr(named, "input_receipts", AsyncMock(return_value=[]))
    with pytest.raises(RuntimeError, match="pending source content differs"):
        await handoff.read_catalog_handoff_outcome(_generation_session([]), candidate)
    assert inventory.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("input_exists", [False, True])
async def test_terminal_cancellation_requires_removed_input_and_exact_receipt(monkeypatch, input_exists):
    candidate = _value()
    run_by_field = {
        "status": "canceled",
        "finished_at": candidate["attempt_started_at"],
        "error": None,
        "phase_detail": "catalog preparation abandoned",
        "metrics": {"scoped_catalog_cancellation": {}},
    }
    monkeypatch.setattr(handoff, "_read_outcome_run", AsyncMock(return_value=run_by_field))
    session = SimpleNamespace(scalar=AsyncMock(return_value=input_exists))
    with pytest.raises(RuntimeError, match="terminal input still exists|cancellation receipt differs"):
        await handoff.read_catalog_handoff_outcome(session, candidate)


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["contract", "destination", "digest", "revision", "origin", "legacy-vector"])
async def test_publication_receipt_keeps_destination_and_complete_logical_vector(monkeypatch, fault):
    candidate = _value()
    retained_by_field = {
        "live_schema": "serving",
        "importer": "code-sets",
        "publication_handoff_sha256": candidate["handoff_sha256"],
        "published_generations": {"code-sets": _generation()},
    }
    receipt_by_field = {"handoff": candidate, "retained": {"contract": "scoped-catalog-retained.v2"}}
    current_by_importer = {"code-sets": _generation()}
    if fault == "contract":
        receipt_by_field.pop("retained")
    if fault == "destination":
        retained_by_field["live_schema"] = "other"
    if fault == "digest":
        retained_by_field["publication_handoff_sha256"] = "c" * 64
    if fault == "revision":
        retained_by_field["published_generations"]["code-sets"]["local_generation"] = 1
    if fault == "origin":
        current_by_importer["code-sets"] = {**_published_generation(), "local_generation": 1}
    if fault == "legacy-vector":
        retained_by_field.pop("published_generations")
    monkeypatch.setattr(handoff.retention, "require_retained", AsyncMock(return_value=retained_by_field))
    monkeypatch.setattr(generations, "read_generations", AsyncMock(return_value=current_by_importer))
    content = AsyncMock()
    monkeypatch.setattr(generations, "validate_published_content", content)
    session = _generation_session([])
    if fault == "legacy-vector":
        assert await handoff.require_catalog_publication(session, receipt_by_field) is receipt_by_field
        content.assert_awaited_once_with(session, "serving", current_by_importer)
    else:
        with pytest.raises(
            RuntimeError, match="contract differs|destination differs|handoff differs|source was replaced"
        ):
            await handoff.require_catalog_publication(session, receipt_by_field)
        content.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel", [False, True])
async def test_compatibility_entry_requires_pending_persisted_status(monkeypatch, cancel):
    from process import scoped_catalog_named_cutover as cutover

    run_by_field = {"status": "succeeded", "finished_at": None}
    monkeypatch.setattr(handoff, "_read_outcome_run", AsyncMock(return_value=run_by_field))
    operation = AsyncMock()
    monkeypatch.setattr(cutover, "cancel" if cancel else "publish", operation)
    settings = SimpleNamespace(schema="serving")
    with pytest.raises(RuntimeError, match="attempt changed"):
        if cancel:
            await handoff.cancel_catalog_handoff(_generation_session([]), _value(), settings=settings)
        else:
            await handoff.publish_catalog_handoff(
                _generation_session([]), _value(), source_copy=object(), settings=settings
            )
    operation.assert_not_awaited()


@pytest.mark.parametrize("importer", ["code-sets", "ms-drg"])
def test_named_handoff_contains_only_logical_attempt_and_data_fields(importer):
    value = _value(importer)
    assert named.validate_handoff(value) is value
    assert set(value["input"]) == {"dataset_id", "schema_name"}
    assert generations.logical_generation({**_generation(), "code_catalog_oid": 999}) == _generation()


@pytest.mark.parametrize(
    "change",
    [
        "importer",
        "run",
        "scope",
        "input",
        "table",
        "count",
        "digest",
        "generation",
        "missing-companion",
        "test",
        "extra",
        "physical",
    ],
)
def test_handoff_refuses_wrong_logical_scope_or_shape(change):
    value = _value()
    if change == "importer":
        value["importer_id"] = "unrelated"
    if change == "run":
        value["attempt_id"] = ""
    if change == "scope":
        value["schema_name"] = "invalid.schema"
    if change == "input":
        value["input"]["schema_name"] = "arbitrary"
    if change == "table":
        value["tables"][0]["table"] = "arbitrary"
    if change == "count":
        value["tables"][0]["row_count"] = -1
    if change == "digest":
        value["tables"][0]["schema_sha256"] = "invalid"
    if change == "generation":
        value["expected_generations"]["code-sets"]["local_generation"] = -1
    if change == "missing-companion":
        value["expected_generations"] = {}
    if change == "test":
        value["test_mode"] = True
    if change == "extra":
        value["extra"] = True
    if change == "physical":
        value["input"]["schema_oid"] = 99
    with pytest.raises((RuntimeError, ValueError, TypeError)):
        named.validate_handoff(_sign(value))


def test_historical_handoff_ignores_copied_physical_fields_but_keeps_logical_scope():
    value = _value()
    value.update(contract="scoped-catalog-handoff.v1", database_oid=123, import_run_oid=456, input_owner_oid=789)
    value.pop("expected_tables")
    value["input"].update(schema_oid=876, relation_oids=[["code_catalog", 987]])
    assert named.validate_handoff(value) is value
    value["input"]["schema_name"] = "wrong"
    with pytest.raises(RuntimeError, match="input name"):
        named.validate_handoff(value)


@pytest.mark.parametrize("contract", ["scoped-catalog-handoff.v1", "scoped-catalog-retained.v1", named.CONTRACT])
@pytest.mark.parametrize("change", [None, "counter", "origin", "wrong-importer", "captured-companion"])
def test_legacy_generation_omission_is_narrow_and_does_not_rewrite_records(contract, change):
    expected_by_importer = {"code-sets": _generation()}
    current_by_importer = {
        **deepcopy(expected_by_importer),
        "ms-drg": {**_generation(), "local_lineage_id": str(UUID(int=8))},
    }
    importer = "code-sets"
    if change == "counter":
        current_by_importer["ms-drg"]["local_generation"] = 1
    if change == "origin":
        current_by_importer["ms-drg"].update(
            origin_lineage_id=str(UUID(int=8)), origin_generation=1, published_at="2026-01-01T00:00:00+00:00"
        )
    if change == "wrong-importer":
        importer = "ms-drg"
        current_by_importer["code-sets"], current_by_importer["ms-drg"] = (
            current_by_importer["ms-drg"],
            current_by_importer["code-sets"],
        )
        expected_by_importer = {"ms-drg": _generation()}
    if change == "captured-companion":
        expected_by_importer["ms-drg"] = {**current_by_importer["ms-drg"], "local_lineage_id": str(UUID(int=9))}
    original = deepcopy((current_by_importer, expected_by_importer))
    normalized_by_importer = generations.compatible_expected_generations(
        current_by_importer, expected_by_importer, contract=contract, importer=importer
    )
    if contract.endswith(".v1") and change is None:
        assert normalized_by_importer == current_by_importer
    else:
        assert normalized_by_importer == expected_by_importer and normalized_by_importer != current_by_importer
    assert (current_by_importer, expected_by_importer) == original


@pytest.mark.asyncio
@pytest.mark.parametrize("resolution", [True, False, "unknown"])
async def test_prepare_reconciles_lost_commit_without_another_copy(monkeypatch, resolution):
    field_value = _value()
    run_by_field = dict(importer="code-sets", node_id="node-1", finished_at=None, metrics={}, params={})
    session = SimpleNamespace(scalar=AsyncMock(return_value="run-1"))
    copied = AsyncMock(return_value=(field_value["input"], field_value["tables"]))
    monkeypatch.setattr(named, "_run", AsyncMock(return_value=run_by_field))
    monkeypatch.setattr(named, "_prepare_input", copied)
    reconcile = AsyncMock(
        side_effect=RuntimeError("unavailable") if resolution == "unknown" else None, return_value=resolution
    )
    monkeypatch.setattr(handoff, "_reconcile_catalog_handoff", reconcile)

    @asynccontextmanager
    async def transaction():
        yield session
        raise RuntimeError("commit response lost")

    database = SimpleNamespace(transaction=transaction)
    ctx_by_field = {
        "context": {
            "control_run_id": "run-1",
            "_control_attempt_id": "attempt-1",
            "_control_attempt_started_at": field_value["attempt_started_at"],
            named._CONTEXT_EXPECTED: {
                "generations": field_value["expected_generations"],
                "tables": field_value["expected_tables"],
            },
        }
    }
    arguments_by_field = dict(
        importer="code-sets",
        schema="serving",
        payloads={},
        options={"include_relationships": False, "test_mode": False},
        metrics=field_value["metrics"],
    )
    if resolution is True:
        result_by_field = await named.prepare(database, ctx_by_field, **arguments_by_field)
        assert (
            result_by_field["status"] == "finalizing"
            and ctx_by_field["context"]["control_run_handoff_committed"] is True
        )
    else:
        with pytest.raises(RuntimeError):
            await named.prepare(database, ctx_by_field, **arguments_by_field)
        assert ctx_by_field["context"].get("control_run_handoff_committed") is None
        assert ctx_by_field["context"].get("scoped_catalog_commit_unknown") is (
            True if resolution == "unknown" else None
        )
    copied.assert_awaited_once()
    reconcile.assert_awaited_once()


@pytest.mark.asyncio
async def test_publisher_library_requires_explicit_operator_settings():
    with pytest.raises(RuntimeError, match="settings"):
        await handoff.publish_catalog_handoff(SimpleNamespace(), _value(), source_copy=object())
    with pytest.raises(RuntimeError, match="settings"):
        await handoff.cancel_catalog_handoff(SimpleNamespace(), _value())


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["attempt", "params", "node", "receipt", "none"])
async def test_outcome_uses_the_actual_persisted_run_and_request(monkeypatch, changed):
    value = _value()
    run_by_field = dict(importer="code-sets", node_id="node-1", metrics={named.METRIC: value}, params={})
    if changed == "params":
        run_by_field["params"] = {"other": True}
    elif changed == "node":
        run_by_field["node_id"] = "other"
    elif changed == "receipt":
        run_by_field["metrics"] = {named.METRIC: {}}
    elif changed == "attempt":
        value = deepcopy(value)
        value["attempt_id"] = "other"
        value = _sign(value)
    observed = AsyncMock(return_value=run_by_field)
    monkeypatch.setattr(named, "_run", observed)
    if changed == "none":
        assert await handoff._read_outcome_run(SimpleNamespace(), value) == run_by_field
    else:
        with pytest.raises(RuntimeError, match="attempt differs"):
            await handoff._read_outcome_run(SimpleNamespace(), value)
    observed.assert_awaited_once()
