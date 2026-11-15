# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Logical attempt fencing, bounded content contracts and uncertain-COMMIT recovery."""

from __future__ import annotations

from contextlib import asynccontextmanager
from copy import deepcopy
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
