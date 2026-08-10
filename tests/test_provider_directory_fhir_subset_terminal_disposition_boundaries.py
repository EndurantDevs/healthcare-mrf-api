# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Boundary coverage for reviewed mixed-terminal disposition helpers."""

from __future__ import annotations

import asyncio
from copy import deepcopy
from dataclasses import replace
import json

import pytest

from process import provider_directory_fhir_subset_terminal_disposition as facade
from process import provider_directory_fhir_subset_terminal_disposition_contract as contract
from process import provider_directory_fhir_subset_terminal_disposition_store as store
from process import provider_directory_fhir_subset_terminal_disposition_util as util
from process.provider_directory_fhir_subset_terminal_disposition_contract import (
    EXPECTED_RESOURCE_TYPES,
    ReviewedSubsetTerminalDispositionError,
    ReviewedSubsetTerminalDispositionResult,
)
from process.provider_directory_fhir_subset_terminal_disposition_selection import (
    selected_reviewed_subset_terminal_disposition,
)
from tests.provider_directory_fhir_subset_terminal_disposition_support import (
    TerminalDispositionDatabase,
)


class _MappedRow:
    def __init__(self, value):
        self._mapping = value


def test_json_and_row_helpers_cover_driver_boundary_shapes():
    assert util.json_object('{"value":1}') == {"value": 1}
    assert util.json_text_tuple('["one","two"]') == ("one", "two")
    assert util.row_mapping(_MappedRow({"value": 1})) == {"value": 1}

    for value in ("{", "[]", 17):
        with pytest.raises(ReviewedSubsetTerminalDispositionError):
            util.json_object(value)
    for value in ("[", [], [""], [1]):
        with pytest.raises(ReviewedSubsetTerminalDispositionError):
            util.json_text_tuple(value)
    with pytest.raises(ReviewedSubsetTerminalDispositionError):
        util.row_mapping(object())


def test_schema_and_relation_helpers_reject_conflicts(monkeypatch):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "runtime_schema")
    monkeypatch.setenv("DB_SCHEMA", "legacy_schema")
    with pytest.raises(ReviewedSubsetTerminalDispositionError):
        util.schema_name()

    monkeypatch.delenv("DB_SCHEMA")
    assert util.schema_name() == "runtime_schema"
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "invalid-schema")
    with pytest.raises(ReviewedSubsetTerminalDispositionError):
        util.schema_name()
    with pytest.raises(ReviewedSubsetTerminalDispositionError):
        util.quoted_relation("invalid-table")


@pytest.mark.asyncio
async def test_facade_translates_catalog_and_runtime_failures(monkeypatch):
    monkeypatch.setenv(contract.TERMINAL_DISPOSITION_ENABLED_ENV, "true")

    def invalid_source_id():
        raise RuntimeError("private detail")

    monkeypatch.setattr(
        "process.provider_directory_fhir_manual_catalog."
        "reviewed_manual_census_source_id",
        invalid_source_id,
    )
    with pytest.raises(ReviewedSubsetTerminalDispositionError) as error:
        await facade.dispose_reviewed_subset_census_drift_root(
            database=TerminalDispositionDatabase()
        )
    assert error.value.code == "evidence"


@pytest.mark.asyncio
async def test_facade_resolves_default_database(monkeypatch):
    monkeypatch.setenv(contract.TERMINAL_DISPOSITION_ENABLED_ENV, "true")
    monkeypatch.setattr(
        "process.provider_directory_fhir_manual_catalog."
        "reviewed_manual_census_source_id",
        lambda: "source-a",
    )
    sentinel = object()
    observed_calls = []

    async def sync(database, expected_source_id):
        observed_calls.append((database, expected_source_id))
        return ReviewedSubsetTerminalDispositionResult(disposed=False)

    monkeypatch.setattr("db.connection.db", sentinel)
    monkeypatch.setattr(facade, "sync_reviewed_subset_terminal_disposition_transaction", sync)
    result = await facade.dispose_reviewed_subset_census_drift_root()
    assert result.is_already_applied is True
    assert observed_calls == [(sentinel, "source-a")]


@pytest.mark.parametrize(
    "raised_error,expected_error",
    (
        (TimeoutError(), TimeoutError),
        (asyncio.CancelledError(), asyncio.CancelledError),
        (ReviewedSubsetTerminalDispositionError("evidence"), ReviewedSubsetTerminalDispositionError),
    ),
)
@pytest.mark.asyncio
async def test_facade_preserves_control_errors(
    monkeypatch,
    raised_error,
    expected_error,
):
    monkeypatch.setenv(contract.TERMINAL_DISPOSITION_ENABLED_ENV, "true")
    monkeypatch.setattr(
        "process.provider_directory_fhir_manual_catalog."
        "reviewed_manual_census_source_id",
        lambda: "source-a",
    )

    async def fail(_database, _expected_source_id):
        raise raised_error

    monkeypatch.setattr(facade, "sync_reviewed_subset_terminal_disposition_transaction", fail)
    with pytest.raises(expected_error):
        await facade.dispose_reviewed_subset_census_drift_root(
            database=TerminalDispositionDatabase()
        )


@pytest.mark.asyncio
async def test_facade_redacts_unexpected_runtime_error(monkeypatch):
    monkeypatch.setenv(contract.TERMINAL_DISPOSITION_ENABLED_ENV, "true")
    monkeypatch.setattr(
        "process.provider_directory_fhir_manual_catalog."
        "reviewed_manual_census_source_id",
        lambda: "source-a",
    )

    async def fail(_database, _expected_source_id):
        raise OSError("private detail")

    monkeypatch.setattr(facade, "sync_reviewed_subset_terminal_disposition_transaction", fail)
    with pytest.raises(ReviewedSubsetTerminalDispositionError) as error:
        await facade.dispose_reviewed_subset_census_drift_root(
            database=TerminalDispositionDatabase()
        )
    assert error.value.code == "state"
    with pytest.raises(ReviewedSubsetTerminalDispositionError):
        facade.terminal_disposition_result_json(object())


@pytest.mark.asyncio
async def test_store_rejects_checkpoint_guard_and_isolation_failures(monkeypatch):
    database = TerminalDispositionDatabase()
    original_status = database.status

    async def fail_checkpoint(statement, **parameters):
        if "provider_directory_pagination_checkpoint" in statement:
            return 0
        return await original_status(statement, **parameters)

    monkeypatch.setattr(database, "status", fail_checkpoint)
    with pytest.raises(ReviewedSubsetTerminalDispositionError):
        await store.sync_reviewed_subset_terminal_disposition_transaction(
            database,
            "source-a",
        )

    database = TerminalDispositionDatabase()
    original_scalar = database.scalar

    async def wrong_isolation(statement, **parameters):
        if "transaction_isolation" in statement:
            return "repeatable read"
        return await original_scalar(statement, **parameters)

    monkeypatch.setattr(database, "scalar", wrong_isolation)
    with pytest.raises(ReviewedSubsetTerminalDispositionError):
        await store.sync_reviewed_subset_terminal_disposition_transaction(
            database,
            "source-a",
        )


@pytest.mark.asyncio
async def test_store_rejects_invalid_transition_and_replay_guards():
    database = TerminalDispositionDatabase()
    database.valid = False
    with pytest.raises(ReviewedSubsetTerminalDispositionError):
        await store.sync_reviewed_subset_terminal_disposition_transaction(
            database,
            "source-a",
        )

    database = TerminalDispositionDatabase()
    await store.sync_reviewed_subset_terminal_disposition_transaction(
        database,
        "source-a",
    )
    database.valid = False
    with pytest.raises(ReviewedSubsetTerminalDispositionError):
        await store.sync_reviewed_subset_terminal_disposition_transaction(
            database,
            "source-a",
        )


@pytest.mark.asyncio
async def test_contract_dataclasses_reject_invalid_closed_state():
    with pytest.raises(ReviewedSubsetTerminalDispositionError):
        ReviewedSubsetTerminalDispositionResult(disposed=1)

    selection, _rows = await selected_reviewed_subset_terminal_disposition(
        TerminalDispositionDatabase(),
        "source-a",
    )
    with pytest.raises(ReviewedSubsetTerminalDispositionError):
        replace(selection, source_scope_sha256="invalid")

    marker = deepcopy(selection.marker_by_field)
    marker["source_scope_sha256"] = "3" * 64
    with pytest.raises(ReviewedSubsetTerminalDispositionError):
        replace(selection, marker_by_field=marker)


def _mutate_top_shape(marker_by_field):
    marker_by_field["reason_code"] = "invalid"


def _mutate_total(marker_by_field):
    marker_by_field["resource_count"] += 1


def _mutate_proof_shards(marker_by_field):
    marker_by_field["proof_shard_count"] = -1


def _mutate_resource_hash(marker_by_field):
    marker_by_field["resource_dispositions"]["Organization"][
        "diagnostic_sha256"
    ] = "invalid"


def _mutate_page_binding(marker_by_field):
    marker_by_field["resource_dispositions"]["Organization"][
        "diagnostic_pages"
    ] += 1


def _mutate_checkpoint_state(marker_by_field):
    marker_by_field["resource_dispositions"]["Organization"][
        "checkpoint_state"
    ] = "active"


def _mutate_resource_set(marker_by_field):
    marker_by_field["resource_dispositions"].pop(EXPECTED_RESOURCE_TYPES[0])


_MARKER_MUTATION_BY_NAME = {
    "top_shape": _mutate_top_shape,
    "total": _mutate_total,
    "proof_shards": _mutate_proof_shards,
    "resource_hash": _mutate_resource_hash,
    "page_binding": _mutate_page_binding,
    "checkpoint_state": _mutate_checkpoint_state,
    "resource_set": _mutate_resource_set,
}


def _change_marker(marker_by_field, mutation):
    changed_marker_by_field = deepcopy(marker_by_field)
    _MARKER_MUTATION_BY_NAME[mutation](changed_marker_by_field)
    return changed_marker_by_field


@pytest.mark.parametrize(
    "mutation",
    (
        "top_shape",
        "total",
        "proof_shards",
        "resource_hash",
        "page_binding",
        "checkpoint_state",
        "resource_set",
    ),
)
@pytest.mark.asyncio
async def test_marker_rejects_closed_shape_and_total_boundaries(mutation):
    selection, _rows = await selected_reviewed_subset_terminal_disposition(
        TerminalDispositionDatabase(),
        "source-a",
    )
    marker = _change_marker(selection.marker_by_field, mutation)
    with pytest.raises(ReviewedSubsetTerminalDispositionError):
        contract.validated_terminal_disposition_marker(marker)


@pytest.mark.asyncio
async def test_marker_builder_rejects_mapping_and_zero_progress():
    selection, _rows = await selected_reviewed_subset_terminal_disposition(
        TerminalDispositionDatabase(),
        "source-a",
    )
    marker = selection.marker_by_field
    resources_by_type = deepcopy(marker["resource_dispositions"])
    resources_by_type["Organization"], resources_by_type["InsurancePlan"] = (
        resources_by_type["InsurancePlan"],
        resources_by_type["Organization"],
    )
    arguments_by_name = {
        "source_scope_sha256": marker["source_scope_sha256"],
        "proof_shard_count": marker["proof_shard_count"],
        "source_diagnostics": {},
        "source_import": {},
        "candidate_metadata": {},
    }
    with pytest.raises(ReviewedSubsetTerminalDispositionError):
        contract.terminal_disposition_marker(
            resource_dispositions=resources_by_type,
            **arguments_by_name,
        )

    resources_by_type = deepcopy(marker["resource_dispositions"])
    resources_by_type["Organization"]["retained_rows"] = 0
    with pytest.raises(ReviewedSubsetTerminalDispositionError):
        contract.terminal_disposition_marker(
            resource_dispositions=resources_by_type,
            **arguments_by_name,
        )


def test_canonical_evidence_hash_rejects_non_json_value():
    with pytest.raises(ReviewedSubsetTerminalDispositionError):
        contract.canonical_evidence_sha256({object()})
