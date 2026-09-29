# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Raw observations reuse production transforms and never assert capacity sufficiency."""

import asyncio
import json
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_cms_native_projection as projection
from process.provider_directory_cms_overlay_projection import DesiredOverlayProjection


def _bounds(**changes):
    return {
        "temp_file_limit_bytes_per_backend": 32 * 1024,
        "max_parallel_workers_per_gather": 0,
        "max_parallel_maintenance_workers": 0,
        **changes,
    }


def test_factory_limits_override_native_environment_settings(monkeypatch):
    monkeypatch.setattr(
        projection._native(),
        "_entity_address_sql_settings",
        lambda: [("work_mem", "4MB"), ("temp_file_limit", "-1"), ("max_parallel_workers_per_gather", "3")],
    )
    assert dict(projection._sql_settings(_bounds())) == {
        "work_mem": "4MB",
        "temp_file_limit": "32kB",
        "max_parallel_workers_per_gather": "0",
        "max_parallel_maintenance_workers": "0",
    }


@pytest.mark.parametrize(
    "changes",
    [{"temp_file_limit_bytes_per_backend": value} for value in (None, True, -1, 0, 1025, 32768.0)]
    + [
        {name: value}
        for name in ("max_parallel_workers_per_gather", "max_parallel_maintenance_workers")
        for value in (None, False, -1, 1)
    ],
)
def test_observer_rejects_unbounded_or_changed_factory_limits(changes):
    with pytest.raises(RuntimeError, match="execution_bounds_invalid"):
        projection._sql_settings(_bounds(**changes))


def test_normalized_select_is_exact_insert_input():
    native = projection._native()
    source = native._source_selects("fixture", {"npi_address": True})[0]
    insert = native._insert_raw_from_source_sql("fixture", "raw_stage", source)
    header = native._insert_raw_header_sql("fixture", "raw_stage")
    assert projection.normalized_raw_select_sql(source) == insert[len(header) :].strip().removesuffix(";")


def test_missing_overlay_never_falls_back_to_incumbent():
    with pytest.raises(RuntimeError, match="desired_overlay_(changed|unavailable)"):
        projection._require_overlay(SimpleNamespace(input_hash="a" * 64), SimpleNamespace(datasets=()), None)


def test_observation_is_explicitly_not_complete_capacity():
    address = SimpleNamespace(input_hash="a" * 64)
    overlay = SimpleNamespace(desired_fence_hash="b" * 64)
    result = projection._result(address, overlay, [{"row_count": 3}])
    assert result["row_count"] == 3
    assert result["capacity_complete"] is False
    assert {"gin_gist_and_other_index_pages", "logging_rewrites_and_wal"} <= set(result["unresolved_terms"])
    assert not any("reservation" in name for name in result)


def _query_inputs():
    """Describe only query semantics, with no staged OID or physical acceptance."""
    candidate = projection.candidate
    return candidate.ProviderDirectoryAddressSourceQueryInput(
        (candidate.ProviderDirectoryAddressDatasetPin("cms-npd", "endpoint", "dataset", "a" * 64, "root"),),
        (("doctor_clinician_address", "desired_doctors"),),
        "2026-01-02",
        DesiredOverlayProjection("SELECT 1", "a" * 64, "b" * 64),
    )


def test_query_context_resets_and_never_grants_physical_preparation():
    candidate = projection.candidate
    with pytest.raises(RuntimeError, match="cancel fixture"):
        with candidate.source_query_scope(_query_inputs()):
            assert candidate.current() is None
            assert candidate.table_name("doctor_clinician_address") == "doctor_clinician_address"
            assert (
                candidate.source_sql("fixture", "FROM fixture.doctor_clinician_address")
                == "FROM fixture.desired_doctors"
            )
            assert candidate.semantic_now_sql() == "TIMESTAMP '2026-01-02 00:00:00'"
            with pytest.raises(ValueError, match="preparation input is invalid"):
                candidate.validate_preparation_input(_query_inputs())
            raise RuntimeError("cancel fixture")
    assert not candidate.has_source_query()
    assert (
        candidate.source_sql("fixture", "FROM fixture.doctor_clinician_address")
        == "FROM fixture.doctor_clinician_address"
    )


@pytest.mark.asyncio
async def test_query_scope_cannot_enter_physical_builder():
    with projection.candidate.source_query_scope(_query_inputs()):
        with pytest.raises(RuntimeError, match="source query cannot prepare stages"):
            await projection.candidate.prepare_provider_directory_entity_address({}, {}, preparation_input=None)


@pytest.mark.parametrize(
    "changes",
    [
        {"overlay": None},
        {"semantic_as_of": None},
        {"semantic_as_of": "2026-02-30"},
        {"relation_overrides": (("unexpected", "desired"),)},
        {"relation_overrides": (("doctor_clinician_address", "unsafe;name"),)},
    ],
)
def test_query_context_rejects_unbound_or_invalid_inputs(changes):
    with pytest.raises((ValueError, RuntimeError)):
        with projection.candidate.source_query_scope(replace(_query_inputs(), **changes)):
            pytest.fail("invalid query inputs reached source generation")
    assert not projection.candidate.has_source_query()


@pytest.mark.parametrize("field", ["native_address_input_hash", "desired_fence_hash"])
def test_overlay_requires_both_exact_input_pins(field):
    fence = SimpleNamespace(datasets=())
    overlay = DesiredOverlayProjection("SELECT 1", "a" * 64, projection.desired_fence_hash(fence))
    projection._require_overlay(SimpleNamespace(input_hash="a" * 64), fence, overlay)
    with pytest.raises(RuntimeError, match="desired_overlay_changed"):
        projection._require_overlay(SimpleNamespace(input_hash="a" * 64), fence, replace(overlay, **{field: "f" * 64}))


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "changed", "cancel", "settings"])
async def test_observation_rechecks_fresh_snapshot_and_propagates_failure(monkeypatch, failure):
    """No result escapes after input drift or cancellation, and both transactions close."""
    transactions = []

    @asynccontextmanager
    async def transaction():
        transactions.append("open")
        try:
            yield SimpleNamespace(execute=AsyncMock(), scalar=AsyncMock(return_value=failure != "settings"))
        finally:
            transactions.append("closed")

    database = SimpleNamespace(transaction=transaction, _transaction_binding=lambda: None)
    address = SimpleNamespace(
        input_hash="a" * 64,
        input_json=json.dumps(_bounds()),
        fhir=SimpleNamespace(db=database, _schema=lambda: "fixture"),
    )
    fence = SimpleNamespace(datasets=())
    overlay = DesiredOverlayProjection("SELECT 1", address.input_hash, projection.desired_fence_hash(fence))
    monkeypatch.setattr(projection._native(), "db", database)
    tuned_settings = []

    @asynccontextmanager
    async def tuned_transaction(database, settings, quote_literal, logger):
        tuned_settings.append(dict(settings))
        yield

    monkeypatch.setattr(projection._native(), "entity_address_tuned_transaction", tuned_transaction)
    monkeypatch.setattr(projection._native(), "_is_address_canon_available", AsyncMock(return_value=True))
    assertions = AsyncMock(side_effect=[None, RuntimeError("inputs changed")] if failure == "changed" else None)
    monkeypatch.setattr(projection, "_assert_snapshot", assertions)
    monkeypatch.setattr(projection, "_availability", AsyncMock(return_value={}))
    monkeypatch.setattr(projection, "_source_queries", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        projection,
        "_observe_sources",
        AsyncMock(side_effect=asyncio.CancelledError() if failure == "cancel" else None, return_value=[]),
    )
    if failure:
        with pytest.raises(asyncio.CancelledError if failure == "cancel" else RuntimeError):
            await projection.observe_native_raw_projection(address, fence, overlay)
    else:
        assert (await projection.observe_native_raw_projection(address, fence, overlay))["capacity_complete"] is False
    count = 1 if failure in {"cancel", "settings"} else 2
    assert transactions == ["open", "closed"] * count
    assert assertions.await_count == (0 if failure == "settings" else count)
    assert all(settings["temp_file_limit"] == "32kB" for settings in tuned_settings)
    assert all(settings["max_parallel_workers_per_gather"] == "0" for settings in tuned_settings)
    assert all(settings["max_parallel_maintenance_workers"] == "0" for settings in tuned_settings)
