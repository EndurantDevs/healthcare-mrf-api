# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Snapshot root reservations include both immutable dictionary copies."""

from dataclasses import replace
from unittest.mock import Mock

import pytest

from process.custom_import import build_graph as graph
from process.custom_import import build_graph_sets as sets
from process.custom_import.runner_types import CandidateRunnerError
from process.custom_import.storage_layout import snapshot_tables
from tests.test_custom_import_build_graph import _registry, _request
from tests.test_custom_import_build_graph_sets import family_input


@pytest.mark.parametrize("retained", [False, True])
def test_root_copy_models_charge_canonical_identity_bytes_and_two_rows(monkeypatch, retained):
    request = _request()
    root_input, _ = family_input(request, 8, retained=retained, count=0)
    root_record, entity = graph._root_identity_models(request, root_input)
    assert root_record is root_input.record
    expected = (
        len(root_record.canonical_logical_key.encode("utf-8"))
        + 64
        + 3
        + len(entity.canonical_value.encode("utf-8"))
        + 32
    )
    assert graph._model_bytes((root_record, entity)) == expected

    check = Mock(wraps=graph._page_cost)
    monkeypatch.setattr(graph, "_page_cost", check)
    codec = graph._retained_root_arguments if retained else graph._source_root_arguments
    registry = _registry(request.definition)
    codec(request, registry, root_input)
    models = check.call_args.args[1]
    projection_rows = check.call_args.kwargs["projection_values"]
    # SOURCE also reserves a possible canonical interner INSERT. Retained roots
    # already own their interned entity; neither path reads it again for costing.
    assert sum(isinstance(model, graph.CustomImportEntityBinding) for model in models) == (1 if retained else 2)
    assert root_record in models
    expected_projections = sum(field.projection_slot is not None for field in request.definition.root_fields)
    expected_projections += sum(
        scope.collection_slot == 0
        for scope in graph.material._profile_scopes(request.definition, registry.child_collection_slots)
    )
    assert len(projection_rows) == expected_projections
    assert (
        len(models) + len(projection_rows) + check.call_args.kwargs.get("reserved_rows", 0) == 6 + expected_projections
    )


@pytest.mark.parametrize("retained", [False, True])
def test_root_copy_exact_row_and_model_byte_edges(retained, monkeypatch):
    request = _request()
    root_input, _ = family_input(request, 8, retained=retained, count=0)
    registry = _registry(request.definition)
    codec = graph._retained_root_arguments if retained else graph._source_root_arguments
    check = Mock(wraps=graph._page_cost)
    monkeypatch.setattr(graph, "_page_cost", check)
    codec(request, registry, root_input)
    models = check.call_args.args[1]
    projection_rows = check.call_args.kwargs["projection_values"]
    row_cost = len(models) + len(projection_rows) + check.call_args.kwargs.get("reserved_rows", 0)
    byte_cost = graph._model_bytes(models) + sum(
        len(column_value.encode("utf-8")) if isinstance(column_value, str) else len(column_value)
        for projection_by_column in projection_rows
        for column_value in projection_by_column.values()
        if isinstance(column_value, (str, bytes, bytearray, memoryview))
    )
    exact = replace(request, page_row_limit=row_cost, page_byte_limit=byte_cost)
    codec(exact, registry, root_input)
    with pytest.raises(CandidateRunnerError, match="row page"):
        codec(replace(exact, page_row_limit=row_cost - 1), registry, root_input)
    with pytest.raises(CandidateRunnerError, match="byte page"):
        codec(replace(exact, page_byte_limit=byte_cost - 1), registry, root_input)


def test_root_set_page_reserves_maximum_interner_and_identity_fanout():
    request = _request(page_row_limit=128)
    registry = _registry(request.definition)
    scopes = graph.material._profile_scopes(request.definition, registry.child_collection_slots)
    fanout = 6 + sum(field.projection_slot is not None for field in request.definition.root_fields)
    fanout += sum(scope.collection_slot == 0 for scope in scopes)
    assert sets._root_limit(request, registry) == request.page_row_limit // fanout
    assert sets._root_limit(request, registry) * fanout <= request.page_row_limit


def test_snapshot_identity_copy_conflict_constraints_keep_fixed_native_names():
    tables = snapshot_tables(1)
    for name in ("custom_import_root_record", "custom_import_entity_binding"):
        assert tables[name].primary_key.name == name + "_pkey"
