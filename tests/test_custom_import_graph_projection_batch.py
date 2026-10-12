# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Whole native child projections retain the independent ordered Python oracle."""

from __future__ import annotations

import datetime as dt
import decimal
from dataclasses import replace
from decimal import Decimal
from unittest.mock import Mock

import pytest

from process.custom_import import build_graph as graph
from process.custom_import import build_graph_sets as sets
from process.custom_import import graph_payloads
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.runner_codec import fields_by_collection
from process.custom_import.runner_types import CandidateRunnerError
from tests.test_custom_import_build_graph import _registry, _request
from tests.test_custom_import_build_graph_sets import family_input
from tests.test_custom_import_definition import _rate, _raw_definition
from tests.test_custom_import_graph_payloads import _definition, _group


def records_for(group):
    return tuple(
        graph_payloads.ProjectionRecord(child, group.state.source.record, group.state.family, group.state.source.values)
        for child in group.children
    )


def consume_projection(request, registry, group, prepared):
    """Model only the proposed caller seam; production callers stay unchanged."""
    scalars, contexts, scalar_bytes = [], [], 0
    fields = fields_by_collection(request.definition)["rates"]
    for index, child in enumerate(group.children):
        span = None if prepared is None else prepared.rows[index]
        if span is None:
            child_scalars, child_contexts, byte_count = sets._serial_child_projection(
                request, registry, group, "rates", child, fields
            )
        else:
            start, count, context_start, context_count, byte_count, _context_bytes, _argument_bytes = span
            child_scalars = list(zip(*(column[start : start + count] for column in prepared.scalars), strict=True))
            child_contexts = [
                dict(
                    build_id=7,
                    profile_slot=prepared.contexts[1][position],
                    entity_binding_id=group.state.family.entity_binding_id,
                    family_revision_id=group.state.family.family_revision_id,
                    context_collection_slot=child.collection_slot,
                    context_child_revision_id=prepared.contexts[0][position],
                    canonical_context_key=prepared.contexts[2][position],
                    context_key_sha256=prepared.contexts[3][position],
                )
                for position in range(context_start, context_start + context_count)
            ]
        scalars.extend(child_scalars)
        contexts.extend(child_contexts)
        scalar_bytes += byte_count
    return scalars, contexts, scalar_bytes


def oracle_encoded(request, registry, group):
    spans, scalars, contexts = [], [[] for _ in range(10)], [[] for _ in range(4)]
    fields = fields_by_collection(request.definition)["rates"]
    for child in group.children:
        child_scalars, child_contexts, scalar_bytes = sets._serial_child_projection(
            request, registry, group, "rates", child, fields
        )
        context_bytes = sum(len(context["canonical_context_key"].encode()) + 32 for context in child_contexts)
        columns = tuple(tuple(scalar[index] for scalar in child_scalars) for index in range(10))
        context_columns = tuple(
            tuple(context[name] for context in child_contexts)
            for name in ("context_child_revision_id", "profile_slot", "canonical_context_key", "context_key_sha256")
        )
        arguments = tuple(("unused", column) for column in (*columns, *context_columns[:3]))
        spans.append(
            (
                len(scalars[0]),
                len(child_scalars),
                len(contexts[0]),
                len(child_contexts),
                scalar_bytes,
                context_bytes,
                sets._child_argument_bytes(arguments),
            )
        )
        for index, column in enumerate(columns):
            scalars[index].extend(
                graph_payloads._projection_decimal(scalar) if index == 6 and scalar is not None else scalar
                for scalar in column
            )
        for output_column, column in zip(contexts, context_columns, strict=True):
            output_column.extend(column)
    return tuple(spans), tuple(map(tuple, scalars)), tuple(map(tuple, contexts))


@pytest.fixture(autouse=True)
def serial_decoder(monkeypatch):
    monkeypatch.setattr(graph_payloads, "native_decoder", lambda: None)


@pytest.fixture
def compiled_projector():
    projector = graph_payloads.native_projector()
    if projector is None:
        pytest.skip("compiled graph projection capability is not installed")
    return projector


@pytest.mark.parametrize("state", ["missing", "null", "value"])
def test_portable_column_adapter_preserves_complete_oracle_and_costs(monkeypatch, state):
    request = _request(definition=_definition())
    values = _rate("0000000001", 'Ω😀"\\\n', Decimal("1.000000000001"))
    if state != "missing":
        values.update(
            {"note": "é\t", "visits": -(1 << 63), "enabled": False}
            if state == "value"
            else dict(note=None, visits=None, enabled=None)
        )
    group, registry = _group(request, [values]), _registry(request.definition)
    encoded = oracle_encoded(request, registry, group)
    projector = Mock(return_value=encoded)
    monkeypatch.setattr(graph_payloads, "native_projector", lambda: projector)
    prepared = graph_payloads.project_batch(request.definition, registry.child_collection_slots, records_for(group))
    assert prepared is not None
    assert consume_projection(request, registry, group, prepared) == sets._source_projections(request, registry, group)
    assert prepared.rows == encoded[0]
    assert prepared.input_bytes <= 131_072 and prepared.workspace_bytes <= 4_194_304
    projector.assert_called_once()
    assert len(projector.call_args.args[1]) == 1


@pytest.mark.parametrize(
    "corruption",
    [
        "prefix",
        "span",
        "span_shape",
        "span_total",
        "columns",
        "column_count",
        "digest",
        "decimal",
        "decimal_empty",
        "decimal_invalid",
        "required_null",
    ],
)
def test_malformed_projection_output_fails_closed(monkeypatch, corruption):
    request = _request()
    group, registry = _group(request, [_rate("0000000001")]), _registry(request.definition)
    rows, scalars, contexts = oracle_encoded(request, registry, group)
    if corruption == "prefix":
        rows = ()
    if corruption == "span":
        rows = ((1, *rows[0][1:]),)
    if corruption == "span_shape":
        rows = (rows[0][:-1],)
    if corruption == "span_total":
        rows = ((rows[0][0], rows[0][1] - 1, *rows[0][2:]),)
    if corruption == "columns":
        scalars = ((), *scalars[1:])
    if corruption == "column_count":
        scalars = scalars[:-1]
    if corruption == "digest":
        contexts = (*contexts[:3], (b"short",))
    if corruption in ("decimal", "decimal_empty", "decimal_invalid"):
        text = {"decimal": "NaN", "decimal_empty": "", "decimal_invalid": "invalid"}[corruption]
        scalars = (*scalars[:6], tuple(text if value is not None else None for value in scalars[6]), *scalars[7:])
    if corruption == "required_null":
        scalars = (tuple(None for _ in scalars[0]), *scalars[1:])
    monkeypatch.setattr(graph_payloads, "native_projector", lambda: Mock(return_value=(rows, scalars, contexts)))
    with pytest.raises(CandidateRunnerError):
        graph_payloads.project_batch(request.definition, registry.child_collection_slots, records_for(group))


@pytest.mark.parametrize("capability", [None, object(), lambda *_args: None])
def test_projector_capability_requires_callable_versioned_entry(monkeypatch, capability):
    graph_payloads.native_projector.cache_clear()
    monkeypatch.setattr(
        graph_payloads.importlib,
        "import_module",
        Mock(return_value=Mock(custom_import_graph_projection_columns_v1=capability)),
    )
    try:
        assert graph_payloads.native_projector() is (capability if callable(capability) else None)
    finally:
        graph_payloads.native_projector.cache_clear()


@pytest.mark.parametrize(
    "kind,values,expected",
    [
        ("string", [], None),
        ("string", {}, (("missing", None),)),
        ("string", {"display_name": None}, (("null", None),)),
        ("string", {"display_name": "Ω" * 2_048}, (("value", "Ω" * 2_048),)),
        ("string", {"display_name": "Ω" * 2_049}, None),
        ("integer", {"display_name": True}, None),
        ("integer", {"display_name": -(1 << 63)}, (("value", -(1 << 63)),)),
        ("integer", {"display_name": 1 << 63}, None),
        ("decimal", {"display_name": Decimal("12.5000")}, (("value", "12.5"),)),
        ("decimal", {"display_name": Decimal("1E-13")}, None),
        ("decimal", {"display_name": Decimal("1E18")}, None),
        ("boolean", {"display_name": False}, (("value", False),)),
    ],
)
def test_projection_root_cells_keep_states_and_bounded_native_types(kind, values, expected):
    field = replace(_request().definition.fields_by_id["display_name"], value_type=kind, nullable=True)
    assert graph_payloads._projection_root_cells((field,), values) == expected


@pytest.mark.parametrize("unsupported", ["collection_slot", "root_type"])
def test_unsupported_projection_metadata_precedes_native_dispatch(monkeypatch, unsupported):
    raw = _raw_definition()
    if unsupported == "collection_slot":
        raw["selection_profiles"][0]["selection"] = [{"field": "display_name", "direction": "asc", "nulls": "last"}]
        raw["selection_profiles"][0]["context_dimensions"] = ["npi"]
    else:
        raw["schema"]["root"]["fields"].append(
            {"id": "root_date", "slot": 9, "type": "date", "nullable": True, "projection_slot": 5}
        )
        raw["query"]["root_fields"].append("root_date")
    request = _request(definition=CustomImportDefinition.from_mapping(raw))
    group, registry = _group(request, [_rate("0000000001")]), _registry(request.definition)
    slots = {"rates": True} if unsupported == "collection_slot" else registry.child_collection_slots
    projector = Mock(side_effect=AssertionError("unsupported metadata entered native"))
    monkeypatch.setattr(graph_payloads, "native_projector", lambda: projector)
    assert graph_payloads.project_batch(request.definition, slots, records_for(group)) is None
    projector.assert_not_called()


@pytest.mark.parametrize(
    "owner,field,value",
    [
        ("root", "canonical_logical_key", b"invalid"),
        pytest.param(
            "root", "canonical_logical_key", "x" * (graph_payloads.MAX_INPUT_BYTES + 1), id="oversize-root-key"
        ),
        ("root", "logical_key_sha256", b"short"),
        ("family", "entity_binding_id", True),
        ("family", "family_revision_id", 1 << 63),
        ("child", "collection_slot", 99),
        ("child", "child_revision_id", True),
        ("child", "payload_sha256", b"short"),
        ("child", "canonical_payload", None),
    ],
)
def test_projection_input_identity_keeps_invalid_rows_on_serial_path(monkeypatch, owner, field, value):
    request = _request()
    group, registry = _group(request, [_rate("0000000001")]), _registry(request.definition)
    record = records_for(group)[0]
    setattr(getattr(record, owner), field, value)
    projector = Mock(return_value=((None,), ((),) * 10, ((),) * 4))
    monkeypatch.setattr(graph_payloads, "native_projector", lambda: projector)
    prepared = graph_payloads.project_batch(request.definition, registry.child_collection_slots, (record,))
    assert prepared is not None and prepared.rows == (None,)
    roots, rows = projector.call_args.args[1:]
    assert (roots[0] is None) == (owner != "child")
    assert (rows[0] is None) == (owner == "child")


@pytest.mark.parametrize("allocation_failure", [False, True])
def test_layout_errors_fall_back_without_swallowing_allocation_failure(monkeypatch, allocation_failure):
    request = _request()
    group, registry = _group(request, [_rate("0000000001")]), _registry(request.definition)
    error = ValueError("layout unavailable")
    if allocation_failure:
        error.__cause__ = MemoryError("allocation")
    projector = Mock(side_effect=AssertionError("invalid layout entered native"))
    monkeypatch.setattr(graph_payloads, "native_projector", lambda: projector)
    monkeypatch.setattr(graph_payloads, "_projection_layout", Mock(side_effect=error))
    if allocation_failure:
        with pytest.raises(ValueError) as actual:
            graph_payloads.project_batch(request.definition, registry.child_collection_slots, records_for(group))
        assert actual.value is error
        with pytest.raises(ValueError) as actual:
            sets._can_project_sources(request, registry)
        assert actual.value is error
    else:
        assert (
            graph_payloads.project_batch(request.definition, registry.child_collection_slots, records_for(group))
            is None
        )
        assert not sets._can_project_sources(request, registry)
    projector.assert_not_called()


@pytest.mark.parametrize("failure", [ValueError("invalid input"), TypeError("invalid input")])
def test_native_input_rejection_is_a_contract_error_without_serial_retry(monkeypatch, failure):
    request = _request()
    group, registry = _group(request, [_rate("0000000001")]), _registry(request.definition)
    projector = Mock(side_effect=failure)
    monkeypatch.setattr(graph_payloads, "native_projector", lambda: projector)
    with pytest.raises(CandidateRunnerError, match="rejected its bounded input") as actual:
        graph_payloads.project_batch(request.definition, registry.child_collection_slots, records_for(group))
    assert actual.value.__cause__ is failure
    projector.assert_called_once()


@pytest.mark.parametrize("failure", [MemoryError("allocation"), KeyboardInterrupt(), SystemExit(2)])
def test_native_fatal_failure_is_not_serial_retried(monkeypatch, failure):
    request = _request()
    group, registry = _group(request, [_rate("0000000001")]), _registry(request.definition)
    projector = Mock(side_effect=failure)
    monkeypatch.setattr(graph_payloads, "native_projector", lambda: projector)
    with pytest.raises(type(failure)):
        graph_payloads.project_batch(request.definition, registry.child_collection_slots, records_for(group))
    projector.assert_called_once()


def test_absent_capability_and_batch_bounds_keep_serial_domain(monkeypatch):
    request = _request()
    group, registry = _group(request, [_rate("0000000001")]), _registry(request.definition)
    records = records_for(group)
    monkeypatch.setattr(graph_payloads, "native_projector", lambda: None)
    assert graph_payloads.project_batch(request.definition, registry.child_collection_slots, records) is None
    projector = Mock(side_effect=AssertionError("outside admitted native batch"))
    monkeypatch.setattr(graph_payloads, "native_projector", lambda: projector)
    for records in ((), records * 65):
        assert graph_payloads.project_batch(request.definition, registry.child_collection_slots, records) is None
    group.children[0].canonical_payload = "x" * 131_072
    assert graph_payloads.project_batch(request.definition, registry.child_collection_slots, records_for(group)) is None
    projector.assert_not_called()


def test_retained_columns_share_one_aggregate_native_window(monkeypatch):
    request = _request()
    group, registry = _group(request, [_rate("0000000001")]), _registry(request.definition)
    projection_records = records_for(group)
    projector = Mock(return_value=oracle_encoded(request, registry, group))
    monkeypatch.setattr(graph_payloads, "native_projector", lambda: projector)
    initial = graph_payloads.project_batch(request.definition, registry.child_collection_slots, projection_records)
    assert initial is not None
    remaining = 4_194_304 - initial.workspace_bytes
    projector.reset_mock()
    for retained_bytes in (remaining + 1, 4_194_305, -1, True, 1.0):
        assert (
            graph_payloads.project_batch(
                request.definition, registry.child_collection_slots, projection_records, retained_bytes=retained_bytes
            )
            is None
        )
    projector.assert_not_called()
    assert (
        graph_payloads.project_batch(
            request.definition, registry.child_collection_slots, projection_records, retained_bytes=remaining
        )
        == initial
    )
    projector.assert_called_once()
    projector.reset_mock()
    group.state.source.values["display_name"] = object()
    assert (
        graph_payloads.project_batch(
            request.definition, registry.child_collection_slots, projection_records, retained_bytes=4_194_304
        )
        is None
    )
    projector.assert_not_called()


def _wide_context_group():
    raw = _raw_definition()
    raw["selection_profiles"] = [
        {
            "id": f"profile_{slot}",
            "selection": [{"field": "amount", "direction": "asc", "nulls": "last"}],
            "context_dimensions": ["display_name"],
        }
        for slot in range(4)
    ]
    request = _request(definition=CustomImportDefinition.from_mapping(raw))
    group = _group(request, [_rate("0000000001", f"{index:02}") for index in range(64)])
    group.state.source.values["display_name"] = "\x01" * 1_000 + "😀"
    return request, _registry(request.definition), group


def test_escaped_non_bmp_contexts_reserve_boxed_unicode_before_dispatch(monkeypatch):
    request, registry, group = _wide_context_group()
    expected = sets._source_projections(request, registry, group)
    assert len(expected[1]) == 256
    # A single non-BMP character makes every code point occupy four bytes.
    assert sum(len(context["canonical_context_key"]) * 4 for context in expected[1]) > 4_194_304
    projector = Mock(side_effect=AssertionError("oversize boxed context entered native"))
    monkeypatch.setattr(graph_payloads, "native_projector", lambda: projector)
    prepared = graph_payloads.project_batch(request.definition, registry.child_collection_slots, records_for(group))
    assert prepared is None
    assert consume_projection(request, registry, group, prepared) == expected
    projector.assert_not_called()


def test_compiled_escaped_non_bmp_contexts_exceed_aggregate_window(compiled_projector):
    request, registry, group = _wide_context_group()
    layout, fields, indices = graph_payloads._projection_layout(request.definition, registry.child_collection_slots)
    roots, rows, record_bytes = graph_payloads._projection_inputs(records_for(group), fields, indices)
    input_bytes, workspace = graph_payloads._projection_bounds(layout, roots, rows, record_bytes)
    assert input_bytes < 131_072 < 4_194_304 < workspace
    with pytest.raises(ValueError, match="batch bound"):
        compiled_projector(layout, roots, rows)
    group = replace(group, children=group.children[:1])
    prepared = graph_payloads.project_batch(request.definition, registry.child_collection_slots, records_for(group))
    assert prepared is not None and prepared.rows[0] is not None
    assert consume_projection(request, registry, group, prepared) == sets._source_projections(request, registry, group)


def test_oversize_decimal_coefficient_keeps_serial_eligibility():
    assert graph_payloads._projection_decimal(Decimal("1." + "0" * 10_000)) is None


@pytest.mark.parametrize("state", ["missing", "null", "value"])
def test_compiled_batch_matches_scalar_context_hash_and_byte_oracle(compiled_projector, state):
    request = _request(definition=_definition())
    values = [_rate("0000000001", f'Ω😀"\\\n{index}', Decimal("-0.000000000001")) for index in range(32)]
    if state != "missing":
        for row in values:
            row.update(
                {"note": "é\t", "visits": (1 << 63) - 1, "enabled": True}
                if state == "value"
                else dict(note=None, visits=None, enabled=None)
            )
    group, registry = _group(request, values), _registry(request.definition)
    prepared = graph_payloads.project_batch(request.definition, registry.child_collection_slots, records_for(group))
    assert prepared is not None and all(span is not None for span in prepared.rows)
    assert consume_projection(request, registry, group, prepared) == sets._source_projections(request, registry, group)
    assert prepared.rows == oracle_encoded(request, registry, group)[0]


def test_compiled_profiles_and_definitions_have_no_cross_batch_state(compiled_projector):
    for profile_id, slot in (("first", 1), ("second", 7), ("first", 1)):
        raw = _raw_definition()
        raw["selection_profiles"] = [
            {
                "id": profile_id,
                "selection": [{"field": "amount", "direction": "desc", "nulls": "first"}],
                "context_dimensions": ["display_name", "amount"],
            },
            {
                "id": "empty",
                "selection": [{"field": "amount", "direction": "asc", "nulls": "last"}],
                "context_dimensions": [],
            },
            {
                "id": "root_only",
                "selection": [{"field": "display_name", "direction": "asc", "nulls": "last"}],
                "context_dimensions": ["npi"],
            },
        ]
        request = _request(definition=CustomImportDefinition.from_mapping(raw))
        group = _group(request, [_rate("0000000001", "A", Decimal("-0.00")), _rate("0000000001", "B", None)])
        registry = replace(_registry(request.definition), child_collection_slots={"rates": slot})
        for child in group.children:
            child.collection_slot = slot
        group.state.source.values["display_name"] = 'quoted "\nΩ😀'
        prepared = graph_payloads.project_batch(request.definition, registry.child_collection_slots, records_for(group))
        assert prepared is not None and all(span is not None for span in prepared.rows)
        assert consume_projection(request, registry, group, prepared) == sets._source_projections(
            request, registry, group
        )
        assert prepared.rows == oracle_encoded(request, registry, group)[0]


def test_compiled_one_packet_keeps_distinct_family_roots(compiled_projector):
    raw = _raw_definition()
    raw["selection_profiles"][0]["context_dimensions"] = ["service_code", "npi"]
    request = _request(definition=CustomImportDefinition.from_mapping(raw))
    registry, groups = _registry(request.definition), []
    for root_id in (1, 2, 3):
        source, children = family_input(request, root_id, count=2)
        family = graph.CustomImportFamilyRevision(
            family_revision_id=900 + root_id,
            root_record_id=root_id,
            entity_binding_id=6000 + root_id,
            family_sha256=source.family_sha256,
        )
        current = graph.CustomImportBuildFamily(build_id=7, root_record_id=root_id, selection_kind="source")
        groups.append(sets._ChildGroup(sets._Started(source, current, family, False), children))
    records = tuple(record for group in groups for record in records_for(group))
    prepared = graph_payloads.project_batch(request.definition, registry.child_collection_slots, records)
    assert prepared is not None and all(span is not None for span in prepared.rows)
    for index, group in enumerate(groups):
        group_projection = replace(prepared, rows=prepared.rows[index * 2 : index * 2 + 2])
        assert consume_projection(request, registry, group, group_projection) == sets._source_projections(
            request, registry, group
        )


@pytest.mark.parametrize(
    "corruption", ["identity", "digest", "payload", "root_type", "required_selection", "required_context"]
)
def test_compiled_fallback_keeps_first_semantic_error(compiled_projector, corruption):
    raw = _raw_definition()
    raw["selection_profiles"][0]["selection"].append({"field": "display_name", "direction": "asc", "nulls": "last"})
    raw["selection_profiles"][0]["context_dimensions"] = ["service_code", "npi"]
    request = _request(definition=CustomImportDefinition.from_mapping(raw))
    group = _group(request, [_rate("0000000001", "A"), _rate("0000000001", "B")])
    registry = _registry(request.definition)
    if corruption == "identity":
        group.children[0].child_key_sha256 = b"x" * 32
        group.state.source.values["display_name"] = 3
    elif corruption == "digest":
        group.children[0].payload_sha256 = b"x" * 32
    if corruption == "payload":
        group.children[0].canonical_payload = "{}"
    if corruption == "root_type":
        group.state.source.values["display_name"] = "\0"
    if corruption == "required_selection":
        del group.state.source.values["display_name"]
    if corruption == "required_context":
        del group.state.source.values["npi"]
    group.children[1].payload_sha256 = b"y" * 32
    with pytest.raises(Exception) as oracle_error:
        sets._source_projections(request, registry, group)
    prepared = graph_payloads.project_batch(request.definition, registry.child_collection_slots, records_for(group))
    assert prepared is not None and prepared.rows[0] is None
    with pytest.raises(type(oracle_error.value)) as native_error:
        consume_projection(request, registry, group, prepared)
    assert str(native_error.value) == str(oracle_error.value)


def test_compiled_closed_abi_rejects_mutated_shapes_without_losing_admission(compiled_projector):
    request = _request()
    group, registry = _group(request, [_rate("0000000001")]), _registry(request.definition)
    layout, fields, indices = graph_payloads._projection_layout(request.definition, registry.child_collection_slots)
    roots, rows, _ = graph_payloads._projection_inputs(records_for(group), fields, indices)
    for bad_roots, bad_rows in (
        (roots, rows * 65),
        (roots, [(0, 99, *rows[0][2:])]),
        (roots, [(0, 0, True, *rows[0][3:])]),
        (roots * 65, rows),
    ):
        with pytest.raises((ValueError, TypeError)):
            compiled_projector(layout, bad_roots, bad_rows)
        assert compiled_projector(layout, roots, rows)[0][0] is not None


@pytest.mark.parametrize(
    "amount", [Decimal("12.5000"), Decimal("-0"), Decimal("0E-999999999"), Decimal("1.000000000001")]
)
def test_compiled_root_decimal_contexts_use_canonical_text_outside_admission(compiled_projector, monkeypatch, amount):
    raw = _raw_definition()
    raw["schema"]["root"]["fields"].append(
        {"id": "root_amount", "slot": 9, "type": "decimal", "nullable": True, "projection_slot": 5}
    )
    raw["query"]["root_fields"].append("root_amount")
    raw["selection_profiles"][0]["context_dimensions"] = ["service_code", "root_amount"]
    request = _request(definition=CustomImportDefinition.from_mapping(raw))
    group, registry = _group(request, [_rate("0000000001")]), _registry(request.definition)
    group.state.source.values["root_amount"] = amount
    expected = sets._source_projections(request, registry, group)
    replacement = Mock(side_effect=AssertionError("reentrant Decimal lookup"))
    monkeypatch.setattr(decimal, "Decimal", replacement)
    prepared = graph_payloads.project_batch(request.definition, registry.child_collection_slots, records_for(group))
    assert prepared is not None and prepared.rows[0] is not None
    assert consume_projection(request, registry, group, prepared) == expected
    replacement.assert_not_called()


def test_unsupported_date_keeps_complete_serial_columns(monkeypatch):
    raw = _raw_definition()
    raw["schema"]["children"][0]["fields"].append(
        {"id": "as_of", "slot": 8, "type": "date", "nullable": True, "projection_slot": 5}
    )
    request = _request(definition=CustomImportDefinition.from_mapping(raw))
    group = _group(request, [_rate("0000000001") | {"as_of": dt.date(2025, 1, 2)}])
    registry = _registry(request.definition)
    projector = Mock(side_effect=AssertionError("unsupported layout entered native"))
    monkeypatch.setattr(graph_payloads, "native_projector", lambda: projector)
    prepared = graph_payloads.project_batch(request.definition, registry.child_collection_slots, records_for(group))
    assert prepared is None
    assert consume_projection(request, registry, group, prepared) == sets._source_projections(request, registry, group)
    projector.assert_not_called()


def test_compiled_aggregate_workspace_and_input_bounds_precede_parallel_work(compiled_projector):
    request = _request()
    group, registry = _group(request, [_rate("0000000001")]), _registry(request.definition)
    layout, fields, indices = graph_payloads._projection_layout(request.definition, registry.child_collection_slots)
    roots, rows, _ = graph_payloads._projection_inputs(records_for(group), fields, indices)
    oversized_rows = [(*rows[0][:8], "x" * 131_072)]
    with pytest.raises(ValueError, match="admitted encoding|input byte bound"):
        compiled_projector(layout, roots, oversized_rows)
    root_values = [(*roots[0][:5], tuple(("value", "x" * 4_096) for _ in fields))]
    graph_layout = layout[1][0]
    profiles = tuple((slot, f"profile_{slot}", (2,), (0, 1)) for slot in range(1, 5))
    wide_layout = (layout[0], ((*graph_layout[:3], profiles),))
    with pytest.raises(ValueError, match="batch bound"):
        compiled_projector(wide_layout, root_values, rows * 64)
    assert compiled_projector(layout, roots, rows)[0][0] is not None


def test_compiled_context_byte_limit_keeps_the_serial_error(compiled_projector):
    raw = _raw_definition()
    raw["selection_profiles"][0]["context_dimensions"] = ["service_code", "display_name"]
    request = _request(definition=CustomImportDefinition.from_mapping(raw))
    group, registry = _group(request, [_rate("0000000001")]), _registry(request.definition)
    group.state.source.values["display_name"] = "\x01" * 2_048
    with pytest.raises(ValueError, match="context exceeds") as expected:
        sets._source_projections(request, registry, group)
    prepared = graph_payloads.project_batch(request.definition, registry.child_collection_slots, records_for(group))
    assert prepared is not None and prepared.rows == (None,)
    with pytest.raises(type(expected.value), match="context exceeds"):
        consume_projection(request, registry, group, prepared)
