# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exact numeric operands across signed transport and shared read normalization."""

from dataclasses import replace
from decimal import Decimal

import pytest
from sqlalchemy import BigInteger, Numeric, column, select

from api import custom_import_provider_http as provider_http
from api import custom_import_read_http as transport
from api.custom_import_provider_sql import compile_npi_entity_relation
from process.custom_import import grouped_child_read, grouped_read, read_core
from process.custom_import.definition import CustomImportDefinition, Field
from tests import custom_import_grouped_child_support as child_fixture
from tests import custom_import_grouped_support as fixture
from tests import test_custom_import_provider_http as provider_fixture
from tests import test_custom_import_read_core as core_fixture
from tests import test_custom_import_read_http as http_fixture


def _context(value_type="decimal", *, grouped=False):
    document = fixture.definition_document()
    next(field for field in document["schema"]["root"]["fields"] if field["id"] == "score")["type"] = value_type
    if not grouped:
        document["query"].pop("entity_selection")
    return replace(fixture.context(), definition=CustomImportDefinition.from_mapping(document))


def _body(value, operator="gt", **changes):
    return provider_fixture._body(
        context=[],
        filters=[{"field_id": "metric_alias", "operator": operator, "value": value}],
        order=None,
        require_match=True,
        **changes,
    )


@pytest.mark.parametrize(
    ("value_type", "lexeme", "expected", "canonical"),
    [
        ("integer", "2.5", Decimal("2.5"), "2.5"),
        ("integer", "-2.5", Decimal("-2.5"), "-2.5"),
        ("integer", "2.0", 2, 2),
        ("integer", "-0.00", 0, 0),
        ("integer", "9E+2", 900, 900),
        ("integer", "9223372036854775807.0", 2**63 - 1, 2**63 - 1),
        ("integer", "-9223372036854775808.0", -(2**63), -(2**63)),
        ("integer", "9223372036854775807.5", Decimal("9223372036854775807.5"), "9223372036854775807.5"),
        ("decimal", "0.85", Decimal("0.85"), "0.85"),
        ("decimal", "0.8500", Decimal("0.8500"), "0.8500"),
        ("decimal", "1e-12", Decimal("1e-12"), "0.000000000001"),
        ("decimal", "-0.00", Decimal(0), "0"),
    ],
)
@pytest.mark.parametrize("operator", ["eq", "gt", "lt"])
def test_gateway_numeric_values_reach_every_shared_query_plan(value_type, lexeme, expected, canonical, operator):
    parsed = provider_http._parse_provider_request(_body({"decimal": lexeme}, operator))
    context = _context(value_type)
    search = read_core._normalize_search_plan(read_core.SearchRequest(context.target, parsed.filters), context)
    _selectors, metrics, _order = read_core._normalized_npi_query(context, (), parsed.filters, None)
    grouped = grouped_read.normalize_plan(
        _context(value_type, grouped=True),
        fixture.query(context_filters=(read_core.ReadFilter("segment", "eq", "segment_a"),), filters=parsed.filters),
        read_core.ExtensionReadScope("synthetic"),
    )
    for predicate in (search.filters[0], metrics[0], grouped.filters[0]):
        assert predicate.field.value_type == value_type
        assert predicate.value == expected and type(predicate.value) is type(expected)
        assert predicate.canonical_value == canonical


@pytest.mark.parametrize("value_type", ["integer", "decimal"])
@pytest.mark.parametrize(
    "value",
    [
        {},
        {"decimal": "0.85", "extra": 1},
        {"decimal": 0.85},
        {"decimal": True},
        {"decimal": None},
        {"decimal": "2"},
        {"decimal": " 0.85"},
        {"decimal": "0.85\n"},
        {"decimal": "+0.85"},
        {"decimal": "00.85"},
        {"decimal": ".85"},
        {"decimal": "0."},
        {"decimal": "0.٨٥"},
        {"decimal": "NaN"},
        {"decimal": "Infinity"},
        {"decimal": "-Infinity"},
        {"decimal": "1e999999999999999999999999999999"},
        {"decimal": "0e" + "0" * 2047},
        {"decimal": "1e-13"},
        {"decimal": "1e19"},
        True,
        0.85,
        float("nan"),
        float("inf"),
    ],
)
def test_numeric_filters_reject_invalid_envelopes_lossy_values_and_excess_precision(value_type, value):
    field = Field("synthetic", 1, value_type, True, 1, None)
    with pytest.raises(read_core.CustomImportReadRequestError):
        read_core._normalized_filter_value(field, "eq", value)


@pytest.mark.parametrize("value_type", ["string", "boolean", "date", "timestamp"])
def test_numeric_envelopes_do_not_coerce_non_numeric_fields(value_type):
    with pytest.raises(read_core.CustomImportReadRequestError):
        read_core._normalized_filter_value(Field("synthetic", 1, value_type, True, 1, None), "eq", {"decimal": "2.5"})


@pytest.mark.parametrize(("value_type", "lexeme", "existing"), [("decimal", "0.85", "0.85"), ("integer", "2.0", 2)])
def test_envelope_preserves_query_cursor_identity_but_does_not_bypass_body_signature(value_type, lexeme, existing):
    body = _body({"decimal": lexeme})
    parsed = provider_http._parse_provider_request(body)
    context = _context(value_type)
    request = read_core.SearchRequest(context.target, parsed.filters)
    plan = read_core._normalize_search_plan(request, context)
    equivalent = replace(request, filters=(read_core.ReadFilter("score", "gt", existing),))
    equivalent_plan = read_core._normalize_search_plan(equivalent, context)
    assert plan == equivalent_plan
    with pytest.raises(read_core.CustomImportReadRequestError, match="repeat"):
        read_core._normalize_search_plan(replace(request, filters=request.filters + equivalent.filters), context)
    state = replace(core_fixture._cursor_state(), target=context.target, query_fingerprint=plan.fingerprint)
    cursor = core_fixture._codec().issue(state)
    assert (
        core_fixture._open_cursor(cursor, pinned_target=context.target, query_fingerprint=equivalent_plan.fingerprint)
        == state
    )
    verification_map = {
        "headers": http_fixture._provider_headers(body=body, path=provider_http.CUSTOM_IMPORT_PROVIDERS_PATH),
        "request": parsed,
        "trusted_now": http_fixture._NOW,
        "keyring": transport._load_keyring(http_fixture._keyring_document()),
        "path": provider_http.CUSTOM_IMPORT_PROVIDERS_PATH,
    }
    transport._verify_provider_transport(body=body, **verification_map)
    assert parsed.filters[0].value == {"decimal": lexeme}
    with pytest.raises(transport.CustomImportReadTransportError):
        transport._verify_provider_transport(body=_body(existing), **verification_map)


def test_fractional_integer_bind_is_numeric_without_casting_the_stored_integer():
    value, _canonical = read_core._normalized_filter_value(
        Field("score", 1, "integer", True, 1, None), "gt", {"decimal": "2.5"}
    )
    score = column("score", BigInteger())
    relation = compile_npi_entity_relation(select(score).where(read_core._has_scalar_comparison(score, "gt", value)))
    assert relation.sql == "SELECT score \nWHERE score > :__custom_import_0"
    assert relation.typed_binds[0].value == Decimal("2.5")
    assert isinstance(relation.typed_binds[0].type, Numeric)


def test_fractional_integer_context_and_persisted_grouped_selection_stay_strict():
    context = fixture.context()
    year = read_core.ReadFilter("year_alias", "eq", {"decimal": "2024.5"})
    normalized = read_core._normalized_filters((year,), context)
    with pytest.raises(read_core.CustomImportReadRequestError):
        grouped_read.normalize_plan(
            context, fixture.query(context_filters=(year,)), read_core.ExtensionReadScope("synthetic")
        )
    with pytest.raises(read_core.CustomImportReadRequestError):
        read_core._normalized_npi_query(context, (year,), (), None)
    with pytest.raises(read_core.CustomImportReadRequestError):
        read_core._require_context_only_filters(normalized, context)
    with pytest.raises(read_core.CustomImportReadRequestError):
        read_core._require_order_context_filters((), normalized, context, require_exact_context=True)
    for value in (Decimal("2024"), Decimal("2024.5"), True):
        with pytest.raises(read_core.CustomImportReadRequestError):
            read_core._normalized_integer(value, "period")


def test_fractional_integer_complete_child_key_stays_strict():
    document = child_fixture.definition_document()
    document["schema"]["children"][0]["fields"][1]["type"] = "integer"
    context = replace(child_fixture.context(), definition=CustomImportDefinition.from_mapping(document))
    predicate = read_core.ReadFilter("service_code", "eq", {"decimal": "2.5"})
    selectors = read_core._normalized_filters((predicate,), context, query_child_collection="rates")
    key = grouped_child_read.declared_child_key(context)
    with pytest.raises(read_core.CustomImportReadRequestError, match="exact complete-key"):
        grouped_child_read.verify_child_selectors(selectors, key)
