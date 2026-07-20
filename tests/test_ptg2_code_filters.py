import pytest

from api import ptg2_code_filters as code_filters


@pytest.mark.parametrize(
    ("code", "code_system", "expected_filter", "expected_params"),
    (
        (None, None, None, {}),
        (
            "42",
            "HP_PROCEDURE_CODE",
            "procedure_code = :procedure_code",
            {"procedure_code": 42},
        ),
        ("invalid", "HP_PROCEDURE_CODE", "FALSE", {}),
        (
            "99213",
            "CPT",
            "reported_code_system = :reported_code_system",
            {"reported_code": "99213", "reported_code_system": "CPT"},
        ),
        (
            "-123456",
            None,
            "procedure_code = :procedure_code",
            {"reported_code": "-123456", "procedure_code": -123456},
        ),
        ("123", None, "reported_code = :reported_code", {"reported_code": "123"}),
    ),
)
def test_append_code_filter_variants(
    code,
    code_system,
    expected_filter,
    expected_params,
):
    filters = []
    params_by_name = {}

    code_filters._append_code_filter(
        filters,
        params_by_name,
        code=code,
        code_system=code_system,
    )

    if expected_filter is None:
        assert filters == []
    else:
        assert expected_filter in filters[0]
    assert params_by_name == expected_params


def test_reported_code_filters_cover_single_and_legacy_values():
    filters = []
    params_by_name = {}

    code_filters._append_code_filter(
        filters,
        params_by_name,
        code="0450",
        code_system="RC",
    )

    assert "reported_code IN" in filters[0]
    assert params_by_name == {
        "reported_code": "0450",
        "reported_code_1": "450",
        "reported_code_system": "RC",
    }
    assert code_filters._reported_code_values_for_system(None, "abc") == (
        "ABC",
    )
    assert code_filters._reported_code_values_for_system(None, None) == ()


@pytest.mark.parametrize(
    ("system", "code", "expected"),
    (
        ("RXNORM", "99213", set()),
        ("CPT", "123", set()),
        ("CPT", "99213", {("CPT", "99213"), ("HCPCS", "99213")}),
        ("CDT", "D0123", {("CDT", "D0123"), ("HCPCS", "D0123")}),
        ("HCPCS", "A0001", {("HCPCS", "A0001")}),
    ),
)
def test_equivalent_external_code_pairs(system, code, expected):
    assert code_filters._ptg2_equivalent_external_pairs(system, code) == expected


def test_resolved_code_filter_combines_external_and_internal_codes():
    filters = []
    params_by_name = {}
    context_by_field = {
        "resolved_codes": [
            {"code_system": "HP_PROCEDURE_CODE", "code": "7"},
            {"code_system": "CPT", "code": "99213"},
            {"code_system": "HCPCS", "code": "99213"},
            {"code_system": "RC", "code": "0450"},
        ],
        "internal_codes": [7, 8],
    }

    code_filters._append_resolved_code_filter(
        filters,
        params_by_name,
        code="99213",
        code_system="CPT",
        code_context=context_by_field,
    )

    assert len(filters) == 1
    assert "reported_code_system_0_0" in filters[0]
    assert "procedure_code IN" in filters[0]
    assert {7, 8}.issubset(set(params_by_name.values()))
    assert {"CPT", "HCPCS"}.issubset(set(params_by_name.values()))


def test_resolved_code_filter_handles_fallback_and_empty_contexts():
    delegated_filters = []
    delegated_params_by_name = {}
    code_filters._append_resolved_code_filter(
        delegated_filters,
        delegated_params_by_name,
        code="7",
        code_system="HP_PROCEDURE_CODE",
    )
    assert delegated_filters == ["procedure_code = :procedure_code"]

    invalid_internal_filters = []
    code_filters._append_resolved_code_filter(
        invalid_internal_filters,
        {},
        code="not-numeric",
        code_system="HP_PROCEDURE_CODE",
        code_context={"resolved_codes": [], "internal_codes": []},
    )
    assert invalid_internal_filters == ["FALSE"]

    empty_external_filters = []
    code_filters._append_resolved_code_filter(
        empty_external_filters,
        {},
        code="99213",
        code_system="CPT",
        code_context={"resolved_codes": [], "internal_codes": []},
    )
    assert empty_external_filters == []

    blank_filters = []
    code_filters._append_resolved_code_filter(
        blank_filters,
        {},
        code=None,
        code_system="CPT",
        code_context={},
    )
    assert blank_filters == []


def test_code_query_fields_and_compact_filter_qualification():
    context_by_field = {
        "input_code": {"code_system": "CPT", "code": "99213"},
        "resolved_codes": None,
        "matched_via": None,
        "code_match_mode": "equivalent_procedure_codes",
    }

    assert code_filters._ptg2_code_query_fields(None, {}) == {}
    assert code_filters._ptg2_code_query_fields(context_by_field, {}) == {}
    assert code_filters._ptg2_code_query_fields(
        context_by_field,
        {"include_details": "true"},
    ) == {
        "input_code": context_by_field["input_code"],
        "resolved_codes": [],
        "matched_via": [],
        "code_match_mode": "equivalent_procedure_codes",
    }
    assert code_filters._qualify_compact_filters(
        ["reported_code = :reported_code AND plan_id = source.plan_id"]
    ) == [
        "r.reported_code = :reported_code AND r.plan_id = source.plan_id"
    ]


@pytest.mark.parametrize(
    ("value", "expected"),
    ((None, None), ("short", None), ("207Q00000X", "207Q00000X")),
)
def test_normalize_taxonomy_code(value, expected):
    assert code_filters._normalize_taxonomy_code(value) == expected


@pytest.mark.parametrize(
    ("args", "expected_fragment"),
    (
        ({"code_system": "HCPCS", "code": "29888"}, None),
        ({"code_system": "CPT", "code": "not-numeric"}, None),
        ({"code_system": "CPT", "code": "50000"}, None),
        ({"code_system": "CPT", "code": "29888"}, "207X00000X"),
    ),
)
def test_inferred_provider_taxonomy_sql(args, expected_fragment):
    sql = code_filters._inferred_provider_taxonomy_sql(
        args,
        nt_alias="nt",
        nucc_alias="nucc",
    )

    if expected_fragment is None:
        assert sql == ""
    else:
        assert expected_fragment in sql
        assert "orthopedic surgery" in sql
