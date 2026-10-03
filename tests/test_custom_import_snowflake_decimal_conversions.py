# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Explicit binary64 conversion does not broaden the shared exact decimal domain."""

from __future__ import annotations

import json
import math
from dataclasses import replace
from decimal import ROUND_UP, Decimal, Inexact, localcontext
from types import SimpleNamespace

import pytest

from process.custom_import import snowflake_python
from process.custom_import.capture import CaptureError, _validated_scalar
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family import normalize_source_decimal
from process.custom_import.snowflake import SnowflakeConnectorError
from process.custom_import.snowflake_binding import SnowflakeSourceBinding, SnowflakeSourceBindingError
from process.custom_import.snowflake_bundle import SnowflakeBundleError, SnowflakeBundleStatementBuilder
from process.custom_import.snowflake_preflight_schema import (
    FLOAT_DECIMAL_CONVERSION,
    _source_type,
    convert_snowflake_float,
    normalize_decimal_conversions,
)
from tests.test_custom_import_processing_policy import _policy_document
from tests.test_custom_import_snowflake_shared_capture import _runtime
from tests.test_custom_import_snowflake_source_binding import _binding_document, _definition

_CONVERSIONS = {"score": FLOAT_DECIMAL_CONVERSION, "amount": FLOAT_DECIMAL_CONVERSION}
_LEGACY_DIGESTS = {
    1: (
        "dcff56976c2a3e340a4e4a511f0643d83a9cbe881f371647cb2995c4f1739ebb",
        "9a5018ac5d8379dfddf2808fd06fadd2f27097372da012bcd2b1ad4768609cd0",
        "db0b1166539462746821ce81e0027fcb4329eb0c8aee6e4e7419edd13bcee974",
    ),
    2: (
        "9f98e3a4060555fcd890cb98c11293eff8386c1c0fa22d9b2ad44053539cb2dd",
        "bbcd99f15f6857ba773abe82bacb14ec227f1eace461a9a60c0b2f9c4ecd1c25",
        "0cdb305b766fa1971f4f62f25f47f739b441df5efe9c02d53e3f2da9e63cfb12",
    ),
}


def _binding(*, opted_in=True, version=1):
    document = json.loads(_definition().canonical)
    document["schema"]["root"]["fields"][1]["type"] = "decimal"
    definition = CustomImportDefinition.from_mapping(document)
    binding_document = _binding_document(definition)
    if opted_in:
        binding_document["decimal_conversions"] = {"score": FLOAT_DECIMAL_CONVERSION}
    if version == 2:
        binding_document["contract"] = "custom-import/source-binding/v2"
        binding_document["processing_policy"] = _policy_document()
    return definition, SnowflakeSourceBinding.from_mapping(binding_document)


def _statement(definition, binding):
    approved, bindings = binding.bundle_components(definition)
    builder = SnowflakeBundleStatementBuilder(approved_relations=approved)
    return builder.build_statement(
        builder.prepare_request(
            definition,
            bindings=bindings,
            processing_policy=binding.processing_policy,
            decimal_conversions=binding.decimal_conversions,
        )
    )


@pytest.mark.parametrize(
    "value, expected",
    [
        (None, None),
        (0.0, "0"),
        (-0.0, "0"),
        (0.1, "0.1"),
        (-0.1, "-0.1"),
        (1.0 / 8192, "0.000122070312"),
        (3.0 / 8192, "0.000366210938"),
        (-1.0 / 8192, "-0.000122070312"),
        (-3.0 / 8192, "-0.000366210938"),
        (math.nextafter(1.0 / 8192, math.inf), "0.000122070313"),
        (math.nextafter(3.0 / 8192, -math.inf), "0.000366210937"),
        (math.ulp(0.0), "0"),
        (-math.ulp(0.0), "0"),
        (math.nextafter(1e18, 0), "999999999999999872"),
    ],
)
def test_float_conversion_has_fixed_rounding_and_no_ambient_context(value, expected):
    with localcontext() as context:
        context.prec = 2
        context.rounding = ROUND_UP
        context.Emax = 2
        context.Emin = -2
        context.traps[Inexact] = True
        actual = convert_snowflake_float(value)
    assert actual == (None if expected is None else Decimal(expected))
    if actual is not None:
        assert format(actual, "f") == expected


@pytest.mark.parametrize(
    "value",
    [
        True,
        1,
        "0.1",
        Decimal("0.1"),
        [],
        float("nan"),
        float("inf"),
        float("-inf"),
        1e18,
        -1e18,
        float.fromhex("0x1.fffffffffffffp+1023"),
    ],
)
def test_invalid_or_out_of_range_float_is_never_silently_null(value):
    with pytest.raises(SnowflakeConnectorError):
        convert_snowflake_float(value)


def test_generic_decimal_and_capture_still_reject_float():
    assert normalize_source_decimal(0.1) is None
    with pytest.raises(CaptureError):
        _validated_scalar(0.1, 1)


@pytest.mark.parametrize("source_type", ["REAL", "FLOAT", "FLOAT4", "FLOAT8", "DOUBLE", "DOUBLE PRECISION"])
def test_source_type_retains_real_and_never_fabricates_fixed_precision(source_type):
    assert _source_type(SimpleNamespace(type_name=source_type)) == "REAL"
    assert _source_type(SimpleNamespace(type_code=1), field_types=(None, SimpleNamespace(name="REAL"))) == "REAL"


@pytest.mark.parametrize("invalid", [None, {}, [], {"score": "round"}, {"missing": FLOAT_DECIMAL_CONVERSION}])
def test_binding_rejects_invalid_present_conversion_map(invalid):
    definition, binding = _binding(opted_in=False)
    document = json.loads(binding.canonical)
    document["decimal_conversions"] = invalid
    with pytest.raises(SnowflakeSourceBindingError):
        SnowflakeSourceBinding.from_mapping(document)


@pytest.mark.parametrize("field_id", ["npi", "detail_npi", "detail_id", "unknown"])
def test_conversion_cannot_change_key_identity_or_unknown_fields(field_id):
    definition, _ = _binding()
    with pytest.raises(SnowflakeConnectorError):
        normalize_decimal_conversions({field_id: FLOAT_DECIMAL_CONVERSION}, definition)


def test_even_decimal_keys_cannot_be_converted():
    definition, _ = _binding()
    document = json.loads(definition.canonical)
    document["schema"]["children"][0]["fields"][1]["type"] = "decimal"
    decimal_key_definition = CustomImportDefinition.from_mapping(document)
    with pytest.raises(SnowflakeConnectorError, match="non-key"):
        normalize_decimal_conversions({"detail_id": FLOAT_DECIMAL_CONVERSION}, decimal_key_definition)


@pytest.mark.parametrize("version", [1, 2])
def test_conversion_wire_identity_is_immutable_and_legacy_omission_is_exact(version):
    definition, legacy = _binding(opted_in=False, version=version)
    _, opted = _binding(version=version)
    assert "decimal_conversions" not in json.loads(legacy.canonical)
    assert replace(opted, decimal_conversions=None) == legacy
    assert SnowflakeSourceBinding.from_json(opted.canonical) == opted
    with pytest.raises(TypeError):
        opted.decimal_conversions["score"] = "different"
    old_statement, new_statement = _statement(definition, legacy), _statement(definition, opted)
    assert (legacy.digest, old_statement.request.request_sha256, old_statement.statement_sha256) == _LEGACY_DIGESTS[
        version
    ]
    assert old_statement.sql == new_statement.sql
    assert old_statement.request.request_sha256 != new_statement.request.request_sha256
    assert old_statement.statement_sha256 != new_statement.statement_sha256
    assert replace(new_statement.request, decimal_conversions=None) == old_statement.request
    assert json.loads(new_statement.request.canonical_request)["decimal_conversions"] == dict(opted.decimal_conversions)


def test_stale_request_seal_and_shared_source_policy_disagreement_fail(monkeypatch):
    connector, request, _, _, _ = _runtime(monkeypatch)
    with pytest.raises(SnowflakeBundleError, match="inconsistent"):
        connector.build_statement(replace(request, decimal_conversions={"score": FLOAT_DECIMAL_CONVERSION}))
    opted = replace(request, decimal_conversions=_CONVERSIONS)
    connector.build_statement(opted)
    object.__setattr__(opted, "decimal_conversions", None)
    with pytest.raises(SnowflakeBundleError, match="stale identity"):
        connector.build_statement(opted)


def test_real_is_rejected_without_opt_in_in_bundle_and_single_stream(monkeypatch):
    connector, request, _, cursor, _ = _runtime(monkeypatch)
    metadata = list(cursor.description)
    metadata[5] = SimpleNamespace(name="score", type_name="REAL", is_nullable=True)
    cursor.description = tuple(metadata)
    with pytest.raises(SnowflakeConnectorError, match="declared decimal conversion"):
        snowflake_python._bundle_result_schemas(connector.build_statement(request), cursor.description)
    from tests.test_custom_import_snowflake_python import _legacy_statement as single_statement

    statement = single_statement()
    result_columns = tuple(
        SimpleNamespace(name=column.field_id, type_name="REAL", is_nullable=True)
        for column in statement.request.selected_columns
    )
    with pytest.raises(SnowflakeConnectorError, match="declared decimal conversion"):
        snowflake_python._result_schema(statement, result_columns)
