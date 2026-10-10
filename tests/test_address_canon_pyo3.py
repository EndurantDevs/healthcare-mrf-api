# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import inspect
import json
import struct
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pytest

from process.ext import address_canon
from tests.test_custom_import_source_preparation import _exercise_source_route, _reference_encoder

ptg2_address_canon = pytest.importorskip("ptg2_address_canon")

FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures"


def _golden_cases():
    payload = json.loads((FIXTURE_DIR / "address_canonical_golden.json").read_text())
    return list(payload["explicit_cases"])


def test_pyo3_canon_version_matches_python():
    assert ptg2_address_canon.canon_version() == address_canon.current_canon_version()


def test_pyo3_v4_graph_primitives_are_exact_and_fail_closed():
    assert ptg2_address_canon.intersect_sorted_u32([1, 3, 7, 9], [2, 3, 4, 9]) == [3, 9]
    with pytest.raises(ValueError):
        ptg2_address_canon.intersect_sorted_u32([2, 1], [1])

    payload = struct.pack("<4I", 0, 1, 255, 0xFFFFFFFF)
    assert ptg2_address_canon.ptg2_decode_u32_le(payload) == [
        0,
        1,
        255,
        0xFFFFFFFF,
    ]
    with pytest.raises(ValueError):
        ptg2_address_canon.ptg2_decode_u32_le(b"bad")


def test_pyo3_address_canonical_golden_corpus_matches_frozen_expected_values():
    cases = _golden_cases()
    assert len(cases) >= 270
    rows = [
        tuple(case.get(key) for key in ("first_line", "second_line", "city", "state", "zip", "country"))
        for case in cases
    ]

    results = ptg2_address_canon.canonicalize_batch(rows)

    assert len(results) == len(cases)
    for case, result in zip(cases, results):
        assert result["identity_key"] == case["expected_identity_key"], case["id"]
        assert result["address_key"] == case["expected_address_key"], case["id"]
        assert result["premise_identity_key"] == case["expected_premise_identity_key"], case["id"]
        assert result["premise_key"] == case["expected_premise_key"], case["id"]


def test_pyo3_location_batch_matches_full_canonicalizer():
    rows = [
        tuple(case.get(key) for key in ("first_line", "second_line", "city", "state", "zip", "country"))
        for case in _golden_cases()
    ]
    rows.append(("10 Downing Street", None, "London", None, "SW1A 2AA", "United Kingdom"))

    full = ptg2_address_canon.canonicalize_batch(rows)
    compact = ptg2_address_canon.canonicalize_location_batch(rows)

    assert compact == [
        (
            row["address_key"],
            row["state_code"],
            row["city_norm"],
        )
        for row in full
    ]


def test_pyo3_contact_canonical_batch_normalizes_us_and_international_values():
    if not hasattr(ptg2_address_canon, "canonicalize_contact_batch"):
        pytest.skip("installed ptg2_address_canon module does not include contact canonicalizer")
    results = ptg2_address_canon.canonicalize_contact_batch(
        [
            ("+1 (312) 555-0100 ext. 45", "312.555.0199 # 22", "US"),
            ("+44 20 7946 0958", None, "GB"),
            ("555-1212", None, "US"),
        ]
    )

    assert results[0]["phone_number"] == "3125550100"
    assert results[0]["phone_extension"] == "45"
    assert results[0]["phone_valid_for_fallback"] is True
    assert results[0]["fax_number"] == "3125550199"
    assert results[0]["fax_number_digits"] == "3125550199"
    assert results[0]["fax_extension"] == "22"
    assert results[1]["phone_number"] == "442079460958"
    assert results[1]["phone_is_international"] is True
    assert results[1]["phone_valid_for_fallback"] is False
    assert results[2]["phone_number"] is None
    assert results[2]["phone_valid_for_fallback"] is False


@pytest.fixture
def native():
    module = pytest.importorskip("ptg2_address_canon")
    encoder = getattr(module, "custom_import_source_documents_v1", None)
    assert inspect.isbuiltin(encoder), "the installed extension must expose the SOURCE batch capability"
    return encoder


def _layout(kind="string", *, child=True):
    return child, (("key", "string"), ("value", kind)), (("root_key", 0),), (0,) if child else ()


@pytest.mark.parametrize("child", [False, True])
@pytest.mark.parametrize(
    "kind,cell",
    [
        ("string", ("value", 'quote" slash\\ / café Ω 漢字 😀 \u2028\u2029')),
        ("string", ("value", "".join(chr(code) for code in range(32)))),
        ("string", ("value", "\x01" * 4096)),
        ("integer", ("value", -(2**63))),
        ("integer", ("value", 2**63 - 1)),
        ("boolean", ("value", True)),
        ("boolean", ("value", False)),
        ("decimal", ("value", "-123.450000000001")),
        ("decimal", ("value", "0")),
        ("date", ("value", "0001-01-01")),
        ("timestamp", ("value", "2024-02-29T01:02:03.123456Z")),
        *(
            (kind, (state, None))
            for kind in ("string", "integer", "boolean", "decimal", "date", "timestamp")
            for state in ("missing", "null")
        ),
    ],
)
def test_native_document_bytes_and_domain_hashes_match_python(native, child, kind, cell):
    layout = _layout(kind, child=child)
    rows = [(("value", "first"), cell), (("value", "second"), cell)]
    assert native(layout, rows) == _reference_encoder(layout, rows)


def test_native_parent_key_can_reuse_one_source_cell(native):
    layout = (True, (("key", "string"),), (("first", 0), ("second", 0)), (0,))
    rows = [(("value", "Ω"),)]
    assert native(layout, rows) == _reference_encoder(layout, rows)


@pytest.mark.parametrize("hosted", [False, True])
@pytest.mark.parametrize("stream", [0, 1])
@pytest.mark.parametrize("same_type", [False, True])
async def test_native_capability_reaches_both_daemon_source_routes(native, monkeypatch, hosted, stream, same_type):
    await _exercise_source_route(monkeypatch, hosted, native, stream=stream, same_type=same_type)


@pytest.mark.parametrize(
    "malformation",
    [
        "too_many_rows",
        "too_many_fields",
        "duplicate_field",
        "field_type",
        "key_index",
        "missing_key",
        "boolean_integer",
        "integer_overflow",
        "surrogate",
        "text_bytes",
        "state",
        "row_shape",
        "input_bytes",
    ],
)
def test_native_rejects_closed_envelopes_and_releases_slot_for_next_call(native, malformation):
    layout = _layout()
    cell_rows = [(("value", "key"), ("value", "value"))]
    match malformation:
        case "too_many_rows":
            cell_rows *= 65
        case "too_many_fields":
            layout = (True, tuple((f"field_{index}", "string") for index in range(65)), (("root", 0),), (0,))
        case "duplicate_field":
            layout = (True, (("key", "string"), ("key", "string")), (("root", 0),), (0,))
        case "field_type":
            layout = _layout("unknown")
        case "key_index":
            layout = (*layout[:2], (("root", 2),), layout[3])
        case "missing_key":
            cell_rows = [(("missing", None), ("value", "value"))]
        case "boolean_integer" | "integer_overflow":
            layout = _layout("integer")
            cell_rows = [(("value", "key"), ("value", True if malformation == "boolean_integer" else 2**63))]
        case "surrogate" | "text_bytes":
            cell_rows = [(("value", "key"), ("value", "\ud800" if malformation == "surrogate" else "Ω" * 4096))]
        case "state":
            cell_rows = [(("value", "key"), ("unknown", "value"))]
        case "row_shape":
            cell_rows = [(("value", "key"),)]
        case _:
            layout = (True, tuple((f"field_{index}", "string") for index in range(64)), (("root", 0),), (0,))
            cell_rows = [(("value", "value"),) * 64] * 64
    with pytest.raises((ValueError, TypeError, OverflowError, UnicodeEncodeError)):
        native(layout, cell_rows)
    valid_rows = [(("value", "key"), ("value", "value"))]
    assert native(_layout(), valid_rows) == _reference_encoder(_layout(), valid_rows)


@pytest.mark.parametrize(
    "layout,message",
    [
        ((), "scalar verification tuple shape differs"),
        ((1, *_layout()[1:]), "source record kind differs"),
        ((True, (), (("root", 0),), (0,)), "source field bound exceeded"),
        *(
            ((True, ((name, "string"),), (("root", 0),), (0,)), "source field identity differs")
            for name in ("", "Key", "1key", "key-name", "clé")
        ),
        (
            (True, (("x" * 64, "string"),), (("root", 0),), (0,)),
            "scalar digest text exceeds its admitted encoding",
        ),
        (
            (True, (("key", "x" * 17),), (("root", 0),), (0,)),
            "scalar digest text exceeds its admitted encoding",
        ),
        ((True, (("key",),), (("root", 0),), (0,)), "scalar verification tuple shape differs"),
        ((*_layout()[:2], (), (0,)), "source root key bound exceeded"),
        (
            (*_layout()[:2], tuple((f"root_{index}", 0) for index in range(4)), (0,)),
            "source root key bound exceeded",
        ),
        ((*_layout()[:2], (("root",),), (0,)), "scalar verification tuple shape differs"),
        ((*_layout()[:2], (("root", 0), ("root", 1)), (0,)), "source root key differs"),
        ((*_layout()[:2], (("root", -1),), (0,)), "source key index differs"),
        (
            (*_layout()[:2], (("root", True),), (0,)),
            "scalar verification identity requires an integer",
        ),
        ((*_layout()[:3], ()), "source child key bound exceeded"),
        ((*_layout()[:3], (0, 1, 0, 1)), "source child key bound exceeded"),
        ((*_layout(child=False)[:3], (0,)), "source child key bound exceeded"),
        ((*_layout()[:3], (0, 0)), "source child key differs"),
        ((*_layout()[:3], (-1,)), "source key index differs"),
    ],
)
def test_native_invalid_layout_releases_slot(native, layout, message):
    rows = [(("value", "key"), ("value", "value"))]
    with pytest.raises(ValueError, match=message):
        native(layout, rows)
    assert native(_layout(), rows) == _reference_encoder(_layout(), rows)


@pytest.mark.parametrize(
    "kind,cell,message",
    [
        ("string", ("missing", "unexpected"), "source scalar state differs"),
        ("string", ("null", "unexpected"), "source scalar state differs"),
        ("string", ("value", None), "source scalar type differs"),
        ("boolean", ("value", 1), "source scalar type differs"),
        ("string", ("value", 1), "scalar digest text requires a native string"),
        ("integer", ("value", 1.0), "scalar verification identity requires an integer"),
        ("string", ("state_too_long", None), "scalar digest text exceeds its admitted encoding"),
        ("string", ("value", "x" * 4097), "scalar digest text exceeds its admitted encoding"),
        ("string", ("value",), "scalar verification tuple shape differs"),
    ],
)
def test_native_invalid_cell_releases_slot(native, kind, cell, message):
    rows = [(("value", "key"), cell)]
    with pytest.raises(ValueError, match=message):
        native(_layout(kind), rows)
    valid_rows = [(("value", "key"), ("value", "value"))]
    assert native(_layout(), valid_rows) == _reference_encoder(_layout(), valid_rows)


@pytest.mark.parametrize("state", ["missing", "null"])
@pytest.mark.parametrize("index", [0, 1])
def test_native_absent_root_or_child_key_releases_slot(native, state, index):
    layout = (*_layout()[:3], (1,))
    cells = [("value", "root"), ("value", "child")]
    cells[index] = (state, None)
    with pytest.raises(ValueError, match="source key value is absent"):
        native(layout, [tuple(cells)])
    rows = [(("value", "root"), ("value", "child"))]
    assert native(layout, rows) == _reference_encoder(layout, rows)


def test_native_empty_batch_releases_slot(native):
    with pytest.raises(ValueError, match="source row bound exceeded"):
        native(_layout(), [])
    rows = [(("value", "key"), ("value", "value"))]
    assert native(_layout(), rows) == _reference_encoder(_layout(), rows)


def test_native_composite_keys_preserve_declared_order(native):
    layout = (
        True,
        (("z", "integer"), ("a", "boolean"), ("m", "string")),
        (("third", 2), ("first", 0), ("second", 1)),
        (1, 2, 0),
    )
    rows = [(("value", 0), ("value", False), ("value", ""))]
    assert native(layout, rows) == _reference_encoder(layout, rows)


def test_shared_native_pool_preserves_batch_order_under_concurrent_callers(native):
    layout = _layout()
    batches = [
        [(("value", f"key_{batch}_{row}"), ("value", "Ω" * (row + 1))) for row in range(64)] for batch in range(4)
    ]
    with ThreadPoolExecutor(max_workers=4) as callers:
        results = list(callers.map(lambda rows: native(layout, rows), batches))
    assert results == [_reference_encoder(layout, rows) for rows in batches]
