# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib
import importlib.util
import inspect
import json
import struct
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from uuid import UUID

import pytest

from process.company_registry_assertion_values import (
    CompanyAssertionProvenance,
    CompanyRegistryAssertionValueError,
)
from process.ext import address_canon
from tests.test_custom_import_source_preparation import _exercise_source_route, _reference_encoder

if importlib.util.find_spec("ptg2_address_canon") is None:
    pytest.importorskip("ptg2_address_canon", exc_type=ModuleNotFoundError)
ptg2_address_canon = importlib.import_module("ptg2_address_canon")

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


@pytest.fixture(scope="module")
def native_validate():
    """Only an absent package may skip; a broken or incomplete installed export fails."""
    if importlib.util.find_spec("ptg2_address_canon") is None:
        pytest.importorskip("ptg2_address_canon", exc_type=ModuleNotFoundError)
    module = importlib.import_module("ptg2_address_canon")
    export = module.validate_company_group_assertions
    assert inspect.isbuiltin(export)
    return export


def _identity(number):
    return f"{number:08x}-0000-4000-8000-000000000001"


def _row(number=1, group=10, start="2024-01-01", end=None, *, source=False):
    return {
        "assertion_id": _identity(number),
        "company_id": _identity(100_000),
        "group_id": _identity(group) if group is not None else None,
        "group_kind": "corporate_parent",
        "relationship_kind": "reported_affiliation",
        "valid_from": start,
        "valid_to": end,
        "provenance": {
            "kind": "source_reference" if source else "manual_reference",
            "evidence_ref": "Retained reference" if source else "Reviewed reference",
            "snapshot_id": _identity(200_000) if source else None,
            "source_record_key": "record-17" if source else None,
        },
    }


def _wire(rows):
    return json.dumps(rows, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode("utf-8")


def _reduce(native_validate, rows):
    encoded = native_validate(_wire(rows))
    assert type(encoded) is bytes
    return json.loads(encoded)


def test_installed_batch_is_byte_deterministic_and_preserves_effective_rows(native_validate):
    """Ordering is canonical; same-group and independent-kind references retain their values."""
    rows = [
        _row(1, end="2024-01-31"),
        _row(2, 11),
        _row(3, start="2024-01-15", end="2024-01-31", source=True),
        _row(4, 12, start="2024-02-01"),
    ]
    rows[1]["group_kind"] = "naic_group"
    forward = native_validate(_wire(rows))
    assert type(forward) is bytes
    assert forward == native_validate(_wire(rows[::-1]))
    assert json.loads(forward) == {
        "assertions": rows,
        "effective_assertion_indices": [0, 1, 2, 3],
        "reviews": [],
        "counts": {"input_rows": 4, "effective_rows": 4, "review_rows": 0},
    }


def test_installed_conflicts_missing_rows_and_unique_accounting_are_retained(native_validate):
    """Connected conflicts have no winner; unknown periods and source identities remain reviewable."""
    rows = [
        _row(1, end="2024-01-10"),
        _row(2, start="2024-01-10", end="2024-01-20"),
        _row(3, 11, start="2024-01-20", end="2024-01-30"),
        _row(4, 12, start="2024-02-01", end="2024-02-02"),
        _row(5, None, source=True),
        _row(6, start=None, end="2024-12-31"),
        _row(7, None, start=None, source=True),
    ]
    reduced = _reduce(native_validate, rows[::-1])
    assert reduced["assertions"] == rows
    assert reduced["effective_assertion_indices"] == [3]
    assert reduced["reviews"] == [
        {"code": "contradictory_overlapping_groups", "assertion_indices": [0, 1, 2]},
        {"code": "missing_effective_start", "assertion_indices": [5]},
        {"code": "missing_group", "assertion_indices": [4]},
        {"code": "missing_group_and_effective_start", "assertion_indices": [6]},
    ]
    assert reduced["counts"] == {"input_rows": 7, "effective_rows": 1, "review_rows": 6}
    assert reduced["counts"]["input_rows"] == sum(reduced["counts"][name] for name in ("effective_rows", "review_rows"))


@pytest.mark.parametrize("end", ["2024-02-29", None], ids=["inclusive-touch", "open-ended"])
def test_installed_single_day_and_touching_periods_conflict(native_validate, end):
    rows = [_row(1, end=end), _row(2, 11, start="2024-02-29", end="2024-02-29")]
    reduced = _reduce(native_validate, rows)
    assert reduced["assertions"] == rows
    assert reduced["effective_assertion_indices"] == []
    assert reduced["reviews"] == [{"code": "contradictory_overlapping_groups", "assertion_indices": [0, 1]}]
    assert reduced["counts"] == {"input_rows": 2, "effective_rows": 0, "review_rows": 2}


@pytest.mark.parametrize(
    "field,value",
    [("company_id", _identity(100_001)), ("group_kind", "naic_group"), ("relationship_kind", "verified_ownership")],
)
def test_installed_conflict_scope_is_exact(native_validate, field, value):
    rows = [_row(1), _row(2, 11)]
    rows[1][field] = value
    reduced = _reduce(native_validate, rows)
    assert reduced["assertions"] == rows
    assert reduced["reviews"] == []
    assert reduced["effective_assertion_indices"] == [0, 1]


@pytest.mark.parametrize(
    "field,value",
    [
        ("company_id", "synthetic-invalid-sentinel"),
        ("group_kind", "parent"),
        ("relationship_kind", "automatic_ownership"),
        ("valid_from", "2023-02-29"),
        ("valid_to", "2023-12-31"),
        ("group_id", 17),
        ("unexpected", True),
    ],
)
def test_installed_malformed_later_row_refuses_whole_batch(native_validate, field, value):
    malformed = _row(2)
    malformed[field] = value
    with pytest.raises(ValueError) as rejected:
        native_validate(_wire([_row(), malformed]))
    assert "synthetic-invalid-sentinel" not in str(rejected.value)


def test_installed_missing_fields_and_duplicate_ids_refuse_without_partial_results(native_validate):
    missing = _row(2)
    del missing["group_id"]
    unknown_provenance = _row(2)
    unknown_provenance["provenance"]["approved"] = True
    for rows in ([_row(), missing], [_row(), unknown_provenance], [_row(), _row()]):
        with pytest.raises(ValueError):
            native_validate(_wire(rows))
    assert _reduce(native_validate, [_row()])["counts"] == {"input_rows": 1, "effective_rows": 1, "review_rows": 0}


@pytest.mark.parametrize("payload", [b"null", b"{}", b"[null]", b"[] []", b"[", b"\xff"])
def test_installed_input_is_one_strict_json_array(native_validate, payload):
    with pytest.raises(ValueError):
        native_validate(payload)


def test_installed_duplicate_json_fields_and_nonbytes_refuse(native_validate):
    row = _wire([_row()])
    duplicate = row.replace(
        b'{"assertion_id":', b'{"company_id":"' + _identity(100_001).encode() + b'","assertion_id":', 1
    )
    with pytest.raises(ValueError):
        native_validate(duplicate)
    with pytest.raises(TypeError):
        native_validate("[]")


@pytest.mark.parametrize("field", ["evidence_ref", "source_record_key"])
@pytest.mark.parametrize("text", ["Reviewed reference", "R\u00e9f\u00e9rence", "e\u0301vidence", "record\U0001f600"])
def test_installed_printable_unicode_matches_current_python_provenance(native_validate, field, text):
    row = _row(source=True)
    row["provenance"][field] = text
    provenance = row["provenance"]
    checked = CompanyAssertionProvenance(
        provenance["kind"], provenance["evidence_ref"], UUID(provenance["snapshot_id"]), provenance["source_record_key"]
    )
    assert getattr(checked, field) == text and text.isprintable()
    assert _reduce(native_validate, [row])["assertions"] == [row]


@pytest.mark.parametrize("field", ["evidence_ref", "source_record_key"])
@pytest.mark.parametrize(
    "scalar",
    ["\u200d", "\u2028", "\ue000", "\u0380", "\u00a0", "\u088f"],
    ids=["format", "line-separator", "private-use", "unassigned", "nonbreaking-space", "unicode-version-gap"],
)
def test_installed_python_printability_gate_covers_even_review_rows(native_validate, field, scalar):
    """Rust control checks alone permit these scalars; the genuine Python gate must reject them."""
    text = "before" + scalar + "after"
    assert not text.isprintable()
    row = _row(2, None, start=None, source=True)
    row["provenance"][field] = text
    provenance = row["provenance"]
    with pytest.raises(CompanyRegistryAssertionValueError):
        CompanyAssertionProvenance(
            provenance["kind"],
            provenance["evidence_ref"],
            UUID(provenance["snapshot_id"]),
            provenance["source_record_key"],
        )
    with pytest.raises(ValueError):
        native_validate(_wire([_row(), row]))


@pytest.mark.parametrize("field", ["evidence_ref", "source_record_key"])
@pytest.mark.parametrize("unit,count", [("x", 128), ("\u00e9", 64)], ids=["ascii", "multibyte"])
def test_installed_provenance_limits_are_utf8_bytes(native_validate, field, unit, count):
    row = _row(source=True)
    exact = unit * count
    assert len(exact.encode("utf-8")) == 128
    row["provenance"][field] = exact
    provenance = row["provenance"]
    CompanyAssertionProvenance(
        provenance["kind"], provenance["evidence_ref"], UUID(provenance["snapshot_id"]), provenance["source_record_key"]
    )
    assert _reduce(native_validate, [row])["assertions"] == [row]
    provenance[field] = exact + unit
    with pytest.raises(CompanyRegistryAssertionValueError):
        CompanyAssertionProvenance(
            provenance["kind"],
            provenance["evidence_ref"],
            UUID(provenance["snapshot_id"]),
            provenance["source_record_key"],
        )
    with pytest.raises(ValueError):
        native_validate(_wire([_row(2), row]))


def test_installed_batch_limits_accept_exact_boundary_and_refuse_excess(native_validate):
    rows = [_row(number) for number in range(1, 5001)]
    reduced = _reduce(native_validate, rows)
    assert reduced["counts"] == {"input_rows": 5000, "effective_rows": 5000, "review_rows": 0}
    assert len(reduced["assertions"]) == len(reduced["effective_assertion_indices"]) == 5000
    with pytest.raises(ValueError):
        native_validate(_wire([*rows, _row(5001)]))
    exact_input = b"[]" + b" " * (8 * 1024 * 1024 - 2)
    assert native_validate(exact_input) == native_validate(b"[]")
    with pytest.raises(ValueError):
        native_validate(exact_input + b" ")
    assert _reduce(native_validate, []) == {
        "assertions": [],
        "effective_assertion_indices": [],
        "reviews": [],
        "counts": {"input_rows": 0, "effective_rows": 0, "review_rows": 0},
    }
