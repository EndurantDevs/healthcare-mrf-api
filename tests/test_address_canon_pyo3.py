# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import hashlib
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


def _compiled_export(name):
    export = getattr(ptg2_address_canon, name)
    assert inspect.isbuiltin(export), f"installed {name} must be compiled"
    return export


def _copy_fields(wire, width):
    """Independently decode complete COPY framing at the Python extension boundary."""
    assert type(wire) is bytes
    assert wire[:19] == b"PGCOPY\n\xff\r\n\0" + bytes(8)
    offset, rows = 19, []
    while True:
        count = struct.unpack_from(">h", wire, offset)[0]
        offset += 2
        if count == -1:
            break
        assert count == width
        fields = []
        for _ in range(count):
            length = struct.unpack_from(">i", wire, offset)[0]
            offset += 4
            assert length >= -1
            fields.append(None if length == -1 else wire[offset : offset + length])
            if length >= 0:
                offset += length
        rows.append(fields)
    assert offset == len(wire)
    return rows


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


@pytest.mark.parametrize(
    "name,arguments,code",
    [
        ("encode_network_membership_batch", (b"\xff",), "invalid_json"),
        ("validate_network_catalog_batch", (b"\xff",), "invalid_json"),
        ("validate_company_network_assertions", (b"\xff",), "invalid_json"),
        ("encode_network_source_binding_batch", (b"\xff",), "invalid_json"),
        ("parse_registry_target_ledger", (b"\xff",), "invalid_utf8"),
        ("parse_registry_network_evidence", (b"\xff",), "registry_network_evidence_invalid"),
        ("extract_fhir_network_identity_batch", (b"\xff",), "invalid_batch"),
        ("encode_fhir_network_identity_batch", (b"\xff",), "invalid_batch"),
        ("build_registry_network_coverage", (b"\xff",), "registry_coverage_invalid"),
        ("validate_registry_required_target_ledger_artifact", (b"\xff",), "registry_target_review_ledger_invalid"),
        ("validate_registry_required_target_review_artifacts", (b"\xff",), "registry_target_review_invalid"),
        ("encode_registry_target_ledger_artifact", (b"\xff", _identity(1)), "invalid_utf8"),
        (
            "encode_registry_required_target_review_artifact",
            (b"\xff", b"{}", _identity(1)),
            "registry_target_review_invalid",
        ),
    ],
)
def test_compiled_registry_interfaces_preserve_validation_failures(name, arguments, code):
    with pytest.raises(ValueError, match=code):
        _compiled_export(name)(*arguments)


def test_compiled_membership_copy_preserves_namespace_and_rejects_id_coercion():
    encode = _compiled_export("encode_network_membership_batch")
    membership_by_field = {
        "network_id": 42,
        "provider_system": "manual",
        "provider_id": "000123",
        "location_id": _identity(1),
        "evidence_id": "review-1",
    }
    wire, count = encode(_wire([membership_by_field]))
    assert count == 1
    assert _copy_fields(wire, 5) == [
        [struct.pack(">i", 42), b"manual", b"000123", UUID(_identity(1)).bytes, b"review-1"]
    ]
    with pytest.raises(ValueError, match="invalid_row"):
        encode(_wire([{**membership_by_field, "network_id": "42"}]))


def test_compiled_catalog_and_company_links_keep_typed_identity():
    catalog = _compiled_export("validate_network_catalog_batch")
    catalog_by_field = {
        "network_id": 42,
        "display_name": " Example Réseau ",
        "aliases": [],
        "source_aliases": [],
        "directory_available": True,
        "priceable": False,
    }
    validated_by_field = json.loads(catalog(_wire([catalog_by_field])))[0]
    assert (
        validated_by_field["network_id"],
        validated_by_field["display_name"],
        validated_by_field["raw_display_name"],
    ) == (
        42,
        "Example Réseau",
        catalog_by_field["display_name"],
    )
    assert validated_by_field["directory_available"] is True and validated_by_field["priceable"] is False
    with pytest.raises(ValueError, match="invalid_network_id"):
        catalog(_wire([{**catalog_by_field, "network_id": True}]))
    validate = _compiled_export("validate_company_network_assertions")
    assertion_by_field = {
        "company_id": _identity(2),
        "network_id": 42,
        "relationship_role": "uses",
        "benefit_domain": None,
        "applicability": "national",
        "states": [],
        "valid_from": None,
        "valid_to": None,
        "evidence_text": "Reviewed relationship",
    }
    validated_by_field = json.loads(validate(_wire([assertion_by_field])))
    assert validated_by_field["assertions"] == [assertion_by_field] and validated_by_field["conflicts"] == []
    selected_by_field = assertion_by_field | {"benefit_plan_ids": ["hpplan_" + "0" * 26]}
    assert json.loads(validate(_wire([selected_by_field])))["assertions"] == [selected_by_field]
    for invalid in (None, ["hpversion_" + "0" * 26], ["hpplan_" + "0" * 26] * 2):
        with pytest.raises(ValueError):
            validate(_wire([assertion_by_field | {"benefit_plan_ids": invalid}]))
    with pytest.raises(ValueError, match="invalid_row"):
        validate(_wire([{**assertion_by_field, "network_id": "42"}]))


def _source_binding():
    return {
        "binding_id": _identity(3),
        "source_system": "ptg",
        "source_id": "source-one",
        "dataset_schema": "source_snapshot",
        "dataset_id": "dataset-one",
        "producer_id": "producer-one",
        "edition_id": "edition-one",
        "source_key": "opaque-key",
        "source_scope_json": {"cohort_id": "cohort-one", "snapshot_id": "snapshot-one", "company_key": "company-one"},
        "network_id": 42,
        "evidence_id": "review-one",
        "evidence_sha256": "a" * 64,
        "operation": "bind",
        "expected_revision": 0,
        "expected_network_id": None,
    }


def test_compiled_source_binding_copy_keeps_exact_scope_and_revision():
    from process.network_source_binding_store import COPY_COLUMNS

    assert COPY_COLUMNS == (
        "binding_id",
        "source_system",
        "source_id",
        "dataset_schema",
        "dataset_id",
        "producer_id",
        "edition_id",
        "source_key",
        "source_scope_json",
        "binding_key",
        "network_id",
        "evidence_id",
        "evidence_sha256",
        "operation",
        "expected_revision",
        "expected_network_id",
        "office_recipe_json",
    )
    encode = _compiled_export("encode_network_source_binding_batch")
    binding_by_field = _source_binding()
    wire, count = encode(_wire([binding_by_field]))
    decoded_rows = _copy_fields(wire, len(COPY_COLUMNS))
    assert count == len(decoded_rows) == 1
    fields = decoded_rows[0]
    assert fields[0] == UUID(binding_by_field["binding_id"]).bytes
    assert fields[1:8] == [binding_by_field[name].encode() for name in COPY_COLUMNS[1:8]]
    assert fields[8][0] == 1 and json.loads(fields[8][1:]) == binding_by_field["source_scope_json"]
    coordinates_by_field = {name: binding_by_field[name] for name in COPY_COLUMNS[1:9]}
    coordinates = json.dumps(coordinates_by_field, sort_keys=True, separators=(",", ":")).encode()
    assert fields[9] == hashlib.sha256(b"registry_network_source_binding:v1:" + coordinates).hexdigest().encode()
    assert fields[10:] == [struct.pack(">i", 42), b"review-one", b"a" * 64, b"bind", struct.pack(">q", 0), None, None]
    with pytest.raises(ValueError, match="invalid_row"):
        encode(_wire([{**binding_by_field, "expected_revision": True}]))


def test_compiled_binding_office_states():
    from process.network_source_binding_store import COPY_COLUMNS

    encode = _compiled_export("encode_network_source_binding_batch")
    binding_by_field = _source_binding()
    binding_by_field["source_scope_json"] = {
        "review_type": "published_complete_snapshot_plan",
        "scope_id": _identity(4),
        "approval_sha256": "b" * 64,
        "snapshot_id": "snapshot-one",
        "plan_id": "plan-one",
        "plan_market_type": "group",
        "selection_mode": "complete_snapshot_source_set",
    }
    wire, count = encode(_wire([binding_by_field]))
    omitted = _copy_fields(wire, len(COPY_COLUMNS))[0]
    assert count == 1 and omitted[16] is None
    pin_by_field = {"capture_id": _identity(5), "recipe_sha256": "c" * 64}
    for selection in (None, pin_by_field):
        selected_by_field = binding_by_field | {"office_recipe_json": selection}
        wire, count = encode(_wire([selected_by_field]))
        fields = _copy_fields(wire, len(COPY_COLUMNS))[0]
        assert count == 1 and fields[:16] == omitted[:16]
        assert fields[16] == b"\x01" + json.dumps(selection, sort_keys=True, separators=(",", ":")).encode()
    with pytest.raises(ValueError, match="invalid_office_recipe"):
        encode(_wire([_source_binding() | {"office_recipe_json": pin_by_field}]))
    with pytest.raises(ValueError, match="invalid_row"):
        encode(_wire([binding_by_field | {"office_recipe_json": pin_by_field | {"extra": True}}]))


def test_compiled_network_evidence_and_coverage_preserve_unknown_pricing():
    parse = _compiled_export("parse_registry_network_evidence")
    evidence_by_field = {"network_id": 42, "expected_record_revision": 1, "pricing_refs": None, "benefit_refs": []}
    assert json.loads(parse(_wire(evidence_by_field))) == evidence_by_field
    with pytest.raises(ValueError, match="registry_network_evidence_invalid"):
        parse(_wire({**evidence_by_field, "expected_record_revision": 0}))
    coverage = _compiled_export("build_registry_network_coverage")
    request_by_field = {
        "targets": [{"target_key": "required:one"}],
        "bindings": [{"target_key": "required:one", "network_id": 42, "resolution_status": "resolved"}],
        "evidence": [
            {
                "network_id": 42,
                "company_link_verified": True,
                "exact_membership_count": 3,
                "pricing_evidence_count": None,
                "source_status": "current",
                "unresolved_location_count": 0,
            }
        ],
    }
    coverage_by_field = json.loads(coverage(_wire(request_by_field)))
    assert coverage_by_field["targets"][0]["directory_available"] is True
    assert coverage_by_field["targets"][0]["priceable"] is None
    assert coverage_by_field["totals"]["exact_membership_count"] == 3
    with pytest.raises(ValueError, match="registry_coverage_invalid"):
        coverage(_wire({**request_by_field, "targets": [{"target_key": "required:one"}] * 2}))


def test_compiled_fhir_network_projection_preserves_reference_order_and_copy():
    extract = _compiled_export("extract_fhir_network_identity_batch")
    encode = _compiled_export("encode_fhir_network_identity_batch")
    resource_by_field = {
        "resourceType": "InsurancePlan",
        "id": "plan-one",
        "network": [{"reference": "Organization/network-two"}, {"reference": "Organization/network-one"}],
    }
    request_by_field = {
        "source_id": "source-one",
        "release_id": "release-one",
        "resource_type": "InsurancePlan",
        "resources": [resource_by_field],
    }
    decoded_by_field = json.loads(extract(_wire(request_by_field)))
    assert decoded_by_field["plans"][0]["network_refs"] == ["Organization/network-two", "Organization/network-one"]
    wire, rows, inputs, duplicates = encode(_wire(request_by_field))
    assert (rows, inputs, duplicates) == (1, 1, 0)
    assert len(_copy_fields(wire, 5)) == 1
    for export in (extract, encode):
        with pytest.raises(ValueError, match="resource_type_mismatch"):
            export(_wire({**request_by_field, "resources": [{**resource_by_field, "resourceType": "Organization"}]}))


@pytest.fixture
def native_ledger_artifact():
    source = (
        b"COMPANY_ALIAS,COMPANY_NAME,PLAN_NAME,PLAN_TYPE,PLAN_CARRIER,FIND_CARE_NETWORK_NAME,NETWORK_FC_ID,RIBBON_IDS\r\n"
        b"Example,Company,Plan,PPO,Carrier,Network,123,[]\r\n"
    )
    ledger = json.loads(_compiled_export("parse_registry_target_ledger")(source))
    wire, descriptor_bytes = _compiled_export("encode_registry_target_ledger_artifact")(source, _identity(4))
    descriptor = json.loads(descriptor_bytes)
    fields = _copy_fields(wire, 6)[0]
    assert fields[:4] == [UUID(_identity(4)).bytes, b"ledger:v1", struct.pack(">i", 1), b"accepted"]
    assert fields[4][0] == 1 and fields[5] == b"\x01[]"
    document = json.loads(fields[4][1:])
    assert document["ledger"] == ledger
    assert descriptor["source_sha256"] == hashlib.sha256(source).hexdigest()
    assert descriptor["artifact_sha256"] == hashlib.sha256(fields[4][1:]).hexdigest()
    return {"snapshot_id": _identity(4), "artifact_sha256": descriptor["artifact_sha256"], "document": document}


def test_compiled_ledger_validation_rejects_body_substitution(native_ledger_artifact):
    validate = _compiled_export("validate_registry_required_target_ledger_artifact")
    result = json.loads(validate(_wire(native_ledger_artifact)))
    assert (result["source_rows"], result["target_count"], result["snapshot_id"]) == (1, 1, _identity(4))
    with pytest.raises(ValueError, match="registry_target_review_ledger_mismatch"):
        validate(_wire({**native_ledger_artifact, "artifact_sha256": "0" * 64}))


def test_compiled_review_artifact_roundtrip_binds_exact_retained_ledger(native_ledger_artifact):
    ledger = native_ledger_artifact
    request_by_field = {
        "ledger_snapshot_id": ledger["snapshot_id"],
        "ledger_artifact_sha256": ledger["artifact_sha256"],
        "decisions": [
            {
                "target_key": "fc:123:ribbon:missing",
                "resolution_status": "unresolved",
                "network_id": None,
                "source_binding": None,
                "evidence_reference": "Reviewed pending identity",
                "evidence_sha256": "a" * 64,
                "reason": "Awaiting evidence",
            }
        ],
    }
    encode = _compiled_export("encode_registry_required_target_review_artifact")
    wire, descriptor_bytes = encode(_wire(request_by_field), _wire(ledger["document"]), _identity(5))
    fields = _copy_fields(wire, 6)[0]
    assert fields[:4] == [UUID(_identity(5)).bytes, b"review:v1", struct.pack(">i", 1), b"accepted"]
    descriptor = json.loads(descriptor_bytes)
    assert descriptor["resolved_count"] == 0
    assert descriptor["artifact_sha256"] == hashlib.sha256(fields[4][1:]).hexdigest()
    retained_by_field = {
        "snapshot_id": _identity(5),
        "artifact_sha256": descriptor["artifact_sha256"],
        "document": json.loads(fields[4][1:]),
    }
    bundle_by_role = {"ledgers": [ledger], "reviews": [retained_by_field]}
    validate = _compiled_export("validate_registry_required_target_review_artifacts")
    validated_by_field = json.loads(validate(_wire(bundle_by_role)))
    assert (validated_by_field["ledger_count"], validated_by_field["review_count"]) == (1, 1)
    with pytest.raises(ValueError, match="registry_target_review_ledger_mismatch"):
        validate(_wire({**bundle_by_role, "ledgers": []}))
    with pytest.raises(ValueError, match="registry_target_review_ledger_mismatch"):
        encode(_wire({**request_by_field, "ledger_artifact_sha256": "0" * 64}), _wire(ledger["document"]), _identity(5))


def test_compiled_source_editions_preserve_copy_metadata_and_reject_digest_changes(tmp_path):
    from tests.test_cms_planfinder_workbook_input import _batches, _workbook
    from tests.test_registry_source_admission_postgres import _input

    mlr = _input()
    path, workbook_digest = _workbook(tmp_path, count=1)
    for name, source, digest, count in (
        ("encode_cms_mlr_observations", mlr, hashlib.sha256(mlr).hexdigest(), 2),
        ("encode_cms_planfinder_observations", _batches(path, workbook_digest)[0], workbook_digest, 1),
    ):
        encode = _compiled_export(name)
        edition_by_field = {"snapshot_id": _identity(6), "reporting_year": 2024, "input_sha256": digest}
        wire, rows, metadata_bytes = encode(source, _wire(edition_by_field))
        fields = _copy_fields(wire, 6)
        metadata = json.loads(metadata_bytes)
        assert rows == len(fields) == metadata["counts"]["input_rows"] == count
        assert metadata["edition"] == edition_by_field
        assert all(row[0] == UUID(_identity(6)).bytes and row[4][0] == 1 for row in fields)
        with pytest.raises(ValueError, match="digest_mismatch"):
            encode(source, _wire({**edition_by_field, "input_sha256": "0" * 64}))
        with pytest.raises(ValueError, match="edition metadata is invalid"):
            encode(source, b"{}")
        with pytest.raises(ValueError, match="edition metadata exceeds byte limit"):
            encode(source, b" " * 65_537)


def test_compiled_source_payload_failure_keeps_original_row_ordinal():
    validate = _compiled_export("custom_import_validate_source_payloads_v1")
    layout = (("key", "string", False),)
    good = '{"contract":"custom-import-record/v1","fields":[{"field":"key","value":{"state":"value","type":"string","value":"a"}}]}'
    assert validate(layout, [good]) is None
    rejected = validate(layout, [good, "{}"])
    assert rejected[0] == 1 and isinstance(rejected[1], str) and rejected[1]


@pytest.mark.parametrize(
    "binding,message",
    [((False, 32768, 1), "graph field slot differs"), ((False, 1, 32768), "graph projection slot differs")],
)
def test_compiled_graph_binding_slots_reject_integer_narrowing(binding, message):
    decode = _compiled_export("custom_import_graph_children_v1")
    source = (True, (("key", "string"),), (("parent", 0),), (0,))
    with pytest.raises(ValueError, match=message):
        decode((source, (binding,)), ["{}"])


def _naic_member_fixture(member="HLTH.csv"):
    headers_by_member = {
        "HLTH.csv": [
            "COMPANY NAME",
            "STATE OF DOMICILE",
            "GROUP CODE",
            "COMPANY CODE",
            "FEIN NUMBER",
            "RJIN FLAG",
            "CRIN ",
            "RJIN",
            "COMPANY STATUS",
            "ADDRESS",
            "CITY",
            "STATE",
            "ZIP ",
            "PHONE",
        ],
        "GPAL.csv": ["GROUP NAME", "GROUP CODE"],
        "GPNM.csv": [
            "GROUP NAME",
            "GROUP CODE",
            "COMPANY CODE",
            "FEIN NUMBER",
            "COMPANY STATUS",
            "STATE OF DOMICILE",
            "COMPANY NAME",
        ],
    }
    fields_by_header = {
        "COMPANY NAME": b"Synthetic Company",
        "GROUP NAME": b"Synthetic Group",
        "COMPANY CODE": b"707",
        "GROUP CODE": b"00707",
        "FEIN NUMBER": b"01-2345678",
        "COMPANY STATUS": b"1",
        "STATE OF DOMICILE": b"CA",
        "ADDRESS": b'"Street, \xa0\xd3\0"',
    }
    headers = headers_by_member[member]
    source_record = b",".join(fields_by_header.get(header, b"") for header in headers)
    member_bytes = b",".join(header.encode("ascii") for header in headers) + b"\r\n" + source_record + b"\r\n"
    edition_by_field = {
        "snapshot_id": _identity(6),
        "edition_id": "2026-synthetic-edition",
        "reporting_year": 2026,
        "artifact_sha256": "a" * 64,
        "member": member,
        "member_sha256": hashlib.sha256(member_bytes).hexdigest(),
        "layout": f"naic-cl-{member.removesuffix('.csv').lower()}-2026-v1",
        "layout_pdf_sha256": "b" * 64,
        "header_sha256": hashlib.sha256(_wire(headers)).hexdigest(),
        "parser_version": "naic-company-listings-byte-projection-v1",
    }
    return member_bytes, edition_by_field, source_record


@pytest.mark.parametrize("member", ["HLTH.csv", "GPAL.csv", "GPNM.csv"])
def test_compiled_naic_preserves_member_evidence(member):
    parse = _compiled_export("parse_naic_company_registry_member")
    member_bytes, edition_by_field, source_record = _naic_member_fixture(member)
    encoded = parse(member_bytes, _wire(edition_by_field))
    assert type(encoded) is bytes
    batch = json.loads(encoded)
    assert batch["edition"] == edition_by_field
    assert batch["counts"]["input_rows"] == len(batch["observations"]) == 1
    observation = batch["observations"][0]
    assert observation["source_record_key"] == f"{member}:row:2"
    assert observation["normalized_naic_group"] == "707"
    assert observation["normalized_naic_company"] == (None if member == "GPAL.csv" else "00707")
    assert observation["valid_from"] is None and observation["valid_to"] is None
    evidence = observation["source_evidence"]
    assert member_bytes[evidence["source_byte_start"] : evidence["source_byte_end"]] == source_record
    assert evidence["raw_record_sha256"] == hashlib.sha256(source_record).hexdigest()
    for field in ("member_sha256", "artifact_sha256", "layout", "layout_pdf_sha256", "header_sha256", "parser_version"):
        assert evidence[field] == edition_by_field[field]
    if member == "HLTH.csv":
        assert "ADDRESS" not in observation["raw_fields"]
        assert bytes.fromhex(evidence["raw_fields_hex"]["ADDRESS"]) == b"Street, \xa0\xd3\0"
    assert observation["status"] == ("accepted" if member == "GPAL.csv" else "unresolved")


def test_compiled_naic_rejects_invalid_metadata():
    parse = _compiled_export("parse_naic_company_registry_member")
    member_bytes, edition_by_field, _ = _naic_member_fixture()
    for edition_bytes in (b"{}", _wire({**edition_by_field, "unknown": True}), b"\xff"):
        with pytest.raises(ValueError, match="edition metadata is invalid"):
            parse(member_bytes, edition_bytes)
    with pytest.raises(ValueError, match="edition metadata exceeds byte limit"):
        parse(member_bytes, b" " * 65_537)
    for field, code in (("member_sha256", "digest_mismatch"), ("header_sha256", "header_digest_mismatch")):
        with pytest.raises(ValueError) as rejected:
            parse(member_bytes, _wire({**edition_by_field, field: "0" * 64}))
        assert json.loads(str(rejected.value))["code"] == code
    with pytest.raises(ValueError, match="input_limit"):
        parse(b"x" * (8 * 1024 * 1024 + 1), _wire(edition_by_field))
    invalid_csv = member_bytes.split(b"\r\n", 1)[0] + b'\r\n"unterminated'
    with pytest.raises(ValueError, match="invalid_csv"):
        parse(invalid_csv, _wire({**edition_by_field, "member_sha256": hashlib.sha256(invalid_csv).hexdigest()}))


def test_compiled_naic_refuses_output_expansion():
    parse = _compiled_export("parse_naic_company_registry_member")
    member_bytes, edition_by_field, _ = _naic_member_fixture()
    header = member_bytes.split(b"\r\n", 1)[0]
    projected_by_header = {
        "COMPANY NAME": b"Synthetic Company",
        "STATE OF DOMICILE": b"CA",
        "GROUP CODE": b"00707",
        "COMPANY CODE": b"707",
        "FEIN NUMBER": b"01-2345678",
        "COMPANY STATUS": b"1",
    }
    source_record = b",".join(
        projected_by_header.get(field, b"x" * 4096) for field in header.decode("ascii").split(",")
    )
    single_member = header + b"\r\n" + source_record + b"\r\n"
    single_edition_by_field = {**edition_by_field, "member_sha256": hashlib.sha256(single_member).hexdigest()}
    observation = json.loads(parse(single_member, _wire(single_edition_by_field)))["observations"][0]
    assert observation["status"] == "unresolved"
    assert len(_wire(observation)) * 255 > 16 * 1024 * 1024
    expanded_member = header + b"\r\n" + (source_record + b"\r\n") * 255
    assert len(expanded_member) <= 8 * 1024 * 1024
    expanded_edition_by_field = {**edition_by_field, "member_sha256": hashlib.sha256(expanded_member).hexdigest()}
    with pytest.raises(ValueError, match="Company listing output exceeds byte limit"):
        parse(expanded_member, _wire(expanded_edition_by_field))


def test_compiled_naic_rejects_projection_bytes():
    parse = _compiled_export("parse_naic_company_registry_member")
    member_bytes, edition_by_field, _ = _naic_member_fixture()
    member_bytes = member_bytes.replace(b"Synthetic Company", b"Synthetic \xd3 Company")
    edition_by_field["member_sha256"] = hashlib.sha256(member_bytes).hexdigest()
    observation = json.loads(parse(member_bytes, _wire(edition_by_field)))["observations"][0]
    assert observation["status"] == "rejected"
    assert "COMPANY NAME" not in observation["raw_fields"]
    assert bytes.fromhex(observation["source_evidence"]["raw_fields_hex"]["COMPANY NAME"]) == b"Synthetic \xd3 Company"
    assert any(issue["code"] == "invalid_ascii_projection" for issue in observation["issues"])


def _review_assertion(record_key="record-1", object_id=10, start="2024-01-01", end=None):
    return {
        "subject_id": _identity(100),
        "predicate": "reported_affiliation",
        "scope": {"system": "group_kind", "value": "corporate_parent"},
        "object_identifier": {"system": "group_uuid", "value": _identity(object_id)},
        "valid_from": start,
        "valid_to": end,
        "evidence": {
            "source_edition": {
                "source_system": "synthetic",
                "source_id": "example-source",
                "edition_id": "example-edition",
                "snapshot_id": _identity(200),
            },
            "record_key": record_key,
            "original_identifier": {"system": "external_reference", "value": "source-17"},
            "document_sha256": "a" * 64,
        },
        "source_label": "Synthetic source label",
    }


def test_compiled_assertions_preserve_raw_evidence():
    classify = _compiled_export("classify_registry_assertions")
    assertion_by_field = _review_assertion()
    raw_assertion = json.dumps(assertion_by_field, indent=2).encode()
    compact_assertion = _wire(assertion_by_field)
    input_bytes = b"[" + raw_assertion + b"," + compact_assertion + b"]"
    encoded = classify(input_bytes, "group_inclusive")
    assert type(encoded) is bytes
    assert raw_assertion in encoded and compact_assertion in encoded
    batch = json.loads(encoded)
    assert batch["policy"] == "group_inclusive"
    assert batch["counts"] == {"input_rows": 2, "accepted_rows": 1, "duplicate_rows": 1, "review_required_rows": 0}
    assert [entry["assertion"] for entry in batch["assertions"]] == [assertion_by_field] * 2
    assert batch["assertions"][1]["duplicate_of"] == 0
    assert encoded == classify(b"[" + compact_assertion + b"," + raw_assertion + b"]", "group_inclusive")
    independent = _review_assertion("record-2")
    independent["evidence"]["source_edition"]["source_id"] = "independent-source"
    batch = json.loads(classify(_wire([assertion_by_field, independent]), "group_inclusive"))
    assert batch["counts"]["accepted_rows"] == 2 and batch["reviews"] == []
    contradictory = _review_assertion()
    contradictory["evidence"]["document_sha256"] = "b" * 64
    batch = json.loads(classify(_wire([assertion_by_field, contradictory]), "group_inclusive"))
    assert batch["counts"]["review_required_rows"] == 2
    assert [review["code"] for review in batch["reviews"]] == ["evidence_digest_conflict"]


def test_compiled_assertions_select_interval_policy():
    classify = _compiled_export("classify_registry_assertions")
    first = _review_assertion(end="2024-02-01")
    second = _review_assertion("record-2", 11, "2024-02-01", "2024-03-01")
    group = json.loads(classify(_wire([first, second]), "group_inclusive"))
    assert group["counts"]["review_required_rows"] == 2
    assert group["reviews"][0]["code"] == "contradictory_overlapping_objects"
    second["object_identifier"] = first["object_identifier"]
    network = json.loads(classify(_wire([first, second]), "network_half_open"))
    assert network["counts"]["accepted_rows"] == 2 and network["reviews"] == []
    unknown = _review_assertion(start=None)
    batch = json.loads(classify(_wire([unknown]), "group_inclusive"))
    assert batch["assertions"][0]["assertion"]["valid_from"] is None
    assert batch["reviews"][0]["code"] == "missing_effective_start"


def test_compiled_assertions_reject_invalid_input():
    classify = _compiled_export("classify_registry_assertions")
    for policy in ("", "GroupInclusive", "group_inclusive ", "unknown"):
        with pytest.raises(ValueError, match="registry_assertion_policy_invalid"):
            classify(b"[]", policy)
    for input_bytes, code in (
        (b"\xff", "invalid_json"),
        (b"[] trailing", "invalid_json"),
        (_wire([{**_review_assertion(), "unknown": True}]), "invalid_row"),
        (_wire({"rows": [_review_assertion()]}), "invalid_json"),
        (b" " * (8 * 1024 * 1024 + 1), "input_limit"),
    ):
        with pytest.raises(ValueError) as rejected:
            classify(input_bytes, "group_inclusive")
        assert json.loads(str(rejected.value))["code"] == code
    duplicate_key = _wire([_review_assertion()]).replace(b'"subject_id":', b'"subject_id":"bad","subject_id":', 1)
    with pytest.raises(ValueError, match="invalid_row"):
        classify(duplicate_key, "group_inclusive")
    with pytest.raises(TypeError):
        classify(_wire([]))


def test_compiled_membership_command_partition_matches_facade_and_legacy():
    from process.network_membership_copy import partition_network_membership_commands

    partition = _compiled_export("partition_network_membership_commands")
    encode = _compiled_export("encode_network_membership_batch")
    addition_by_field = {
        "network_id": 42,
        "provider_system": "manual",
        "provider_id": "000123",
        "location_id": _identity(1),
        "evidence_id": "review-1",
    }
    removal_by_field = {**addition_by_field, "location_id": _identity(2), "operation": "remove"}
    payload = _wire([addition_by_field, removal_by_field])
    adds, add_count, removes, remove_count = partition(payload)
    assert add_count == remove_count == 1
    assert (adds, add_count) == encode(_wire([addition_by_field]))
    assert _copy_fields(removes, 5)[0][3] == UUID(_identity(2)).bytes
    receipt = partition_network_membership_commands(payload)
    assert (receipt.additions_copy_bytes, receipt.additions_count) == (adds, add_count)
    assert (receipt.removals_copy_bytes, receipt.removals_count) == (removes, remove_count)
    with pytest.raises(ValueError, match="invalid_row"):
        encode(payload)
    with pytest.raises(ValueError, match="duplicate_command"):
        partition_network_membership_commands(_wire([addition_by_field, {**addition_by_field, "operation": "remove"}]))
    empty = partition_network_membership_commands(b"[]")
    assert empty.additions_count == empty.removals_count == 0
    assert len(empty.additions_copy_bytes) == len(empty.removals_copy_bytes) == 21


@pytest.mark.parametrize(
    "native_result",
    [None, (), (b"bad", 1, b"bad", 0), (b"", True, b"", 0), (b"", -1, b"", 0)],
)
def test_membership_command_facade_rejects_invalid_native_shape(monkeypatch, native_result):
    from process.network_membership_copy import MembershipCopyError, partition_network_membership_commands

    monkeypatch.setattr(ptg2_address_canon, "partition_network_membership_commands", lambda _input: native_result)
    with pytest.raises(MembershipCopyError, match="shape|framing"):
        partition_network_membership_commands(b"[]")


def test_membership_command_facade_checks_bounds_before_native_call(monkeypatch):
    from process.network_membership_copy import (
        MAX_INPUT_BYTES,
        MembershipCopyError,
        partition_network_membership_commands,
    )

    def unexpected(_input):
        pytest.fail("invalid input must not reach native partitioner")

    monkeypatch.setattr(ptg2_address_canon, "partition_network_membership_commands", unexpected)
    for invalid in ["[]", bytearray(b"[]"), b" " * (MAX_INPUT_BYTES + 1)]:
        with pytest.raises(MembershipCopyError, match="bounded bytes"):
            partition_network_membership_commands(invalid)


def test_membership_command_facade_checks_combined_native_totals(monkeypatch):
    from process.network_membership_copy import (
        MAX_COPY_BYTES,
        MembershipCopyError,
        partition_network_membership_commands,
    )

    encode = _compiled_export("encode_network_membership_batch")
    command_by_field = {
        "network_id": 42,
        "provider_system": "manual",
        "provider_id": "p-1",
        "location_id": _identity(1),
        "evidence_id": "review-1",
    }
    wire, _ = encode(_wire([command_by_field]))
    oversized = wire[:-2] + b"x" * (MAX_COPY_BYTES - len(wire)) + wire[-2:]
    for native_result in [(wire, 2500, wire, 2501), (oversized, 1, wire, 1)]:
        monkeypatch.setattr(ptg2_address_canon, "partition_network_membership_commands", lambda _input: native_result)
        with pytest.raises(MembershipCopyError, match="total bounds"):
            partition_network_membership_commands(b"[]")


def test_compiled_membership_partition_keeps_legacy_duplicate_bytes_and_late_operation_fence():
    from process.network_membership_copy import partition_network_membership_commands

    partition = _compiled_export("partition_network_membership_commands")
    encode = _compiled_export("encode_network_membership_batch")
    legacy_by_field = {
        "network_id": 42,
        "provider_system": "manual",
        "provider_id": "000123",
        "location_id": _identity(1),
        "evidence_id": "review-1",
    }
    rows = [legacy_by_field, legacy_by_field, {**legacy_by_field, "evidence_id": "review-2"}]
    payload = _wire(rows)
    adds, add_count, removes, remove_count = partition(payload)
    assert (adds, add_count) == encode(payload)
    assert (add_count, remove_count, len(removes)) == (3, 0, 21)
    receipt = partition_network_membership_commands(payload)
    assert receipt.additions_copy_bytes == adds and receipt.additions_count == 3
    for operation in ("add", "remove"):
        command_by_field = {**legacy_by_field, "network_id": 43, "operation": operation}
        with pytest.raises(ValueError) as failure:
            partition_network_membership_commands(_wire([*rows, command_by_field]))
        assert json.loads(str(failure.value))["code"] == "duplicate_command"
        assert json.loads(str(failure.value))["row_index"] == 1
    for operation in (None, "legacy", "replace"):
        with pytest.raises(ValueError, match="invalid_row"):
            partition_network_membership_commands(_wire([{**legacy_by_field, "operation": operation}]))


@pytest.mark.asyncio
async def test_actual_native_aca_batch_runs_once_during_loading(monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock, Mock

    from process import registry_aca_address_carry as carry
    from process.registry_aca_source_recipe import _recipe
    from tests.test_registry_aca_address_carry import _batch
    from tests.test_registry_aca_source_recipe import _ROLES, _prepared

    prepared, validation = await _prepared(monkeypatch)
    recipe = _recipe(prepared, validation, _ROLES)
    native = ptg2_address_canon

    raw_address = ["123 Example Street", "Suite 2", "Sample City", "CA", "90210", "US"]
    office_records = [
        {
            "provider_id": "1000000491",
            "location_id": str(UUID(int=8)),
            "location_key": "c" * 64,
            "evidence_address": raw_address,
            "address": raw_address,
        }
    ]
    binding = ("npi", "1000000491", str(UUID(int=8)), "c" * 64, "npi", "1000000491")
    encoded = json.dumps([dict(zip(carry._BINDING_COLUMNS, binding, strict=True))])
    connection = SimpleNamespace(
        execute=AsyncMock(),
        fetchval=AsyncMock(return_value=False),
        fetchrow=AsyncMock(return_value={"valid": True, "bindings_json": encoded}),
    )
    monkeypatch.setattr(carry, "_source_documents", AsyncMock(return_value=[]))
    monkeypatch.setattr(carry, "_office_records", AsyncMock(return_value=office_records))
    canonicalize = Mock(wraps=native.canonicalize_batch)
    monkeypatch.setattr(native, "canonicalize_batch", canonicalize)
    written = await carry._page(connection, _batch(recipe), "target_fixture", writing=True)
    canonicalize.assert_called_once_with([tuple(raw_address), tuple(raw_address)])
    assert connection.fetchrow.await_count == 2
    canonicalize.reset_mock()
    connection.fetchrow.reset_mock()
    connection.execute.reset_mock()
    assert await carry._page(connection, _batch(recipe), "target_fixture", writing=False) == written
    canonicalize.assert_not_called()
    connection.execute.assert_not_awaited()
    assert connection.fetchrow.await_count == 1
