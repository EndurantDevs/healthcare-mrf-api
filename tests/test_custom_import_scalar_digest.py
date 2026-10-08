# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exact scalar framing and bounded native-adapter lifecycle checks."""

import asyncio
import datetime as dt
import sys
from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from db.models.custom_import import CustomImportChildScalar, CustomImportRootScalar
from process.custom_import import build_graph as graph
from process.custom_import import build_output as output
from process.custom_import import publication, scalar_digest
from process.custom_import.runner_codec import record_payload
from process.custom_import.runner_types import CancellationRequested, CandidateRunnerError, LeaseAuthorityLost
from tests.test_custom_import_build_graph import _registry
from tests.test_custom_import_build_output import _generation
from tests.test_custom_import_output_bulk_verification import (
    _digests,
    _projection_records,
    _projection_responses,
    _read_session,
    _twenty_projection_family,
)


class _Frames:
    def __init__(self):
        self.parts = []

    def update(self, encoded):
        self.parts.append(encoded)


def _reference_frames(projection_records, *, child):
    digest = _Frames()
    for scalar, revision, root_key in projection_records:
        document_by_field = {
            "root_key_sha256": bytes(root_key).hex(),
            "scalar": publication._materialization_document(scalar),
        }
        if child:
            document_by_field["child_key_sha256"] = bytes(revision.child_key_sha256).hex()
        publication._add_digest_record(digest, "child_scalar" if child else "root_scalar", document_by_field)
    return b"".join(digest.parts)


@pytest.fixture
def native_encoder():
    module = pytest.importorskip("ptg2_address_canon")
    encoder = getattr(module, "custom_import_scalar_frames_v1", None)
    if encoder is None:
        pytest.skip("native module does not include v1 scalar framing")
    return encoder


def _typed_records(*, child):
    model = CustomImportChildScalar if child else CustomImportRootScalar
    scalar_values = [
        ("string", 'quotes" slash\\ control\n\t\b\f\r\x01 / café 漢字 😀 \u2028\u2029'),
        ("string", "\x01" * 2_048),
        ("integer", -(2**63)),
        ("integer", 2**63 - 1),
        ("decimal", Decimal("12.000000000000")),
        ("decimal", Decimal("-0.000000000000")),
        ("boolean", False),
        ("boolean", True),
        ("date", dt.date(1, 1, 1)),
        ("timestamp", dt.datetime(2025, 1, 2, 3, 4, 5, 123456, dt.timezone(dt.timedelta(hours=5, minutes=30)))),
        *((field_type, None) for field_type in ("string", "integer", "decimal", "boolean", "date", "timestamp")),
    ]
    projection_records = []
    for index, (field_type, typed_value) in enumerate(scalar_values, 1):
        scalar = model(
            dataset_id=1,
            schema_revision_id=2,
            root_record_id=3,
            field_slot=index,
            field_collection_slot=int(child),
            projection_slot=index,
            field_type=field_type,
            value_state="null" if typed_value is None else "value",
            **{field_type + "_value": typed_value},
            **({"child_revision_id": index, "collection_slot": 1} if child else {"root_revision_id": index}),
        )
        projection_records.append((scalar, SimpleNamespace(child_key_sha256=bytes([index]) * 32), b"r" * 32))
    return projection_records


@pytest.mark.parametrize("child", [False, True])
def test_native_frames_match_python_bytes_for_all_scalar_types(native_encoder, child):
    projection_records = _typed_records(child=child)
    prepared_rows = [scalar_digest._scalar_tuple(*record, child=child) for record in projection_records]
    assert native_encoder(child, prepared_rows) == _reference_frames(projection_records, child=child)
    assert native_encoder(child, []) == b""
    with pytest.raises(ValueError, match="row bound"):
        native_encoder(child, prepared_rows[:1] * (scalar_digest.BATCH_ROWS + 1))
    with pytest.raises(ValueError, match="kind"):
        native_encoder(not child, prepared_rows)


@pytest.mark.parametrize("corruption", ["key", "text", "utf8", "integer_bool", "surrogate", "tuple"])
def test_native_input_bounds_reject_before_owned_text_extraction(native_encoder, corruption):
    prepared = scalar_digest._scalar_tuple(*_typed_records(child=False)[0], child=False)
    root_key, child_key, binding, values = prepared
    if corruption == "key":
        root_key = "a" * 65
    elif corruption == "integer_bool":
        values = (None, True, *values[2:])
    elif corruption == "tuple":
        binding = binding[:-1]
    else:
        text_by_corruption = {"text": "a" * 2_049, "utf8": "😀" * 513, "surrogate": "\ud800"}
        values = (text_by_corruption[corruption], *values[1:])
    with pytest.raises((ValueError, TypeError)):
        native_encoder(False, [(root_key, child_key, binding, values)])


def test_native_text_bound_does_not_call_subclass_length(native_encoder):
    class OversizedText(str):
        def __len__(self):
            raise AssertionError("custom length must not be called")

    root_key, child_key, binding, values = scalar_digest._scalar_tuple(*_typed_records(child=False)[0], child=False)
    with pytest.raises(ValueError, match="native string"):
        native_encoder(False, [(root_key, child_key, binding, (OversizedText("a" * 2_049), *values[1:]))])


@pytest.mark.parametrize("child", [False, True])
def test_native_batches_preserve_digests_across_physical_pages(monkeypatch, native_encoder, child):
    monkeypatch.setattr(scalar_digest, "native_verifier", lambda: None)
    request, family = _twenty_projection_family(child=child)
    projection_records = _projection_records(request, [family], child=child)
    expected_frames = _reference_frames(projection_records, child=child)
    with monkeypatch.context() as fixture_patch:
        fixture_patch.setattr(graph, "MAX_BATCH_ROWS", 17)
        responses = _projection_responses(request, projection_records, child=child)
    monkeypatch.setattr(graph, "MAX_BATCH_ROWS", 23)
    monkeypatch.setattr(output, "MAX_BATCH_ROWS", 23)
    monkeypatch.setattr(scalar_digest, "BATCH_ROWS", 3)
    session = _read_session(monkeypatch, responses)
    batch_sizes = []

    def tracked_encoder(section, prepared_rows):
        batch_sizes.append(len(prepared_rows))
        return native_encoder(section, prepared_rows)

    monkeypatch.setattr(scalar_digest, "native_encoder", lambda: tracked_encoder)
    monkeypatch.setattr(publication, "_materialization_document", Mock(side_effect=AssertionError("Python document")))
    monkeypatch.setattr(publication, "_add_digest_record", Mock(side_effect=AssertionError("Python serialization")))
    monkeypatch.setattr(publication, "_add_digest_record_to_all", Mock(side_effect=AssertionError("Python fanout")))
    digests, expected = _digests(), _digests()
    for digest in expected:
        digest.update(expected_frames)
    assert (
        output._scalar_material(
            session, request, _registry(request.definition), 7, _generation(request), digests, child=child
        )
        == 20
    )
    assert [digest.digest() for digest in digests] == [digest.digest() for digest in expected]
    assert batch_sizes == [3] * 6 + [2]
    assert session.execute.call_count == 7
    assert session.transactions == session.closed


@pytest.mark.parametrize(
    "failure",
    [
        "native_value",
        "native_type",
        "native_overflow",
        "before_deadline",
        "after_deadline",
        "oversized",
        "wrong_type",
        "end_count",
    ],
)
def test_native_failure_closes_upstream_without_fallback(monkeypatch, failure):
    closed_flags = []
    projection_records = _typed_records(child=False)

    def stream():
        try:
            yield projection_records[0]
            if failure == "end_count":
                raise CandidateRunnerError("missing final scalar")
            yield projection_records[1]
        finally:
            closed_flags.append(True)

    monkeypatch.setattr(scalar_digest, "BATCH_ROWS", 1 if failure != "end_count" else 3)
    error_type = {"native_value": ValueError, "native_type": TypeError, "native_overflow": OverflowError}.get(failure)
    encoder = Mock(
        side_effect=error_type("synthetic private batch detail") if error_type else None,
        return_value=b"x" * (scalar_digest.MAX_FRAME_BYTES + 1) if failure == "oversized" else b"encoded",
    )
    if failure == "wrong_type":
        encoder.return_value = "encoded"
    check_budget = Mock(
        side_effect=([None] if failure == "after_deadline" else []) + [LeaseAuthorityLost("deadline")]
        if failure.endswith("deadline")
        else None
    )
    digests = _digests()
    expected_error = LeaseAuthorityLost if failure.endswith("deadline") else CandidateRunnerError
    with pytest.raises(expected_error) as caught:
        scalar_digest.scalar_material(encoder, stream(), digests, child=False, check_budget=check_budget)
    if error_type:
        assert str(caught.value) == "native scalar digest validation failed"
        assert caught.value.__cause__ is None and caught.value.__suppress_context__
    assert closed_flags == [True]
    assert [digest.digest() for digest in digests] == [digest.digest() for digest in _digests()]
    assert encoder.call_count == int(failure not in {"before_deadline", "end_count"})


@pytest.mark.parametrize("limit", ["rows", "bytes"])
def test_insufficient_native_buffer_budget_uses_python(monkeypatch, limit):
    monkeypatch.setattr(scalar_digest, "native_verifier", lambda: None)
    request, family = _twenty_projection_family(child=False)
    projection_records = _projection_records(request, [family], child=False)
    session = _read_session(monkeypatch, _projection_responses(request, projection_records, child=False))
    encoder = Mock(side_effect=AssertionError("unreserved native buffer"))
    add_record = Mock(wraps=publication._add_digest_record_to_all)
    monkeypatch.setattr(publication, "_add_digest_record_to_all", add_record)
    monkeypatch.setattr(scalar_digest, "native_encoder", lambda: encoder)
    if limit == "rows":
        monkeypatch.setattr(output, "MAX_BATCH_ROWS", 2 * scalar_digest.BATCH_ROWS + 1)
    else:
        monkeypatch.setattr(output, "MAX_BATCH_BYTES", scalar_digest.RESERVE_BYTES)
    assert (
        output._scalar_material(
            session, request, _registry(request.definition), 7, _generation(request), _digests(), child=False
        )
        == 20
    )
    encoder.assert_not_called()
    assert add_record.call_count == 20
    assert all(len(call.args[0]) == 2 for call in add_record.call_args_list)


def test_native_capability_absence_and_column_drift_select_python(monkeypatch):
    importer = Mock(return_value=SimpleNamespace())
    monkeypatch.setattr(scalar_digest.importlib, "import_module", importer)
    assert scalar_digest.native_encoder.__wrapped__() is None
    importer.side_effect = ImportError("optional extension missing")
    assert scalar_digest.native_encoder.__wrapped__() is None
    importer.side_effect = None
    importer.return_value = SimpleNamespace(custom_import_scalar_frames_v1=lambda *_args: b"")
    assert scalar_digest.native_encoder.__wrapped__() is importer.return_value.custom_import_scalar_frames_v1
    monkeypatch.setattr(scalar_digest, "SCALAR_COLUMNS", scalar_digest.SCALAR_COLUMNS - {"value_state"})
    importer.reset_mock()
    assert scalar_digest.native_encoder.__wrapped__() is None
    importer.assert_not_called()


def test_scalar_encoding_schema_matches_retained_model_columns():
    omitted = publication._MATERIALIZATION_IDENTITY_COLUMNS | publication._MATERIALIZATION_VOLATILE_COLUMNS
    assert set(CustomImportRootScalar.__table__.columns.keys()) - omitted == scalar_digest.SCALAR_COLUMNS
    assert set(CustomImportChildScalar.__table__.columns.keys()) - omitted == scalar_digest.SCALAR_COLUMNS | {
        "collection_slot"
    }
    assert scalar_digest.RESERVE_BYTES >= 4 * scalar_digest.BATCH_ROWS * scalar_digest.MAX_FRAME_BYTES


@pytest.fixture
def native_verifier():
    module = pytest.importorskip("ptg2_address_canon")
    verifier = getattr(module, "custom_import_verified_scalar_frames_v1", None)
    assert callable(verifier), "installed native wheel must include scalar verification"
    return verifier


def _verification_fixture(*, child):
    records = _typed_records(child=child)
    fields, expected, rows = [], [], []
    for scalar, revision, _key in records:
        setattr(scalar, "child_revision_id" if child else "root_revision_id", 7)
        revision.child_key_sha256 = b"c" * 32
        value = getattr(scalar, scalar.field_type + "_value")
        if isinstance(value, Decimal):
            value = Decimal("12") if value else Decimal("0")
        elif isinstance(value, dt.datetime):
            value = value.astimezone(dt.UTC)
        fields.append((scalar.field_slot, scalar.projection_slot, scalar.field_type, value is None))
        expected.append((scalar.value_state, value))
        rows.append(scalar_digest._verification_row(scalar, child=child))
    group = ((3, 7, int(child)), ((b"r" * 32).hex(), (b"c" * 32).hex() if child else None), tuple(expected), rows)
    return [(int(child), tuple(fields))], [group], records


@pytest.mark.parametrize("child", [False, True])
def test_verified_native_keeps_typed_equality_and_actual_frame_spelling(native_verifier, child):
    layouts, groups, records = _verification_fixture(child=child)
    actual = native_verifier(child, (1, 2), layouts, groups)
    assert actual == _reference_frames(records, child=child)
    assert b"-0.000000000000" in actual and b"12.000000000000" in actual
    assert native_verifier(child, (1, 2), layouts, []) == b""


@pytest.mark.parametrize(
    "decimal_text",
    ["999999999999999999.999999999999", "-999999999999999999.999999999999", "0.000000000001", "0.125"],
)
def test_verified_decimal_storage_edges_keep_numeric_and_frame_parity(native_verifier, decimal_text):
    layouts, groups, records = _verification_fixture(child=False)
    value = Decimal(decimal_text)
    records[4][0].decimal_value = value
    target, keys, expected, rows = groups[0]
    rows[4] = scalar_digest._verification_row(records[4][0], child=False)
    expected = (*expected[:4], ("value", value), *expected[5:])
    assert native_verifier(False, (1, 2), layouts, [(target, keys, expected, rows)]) == _reference_frames(
        records, child=False
    )


@pytest.mark.parametrize("empty", [False, True])
def test_verified_material_rolls_over_only_complete_revisions(native_verifier, empty):
    layouts, groups, records = _verification_fixture(child=False)
    group = groups[0]
    count, limit = 9, 8
    if empty:
        layouts, group = [(0, ())], (*group[:2], (), [])
        count, limit = 17, 16
    batch_sizes = []

    def verifier(*args):
        batch_sizes.append(len(args[3]))
        return native_verifier(*args)

    digests, reference = _digests(), _digests()
    for digest in reference:
        digest.update(b"" if empty else _reference_frames(records, child=False) * count)
    assert scalar_digest.verified_material(
        verifier, (group for _ in range(count)), (1, 2), layouts, digests, child=False, check_budget=lambda: None
    ) == (0 if empty else 16 * count)
    assert [digest.digest() for digest in digests] == [digest.digest() for digest in reference]
    assert batch_sizes == [limit, 1]


@pytest.mark.parametrize("child", [False, True])
@pytest.mark.parametrize("corruption", ["type", "value"])
def test_verified_native_compares_every_actual_column(native_verifier, child, corruption):
    layouts, groups, records = _verification_fixture(child=child)
    scalar = records[0][0]
    for column in scalar.__table__.columns:
        original = getattr(scalar, column.name)
        replacement = object()
        if corruption == "value":
            replacement = original + 1 if type(original) is int else str(original) + "changed"
            if original is None:
                replacement = {
                    "string_value": "changed",
                    "integer_value": 1,
                    "decimal_value": Decimal("1"),
                    "boolean_value": False,
                    "date_value": dt.date(2000, 1, 1),
                    "timestamp_value": dt.datetime(2000, 1, 1, tzinfo=dt.UTC),
                }[column.name]
        setattr(scalar, column.name, replacement)
        try:
            with pytest.raises((ValueError, TypeError, OverflowError, CandidateRunnerError)):
                actual_row = scalar_digest._verification_row(scalar, child=child)
                changed_groups = [(*groups[0][:3], [actual_row, *groups[0][3][1:]])]
                native_verifier(child, (1, 2), layouts, changed_groups)
        finally:
            setattr(scalar, column.name, original)


@pytest.mark.parametrize("change", ["missing", "extra", "order", "owner", "type", "required_missing"])
def test_verified_native_rejects_revision_corruption(native_verifier, change):
    layouts, groups, _records = _verification_fixture(child=False)
    target, keys, expected, rows = groups[0]
    owner = (9, 2) if change == "owner" else (1, 2)
    corruptions_by_name = {
        "missing": (expected, rows[:-1]),
        "extra": (expected, [*rows, rows[0]]),
        "order": (expected, list(reversed(rows))),
        "owner": (expected, rows),
        "type": ((("value", True), *expected[1:]), rows),
        "required_missing": ((("missing", None), *expected[1:]), rows[1:]),
    }
    expected, rows = corruptions_by_name[change]
    with pytest.raises((ValueError, TypeError)):
        native_verifier(False, owner, layouts, [(target, keys, expected, rows)])


@pytest.mark.parametrize("child", [False, True])
def test_verified_native_zero_hot_and_missing_are_not_null(native_verifier, child):
    slot = int(child)
    keys = ((b"r" * 32).hex(), (b"c" * 32).hex() if child else None)
    target = (3, 7, slot)
    assert native_verifier(child, (1, 2), [(slot, ())], [(target, keys, (), [])]) == b""
    layouts = [(slot, ((1, 1, "string", True),))]
    assert native_verifier(child, (1, 2), layouts, [(target, keys, (("missing", None),), [])]) == b""
    with pytest.raises(ValueError):
        native_verifier(child, (1, 2), layouts, [(target, keys, (("null", None),), [])])


@pytest.mark.parametrize("child", [False, True])
def test_verified_caller_normalizes_once_across_physical_pages(monkeypatch, native_verifier, child):
    request, family = _twenty_projection_family(child=child)
    records = _projection_records(request, [family], child=child)
    reference = _reference_frames(records, child=child)
    with monkeypatch.context() as page_patch:
        page_patch.setattr(graph, "MAX_BATCH_ROWS", 7)
        responses = _projection_responses(request, records, child=child)
    session = _read_session(monkeypatch, responses)
    decode = Mock(wraps=scalar_digest.payload_values)
    verifier = Mock(wraps=native_verifier)
    monkeypatch.setattr(scalar_digest, "payload_values", decode)
    monkeypatch.setattr(scalar_digest, "native_verifier", lambda: verifier)
    monkeypatch.setattr(output, "_expected_projections", Mock(side_effect=AssertionError("projection objects")))
    digests, expected = _digests(), _digests()
    for digest in expected:
        digest.update(reference)
    assert (
        output._scalar_material(
            session, request, _registry(request.definition), 7, _generation(request), digests, child=child
        )
        == 20
    )
    assert [digest.digest() for digest in digests] == [digest.digest() for digest in expected]
    assert decode.call_count == 1 and verifier.call_count == 1
    assert session.execute.call_count > 3 and session.transactions == session.closed


@pytest.mark.parametrize(
    "failure",
    ["native_value", "native_type", "native_overflow", "before", "after", "oversized", "wrong_type", "upstream"],
)
def test_verified_batch_failure_closes_without_hash_or_fallback(failure):
    layouts, groups, _records = _verification_fixture(child=False)
    closed_flags = []

    def revisions():
        try:
            yield groups[0]
            if failure == "upstream":
                raise CandidateRunnerError("upstream failed")
        finally:
            closed_flags.append(True)

    error_type = {"native_value": ValueError, "native_type": TypeError, "native_overflow": OverflowError}.get(failure)
    verifier = Mock(
        side_effect=error_type("synthetic private batch detail") if error_type else None,
        return_value="wrong"
        if failure == "wrong_type"
        else b"x" * (16 * scalar_digest.MAX_FRAME_BYTES + 1 if failure == "oversized" else 1),
    )
    budget = Mock(
        side_effect=([None] if failure == "after" else []) + [LeaseAuthorityLost("expired")]
        if failure in {"before", "after"}
        else None
    )
    digests = _digests()
    expected_error = LeaseAuthorityLost if failure in {"before", "after"} else CandidateRunnerError
    with pytest.raises(expected_error) as caught:
        scalar_digest.verified_material(
            verifier, revisions(), (1, 2), layouts, digests, child=False, check_budget=budget
        )
    if error_type:
        assert str(caught.value) == "native scalar verification failed"
        assert caught.value.__cause__ is None and caught.value.__suppress_context__
    assert closed_flags == [True]
    assert [digest.digest() for digest in digests] == [digest.digest() for digest in _digests()]
    assert verifier.call_count == int(failure not in {"before", "upstream"})


@pytest.mark.parametrize("adapter", ["encoder", "verifier"])
@pytest.mark.parametrize(
    ("origin", "error_type"),
    [
        ("native", CancellationRequested),
        ("native", LeaseAuthorityLost),
        ("native", asyncio.CancelledError),
        ("native", RuntimeError),
        *(
            (origin, error)
            for origin in ("before", "after", "upstream")
            for error in (ValueError, TypeError, OverflowError)
        ),
    ],
)
def test_native_adapter_preserves_other_errors(adapter, origin, error_type):
    layouts, groups, records = _verification_fixture(child=False)
    error = error_type("synthetic failure")
    closed_flags = []

    def stream():
        try:
            if origin == "upstream":
                raise error
            yield from records if adapter == "encoder" else groups
        finally:
            closed_flags.append(True)

    native = Mock(side_effect=error if origin == "native" else None, return_value=b"encoded")
    budget = Mock(
        side_effect=([None] if origin == "after" else []) + [error] if origin in {"before", "after"} else None
    )
    digests = _digests()
    with pytest.raises(error_type) as caught:
        if adapter == "encoder":
            scalar_digest.scalar_material(native, stream(), digests, child=False, check_budget=budget)
        else:
            scalar_digest.verified_material(
                native, stream(), (1, 2), layouts, digests, child=False, check_budget=budget
            )
    assert caught.value is error
    assert closed_flags == [True]
    assert [digest.digest() for digest in digests] == [digest.digest() for digest in _digests()]
    assert native.call_count == int(origin in {"native", "after"})


def test_verified_capability_and_memory_envelope_fail_closed(monkeypatch):
    capability = Mock()
    module = SimpleNamespace(custom_import_verified_scalar_frames_v1=capability)
    monkeypatch.setattr(scalar_digest.importlib, "import_module", lambda _name: module)
    assert scalar_digest.native_verifier.__wrapped__() is capability
    del module.custom_import_verified_scalar_frames_v1
    assert scalar_digest.native_verifier.__wrapped__() is None
    module.custom_import_verified_scalar_frames_v1 = capability
    assert scalar_digest.VERIFY_BUFFER_BYTES < scalar_digest.RESERVE_BYTES
    with monkeypatch.context() as schema_patch:
        schema_patch.setattr(scalar_digest, "SCALAR_COLUMNS", scalar_digest.SCALAR_COLUMNS - {"value_state"})
        assert scalar_digest.native_verifier.__wrapped__() is None
    monkeypatch.setattr(scalar_digest, "RESERVE_BYTES", scalar_digest.VERIFY_BUFFER_BYTES - 1)
    assert scalar_digest.native_verifier.__wrapped__() is None


def test_verified_python_cell_allowance_covers_unicode_and_typed_containers():
    actual, expected = "😀" * 2048, "😀" + "x" * 2044
    # Include UTF-8 caches even before their lazy creation, two maximum Decimal
    # objects and a conservative allowance for every tuple/binding/typed lane.
    strings = sum(sys.getsizeof(value) + len(value.encode("utf-8")) for value in (actual, expected))
    assert strings + 2 * 512 + 4096 < 32_768
    assert 2 * scalar_digest.VERIFY_BATCH_ROWS * scalar_digest.MAX_FRAME_BYTES == 4 * 1024 * 1024


@pytest.mark.parametrize("limit", ["rows", "bytes"])
def test_verified_caller_reservation_precedes_selection(monkeypatch, limit):
    request, family = _twenty_projection_family(child=False)
    records = _projection_records(request, [family], child=False)
    session = _read_session(monkeypatch, _projection_responses(request, records, child=False))
    verifier = Mock(side_effect=AssertionError("unreserved verifier"))
    monkeypatch.setattr(scalar_digest, "native_verifier", lambda: verifier)
    monkeypatch.setattr(scalar_digest, "native_encoder", lambda: None)
    if limit == "rows":
        monkeypatch.setattr(output, "MAX_BATCH_ROWS", scalar_digest.VERIFY_RESERVED_ROWS + 1)
    else:
        monkeypatch.setattr(output, "MAX_BATCH_BYTES", scalar_digest.RESERVE_BYTES)
    assert (
        output._scalar_material(
            session, request, _registry(request.definition), 7, _generation(request), _digests(), child=False
        )
        == 20
    )
    verifier.assert_not_called()


@pytest.mark.parametrize("bound", ["groups", "cells", "rows", "decimal", "huge_zero", "utf8"])
def test_verified_native_bounds_precede_owned_conversion(native_verifier, bound):
    layouts, groups, _records = _verification_fixture(child=False)
    target, keys, expected, rows = groups[0]
    if bound == "groups":
        groups *= 17
    elif bound == "cells":
        groups *= 9
    elif bound == "rows":
        groups = [(target, keys, expected, rows * 9)]
    else:
        changed_rows = list(rows)
        if bound == "utf8":
            changed_rows[0] = (*rows[0][:2], ("😀" * 2048, *rows[0][2][1:]))
        else:
            cells = list(rows[4][2])
            cells[2] = Decimal("1E+999999999") if bound == "decimal" else Decimal("0E-999999999")
            changed_rows[4] = (*rows[4][:2], tuple(cells))
        groups = [(target, keys, expected, changed_rows)]
    with pytest.raises((ValueError, TypeError, OverflowError)):
        native_verifier(False, (1, 2), layouts, groups)


def test_verified_decimal_positive_exponent_zero_uses_bounded_exact_frame(native_verifier):
    layouts, groups, records = _verification_fixture(child=False)
    records[5][0].decimal_value = Decimal("-0E+999999999")
    groups[0][3][5] = scalar_digest._verification_row(records[5][0], child=False)
    # The positive zero exponent is never expanded into a power or a digit run.
    actual = native_verifier(False, (1, 2), layouts, groups)
    records[5][0].decimal_value = Decimal("-0")
    assert actual == _reference_frames(records, child=False)


@pytest.mark.parametrize("child", [False, True])
def test_verification_revisions_preserve_missing_and_null_cells(child):
    request, family = _twenty_projection_family(child=child)
    revision = _projection_records(request, [family], child=child)[0][1]
    _layouts, contexts = scalar_digest.verification_layouts(
        request.definition, _registry(request.definition).child_collection_slots, child=child
    )
    fields, hot_fields = contexts[int(child)]
    values_by_field = dict(scalar_digest.payload_values(fields, revision.canonical_payload, label="test"))
    missing, null = [field.field_id for field in fields if field.field_id.startswith("extra_")][:2]
    del values_by_field[missing]
    values_by_field[null] = None
    revision.canonical_payload = record_payload(fields, values_by_field)
    records = _projection_records(request, [family], child=child)
    groups = list(scalar_digest.verification_revisions((record for record in records), contexts, child=child))
    assert len(groups) == 1 and len(groups[0][3]) == 19
    expected_by_field = dict(zip((field.field_id for field in hot_fields), groups[0][2], strict=True))
    assert expected_by_field[missing] == ("missing", None)
    assert expected_by_field[null] == ("null", None)
    assert sum(cell[0] == "value" for cell in expected_by_field.values()) == 18


@pytest.mark.parametrize("child", [False, True])
def test_verification_revisions_keep_empty_groups_and_close_on_exhaustion(child):
    closed_flags = []

    def records():
        try:
            for identity in (7, 8):
                revision = SimpleNamespace(
                    root_record_id=3,
                    root_revision_id=identity,
                    child_revision_id=identity,
                    collection_slot=1,
                    child_key_sha256=b"c" * 32,
                    canonical_payload=record_payload((), {}),
                )
                yield None, revision, b"r" * 32
        finally:
            closed_flags.append(True)

    groups = list(scalar_digest.verification_revisions(records(), {int(child): ((), ())}, child=child))
    assert [group[0] for group in groups] == [(3, 7, int(child)), (3, 8, int(child))]
    assert [group[2:] for group in groups] == [((), []), ((), [])]
    assert closed_flags == [True]


@pytest.mark.parametrize("corruption", ["empty_first", "empty_last", "duplicate_empty", "overflow"])
def test_verification_revisions_reject_malformed_groups_and_close(corruption):
    request, family = _twenty_projection_family(child=False)
    record = _projection_records(request, [family], child=False)[0]
    _layouts, contexts = scalar_digest.verification_layouts(request.definition, {}, child=False)
    empty = (None, *record[1:])
    malformed = {
        "empty_first": [empty, record],
        "empty_last": [record, empty],
        "duplicate_empty": [empty, empty],
        "overflow": [record] * (scalar_digest.VERIFY_PENDING_ROWS + 1),
    }[corruption]
    closed_flags = []

    def records():
        try:
            yield from malformed
        finally:
            closed_flags.append(True)

    with pytest.raises(CandidateRunnerError, match="differs from the frozen payload"):
        list(scalar_digest.verification_revisions(records(), contexts, child=False))
    assert closed_flags == [True]


@pytest.mark.parametrize(
    ("column", "value"),
    [("string_value", "x" * 2049), ("field_type", 1), ("value_state", "x" * 9)],
)
def test_verification_row_rejects_unbounded_or_untyped_text(column, value):
    scalar = _typed_records(child=False)[0][0]
    setattr(scalar, column, value)
    with pytest.raises(CandidateRunnerError, match="differs from the frozen payload"):
        scalar_digest._verification_row(scalar, child=False)


def test_scalar_adapters_leave_empty_streams_unhashed():
    native, budget = Mock(), Mock()
    digests = _digests()
    assert list(scalar_digest.verification_revisions((record for record in ()), {}, child=False)) == []
    assert (
        scalar_digest.verified_material(
            native, (group for group in ()), (1, 2), [], digests, child=False, check_budget=budget
        )
        == 0
    )
    assert (
        scalar_digest.scalar_material(native, (record for record in ()), digests, child=False, check_budget=budget) == 0
    )
    native.assert_not_called()
    budget.assert_not_called()
    assert [digest.digest() for digest in digests] == [digest.digest() for digest in _digests()]


def test_verified_material_hashes_each_complete_bounded_batch():
    layouts, groups, _records = _verification_fixture(child=False)
    batches = []

    def verifier(child, owner, actual_layouts, batch):
        assert child is False and owner == (1, 2) and actual_layouts == layouts
        batches.append(tuple(batch))
        return b"verified"

    digests, expected = _digests(), _digests()
    budget = Mock()
    assert (
        scalar_digest.verified_material(
            verifier, (groups[0] for _ in range(9)), (1, 2), layouts, digests, child=False, check_budget=budget
        )
        == 144
    )
    assert [len(batch) for batch in batches] == [8, 1]
    for digest in expected:
        digest.update(b"verified" * 2)
    assert [digest.digest() for digest in digests] == [digest.digest() for digest in expected]
    assert budget.call_count == 4


def test_scalar_material_hashes_each_complete_bounded_batch():
    records = _typed_records(child=False)
    batches = []

    def encoder(child, batch):
        assert child is False
        batches.append(tuple(batch))
        return b"encoded"

    digests, expected = _digests(), _digests()
    budget = Mock()
    count = scalar_digest.BATCH_ROWS + 1
    assert (
        scalar_digest.scalar_material(
            encoder, (records[0] for _ in range(count)), digests, child=False, check_budget=budget
        )
        == count
    )
    assert [len(batch) for batch in batches] == [scalar_digest.BATCH_ROWS, 1]
    for digest in expected:
        digest.update(b"encoded" * 2)
    assert [digest.digest() for digest in digests] == [digest.digest() for digest in expected]
    assert budget.call_count == 4
