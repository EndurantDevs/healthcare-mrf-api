# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exact landing framing, bounded dispatch and genuine compiled parity cases."""

import inspect
import struct
import sys
from types import SimpleNamespace
from uuid import UUID

import pytest

from process.custom_import import bulk_page_codec as codec
from tests.test_custom_import_build_source import _child, _root
from tests.test_custom_import_build_source_bulk import _bulk_context, _prepared_page


def _row(**changes):
    row = codec.LandingRow(
        UUID(int=7),
        -(1 << 63),
        b"\x00\xff",
        (1 << 63) - 1,
        0,
        -1,
        " 1.200e+02 ",
        b"",
        None,
        'Ω\n\x00"\\🙂',
        bytes(range(32)),
        "",
        None,
        "orphan_child",
        None,
        None,
        '{"legacy":"1.200"}',
        b"\xff",
    )
    return row._replace(**changes)


def _python_manifest(records):
    """Arbitrary iterators always execute the unchanged Python hash body."""
    return codec.landing_manifest_sha256(iter(records))


def _install_encoder(monkeypatch, encoder):
    monkeypatch.setitem(sys.modules, "ptg2_address_canon", SimpleNamespace(custom_import_landing_manifest_v1=encoder))


def _outcome(operation):
    try:
        return "value", operation()
    except (TypeError, ValueError, OverflowError, struct.error) as error:
        return type(error), error.args


class _Text(str):
    __slots__ = ()


class _Integer(int):
    __slots__ = ()


class _Bytes(bytes):
    __slots__ = ()


class _List(list):
    __slots__ = ()


class _Tuple(tuple):
    __slots__ = ()


class _Row(codec.LandingRow):
    __slots__ = ()


_FALLBACK_VALUES = (
    True,
    1 << 63,
    -(1 << 63) - 1,
    "\ud800",
    _Text("legacy"),
    _Integer(3),
    _Bytes(b"legacy"),
    bytearray(b"legacy"),
    memoryview(b"legacy"),
    1.25,
)


def test_python_manifest_matches_independent_golden_frames():
    assert _python_manifest([]).hex() == "f86d840d4bec8e65f7be1ed46559ab67ef2b8c9e8d5c18b6e812d6cad6b0c441"
    assert _python_manifest([_row()]).hex() == "d5478a25af5ae693422cdfbe80d4dbccf6a4ef8e29bf9b4fccbde3e28425b6eb"


@pytest.mark.parametrize("container", [tuple, list])
@pytest.mark.parametrize("count", [0, 1, 256])
def test_host_dispatch_preserves_original_concrete_rows(monkeypatch, container, count):
    """This explicit Python adapter double checks dispatch, not compiled parity."""
    records = container(_row(source_ordinal=index) for index in range(count))
    calls = []

    def encoder(original):
        calls.append(original)
        return _python_manifest(original)

    _install_encoder(monkeypatch, encoder)
    assert codec.landing_manifest_sha256(records) == _python_manifest(records)
    assert len(calls) == 1 and calls[0] is records


@pytest.mark.parametrize(
    "shape", ["iterator", "list_subclass", "tuple_subclass", "row_subclass", "plain_tuple", "over_rows"]
)
def test_nonconcrete_or_oversized_batches_never_enter_native(monkeypatch, shape):
    def unexpected(_records):
        pytest.fail("unsupported outer shape reached native")

    _install_encoder(monkeypatch, unexpected)
    rows = [_row()] * (257 if shape == "over_rows" else 1)
    expected = _python_manifest(rows)
    records = {
        "iterator": iter(rows),
        "list_subclass": _List(rows),
        "tuple_subclass": _Tuple(rows),
        "row_subclass": [_Row(*rows[0])],
        "plain_tuple": [tuple(rows[0])],
        "over_rows": rows,
    }[shape]
    assert codec.landing_manifest_sha256(records) == expected


@pytest.mark.parametrize("value", _FALLBACK_VALUES)
def test_native_refusal_keeps_original_scalar_results_and_errors(monkeypatch, value):
    _install_encoder(monkeypatch, lambda _records: None)
    records = [_row(canonical_payload=value)]
    assert _outcome(lambda: codec.landing_manifest_sha256(records)) == _outcome(lambda: _python_manifest(records))


def test_arbitrary_iterator_keeps_first_error_before_later_input_failure(monkeypatch):
    def records():
        yield _row(canonical_payload=True)
        raise AssertionError("later input must remain unread")

    _install_encoder(monkeypatch, lambda _records: pytest.fail("iterator reached native"))
    with pytest.raises(TypeError):
        codec.landing_manifest_sha256(records())


@pytest.mark.parametrize("availability", ["missing_module", "missing_capability"])
def test_older_or_absent_extension_keeps_python(monkeypatch, availability):
    def imported(_name):
        if availability == "missing_module":
            raise ImportError("synthetic absent extension")
        return SimpleNamespace()

    monkeypatch.setattr(codec.importlib, "import_module", imported)
    assert codec.landing_manifest_sha256([_row()]) == _python_manifest([_row()])


@pytest.mark.parametrize("result", [b"short", bytearray(32), True])
def test_invalid_native_result_is_not_reinterpreted_as_python_fallback(monkeypatch, result):
    _install_encoder(monkeypatch, lambda _records: result)
    with pytest.raises(ValueError, match="native landing manifest"):
        codec.landing_manifest_sha256([_row()])


def test_native_allocation_failure_propagates(monkeypatch):
    failure = MemoryError("synthetic native allocation failure")

    def failed(_records):
        raise failure

    _install_encoder(monkeypatch, failed)
    with pytest.raises(MemoryError) as caught:
        codec.landing_manifest_sha256([_row()])
    assert caught.value is failure


@pytest.fixture
def native_encoder():
    module = pytest.importorskip("ptg2_address_canon")
    encoder = getattr(module, "custom_import_landing_manifest_v1", None)
    if encoder is None:
        pytest.skip("compiled landing manifest unavailable")
    assert inspect.isbuiltin(encoder)
    return encoder


@pytest.mark.parametrize("container", [tuple, list])
@pytest.mark.parametrize("count", [0, 1, 256])
def test_real_compiled_manifest_matches_python_in_original_order(native_encoder, monkeypatch, container, count):
    records = container(_row(source_ordinal=index) for index in range(count))
    expected = _python_manifest(records)
    assert native_encoder(records) == expected
    _install_encoder(monkeypatch, native_encoder)
    assert codec.landing_manifest_sha256(records) == expected
    changed_uuids = container(row._replace(batch_id=UUID(int=99)) for row in records)
    assert native_encoder(changed_uuids) == expected
    if count > 1:
        assert native_encoder(container(reversed(records))) != expected


@pytest.mark.parametrize("value", _FALLBACK_VALUES)
def test_real_compiled_refusal_retains_python_error_contract(native_encoder, monkeypatch, value):
    records = [_row(canonical_payload=value)]
    assert native_encoder(records) is None
    _install_encoder(monkeypatch, native_encoder)
    assert _outcome(lambda: codec.landing_manifest_sha256(records)) == _outcome(lambda: _python_manifest(records))


@pytest.mark.parametrize("left,right", [("1.200", "1.2"), ("é", "e\u0301"), ("", None), (b"", "")])
def test_real_compiled_lexical_cells_remain_distinct(native_encoder, left, right):
    rows = [[_row(canonical_payload=value)] for value in (left, right)]
    digests = [native_encoder(records) for records in rows]
    assert digests == [_python_manifest(records) for records in rows]
    assert digests[0] != digests[1]


@pytest.mark.parametrize("binary", [False, True])
def test_real_compiled_bounds_keep_python_fallback(native_encoder, monkeypatch, binary):
    row = _row(canonical_payload="")
    framed_bytes = sum(
        1 if value is None else 9 if type(value) is int else 9 + len(value.encode() if type(value) is str else value)
        for value in row[1:]
    )
    suffix = b"x" if binary else "x"
    admitted = row._replace(canonical_payload=suffix * (1_048_576 - framed_bytes))
    assert native_encoder([admitted]) == _python_manifest([admitted])
    excessive_rows = [admitted._replace(canonical_payload=admitted.canonical_payload + suffix)]
    assert native_encoder(excessive_rows) is None and native_encoder([row] * 257) is None
    _install_encoder(monkeypatch, native_encoder)
    assert codec.landing_manifest_sha256(excessive_rows) == _python_manifest(excessive_rows)
    assert codec.landing_manifest_sha256([row] * 257) == _python_manifest([row] * 257)


@pytest.mark.parametrize("records", [iter(()), _List(), _Tuple(), [()], [[None] * 18], [(None,) * 17]])
def test_real_compiled_abi_refuses_unsupported_shapes(native_encoder, records):
    assert native_encoder(records) is None


@pytest.mark.parametrize("stream", [0, 1])
def test_real_compiled_source_landing_keeps_rejection_and_legacy_payloads(native_encoder, monkeypatch, stream):
    context = _bulk_context(stream)
    values = [_child(key='child-Ω\n\x00"\\'), _child(key=None)] if stream else [_root(), _root(npi=None)]
    page = _prepared_page(context, values)
    _install_encoder(monkeypatch, native_encoder)
    batch = codec.encode_landing_batch(context, (page,), batch_id=UUID(int=7), first_pack_ordinal=0)
    assert native_encoder(batch.records) == batch.manifest_sha256 == _python_manifest(batch.records)
    assert any(row.rejection_code is not None for row in batch.records)
