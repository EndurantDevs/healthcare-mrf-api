# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native address preparation preserves bounded input and SQL fallback."""

from types import SimpleNamespace

import pytest

from process.ext import address_batch

ROW = ("10 Main St", None, "Boston", "MA", "02101", "US")


def _result(**values):
    return {**dict.fromkeys(address_batch.RESULT_FIELDS), **values}


def _native(monkeypatch, function):
    monkeypatch.setattr(
        address_batch.address_fast, "_fast_module", lambda: SimpleNamespace(prepare_address_batch=function)
    )


@pytest.mark.parametrize("module", [None, SimpleNamespace()])
def test_missing_native_retains_sql(monkeypatch, module):
    monkeypatch.setattr(address_batch.address_fast, "_fast_module", lambda: module)
    assert address_batch.prepare_address_batch([ROW]) is None


def test_empty_batch_needs_no_native(monkeypatch):
    monkeypatch.setattr(address_batch.address_fast, "_fast_module", lambda: pytest.fail("unexpected module lookup"))
    assert address_batch.prepare_address_batch([]) == []


def test_distinct_inputs_keep_order_and_independent_results(monkeypatch):
    other = (None, None, "Boston", "MA", "02101", "US")

    def prepare(rows):
        assert rows == [ROW, other]
        return [_result(formatted_address="10 Main St, Boston, MA 02101"), None]

    _native(monkeypatch, prepare)
    result = address_batch.prepare_address_batch(iter([ROW, other, ROW]))
    assert result[0] == result[2]
    assert result[0]["formatted_address"] == "10 Main St, Boston, MA 02101"
    assert result[1] is None
    result[0]["formatted_address"] = "changed"
    assert result[2]["formatted_address"] == "10 Main St, Boston, MA 02101"


def test_input_values_follow_existing_text_contract(monkeypatch):
    def prepare(rows):
        assert rows == [("10", None, None, None, "2101", None)]
        return [None]

    _native(monkeypatch, prepare)
    assert address_batch.prepare_address_batch([(10, None, None, None, 2101, None)]) == [None]


@pytest.mark.parametrize(
    "output",
    [[], [None, None], (None,), [False], [{}], [_result(unexpected="extra")], [_result(state_code=1)]],
)
def test_malformed_native_retains_sql(monkeypatch, output):
    _native(monkeypatch, lambda rows: output)
    assert address_batch.prepare_address_batch([ROW]) is None


def test_native_exception_retains_sql(monkeypatch):
    def prepare(rows):
        raise RuntimeError("unavailable")

    _native(monkeypatch, prepare)
    assert address_batch.prepare_address_batch([ROW]) is None


def test_invalid_width_is_an_input_error():
    with pytest.raises(ValueError, match="6 fields"):
        address_batch.prepare_address_batch([ROW[:-1]])


def test_row_limit_stops_an_unbounded_iterator(monkeypatch):
    monkeypatch.setattr(address_batch, "MAX_ROWS", 2)

    def rows():
        yield ROW
        yield ROW
        yield ROW
        pytest.fail("iterator consumed beyond batch bound")

    with pytest.raises(ValueError, match="row limit"):
        address_batch.prepare_address_batch(rows())


def test_field_and_batch_bytes_are_bounded(monkeypatch):
    monkeypatch.setattr(address_batch, "MAX_FIELD_BYTES", 4)
    with pytest.raises(ValueError, match="field exceeds"):
        address_batch.prepare_address_batch([("ééé", None, None, None, None, None)])
    monkeypatch.setattr(address_batch, "MAX_INPUT_BYTES", 7)
    with pytest.raises(ValueError, match="batch exceeds"):
        address_batch.prepare_address_batch([("éé", "abcd", None, None, None, None)])
