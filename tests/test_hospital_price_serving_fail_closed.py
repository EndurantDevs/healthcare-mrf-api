# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import builtins
from dataclasses import replace
import json
import runpy
from types import SimpleNamespace

import pytest

from api.endpoint import hospital_prices as endpoint
from api import hospital_price_serving as serving
from api import hospital_price_serving_sql as serving_sql
from api.hospital_price_request import decode_hospital_price_cursor
from api.hospital_price_request import encode_hospital_price_cursor
from tests.test_hospital_price_serving import _Native
from tests.test_hospital_price_serving import _query
from tests.test_hospital_price_serving import _Result
from tests.test_hospital_price_serving import _Session
from tests.test_hospital_price_serving import _version
from tests.test_hospital_price_serving import VERSION_ID


class _RowsSession:
    def __init__(self, rows=()):
        self.rows = rows

    async def execute(self, _statement, _params=None):
        return _Result(self.rows)


class _MissingBlockSession(_Session):
    def _statement_result(self, statement, params=None):
        if statement in {serving.SERVICE_BLOCK_SQL, serving.FACT_BLOCK_SQL}:
            self.statements.append((statement, params or {}))
            return _Result()
        return super()._statement_result(statement, params)


def test_native_import_fails_closed_without_mutating_the_loaded_module(monkeypatch):
    real_import = builtins.__import__

    def import_without_native(name, *args, **kwargs):
        if name == "ptg2_address_canon":
            raise ImportError("synthetic missing native module")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", import_without_native)
    isolated_globals_by_field = runpy.run_path(serving.__file__)
    assert isolated_globals_by_field["_NATIVE"] is None


def test_endpoint_helpers_reject_missing_or_ambiguous_inputs():
    with pytest.raises(serving.HospitalPriceServingUnavailableError):
        endpoint._get_session(SimpleNamespace(ctx=SimpleNamespace()))
    assert endpoint._query_values(SimpleNamespace(args=None)) == {}

    with pytest.raises(serving.HospitalPriceInvalidRequestError):
        endpoint._query_values(SimpleNamespace(args=object()))

    class PlainArgs(dict):
        pass

    assert endpoint._query_values(
        SimpleNamespace(args=PlainArgs(code="70551"))
    ) == {"code": "70551"}

    class RepeatedArgs:
        @staticmethod
        def keys():
            return ("code",)

        @staticmethod
        def getlist(_field):
            return ["70551", "70551"]

    with pytest.raises(serving.HospitalPriceInvalidRequestError):
        endpoint._query_values(SimpleNamespace(args=RepeatedArgs()))


def test_endpoint_failure_mapping_is_stable_and_source_hidden():
    responses_by_status = {
        400: endpoint._failure_response(
            serving.HospitalPriceInvalidRequestError("private")
        ),
        404: endpoint._failure_response(serving.HospitalPriceNotFoundError("private")),
        409: endpoint._failure_response(serving.HospitalPriceCursorStaleError("private")),
        503: endpoint._failure_response(RuntimeError("private")),
    }
    for status, response in responses_by_status.items():
        assert response.status == status
        assert "private" not in json.loads(response.body)["error"]["message"].lower()


@pytest.mark.parametrize(
    "overrides",
    [
        {"payer_name": "", "plan_name": "Plan"},
        {"payer_name": " Payer", "plan_name": "Plan"},
        {"payer_name": 1, "plan_name": "Plan"},
        {"payer_name": "x" * 4097, "plan_name": "Plan"},
        {"limit": 25},
        {"limit": "0"},
    ],
)
def test_request_rejects_invalid_exact_fields(overrides):
    with pytest.raises(serving.HospitalPriceInvalidRequestError):
        _query(**overrides)


def test_cursor_decoder_rejects_bad_encoding_scope_and_explicit_version():
    query = _query()
    cursor = encode_hospital_price_cursor(query, VERSION_ID, 7)
    assert decode_hospital_price_cursor(replace(query, cursor=cursor), VERSION_ID) == 7
    with pytest.raises(serving.HospitalPriceInvalidRequestError):
        decode_hospital_price_cursor(replace(query, cursor="!" * 96), VERSION_ID)
    with pytest.raises(serving.HospitalPriceInvalidRequestError):
        decode_hospital_price_cursor(replace(query, cursor="A" * 96), VERSION_ID)
    with pytest.raises(serving.HospitalPriceInvalidRequestError):
        decode_hospital_price_cursor(
            replace(query, cursor=cursor, version_id="b" * 64),
            "b" * 64,
        )


def test_schema_identifier_is_validated(monkeypatch):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "invalid-name")
    with pytest.raises(RuntimeError, match="schema"):
        serving_sql._schema()
    monkeypatch.delenv("HLTHPRT_DB_SCHEMA")
    monkeypatch.setenv("DB_SCHEMA", "hospital_test")
    assert serving_sql._schema() == '"hospital_test"'
    monkeypatch.delenv("DB_SCHEMA")
    assert serving_sql._schema() == '"mrf"'


@pytest.mark.asyncio
async def test_native_and_database_adapter_failures_are_collapsed(monkeypatch):
    monkeypatch.setattr(serving, "_NATIVE", None)
    with pytest.raises(serving.HospitalPriceServingUnavailableError):
        await serving._native_call("missing")
    monkeypatch.setattr(serving, "_NATIVE", object())
    with pytest.raises(serving.HospitalPriceServingUnavailableError):
        serving._native_module()

    native = _Native()

    def fail_decode(_payload):
        raise ValueError("private decoder detail")

    native.hospital_price_decode_service_block = fail_decode
    monkeypatch.setattr(serving, "_NATIVE", native)
    with pytest.raises(
        serving.HospitalPriceServingUnavailableError,
        match="packed block",
    ):
        await serving._native_call("hospital_price_decode_service_block", b"bad")
    with pytest.raises(serving.HospitalPriceServingUnavailableError, match="database"):
        serving._mappings(object())


def test_version_and_selector_metadata_fail_closed():
    with pytest.raises(serving.HospitalPriceNotFoundError):
        serving._validated_version(())
    with pytest.raises(serving.HospitalPriceServingUnavailableError, match="ambiguous"):
        serving._validated_version((_version(), _version()))
    with pytest.raises(serving.HospitalPriceServingUnavailableError, match="version"):
        serving._validated_version((_version(current_charge_count=2),))

    with pytest.raises(serving.HospitalPriceServingUnavailableError, match="selector"):
        serving._validated_selector_page({}, None, b"s" * 32)
    with pytest.raises(serving.HospitalPriceServingUnavailableError, match="selector"):
        serving._validated_selector_page(
            {"page_index": 0, "page_count": 1, "secondary_count": 1,
             "secondary_first": 0},
            {"page_index": -1, "page_count": 1, "ref_count": 1,
             "first_ref": 0, "refs": [0], "truncated": False},
            b"s" * 32,
        )


def _selector_records(*, duplicate_page=False):
    return tuple(
        {
            "format_version": 2,
            "logical_first": 0,
            "logical_count": 1,
            "page_index": 0 if duplicate_page else index,
            "page_count": 2,
            "secondary_count": 1,
            "secondary_first": index,
            "key_sha256": b"s" * 32,
            "parent_sha256": b"s" * 32,
            "payload": bytes([index]),
        }
        for index in range(2)
    )


async def _decode_selector_page(
    _name, payload, _kind, _first, _second, _ranges, _maximum
):
    index = payload[0]
    return {
        "page_index": index,
        "page_count": 2,
        "row_count": 1,
        "page_ref_count": 1,
        "found": True,
        "ref_count": 1,
        "first_ref": index,
        "refs": [index],
        "truncated": False,
    }


@pytest.mark.asyncio
async def test_selector_page_aggregation_is_bounded_and_rejects_drift(monkeypatch):
    monkeypatch.setattr(serving, "_native_call", _decode_selector_page)
    refs, truncated, pages, count = await serving._selector_refs(
        _selector_records(), "code", "CPT", "70551", [(0, 2)], 1,
        b"s" * 32,
    )
    assert (refs, truncated, pages, count) == ([0], True, [0], 2)

    drifted_record_list = list(_selector_records())
    drifted_record_list[1] = {
        **drifted_record_list[1], "page_count": 3
    }

    async def drift_decode(
        _name, payload, _kind, _first, _second, _ranges, _maximum
    ):
        decoded = await _decode_selector_page(
            _name, payload, _kind, _first, _second, _ranges, _maximum
        )
        decoded["page_count"] = 3 if payload[0] else 2
        return decoded

    monkeypatch.setattr(serving, "_native_call", drift_decode)
    with pytest.raises(serving.HospitalPriceServingUnavailableError, match="metadata"):
        await serving._selector_refs(
            tuple(drifted_record_list),
            "code", "CPT", "70551", [(0, 2)], 3,
            b"s" * 32,
        )


@pytest.mark.asyncio
async def test_selector_page_aggregation_rejects_duplicate_or_outside_refs(
    monkeypatch,
):
    async def duplicate_ref(
        _name, payload, _kind, _first, _second, _ranges, _maximum
    ):
        decoded = await _decode_selector_page(
            _name, payload, _kind, _first, _second, _ranges, _maximum
        )
        decoded["refs"] = [0]
        return decoded

    monkeypatch.setattr(serving, "_native_call", duplicate_ref)
    with pytest.raises(serving.HospitalPriceServingUnavailableError, match="references"):
        await serving._selector_refs(
            _selector_records(), "code", "CPT", "70551", [(0, 2)], 3,
            b"s" * 32,
        )

    monkeypatch.setattr(serving, "_native_call", _decode_selector_page)
    with pytest.raises(serving.HospitalPriceServingUnavailableError, match="references"):
        await serving._selector_refs(
            _selector_records(), "code", "CPT", "70551", [(0, 1)], 3,
            b"s" * 32,
        )

    async def duplicate_page(
        _name, payload, _kind, _first, _second, _ranges, _maximum
    ):
        decoded = await _decode_selector_page(
            _name, payload, _kind, _first, _second, _ranges, _maximum
        )
        decoded["page_index"] = 0
        return decoded

    monkeypatch.setattr(serving, "_native_call", duplicate_page)
    with pytest.raises(serving.HospitalPriceServingUnavailableError, match="pages"):
        await serving._selector_refs(
            _selector_records(duplicate_page=True),
            "code", "CPT", "70551", [(0, 2)], 3,
            b"s" * 32,
        )


@pytest.mark.asyncio
async def test_charge_page_continuation_metadata_is_complete(monkeypatch):
    native = _Native()
    monkeypatch.setattr(serving, "_NATIVE", native)

    async def incomplete(*_args):
        return [1], False, [1], 2

    monkeypatch.setattr(serving, "_selector_refs", incomplete)
    with pytest.raises(serving.HospitalPriceServingUnavailableError, match="incomplete"):
        await serving._charge_page(_RowsSession([{"secondary_first": 1}]), _query(), VERSION_ID, -1)

    async def next_page(*_args):
        return [0], False, [0], 2

    monkeypatch.setattr(serving, "_selector_refs", next_page)
    assert await serving._charge_page(
        _RowsSession([{"secondary_first": 0}]), _query(), VERSION_ID, -1
    ) == ([0], True)

    async def empty_next_page(*_args):
        return [], False, [0], 2

    monkeypatch.setattr(serving, "_selector_refs", empty_next_page)
    with pytest.raises(serving.HospitalPriceServingUnavailableError, match="continuation"):
        await serving._charge_page(_RowsSession([{"secondary_first": 0}]), _query(), VERSION_ID, -1)


@pytest.mark.asyncio
async def test_service_blocks_project_only_selected_metadata(monkeypatch):
    session = _Session()
    monkeypatch.setattr(serving, "_NATIVE", session.native)
    assert await serving._charges_by_key(
        session, VERSION_ID, [], "CPT", "12345"
    ) == {}
    selected = await serving._charges_by_key(
        session, VERSION_ID, [0], "CPT", "12345"
    )
    assert "charges" not in selected[0][0]

    with pytest.raises(serving.HospitalPriceServingUnavailableError, match="selector"):
        await serving._charges_by_key(
            session, VERSION_ID, [0], "CPT", "99999"
        )

    with pytest.raises(serving.HospitalPriceServingUnavailableError, match="service block"):
        serving._validated_service_block(
            {"logical_count": 1, "logical_first": 0,
             "secondary_first": 1, "secondary_count": 1},
            [session.native.service_rows[0]],
        )
    with pytest.raises(serving.HospitalPriceServingUnavailableError, match="coverage"):
        await serving._charges_by_key(
            _MissingBlockSession(), VERSION_ID, [0], "CPT", "12345"
        )


def test_fact_ranges_reject_invalid_or_non_monotonic_metadata():
    service_by_field = {}
    empty_charge_by_field = {"first_fact_ordinal": 0, "fact_count": 0}
    assert serving._fact_ranges(
        {0: (service_by_field, empty_charge_by_field)}, [0]
    ) == []
    invalid_charge_by_field = {"first_fact_ordinal": 1 << 63, "fact_count": 1}
    with pytest.raises(serving.HospitalPriceServingUnavailableError, match="fact range"):
        serving._fact_ranges(
            {0: (service_by_field, invalid_charge_by_field)}, [0]
        )


@pytest.mark.asyncio
async def test_fact_selection_and_blocks_fail_closed(monkeypatch):
    session = _Session()
    monkeypatch.setattr(serving, "_NATIVE", session.native)

    async def inside_range(*_args):
        return [0], False, [0], 1

    monkeypatch.setattr(serving, "_selector_refs", inside_range)
    assert await serving._selected_fact_ordinals(
        session, _query(), VERSION_ID, [(0, 1, 0)]
    ) == {0: 0}

    session.native.fact_rows = session.native.fact_rows[:1]
    with pytest.raises(serving.HospitalPriceServingUnavailableError, match="fact block"):
        await serving._facts_by_charge(
            session, _query(), VERSION_ID, {0: 0}
        )

    session.native.fact_rows = [
        {**session.native.fact_rows[0], "charge_key": 1}
    ] * 3
    with pytest.raises(serving.HospitalPriceServingUnavailableError, match="identity"):
        await serving._facts_by_charge(
            session, _query(), VERSION_ID, {0: 0}
        )

    with pytest.raises(serving.HospitalPriceServingUnavailableError, match="coverage"):
        await serving._facts_by_charge(
            _MissingBlockSession(), _query(), VERSION_ID, {0: 0}
        )


@pytest.mark.asyncio
async def test_payer_selector_coverage_is_contiguous_per_fact_range(monkeypatch):
    monkeypatch.setattr(serving, "_NATIVE", _Native())

    class CountingSession(_RowsSession):
        def __init__(self, rows):
            super().__init__(rows)
            self.calls = 0

        async def execute(self, statement, params=None):
            assert statement is serving.PAYER_SELECTOR_SQL
            self.calls += 1
            return await super().execute(statement, params)

    async def selected_pages(*_args):
        return [0, 2], False, [0, 2], 3

    monkeypatch.setattr(serving, "_selector_refs", selected_pages)
    incomplete = CountingSession([
        {"page_index": 0, "page_count": 3, "range_indexes": [0],
         "key_page_count": 3},
        {"page_index": 2, "page_count": 3, "range_indexes": [0],
         "key_page_count": 3},
    ])
    with pytest.raises(serving.HospitalPriceServingUnavailableError, match="incomplete"):
        await serving._selected_fact_ordinals(
            incomplete, _query(), VERSION_ID, [(0, 3, 0)]
        )
    assert incomplete.calls == 1

    disjoint = CountingSession([
        {"page_index": 0, "page_count": 3, "range_indexes": [0],
         "key_page_count": 3},
        {"page_index": 2, "page_count": 3, "range_indexes": [1],
         "key_page_count": 3},
    ])
    assert await serving._selected_fact_ordinals(
        disjoint, _query(), VERSION_ID, [(0, 1, 0), (2, 3, 2)]
    ) == {0: 0, 2: 2}
    assert disjoint.calls == 1


@pytest.mark.asyncio
async def test_public_byte_budget_fails_before_retaining_large_rows(monkeypatch):
    session = _Session()
    monkeypatch.setattr(serving, "_NATIVE", session.native)

    with pytest.raises(serving.HospitalPriceServingUnavailableError, match="bound"):
        await serving._charges_by_key(
            session, VERSION_ID, [0], "CPT", "12345", [1]
        )
    with pytest.raises(serving.HospitalPriceServingUnavailableError, match="bound"):
        await serving._facts_by_charge(
            session, _query(), VERSION_ID, {0: 0}, [1]
        )


@pytest.mark.parametrize(
    "range_indexes",
    [None, [0, 0], [True], [1]],
)
def test_serving_support_rejects_invalid_payload_and_payer_coverage(range_indexes):
    with pytest.raises(serving.HospitalPriceServingUnavailableError, match="payload"):
        serving.consume_public_bytes([100], object())
    with pytest.raises(serving.HospitalPriceServingUnavailableError, match="incomplete"):
        serving.validate_payer_page_coverage((), [0], 1)
    with pytest.raises(serving.HospitalPriceServingUnavailableError, match="incomplete"):
        serving.validate_payer_page_coverage(
            ({"page_index": 0, "page_count": 2, "range_indexes": [0],
              "key_page_count": None},),
            [0],
            1,
        )
    with pytest.raises(serving.HospitalPriceServingUnavailableError, match="coverage"):
        serving.validate_payer_page_coverage(
            ({"page_index": 0, "page_count": 1, "key_page_count": 1,
              "range_indexes": range_indexes},),
            [0],
            1,
        )
    serving.validate_payer_page_coverage((), [], 1)
    serving.validate_payer_page_coverage(
        ({"page_index": 0, "page_count": 1, "range_indexes": [],
          "key_page_count": 1},),
        [0],
        1,
    )
