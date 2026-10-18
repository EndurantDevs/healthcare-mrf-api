# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure closed diagnostics and original native-failure annotation regressions."""

import asyncio
import json
from types import SimpleNamespace

import pytest

from tests.retained_wal_diagnostics import (
    MAX_INSPECTION_BYTES,
    RetainedWalDiagnostic,
    _closed_attribution,
    retained_bound_operands,
    retained_wal_attribution,
)


def _operands(**changes):
    return {
        "data_delta_bytes": 1,
        "data_limit_bytes": 8,
        "wal_bytes": 9,
        "wal_limit_bytes": 8,
        **changes,
    }


def test_phase_aggregates_translate_existing_baselines_without_row_history():
    """Many observations retain one fixed aggregate for the actual operation phase."""
    trace = RetainedWalDiagnostic()
    trace.record("0/0", 1)
    assert trace.samples == {}
    trace.begin("0/100")
    trace.enter("raw_copy")
    for amount in range(257, 1257):
        trace.record("0/0", amount)
    trace.enter("stamp")
    trace.enter("readiness")
    trace.record("0/100", 1001)
    note = json.loads(trace.note(_operands(wal_bytes=1001)).split(" ", 1)[1])
    assert note["wal_start_lsn"] == "0/100" and note["wal_end_lsn"] == "0/4E9"
    assert note["cause"] == "wal_limit" and note["phase"] == "readiness"
    assert set(note["phase_samples"]) == {"raw_copy", "readiness"}
    assert note["phase_samples"]["raw_copy"]["checks"] == 1000
    assert note["phase_samples"]["raw_copy"]["first_wal_bytes"] == 1
    assert note["phase_samples"]["raw_copy"]["last_wal_bytes"] == 1000
    assert "stamp" in note["markers_elapsed_ns"] and "stamp" not in note["phase_samples"]
    assert len(note["phase_samples"]) <= 9


@pytest.mark.parametrize(
    "changes,cause",
    [
        ({"data_delta_bytes": 9, "wal_bytes": 1}, "data_limit"),
        ({"data_delta_bytes": 9}, "data_and_wal_limits"),
    ],
)
def test_cause_is_derived_only_from_original_signed_operands(changes, cause):
    trace = RetainedWalDiagnostic()
    trace.begin("0/100")
    assert json.loads(trace.note(_operands(**changes)).split(" ", 1)[1])["cause"] == cause


@pytest.mark.parametrize(
    "changes",
    [
        {"wal_bytes": True},
        {"wal_bytes": -1},
        {"wal_bytes": 2**63},
        {"wal_limit_bytes": 0},
        {"data_delta_bytes": "secret"},
        {"wal_bytes": 8},
        {"unknown": "private payload"},
    ],
)
def test_malformed_or_non_overflow_operands_emit_no_note(changes):
    trace = RetainedWalDiagnostic()
    trace.begin("0/100")
    assert trace.note(_operands(**changes)) is None


@pytest.mark.parametrize("value", [None, True, "0/xyz", "0/1;secret", "100000000/0", "0/123456789", "a/1"])
def test_inexact_lsn_is_refused_without_retaining_input(value):
    trace = RetainedWalDiagnostic()
    trace.begin(value)
    assert trace.start_lsn is None and trace.note(_operands()) is None


@pytest.mark.parametrize("invalid", ["phase", "repeat_baseline", "negative_delta", "boolean_delta"])
def test_unsafe_diagnostic_state_refuses_closed_output(invalid):
    trace = RetainedWalDiagnostic()
    trace.begin("0/100")
    if invalid == "phase":
        trace.enter("private unregistered phase")
    elif invalid == "repeat_baseline":
        trace.begin("0/100")
    elif invalid == "negative_delta":
        trace.record("0/0", 0)
    else:
        trace.record("0/100", True)
    assert trace.note(_operands()) is None


@pytest.mark.parametrize("sizes,before", [([True], 0), (["text"], 0), ([1], True), ([1] * 26, 0)])
def test_malformed_bound_frame_is_refused_without_replacing_original_error(sizes, before):
    def bounds_check():
        relations = [SimpleNamespace(total_bytes=size) for size in sizes]
        request = SimpleNamespace(extra_data_upper_bound_bytes=8, extra_wal_upper_bound_bytes=8)
        before_bytes, wal_bytes = before, 9
        assert relations and request and before_bytes is not None and wal_bytes
        raise ValueError("synthetic bound refusal")

    with pytest.raises(ValueError) as captured:
        bounds_check()
    assert retained_bound_operands(captured.value, bounds_check, relation_limit=25) is None
    assert str(captured.value) == "synthetic bound refusal"


def _install_bound_probe(monkeypatch, wrapped):
    """Replace unavailable observations while retaining the actual bound check and wrapper."""
    from tests import test_network_registry_cms_prepared_pair_postgres as native

    async def no_work(*_args, **_kwargs):
        return None

    class Connection:
        calls = 0

        async def fetchval(self, query, start):
            self.calls += 1
            assert start == "0/100" and "pg_current_wal_insert_lsn" in query
            return 9

    connection = Connection()

    async def measure(*_args):
        return (SimpleNamespace(total_bytes=1),)

    prepared = SimpleNamespace(
        nonprofile_admission=SimpleNamespace(measure=measure, _relations={}),
        fhir=object(),
        address=SimpleNamespace(db_schema="synthetic"),
    )
    request = SimpleNamespace(extra_data_upper_bound_bytes=8, extra_wal_upper_bound_bytes=8)

    async def raw_admission(*_args):
        return SimpleNamespace(wal_start="0/100")

    async def execute(*_args, **_kwargs):
        await native.retention._raw_copy_admission(None, None, connection, 0)
        if not wrapped:
            await native.retention._stamp_prepared(None, None)

        async def actual_admission(*_args):
            await native.retention._assert_retained_bounds(connection, prepared, request, 0, "0/100")

        if wrapped:
            await native.epoch_capture._admit_epoch_copy(
                actual_admission, "before_copy_batch", "synthetic", "synthetic", 1
            )
        else:
            await actual_admission()

    for name in (
        "_source_evidence_sequence_state",
        "_reject_copy_batch_interruptions",
        "_reject_address_copy_interruptions",
    ):
        monkeypatch.setattr(native, name, no_work)
    monkeypatch.setattr(native.retention, "_raw_copy_admission", raw_admission)
    monkeypatch.setattr(native.retention, "_stamp_prepared", no_work)
    monkeypatch.setattr(native.retention, "prepare_registry_cms_source_pair", execute)
    return native, prepared, request, connection


@pytest.mark.asyncio
@pytest.mark.parametrize("wrapped", [False, True])
@pytest.mark.parametrize("diagnostic_error", [None, RuntimeError, asyncio.CancelledError])
async def test_actual_bound_failure_keeps_exception_and_notes_through_fhir_wrapper(
    monkeypatch, wrapped, diagnostic_error
):
    """The final reservation and intermediate admission retain their original failure identity."""
    native, prepared, request, connection = _install_bound_probe(monkeypatch, wrapped)
    if diagnostic_error is not None:

        async def unavailable(*_args):
            raise diagnostic_error("synthetic diagnostic failure")

        monkeypatch.setattr(native, "retained_wal_attribution", unavailable)
    expected = native.epoch_capture.FHIRSourceEpochError if wrapped else ValueError
    with pytest.raises(expected) as captured:
        await native._prepare_observed(prepared, None, request, ("synthetic", "synthetic"), None, monkeypatch)
    error = captured.value
    assert str(error) == (
        "fhir_source_epoch_copy_admission_refused" if wrapped else "registry_cms_retention_reservation_exceeded"
    )
    assert connection.calls == 1
    assert len(error.__notes__) == 2
    assert json.loads(error.__notes__[0].split(" ", 1)[1]) == _operands()
    phases = json.loads(error.__notes__[1].split(" ", 1)[1])
    assert phases["phase"] == ("raw_validation" if wrapped else "final_bounds")
    assert phases["wal_end_lsn"] == "0/109"
    if wrapped:
        assert error.__suppress_context__ is True
        assert isinstance(error.__context__, ValueError)
        assert error.__context__.__notes__ == error.__notes__
    assert (
        retained_bound_operands(
            ValueError("private text"),
            native.retention._assert_retained_bounds,
            relation_limit=25,
        )
        is None
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "start,size",
    [("private", 1), ("0/100", True), ("0/100", 0), ("0/100", MAX_INSPECTION_BYTES + 1), ("FFFFFFFF/FFFFFFFF", 1)],
)
async def test_attribution_refuses_unsafe_interval_before_connection_use(start, size):
    assert await retained_wal_attribution(object(), start, size) is None


def test_attribution_contains_only_closed_aggregate_metadata():
    records = [{"kind": "Heap/INSERT", "records": 2, "record_bytes": 100, "fpi_bytes": 8000, "total_bytes": 8100}]
    blocks = [
        {
            "reltablespace": 1663,
            "reldatabase": 16384,
            "relfilenode": 16400,
            "relforknumber": 0,
            "block_references": 2,
            "block_data_bytes": 10,
            "fpi_bytes": 8000,
            "relation_groups": 1,
        }
    ]
    document = _closed_attribution(records, blocks)
    assert document["record_types"] == records and document["top_physical_relations"] == blocks
    assert document["accounting_authority"] is False
    with pytest.raises(ValueError):
        _closed_attribution([{**records[0], "description": "synthetic private record"}], blocks)
    with pytest.raises(ValueError):
        _closed_attribution(records, [{**blocks[0], "fpi_bytes": True}])
    with pytest.raises(ValueError):
        _closed_attribution(records * 257, blocks)


def test_original_bound_operands_survive_a_cyclic_wrapper_cause():
    """Read original numeric operands through the wrapper without retaining messages."""

    def bounds_check():
        relations = [SimpleNamespace(total_bytes=2)]
        request = SimpleNamespace(extra_data_upper_bound_bytes=8, extra_wal_upper_bound_bytes=8)
        before_bytes, wal_bytes = 1, 9
        assert relations and request and before_bytes and wal_bytes
        raise ValueError("synthetic bound refusal")

    try:
        bounds_check()
    except ValueError as original:
        wrapper = RuntimeError("synthetic wrapper")
        wrapper.__cause__ = original
        original.__context__ = wrapper
        assert retained_bound_operands(wrapper, bounds_check, relation_limit=25) == _operands()
    unrelated = RuntimeError("synthetic unrelated")
    unrelated.__cause__ = unrelated
    assert retained_bound_operands(unrelated, bounds_check, relation_limit=25) is None
