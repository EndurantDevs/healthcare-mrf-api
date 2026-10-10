# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Diagnostic LSN reconstruction preserves the original global counter offset."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process.provider_directory_profile_capacity_control_projection import _settle_mutation_window
from tests.cms_registry_wal_support import failed_window_counter, wal_interval

_WINDOW_NUMBERS = 'NATIVE_WAL_WINDOW {"actual_wal":20,"pending_control":5,"pending_relation":0}'


def test_wal_interval_preserves_offset_and_segment_carry():
    admission = SimpleNamespace(initial_wal_lsn="1/FFFFFFF0", initial_wal_offset_bytes=100)
    assert wal_interval(admission, 108, 132) == {"start": "1/FFFFFFF8", "end": "2/10"}


@pytest.mark.parametrize(
    "origin,offset,before,after",
    [
        ("1/XYZ", 0, 1, 2),
        ("100000000/0", 0, 1, 2),
        ("0/0", True, 1, 2),
        ("0/0", 2, 1, 3),
        ("0/0", 0, 2, 1),
        ("0/0", 0, 1, 1),
        ("0/0", 0, 1, 16 * 1024 * 1024 + 2),
        ("FFFFFFFF/FFFFFFFF", 0, 1, 2),
    ],
)
def test_wal_interval_rejects_unbounded_or_invalid_counters(origin, offset, before, after):
    admission = SimpleNamespace(initial_wal_lsn=origin, initial_wal_offset_bytes=offset)
    with pytest.raises(ValueError, match="WAL interval"):
        wal_interval(admission, before, after)


def test_counter_without_original_settlement_is_unavailable():
    admission = SimpleNamespace()
    try:
        raise RuntimeError("unrelated failure")
    except RuntimeError as error:
        assert failed_window_counter(error, admission, 100) is None


def test_counter_uses_the_failed_settlement_without_another_query():
    owner = object()
    tracker = SimpleNamespace(pending_control_wal_bytes={owner: 5}, pending_relation_wal_bytes={})
    admission = SimpleNamespace(wal_tracker=tracker)
    fhir = SimpleNamespace(
        _profile_capacity_remaining_ms=AsyncMock(),
        _provider_directory_profile_current_wal_bytes=AsyncMock(return_value=99),
    )
    with pytest.raises(RuntimeError, match="window_wal_overrun") as caught:
        asyncio.run(_settle_mutation_window(fhir, admission, owner, None, 100, 0))
    assert failed_window_counter(caught.value, admission, 100) == 99
    assert failed_window_counter(caught.value, SimpleNamespace(), 100) is None
    assert failed_window_counter(caught.value, admission, 99) is None
    fhir._provider_directory_profile_current_wal_bytes.assert_awaited_once_with(admission)


def _rows(values):
    result = SimpleNamespace()
    result.mappings = lambda: SimpleNamespace(fetchmany=lambda limit: values[:limit])
    return result


def test_record_links_preserve_commit_and_record_block_xid_relationships():
    from tests.cms_registry_wal_support import _record_links

    records = [
        {"start_lsn": "0/100", "end_lsn": "0/140", "xid": 10},
        {"start_lsn": "0/140", "end_lsn": "0/180", "xid": 10},
    ]
    blocks = [{"start_lsn": "0/100", "end_lsn": "0/140", "xid": 10, "block_id": 0}]
    connection = SimpleNamespace(
        scalar=AsyncMock(return_value=True), execute=AsyncMock(side_effect=[_rows(records), _rows(blocks)])
    )
    result = asyncio.run(_record_links(connection, {"start": "0/100", "end": "0/180"}))
    assert result["status"] == "complete"
    assert result["records"] == records  # The block-free commit remains present.
    assert result["blocks"] == blocks
    queries = [str(call.args[0]) for call in connection.execute.await_args_list]
    assert "xid::text::bigint" in queries[0] and "xid::text::bigint" in queries[1]
    assert "LIMIT 257" in queries[0] and "LIMIT 513" in queries[1]
    assert ",false)" in queries[1] and "ORDER BY wal.start_lsn,wal.block_id" in queries[1]
    assert all("description" not in query and "block_fpi_data" not in query for query in queries)
    assert result["native_decoder_allocation_bounded_by_row_limits"] is False
    assert result["accounting_authority"] is False


@pytest.mark.parametrize("end", ["0/100101", "invalid"])
def test_record_links_reject_large_or_invalid_input_before_query(end):
    from tests.cms_registry_wal_support import _record_links

    connection = SimpleNamespace(execute=AsyncMock())
    if end == "invalid":
        with pytest.raises(ValueError):
            asyncio.run(_record_links(connection, {"start": "0/100", "end": end}))
    else:
        result = asyncio.run(_record_links(connection, {"start": "0/100", "end": end}))
        assert result["status"] == "interval_limit_exceeded"
    connection.execute.assert_not_awaited()


@pytest.mark.parametrize("block_overflow", [False, True])
def test_record_link_overflow_is_explicit_and_does_not_claim_complete(block_overflow):
    from tests.cms_registry_wal_support import _record_links

    values = [_rows([]), _rows([{}] * 513)] if block_overflow else [_rows([{}] * 257)]
    connection = SimpleNamespace(scalar=AsyncMock(return_value=True), execute=AsyncMock(side_effect=values))
    result = asyncio.run(_record_links(connection, {"start": "0/100", "end": "0/180"}))
    assert result == {"status": "row_limit_exceeded", "records": [], "blocks": []}


def test_failed_owner_snapshot_uses_original_native_terminal_state_and_holds_tails():
    import contextvars
    import datetime
    import json
    from decimal import Decimal

    from process import provider_directory_backend_wal_diagnostic as diagnostic
    from process import provider_directory_owned_wal_transaction as owned
    from tests.cms_registry_wal_support import failed_window_owners

    identity = diagnostic.BackendWalIdentity(
        10, datetime.datetime.now(datetime.timezone.utc), 20, "synthetic-test", "123", None
    )
    baseline = diagnostic.BackendWalSnapshot(identity, 1, 0, Decimal(1), 0, "0/100")
    final = diagnostic.BackendWalSnapshot(identity, 2, 0, Decimal(69), 0, "0/180")
    measurement = diagnostic.BackendWalDiagnosticResult(baseline, final, 1, 0, Decimal(68), 0, 128)
    outcome = owned.OwnedWalTransaction(
        commit_state="uncertain",
        status="commit_uncertain_accounting_incomplete",
        measurement=measurement,
        owner_measurement=measurement,
    )
    owner = object()
    window = (owner, "evidence_stage")
    current = contextvars.ContextVar("synthetic_window", default=window)
    tracker = SimpleNamespace(
        owned_evidence_preflight_native_outcomes=[],
        owned_evidence_wave_native_outcomes=[],
        owned_control_transaction_groups=[],
    )
    admission = SimpleNamespace(wal_tracker=tracker)
    tracker.owned_evidence_preflight_native_outcomes.append(
        {
            "admission": admission,
            "window": window,
            "coordinates": (),
            "readers": [{"outcome": outcome}],
        }
    )
    fhir = SimpleNamespace(
        _PROFILE_CAPACITY_MUTATION_WINDOW=current,
        BackendWalDiagnosticResult=diagnostic.BackendWalDiagnosticResult,
        profile_owned_wal=owned,
    )
    owner_snapshot = failed_window_owners(fhir, admission, owner, "evidence_stage")
    capture = owner_snapshot["owners"][0]
    assert capture["commit_state"] == "uncertain"
    assert capture["original_status"] == outcome.status
    assert capture["body"]["wal_bytes"] == 68
    assert capture["before_setup_after_restoration"]["global_insert_lsn_span"] == 128
    assert capture["cleanup_complete"] is False and capture["whole_owner_complete"] is False
    assert owner_snapshot["parent_coverage_complete"] is False and owner_snapshot["accounting_authority"] is False
    assert "synthetic-test" not in json.dumps(owner_snapshot)
    tracker.owned_evidence_preflight_native_outcomes[0]["window"] = (object(), "evidence_stage")
    assert failed_window_owners(fhir, admission, owner, "evidence_stage")["owners"] == []
    with pytest.raises(ValueError, match="custody"):
        failed_window_owners(fhir, admission, object(), "evidence_stage")


def test_original_complete_publication_failure_capture_runs_once_and_retains_error(monkeypatch, capsys):
    import contextvars

    from tests import cms_registry_complete_publication_postgres_support as native
    from tests import cms_registry_wal_support as wal

    owner = object()
    current = contextvars.ContextVar("synthetic_window", default=(owner, "evidence_stage"))
    tracker = SimpleNamespace(
        pending_control_wal_bytes={owner: 5},
        pending_relation_wal_bytes={},
        pending_metadata_wal_bytes=0,
        relation_refs_by_class={"evidence_stage": {"stage"}},
        target_bytes_before={},
        pending_growth_bytes={},
        accounted_relation_wal_bytes={},
    )
    admission = SimpleNamespace(
        wal_tracker=tracker, geometry=SimpleNamespace(reservation_bytes_by_storage_class={"wal": 1000})
    )
    fhir = SimpleNamespace(
        _PROFILE_CAPACITY_MUTATION_WINDOW=current,
        _profile_capacity_remaining_ms=AsyncMock(),
        _provider_directory_profile_current_wal_bytes=AsyncMock(return_value=120),
        _provider_directory_profile_capacity_relation_bytes=AsyncMock(return_value=0),
        _provider_directory_profile_capacity_relation_cap=Mock(
            return_value=SimpleNamespace(max_scratch_bytes=0, max_target_growth_bytes=0, max_wal_bytes=19)
        ),
    )
    inspect_records = AsyncMock(side_effect=asyncio.CancelledError())
    snapshot = SimpleNamespace(initial_wal_lsn="0/100", initial_wal_offset_bytes=0)
    monkeypatch.setattr(
        wal, "failed_window_owners", lambda *args: {"owners": [], "accounting_authority": False}, raising=False
    )
    monkeypatch.setattr(wal, "wal_interval", lambda *args: wal_interval(snapshot, 100, 120))
    monkeypatch.setattr(wal, "failed_window_records", inspect_records)
    native.observe_geometry(monkeypatch, object())
    for _ in range(2):
        with pytest.raises(RuntimeError, match="window_data_or_wal_overrun"):
            asyncio.run(
                native.control_projection._settle_mutation_window(fhir, admission, owner, "evidence_stage", 100, 0)
            )
    assert tracker.pending_control_wal_bytes == {owner: 5}
    assert tracker.accounted_relation_wal_bytes == {}
    assert fhir._provider_directory_profile_current_wal_bytes.await_count == 2
    inspect_records.assert_awaited_once()
    output = capsys.readouterr().out
    assert output.splitlines() == [
        "NATIVE_WAL_OWNERS",
        _WINDOW_NUMBERS,
        "NATIVE_WAL_RECORDS_UNAVAILABLE",
    ]


@pytest.mark.parametrize("private", [False, True])
def test_failed_receipts_stay_private_and_default_status_is_neutral(monkeypatch, capsys, private):
    from tests import cms_registry_complete_publication_postgres_support as native
    from tests import cms_registry_wal_support as wal

    failure = RuntimeError("synthetic settlement failure")
    original = AsyncMock(side_effect=failure)
    owners_by_field = {"owners": [dict(pid=12345, database_oid=43210, start_lsn="A/111", end_lsn="A/222")]}
    records_by_field = {"interval": {"start": "A/111", "end": "A/222"}, "records": [dict(xid=67890, relfilenode=98765)]}
    capture_records = AsyncMock(return_value=records_by_field)
    owner = object()
    admission = SimpleNamespace(
        wal_tracker=SimpleNamespace(
            pending_control_wal_bytes={owner: 5},
            pending_relation_wal_bytes={},
            pending_metadata_wal_bytes=0,
        ),
        geometry=SimpleNamespace(reservation_bytes_by_storage_class={"wal": 1000}),
    )
    monkeypatch.setattr(native.control_projection, "_settle_mutation_window", original)
    monkeypatch.setattr(wal, "failed_window_counter", lambda *args: 120)
    monkeypatch.setattr(wal, "failed_window_owners", lambda *args: owners_by_field)
    monkeypatch.setattr(wal, "wal_interval", lambda *args: records_by_field["interval"])
    monkeypatch.setattr(wal, "failed_window_records", capture_records)
    captured_receipts = []
    sink = (lambda label, payload: captured_receipts.append((label, payload))) if private else None
    native.observe_geometry(monkeypatch, object(), private_capture=sink)
    for _ in range(2):
        with pytest.raises(RuntimeError) as caught:
            asyncio.run(
                native.control_projection._settle_mutation_window(object(), admission, owner, "evidence_stage", 100, 0)
            )
        assert caught.value is failure
    assert original.await_count == 2
    capture_records.assert_awaited_once()
    assert capsys.readouterr().out.splitlines() == ["NATIVE_WAL_OWNERS", _WINDOW_NUMBERS, "NATIVE_WAL_RECORDS"]
    if private:
        assert (
            captured_receipts[0] == ("NATIVE_WAL_OWNERS", owners_by_field)
            and captured_receipts[0][1] is owners_by_field
        )
        assert captured_receipts[1] == (
            "NATIVE_WAL_WINDOW",
            {
                "relation_name": "evidence_stage",
                "actual_wal": 20,
                "pending_control": 5,
                "pending_relation": 0,
                "pending_metadata": 0,
                "signed_reservation": {"wal": 1000},
            },
        )
        assert (
            captured_receipts[2] == ("NATIVE_WAL_RECORDS", records_by_field)
            and captured_receipts[2][1] is records_by_field
        )
        captured_receipts[1][1]["signed_reservation"]["wal"] = 0
        assert admission.geometry.reservation_bytes_by_storage_class == {"wal": 1000}
    else:
        assert captured_receipts == []


@pytest.mark.parametrize("private", [False, True])
def test_geometry_status_omits_values_and_private_sink_preserves_computed_changes(monkeypatch, capsys, private):
    from dataclasses import dataclass
    from unittest.mock import Mock

    from tests import cms_registry_complete_publication_postgres_support as native

    @dataclass
    class Geometry:
        value: str

    before = SimpleNamespace(geometry=Geometry("synthetic-before"))
    after = SimpleNamespace(geometry=Geometry("synthetic-after"))
    original = Mock(side_effect=[before, after])
    monkeypatch.setattr(native.source.fhir, "_profile_admission_geometry", original)
    captured_receipts = []
    sink = (lambda label, payload: captured_receipts.append((label, payload))) if private else None
    native.observe_geometry(monkeypatch, object(), private_capture=sink)
    assert (
        native.source.fhir._profile_admission_geometry(
            SimpleNamespace(control_wal_plan_input=Geometry("plan-before")), None
        )
        is before
    )
    assert (
        native.source.fhir._profile_admission_geometry(
            SimpleNamespace(control_wal_plan_input=Geometry("plan-after")), None
        )
        is after
    )
    assert capsys.readouterr().out.splitlines() == [
        "NATIVE_GEOMETRY_CHANGED",
        "NATIVE_CONTROL_PLAN_CHANGED",
    ]
    assert captured_receipts == (
        [
            (
                "NATIVE_GEOMETRY_CHANGED",
                {"value": ["synthetic-before", "synthetic-after"]},
            ),
            ("NATIVE_CONTROL_PLAN_CHANGED", {"value": ["plan-before", "plan-after"]}),
        ]
        if private
        else []
    )


@pytest.mark.parametrize(
    "callback_error",
    [RuntimeError("synthetic callback failure"), asyncio.CancelledError()],
)
def test_private_capture_error_preserves_original_settlement_error(monkeypatch, capsys, callback_error):
    from unittest.mock import Mock

    from tests import cms_registry_complete_publication_postgres_support as native
    from tests import cms_registry_wal_support as wal

    failure = RuntimeError("synthetic settlement failure")
    original = AsyncMock(side_effect=failure)
    monkeypatch.setattr(native.control_projection, "_settle_mutation_window", original)
    monkeypatch.setattr(wal, "failed_window_counter", lambda *args: None)
    monkeypatch.setattr(wal, "failed_window_owners", lambda *args: {"owners": []})
    admission = SimpleNamespace(
        wal_tracker=SimpleNamespace(
            pending_control_wal_bytes={},
            pending_relation_wal_bytes={},
            pending_metadata_wal_bytes=0,
        ),
        geometry=SimpleNamespace(reservation_bytes_by_storage_class={"wal": 1000}),
    )
    private_capture = Mock(side_effect=callback_error)
    native.observe_geometry(monkeypatch, object(), private_capture=private_capture)
    with pytest.raises(RuntimeError) as caught:
        asyncio.run(native.control_projection._settle_mutation_window(object(), admission, object(), None, 100, 0))
    assert caught.value is failure
    original.assert_awaited_once()
    assert private_capture.call_count == 2
    assert capsys.readouterr().out.splitlines() == [
        "NATIVE_WAL_OWNERS",
        "NATIVE_PRIVATE_CAPTURE_UNAVAILABLE",
        'NATIVE_WAL_WINDOW {"actual_wal":null,"pending_control":0,"pending_relation":0}',
        "NATIVE_PRIVATE_CAPTURE_UNAVAILABLE",
    ]


def test_original_settlement_cancellation_does_not_activate_capture(monkeypatch):
    from tests import cms_registry_complete_publication_postgres_support as native
    from tests import cms_registry_wal_support as wal

    failure = asyncio.CancelledError()
    fhir = SimpleNamespace(_profile_capacity_remaining_ms=AsyncMock(side_effect=failure))
    capture = AsyncMock()
    monkeypatch.setattr(wal, "failed_window_records", capture)
    native.observe_geometry(monkeypatch, object())
    with pytest.raises(asyncio.CancelledError) as caught:
        asyncio.run(native.control_projection._settle_mutation_window(fhir, object(), object(), None, 0, 0))
    assert caught.value is failure
    capture.assert_not_awaited()


def test_record_links_reject_unflushed_end():
    from tests.cms_registry_wal_support import _record_links

    connection = SimpleNamespace(scalar=AsyncMock(return_value=False), execute=AsyncMock())
    result = asyncio.run(_record_links(connection, {"start": "0/100", "end": "0/180"}))
    assert result == {"status": "end_not_flushed", "records": [], "blocks": []}
    connection.execute.assert_not_awaited()


def test_successful_original_settlement_returns_same_result_without_capture(monkeypatch):
    from tests import cms_registry_complete_publication_postgres_support as native
    from tests import cms_registry_wal_support as wal

    result = object()
    original = AsyncMock(return_value=result)
    capture = AsyncMock()
    monkeypatch.setattr(native.control_projection, "_settle_mutation_window", original)
    monkeypatch.setattr(wal, "failed_window_records", capture)
    native.observe_geometry(monkeypatch, object())
    assert (
        asyncio.run(native.control_projection._settle_mutation_window(object(), object(), object(), None, 0, 0))
        is result
    )
    original.assert_awaited_once()
    capture.assert_not_awaited()


def test_consumed_record_helper_retains_original_aggregates_and_links():
    from contextlib import asynccontextmanager

    from tests.cms_registry_wal_support import failed_window_records

    class MappingRows(list):
        def fetchmany(self, limit):
            return self[:limit]

    def result(rows):
        return SimpleNamespace(mappings=lambda: MappingRows(rows))

    record_totals = [{"resource_manager": "XLOG", "record_type": "FPI_FOR_HINT", "records": 1}]
    block_totals = [{"reldatabase": 20, "relfilenode": 30, "block_references": 1}]
    links = [{"start_lsn": "0/100", "end_lsn": "0/180", "xid": 0}]
    blocks = [{"start_lsn": "0/100", "xid": 0, "block_id": 0}]
    connection = SimpleNamespace(
        scalar=AsyncMock(side_effect=["hc_cms_admission_test_" + "a" * 32, True]),
        exec_driver_sql=AsyncMock(),
        execute=AsyncMock(side_effect=[result(record_totals), result(block_totals), result(links), result(blocks)]),
    )

    @asynccontextmanager
    async def begin():
        yield connection

    interval_by_bound = {"start": "0/100", "end": "0/180"}
    database = SimpleNamespace(engine=SimpleNamespace(begin=begin))
    captured = asyncio.run(failed_window_records(database, interval_by_bound))
    assert captured["interval"] is interval_by_bound
    assert captured["records"] == record_totals and captured["blocks"] == block_totals
    assert captured["record_links"]["records"] == links and captured["record_links"]["blocks"] == blocks
    assert all(call.args[1] is interval_by_bound for call in connection.execute.await_args_list)
    assert "show_data" not in str(connection.execute.await_args_list[-1].args[0])


def test_owner_member_bound_rejects_before_detaching_outcomes():
    import contextvars

    from tests.cms_registry_wal_support import failed_window_owners

    owner = object()
    window = (owner, "evidence_stage")
    tracker = SimpleNamespace(owned_evidence_preflight_native_outcomes=[])
    admission = SimpleNamespace(wal_tracker=tracker)
    tracker.owned_evidence_preflight_native_outcomes = [
        {
            "admission": admission,
            "window": window,
            "readers": [object()] * 33,
        }
    ]
    fhir = SimpleNamespace(_PROFILE_CAPACITY_MUTATION_WINDOW=contextvars.ContextVar("bounded", default=window))
    with pytest.raises(ValueError, match="member diagnostic limit"):
        failed_window_owners(fhir, admission, owner, "evidence_stage")


@pytest.mark.parametrize("current_worker", [False, True])
def test_worker_and_control_provenance_keep_owner_deduplication(current_worker):
    import contextvars
    from unittest.mock import Mock

    from process import provider_directory_backend_wal_diagnostic as diagnostic
    from process import provider_directory_owned_wal_transaction as owned
    from tests.cms_registry_wal_support import failed_window_owners

    owner, task, build, identity = object(), object(), object(), object()
    current = (owner, "evidence_stage")
    worker_outcome, direct_outcome = owned.OwnedWalTransaction(), owned.OwnedWalTransaction()
    worker_by_field = {"outcome": worker_outcome, "capture": {"committed": True, "measurement_status": "complete"}}
    tracker = SimpleNamespace(
        owned_evidence_preflight_native_outcomes=[],
        owned_evidence_wave_native_outcomes=[],
        owned_control_transaction_groups=[],
    )
    admission = SimpleNamespace(wal_tracker=tracker)
    tracker.owned_evidence_wave_native_outcomes.append(
        {
            "admission": admission,
            "window": current,
            "build": build,
            "coordinates": (("synthetic-coordinate", 17),),
            "workers": [
                {"owner": worker_by_field, "coordinate": "synthetic-coordinate", "task": task},
                {"outcome": direct_outcome},
            ],
        }
    )
    group_by_field = dict(admission=admission, window=current, identity=identity, original_identity=identity)
    group_by_field.update(outcome=worker_outcome, original_outcome=worker_outcome)
    tracker.owned_control_transaction_groups.extend(
        [
            group_by_field,
            group_by_field,
            dict(group_by_field, original_outcome=owned.OwnedWalTransaction()),
            dict(group_by_field, original_identity=object()),
        ]
    )
    current_evidence_worker = Mock(return_value=current_worker)
    fhir = SimpleNamespace(
        _PROFILE_CAPACITY_MUTATION_WINDOW=contextvars.ContextVar("synthetic_window", default=current),
        BackendWalDiagnosticResult=diagnostic.BackendWalDiagnosticResult,
        profile_owned_wal=owned,
        _is_current_evidence_worker=current_evidence_worker,
    )
    snapshot = failed_window_owners(fhir, admission, owner, "evidence_stage")
    current_evidence_worker.assert_called_once_with(
        worker_by_field, admission, current, task, build, ("synthetic-coordinate", 17)
    )
    assert len(snapshot["owners"]) == 2
    worker = snapshot["owners"][0 if current_worker else 1]
    assert worker["family"] == ("payload" if current_worker else "control")
    assert worker["original_callback"] == (
        {"committed": True, "measurement_complete": True} if current_worker else None
    )
    assert snapshot["parent_coverage_complete"] is False and snapshot["accounting_authority"] is False


def _compact_owner_fixture():
    """Build native-shaped counters for pure custody checks, never acceptance."""
    import datetime
    from decimal import Decimal

    from process import provider_directory_backend_wal_diagnostic as diagnostic
    from process import provider_directory_owned_wal_transaction as owned

    identity = diagnostic.BackendWalIdentity(
        10, datetime.datetime.now(datetime.timezone.utc), 20, "synthetic", "123", None
    )
    baseline = diagnostic.BackendWalSnapshot(identity, 1, 0, Decimal(1), 0, "0/100")
    final = diagnostic.BackendWalSnapshot(identity, 2, 0, Decimal(69), 0, "0/180")
    return owned.OwnedWalTransaction(
        commit_state="confirmed",
        status="committed_measured",
        cleanup_complete=True,
        measurement=diagnostic.BackendWalDiagnosticResult(baseline, final, 1, 0, Decimal(68), 0, 128),
    )


@pytest.fixture
def fhir_module(monkeypatch):
    """Exercise custody with the package's genuine CLI command still bound."""
    import importlib

    import process

    command = process.process_group.commands["provider-directory-fhir"]
    monkeypatch.setattr(process, "provider_directory_fhir", command)
    return importlib.import_module("process.provider_directory_fhir")


@pytest.mark.asyncio
@pytest.mark.parametrize("wrong_field", [None, "reader_admission", "reader_window", "reader_task", "native_outcome"])
async def test_compact_original_reader_capture_reaches_custody_predicate(monkeypatch, wrong_field, fhir_module):
    fhir = fhir_module
    from tests.cms_registry_wal_support import failed_window_owners

    owner, build, batch = object(), object(), object()
    window = (owner, "profile_stage")
    tracker = SimpleNamespace(owned_compact_preflight_native_outcomes=[])
    admission = SimpleNamespace(wal_tracker=tracker)
    task = asyncio.create_task(asyncio.sleep(0))
    await task
    projection_capture_by_field = dict(
        native_outcome=_compact_owner_fixture(),
        reader_task=task,
        coordinate=0,
        reader_scope="compact_projection",
        reader_admission=admission,
        reader_window=window,
    )
    stage_capture_by_field = dict(
        native_outcome=_compact_owner_fixture(),
        reader_task=asyncio.current_task(),
        coordinate="stage",
        reader_scope="compact_stage_preflight",
        reader_admission=admission,
        reader_window=window,
    )
    token = fhir._PROFILE_CAPACITY_MUTATION_WINDOW.set(window)
    try:
        fhir._retain_profile_preflight_native_outcomes(
            build,
            [(0, batch)],
            admission,
            {"native_readers": [(build, batch, 0, task, projection_capture_by_field)]},
            {asyncio.current_task(): stage_capture_by_field},
            compact=True,
        )
        retained = tracker.owned_compact_preflight_native_outcomes[0]
        assert retained["status"] == "complete"
        assert retained["readers"][0]["capture"] is projection_capture_by_field
        assert retained["readers"][1]["capture"] is stage_capture_by_field
        original = fhir._is_native_preflight_reader_complete
        predicate = Mock(wraps=original)
        monkeypatch.setattr(fhir, "_is_native_preflight_reader_complete", predicate)
        if wrong_field is not None:
            projection_capture_by_field[wrong_field] = object()
        snapshot = failed_window_owners(fhir, admission, owner, "profile_stage")
        assert snapshot["owners"][0]["current_custody_verified"] is (wrong_field is None)
        assert snapshot["owners"][1]["current_custody_verified"] is True
        if wrong_field != "native_outcome":
            assert predicate.call_args_list[0].args[2] is projection_capture_by_field
        assert not snapshot["parent_coverage_complete"] and not snapshot["accounting_authority"]
    finally:
        fhir._PROFILE_CAPACITY_MUTATION_WINDOW.reset(token)


@pytest.mark.parametrize("wrong_identity", [None, "admission", "window", "task"])
def test_compact_worker_uses_original_predicate_and_deduplicates(wrong_identity, fhir_module):
    fhir = fhir_module
    from tests.cms_registry_wal_support import failed_window_owners

    owner, task, build, batch = object(), object(), object(), object()
    window = (owner, "profile_stage")
    tracker = SimpleNamespace(owned_compact_wave_native_outcomes=[])
    admission = SimpleNamespace(wal_tracker=tracker)
    worker_by_field = dict(
        task=task,
        build=build,
        batch=batch,
        coordinate=0,
        admission=admission,
        window=window,
        outcome=_compact_owner_fixture(),
        capture={"committed": True},
    )
    wave_by_field = dict(
        admission=admission,
        window=window,
        build=build,
        coordinates=((0, batch),),
        workers=[dict(coordinate=0, task=task, owner=worker_by_field)] * 2,
    )
    tracker.owned_compact_wave_native_outcomes.append(wave_by_field)
    if wrong_identity is not None:
        worker_by_field[wrong_identity] = object()
    token = fhir._PROFILE_CAPACITY_MUTATION_WINDOW.set(window)
    try:
        snapshot = failed_window_owners(fhir, admission, owner, "profile_stage")
        assert len(snapshot["owners"]) == (1 if wrong_identity is None else 0)
        if wrong_identity is None:
            assert snapshot["owners"][0]["family"] == "compact_payload"
            assert snapshot["owners"][0]["current_custody_verified"] is True
    finally:
        fhir._PROFILE_CAPACITY_MUTATION_WINDOW.reset(token)


def test_compact_owner_limit_applies_across_reader_and_worker_families(fhir_module):
    fhir = fhir_module
    from tests.cms_registry_wal_support import failed_window_owners

    owner, task, build, batch = object(), object(), object(), object()
    window = (owner, "profile_stage")
    tracker = SimpleNamespace(owned_compact_preflight_native_outcomes=[], owned_compact_wave_native_outcomes=[])
    admission = SimpleNamespace(wal_tracker=tracker)
    tracker.owned_compact_preflight_native_outcomes.append(
        dict(admission=admission, window=window, readers=[dict(outcome=_compact_owner_fixture()) for _ in range(32)])
    )
    worker_by_field = dict(
        task=task,
        build=build,
        batch=batch,
        coordinate=0,
        admission=admission,
        window=window,
        outcome=_compact_owner_fixture(),
    )
    tracker.owned_compact_wave_native_outcomes.append(
        dict(
            admission=admission,
            window=window,
            build=build,
            coordinates=((0, batch),),
            workers=[dict(coordinate=0, task=task, owner=worker_by_field)],
        )
    )
    token = fhir._PROFILE_CAPACITY_MUTATION_WINDOW.set(window)
    try:
        with pytest.raises(ValueError, match="owner diagnostic limit"):
            failed_window_owners(fhir, admission, owner, "profile_stage")
    finally:
        fhir._PROFILE_CAPACITY_MUTATION_WINDOW.reset(token)


@pytest.mark.parametrize(
    "unsupported",
    [True, -1, 1.0, "private-value", {"pid": 999}, object(), 2**64, 1 << 16384],
    ids=["boolean", "negative", "float", "text", "mapping", "object", "overflow", "huge"],
)
def test_window_numbers_whitelist_preserves_private_capture(monkeypatch, capsys, unsupported):
    from tests import cms_registry_complete_publication_postgres_support as native

    payload_by_field = {
        "actual_wal": unsupported,
        "pending_control": 7,
        "pending_relation": None,
        "relation_name": "private-relation",
        "owner": {"pid": 999, "lsn": "A/B", "resource": "private-resource"},
    }
    original_error = RuntimeError("synthetic original error")
    private_records = []

    def original_observer(monkeypatch, database, report):
        report("NATIVE_WAL_WINDOW", payload_by_field)
        raise original_error

    monkeypatch.setattr(native, "_observe_admission_geometry", lambda *arguments: None)
    monkeypatch.setattr(native, "_observe_mutation_window", original_observer)
    with pytest.raises(RuntimeError) as observed:
        native.observe_geometry(monkeypatch, object(), private_capture=lambda *record: private_records.append(record))
    assert observed.value is original_error
    assert private_records == [("NATIVE_WAL_WINDOW", payload_by_field)]
    assert private_records[0][1] is payload_by_field
    assert capsys.readouterr().out == (
        'NATIVE_WAL_WINDOW {"actual_wal":null,"pending_control":7,"pending_relation":null}\n'
    )


def test_window_numbers_accept_native_unsigned_maximum(monkeypatch, capsys):
    from tests import cms_registry_complete_publication_postgres_support as native

    payload_by_field = {"actual_wal": 2**64 - 1, "pending_control": 0, "pending_relation": 2**64 - 1}
    captured_records = []
    monkeypatch.setattr(native, "_observe_admission_geometry", lambda *arguments: None)
    monkeypatch.setattr(
        native,
        "_observe_mutation_window",
        lambda monkeypatch, database, report: report("NATIVE_WAL_WINDOW", payload_by_field),
    )
    native.observe_geometry(monkeypatch, object(), private_capture=lambda *record: captured_records.append(record))
    assert captured_records[0][1] is payload_by_field
    assert capsys.readouterr().out == (
        'NATIVE_WAL_WINDOW {"actual_wal":18446744073709551615,"pending_control":0,'
        '"pending_relation":18446744073709551615}\n'
    )
