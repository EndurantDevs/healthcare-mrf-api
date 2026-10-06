# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Portable SQL preservation and page ownership checks; no database required."""

from __future__ import annotations

import asyncio
import datetime as dt
import re
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, call

import pytest
from sqlalchemy.dialects.postgresql import dialect
from sqlalchemy.exc import DBAPIError

import process.custom_import.admission_sql as admission
import process.custom_import.build_source as staging
from process.custom_import.runner_types import CandidateRunnerError, LeaseAuthorityLost
from tests.test_custom_import_build_source import _request
from tests.test_custom_import_segmented_runner import _install_flow, _policy, _run

_ROOT = Path(__file__).resolve().parents[1]


def _leaf():
    historical = (_ROOT / "alembic/sql/custom_import_bulk_snapshot_writers/snapshot_writers.sql").read_text()
    return historical.split("CREATE FUNCTION __CANDIDATE__.admit_custom_import_build_page(", 1)[1].split(
        "$function$;", 1
    )[0]


def _unbound(query):
    for name in admission._BUILD_FIELDS:
        query = re.sub(rf"(?<!:):{name}\b", f"b.{name}", query)
    query = query.replace("CAST(:memberships AS text)", "memberships::text")
    return re.sub(r"(?<!:):(definition_streams|memberships|decisions)\b", r"\1", query)


def _compact(query):
    return " ".join(query.split())


def test_decision_ctes_keep_canonical_null_duplicate_membership_and_error_order():
    historical = _leaf()
    ordinary = _unbound(admission._queries()[1])
    old_start = historical.split("    WITH\n", 1)[1].split("    sized AS MATERIALIZED (", 1)[0]
    new_start = ordinary.split("WITH\n", 1)[1].split("logical_rows AS MATERIALIZED (", 1)[0]
    old_start = old_start.replace(
        "LIMIT b.page_row_limit", "LIMIT (:physical_row_cap/b.page_row_limit)*b.page_row_limit"
    )
    assert _compact(new_start) == _compact(old_start)
    old_end = historical.split("    primary_codes AS MATERIALIZED (", 1)[1].split("    SELECT (SELECT f.problem", 1)[0]
    new_end = ordinary.split("primary_codes AS MATERIALIZED (", 1)[1].split("SELECT (SELECT f.problem", 1)[0]
    assert _compact(new_end) == _compact(old_end)
    # Full equality above covers first_child_rejection and earliest failure occurrence/stage.
    assert "first_child_rejection AS MATERIALIZED" in ordinary
    assert "ORDER BY f.occurrence_id,f.stage LIMIT 1" in ordinary


def test_physical_prefix_uses_ordinal_groups_real_eof_and_first_group_fallback():
    query = _unbound(admission._queries()[1])
    prefix = query.split("logical_rows AS MATERIALIZED (", 1)[1].split("primary_codes AS MATERIALIZED (", 1)[0]
    assert _compact(prefix) == _compact("""
        SELECT m.*,(row_number() OVER (ORDER BY m.occurrence_id)-1)/b.page_row_limit logical_group
        FROM metadata m
    ),
    sized AS MATERIALIZED (
        SELECT m.*,sum(m.raw_bytes::bigint) OVER (
            PARTITION BY m.logical_group ORDER BY m.occurrence_id ROWS UNBOUNDED PRECEDING) prefix_bytes
        FROM logical_rows m
    ),
    logical_groups AS MATERIALIZED (
        SELECT s.logical_group,count(*) row_count,sum(s.raw_bytes::bigint) group_bytes,
            max(s.occurrence_id) last_id
        FROM sized s GROUP BY s.logical_group
    ),
    group_bounds AS MATERIALIZED (
        SELECT g.*,sum(g.group_bytes) OVER (ORDER BY g.logical_group ROWS UNBOUNDED PRECEDING) physical_bytes,
            CASE WHEN g.row_count=b.page_row_limit THEN true ELSE NOT EXISTS (
                SELECT 1 FROM __CANDIDATE__.custom_import_build_occurrence x
                WHERE x.build_id=b.build_id AND x.origin='source' AND x.occurrence_id>g.last_id
            ) END complete_group
        FROM logical_groups g
    ),
    group_boundary AS MATERIALIZED (
        SELECT g.logical_group,g.group_bytes>b.page_byte_limit logical_byte_overflow
        FROM group_bounds g
        WHERE NOT g.complete_group OR g.group_bytes>b.page_byte_limit OR g.physical_bytes>268435456
        ORDER BY g.logical_group LIMIT 1
    ),
    page AS MATERIALIZED (
        SELECT s.* FROM sized s
        WHERE s.logical_group<coalesce((SELECT g.logical_group FROM group_boundary g),100000)
          OR (s.logical_group=0 AND s.prefix_bytes<=b.page_byte_limit AND EXISTS (
            SELECT 1 FROM group_boundary g WHERE g.logical_group=0 AND g.logical_byte_overflow
          ))
    ),
    byte_boundary AS MATERIALIZED (
        SELECT s.occurrence_id,s.raw_bytes FROM sized s
        WHERE s.logical_group=0 AND s.prefix_bytes>b.page_byte_limit AND EXISTS (
            SELECT 1 FROM group_boundary g WHERE g.logical_group=0 AND g.logical_byte_overflow
        ) ORDER BY s.occurrence_id LIMIT 1
    ),
    """)


def test_apply_ctes_keep_ordered_rejections_targets_and_resolution_identity():
    historical = _leaf().split("    WITH\n    page_decisions AS MATERIALIZED (", 1)[1]
    ordinary = _unbound(admission._queries()[2]).split("page_decisions AS MATERIALIZED (", 1)[1]
    ordinary = ordinary.replace(
        "nextval(CAST(:rejection_sequence AS regclass))",
        "nextval('__CONTROL__.custom_import_rejection_rejection_id_seq'::regclass)",
    )
    assert _compact(ordinary.split("SELECT (SELECT count(*) FROM new_rejections)", 1)[0]) == _compact(
        historical.split("SELECT (SELECT count(*) FROM new_rejections)", 1)[0]
    )
    old_identity = historical.split("(SELECT count(*) FROM updated u", 1)[1].split("      INTO inserted_n", 1)[0]
    new_identity = ordinary.split("(SELECT count(*) FROM updated u", 1)[1].split("  invalid_resolution_n", 1)[0]
    assert _compact(new_identity) == _compact(old_identity)


def test_prerequisite_codes_keep_existing_precedence_and_accounting_is_aggregate():
    codes = re.findall(r"RAISE EXCEPTION '([^']+)'", _leaf().split("    WITH\n", 1)[0])
    assert re.findall(r"THEN '(custom_import_[^']+)'", admission._queries()[0]) == codes
    guard, decision, apply, advance = admission._queries()
    assert "f.landing_table_owner IS NOT NULL AND f.landing_columns_sha256 IS NOT NULL" in guard
    assert "f.frozen_at IS NULL" in guard and "s.replay_verified_at IS NULL" in guard
    assert "c.capture_state='sealed'" in guard and "p_expected_after_id" not in guard
    assert "byte_boundary boundary WHERE boundary.raw_bytes>:page_byte_limit" in decision
    assert apply.count("INSERT INTO ") == 1 and apply.count("UPDATE ") == 1
    assert "next_rejection_ordinal=a.next_rejection_ordinal+:inserted_n" in advance
    assert "NOT t.remaining AND b.refresh_mode='snapshot' AND NOT b.complete_scope" in advance
    assert "WHEN a.candidate_error_count+d.errors>0 THEN 'rejected' ELSE 'graph' END" in advance
    assert not re.search(
        r"\b(CREATE|DROP|ALTER|TRIGGER|FUNCTION|DO|CALL|EXECUTE)\b", "\n".join(admission._queries()), re.I
    )


class _Result:
    def __init__(self, row):
        self.row = row

    def mappings(self):
        return self

    def one(self):
        return self.row


class _Transaction:
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        self.session.events.append("begin")
        self.original_search_path = self.session.search_path
        return self

    async def __aexit__(self, kind, error, traceback):
        self.session.events.append("rollback" if error else "commit")
        if error is None:
            self.session.durable.extend(self.session.pending)
        self.session.pending.clear()
        self.session.search_path = self.original_search_path


class _Session:
    def __init__(self, rows, schema="synthetic_control"):
        self.rows, self.events, self.pending, self.durable, self.executed = list(rows), [], [], [], []
        self.schema = schema
        self.search_path = '"synthetic_untrusted", pg_catalog'
        self.catalog_statements = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, kind, error, traceback):
        self.events.append("close")

    def begin(self):
        return _Transaction(self)

    async def connection(self):
        return SimpleNamespace(
            dialect=dialect(),
            sync_connection=SimpleNamespace(
                get_execution_options=lambda: {
                    "schema_translate_map": {admission.CustomImportBuildAttempt.__table__.schema: self.schema}
                }
            ),
        )

    async def execute(self, statement, parameters=None):
        if str(statement) == "SELECT pg_catalog.set_config('search_path', 'pg_catalog', true)":
            self.events.append("catalog_path")
            self.catalog_statements.append(statement)
            self.search_path = "pg_catalog"
            return _Result({"set_config": "pg_catalog"})
        assert self.search_path == "pg_catalog"
        step = ("prerequisites", "decision", "apply", "advance")[len(self.executed)]
        self.events.append(step)
        self.executed.append((statement, dict(parameters)))
        if step in ("apply", "advance"):
            self.pending.append(step)
        row = self.rows.pop(0)
        if isinstance(row, BaseException):
            raise row
        return _Result(row)

    def expire(self, build):
        self.events.append("expire")


def _rows():
    return [
        dict(problem=None, definition_streams=[{"duplicate_policy": "reject"}], memberships=[]),
        dict(fatal_code=None, rows_processed=512, last_id=999, errors=2, decisions=[]),
        dict(inserted_n=1, updated_n=2, expected_inserted_n=1, expected_updated_n=2, invalid_resolution_n=0),
        dict(phase="rejected", after_occurrence_id=999, rows_processed=512, candidate_error_count=2),
    ]


def _database_error(message="canceling statement due to statement timeout", *, driver="asyncpg", sqlstate="57014"):
    original = SimpleNamespace(sqlstate=sqlstate)
    if driver == "asyncpg":
        original.__cause__ = SimpleNamespace(message=message)
    elif driver == "psycopg":
        original.diag = SimpleNamespace(message_primary=message)
    return DBAPIError(None, None, original)


def _page(monkeypatch, rows=None, final_error=None, schema="synthetic_control"):
    session = _Session(_rows() if rows is None else rows, schema)
    build = SimpleNamespace(**{name: 1 for name in admission._BUILD_FIELDS})
    build.producing_token_sha256 = b"s" * 32
    build.page_row_limit, build.page_byte_limit = 256, 16_384
    build.admission_after_occurrence_id = 0
    monkeypatch.setattr(staging, "_lock_page", AsyncMock(return_value=build))
    monkeypatch.setattr(staging, "_flush_page", AsyncMock())
    monkeypatch.setattr(staging, "_prepare_statement", AsyncMock())
    monkeypatch.setattr(admission, "_prepare_statement", AsyncMock())
    monkeypatch.setattr(admission, "_resolve_build_snapshot", AsyncMock(return_value=7))
    monkeypatch.setattr(admission, "_call", AsyncMock())
    monkeypatch.setattr(staging, "lock_execution", AsyncMock())
    monkeypatch.setattr(staging, "lock_lease", AsyncMock())

    async def verify(page, *arguments):
        page.events.append("verify_before_commit")
        if final_error is not None:
            raise final_error
        return dt.datetime(2029, 1, 1, tzinfo=dt.UTC)

    monkeypatch.setattr(staging, "verify_live_attempt", verify)
    return session, build


@pytest.mark.asyncio
async def test_operation_preserves_return_shape_and_fresh_authority_before_commit(monkeypatch):
    session, build = _page(monkeypatch)
    result = await admission.admit_source_batch(lambda: session, _request(page_row_limit=256), 1, 0)
    assert result == ("rejected", 999, 512, 2)
    assert session.events == [
        "begin",
        "catalog_path",
        "prerequisites",
        "decision",
        "apply",
        "advance",
        "expire",
        "verify_before_commit",
        "commit",
        "close",
    ]
    assert session.durable == ["apply", "advance"] and not session.pending
    assert session.executed[2][1]["decisions"] == []
    assert session.executed[3][1]["inserted_n"] == 1
    assert session.executed[1][1]["physical_row_cap"] == 100_000
    admission._resolve_build_snapshot.assert_awaited_once_with(session, build.build_id)
    admission._call.assert_awaited_once_with(
        session, "resolve_custom_import_build_base_snapshot", (("bigint", build.build_id),)
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("driver", ["asyncpg", "psycopg"])
async def test_decision_timeout_retries_fresh_page(monkeypatch, driver):
    rows = _rows()
    rows[1] = _database_error(driver=driver)
    failed, build = _page(monkeypatch, rows)
    retry = _Session(_rows())
    retry.rows[1]["rows_processed"] = retry.rows[3]["rows_processed"] = 256
    fresh_build = SimpleNamespace(**vars(build))
    staging._lock_page.side_effect = [build, fresh_build]
    request = _request(page_row_limit=256)

    def sessions():
        if not failed.events:
            return failed
        assert failed.events[-2:] == ["rollback", "close"]
        return retry

    assert await admission.admit_source_batch(sessions, request, 1, 0) == ("rejected", 999, 256, 2)
    assert staging._lock_page.await_args_list == [call(failed, request, 1), call(retry, request, 1)]
    assert not failed.durable and retry.durable == ["apply", "advance"]
    assert failed.search_path == retry.search_path == '"synthetic_untrusted", pg_catalog'
    assert retry.events[-3:] == ["verify_before_commit", "commit", "close"]
    assert [page.executed[1][1]["physical_row_cap"] for page in (failed, retry)] == [100_000, 256]
    assert [page.executed[1][1]["expected_after_id"] for page in (failed, retry)] == [0, 0]
    assert admission._resolve_build_snapshot.await_args_list == [call(failed, 1), call(retry, 1)]


@pytest.mark.asyncio
@pytest.mark.parametrize("step", [0, 2, 3])
async def test_other_statement_timeouts_never_retry(monkeypatch, step):
    rows = _rows()
    rows[step] = error = _database_error()
    session, _ = _page(monkeypatch, rows)
    sessions = Mock(return_value=session)
    with pytest.raises(DBAPIError) as caught:
        await admission.admit_source_batch(sessions, _request(), 1, 0)
    assert caught.value is error
    sessions.assert_called_once()
    assert session.events[-2:] == ["rollback", "close"] and not session.durable


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error",
    [
        _database_error("canceling statement due to user request"),
        _database_error("canceling statement due to conflict with recovery"),
        _database_error("unknown localized diagnostic"),
        _database_error(sqlstate="P0001"),
        _database_error(driver="missing"),
    ],
)
async def test_unconfirmed_cancellation_never_retries(monkeypatch, error):
    rows = _rows()
    rows[1] = error
    session, _ = _page(monkeypatch, rows)
    sessions = Mock(return_value=session)
    with pytest.raises(DBAPIError) as caught:
        await admission.admit_source_batch(sessions, _request(), 1, 0)
    assert caught.value is error and not hasattr(error, "_custom_import_admission_cursor")
    sessions.assert_called_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("invalidated", [False, True])
async def test_preparation_or_transport_never_retries(monkeypatch, invalidated):
    rows, error = _rows(), _database_error()
    rows[1] = error
    session, _ = _page(monkeypatch, rows)
    if invalidated:
        error.connection_invalidated = True
    else:
        admission._prepare_statement.side_effect = [None, error]
    sessions = Mock(return_value=session)
    with pytest.raises(DBAPIError) as caught:
        await admission.admit_source_batch(sessions, _request(), 1, 0)
    assert caught.value is error
    sessions.assert_called_once()
    assert session.events[-2:] == ["rollback", "close"] and not session.durable


@pytest.mark.asyncio
async def test_second_decision_timeout_never_retries(monkeypatch):
    rows = _rows()
    rows[1] = _database_error()
    failed, _ = _page(monkeypatch, rows)
    retry_rows = _rows()
    retry_rows[1] = error = _database_error()
    retry = _Session(retry_rows)
    sessions = Mock(side_effect=[failed, retry])
    with pytest.raises(DBAPIError) as caught:
        await admission.admit_source_batch(sessions, _request(), 1, 0)
    assert caught.value is error and sessions.call_count == 2
    assert all(page.events[-2:] == ["rollback", "close"] and not page.durable for page in (failed, retry))


@pytest.mark.asyncio
@pytest.mark.parametrize("lost_authority", [False, True])
async def test_timeout_retry_rechecks_original_cursor(monkeypatch, lost_authority):
    rows = _rows()
    rows[1] = _database_error()
    failed, build = _page(monkeypatch, rows)
    fresh_build = SimpleNamespace(**vars(build))
    fresh_build.admission_after_occurrence_id = 1
    retry_rows = _rows()
    retry_rows[0]["problem"] = "custom_import_build_progress_conflict"
    retry = _Session(retry_rows)
    error = LeaseAuthorityLost("retry authority changed")
    staging._lock_page.side_effect = [build, error if lost_authority else fresh_build]
    sessions = Mock(side_effect=[failed, retry])
    with pytest.raises(LeaseAuthorityLost if lost_authority else admission.AdmissionError):
        await admission.admit_source_batch(sessions, _request(), 1, 0)
    assert sessions.call_count == 2 and not retry.durable
    assert retry.events[-2:] == ["rollback", "close"]
    if lost_authority:
        assert not retry.executed
    else:
        assert retry.executed[0][1]["expected_after_id"] == 0
        assert retry.executed[0][1]["admission_after_occurrence_id"] == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("context", [_Transaction, _Session])
@pytest.mark.parametrize("cleanup_error", [RuntimeError("cleanup failed"), asyncio.CancelledError("cleanup canceled")])
async def test_timeout_cleanup_failure_blocks_retry(monkeypatch, context, cleanup_error):
    rows = _rows()
    rows[1] = error = _database_error()
    session, _ = _page(monkeypatch, rows)
    original_exit = context.__aexit__

    async def failed_exit(context, *arguments):
        await original_exit(context, *arguments)
        raise cleanup_error

    monkeypatch.setattr(context, "__aexit__", failed_exit)
    sessions = Mock(return_value=session)
    with pytest.raises(DBAPIError) as caught:
        await admission.admit_source_batch(sessions, _request(), 1, 0)
    assert caught.value is error and error._custom_import_retry_blocked
    sessions.assert_called_once()


@pytest.mark.asyncio
async def test_uncertain_commit_never_retries(monkeypatch):
    session, _ = _page(monkeypatch)
    error = _database_error()

    async def uncertain_commit(transaction, kind, primary, traceback):
        assert kind is primary is traceback is None
        transaction.session.events.append("uncertain_commit")
        raise error

    monkeypatch.setattr(_Transaction, "__aexit__", uncertain_commit)
    sessions = Mock(return_value=session)
    with pytest.raises(DBAPIError) as caught:
        await admission.admit_source_batch(sessions, _request(), 1, 0)
    assert caught.value is error and not hasattr(error, "_custom_import_admission_cursor")
    sessions.assert_called_once()
    assert session.events[-2:] == ["uncertain_commit", "close"]


@pytest.mark.asyncio
@pytest.mark.parametrize("cap", [True, 255, 100_001])
async def test_physical_cap_bounds_precede_sql(monkeypatch, cap):
    session, build = _page(monkeypatch)
    with pytest.raises(ValueError, match="physical_row_cap"):
        await admission._admit_locked(session, build, 0, physical_row_cap=cap)
    assert not session.events


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [RuntimeError("retained base differs"), asyncio.CancelledError("base check cancel")])
async def test_retained_base_binding_failure_rolls_back_before_candidate_sql(monkeypatch, error):
    session, build = _page(monkeypatch)
    admission._call.side_effect = error
    with pytest.raises(type(error)) as caught:
        await admission.admit_source_batch(lambda: session, _request(), 1, 0)
    assert caught.value is error
    admission._resolve_build_snapshot.assert_awaited_once_with(session, build.build_id)
    admission._call.assert_awaited_once_with(
        session, "resolve_custom_import_build_base_snapshot", (("bigint", build.build_id),)
    )
    assert session.events == ["begin", "catalog_path", "rollback", "close"]
    assert not session.executed and not session.pending and not session.durable


@pytest.mark.asyncio
@pytest.mark.parametrize("step", range(4))
@pytest.mark.parametrize("error", [RuntimeError("synthetic DB error"), asyncio.CancelledError("synthetic cancel")])
async def test_statement_failure_or_cancel_rolls_back_whole_prefix(monkeypatch, step, error):
    rows = _rows()
    rows[step] = error
    session, _ = _page(monkeypatch, rows)
    with pytest.raises(type(error)) as caught:
        await admission.admit_source_batch(lambda: session, _request(), 1, 0)
    assert caught.value is error
    assert session.events[-2:] == ["rollback", "close"]
    assert "commit" not in session.events and not session.durable and not session.pending


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "step,code",
    [
        (0, "custom_import_build_progress_conflict"),
        (0, "custom_import_sealed_append"),
        (1, "custom_import_build_page_too_large"),
        (1, "custom_import_build_identity_mismatch"),
        (1, "custom_import_build_structure_mismatch: raw key digest collision"),
        (1, "custom_import_build_structure_mismatch: membership key differs"),
        (1, "custom_import_build_structure_mismatch: child key digest collision"),
    ],
)
async def test_fatal_before_writes_preserves_code_and_retry_classification(monkeypatch, step, code):
    rows = _rows()
    rows[step]["problem" if step == 0 else "fatal_code"] = code
    session, _ = _page(monkeypatch, rows)
    with pytest.raises(admission.AdmissionError, match=re.escape(code)) as caught:
        await admission.admit_source_batch(lambda: session, _request(), 1, 0)
    assert caught.value.sqlstate == ("40001" if "progress_conflict" in code else "P0001")
    assert "apply" not in session.events and not session.durable
    assert session.events[-2:] == ["rollback", "close"]


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["inserted_n", "updated_n", "invalid_resolution_n"])
async def test_aggregate_failure_rolls_back_candidate_changes_without_cursor_advance(monkeypatch, field):
    rows = _rows()
    rows[2][field] += 1
    session, _ = _page(monkeypatch, rows)
    with pytest.raises(admission.AdmissionError, match="admission aggregate differs"):
        await admission.admit_source_batch(lambda: session, _request(), 1, 0)
    assert "apply" in session.events and "advance" not in session.events
    assert session.events[-2:] == ["rollback", "close"] and not session.durable and not session.pending


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error", [LeaseAuthorityLost("stale fence"), asyncio.CancelledError("late cancel"), _database_error()]
)
async def test_final_authority_failure_rolls_back_even_after_cursor_update(monkeypatch, error):
    session, _ = _page(monkeypatch, final_error=error)
    with pytest.raises(type(error)) as caught:
        await admission.admit_source_batch(lambda: session, _request(), 1, 0)
    assert caught.value is error and session.events[-3:] == ["verify_before_commit", "rollback", "close"]
    assert "advance" in session.events and not session.pending and not session.durable


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "step,code",
    [
        (0, "custom_import_build_progress_conflict"),
        (1, "custom_import_build_structure_mismatch: membership key differs"),
        (2, "custom_import_build_structure_mismatch: admission aggregate differs"),
    ],
)
async def test_actual_runner_keeps_old_dbapi_and_ordinary_admission_errors_nonterminal(monkeypatch, step, code):
    sqlstate = "40001" if step == 0 else "P0001"
    old_error = DBAPIError(None, None, SimpleNamespace(sqlstate=sqlstate, message=code))
    old_flow = _install_flow(monkeypatch)
    old_flow.calls["stage_segmented_source"].side_effect = old_error
    with pytest.raises(DBAPIError) as old_caught:
        await _run(old_flow)
    assert old_caught.value is old_error
    old_flow.calls["_finish"].assert_not_awaited()
    old_flow.calls["_activate"].assert_not_awaited()

    new_flow = _install_flow(monkeypatch)
    case_rows = _rows()
    if step == 2:
        case_rows[2]["invalid_resolution_n"] = 1
    else:
        case_rows[step]["problem" if step == 0 else "fatal_code"] = code
    session, _ = _page(monkeypatch, case_rows)

    async def ordinary_admission(*arguments):
        return await admission.admit_source_batch(lambda: session, _request(), 1, 0)

    new_flow.calls["stage_segmented_source"].side_effect = ordinary_admission
    with pytest.raises(admission.AdmissionError, match=re.escape(code)) as new_caught:
        await _run(new_flow)
    assert new_caught.value.sqlstate == old_caught.value.orig.sqlstate
    assert not isinstance(new_caught.value, CandidateRunnerError)
    new_flow.calls["_finish"].assert_not_awaited()
    new_flow.calls["_activate"].assert_not_awaited()
    new_flow.calls["count_source_outcomes"].assert_not_awaited()
    assert session.events[-2:] == ["rollback", "close"] and not session.pending and not session.durable


@pytest.mark.asyncio
async def test_actual_runner_terminal_validation_branch_remains_distinct(monkeypatch):
    flow = _install_flow(monkeypatch)
    flow.calls["stage_segmented_source"].side_effect = CandidateRunnerError("synthetic terminal validation")
    assert (await _run(flow)).status == "candidate_rejected"
    assert flow.calls["_finish"].await_args.args[-1] == "failed"
    assert flow.calls["_finish"].await_args.kwargs == {"failure_reason": "segmented_build_failed"}
    flow.calls["_activate"].assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["success", "fatal", "aggregate", "cancel", "lease"])
async def test_trusted_catalog_context_precedes_resolvers_and_resets_at_transaction_end(monkeypatch, failure):
    case_rows, error = _rows(), None
    if failure == "fatal":
        case_rows[1]["fatal_code"] = "custom_import_build_identity_mismatch"
        error = admission.AdmissionError
    elif failure == "aggregate":
        case_rows[2]["invalid_resolution_n"] = 1
        error = admission.AdmissionError
    elif failure == "cancel":
        case_rows[2] = asyncio.CancelledError("catalog rollback")
        error = asyncio.CancelledError
    final_error = LeaseAuthorityLost("catalog late lease loss") if failure == "lease" else None
    error = LeaseAuthorityLost if final_error is not None else error
    session, _ = _page(monkeypatch, case_rows, final_error=final_error)
    original_path = session.search_path

    async def resolve(*arguments):
        assert session.search_path == "pg_catalog"
        return 7

    monkeypatch.setattr(admission, "_resolve_build_snapshot", AsyncMock(side_effect=resolve))
    if error is None:
        await admission.admit_source_batch(lambda: session, _request(), 1, 0)
    else:
        with pytest.raises(error):
            await admission.admit_source_batch(lambda: session, _request(), 1, 0)
    assert len(session.catalog_statements) == 1
    assert str(session.catalog_statements[0]) == "SELECT pg_catalog.set_config('search_path', 'pg_catalog', true)"
    assert session.events[:2] == ["begin", "catalog_path"]
    assert session.search_path == original_path
    # The portable transaction model checks routing/local flags, not live PostgreSQL GUC behavior.
    async with session.begin():
        assert session.search_path == original_path


@pytest.mark.asyncio
async def test_mapped_schema_quoted_identifiers_and_regclass_are_separate_bound_values(monkeypatch):
    schema = "synthetic'control\"; SELECT 1; --"
    session, _ = _page(monkeypatch, schema=schema)
    statements, relations = await admission._statements(session, 7)
    for statement in statements:
        compiled = statement.compile(dialect=dialect())
        assert "__CONTROL__" not in str(compiled) and "__CANDIDATE__" not in str(compiled)
        assert set(compiled.params) <= set(admission._PARAM_TYPES)
        assert not re.search(r"\$\d+|(?<!:):[a-z_]", str(compiled))
    assert (
        relations["rejection_sequence"]
        == '"synthetic\'control""; SELECT 1; --".custom_import_rejection_rejection_id_seq'
    )
    assert relations["root_relation"] == "ci_snapshot_7.custom_import_root_record"
    assert schema not in str(statements[0])  # Identifier quoting doubles the quote; regclass is a value bind.
    assert isinstance(statements[1]._bindparams["physical_row_cap"].type, admission.Integer)


@pytest.mark.asyncio
@pytest.mark.parametrize("build_id,cursor", [(True, 0), (0, 0), (1, -1), (1, True), (1, 2**63)])
async def test_request_bounds_fail_before_opening_a_session(build_id, cursor):
    def forbidden():
        raise AssertionError("invalid request opened a session")

    with pytest.raises(ValueError):
        await admission.admit_source_batch(forbidden, _request(), build_id, cursor)


@pytest.mark.asyncio
@pytest.mark.parametrize("owner_lookup", [True, False])
async def test_absent_control_schema_fails_closed(monkeypatch, owner_lookup):
    session, _ = _page(monkeypatch, schema=None)
    with pytest.raises(CandidateRunnerError, match="explicit model schema"):
        if owner_lookup:
            await admission._has_admission_owner(session)
        else:
            await admission._statements(session, 7)


def _staging_page(monkeypatch, session, build, direct=True):
    build.phase, build.source_occurrence_count, build.candidate_error_count = "admission", 4, 0
    policy = _policy().capture
    bundle = SimpleNamespace(canonical_policy=policy.canonical, policy_sha256=bytes.fromhex(policy.digest))
    monkeypatch.setattr(session, "get", AsyncMock(return_value=bundle), raising=False)
    monkeypatch.setattr(session, "scalars", AsyncMock(return_value=SimpleNamespace(all=lambda: [])), raising=False)
    monkeypatch.setattr(staging, "_begin_build", AsyncMock(return_value=(1, SimpleNamespace(stream_slots={}))))
    monkeypatch.setattr(staging, "_prepare_snapshot_indexes", AsyncMock())
    selected = AsyncMock(return_value=direct)
    monkeypatch.setattr(admission, "_has_admission_owner", selected)
    return selected


@pytest.mark.asyncio
@pytest.mark.parametrize("direct", [True, False])
async def test_staging_admits_inside_its_existing_page_without_a_nested_transaction(monkeypatch, direct):
    session, build = _page(monkeypatch)
    selected = _staging_page(monkeypatch, session, build, direct)
    original = admission._admit_locked

    async def admit_in_page(page, locked_build, cursor):
        assert page is session and locked_build is build and cursor == 0
        assert session.events.count("begin") == 2 and session.events.count("commit") == 1
        result = await original(page, locked_build, cursor)
        build.phase, build.admission_after_occurrence_id, _, build.candidate_error_count = result
        return result

    monkeypatch.setattr(admission, "_admit_locked", admit_in_page)

    async def legacy(page, name, arguments):
        assert page is session and name == "admit_custom_import_build_page"
        assert arguments == (("bigint", 1), ("bigint", 0))
        build.phase, build.admission_after_occurrence_id, build.candidate_error_count = "rejected", 999, 2

    dispatched = AsyncMock(side_effect=legacy)
    monkeypatch.setattr(staging, "_call", dispatched)
    staged = await staging.stage_segmented_source(lambda: session, _request())
    assert staged == staging.SourceBuildResult(1, 4, "rejected", 4, 2)
    assert session.events.count("begin") == session.events.count("commit") == 3
    selected.assert_awaited_once_with(session)
    if direct:
        assert session.durable == ["apply", "advance"]
        admission._call.assert_awaited_once_with(
            session, "resolve_custom_import_build_base_snapshot", (("bigint", build.build_id),)
        )
        dispatched.assert_not_awaited()
    else:
        assert not session.executed and not session.durable
        admission._call.assert_not_awaited()
        dispatched.assert_awaited_once()


@pytest.mark.asyncio
async def test_staging_timeout_uses_fresh_page(monkeypatch):
    rows = _rows()
    rows[1] = _database_error()
    failed, build = _page(monkeypatch, rows)
    initial, retry, terminal = _Session([]), _Session(_rows()), _Session([])
    retry.rows[1]["rows_processed"] = retry.rows[3]["rows_processed"] = 256
    selected = _staging_page(monkeypatch, initial, build)
    complete = SimpleNamespace(**vars(build))
    complete.phase, complete.admission_after_occurrence_id, complete.candidate_error_count = "rejected", 999, 2
    staging._lock_page.side_effect = [build, SimpleNamespace(**vars(build)), SimpleNamespace(**vars(build)), complete]
    sessions = Mock(side_effect=[initial, failed, retry, terminal])
    result = await staging.stage_segmented_source(sessions, _request())
    assert result == staging.SourceBuildResult(1, 4, "rejected", 4, 2)
    assert sessions.call_count == 4
    selected.assert_awaited_once_with(initial)
    assert failed.events[-2:] == ["rollback", "close"] and not failed.durable
    assert retry.events[-3:] == ["verify_before_commit", "commit", "close"]
    assert retry.executed[1][1]["physical_row_cap"] == 256
    assert retry.executed[1][1]["expected_after_id"] == failed.executed[1][1]["expected_after_id"] == 0
    assert initial.events.count("commit") == terminal.events.count("commit") == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("owns_both", [True, False])
async def test_direct_selection_binds_both_native_signatures_and_exact_current_user(monkeypatch, owns_both):
    session, _ = _page(monkeypatch, schema='synthetic"control')
    executed = AsyncMock(return_value=SimpleNamespace(scalar_one=lambda: owns_both))
    monkeypatch.setattr(session, "execute", executed)
    assert await admission._has_admission_owner(session) is owns_both
    statement, parameters = executed.await_args.args
    assert '"synthetic""control".begin_custom_import_build(' in parameters["begin"]
    assert "pg_catalog.timestamptz" in parameters["begin"]
    assert parameters["admit"] == '"synthetic""control".admit_custom_import_build_page(pg_catalog.int8,pg_catalog.int8)'
    assert "pg_catalog.count(*)=2" in str(statement) and "=CURRENT_USER" in str(statement)
    assert "session_user" not in str(statement) and "pg_has_role" not in str(statement)
    assert 'synthetic"control' not in str(statement)
    assert all(isinstance(parameter.type, admission.String) for parameter in statement._bindparams.values())


def test_fixed_package_sql_does_not_transport_caller_statements():
    module = (_ROOT / "process/custom_import/admission_sql.py").read_text()
    assert "session.info" not in module and "alembic/" not in module
    assert 'Path(__file__).with_name("admission.sql")' in module
