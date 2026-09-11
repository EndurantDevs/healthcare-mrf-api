# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import copy
import hashlib
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, call

import pytest
from sqlalchemy.engine import make_url

from process import new_york_profile_binding as binding
from process import new_york_profile_registry as registry
from process.massachusetts_profile_acquisition import encoded_json
from tests.test_new_york_profile_binding import (
    _candidate as candidate,
)
from tests.test_new_york_profile_binding import (
    _save_snapshot as retained_snapshot,
)
from tests.test_new_york_profile_binding import (
    _snapshot as snapshot,
)


class RegistryCursor:
    def __init__(self, candidates):
        self.candidates = candidates

    async def cursor(self, _query, *, prefetch):
        assert _query == registry.CAPTURE_QUERY and prefetch == 500
        for candidate_by_field in self.candidates:
            yield candidate_by_field


@pytest.mark.parametrize(
    ("failure", "reason"),
    [
        ("count", "count_changed"),
        ("row_size", "row_too_large"),
        ("total_size", "snapshot_too_large"),
        ("row_limit", "snapshot_too_large"),
        ("order", "row_order_changed"),
        ("cancel", None),
    ],
)
async def test_incomplete_or_unbounded_cursor_is_rejected(monkeypatch, failure, reason):
    candidates = [candidate(), candidate(taxonomy_occurrence_checksum=18)]
    expected, progress = 2, AsyncMock()
    if failure == "count":
        expected = 3
    if failure == "row_size":
        monkeypatch.setattr(registry, "MAX_ROW_BYTES", 1)
    if failure == "total_size":
        monkeypatch.setattr(registry, "MAX_SNAPSHOT_BYTES", 1024 * 1024)
    if failure == "row_limit":
        monkeypatch.setattr(registry, "MAX_REGISTRY_ROWS", 1)
    if failure == "order":
        candidates.reverse()
    if failure == "cancel":
        progress.side_effect = asyncio.CancelledError
    with pytest.raises(asyncio.CancelledError if failure == "cancel" else ValueError, match=reason):
        await registry._registry_rows(RegistryCursor(candidates), expected, progress)


async def test_empty_complete_cursor_stays_empty():
    captured = await registry._registry_rows(RegistryCursor([]), 0, AsyncMock())
    assert captured["registry_rows"] == [] and captured["row_count"] == 0
    assert captured["registry_rows_sha256"] == hashlib.sha256(b"[]").hexdigest()


@pytest.mark.parametrize("initialized", [False, True])
async def test_owned_connection_uses_database_url_and_timeouts(monkeypatch, initialized):
    url = make_url("postgresql+asyncpg://synthetic@localhost/registry_test")
    engine = SimpleNamespace(url=url)
    database = SimpleNamespace(engine=engine if initialized else None)

    async def initialize():
        database.engine = engine

    database.connect = AsyncMock(side_effect=initialize)
    open_connection = AsyncMock(return_value=object())
    monkeypatch.setattr(registry, "db", database)
    monkeypatch.setattr(registry.asyncpg, "connect", open_connection)
    assert await registry._open_connection() is open_connection.return_value
    assert database.connect.await_count == (0 if initialized else 1)
    open_connection.assert_awaited_once_with(
        dsn="postgresql://synthetic@localhost/registry_test",
        timeout=10,
        server_settings={"statement_timeout": "90000", "lock_timeout": "5000"},
    )


def snapshot_connection(candidates):
    captured = snapshot(candidates)
    connection = SimpleNamespace(active=False, closed=False)

    @asynccontextmanager
    async def transaction(**options):
        assert options == {"isolation": "repeatable_read", "readonly": True}
        connection.active = True
        try:
            yield
        finally:
            connection.active = False

    async def read_count(query):
        assert connection.active
        assert query == registry.COUNT_QUERY
        return len(candidates)

    async def read_rows(query, *, prefetch):
        assert connection.active and query == registry.CAPTURE_QUERY and prefetch == 500
        for candidate_by_field in candidates:
            yield candidate_by_field

    async def close(*, timeout):
        assert timeout == 10 and not connection.active
        connection.closed = True

    connection.transaction = transaction
    connection.fetchrow = AsyncMock(return_value=captured["snapshot"])
    connection.fetchval = AsyncMock(side_effect=read_count)
    connection.cursor = read_rows
    connection.fetch = AsyncMock(
        return_value=[{"name": name, "oid": oid} for name, oid in captured["registry_relations"].items()]
    )
    connection.close = AsyncMock(side_effect=close)
    connection.is_closed = lambda: connection.closed
    connection.terminate = Mock(side_effect=lambda: setattr(connection, "closed", True))
    return connection


async def test_capture_retains_eof_progress_and_replayable_snapshot(monkeypatch, tmp_path):
    candidates = [candidate(taxonomy_occurrence_checksum=index) for index in range(501)]
    connection = snapshot_connection(candidates)
    monkeypatch.setattr(registry, "_open_connection", AsyncMock(return_value=connection))
    progress = AsyncMock()
    captured = await registry.capture_registry_snapshot(progress)
    assert captured == snapshot(candidates)
    assert progress.await_args_list == [call(0, 0), call(500, 501), call(501, 501)]
    assert connection.fetchrow.await_args_list == [call(registry.SNAPSHOT_SQL), call(registry.SNAPSHOT_SQL)]
    connection.fetch.assert_awaited_once_with(
        registry.RELATIONS_SQL,
    )
    connection.close.assert_awaited_once_with(timeout=10)
    connection.terminate.assert_not_called()
    assert connection.closed and not connection.active
    retained = retained_snapshot(tmp_path, captured)
    assert (
        binding.read_registry_snapshot(retained["snapshot_path"], snapshot_sha256=retained["snapshot_sha256"])
        == captured
    )


@pytest.mark.parametrize("failure", ["connect", "snapshot", "count", "cancel", "close"])
async def test_capture_failure_never_leaks_owned_connection(monkeypatch, failure):
    connection = snapshot_connection([candidate()])
    open_connection = AsyncMock(return_value=connection)
    monkeypatch.setattr(registry, "_open_connection", open_connection)
    progress = AsyncMock()
    expected_exception, reason = ValueError, "snapshot_changed"
    if failure == "connect":
        open_connection.side_effect = RuntimeError("synthetic_connect_failure")
        expected_exception, reason = RuntimeError, "synthetic_connect_failure"
    elif failure == "snapshot":
        metadata = snapshot()["snapshot"]
        connection.fetchrow.side_effect = [metadata, {**metadata, "snapshot_id": "101:102:"}]
    elif failure == "count":
        connection.fetchval.side_effect = None
        connection.fetchval.return_value = registry.MAX_REGISTRY_ROWS + 1
        reason = "count_invalid"
    elif failure == "cancel":
        progress.side_effect = [None, asyncio.CancelledError()]
        expected_exception, reason = asyncio.CancelledError, None
    else:
        connection.close.side_effect = TimeoutError("synthetic_close_timeout")
        expected_exception, reason = TimeoutError, "synthetic_close_timeout"
    with pytest.raises(expected_exception, match=reason):
        await registry.capture_registry_snapshot(progress)
    assert not connection.active
    if failure == "connect":
        connection.close.assert_not_awaited()
        connection.terminate.assert_not_called()
    else:
        connection.close.assert_awaited_once_with(timeout=10)
        assert connection.closed
        assert connection.terminate.call_count == (1 if failure == "close" else 0)


def test_capture_query_preserves_reviewed_literal_snapshot_contract():
    assert hashlib.sha256(registry.CAPTURE_QUERY.encode()).hexdigest() == binding.QUERY_SHA256
    assert registry.CAPTURE_QUERY.count("LEFT JOIN") == 2
    assert registry.CAPTURE_QUERY.split(" WHERE ")[1] == (
        "t.provider_license_number_state_code = 'NY'\n ORDER BY t.npi, t.checksum;\n"
    )
    assert registry.COUNT_QUERY == (
        "SELECT count(*) AS expected_taxonomy_occurrences FROM mrf.npi_taxonomy "
        "WHERE provider_license_number_state_code = 'NY';\n"
    )


async def test_capture_preserves_invalid_identities_nullable_joins_and_duplicates(monkeypatch):
    rows = [
        candidate(npi=1234567890, joined_npi=None, entity_type_code=None, license_number=None),
        candidate(npi=1234567890, joined_npi=None, entity_type_code=None, license_number=None),
        candidate(npi=1234567890, taxonomy_occurrence_checksum=18, joined_taxonomy_code=None, taxonomy_grouping=None),
        candidate(npi=1234567890, taxonomy_occurrence_checksum=19, license_number=" 654321 ", entity_type_code=2),
    ]
    original = copy.deepcopy(rows)
    connection = snapshot_connection(rows)
    monkeypatch.setattr(registry, "_open_connection", AsyncMock(return_value=connection))
    captured = await registry.capture_registry_snapshot()
    assert captured == snapshot(original)
    assert rows == original


@pytest.mark.parametrize(
    "changes",
    [
        {"license_state": "RI"},
        {"license_number": 654321},
        {"npi": True},
        {"taxonomy_occurrence_checksum": None},
        {"unexpected": None},
    ],
)
async def test_changed_capture_row_contract_fails_closed(monkeypatch, changes):
    connection = snapshot_connection([candidate(**changes)])
    monkeypatch.setattr(registry, "_open_connection", AsyncMock(return_value=connection))
    with pytest.raises(ValueError, match="row_invalid"):
        await registry.capture_registry_snapshot()
    assert connection.closed


@pytest.mark.parametrize("failure", ["partial", "join_multiplication", "cursor", "cancel", "transaction", "relations"])
async def test_capture_requires_full_receive_and_closed_valid_snapshot(monkeypatch, failure):
    rows = [candidate(), candidate(taxonomy_occurrence_checksum=18)]
    connection = snapshot_connection(rows)
    monkeypatch.setattr(registry, "_open_connection", AsyncMock(return_value=connection))
    exception, reason = ValueError, "count_changed"
    if failure in {"partial", "join_multiplication"}:
        connection.fetchval = AsyncMock(return_value=3 if failure == "partial" else 1)
    elif failure in {"cursor", "cancel"}:
        exception = TimeoutError if failure == "cursor" else asyncio.CancelledError
        reason = None

        async def failing_cursor(*args, **kwargs):
            yield rows[0]
            raise exception()

        connection.cursor = failing_cursor
    elif failure == "transaction":
        connection.fetchrow.return_value = {**snapshot()["snapshot"], "read_only": "off"}
        reason = "transaction_invalid"
    else:
        connection.fetch.return_value = []
        reason = "snapshot_relations_invalid"
    with pytest.raises(exception, match=reason):
        await registry.capture_registry_snapshot()
    connection.close.assert_awaited_once_with(timeout=10)
    assert connection.closed and not connection.active


async def test_changed_query_pin_never_opens_connection(monkeypatch):
    open_connection = AsyncMock()
    monkeypatch.setattr(registry, "_open_connection", open_connection)
    monkeypatch.setattr(registry, "CAPTURE_QUERY", registry.CAPTURE_QUERY.rstrip())
    with pytest.raises(ValueError, match="query_changed"):
        await registry.capture_registry_snapshot()
    open_connection.assert_not_awaited()


def build_cohort(tmp_path, rows):
    return registry.build_acquisition_cohort(**retained_snapshot(tmp_path, snapshot(rows)))


def test_cohort_selects_any_physician_but_retains_all_original_occurrences(tmp_path):
    registry_rows = [
        candidate(license_number=None),
        candidate(license_number="999999", entity_type_code=2),
        candidate(license_number="222222"),
        candidate(license_number="111111"),
        candidate(license_number="222222"),
        candidate(license_number="222222", joined_npi=None),
        candidate(license_number="222222", taxonomy_grouping="Other"),
        candidate(license_number="222222", entity_type_code=2),
    ]
    original = copy.deepcopy(registry_rows)
    cohort = build_cohort(tmp_path, registry_rows)
    assert cohort["roots"] == [
        {
            "license_number": "111111",
            "registry_occurrence_indexes": [3],
            "registry_only_precondition": "single_npi_source_identity_unverified",
        },
        {
            "license_number": "222222",
            "registry_occurrence_indexes": [2, 4, 5, 6, 7],
            "registry_only_precondition": "registry_occurrence_or_name_conflict",
        },
    ]
    summary = cohort["summary"]
    assert summary["registry_row_count"] == 8
    assert summary["exact_six_digit_row_count"] == 7
    assert summary["exact_six_digit_root_count"] == 3
    assert summary["acquisition_root_count"] == 2
    assert summary["selected_row_count"] == 6
    assert summary["selected_physician_row_count"] == 3
    assert summary["selected_conflicting_occurrence_count"] == 3
    assert summary["excluded_no_physician_root_count"] == summary["excluded_no_physician_row_count"] == 1
    assert summary["selected_distinct_valid_physician_npis"] == 1
    assert registry_rows == original
    assert cohort["state_census"] is False and cohort["source_identity"] == "unverified"
    assert cohort["capture_acceptance"] == "not_established_by_replay"
    assert all("npi" not in root and "matched_npi" not in root for root in cohort["roots"])


@pytest.mark.parametrize(
    "changes",
    [
        {"npi": 1234567890},
        {"npi": None},
        {"npi": []},
        {"npi": True},
        {"joined_npi": None},
        {"joined_npi": 1000000012},
        {"taxonomy_occurrence_checksum": None},
        {"license_state": "RI"},
        {"entity_type_code": 2},
        {"entity_type_code": True},
        {"taxonomy": None},
        {"joined_taxonomy_code": None},
        {"taxonomy_grouping": "Other"},
        {"first_name": "Different"},
        {"first_name": None},
        {"last_name": ""},
        {"middle_name": False},
        {"suffix": "Jr"},
    ],
)
def test_cohort_conflicts_are_diagnostics_not_acquisition_exclusions(tmp_path, changes):
    rows = [candidate(), candidate(**changes)]
    cohort = build_cohort(tmp_path, rows)
    assert cohort["roots"] == [
        {
            "license_number": "654321",
            "registry_occurrence_indexes": [0, 1],
            "registry_only_precondition": "registry_occurrence_or_name_conflict",
        }
    ]
    assert cohort["summary"]["registry_only_conflict_or_ambiguity_root_count"] == 1


@pytest.mark.parametrize("field", ["npi", "joined_npi", "taxonomy_grouping", "first_name", "middle_name", "suffix"])
def test_cohort_preserves_occurrences_missing_identity_columns(tmp_path, field):
    missing = candidate()
    del missing[field]
    cohort = build_cohort(tmp_path, [candidate(), missing])
    assert cohort["roots"][0]["registry_occurrence_indexes"] == [0, 1]
    assert cohort["roots"][0]["registry_only_precondition"] == "registry_occurrence_or_name_conflict"


def test_cohort_multiple_npis_and_name_differences_remain_acquisition_targets(tmp_path):
    rows = [
        candidate(license_number="111111"),
        candidate(license_number="222222"),
        candidate(license_number="333333"),
        candidate(license_number="333333", first_name=" ALEX ", middle_name="  ", suffix=""),
        candidate(license_number="111111", npi=1000000012, joined_npi=1000000012),
        candidate(license_number="222222", npi=1000000012, joined_npi=1000000012, first_name="Other"),
    ]
    cohort = build_cohort(tmp_path, rows)
    assert [root["registry_only_precondition"] for root in cohort["roots"]] == [
        "multiple_npis_even_if_source_names_agree",
        "multiple_npis_even_if_source_names_agree",
        "single_npi_source_identity_unverified",
    ]
    assert cohort["summary"]["registry_only_conflict_or_ambiguity_root_count"] == 2
    assert cohort["summary"]["selected_roots_with_multiple_npis"] == 2
    assert cohort["summary"]["selected_roots_with_conflicting_name_components"] == 1
    assert cohort["summary"]["selected_distinct_valid_physician_npis"] == 2


def test_unsupported_formats_are_counted_without_guessing_equivalent_roots(tmp_path):
    values = [None, "", " ", " 654321 ", "65432", "060654321", "MD654321", "654321-1", "６５４３２１", "654321"]
    rows = [candidate(license_number=value) for value in values]
    cohort = build_cohort(tmp_path, rows)
    assert cohort["roots"][0]["registry_occurrence_indexes"] == [9]
    assert cohort["summary"]["unsupported_license_rows_by_format"] == {
        "null": 1,
        "empty": 1,
        "whitespace_only": 1,
        "padded_six_digits": 1,
        "other_digit_lengths": 2,
        "other_text": 3,
    }
    assert cohort["summary"]["unsupported_license_row_count"] == 9
    assert cohort["summary"]["unsupported_valid_physician_row_count"] == 9


def test_cohort_validates_complete_snapshot_once_and_is_deterministic(monkeypatch, tmp_path):
    options = retained_snapshot(tmp_path, snapshot([candidate()]))
    before = options["snapshot_path"].read_bytes()
    read_snapshot = Mock(wraps=registry.read_registry_snapshot)
    monkeypatch.setattr(registry, "read_registry_snapshot", read_snapshot)
    first = registry.build_acquisition_cohort(**options)
    read_snapshot.assert_called_once_with(options["snapshot_path"], snapshot_sha256=options["snapshot_sha256"])
    second = registry.build_acquisition_cohort(**options)
    assert encoded_json(first) == encoded_json(second)
    assert first["snapshot_sha256"] == options["snapshot_sha256"]
    assert options["snapshot_path"].read_bytes() == before


@pytest.mark.parametrize("failure", ["pin", "count", "rows_digest", "incomplete", "query", "order", "license_type"])
def test_cohort_rejects_invalid_snapshot_before_building_scope(tmp_path, failure):
    captured = snapshot([candidate()])
    if failure == "count":
        captured["expected_source_row_count"] += 1
    if failure == "rows_digest":
        captured["registry_rows_sha256"] = "0" * 64
    if failure == "incomplete":
        captured["all_rows_received"] = False
    if failure == "query":
        captured["query_sha256"] = "0" * 64
    if failure == "order":
        captured = snapshot([candidate(taxonomy_occurrence_checksum=18), candidate()])
    if failure == "license_type":
        captured = snapshot([candidate(license_number=654321)])
    options = retained_snapshot(tmp_path, captured)
    if failure == "pin":
        options["snapshot_sha256"] = "0" * 64
    with pytest.raises(ValueError):
        registry.build_acquisition_cohort(**options)


def test_empty_snapshot_has_complete_zero_cohort_diagnostics(tmp_path):
    cohort = build_cohort(tmp_path, [])
    assert cohort["roots"] == []
    for value in cohort["summary"].values():
        assert all(count == 0 for count in value.values()) if isinstance(value, dict) else value == 0
