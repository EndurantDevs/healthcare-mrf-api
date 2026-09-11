# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import copy
import hashlib
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, call

import pytest
from sqlalchemy.engine import make_url

from process import rhode_island_profile_registry as registry
from process.massachusetts_profile_acquisition import encoded_json, write_new_json
from tests.test_rhode_island_profile_binding import candidate
from tests.test_rhode_island_profile_rows import evidence, occurrence


def snapshot(candidates=None, schema="mrf"):
    registry_rows = [candidate()] if candidates is None else candidates
    content = encoded_json(registry_rows)
    return {
        "schema_version": registry.SNAPSHOT_SCHEMA,
        "coverage_scope": registry.COVERAGE_SCOPE,
        "source_schema": schema,
        "source_state": "RI",
        "columns": list(registry.COLUMNS),
        "query_sha256": hashlib.sha256(registry.capture_query(schema).encode()).hexdigest(),
        "status": "passed",
        "all_rows_received": True,
        "connection_closed": True,
        "row_count": len(registry_rows),
        "expected_source_row_count": len(registry_rows),
        "registry_rows": registry_rows,
        "registry_rows_bytes": len(content),
        "registry_rows_sha256": hashlib.sha256(content).hexdigest(),
        "registry_relations": {
            schema + "." + name: index for index, name in enumerate(("npi", "npi_taxonomy", "nucc_taxonomy"), 1)
        },
        "snapshot": {
            "read_only": "on",
            "isolation": "repeatable read",
            "snapshot_id": "100:101:",
            "snapshot_started_at": "2026-09-11T00:00:00+00:00",
            "server_version": "18",
            "database_name": "synthetic_test",
            "backend_pid": 123,
        },
    }


def retained_snapshot(tmp_path, snapshot_by_field):
    path = tmp_path / "snapshot.json"
    write_new_json(path, snapshot_by_field)
    return {"snapshot_path": path, "snapshot_sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


def profile(license_number="MD00001"):
    content = json.dumps(occurrence(license_number) * 2).encode()
    return license_number, content, evidence(content, license_number)


@pytest.mark.parametrize("schema", [None, "", "mrf;DROP", "mrf.other", "mrf--", 123])
def test_schema_cannot_alter_capture_scope(schema):
    with pytest.raises(ValueError, match="schema_invalid"):
        registry.capture_query(schema)


def test_query_preserves_all_literal_ri_occurrences_and_binding_columns():
    query = registry.capture_query("owned_schema")
    assert set(registry.COLUMNS) == registry.REGISTRY_COLUMNS and len(registry.COLUMNS) == 14
    assert query.count("LEFT JOIN") == 2 and "mrf." not in query
    assert query.split(" WHERE ")[1] == "t.provider_license_number_state_code = 'RI'\n ORDER BY t.npi, t.checksum"
    assert "DISTINCT" not in query


@pytest.mark.parametrize(
    ("field", "replacement", "reason"),
    [
        ("schema_version", "tn-nppes-retained-snapshot/v1", "snapshot_scope_invalid"),
        ("coverage_scope", "filtered_physicians", "snapshot_scope_invalid"),
        ("source_state", "TN", "snapshot_scope_invalid"),
        ("columns", [], "snapshot_scope_invalid"),
        ("query_sha256", "b" * 64, "snapshot_scope_invalid"),
        ("all_rows_received", False, "snapshot_incomplete"),
        ("connection_closed", False, "snapshot_incomplete"),
        ("status", "partial", "snapshot_incomplete"),
        ("row_count", True, "count_changed"),
        ("expected_source_row_count", 2, "count_changed"),
        ("registry_rows_sha256", "b" * 64, "rows_changed"),
        ("registry_rows_bytes", 1, "rows_changed"),
        ("registry_relations", {}, "snapshot_relations_invalid"),
    ],
)
def test_rehashed_incomplete_or_wrong_scope_snapshot_still_fails(tmp_path, field, replacement, reason):
    captured = snapshot()
    captured[field] = replacement
    retained = retained_snapshot(tmp_path, captured)
    with pytest.raises(ValueError, match=reason):
        registry.read_registry_snapshot(retained["snapshot_path"], snapshot_sha256=retained["snapshot_sha256"])


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("read_only", "off"),
        ("isolation", "read committed"),
        ("snapshot_id", "unknown"),
        ("snapshot_started_at", ""),
        ("database_name", None),
        ("server_version", 18),
        ("backend_pid", True),
    ],
)
def test_rehashed_snapshot_requires_complete_readonly_transaction_metadata(tmp_path, field, replacement):
    captured = snapshot()
    captured["snapshot"][field] = replacement
    retained = retained_snapshot(tmp_path, captured)
    with pytest.raises(ValueError, match="snapshot_transaction_invalid"):
        registry.read_registry_snapshot(retained["snapshot_path"], snapshot_sha256=retained["snapshot_sha256"])


@pytest.mark.parametrize(
    ("changes", "reason"),
    [
        ({"license_state": "MA"}, "row_invalid"),
        ({"license_number": 1}, "row_invalid"),
        ({"npi": None}, "row_invalid"),
        ({"taxonomy_occurrence_checksum": True}, "row_invalid"),
        ({"extra": "field"}, "row_invalid"),
        ({"last_name": "a" * registry.MAX_ROW_BYTES}, "row_too_large"),
    ],
)
def test_invalid_occurrence_cannot_be_silently_removed(tmp_path, changes, reason):
    retained = retained_snapshot(tmp_path, snapshot([candidate(**changes)]))
    with pytest.raises(ValueError, match=reason):
        registry.read_registry_snapshot(retained["snapshot_path"], snapshot_sha256=retained["snapshot_sha256"])


@pytest.mark.parametrize("change", ["hash", "formatting", "pin", "symlink", "bytes", "row_limit", "row_order"])
def test_retained_artifact_identity_and_limits(tmp_path, monkeypatch, change):
    captured = snapshot([candidate(), candidate(taxonomy_occurrence_checksum=2)])
    if change == "row_order":
        captured = snapshot(list(reversed(captured["registry_rows"])))
    retained = retained_snapshot(tmp_path, captured)
    path, pin = retained["snapshot_path"], retained["snapshot_sha256"]
    reason = {
        "hash": "snapshot_changed",
        "formatting": "snapshot_not_canonical",
        "pin": "pin_invalid",
        "symlink": "symlink",
        "bytes": "snapshot_too_large",
        "row_limit": "rows_invalid",
        "row_order": "row_order_changed",
    }[change]
    if change == "hash":
        path.write_bytes(path.read_bytes() + b" ")
    if change == "formatting":
        path.write_bytes(json.dumps(captured, indent=2).encode())
        pin = hashlib.sha256(path.read_bytes()).hexdigest()
    if change == "pin":
        pin = ""
    if change == "symlink":
        link = tmp_path / "link.json"
        link.symlink_to(path)
        path = link
    if change == "bytes":
        monkeypatch.setattr(registry, "MAX_SNAPSHOT_BYTES", path.stat().st_size - 1)
    if change == "row_limit":
        monkeypatch.setattr(registry, "MAX_REGISTRY_ROWS", 1)
    with pytest.raises(ValueError, match=reason):
        registry.read_registry_snapshot(path, snapshot_sha256=pin)


def test_adapter_keeps_duplicate_indices_and_literal_board_boundaries(tmp_path):
    candidates = [
        candidate(license_number=None),
        candidate(license_number="00001"),
        candidate(),
        candidate(),
        candidate(license_number="DO00001"),
        candidate(license_number="MD00002", joined_npi=None),
    ]
    captured = snapshot(candidates)
    original = copy.deepcopy(captured)
    retained = retained_snapshot(tmp_path, captured)
    bound_profiles = list(
        registry.bind_snapshot_profiles([profile(), profile("DO00001"), profile("MD00002")], **retained)
    )
    for index, (source_record, facts) in enumerate(bound_profiles):
        decision = source_record["match_evidence"]["registry_binding"]
        assert decision["retained_snapshot"]["candidate_occurrence_indexes"] == [[2, 3], [4], [5]][index]
        assert decision["retained_snapshot"]["snapshot_sha256"] == retained["snapshot_sha256"]
        assert decision["retained_snapshot"]["row_count"] == 6
        assert decision["retained_snapshot"]["integrity_verified"] is True
        assert decision["retained_snapshot"]["capture_acceptance"] == "not_established_by_replay"
        assert decision["registry_completeness_verified"] is False
        assert all(fact["published_at"] is None for fact in facts)
    assert bound_profiles[0][0]["matched_npi"] == bound_profiles[1][0]["matched_npi"] == 1003000126
    assert bound_profiles[2][0]["matched_npi"] is None and bound_profiles[2][0]["match_status"] == "identity_conflict"
    assert bound_profiles[0][0]["match_evidence"]["registry_binding"]["candidate_rows"] == candidates[2:4]
    assert captured == original


def test_adapter_retains_ambiguity_and_does_not_infer_missing_prefix(tmp_path):
    candidates = [
        candidate(),
        candidate(npi=1003000134, joined_npi=1003000134),
        candidate(npi=1003000134, joined_npi=1003000134, license_number="00002"),
    ]
    retained = retained_snapshot(tmp_path, snapshot(candidates))
    bound_profiles = list(registry.bind_snapshot_profiles([profile(), profile("MD00002")], **retained))
    assert bound_profiles[0][0]["match_status"] == "ambiguous" and bound_profiles[0][0]["matched_npi"] is None
    assert bound_profiles[1][0]["match_status"] == "unmatched" and bound_profiles[1][0]["matched_npi"] is None
    assert all(fact["npi"] is None for _, facts in bound_profiles for fact in facts)


class RegistryCursor:
    def __init__(self, candidates):
        self.candidates = candidates

    async def cursor(self, _query, *, prefetch):
        assert prefetch == 500
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
    candidates = [candidate(), candidate(taxonomy_occurrence_checksum=2)]
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
        await registry._registry_rows(RegistryCursor(candidates), "synthetic", expected, progress)


async def test_empty_complete_cursor_stays_empty():
    captured = await registry._registry_rows(RegistryCursor([]), "synthetic", 0, AsyncMock())
    assert captured["registry_rows"] == [] and captured["row_count"] == 0
    assert captured["registry_rows_sha256"] == hashlib.sha256(b"[]").hexdigest()


@pytest.mark.parametrize("initialized", [False, True])
async def test_owned_connection_uses_database_url_and_timeouts(monkeypatch, initialized):
    url = make_url("postgresql+asyncpg://synthetic:example@localhost/registry_test")
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
        dsn="postgresql://synthetic:example@localhost/registry_test",
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
        assert query == "SELECT count(*) FROM (" + registry.capture_query() + ") AS occurrences"
        return len(candidates)

    async def read_rows(query, *, prefetch):
        assert connection.active and query == registry.capture_query() and prefetch == 500
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
    captured = await registry.capture_registry_snapshot("mrf", progress)
    assert captured == snapshot(candidates)
    assert progress.await_args_list == [call(0, 0), call(500, 501), call(501, 501)]
    assert connection.fetchrow.await_args_list == [call(registry.SNAPSHOT_SQL), call(registry.SNAPSHOT_SQL)]
    connection.fetch.assert_awaited_once_with(
        "SELECT name, to_regclass(name)::oid::bigint AS oid FROM unnest($1::text[]) name",
        ["mrf.npi", "mrf.npi_taxonomy", "mrf.nucc_taxonomy"],
    )
    connection.close.assert_awaited_once_with(timeout=10)
    connection.terminate.assert_not_called()
    assert connection.closed and not connection.active
    retained = retained_snapshot(tmp_path, captured)
    assert (
        registry.read_registry_snapshot(retained["snapshot_path"], snapshot_sha256=retained["snapshot_sha256"])
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
        await registry.capture_registry_snapshot("mrf", progress)
    assert not connection.active
    if failure == "connect":
        connection.close.assert_not_awaited()
        connection.terminate.assert_not_called()
    else:
        connection.close.assert_awaited_once_with(timeout=10)
        assert connection.closed
        assert connection.terminate.call_count == (1 if failure == "close" else 0)
