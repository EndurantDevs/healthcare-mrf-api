# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Portable SOURCE SQL preservation and transaction/caller checks, not native proof."""

from __future__ import annotations

import asyncio
import datetime as dt
import re
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from sqlalchemy import BigInteger
from sqlalchemy.dialects.postgresql import dialect
from sqlalchemy.dialects.postgresql.asyncpg import dialect as asyncpg_dialect
from sqlalchemy.exc import DBAPIError

import process.custom_import.build_source as staging
import process.custom_import.source_finalize_sql as finalizer
from process.custom_import.runner_types import CandidateRunnerError, LeaseAuthorityLost
from tests.test_custom_import_build_source import _request
from tests.test_custom_import_build_source_bulk import _bulk_context, _prepared_page
from tests.test_custom_import_segmented_runner import _install_flow, _run

_ROOT = Path(__file__).resolve().parents[1]
_BATCH = UUID(int=17)

_NATIVE_SCALAR_FORMS = [
    [
        "sha256(decode('637573746f6d2d696d706f72742f76310063616e6469646174652d72756e6e65722f3100','hex')\n            ||convert_to('root-key','UTF8')||decode('00','hex')||convert_to(l.typed_key,'UTF8'))",
        "__CONTROL__.source_bulk_digest('root-key',l.typed_key)",
    ],
    [
        "sha256(decode('637573746f6d2d696d706f72742f76310063616e6469646174652d72756e6e65722f3100','hex')\n                ||convert_to(:kind||'-payload','UTF8')||decode('00','hex')||convert_to(l.payload,'UTF8'))",
        "__CONTROL__.source_bulk_digest(:kind||'-payload',l.payload)",
    ],
    [
        "sha256(decode('637573746f6d2d696d706f72742f76310063616e6469646174652d72756e6e65722f3100','hex')\n                ||convert_to('child-key','UTF8')||decode('00','hex')||convert_to(l.child_key,'UTF8'))",
        "__CONTROL__.source_bulk_digest('child-key',l.child_key)",
    ],
    [
        "('{\"code\":'||to_json(l.rejection_code)::text\n                ||',\"contract\":\"custom-import-rejection/v1\",\"root_key_sha256\":'\n                ||coalesce(to_json(encode(l.rejection_hash,'hex'))::text,'null')||'}')",
        "__CONTROL__.source_bulk_canonical(jsonb_build_object(\n                'code',l.rejection_code,'contract','custom-import-rejection/v1',\n                'root_key_sha256',CASE WHEN l.rejection_hash IS NULL THEN NULL ELSE encode(l.rejection_hash,'hex') END))",
    ],
    [
        "array_to_json(ARRAY[\n                to_json(pack_ordinal),to_json(encode(pack_sha256,'hex')),to_json(part_ordinal),\n                to_json(part_row_ordinal),to_json(source_ordinal),to_json(raw_key),to_json(encode(raw_hash,'hex')),\n                to_json(typed_key),to_json(encode(typed_hash,'hex')),to_json(payload),to_json(encode(payload_hash,'hex')),\n                to_json(child_key),to_json(encode(child_hash,'hex')),to_json(rejection_code),to_json(rejection_key),\n                to_json(encode(rejection_hash,'hex')),to_json(rejection_evidence)])::text",
        "__CONTROL__.source_bulk_canonical(jsonb_build_array(\n                pack_ordinal,encode(pack_sha256,'hex'),part_ordinal,part_row_ordinal,source_ordinal,\n                raw_key,encode(raw_hash,'hex'),typed_key,encode(typed_hash,'hex'),payload,encode(payload_hash,'hex'),\n                child_key,encode(child_hash,'hex'),rejection_code,rejection_key,encode(rejection_hash,'hex'),\n                rejection_evidence))",
    ],
]


def _leaf():
    historical = (_ROOT / "alembic/sql/custom_import_bulk_snapshot_writers/snapshot_writers.sql").read_text()
    return historical.split("CREATE FUNCTION __CANDIDATE__.source_set_finalize(", 1)[1].split("END $fn$;", 1)[0]


def _compact(query):
    return " ".join(query.split())


def _unbound(query):
    query = _compact(query)
    # The legacy local n is INTEGER; the application's bounded count bind is BIGINT.
    query = query.replace("CAST(:n-1 AS integer)", ":n-1")
    for native, legacy in _NATIVE_SCALAR_FORMS:
        query = query.replace(_compact(native), _compact(legacy))
    for name, (namespace, table) in finalizer._RELATIONS_BY_NAME.items():
        query = query.replace(f"CAST(:{name} AS regclass)", f"'__{namespace}__.{table}'::regclass")
    query = query.replace("CAST(:a_transaction_id AS xid8)", "a.transaction_id")
    query = re.sub(r"(?<!:):(a|b|s|stream)_([a-z0-9_]+)\b", r"\1.\2", query)
    return re.sub(r"(?<!:):([a-z0-9_]+)\b", r"\1", query)


def _checks(query):
    return re.findall(r"IF ([\s\S]+?) THEN\s*RAISE EXCEPTION '([^']+)'; END IF;", query)


def test_error_predicates_preserve_canonical_provenance_null_positions_and_bounds():
    # Exclude the non-error ANALYZE conditional before extracting later checks.
    historical = re.sub(r"IF a.first_source=0[\s\S]+?END IF;", "", _leaf())
    historical = re.sub(
        r"IF NOT EXISTS\(SELECT 1 FROM __CANDIDATE__.source_bulk_landing\)[\s\S]+?END IF;", "", historical
    )
    # Assignment fusion resolves dictionary keys before the single landing update.
    historical = historical.replace(
        "LEFT JOIN __CONTROL__.custom_import_root_record r ON r.root_record_id=l.root_id",
        "LEFT JOIN __CONTROL__.custom_import_root_record r ON r.dataset_id=b.dataset_id AND r.key_contract_sha256=key_contract AND r.logical_key_sha256=l.typed_hash",
    )
    historical = historical.replace(
        "JOIN __CONTROL__.custom_import_root_record global_key ON global_key.root_record_id=l.root_id",
        "JOIN __CONTROL__.custom_import_root_record global_key ON global_key.dataset_id=b.dataset_id AND global_key.key_contract_sha256=key_contract AND global_key.logical_key_sha256=l.typed_hash",
    )
    historical = historical.replace(
        "(SELECT count(DISTINCT root_id) FROM __CANDIDATE__.source_bulk_landing WHERE batch_id=p_batch)",
        "(SELECT count(DISTINCT r.root_record_id) FROM __CANDIDATE__.source_bulk_landing l JOIN __CONTROL__.custom_import_root_record r ON r.dataset_id=b.dataset_id AND r.key_contract_sha256=key_contract AND r.logical_key_sha256=l.typed_hash WHERE l.batch_id=p_batch)",
    )
    checks = _checks(historical)
    ordinary_checks = [
        re.fullmatch(r"SELECT CASE WHEN ([\s\S]+) THEN '([^']+)' END problem;\s*", _unbound(query)).groups()
        for name, query in finalizer._load_sql().items()
        if name not in ("fresh_lease",) and " END problem;" in query
    ]
    assert [(_compact(predicate), code) for predicate, code in ordinary_checks] == [
        (_compact(predicate), code) for predicate, code in checks[:-1]
    ]


@pytest.mark.parametrize(
    "name,table",
    [
        ("insert_roots", "custom_import_root_revision"),
        ("insert_children", "custom_import_child_revision"),
        ("insert_rejections", "custom_import_rejection"),
        ("insert_occurrences", "custom_import_build_occurrence"),
        ("completion", "source_bulk_completion"),
        ("advance_stream", "custom_import_build_stream"),
        ("advance_build", "custom_import_build_attempt"),
    ],
)
def test_unchanged_promotion_completion_and_accounting_statements(name, table):
    query = _unbound(finalizer._load_sql()[name])
    if query.startswith("WITH written AS ("):
        query = query.removeprefix("WITH written AS (").split(" RETURNING 1)", 1)[0] + ";"
    marker = "UPDATE " if name.startswith("advance") else "INSERT INTO "
    namespace = (
        "__CONTROL__"
        if table in ("source_bulk_completion", "custom_import_build_stream", "custom_import_build_attempt")
        else "__CANDIDATE__"
    )
    historical = _leaf().split(marker + namespace + "." + table + "(", 1) if marker.startswith("INSERT") else []
    if historical:
        historical = marker + namespace + "." + table + "(" + historical[1].split(";", 1)[0] + ";"
    else:
        historical = (
            marker
            + namespace
            + "."
            + table
            + " "
            + _leaf().split(marker + namespace + "." + table + " ", 1)[1].split(";", 1)[0]
            + ";"
        )
    assert _compact(query) == _compact(historical)


def test_fused_assignment_is_one_ordered_bounded_landing_update():
    query = _unbound(finalizer._load_sql()["assign"])
    assert query.count("UPDATE __CANDIDATE__.source_bulk_landing") == 1
    assert "SET root_id=assigned.root_record_id,pack_id=assigned.pack_id,outcome_id=assigned.id" in query
    assert "assigned AS MATERIALIZED" in query and "ORDER BY l.landing_ordinal" in query
    assert "JOIN inserted p ON p.pack_ordinal=l.pack_ordinal" in query
    assert "r.key_contract_sha256=key_contract AND r.logical_key_sha256=l.typed_hash" in query
    assert "WHEN l.rejection_code IS NOT NULL THEN" in query
    assert query.count("nextval(") == 1
    assert "array_to_json(ARRAY[" in finalizer._load_sql()["completion"]
    assert "to_json(rejection_evidence)])::text,E'\\n' ORDER BY landing_ordinal)" in finalizer._load_sql()["completion"]


def test_metadata_scope_and_closure_remain_server_derived_and_no_new_routines():
    queries = finalizer._load_sql()
    assert "FOR UPDATE OF a,b,s" in queries["scope"]
    assert (
        "a.batch_id=:p_batch" in queries["scope"]
        and "stream.definition_revision_id=b.definition_revision_id" in queries["scope"]
    )
    assert "d.canonical_definition::jsonb#>'{schema,root,logical_key}'" in queries["metadata"]
    assert "d.definition_revision_id=:b_definition_revision_id" in queries["metadata"]
    assert "a.opened_by=session_user" in queries["copy_closure"]
    assert "a.transaction_id=pg_current_xact_id() AND a.accepting" in queries["copy_closure"]
    assert "session_user::text AS role_name" in queries["copy_closure"]
    assert "least(l.expires_at,:b_build_deadline_at)<=clock_timestamp()" in queries["fresh_lease"]
    assert not re.search(
        r"\b(CREATE|DROP|ALTER|FUNCTION|PROCEDURE|TRIGGER|CALL|EXECUTE|GRANT)\b|^\s*DO\b",
        "\n".join(queries.values()),
        re.I | re.M,
    )
    assert not re.search(r"snapshot_writers\.sql|plpgsql", Path(finalizer.__file__).read_text(), re.I)


def test_native_scalar_fields_keep_only_batch_helpers():
    queries = finalizer._load_sql()
    for native, legacy in _NATIVE_SCALAR_FORMS:
        query = queries["completion"] if "jsonb_build_array" in legacy else queries["row_identity"]
        assert _compact(query).count(_compact(native)) == 1
        assert legacy not in query
    completion = queries["completion"]
    assert len(re.findall(r"\bto_json\(", completion)) == 17
    assert not re.search(r"(raw_key|typed_key|payload|child_key|rejection_key|rejection_evidence)::jsonb?", completion)
    assert " ORDER BY landing_ordinal),'UTF8'))" in completion
    assert "source_bulk_canonical" not in queries["row_identity"] + completion
    assert "source_bulk_digest" not in queries["row_identity"]
    assert "source_bulk_digest('root-key-contract',__CONTROL__.source_bulk_canonical(" in queries["metadata"]


def test_home_ids_stay_in_the_verified_database_aggregate():
    query = finalizer._load_sql()["homes"]
    assert "__CONTROL__.append_custom_import_revision_home(:family_id," in query
    assert "coalesce(array_agg(outcome_id) FILTER(WHERE :kind='root'),'{}'::bigint[])" in query
    assert "coalesce(array_agg(outcome_id) FILTER(WHERE :kind='child'),'{}'::bigint[])" in query
    assert "WHERE batch_id=:p_batch AND rejection_code IS NULL" in query
    assert "fresh_root_ids" not in Path(finalizer.__file__).read_text()
    assert "fresh_child_ids" not in Path(finalizer.__file__).read_text()


async def test_ordinal_record_bounds_match_native_integer_fields_with_bigint_count_bind():
    query = finalizer._load_sql()["ordinal_bounds"]
    assert "SELECT ROW(min(landing_ordinal),max(landing_ordinal))" in query
    assert "IS DISTINCT FROM ROW(0,CAST(:n-1 AS integer))" in query
    assert isinstance(finalizer._parameter_type("n"), BigInteger)
    statements, _, _ = await finalizer._statements(_Session(), 7)
    compiled = str(statements["ordinal_bounds"].compile(dialect=asyncpg_dialect()))
    assert re.search(r"ROW\(0,CAST\(\$\d+::BIGINT-1 AS integer\)\)", compiled)
    assert "n" in statements["ordinal_bounds"]._bindparams
    assert "n NOT BETWEEN 1 AND 100000" in _leaf()
    assert ":n NOT BETWEEN 1 AND 100000" in finalizer._load_sql()["batch_bounds"]
    assert "THEN 'source_bulk_bounds'" in finalizer._load_sql()["batch_bounds"]
    # This is the only record-valued scalar subquery, not a blanket bind-type rewrite.
    assert sum(len(re.findall(r"\(SELECT ROW\(", value)) for value in finalizer._load_sql().values()) == 1
    assert sum(value.count("CAST(:n-1 AS integer)") for value in finalizer._load_sql().values()) == 1


class _Result:
    def __init__(self, row=None):
        self.row, self.returns_rows = row, row is not None

    def mappings(self):
        return self

    def one(self):
        return self.row

    def scalar_one(self):
        return 7


class _Transaction:
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        self.session.events.append("begin")
        self.session.active = True
        self.original_path = self.session.search_path

    async def __aexit__(self, kind, error, traceback):
        self.session.events.append("rollback" if error else "commit")
        if error is None:
            self.session.durable.extend(self.session.pending)
        self.session.pending.clear()
        self.session.active = False
        self.session.search_path = self.original_path


class _Session:
    def __init__(self, fail_at=None, error=None, close_copy=True):
        self.fail_at, self.error, self.close_copy = fail_at, error, close_copy
        self.events, self.pending, self.durable, self.executed = [], [], [], []
        self.active, self.search_path = False, '"synthetic_untrusted", pg_catalog'
        self.schema, self.role = 'synthetic_control"; SELECT 1; --', 'synthetic_role"; SELECT 1; --'
        quote = dialect().identifier_preparer.quote_schema
        self.name_by_sql = {
            query.replace("__CONTROL__", quote(self.schema)).replace("__CANDIDATE__", "ci_snapshot_7"): name
            for name, query in finalizer._load_sql().items()
        }

    async def __aenter__(self):
        return self

    async def __aexit__(self, kind, error, traceback):
        self.events.append("close")

    def begin(self):
        return _Transaction(self)

    def in_transaction(self):
        return self.active

    async def connection(self):
        return SimpleNamespace(
            dialect=dialect(),
            sync_connection=SimpleNamespace(
                get_execution_options=lambda: {
                    "schema_translate_map": {finalizer.CustomImportBuildAttempt.__table__.schema: self.schema}
                }
            ),
        )

    async def execute(self, statement, parameters=None):
        query = str(statement)
        if query == "SELECT pg_catalog.set_config('search_path', 'pg_catalog', true)":
            self.events.append("catalog_path")
            self.search_path = "pg_catalog"
            return _Result({"set_config": "pg_catalog"})
        assert self.search_path == "pg_catalog"
        name = "revoke" if query.startswith("REVOKE") else self.name_by_sql[query]
        self.events.append(name)
        self.executed.append((name, statement, dict(parameters or {})))
        if name == self.fail_at:
            raise self.error
        if name.startswith(("insert_", "advance_")) or name in (
            "assign",
            "completion",
            "homes",
            "delete_landing",
            "truncate_landing",
            "close_authorization",
            "revoke",
        ):
            self.pending.append(name)
        return _Result(_row(name, self.close_copy, self.role))


def _row(name, close_copy, role):
    query = finalizer._load_sql().get(name, "")
    if name == "scope":
        row_by_name = {key: 1 for key in re.findall(r" AS ((?:a|b|s|stream)_[a-z0-9_]+)", query)}
        row_by_name.update(
            b_phase="source",
            b_producing_token_sha256=b"s" * 32,
            a_token_hash=b"s" * 32,
            b_build_deadline_at=dt.datetime(2029, 1, 1, tzinfo=dt.UTC),
            a_transaction_id="42",
            s_replay_verified_at=None,
            stream_record_kind="root",
            stream_collection_slot=None,
        )
        return row_by_name
    if " END problem;" in query:
        return {"problem": None}
    return {
        "metadata": dict(kind="root", label="root", key_contract=b"k" * 32, part_count=1),
        "landing_counts": dict(n=1, bytes=100),
        "pack_counts": dict(pack_n=1, last_part=1, rejected_n=0),
        "last_row": dict(last_row=1),
        "dictionary_counts": dict(dictionary_n=1, dictionary_bytes=100, dictionary_reference_n=1),
        "insert_global_keys": dict(dictionary_written_n=1),
        "global_key_reads": dict(dictionary_read_n=1, dictionary_bytes=100),
        "insert_candidate_keys": dict(dictionary_written_n=1),
        "candidate_key_reads": dict(dictionary_read_n=1, dictionary_bytes=100),
        "insert_roots": dict(root_written_n=1),
        "insert_children": dict(child_written_n=0),
        "insert_rejections": dict(written_n=0),
        "insert_occurrences": dict(written_n=1),
        "needs_occurrence_analyze": dict(needed=True),
        "homes": dict(appended=None),
        "landing_empty": dict(empty=True),
        "copy_closure": dict(close_copy=close_copy, role_name=role),
    }.get(name)


def _page(monkeypatch, *, fail_at=None, error=None, close_copy=True, final_error=None):
    session = _Session(fail_at, error, close_copy)
    build = SimpleNamespace(phase="source")
    for name in ("_prepare_statement", "_flush_page", "lock_execution", "lock_lease"):
        monkeypatch.setattr(staging, name, AsyncMock())
    monkeypatch.setattr(staging, "_lock_page", AsyncMock(return_value=build))
    monkeypatch.setattr(finalizer, "_prepare_statement", AsyncMock())

    async def call(_session, name, arguments):
        assert session.active and session.search_path == "pg_catalog"
        session.events.append(name)
        if name == fail_at:
            raise error
        return _Result()

    called = AsyncMock(side_effect=call)
    monkeypatch.setattr(finalizer, "_call", called)

    async def verify(*arguments):
        session.events.append("verify_before_commit")
        if final_error:
            raise final_error
        return dt.datetime(2029, 1, 1, tzinfo=dt.UTC)

    monkeypatch.setattr(staging, "verify_live_attempt", verify)
    return session, called


async def _finalize(session):
    async with staging._page_session(lambda: session, _request(), 1):
        return await finalizer.finalize_source_batch(session, _BATCH, [])


async def test_full_fixed_stage_order_closure_and_outer_fresh_lease(monkeypatch):
    session, called = _page(monkeypatch)
    assert await _finalize(session) == 1
    assert session.events[-4:] == ["lock_custom_import_build", "verify_before_commit", "commit", "close"]
    assert session.events.index("completion") < session.events.index("homes") < session.events.index("delete_landing")
    assert next(parameters for name, _, parameters in session.executed if name == "homes")["family_id"] == 7
    assert (
        session.events.index("close_authorization")
        < session.events.index("fresh_lease")
        < session.events.index("revoke")
    )
    assert session.search_path == '"synthetic_untrusted", pg_catalog' and not session.pending
    assert "completion" in session.durable and "revoke" in session.durable
    revoke = next(str(statement) for name, statement, _ in session.executed if name == "revoke")
    expected_columns = ",".join(dialect().identifier_preparer.quote(column) for column in staging._SOURCE_COPY_COLUMNS)
    assert (
        revoke
        == f'REVOKE INSERT ({expected_columns}) ON TABLE ci_snapshot_7.source_bulk_landing FROM "synthetic_role""; SELECT 1; --"'
    )
    assert [invocation.args[1] for invocation in called.await_args_list] == [
        "resolve_custom_import_source_batch_snapshot",
        "verify_custom_import_snapshot_writers",
        "resolve_custom_import_build_base_snapshot",
        "lock_custom_import_build",
    ]
    assert all(
        not isinstance(parameter.type, type(None))
        for _, statement, _ in session.executed
        for parameter in statement._bindparams.values()
    )


async def test_other_open_authorization_keeps_exact_copy_grant(monkeypatch):
    session, _ = _page(monkeypatch, close_copy=False)
    assert await _finalize(session) == 1
    assert "revoke" not in session.events and session.events[-2:] == ["commit", "close"]


@pytest.mark.parametrize(
    "step",
    [
        "resolve_custom_import_build_base_snapshot",
        "row_identity",
        "assign",
        "stored_identity",
        "counter_cursor",
        "completion",
        "homes",
        "close_authorization",
        "fresh_lease",
        "revoke",
        "lock_custom_import_build",
    ],
)
@pytest.mark.parametrize("error", [RuntimeError("synthetic storage error"), asyncio.CancelledError("synthetic cancel")])
async def test_every_batch_failure_or_cancel_escapes_and_rolls_back(monkeypatch, step, error):
    session, _ = _page(monkeypatch, fail_at=step, error=error)
    with pytest.raises(type(error)) as caught:
        await _finalize(session)
    assert caught.value is error
    assert session.events[-2:] == ["rollback", "close"] and "commit" not in session.events
    assert not session.durable and not session.pending
    assert session.search_path == '"synthetic_untrusted", pg_catalog'


async def test_final_outer_authority_failure_rolls_back_completed_batch_and_closure(monkeypatch):
    error = LeaseAuthorityLost("synthetic expiry before commit")
    session, _ = _page(monkeypatch, final_error=error)
    with pytest.raises(LeaseAuthorityLost):
        await _finalize(session)
    assert "completion" in session.events and "revoke" in session.events
    assert session.events[-2:] == ["rollback", "close"] and not session.durable


async def test_no_transaction_is_not_an_authority_pass_marker(monkeypatch):
    session, called = _page(monkeypatch)
    with pytest.raises(finalizer.SourceFinalizationError, match="authorization transaction"):
        await finalizer.finalize_source_batch(session, _BATCH, [])
    called.assert_not_awaited()
    assert not session.events


@pytest.mark.parametrize("old", [True, False])
async def test_real_segmented_caller_does_not_terminally_relabel_storage_failure(monkeypatch, old):
    flow = _install_flow(monkeypatch)
    error = (
        DBAPIError("SELECT synthetic", {}, RuntimeError("synthetic old source error"))
        if old
        else finalizer.SourceFinalizationError("source_bulk_row_mismatch")
    )
    session, _ = _page(monkeypatch, fail_at="row_identity", error=error)
    context = _bulk_context()
    copied = AsyncMock()
    session.scalars = AsyncMock(
        return_value=SimpleNamespace(
            one=lambda: SimpleNamespace(
                next_pack_ordinal=0, next_part_ordinal=1, next_part_row_ordinal=0, next_source_ordinal=0
            )
        )
    )
    monkeypatch.setattr(staging, "_copy_source_landing", copied)
    monkeypatch.setattr(staging, "_is_source_writer_owner", AsyncMock(return_value=not old))

    async def call(_session, name, arguments):
        if name == "source_bulk_authorize":
            return SimpleNamespace(scalar_one=lambda: _BATCH)
        assert name == "source_set_finalize"
        assert old, "the owner route must not call the legacy finalizer"
        raise error

    monkeypatch.setattr(staging, "_call", call)

    async def stage(*arguments, **keywords):
        return await staging._store_pages(lambda: session, context, (_prepared_page(context),))

    flow.calls["stage_segmented_source"].side_effect = stage
    with pytest.raises(type(error)) as caught:
        await _run(flow)
    assert caught.value is error and not isinstance(error, CandidateRunnerError)
    flow.calls["_finish"].assert_not_awaited()
    flow.calls["_activate"].assert_not_awaited()
    flow.calls["count_source_outcomes"].assert_not_awaited()
    copied.assert_awaited_once()
    assert session.events[-2:] == ["rollback", "close"] and not session.durable
