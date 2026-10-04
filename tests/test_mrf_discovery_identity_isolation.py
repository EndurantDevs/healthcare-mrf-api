# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Ambiguous catalog identities must not block independent source candidates."""

import copy
import datetime as dt
import re
from types import SimpleNamespace

import pytest

from process import mrf_payer_identity as identity
from process import mrf_source_discovery as discovery


class _SourceSession:
    def __init__(self, source_rows=(), *, error=None):
        self.source_rows = source_rows
        self.error = error

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def execute(self, *args):
        if self.error is not None:
            raise self.error
        return self

    def mappings(self):
        return self.source_rows


class _CrawlSession(_SourceSession):
    def __init__(self, errors):
        super().__init__()
        self.errors = errors
        self.statement = None

    async def scalar(self, statement):
        self.statement = statement
        return self.errors


def _candidate(name="Example Plan", *, url="https://example.test/ambiguous", **kwargs):
    return discovery.SourceCandidate(
        payer_name=name,
        provider="master-list",
        index_url=url,
        **kwargs,
    )


def _source(candidate, suffix):
    _, source_row = discovery._candidate_to_rows(
        candidate,
        dt.datetime(2025, 1, 1),
        payer_id=f"mrfpayer_{suffix}",
        source_id=f"mrfsource_{suffix}",
    )
    assert source_row is not None
    return source_row


def _curated_candidates():
    return discovery.parse_master_list(
        "| Payer | Type | Public MRF TOC / landing URL | Notes |\n"
        "|---|---|---|---|\n"
        "| Example Group | regional | "
        "https://example.test/one · https://example.test/two | public indexes |\n"
    )


@pytest.mark.asyncio
async def test_ambiguous_source_matching_remains_strict_by_default():
    candidate = _candidate()
    rows = [_source(candidate, "first"), _source(candidate, "second")]

    with pytest.raises(ValueError, match="^mrf_discovery_source_identity_ambiguous$"):
        await identity._match_existing_sources(_SourceSession(rows), [candidate])


@pytest.mark.asyncio
async def test_opt_in_isolation_preserves_source_facts_and_stable_error_metadata():
    ambiguous = _candidate()
    safe = _candidate("Independent Plan", url="https://example.test/safe")
    rows = [_source(ambiguous, "second"), _source(ambiguous, "first"), _source(safe, "safe")]
    original_rows = copy.deepcopy(rows)
    first_errors = []
    resolved, matches, payer_groups = await identity._match_existing_sources(
        _SourceSession(rows), [ambiguous, safe], identity_errors=first_errors
    )
    second_errors = []
    repeated = await identity._match_existing_sources(
        _SourceSession(list(reversed(rows))), [safe, ambiguous], identity_errors=second_errors
    )

    assert resolved == [safe]
    assert matches == [rows[2]]
    assert payer_groups == {}
    assert repeated == (resolved, matches, payer_groups)
    assert rows == original_rows
    assert second_errors == first_errors
    assert len(first_errors) == 1
    error = first_errors[0]
    assert error["code"] == "mrf_discovery_source_identity_ambiguous"
    assert re.fullmatch(r"[0-9a-f]{64}", error["candidate_sha256"])
    assert error["matched_source_ids"] == ["mrfsource_first", "mrfsource_second"]
    assert error["curated_row_pending"] is False
    assert "Example Plan" not in str(error)
    assert "https://" not in str(error)


@pytest.mark.asyncio
@pytest.mark.parametrize("reverse_candidates", [False, True])
async def test_ambiguous_curated_row_is_isolated_before_any_sibling_can_persist(reverse_candidates):
    first, sibling = _curated_candidates()
    safe = _candidate("Independent Plan", url="https://example.test/safe")
    candidates = [first, sibling, safe]
    if reverse_candidates:
        candidates.reverse()
    errors = []

    resolved, matches, payer_groups = await identity._match_existing_sources(
        _SourceSession([_source(first, "first"), _source(first, "second")]),
        candidates,
        identity_errors=errors,
    )

    assert resolved == [safe]
    assert matches == [None]
    assert payer_groups == {}
    assert len(errors) == 2
    direct_error = next(error for error in errors if not error["curated_row_pending"])
    dependent_error = next(error for error in errors if error["curated_row_pending"])
    assert direct_error["matched_source_ids"] == ["mrfsource_first", "mrfsource_second"]
    assert dependent_error["matched_source_ids"] == []
    assert dependent_error["candidate_sha256"] != direct_error["candidate_sha256"]


@pytest.mark.asyncio
async def test_curated_row_ambiguity_does_not_hold_an_independent_employer_query():
    first, sibling = _curated_candidates()
    ambiguous = discovery._candidate_with_target_payer_query(first, "Example First Employer")
    held_sibling = discovery._candidate_with_target_payer_query(sibling, "Example First Employer")
    safe_first = discovery._candidate_with_target_payer_query(first, "Example Second Employer")
    safe_sibling = discovery._candidate_with_target_payer_query(sibling, "Example Second Employer")
    errors = []

    resolved, matches, payer_groups = await identity._match_existing_sources(
        _SourceSession([_source(ambiguous, "first"), _source(ambiguous, "second")]),
        [held_sibling, safe_first, ambiguous, safe_sibling],
        identity_errors=errors,
    )

    assert resolved == [safe_first, safe_sibling]
    assert matches == [None, None]
    assert payer_groups == {}
    assert len(errors) == 2
    assert identity._curated_payer_row_key(ambiguous) != identity._curated_payer_row_key(safe_first)
    assert identity._curated_payer_row_key(safe_first) == identity._curated_payer_row_key(safe_sibling)


@pytest.mark.asyncio
async def test_opt_in_isolation_does_not_hide_payer_conflicts_or_database_failures():
    first, second = _curated_candidates()
    errors = []
    with pytest.raises(ValueError, match="^mrf_discovery_payer_identity_conflict$"):
        await identity._match_existing_sources(
            _SourceSession([_source(first, "first"), _source(second, "second")]),
            [first, second],
            identity_errors=errors,
        )
    assert errors == []

    database_failure = RuntimeError("synthetic database failure")
    with pytest.raises(RuntimeError, match="^synthetic database failure$") as caught:
        await identity._match_existing_sources(_SourceSession(error=database_failure), [first], identity_errors=errors)
    assert caught.value is database_failure
    assert errors == []


@pytest.mark.asyncio
async def test_store_candidates_only_saves_independent_candidates(monkeypatch):
    first, sibling = _curated_candidates()
    safe = _candidate("Independent Plan", url="https://example.test/safe")
    rows = [_source(first, "first"), _source(first, "second")]
    session = _SourceSession(rows)
    monkeypatch.setattr(identity.db, "session", lambda: session)
    saved_by_kind = {}

    async def save_payers(_session, candidates, payer_rows, renames, now):
        saved_by_kind["candidates"] = candidates
        saved_by_kind["payers"] = list(payer_rows.values())

    async def save_sources(_session, source_rows, updates, now):
        saved_by_kind["sources"] = list(source_rows.values())

    monkeypatch.setattr(identity, "_save_payers", save_payers)
    monkeypatch.setattr(identity, "_save_sources", save_sources)
    errors = []
    payer_rows, source_rows = await identity.store_candidates(
        [sibling, safe, first], discovery_run_id="run_isolated", identity_errors=errors
    )

    assert saved_by_kind["candidates"] == [safe]
    assert payer_rows == saved_by_kind["payers"]
    assert source_rows == saved_by_kind["sources"]
    assert len(payer_rows) == len(source_rows) == 1
    assert source_rows[0]["index_url"] == safe.index_url
    assert source_rows[0]["metadata_json"]["discovery_run_id"] == "run_isolated"
    assert len(errors) == 2


@pytest.mark.asyncio
async def test_retry_load_is_parent_scoped_and_keeps_only_pending_identity(monkeypatch):
    pending_error_by_field = {"code": identity.SOURCE_IDENTITY_AMBIGUOUS, "candidate_sha256": "a" * 64}
    errors = [pending_error_by_field, {"code": "source_discovery_failed"}, "invalid", None]
    session = _CrawlSession(errors)
    monkeypatch.setattr(identity.db, "session", lambda: session)

    restored_errors = await identity.load_pending_identity_errors("run_exact_parent")

    assert restored_errors == [pending_error_by_field]
    assert restored_errors[0] is not pending_error_by_field
    compiled_query = session.statement.compile()
    assert "run_exact_parent" in compiled_query.params.values()
    assert "WHERE" in str(compiled_query) and "run_id =" in str(compiled_query)
    assert "ORDER BY" in str(compiled_query) and "LIMIT" in str(compiled_query)
    assert 1 in compiled_query.params.values()
    assert session.statement.selected_columns.keys() == ["errors"]


@pytest.mark.asyncio
@pytest.mark.parametrize("has_checkpoint", [False, True, None])
async def test_checkpoint_resume_restores_pending_errors_only_from_exact_parent(monkeypatch, has_checkpoint):
    restored_sources = [{"source_id": "mrfsource_safe", "payer_id": "mrfpayer_safe"}]
    pending_error_by_field = {"code": identity.SOURCE_IDENTITY_AMBIGUOUS, "candidate_sha256": "a" * 64}
    resumed_calls = []
    loaded_parents = []
    phase_events = []
    persisted_crawl_rows = []

    async def no_op(*args, **kwargs):
        return None

    async def resume_batch(*args):
        resumed_calls.append(args)
        phase_events.append("resume_claim_started")
        if has_checkpoint is None:
            raise SystemExit("synthetic stop during resume claim")
        return restored_sources if has_checkpoint else None

    async def load_pending_errors(parent_run_id):
        loaded_parents.append(parent_run_id)
        phase_events.append("parent_pending_loaded")
        return [pending_error_by_field]

    async def capture_running_rows(crawl_rows, *args, **kwargs):
        persisted_crawl_rows.extend(copy.deepcopy(crawl_rows))
        phase_events.append("running_row_persisted")

    for name in ("init_db", "ensure_database", "_ensure_catalog_tables", "_record_discovery_command_failure"):
        monkeypatch.setattr(discovery, name, no_op)
    monkeypatch.setattr(discovery, "push_objects", capture_running_rows)
    monkeypatch.setattr(
        discovery, "DatabaseDiscoveryCheckpointStore", lambda: SimpleNamespace(resume_batch=resume_batch)
    )
    monkeypatch.setattr(discovery, "_load_pending_identity_errors", load_pending_errors)
    state = _pending_discovery_state(False, [], control_run_id="run_retry")
    state.retry_of_run_id = "run_exact_parent"
    state.mrf_discovery_root_run_id = "run_initial"

    if has_checkpoint is None:
        with pytest.raises(SystemExit, match="^synthetic stop during resume claim$"):
            await discovery._initialize_discovery_persistence(state)
    else:
        await discovery._initialize_discovery_persistence(state)

    assert resumed_calls == [("run_initial", "run_retry", "run_exact_parent")]
    assert loaded_parents == ["run_exact_parent"]
    assert phase_events == ["parent_pending_loaded", "running_row_persisted", "resume_claim_started"]
    assert persisted_crawl_rows[0]["errors"] == [pending_error_by_field]
    assert persisted_crawl_rows[0]["status"] == "running"
    assert state.result.errors == [pending_error_by_field]
    assert state.checkpoint_root_run_id == ("run_retry" if has_checkpoint is False else "run_initial")


@pytest.mark.asyncio
async def test_handled_resume_failure_preserves_pending_identity_in_failed_crawl(monkeypatch):
    pending_error_by_field = {"code": identity.SOURCE_IDENTITY_AMBIGUOUS, "candidate_sha256": "a" * 64}
    persisted_crawl_rows = []

    async def no_op(*args, **kwargs):
        return None

    async def load_pending_errors(parent_run_id):
        assert parent_run_id == "run_exact_parent"
        return [pending_error_by_field]

    async def fail_resume_claim(*args):
        raise RuntimeError("synthetic checkpoint claim failure")

    async def capture_crawl_rows(crawl_rows, *args, **kwargs):
        persisted_crawl_rows.extend(copy.deepcopy(crawl_rows))

    for name in ("init_db", "ensure_database", "_ensure_catalog_tables", "_publish_failed_discovery_state"):
        monkeypatch.setattr(discovery, name, no_op)
    monkeypatch.setattr(discovery, "push_objects", capture_crawl_rows)
    monkeypatch.setattr(discovery, "_load_pending_identity_errors", load_pending_errors)
    monkeypatch.setattr(
        discovery, "DatabaseDiscoveryCheckpointStore", lambda: SimpleNamespace(resume_batch=fail_resume_claim)
    )
    state = _pending_discovery_state(False, [])
    state.retry_of_run_id = "run_exact_parent"
    state.mrf_discovery_root_run_id = "run_initial"

    with pytest.raises(RuntimeError, match="^synthetic checkpoint claim failure$"):
        await discovery._initialize_discovery_persistence(state)

    assert [crawl_row["status"] for crawl_row in persisted_crawl_rows] == ["running", "failed"]
    assert persisted_crawl_rows[-1]["errors"] == [
        pending_error_by_field,
        {"code": "source_discovery_failed", "message": "synthetic checkpoint claim failure"},
    ]
    assert state.result.pending_identity_count == 1
    assert state.result.as_dict()["identity_resolution_complete"] is False


def _install_fallback_startup_fakes(monkeypatch):
    pending_error_by_field = {"code": identity.SOURCE_IDENTITY_AMBIGUOUS, "candidate_sha256": "a" * 64}
    persisted_crawl_rows = []

    async def no_op(*args, **kwargs):
        return None

    async def load_pending_errors(parent_run_id):
        assert parent_run_id == "run_exact_parent"
        return [pending_error_by_field]

    async def capture_crawl_rows(crawl_rows, *args, **kwargs):
        persisted_crawl_rows.extend(copy.deepcopy(crawl_rows))

    for name in (
        "init_db",
        "ensure_database",
        "_ensure_catalog_tables",
        "_publish_failed_discovery_state",
        "_publish_discovery_success",
    ):
        monkeypatch.setattr(discovery, name, no_op)
    for name in (
        "_normalize_discovery_command_state",
        "_initialize_discovery_command_context",
        "_announce_discovery_run",
        "enqueue_live_progress",
    ):
        monkeypatch.setattr(discovery, name, lambda *_args, **_kwargs: None)
    monkeypatch.setattr(discovery, "push_objects", capture_crawl_rows)
    monkeypatch.setattr(discovery, "_load_pending_identity_errors", load_pending_errors)
    monkeypatch.setattr(discovery, "DatabaseDiscoveryCheckpointStore", lambda: SimpleNamespace(resume_batch=no_op))
    monkeypatch.setattr(discovery, "_filter_discovery_candidates", lambda _state, candidates: candidates)
    state = _pending_discovery_state(False, [])
    state.retry_of_run_id = "run_exact_parent"
    state.mrf_discovery_root_run_id = "run_initial"
    state.providers = ["master-list"]
    return state, persisted_crawl_rows


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["provider_failed", "store_failed", "resolved", "current_pending", "resolved_stop"])
async def test_no_checkpoint_retry_preserves_or_replaces_inherited_identity_errors(monkeypatch, outcome):
    state, persisted_crawl_rows = _install_fallback_startup_fakes(monkeypatch)
    store_calls = []
    pending_error_by_field = {"code": identity.SOURCE_IDENTITY_AMBIGUOUS, "candidate_sha256": "b" * 64}
    safe_sources = [{"source_id": "mrfsource_safe", "payer_id": "mrfpayer_safe"}]

    async def load_candidates(provider_name, **kwargs):
        if outcome == "provider_failed":
            raise RuntimeError("synthetic provider failure")
        return [_candidate(), _candidate("Safe Plan", url="https://example.test/safe")]

    async def store_candidates(candidates, *, discovery_run_id, identity_errors):
        store_calls.append(discovery_run_id)
        assert identity_errors == []
        if outcome == "store_failed":
            raise RuntimeError("synthetic identity storage failure")
        if outcome == "current_pending":
            identity_errors.append(pending_error_by_field)
        return [{"payer_id": "mrfpayer_safe"}], safe_sources

    async def checkpoint_boundary(*args):
        if outcome == "resolved_stop":
            assert persisted_crawl_rows[-1]["errors"] == []
            raise SystemExit("synthetic stop after identity resolution")

    monkeypatch.setattr(discovery, "_load_candidates", load_candidates)
    monkeypatch.setattr(discovery, "_store_candidates", store_candidates)
    if outcome == "resolved_stop":
        monkeypatch.setattr(discovery, "_execute_discovery_source_batch", checkpoint_boundary)
        with pytest.raises(SystemExit, match="^synthetic stop after identity resolution$"):
            await discovery._execute_discovery_command(state)
    elif outcome in {"provider_failed", "store_failed"}:
        with pytest.raises(RuntimeError):
            await discovery._execute_discovery_command(state)
    else:
        monkeypatch.setattr(discovery, "_execute_discovery_source_work", checkpoint_boundary)
        await discovery._execute_discovery_command(state)

    assert persisted_crawl_rows[0]["errors"][0]["candidate_sha256"] == "a" * 64
    assert store_calls == ([] if outcome == "provider_failed" else ["run_current"])
    final_errors = persisted_crawl_rows[-1]["errors"]
    if outcome in {"provider_failed", "store_failed"}:
        assert final_errors[0]["candidate_sha256"] == "a" * 64
        assert final_errors[-1]["code"] == "source_discovery_failed"
        assert state.result.pending_identity_count == 1
    elif outcome == "current_pending":
        assert final_errors == [pending_error_by_field]
        assert state.result.pending_identity_count == 1
    else:
        assert all(error.get("code") != identity.SOURCE_IDENTITY_AMBIGUOUS for error in final_errors)
        assert state.result.pending_identity_count == 0
        assert state.result.as_dict()["identity_resolution_complete"] is True


@pytest.mark.asyncio
async def test_all_pending_sources_still_fail_native_discovery(monkeypatch):
    recorded_errors = []

    async def store_pending(candidates, *, discovery_run_id, identity_errors):
        identity_errors.append({"code": identity.SOURCE_IDENTITY_AMBIGUOUS})
        return [], []

    async def record_failure(state, error):
        recorded_errors.append(error)

    monkeypatch.setattr(discovery, "_store_candidates", store_pending)
    monkeypatch.setattr(discovery, "_record_discovery_command_failure", record_failure)
    state = SimpleNamespace(
        resumed_source_rows=None,
        control_run_id="run_pending",
        result=discovery.DiscoveryResult(providers=["master-list"]),
    )

    with pytest.raises(ValueError, match="^mrf_discovery_source_identity_ambiguous$"):
        await discovery._store_discovery_sources(state, [_candidate()])

    assert len(recorded_errors) == 1
    assert state.result.pending_identity_count == 1
    assert state.result.as_dict()["identity_resolution_complete"] is False


@pytest.mark.asyncio
async def test_resumed_sources_keep_pending_identity_visible_without_rebinding(monkeypatch):
    source_rows = [{"source_id": "mrfsource_safe", "payer_id": "mrfpayer_safe"}]
    retagged_sources = []

    async def retag_sources(rows, run_id):
        retagged_sources.append((rows, run_id))

    async def unexpected_rebinding(*args, **kwargs):
        raise AssertionError("checkpoint resume must not rebind catalog identities")

    monkeypatch.setattr(discovery, "_retag_sources_for_discovery_run", retag_sources)
    monkeypatch.setattr(discovery, "_store_candidates", unexpected_rebinding)
    state = SimpleNamespace(
        resumed_source_rows=source_rows,
        control_run_id="run_retry",
        result=discovery.DiscoveryResult(
            providers=["master-list"], errors=[{"code": identity.SOURCE_IDENTITY_AMBIGUOUS}]
        ),
    )

    assert await discovery._store_discovery_sources(state, []) is source_rows
    assert retagged_sources == [(source_rows, "run_retry")]
    assert state.result.payers == 1
    assert state.result.candidates == 2
    assert state.result.pending_identity_count == 1
    assert state.result.as_dict()["identity_resolution_complete"] is False


def _pending_discovery_state(has_checkpoint, safe_sources, *, control_run_id="run_current"):
    return SimpleNamespace(
        dry_run=False,
        control_run_id=control_run_id,
        run_id=control_run_id,
        recorded_failure_run_ids=set(),
        emit_standalone_control_events=False,
        resumed_source_rows=safe_sources if has_checkpoint else None,
        needs_source_load=True,
        bounded_limit=5,
        parsed_source_entity_types=None,
        parsed_source_payer_query=None,
        run_params_dict={},
        test_mode=True,
        check_urls=False,
        crawl=True,
        probe_files=False,
        checkpoint_root_run_id="run_initial" if has_checkpoint else control_run_id,
        checkpoint_store=object(),
        concurrency=1,
        process_workers=1,
        max_toc_bytes=1024,
        crawl_target_limit=None,
        target_concurrency=1,
        source_http_connection_limit=2,
        source_http_per_host_limit=1,
        result=discovery.DiscoveryResult(providers=["master-list"]),
        run_context_dict={
            "crawl_run_id": "crawl_current",
            "control_run_id": control_run_id,
            "providers": ["master-list"],
            "run_mode": "crawl",
            "started_at": dt.datetime(2026, 1, 1),
            "run_params": {},
        },
    )


def _install_pending_kill_window_fakes(monkeypatch, has_checkpoint):
    safe_sources = [{"source_id": "mrfsource_safe", "payer_id": "mrfpayer_safe"}]
    pending_error_by_field = {"code": identity.SOURCE_IDENTITY_AMBIGUOUS, "candidate_sha256": "a" * 64}
    state = _pending_discovery_state(has_checkpoint, safe_sources)
    persisted_crawl_rows = []
    phase_events = []

    async def capture_crawl_rows(crawl_rows, *args, **kwargs):
        persisted_crawl_rows.extend(copy.deepcopy(crawl_rows))
        phase_events.append("crawl_row_persisted")

    async def initialize_persistence(command_state):
        await capture_crawl_rows([discovery._discovery_crawl_run_row(command_state.run_context_dict, status="running")])
        if has_checkpoint:
            command_state.result.errors.append(pending_error_by_field)

    async def load_candidates(command_state):
        return [] if has_checkpoint else [_candidate(), _candidate("Safe Plan", url="https://example.test/safe")]

    async def store_safe_candidates(candidates, *, discovery_run_id, identity_errors):
        assert not has_checkpoint
        identity_errors.append(pending_error_by_field)
        return [{"payer_id": "mrfpayer_safe"}], safe_sources

    async def no_op(*args, **kwargs):
        return None

    async def abrupt_checkpoint_stop(batch_context, checkpoint_store):
        phase_events.append("checkpoint_work_started")
        assert batch_context.source_records == safe_sources
        assert persisted_crawl_rows[-1]["errors"] == [pending_error_by_field]
        raise SystemExit("synthetic abrupt worker stop")

    for name in (
        "_normalize_discovery_command_state",
        "_initialize_discovery_command_context",
        "_announce_discovery_run",
    ):
        monkeypatch.setattr(discovery, name, lambda *_args: None)
    monkeypatch.setattr(discovery, "_initialize_discovery_persistence", initialize_persistence)
    monkeypatch.setattr(discovery, "_load_discovery_candidates", load_candidates)
    monkeypatch.setattr(discovery, "_filter_discovery_candidates", lambda _state, candidates: candidates)
    monkeypatch.setattr(discovery, "_store_candidates", store_safe_candidates)
    monkeypatch.setattr(discovery, "_retag_sources_for_discovery_run", no_op)
    monkeypatch.setattr(discovery, "_record_discovery_command_failure", no_op)
    monkeypatch.setattr(discovery, "push_objects", capture_crawl_rows)
    monkeypatch.setattr(discovery, "_execute_discovery_source_batch", abrupt_checkpoint_stop)
    return state, persisted_crawl_rows, phase_events


@pytest.mark.asyncio
@pytest.mark.parametrize("has_checkpoint", [False, True])
async def test_pending_identities_are_durable_before_checkpoint_worker_can_stop(monkeypatch, has_checkpoint):
    state, persisted_crawl_rows, phase_events = _install_pending_kill_window_fakes(monkeypatch, has_checkpoint)

    with pytest.raises(SystemExit, match="^synthetic abrupt worker stop$"):
        await discovery._execute_discovery_command(state)

    assert phase_events == ["crawl_row_persisted", "crawl_row_persisted", "checkpoint_work_started"]
    assert persisted_crawl_rows[0]["errors"] == []
    assert persisted_crawl_rows[-1]["run_id"] == "run_current"
    assert persisted_crawl_rows[-1]["status"] == "running"
    assert persisted_crawl_rows[-1]["finished_at"] is None
    assert state.result.pending_identity_count == 1
    assert state.result.as_dict()["identity_resolution_complete"] is False


@pytest.mark.asyncio
async def test_failed_crawl_preserves_pending_identity_and_terminal_error(monkeypatch):
    pending_error_by_field = {"code": identity.SOURCE_IDENTITY_AMBIGUOUS, "candidate_sha256": "a" * 64}
    terminal_error_by_field = {"code": "source_discovery_failed", "message": "synthetic source failure"}
    discovery_result = discovery.DiscoveryResult(providers=["master-list"], errors=[pending_error_by_field])
    persisted_rows = []

    async def capture_rows(rows, *args, **kwargs):
        persisted_rows.extend(rows)

    monkeypatch.setattr(discovery, "push_objects", capture_rows)
    await discovery._persist_failed_discovery_crawl_row(
        {
            "result": discovery_result,
            "error_dict": terminal_error_by_field,
            "finished_at": dt.datetime(2026, 1, 2),
            "run_context_dict": {
                "crawl_run_id": "crawl_failed",
                "control_run_id": "run_failed",
                "providers": ["master-list"],
                "run_mode": "crawl",
                "started_at": dt.datetime(2026, 1, 1),
                "run_params": {},
            },
        }
    )

    assert len(persisted_rows) == 1
    assert persisted_rows[0]["status"] == "failed"
    assert persisted_rows[0]["errors"] == [pending_error_by_field, terminal_error_by_field]
    assert discovery_result.errors == [pending_error_by_field]


def test_pending_identity_counters_remain_distinct_from_completed_source_work():
    result = discovery.DiscoveryResult(
        providers=["master-list"],
        sources=1,
        errors=[{"code": identity.SOURCE_IDENTITY_AMBIGUOUS}, {"code": "independent_probe_failed"}],
    )

    assert result.pending_identity_count == 1
    assert result.as_dict()["pending_identity_count"] == 1
    assert result.as_dict()["identity_resolution_complete"] is False
    result.errors.pop(0)
    assert result.pending_identity_count == 0
    assert result.as_dict()["identity_resolution_complete"] is True
