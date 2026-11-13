# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""A discovery run finishes when a minority of its sources failed."""

from types import SimpleNamespace

import pytest
from sqlalchemy.dialects import postgresql

from process import mrf_discovery_partial_batch as partial_batch
from process import mrf_source_discovery as discovery
from process.mrf_discovery_checkpoints import DiscoverySourceBatchIncomplete, SourceBatchSummary


def _summary(*, total=10, completed=9, failed=1):
    return SourceBatchSummary(
        root_run_id="run_root",
        source_set_count=total,
        source_set_sha256="a" * 64,
        completed_source_count=completed,
        completed_source_set_sha256="b" * 64,
        failed_source_count=failed,
        urls_checked=completed,
        plans_discovered=3,
        files_discovered=5,
        bytes_streamed=7,
    )


def test_withdrawal_untags_only_the_named_sources_of_this_run():
    statement = partial_batch.source_withdrawal_statement(["source_b"], "run_current")
    sql = str(statement.compile(dialect=postgresql.dialect()))

    assert "SET metadata_json=CAST(CAST(mrf_source.metadata_json AS JSONB) - " in sql.replace(
        "mrf.mrf_source", "mrf_source"
    )
    assert "source_id = any(CAST(" in sql
    assert "metadata_json ->> " in sql


async def test_withdrawal_runs_one_update_and_skips_an_empty_set(monkeypatch):
    executed_statements = []

    class _Session:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return False

        async def execute(self, statement):
            executed_statements.append(statement)

    monkeypatch.setattr(partial_batch, "db", SimpleNamespace(session=_Session))

    await partial_batch.withdraw_sources_from_discovery_run([], "run_current")
    assert executed_statements == []
    await partial_batch.withdraw_sources_from_discovery_run(["source_b"], "run_current")
    assert len(executed_statements) == 1


def _state(pending_source_ids, *, crawl=True):
    async def pending_sources(root_run_id):
        assert root_run_id == "run_root"
        return [{"source_id": source_id} for source_id in pending_source_ids]

    return SimpleNamespace(
        needs_source_load=True,
        run_context_dict={},
        result=SimpleNamespace(source_batch_summary=None),
        checkpoint_root_run_id="run_root",
        control_run_id="run_current",
        checkpoint_store=SimpleNamespace(pending_sources=pending_sources),
        concurrency=1,
        process_workers=1,
        test_mode=False,
        check_urls=False,
        crawl=crawl,
        max_toc_bytes=1,
        crawl_target_limit=None,
        target_concurrency=1,
        source_http_connection_limit=1,
        source_http_per_host_limit=1,
    )


def _source_row(source_id, index_url):
    return {"source_id": source_id, "index_url": index_url, "source_tier": "mrf_importable", "status": "active"}


def _discovery_sources(independent_count, shared_count=0):
    """Build sources with their own index page plus sources that share one page."""
    source_rows = [
        _source_row(f"source_{position:03d}", f"https://payer.example/{position}")
        for position in range(independent_count)
    ]
    source_rows += [
        _source_row(f"shared_{position:03d}", "https://payer.example/shared") for position in range(shared_count)
    ]
    return source_rows


def _shared_crawl_owner_id(source_rows):
    shared_source_ids = {row["source_id"] for row in source_rows if row["source_id"].startswith("shared_")}
    (owner_id,) = discovery._crawl_representative_source_ids(source_rows) & shared_source_ids
    return owner_id


@pytest.fixture
def discovery_calls(monkeypatch):
    calls_by_name = {"withdrawn": [], "manifests": [], "failures": []}

    async def persist_identity_errors(*_args):
        return None

    async def withdraw(source_ids, discovery_run_id):
        calls_by_name["withdrawn"].append((source_ids, discovery_run_id))

    async def refresh(source_rows, *, source_discovery_run_id):
        calls_by_name["manifests"].append(([row["source_id"] for row in source_rows], source_discovery_run_id))

    async def record_failure(_state, error):
        calls_by_name["failures"].append(error)

    monkeypatch.setattr(discovery, "_persist_running_identity_errors", persist_identity_errors)
    monkeypatch.setattr(partial_batch, "withdraw_sources_from_discovery_run", withdraw)
    monkeypatch.setattr(discovery, "refresh_catalog_paging_manifests", refresh)
    monkeypatch.setattr(discovery, "_record_discovery_command_failure", record_failure)
    return calls_by_name


def _fail_batch_with(monkeypatch, error):
    async def execute_batch(*_args):
        raise error

    monkeypatch.setattr(discovery, "_execute_discovery_source_batch", execute_batch)


async def test_a_run_finishes_and_lists_only_sources_whose_crawl_completed(monkeypatch, discovery_calls):
    source_rows = _discovery_sources(28, shared_count=2)
    _fail_batch_with(monkeypatch, DiscoverySourceBatchIncomplete(_summary(total=30, completed=29, failed=1)))
    state = _state([_shared_crawl_owner_id(source_rows)])

    await discovery._execute_discovery_source_work(state, source_rows)

    assert discovery_calls["failures"] == []
    assert discovery_calls["withdrawn"] == [(["shared_000", "shared_001"], "run_current")]
    assert discovery_calls["manifests"] == [([row["source_id"] for row in source_rows[:28]], "run_current")]
    metrics_by_name = state.result.source_batch_summary.proof_metrics()
    assert "discovery_proof_version" not in metrics_by_name
    assert (metrics_by_name["failed_source_count"], metrics_by_name["withdrawn_source_count"]) == (1, 2)
    assert state.result.files == 5
    assert _summary().proof_metrics()["discovery_proof_version"] == 2


@pytest.mark.parametrize(
    ("source_rows", "summary", "max_failed_percent"),
    [
        # Every source depends on the one failed crawl: nothing would stay listed.
        (_discovery_sources(0, shared_count=10), _summary(total=10, completed=9, failed=1), None),
        # One failed checkpoint, but five of twenty sources would be left out.
        (_discovery_sources(15, shared_count=5), _summary(total=20, completed=19, failed=1), None),
        # One source never settled.
        (_discovery_sources(18, shared_count=2), _summary(total=20, completed=18, failed=1), None),
        # A limit of zero keeps the all-or-nothing rule.
        (_discovery_sources(98, shared_count=2), _summary(total=100, completed=99, failed=1), "0"),
    ],
)
async def test_a_run_still_fails_when_too_much_would_be_left_out(
    monkeypatch, discovery_calls, source_rows, summary, max_failed_percent
):
    if max_failed_percent is not None:
        monkeypatch.setenv(partial_batch.MAX_FAILED_SOURCE_PERCENT_ENV, max_failed_percent)
    error = DiscoverySourceBatchIncomplete(summary)
    _fail_batch_with(monkeypatch, error)
    state = _state([_shared_crawl_owner_id(source_rows)])

    with pytest.raises(DiscoverySourceBatchIncomplete):
        await discovery._execute_discovery_source_work(state, source_rows)

    assert discovery_calls["failures"] == [error]
    assert discovery_calls["withdrawn"] == [] and discovery_calls["manifests"] == []
    assert state.result.source_batch_summary is summary


async def test_a_url_only_run_withdraws_only_the_failed_source(monkeypatch, discovery_calls):
    # Without a crawl every source is checked on its own, so a shared page creates no dependency.
    source_rows = _discovery_sources(9, shared_count=2)
    _fail_batch_with(monkeypatch, DiscoverySourceBatchIncomplete(_summary(total=11, completed=10, failed=1)))
    state = _state([_shared_crawl_owner_id(source_rows)], crawl=False)

    await discovery._execute_discovery_source_work(state, source_rows)

    assert discovery_calls["failures"] == []
    (withdrawn_source_ids, _run_id) = discovery_calls["withdrawn"][0]
    assert withdrawn_source_ids == [_shared_crawl_owner_id(source_rows)]
    assert discovery_calls["manifests"] == []
    assert state.result.source_batch_summary.proof_metrics()["withdrawn_source_count"] == 1


async def test_the_limit_is_configurable(monkeypatch, discovery_calls):
    monkeypatch.setenv(partial_batch.MAX_FAILED_SOURCE_PERCENT_ENV, "25")
    source_rows = _discovery_sources(15, shared_count=5)
    _fail_batch_with(monkeypatch, DiscoverySourceBatchIncomplete(_summary(total=20, completed=19, failed=1)))

    await discovery._execute_discovery_source_work(_state([_shared_crawl_owner_id(source_rows)]), source_rows)

    assert discovery_calls["failures"] == []
    assert len(discovery_calls["withdrawn"][0][0]) == 5


async def test_a_failed_withdrawal_is_recorded_as_the_run_failure(monkeypatch, discovery_calls):
    write_error = RuntimeError("catalog write failed")

    async def withdraw(*_args):
        raise write_error

    monkeypatch.setattr(partial_batch, "withdraw_sources_from_discovery_run", withdraw)
    source_rows = _discovery_sources(28, shared_count=2)
    _fail_batch_with(monkeypatch, DiscoverySourceBatchIncomplete(_summary(total=30, completed=29, failed=1)))

    with pytest.raises(RuntimeError, match="catalog write failed"):
        await discovery._execute_discovery_source_work(_state([_shared_crawl_owner_id(source_rows)]), source_rows)

    assert discovery_calls["failures"] == [write_error]
    assert discovery_calls["manifests"] == []


async def test_any_other_batch_error_is_recorded_and_raised(monkeypatch, discovery_calls):
    batch_error = ValueError("source set changed")
    _fail_batch_with(monkeypatch, batch_error)

    with pytest.raises(ValueError, match="source set changed"):
        await discovery._execute_discovery_source_work(_state([]), _discovery_sources(2))

    assert discovery_calls["failures"] == [batch_error]
    assert discovery_calls["withdrawn"] == []
