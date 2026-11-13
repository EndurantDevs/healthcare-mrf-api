# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Stopped recovery changes status while retaining candidate data and serving state."""

import json
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_stopped_source_failure as failure
from process.provider_directory_terminal_root_retirement_contract import (
    RETIREMENT_ENABLED_ENV,
    TerminalRootRetirementError,
    TerminalRootRetirementRequest,
)


class Connection:
    def __init__(self):
        self.header = dict(
            dataset_id="dataset-synthetic",
            endpoint_id="endpoint-synthetic",
            import_run_id="run-root",
            acquisition_root_run_id="run-root",
            previous_dataset_id=None,
            status="acquiring",
            is_current=False,
            dataset_hash=None,
            validated_at=None,
            published_at=None,
            superseded_at=None,
            resource_count=0,
            completion_proof_json=None,
            completion_proof_sha256=None,
            publication_metadata_json={"source_ids": ["source-synthetic"], "original": {"value": 1}},
        )
        self.source = dict(
            source_id="source-synthetic",
            endpoint_id="endpoint-synthetic",
            canonical_api_base="https://example.test/fhir",
        )
        self.endpoint = {field: self.source[field] for field in ("endpoint_id", "canonical_api_base")}
        self.lineage = [
            dict(
                run_id="run-root",
                retry_of_run_id=None,
                importer="provider-directory-fhir",
                status="failed",
                finished_at="2026-01-01T00:00:00Z",
                depth=0,
                terminal_age_satisfied=True,
                metrics=None,
                params='{"source_ids":["source-synthetic"]}',
            )
        ]
        self.payloads = [{"id": "retained", "name": "original"}]
        self.checkpoints = [{"next_url": "https://example.test/fhir?cursor=%2f%2B%3d&x=1"}]
        self.calls = []
        self.competing = []
        self.private_competing = []
        self.published = []
        self.predecessor = None
        self.busy_lock = None
        self.in_transaction = True
        self.update_result = "UPDATE 1"
        self.write_error = None
        self.lineage_reads = 0
        self.change_lineage = False

    def is_in_transaction(self):
        return self.in_transaction

    async def fetch(self, query, *arguments):
        self.calls.append((query, arguments))
        if "WITH RECURSIVE lineage" in query:
            self.lineage_reads += 1
            rows = deepcopy(self.lineage)
            if self.change_lineage and self.lineage_reads > 1:
                rows[0]["metrics"] = {"changed": True}
            return rows
        if "provider_directory_api_endpoint" in query:
            return [deepcopy(self.endpoint)]
        if 'FROM "catalog_schema"."provider_directory_source"' in query:
            return [deepcopy(self.source)]
        if "dataset.is_current OR dataset.published_at" in query:
            return self.published
        if "dataset.dataset_id =" in query:
            return [deepcopy(self.predecessor)] if self.predecessor is not None else []
        if "dataset.dataset_id <>" in query:
            return self.competing
        if "AND (status IN" in query:
            return self.private_competing
        if "SELECT run_id" in query:
            return [{"run_id": row["run_id"]} for row in self.lineage]
        raise AssertionError(query)

    async def fetchval(self, query, *arguments):
        self.calls.append((query, arguments))
        assert "pg_try_advisory_xact_lock" in query
        is_available = arguments[0] != self.busy_lock
        return is_available

    async def fetchrow(self, query, *arguments):
        self.calls.append((query, arguments))
        assert "FOR UPDATE NOWAIT" in query
        fields = ("dataset_id", "endpoint_id", "import_run_id", "acquisition_root_run_id", "previous_dataset_id")
        return deepcopy(self.header) if tuple(self.header[field] for field in fields) == arguments else None

    async def execute(self, query, *arguments):
        self.calls.append((query, arguments))
        if query == "SET LOCAL TIME ZONE 'UTC'":
            return "SET"
        assert query.startswith('UPDATE "source_schema"."provider_directory_endpoint_dataset" SET status=')
        if self.write_error is not None:
            raise self.write_error
        if self.update_result == "UPDATE 1":
            self.header["status"] = "failed"
        return self.update_result


@pytest.fixture
def case(monkeypatch):
    monkeypatch.setenv(RETIREMENT_ENABLED_ENV, "true")
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "catalog_schema")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    stopped = AsyncMock()
    monkeypatch.setattr(failure, "require_stopped_lineage", stopped)
    connection = Connection()
    request = TerminalRootRetirementRequest(
        source_id="source-synthetic",
        endpoint_id="endpoint-synthetic",
        dataset_id="dataset-synthetic",
        acquisition_root_run_id="run-root",
        owner_run_id="run-root",
        expected_current_dataset_id=None,
        expect_no_current_dataset=True,
    )
    return SimpleNamespace(
        connection=connection,
        request=request,
        stopped=stopped,
        arguments=dict(
            connection=connection,
            policy=SimpleNamespace(schema="source_schema"),
            storage_binding={"catalog_schema": "catalog_schema", "storage_schema": "source_schema"},
        ),
    )


@pytest.mark.asyncio
async def test_preview_is_read_only_and_apply_changes_only_status(case):
    before = deepcopy(vars(case.connection))
    assert await failure.preview_stopped_source_failure(case.request, **case.arguments) == {
        "candidate_status": "acquiring",
        "would_change": True,
    }
    assert not any(query.startswith("UPDATE") for query, _ in case.connection.calls)
    assert await failure.apply_stopped_source_failure(case.request, **case.arguments) == {"changed": True}
    assert case.connection.header == {**before["header"], "status": "failed"}
    assert case.connection.payloads == before["payloads"]
    assert case.connection.checkpoints == before["checkpoints"]
    assert case.connection.source == before["source"]
    assert case.connection.lineage == before["lineage"]
    assert await failure.apply_stopped_source_failure(case.request, **case.arguments) == {"changed": False}
    assert await failure.preview_stopped_source_failure(case.request, **case.arguments) == {
        "candidate_status": "failed",
        "would_change": False,
    }
    writes = [query for query, _ in case.connection.calls if query.startswith("UPDATE")]
    assert len(writes) == 1
    assert writes[0].split(" WHERE ")[0].endswith(" SET status='failed'")
    assert case.stopped.await_count == 4


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field", ["dataset_id", "endpoint_id", "import_run_id", "acquisition_root_run_id", "previous_dataset_id"]
)
async def test_wrong_logical_header_selection_refuses(case, field):
    case.connection.header[field] = "different"
    with pytest.raises(TerminalRootRetirementError):
        await failure.apply_stopped_source_failure(case.request, **case.arguments)
    assert not any(query.startswith("UPDATE") for query, _ in case.connection.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("status", "published"),
        ("status", "incomplete"),
        ("is_current", True),
        ("dataset_hash", "validated-data"),
        ("validated_at", "2026-01-01"),
        ("published_at", "2026-01-01"),
        ("superseded_at", "2026-01-01"),
        ("completion_proof_json", {}),
        ("completion_proof_sha256", "completed-data"),
        ("resource_count", -1),
        ("publication_metadata_json", {"source_ids": ["another-source"]}),
    ],
)
async def test_serving_completed_or_unrelated_header_refuses(case, field, value):
    case.connection.header[field] = value
    with pytest.raises(TerminalRootRetirementError):
        await failure.apply_stopped_source_failure(case.request, **case.arguments)
    assert not any(query.startswith("UPDATE") for query, _ in case.connection.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["competing", "private_competing", "published"])
async def test_another_candidate_or_unexpected_serving_snapshot_refuses(case, field):
    setattr(case.connection, field, [{"dataset_id": "another-dataset"}])
    with pytest.raises(TerminalRootRetirementError):
        await failure.apply_stopped_source_failure(case.request, **case.arguments)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value", [("status", "running"), ("terminal_age_satisfied", False), ("importer", "other")]
)
async def test_live_recent_or_wrong_run_refuses(case, field, value):
    case.connection.lineage[0][field] = value
    with pytest.raises(TerminalRootRetirementError):
        await failure.apply_stopped_source_failure(case.request, **case.arguments)
    case.stopped.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "lock", [failure._ADMISSION_LOCK, "endpoint-synthetic", "control-run-worker-action:v1:run-root"]
)
async def test_existing_lock_contention_refuses(case, lock):
    case.connection.busy_lock = lock
    with pytest.raises(TerminalRootRetirementError, match="busy"):
        await failure.apply_stopped_source_failure(case.request, **case.arguments)
    assert not any(query.startswith("UPDATE") for query, _ in case.connection.calls)


@pytest.mark.asyncio
async def test_changed_lineage_and_unstopped_transports_refuse(case):
    case.connection.change_lineage = True
    with pytest.raises(TerminalRootRetirementError, match="evidence_changed"):
        await failure.apply_stopped_source_failure(case.request, **case.arguments)
    case.connection.change_lineage = False
    case.stopped.side_effect = RuntimeError("worker_not_stopped")
    with pytest.raises(RuntimeError, match="worker_not_stopped"):
        await failure.apply_stopped_source_failure(case.request, **case.arguments)
    assert not any(query.startswith("UPDATE") for query, _ in case.connection.calls)


@pytest.mark.asyncio
async def test_database_write_denial_is_not_bypassed(case):
    case.connection.write_error = PermissionError("read-only application role")
    with pytest.raises(PermissionError):
        await failure.apply_stopped_source_failure(case.request, **case.arguments)
    assert case.connection.header["status"] == "acquiring"


@pytest.mark.asyncio
async def test_compare_failure_does_not_report_success(case):
    case.connection.update_result = "UPDATE 0"
    with pytest.raises(TerminalRootRetirementError, match="state_invalid"):
        await failure.apply_stopped_source_failure(case.request, **case.arguments)


@pytest.mark.asyncio
async def test_disabled_gate_inactive_transaction_and_old_digest_refuse(case, monkeypatch):
    monkeypatch.delenv(RETIREMENT_ENABLED_ENV)
    with pytest.raises(TerminalRootRetirementError, match="disabled"):
        await failure.apply_stopped_source_failure(case.request, **case.arguments)
    monkeypatch.setenv(RETIREMENT_ENABLED_ENV, "true")
    case.connection.in_transaction = False
    with pytest.raises(TerminalRootRetirementError, match="state_invalid"):
        await failure.apply_stopped_source_failure(case.request, **case.arguments)
    case.connection.in_transaction = True
    with pytest.raises(TerminalRootRetirementError, match="request_invalid"):
        await failure.apply_stopped_source_failure(
            replace(case.request, expected_evidence_sha256="a" * 64), **case.arguments
        )
    assert not case.connection.calls


@pytest.mark.asyncio
@pytest.mark.parametrize("field,value", [("catalog_schema", "another_catalog"), ("storage_schema", "another_storage")])
async def test_wrong_schema_names_refuse_before_queries(case, field, value):
    case.arguments["storage_binding"][field] = value
    with pytest.raises(TerminalRootRetirementError):
        await failure.apply_stopped_source_failure(case.request, **case.arguments)
    assert not case.connection.calls


@pytest.mark.asyncio
@pytest.mark.parametrize("is_current", [False, True])
async def test_existing_serving_predecessor_must_remain_current(case, is_current):
    case.request = replace(case.request, expected_current_dataset_id="serving-dataset", expect_no_current_dataset=False)
    case.connection.header["previous_dataset_id"] = "serving-dataset"
    predecessor_by_field = dict(
        dataset_id="serving-dataset",
        endpoint_id="endpoint-synthetic",
        status="published",
        is_current=is_current,
        dataset_hash="a" * 64,
        validated_at="2026-01-01",
        published_at="2026-01-01",
        superseded_at=None,
    )
    case.connection.predecessor = deepcopy(predecessor_by_field)
    if is_current:
        assert await failure.apply_stopped_source_failure(case.request, **case.arguments) == {"changed": True}
    else:
        with pytest.raises(TerminalRootRetirementError):
            await failure.apply_stopped_source_failure(case.request, **case.arguments)
    assert case.connection.predecessor == predecessor_by_field


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy_executable", [False, True])
async def test_driver_json_strings_retain_canonical_and_legacy_queue_probes(monkeypatch, legacy_executable):
    from process.provider_directory_terminal_root_retirement_runtime import require_stopped_lineage
    from tests.test_provider_directory_terminal_root_retirement_edge_runtime import (
        _install_queue_presence,
        _terminal_run,
    )

    run = _terminal_run()
    metrics = deepcopy(run["metrics"])
    run["metrics"] = json.dumps(metrics)
    run["params"] = json.dumps({"source_ids": ["source-synthetic"]})
    connection = SimpleNamespace(fetch=AsyncMock(return_value=[run]))
    lineage = await failure._Executor(connection).all("SELECT * FROM synthetic_runs")
    assert lineage[0]["metrics"] == metrics
    assert lineage[0]["params"] == {"source_ids": ["source-synthetic"]}
    probes = _install_queue_presence(
        monkeypatch, present=[(metrics["job_id"], "queue_member")] if legacy_executable else []
    )
    if legacy_executable:
        with pytest.raises(TerminalRootRetirementError, match="busy"):
            await require_stopped_lineage(lineage)
    else:
        await require_stopped_lineage(lineage)
    assert probes == [
        ("arq:ProviderDirectoryFHIR", "provider_directory_start_" + run["run_id"]),
        ("arq:ProviderDirectoryFHIR", metrics["job_id"]),
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("canonical_executable", [False, True])
async def test_json_null_metadata_retains_canonical_queue_probe(monkeypatch, canonical_executable):
    from process.provider_directory_terminal_root_retirement_runtime import require_stopped_lineage
    from tests.test_provider_directory_terminal_root_retirement_edge_runtime import (
        _install_queue_presence,
        _terminal_run,
    )

    run_by_field = {**_terminal_run(), "metrics": "null", "params": "null"}
    connection = SimpleNamespace(fetch=AsyncMock(return_value=[run_by_field]))
    lineage = await failure._Executor(connection).all("SELECT * FROM synthetic_runs")
    assert lineage[0]["metrics"] is None and lineage[0]["params"] is None
    job_id = "provider_directory_start_" + run_by_field["run_id"]
    probes = _install_queue_presence(monkeypatch, present=[(job_id, "queue_member")] if canonical_executable else [])
    if canonical_executable:
        with pytest.raises(TerminalRootRetirementError, match="busy"):
            await require_stopped_lineage(lineage)
    else:
        await require_stopped_lineage(lineage)
    assert probes == [("arq:ProviderDirectoryFHIR", job_id)]


@pytest.mark.asyncio
@pytest.mark.parametrize("encoded", ["[]", "false", "1", '"text"'])
async def test_non_object_run_metadata_refuses(encoded):
    connection = SimpleNamespace(fetch=AsyncMock(return_value=[{"metrics": encoded}]))
    with pytest.raises(TerminalRootRetirementError):
        await failure._Executor(connection).all("SELECT * FROM synthetic_runs")
