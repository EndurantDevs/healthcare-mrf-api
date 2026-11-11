# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Stopped native failure reconciliation preserves retained inputs and exact custody."""

import asyncio
import re
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import asdict, replace
from hashlib import sha256
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_generic_source_storage as storage
from process import provider_directory_stopped_source_failure as failure
from process.provider_directory_generic_candidate import GenericCandidateError
from process.provider_directory_generic_source_policy import GenericSourcePolicy
from process.provider_directory_source_storage_binding import FHIRSourceStorageBinding
from process.provider_directory_terminal_root_retirement_contract import (
    RETIREMENT_ENABLED_ENV,
    TerminalRootRetirementError,
    TerminalRootRetirementRequest,
)


class Connection:
    def __init__(self, header_by_field, binding):
        self.header_by_field = header_by_field
        self.coordinates = {key: field_value for key, field_value in asdict(binding).items() if key.endswith("_oid")}
        self.actor = dict(
            session="maintenance",
            current="maintenance",
            administrator=True,
            role="none",
            isolation="serializable",
            readonly="off",
            replication="origin",
            backend=10,
            transaction_id="20",
            admission_fenced=True,
        )
        self.source = dict(
            source_id="source-synthetic",
            endpoint_id="endpoint-synthetic",
            canonical_api_base="https://example.test/fhir",
        )
        self.endpoint = {key: self.source[key] for key in ("endpoint_id", "canonical_api_base")}
        self.lineage = [
            dict(
                run_id="run-root",
                retry_of_run_id=None,
                importer="provider-directory-fhir",
                status="failed",
                finished_at="2026-01-01T00:00:00Z",
                depth=0,
                terminal_age_satisfied=True,
            )
        ]
        self.rows = {
            proof["name"]: [
                dict(dataset_id="dataset-synthetic", resource_type="Organization", resource_id=str(index))
                for index in range(3)
            ]
            if root == "resource_input"
            else []
            for root, proof in header_by_field["candidate_storage_json"]["tables"].items()
        }
        self.rows["provider_directory_pagination_checkpoint"] = [
            dict(dataset_id="dataset-synthetic", next_url="opaque-cursor")
        ]
        self.calls = []
        self.busy_lock = None
        self.competing = []
        self.private_competing = []
        self.published = []
        self.predecessor = None
        self.update_result = "UPDATE 1"
        self.in_transaction = True
        self.corrupt_readback = False

    def is_in_transaction(self):
        return self.in_transaction

    async def fetch(self, query, *arguments):
        self.calls.append((query, arguments))
        if "WITH RECURSIVE lineage" in query:
            return deepcopy(self.lineage)
        if "provider_directory_api_endpoint" in query:
            return [deepcopy(self.endpoint)]
        if 'FROM "catalog_schema"."provider_directory_source"' in query:
            return [{key: self.source[key] for key in ("source_id", "endpoint_id", "canonical_api_base")}]
        if "dataset.is_current OR dataset.published_at" in query:
            return self.published
        if "dataset.dataset_id =" in query:
            return [deepcopy(self.predecessor)] if self.predecessor is not None else []
        if "dataset.dataset_id <>" in query:
            return self.competing
        if "AND (status IN" in query:
            return [
                row
                for row in self.private_competing
                if row.get("status", "acquiring") in {"acquiring", "incomplete"}
                or row.get("is_current", False)
                and row["dataset_id"] != arguments[2]
                or arguments[3]
                and row.get("published_at") is not None
            ]
        if "FOR UPDATE OF row NOWAIT" in query or "WHERE run_id=ANY" in query:
            return []
        raise AssertionError(query)

    async def fetchrow(self, query, *arguments):
        self.calls.append((query, arguments))
        if "AS administrator" in query:
            return deepcopy(self.actor)
        if "AS database_oid" in query:
            return deepcopy(self.coordinates)
        if "AS header" in query:
            selectors = tuple(
                self.header_by_field[key]
                for key in (
                    "dataset_id",
                    "endpoint_id",
                    "import_run_id",
                    "acquisition_root_run_id",
                    "previous_dataset_id",
                )
            )
            return {"header": deepcopy(self.header_by_field)} if arguments == selectors else None
        raise AssertionError(query)

    def cursor(self, query, *arguments, prefetch):
        self.calls.append((query, arguments))
        assert prefetch == failure._CHUNK_PREFETCH
        root = re.findall(r'FROM "[^"]+"\."([^"]+)"', query)[-1]
        rows = self.rows.get(root, [])
        if root == "import_run":
            rows = self.lineage
        if root == "provider_directory_source":
            rows = [self.source]
        if root == "provider_directory_api_endpoint":
            rows = [self.endpoint]

        async def chunks():
            row_hashes = sorted(sha256(storage._encoded(row).encode()).hexdigest() for row in rows)
            for ordinal, start in enumerate(range(0, len(row_hashes), failure._ROW_CHUNK_ROWS)):
                selected_hashes = row_hashes[start : start + failure._ROW_CHUNK_ROWS]
                yield dict(
                    chunk_ordinal=ordinal,
                    row_count=len(selected_hashes),
                    row_sha256=sha256("".join(selected_hashes).encode()).hexdigest(),
                )

        return chunks()

    async def fetchval(self, query, *arguments):
        self.calls.append((query, arguments))
        if "pg_try_advisory_xact_lock" in query:
            return arguments[0] != self.busy_lock
        if "SELECT to_char" in query:
            return "2026-01-02T00:00:00.000000Z"
        if "SELECT EXISTS(SELECT FROM" in query:
            root = re.findall(r'FROM "[^"]+"\."([^"]+)"', query)[-1]
            return any(row["dataset_id"] != arguments[0] for row in self.rows.get(root, []))
        raise AssertionError(query)

    async def execute(self, query, *arguments):
        self.calls.append((query, arguments))
        if query.startswith("UPDATE"):
            assert storage._json(arguments[3]) == self.header_by_field
            if self.update_result == "UPDATE 1":
                self.header_by_field["status"] = "failed"
                self.header_by_field["publication_metadata_json"][arguments[1]] = storage._json(arguments[2])
                if self.corrupt_readback:
                    self.header_by_field["resource_count"] = 1
            return self.update_result
        assert query.startswith(("SET LOCAL", "LOCK TABLE"))
        return "OK"

    @asynccontextmanager
    async def transaction(self):
        before = deepcopy(self.header_by_field)
        try:
            yield
        except BaseException:
            self.header_by_field.clear()
            self.header_by_field.update(before)
            raise


def _native_header(storage_by_field):
    metadata = dict(
        source_ids=["source-synthetic"],
        selected_resources=["Organization"],
        expected_resources=["Organization"],
        resource_hash_contract="semantic_content_v4",
    )
    candidate_by_field = dict(
        contract=storage.ISOLATED_CONTRACT,
        identity=dict(
            dataset_id="dataset-synthetic",
            endpoint_id="endpoint-synthetic",
            acquisition_root_run_id="run-root",
            metadata=metadata,
        ),
        phase="loading",
        ordinal=3,
        resets={},
        auxiliary=None,
        relationship_sha256=None,
        admission=None,
        storage=deepcopy(storage_by_field),
        tables={
            root: dict(name="pdgs_" + "d" * 32 + suffix, oid=index + 40, owner=100, layout_sha256="e" * 64)
            for index, (root, suffix) in enumerate((("resource_input", "_ri"), ("witness_input", "_wi")))
        },
    )
    return dict(
        dataset_id="dataset-synthetic",
        endpoint_id="endpoint-synthetic",
        import_run_id="run-root",
        acquisition_root_run_id="run-root",
        previous_dataset_id=None,
        status="acquiring",
        is_current=False,
        resource_count=0,
        dataset_hash=None,
        validated_at=None,
        published_at=None,
        superseded_at=None,
        completion_proof_required_version=None,
        completion_proof_json=None,
        completion_proof_sha256=None,
        publication_metadata_json=deepcopy(metadata),
        candidate_storage_json=candidate_by_field,
    )


@pytest.fixture
def case(monkeypatch):
    monkeypatch.setenv(RETIREMENT_ENABLED_ENV, "true")
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "catalog_schema")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    policy = GenericSourcePolicy("source_schema", "source_owner", "source_publisher", ("source_reader",))
    binding = FHIRSourceStorageBinding("catalog_schema", "source_schema", 10, 20, 30, 100, 101, "a" * 64)
    storage_by_field = {**asdict(binding), "producer": ["b" * 40, "sha256:" + "c" * 64]}
    header_by_field = _native_header(storage_by_field)
    connection = Connection(header_by_field, binding)
    monkeypatch.setattr(storage, "_verify_tables", AsyncMock())
    monkeypatch.setattr(failure, "require_stopped_lineage", AsyncMock())
    monkeypatch.setattr(
        failure, "capture_checkpoint_controls", AsyncMock(return_value={"checkpoint": {"oid": 50, "layout": "f" * 64}})
    )
    monkeypatch.setattr(
        failure,
        "verify_proof_control",
        AsyncMock(return_value={"oid": 51, "columns": [], "constraints": [], "indexes": []}),
    )
    request = TerminalRootRetirementRequest(
        "source-synthetic",
        "endpoint-synthetic",
        "dataset-synthetic",
        "run-root",
        "run-root",
        None,
        expect_no_current_dataset=True,
    )
    return SimpleNamespace(
        connection=connection,
        policy=policy,
        storage_by_field=storage_by_field,
        header_by_field=header_by_field,
        request=request,
        arguments=dict(connection=connection, policy=policy, storage_binding=storage_by_field),
    )


def writes(case):
    return [
        query
        for query, _ in case.connection.calls
        if query.startswith(("UPDATE", "INSERT", "DELETE", "CREATE", "DROP"))
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("row_count", [0, 1, 4095, 4096, 4097, 8193])
async def test_relation_hash_chunk_boundaries_and_scoped_row_locks(case, row_count):
    rows = [dict(dataset_id="dataset-synthetic", ordinal=index) for index in range(row_count)]
    case.connection.rows["synthetic_rows"] = rows
    evidence = await failure._relation_evidence(
        case.connection, '"source_schema"."synthetic_rows"', "row.dataset_id=$1", ("dataset-synthetic",), lock_rows=True
    )
    hashes = sorted(sha256(storage._encoded(row).encode()).hexdigest() for row in rows)
    expected = sha256(failure._ROW_HASH_DOMAIN)
    for start in range(0, row_count, failure._ROW_CHUNK_ROWS):
        selected = hashes[start : start + failure._ROW_CHUNK_ROWS]
        expected.update(len(selected).to_bytes(4, "big"))
        expected.update(sha256("".join(selected).encode()).digest())
    assert evidence == dict(row_count=row_count, row_sha256=expected.hexdigest())
    query, arguments = case.connection.calls[-1]
    assert "FOR UPDATE OF row NOWAIT" in query and "row.dataset_id=$1" in query
    assert "GROUP BY ordinal/4096" in query and "ORDER BY chunk_ordinal" in query
    assert arguments == ("dataset-synthetic",)


@pytest.mark.asyncio
async def test_relation_hash_streams_beyond_the_former_single_value_limit(monkeypatch):
    chunk_count = 4097
    block_sizes = []
    expected = sha256(failure._ROW_HASH_DOMAIN)
    actual = sha256(failure._ROW_HASH_DOMAIN)
    chunk_digest = sha256(b"synthetic bounded chunk").digest()

    def update(block):
        block_sizes.append(len(block))
        actual.update(block)

    monkeypatch.setattr(failure, "sha256", lambda _seed: SimpleNamespace(update=update, hexdigest=actual.hexdigest))

    async def chunks():
        for ordinal in range(chunk_count):
            assert len(block_sizes) == ordinal * 2
            expected.update(failure._ROW_CHUNK_ROWS.to_bytes(4, "big"))
            expected.update(chunk_digest)
            yield dict(chunk_ordinal=ordinal, row_count=failure._ROW_CHUNK_ROWS, row_sha256=chunk_digest.hex())

    def cursor(_query, *, prefetch):
        assert prefetch == 64
        return chunks()

    connection = SimpleNamespace(cursor=cursor)
    evidence = await failure._relation_evidence(connection, '"source_schema"."synthetic_rows"', "TRUE", ())
    assert evidence == dict(row_count=chunk_count * failure._ROW_CHUNK_ROWS, row_sha256=expected.hexdigest())
    assert evidence["row_count"] * 64 > 2**30
    assert set(block_sizes) == {4, 32}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "receipts",
    [
        [dict(chunk_ordinal=1, row_count=1, row_sha256="a" * 64)],
        [dict(chunk_ordinal=0, row_count=0, row_sha256="a" * 64)],
        [dict(chunk_ordinal=0, row_count=4097, row_sha256="a" * 64)],
        [dict(chunk_ordinal=0, row_count=True, row_sha256="a" * 64)],
        [dict(chunk_ordinal=0, row_count=1, row_sha256="invalid")],
        [
            dict(chunk_ordinal=0, row_count=1, row_sha256="a" * 64),
            dict(chunk_ordinal=1, row_count=1, row_sha256="b" * 64),
        ],
    ],
)
async def test_relation_hash_refuses_malformed_chunk_accounting(receipts):
    async def chunks():
        for receipt in receipts:
            yield receipt

    connection = SimpleNamespace(cursor=lambda *_args, **_kwargs: chunks())
    with pytest.raises(TerminalRootRetirementError, match="evidence_invalid"):
        await failure._relation_evidence(connection, '"source_schema"."synthetic_rows"', "TRUE", ())


@pytest.mark.asyncio
async def test_named_query_bridge_normalizes_driver_records():
    class Record:
        def keys(self):
            return ["endpoint_id"]

        def __getitem__(self, name):
            assert name == "endpoint_id"
            return "endpoint-synthetic"

    connection = SimpleNamespace(fetch=AsyncMock(return_value=[Record()]))
    rows = await failure._Executor(connection).all(
        "SELECT :endpoint_id AS endpoint_id", endpoint_id="endpoint-synthetic"
    )
    assert rows == [{"endpoint_id": "endpoint-synthetic"}]
    connection.fetch.assert_awaited_once_with("SELECT $1 AS endpoint_id", "endpoint-synthetic")


@pytest.mark.asyncio
async def test_driver_json_strings_preserve_bound_legacy_job_presence_checks(monkeypatch):
    from process.provider_directory_terminal_root_retirement_runtime import require_stopped_lineage
    from tests.test_provider_directory_terminal_root_retirement_edge_runtime import (
        _install_queue_presence,
        _terminal_run,
    )

    run_by_field = _terminal_run()
    expected_metrics = deepcopy(run_by_field["metrics"])
    run_by_field["metrics"] = storage._encoded(expected_metrics)
    run_by_field["params"] = storage._encoded({"source_ids": ["source-synthetic"]})
    connection = SimpleNamespace(fetch=AsyncMock(return_value=[run_by_field]))
    lineage = await failure._Executor(connection).all("SELECT * FROM synthetic_runs")
    assert lineage[0]["metrics"] == expected_metrics
    assert lineage[0]["params"] == {"source_ids": ["source-synthetic"]}
    probes = _install_queue_presence(monkeypatch)
    await require_stopped_lineage(lineage)
    assert probes == [
        ("arq:ProviderDirectoryFHIR", "provider_directory_start_" + run_by_field["run_id"]),
        ("arq:ProviderDirectoryFHIR", expected_metrics["job_id"]),
    ]


@pytest.mark.asyncio
async def test_payload_and_registry_relations_lock_before_native_verification(case):
    async def verify(connection, *_args):
        queries = [query for query, _ in connection.calls]
        assert sum(query.startswith("LOCK TABLE") and "IN SHARE MODE" in query for query in queries) == 2
        assert any("IN ACCESS SHARE MODE" in query and failure.PROOF_ROOT in query for query in queries)

    storage._verify_tables.side_effect = verify
    await failure.preview_stopped_source_failure(case.request, **case.arguments)


@pytest.mark.asyncio
async def test_expected_published_predecessor_and_prior_private_history_remain_eligible(case):
    case.connection.predecessor = dict(
        dataset_id="dataset-current",
        endpoint_id="endpoint-synthetic",
        status="published",
        is_current=True,
        dataset_hash="a" * 64,
        validated_at="2025-01-01T00:00:00Z",
        published_at="2025-01-01T00:00:00Z",
        superseded_at=None,
    )
    case.connection.private_competing = [
        deepcopy(case.connection.predecessor),
        dict(dataset_id="dataset-previous", status="superseded", is_current=False, published_at="2024-01-01T00:00:00Z"),
    ]
    case.header_by_field["previous_dataset_id"] = "dataset-current"
    request = replace(case.request, expected_current_dataset_id="dataset-current", expect_no_current_dataset=False)
    token = await failure.preview_stopped_source_failure(request, **case.arguments)
    assert len(token) == 64 and writes(case) == []


@pytest.mark.asyncio
async def test_preview_apply_and_replay_preserve_zero_header_count_and_retained_inputs(case):
    before, rows = deepcopy(case.header_by_field), deepcopy(case.connection.rows)
    token = await failure.preview_stopped_source_failure(case.request, **case.arguments)
    assert len(token) == 64 and case.header_by_field == before and writes(case) == []
    request = replace(case.request, expected_evidence_sha256=token)
    assert await failure.apply_stopped_source_failure(request, **case.arguments) == dict(
        changed=True, evidence_sha256=token
    )
    marker = case.header_by_field["publication_metadata_json"][failure.STOPPED_SOURCE_FAILURE_METADATA_KEY]
    expected = deepcopy(before)
    expected["status"] = "failed"
    expected["publication_metadata_json"][failure.STOPPED_SOURCE_FAILURE_METADATA_KEY] = marker
    assert case.header_by_field == expected and case.connection.rows == rows
    assert marker["evidence"]["relations"]["resource_input"]["row_count"] == 3
    assert case.header_by_field["resource_count"] == 0
    assert await failure.preview_stopped_source_failure(case.request, **case.arguments) == token
    assert await failure.apply_stopped_source_failure(request, **case.arguments) == dict(
        changed=False, evidence_sha256=token
    )
    assert len(writes(case)) == 1
    failure.require_stopped_lineage.assert_awaited()
    storage._verify_tables.assert_awaited()


@pytest.mark.asyncio
async def test_unsealed_completion_requirement_is_preserved_through_apply_and_replay(case):
    case.header_by_field["completion_proof_required_version"] = 3
    case.header_by_field["publication_metadata_json"]["completion_proof_required_version"] = 3
    case.header_by_field["candidate_storage_json"]["identity"]["metadata"]["completion_proof_required_version"] = 3
    before = deepcopy(case.header_by_field)
    token = await failure.preview_stopped_source_failure(case.request, **case.arguments)
    request = replace(case.request, expected_evidence_sha256=token)
    assert await failure.apply_stopped_source_failure(request, **case.arguments) == dict(
        changed=True, evidence_sha256=token
    )
    marker = case.header_by_field["publication_metadata_json"][failure.STOPPED_SOURCE_FAILURE_METADATA_KEY]
    expected = deepcopy(before)
    expected["status"] = "failed"
    expected["publication_metadata_json"][failure.STOPPED_SOURCE_FAILURE_METADATA_KEY] = marker
    assert case.header_by_field == expected
    assert await failure.apply_stopped_source_failure(request, **case.arguments) == dict(
        changed=False, evidence_sha256=token
    )


@pytest.mark.asyncio
async def test_grouped_source_candidate_refuses_before_source_locks_or_native_evidence(case):
    case.header_by_field["publication_metadata_json"]["source_ids"] = ["source-synthetic", "source-other"]
    case.header_by_field["candidate_storage_json"]["identity"]["metadata"]["source_ids"] = [
        "source-synthetic",
        "source-other",
    ]
    with pytest.raises(TerminalRootRetirementError, match="evidence_invalid"):
        await failure.preview_stopped_source_failure(case.request, **case.arguments)
    assert not any('FROM "catalog_schema"."provider_directory_source"' in query for query, _ in case.connection.calls)
    assert writes(case) == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changed", ["resource_input", "cursor", "count", "source", "endpoint", "checkpoint_oid", "proof_index"]
)
async def test_apply_rejects_preservation_drift_since_preview(case, changed):
    token = await failure.preview_stopped_source_failure(case.request, **case.arguments)
    match changed:
        case "resource_input":
            name = case.header_by_field["candidate_storage_json"]["tables"]["resource_input"]["name"]
            case.connection.rows[name][0]["resource_id"] = "changed"
        case "cursor":
            case.connection.rows["provider_directory_pagination_checkpoint"][0]["next_url"] = "changed"
        case "count":
            case.header_by_field["resource_count"] = 2
        case "checkpoint_oid":
            failure.capture_checkpoint_controls.return_value["checkpoint"]["oid"] += 1
        case "proof_index":
            failure.verify_proof_control.return_value["indexes"] = ["changed"]
        case _:
            getattr(case.connection, changed)["additional_metadata"] = "changed"
    with pytest.raises(TerminalRootRetirementError, match="evidence_changed"):
        await failure.apply_stopped_source_failure(
            replace(case.request, expected_evidence_sha256=token), **case.arguments
        )
    assert writes(case) == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,field_value",
    [
        ("administrator", False),
        ("current", "other"),
        ("role", "maintenance"),
        ("isolation", "read committed"),
        ("readonly", "on"),
        ("replication", "replica"),
        ("admission_fenced", False),
    ],
)
async def test_admin_and_original_transaction_are_required_before_target_lookup(case, field, field_value):
    case.connection.actor[field] = field_value
    with pytest.raises(TerminalRootRetirementError):
        await failure.preview_stopped_source_failure(case.request, **case.arguments)
    assert len(case.connection.calls) == 1 and writes(case) == []


@pytest.mark.asyncio
async def test_disabled_gate_missing_transaction_and_missing_apply_token_refuse(case, monkeypatch):
    monkeypatch.delenv(RETIREMENT_ENABLED_ENV)
    with pytest.raises(TerminalRootRetirementError, match="disabled"):
        await failure.preview_stopped_source_failure(case.request, **case.arguments)
    assert case.connection.calls == []
    monkeypatch.setenv(RETIREMENT_ENABLED_ENV, "true")
    case.connection.in_transaction = False
    with pytest.raises(TerminalRootRetirementError, match="administrator_required"):
        await failure.preview_stopped_source_failure(case.request, **case.arguments)
    with pytest.raises(TerminalRootRetirementError, match="request_invalid"):
        await failure.apply_stopped_source_failure(case.request, **case.arguments)
    assert case.connection.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,field_value",
    [
        ("is_current", True),
        ("status", "validated"),
        ("dataset_hash", "f" * 64),
        ("completion_proof_json", {}),
        ("completion_proof_sha256", "f" * 64),
        ("content_proof_admission_version", 1),
        ("artifact_selection_receipt_json", {}),
        ("import_run_id", "other"),
        ("previous_dataset_id", "other"),
    ],
)
async def test_sealed_current_or_mismatched_candidate_refuses(case, field, field_value):
    case.header_by_field[field] = field_value
    with pytest.raises(TerminalRootRetirementError):
        await failure.preview_stopped_source_failure(case.request, **case.arguments)
    assert writes(case) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", ["producer", "oid", "phase", "source", "table", "foreign_row"])
async def test_exact_native_document_and_row_scope_are_required(case, changed):
    candidate_by_field = case.header_by_field["candidate_storage_json"]
    match changed:
        case "producer":
            candidate_by_field["storage"]["producer"][0] = "f" * 40
        case "oid":
            case.connection.coordinates["storage_schema_oid"] += 1
        case "phase":
            candidate_by_field["phase"] = "ready_for_freeze"
        case "source":
            case.header_by_field["publication_metadata_json"]["source_ids"] = ["other"]
        case "table":
            storage._verify_tables.side_effect = GenericCandidateError("storage_binding_changed")
        case _:
            table = candidate_by_field["tables"]["resource_input"]["name"]
            case.connection.rows[table][0]["dataset_id"] = "other"
    with pytest.raises((TerminalRootRetirementError, GenericCandidateError)):
        await failure.preview_stopped_source_failure(case.request, **case.arguments)
    assert writes(case) == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,field_value", [("status", "running"), ("terminal_age_satisfied", False), ("retry_of_run_id", "other")]
)
async def test_nonterminal_young_or_unbound_lineage_refuses(case, field, field_value):
    case.connection.lineage[0][field] = field_value
    with pytest.raises(TerminalRootRetirementError):
        await failure.preview_stopped_source_failure(case.request, **case.arguments)
    assert writes(case) == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "lock",
    ["import-run-admission:provider-directory-fhir", "endpoint-synthetic", "control-run-worker-action:v1:run-root"],
)
async def test_admission_and_worker_locks_refuse_busy_state(case, lock):
    case.connection.busy_lock = lock
    with pytest.raises(TerminalRootRetirementError, match="busy"):
        await failure.preview_stopped_source_failure(case.request, **case.arguments)
    assert writes(case) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("conflict", ["competing", "private_competing", "published", "worker"])
async def test_competing_candidate_publication_or_live_worker_refuses(case, conflict):
    if conflict == "worker":
        failure.require_stopped_lineage.side_effect = TerminalRootRetirementError("busy")
    else:
        setattr(case.connection, conflict, [dict(dataset_id="another")])
    with pytest.raises(TerminalRootRetirementError):
        await failure.preview_stopped_source_failure(case.request, **case.arguments)
    assert writes(case) == []


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_mode", ["cas", "readback", "cancel"])
async def test_cas_readback_or_cancellation_failure_rolls_back_caller_transaction(case, failure_mode):
    token = await failure.preview_stopped_source_failure(case.request, **case.arguments)
    before = deepcopy(case.header_by_field)
    if failure_mode == "cas":
        case.connection.update_result = "UPDATE 0"
    elif failure_mode == "readback":
        case.connection.corrupt_readback = True
    else:
        failure.require_stopped_lineage.side_effect = asyncio.CancelledError
    expected = asyncio.CancelledError if failure_mode == "cancel" else TerminalRootRetirementError
    with pytest.raises(expected):
        async with case.connection.transaction():
            await failure.apply_stopped_source_failure(
                replace(case.request, expected_evidence_sha256=token), **case.arguments
            )
    assert case.header_by_field == before


@pytest.mark.asyncio
async def test_replay_rejects_modified_marker_or_retained_row(case):
    token = await failure.preview_stopped_source_failure(case.request, **case.arguments)
    request = replace(case.request, expected_evidence_sha256=token)
    await failure.apply_stopped_source_failure(request, **case.arguments)
    case.header_by_field["publication_metadata_json"][failure.STOPPED_SOURCE_FAILURE_METADATA_KEY]["extra"] = True
    with pytest.raises(TerminalRootRetirementError, match="evidence_changed"):
        await failure.apply_stopped_source_failure(request, **case.arguments)
    assert len(writes(case)) == 1


@pytest.mark.asyncio
async def test_prepared_unsealed_candidate_retains_all_existing_table_proofs(case):
    candidate_by_field = case.header_by_field["candidate_storage_json"]
    candidate_by_field["phase"] = "prepared"
    for index, root in enumerate(storage.RELATIONS):
        candidate_by_field["tables"][root] = dict(
            name="pdgs_" + "d" * 32 + "_" + storage._SUFFIXES[root],
            oid=60 + index,
            owner=100,
            layout_sha256="e" * 64,
        )
    token = await failure.preview_stopped_source_failure(case.request, **case.arguments)
    await failure.apply_stopped_source_failure(replace(case.request, expected_evidence_sha256=token), **case.arguments)
    assert set(case.header_by_field["candidate_storage_json"]["tables"]) == {
        "resource_input",
        "witness_input",
        *storage.RELATIONS,
    }


@pytest.mark.asyncio
async def test_reused_failed_candidate_still_checks_other_active_endpoint_candidate(monkeypatch):
    from importlib import import_module

    from process import provider_directory_generic_source_adapters as adapters
    from process import provider_directory_generic_source_policy as policy

    directory = import_module("process.provider_directory_fhir")
    candidate_by_field = directory.EndpointDatasetCandidate(
        endpoint_id="endpoint-synthetic",
        dataset_id="dataset-failed",
        acquisition_root_run_id="run-root",
        import_run_id="run-retry",
        source_ids=("source-synthetic",),
        selected_resources=("Organization",),
        previous_dataset_id=None,
        reused_from_checkpoint=True,
    )

    @asynccontextmanager
    async def transaction(*_args):
        yield

    monkeypatch.setattr(policy, "source_transaction", transaction)
    monkeypatch.setattr(adapters, "require_source_transaction", lambda: None)
    monkeypatch.setattr(adapters, "require_generic_source_layout", AsyncMock())
    monkeypatch.setattr(directory.db, "acquire", transaction)
    admission = AsyncMock()
    monkeypatch.setattr(directory, "_lock_endpoint_dataset_candidate_admission", admission)
    monkeypatch.setattr(directory, "_has_empty_locked_candidate", AsyncMock(return_value=False))
    monkeypatch.setattr(directory, "_assert_empty_endpoint_dataset_orphan", AsyncMock())
    contender = AsyncMock(return_value={"dataset_id": "dataset-active", "status": "acquiring"})
    monkeypatch.setattr(directory, "_locked_endpoint_verification_state", contender)
    upsert = AsyncMock()
    monkeypatch.setattr(directory, "_upsert_endpoint_dataset_candidate", upsert)
    prepare = AsyncMock()
    monkeypatch.setattr(storage, "prepare_generic_source_candidate", prepare)
    with pytest.raises(RuntimeError, match="provider_directory_endpoint_dataset_active_conflict"):
        await directory._ensure_endpoint_dataset_candidate(candidate_by_field)
    admission.assert_awaited_once()
    contender.assert_awaited_once_with(None, candidate_by_field)
    upsert.assert_not_awaited()
    prepare.assert_not_awaited()
