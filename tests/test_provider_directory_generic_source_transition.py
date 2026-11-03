# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""A successor producer may adopt only one verified, detached stopped intake."""

import hashlib
import re
from copy import deepcopy
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_generic_source_storage as storage
from process import provider_directory_source_storage_binding as binding
from process.provider_directory_generic_candidate import GenericCandidateError
from process.provider_directory_generic_source_policy import GenericSourcePolicy
from tests.test_provider_directory_generic_source_storage import _case

ROOT = "run_" + "a" * 32
FAILED = "run_" + "b" * 32
DATASET = "synthetic-dataset"
ENDPOINT = "synthetic-endpoint"


def _candidate_storage_evidence():
    """Build a private, unpublished candidate and two issued storage seals."""
    _, _, header = _case()
    old_storage_by_field = {
        **asdict(
            binding.FHIRSourceStorageBinding(
                "catalog_schema",
                "source_schema",
                10,
                20,
                30,
                100,
                101,
                "d" * 64,
            )
        ),
        "producer": ["a" * 40, "sha256:" + "b" * 64],
    }
    new_storage_by_field = {
        **old_storage_by_field,
        "enrollment_sha256": "e" * 64,
        "producer": ["c" * 40, "sha256:" + "f" * 64],
    }
    document = header["candidate_storage_json"]
    document.update(contract=storage.ISOLATED_CONTRACT, storage=old_storage_by_field, admission=None)
    document["identity"].update(
        dataset_id=DATASET,
        endpoint_id=ENDPOINT,
        acquisition_root_run_id=ROOT,
    )
    document["identity"]["metadata"].update(expected_resources=["Organization"])
    header.update(
        dataset_id=DATASET,
        endpoint_id=ENDPOINT,
        acquisition_root_run_id=ROOT,
        import_run_id=FAILED,
        published_at=None,
    )
    return header, old_storage_by_field, new_storage_by_field


def _completed_input_evidence():
    """Keep the failed child, checkpoint and raw-row census aligned."""
    run_rows = [
        dict(
            run_id=FAILED,
            status="failed",
            finished_at="now",
            retry_of_run_id=ROOT,
            root_run_id=ROOT,
            source_ids=["cms-npd"],
        ),
        dict(
            run_id=ROOT,
            status="failed",
            finished_at="before",
            retry_of_run_id=None,
            root_run_id=ROOT,
            source_ids=["cms-npd"],
        ),
    ]
    checkpoint_rows = [
        dict(
            resource_type="Organization",
            state="complete",
            owner_run_id=FAILED,
            dataset_id=DATASET,
            acquisition_root_run_id=ROOT,
            rows_processed=3,
            pages_processed=1,
            completed_at="now",
            canonical_api_base="https://example.test/fhir",
            start_url_hash=hashlib.sha256(b"https://example.test/fhir/Organization?_count=100").hexdigest(),
            next_url=None,
        )
    ]
    input_rows = [dict(dataset_id=DATASET, resource_type="Organization", total=3, active=3)]
    return run_rows, checkpoint_rows, input_rows


@pytest.fixture
def case(monkeypatch):
    """Expose a transaction-shaped fake and native proof seams."""
    policy = GenericSourcePolicy("source_schema", "source_owner", "source_publisher", ("source_reader",))
    header, old_storage_by_field, new_storage_by_field = _candidate_storage_evidence()
    run_rows, checkpoint_rows, input_rows = _completed_input_evidence()
    bulk_checkpoints, bulk_outputs = [], []
    writes = []

    async def fetchrow(sql, *args):
        if sql.startswith("SELECT session_user"):
            return dict(session="administrator", current="administrator", administrator=True)
        return deepcopy(header)

    async def fetch(sql, *args):
        placeholders = {int(value) for value in re.findall(r"\$(\d+)", sql)}
        assert placeholders == set(range(1, len(args) + 1))
        if 'FROM "catalog_schema"."import_run"' in sql:
            return deepcopy(run_rows)
        if "provider_directory_pagination_checkpoint" in sql:
            return deepcopy(checkpoint_rows)
        if "provider_directory_bulk_acquisition_checkpoint" in sql:
            return deepcopy(bulk_checkpoints)
        if "provider_directory_bulk_output_checkpoint" in sql:
            return deepcopy(bulk_outputs)
        return deepcopy(input_rows)

    async def execute(sql, *args):
        writes.append((sql, args))
        if storage._json(args[2]) != header["candidate_storage_json"]:
            return "UPDATE 0"
        header["candidate_storage_json"] = storage._json(args[1])
        return "UPDATE 1"

    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetchrow=AsyncMock(side_effect=fetchrow),
        fetchval=AsyncMock(
            side_effect=lambda sql, *args: "serializable" if sql == "SHOW transaction_isolation" else True
        ),
        fetch=AsyncMock(side_effect=fetch),
        execute=AsyncMock(side_effect=execute),
    )
    native, tables = AsyncMock(), AsyncMock()
    monkeypatch.setattr(binding, "verify_isolated_source_storage_coordinates", native)
    monkeypatch.setattr(storage, "_verify_tables", tables)
    return SimpleNamespace(
        connection=connection,
        policy=policy,
        header=header,
        old=old_storage_by_field,
        new=new_storage_by_field,
        runs=run_rows,
        checkpoints=checkpoint_rows,
        bulk_checkpoints=bulk_checkpoints,
        bulk_outputs=bulk_outputs,
        input_rows=input_rows,
        writes=writes,
        native=native,
        tables=tables,
    )


async def _transition(case, expected_counts=None, *, stopped_run_id=FAILED):
    return await storage.transition_isolated_loading_candidate_storage(
        case.connection,
        policy=case.policy,
        identity_by_field={
            "dataset_id": DATASET,
            "endpoint_id": ENDPOINT,
            "acquisition_root_run_id": ROOT,
            "failed_run_id": stopped_run_id,
        },
        old_storage=case.old,
        new_storage=case.new,
        expected_resource_counts={"Organization": 3} if expected_counts is None else expected_counts,
    )


def _include_completed_empty_collection(case):
    """Model an empty selected resource with its own completed checkpoint."""
    metadata = case.header["candidate_storage_json"]["identity"]["metadata"]
    metadata["selected_resources"] = ["Organization", "InsurancePlan"]
    metadata["expected_resources"] = ["Organization", "InsurancePlan"]
    case.header["publication_metadata_json"] = deepcopy(metadata)
    case.checkpoints.append({**case.checkpoints[0], "resource_type": "InsurancePlan", "rows_processed": 0})


@pytest.mark.asyncio
async def test_completed_empty_collection_retains_isolated_candidate(case):
    _include_completed_empty_collection(case)
    result = await _transition(case, {"Organization": 3, "InsurancePlan": 0})
    assert result["rows"] == 3 and result["already_applied"] is False
    assert case.header["candidate_storage_json"]["storage"] == case.new


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["missing_positive", "foreign_dataset", "unexpected_type"])
async def test_empty_collection_still_rejects_foreign_or_missing_input(case, drift):
    _include_completed_empty_collection(case)
    if drift == "missing_positive":
        case.input_rows.clear()
    else:
        case.input_rows.append(
            {
                "dataset_id": "other" if drift == "foreign_dataset" else DATASET,
                "resource_type": "InsurancePlan" if drift == "foreign_dataset" else "Endpoint",
                "total": 1,
                "active": 1,
            }
        )
    with pytest.raises(GenericCandidateError, match="transition_input_changed"):
        await _transition(case, {"Organization": 3, "InsurancePlan": 0})
    assert case.writes == []


@pytest.mark.asyncio
async def test_exact_detached_candidate_changes_only_its_storage_binding(case):
    before = deepcopy(case.header["candidate_storage_json"])
    result = await _transition(case)
    assert result["rows"] == 3 and result["already_applied"] is False
    assert result["lineage_run_ids"] == sorted([ROOT, FAILED])
    assert case.header["candidate_storage_json"] == {**before, "storage": case.new}
    assert len(case.writes) == 1
    assert "candidate_storage_json=$2::jsonb" in case.writes[0][0]
    case.native.assert_awaited_once()
    case.tables.assert_awaited_once()
    assert (await _transition(case))["already_applied"] is True
    assert len(case.writes) == 1


def _retain_original_root(case, status):
    """An original run has no retry parent or explicit reference to itself."""
    case.runs[:] = [case.runs[-1]]
    case.runs[0].update(status=status, root_run_id=None)
    case.header["import_run_id"] = ROOT
    for checkpoint in (*case.checkpoints, *case.bulk_checkpoints):
        checkpoint["owner_run_id"] = ROOT


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fixture_name,status", [("case", "failed"), ("bulk_case", "failed"), ("bulk_case", "canceled")]
)
async def test_original_root_without_self_reference_transitions_and_replays(request, fixture_name, status):
    case = request.getfixturevalue(fixture_name)
    _retain_original_root(case, status)
    expected_counts_by_resource = (
        {"Organization": 3} if fixture_name == "case" else {"Organization": 3, "Location": 2, "Endpoint": 0}
    )
    evidence = deepcopy((case.checkpoints, case.bulk_checkpoints, case.bulk_outputs, case.input_rows, case.runs))
    before = deepcopy(case.header["candidate_storage_json"])
    result = await _transition(case, expected_counts_by_resource, stopped_run_id=ROOT)
    assert result["lineage_run_ids"] == [ROOT] and result["rows"] == sum(expected_counts_by_resource.values())
    assert result["already_applied"] is False
    assert case.header["candidate_storage_json"] == {**before, "storage": case.new}
    assert (await _transition(case, expected_counts_by_resource, stopped_run_id=ROOT))["already_applied"] is True
    assert len(case.writes) == 1
    assert (case.checkpoints, case.bulk_checkpoints, case.bulk_outputs, case.input_rows, case.runs) == evidence


@pytest.mark.asyncio
@pytest.mark.parametrize("root_reference", [None, "run_" + "c" * 32])
async def test_retry_child_still_requires_its_exact_root_reference(case, root_reference):
    case.runs[0]["root_run_id"] = root_reference
    with pytest.raises(GenericCandidateError, match="transition_run_changed"):
        await _transition(case)
    assert case.writes == []


@pytest.mark.asyncio
async def test_original_root_rejects_a_foreign_self_reference(case):
    _retain_original_root(case, "failed")
    case.runs[0]["root_run_id"] = "run_" + "c" * 32
    with pytest.raises(GenericCandidateError, match="transition_run_changed"):
        await _transition(case, stopped_run_id=ROOT)
    assert case.writes == []


def _include_active_collection(case):
    """Retain an opaque cursor and the exact already-committed input census."""
    metadata = case.header["candidate_storage_json"]["identity"]["metadata"]
    metadata["selected_resources"] = ["Organization", "Location"]
    metadata["expected_resources"] = ["Organization", "Location"]
    case.header["publication_metadata_json"] = deepcopy(metadata)
    case.checkpoints.append(
        {
            **case.checkpoints[0],
            "resource_type": "Location",
            "state": "active",
            "rows_processed": 2,
            "completed_at": None,
            "start_url_hash": hashlib.sha256(b"https://example.test/fhir/Location?_count=100").hexdigest(),
            "next_url": "https://example.test/fhir/Location?_getpages=opaque%2Btoken&_count=100",
        }
    )
    case.input_rows.append(dict(dataset_id=DATASET, resource_type="Location", total=2, active=2))


@pytest.mark.asyncio
async def test_mixed_checkpoint_transition_and_replay_preserve_all_acquisition_evidence(case):
    _include_active_collection(case)
    checkpoints, inputs, runs = deepcopy((case.checkpoints, case.input_rows, case.runs))
    before = deepcopy(case.header["candidate_storage_json"])
    result = await _transition(case, {"Organization": 3, "Location": 2})
    assert result["rows"] == 5 and result["already_applied"] is False
    assert case.header["candidate_storage_json"] == {**before, "storage": case.new}
    assert (await _transition(case, {"Organization": 3, "Location": 2}))["already_applied"] is True
    assert len(case.writes) == 1
    assert (case.checkpoints, case.input_rows, case.runs) == (checkpoints, inputs, runs)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("state", "failed"),
        ("completed_at", "now"),
        ("pages_processed", 0),
        ("pages_processed", -1),
        ("rows_processed", 1),
        ("dataset_id", "another-dataset"),
        ("owner_run_id", ROOT),
        ("next_url", None),
        ("next_url", ""),
        ("next_url", " https://example.test/fhir/Location?cursor=one"),
        ("next_url", "https://example.test/fhir/Location?cursor=one\n"),
        ("next_url", "/fhir/Location?cursor=one"),
        ("next_url", "http://example.test/fhir/Location?cursor=one"),
        ("next_url", "https://elsewhere.test/fhir/Location?cursor=one"),
        ("next_url", "https://user@example.test/fhir/Location?cursor=one"),
        ("next_url", "https://example.test/fhir/Location?cursor=one#fragment"),
        ("next_url", "https://example.test:invalid/fhir/Location?cursor=one"),
        ("next_url", "https://[invalid/fhir/Location?cursor=one"),
        ("next_url", "https://example.test/fhir/Location?_count=100"),
    ],
)
async def test_malformed_active_checkpoint_refuses_before_any_candidate_write(case, field, value):
    _include_active_collection(case)
    case.checkpoints[1][field] = value
    with pytest.raises(GenericCandidateError, match="transition_checkpoint_changed"):
        await _transition(case, {"Organization": 3, "Location": 2})
    assert case.writes == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "drift", ["missing", "duplicate", "complete_without_time", "complete_without_pages", "inactive"]
)
async def test_mixed_transition_preserves_complete_checkpoint_and_full_input_requirements(case, drift):
    _include_active_collection(case)
    reason = "transition_checkpoint_changed"
    if drift == "missing":
        case.checkpoints.pop()
    elif drift == "duplicate":
        case.checkpoints.append(deepcopy(case.checkpoints[1]))
    elif drift == "complete_without_time":
        case.checkpoints[0]["completed_at"] = None
    elif drift == "complete_without_pages":
        case.checkpoints[0]["pages_processed"] = 0
    else:
        case.input_rows[1]["active"] = 1
        reason = "transition_input_changed"
    with pytest.raises(GenericCandidateError, match=reason):
        await _transition(case, {"Organization": 3, "Location": 2})
    assert case.writes == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "drift,reason",
    [
        ("active", "transition_run_changed"),
        ("newest", "transition_run_changed"),
        ("cycle", "transition_run_changed"),
        ("checkpoint", "transition_checkpoint_changed"),
        ("row_count", "transition_input_changed"),
        ("metadata", "transition_candidate_changed"),
        ("producer", "transition_storage_changed"),
        ("identity", "transition_candidate_changed"),
    ],
)
async def test_drift_refuses_without_candidate_write(case, drift, reason):
    match drift:
        case "active":
            case.runs[1]["status"] = "running"
        case "newest":
            case.runs.reverse()
        case "cycle":
            case.runs[0]["retry_of_run_id"] = FAILED
        case "checkpoint":
            case.checkpoints[0]["rows_processed"] = 2
        case "row_count":
            case.input_rows[0]["total"] = 2
        case "metadata":
            case.header["publication_metadata_json"] = {}
        case "producer":
            case.new["catalog_schema"] = "other_schema"
        case "identity":
            case.header["endpoint_id"] = "other-endpoint"
    with pytest.raises(GenericCandidateError, match=reason):
        await _transition(case)
    assert case.writes == []


@pytest.mark.asyncio
async def test_native_table_identity_failure_refuses_before_write(case):
    case.tables.side_effect = GenericCandidateError("native_table_changed")
    with pytest.raises(GenericCandidateError, match="native_table_changed"):
        await _transition(case)
    assert case.writes == []


@pytest.mark.asyncio
async def test_candidate_compare_race_refuses_without_replacing_storage(case):
    before = deepcopy(case.header["candidate_storage_json"])
    case.connection.execute = AsyncMock(return_value="UPDATE 0")
    with pytest.raises(GenericCandidateError, match="transition_header_compare_failed"):
        await _transition(case)
    assert case.header["candidate_storage_json"] == before


@pytest.mark.asyncio
async def test_transition_requires_independent_serializable_administrator(case):
    case.connection.fetchval = AsyncMock(return_value="read committed")
    with pytest.raises(GenericCandidateError, match="transition_isolation_required"):
        await _transition(case)
    assert case.writes == []


@pytest.mark.asyncio
async def test_failed_run_worker_action_lock_refuses_a_concurrent_launch(case):
    async def fetchval(sql, *args):
        if sql == "SHOW transaction_isolation":
            return "serializable"
        return not (args and args[0] == "control-run-worker-action:v1:" + FAILED)

    case.connection.fetchval = AsyncMock(side_effect=fetchval)
    with pytest.raises(GenericCandidateError, match="transition_worker_action_busy"):
        await _transition(case)
    assert case.writes == []


def _bulk_evidence(resource, state, counts, lengths, committed):
    """Build URL-free completed and partial export evidence for the same candidate."""
    directory = storage.import_module("process.provider_directory_fhir")
    start = "https://example.test/fhir/$export?_type=" + resource
    identity_by_field = dict(
        canonical_api_base="https://example.test/fhir",
        resource_type=resource,
        source_scope_hash="5" * 64,
        strategy_version=directory.BULK_EXPORT_CHECKPOINT_STRATEGY_VERSION,
        acquisition_root_run_id=ROOT,
        endpoint_id=ENDPOINT,
        dataset_id=DATASET,
        start_url_hash=hashlib.sha256(start.encode()).hexdigest(),
    )
    checkpoint_id = directory._identity_hash(identity_by_field)
    outputs = [
        dict(
            checkpoint_id=checkpoint_id,
            output_id=hashlib.sha256(f"{resource}:{index}".encode()).hexdigest(),
            output_index=index,
            resource_type=resource,
            output_url_hash=hashlib.sha256(f"synthetic-output:{resource}:{index}".encode()).hexdigest(),
            state="complete" if size == length else "streaming" if count else "pending",
            rows_written=count,
            content_length_bytes=length,
            committed_bytes=size,
            etag_hash="6" * 64,
            validator_checked_at="before",
            completed_at="now" if size == length else None,
            error=None,
            has_output_url=size != length,
            has_etag=size != length,
        )
        for index, (count, length, size) in enumerate(zip(counts, lengths, committed, strict=True))
    ]
    checkpoint_by_field = dict(
        **identity_by_field,
        checkpoint_id=checkpoint_id,
        owner_run_id=FAILED,
        state=state,
        rows_written=sum(counts),
        manifest_hash="7" * 64,
        failed_at=None,
        completed_at="now" if state == "complete" else None,
        error=None,
        manifest_json=dict(
            transactionTime="2026-01-01T00:00:00Z",
            request_hash=identity_by_field["start_url_hash"],
            requiresAccessToken=False,
            output=[dict(type=resource, url_hash=output["output_url_hash"]) for output in outputs],
            deleted=[],
        ),
    )
    return checkpoint_by_field, outputs


@pytest.fixture
def bulk_case(case):
    case.checkpoints.clear()
    metadata = case.header["candidate_storage_json"]["identity"]["metadata"]
    metadata.update(
        selected_resources=["Organization", "Location", "Endpoint"],
        expected_resources=["Organization", "Location", "Endpoint"],
    )
    case.header["publication_metadata_json"] = deepcopy(metadata)
    for evidence in (
        _bulk_evidence("Organization", "complete", [1, 2], [60, 120], [60, 120]),
        _bulk_evidence("Location", "streaming", [2, 0], [360, 240], [120, 0]),
    ):
        checkpoint, outputs = evidence
        case.bulk_checkpoints.append(checkpoint)
        case.bulk_outputs.extend(outputs)
    case.input_rows.append(dict(dataset_id=DATASET, resource_type="Location", total=2, active=2))
    return case


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["failed", "canceled"])
async def test_bulk_transition_preserves_completed_partial_and_unstarted_resources(bulk_case, status):
    bulk_case.runs[0]["status"] = status
    before = deepcopy((bulk_case.bulk_checkpoints, bulk_case.bulk_outputs, bulk_case.input_rows, bulk_case.runs))
    candidate = deepcopy(bulk_case.header["candidate_storage_json"])
    expected_counts_by_resource = {"Organization": 3, "Location": 2, "Endpoint": 0}
    receipt = await _transition(bulk_case, expected_counts_by_resource)
    assert receipt["rows"] == 5 and receipt["already_applied"] is False
    assert bulk_case.header["candidate_storage_json"] == {**candidate, "storage": bulk_case.new}
    assert (await _transition(bulk_case, expected_counts_by_resource))["already_applied"] is True
    assert len(bulk_case.writes) == 1
    assert (bulk_case.bulk_checkpoints, bulk_case.bulk_outputs, bulk_case.input_rows, bulk_case.runs) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("completed_owner", [ROOT, "run_" + "c" * 32])
async def test_bulk_transition_reuses_completed_export_from_verified_ancestor(bulk_case, completed_owner):
    predecessor = "run_" + "c" * 32
    bulk_case.runs[0]["retry_of_run_id"] = predecessor
    bulk_case.runs.insert(1, {**bulk_case.runs[-1], "run_id": predecessor, "retry_of_run_id": ROOT})
    bulk_case.bulk_checkpoints[0]["owner_run_id"] = completed_owner
    before = deepcopy((bulk_case.bulk_checkpoints, bulk_case.bulk_outputs))

    receipt = await _transition(bulk_case, {"Organization": 3, "Location": 2, "Endpoint": 0})

    assert receipt["lineage_run_ids"] == sorted([ROOT, predecessor, FAILED])
    assert receipt["rows"] == 5
    assert (bulk_case.bulk_checkpoints, bulk_case.bulk_outputs) == before


@pytest.mark.asyncio
async def test_bulk_transition_rejects_completed_export_owned_by_sibling_retry(bulk_case):
    sibling = "run_" + "c" * 32
    bulk_case.runs.insert(1, {**bulk_case.runs[0], "run_id": sibling, "retry_of_run_id": ROOT})
    bulk_case.bulk_checkpoints[0]["owner_run_id"] = sibling

    with pytest.raises(GenericCandidateError, match="transition_bulk_checkpoint_changed"):
        await _transition(bulk_case, {"Organization": 3, "Location": 2, "Endpoint": 0})
    assert bulk_case.writes == []


@pytest.mark.asyncio
async def test_bulk_transition_receipt_retains_terminal_siblings_for_worker_presence_checks(bulk_case):
    sibling = "run_" + "c" * 32
    bulk_case.runs.insert(1, {**bulk_case.runs[0], "run_id": sibling, "retry_of_run_id": ROOT})
    bulk_case.bulk_checkpoints[0]["owner_run_id"] = ROOT

    receipt = await _transition(bulk_case, {"Organization": 3, "Location": 2, "Endpoint": 0})

    assert receipt["lineage_run_ids"] == sorted([ROOT, FAILED, sibling])


def _retain_reordered_bulk_manifest(transition_case, *, omit_format, legacy, query_suffix=""):
    """Use genuine parser/audit output while preserving the saved file boundaries."""
    directory = storage.import_module("process.provider_directory_fhir")
    checkpoint = transition_case.bulk_checkpoints[1]
    start = directory._bulk_export_start_url(
        {
            "api_base": checkpoint["canonical_api_base"],
            "metadata_json": {"provider_directory_bulk_export_omit_output_format": omit_format},
        },
        "Location",
    )
    start += query_suffix
    parsed_request = directory.urllib.parse.urlsplit(start)
    query = directory.urllib.parse.urlencode(list(reversed(directory.urllib.parse.parse_qsl(parsed_request.query))))
    manifest_by_field = dict(
        transactionTime="2026-01-01T00:00:00Z",
        requiresAccessToken=False,
        request=directory.urllib.parse.urlunsplit(parsed_request._replace(query=query)),
        output=[dict(type="Location", url=f"https://example.test/output/{i}") for i in range(2)],
    )
    manifest = directory._bulk_export_manifest_from_payload(manifest_by_field, "Location", expected_request_url=start)
    audit = directory._bulk_manifest_audit_payload(manifest.payload)
    if legacy:
        audit.pop("normalized_request_hash")
    checkpoint.update(
        start_url_hash=hashlib.sha256(start.encode()).hexdigest(),
        manifest_json=audit,
        manifest_hash=manifest.identity_hash,
    )
    fields = (
        "canonical_api_base",
        "resource_type",
        "source_scope_hash",
        "strategy_version",
        "acquisition_root_run_id",
        "endpoint_id",
        "dataset_id",
        "start_url_hash",
    )
    checkpoint["checkpoint_id"] = directory._identity_hash({key: checkpoint[key] for key in fields})
    for output, manifest_output in zip(transition_case.bulk_outputs[2:], manifest.outputs, strict=True):
        output.update(checkpoint_id=checkpoint["checkpoint_id"], output_url_hash=manifest_output.url_hash)


@pytest.mark.asyncio
@pytest.mark.parametrize("omit_format", [False, True])
@pytest.mark.parametrize("legacy", [False, True])
async def test_bulk_transition_preserves_reordered_request_without_decryption_keys(
    bulk_case, monkeypatch, omit_format, legacy
):
    monkeypatch.delenv("HLTHPRT_PROVIDER_DIRECTORY_CHECKPOINT_KEY", raising=False)
    monkeypatch.delenv("HLTHPRT_CONTROL_API_TOKEN", raising=False)
    _retain_reordered_bulk_manifest(bulk_case, omit_format=omit_format, legacy=legacy)
    before = deepcopy((bulk_case.bulk_checkpoints, bulk_case.bulk_outputs))

    receipt = await _transition(bulk_case, {"Organization": 3, "Location": 2, "Endpoint": 0})

    assert receipt["rows"] == 5 and receipt["already_applied"] is False
    assert (bulk_case.bulk_checkpoints, bulk_case.bulk_outputs) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("commitment", [None, "invalid", "8" * 64])
async def test_bulk_transition_never_falls_back_from_invalid_canonical_commitment(bulk_case, commitment):
    checkpoint = bulk_case.bulk_checkpoints[1]
    checkpoint["manifest_json"]["normalized_request_hash"] = commitment

    with pytest.raises(GenericCandidateError, match="transition_bulk_checkpoint_changed"):
        await _transition(bulk_case, {"Organization": 3, "Location": 2, "Endpoint": 0})
    assert bulk_case.writes == []


@pytest.mark.asyncio
async def test_bulk_transition_rejects_unrecognized_start_even_with_consistent_manifest(bulk_case):
    """A retained delta query must not stand in for either supported full export."""
    _retain_reordered_bulk_manifest(
        bulk_case, omit_format=False, legacy=False, query_suffix="&_since=2025-12-31T00:00:00Z"
    )
    before = deepcopy((bulk_case.header, bulk_case.bulk_checkpoints, bulk_case.bulk_outputs, bulk_case.input_rows))
    with pytest.raises(
        GenericCandidateError, match="^provider_directory_generic_source_transition_bulk_checkpoint_changed$"
    ):
        await _transition(bulk_case, {"Organization": 3, "Location": 2, "Endpoint": 0})
    assert bulk_case.writes == []
    assert (bulk_case.header, bulk_case.bulk_checkpoints, bulk_case.bulk_outputs, bulk_case.input_rows) == before


@pytest.mark.asyncio
@pytest.mark.parametrize("original_root", [False, True])
async def test_canceled_rest_candidate_remains_ineligible(case, original_root):
    if original_root:
        _retain_original_root(case, "canceled")
    else:
        case.runs[0]["status"] = "canceled"
    with pytest.raises(GenericCandidateError, match="transition_run_changed"):
        await _transition(case, stopped_run_id=ROOT if original_root else FAILED)
    assert case.writes == []


@pytest.mark.asyncio
async def test_bulk_transition_selects_only_the_requested_manifest_resource(bulk_case):
    for checkpoint in bulk_case.bulk_checkpoints:
        checkpoint["manifest_json"]["output"].insert(0, {"type": "Practitioner", "url_hash": "a" * 64})
    receipt = await _transition(bulk_case, {"Organization": 3, "Location": 2, "Endpoint": 0})
    assert receipt["rows"] == 5 and receipt["already_applied"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["running", "canceling", "succeeded", "dead_letter"])
async def test_bulk_transition_rejects_other_run_states(bulk_case, status):
    bulk_case.runs[0]["status"] = status
    with pytest.raises(GenericCandidateError, match="transition_run_changed"):
        await _transition(bulk_case, {"Organization": 3, "Location": 2, "Endpoint": 0})
    assert bulk_case.writes == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value,expected_error",
    [
        ("acquisition_root_run_id", "run_" + "c" * 32, "transition_bulk_checkpoint_changed"),
        ("dataset_id", "another-dataset", "transition_bulk_checkpoint_changed"),
        ("endpoint_id", "another-endpoint", "transition_bulk_checkpoint_changed"),
        ("owner_run_id", ROOT, "transition_bulk_checkpoint_changed"),
        ("resource_type", "Practitioner", "transition_bulk_checkpoint_changed"),
        ("source_scope_hash", "8" * 64, "transition_bulk_checkpoint_changed"),
        ("canonical_api_base", "https://elsewhere.test/fhir", "transition_bulk_checkpoint_changed"),
        ("checkpoint_id", "9" * 64, "transition_bulk_output_changed"),
        ("start_url_hash", "9" * 64, "transition_bulk_checkpoint_changed"),
        ("strategy_version", "different", "transition_bulk_checkpoint_changed"),
        ("manifest_hash", None, "transition_bulk_checkpoint_changed"),
        ("manifest_json", {}, "transition_bulk_checkpoint_changed"),
        ("state", "failed", "transition_bulk_checkpoint_changed"),
        ("state", "starting", "transition_bulk_checkpoint_changed"),
        ("completed_at", "now", "transition_bulk_checkpoint_changed"),
        ("failed_at", "now", "transition_bulk_checkpoint_changed"),
        ("error", "invalid_ndjson", "transition_bulk_checkpoint_changed"),
        ("error", {}, "transition_bulk_checkpoint_changed"),
    ],
)
async def test_bulk_transition_rejects_foreign_or_unresumable_exports(bulk_case, field, value, expected_error):
    bulk_case.bulk_checkpoints[1][field] = value
    with pytest.raises(GenericCandidateError, match=rf"^provider_directory_generic_source_{expected_error}$"):
        await _transition(bulk_case, {"Organization": 3, "Location": 2, "Endpoint": 0})
    assert bulk_case.writes == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("checkpoint_id", "8" * 64),
        ("output_id", "invalid"),
        ("output_index", 1),
        ("resource_type", "Practitioner"),
        ("output_url_hash", "9" * 64),
        ("state", "failed"),
        ("completed_at", "now"),
        ("rows_written", 3),
        ("rows_written", -1),
        ("committed_bytes", -1),
        ("committed_bytes", 361),
        ("committed_bytes", 0),
        ("content_length_bytes", 0),
        ("etag_hash", "bad"),
        ("validator_checked_at", None),
        ("has_output_url", False),
        ("has_etag", False),
        ("error", "invalid_ndjson"),
    ],
)
async def test_bulk_transition_rejects_changed_file_evidence(bulk_case, field, value):
    bulk_case.bulk_outputs[2][field] = value
    with pytest.raises(GenericCandidateError, match="transition_bulk_output_changed"):
        await _transition(bulk_case, {"Organization": 3, "Location": 2, "Endpoint": 0})
    assert bulk_case.writes == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "drift,expected_error",
    [
        ("mixed", "transition_checkpoint_changed"),
        ("missing_export", "transition_bulk_checkpoint_changed"),
        ("missing_file", "transition_bulk_output_changed"),
        ("duplicate_export", "transition_bulk_checkpoint_changed"),
        ("duplicate_file", "transition_bulk_output_changed"),
        ("unstarted_rows", "transition_bulk_checkpoint_changed"),
        ("manifest_file", "transition_bulk_output_changed"),
        ("manifest_time", "transition_bulk_checkpoint_changed"),
        ("manifest_request", "transition_bulk_checkpoint_changed"),
        ("parent_total", "transition_bulk_output_changed"),
        ("completed_short", "transition_bulk_output_changed"),
        ("completed_unfinished", "transition_bulk_output_changed"),
        ("inactive_input", "transition_input_changed"),
    ],
)
async def test_bulk_transition_requires_the_complete_unchanged_census(bulk_case, drift, expected_error):
    expected_counts_by_resource = {"Organization": 3, "Location": 2, "Endpoint": 0}
    match drift:
        case "mixed":
            bulk_case.checkpoints.extend(_completed_input_evidence()[1])
        case "missing_export":
            bulk_case.bulk_checkpoints.pop()
        case "missing_file":
            bulk_case.bulk_outputs.pop()
        case "duplicate_export":
            bulk_case.bulk_checkpoints.append(deepcopy(bulk_case.bulk_checkpoints[0]))
        case "duplicate_file":
            bulk_case.bulk_outputs.append(deepcopy(bulk_case.bulk_outputs[0]))
        case "unstarted_rows":
            expected_counts_by_resource["Endpoint"] = 1
        case "manifest_file":
            bulk_case.bulk_checkpoints[1]["manifest_json"]["output"][0]["url_hash"] = "8" * 64
        case "manifest_time":
            bulk_case.bulk_checkpoints[1]["manifest_json"]["transactionTime"] = "invalid"
        case "manifest_request":
            bulk_case.bulk_checkpoints[1]["manifest_json"]["request_hash"] = "8" * 64
        case "parent_total":
            bulk_case.bulk_checkpoints[1]["rows_written"] = 1
        case "completed_short":
            bulk_case.bulk_outputs[0]["committed_bytes"] = 59
        case "completed_unfinished":
            bulk_case.bulk_outputs[0].update(state="streaming", completed_at=None, has_output_url=True, has_etag=True)
        case "inactive_input":
            bulk_case.input_rows[1]["active"] = 1
    with pytest.raises(GenericCandidateError, match=rf"^provider_directory_generic_source_{expected_error}$"):
        await _transition(bulk_case, expected_counts_by_resource)
    assert bulk_case.writes == []
