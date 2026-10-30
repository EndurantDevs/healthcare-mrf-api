# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""A successor producer may adopt only one verified, detached failed intake."""

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
    writes = []

    async def fetchrow(sql, *args):
        if sql.startswith("SELECT session_user"):
            return dict(session="administrator", current="administrator", administrator=True)
        return deepcopy(header)

    async def fetchval(sql, *args):
        if sql == "SHOW transaction_isolation":
            return "serializable"
        return True

    async def fetch(sql, *args):
        placeholders = {int(value) for value in re.findall(r"\$(\d+)", sql)}
        assert placeholders == set(range(1, len(args) + 1))
        if 'FROM "catalog_schema"."import_run"' in sql:
            return deepcopy(run_rows)
        if "provider_directory_pagination_checkpoint" in sql:
            return deepcopy(checkpoint_rows)
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
        fetchval=AsyncMock(side_effect=fetchval),
        fetch=AsyncMock(side_effect=fetch),
        execute=AsyncMock(side_effect=execute),
    )
    native = AsyncMock()
    tables = AsyncMock()
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
        input_rows=input_rows,
        writes=writes,
        native=native,
        tables=tables,
    )


async def _transition(case, expected_counts=None):
    return await storage.transition_isolated_loading_candidate_storage(
        case.connection,
        policy=case.policy,
        identity_by_field={
            "dataset_id": DATASET,
            "endpoint_id": ENDPOINT,
            "acquisition_root_run_id": ROOT,
            "failed_run_id": FAILED,
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
