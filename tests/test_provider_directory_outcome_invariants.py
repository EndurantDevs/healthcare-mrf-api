# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest


importer = importlib.import_module("process.provider_directory_fhir")


def _context() -> importer.PaginationCheckpointContext:
    return importer.PaginationCheckpointContext(
        canonical_api_base="https://example.test/fhir",
        source_scope_hash="scope-1",
        source_ids=("source-1",),
        owner_run_id="run-1",
        acquisition_root_run_id="root-1",
        endpoint_id="endpoint-1",
        dataset_id="dataset-1",
        lineage_verified=True,
    )


def _candidate(**overrides) -> importer.EndpointDatasetCandidate:
    candidate_by_field = {
        "endpoint_id": "endpoint-1",
        "dataset_id": "dataset-1",
        "acquisition_root_run_id": "root-1",
        "source_ids": ("source-1",),
        "selected_resources": ("Organization", "Practitioner"),
        "expected_resources": ("Organization", "Practitioner"),
        "import_run_id": "run-1",
        "previous_dataset_id": "dataset-current",
    }
    candidate_by_field.update(overrides)
    return importer.EndpointDatasetCandidate(**candidate_by_field)


@pytest.mark.parametrize(
    "control,error",
    [
        ({"census": None}, "census missing"),
        (
            {"census": {"unfiltered_pre": True}},
            "census count invalid",
        ),
        (
            {"census": {"failure": ""}},
            "census failure invalid",
        ),
        (
            {"census": {"ranged_root_pre": 1}},
            "ranged pre census lacks unfiltered pre",
        ),
        (
            {
                "census": {
                    "unfiltered_pre": 1,
                    "ranged_root_post": 1,
                }
            },
            "ranged post census lacks ranged pre",
        ),
        (
            {
                "census": {
                    "unfiltered_pre": 1,
                    "ranged_root_pre": 1,
                    "unfiltered_post": 1,
                }
            },
            "unfiltered post census lacks ranged post",
        ),
    ],
)
def test_partition_census_rejects_incomplete_checkpoint_chains(control, error):
    with pytest.raises(ValueError, match=error):
        importer._partition_census_from_control(control)


@pytest.mark.parametrize(
    "control,error",
    [
        ({"windows": None}, "windows missing"),
        ({"windows": [None]}, "window invalid"),
        (
            {
                "windows": [
                    {"window_id": "window-1", "completed_passes": [2]}
                ]
            },
            "pass markers invalid",
        ),
    ],
)
def test_partition_windows_reject_malformed_checkpoint_markers(control, error):
    with pytest.raises(ValueError, match=error):
        importer._partition_checkpoint_windows(control)


def test_partition_pass_restore_rejects_missing_and_uncounted_windows():
    counted_plan = SimpleNamespace(
        windows={"window-1": SimpleNamespace(count=1, passes={})}
    )
    with pytest.raises(ValueError, match="repeats a window ID"):
        importer._restore_partition_pass_markers(counted_plan, {})

    uncounted_plan = SimpleNamespace(
        windows={"window-1": SimpleNamespace(count=None, passes={})}
    )
    with pytest.raises(ValueError, match="completed an uncounted window"):
        importer._restore_partition_pass_markers(
            uncounted_plan,
            {"window-1": (1,)},
        )


@pytest.mark.parametrize(
    "row",
    [
        {"resource_id": "", "payload_json": {}},
        {"resource_id": "resource-1", "payload_json": None},
        {
            "resource_id": "resource-1",
            "payload_json": {"window_id": "", "fingerprint": "hash-1"},
        },
    ],
)
def test_partition_fingerprint_row_rejects_partial_identity(row):
    with pytest.raises(RuntimeError, match="fingerprint_row_invalid"):
        importer._last_updated_partition_fingerprint_row(row)


def test_partition_fingerprint_row_returns_complete_identity():
    assert importer._last_updated_partition_fingerprint_row(
        {
            "resource_id": "resource-1",
            "payload_json": {
                "window_id": "window-1",
                "fingerprint": "hash-1",
            },
        }
    ) == ("resource-1", "window-1", "hash-1")


@pytest.mark.asyncio
async def test_partition_window_proof_handles_empty_and_candidate_hash_rows():
    database = AsyncMock()
    database.all.return_value = []

    assert await importer._load_last_updated_partition_window_proof(
        _context(),
        "Practitioner",
        "window-1",
        1,
        database_connection=database,
    ) == ({}, {})

    database.all.return_value = [
        {
            "resource_id": "resource-1",
            "payload_json": {
                "window_id": "window-1",
                "fingerprint": "hash-1",
                "candidate_payload_hash": "payload-1",
            },
        }
    ]
    assert await importer._load_last_updated_partition_window_proof(
        _context(),
        "Practitioner",
        "window-1",
        1,
        database_connection=database,
    ) == ({"resource-1": "hash-1"}, {"resource-1": "payload-1"})


@pytest.mark.asyncio
async def test_partition_window_proof_rejects_duplicate_or_wrong_window_rows():
    database = AsyncMock()
    database.all.return_value = [
        {
            "resource_id": "resource-1",
            "payload_json": {
                "window_id": "other-window",
                "fingerprint": "hash-1",
            },
        }
    ]

    with pytest.raises(RuntimeError, match="fingerprint_row_invalid"):
        await importer._load_last_updated_partition_window_proof(
            _context(),
            "Practitioner",
            "window-1",
            1,
            database_connection=database,
        )

    duplicate_row_by_field = {
        "resource_id": "resource-1",
        "payload_json": {
            "window_id": "window-1",
            "fingerprint": "hash-1",
        },
    }
    database.all.return_value = [duplicate_row_by_field, duplicate_row_by_field]
    with pytest.raises(RuntimeError, match="fingerprint_row_invalid"):
        await importer._load_last_updated_partition_window_proof(
            _context(),
            "Practitioner",
            "window-1",
            1,
            database_connection=database,
        )


@pytest.mark.asyncio
async def test_partition_fingerprint_replay_is_empty_exact_or_mismatched(
    monkeypatch,
):
    loader = AsyncMock(return_value=({}, {}))
    monkeypatch.setattr(
        importer,
        "_load_last_updated_partition_window_proof",
        loader,
    )
    assert not await importer._has_partition_fingerprint_replay(
        _context(), "Practitioner", "window-1", 1, {}, {}
    )

    loader.return_value = ({"resource-1": "hash-1"}, {})
    assert await importer._has_partition_fingerprint_replay(
        _context(),
        "Practitioner",
        "window-1",
        1,
        {"resource-1": "hash-1"},
        {},
    )
    with pytest.raises(RuntimeError, match="fingerprint_replay_mismatch"):
        await importer._has_partition_fingerprint_replay(
            _context(),
            "Practitioner",
            "window-1",
            1,
            {"resource-1": "different"},
            {},
        )


@pytest.mark.asyncio
async def test_partition_resource_identity_is_empty_exact_or_conflicting():
    database = AsyncMock()
    await importer._assert_partition_resource_ids_unique(
        _context(),
        "LU:Practitioner:pass:1",
        "window-1",
        {},
        database_connection=database,
    )
    database.all.assert_not_awaited()

    database.all.return_value = []
    await importer._assert_partition_resource_ids_unique(
        _context(),
        "LU:Practitioner:pass:1",
        "window-1",
        {"resource-1": "hash-1"},
        database_connection=database,
    )

    database.all.return_value = [
        {
            "resource_id": "resource-1",
            "payload_json": {
                "window_id": "window-1",
                "fingerprint": "hash-1",
            },
        }
    ]
    await importer._assert_partition_resource_ids_unique(
        _context(),
        "LU:Practitioner:pass:1",
        "window-1",
        {"resource-1": "hash-1"},
        database_connection=database,
    )

    database.all.return_value[0]["payload_json"]["fingerprint"] = "old-hash"
    with pytest.raises(RuntimeError, match="duplicate_resource_id"):
        await importer._assert_partition_resource_ids_unique(
            _context(),
            "LU:Practitioner:pass:1",
            "window-1",
            {"resource-1": "hash-1"},
            database_connection=database,
        )


def test_partition_fingerprint_rows_cover_empty_and_candidate_payloads():
    assert importer._partition_fingerprint_rows(
        _context(), "LU:Practitioner:pass:1", "window-1", "proof-1", {}, {}
    ) == []
    rows = importer._partition_fingerprint_rows(
        _context(),
        "LU:Practitioner:pass:1",
        "window-1",
        "proof-1",
        {"resource-2": "hash-2", "resource-1": "hash-1"},
        {"resource-1": "payload-1"},
    )
    assert [row["resource_id"] for row in rows] == ["resource-1", "resource-2"]
    assert rows[0]["payload_json"]["candidate_payload_hash"] == "payload-1"
    assert "candidate_payload_hash" not in rows[1]["payload_json"]


@pytest.mark.asyncio
async def test_partition_fingerprint_store_rejects_mismatch_and_replays(
    monkeypatch,
):
    replay = AsyncMock(return_value=True)
    unique = AsyncMock()
    writer = AsyncMock()
    monkeypatch.setattr(importer, "_has_partition_fingerprint_replay", replay)
    monkeypatch.setattr(importer, "_assert_partition_resource_ids_unique", unique)
    monkeypatch.setattr(
        importer,
        "_write_last_updated_partition_fingerprint_rows",
        writer,
    )

    with pytest.raises(RuntimeError, match="candidate_hash_mismatch"):
        await importer._store_last_updated_partition_window_fingerprints(
            _context(),
            "Practitioner",
            "window-1",
            1,
            {"resource-1": "hash-1"},
            {"other": "payload-1"},
        )
    await importer._store_last_updated_partition_window_fingerprints(
        _context(),
        "Practitioner",
        "window-1",
        1,
        {"resource-1": "hash-1"},
    )
    unique.assert_not_awaited()
    writer.assert_not_awaited()


@pytest.mark.asyncio
async def test_partition_fingerprint_store_writes_only_nonempty_new_proof(
    monkeypatch,
):
    monkeypatch.setattr(
        importer,
        "_has_partition_fingerprint_replay",
        AsyncMock(return_value=False),
    )
    monkeypatch.setattr(
        importer,
        "_assert_partition_resource_ids_unique",
        AsyncMock(),
    )
    writer = AsyncMock()
    monkeypatch.setattr(
        importer,
        "_write_last_updated_partition_fingerprint_rows",
        writer,
    )

    await importer._store_last_updated_partition_window_fingerprints(
        _context(), "Practitioner", "window-1", 1, {}
    )
    writer.assert_not_awaited()

    await importer._store_last_updated_partition_window_fingerprints(
        _context(),
        "Practitioner",
        "window-1",
        1,
        {"resource-1": "hash-1"},
        {"resource-1": "payload-1"},
    )
    writer.assert_awaited_once()


@pytest.mark.asyncio
async def test_partition_window_staging_handles_empty_invalid_and_exact_rows(
    monkeypatch,
):
    endpoint_rows = Mock(return_value=[])
    monkeypatch.setattr(importer, "_endpoint_dataset_resource_rows", endpoint_rows)

    empty = await importer._stage_last_updated_partition_window(
        _context(),
        {"source_id": "source-1"},
        "Practitioner",
        object,
        (),
        run_id="run-1",
        fetch_url="https://example.test/fhir/Practitioner",
    )
    assert empty.rows == ()

    monkeypatch.setattr(importer, "parse_fhir_resource", lambda *_args, **_kwargs: None)
    with pytest.raises(RuntimeError, match="resource_parse_failed"):
        await importer._stage_last_updated_partition_window(
            _context(),
            {"source_id": "source-1"},
            "Practitioner",
            object,
            ({"resourceType": "Practitioner", "id": "p1"},),
            run_id="run-1",
            fetch_url="https://example.test/fhir/Practitioner",
        )

    monkeypatch.setattr(
        importer,
        "parse_fhir_resource",
        lambda *_args, **_kwargs: (object, {"resource_id": "p1"}),
    )
    with pytest.raises(RuntimeError, match="stage_count_mismatch"):
        await importer._stage_last_updated_partition_window(
            _context(),
            {"source_id": "source-1"},
            "Practitioner",
            object,
            ({"resourceType": "Practitioner", "id": "p1"},),
            run_id="run-1",
            fetch_url="https://example.test/fhir/Practitioner",
        )

    endpoint_rows.return_value = [
        {"resource_id": "p1", "payload_hash": "payload-1"}
    ]
    staged = await importer._stage_last_updated_partition_window(
        _context(),
        {"source_id": "source-1"},
        "Practitioner",
        object,
        ({"resourceType": "Practitioner", "id": "p1"},),
        run_id="run-1",
        fetch_url="https://example.test/fhir/Practitioner",
    )
    assert staged.candidate_hashes_by_id == {"p1": "payload-1"}



