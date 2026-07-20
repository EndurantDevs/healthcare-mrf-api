# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import datetime
import importlib
import io
import json

import pytest

from process.ptg_parts import artifacts as ptg_artifacts
from process.ptg_parts import input_artifact_retention as retention
from process.ptg_parts import json_streams
from process.ptg_parts.domain import PTG2HeadMetadata


fhir = importlib.import_module("process.provider_directory_fhir")


def _source() -> dict[str, str]:
    return {
        "source_id": "uhc",
        "api_base": fhir.UHC_PROVIDER_DIRECTORY_BASE,
        "canonical_api_base": fhir.UHC_PROVIDER_DIRECTORY_BASE,
    }


def test_uhc_reference_helpers_cover_multi_value_and_state_only_rows():
    assert fhir._insurance_plan_network_references(
        {
            "network": [{"reference": "Organization/n1"}],
            "plan": [
                {"network": [{"reference": "Organization/n2"}]},
                {"network": [{"reference": "Organization/n3"}]},
            ],
        }
    ) == ["Organization/n1", "Organization/n2", "Organization/n3"]
    assert fhir._uhc_location_partition_predicate_error(
        _source(),
        "Location",
        f"{fhir.UHC_PROVIDER_DIRECTORY_BASE}/Location?address-state=CA",
        {
            "resourceType": "Bundle",
            "entry": [
                {
                    "resource": {
                        "resourceType": "Location",
                        "address": {"state": "CA"},
                    }
                }
            ],
        },
    ) is None


def test_uhc_plan_graph_merge_accepts_empty_network_page():
    assert fhir._merge_uhc_plan_graph_bundle_rows(
        _source(),
        "PractitionerRole",
        {"resourceType": "Bundle", "total": 0, "entry": []},
        fetch_url="https://example.test/PractitionerRole",
        network_reference="Organization/n1",
        run_id="run",
        rows_by_id={},
        fingerprints_by_id={},
    ) is None


@pytest.mark.asyncio
async def test_uhc_linked_rows_cover_empty_and_successful_collections(monkeypatch):
    monkeypatch.setattr(fhir, "_linked_resource_refs", lambda _rows: [])
    assert await fhir._fetch_uhc_plan_graph_linked_rows(
        _source(),
        {},
        timeout=1,
        run_id="run",
        deadline_at=None,
    ) == ({}, None, False)

    linked_reference = (
        "Practitioner",
        "p1",
        "Practitioner/p1",
        "practitioner_ref",
    )
    monkeypatch.setattr(fhir, "_linked_resource_refs", lambda _rows: [linked_reference])

    async def linked_row(*_args, **_kwargs):
        return fhir.RESOURCE_MODELS_BY_TYPE["Practitioner"], {"resource_id": "p1"}

    monkeypatch.setattr(fhir, "_fetch_linked_resource_row", linked_row)
    linked_rows_by_resource, error, deadline = await fhir._fetch_uhc_plan_graph_linked_rows(
        _source(),
        {},
        timeout=1,
        run_id="run",
        deadline_at=None,
    )
    assert linked_rows_by_resource == {"Practitioner": [{"resource_id": "p1"}]}
    assert error is None and deadline is False


@pytest.mark.asyncio
async def test_uhc_snapshot_reports_network_failure_and_merges_linked_rows(monkeypatch):
    async def insurance_plans(*_args, **_kwargs):
        return [
            {"resource_id": "plan", "network_refs": ["Organization/n1"]}
        ], 1, None, False

    async def failed_network(*_args, **_kwargs):
        return {}, {}, "network-failed", False

    monkeypatch.setattr(fhir, "_fetch_uhc_plan_graph_collection", insurance_plans)
    monkeypatch.setattr(fhir, "_fetch_uhc_plan_graph_network_shards", failed_network)
    failed = await fhir._fetch_uhc_plan_graph_snapshot(
        _source(),
        page_count=100,
        timeout=1,
        run_id="run",
        deadline_at=None,
    )
    assert failed.error == "network-failed"

    async def linked_rows(*_args, **_kwargs):
        return {"Location": [{"resource_id": "l1"}]}, None, False

    monkeypatch.setattr(fhir, "_fetch_uhc_plan_graph_linked_rows", linked_rows)
    resolved = await fhir._resolve_uhc_plan_graph_snapshot_links(
        _source(),
        {"InsurancePlan": [{"resource_id": "plan"}]},
        {"InsurancePlan": 1},
        1,
        timeout=1,
        run_id="run",
        deadline_at=None,
    )
    assert resolved.rows_by_resource["Location"] == [{"resource_id": "l1"}]


@pytest.mark.asyncio
async def test_provider_directory_persistence_helpers_cover_empty_edges(monkeypatch):
    await fhir._update_source_resource_import_metadata(
        [],
        run_id=None,
        diagnostics={},
    )

    monkeypatch.setattr(fhir, "_endpoint_dataset_resource_rows", lambda *_a, **_k: [])
    monkeypatch.setattr(fhir, "_canonical_resource_rows", lambda *_a, **_k: [])
    monkeypatch.setattr(fhir, "_source_resource_edge_rows", lambda *_a, **_k: [])

    async def upsert(_model, rows, **_kwargs):
        return len(rows)

    monkeypatch.setattr(fhir, "_upsert_rows", upsert)
    written = await fhir._upsert_resource_rows(
        fhir.RESOURCE_MODELS_BY_TYPE["Organization"],
        [{"resource_id": "o1"}],
        run_id="run",
        track_seen=False,
        canonical_api_base=fhir.UHC_PROVIDER_DIRECTORY_BASE,
        source_ids=["uhc"],
    )
    assert written == 1

    assert await fhir._delete_stale_resource_rows(
        fhir.RESOURCE_MODELS_BY_TYPE["Organization"],
        "uhc",
        None,
    ) == 0

    class UnknownResource:
        __tablename__ = "unknown_resource"

    async def status(*_args, **_kwargs):
        return 0

    monkeypatch.setattr(fhir.db, "status", status)
    assert await fhir._delete_stale_resource_rows(
        UnknownResource,
        "uhc",
        "run",
        use_seen_table=True,
    ) == 0
    assert await fhir._delete_stale_resource_rows(
        UnknownResource,
        "uhc",
        "run",
    ) == 0


@pytest.mark.asyncio
async def test_uhc_import_group_falls_back_to_canonical_source_id(monkeypatch):
    acquisition = fhir.UHCPlanGraphAcquisition(
        fhir.UHCPlanGraphSnapshot({}, {}, 0, error="not-ready"),
        False,
    )

    async def acquire(*_args, **_kwargs):
        return acquisition

    async def write(*_args, **_kwargs):
        return {}

    async def finalize(*_args, **_kwargs):
        return {}, {}

    monkeypatch.setattr(fhir, "_acquire_uhc_plan_graph", acquire)
    monkeypatch.setattr(fhir, "_write_uhc_plan_graph_resources", write)
    monkeypatch.setattr(
        fhir,
        "_uhc_plan_graph_result_metadata",
        lambda *_args, **_kwargs: ({}, {}, {}),
    )
    monkeypatch.setattr(fhir, "_finalize_uhc_plan_graph_stale_rows", finalize)
    import_group_result = await fhir._import_uhc_plan_graph_source_group(
        _source(),
        ["InsurancePlan"],
        page_count=100,
        timeout=1,
        run_id="run",
        deadline_seconds=1,
        stale_cleanup=False,
        seen_table=None,
    )
    assert import_group_result[0] == ["uhc"]


def test_ptg_lock_and_manifest_loop_edges(tmp_path, monkeypatch):
    lock = ptg_artifacts._PTG2FileLock(tmp_path / "open-failure.lock")

    def fail_open(*_args, **_kwargs):
        raise OSError("injected open failure")

    monkeypatch.setattr(ptg_artifacts.os, "open", fail_open)
    with pytest.raises(OSError, match="injected open failure"):
        lock.acquire()
    assert lock._thread_lock.acquire(blocking=False)
    lock._thread_lock.release()

    monkeypatch.undo()
    store = ptg_artifacts.PTG2ArtifactStore(tmp_path / "manifest")
    store.manifest_path.write_text(
        '{"one":1}\n[]\n{"two":2}\n',
        encoding="utf-8",
    )
    assert store._manifest_entries() == [{"one": 1}, {"two": 2}]


def test_ptg_reuse_policy_loop_edges(tmp_path):
    candidate_metadata_by_field = {"raw_sha256": "a" * 64, "content_length": 1}
    head = PTG2HeadMetadata(url="https://example.test", content_length=1)
    assert ptg_artifacts.choose_reusable_raw_artifact(
        [candidate_metadata_by_field], None
    ) == (None, None)
    assert ptg_artifacts.choose_reusable_raw_artifact(
        [candidate_metadata_by_field],
        head,
        reuse_policy="hash",
    ) == (None, None)

    store = ptg_artifacts.PTG2ArtifactStore(tmp_path / "reuse")
    assert ptg_artifacts.choose_reusable_raw_artifact(
        [candidate_metadata_by_field],
        head,
        store,
        reuse_policy="metadata",
    ) == (None, None)

    first = store.root / "first"
    second = store.root / "second"
    first.write_bytes(b"not-a")
    second.write_bytes(b"not-b")
    candidates = [
        {"raw_sha256": "a" * 64, "raw_storage_uri": first.as_uri()},
        {"raw_sha256": "b" * 64, "raw_storage_uri": second.as_uri()},
    ]
    assert ptg_artifacts.choose_reusable_raw_artifact(
        candidates,
        None,
        store,
        reuse_policy="hash",
    ) == (None, None)


def test_ptg_retention_loops_over_released_paths_and_prefixes(tmp_path):
    store = ptg_artifacts.PTG2ArtifactStore(tmp_path / "retention")
    retained_paths = [store.root / "raw" / name for name in ("one", "two")]
    for path in retained_paths:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(path.name.encode())

    now = datetime.datetime(2026, 7, 20, tzinfo=datetime.UTC)
    marked = retention._mark_released_paths_unleased_locked(
        store,
        {
            "paths": [path.relative_to(store.root).as_posix() for path in retained_paths],
            "prefixes": [],
        },
        active_exact_paths=set(),
        active_prefixes=set(),
        now=now,
    )
    assert marked == {"raw/one", "raw/two"}
    assert retention._mark_released_paths_unleased_locked(
        store,
        {
            "paths": [path.relative_to(store.root).as_posix() for path in retained_paths],
            "prefixes": [],
        },
        active_exact_paths=set(),
        active_prefixes=set(),
        now=now,
    ) == set()

    (store.root / "raw" / "aaa-directory").mkdir()
    lease = retention.PTG2ArtifactLease(
        store=store,
        owner="prefix",
        heartbeat_seconds=0,
    ).start()
    try:
        with retention.bind_artifact_lease(lease.lease_id):
            retention.protect_artifact_prefix(store, store.root / "raw")
    finally:
        lease.release()


def test_ptg_retention_capacity_selection_checks_multiple_candidates(tmp_path):
    observed_at = datetime.datetime(2026, 7, 20, tzinfo=datetime.UTC)
    stored_artifacts = [
        retention._StoredArtifact(
            path=tmp_path / name,
            relative_path=f"raw/{name}",
            size=10,
            modified_at=0,
            protected=False,
            unleased_since=observed_at,
        )
        for name in ("one", "two")
    ]
    eligible, selected = retention._select_artifacts(
        stored_artifacts,
        now_timestamp=observed_at.timestamp() + 1,
        retention_seconds=3600,
        min_age_seconds=0,
        target_bytes=0,
        max_delete_bytes=None,
        max_delete_files=None,
    )
    assert eligible == stored_artifacts
    assert selected == stored_artifacts


@pytest.mark.asyncio
async def test_ptg_lease_guard_observes_released_lease(monkeypatch, tmp_path):
    lease = retention.PTG2ArtifactLease(
        store=ptg_artifacts.PTG2ArtifactStore(tmp_path / "guard"),
        owner="released",
        heartbeat_seconds=1,
    )
    lease._released = True
    monitor_observed_release = asyncio.Event()
    original_sleep = asyncio.sleep

    async def yielding_sleep(_seconds):
        monitor_observed_release.set()
        await original_sleep(0)

    async def operation():
        await monitor_observed_release.wait()
        return "complete"

    monkeypatch.setattr(retention.asyncio, "sleep", yielding_sleep)
    assert await retention.guard_artifact_lease(lease, operation()) == "complete"


def test_ptg_json_iterators_cover_multi_prefix_and_eof_loops():
    parsed_objects = list(
        json_streams._iter_top_level_objects(
            io.BytesIO(b'{"items":[{"id":1}]}'),
            {"other": "other.item", "items": "items.item"},
        )
    )
    assert parsed_objects == [("items", {"id": 1})]

    decoded_objects = list(
        json_streams._iter_top_level_objects_jsondecoder(
            io.BytesIO(b'{"items":[{"id":1}]}'),
            {"items": "items.item"},
            chunk_size=1,
        )
    )
    assert decoded_objects == [("items", {"id": 1})]
