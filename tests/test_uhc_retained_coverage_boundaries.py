# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import builtins
import datetime
import hashlib
import importlib
import io
import json
from pathlib import Path
import sys

import pytest

from process.ptg_parts import artifacts as ptg_artifacts
from process.ptg_parts import input_artifact_retention as retention
from process.ptg_parts import json_streams
from process.ptg_parts.domain import PTG2HeadMetadata


fhir = importlib.import_module("process.provider_directory_fhir")
discovery = importlib.import_module("process.mrf_source_discovery")


def _uhc_source() -> dict[str, str]:
    return {
        "source_id": "uhc",
        "api_base": fhir.UHC_PROVIDER_DIRECTORY_BASE,
        "canonical_api_base": fhir.UHC_PROVIDER_DIRECTORY_BASE,
    }


def test_uhc_partition_helpers_fail_closed_at_bundle_edges():
    source_config = _uhc_source()
    capped_bundle_by_field = {"resourceType": "Bundle", "total": 10_000}
    base_url = f"{fhir.UHC_PROVIDER_DIRECTORY_BASE}/Practitioner"

    assert fhir._uhc_provider_directory_partition_values("unsupported") == ()
    assert fhir._url_with_query_item(f"{base_url}?family=A", "family", "B").endswith(
        "family=A"
    )
    assert not fhir._uhc_adaptive_partition_child_urls(
        source_config,
        "Practitioner",
        base_url,
        capped_bundle_by_field,
    )
    assert (
        fhir._uhc_exact_partition_entries(
            "PractitionerRole",
            base_url,
            capped_bundle_by_field,
        )
        is None
    )
    assert fhir._uhc_exact_partition_entries(
        "Practitioner",
        f"{base_url}?family=A",
        {
            "resourceType": "Bundle",
            "entry": [
                {"resource": "invalid"},
                {"resource": {"resourceType": "Organization", "name": "A"}},
            ],
        },
    ) == []
    assert not fhir._is_uhc_role_zip_cap_hit(
        source_config,
        "Practitioner",
        base_url,
        capped_bundle_by_field,
    )
    assert not fhir._is_uhc_role_zip_cap_hit(
        source_config,
        "PractitionerRole",
        (
            f"{fhir.UHC_PROVIDER_DIRECTORY_BASE}/PractitionerRole?"
            "location.address-postalcode=60601&_page_token=next"
        ),
        capped_bundle_by_field,
    )


def test_uhc_partition_identity_respects_postal_gate(monkeypatch):
    source_config = _uhc_source()
    base_url = f"{fhir.UHC_PROVIDER_DIRECTORY_BASE}/Practitioner"
    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_UHC_ROLE_POSTAL_PARTITIONS",
        "off",
    )
    assert fhir._uhc_partition_residual_error(source_config, "PractitionerRole") is None
    assert fhir._uhc_partition_checkpoint_type(source_config, "PractitionerRole") is None
    assert (
        fhir._uhc_partition_identity(
            source_config,
            "PractitionerRole",
            (
                f"{fhir.UHC_PROVIDER_DIRECTORY_BASE}/PractitionerRole?"
                "location.address-postalcode=606"
            ),
        )
        is None
    )
    assert fhir._uhc_partition_identity(source_config, "HealthcareService", base_url) is None
    assert (
        fhir._uhc_partition_identity(
            source_config,
            "Location",
            f"{fhir.UHC_PROVIDER_DIRECTORY_BASE}/Location?address-city=Chicago",
        )
        is None
    )
    assert fhir._uhc_role_postal_prefix(source_config, "Location", base_url) is None

    monkeypatch.setenv(
        "HLTHPRT_PROVIDER_DIRECTORY_UHC_ROLE_POSTAL_PARTITIONS",
        "1",
    )
    role_url = (
        f"{fhir.UHC_PROVIDER_DIRECTORY_BASE}/PractitionerRole?"
        "location.address-postalcode=606"
    )
    assert fhir._uhc_role_postal_prefix(source_config, "PractitionerRole", role_url) == "606"
    source_config["_partition_checkpoint_source_ids"] = "uhc-alias"
    assert fhir._uhc_role_postal_checkpoint_type(source_config).startswith(
        fhir.UHC_ROLE_POSTAL_CHECKPOINT_TYPE + ":"
    )


@pytest.mark.asyncio
async def test_uhc_partition_checkpoint_load_without_run_is_empty():
    assert await fhir._load_partition_checkpoints(
        _uhc_source(),
        "Practitioner",
        None,
    ) == set()


def test_uhc_discovery_helpers_reject_invalid_listing_edges():
    assert discovery._parse_uhc_blob_listing(None) == []
    assert discovery._parse_uhc_blob_listing(
        {"blobs": [None, {}, {"name": "index.json"}, {"downloadUrl": "x"}]}
    ) == []
    targets = [{"label": "A", "name": "a_index.json", "url": "https://a"}]
    assert discovery._uhc_blob_targets_matching_query(targets, None) == targets

    assert discovery._uhc_provider_mrf_collection_from_url("/api/files/ui/cs/x") == "cs"
    assert discovery._uhc_provider_mrf_collection_from_url("/api/stream/ui/other/x") == "ifp"
    assert discovery._uhc_provider_mrf_stream_url(None) is None
    assert discovery._uhc_provider_mrf_file_type("other") is None

    source_config_by_field = {"source_id": "uhc", "display_name": "UHC"}
    payload = {
        "providers": [
            {"name": "missing-url"},
            {"name": "one.json", "blobPath": "ui/ifp/providers/one.json"},
            {"name": "two.json", "blobPath": "ui/ifp/providers/two.json"},
        ]
    }
    crawl_targets = discovery._uhc_provider_mrf_targets_from_payload(
        source_config_by_field,
        payload,
        listing_url="https://providermrf.uhc.com/api/files/ui/ifp/",
        max_targets=1,
    )
    assert len(crawl_targets) == 1
    assert crawl_targets[0].url.endswith("one.json")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("resolver_type", "target_query", "message"),
    (
        ("uhc_provider_mrf_files", None, "no UHC provider MRF files"),
        ("uhc_blob_listing", None, "no UHC blob index links"),
        ("uhc_blob_listing", "missing payer", "matching target payer query"),
    ),
)
async def test_uhc_resolvers_report_empty_catalogs(
    monkeypatch,
    resolver_type,
    target_query,
    message,
):
    resolver_config_by_field = {
        "type": resolver_type,
        "path_templates": ["/listing"],
    }
    monkeypatch.setattr(
        discovery,
        "_platform_resolver_config",
        lambda _platform: resolver_config_by_field,
    )
    monkeypatch.setattr(
        discovery,
        "_source_target_payer_query",
        lambda _source: target_query,
    )

    async def empty_fetch(_url, **_kwargs):
        return {}

    monkeypatch.setattr(discovery, "_fetch_json", empty_fetch)
    with pytest.raises(ValueError, match=message):
        await discovery._crawl_targets_for_source(
            {"source_id": "uhc", "hosting_platform": "synthetic"},
            "https://providermrf.uhc.com/IFP",
            session=None,
        )


def test_ptg_artifact_lock_failure_releases_local_state(tmp_path, monkeypatch):
    lock = ptg_artifacts._PTG2FileLock(tmp_path / "lock")
    lock.release()

    def fail_flock(_descriptor, _operation):
        raise OSError("injected flock failure")

    monkeypatch.setattr(ptg_artifacts.fcntl, "flock", fail_flock)
    with pytest.raises(OSError, match="injected flock failure"):
        lock.acquire()
    assert lock._fd is None
    assert lock._thread_lock.acquire(blocking=False)
    lock._thread_lock.release()


def test_ptg_manifest_read_race_and_short_write_fail_closed(tmp_path, monkeypatch):
    store = ptg_artifacts.PTG2ArtifactStore(tmp_path / "store")
    store.manifest_path.write_text("{}\n", encoding="utf-8")
    real_open = builtins.open

    def missing_manifest(path, *args, **kwargs):
        if Path(path) == store.manifest_path:
            raise FileNotFoundError(store.manifest_path)
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", missing_manifest)
    assert store._manifest_entries() == []
    monkeypatch.setattr(builtins, "open", real_open)

    store.manifest_path.write_bytes(b"")
    monkeypatch.setattr(ptg_artifacts.os, "write", lambda _fd, _data: 0)
    with pytest.raises(OSError, match="short write"):
        store.record_manifest({"artifact_kind": "raw"})


def test_ptg_reuse_policy_covers_metadata_and_missing_file_edges(tmp_path):
    digest = "a" * 64
    candidate_metadata_by_field = {
        "raw_sha256": digest,
        "content_length": 8,
        "etag": '"strong"',
        "last_modified": "today",
    }
    strong_head = PTG2HeadMetadata(
        url="https://example.test/raw",
        content_length=8,
        etag='"strong"',
        last_modified="today",
    )
    assert ptg_artifacts.choose_reusable_raw_artifact(
        [candidate_metadata_by_field], strong_head
    ) == (
        candidate_metadata_by_field,
        "strong_etag_length",
    )
    metadata_head = PTG2HeadMetadata(
        url="https://example.test/raw",
        content_length=8,
        last_modified="today",
    )
    assert ptg_artifacts.choose_reusable_raw_artifact(
        [candidate_metadata_by_field], metadata_head
    ) == (
        candidate_metadata_by_field,
        "length_last_modified",
    )
    store = ptg_artifacts.PTG2ArtifactStore(tmp_path / "reuse")
    assert ptg_artifacts.choose_reusable_raw_artifact(
        [candidate_metadata_by_field],
        strong_head,
        store,
    ) == (None, None)


def _gc_result(root: Path, *, target_bytes: int | None = None):
    return retention.PTG2InputArtifactGCResult(
        executed=True,
        root=root,
        active_lease_ids=("lease",),
        stale_lease_files=(root / "stale",),
        protected_files=(root / "protected",),
        newly_unleased_files=(root / "new",),
        eligible_files=(root / "eligible",),
        selected_files=(root / "selected",),
        deleted_files=(root / "deleted",),
        total_bytes_before=10,
        total_bytes_after=8,
        selected_bytes=2,
        deleted_bytes=2,
        target_bytes=target_bytes,
        manifest_entries_before=3,
        manifest_entries_after=2,
        manifest_invalid_lines=1,
    )


def test_ptg_retention_validation_and_summary_edges(tmp_path, monkeypatch, capsys):
    monkeypatch.setenv("PTG_BAD_INT", "not-an-int")
    monkeypatch.setenv("PTG_BAD_FLOAT", "not-a-float")
    assert retention._env_int("PTG_BAD_INT", 7) == 7
    assert retention._env_float("PTG_BAD_FLOAT", 1.5) == 1.5
    with pytest.raises(ValueError, match="references must be lists"):
        retention._payload_reference_sets({"paths": "raw/x", "prefixes": []})

    store = ptg_artifacts.PTG2ArtifactStore(tmp_path / "retention")
    lease = retention.PTG2ArtifactLease(
        store=store,
        owner="released",
        heartbeat_seconds=0,
    )
    lease._released = True
    lease.heartbeat()
    unmanaged = store.root / "outside-kind" / "value"
    with pytest.raises(ValueError, match="not a managed"):
        retention._relative_artifact_path(store, unmanaged)
    with pytest.raises(retention.PTG2ArtifactLeaseLostError, match="marker was lost"):
        retention._updated_lease_payload_locked(
            store,
            "missing",
            relative_path="raw/x",
            prefix=False,
        )

    summary = _gc_result(store.root)
    assert summary.over_target_bytes == 0
    retention._print_retention_summary(summary)
    output = capsys.readouterr().out
    assert "cleanup_executed=true" in output
    assert "manifest_invalid_lines=1" in output


def test_ptg_retention_rejects_invalid_lease_expiry_and_references(tmp_path):
    store = ptg_artifacts.PTG2ArtifactStore(tmp_path / "leases")
    store.leases_dir.mkdir(parents=True)
    marker = store.leases_dir / "lease.json"
    now = datetime.datetime(2026, 7, 20, tzinfo=datetime.UTC)
    lease_fields_by_name = {
        "schema_version": retention.LEASE_SCHEMA_VERSION,
        "lease_id": "lease",
        "expires_at": "invalid",
        "paths": [],
        "prefixes": [],
    }
    marker.write_text(json.dumps(lease_fields_by_name), encoding="utf-8")
    with pytest.raises(RuntimeError, match="expiration"):
        retention._active_lease_references_locked(store, now=now)

    lease_fields_by_name["expires_at"] = "2026-07-21T00:00:00Z"
    lease_fields_by_name["paths"] = "raw/x"
    marker.write_text(json.dumps(lease_fields_by_name), encoding="utf-8")
    with pytest.raises(RuntimeError, match="references"):
        retention._active_lease_references_locked(store, now=now)


def test_ptg_retention_handles_metadata_races(tmp_path, monkeypatch):
    store = ptg_artifacts.PTG2ArtifactStore(tmp_path / "races")
    store.manifest_path.write_text("{}\n", encoding="utf-8")
    real_path_open = Path.open

    def missing_path_open(path, *args, **kwargs):
        if path == store.manifest_path:
            raise FileNotFoundError(path)
        return real_path_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", missing_path_open)
    assert retention._validate_manifest_locked(store) == 0
    assert retention._compact_manifest_locked(store) == (0, 0, 0)
    monkeypatch.setattr(Path, "open", real_path_open)

    temporary = store.root / ".manifest.jsonl.race.tmp"
    temporary.write_text("x", encoding="utf-8")
    real_unlink = Path.unlink

    def raced_unlink(path, *args, **kwargs):
        if path == temporary:
            raise FileNotFoundError(path)
        return real_unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", raced_unlink)
    retention._prune_atomic_metadata_temps_locked(store)


def test_ptg_retention_handles_delete_race(tmp_path, monkeypatch):
    store = ptg_artifacts.PTG2ArtifactStore(tmp_path / "delete-race")
    real_unlink = Path.unlink
    artifact = store.root / "raw" / "candidate"
    artifact.parent.mkdir(parents=True)
    artifact.write_bytes(b"retained")
    first_observation = datetime.datetime(2026, 7, 20, 12, 0, 0)
    first = retention.collect_ptg2_input_artifacts(
        root=store.root,
        execute=True,
        now=first_observation,
        retention_hours=0,
        min_age_hours=0,
        target_bytes=0,
        max_delete_bytes=None,
        max_delete_files=None,
    )
    assert artifact in first.newly_unleased_files

    def missing_artifact(path, *args, **kwargs):
        if path == artifact:
            raise FileNotFoundError(path)
        return real_unlink(path, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", missing_artifact)
    second = retention.collect_ptg2_input_artifacts(
        root=store.root,
        execute=True,
        now=first_observation + datetime.timedelta(hours=1),
        retention_hours=0,
        min_age_hours=0,
        target_bytes=0,
        max_delete_bytes=None,
        max_delete_files=None,
    )
    assert second.deleted_files == ()


def test_ptg_retention_cli_converts_unbounded_limits(tmp_path, monkeypatch, capsys):
    observed_options_by_name = {}
    retention_summary = _gc_result(tmp_path)

    def fake_collect(**options):
        observed_options_by_name.update(options)
        return retention_summary

    monkeypatch.setattr(retention, "collect_ptg2_input_artifacts", fake_collect)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "ptg-retention",
            "--root",
            str(tmp_path),
            "--execute",
            "--retention-hours",
            "2",
            "--min-age-hours",
            "1",
            "--target-bytes",
            "-1",
            "--max-delete-bytes",
            "-1",
            "--max-delete-files",
            "-1",
        ],
    )
    retention._run_cli()
    assert observed_options_by_name["execute"] is True
    assert observed_options_by_name["target_bytes"] is None
    assert observed_options_by_name["max_delete_bytes"] is None
    assert observed_options_by_name["max_delete_files"] is None
    assert "artifact_root=" in capsys.readouterr().out


def test_ptg_json_stream_missing_array_is_empty():
    assert list(
        json_streams._iter_top_level_objects_jsondecoder(
            io.BytesIO(b'{"unrelated": []}'),
            {"in_network": "in_network.item"},
            chunk_size=3,
        )
    ) == []
