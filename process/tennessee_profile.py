# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Acquire and publish the paired Tennessee physician reports as one managed source."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
from collections import Counter
from pathlib import Path

import click

from db.models import NPIData, NPIDataTaxonomy, NUCCTaxonomy, ProviderProfileArtifact, ProviderProfileFact, ProviderProfileSourceRecord, db
from process.control_cancel import raise_if_cancelled
from process.florida_mqa_profile import _upsert_rows
from process.kentucky_profile_acquisition import _reject_symlinks
from process.live_progress import enqueue_live_progress
from process.massachusetts_profile import _hash, _now
from process.massachusetts_profile_acquisition import encoded_json, write_new_json
from process.provider_profile_source_store import ensure_tables
from process import tennessee_profile_acquisition as acquisition
from process import tennessee_profile_binding as binding
from process.tennessee_profile_registry import capture_registry_snapshot
from process.tennessee_profile_rows import SOURCE_KEY, SCHEMA_VERSION
from process.tennessee_profile_store import IMPORTER, store, completion

logger = logging.getLogger(__name__)


def _parameters(task):
    if not isinstance(task.get("run_id"), str) or not task["run_id"].strip():
        raise ValueError("tennessee_profile_managed_run_required")
    if any(task.get(field) is not None for field in ("max_providers", "resume_from", "sources", "professions")):
        raise ValueError("tennessee_profile_complete_pair_required")


def _artifact_root():
    configured = os.environ.get("HLTHPRT_TN_TDH_ARTIFACT_ROOT", "/work/tennessee-tdh")
    if not configured.strip():
        raise ValueError("tennessee_profile_artifact_root_invalid")
    path = Path(configured).absolute()
    _reject_symlinks(path)
    return path


def _progress(task, phase, unit, done, total):
    enqueue_live_progress(
        run_id=task["run_id"], importer=IMPORTER, status="running", phase=phase, unit=unit,
        done=done, total=total, pct=100 * done / total if total else 0,
        message=f"Tennessee profiles: {phase} {done}/{total}",
    )


def _source_manifest(task, snapshot, expected):
    snapshot_sha256 = _hash(snapshot)
    return {"control_run_id": task["run_id"], "expected_current_run_id": expected,
            "max_providers": None, "resume_from": None, "categories": ["education", "training", "specialties"],
            "report_professions": sorted(binding.PROFESSIONS), "snapshot_sha256": snapshot_sha256,
            "snapshot_row_count": snapshot["row_count"], "source": {
                "source_key": SOURCE_KEY, "source_kind": "state_regulator", "jurisdiction": "TN",
                "agency": "Tennessee Department of Health", "source_url": acquisition.BASE_URL,
                "coverage_scope": "regular_md_do_all_ranks_statuses_locations", "registry_generation": snapshot_sha256}}


def _read_reports(directory, acquired, run_id):
    binding._require(acquired.get("complete") is True and set(acquired.get("reports", {})) == binding.PROFESSIONS
                     and len(acquired.get("responses", [])) == 8
                     and all(response.get("complete") is True and response.get("status") == 200
                             for response in acquired["responses"]), "acquisition_incomplete")
    reports, descriptors = {}, {}
    artifact_id = _hash([run_id, SOURCE_KEY])
    for label, _, profession, _ in acquisition.REPORTS:
        receipt = acquired["reports"][profession]
        path = directory / f"{label}-report.csv"
        _reject_symlinks(path)
        binding._require(receipt["filepath"] == str(path) and receipt["source_url"] == acquisition.REPORT_URL
                         and 0 < path.stat().st_size <= acquisition.MAX_REPORT_BYTES, "report_file_invalid")
        content = path.read_bytes()
        binding._require(len(content) == receipt["content_bytes"] <= acquisition.MAX_REPORT_BYTES
                         and hashlib.sha256(content).hexdigest() == receipt["content_sha256"], "report_changed")
        evidence_by_field = {field: receipt[field] for field in ("source_url", "downloaded_at", "content_sha256")}
        reports[profession] = {"content": content, "evidence": {**evidence_by_field, "run_id": run_id, "artifact_id": artifact_id}}
        descriptors[profession] = {**evidence_by_field, "file_name": path.name, "content_bytes": len(content)}
    return reports, descriptors


async def _bind_reports(reports, reports_sha256, directory, manifest, source_schema):
    # Parsing is CPU-bound. Keep the event loop responsive and drain cancellation
    # before the caller can persist or publish anything from this pure operation.
    parser = asyncio.create_task(asyncio.to_thread(
        binding.bind_reports, reports, reports_sha256=reports_sha256,
        snapshot_path=directory / "snapshot.json", snapshot_sha256=manifest["snapshot_sha256"], source_schema=source_schema,
    ))
    try:
        return await asyncio.shield(parser)
    except asyncio.CancelledError:
        while not parser.done():
            try:
                await asyncio.shield(parser)
            except asyncio.CancelledError:
                continue
            except Exception:
                break
        if not parser.cancelled():
            parser.exception()
        raise


def _artifact(run_by_field, directory, descriptors, acquired, bound):
    profession_by_record = {record["record_id"]: record["profession_code"] for record in bound["source_records"]}
    metrics_by_field = {"acquisition_complete": True, "transport_failures": 0,
               "http_responses": len(acquired["responses"]), "report_responses": len(descriptors),
               "response_bytes": sum(response["content_bytes"] for response in acquired["responses"]),
               "reports_sha256": bound["reports_sha256"], "snapshot_sha256": bound["snapshot_sha256"],
               "source_records_by_profession": dict(Counter(profession_by_record.values())),
               "facts_by_profession": {profession: 0 for profession in binding.PROFESSIONS}}
    metrics_by_field["facts_by_profession"].update(Counter(profession_by_record[fact["source_record_id"]] for fact in bound["facts"]))
    bundle_by_field = {"schema_version": SCHEMA_VERSION, "run_id": run_by_field["run_id"], "source_manifest": run_by_field["source_manifest"],
              "reports_sha256": bound["reports_sha256"], "reports": descriptors, "acquisition": metrics_by_field,
              "snapshot": {"content_sha256": bound["snapshot_sha256"], "file_name": "snapshot.json",
                           "content_bytes": (directory / "snapshot.json").stat().st_size}}
    write_new_json(directory / "manifest.json", bundle_by_field)
    return {"artifact_id": _hash([run_by_field["run_id"], SOURCE_KEY]), "run_id": run_by_field["run_id"],
            "source_key": SOURCE_KEY, "file_name": "manifest.json", "source_url": acquisition.BASE_URL,
            "category": "profile", "content_sha256": _hash(bundle_by_field), "content_bytes": len(encoded_json(bundle_by_field)),
            "header": None, "downloaded_at": _now(), "metadata_json": bundle_by_field}, metrics_by_field


async def _persist_profiles(ctx, task, artifact, bound):
    await raise_if_cancelled(ctx, task)
    await _upsert_rows(ProviderProfileArtifact, [artifact], "artifact_id")
    for model, rows, identity in ((ProviderProfileSourceRecord, bound["source_records"], "record_id"),
                                  (ProviderProfileFact, bound["facts"], "fact_id")):
        for offset in range(0, len(rows), 500):
            await raise_if_cancelled(ctx, task)
            async with db.transaction():
                await _upsert_rows(model, rows[offset:offset + 500], identity)
            _progress(task, "retaining " + model.__tablename__, "record", min(offset + 500, len(rows)), len(rows))


async def _run_claimed(ctx, task, run_by_field, snapshot, directory):
    await raise_if_cancelled(ctx, task)
    _reject_symlinks(directory)
    directory.mkdir(mode=0o700, exist_ok=False)
    write_new_json(directory / "snapshot.json", snapshot)
    completed_responses = [-1]

    async def progress(done, total):
        """Check cancellation on every chunk and emit progress once per response."""
        await raise_if_cancelled(ctx, task)
        if done != completed_responses[0]:
            _progress(task, "acquiring", "response", done, total)
            completed_responses[0] = done

    acquired = await acquisition.acquire_reports(directory, progress)
    reports, descriptors = _read_reports(directory, acquired, run_by_field["run_id"])
    await raise_if_cancelled(ctx, task)
    _progress(task, "matching", "report", 0, 2)
    bound = await _bind_reports(reports, binding.reports_content_sha256(reports), directory,
                                run_by_field["source_manifest"], snapshot["source_schema"])
    await raise_if_cancelled(ctx, task)
    artifact, metrics_by_field = _artifact(run_by_field, directory, descriptors, acquired, bound)
    await _persist_profiles(ctx, task, artifact, bound)
    await raise_if_cancelled(ctx, task)
    await store.update_run(run_by_field["run_id"], {"status": "validating", "metrics": metrics_by_field})
    return await completion.complete_run(ctx, task, run_by_field, metrics_by_field)


async def import_profiles(ctx, task):
    """Claim one source before HTTP or file creation; publish only the complete pair."""
    _parameters(task)
    await raise_if_cancelled(ctx, task)
    artifact_root = _artifact_root()
    await ensure_tables()
    await completion.reconcile_failed_control_runs()
    publication = await store.read_publication()
    expected = publication["current_run_id"] if publication else None

    async def progress(done, total):
        """Check cancellation and report the complete registry capture's progress."""
        await raise_if_cancelled(ctx, task)
        _progress(task, "registry snapshot", "occurrence", done, total)

    source_schemas = {model.__table__.schema or "mrf" for model in (NPIData, NPIDataTaxonomy, NUCCTaxonomy)}
    if len(source_schemas) != 1:
        raise ValueError("tennessee_profile_registry_schema_mismatch")
    snapshot = await capture_registry_snapshot(source_schemas.pop(), progress)
    run_id = _hash([SOURCE_KEY, task["run_id"]])
    run_by_field = {"run_id": run_id, "source_key": SOURCE_KEY, "jurisdiction": "TN", "schema_version": SCHEMA_VERSION,
               "status": "running", "source_manifest": _source_manifest(task, snapshot, expected),
               "metrics": {}, "error": None, "started_at": _now(), "finished_at": None}
    await raise_if_cancelled(ctx, task)
    await store.claim_run(run_by_field)
    try:
        artifact_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        completed = await _run_claimed(ctx, task, run_by_field, snapshot, artifact_root / run_id)
    except BaseException as exc:
        await store.mark_run_failed(run_id, exc)
        raise
    try:
        await store.retain_source_history(artifact_root)
    except Exception:
        logger.exception("Tennessee profile retention failed after completed import %s", run_id)
    return completed


@click.command(help="Submit the complete Tennessee MD and DO profile report pair through the managed import API.")
def tennessee_tdh_profile():
    """Reject unmanaged execution; the authenticated import API owns source runs."""
    raise click.UsageError("Tennessee profile imports require the managed import API.")
