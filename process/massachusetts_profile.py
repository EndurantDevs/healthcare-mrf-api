# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Import public Massachusetts school and training facts as a managed job."""

from __future__ import annotations

import hashlib
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import click

from db.models import ProviderProfileArtifact, ProviderProfileSourceRecord, ProviderProfileFact, db
from process.control_cancel import raise_if_cancelled
from process.florida_mqa_profile import _upsert_rows
from process.live_progress import enqueue_live_progress
from process.massachusetts_profile_completion import complete_run, reconcile_failed_control_runs
from process import massachusetts_profile_acquisition as acquisition
from process import massachusetts_profile_store as store
from process.massachusetts_profile_rows import SCHEMA_VERSION, SOURCE_KEY, parse_profile

logger = logging.getLogger(__name__)


def _hash(value):
    return hashlib.sha256(acquisition.encoded_json(value)).hexdigest()


def _now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _parameters(task):
    limit = task.get("max_providers")
    if limit is not None and (type(limit) is not int or limit <= 0):
        raise ValueError("massachusetts_profile_limit_invalid")
    resume_from = task.get("resume_from")
    if resume_from is not None and (
        not isinstance(resume_from, str) or not store.RUN_ID_PATTERN.fullmatch(resume_from)
    ):
        raise ValueError("massachusetts_profile_resume_id_invalid")
    return limit, resume_from


def _artifact_root():
    return Path(os.environ.get("HLTHPRT_MA_BORIM_ARTIFACT_ROOT", "/work/massachusetts-borim")).resolve()


def _retained_directory(artifact_root, run_id):
    directory = artifact_root / run_id
    if directory.is_symlink() or (directory / "profiles").is_symlink():
        raise ValueError("massachusetts_profile_retained_directory_invalid")
    return directory


async def _cohort_for_run(task, artifact_root, expected_current_run_id):
    limit, resume_from = _parameters(task)
    if resume_from is None:
        schema = ProviderProfileSourceRecord.__table__.schema or "mrf"
        return await acquisition.capture_registry_cohort(schema), None
    previous_run = await store.read_resume_run(
        resume_from, max_providers=limit, expected_current_run_id=expected_current_run_id,
    )
    directory = _retained_directory(artifact_root, resume_from)
    cohort = acquisition.read_cohort(directory / "cohort.json")
    if _hash(cohort) != previous_run["source_manifest"]["cohort_sha256"]:
        raise ValueError("massachusetts_profile_resume_cohort_changed")
    return cohort, directory / "profiles"


def _source_manifest(task, cohort, expected_current_run_id):
    limit, resume_from = _parameters(task)
    count = len(cohort["roots"])
    return {
        "max_providers": limit, "resume_from": resume_from,
        "expected_current_run_id": expected_current_run_id,
        "full_cohort_licenses": count, "requested_licenses": min(limit, count) if limit is not None else count,
        "cohort_sha256": _hash(cohort), "control_run_id": task.get("run_id"),
        "categories": ["education", "training"],
        "source": {
            "source_key": SOURCE_KEY, "source_kind": "state_regulator", "jurisdiction": "MA",
            "agency": "Massachusetts Board of Registration in Medicine",
            "source_url": acquisition.API_BASE,
            "coverage_scope": cohort["coverage_scope"], "registry_generation": cohort["registry_generation"],
        },
    }


def _selected_roots(cohort, limit):
    # A stable hash sample avoids testing only the oldest lexical license numbers.
    roots = cohort["roots"]
    if limit is None:
        return roots
    return sorted(roots, key=lambda root: _hash(root["license_number"]))[:limit]


def _progress(task, run_id, phase, done, total):
    enqueue_live_progress(
        run_id=task.get("run_id"), importer="massachusetts-borim-profile", status="running",
        phase=phase, unit="license", done=done, total=total,
        pct=100 * done / total if total else 0,
        message=f"Massachusetts profiles: {phase} {done}/{total}",
        metrics={"provider_profile_run_id": run_id, "coverage_scope": "nppes_ma_numeric_physician_license_cohort"},
    )


async def _acquire(ctx, task, run_row, cohort, directory, retained):
    roots = _selected_roots(cohort, run_row["source_manifest"]["max_providers"])

    async def progress(done, total):
        """Check cancellation for every root and throttle visible updates."""
        await raise_if_cancelled(ctx, task)
        if done % 50 == 0 or done == total:
            _progress(task, run_row["run_id"], "acquiring", done, total)

    metrics = await acquisition.acquire_profiles(roots, directory / "profiles", progress, retained=retained)
    return roots, {**metrics, "acquisition_complete": True, "transport_failures": 0}


def _artifact(run_row, directory, metrics):
    manifest_by_field = {"schema_version": SCHEMA_VERSION, "run_id": run_row["run_id"],
                "source_manifest": run_row["source_manifest"], "acquisition": metrics}
    acquisition.write_new_json(directory / "manifest.json", manifest_by_field)
    return {
        "artifact_id": _hash([run_row["run_id"], SOURCE_KEY]), "run_id": run_row["run_id"],
        "source_key": SOURCE_KEY, "file_name": "manifest.json", "source_url": acquisition.API_BASE,
        "category": "profile", "content_sha256": _hash(manifest_by_field),
        "content_bytes": len(acquisition.encoded_json(manifest_by_field)), "header": None,
        "downloaded_at": _now(), "metadata_json": {"cohort_sha256": run_row["source_manifest"]["cohort_sha256"], **metrics},
    }


def _source_rows(root, response, artifact, row_number):
    evidence_by_field = {key: response[key] for key in ("source_url", "downloaded_at", "content_sha256")}
    evidence_by_field.update(run_id=artifact["run_id"], artifact_id=artifact["artifact_id"], row_number=row_number)
    profile = acquisition.decoded_profile(response)
    if profile is not None:
        return parse_profile(profile, license_number=root["license_number"], candidates=root["candidates"], evidence=evidence_by_field)
    record_key = f"{SOURCE_KEY}:{root['license_number']}"
    return {
        "record_id": _hash([artifact["run_id"], record_key]), "run_id": artifact["run_id"],
        "artifact_id": artifact["artifact_id"], "source_key": SOURCE_KEY, "source_record_key": record_key,
        "profession_code": None, "license_id": None, "license_number": root["license_number"],
        "raw_payload": {}, "normalized_payload": {"schema_version": SCHEMA_VERSION, "visibility": "not_found"},
        "matched_npi": None, "match_status": "not_found", "match_evidence": evidence_by_field, "row_number": row_number,
    }, []


async def _persist_profiles(ctx, task, roots, directory, artifact):
    await _upsert_rows(ProviderProfileArtifact, [artifact], "artifact_id")
    for offset in range(0, len(roots), 250):
        await raise_if_cancelled(ctx, task)
        source_records, facts = [], []
        for index, root in enumerate(roots[offset:offset + 250], offset + 1):
            response = acquisition.read_response(directory / "profiles" / f"{root['license_number']}.json", root["license_number"])
            record, parsed_facts = _source_rows(root, response, artifact, index)
            source_records.append(record)
            facts.extend(parsed_facts)
        async with db.transaction():
            await _upsert_rows(ProviderProfileSourceRecord, source_records, "record_id")
            await _upsert_rows(ProviderProfileFact, facts, "fact_id")
        _progress(task, artifact["run_id"], "retaining", min(offset + 250, len(roots)), len(roots))


async def _finish_run(ctx, task, run_row, metrics):
    await raise_if_cancelled(ctx, task)
    await store.update_run(run_row["run_id"], {"status": "validating", "metrics": metrics})
    return await complete_run(ctx, task, run_row, metrics)


async def _run_claimed(ctx, task, run_row, cohort, retained, directory):
    directory.mkdir(exist_ok=False)
    acquisition.write_new_json(directory / "cohort.json", cohort)
    roots, metrics = await _acquire(ctx, task, run_row, cohort, directory, retained)
    await store.update_run(run_row["run_id"], {"metrics": metrics})
    artifact = _artifact(run_row, directory, metrics)
    await _persist_profiles(ctx, task, roots, directory, artifact)
    return await _finish_run(ctx, task, run_row, metrics)


async def process_data(ctx, task):
    """Acquire every eligible root; bounded tests cannot advance public data."""
    _parameters(task)
    control_run_id = task.get("run_id")
    if not isinstance(control_run_id, str) or not control_run_id.strip():
        raise ValueError("massachusetts_profile_managed_run_required")
    await raise_if_cancelled(ctx, task)
    await store.ensure_tables()
    await reconcile_failed_control_runs()
    publication = await store.read_publication()
    expected = publication["current_run_id"] if publication else None
    artifact_root = _artifact_root()
    cohort, retained = await _cohort_for_run(task, artifact_root, expected)
    run_id = _hash([SOURCE_KEY, control_run_id])
    run_by_field = {
        "run_id": run_id, "source_key": SOURCE_KEY, "jurisdiction": "MA", "schema_version": SCHEMA_VERSION,
        "status": "running", "source_manifest": _source_manifest(task, cohort, expected),
        "metrics": {}, "error": None, "started_at": _now(), "finished_at": None,
    }
    await store.claim_run(run_by_field)
    try:
        artifact_root.mkdir(parents=True, exist_ok=True)
        result = await _run_claimed(ctx, task, run_by_field, cohort, retained, artifact_root / run_id)
    except BaseException as exc:
        await store.mark_run_failed(run_id, exc)
        raise
    try:
        await store.retain_source_history(artifact_root)
    except Exception:
        logger.exception("Massachusetts profile retention failed after completed import %s", run_id)
    return result


@click.command(help="Submit Massachusetts BORIM school and training imports through the managed import API.")
@click.option("--max-providers", type=click.IntRange(min=1), default=None, help="Bounded acquisition without publication.")
@click.option("--resume-from", default=None, help="Replay a recent failed run's frozen cohort and verified responses.")
def massachusetts_borim_profile(max_providers, resume_from):
    """Retain importer registry metadata while refusing unmanaged execution."""
    raise click.UsageError("Massachusetts profile imports require the managed import API.")
