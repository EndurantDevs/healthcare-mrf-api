# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Retain and publish the complete public active Rhode Island MD/DO profile cohort."""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import os
from pathlib import Path

import click

from db.models import (
    NPIData,
    NPIDataTaxonomy,
    NUCCTaxonomy,
    ProviderProfileArtifact,
    ProviderProfileFact,
    ProviderProfileSourceRecord,
    db,
)
from process import rhode_island_profile_acquisition as acquisition
from process import rhode_island_profile_registry as registry
from process.control_cancel import raise_if_cancelled
from process.florida_mqa_profile import _upsert_rows
from process.kentucky_profile_acquisition import _reject_symlinks
from process.live_progress import enqueue_live_progress
from process.massachusetts_profile import _hash, _now
from process.massachusetts_profile_acquisition import encoded_json, write_new_json
from process.provider_profile_source_store import ensure_tables
from process.rhode_island_profile_cohort import COVERAGE_SCOPE, acquire_rosters
from process.rhode_island_profile_roster import SOURCE_URL
from process.rhode_island_profile_rows import LICENSE_TYPES, PROFILE_FIELDS, SCHEMA_VERSION, SOURCE_KEY
from process.rhode_island_profile_store import CATEGORIES, IMPORTER, completion, store

logger = logging.getLogger(__name__)
MAX_CAPTURE_BYTES = 512 * 1024 * 1024


def _parameters(task):
    if not isinstance(task.get("run_id"), str) or not task["run_id"].strip():
        raise ValueError("rhode_island_profile_managed_run_required")
    if any(
        task.get(key) is not None for key in ("max_providers", "resume_from", "sources", "professions", "license_types")
    ):
        raise ValueError("rhode_island_profile_complete_pair_required")


def _artifact_root():
    configured = os.environ.get("HLTHPRT_RI_DOH_ARTIFACT_ROOT", "/work/rhode-island-doh")
    if not configured.strip():
        raise ValueError("rhode_island_profile_artifact_root_invalid")
    path = Path(configured).absolute()
    _reject_symlinks(path)
    return path


def _source_manifest(task, snapshot, expected):
    digest = _hash(snapshot)
    return {
        "control_run_id": task["run_id"],
        "expected_current_run_id": expected,
        "max_providers": None,
        "resume_from": None,
        "categories": list(CATEGORIES),
        "license_types": list(LICENSE_TYPES),
        "snapshot_sha256": digest,
        "snapshot_row_count": snapshot["row_count"],
        "source": {
            "source_key": SOURCE_KEY,
            "source_kind": "state_regulator",
            "jurisdiction": "RI",
            "agency": "Rhode Island Department of Health",
            "source_url": SOURCE_URL,
            "coverage_scope": COVERAGE_SCOPE,
            "registry_generation": digest,
        },
    }


def _progress(task, phase, done, total):
    enqueue_live_progress(
        run_id=task["run_id"],
        importer=IMPORTER,
        status="running",
        phase=phase,
        unit="license",
        done=done,
        total=total,
        pct=100 * done / total if total else 0,
        message=f"Rhode Island profiles: {phase} {done}/{total}",
    )


def _read_response(path, *, digest=None):
    _reject_symlinks(path)
    with path.open("rb") as stream:
        content = stream.read(2 * acquisition.MAX_RESPONSE_BYTES + 1)
    if len(content) > 2 * acquisition.MAX_RESPONSE_BYTES:
        raise ValueError("rhode_island_profile_receipt_too_large")
    if digest is not None and hashlib.sha256(content).hexdigest() != digest:
        raise ValueError("rhode_island_profile_receipt_changed")
    receipt = json.loads(content)
    body = base64.b64decode(receipt["body_base64"], validate=True)
    if (
        receipt.get("schema_version") != acquisition.RESPONSE_SCHEMA
        or receipt.get("source_key") != SOURCE_KEY
        or receipt.get("method") != "GET"
        or receipt.get("eof") is not True
        or receipt.get("error") is not None
        or receipt.get("truncated") is not False
        or receipt.get("received_bytes") != len(body)
        or receipt.get("retained_bytes") != len(body)
        or len(body) > acquisition.MAX_RESPONSE_BYTES
        or receipt.get("content_sha256") != hashlib.sha256(body).hexdigest()
    ):
        raise ValueError("rhode_island_profile_receipt_incomplete")
    acquisition._validate_response(body, receipt)
    return body, receipt, hashlib.sha256(content).hexdigest()


def _profile_capture(directory, license_number, expected=None):
    path = directory / "profiles" / license_number
    page, page_receipt, page_hash = _read_response(
        path / "page.json", digest=(expected or {}).get("page_receipt_sha256")
    )
    record_url = acquisition.verify_page(page, license_number)
    body, receipt, record_hash = _read_response(
        path / "record.json", digest=(expected or {}).get("record_receipt_sha256")
    )
    if (
        receipt["source_url"] != record_url
        or page_receipt["source_url"] != acquisition.PAGE_URL + "?license=" + license_number
    ):
        raise ValueError("rhode_island_profile_capture_identity_changed")
    capture_by_field = {key: receipt[key] for key in ("source_url", "downloaded_at", "content_sha256")}
    capture_by_field.update(
        record_receipt_sha256=record_hash,
        page_receipt_sha256=page_hash,
        schema_page={
            "artifact_file": "page.json",
            **{key: page_receipt[key] for key in ("source_url", "downloaded_at", "content_sha256")},
        },
    )
    if expected is not None and capture_by_field != {
        key: value for key, value in expected.items() if key != "roster_occurrences"
    }:
        raise ValueError("rhode_island_profile_capture_changed")
    return body, capture_by_field, len(body) + len(page)


async def _acquire(ctx, task, run_by_field, directory):
    async def progress():
        """Honor managed cancellation at every roster response checkpoint."""
        await raise_if_cancelled(ctx, task)

    artifact_id = _hash([run_by_field["run_id"], SOURCE_KEY])
    cohort = await acquire_rosters(directory, run_id=run_by_field["run_id"], artifact_id=artifact_id, progress=progress)
    write_new_json(directory / "cohort.json", cohort)
    (directory / "profiles").mkdir(mode=0o700)
    descriptors, response_bytes, fact_count = {}, 0, 0
    for index, root in enumerate(cohort["roots"], 1):
        await progress()
        license_number = root["license_number"]
        acquired_profile = await acquisition.acquire_profile(
            license_number, directory / "profiles" / license_number, run_id=run_by_field["run_id"]
        )
        _, capture_by_field, size = _profile_capture(directory, license_number)
        descriptors[license_number] = {**capture_by_field, "roster_occurrences": root["originals"]}
        response_bytes += size
        if response_bytes > MAX_CAPTURE_BYTES:
            raise ValueError("rhode_island_profile_capture_too_large")
        fact_count += len(acquired_profile["facts"])
        _progress(task, "acquiring", index, len(cohort["roots"]))
    return (
        cohort,
        descriptors,
        {
            "acquisition_complete": True,
            "transport_failures": 0,
            "responses": len(descriptors),
            "response_bytes": response_bytes,
            "facts": fact_count,
            "cohort_sha256": _hash(cohort),
            "md_source_records": cohort["rosters"]["MD"]["unique_licenses"],
            "do_source_records": cohort["rosters"]["DO"]["unique_licenses"],
        },
    )


def _artifact(run_by_field, cohort, profiles, metrics):
    bundle_by_field = {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_by_field["run_id"],
        "source_manifest": run_by_field["source_manifest"],
        "cohort": {"coverage_scope": COVERAGE_SCOPE, "content_sha256": _hash(cohort), "rosters": cohort["rosters"]},
        "profiles": profiles,
        "acquisition": metrics,
    }
    return {
        "artifact_id": _hash([run_by_field["run_id"], SOURCE_KEY]),
        "run_id": run_by_field["run_id"],
        "source_key": SOURCE_KEY,
        "file_name": "manifest.json",
        "source_url": SOURCE_URL,
        "category": "profile",
        "content_sha256": _hash(bundle_by_field),
        "content_bytes": len(encoded_json(bundle_by_field)),
        "header": None,
        "downloaded_at": _now(),
        "metadata_json": bundle_by_field,
    }


def _profile_inputs(cohort, profiles, directory, artifact):
    for index, root in enumerate(cohort["roots"], 1):
        license_number = root["license_number"]
        body, capture_by_field, _ = _profile_capture(directory, license_number, profiles[license_number])
        yield (
            license_number,
            body,
            {key: capture_by_field[key] for key in ("source_url", "downloaded_at", "content_sha256", "schema_page")}
            | {"run_id": artifact["run_id"], "artifact_id": artifact["artifact_id"], "row_number": index},
        )


def _retain_roster(record, root, artifact):
    profile_by_field = dict(zip(PROFILE_FIELDS, record["raw_payload"]["values"][: len(PROFILE_FIELDS)], strict=True))
    roster = root["originals"][0]["raw_payload"]
    for source, profile in (("First", "First_Name"), ("Middle", "Middle_Name"), ("Last", "Last_Name")):
        if " ".join(roster[source].split()).casefold() != " ".join(profile_by_field[profile].split()).casefold():
            raise ValueError("rhode_island_profile_roster_identity_changed")
    record["raw_payload"]["roster_occurrences"] = root["originals"]
    record["normalized_payload"].update(
        cohort_sha256=artifact["metadata_json"]["cohort"]["content_sha256"],
        profile_capture=artifact["metadata_json"]["profiles"][root["license_number"]],
    )


async def _persist(ctx, task, run_by_field, cohort, profiles, directory, artifact, source_schema):
    await _upsert_rows(ProviderProfileArtifact, [artifact], "artifact_id")
    bound = registry.bind_snapshot_profiles(
        _profile_inputs(cohort, profiles, directory, artifact),
        snapshot_path=directory / "snapshot.json",
        snapshot_sha256=run_by_field["source_manifest"]["snapshot_sha256"],
        source_schema=source_schema,
    )
    source_records, retained_facts = [], []
    for index, (root, (record, facts)) in enumerate(zip(cohort["roots"], bound, strict=True), 1):
        await raise_if_cancelled(ctx, task)
        _retain_roster(record, root, artifact)
        source_records.append(record)
        retained_facts.extend(facts)
        if len(source_records) == 250 or index == len(cohort["roots"]):
            async with db.transaction():
                await _upsert_rows(ProviderProfileSourceRecord, source_records, "record_id")
                await raise_if_cancelled(ctx, task)
                await _upsert_rows(ProviderProfileFact, retained_facts, "fact_id")
            source_records, retained_facts = [], []
            _progress(task, "retaining", index, len(cohort["roots"]))


async def _run_claimed(ctx, task, run_by_field, snapshot, directory):
    await raise_if_cancelled(ctx, task)
    _reject_symlinks(directory)
    directory.mkdir(mode=0o700, exist_ok=False)
    write_new_json(directory / "snapshot.json", snapshot)
    cohort, profiles, metrics = await _acquire(ctx, task, run_by_field, directory)
    artifact = _artifact(run_by_field, cohort, profiles, metrics)
    write_new_json(directory / "manifest.json", artifact["metadata_json"])
    await _persist(ctx, task, run_by_field, cohort, profiles, directory, artifact, snapshot["source_schema"])
    await raise_if_cancelled(ctx, task)
    await store.update_run(run_by_field["run_id"], {"status": "validating", "metrics": metrics})
    return await completion.complete_run(ctx, task, run_by_field, metrics)


async def import_profiles(ctx, task):
    """Claim the source before HTTP; a failed or canceled cohort never changes its pointer."""
    _parameters(task)
    await raise_if_cancelled(ctx, task)
    artifact_root = _artifact_root()
    await ensure_tables()
    await completion.reconcile_failed_control_runs()
    publication = await store.read_publication()
    schemas = {model.__table__.schema or "mrf" for model in (NPIData, NPIDataTaxonomy, NUCCTaxonomy)}
    if len(schemas) != 1:
        raise ValueError("rhode_island_profile_registry_schema_mismatch")

    async def progress(done, total):
        """Keep the registry snapshot capture responsive to cancellation."""
        await raise_if_cancelled(ctx, task)
        _progress(task, "registry snapshot", done, total)

    snapshot = await registry.capture_registry_snapshot(schemas.pop(), progress)
    run_id = _hash([SOURCE_KEY, task["run_id"]])
    run_by_field = {
        "run_id": run_id,
        "source_key": SOURCE_KEY,
        "jurisdiction": "RI",
        "schema_version": SCHEMA_VERSION,
        "status": "running",
        "source_manifest": _source_manifest(task, snapshot, publication["current_run_id"] if publication else None),
        "metrics": {},
        "error": None,
        "started_at": _now(),
        "finished_at": None,
    }
    await raise_if_cancelled(ctx, task)
    await store.claim_run(run_by_field)
    try:
        artifact_root.mkdir(mode=0o700, parents=True, exist_ok=True)
        completed = await _run_claimed(ctx, task, run_by_field, snapshot, artifact_root / run_id)
    except BaseException as error:
        await store.mark_run_failed(run_id, error)
        raise
    try:
        await store.retain_source_history(artifact_root)
    except Exception:
        logger.exception("Rhode Island profile retention failed after completed import %s", run_id)
    return completed


@click.command(help="Submit the complete active Rhode Island MD and DO profile cohort through the managed import API.")
def rhode_island_doh_profile():
    """Reject unmanaged execution; source runs require a managed control attempt."""
    raise click.UsageError("Rhode Island profile imports require the managed import API.")
