# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Retain and publish every supported NYPP license root with pinned source evidence."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
import shutil
import tempfile
import time
from pathlib import Path

import click

from db.models import ProviderProfileArtifact, ProviderProfileFact, ProviderProfileSourceRecord, db
from process import new_york_nysed_profile as nysed
from process import new_york_nysed_profile_acquisition as nysed_acquisition
from process import new_york_profile_acquisition as acquisition
from process import new_york_profile_registry as registry
from process.control_cancel import raise_if_cancelled
from process.florida_mqa_profile import _upsert_rows
from process.kentucky_profile_acquisition import _read_artifact, _reject_symlinks
from process.live_progress import enqueue_live_progress
from process.massachusetts_profile_acquisition import encoded_json
from process.new_york_profile_binding import ACQUISITION_FILES, MAX_SNAPSHOT_BYTES, RegistrySnapshot
from process.new_york_profile_retained import HELD_FILES, read_held_acquisition
from process.new_york_profile_store import (
    CATEGORIES,
    COVERAGE_SCOPE,
    IMPORTER,
    SCHEMA_VERSION,
    SOURCE_KEY,
    SOURCE_URL,
    completion,
    prepare_profile,
    store,
)
from process.provider_profile_source_store import _now, ensure_tables

logger = logging.getLogger(__name__)
NYSED_FILES = {
    "manifest.json": nysed.MAX_METADATA_BYTES,
    "request.json": nysed.MAX_METADATA_BYTES,
    "response.json": nysed.MAX_RESPONSE_BYTES * 2,
    "result.json": nysed.MAX_METADATA_BYTES,
}
DISK_RESERVE_BYTES = 64 * 1024 * 1024
BATCH_SIZE = 250


def _require(condition, reason):
    if not condition:
        raise ValueError("new_york_profile_" + reason)


def _hash(content):
    return hashlib.sha256(encoded_json(content)).hexdigest()


def _parameters(task):
    _require(isinstance(task.get("run_id"), str) and task["run_id"].strip(), "managed_run_required")
    _require(
        all(
            task.get(key) is None for key in ("max_providers", "resume_from", "sources", "professions", "license_types")
        ),
        "complete_cohort_required",
    )
    api_key = os.getenv("HLTHPRT_NYSED_PUBLIC_API_KEY", "")
    _require(isinstance(api_key, str) and re.fullmatch(r"[!-~]{1,512}", api_key), "public_header_invalid")
    _require(api_key not in task["run_id"], "public_header_overlaps_identity")
    configured = os.getenv("HLTHPRT_NYPP_ARTIFACT_ROOT", "/work/new-york-nypp")
    _require(configured.strip() and api_key not in configured, "artifact_root_invalid")
    root = Path(configured).absolute()
    _reject_symlinks(root)
    limits_by_field = {}
    for field, environment in (
        ("max_bytes", "HLTHPRT_NYPP_MAX_RETAINED_BYTES"),
        ("max_bundle_bytes", "HLTHPRT_NYPP_MAX_BUNDLE_BYTES"),
        ("seconds", "HLTHPRT_NYPP_DEADLINE_SECONDS"),
    ):
        configured_limit = os.getenv(environment, "")
        _require(re.fullmatch(r"[1-9][0-9]{0,14}", configured_limit), "resource_budget_required")
        limits_by_field[field] = int(configured_limit)
    return (
        root,
        api_key,
        {
            "path": root,
            "max_bytes": limits_by_field["max_bytes"],
            "max_bundle_bytes": limits_by_field["max_bundle_bytes"],
            "retained_bytes": 0,
            "deadline": time.monotonic() + limits_by_field["seconds"],
        },
    )


def _check_resources(budget, needed_bytes=0, needed_inodes=0):
    _require(time.monotonic() < budget["deadline"], "deadline_exceeded")
    _require(budget["retained_bytes"] + needed_bytes <= budget["max_bytes"], "retained_byte_budget_exceeded")
    space = os.statvfs(budget["path"])
    _require(space.f_bavail * space.f_frsize >= DISK_RESERVE_BYTES + needed_bytes, "disk_reserve_exhausted")
    _require(not space.f_files or space.f_favail >= needed_inodes + 32, "inode_reserve_exhausted")


async def _checkpoint(ctx, task, budget):
    await raise_if_cancelled(ctx, task)
    _check_resources(budget)


def _json_chunks(content, *, max_bytes=None):
    pending, size = bytearray(), 0
    for part in json.JSONEncoder(sort_keys=True, ensure_ascii=False, allow_nan=False).iterencode(content):
        encoded = part.encode("utf-8")
        size += len(encoded)
        _require(max_bytes is None or size <= max_bytes, "bundle_byte_budget_exceeded")
        pending.extend(encoded)
        if len(pending) >= 1024 * 1024:
            yield pending
            pending = bytearray()
    if pending:
        yield pending


def _json_size(content, limit):
    return sum(len(chunk) for chunk in _json_chunks(content, max_bytes=limit))


def _write_json(path, content, budget, *, max_bytes=None):
    """Stream canonical JSON once, retaining only a bounded encoding buffer."""
    temporary_path, is_linked, size = None, False, 0
    digest = hashlib.sha256()
    try:
        _check_resources(budget, needed_inodes=2)
        with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as temporary:
            temporary_path = Path(temporary.name)
            for chunk in _json_chunks(content, max_bytes=max_bytes):
                _check_resources(budget, len(chunk), 2)
                temporary.write(chunk)
                digest.update(chunk)
                size += len(chunk)
                budget["retained_bytes"] += len(chunk)
            temporary.flush()
            os.fsync(temporary.fileno())
        os.link(temporary_path, path)
        is_linked = True
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
        _check_resources(budget)
        return digest.hexdigest(), size
    finally:
        if temporary_path is not None:
            temporary_path.unlink(missing_ok=True)
        if not is_linked:
            budget["retained_bytes"] -= size


def _capture_inventory(directory, limits):
    _reject_symlinks(directory)
    _require({path.name for path in directory.iterdir()} == set(limits), "capture_inventory_changed")
    hashes_by_name = {}
    for name, limit in limits.items():
        path = directory / name
        _reject_symlinks(path)
        _require(path.is_file() and 0 < path.stat().st_size <= limit, "capture_file_invalid")
        with path.open("rb") as stream:
            hashes_by_name[name] = hashlib.file_digest(stream, "sha256").hexdigest()
    return hashes_by_name


def _account_capture(directory, budget):
    if directory.exists():
        budget["retained_bytes"] += sum(path.stat().st_size for path in directory.iterdir() if path.is_file())


def _progress(task, phase, done, total):
    enqueue_live_progress(
        run_id=task["run_id"],
        importer=IMPORTER,
        status="running",
        phase=phase,
        unit="record" if phase == "registry snapshot" else "license",
        done=done,
        total=total,
        pct=100 * done / total if total else 0,
        message=f"New York profiles: {phase} {done}/{total}",
    )


async def _capture_inputs(ctx, task, directory, budget):
    async def progress(done, total):
        """Check cancellation and budgets at each registry capture checkpoint."""
        await _checkpoint(ctx, task, budget)
        _progress(task, "registry snapshot", done, total)

    _check_resources(budget, MAX_SNAPSHOT_BYTES, 2)
    snapshot = await registry.capture_registry_snapshot(progress)
    snapshot_pin, _ = _write_json(directory / "snapshot.json", snapshot, budget)
    row_count = snapshot["row_count"]
    del snapshot
    cohort = registry.build_acquisition_cohort(directory / "snapshot.json", snapshot_sha256=snapshot_pin)
    _write_json(directory / "cohort.json", cohort, budget)
    await _checkpoint(ctx, task, budget)
    return snapshot_pin, row_count, cohort


def _run_row(task, snapshot_pin, row_count, cohort, previous):
    manifest_by_field = {
        "control_run_id": task["run_id"],
        "expected_current_run_id": previous,
        "max_providers": None,
        "resume_from": None,
        "categories": list(CATEGORIES),
        "snapshot_sha256": snapshot_pin,
        "snapshot_row_count": row_count,
        "cohort_sha256": _hash(cohort),
        "full_cohort_licenses": len(cohort["roots"]),
        "requested_licenses": len(cohort["roots"]),
        "source": {
            "source_key": SOURCE_KEY,
            "source_kind": "state_regulator",
            "jurisdiction": "NY",
            "agency": "New York State Department of Health",
            "source_url": SOURCE_URL,
            "coverage_scope": COVERAGE_SCOPE,
            "registry_generation": snapshot_pin,
        },
    }
    return {
        "run_id": _hash([SOURCE_KEY, task["run_id"]]),
        "source_key": SOURCE_KEY,
        "jurisdiction": "NY",
        "schema_version": SCHEMA_VERSION,
        "status": "running",
        "source_manifest": manifest_by_field,
        "metrics": {},
        "error": None,
        "started_at": _now(),
        "finished_at": None,
    }


async def _acquire_support(ctx, task, license_number, directory, run_id, api_key, budget):
    await _checkpoint(ctx, task, budget)
    _check_resources(budget, sum(NYSED_FILES.values()), len(NYSED_FILES) + 2)
    try:
        await nysed_acquisition.acquire_license(license_number, directory, run_id=run_id, api_key=api_key)
    finally:
        _account_capture(directory, budget)
    _check_resources(budget)
    hashes_by_name = _capture_inventory(directory, NYSED_FILES)
    receipt = _read_artifact(directory / "result.json", nysed.MAX_METADATA_BYTES)
    receipt_pin = _hash(receipt)
    reader = nysed.read_held_acquisition if receipt.get("outcome") == "held" else nysed.read_acquisition
    replayed = reader(directory, receipt_sha256=receipt_pin)
    manifest_by_field = _read_artifact(directory / "manifest.json", nysed.MAX_METADATA_BYTES)
    _require(
        manifest_by_field["run_id"] == run_id and manifest_by_field["license_number"] == license_number,
        "support_identity_changed",
    )
    identity_by_field = None
    if replayed["source_record"] is not None:
        source_record = replayed["source_record"]
        evidence = replayed["facts"][0]["source_json"]
        identity_by_field = {
            "source_record_id": source_record["record_id"],
            "artifact_id": source_record["artifact_id"],
            **{key: evidence[key] for key in ("source_key", "source_url", "content_sha256", "downloaded_at")},
            "profession_code": source_record["profession_code"],
            "license_number": source_record["license_number"],
            "legal_name": source_record["raw_payload"]["name"]["value"],
        }
    await _checkpoint(ctx, task, budget)
    return {
        "capture_manifest": manifest_by_field,
        "file_sha256": hashes_by_name,
        "receipt": receipt,
        "receipt_sha256": receipt_pin,
        "source_identity": identity_by_field,
    }


async def _acquire_profile(ctx, task, root, directory, run_id, loaded, api_key, budget):
    await _checkpoint(ctx, task, budget)
    license_number = root["license_number"]
    destination = directory / "profiles" / license_number
    _check_resources(budget, sum(limit for _, limit in ACQUISITION_FILES), len(ACQUISITION_FILES) + 2)
    try:
        acquired = await acquisition.acquire_license(license_number, destination, run_id=run_id)
    finally:
        _account_capture(destination, budget)
    _check_resources(budget)
    limits = HELD_FILES if acquired["outcome"] == "held" else dict(ACQUISITION_FILES)
    hashes_by_name = _capture_inventory(destination, limits)
    manifest_by_field = _read_artifact(destination / "manifest.json", HELD_FILES["manifest.json"])
    binding_options_by_field = {
        "manifest_sha256": hashes_by_name["manifest.json"],
        "acquisition_sha256": _hash(hashes_by_name),
    }
    support = None
    if acquired["outcome"] == "held":
        bound = read_held_acquisition(destination, **binding_options_by_field)
    else:
        support_path = directory / "nysed" / license_number
        support = await _acquire_support(ctx, task, license_number, support_path, run_id, api_key, budget)
        if support["source_identity"] is None:
            bound = loaded.bind_retained_acquisition(destination, **binding_options_by_field)
        else:
            bound = loaded.bind_corroborated_acquisition(
                destination,
                **binding_options_by_field,
                nysed_destination=support_path,
                nysed_receipt_sha256=support["receipt_sha256"],
            )
    await _checkpoint(ctx, task, budget)
    prepared = prepare_profile(run_id, root, bound, capture_manifest=manifest_by_field, file_sha256=hashes_by_name)
    return prepared, support


async def _persist_batch(ctx, task, source_records, facts, budget):
    await _checkpoint(ctx, task, budget)
    async with db.transaction():
        for model, rows, identifier in (
            (ProviderProfileSourceRecord, source_records, "record_id"),
            (ProviderProfileFact, facts, "fact_id"),
        ):
            for offset in range(0, len(rows), BATCH_SIZE):
                await _checkpoint(ctx, task, budget)
                await _upsert_rows(model, rows[offset : offset + BATCH_SIZE], identifier)


def _artifact(run, cohort, profiles, metrics_by_field, directory, budget):
    bundle_by_field = {
        "schema_version": SCHEMA_VERSION,
        "run_id": run["run_id"],
        "source_manifest": run["source_manifest"],
        "cohort": cohort,
        "profiles": profiles,
        "acquisition": metrics_by_field,
    }
    content_pin, content_bytes = _write_json(
        directory / "manifest.json", bundle_by_field, budget, max_bytes=budget["max_bundle_bytes"]
    )
    _require(content_bytes == budget["bundle_bytes"], "bundle_byte_accounting_changed")
    return {
        "artifact_id": _hash([run["run_id"], SOURCE_KEY]),
        "run_id": run["run_id"],
        "source_key": SOURCE_KEY,
        "file_name": "manifest.json",
        "source_url": SOURCE_URL,
        "category": "profile",
        "content_sha256": content_pin,
        "content_bytes": content_bytes,
        "header": None,
        "downloaded_at": _now(),
        "metadata_json": bundle_by_field,
    }


def _aggregate_metrics(metrics_by_field):
    return {key: value for key, value in metrics_by_field.items() if key != "nysed_support"}


def _append_profile(profiles, metrics_by_field, license_number, descriptor, support, budget):
    counts_by_field = _aggregate_metrics(metrics_by_field)
    counts_by_field["responses"] += 1
    counts_by_field["acquired_profiles" if descriptor["acquisition_outcome"] == "acquired" else "held_attempts"] += 1
    counts_by_field["facts"] += len(descriptor["facts"])
    growth = len(encoded_json(counts_by_field)) - len(encoded_json(_aggregate_metrics(metrics_by_field)))
    growth += _json_size({license_number: descriptor}, budget["max_bundle_bytes"]) - 2 + (2 if profiles else 0)
    if support is not None:
        growth += _json_size({license_number: support}, budget["max_bundle_bytes"]) - 2
        growth += 2 if metrics_by_field["nysed_support"] else 0
    _require(budget["bundle_bytes"] + growth <= budget["max_bundle_bytes"], "bundle_byte_budget_exceeded")
    budget["bundle_bytes"] += growth
    profiles[license_number] = descriptor
    if support is not None:
        metrics_by_field["nysed_support"][license_number] = support
    metrics_by_field.update(counts_by_field)


async def _run_claimed(ctx, task, run, cohort, directory, api_key, budget):
    (directory / "profiles").mkdir(mode=0o700)
    (directory / "nysed").mkdir(mode=0o700)
    loaded = RegistrySnapshot(directory / "snapshot.json", snapshot_sha256=run["source_manifest"]["snapshot_sha256"])
    profiles, source_records, facts = {}, [], []
    metrics_by_field = {
        "acquisition_complete": False,
        "transport_failures": 0,
        "responses": 0,
        "acquired_profiles": 0,
        "held_attempts": 0,
        "facts": 0,
        "cohort_sha256": run["source_manifest"]["cohort_sha256"],
        "nysed_support": {},
    }
    budget["bundle_bytes"] = _json_size(
        {
            "schema_version": SCHEMA_VERSION,
            "run_id": run["run_id"],
            "source_manifest": run["source_manifest"],
            "cohort": cohort,
            "profiles": profiles,
            "acquisition": metrics_by_field,
        },
        budget["max_bundle_bytes"],
    )
    for index, root in enumerate(cohort["roots"], 1):
        (descriptor, source_record, captured_facts), support = await _acquire_profile(
            ctx, task, root, directory, run["run_id"], loaded, api_key, budget
        )
        _append_profile(profiles, metrics_by_field, root["license_number"], descriptor, support, budget)
        if source_record is not None:
            source_records.append(source_record)
            facts.extend(captured_facts)
        if len(source_records) >= BATCH_SIZE or len(facts) >= BATCH_SIZE or index == len(cohort["roots"]):
            await _persist_batch(ctx, task, source_records, facts, budget)
            source_records, facts = [], []
        _progress(task, "retaining", index, len(cohort["roots"]))
    del loaded
    metrics_by_field["acquisition_complete"] = True
    budget["bundle_bytes"] -= 1  # Canonical JSON changes false to true.
    artifact = _artifact(run, cohort, profiles, metrics_by_field, directory, budget)
    await _checkpoint(ctx, task, budget)
    await _upsert_rows(ProviderProfileArtifact, [artifact], "artifact_id")
    await store.update_run(run["run_id"], {"status": "validating", "metrics": _aggregate_metrics(metrics_by_field)})
    await _checkpoint(ctx, task, budget)
    return await completion.complete_run(ctx, task, run, metrics_by_field)


async def import_profiles(ctx, task):
    """Claim before HTTP and publish only after the complete supported cohort is retained."""
    artifact_root, api_key, budget = _parameters(task)
    await raise_if_cancelled(ctx, task)
    artifact_root.mkdir(mode=0o700, parents=True, exist_ok=True)
    directory = artifact_root / _hash([SOURCE_KEY, task["run_id"]])
    is_claimed = is_created = False
    try:
        async with asyncio.timeout_at(budget["deadline"]):
            _check_resources(budget, MAX_SNAPSHOT_BYTES, 4)
            directory.mkdir(mode=0o700, exist_ok=False)
            is_created = True
            await ensure_tables()
            await completion.reconcile_failed_control_runs()
            publication = await store.read_publication()
            snapshot_pin, row_count, cohort = await _capture_inputs(ctx, task, directory, budget)
            run = _run_row(
                task, snapshot_pin, row_count, cohort, publication["current_run_id"] if publication else None
            )
            await _checkpoint(ctx, task, budget)
            await store.claim_run(run)
            is_claimed = True
            completed = await _run_claimed(ctx, task, run, cohort, directory, api_key, budget)
    except BaseException as error:
        if is_claimed:
            await store.mark_run_failed(run["run_id"], error)
        elif is_created:
            shutil.rmtree(directory)
        raise
    try:
        await store.retain_source_history(artifact_root)
    except Exception:
        logger.exception("New York profile retention failed after completed import %s", run["run_id"])
    return completed


@click.command(help="Submit the complete supported New York physician profile cohort through the managed import API.")
def new_york_nypp_profile():
    """Require a managed control attempt for source publication."""
    raise click.UsageError("New York profile imports require the managed import API.")
