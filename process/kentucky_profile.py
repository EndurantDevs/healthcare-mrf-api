# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Import public Kentucky physician profile facts as a managed job."""

from __future__ import annotations

import hashlib
import logging
import os
from pathlib import Path

import click

from db.models import ProviderProfileArtifact, ProviderProfileSourceRecord, ProviderProfileFact, db
from process.control_cancel import raise_if_cancelled
from process.florida_mqa_profile import _upsert_rows
from process.live_progress import enqueue_live_progress
from process.massachusetts_profile import _hash, _now, _selected_roots as _hash_selected_roots
from process.kentucky_profile_completion import complete_run, reconcile_failed_control_runs
from process import kentucky_profile_acquisition as acquisition
from process import kentucky_profile_store as store
from process.kentucky_profile_rows import (
    LEGACY_CATEGORIES, PROFILE_CATEGORIES, SCHEMA_VERSION, SOURCE_KEY,
    _parsed_profiles, extract_profiles, parse_profile,
)

logger = logging.getLogger(__name__)
SAMPLING_STRATEGY = "lexical_first_then_stable_hash/v2"
LEGACY_SAMPLING_STRATEGY = "stable_hash/v1"
ACQUISITION_STRATEGY = "literal_license_then_candidate_surnames/v2"


def _selected_roots(cohort, limit, *, strategy=SAMPLING_STRATEGY):
    """Exercise the lexical boundary in new bounded runs without changing full order."""
    if strategy not in (SAMPLING_STRATEGY, LEGACY_SAMPLING_STRATEGY):
        raise ValueError("kentucky_profile_sampling_strategy_invalid")
    if strategy == LEGACY_SAMPLING_STRATEGY or limit is None or not cohort["roots"]:
        return _hash_selected_roots(cohort, limit)
    first = min(cohort["roots"], key=lambda root: root["license_number"])
    remaining_roots = [root for root in _hash_selected_roots(cohort, len(cohort["roots"])) if root != first]
    return [first, *remaining_roots[:limit - 1]]


def _parameters(task):
    limit = task.get("max_providers")
    if limit is not None and (type(limit) is not int or limit <= 0):
        raise ValueError("kentucky_profile_limit_invalid")
    resume_from = task.get("resume_from")
    if resume_from is not None and (
        not isinstance(resume_from, str) or not store.RUN_ID_PATTERN.fullmatch(resume_from)
    ):
        raise ValueError("kentucky_profile_resume_id_invalid")
    return limit, resume_from


def _artifact_root():
    root = os.environ.get("HLTHPRT_KY_KBML_ARTIFACT_ROOT", "/work/kentucky-kbml")
    if not root.strip():
        raise ValueError("kentucky_profile_artifact_root_invalid")
    path = Path(root).absolute()
    acquisition._reject_symlinks(path)
    return path


def _retained_directory(artifact_root, run_id):
    store._run_id(run_id)
    directory = artifact_root / run_id
    acquisition._reject_symlinks(directory / "profiles")
    return directory


async def _cohort_for_run(task, artifact_root, expected_current_run_id):
    limit, resume_from = _parameters(task)
    if resume_from is None:
        schema = ProviderProfileSourceRecord.__table__.schema or "mrf"
        return await acquisition.capture_registry_cohort(schema), None, PROFILE_CATEGORIES, SAMPLING_STRATEGY
    previous_run = await store.read_resume_run(
        resume_from, max_providers=limit, expected_current_run_id=expected_current_run_id,
    )
    previous_manifest = previous_run["source_manifest"]
    if (previous_manifest.get("acquisition_strategy") != ACQUISITION_STRATEGY
            and "response_too_large" in str(previous_run.get("error"))):
        raise ValueError("kentucky_profile_legacy_oversized_checkpoint_incomplete")
    directory = _retained_directory(artifact_root, resume_from)
    cohort = acquisition.read_cohort(directory / "cohort.json")
    if _hash(cohort) != previous_run["source_manifest"]["cohort_sha256"]:
        raise ValueError("kentucky_profile_resume_cohort_changed")
    strategy = previous_manifest.get("sampling_strategy", LEGACY_SAMPLING_STRATEGY)
    _selected_roots(cohort, limit, strategy=strategy)
    return cohort, directory / "profiles", previous_manifest["categories"], strategy


def _source_manifest(task, cohort, expected_current_run_id, *, categories=PROFILE_CATEGORIES, sampling_strategy=SAMPLING_STRATEGY):
    limit, resume_from = _parameters(task)
    count = len(cohort["roots"])
    return {
        "max_providers": limit, "resume_from": resume_from,
        "expected_current_run_id": expected_current_run_id,
        "full_cohort_licenses": count, "requested_licenses": min(limit, count) if limit is not None else count,
        "cohort_sha256": _hash(cohort), "control_run_id": task.get("run_id"), "categories": list(categories),
        "sampling_strategy": sampling_strategy, "acquisition_strategy": ACQUISITION_STRATEGY,
        "source": {
            "source_key": SOURCE_KEY, "source_kind": "state_regulator", "jurisdiction": "KY",
            "agency": "Kentucky Board of Medical Licensure", "source_url": acquisition.LOOKUP_URL,
            "coverage_scope": cohort["coverage_scope"], "registry_generation": cohort["registry_generation"],
        },
    }


def _progress(task, run_id, phase, done, total):
    enqueue_live_progress(
        run_id=task.get("run_id"), importer="kentucky-kbml-profile", status="running",
        phase=phase, unit="license", done=done, total=total, pct=100 * done / total if total else 0,
        message=f"Kentucky profiles: {phase} {done}/{total}",
        metrics={"provider_profile_run_id": run_id, "coverage_scope": acquisition.COVERAGE_SCOPE},
    )


async def _acquire(ctx, task, run_row, cohort, directory, retained):
    manifest = run_row["source_manifest"]
    roots = _selected_roots(cohort, manifest["max_providers"], strategy=manifest.get("sampling_strategy", LEGACY_SAMPLING_STRATEGY))

    async def progress(done, total):
        """Check every request checkpoint for cancellation and throttle visible updates."""
        await raise_if_cancelled(ctx, task)
        if done % 50 == 0 or done == total:
            _progress(task, run_row["run_id"], "acquiring", done, total)

    metrics = await acquisition.acquire_profiles(roots, directory / "profiles", progress, retained=retained)
    return roots, {**metrics, "acquisition_complete": True, "transport_failures": 0,
                   "acquisition_complete_scope": acquisition.NARROWED_SCOPE}


def _artifact(run_row, directory, metrics):
    manifest_by_field = {"schema_version": SCHEMA_VERSION, "run_id": run_row["run_id"],
                         "source_manifest": run_row["source_manifest"], "acquisition": metrics}
    acquisition.write_new_json(directory / "manifest.json", manifest_by_field)
    return {
        "artifact_id": _hash([run_row["run_id"], SOURCE_KEY]), "run_id": run_row["run_id"],
        "source_key": SOURCE_KEY, "file_name": "manifest.json", "source_url": acquisition.LOOKUP_URL,
        "category": "profile", "content_sha256": _hash(manifest_by_field),
        "content_bytes": len(acquisition.encoded_json(manifest_by_field)), "header": None,
        "downloaded_at": _now(), "metadata_json": {"cohort_sha256": run_row["source_manifest"]["cohort_sha256"], **metrics},
    }


def _source_rows(root, response, artifact, row_number, *, categories=LEGACY_CATEGORIES):
    if response["schema_version"] == acquisition.NARROWED_SCHEMA:
        return _narrowed_rows(root, response, artifact, row_number, categories=categories)
    evidence_by_field = {key: response[key] for key in ("source_url", "downloaded_at", "content_sha256")}
    evidence_by_field.update(run_id=artifact["run_id"], artifact_id=artifact["artifact_id"], row_number=row_number)
    return parse_profile(response["body_text"], license_number=root["license_number"],
                         candidates=root["candidates"], evidence=evidence_by_field, categories=categories)


def _narrowed_rows(root, response, artifact, row_number, *, categories):
    """Retain every narrowed result; deduplicate only identical complete profile payloads."""
    acquisition._validate_narrowed(response, root)
    profiles_by_payload, sources_by_payload = {}, {}
    for query, receipt in zip(response["query_scope"]["queries"], response["responses"], strict=True):
        profiles = extract_profiles(receipt["body_text"], license_number=root["license_number"],
                                    last_name=query["last_name"], reject_hidden="specialties" in categories)
        for profile in profiles:
            payload = acquisition.encoded_json(profile)
            profiles_by_payload.setdefault(payload, profile)
            sources_by_payload.setdefault(payload, receipt)
    profiles = list(profiles_by_payload.values())
    source = next(iter(sources_by_payload.values()), response["responses"][0])
    evidence_by_field = {key: source[key] for key in ("source_url", "downloaded_at", "content_sha256")}
    evidence_by_field.update(run_id=artifact["run_id"], artifact_id=artifact["artifact_id"], row_number=row_number)
    record, facts = _parsed_profiles(profiles, {"acquisition": response}, license_number=root["license_number"],
                                     candidates=root["candidates"], evidence=evidence_by_field, categories=categories)
    record["match_evidence"]["query_scope"] = response["query_scope"]
    if not profiles:
        record.update(match_status="unmatched")
        record["normalized_payload"]["visibility"] = "held_identity"
        record["match_evidence"]["reason"] = "no_profile_within_candidate_name_queries"
    for fact in facts:
        fact["source_json"]["acquisition_scope"] = acquisition.NARROWED_SCOPE
    return record, facts


async def _persist_profiles(ctx, task, roots, directory, artifact, *, categories=LEGACY_CATEGORIES):
    """Retain each parsed batch atomically while honoring cancellation between writes."""
    acquisition._license_numbers(roots)
    await raise_if_cancelled(ctx, task)
    await _upsert_rows(ProviderProfileArtifact, [artifact], "artifact_id")
    response_hash = hashlib.sha256()
    response_bytes = 0
    for offset in range(0, len(roots), 250):
        source_records, facts = [], []
        for index, root in enumerate(roots[offset:offset + 250], offset + 1):
            await raise_if_cancelled(ctx, task)
            response = acquisition.read_response(directory / "profiles" / f"{root['license_number']}.json", root["license_number"],
                                                 candidates=root.get("candidates"))
            response_hash.update(acquisition.encoded_json(response))
            response_bytes += acquisition.response_bytes(response)
            record, parsed_facts = _source_rows(root, response, artifact, index, categories=categories)
            source_records.append(record)
            facts.extend(parsed_facts)
        async with db.transaction():
            await _upsert_rows(ProviderProfileSourceRecord, source_records, "record_id")
            await raise_if_cancelled(ctx, task)
            await _upsert_rows(ProviderProfileFact, facts, "fact_id")
        _progress(task, artifact["run_id"], "retaining", min(offset + 250, len(roots)), len(roots))
    if (response_hash.hexdigest() != artifact["metadata_json"]["responses_sha256"]
            or response_bytes != artifact["metadata_json"]["response_bytes"]):
        raise ValueError("kentucky_profile_retained_responses_changed")


async def _finish_run(ctx, task, run_row, metrics):
    await raise_if_cancelled(ctx, task)
    await store.update_run(run_row["run_id"], {"status": "validating", "metrics": metrics})
    return await complete_run(ctx, task, run_row, metrics)


async def _run_claimed(ctx, task, run_row, cohort, retained, directory):
    await raise_if_cancelled(ctx, task)
    acquisition._reject_symlinks(directory)
    directory.mkdir(exist_ok=False)
    acquisition.write_new_json(directory / "cohort.json", cohort)
    roots, metrics = await _acquire(ctx, task, run_row, cohort, directory, retained)
    await store.update_run(run_row["run_id"], {"metrics": metrics})
    artifact = _artifact(run_row, directory, metrics)
    await _persist_profiles(ctx, task, roots, directory, artifact, categories=run_row["source_manifest"]["categories"])
    return await _finish_run(ctx, task, run_row, metrics)


async def import_profiles(ctx, task):
    """Acquire the frozen Kentucky cohort under a source claim before creating artifacts."""
    _parameters(task)
    control_run_id = task.get("run_id")
    if not isinstance(control_run_id, str) or not control_run_id.strip():
        raise ValueError("kentucky_profile_managed_run_required")
    await raise_if_cancelled(ctx, task)
    await store.ensure_tables()
    await reconcile_failed_control_runs()
    publication = await store.read_publication()
    expected = publication["current_run_id"] if publication else None
    artifact_root = _artifact_root()
    cohort, retained, categories, sampling_strategy = await _cohort_for_run(task, artifact_root, expected)
    acquisition._license_numbers(cohort["roots"])
    await raise_if_cancelled(ctx, task)
    run_id = _hash([SOURCE_KEY, control_run_id])
    run_by_field = {
        "run_id": run_id, "source_key": SOURCE_KEY, "jurisdiction": "KY", "schema_version": SCHEMA_VERSION,
        "status": "running", "source_manifest": _source_manifest(task, cohort, expected, categories=categories,
                                                                  sampling_strategy=sampling_strategy),
        "metrics": {}, "error": None, "started_at": _now(), "finished_at": None,
    }
    await store.claim_run(run_by_field)
    try:
        artifact_root.mkdir(parents=True, exist_ok=True)
        completed_run = await _run_claimed(ctx, task, run_by_field, cohort, retained, artifact_root / run_id)
    except BaseException as exc:
        await store.mark_run_failed(run_id, exc)
        raise
    try:
        await store.retain_source_history(artifact_root)
    except Exception:
        logger.exception("Kentucky profile retention failed after completed import %s", run_id)
    return completed_run


@click.command(help="Submit Kentucky physician profile imports through the managed import API.")
@click.option("--max-providers", type=click.IntRange(min=1), default=None, help="Bounded acquisition without publication.")
@click.option("--resume-from", default=None, help="Replay a recent failed run's frozen cohort and verified responses.")
def kentucky_kbml_profile(max_providers, resume_from):
    """Retain importer registry metadata while refusing unmanaged execution."""
    raise click.UsageError("Kentucky profile imports require the managed import API.")
