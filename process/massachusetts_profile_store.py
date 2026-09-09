# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Source-scoped retention and atomic publication of complete BORIM imports."""

from __future__ import annotations

import asyncio
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

from sqlalchemy import select, text

from db.models import (
    ProviderProfileArtifact, ProviderProfileFact, ProviderProfileImportRun,
    ProviderProfileSourcePublication, ProviderProfileSourceRecord, db,
)
from process.florida_mqa_profile import (
    _claim_import_run, _delete_retained_payload_rows, _remove_artifact_run_directories,
)
from process.massachusetts_profile_rows import LEGACY_CATEGORIES, PROFILE_CATEGORIES, SCHEMA_VERSION, SOURCE_KEY

RUN_ID_PATTERN = re.compile(r"(?:[a-f0-9]{32}|[a-f0-9]{64})")
ACTIVE_STATUSES = ("running", "validating")


def _now():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _table(model):
    schema = model.__table__.schema or "mrf"
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema):
        raise ValueError("massachusetts_profile_schema_invalid")
    return f'"{schema}"."{model.__table__.name}"'


def _run_id(run_id):
    if not isinstance(run_id, str) or not RUN_ID_PATTERN.fullmatch(run_id):
        raise ValueError("massachusetts_profile_run_id_invalid")
    return run_id


async def _lock_source():
    await db.scalar(text("SELECT pg_advisory_xact_lock(hashtext(:lock_name))"),
                    lock_name=f"{_table(ProviderProfileSourcePublication)}.{SOURCE_KEY}.publication")


async def ensure_tables():
    """Create only the shared source tables used by this importer."""
    for model in (ProviderProfileImportRun, ProviderProfileArtifact, ProviderProfileSourceRecord,
                  ProviderProfileFact, ProviderProfileSourcePublication):
        await db.create_table(model.__table__, checkfirst=True)


async def _read_run(run_id):
    table = ProviderProfileImportRun.__table__
    source_row = await db.first(select(table).where(table.c.run_id == _run_id(run_id)))
    if source_row is None:
        raise RuntimeError("massachusetts_profile_run_missing")
    run_by_field = dict(source_row._mapping)
    if run_by_field["source_key"] != SOURCE_KEY or run_by_field["schema_version"] != SCHEMA_VERSION:
        raise RuntimeError("massachusetts_profile_run_source_mismatch")
    return run_by_field


async def read_publication():
    """Return the MA pointer only when it refers to a completed full run."""
    table = ProviderProfileSourcePublication.__table__
    source_row = await db.first(select(table).where(table.c.source_key == SOURCE_KEY))
    if source_row is None:
        return None
    publication_by_field = dict(source_row._mapping)
    current_run = await _read_run(publication_by_field["current_run_id"])
    if current_run["status"] != "completed":
        raise RuntimeError("massachusetts_profile_publication_invalid")
    if (_manifest(current_run)["max_providers"] is not None
            or (current_run.get("metrics") or {}).get("published") is not True):
        raise RuntimeError("massachusetts_profile_publication_invalid")
    return publication_by_field


def _manifest(run_by_field):
    manifest_by_field = run_by_field.get("source_manifest")
    if not isinstance(manifest_by_field, dict):
        raise ValueError("massachusetts_profile_manifest_invalid")
    required_fields = {"max_providers", "expected_current_run_id", "full_cohort_licenses",
                       "requested_licenses", "cohort_sha256", "source", "categories", "resume_from"}
    if not required_fields <= manifest_by_field.keys():
        raise ValueError("massachusetts_profile_manifest_missing")
    descriptor = manifest_by_field["source"]
    if (not isinstance(descriptor, dict) or descriptor.get("source_key") != SOURCE_KEY
            or descriptor.get("source_kind") != "state_regulator" or descriptor.get("jurisdiction") != "MA"):
        raise ValueError("massachusetts_profile_manifest_source_invalid")
    if manifest_by_field["categories"] not in (list(LEGACY_CATEGORIES), list(PROFILE_CATEGORIES)):
        raise ValueError("massachusetts_profile_manifest_categories_invalid")
    if not re.fullmatch(r"[a-f0-9]{64}", str(manifest_by_field["cohort_sha256"])):
        raise ValueError("massachusetts_profile_cohort_hash_invalid")
    for key in ("full_cohort_licenses", "requested_licenses"):
        if type(manifest_by_field[key]) is not int or manifest_by_field[key] <= 0:
            raise ValueError("massachusetts_profile_manifest_count_invalid")
    limit = manifest_by_field["max_providers"]
    if limit is not None and (type(limit) is not int or limit <= 0):
        raise ValueError("massachusetts_profile_limit_invalid")
    expected_count = min(limit, manifest_by_field["full_cohort_licenses"]) if limit is not None else manifest_by_field["full_cohort_licenses"]
    if manifest_by_field["requested_licenses"] != expected_count:
        raise ValueError("massachusetts_profile_manifest_scope_invalid")
    for key in ("expected_current_run_id", "resume_from"):
        if manifest_by_field[key] is not None:
            _run_id(manifest_by_field[key])
    return manifest_by_field


async def _expected_publication(expected_run_id):
    publication = await read_publication()
    current_run_id = publication["current_run_id"] if publication else None
    if current_run_id != expected_run_id:
        raise RuntimeError("massachusetts_profile_predecessor_changed")
    return publication


async def claim_run(run_row):
    """Claim a fresh run under the source publication lock; never replay an ID."""
    _run_id(run_row.get("run_id"))
    if (run_row.get("source_key") != SOURCE_KEY or run_row.get("schema_version") != SCHEMA_VERSION
            or run_row.get("jurisdiction") != "MA" or run_row.get("status") != "running"):
        raise ValueError("massachusetts_profile_initial_run_invalid")
    manifest = _manifest(run_row)
    async with db.transaction():
        await _lock_source()
        await _expected_publication(manifest["expected_current_run_id"])
        table = ProviderProfileImportRun.__table__
        if await db.scalar(select(table.c.run_id).where(
                table.c.source_key == SOURCE_KEY, table.c.status.in_(ACTIVE_STATUSES)).limit(1)):
            raise RuntimeError("massachusetts_profile_source_already_running")
        if manifest["resume_from"] is not None:
            resume_run = await _resume_run(manifest["resume_from"], manifest["max_providers"], manifest["expected_current_run_id"])
            if any(manifest[key] != resume_run["source_manifest"][key]
                   for key in ("cohort_sha256", "full_cohort_licenses", "requested_licenses", "categories")):
                raise RuntimeError("massachusetts_profile_resume_cohort_mismatch")
        await _claim_import_run(run_row)


async def update_run(run_id, fields: dict):
    """Update active-run progress without changing its frozen acquisition scope."""
    if not isinstance(fields, dict) or not fields or not set(fields) <= {"status", "metrics"}:
        raise ValueError("massachusetts_profile_run_update_invalid")
    if "status" in fields and fields["status"] != "validating":
        raise ValueError("massachusetts_profile_run_transition_invalid")
    if "metrics" in fields and not isinstance(fields["metrics"], dict):
        raise ValueError("massachusetts_profile_metrics_invalid")
    async with db.transaction():
        await _lock_source()
        run_by_field = await _read_run(run_id)
        if run_by_field["status"] not in ACTIVE_STATUSES:
            raise RuntimeError("massachusetts_profile_run_not_active")
        await db.update(ProviderProfileImportRun.__table__).where(
            ProviderProfileImportRun.__table__.c.run_id == run_id).values(fields).status()


async def retained_counts(run_id):
    """Count stored source records and public NPIs, including integrity failures."""
    _run_id(run_id)
    source_records = _table(ProviderProfileSourceRecord)
    facts = _table(ProviderProfileFact)
    artifacts = _table(ProviderProfileArtifact)
    runs = _table(ProviderProfileImportRun)
    count_row = await db.first(text(f"""
        WITH source_counts AS (
            SELECT count(*) AS retained_source_records,
               count(*) FILTER (WHERE raw_payload->>'licenseNumber' = license_number
                                  AND raw_payload->>'licenseMetaId' = '1') AS received_profiles,
               count(*) FILTER (WHERE source_key <> :source_key
                     OR normalized_payload->>'schema_version' IS DISTINCT FROM :schema_version) AS invalid_source_records
              FROM {source_records} WHERE run_id = :run_id
        ), fact_counts AS (
            SELECT count(*) AS retained_facts,
               count(DISTINCT f.npi) FILTER (WHERE r.run_id = f.run_id AND r.source_key = :source_key
                   AND r.match_status = 'deterministic' AND r.matched_npi = f.npi
                   AND r.normalized_payload->>'visibility' = 'public'
                   AND NOT f.sensitive AND f.public_default AND f.availability = 'available') AS all_public_providers,
               count(DISTINCT f.npi) FILTER (WHERE r.run_id = f.run_id AND r.source_key = :source_key
                   AND r.match_status = 'deterministic' AND r.matched_npi = f.npi
                   AND r.normalized_payload->>'visibility' = 'public'
                   AND f.category IN ('education', 'training')
                   AND NOT f.sensitive AND f.public_default AND f.availability = 'available') AS matched_public_providers,
               count(*) FILTER (WHERE r.record_id IS NULL OR r.run_id IS DISTINCT FROM f.run_id
                    OR r.source_key <> :source_key
                    OR (source_run.source_manifest::jsonb->'categories' ? f.category) IS DISTINCT FROM TRUE
                    OR f.source_json->>'source_key' IS DISTINCT FROM :source_key
                    OR f.source_json->>'schema_version' IS DISTINCT FROM :schema_version
                    OR f.source_json->>'source_record_id' IS DISTINCT FROM f.source_record_id
                    OR f.npi IS DISTINCT FROM r.matched_npi
                    OR r.normalized_payload->>'visibility' IS DISTINCT FROM 'public'
                    OR (f.npi IS NOT NULL AND r.match_status <> 'deterministic')) AS invalid_facts
              FROM {facts} f LEFT JOIN {source_records} r ON r.record_id = f.source_record_id
              LEFT JOIN {runs} source_run ON source_run.run_id = f.run_id
             WHERE f.run_id = :run_id
        )
        SELECT source_counts.*, fact_counts.retained_facts, fact_counts.matched_public_providers,
               fact_counts.all_public_providers - fact_counts.matched_public_providers AS portfolio_only_public_providers,
               fact_counts.invalid_facts,
               (SELECT count(*) FROM {artifacts} WHERE run_id = :run_id
                    AND source_key <> :source_key) AS foreign_artifacts
          FROM source_counts CROSS JOIN fact_counts
    """), run_id=run_id, source_key=SOURCE_KEY, schema_version=SCHEMA_VERSION)
    return {key: int(count) for key, count in count_row._mapping.items()}


def _completion_metrics(run_by_field, metrics, counts_by_field):
    manifest = _manifest(run_by_field)
    if not isinstance(metrics, dict) or metrics.get("acquisition_complete") is not True:
        raise RuntimeError("massachusetts_profile_acquisition_incomplete")
    if type(metrics.get("transport_failures")) is not int or metrics["transport_failures"] != 0:
        raise RuntimeError("massachusetts_profile_transport_failures")
    if (type(metrics.get("responses")) is not int
            or metrics["responses"] != manifest["requested_licenses"]
            or metrics["responses"] != counts_by_field["retained_source_records"]):
        raise RuntimeError("massachusetts_profile_response_count_mismatch")
    if any(counts_by_field[key] for key in ("invalid_source_records", "invalid_facts", "foreign_artifacts")):
        raise RuntimeError("massachusetts_profile_retained_integrity_invalid")
    return {**metrics, **counts_by_field, "requested_licenses": manifest["requested_licenses"],
            "full_cohort_licenses": manifest["full_cohort_licenses"]}


def _publication_volume(metrics, incumbent_metrics):
    if metrics["received_profiles"] * 2 < metrics["requested_licenses"]:
        raise RuntimeError("massachusetts_profile_received_profile_ratio")
    if incumbent_metrics is None:
        if metrics["matched_public_providers"] < 10000:
            raise RuntimeError("massachusetts_profile_first_publication_too_small")
    else:
        for key in ("matched_public_providers", "received_profiles"):
            if metrics[key] * 5 < incumbent_metrics[key] * 4:
                raise RuntimeError(f"massachusetts_profile_publication_volume_drop:{key}")


async def _complete_run(run_id, metrics):
    finished_at = _now()
    await db.update(ProviderProfileImportRun.__table__).where(
        ProviderProfileImportRun.__table__.c.run_id == run_id).values(
            status="completed", metrics=metrics, finished_at=finished_at, error=None).status()
    return finished_at


async def publish_run(run_id, *, expected_current_run_id, metrics):
    """Atomically complete a validated full run and advance its source pointer."""
    async with db.transaction():
        await _lock_source()
        candidate_run = await _read_run(run_id)
        if candidate_run["status"] not in ACTIVE_STATUSES:
            raise RuntimeError("massachusetts_profile_run_not_active")
        manifest = _manifest(candidate_run)
        if manifest["max_providers"] is not None:
            raise RuntimeError("massachusetts_profile_bounded_publication_forbidden")
        if manifest["expected_current_run_id"] != expected_current_run_id:
            raise RuntimeError("massachusetts_profile_frozen_predecessor_mismatch")
        await _expected_publication(expected_current_run_id)
        final_metrics = _completion_metrics(candidate_run, metrics, await retained_counts(run_id))
        incumbent_metrics = await retained_counts(expected_current_run_id) if expected_current_run_id else None
        _publication_volume(final_metrics, incumbent_metrics)
        published_at = await _complete_run(run_id, {**final_metrics, "published": True})
        await db.update(ProviderProfileFact.__table__).where(
            ProviderProfileFact.__table__.c.run_id == run_id).values(published_at=published_at).status()
        pointer_by_field = {"source_key": SOURCE_KEY, "current_run_id": run_id,
                            "previous_run_id": expected_current_run_id, "published_at": published_at}
        await db.insert(ProviderProfileSourcePublication.__table__).values(pointer_by_field).on_conflict_do_update(
            index_elements=["source_key"], set_=pointer_by_field).status()
    return {**final_metrics, "published": True, "run_id": run_id, "previous_run_id": expected_current_run_id}


async def finish_unpublished_run(run_id, metrics):
    """Complete a bounded acquisition while leaving the public pointer untouched."""
    async with db.transaction():
        await _lock_source()
        candidate_run = await _read_run(run_id)
        if candidate_run["status"] not in ACTIVE_STATUSES or _manifest(candidate_run)["max_providers"] is None:
            raise RuntimeError("massachusetts_profile_bounded_completion_invalid")
        final_metrics = _completion_metrics(candidate_run, metrics, await retained_counts(run_id))
        await _complete_run(run_id, {**final_metrics, "published": False})
    return {**final_metrics, "published": False, "run_id": run_id}


async def mark_run_failed(run_id, error):
    """Record failure without downgrading a completed publication or test run."""
    async with db.transaction():
        await _lock_source()
        run_by_field = await _read_run(run_id)
        if run_by_field["status"] == "completed":
            return
        await db.update(ProviderProfileImportRun.__table__).where(
            ProviderProfileImportRun.__table__.c.run_id == run_id).values(
                status="failed", error={"message": str(error)}, finished_at=_now()).status()


async def _resume_run(run_id, max_providers, expected_current_run_id):
    if max_providers is not None and (type(max_providers) is not int or max_providers <= 0):
        raise ValueError("massachusetts_profile_limit_invalid")
    candidate_run = await _read_run(run_id)
    manifest = _manifest(candidate_run)
    finished_at = candidate_run.get("finished_at")
    if (candidate_run["status"] != "failed" or not isinstance(finished_at, datetime)
            or finished_at < _now() - timedelta(days=7) or finished_at > _now()):
        raise RuntimeError("massachusetts_profile_resume_not_eligible")
    if (manifest["max_providers"] != max_providers
            or manifest["expected_current_run_id"] != expected_current_run_id):
        raise RuntimeError("massachusetts_profile_resume_scope_mismatch")
    await _expected_publication(expected_current_run_id)
    return candidate_run


async def read_resume_run(run_id, *, max_providers, expected_current_run_id):
    """Return only a recent failed acquisition with the same frozen scope."""
    async with db.transaction():
        await _lock_source()
        return await _resume_run(run_id, max_providers, expected_current_run_id)


def _retention_candidates(run_rows, publication, now):
    protected_run_ids = set()
    if publication:
        protected_run_ids.update(publication[key] for key in ("current_run_id", "previous_run_id") if publication[key])
    started_times = [source_row["started_at"] for source_row in run_rows if isinstance(source_row.get("started_at"), datetime)]
    latest_started_at = max(started_times, default=None)
    for source_row in run_rows:
        # The newest bounded completion is still needed for acceptance and inspection.
        if source_row.get("started_at") == latest_started_at or not isinstance(source_row.get("started_at"), datetime):
            protected_run_ids.add(source_row["run_id"])
        if source_row["status"] in ACTIVE_STATUSES:
            protected_run_ids.add(source_row["run_id"])
            dependency = (source_row.get("source_manifest") or {}).get("resume_from")
            if dependency:
                protected_run_ids.add(dependency)
    eligible_run_ids = []
    for source_row in run_rows:
        if source_row["run_id"] in protected_run_ids:
            continue
        if source_row["status"] == "completed" or (
                source_row["status"] == "failed" and isinstance(source_row.get("finished_at"), datetime)
                and source_row["finished_at"] < now - timedelta(days=7)):
            eligible_run_ids.append(_run_id(source_row["run_id"]))
    return sorted(eligible_run_ids), sorted(protected_run_ids)


async def _assert_source_ownership(run_ids):
    for run_id in run_ids:
        counts_by_field = await retained_counts(run_id)
        if any(counts_by_field[key] for key in ("invalid_source_records", "invalid_facts", "foreign_artifacts")):
            raise RuntimeError("massachusetts_profile_retention_foreign_payload")


async def retain_source_history(artifact_root):
    """Retain current, previous and resumable source data while keeping run audits."""
    async with db.transaction():
        await _lock_source()
        publication = await read_publication()
        table = ProviderProfileImportRun.__table__
        run_rows = await db.all(select(table).where(table.c.source_key == SOURCE_KEY))
        eligible_run_ids, protected_run_ids = _retention_candidates(
            [dict(source_row._mapping) for source_row in run_rows], publication, _now())
        await _assert_source_ownership(eligible_run_ids)
        deleted_by_kind = await _delete_retained_payload_rows(eligible_run_ids) if eligible_run_ids else {}
        # Keep the same source lock through exact directory deletion so a resume cannot race cleanup.
        directory_receipt = await asyncio.to_thread(_remove_artifact_run_directories, Path(artifact_root), eligible_run_ids)
    return {"status": "completed_with_directory_errors" if directory_receipt["errors"] else "completed",
            "source_key": SOURCE_KEY, "failed_retention_days": 7, "deleted_run_ids": eligible_run_ids,
            "protected_audit_run_ids": protected_run_ids, "deleted_rows": deleted_by_kind,
            "artifact_directories": directory_receipt}
