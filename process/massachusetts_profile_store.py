# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Massachusetts policy binding for source-scoped profile retention and publication."""

import re

from sqlalchemy import select

from process import provider_profile_source_store as shared_store
from db.models import (
    ProviderProfileArtifact as ProviderProfileArtifact,
    ProviderProfileFact as ProviderProfileFact,
    ProviderProfileImportRun as ProviderProfileImportRun,
    ProviderProfileSourcePublication as ProviderProfileSourcePublication,
    ProviderProfileSourceRecord as ProviderProfileSourceRecord,
)
from process.provider_profile_source_store import (
    ACTIVE_STATUSES as ACTIVE_STATUSES, RUN_ID_PATTERN as RUN_ID_PATTERN,
    ProfileSourcePolicy, SourceProfileStore,
    _now as _now, ensure_tables as ensure_tables,
)
from process.massachusetts_profile_rows import (
    LEGACY_CATEGORIES as LEGACY_CATEGORIES, PROFILE_CATEGORIES as PROFILE_CATEGORIES,
    SCHEMA_VERSION as SCHEMA_VERSION, SOURCE_KEY as SOURCE_KEY,
)


class MassachusettsProfileStore(SourceProfileStore):
    """Accept historical education runs and the expanded BORIM portfolio."""

    def _has_valid_categories(self, categories):
        return categories in (list(LEGACY_CATEGORIES), list(PROFILE_CATEGORIES))

    def _manifest(self, run_by_field):
        manifest = super()._manifest(run_by_field)
        parent_id = manifest.get("reprocess_from")
        lineage = manifest.get("reprocessing")
        if parent_id is not None:
            self._run_id(parent_id)
            if (manifest["resume_from"] is not None or parent_id != manifest["expected_current_run_id"]
                    or manifest["categories"] != list(PROFILE_CATEGORIES)
                    or not isinstance(lineage, dict)
                    or set(lineage) != {"source_run_id", "artifact_id", "manifest_sha256", "response_envelopes_sha256"}
                    or lineage["source_run_id"] != parent_id
                    or any(not isinstance(lineage[key], str) or not re.fullmatch(r"[a-f0-9]{64}", lineage[key])
                           for key in ("artifact_id", "manifest_sha256", "response_envelopes_sha256"))):
                raise ValueError("massachusetts_profile_reprocessing_manifest_invalid")
        elif lineage is not None:
            raise ValueError("massachusetts_profile_reprocessing_manifest_invalid")
        return manifest

    async def claim_run(self, run_row):
        """Freeze retained-parent identity within the shared source claim lock."""
        if self._manifest(run_row).get("reprocess_from") is None:
            return await super().claim_run(run_row)
        async with shared_store.db.transaction():
            await self._lock_source()
            manifest = self._manifest(run_row)
            parent, artifact = await self._reprocess_run(manifest["reprocess_from"], manifest["expected_current_run_id"])
            if (any(manifest[key] != parent["source_manifest"][key] for key in ("cohort_sha256", "full_cohort_licenses", "source"))
                    or manifest["reprocessing"]["artifact_id"] != artifact["artifact_id"]
                    or manifest["reprocessing"]["manifest_sha256"] != artifact["content_sha256"]):
                raise RuntimeError("massachusetts_profile_reprocessing_parent_changed")
            await super().claim_run(run_row)

    def _completion_metrics(self, run_by_field, metrics, counts_by_field):
        result = super()._completion_metrics(run_by_field, metrics, counts_by_field)
        manifest = self._manifest(run_by_field)
        if manifest.get("reprocess_from") is not None and (
                type(metrics.get("reused_responses")) is not int or metrics["reused_responses"] != metrics["responses"]
                or not re.fullmatch(r"[a-f0-9]{64}", str(metrics.get("response_envelopes_sha256")))
                or (manifest["max_providers"] is None
                    and metrics["response_envelopes_sha256"] != manifest["reprocessing"]["response_envelopes_sha256"])):
            raise RuntimeError("massachusetts_profile_reprocessing_incomplete")
        return result

    async def _resume_run(self, run_id, max_providers, expected_current_run_id):
        candidate = await super()._resume_run(run_id, max_providers, expected_current_run_id)
        if self._manifest(candidate).get("reprocess_from") is not None:
            raise RuntimeError("massachusetts_profile_resume_not_eligible")
        return candidate

    async def _reprocess_run(self, run_id, expected_current_run_id):
        parent = await self._read_run(run_id)
        manifest = self._manifest(parent)
        if (run_id != expected_current_run_id or parent["status"] != "completed"
                or manifest["max_providers"] is not None or (parent.get("metrics") or {}).get("published") is not True):
            raise RuntimeError("massachusetts_profile_reprocessing_parent_ineligible")
        await self._expected_publication(expected_current_run_id)
        self._completion_metrics(parent, parent["metrics"], await self.retained_counts(run_id))
        table = ProviderProfileArtifact.__table__
        artifacts = await shared_store.db.all(select(table).where(table.c.run_id == run_id))
        if len(artifacts) != 1:
            raise RuntimeError("massachusetts_profile_reprocessing_artifact_missing")
        artifact_by_field = dict(artifacts[0]._mapping)
        if (artifact_by_field["source_key"] != SOURCE_KEY or artifact_by_field["file_name"] != "manifest.json" or artifact_by_field["category"] != "profile"):
            raise RuntimeError("massachusetts_profile_reprocessing_artifact_invalid")
        return parent, artifact_by_field

    async def read_reprocess_run(self, run_id, *, expected_current_run_id):
        """Read the current completed full acquisition under its publication lock."""
        async with shared_store.db.transaction():
            await self._lock_source()
            return await self._reprocess_run(run_id, expected_current_run_id)

    def _retention_candidates(self, run_rows, publication, now):
        eligible, protected = super()._retention_candidates(run_rows, publication, now)
        run_by_id = {row["run_id"]: row for row in run_rows}
        retained_ids = set(run_by_id) - set(eligible)
        pending_ids = list(retained_ids)
        # Keep private acquisition ancestry while any retained descendant needs it.
        while pending_ids:
            manifest = run_by_id[pending_ids.pop()].get("source_manifest") or {}
            parent_id = manifest.get("reprocess_from")
            if parent_id is not None:
                self._run_id(parent_id)
                if parent_id not in run_by_id:
                    raise RuntimeError("massachusetts_profile_reprocessing_parent_missing")
                if parent_id not in retained_ids:
                    retained_ids.add(parent_id)
                    pending_ids.append(parent_id)
                protected.append(parent_id)
        return sorted(set(eligible) - retained_ids), sorted(set(protected))


_store = MassachusettsProfileStore(ProfileSourcePolicy(
    source_key=SOURCE_KEY, schema_version=SCHEMA_VERSION, jurisdiction="MA",
    categories=("education", "training"), error_prefix="massachusetts_profile",
    guarded_public_sql="f.category IN ('education', 'training')",
    received_profile_sql="raw_payload->>'licenseNumber' = license_number AND raw_payload->>'licenseMetaId' = '1'",
))

_table = _store._table
_run_id = _store._run_id
_lock_source = _store._lock_source
_read_run = _store._read_run
read_publication = _store.read_publication
_manifest = _store._manifest
_expected_publication = _store._expected_publication
claim_run = _store.claim_run
update_run = _store.update_run
retained_counts = _store.retained_counts
_completion_metrics = _store._completion_metrics
_publication_volume = _store._publication_volume
_complete_run = _store._complete_run
publish_run = _store.publish_run
finish_unpublished_run = _store.finish_unpublished_run
mark_run_failed = _store.mark_run_failed
_resume_run = _store._resume_run
read_resume_run = _store.read_resume_run
read_reprocess_run = _store.read_reprocess_run
_retention_candidates = _store._retention_candidates
_assert_source_ownership = _store._assert_source_ownership
retain_source_history = _store.retain_source_history
