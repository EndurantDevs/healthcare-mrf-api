# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Massachusetts policy binding for source-scoped profile retention and publication."""

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
_retention_candidates = _store._retention_candidates
_assert_source_ownership = _store._assert_source_ownership
retain_source_history = _store.retain_source_history
