# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Kentucky profile policy with the conservative 10,000-provider publication floor."""

from process.provider_profile_source_store import (
    ACTIVE_STATUSES as ACTIVE_STATUSES, RUN_ID_PATTERN as RUN_ID_PATTERN,
    ProfileSourcePolicy, SourceProfileStore,
    _now as _now, ensure_tables as ensure_tables,
)
from process.kentucky_profile_rows import (
    LEGACY_CATEGORIES, PROFILE_CATEGORIES,
    SCHEMA_VERSION as SCHEMA_VERSION, SOURCE_KEY as SOURCE_KEY,
)


class KentuckyProfileStore(SourceProfileStore):
    """Accept historical education runs and the full reported practice portfolio."""

    def _has_valid_categories(self, categories):
        return categories in (list(LEGACY_CATEGORIES), list(PROFILE_CATEGORIES))


_store = KentuckyProfileStore(ProfileSourcePolicy(
    source_key=SOURCE_KEY, schema_version=SCHEMA_VERSION, jurisdiction="KY",
    categories=PROFILE_CATEGORIES, error_prefix="kentucky_profile",
    # Keep the original publication floor/drop guard about providers with education.
    guarded_public_sql="f.category = 'education' AND f.fact_type = 'education_history'",
    received_profile_sql="""
        CASE WHEN json_typeof(raw_payload->'profiles') = 'array' THEN
            json_array_length(raw_payload->'profiles') = 1
            AND json_typeof(raw_payload->'profiles'->0->'License') = 'string'
            AND regexp_replace(raw_payload->'profiles'->0->>'License',
                               '^[[:space:]]+|[[:space:]]+$', '', 'g') = license_number
            AND normalized_payload->>'visibility' IN ('public', 'education_not_reported', 'education_unusable')
        ELSE FALSE END
    """,
    invalid_fact_sql="""
        (f.category = 'education' AND f.fact_type = 'education_history'
         OR f.category = 'specialties' AND f.fact_type = 'specialty'
         OR f.category = 'services' AND f.fact_type = 'practice_type') IS DISTINCT FROM TRUE
    """,
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
