# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from api.mrf_discovery_catalog_filters import (
    source_file_identity_scope_condition,
)
from db.models import MRFFile, MRFSource


def test_exact_employer_filter_rejects_explicit_ein_mismatches():
    """Fence stale name-index matches from an exact employer-EIN source."""

    condition = source_file_identity_scope_condition(
        MRFFile.__table__,
        MRFSource.__table__,
    )
    compiled_condition = condition.compile()
    compiled_sql = str(compiled_condition)
    compiled_values = set(compiled_condition.params.values())

    assert "regexp_replace" in compiled_sql
    assert "query_context_lookup_type" in compiled_values
    assert "query_context_employer_ein" in compiled_values
    assert "anthem_matched_ein" in compiled_values
    assert "employer_ein" in compiled_values
