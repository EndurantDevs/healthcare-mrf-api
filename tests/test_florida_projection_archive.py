# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Only an actually published Florida generation crosses the native boundary."""

from datetime import datetime, timezone

import pytest

from process import florida_projection_archive as archive


def _run(publication="atomic_table_swap"):
    return {
        "run_id": "a" * 32,
        "source_key": "florida-mqa",
        "jurisdiction": "FL",
        "schema_version": "provider-profile/v1",
        "status": "completed",
        "started_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        "finished_at": datetime(2026, 1, 2, tzinfo=timezone.utc),
        "error": None,
        "source_manifest": {"sources": ["profile_master"]},
        "metrics": {
            "published_providers": 1,
            "publication": {"publication": publication, "published_rows": 1},
        },
    }


def test_published_and_partial_flows_are_distinct():
    archive._validate_run(_run())
    for changed in (
        {"metrics": {"published_providers": 0, "publication": {"publication": "skipped_partial", "published_rows": 0}}},
        {"status": "validating"},
        {"source_key": "other"},
    ):
        with pytest.raises(archive.FloridaProjectionArchiveError):
            archive._validate_run({**_run(), **changed})


def test_florida_archive_owns_projection_and_audits():
    assert archive.TABLES == (
        "provider_profile_import_run",
        "provider_profile_artifact",
        "provider_profile_source_record",
        "provider_profile_fact",
        "provider_profile_projection",
    )
    with pytest.raises(archive.FloridaProjectionArchiveError):
        archive._identity({"run_id": "a" * 32, "relation_oid": 0})
