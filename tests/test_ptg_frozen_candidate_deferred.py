# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Deferred logical-hash candidate corroboration contracts."""

from __future__ import annotations

import pytest

from process.ptg_parts.frozen_rate_candidate import (
    validate_frozen_candidate_evidence,
)
from process.ptg_parts.frozen_rate_files import FrozenRateFileMismatchError
from tests.test_ptg_frozen_candidate_audit import _candidate_fixture


def test_candidate_accepts_deferred_raw_alias_without_changing_declaration():
    """A deferred declaration remains null while observed bytes use raw SHA."""

    manifest, binding, database_sources = _candidate_fixture(
        deferred_first=True
    )
    descriptor = manifest["frozen_rate_files"][0]
    source_version = manifest["source_file_versions"][0]
    database_source = database_sources[0]

    assert descriptor["logical_sha256"] is None
    assert descriptor["logical_hash_deferred"] is True
    assert source_version["logical_sha256"] == descriptor["raw_sha256"]
    assert (
        database_source["version_logical_sha256"]
        == descriptor["raw_sha256"]
    )
    assert validate_frozen_candidate_evidence(
        manifest,
        candidate_run_id="ptg2:source-file-import-001",
        database_binding=binding,
        database_sources=database_sources,
    )


@pytest.mark.parametrize("evidence_location", ["manifest", "database"])
def test_candidate_rejects_deferred_source_version_without_raw_alias(
    evidence_location,
):
    """Deferred source-version evidence must retain the observed raw alias."""

    manifest, binding, database_sources = _candidate_fixture(
        deferred_first=True
    )
    if evidence_location == "manifest":
        manifest["source_file_versions"][0]["logical_sha256"] = None
    else:
        database_sources[0]["version_logical_sha256"] = None

    with pytest.raises(
        FrozenRateFileMismatchError,
        match="source-version|database source",
    ):
        validate_frozen_candidate_evidence(
            manifest,
            candidate_run_id="ptg2:source-file-import-001",
            database_binding=binding,
            database_sources=database_sources,
        )
