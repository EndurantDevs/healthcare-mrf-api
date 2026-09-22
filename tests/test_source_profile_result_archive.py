# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Only completed self-contained assertion graphs cross the archive boundary."""

import pytest

from process import source_profile_result_archive as archive
from tests.source_profile_archive_support import retained_run


@pytest.mark.parametrize("importer", archive.SOURCES)
def test_completed_source_result_has_no_live_reference_dependency(importer):
    run = retained_run(importer)
    assert archive._validate_run(importer, run) == {}
    assert archive.source_spec(importer).dependencies == ()
    for field, replacement in (
        ("status", "running"),
        ("source_key", "other-source"),
        ("metrics", {"published": False}),
        ("finished_at", None),
    ):
        with pytest.raises(archive.SourceProfileArchiveError):
            archive._validate_run(importer, {**run, field: replacement})
    run["source_manifest"]["max_providers"] = 1
    with pytest.raises(archive.SourceProfileArchiveError):
        archive._validate_run(importer, run)


def test_projection_control_and_local_authority_are_not_portable():
    with pytest.raises(archive.SourceProfileArchiveError, match="unsupported"):
        archive.source_spec("florida-mqa-profile")
    assert archive.TABLES == (
        "provider_profile_import_run",
        "provider_profile_artifact",
        "provider_profile_source_record",
        "provider_profile_fact",
    )
    with pytest.raises(archive.SourceProfileArchiveError):
        archive.result_dependencies({"npi": "a" * 64})
    with pytest.raises(archive.SourceProfileArchiveError):
        archive.stage_schema("peer-selected")


@pytest.mark.parametrize("importer", archive.SOURCES)
@pytest.mark.parametrize("fault", ["agency", "categories", "missing_registry"])
def test_unservable_publication_scope_is_rejected(importer, fault):
    run = retained_run(importer)
    manifest = run["source_manifest"]
    if fault == "agency":
        manifest["source"]["agency"] = "Incorrect agency"
    elif fault == "categories":
        manifest["categories"] = ["unsupported"]
    else:
        del manifest["source"]["registry_generation"]
    with pytest.raises(archive.SourceProfileArchiveError):
        archive._validate_run(importer, run)


@pytest.mark.parametrize("importer", ["tennessee-tdh-profile", "rhode-island-doh-profile", "new-york-nypp-profile"])
@pytest.mark.parametrize("field", ["coverage_scope", "registry_generation"])
def test_registry_bound_publication_scope_is_preserved(importer, field):
    run = retained_run(importer)
    run["source_manifest"]["source"][field] = "different"
    with pytest.raises(archive.SourceProfileArchiveError, match="serving scope differs"):
        archive._validate_run(importer, run)
