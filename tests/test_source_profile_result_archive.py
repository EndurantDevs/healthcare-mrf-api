# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Only completed self-contained assertion graphs cross the archive boundary."""

import hashlib

import pytest

from process import source_profile_result_archive as archive
from tests.source_profile_archive_support import retained_run


def test_historical_pin_guard_ddl_is_unchanged():
    statements = "\n".join(archive.pins.pin_guard_statements("fixture"))
    assert (
        hashlib.sha256(statements.encode()).hexdigest()
        == "d8c97782387576c62ac6e45a391a7c1d057f2bb8592531171ba8ec9ee0f389f4"
    )
    current = "\n".join(archive.pins.statement_pin_guard_statements("fixture"))
    assert "FOR EACH ROW" not in current
    assert current.count("FOR EACH STATEMENT") == 19
    assert "OLD TABLE AS profile_guard_old NEW TABLE AS profile_guard_new" in current


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


def _versioned_manifest(contract):
    return {
        "contract": contract,
        "importer_id": "massachusetts-borim-profile",
        "source_key": "massachusetts-borim",
        "run_id": "a" * 32,
        "run_ids": ["a" * 32],
        "source_completed_at": "2026-01-02T00:00:00+00:00",
        "source_manifest_sha256": "b" * 64,
        "dependencies": {},
        "tables": [
            {
                "table_name": name,
                "row_count": 1,
                "schema_sha256": "c" * 64,
                **({"content_sha256": "d" * 64} if contract == archive.LEGACY_CONTRACT else {}),
            }
            for name in archive.TABLES
        ],
    }


@pytest.mark.parametrize("contract", (archive.CONTRACT, archive.LEGACY_CONTRACT))
def test_versioned_receipts_preserve_exact_original_meaning(contract):
    manifest = _versioned_manifest(contract)
    assert archive.validate_manifest(manifest) == manifest
    assert archive._validation_contract(manifest) == (
        archive.VALIDATION_CONTRACT if contract == archive.CONTRACT else archive.LEGACY_VALIDATION_CONTRACT
    )
    manifest["contract"] = archive.LEGACY_CONTRACT if contract == archive.CONTRACT else archive.CONTRACT
    with pytest.raises(archive.SourceProfileArchiveError, match="table receipt"):
        archive.validate_manifest(manifest)


def test_set_validated_receipt_rejects_legacy_hash_and_unknown_fields():
    for field in ("content_sha256", "approved_by_peer", "payload_sha256"):
        manifest = _versioned_manifest(archive.CONTRACT)
        manifest["tables"][0][field] = "d" * 64
        with pytest.raises(archive.SourceProfileArchiveError, match="table receipt"):
            archive.validate_manifest(manifest)
