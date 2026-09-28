# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""The historical payload exception is limited to the exact CMS two-table family."""

from copy import deepcopy

import pytest

from process import reference_family_archive as archive


def _manifest():
    receipts = tuple(
        archive.ReferenceTableReceipt(model.__name__, model.__tablename__, "a" * 64, 1)
        for model in archive.reference_family_spec("cms-doctors").model_types[:2]
    )
    metadata, digest = archive._source_metadata({"release": "synthetic-legacy"})
    return archive.ReferenceFamilyManifest(
        "cms-doctors",
        receipts,
        metadata,
        digest,
        archive._schema_digest(receipts),
    ).as_dict()


def test_legacy_manifest_roundtrips_without_rewriting_payload_or_authority():
    original = _manifest()
    assert archive.validate_reference_family_manifest(original).as_dict() == original
    assert len(archive.reference_family_spec("cms-doctors").table_names) == 3


@pytest.mark.parametrize("mutation", ["order", "group_substitution", "missing", "extra", "other_family", "digest"])
def test_legacy_exception_rejects_other_table_sets_and_invalid_digest(mutation):
    manifest = deepcopy(_manifest())
    match mutation:
        case "order":
            manifest["tables"].reverse()
        case "group_substitution":
            manifest["tables"][1].update(model_name="CMSDoctorGroupSite", table_name="cms_doctor_group_site")
        case "missing":
            manifest["tables"].pop()
        case "extra":
            manifest["tables"].extend(manifest["tables"])
        case "other_family":
            manifest["importer_id"] = "plan-attributes"
        case "digest":
            manifest["schema_sha256"] = "b" * 64
    with pytest.raises(archive.ReferenceFamilyArchiveError):
        archive.validate_reference_family_manifest(manifest)


def test_legacy_stage_receipts_require_exact_payload_and_empty_third_relation():
    manifest = archive.validate_reference_family_manifest(_manifest())
    empty = archive.ReferenceTableReceipt("CMSDoctorGroupSite", "cms_doctor_group_site", "b" * 64, 0)
    assert archive._has_matching_manifest_stage_tables(manifest, (*manifest.tables, empty))
    assert not archive._has_matching_manifest_stage_tables(manifest, manifest.tables)
    populated = archive.ReferenceTableReceipt(empty.model_name, empty.table_name, empty.schema_sha256, 1)
    assert not archive._has_matching_manifest_stage_tables(manifest, (*manifest.tables, populated))
    assert not archive._has_matching_manifest_stage_tables(manifest, (*reversed(manifest.tables), empty))
