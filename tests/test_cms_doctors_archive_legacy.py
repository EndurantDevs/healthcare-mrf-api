# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""The historical payload exception is limited to the exact CMS two-table family."""

from copy import deepcopy
from dataclasses import replace
from unittest.mock import AsyncMock
from uuid import uuid4

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


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["empty", "populated", "invalid_schema", "current"])
async def test_legacy_stage_observes_the_complete_owned_family_and_preserves_guards(monkeypatch, stage):
    manifest = archive.validate_reference_family_manifest(_manifest())
    group = archive.ReferenceTableReceipt(
        "CMSDoctorGroupSite", "cms_doctor_group_site", "b" * 64, int(stage == "populated")
    )
    tables = (*manifest.tables, group)
    if stage == "current":
        manifest = replace(manifest, tables=tables, schema_sha256=archive._schema_digest(tables))
    receipts_by_table = {table.table_name: table for table in tables}
    read = AsyncMock(side_effect=lambda _session, **options: receipts_by_table[options["model_type"].__tablename__])
    schema_guard = AsyncMock(
        side_effect=archive.ReferenceFamilyArchiveError("synthetic group schema differs")
        if stage == "invalid_schema"
        else None
    )
    monkeypatch.setattr(archive, "_table_receipt", read)
    monkeypatch.setattr(archive, "_require_legacy_cms_group_schema", schema_guard)
    ownership = archive.ReferenceFamilyStageOwnership(
        "cms-doctors",
        uuid4(),
        "synthetic_stage",
        1,
        tuple((table.table_name, index + 2) for index, table in enumerate(tables)),
    )
    if stage in {"populated", "invalid_schema"}:
        reason = "restored stage differs" if stage == "populated" else "synthetic group schema differs"
        with pytest.raises(archive.ReferenceFamilyArchiveError, match=reason):
            await archive._validate_stage_manifest(object(), ownership=ownership, manifest=manifest)
    else:
        assert await archive._validate_stage_manifest(object(), ownership=ownership, manifest=manifest) == tables
    assert tuple(call.kwargs["model_type"].__tablename__ for call in read.await_args_list) == tuple(receipts_by_table)
    if stage in {"empty", "invalid_schema"}:
        schema_guard.assert_awaited_once()
        assert schema_guard.await_args.args[1] == ownership.schema_name
    else:
        schema_guard.assert_not_awaited()
