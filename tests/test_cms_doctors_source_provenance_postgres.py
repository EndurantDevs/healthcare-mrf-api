# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Actual source validation and retained bytes mint the prepared provenance."""

from datetime import datetime
from unittest.mock import AsyncMock

import pytest

from process import cms_doctors_education as education
from process import cms_doctors_groups as groups
from process import cms_doctors_preparation as preparation
from process.cms_doctors_source_provenance import read_doctors_source_provenance
from tests.cms_doctors_preparation_postgres_support import doctors_database, stage_family, stage_oids
from tests.test_cms_doctors_source_provenance import source_metrics


async def _source_stages(fixture, tmp_path, monkeypatch):
    """Populate the native stage family with all three real parsers' synthetic source rows."""
    native = preparation._native()
    metrics = source_metrics(tmp_path, monkeypatch)
    ctx = await stage_family(fixture.database, fixture.schema)
    ctx["context"].update({name: metrics[name] for name in ("education", "group_site", "artifact")})
    with education.open_doctors_csv(tmp_path / "doctors.csv") as reader:
        row = next(reader)
    manifest = metrics["education"]
    mapped_rows = (
        native.doctor_address_row(row, datetime.fromisoformat(manifest["downloaded_at"])),
        education.doctor_education_row(row, 1, manifest),
        groups.doctor_group_site_row(row, 1, manifest),
    )
    async with fixture.database.transaction() as session:
        for model, mapped in zip(preparation._models(), mapped_rows, strict=True):
            stage = native.make_class(model, ctx["import_date"])
            await session.execute(stage.__table__.delete())
            await session.execute(stage.__table__.insert().values(**mapped))
    return ctx


@pytest.mark.parametrize("failure", [None, "artifact", "education"])
async def test_prepared_provenance_requires_actual_bytes_and_stage_validation(monkeypatch, tmp_path, failure):
    """Exercise real small source validation; identity enrichment is outside this fixture."""
    native = preparation._native()
    real_prepare = native._prepare_cms_doctors_sources
    async with doctors_database(monkeypatch, cms_active=False) as fixture:
        monkeypatch.setattr(native, "_prepare_cms_doctors_sources", real_prepare)
        monkeypatch.setattr(native, "DEFAULT_MIN_ROWS", 1)
        monkeypatch.setattr(education, "MIN_EDUCATION_ROWS", 1)
        monkeypatch.setattr(native, "bind_group_site_organizations", AsyncMock(return_value=0))
        monkeypatch.setattr(native, "bind_cms_doctors_sites", AsyncMock(return_value=0))
        monkeypatch.setattr(native, "_resolve_cms_doctors_addresses", AsyncMock(return_value=None))
        monkeypatch.setattr(native, "raise_if_cancelled", AsyncMock())
        ctx = await _source_stages(fixture, tmp_path, monkeypatch)
        if failure == "artifact":
            (tmp_path / "retained" / ctx["context"]["artifact"]["file_name"]).write_bytes(b"changed")
        if failure == "education":
            ctx["context"]["education"]["generation_id"] = "f" * 64
        if failure:
            with pytest.raises(RuntimeError, match="artifact_changed|education_stage_incomplete"):
                async with preparation.prepare_cms_doctors_generation(ctx):
                    pytest.fail("invalid source cannot produce prepared provenance")
        else:
            async with preparation.prepare_cms_doctors_generation(ctx) as prepared:
                proof = read_doctors_source_provenance(prepared.source_provenance_json)
                assert proof["artifact"] == ctx["context"]["artifact"]
                assert proof["education"] == ctx["context"]["education"]
                assert len(prepared.sealed_filenodes) == 3
                assert prepared.native_receipt is None and not prepared.committed
                assert proof["rows"] == 1
        assert await stage_oids(fixture, ctx) == ()
