# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib
import os
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from db.models import FacilityAnchor, db
from process import reference_family_archive as archive
from process import reference_family_result_generation as generation
from tests.test_reference_family_result_generation_postgres import _run_migration

anchors = importlib.import_module("process.facility_anchors")
MIGRATION = Path(__file__).resolve().parents[1] / "alembic/versions/20260923000000_facility_address_contribution.py"


def test_facility_family_requires_both_source_relations():
    assert archive.reference_family_spec("facility-anchors").table_names == (
        "facility_anchor",
        "facility_address_contribution",
    )
    assert generation.RELATION_NAMES_BY_IMPORTER["facility-anchors"] == (
        "facility_anchor",
        "facility_address_contribution",
    )


async def test_captured_geocodes_require_archive_but_unhooked_calls_keep_native_skip(monkeypatch):
    monkeypatch.setattr(anchors, "_canonical_archive_table", AsyncMock(return_value=None))
    assert await anchors._refresh_archive_geocodes_from_facility_anchors("synthetic_stage", "mrf") == 0
    with pytest.raises(RuntimeError, match="requires the canonical archive"):
        await anchors._refresh_archive_geocodes_from_facility_anchors(
            "synthetic_stage",
            "mrf",
            contribution_table="synthetic_contribution",
        )


@pytest.mark.asyncio(loop_scope="session")
async def test_native_facility_migration_preserves_legacy_boundary_and_evidence():
    if os.getenv("HP_FACILITY_CAPTURE_POSTGRES_TEST") != "1" or "test" not in os.getenv("HLTHPRT_DB_DATABASE", ""):
        pytest.skip("requires an explicitly enabled disposable migrated test database")
    schema = os.environ.get("HLTHPRT_DB_SCHEMA", "mrf")
    await db.create_table(FacilityAnchor.__table__, checkfirst=True)
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="source generation is unavailable"):
        async with db.transaction() as session:
            await archive.capture_reference_family_source(
                session, importer_id="facility-anchors", schema_name=schema, source_metadata={"source": "synthetic"}
            )
    async with db.transaction() as session:
        authority = await generation.read_reference_family_result_generation_authority(
            session,
            importer_id="facility-anchors",
            schema_name=schema,
        )
        assert authority.local_generation == 0
        assert authority.serving_generation is None
        assert authority.relation_oids is None
        connection = await session.connection()
        await _run_migration(connection, MIGRATION, "downgrade")
        assert await db.scalar(f"SELECT to_regclass('{schema}.facility_address_contribution')") is None
        await _run_migration(connection, MIGRATION, "upgrade")
    with pytest.raises(RuntimeError, match="evidence prevents downgrade"):
        async with db.transaction() as session:
            await db.status(
                f"INSERT INTO {schema}.facility_address_contribution VALUES "
                "('metadata','00000000-0000-0000-0000-000000000000','{}'::jsonb)"
            )
            await _run_migration(await session.connection(), MIGRATION, "downgrade")
    with pytest.raises(RuntimeError, match="evidence prevents downgrade"):
        async with db.transaction() as session:
            await generation.publish_local_reference_family_generation(
                session,
                importer_id="facility-anchors",
                schema_name=schema,
            )
            await _run_migration(await session.connection(), MIGRATION, "downgrade")
    assert await db.scalar(f"SELECT count(*) FROM {schema}.facility_address_contribution") == 0
    assert (
        await db.scalar(
            f"SELECT local_generation FROM {schema}.reference_family_result_generation WHERE importer_id='facility-anchors'"
        )
        == 0
    )
    await db.disconnect()
