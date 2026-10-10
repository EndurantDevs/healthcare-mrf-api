# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native readiness distinguishes the archive preimage from its owned merge."""

from types import SimpleNamespace

import pytest

from process import provider_directory_cms_archive as archive
from process.provider_directory_cms_preparation import PreparedServingArtifacts
from tests.cms_npd_admission_postgres_support import _database_url
from tests.test_provider_directory_cms_archive_postgres import _fixture, _prepare, fhir
from tests.test_provider_directory_cms_resource_batch_postgres import cms_resource_template as cms_resource_template


def _bundle(delta, fence, admission):
    return PreparedServingArtifacts(
        fhir=fhir,
        fence=fence,
        execution=None,
        nonprofile_admission=admission,
        nonprofile_bundle=SimpleNamespace(archive_delta=delta),
        profile_bundle=None,
        metrics={},
        relation_overrides={},
        overlay_identity=None,
    )


@pytest.mark.asyncio
async def test_archive_readiness_requires_the_exact_owned_applied_revision(monkeypatch):
    owned_database = _database_url().database
    monkeypatch.setenv("HLTHPRT_DB_DATABASE", owned_database)
    monkeypatch.setenv("HLTHPRT_DB_DATABASE_OVERRIDE", owned_database)
    async with _fixture(monkeypatch) as (database, schema, fence, admission, _checks):
        delta, _metrics = await _prepare(schema, fence)
        prepared = _bundle(delta, fence, admission)
        try:
            async with database.transaction() as session:
                await prepared.assert_ready(cutover=True)
                with pytest.raises(RuntimeError, match="cms_archive_preparation_changed"):
                    await prepared.assert_ready(cutover=True, archive_applied=True)

            async with database.transaction() as session:
                await delta.before_lock(fhir, session)
                await delta.apply(fhir, session)
                assert await archive._revision(database, schema, delta.target_oid) == delta.from_revision + 2
                await prepared.assert_ready(cutover=True, archive_applied=True)
                with pytest.raises(RuntimeError, match="cms_archive_preparation_changed"):
                    await prepared.assert_ready(cutover=True)

            async with database.transaction():
                with pytest.raises(RuntimeError, match="cms_archive_applied_owner_changed"):
                    await prepared.assert_ready(cutover=True, archive_applied=True)
        finally:
            await delta.cleanup(fhir)
