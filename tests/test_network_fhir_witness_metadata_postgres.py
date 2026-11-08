# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic native COPY verifies witness projection without creating Source authority."""

import asyncio
import os
from dataclasses import replace
from functools import partial
from tempfile import TemporaryDirectory

import asyncpg
import pytest

from process import network_fhir_source_witness_publication as publication
from tests.test_network_fhir_witness_metadata import _admitted_inputs


@pytest.mark.asyncio
async def test_native_oversized_witness_metadata_copy_preserves_seal_and_rollback(tmp_path, monkeypatch):
    dsn = os.getenv("NETWORK_REGISTRY_TEST_DSN")
    if not dsn:
        pytest.skip("NETWORK_REGISTRY_TEST_DSN must explicitly select an isolated database")
    connection = await asyncpg.connect(dsn.replace("postgresql+asyncpg://", "postgresql://"))
    try:
        monkeypatch.setattr(publication.retained, "TemporaryDirectory", partial(TemporaryDirectory, dir=tmp_path))
        encoded, origin, header_by_field = _admitted_inputs(tmp_path, oversized=True)
        await connection.execute(
            "CREATE TEMP TABLE provider_directory_endpoint_dataset "
            "(dataset_id text PRIMARY KEY,publication_metadata_json json NOT NULL)"
        )
        await connection.execute(
            "INSERT INTO pg_temp.provider_directory_endpoint_dataset VALUES($1,$2::json)",
            origin.dataset_id,
            encoded.decode(),
        )
        oid = await connection.fetchval("SELECT 'pg_temp.provider_directory_endpoint_dataset'::regclass::oid::bigint")
        origin = replace(origin, schema_name="pg_temp", header_oid=oid)
        header_by_field = {**header_by_field, "oid": oid}
        async with connection.transaction(isolation="repeatable_read", readonly=True):
            assert await connection.fetchval("SHOW transaction_read_only") == "on"
            await publication.verify_publication(connection, origin, header_by_field)
            with pytest.raises(ValueError):
                await publication.verify_publication(connection, origin, {**header_by_field, "admitted_summary": "{}"})
            await publication.verify_publication(connection, origin, header_by_field)
            assert await connection.fetchval("SELECT count(*) FROM pg_cursors WHERE name<>''") == 0
            with monkeypatch.context() as patch:

                def cancel(*_args):
                    raise asyncio.CancelledError

                patch.setattr(publication._SkippedMetadataCapture, "_is_complete_after_event", cancel)
                with pytest.raises(asyncio.CancelledError):
                    await publication.verify_publication(connection, origin, header_by_field)
            await publication.verify_publication(connection, origin, header_by_field)
        assert not connection.is_in_transaction()
        assert list(tmp_path.iterdir()) == [tmp_path / "metadata.copy"]
    finally:
        await connection.close()
    assert connection.is_closed()
