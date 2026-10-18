# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real Geo publication binds native address repair to one immutable lookup cut."""

import asyncio
import importlib
from contextlib import asynccontextmanager
from contextvars import copy_context
from datetime import UTC, datetime

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from db import models
from process import geo_import
from process import provider_directory_output_address_key_backfill as addresses
from process.ext import address_fast
from process.provider_directory_generic_source_policy import source_transaction
from tests import provider_directory_source_postgres_support as source
from tests.cms_output_snapshot_postgres_support import provision_native_outputs
from tests.geo_source_postgres_support import geo_source_component_database
from tests.provider_directory_dataset_rehydrate_postgres_support import _migrations
from tests.test_geo_reference_family_postgres import _load_snapshot

directory = importlib.import_module("process.provider_directory_fhir")
SOURCE = "address-example"
RUN = "address-run"


@asynccontextmanager
async def _address_scope(monkeypatch, tmp_path):
    # Keep the Geo fixture's original bootstrap and three migrations; add current output prerequisites.
    output_migrations = _migrations()
    with monkeypatch.context() as settings:
        settings.setattr(source, "source_component_migrations", lambda: output_migrations)
        async with geo_source_component_database(settings) as component:
            settings.setattr(directory, "db", component.database)
            settings.setattr(geo_import, "db", component.database)
            observed_at = datetime(2026, 1, 1, tzinfo=UTC).replace(tzinfo=None)
            async with component.admin_engine.begin() as connection:
                await connection.execute(
                    models.ProviderDirectoryLocation.__table__.insert(),
                    [
                        {
                            "source_id": SOURCE,
                            "resource_id": resource,
                            "name": "Example office",
                            "first_line": "123 Main Street",
                            "second_line": suite,
                            "state_name": "36",
                            "postal_code": "10001-1234",
                            "country_code": "840",
                            "fhir_meta": {"versionId": "original"},
                            "last_seen_run_id": RUN,
                            "observed_at": observed_at,
                            "updated_at": observed_at,
                        }
                        for resource, suite in (("office-a", "Suite 101"), ("office-b", "Suite 202"))
                    ],
                )
            await provision_native_outputs(component)
            authority = await _load_snapshot(component, tmp_path, "New York")
            assert authority.serving_generation is not None
            yield component, authority


async def _locations(component):
    async with source_transaction(component.database):
        oid = await component.database.scalar("SELECT 'mrf.provider_directory_location'::regclass::oid::bigint")
        rows = [
            dict(row._mapping)
            for row in await component.database.all(
                "SELECT * FROM mrf.provider_directory_location ORDER BY source_id,resource_id"
            )
        ]
        return oid, rows


@pytest.mark.asyncio
async def test_native_address_keys_publish_distinct_offices_and_preserve_complete_tuples(monkeypatch, tmp_path):
    async with _address_scope(monkeypatch, tmp_path) as (component, _authority):
        before_oid, before = await _locations(component)
        expected_keys = address_fast._fast_module().canonicalize_location_batch(
            [("123 Main Street", suite, "New York", "NY", "10001-1234", "US") for suite in ("Suite 101", "Suite 202")]
        )
        assert (
            await addresses.publish_location_address_keys(directory, run_id=RUN, source_ids=[SOURCE], batch_size=1) == 2
        )
        after_oid, after = await _locations(component)
        assert after_oid != before_oid
        assert len({location["address_key"] for location in after}) == 2
        for previous, current, canonical in zip(before, after, expected_keys, strict=True):
            assert current == dict(
                previous,
                address_key=canonical[0],
                zip5="10001",
                city_name="New York",
                state_name="NY",
                state_code=canonical[1],
                city_norm=canonical[2],
                country_code="US",
                updated_at=current["updated_at"],
            )
        async with component.admin_engine.begin() as connection:
            assert await connection.scalar(
                text("SELECT EXISTS(SELECT FROM pg_class WHERE oid=:oid)"), {"oid": before_oid}
            )
        assert (
            await addresses.publish_location_address_keys(directory, run_id=RUN, source_ids=[SOURCE], batch_size=1) == 0
        )
        assert await _locations(component) == (after_oid, after)


@pytest.mark.asyncio
async def test_native_late_geo_publication_refuses_address_candidate_and_preserves_incumbent(monkeypatch, tmp_path):
    async with _address_scope(monkeypatch, tmp_path) as (component, authority):
        before = await _locations(component)
        factory_context = copy_context()
        lock_geo_cut = addresses._lock_geo_cut
        successor_authorities = []

        async def publish_geo_then_check(capability, expected):
            # Commit through the actual producer after the pinned reader closes, then retain the real fence.
            successor_authorities.append(
                await asyncio.create_task(
                    _load_snapshot(component, tmp_path, "Changed City"), context=factory_context.copy()
                )
            )
            await lock_geo_cut(capability, expected)

        monkeypatch.setattr(addresses, "_lock_geo_cut", publish_geo_then_check)
        with pytest.raises(DBAPIError) as refused:
            await addresses.publish_location_address_keys(directory, run_id=RUN, source_ids=[SOURCE], batch_size=1)
        assert refused.value.orig.sqlstate == "40001"
        assert "reference_family_result_generation" in refused.value.statement
        assert "FOR SHARE NOWAIT" in refused.value.statement
        assert len(successor_authorities) == 1
        assert successor_authorities[0].local_generation > authority.local_generation
        assert successor_authorities[0].relation_oids != authority.relation_oids
        assert await _locations(component) == before
        async with source_transaction(component.database):
            assert (
                await component.database.scalar("SELECT city FROM mrf.geo_zip_lookup WHERE zip_code='10001'")
                == "Changed City"
            )
