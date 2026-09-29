# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native compare-and-swap proof for a desired snapshot date transition."""

from dataclasses import replace
from types import SimpleNamespace

import pytest

from process import provider_directory_profile_capacity as capacity
from tests.provider_directory_profile_delta_publication import _prepared_scenario_delta
from tests.provider_directory_profile_delta_scenario import (
    _delta_lineage,
    _delta_relation_oid_by_name,
    _delta_relation_scenario,
    _insert_delta_serving_generation,
)
from tests.provider_directory_profile_delta_test_support import _delta_database, importer
from tests.test_provider_directory_profile_capacity import _geometry_payload


@pytest.mark.asyncio
@pytest.mark.parametrize("base_date", ["2026-07-30", "2026-07-29"])
async def test_desired_date_cas_advances_only_the_exact_incumbent(monkeypatch, base_date):
    async with _delta_database(monkeypatch) as (database, schema):
        monkeypatch.setattr(importer, "db", database)
        scenario = await _delta_relation_scenario(database, schema)
        lineage = _delta_lineage()
        oid_by_name = await _delta_relation_oid_by_name(database, scenario)
        await _insert_delta_serving_generation(database, scenario, lineage, oid_by_name)
        geometry = capacity.validated_capacity_geometry(_geometry_payload())
        delta = replace(
            _prepared_scenario_delta(scenario, lineage, oid_by_name, geometry),
            profile_as_of="2026-07-31",
            from_profile_as_of=base_date,
        )
        forecast = SimpleNamespace(forecast_hash="a" * 64)
        counts_by_name = {"evidence_rows": 2, "profile_rows": 1}
        if base_date == "2026-07-30":
            await importer._update_profile_delta_serving_generation(delta, counts_by_name, forecast)
            assert await database.scalar(f"SELECT profile_as_of FROM {scenario.serving_ref}") == "2026-07-31"
        else:
            with pytest.raises(importer.ProviderDirectoryArtifactBuildStale, match="serving_generation_changed"):
                await importer._update_profile_delta_serving_generation(delta, counts_by_name, forecast)
            assert await database.scalar(f"SELECT profile_as_of FROM {scenario.serving_ref}") == "2026-07-30"
