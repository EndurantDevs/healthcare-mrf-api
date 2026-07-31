# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from unittest.mock import ANY, AsyncMock

import pytest

from api import ptg2_serving as serving
from tests.ptg2_serving_coverage_paydown_support import strict_v3_tables


@pytest.mark.asyncio
async def test_spatial_location_query_fails_closed_without_geo_capability(
    monkeypatch,
):
    address_table_lookup = AsyncMock(
        return_value="mrf.entity_address_unified"
    )
    monkeypatch.setattr(
        serving,
        "_ptg2_address_serving_table",
        address_table_lookup,
    )
    capability_check = AsyncMock(return_value=False)
    monkeypatch.setattr(
        serving,
        "is_provider_address_geo_capability_available",
        capability_check,
    )

    with pytest.raises(
        serving.PTG2ManifestArtifactError,
        match="requires canonical ZIP geometry",
    ):
        await serving._membership_location_query(
            object(),
            strict_v3_tables(),
            {"zip5": "00000"},
            candidate_npis=None,
            limit=10,
        )

    capability_check.assert_awaited_once_with(
        ANY,
        schema_name=serving.PTG2_SCHEMA,
    )
    assert address_table_lookup.await_args.args[1] == (
        serving._PTG2_UNIFIED_ADDRESS_COLUMNS
    )


@pytest.mark.asyncio
async def test_text_location_filter_requires_complete_unified_schema(monkeypatch):
    address_table_lookup = AsyncMock(return_value="mrf.npi_address")
    monkeypatch.setattr(
        serving,
        "_ptg2_address_serving_table",
        address_table_lookup,
    )

    with pytest.raises(
        serving.PTG2ManifestArtifactError,
        match="location filtering requires unified source-backed addresses",
    ):
        await serving._membership_address_table_for_request(
            object(),
            {"state": "TS"},
        )

    assert address_table_lookup.await_args.args[1] == (
        serving._PTG2_UNIFIED_ADDRESS_COLUMNS
    )
