# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Address-only provider identity survives source filtering within its original selectors."""

import json
from copy import deepcopy
from unittest.mock import AsyncMock

import pytest
from sanic.exceptions import NotFound

from api.endpoint import npi
from tests import test_npi_location_paging as paging
from tests.provider_profile_snapshot_test_support import (
    stub_provider_profile_snapshot as stub_provider_profile_snapshot,
)


def _address_only_detail(monkeypatch, precision):
    paging._install_route_mocks(monkeypatch)
    office = deepcopy(paging._detail_payload()["address_list"][0])
    office["address_precision"] = precision

    async def candidates(_npi, **options):
        if precision is None or options.get("min_address_sources", 0) > office["source_count"]:
            return []
        return [deepcopy(office)]

    candidate_reader = AsyncMock(side_effect=candidates)
    monkeypatch.setattr(npi, "current_network_address_scope", lambda: None)
    monkeypatch.setattr(npi, "_build_npi_details", AsyncMock(return_value={}))
    monkeypatch.setattr(npi, "_fetch_provider_directory_address_overlay", AsyncMock(return_value=[]))
    monkeypatch.setattr(npi, "_fetch_npi_location_candidates", candidate_reader)
    return candidate_reader


@pytest.mark.asyncio
@pytest.mark.parametrize("minimum", [None, 0, 3])
@pytest.mark.parametrize(
    "precision,extra_info,visible",
    [("street", False, True), ("city_zip", False, False), ("city_zip", True, True), (None, False, False)],
)
async def test_address_only_identity_respects_original_visibility(monkeypatch, minimum, precision, extra_info, visible):
    candidates = _address_only_detail(monkeypatch, precision)
    request = paging._detail_request(limit="5", include_profile=False)
    request.args["extra_info"] = str(extra_info).lower()
    if minimum is not None:
        request.args["min_address_sources"] = str(minimum)

    if visible:
        result = await npi.read_npi_detail(request, "1234567890")
        assert result.status == 200
        body_by_field = json.loads(result.body)
        assert body_by_field["npi"] == 1234567890
        expected_count = 0 if minimum else 1
        assert len(body_by_field["address_list"]) == expected_count
        assert body_by_field["address_pagination"]["total"] == expected_count
        assert body_by_field["address_pagination"]["returned"] == expected_count
        assert body_by_field["address_pagination"]["has_more"] is False
    else:
        with pytest.raises(NotFound):
            await npi.read_npi_detail(request, "1234567890")
    assert candidates.await_count == (2 if minimum else 1)
    if minimum:
        assert candidates.await_args_list[0].kwargs["min_address_sources"] == minimum
        assert "min_address_sources" not in candidates.await_args_list[1].kwargs
