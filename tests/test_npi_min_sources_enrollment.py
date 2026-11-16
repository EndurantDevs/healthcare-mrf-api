# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
from copy import deepcopy
from unittest.mock import AsyncMock

import pytest

from api.endpoint import npi
from tests import test_npi_location_paging as paging
from tests.provider_profile_snapshot_test_support import (
    stub_provider_profile_snapshot as stub_provider_profile_snapshot,
)


@pytest.mark.asyncio
@pytest.mark.parametrize("minimum", [0, 3])
async def test_full_detail_filters_uncounted_enrollment_postal_fields(monkeypatch, minimum):
    detail = paging._detail_payload()
    qualified = detail["address_list"][0]
    qualified.update(
        source_count=3,
        independent_source_count=3,
        multi_source_confirmed=True,
        address_sources=["nppes", "mrf", "provider_directory_fhir"],
    )
    monkeypatch.setattr(paging, "_detail_payload", lambda: deepcopy(detail))
    paging._install_route_mocks(monkeypatch)
    enrollment_by_field = {
        "npi": 1234567890,
        "enrollment_id": "synthetic-enrollment",
        "reporting_year": 2025,
        "address_line_1": "900 Unverified Lane",
        "address_line_2": "Suite 200",
        "city": "Example City",
        "state": "IL",
        "zip_code": "60001",
    }
    monkeypatch.setattr(
        npi,
        "_fetch_provider_enrichment_detail",
        AsyncMock(return_value={"summary": None, "enrollments": {"ffs_public": [deepcopy(enrollment_by_field)]}}),
    )
    request = paging._detail_request(limit="5")
    request.args.update(view="full", min_address_sources=str(minimum))

    response = await npi.get_npi(request, "1234567890")

    assert response.status == 200
    response_by_field = json.loads(response.body)
    assert response_by_field["npi"] == 1234567890
    returned_enrollment = response_by_field["provider_enrichment"]["enrollments"]["ffs_public"][0]
    assert returned_enrollment["enrollment_id"] == "synthetic-enrollment"
    assert returned_enrollment["reporting_year"] == 2025
    if minimum:
        assert not {"address_line_1", "address_line_2", "city", "state", "zip_code"} & returned_enrollment.keys()
        assert "900 Unverified Lane" not in response.body.decode()
        assert len(response_by_field["address_list"]) == 1
        assert response_by_field["address_list"][0]["address_key"] == paging.ADDRESS_A
        assert response_by_field["address_list"][0]["source_count"] >= minimum
        assert response_by_field["address_list"][0]["first_line"] == "100 Example Avenue"
    else:
        assert returned_enrollment == enrollment_by_field
        assert len(response_by_field["address_list"]) > 1
    assert enrollment_by_field["address_line_1"] == "900 Unverified Lane"
