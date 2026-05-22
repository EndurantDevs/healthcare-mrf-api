# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import types

import pytest
import sanic.exceptions

from api.endpoint import npi as npi_module


@pytest.mark.asyncio
async def test_get_all_rejects_section_without_classification_or_codes():
    request = types.SimpleNamespace(
        args={"section": "Individual", "limit": "1"},
        app=types.SimpleNamespace(),
    )

    with pytest.raises(sanic.exceptions.InvalidUsage):
        await npi_module.get_all(request)


@pytest.mark.asyncio
async def test_get_near_rejects_section_without_classification_or_codes():
    request = types.SimpleNamespace(
        args={"section": "Individual", "long": "-87.0", "lat": "41.0"},
        app=types.SimpleNamespace(),
    )

    with pytest.raises(sanic.exceptions.InvalidUsage):
        await npi_module.get_near_npi(request)


def test_section_filter_guard_allows_classification_or_codes():
    npi_module._validate_section_filters("Individual", "Clinic/Center", None)
    npi_module._validate_section_filters("Individual", None, ["261Q00000X"])
