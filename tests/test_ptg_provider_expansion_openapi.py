# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from pathlib import Path

import yaml


OPENAPI_PATH = Path("doc/openapi.yaml")


def test_procedure_provider_expansion_is_explicit_opt_in():
    spec_by_field = yaml.safe_load(OPENAPI_PATH.read_text())
    for path in (
        "/pricing/providers/search-by-procedure",
        "/pricing/providers/by-procedure",
    ):
        parameters = spec_by_field["paths"][path]["get"]["parameters"]
        include_providers_by_field = next(
            parameter
            for parameter in parameters
            if parameter["name"] == "include_providers"
        )
        assert include_providers_by_field["schema"]["default"] is False
        assert "provider filters remain active" in include_providers_by_field["description"]
        provider_sex_by_field = next(
            parameter
            for parameter in parameters
            if parameter["name"] == "provider_sex_code"
        )
        assert "include_providers=true" in provider_sex_by_field["description"]
