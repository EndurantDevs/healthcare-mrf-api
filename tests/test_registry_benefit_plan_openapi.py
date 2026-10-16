# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Optional selections preserve the existing assertion and trusted route boundary."""

import re

from tests.openapi_route_contract_support import load_openapi_document
from tests.test_openapi_spec import OPENAPI_PATH


def test_benefit_selection_is_optional_bounded_and_uses_permanent_family_ids():
    spec = load_openapi_document(OPENAPI_PATH)
    assertion = spec["components"]["schemas"]["RegistryCompanyNetworkAssertion"]
    assert assertion["additionalProperties"] is False
    assert len(assertion["required"]) == 9 and "benefit_plan_ids" not in assertion["required"]
    selected = assertion["properties"]["benefit_plan_ids"]
    assert selected["maxItems"] == 512 and selected["uniqueItems"] is True
    assert not selected.get("nullable", False)
    assert re.fullmatch(selected["items"]["pattern"], "hpplan_" + "Z" * 26)
    for invalid in ("hpversion_" + "0" * 26, "hpplan_" + "a" * 26, "hpplan_" + "I" * 26):
        assert re.fullmatch(selected["items"]["pattern"], invalid) is None
    assert "5000" in selected["description"] and "503" in selected["description"]
    for path in (
        "/registry/manage/{kind}",
        "/registry/manage/company_links/batch",
        "/registry/manage/{kind}/{record_id}/undo",
    ):
        operation = spec["paths"][path]["post"]
        assert operation["security"] == [{"RegistryControlBearer": []}, {"RegistryControlHeader": []}]
        assert "503" in operation["responses"]
