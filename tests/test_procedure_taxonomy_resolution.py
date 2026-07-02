# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
import types
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import pytest

from api import ptg2_serving

MODULE_PATH = Path(__file__).resolve().parents[1] / "api" / "endpoint" / "pricing.py"
MODULE_SPEC = spec_from_file_location("pricing_endpoint_taxonomy_unit", MODULE_PATH)
pricing_module = module_from_spec(MODULE_SPEC)
assert MODULE_SPEC and MODULE_SPEC.loader
MODULE_SPEC.loader.exec_module(pricing_module)

resolve_procedure_taxonomy = pricing_module.resolve_procedure_taxonomy


def make_request(args=None):
    return types.SimpleNamespace(
        args=args or {},
        ctx=types.SimpleNamespace(sa_session=types.SimpleNamespace()),
    )


@pytest.mark.asyncio
async def test_resolve_procedure_taxonomy_psychotherapy_uses_behavioral_health_scope(monkeypatch):
    async def fake_resolve_internal_codes(_session, _code, _args, default_system=None):
        return [190837], {
            "input_code": {"code_system": default_system, "code": "90837"},
            "resolved_codes": [{"code_system": pricing_module.INTERNAL_PROCEDURE_CODE_SYSTEM, "code": "190837"}],
            "internal_codes": [190837],
            "matched_via": [],
            "expanded": False,
        }

    async def fake_load_evidence(_session, *, year, internal_codes, limit):
        assert year == 2023
        assert internal_codes == [190837]
        assert limit == 10
        return []

    monkeypatch.setattr(pricing_module, "_resolve_internal_codes_for_request", fake_resolve_internal_codes)
    monkeypatch.setattr(pricing_module, "_load_procedure_taxonomy_evidence", fake_load_evidence)
    request = make_request(args={"code": "90837", "code_system": "CPT", "year": "2023"})

    response = await resolve_procedure_taxonomy(request)
    payload = json.loads(response.body)

    assert payload["resolution"]["status"] == "resolved"
    assert payload["resolution"]["recommended_mode"] == "soft_boost"
    assert payload["resolution"]["taxonomy_source"] == "curated_code_range"
    assert "101YP2500X" in payload["resolution"]["taxonomy_codes"]
    assert "1041C0700X" in payload["resolution"]["taxonomy_codes"]
    assert "2084P0800X" in payload["resolution"]["taxonomy_codes"]
    assert payload["resolution"]["provider_boost"]["primary_only"] is False
    assert payload["resolution"]["provider_filter"] is None


def test_psychotherapy_cpt_infers_behavioral_health_taxonomy_for_ptg2_serving():
    rule = ptg2_serving._inferred_provider_taxonomy_rule({"code": "90837", "code_system": "cpt"})
    assert rule is not None
    assert "101YP2500X" in rule.taxonomy_codes
    assert "1041C0700X" in rule.taxonomy_codes
    assert "2084P0800X" in rule.taxonomy_codes
    assert "psychotherapy" in rule.display_terms
