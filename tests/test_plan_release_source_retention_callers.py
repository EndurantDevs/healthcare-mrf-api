# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Derived storage retirement preserves canonical release references."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import plan_pricing_code_storage, ptg_membership_storage
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from tests.test_plan_pricing_code_projection import _identity
from tests.test_ptg_membership_projection import PARAMS


@pytest.mark.asyncio
@pytest.mark.parametrize("family", ["membership", "code"])
@pytest.mark.parametrize("storage_state", ["referenced", "legacy", "missing"])
async def test_retirement_reads_canonical_references_in_its_existing_transaction(monkeypatch, family, storage_state):
    monkeypatch.setenv("HLTHPRT_PLAN_REGISTRY_SCHEMA", "canonical_registry")

    async def execute(statement, params=None):
        sql = str(statement)
        rows = []
        if "AS source_header_exists" in sql:
            rows = [
                {
                    "source_header_exists": storage_state != "legacy",
                    "bindings_exist": storage_state == "referenced",
                    "header_exists": True,
                }
            ]
        elif "AS claimed" in sql:
            rows = [{"claimed": False}]
        elif '"canonical_registry"."hp_plan_release_binding"' in sql:
            rows = [{"serving_revision_id": "retained_revision", "role": "in_network", "binding_ordinal": 0}]
        return SimpleNamespace(all=lambda: rows, mappings=lambda: SimpleNamespace(all=lambda: rows))

    session = SimpleNamespace(execute=AsyncMock(side_effect=execute))
    if family == "membership":
        attempt = ptg_membership_storage._require_no_references(session, PARAMS)
    else:
        monkeypatch.setattr(plan_pricing_code_storage, "source_pins", AsyncMock())
        attempt = plan_pricing_code_storage._unreferenced_source_schema(session, _identity())

    if storage_state == "referenced":
        with pytest.raises(PTG2ManifestArtifactError, match="still has retention references"):
            await attempt
    elif storage_state == "missing":
        with pytest.raises(RuntimeError, match="Source release binding storage is missing"):
            await attempt
    else:
        assert await attempt

    statements = [str(call.args[0]) for call in session.execute.call_args_list]
    assert any("AS source_header_exists" in sql for sql in statements)
    assert not any('"mrf"."hp_plan_release_binding"' in sql for sql in statements)
    assert any('"canonical_registry"."hp_plan_release_binding"' in sql for sql in statements) == (
        storage_state == "referenced"
    )
